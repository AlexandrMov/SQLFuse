/*
  Copyright (C) 2013, 2014 Movsunov A.N.

  This file is part of SQLFuse

  SQLFuse is free software: you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.

  SQLFuse is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with SQLFuse.  If not, see <http://www.gnu.org/licenses/>.
*/

#include <sqlfuse.h>
#include <conf/keyconf.h>

#include "msctx.h"
#include "util.h"

#include <string.h>

struct sqlcache {
  GMutex m;
  
  GHashTable *db_table, *app_table, *mask_table;
};

struct sqldeploy {
  GCond cond;
  GMutex lock;
  GTimer *timer;
  volatile int run;
  GThread *thread;
  GSequence *sql_seq;
};

enum action {
  CREP,
  DROP,
  RENAME
};

struct sqlcmd {
  enum action act;
  char *path, *path2;
  unsigned int mstype;

  struct sqlfs_object *obj;

  unsigned int flags;
  char *sql;
};

#define SAFE_REMOVE_ALL(p)						\
  g_mutex_lock(&cache.m);						\
  if (g_hash_table_contains(cache.db_table, p))				\
    g_hash_table_remove(cache.db_table, p);				\
  if (g_hash_table_contains(cache.app_table, p))			\
    g_hash_table_remove(cache.app_table, p);				\
  g_mutex_unlock(&cache.m);


#define CLEAR_DEPLOY()							\
  g_sequence_remove_range(g_sequence_get_begin_iter(deploy.sql_seq),	\
			  g_sequence_get_end_iter(deploy.sql_seq));	\
  g_hash_table_remove_all(cache.mask_table);

#define IS_DIR(object) object->type < 0x08
#define IS_REG(object) object->type >= 0x08

#define IS_SCHOBJ(object) (object->type >= R_FN && object->type <= R_P	\
			   || object->type == R_FT || object->type == R_TF \
			   || object->type == R_IF)
#define IS_TBLCMD(cmd) (cmd->mstype == R_C || cmd->mstype == R_D	\
			|| cmd->mstype == R_PK || cmd->mstype == R_UQ	\
			|| cmd->mstype == R_X || cmd->mstype == R_F)

static struct sqlcache cache;
static struct sqldeploy deploy;

#define CMD_DISABLED 0x0
#define CMD_IDENTITY 0x1
#define CMD_EXECUTED 0x2

static inline void set_flag(struct sqlcmd *cmd, unsigned int flag)
{
  cmd->flags |= 1 << flag;
}

static inline void clear_flag(struct sqlcmd *cmd, unsigned int flag)
{
  cmd->flags &= ~(1 << flag);
}

static inline gboolean is_flag(struct sqlcmd *cmd, unsigned int flag)
{
  return (cmd->flags & (1 << flag));
}

static inline gboolean is_temp(struct sqlfs_ms_obj *obj)
{
  if (obj->type == R_TEMP)
    return TRUE;
  
  return FALSE;
}

static inline gboolean is_masked(const char *path)
{
  struct sqlcmd *cmd = g_hash_table_lookup(cache.mask_table, path);
  if (cmd != NULL) {
    if (cmd->act == DROP)
      return TRUE;

    if (cmd->act == RENAME && !g_strcmp0(cmd->path, path))
      return TRUE;
  }

  return FALSE;
}

static int get_mask_id(const char *path)
{
  gboolean res = FALSE;
  int obj_id = 0;
  struct sqlcmd *cmd = g_hash_table_lookup(cache.mask_table, path);
  if (cmd != NULL) {

    if (cmd->act == CREP) {
      obj_id = cmd->obj->object_id;

      if (cmd->mstype != R_TEMP)
	res = TRUE;
    }

    if (cmd->act == RENAME && g_strcmp0(cmd->path, path)) {
      obj_id = cmd->obj->object_id;
      
      if (cmd->mstype != R_TEMP)
	res = TRUE;
    }
      
  }
  else {
    
    struct sqlfs_ms_obj *obj = g_hash_table_lookup(cache.db_table, path);
    if (obj) {
      obj_id = obj->object_id;
      res = TRUE;
    }
    else {
      obj = g_hash_table_lookup(cache.app_table, path);
      if (obj) {
	obj_id = obj->object_id;
      }
    }
    
  }

  if (res && !obj_id)
    obj_id = g_get_monotonic_time();

  return obj_id;
}

static void do_mask(const char *path, struct sqlcmd *cmd)
{
  if (g_hash_table_contains(cache.mask_table, path))
    g_hash_table_steal(cache.mask_table, path);

  g_hash_table_insert(cache.mask_table, g_strdup(path), cmd);
}

static struct sqlfs_ms_obj * do_find(const char *pathname, GError **error)
{
  GError *terr = NULL;
  GList *list = NULL;

  unsigned int i = 0;
  const char *pn = g_path_skip_root(pathname);
  char **tree = NULL;
  if (pn == NULL || strlen(pn) < 2) {
    pn = pathname;
    tree = g_malloc0_n(2, sizeof(char *));
    *tree = g_strdup(pn);
  }
  else {
    tree = g_strsplit(pn, G_DIR_SEPARATOR_S, -1);
  }
  
  GString *path = g_string_new(NULL);
  
  while(*tree && !terr) {
    g_string_append_printf(path, "%s%s", G_DIR_SEPARATOR_S, *tree);
  
    struct sqlfs_ms_obj *obj = g_hash_table_lookup(cache.db_table, path->str);
    
    if (!obj) {
      if (i == 0) {
	obj = find_ms_object(NULL, *tree, &terr);
      }
      if (i > 0) {
	obj = find_ms_object(g_list_last(list)->data, *tree, &terr);
      }
    }
    
    if (!terr) {
      
      if (obj && obj->name) {
	list = g_list_append(list, obj);
	if (!g_hash_table_contains(cache.db_table, path->str)) {
	  g_mutex_lock(&cache.m);
	  g_hash_table_insert(cache.db_table, g_strdup(path->str), obj);
	  g_mutex_unlock(&cache.m);
	}
      }
      else {
	g_set_error(&terr, EENOTFOUND, EENOTFOUND,
		    "%d: Object not found\n", __LINE__);
	break;
      }
      
    } else {
      g_clear_error(&terr);
      g_set_error(&terr, EERES, EERES,
		  "%d: Find object failed\n", __LINE__);
      break;
    }
    
    tree++;
    i++;
  }

  struct sqlfs_ms_obj *result = NULL;
  
  GList *last = g_list_last(list);
  if (last && terr == NULL)
    result = last->data;
  
  g_list_free(list);
  
  tree -= i;
  g_strfreev(tree);
  g_string_free(path, TRUE);
  
  if (terr != NULL)
    g_propagate_error(error, terr);

  return result;
}

static struct sqlfs_ms_obj * find_cache_obj(const char *pathname, GError **error)
{
  if (!g_path_is_absolute(pathname))
    return NULL;

  GError *terr = NULL;

  // объект в кэше удалён
  if (is_masked(pathname)) {	
    g_set_error(&terr, EENOTFOUND, EENOTFOUND,
		"%d: Object not found\n", __LINE__);
  }
  
  struct sqlfs_ms_obj *obj = g_hash_table_lookup(cache.app_table, pathname);
  if (terr == NULL) {
    if (!obj)
      obj = g_hash_table_lookup(cache.db_table, pathname);
    
    if (!obj) {
      obj = do_find(pathname, &terr);
    }
  }

  if (terr != NULL)
    g_propagate_error(error, terr);

  return obj;
}

static inline struct sqlfs_object * ms2sqlfs(struct sqlfs_ms_obj *src)
{
  struct sqlfs_object *result = g_try_new0(struct sqlfs_object, 1);
  g_rw_lock_init(&result->lock);
  
  result->object_id = src->object_id;
  result->name = g_strdup(src->name);
  
  if (IS_DIR(src))
    result->type = SF_DIR;
  else {
    result->type = SF_REG;
    
    result->len = src->len;
    if (src->len > 0)
      result->def = g_strdup(src->def);
  }
  
  result->ctime = src->ctime;
  result->mtime = src->mtime;
  result->cached_time = g_get_monotonic_time();
  
  return result;
}

static void free_sqlcmd_object(gpointer object)
{
  if (object == NULL)
    return ;

  struct sqlcmd *obj = (struct sqlcmd *) object;

  if (obj->path != NULL)
    g_free(obj->path);

  if (obj->path2 != NULL)
    g_free(obj->path2);

  if (obj->sql != NULL)
    g_free(obj->sql);

  if (obj->obj != NULL)
    free_sqlfs_object(obj->obj);

  if (obj != NULL) {
    g_free(obj);
    obj = NULL;
  }
  
}

static inline struct sqlcmd * start_cache()
{
  g_mutex_lock(&deploy.lock);
  struct sqlcmd *cmd = g_try_new0(struct sqlcmd, 1);
  cmd->flags = 0;
  
  return cmd;
}

static inline void lock_cache() {
  g_mutex_lock(&deploy.lock);
}

static inline void end_cache() {
  g_timer_start(deploy.timer);
  
  g_cond_signal(&deploy.cond);
  g_mutex_unlock(&deploy.lock);
}

static inline gboolean pause_timer()
{
  if (g_mutex_trylock(&deploy.lock)) {
    g_timer_stop(deploy.timer);
    return TRUE;
  }

  return FALSE;
}

static inline void continue_timer(gboolean locked) {
  if (locked) {
    g_timer_continue(deploy.timer);
    g_mutex_unlock(&deploy.lock);
  }
}

static void insert2cache_sorted(struct sqlcmd *cmd)
{
  if (cmd->mstype == R_COL || IS_TBLCMD(cmd)) {

    gchar *cc = g_path_get_dirname(cmd->path);
    GSequenceIter *mark = NULL;
    GSequenceIter *iter = g_sequence_get_end_iter(deploy.sql_seq);
    while(!g_sequence_iter_is_begin(iter)) {
      iter = g_sequence_iter_prev(iter);
      struct sqlcmd *pcmd = g_sequence_get(iter);
      gchar *pc = g_path_get_dirname(pcmd->path);

      // смотреть дальше создания таблицы нет смысла
      if ((pcmd->mstype == D_U && pcmd->act == CREP || pcmd->act == RENAME)
	  && !g_strcmp0(pc, cmd->path)) {
	g_free(pc);
	break;
      }
    
      // только в одном каталоге
      if (!g_strcmp0(pc, cc)) {
	
	if (IS_TBLCMD(pcmd) && cmd->mstype == R_COL && cmd->act == CREP) {
	  mark = iter;
	}

	if (IS_TBLCMD(cmd) && pcmd->mstype == R_COL && cmd->act == DROP) {
	  mark = iter;
	}

	// не смотреть дальше других операций
	if (mark != NULL && pcmd->act != cmd->act) {
	  g_free(pc);
	  break;
	}
      }
      else
	// другая последовательность
	if (mark != NULL) {
	  g_free(pc);
	  break;
	}
      
      g_free(pc);
    }

    if (mark == NULL)
      g_sequence_append(deploy.sql_seq, cmd);
    else
      g_sequence_insert_before(mark, cmd);
    
    g_free(cc);
  }
  else
    g_sequence_append(deploy.sql_seq, cmd);
}

static void crep_object(const char *path, struct sqlcmd *cmd,
			struct sqlfs_ms_obj *obj)
{
  cmd->path = g_strdup(path);
  cmd->mstype = obj->type;
  
  if (obj->type == R_COL && obj->column)
    if (obj->column->identity)
      set_flag(cmd, CMD_IDENTITY);
    else
      clear_flag(cmd, CMD_IDENTITY);

  cmd->act = CREP;

  gboolean stop = TRUE;
  GString *sql = g_string_new(NULL);
  gchar **schema = g_strsplit(g_path_skip_root(path), G_DIR_SEPARATOR_S, -1);
  GSequenceIter *iter = g_sequence_get_end_iter(deploy.sql_seq);
  while(!g_sequence_iter_is_begin(iter)) {
    iter = g_sequence_iter_prev(iter);
    struct sqlcmd *pcmd = g_sequence_get(iter);

    // операция была выключена при рекурсивном удалении директории
    if (IS_DIR(obj) && is_flag(pcmd, CMD_DISABLED)) {

      gchar *dc = g_path_get_dirname(pcmd->path);

      // включать операции только текущего каталога (не подкаталогов)
      if (!g_strcmp0(cmd->path, dc)) {
	clear_flag(pcmd, CMD_DISABLED);
	stop = FALSE;
      }
      
      g_free(dc);

    }

    // включить колонку в таблицу
    if (pcmd->act == CREP && pcmd->mstype == D_U && cmd->mstype == R_COL
	&& g_str_has_prefix(cmd->path, pcmd->path)
	&& !is_flag(pcmd, CMD_EXECUTED)) {
      
      if (g_str_has_suffix(pcmd->sql, "("))
	g_string_append_printf(sql, "%s\n\t%s", pcmd->sql, cmd->sql);
      else
	g_string_append_printf(sql, "%s,\n\t%s", pcmd->sql, cmd->sql);
      
      g_free(pcmd->sql);
      pcmd->sql = g_strdup(sql->str);
	
      set_flag(cmd, CMD_EXECUTED);
      break;
    }

    if (!g_strcmp0(cmd->path, pcmd->path)) {

      // колонка уже включена в состав таблицы, либо редактировалась ранее
      if (pcmd->mstype == R_COL && pcmd->act != DROP) {
	g_string_append_printf(sql, "ALTER TABLE [%s].[%s]",
			       *schema, *(schema + 1));

	if (pcmd->act == CREP) {
	  
	  // не допускать редактирование колонок IDENTITY
	  if (is_flag(cmd, CMD_IDENTITY)) {
	    set_flag(cmd, CMD_EXECUTED);
	  }
	  else {
	    g_string_append_printf(sql, " ALTER COLUMN ");
	  }
	  
	}
	else {
	  g_string_append(sql, " ADD ");
	}
	
	g_string_append_printf(sql, "%s", cmd->sql);
	
	g_free(cmd->sql);
	cmd->sql = g_strdup(sql->str);
	
	break;
      }
      
      // удалённый объект имеет разный тип с добавленным
      // или произошло переименование, - использовать CREATE
      if (pcmd->mstype != obj->type && obj->type != R_TEMP && pcmd->act == DROP
	  || pcmd->act == RENAME) {
	obj->object_id = 0;
      }

      // объект удалён, имеет один тип с новым или новый временный, -
      // информация об удалении не нужна, использовать ALTER
      if ((pcmd->mstype == obj->type || obj->type == R_TEMP)
	  && pcmd->act == DROP) {
	obj->object_id = pcmd->obj->object_id;

	// не допускать пересоздание колонок IDENTITY
	if (is_flag(pcmd, CMD_IDENTITY))
	  set_flag(cmd, CMD_EXECUTED);
	
	g_sequence_remove(iter);
	iter = NULL;

	// не пересоздавать схемы/таблицы
	// повторить цикл для включения операций с флагом CMD_DISABLED
	if (IS_DIR(obj)) {
	  stop = FALSE;
	  set_flag(cmd, CMD_EXECUTED);
	}
	
      }

      // информация об создании пустого объекта не нужна
      // если новый объект - колонка, возможно, создана с таблицей
      if (iter != NULL && cmd->mstype != R_TEMP
	  && pcmd->mstype == R_TEMP && pcmd->act == CREP) {
	obj->object_id = pcmd->obj->object_id;

       	g_sequence_remove(iter);
	g_hash_table_remove(cache.mask_table, cmd->path);
	
	stop = FALSE;
      }

      if (stop) {
	break;
      } else {
	iter = g_sequence_get_end_iter(deploy.sql_seq);
	stop = TRUE;
      }
      
    }
    
  }

  cmd->obj = ms2sqlfs(obj);
  
  if (stop) {

    // колонка добавляется/редактируется впервые
    if (cmd->mstype == R_COL && !is_flag(cmd, CMD_EXECUTED)
	&& !g_str_has_prefix(cmd->sql, "ALTER TABLE")) {
     
      g_string_append_printf(sql, "ALTER TABLE [%s].[%s]",
			     *schema, *(schema + 1));

      if (get_mask_id(cmd->path)) {
	
	// не допускать редактирование колонок IDENTITY
	if (is_flag(cmd, CMD_IDENTITY)) {
	  set_flag(cmd, CMD_EXECUTED);
	}
	else {
	  g_string_append_printf(sql, " ALTER COLUMN %s", cmd->sql);
	}
	
      }
      else
	g_string_append_printf(sql, " ADD %s", cmd->sql);

      g_free(cmd->sql);
      cmd->sql = g_strdup(sql->str);
      
      g_string_truncate(sql, 0);
    }

    insert2cache_sorted(cmd);
  }

  do_mask(path, cmd);

  if (g_strv_length(schema) > 0) {
    g_strfreev(schema);
  }
    
  g_string_free(sql, TRUE);
}

static struct sqlcmd * drop_object(const char *path, struct sqlcmd *cmd,
				   struct sqlfs_ms_obj *obj)
{
  cmd->path = g_strdup(path);
  cmd->obj = ms2sqlfs(obj);
  cmd->mstype = obj->type;
  
  if (obj->type == R_COL && obj->column)
    if (obj->column->identity)
      set_flag(cmd, CMD_IDENTITY);
    else
      clear_flag(cmd, CMD_IDENTITY);
  
  cmd->act = DROP;

  if (IS_DIR(obj)) {
    GSequenceIter *iter = g_sequence_get_end_iter(deploy.sql_seq);
    gboolean is_remove = FALSE;
    while(!g_sequence_iter_is_begin(iter)) {
      iter = g_sequence_iter_prev(iter);
      struct sqlcmd *pcmd = g_sequence_get(iter);

      // оставить операции над таблицами и модулями
      if (cmd->mstype == D_SCHEMA && pcmd->mstype != D_U
	  && IS_SCHOBJ(obj) && g_str_has_prefix(pcmd->path, cmd->path)) {
	set_flag(pcmd, CMD_DISABLED);
      }

      // оставить только операции над ограничениями FK
      if (cmd->mstype == D_U && g_str_has_prefix(pcmd->path, cmd->path)
	  && pcmd->mstype != R_F) {
	set_flag(pcmd, CMD_DISABLED);
      }

    }
  }
  
  do_mask(path, cmd);
  insert2cache_sorted(cmd);
}

static struct sqlcmd * rename_obj(const char *oldname, const char *newname,
				  struct sqlcmd *cmd, struct sqlfs_ms_obj *obj)
{
  cmd->path = g_strdup(oldname);
  cmd->path2 = g_strdup(newname);
  cmd->obj = ms2sqlfs(obj);
  cmd->mstype = obj->type;
  cmd->act = RENAME;
  
  do_mask(oldname, cmd);
  do_mask(newname, cmd);
  g_sequence_append(deploy.sql_seq, cmd);
}

static inline void cut_deploy_sql()
{
  GSequenceIter *iter = g_sequence_get_begin_iter(deploy.sql_seq);
  GString *sql = g_string_new(NULL);

  while(!g_sequence_iter_is_end(iter)) {
    struct sqlcmd *cmd = g_sequence_get(iter);

#ifdef SQLDEBUG
    g_message("DEPLOY: %s, flags: %d; SQL:\n%s\n", cmd->path,
	      cmd->flags, cmd->sql);
#endif
    
    if (cmd->act == CREP && cmd->mstype == D_U) {

      if (g_str_has_suffix(cmd->sql, "("))
	g_string_append_printf(sql, "-- empty table: %s", cmd->sql);
      else
	g_string_append_printf(sql, "%s\n)", cmd->sql);
      
      g_free(cmd->sql);
      cmd->sql = g_strdup(sql->str);
      g_string_truncate(sql, 0);
      
    }
    
    iter = g_sequence_iter_next(iter); 
  }

  g_string_free(sql, TRUE);
}

static gboolean clear_tbl_files(gpointer key, gpointer value,
				gpointer user_data)
{
  if (key != NULL && user_data != NULL) {
    gchar *path = (gchar *) key, *pathdir = (gchar *) user_data;
    if (g_str_has_prefix(key, pathdir))
      return TRUE;
  }

  return FALSE;
}

static gboolean clear_dir_files(gpointer key, gpointer value,
				gpointer user_data)
{
  gboolean result = FALSE;
  if (key != NULL) {
    gchar *path = (gchar *) key, *pathdir = (gchar *) user_data;
    gchar *dc = g_path_get_dirname(path);
    if (!g_strcmp0(dc, pathdir)) {
      result = TRUE;
    }
    g_free(dc);
  }

  return result;
}

static inline void do_deploy_sql()
{
  GSequenceIter *iter = g_sequence_get_begin_iter(deploy.sql_seq);
  GError *terr = NULL;
  msctx_t *ctx = get_msctx(&terr);
  GString *sql = g_string_new(NULL);
  if (terr == NULL) {
    g_string_append(sql, "SET XACT_ABORT ON\n");
    g_string_append(sql, "BEGIN TRANSACTION\n");

    exec_sql_cmd(sql->str, ctx, &terr);
  }

  while(!g_sequence_iter_is_end(iter)) {
    struct sqlcmd *cmd = g_sequence_get(iter);

    // в том числе временные схемы и таблицы
    SAFE_REMOVE_ALL(cmd->path);

    if (cmd->sql != NULL && terr == NULL && !is_flag(cmd, CMD_DISABLED)
	&& !is_flag(cmd, CMD_EXECUTED)) {

      exec_sql_cmd(cmd->sql, ctx, &terr);
      
      // если ошибка, - откатить транзакцию
      if (terr != NULL) {
	GError *rerr = NULL;

	g_string_truncate(sql, 0);
	g_string_append(sql, "IF @@TRANCOUNT > 0");
	g_string_append(sql, " ROLLBACK TRANSACTION\n");

	exec_sql_cmd(sql->str, ctx, &rerr);

	if (rerr != NULL) {
	  g_clear_error(&terr);
	  g_propagate_error(&terr, rerr);
	}
      }
      
    }
    
    iter = g_sequence_iter_next(iter);
  }

  // если нет ошибки, - зафиксировать транзакцию
  if (terr == NULL) {
    g_string_truncate(sql, 0);
    g_string_append(sql, "IF @@TRANCOUNT > 0\n");
    g_string_append(sql, " COMMIT TRANSACTION\n");

    exec_sql_cmd(sql->str, ctx, &terr);
  }
  
  g_string_free(sql, TRUE);
  close_sql(ctx);

  g_sequence_remove_range(g_sequence_get_begin_iter(deploy.sql_seq),
			  g_sequence_get_end_iter(deploy.sql_seq));
}

static gpointer deploy_thread(gpointer data) {
  while (deploy.run) {
    g_mutex_lock(&deploy.lock);
    
    while(!g_sequence_get_length(deploy.sql_seq) && deploy.run) {
      g_cond_wait(&deploy.cond, &deploy.lock);
    }

    if (!deploy.run) {
      g_mutex_unlock(&deploy.lock);
      break;
    }

    gdouble tm = g_timer_elapsed(deploy.timer, NULL);
    if (tm > get_context()->depltime) {
      cut_deploy_sql();
      do_deploy_sql();

      // очистить маскировку
      g_hash_table_remove_all(cache.mask_table);

      // очистить APP-кэш
      g_hash_table_remove_all(cache.app_table);
      
      g_timer_stop(deploy.timer);
      g_mutex_unlock(&deploy.lock);
    }
    else {
      g_mutex_unlock(&deploy.lock);
      g_usleep((get_context()->depltime - tm) * 1000000);
    }
  }

  return 0;
}

static void hotstart(GError **error)
{
  GError *terr = NULL;
  GList *wrk = NULL;
  GString *sql = g_string_new(NULL);
  g_mutex_lock(&cache.m);

  msctx_t *ctx = get_msctx(&terr);

  if (terr == NULL) {
  
    g_string_append(sql, "CREATE TABLE #schemas (");
    g_string_append(sql, "dir_path NVARCHAR(MAX), sch_id INT)\n");
    
    g_string_append(sql, "CREATE TABLE #sch_objs (");
    g_string_append(sql, "dir_path NVARCHAR(MAX), obj_id INT");
    g_string_append(sql, ", obj_type NVARCHAR(5), ctime BIGINT");
    g_string_append(sql, ", mtime BIGINT, def_len INT NULL )\n");

    exec_sql_cmd(sql->str, ctx, &terr);
  }

  if (terr == NULL) {
    wrk = fetch_schemas(NULL, ctx, TRUE, &terr);
    if (terr == NULL)
      wrk = g_list_concat(wrk, fetch_schema_obj(FALSE, NULL, ctx, &terr));

    if (terr == NULL)
      wrk = g_list_concat(wrk, fetch_table_obj(FALSE, FALSE, NULL, ctx, &terr));

    if (terr == NULL) {
      struct sqlfs_ms_obj *object = NULL;
      wrk = g_list_first(wrk);
      while (wrk) {
	object = wrk->data;
	gchar *str = object->name;
	object->name = g_path_get_basename(str);
	g_hash_table_insert(cache.db_table, str, object);
	wrk = g_list_next(wrk);
      }
      g_list_free(wrk);
    }
    
  }

  if (terr == NULL) {
    g_string_truncate(sql, 0);
    
    g_string_append(sql, "DROP TABLE #sch_objs\n");
    g_string_append(sql, "DROP TABLE #schemas");
    
    exec_sql_cmd(sql->str, ctx, &terr);
  }
  
  g_string_free(sql, TRUE);
  close_sql(ctx);
  
  g_mutex_unlock(&cache.m);
  
  if (terr != NULL)
    g_propagate_error(error, terr);
}

void init_cache(GError **error)
{
  GError *terr = NULL;

  g_mutex_init(&cache.m);
  cache.db_table = g_hash_table_new_full(g_str_hash, g_str_equal,
					 g_free, free_ms_obj);
  cache.app_table = g_hash_table_new_full(g_str_hash, g_str_equal,
					  g_free, free_ms_obj);
  cache.mask_table = g_hash_table_new_full(g_str_hash, g_str_equal,
					   g_free, NULL);

  g_mutex_init(&deploy.lock);
  g_cond_init(&deploy.cond);
  deploy.sql_seq = g_sequence_new(&free_sqlcmd_object);
  deploy.timer = g_timer_new();
  deploy.run = 1;
  deploy.thread = g_thread_new(NULL, &deploy_thread, NULL);
  
  init_msctx(&terr);

  if (terr == NULL && get_context()->hotstart) {
    hotstart(&terr);
  }
  
  if (terr != NULL)
    g_propagate_error(error, terr);
}

struct sqlfs_object * find_object(const char *pathfile, GError **error)
{
  // отключаем таймер деплоя на время выборки из БД
  gboolean paused = pause_timer();
  
  GError *terr = NULL;
  struct sqlfs_object *result;
  struct sqlfs_ms_obj *obj = find_cache_obj(pathfile, &terr);

  if (obj == NULL && terr == NULL)
    g_set_error(&terr, EENOTFOUND, EENOTFOUND,
		"%d: Object not found\n", __LINE__);
    
  if (terr == NULL) {
    result = ms2sqlfs(obj);
  }

  continue_timer(paused);

  if (terr != NULL)
    g_propagate_error(error, terr);
  
  return result;
}


GList * fetch_dir_objects(const char *pathdir, GError **error)
{
  GList *reslist = NULL, *wrk = NULL;
  GError *terr = NULL;
  struct sqlfs_ms_obj *object = NULL;
  int nschema = g_strcmp0(pathdir, G_DIR_SEPARATOR_S);

  // отключаем таймер деплоя на время выборки из БД
  gboolean paused = pause_timer();

  // очистить устаревшие данные из DB-кэша
  gchar *p = g_strdup(pathdir);
  g_hash_table_foreach_remove(cache.db_table, &clear_dir_files, p);
  g_free(p);

  msctx_t *ctx = get_msctx(&terr);

  // получить объекты в соответствии с уровнем
  if (!nschema) {
    wrk = fetch_schemas(NULL, ctx, FALSE, &terr);
  } else {
    object = find_cache_obj(pathdir, &terr);
    if (terr == NULL && !g_hash_table_contains(cache.app_table, pathdir)) {
      if (object->type == D_SCHEMA) 
	wrk = fetch_schema_obj(object->schema_id, NULL, ctx, &terr);
      else
	if (object->type == D_U || object->type == D_V) {
	  wrk = fetch_table_obj(object->schema_id, object->object_id,
				NULL, ctx, &terr);
	}
	else
	  g_set_error(&terr, EENOTSUP, EENOTSUP,
		      "%d: Operation not supported", __LINE__);
    }
  }

  close_sql(ctx);

  if (terr == NULL) {
    
    // добавить объекты в кэш DB, если их нет в кэше APP
    wrk = g_list_first(wrk);
    gchar * str = NULL;
    while (wrk) {
      object = wrk->data;
      str = (nschema) ? g_strjoin(G_DIR_SEPARATOR_S, pathdir, object->name, NULL)
	: g_strconcat(pathdir, object->name, NULL);
      if (!g_hash_table_contains(cache.app_table, str) && !is_masked(str)) {
	if (!g_hash_table_contains(cache.db_table, str)) {
	  g_hash_table_insert(cache.db_table, g_strdup(str), object);
	}
	reslist = g_list_append(reslist, ms2sqlfs(object));
      }
      g_free(str);
      wrk = g_list_next(wrk);
    }
    g_list_free(wrk);

    // дополнить список объектов из APP кэша
    GHashTableIter iter;
    gpointer key, value;
    g_mutex_lock(&cache.m);
    g_hash_table_iter_init(&iter, cache.app_table);
    while (g_hash_table_iter_next(&iter, &key, &value)) {
      gchar *keydir = g_path_get_dirname(key);
      if (!g_strcmp0(keydir, pathdir)) {
	object = (struct sqlfs_ms_obj *) value;
	reslist = g_list_append(reslist, ms2sqlfs(object));
      }
      g_free(keydir);
    }
    g_mutex_unlock(&cache.m);

  }
  
  continue_timer(paused);
  
  if (terr != NULL)
    g_propagate_error(error, terr);

  return reslist;
}

char * fetch_object_text(const char *path, GError **error)
{
  char *text = NULL;
  GError *terr = NULL;
  
  // отключаем таймер деплоя на время выборки из БД
  gboolean paused = pause_timer();
  
  struct sqlfs_ms_obj *object = find_cache_obj(path, &terr);

  if (terr == NULL) {
    gchar **schema = g_strsplit(g_path_skip_root(path), G_DIR_SEPARATOR_S, -1);

    if (!is_masked(path) && !is_temp(object)
	&& !g_hash_table_contains(cache.app_table, path)) {
      text = load_module_text(*schema, object, &terr);

      if (object->def != NULL)
	g_free(object->def);
      
      object->def = text;
      object->len = strlen(object->def);
    }
    else {
      text = object->def;
    }
    
    g_strfreev(schema); 
  }

  continue_timer(paused);
  
  if (terr != NULL)
    g_propagate_error(error, terr);
  
  return text;
}

void create_dir(const char *pathdir, GError **error)
{
  GError *terr = NULL;
  
  gchar **parent = g_strsplit(g_path_skip_root(pathdir), G_DIR_SEPARATOR_S, -1);
  if (parent == NULL)
    g_set_error(&terr, EENULL, EENULL,
		"%d: parent is not defined!", __LINE__);
  GString *sql = g_string_new(NULL);
  
  if (terr == NULL) {
    struct sqlcmd *cmd = start_cache();

    int level = g_strv_length(parent);
    unsigned int type = 0;
    switch (level) {
    case 1:
      type = D_SCHEMA;
      g_string_append_printf(sql, "CREATE SCHEMA [%s]", *parent);
      break;
    case 2:
      type = D_U;
      g_string_append_printf(sql, "CREATE TABLE [%s].[%s] (",
			     *parent, *(parent + 1));
      break;
    default:
      g_set_error(&terr, EENOTSUP, EENOTSUP,
		  "%d: Operation not supported", __LINE__);
      break;
    }

    if (terr == NULL) {
      struct sqlfs_ms_obj *obj = g_try_new0(struct sqlfs_ms_obj, 1);
      obj->type = type;
      obj->name = g_path_get_basename(pathdir);
      obj->object_id = 0;

      g_hash_table_insert(cache.app_table, g_strdup(pathdir), obj);
      cmd->sql = g_strdup(sql->str);
      crep_object(pathdir, cmd, obj);
    } else {
      free_sqlcmd_object(cmd);
    }

    end_cache();
  }
  
  if (g_strv_length(parent) > 0) {
    g_strfreev(parent);
  }

  g_string_free(sql, TRUE);

  if (terr != NULL)
    g_propagate_error(error, terr);
}

void create_node(const char *pathfile, GError **error)
{
  GError *terr = NULL;
  
  gchar **parent = g_strsplit(g_path_skip_root(pathfile), G_DIR_SEPARATOR_S, -1);
  if (parent == NULL)
    g_set_error(&terr, EENULL, EENULL,
		"%d: parent is not defined!", __LINE__);
  
  if (terr == NULL) {
    struct sqlcmd *cmd = start_cache();
    struct sqlfs_ms_obj *tobj = g_try_new0(struct sqlfs_ms_obj, 1);

    tobj->object_id = 0;
    tobj->name = g_path_get_basename(pathfile);
    tobj->type = R_TEMP;
    tobj->def = g_strdup("\0");

    g_hash_table_insert(cache.app_table, g_strdup(pathfile), tobj);
    crep_object(pathfile, cmd, tobj);

    end_cache();
  }
  
  if (g_strv_length(parent) > 0) {
    g_strfreev(parent);
  }

  if (terr != NULL)
    g_propagate_error(error, terr);
  
}

void write_object(const char *path, const char *buffer, GError **error)
{
  GError *terr = NULL;
  
  gchar **schema = g_strsplit(g_path_skip_root(path), G_DIR_SEPARATOR_S, -1);
  if (schema == NULL)
    g_set_error(&terr, EENULL, EENULL,
		"%d: parent is not defined!", __LINE__);
  
  if (terr == NULL) {
    gchar *pp = g_path_get_dirname(path);
    struct sqlcmd *cmd = start_cache();
    struct sqlfs_ms_obj *pobj = find_cache_obj(pp, &terr),
      *object = find_cache_obj(path, &terr);

    if (terr != NULL) {
      g_clear_error(&terr);
      object->name = g_path_get_basename(path);
    }

    if (object && buffer && strlen(buffer) > 0) {
      object->object_id = get_mask_id(path);
      char *sql = write_ms_object(*schema, pobj, buffer, object, &terr);
      if (terr == NULL && sql != NULL) {

	if (object->def != NULL)
	  g_free(object->def);

	object->def = g_strdup(buffer);
	object->len = strlen(object->def);
	cmd->sql = sql;
	crep_object(path, cmd, object);
      }
      else {
	SAFE_REMOVE_ALL(path);
	CLEAR_DEPLOY();
      }

    }

    if (!cmd->path)
      free_sqlcmd_object(cmd);

    end_cache();

    g_free(pp);
  }

  if (schema != NULL)
    g_strfreev(schema);

  if (terr != NULL)
    g_propagate_error(error, terr);
  
}

void truncate_object(const char *path, off_t offset, GError **error)
{
  GError *terr = NULL;

  struct sqlcmd *cmd = start_cache();
  struct sqlfs_ms_obj *obj = g_hash_table_lookup(cache.app_table, path);
  if (!obj) {
    obj = find_cache_obj(path, &terr);
    if (terr == NULL) {
      g_hash_table_steal(cache.db_table, path);
    }
  }

  if (obj != NULL && terr == NULL) {
    gchar **schema = g_strsplit(g_path_skip_root(path), G_DIR_SEPARATOR_S, -1);
    char *def = NULL;
    if (obj->object_id != 0 && !g_hash_table_contains(cache.app_table, path)) {
      char *load = load_module_text(*schema, obj, &terr);
      def = g_strndup(load, offset);
      g_free(load);
    }
    else
      if (obj->def != NULL) {
	def = g_strndup(obj->def, offset);
      }

    if (obj->def != NULL) {
      g_free(obj->def);
      def = NULL;
    }

    if (offset > 0 && def != NULL) {
      obj->def = def;
      obj->len = strlen(obj->def);
      
      cmd->sql = g_strdup(obj->def);
      crep_object(path, cmd, obj);
    }
    else {
      obj->def = g_strdup("\0");
      obj->len = 0;
    }

    if (!g_hash_table_contains(cache.app_table, path)) {
      g_hash_table_insert(cache.app_table, g_strdup(path), obj);
    }

    if (g_strv_length(schema) > 0) {
      g_strfreev(schema);
    }

  }

  if (!cmd->path)
    free_sqlcmd_object(cmd);
  
  end_cache();

  if (terr != NULL)
    g_propagate_error(error, terr);
}

void remove_object(const char *path, GError **error)
{
  GError *terr = NULL;

  gchar **schema = g_strsplit(g_path_skip_root(path), G_DIR_SEPARATOR_S, -1);
  if (schema == NULL)
    g_set_error(&terr, EENULL, EENULL,
		"%d: parent is not defined!", __LINE__);

  if (terr == NULL) {
    struct sqlcmd *cmd = start_cache();
    struct sqlfs_ms_obj *object = find_cache_obj(path, &terr);
    if (terr == NULL && !is_temp(object)) {
      char *sql = remove_ms_object(*schema, *(schema + 1), object, &terr);
      if (terr == NULL && sql != NULL) {
	cmd->sql = sql;
	drop_object(path, cmd, object);
      }
    }

    // очистить файлы директории из кэша
    if (IS_DIR(object)) {
      char *p = g_strdup(path);
      g_hash_table_foreach_remove(cache.app_table, &clear_tbl_files, p);
      g_hash_table_foreach_remove(cache.db_table, &clear_tbl_files, p);
      g_free(p);
    }
    
    SAFE_REMOVE_ALL(path);

    if (!cmd->path)
      free_sqlcmd_object(cmd);
    
    end_cache();
  }

  if (g_strv_length(schema) > 0) {
    g_strfreev(schema);
  }

  if (terr != NULL)
    g_propagate_error(error, terr);
  
}


void rename_object(const char *oldname, const char *newname, GError **error)
{
  GError *terr = NULL;
  
  gchar **schemaold = g_strsplit(g_path_skip_root(oldname),
				 G_DIR_SEPARATOR_S, -1);
  gchar **schemanew = g_strsplit(g_path_skip_root(newname),
				 G_DIR_SEPARATOR_S, -1);

  // не перемещать объект в корневой уровень
  if (g_strv_length(schemanew) == 1)
    g_set_error(&terr, EENOTSUP, EENOTSUP,
		"%d: Operation not supported", __LINE__);
  
  // не перемещать между разными уровнями
  if (terr == NULL &&
      g_strv_length(schemaold) != g_strv_length(schemanew))
    g_set_error(&terr, EENOTSUP, EENOTSUP,
		"%d: Operation not supported", __LINE__);

  // не перемещать объекты таблиц в другие таблицы
  if (terr == NULL &&
      g_strv_length(schemanew) > 2
      && g_strcmp0(*(schemanew + 1), *(schemaold + 1)))
    g_set_error(&terr, EENOTSUP, EENOTSUP,
		"%d: Operation not supported", __LINE__);

  if (terr == NULL) {
    lock_cache();
    
    gboolean is_exists = FALSE;
    struct sqlfs_ms_obj *obj_new = g_hash_table_lookup(cache.app_table, newname);
    
    if (obj_new == NULL)
      obj_new = g_hash_table_lookup(cache.db_table, newname);
    else
      is_exists = g_hash_table_steal(cache.app_table, newname);
    
    if (obj_new == NULL) {

      if (get_mask_id(newname)) {
	obj_new = do_find(newname, &terr);
      }
      
      if (terr != NULL) {
	g_clear_error(&terr);
	obj_new = NULL;
      }
      
    }
    else
      if (!is_exists)
	is_exists = g_hash_table_steal(cache.db_table, newname);

    struct sqlfs_ms_obj *obj_old = find_cache_obj(oldname, &terr);
    if (obj_new != NULL) {

      if (!is_temp(obj_new)
	  && (obj_old->type != obj_new->type || !is_temp(obj_old))) {

	struct sqlcmd *cmd = g_try_new0(struct sqlcmd, 1);
	cmd->flags = 0;
	
	char *sql = remove_ms_object(*schemanew, *(schemanew + 1), obj_new, &terr);
	if (terr == NULL && sql != NULL) {
	  cmd->sql = sql;
	  drop_object(newname, cmd, obj_new);
	}
	
      }

    }

    if (!is_temp(obj_old)) {
      gchar *ppold = g_path_get_dirname(oldname);
      struct sqlfs_ms_obj
	*ppobj_old = find_cache_obj(ppold, &terr);

      gboolean allocated = FALSE;
      if (obj_new == NULL) {
	obj_new = g_try_new0(struct sqlfs_ms_obj, 1);
	obj_new->name = g_path_get_basename(newname);
	allocated = TRUE;
      }

      // для старого объекта ещё не был прочитан текст
      if (g_hash_table_contains(cache.db_table, oldname) && !obj_old->def
	  && IS_REG(obj_old)) {
	char *def = load_module_text(*schemaold, obj_old, &terr);
	if (terr == NULL) {
	  if (obj_old->def != NULL)
	    g_free(obj_old->def);

	  obj_old->def = def;
	  obj_old->len = strlen(def);
	}
      }

      struct sqlcmd *cmd = g_try_new0(struct sqlcmd, 1);
      cmd->flags = 0;
      
      char *sql = rename_ms_object(*schemaold, *schemanew, obj_old, obj_new,
				   ppobj_old, &terr);
      if (terr == NULL && sql != NULL) {
	cmd->sql = sql;
	rename_obj(oldname, newname, cmd, obj_old);
      }

      g_free(ppold);

      if (obj_new != NULL && allocated)
	free_ms_obj(obj_new);
    }
    
    // переименование в кэше
    if (terr == NULL) {
      if (g_hash_table_contains(cache.app_table, oldname))
	g_hash_table_steal(cache.app_table, oldname);
      
      if (g_hash_table_contains(cache.db_table, oldname))
	g_hash_table_steal(cache.db_table, oldname);

      g_free(obj_old->name);
      obj_old->name = g_path_get_basename(newname);
      
      g_hash_table_insert(cache.app_table, g_strdup(newname), obj_old);
    }

    end_cache();
  }
  
  g_strfreev(schemanew);
  g_strfreev(schemaold);
  
  if (terr != NULL)
    g_propagate_error(error, terr);
}

GList * fetch_listxattr(const char *path, GError **error)
{
  GError *terr = NULL;
  GList *listx = NULL;

  // все объекты обладают этими атрибутами
  listx = g_list_append(listx, "user.sqlfuse.object_id");
  listx = g_list_append(listx, "user.sqlfuse.type");

  int class_id = 1, major_id = 0, minor_id = 0;

  if (!g_strcmp0(path, "/"))
    class_id = 0;
  
  struct sqlfs_ms_obj *object = find_cache_obj(path, &terr);

  if (terr == NULL) {
    major_id = object->object_id;
    
    switch(object->type) {
    case D_SCHEMA:
      class_id = 3;
      major_id = object->schema_id;
      break;
    case D_V:
      listx = g_list_append(listx, "user.sqlfuse.viewdef");
      break;
    case R_COL:
      listx = g_list_append(listx, "user.sqlfuse.column_id");
      if (object->column != NULL)
	minor_id = object->column->column_id;
      break;
    case R_TR:
      listx = g_list_append(listx, "user.sqlfuse.trigger.is_disabled");
      break;
    }

    if (!object->acls) {
      msctx_t *ctx = get_msctx(&terr);

      //список разрешений для объекта
      object->acls = fetch_xattr_list(class_id, major_id, minor_id,
				      ctx, &terr);
    
      close_sql(ctx);
    }

    if (object->acls && terr == NULL) {
      GList *wrk = g_list_first(object->acls);
      struct sqlfs_ms_acl *acl = NULL;
      while(wrk) {
	acl = wrk->data;
	gchar *str = g_strjoin(".", "user.sqlfuse.rights",
			       acl->principal_name, acl->state, acl->type,
			       NULL);
	listx = g_list_append(listx, str);
	wrk = g_list_next(wrk);
      }
    }
    
  }
  
  if (terr != NULL)
    g_propagate_error(error, terr);

  return listx;
}

char * fetch_xattr(const char *path, const char *name, GError **error)
{
  GError *terr = NULL;
  gchar *res = NULL;

  gchar **schema = g_strsplit(g_path_skip_root(path), G_DIR_SEPARATOR_S, -1);
  if (schema == NULL)
    g_set_error(&terr, EENULL, EENULL,
		"%d: parent is not defined!", __LINE__);
  
  struct sqlfs_ms_obj *object = find_cache_obj(path, &terr);
  if (terr == NULL) {
    if (!g_strcmp0(name, "user.sqlfuse.object_id")) {
      if (object->type == D_SCHEMA)
	res = g_strdup_printf("%d", object->schema_id);
      else
	res = g_strdup_printf("%d", object->object_id);
    }

    if (!g_strcmp0(name, "user.sqlfuse.column_id")
	&& object->type == R_COL && object->column != NULL) {
      res = g_strdup_printf("%d", object->column->column_id);
    }

    if (!g_strcmp0(name, "user.sqlfuse.type")) {
      res = g_strdup_printf("%s", mstype2str(object->type));
    }

    if (!g_strcmp0(name, "user.sqlfuse.viewdef")) {
      res = fetch_object_text(path, &terr);
    }

    if (!g_strcmp0(name, "user.sqlfuse.trigger.is_disabled")
	&& object->type == R_TR) {
      res = g_strdup_printf("%d", object->is_disabled);
    }

    if (g_str_has_prefix(name, "user.sqlfuse.rights.")
	&& object->acls) {
      res = g_strdup_printf("%d", 1);
    }

  }
  
  if (terr != NULL)
    g_propagate_error(error, terr);

  if (g_strv_length(schema) > 0) {
    g_strfreev(schema);
  }

  return res;
}

void free_sqlfs_object(gpointer object)
{
  if (object == NULL)
    return ;

  struct sqlfs_object *obj = (struct sqlfs_object *) object;

  if (obj->name != NULL)
    g_free(obj->name);

  if (obj->def != NULL)
    g_free(obj->def);

  g_rw_lock_clear(&obj->lock);
  
  if (obj != NULL)
    g_free(obj);

  obj = NULL;
}


void destroy_cache(GError **error)
{
  GError *terr = NULL;

  g_mutex_lock(&deploy.lock);
  deploy.run = 0;
  g_cond_signal(&deploy.cond);
  g_mutex_unlock(&deploy.lock);

  g_thread_join(deploy.thread);
  
  close_msctx(&terr);
  
  g_hash_table_destroy(cache.db_table);
  g_hash_table_destroy(cache.app_table);
  g_hash_table_destroy(cache.mask_table);
  
  g_sequence_free(deploy.sql_seq);
  g_timer_destroy(deploy.timer);
  
  g_mutex_clear(&cache.m);
  g_mutex_clear(&deploy.lock);
  g_cond_clear(&deploy.cond);
  
  
  if (terr != NULL)
    g_propagate_error(error, terr);
}
