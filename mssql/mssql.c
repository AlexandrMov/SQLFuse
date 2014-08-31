#include <sqlfuse.h>
#include <conf/keyconf.h>
#include "msctx.h"
#include "exec.h"

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

struct sqlmask {
  gboolean masked;
  unsigned int object_id;
};

enum action {
  CREP,
  DROP,
  RENAME
};

struct sqlcmd {
  enum action act;
  char *path, *path2;

  struct sqlfs_object *obj;
  
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
			  g_sequence_get_end_iter(deploy.sql_seq));

static struct sqlcache cache;
static struct sqldeploy deploy;

static gboolean is_masked(const char *path)
{
  struct sqlmask *mask = g_hash_table_lookup(cache.mask_table, path);
  if (mask == NULL) {
    return FALSE;
  }
  else {
    return mask->masked;
  }
}

static void mask_path(const char *path, unsigned int object_id)
{
  struct sqlmask *mask = g_hash_table_lookup(cache.mask_table, path);
  if (mask == NULL) {
    mask = g_try_new0(struct sqlmask, 1);
    mask->object_id = object_id;
    
    g_hash_table_insert(cache.mask_table, g_strdup(path), mask);
  }
  else {
    mask->object_id = object_id;
  }
  
  mask->masked = TRUE;

  g_timer_start(deploy.timer);
}

static void unmask_path(const char *path)
{
  struct sqlmask *mask = g_hash_table_lookup(cache.mask_table, path);
  if (mask != NULL) {
    mask->masked = FALSE;
  }

  g_timer_start(deploy.timer);
}

static struct sqlfs_ms_obj * do_find(const char *pathname, GError **error)
{
  GError *terr = NULL;
  GList *list = NULL;

  unsigned int i = 0;
  const char *pn = g_path_skip_root(pathname);
  if (pn == NULL)
    pn = pathname;

  char **tree = g_strsplit(pn, G_DIR_SEPARATOR_S, -1);
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
  if (!obj && terr == NULL)
    obj = g_hash_table_lookup(cache.db_table, pathname);
  
  if (!obj && terr == NULL) {
    obj = do_find(pathname, &terr);
  }

  if (terr != NULL)
    g_propagate_error(error, terr); 
  
  return obj;
}

static struct sqlfs_object * ms2sqlfs(struct sqlfs_ms_obj *src)
{
  struct sqlfs_object *result = g_try_new0(struct sqlfs_object, 1);
  g_rw_lock_init(&result->lock);
  
  result->object_id = src->object_id;
  result->name = g_strdup(src->name);
  
  if (src->type < 0x08)
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


static void add2deploy(enum action act, const char *path, const char *path2,
		       struct sqlfs_object *obj, const char *sql)
{
  g_mutex_lock(&deploy.lock);

  if (act == CREP) {
    unmask_path(path);
  }

  if (act == DROP) {
    mask_path(path, obj->object_id);
  }

  struct sqlcmd *cmd = g_try_new0(struct sqlcmd, 1);
  cmd->path = g_strdup(path);
  cmd->path2 = g_strdup(path2);

  cmd->obj = obj;
  
  if (sql != NULL)
    cmd->sql = g_strdup(sql);

  cmd->act = act;
  g_sequence_append(deploy.sql_seq, cmd);

  g_timer_start(deploy.timer);

  g_cond_signal(&deploy.cond);
  g_mutex_unlock(&deploy.lock);
}


static inline void dir2deploy(enum action act, const char *path,
			      unsigned int objtype)
{
  struct sqlfs_object *res = g_try_new0(struct sqlfs_object, 1);
  res->type = objtype;
  res->cached_time = g_get_monotonic_time();

  add2deploy(act, path, NULL, res, NULL);
}


static inline void cut_deploy_sql()
{
  GSequenceIter *iter = g_sequence_get_begin_iter(deploy.sql_seq);

  while(!g_sequence_iter_is_end(iter)) {
    struct sqlcmd *cmd = g_sequence_get(iter);
    if (!g_sequence_iter_is_begin(iter)) {
      GSequenceIter *prev = g_sequence_iter_prev(iter);
      struct sqlcmd *pcmd = g_sequence_get(prev);

      if (pcmd->obj != NULL && cmd->obj != NULL) {
	
	//оставить крайнее действие из последовательности одинаковых
	//заменить пересоздание->изменение
	if ((pcmd->act == cmd->act || pcmd->act == DROP && cmd->act == CREP)
	    && pcmd->obj->type == cmd->obj->type
	    && !g_strcmp0(pcmd->path, cmd->path))
	  g_sequence_remove(prev);
      }
      
    }
    
    
    iter = g_sequence_iter_next(iter); 
  }
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
    
    SAFE_REMOVE_ALL(cmd->path);
    if (cmd->sql != NULL && terr == NULL) {
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

  if (obj != NULL)
    g_free(obj);
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
					   g_free, g_free);

  g_mutex_init(&deploy.lock);
  g_cond_init(&deploy.cond);
  deploy.sql_seq = g_sequence_new(&free_sqlcmd_object);
  deploy.timer = g_timer_new();
  deploy.run = 1;
  deploy.thread = g_thread_new(NULL, &deploy_thread, NULL);
  
  init_msctx(&terr);
  
  if (terr != NULL)
    g_propagate_error(error, terr);
}


struct sqlfs_object * find_object(const char *pathfile, GError **error)
{
  GError *terr = NULL;
  struct sqlfs_object *result;
  struct sqlfs_ms_obj *obj = find_cache_obj(pathfile, &terr);

  if (obj == NULL && terr == NULL)
    g_set_error(&terr, EENOTFOUND, EENOTFOUND,
		"%d: Object not found\n", __LINE__);
    
  if (terr == NULL) {
    result = ms2sqlfs(obj);
  }
  
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
  
  // получить объекты в соответствии с уровнем
  if (!nschema) {
    wrk = fetch_schemas(NULL, &terr);
  } else {
    object = find_cache_obj(pathdir, &terr);
    if (terr == NULL) {
      if (object->type == D_SCHEMA) 
	wrk = fetch_schema_obj(object->schema_id, NULL, &terr);
      else
	if (object->type == D_U || object->type == D_V) {
	  wrk = fetch_table_obj(object->schema_id, object->object_id,
				NULL, &terr);
	}
	else
	  g_set_error(&terr, EENOTSUP, EENOTSUP,
		      "%d: Operation not supported", __LINE__);
    }
  }

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
	  g_mutex_lock(&cache.m);
	  g_hash_table_insert(cache.db_table, g_strdup(str), object);
	  g_mutex_unlock(&cache.m);
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
  
  if (terr != NULL)
    g_propagate_error(error, terr);

  return reslist;
}

char * fetch_object_text(const char *path, GError **error)
{
  char *text = NULL;

  GError *terr = NULL;
  struct sqlfs_ms_obj *object = find_cache_obj(path, &terr);

  if (terr == NULL) {
    gchar **schema = g_strsplit(g_path_skip_root(path), G_DIR_SEPARATOR_S, -1);

    if (!g_hash_table_contains(cache.app_table, path)
	&& !is_masked(path)
	&& object->type != R_TEMP)
      text = load_module_text(*schema, object, &terr);
    else
      text = object->def;
    
    g_strfreev(schema); 
  }
  
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
  
  if (terr == NULL) {
    int level = g_strv_length(parent);
  
    if (level == 1) {
      dir2deploy(CREP, pathdir, D_SCHEMA);
      create_schema(g_path_get_basename(pathdir), &terr);
    }
    else
      if (level == 2) {
	dir2deploy(CREP, pathdir, D_U);
	create_table(*parent, g_path_get_basename(pathdir), &terr);
      }
      else
	g_set_error(&terr, EENOTSUP, EENOTSUP,
		    "%d: Operation not supported", __LINE__);
  
  }
  
  if (g_strv_length(parent) > 0) {
    g_strfreev(parent);
  }

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
    struct sqlfs_ms_obj *tobj = g_try_new0(struct sqlfs_ms_obj, 1);

    // объект существовал ранее в БД
    struct sqlmask *mask = g_hash_table_lookup(cache.mask_table, pathfile);
    tobj->object_id = 0;
    if (mask != NULL)
      tobj->object_id = mask->object_id;
    
    tobj->name = g_path_get_basename(pathfile);
    tobj->type = R_TEMP;
    tobj->def = g_strdup("\0");
    
    g_mutex_lock(&cache.m);
    g_hash_table_insert(cache.app_table, g_strdup(pathfile), tobj);
    unmask_path(pathfile);
    g_mutex_unlock(&cache.m);
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
    struct sqlfs_ms_obj *pobj = find_cache_obj(pp, &terr),
      *object = find_cache_obj(path, &terr);

    if (terr != NULL) {
      g_clear_error(&terr);
      object->name = g_path_get_basename(path);
    }

    if (object && buffer && strlen(buffer) > 0) {
      char *sql = write_ms_object(*schema, pobj, buffer, object, &terr);
      if (terr == NULL && sql != NULL) {
	add2deploy(CREP, path, NULL, ms2sqlfs(object), sql);
	
	if (object->def != NULL)
	  g_free(object->def);

	object->def = g_strdup(buffer);
	object->len = strlen(object->def);
      }
      else {
	SAFE_REMOVE_ALL(path);
	CLEAR_DEPLOY();
      }
    }

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
      def = g_strndup(load_module_text(*schema, obj, &terr), offset);
    }
    else
      if (obj->def != NULL) {
	def = g_strndup(obj->def, offset);
      }

    if (obj->def != NULL)
      g_free(obj->def);

    if (offset > 0 && def != NULL) {
      obj->def = def;
      obj->len = strlen(obj->def);

      add2deploy(CREP, path, NULL, ms2sqlfs(obj), obj->def);
    }
    else {
      obj->def = g_strdup("\0");
      obj->len = 0;
    }

    if (!g_hash_table_contains(cache.app_table, path)) {
      g_mutex_lock(&cache.m);
      g_hash_table_insert(cache.app_table, g_strdup(path), obj);
      g_mutex_unlock(&cache.m);
    }

    if (g_strv_length(schema) > 0) {
      g_strfreev(schema);
    }

  }

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
    struct sqlfs_ms_obj *object = find_cache_obj(path, &terr);
    if (terr == NULL) {
      char *sql = remove_ms_object(*schema, *(schema + 1), object, &terr);
      if (terr == NULL) {
	add2deploy(DROP, path, NULL, ms2sqlfs(object), sql);
	SAFE_REMOVE_ALL(path);
      }
    }

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
  if (g_strv_length(schemaold) != g_strv_length(schemanew))
    g_set_error(&terr, EENOTSUP, EENOTSUP,
		"%d: Operation not supported", __LINE__);

  // не перемещать объекты таблиц в другие таблицы
  if (g_strv_length(schemanew) > 2
      && g_strcmp0(*(schemanew + 1), *(schemaold + 1)))
    g_set_error(&terr, EENOTSUP, EENOTSUP,
		"%d: Operation not supported", __LINE__);

  if (terr == NULL) {
    gboolean is_app = FALSE, is_db = FALSE;
    struct sqlfs_ms_obj *obj_new = g_hash_table_lookup(cache.app_table, newname);
    
    if (obj_new == NULL)
      obj_new = g_hash_table_lookup(cache.db_table, newname);
    else
      is_app = g_hash_table_steal(cache.app_table, newname);
    
    if (obj_new == NULL) {
      struct sqlmask *mask = g_hash_table_lookup(cache.mask_table, newname);
      if (mask != NULL && mask->object_id)
	obj_new = do_find(newname, &terr);

      if (terr != NULL) {
	g_clear_error(&terr);
	obj_new = NULL;
      }
      
    }
    else
      if (!is_app)
	is_db = g_hash_table_steal(cache.db_table, newname);

    struct sqlfs_ms_obj *obj_old = find_cache_obj(oldname, &terr);
    if (obj_new != NULL) {

      // новый объект не временный, и он не соответствует типу старого
      // или старый объект с неопределённым типом, - удалить новый объект
      if (obj_new->object_id && is_db
	  && (obj_old->type != obj_new->type || obj_old->type != R_TEMP)) {
	char *sql = remove_ms_object(*schemanew, *(schemanew + 1), obj_new, &terr);
	if (terr == NULL) {
	  add2deploy(DROP, newname, NULL, ms2sqlfs(obj_new), sql);
	}
      }

    }
    
    gchar *ppold = g_path_get_dirname(oldname);
    struct sqlfs_ms_obj
      *ppobj_old = find_cache_obj(ppold, &terr);

    gboolean allocated = FALSE;
    if (obj_new == NULL) {
      obj_new = g_try_new0(struct sqlfs_ms_obj, 1);
      obj_new->name = g_path_get_basename(newname);
      allocated = TRUE;
    }

    char *sql = rename_ms_object(*schemaold, *schemanew, obj_old, obj_new,
				 ppobj_old, &terr);
    if (terr == NULL && sql != NULL) {
      add2deploy(RENAME, oldname, newname, ms2sqlfs(obj_old), sql);
    }

    g_free(ppold);

    if (obj_new != NULL && allocated)
      free_ms_obj(obj_new);
    
    // переименование в кэше
    if (terr == NULL) {
      g_mutex_lock(&cache.m);

      // объект более не существует
      mask_path(oldname, 0);

      if (g_hash_table_contains(cache.app_table, oldname))
	g_hash_table_steal(cache.app_table, oldname);
      
      if (g_hash_table_contains(cache.db_table, oldname))
	g_hash_table_steal(cache.db_table, oldname);

      g_free(obj_old->name);
      obj_old->name = g_path_get_basename(newname);
      
      g_hash_table_insert(cache.app_table, g_strdup(newname), obj_old);
      unmask_path(newname);
      
      g_mutex_unlock(&cache.m);
    }
    
  }
  
  g_strfreev(schemanew);
  g_strfreev(schemaold);
  
  if (terr != NULL)
    g_propagate_error(error, terr);
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
