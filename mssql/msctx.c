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

#include <conf/keyconf.h>
#include "msctx.h"
#include "util.h"
#include "tsqlcheck.h"
#include "tsql.tab.h"
#include "tsql.parser.h"
#include "table.h"


struct pt_task {
  GMutex lock;
  GCond cond;
  gboolean fetched;
  
  int table_id;
  char *name;
  
  GList *list;
  GError *error;
};

GThreadPool *pt_help;


static int msg_handler(DBPROCESS *dbproc, DBINT msgno, int msgstate, int severity,
		       char *msgtext, char *srvname, char *procname, int line)
{
  enum {
    changed_database = 5701,
    changed_language = 5703
  };
  
  if (msgno == changed_database || msgno == changed_language)
    return 0;
  
  if (msgno > 0) {
    g_printerr("Msg %ld, Level %d, State %d\n",
	       (long) msgno, severity, msgstate);
    
    if (strlen(srvname) > 0)
      g_printerr("Server '%s', ", srvname);
    if (strlen(procname) > 0)
      g_printerr("Procedure '%s', ", procname);
    if (line > 0)
      g_printerr("Line %d", line);
    
    g_printerr("\n\t");
  }
  g_printerr("%s\n", msgtext);
  
  return 0;
}

static int err_handler(DBPROCESS * dbproc, int severity, int dberr, int oserr,
		       char *dberrstr, char *oserrstr)
{
  if (dberr) {
    g_printerr("Msg %d, Level %d\n",
	       dberr, severity);
    g_printerr("%s\n\n", dberrstr);
  } else {
    g_printerr("DB-LIBRARY error:\n\t");
    g_printerr("%s\n", dberrstr);
  }
  
  return INT_CANCEL;
}

static void help_thread(gpointer data, gpointer user_data)
{
  if (!data)
    return ;

  struct pt_task *task = (struct pt_task *) data;
  g_mutex_lock(&task->lock);

  msctx_t *ctx = get_msctx(&task->error);

  if (task->error == NULL)
    task->list = fetch_foreignes(task->table_id, task->name, ctx, &task->error);
  
  if (task->error == NULL)
    task->list = g_list_concat(task->list,
			       fetch_constraints(task->table_id, task->name,
						 ctx, &task->error)
			       );

  if (task->error == NULL)
    task->list = g_list_concat(task->list,
			       fetch_modules(task->table_id, task->name,
					     ctx, &task->error));

  close_sql(ctx);

  task->fetched = TRUE;

  g_cond_signal(&task->cond);
  g_mutex_unlock(&task->lock);
}

void init_msctx(GError **error)
{
  GError *terr = NULL;
  init_context(err_handler, msg_handler, &terr);
  int maxconn = get_context()->maxconn;

  if (maxconn > 1)
    pt_help = g_thread_pool_new(&help_thread, NULL,
				maxconn - 1, FALSE, &terr);
  
  initobjtypes();
  init_checker();

  if (terr != NULL)
    g_propagate_error(error, terr);
}

struct sqlfs_ms_obj * find_ms_object(struct sqlfs_ms_obj *parent,
				     const char *name, GError **error)
{
  GError *terr = NULL;
  GList *list = NULL;
  struct sqlfs_ms_obj *result = NULL;

  msctx_t *ctx = get_msctx(&terr);
  
  if (!parent) {

    // информация по базе данных
    if (!g_strcmp0(name, "/")) {
      result = g_try_new0(struct sqlfs_ms_obj, 1);
      result->name = g_strdup(name);
      result->type = D_DB;
    }
    // список схем
    else {
      list = fetch_schemas(name, ctx, FALSE, &terr);
    }
    
  }
  else {
    switch (parent->type) {
    case D_SCHEMA:
      list = fetch_schema_obj(parent->schema_id, name, ctx, &terr);
      break;
    case D_S:
    case D_U:
    case D_V:
      list = fetch_table_obj(parent->schema_id, parent->object_id,
			     name, ctx, &terr);
      break;
    default:
      g_set_error(&terr, EENOTSUP, EENOTSUP, NULL);
      break;
    }
  }
  
  close_sql(ctx);  
  
  if (terr != NULL)
    g_propagate_error(error, terr);
  else
    if (list != NULL) {
      result = g_list_first(list)->data;
      g_list_free(list);
    }
  
  return result;
}

#define CHECK_NODE(node, ctrt, def)					\
  if (node != NULL) {							\
    ctrt = g_try_new0(def, 1);						\
    if (node->check == WITH_CHECK)					\
      ctrt->disabled = 0;						\
    else								\
      ctrt->disabled = 1;						\
  }

char * write_ms_object(const char *schema, struct sqlfs_ms_obj *parent,
		       const char *text, struct sqlfs_ms_obj *obj, GError **err)
{
  int error = 0;
  GError *terr = NULL;
  char *wrktext = NULL;
  start_checker();

  YY_BUFFER_STATE bp;
  bp = yy_scan_string(text);
  yy_switch_to_buffer(bp);
  error = yyparse();
  objnode_t *node = NULL;

  if (!error)
    node = get_node();
  
  if (!error && node != NULL) {
    switch (node->type) {
    case COLUMN:
      obj->type = R_COL;

      if (!obj->column) {
	obj->column = g_try_new0(struct sqlfs_ms_column, 1);
      }

      if (node->column_node && node->column_node->is_identity > 0)
	obj->column->identity = 1;
      else
	obj->column->identity = 0;
      
      wrktext = create_column_def(schema, parent->name, obj,
				  text + node->first_column - 1);
      break;
    case CHECK:
      obj->type = R_C;
      CHECK_NODE(node->check_node, obj->clmn_ctrt, struct sqlfs_ms_constraint);
      wrktext = create_constr_def(schema, parent->name, obj,
				  text + node->last_column);
      break;
    case DEFAULT:
      obj->type = R_D;
      wrktext = create_constr_def(schema, parent->name, obj,
				  text + node->last_column);
      break;
    case PRIMARY_KEY:

      obj->type = R_PK;
      wrktext = create_index_def(schema, parent->name, obj,
				 text + node->last_column);
      break;
    case FOREIGN_KEY:
      obj->type = R_F;
      CHECK_NODE(node->check_node, obj->foreign_ctrt, struct sqlfs_ms_fk);
      wrktext = create_foreign_def(schema, parent->name, obj,
				   text + node->last_column);
      break;
    case UNIQUE:
      obj->type = R_UQ;
      wrktext = create_index_def(schema, parent->name, obj,
				 text + node->last_column);
      break;
    case INDEX:
      obj->type = R_X;
      wrktext = create_index_def(schema, parent->name, obj,
				 text + node->last_column);
      break;
    case PROC:
      obj->type = R_P;
      break;
    case FUNCTION:
      obj->type = R_FN;
      break;
    case TRIGGER:
      obj->type = R_TR;
      break;
    default:
      wrktext = g_strdup(text);
    }

    if (obj->type == R_P || obj->type == R_FN || obj->type == R_TR) {
      GString *sql = g_string_new(NULL);
      g_string_append_len(sql, text, node->module_node->first_columnm - 1);
      if (obj->object_id)
	g_string_append(sql, "ALTER");
      else
	g_string_append(sql, "CREATE");
      
      g_string_append_len(sql, text + node->module_node->last_columnm,
			  node->first_column - node->module_node->last_columnm - 1);
      
      g_string_append_printf(sql, "[%s].[%s]", schema, obj->name);
      
      if (obj->type == R_TR) {
	g_string_append_printf(sql, " ON [%s].[%s]", schema, parent->name);
      }
      
      g_string_append(sql, text + node->last_column);
      wrktext = g_strdup(sql->str);
      g_string_free(sql, TRUE);
    }

    wrktext = g_strchomp(wrktext);
    
  }
  else {
    g_set_error(&terr, EEPARSE, EEPARSE, "Parse error");
  }

  yy_flush_buffer(bp);
  yy_delete_buffer(bp);
  end_checker();

  if (terr != NULL)
    g_propagate_error(err, terr);
  
  return wrktext;

}
  
char * remove_ms_object(const char *schema, const char *parent,
			struct sqlfs_ms_obj *obj, GError **error)
{
  GError *terr = NULL;
  char *result = NULL;
  GString *sql = g_string_new(NULL);
  if (obj->type == R_COL) {
    g_string_append_printf(sql, "ALTER TABLE [%s].[%s] DROP COLUMN [%s]",
			   schema, parent, obj->name);
  }
  else if (obj->type == R_C || obj->type == R_D || obj->type == R_F
	   || obj->type == R_PK || obj->type == R_UQ) {
    g_string_append_printf(sql, "ALTER TABLE [%s].[%s] DROP CONSTRAINT [%s]",
			   schema, parent, obj->name);
  }
  else {
    g_string_append(sql, "DROP ");
    switch(obj->type) {
    case D_SCHEMA:
      g_string_append(sql, "SCHEMA");
      break;
    case D_U:
      g_string_append(sql, "TABLE");
      break;
    case D_V:
      g_string_append(sql, "VIEW");
      break;
    case R_FN:
    case R_FT:
    case R_FS:
    case R_TF:
    case R_IF:
      g_string_append(sql, "FUNCTION");
      break;
    case R_P:
      g_string_append(sql, "PROCEDURE");
      break;
    case R_TR:
      g_string_append(sql, "TRIGGER");
      break;
    case R_X:
      g_string_append(sql, "INDEX");
      break;
    default:
      g_set_error(&terr, EENOTSUP, EENOTSUP, "Module is not support");
    }

    if (obj->type != D_SCHEMA) {

      if (obj->type == R_X) {
	g_string_append_printf(sql, " [%s] ON [%s].[%s]", obj->name, schema, parent);
      }
      else
	g_string_append_printf(sql, " [%s].[%s]", schema, obj->name);

    }
    else
      g_string_append_printf(sql, " [%s]", obj->name);
  }

  if (terr == NULL) {
    result = g_strdup(sql->str);
  }

  if (terr != NULL)
    g_propagate_error(error, terr);
  
  g_string_free(sql, TRUE);

  return result;
}

static inline char * load_help_text(const char *parent, struct sqlfs_ms_obj *obj,
				    GError **error)
{
  GError *terr = NULL;
  char *def = NULL;
  GString *sql = g_string_new(NULL);
  
  g_string_append_printf(sql, "EXEC sp_helptext '[%s].[%s]'", parent, obj->name);
  msctx_t *ctx = exec_sql(sql->str, &terr);
  if (!terr) {
    g_string_truncate(sql, 0);
    
    DBCHAR def_buf[256];
    dbbind(ctx->dbproc, 1, NTBSTRINGBIND, (DBINT) 0, (BYTE *) def_buf);
    int rowcode;
    sqlctx_t *sqlctx = fetch_context(FALSE, &terr);
    gchar *convert = NULL;
    while (!terr && (rowcode = dbnextrow(ctx->dbproc)) != NO_MORE_ROWS) {
      switch(rowcode) {
      case REG_ROW:
	if (sqlctx->from_codeset != NULL && sqlctx->to_codeset != NULL)
	  convert = g_convert(def_buf, strlen(def_buf),
			      sqlctx->from_codeset, sqlctx->to_codeset,
			      NULL, NULL, &terr);
	else
	  convert = g_strdup(def_buf);
	
	g_string_append(sql, convert);
	
	g_free(convert);
	break;
      case BUF_FULL:
	g_set_error(&terr, EEFULL, EEFULL,
		    "%d: dbresults failed\n", __LINE__);
	break;
      case FAIL:
	g_set_error(&terr, EERES, EERES,
		    "%d: dbresults failed\n", __LINE__);
	break;
      }
    }
      
    if (!terr) {
      /* FIXME: #23 */
      if (obj->len > strlen(sql->str)) {
	char *nlspc = g_strnfill(obj->len - strlen(sql->str), ' ');
	g_string_append_printf(sql, nlspc);
	g_free(nlspc);
      }

      def = g_strdup(sql->str);
    }
    
  }
  
  close_sql(ctx);
  g_string_free(sql, TRUE);

  if (terr != NULL)
    g_propagate_error(error, terr);

  return def;
}

char * load_module_text(const char *parent, struct sqlfs_ms_obj *obj,
			GError **error)
{
  GError *terr = NULL;
  char *def = NULL;
  
  switch(obj->type) {
  case R_COL:
  case R_C:
  case R_D:
  case R_PK:
  case R_UQ:
  case R_X:
  case R_F:
    def = g_strdup(obj->def);
    break;
  default:
    def = load_help_text(parent, obj, &terr);
    break;
  }
  
  if (terr != NULL)
    g_propagate_error(error, terr);

  return def;
}

char * rename_ms_object(const char *schema_old, const char *schema_new,
			struct sqlfs_ms_obj *obj_old, struct sqlfs_ms_obj *obj_new,
			struct sqlfs_ms_obj *parent, GError **error)
{
  char *result = NULL;
  GError *terr = NULL;
  
  if (obj_old->type == R_TR && g_str_has_prefix(obj_old->name, "#"))
    g_set_error(&terr, EENOTSUP, EENOTSUP,
		"%d: operation not supported\n", __LINE__);

  if (terr == NULL) {
    GString *sql = g_string_new(NULL);

    const gchar *wrksch = schema_old;
    if (schema_old != NULL && schema_new != NULL
	&& g_strcmp0(schema_old, schema_new)) {

      switch(obj_old->type) {
      case D_IT:
      case D_S:
      case D_TT:
      case D_U:
      case D_V:
      case R_P:
      case R_FN:
      case R_FS:
      case R_FT:
      case R_TF:
      case R_IF:
	g_string_append_printf(sql, "ALTER SCHEMA [%s] TRANSFER [%s].[%s];\n",
			       schema_new, schema_old, obj_old->name);
	wrksch = schema_new;
	break;
      default:
	g_set_error(&terr, EENOTSUP, EENOTSUP,
		    "%d: operation not supported\n", __LINE__);
      }
      
    }

    if (terr == NULL && g_strcmp0(obj_old->name, obj_new->name)) {
      switch (obj_old->type) {
      case R_COL:
      case R_X:
	g_string_append_printf(sql, "EXEC sp_rename '[%s].[%s].[%s]', '%s'",
			       wrksch, parent->name, obj_old->name, obj_new->name);
	break;
      default:
	g_string_append_printf(sql, "EXEC sp_rename '[%s].[%s]', '%s'",
			       wrksch, obj_old->name, obj_new->name);
	break;
      }
    }

    if (terr == NULL) {
      result = g_strdup(sql->str);
    }
    
    g_string_free(sql, TRUE);
  }
    
  if (terr != NULL)
    g_propagate_error(error, terr);

  return result;
}

GList * fetch_table_obj(int schema_id, int table_id, const char *name,
			msctx_t *ctx, GError **error)
{
  GList *reslist = NULL;
  GError *terr = NULL;
  struct pt_task *task = NULL;

  // отрицательное, когда есть ожидающие процессы
  int free_conn = get_count_free_contexts();

  if (free_conn > 0 && table_id) {
    task = g_try_new0(struct pt_task, 1);

    if (name != NULL)
      task->name = g_strdup(name);
    else
      task->name = NULL;

    task->error = NULL;
    task->fetched = FALSE;
    task->table_id = table_id;
    g_mutex_init(&task->lock);
    g_cond_init(&task->cond);
    
    g_thread_pool_push(pt_help, task, &terr);
  }
  
  reslist = fetch_columns(table_id, name, ctx, &terr);

  // необходимо загрузить основной поток
  if (terr == NULL)
    reslist = g_list_concat(reslist, fetch_indexes(table_id, name, ctx, &terr));

  if (free_conn > 0 && table_id) {
    g_mutex_lock(&task->lock);

    // ждать сигнала о завершении из пула потоков
    while (!task->fetched) {
      g_cond_wait(&task->cond, &task->lock);
    }

    if (task->error != NULL) {
      terr = g_error_copy(task->error);
    }

    reslist = g_list_concat(reslist, task->list);

    if (task->error != NULL)
      g_error_free(task->error);

    g_mutex_unlock(&task->lock);
    g_mutex_clear(&task->lock);
    g_cond_clear(&task->cond);
    
    if (task->name != NULL)
      g_free(task->name);
    
    g_free(task);
  }
  else {
    if (terr == NULL)
      reslist = g_list_concat(reslist, fetch_modules(table_id, name, ctx, &terr));

    if (terr == NULL)
      reslist = g_list_concat(reslist, fetch_foreignes(table_id, name, ctx, &terr));

    if (terr == NULL)
      reslist = g_list_concat(reslist, fetch_constraints(table_id, name, ctx, &terr));
  }

  if (terr != NULL)
    g_propagate_error(error, terr);

  return reslist;
}

GList * fetch_xattr_list(int class_id, int major_id, int minor_id,
			 msctx_t *ctx, GError **error)
{
  GList *lst = NULL;
  GError *terr = NULL;

  GString *sql = g_string_new(NULL);
  g_string_append(sql, "SELECT ");
  g_string_append(sql, " perm.type, perm.permission_name");
  g_string_append(sql, " , prin.name, perm.state, perm.state_desc ");
  g_string_append(sql, "FROM sys.database_permissions perm");
  g_string_append(sql, " INNER JOIN sys.database_principals prin");
  g_string_append(sql, "   ON prin.principal_id = perm.grantee_principal_id");
  g_string_append_printf(sql, " WHERE perm.major_id = %d", major_id);
  g_string_append_printf(sql, "  AND perm.minor_id = %d", minor_id);
  g_string_append_printf(sql, "  AND perm.class = %d", class_id);

  if (terr == NULL)
    exec_sql_cmd(sql->str, ctx, &terr);

  if (!terr) {
    DBCHAR state_buf[2], type_buf[5];
    
    char *perm_buf = g_malloc0_n(dbcollen(ctx->dbproc, 2) + 1,
				 sizeof(char ));
    char *name_buf = g_malloc0_n(dbcollen(ctx->dbproc, 3) + 1,
				 sizeof(char ));
    
    char *state_desc_buf = g_malloc0_n(dbcollen(ctx->dbproc, 5) + 1,
				       sizeof(char ));
    
    dbbind(ctx->dbproc, 1, STRINGBIND, (DBINT) 0, (BYTE *) type_buf);
    
    dbbind(ctx->dbproc, 2, STRINGBIND,
	   dbcollen(ctx->dbproc, 2), (BYTE *) perm_buf);
    dbbind(ctx->dbproc, 3, STRINGBIND,
	   dbcollen(ctx->dbproc, 3), (BYTE *) name_buf);

    dbbind(ctx->dbproc, 4, STRINGBIND, (DBINT) 0, (BYTE *) state_buf);
    
    dbbind(ctx->dbproc, 5, STRINGBIND,
	   dbcollen(ctx->dbproc, 5), (BYTE *) state_desc_buf);

    int rowcode;
    struct sqlfs_ms_acl *acl = NULL;
    while (!terr && (rowcode = dbnextrow(ctx->dbproc)) != NO_MORE_ROWS) {
      switch(rowcode) {
      case REG_ROW:
	acl = g_try_new0(struct sqlfs_ms_acl, 1);

	acl->type = g_strdup(g_strchomp(type_buf));
	acl->perm_name = g_strdup(g_strchomp(perm_buf));
	acl->state = g_strdup(g_strchomp(state_buf));
	acl->state_desc = g_strdup(g_strchomp(state_desc_buf));
	acl->principal_name = g_strdup(g_strchomp(name_buf));
	
    	lst = g_list_append(lst, acl);
	break;
      case BUF_FULL:
	g_set_error(&terr, EEFULL, EEFULL,
		    "%d: dbresults failed\n", __LINE__);
	break;
      case FAIL:
	g_set_error(&terr, EERES, EERES,
		    "%d: dbresults failed\n", __LINE__);
	break;
      }
    }
    
    g_free(perm_buf);
    g_free(name_buf);
    g_free(state_desc_buf);
  }
  
  g_string_free(sql, TRUE);
  
  if (terr != NULL)
    g_propagate_error(error, terr);
  
  return lst;
}

GList * fetch_schema_obj(int schema_id, const char *name,
			 msctx_t *ctx, GError **error)
{
  GList *lst = NULL;
  GError *terr = NULL;
  
  GString * sql = g_string_new(NULL);
  if (!schema_id) {
    g_string_append(sql, "INSERT INTO #sch_objs (dir_path, obj_id, ");
    g_string_append(sql, "obj_type, ctime, mtime, def_len)\n");
    g_string_append(sql, "SELECT ss.dir_path + '/' + so.name");
  }
  else
    g_string_append(sql, "SELECT so.name");
  
  g_string_append(sql, ", so.object_id, so.type");
  g_string_append(sql, ", DATEDIFF(second, {d '1970-01-01'}, so.create_date)");
  g_string_append(sql, ", DATEDIFF(second, {d '1970-01-01'}, so.modify_date) ");
  g_string_append(sql, ", DATALENGTH(sm.definition) ");
  g_string_append(sql, "FROM sys.objects so LEFT JOIN sys.sql_modules sm");  
  g_string_append(sql, " ON sm.object_id = so.object_id");

  if (!schema_id)
    g_string_append(sql, " INNER JOIN #schemas ss ON ss.sch_id = so.schema_id");

  g_string_append(sql, " WHERE parent_object_id = 0 \n");
  
  if (schema_id) {
    g_string_append_printf(sql, " AND schema_id = %d", schema_id);
    
    if (name)
      g_string_append_printf(sql, " AND so.name = '%s'", name);
  }
  else {
    exec_sql_cmd(sql->str, ctx, &terr);

    g_string_truncate(sql, 0);

    g_string_append(sql, "SELECT dir_path, obj_id, obj_type,");
    g_string_append(sql, " ctime, mtime, def_len ");
    g_string_append(sql, "FROM #sch_objs");
  }

  if (terr == NULL)
    exec_sql_cmd(sql->str, ctx, &terr);
  
  if (!terr) {
    DBINT obj_id_buf;
    DBINT def_len_buf;
    char * name_buf = g_malloc0_n(dbcollen(ctx->dbproc, 1) + 1,
				  sizeof(char ));
    DBCHAR type_buf[2];
    DBINT cdate_buf, mdate_buf;

    dbbind(ctx->dbproc, 1, STRINGBIND,
	   dbcollen(ctx->dbproc, 1), (BYTE *) name_buf);
    dbbind(ctx->dbproc, 2, INTBIND, (DBINT) 0, (BYTE *) &obj_id_buf);
    dbbind(ctx->dbproc, 3, STRINGBIND, (DBINT) 0, (BYTE *) type_buf);
    dbbind(ctx->dbproc, 4, INTBIND, (DBINT) 0, (BYTE *) &cdate_buf);
    dbbind(ctx->dbproc, 5, INTBIND, (DBINT) 0, (BYTE *) &mdate_buf);
    dbbind(ctx->dbproc, 6, INTBIND, (DBINT) 0, (BYTE *) &def_len_buf);
    
    int rowcode;
    while (!terr && (rowcode = dbnextrow(ctx->dbproc)) != NO_MORE_ROWS) {
      switch(rowcode) {
      case REG_ROW:
	name_buf = g_strchomp(name_buf);
	char *typen = g_strndup(type_buf, 2);
	struct sqlfs_ms_obj *obj = g_try_new0(struct sqlfs_ms_obj, 1);
	
	obj->name = g_strdup(name_buf);
	obj->type = str2mstype(g_strchomp(typen));
	obj->schema_id = schema_id;
	obj->object_id = obj_id_buf;
	obj->ctime = cdate_buf;
	obj->mtime = mdate_buf;

	if (obj->type == R_P || obj->type == R_FT || obj->type == R_FS
	    || obj->type == R_FN || obj->type == R_TF || obj->type == R_IF) {
	  obj->len = def_len_buf;
	}

	g_free(typen);
	
	lst = g_list_append(lst, obj);
	break;
      case BUF_FULL:
	g_set_error(&terr, EEFULL, EEFULL,
		    "%d: dbresults failed\n", __LINE__);
	break;
      case FAIL:
	g_set_error(&terr, EERES, EERES,
		    "%d: dbresults failed\n", __LINE__);
	break;
      }
    }
    
    g_free(name_buf);
  }
  
  g_string_free(sql, TRUE);

  if (terr != NULL)
    g_propagate_error(error, terr);

  return lst;
}

GList * fetch_schemas(const char *name, msctx_t *ctx, int astart, GError **error)
{
  GString * sql = g_string_new(NULL);
  GError *terr = NULL;
  if (astart) {
    g_string_append(sql, "INSERT INTO #schemas (dir_path, sch_id)\n");
    g_string_append(sql, "SELECT '/' + name, ");
  }
  else
    g_string_append(sql, "SELECT name, ");
  
  g_string_append(sql, "schema_id FROM sys.schemas ");
  
  if (name)
    g_string_append_printf(sql, "WHERE name = '%s'", name);
  else {
    gchar **excl = get_context()->excl_sch;
    if (excl != NULL && g_strv_length(excl) > 0) {
      gchar *excl_sch = g_strjoinv("','", excl);
      g_string_append_printf(sql, "WHERE name NOT IN ('%s')", excl_sch);
      g_free(excl_sch);
    }
  }

  if (astart) {
    exec_sql_cmd(sql->str, ctx, &terr);
    g_string_truncate(sql, 0);

    g_string_append(sql, "SELECT dir_path, sch_id FROM #schemas");
  }

  GList *lst = NULL;

  if (terr == NULL)
    exec_sql_cmd(sql->str, ctx, &terr);
  
  if (!terr && ctx) {
    int rowcode;
    DBINT schid_buf;
    char * schname_buf = g_malloc0_n(dbcollen(ctx->dbproc, 1) + 1, sizeof(char ));
    dbbind(ctx->dbproc, 1, STRINGBIND, dbcollen(ctx->dbproc, 1) + 1,
	   (BYTE *) schname_buf);
    dbbind(ctx->dbproc, 2, INTBIND, (DBINT) 0, (BYTE *) &schid_buf);

    while (!terr && (rowcode = dbnextrow(ctx->dbproc)) != NO_MORE_ROWS) {
      switch(rowcode) {
      case REG_ROW: {
	struct sqlfs_ms_obj *obj = g_try_new0(struct sqlfs_ms_obj, 1);
	obj->name = g_strdup(g_strchomp(schname_buf));
	obj->type = D_SCHEMA;
	obj->schema_id = schid_buf;
	
	lst = g_list_append(lst, obj);
      }
	break;
      case BUF_FULL:
	g_set_error(&terr, EEFULL, EEFULL,
		    "%d: dbresults failed\n", __LINE__);
	break;
      case FAIL:
	g_set_error(&terr, EERES, EERES,
		    "%d: dbresults failed\n", __LINE__);
	break;
      }
    }
    
    g_free(schname_buf);
  }
  
  g_string_free(sql, TRUE);

  if (terr != NULL)
    g_propagate_error(error, terr);
  
  return lst;
}

void free_ms_obj(gpointer msobj)
{
  if (!msobj)
    return ;
  
  struct sqlfs_ms_obj *obj = (struct sqlfs_ms_obj *) msobj;
  
  switch(obj->type) {
  case R_D:
    if (obj->clmn_ctrt != NULL) {
      if (obj->clmn_ctrt->column_name != NULL) {
	g_free(obj->clmn_ctrt->column_name);
      }

      g_free(obj->clmn_ctrt);
    }
    break;
  case R_C:
    if (obj->clmn_ctrt != NULL) {
      g_free(obj->clmn_ctrt);
    }
    break;
  case R_F:
    if (obj->foreign_ctrt != NULL) {
      if (obj->foreign_ctrt->columns_def != NULL)
	g_free(obj->foreign_ctrt->columns_def);
      
      if (obj->foreign_ctrt->ref_object_def != NULL)
	g_free(obj->foreign_ctrt->ref_object_def);

      if (obj->foreign_ctrt->ref_columns_def != NULL)
	g_free(obj->foreign_ctrt->ref_columns_def);

      g_free(obj->foreign_ctrt);
    }
    break;
  case R_PK:
  case R_UQ:
  case R_X:
    if (obj->index != NULL) {
      if (obj->index->filter_def != NULL)
	g_free(obj->index->filter_def);

      if (obj->index->columns_def != NULL)
	g_free(obj->index->columns_def);

      if (obj->index->incl_columns_def != NULL)
	g_free(obj->index->incl_columns_def);

      if (obj->index->data_space != NULL)
	g_free(obj->index->data_space);
      
      g_free(obj->index);
    }
    break;
  case R_P:
  case R_FN:
  case R_TF:
  case R_IF:
  case R_FT:
    if (obj->sql_module != NULL) {
      
      g_free(obj->sql_module);
    }
    break;
  case R_COL:
    if (obj->column != NULL) {
      if (obj->column->type_name != NULL)
	g_free(obj->column->type_name);
      
      if (obj->column->identity) {
	if (obj->column->seed_val != NULL)
	  g_free(obj->column->seed_val);

	if (obj->column->inc_val != NULL)
	  g_free(obj->column->inc_val);
      }

      g_free(obj->column);
    }
    break;
  case R_TYPE:
    if (obj->mstype != NULL) {
      if (obj->mstype->collation_name != NULL)
	g_free(obj->mstype->collation_name);
    
      g_free(obj->mstype);
    }
    break;
  }

  if (obj->def != NULL)
    g_free(obj->def);
  
  if (obj->name)
    g_free(obj->name);

  if (obj != NULL)
    g_free(obj);

  obj = NULL;
}

void close_msctx(GError **error)
{

  if (get_context()->maxconn > 1)
    g_thread_pool_free(pt_help, TRUE, TRUE);
  
  close_checker();
  close_context(error);
  dbexit();
}
