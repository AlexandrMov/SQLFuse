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

#include "mssqlfs.h"
#include "util.h"
#include "exec.h"
#include "tsqlcheck.h"
#include "tsql.tab.h"
#include "tsql.parser.h"
#include "table.h"

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


void init_msctx(struct sqlctx *ctx, GError **error)
{
  init_context(ctx, err_handler, msg_handler, error);
  initobjtypes();
  init_checker();
}

struct sqlfs_ms_obj * find_ms_object(const struct sqlfs_ms_obj *parent,
				     const char *name, GError **error)
{
  GError *terr = NULL;
  GList *list = NULL;
  if (!parent) {
    list = fetch_schemas(name, &terr);
  }
  else {
    switch (parent->type) {
    case D_SCHEMA:
      list = fetch_schema_obj(parent->schema_id, name, &terr);
      break;
    case D_S:
    case D_U:
    case D_V:
      list = fetch_table_obj(parent->schema_id, parent->object_id,
			     name, &terr);
      break;
    default:
      g_set_error(&terr, EENOTSUP, EENOTSUP, NULL);
      break;
    }
  }

  struct sqlfs_ms_obj *result = NULL;
  if (terr != NULL)
    g_propagate_error(error, terr);
  else
    if (list != NULL)
      result = g_list_first(list)->data;
  
  return result;
}

void write_ms_object(const char *schema, struct sqlfs_ms_obj *parent,
		     const char *text, struct sqlfs_ms_obj *obj, GError **err)
{
  int error = 0;
  GError *terr = NULL;
  start_checker();

  YY_BUFFER_STATE bp;
  bp = yy_scan_string(text);
  yy_switch_to_buffer(bp);  
  error = yyparse();
  objnode_t *node = NULL;
  
  if (!error)
    node = get_node();
  
  if (!error && node != NULL) {
    char *wrktext = NULL;
    switch (node->type) {
    case COLUMN:
      wrktext = create_column_def(schema, parent->name, obj,
				  text + node->first_column - 1);
      break;
    case CHECK:
      obj->type = R_C;
      if (node->check_node != NULL) {
	obj->clmn_ctrt = g_try_new0(struct sqlfs_ms_constraint, 1);
	if (node->check_node->check == WITH_CHECK)
	  obj->clmn_ctrt->disabled = 0;
	else
	  obj->clmn_ctrt->disabled = 1;
      }
      wrktext = create_constr_def(schema, parent->name, obj,
				  text + node->first_column - 1);
      break;
    case DEFAULT:
      obj->type = R_D;
      wrktext = create_constr_def(schema, parent->name, obj,
				  text + node->first_column - 1);
      break;
    case PROC:
    case FUNCTION:
    case VIEW:
    case TRIGGER: {
      GString *sql = g_string_new(NULL);
      g_string_append_len(sql, text, node->module_node->first_columnm - 1);
      if (obj->object_id)
	g_string_append(sql, "ALTER ");
      else
	g_string_append(sql, "CREATE");
      
      g_string_append_len(sql, text + node->module_node->last_columnm,
			  node->first_column - node->module_node->last_columnm - 1);
      g_string_append_printf(sql, "%s.%s", schema, obj->name);
      
      if (node->type == TRIGGER) {
	g_string_append_printf(sql, " ON %s.%s", schema, parent->name);
      }
      
      g_string_append(sql, text + node->last_column);
      wrktext = g_strdup(sql->str);
      
      g_string_free(sql, TRUE);
    }
      break;
    default:
      wrktext = g_strdup(text);
    }

    msctx_t *ctx = exec_sql(wrktext, &terr);
    close_sql(ctx);
    g_free(wrktext);
  }
  else {
    g_set_error(&terr, EEPARSE, EEPARSE, NULL);
  }

  yy_flush_buffer(bp);
  yy_delete_buffer(bp);
  end_checker();

  if (terr != NULL)
    g_propagate_error(err, terr);
}
  
void remove_ms_object(const char *schema, const char *parent,
		      struct sqlfs_ms_obj *obj, GError **error)
{
  GError *terr = NULL;
  GString *sql = g_string_new(NULL);
  if (obj->type == R_COL) {
    g_string_append_printf(sql, "ALTER TABLE [%s].[%s] DROP COLUMN [%s]",
			   schema, parent, obj->name);
  }
  else if (obj->type == R_C || obj->type == R_D) {
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
      g_string_append(sql, "FUNCTION");
      break;
    case R_P:
      g_string_append(sql, "PROCEDURE");
      break;
    case R_TR:
      g_string_append(sql, "TRIGGER");
      break;
    default:
      g_set_error(&terr, EENOTSUP, EENOTSUP, NULL);
    }
    
    g_string_append_printf(sql, " [%s].[%s]", schema, obj->name);
  }
  
  if (terr == NULL) {
    msctx_t *ctx = exec_sql(sql->str, &terr);
    close_sql(ctx);
  }

  if (terr != NULL)
    g_propagate_error(error, terr);
  
  g_string_free(sql, TRUE);

}

static inline char * load_help_text(const char *parent, struct sqlfs_ms_obj *obj,
				    GError **error)
{
  GError *terr = NULL;
  char *def = NULL;
  
  GString *sql = g_string_new(NULL);
  g_string_append_printf(sql, "EXEC sp_helptext '%s.%s'", parent, obj->name);
  msctx_t *ctx = exec_sql(sql->str, &terr);

  if (!terr) {
    g_string_truncate(sql, 0);
    
    struct sqlfs_ms_module * module = NULL;
    DBCHAR def_buf[256];
    dbbind(ctx->dbproc, 1, NTBSTRINGBIND, (DBINT) 0, (BYTE *) def_buf);
    int rowcode;
    
    while (!terr && (rowcode = dbnextrow(ctx->dbproc)) != NO_MORE_ROWS) {
      switch(rowcode) {
      case REG_ROW:
	g_string_append_printf(sql, g_convert(def_buf, strlen(def_buf), "UTF-8", "CP1251",
					      NULL, NULL, &terr));
	break;
      case BUF_FULL:
	break;
      case FAIL:
	g_set_error(&terr, EERES, EERES,
		    "%d: dbresults failed\n", __LINE__);
	break;
      }
    }
      
    if (!terr) {
      module = g_try_new0(struct sqlfs_ms_module, 1);
      module->def = g_strdup(sql->str);
      obj->sql_module = module;
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
    if (obj->column)
      def = g_strdup(obj->column->def);
    break;
  case R_C:
  case R_D:
    if (obj->clmn_ctrt)
      def = g_strdup(obj->clmn_ctrt->def);
    break;
  default:
    def = load_help_text(parent, obj, &terr);
    break;
  }
  
  if (terr != NULL)
    g_propagate_error(error, terr);

  return def;
}

GList * fetch_table_obj(int schema_id, int table_id, const char *name,
			GError **error)
{
  GList *reslist = NULL;
  GError *terr = NULL;

  reslist = fetch_columns(table_id, name, &terr);
  
  if (terr == NULL)
    reslist = g_list_concat(reslist, fetch_modules(table_id, name, &terr));

  /*if (terr == NULL)
    reslist = g_list_concat(reslist, fetch_indexes(table_id, name, &terr));*/

  if (terr == NULL) {
    reslist = g_list_concat(reslist, fetch_constraints(table_id, name, &terr));
  }

  if (terr != NULL) {
    g_message("%d: %s\n", terr->code, terr->message);
  }
  
  return reslist;
}

GList * fetch_schema_obj(int schema_id, const char *name,
			 GError **error)
{
  GList *lst = NULL;
  GError *terr = NULL;
  
  GString * sql = g_string_new(NULL);
  g_string_append(sql, "SELECT so.object_id, so.name, so.type");
  g_string_append(sql, ", DATEDIFF(second, {d '1970-01-01'}, so.create_date)");
  g_string_append(sql, ", DATEDIFF(second, {d '1970-01-01'}, so.modify_date) ");
  g_string_append(sql, ", DATALENGTH(sm.definition) ");
  g_string_append(sql, "FROM sys.objects so LEFT JOIN sys.sql_modules sm");
  g_string_append(sql, " ON sm.object_id = so.object_id");
  g_string_append_printf(sql, " WHERE schema_id = %d", schema_id);
  g_string_append(sql, " AND parent_object_id = 0");

  if (name)
    g_string_append_printf(sql, " AND so.name = '%s'", name);

  msctx_t *ctx = exec_sql(sql->str, &terr);
  if (!terr) {
    DBINT obj_id_buf;
    DBINT def_len_buf;
    char * name_buf = g_malloc0_n(dbcollen(ctx->dbproc, 2) + 1,
				  sizeof(char ));
    DBCHAR type_buf[2];
    DBINT cdate_buf, mdate_buf;

    dbbind(ctx->dbproc, 1, INTBIND, (DBINT) 0, (BYTE *) &obj_id_buf);
    dbbind(ctx->dbproc, 2, STRINGBIND,
	   dbcollen(ctx->dbproc, 2), (BYTE *) name_buf);
    dbbind(ctx->dbproc, 3, STRINGBIND, (DBINT) 0, (BYTE *) type_buf);
    dbbind(ctx->dbproc, 4, INTBIND, (DBINT) 0, (BYTE *) &cdate_buf);
    dbbind(ctx->dbproc, 5, INTBIND, (DBINT) 0, (BYTE *) &mdate_buf);
    dbbind(ctx->dbproc, 6, INTBIND, (DBINT) 0, (BYTE *) &def_len_buf);
    int rowcode;
    struct sqlfs_ms_obj * obj = NULL;
    while (!terr && (rowcode = dbnextrow(ctx->dbproc)) != NO_MORE_ROWS) {
      switch(rowcode) {
      case REG_ROW:
	name_buf = trimwhitespace(name_buf);
	obj = g_try_new0(struct sqlfs_ms_obj, 1);
	obj->name = g_strdup(name_buf);
	obj->type = str2mstype(trimwhitespace(type_buf));
	obj->schema_id = schema_id;
	obj->object_id = obj_id_buf;
	obj->ctime = cdate_buf;
	obj->mtime = mdate_buf;
	obj->cached_time = g_get_monotonic_time();

	if (obj->type == R_P || obj->type == D_V ||
	    obj->type == R_FN || obj->type == R_TF) {
	  obj->len = def_len_buf;
	}
	lst = g_list_append(lst, obj);
	break;
      case BUF_FULL:
	break;
      case FAIL:
	g_set_error(&terr, EERES, EERES,
		    "%d: dbresults failed\n", __LINE__);
	break;
      }
    }
    
    g_free(name_buf);
  }
  
  close_sql(ctx);
  g_string_free(sql, TRUE);

  if (terr != NULL)
    g_propagate_error(error, terr);

  return lst;
}

GList * fetch_schemas(const char *name, GError **error)
{
  GString * sql = g_string_new(NULL);
  g_string_append(sql, "SELECT schema_id, name FROM sys.schemas");
  if (name)
    g_string_append_printf(sql, " WHERE name = '%s'", name);

  GList *lst = NULL;
  GError *terr = NULL;
  msctx_t *ctx = exec_sql(sql->str, &terr);
  if (!terr && ctx) {
    int rowcode;
    DBINT schid_buf;
    char * schname_buf = g_malloc0_n(dbcollen(ctx->dbproc, 2) + 1, sizeof(char ));
    dbbind(ctx->dbproc, 1, INTBIND, (DBINT) 0, (BYTE *) &schid_buf);
    dbbind(ctx->dbproc, 2, STRINGBIND, dbcollen(ctx->dbproc, 2) + 1,
	   (BYTE *) schname_buf);

    struct sqlfs_ms_obj *obj = NULL;
    while ((rowcode = dbnextrow(ctx->dbproc)) != NO_MORE_ROWS) {
      switch(rowcode) {
      case REG_ROW:
	obj = g_try_new0(struct sqlfs_ms_obj, 1);
	obj->name = g_strdup(trimwhitespace(schname_buf));
	obj->type = D_SCHEMA;
	obj->schema_id = schid_buf;
	lst = g_list_append(lst, obj);
	break;
      case BUF_FULL:
	break;
      case FAIL:
	g_set_error(&terr, EERES, EERES,
		    "%d: dbresults failed\n", __LINE__);
	break;
      }
    }
    
    g_free(schname_buf);
  }
  
  close_sql(ctx);
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
  case R_P:
  case D_V:
  case R_FN:
  case R_TF:
    if (obj->sql_module != NULL) {
      if (obj->sql_module->def != NULL)
	g_free(obj->sql_module->def);
      
      g_free(obj->sql_module);
    }
    break;
  case R_COL:
    if (obj->column != NULL) {
      if (obj->column->type_name != NULL)
	g_free(obj->column->type_name);

      if (obj->column->def != NULL)
	g_free(obj->column->def);
      
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

  if (obj->name)
    g_free(obj->name);
  
  g_free(obj);
}

void close_msctx(GError **error)
{
  close_checker();
  close_context(error);
  dbexit();
}
