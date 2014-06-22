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
#include "table.h"
#include "exec.h"
#include "util.h"

char * create_column_def(const char *schema, const char *table,
			 struct sqlfs_ms_obj *obj, const char *def)
{
  char *result = NULL;
  GString *sql = g_string_new(NULL);
  g_string_append_printf(sql, "ALTER TABLE %s.%s", schema, table);
  if (obj->object_id) {
    g_string_append(sql, " ALTER COLUMN ");
  }
  else {
    g_string_append(sql, " ADD ");
  }
  g_string_append_printf(sql, "%s %s", obj->name, def);

  result = g_strdup(sql->str);
  g_string_free(sql, TRUE);
  
  return result;
}

char * create_constr_def(const char *schema, const char *table,
			 struct sqlfs_ms_obj *obj, const char *def)
{
  char *result = NULL;

  GString *sql = g_string_new(NULL);
  g_string_append_printf(sql, "ALTER TABLE [%s].[%s]", schema, table);
  if (obj->object_id) {
    g_string_append_printf(sql, " DROP CONSTRAINT [%s] \n", obj->name);
    g_string_append_printf(sql, "ALTER TABLE [%s].[%s]", schema, table);
  }

  if (obj->type == R_C && obj->clmn_ctrt) {
    if (obj->clmn_ctrt->disabled)
      g_string_append(sql, " WITH NOCHECK ");
    else
      g_string_append(sql, " WITH CHECK ");
  }

  g_string_append_printf(sql, " ADD CONSTRAINT [%s] ", obj->name);

  if (obj->type == R_C) {
    g_string_append_printf(sql, "CHECK", def);
    if (obj->clmn_ctrt->not4repl)
      g_string_append(sql, " NOT FOR REPLICATION");

    g_string_append_printf(sql, " %s", def);
  }

  if (obj->type == R_D) {
    g_string_append_printf(sql, " DEFAULT %s", def);
  }

  result = g_strdup(sql->str);
  g_string_free(sql, TRUE);
  
  return result;
}

char * make_column_def(const struct sqlfs_ms_obj *obj)
{
  if (!obj || !obj->column)
    return NULL;

  char *text = NULL;
  struct sqlfs_ms_column *col = obj->column;
  GString *def = g_string_new(NULL);
  g_string_append_printf(def, "COLUMN %s %s", obj->name, col->type_name);
  
  if (!g_strcmp0(col->type_name, "float"))
    g_string_append_printf(def, "(%d)", col->precision);
  
  if (!g_strcmp0(col->type_name, "numeric")
      || !g_strcmp0(col->type_name, "decimal"))
    g_string_append_printf(def, "(%d, %d)", col->precision, col->scale);

  if (g_str_has_suffix(col->type_name, "char")
      || g_str_has_suffix(col->type_name, "binary")) {
    if (col->max_len < 0)
      g_string_append(def, "(MAX)");
    else
      g_string_append_printf(def, "(%d)", col->max_len);
  }

  if (col->identity) {
    g_string_append_printf(def, " IDENTITY ('%s', '%s')",
			   col->seed_val, col->inc_val);
    
    if (col->not4repl)
      g_string_append(def, " NOT FOR REPLICATION");
  }
  
  if (!col->nullable)
    g_string_append(def, " NOT NULL");
  
  text = g_strconcat(def->str, "\n", NULL);
  g_string_free(def, TRUE);

  return text;
}

GList * fetch_columns(int tid, const char *name, GError **error)
{
  GError *terr = NULL;
  GList *reslist = NULL;
  GString *sql = g_string_new(NULL);
  g_string_append(sql, "SELECT sc.column_id, sc.name");
  g_string_append(sql, ", sc.system_type_id, sc.max_length");
  g_string_append(sql, ", sc.precision, sc.scale, sc.is_nullable");
  g_string_append(sql, ", sc.is_ansi_padded, sc.is_identity, st.name");
  g_string_append(sql, ", CAST(idc.seed_value AS NVARCHAR(MAX))");
  g_string_append(sql, ", CAST(idc.increment_value AS NVARCHAR(MAX))");
  g_string_append(sql, ", idc.is_not_for_replication");
  g_string_append(sql, " FROM sys.columns sc INNER JOIN sys.types st");
  g_string_append(sql, "  ON st.user_type_id = sc.user_type_id");
  g_string_append(sql, " LEFT JOIN sys.identity_columns idc ");
  g_string_append(sql, "  ON idc.object_id = sc.object_id ");
  g_string_append(sql, "    AND idc.column_id = sc.column_id ");
  g_string_append_printf(sql, " WHERE sc.object_id = %d", tid);

  if (name)
    g_string_append_printf(sql, " AND sc.name = '%s'", name);

  msctx_t *ctx = exec_sql(sql->str, &terr);

  if (!terr) {
    DBINT col_id_buf, type_id_buf, mlen, precision, scale, nullable,
      ansi, identity, not4repl;
    char *colname_buf = g_malloc0_n(dbcollen(ctx->dbproc, 2) + 1, sizeof(char ));
    char *typename_buf = g_malloc0_n(dbcollen(ctx->dbproc, 10) + 1, sizeof(char ));
    char *seed_val = g_malloc0_n(dbcollen(ctx->dbproc, 11) + 1, sizeof(char ));
    char *inc_val = g_malloc0_n(dbcollen(ctx->dbproc, 12) + 1, sizeof(char ));

    dbbind(ctx->dbproc, 1, INTBIND, (DBINT) 0, (BYTE *) &col_id_buf);
    dbbind(ctx->dbproc, 2, STRINGBIND,
	   dbcollen(ctx->dbproc, 2), (BYTE *) colname_buf);
    dbbind(ctx->dbproc, 3, INTBIND, (DBINT) 0, (BYTE *) &type_id_buf);
    dbbind(ctx->dbproc, 4, INTBIND, (DBINT) 0, (BYTE *) &mlen);
    dbbind(ctx->dbproc, 5, INTBIND, (DBINT) 0, (BYTE *) &precision);
    dbbind(ctx->dbproc, 6, INTBIND, (DBINT) 0, (BYTE *) &scale);
    dbbind(ctx->dbproc, 7, INTBIND, (DBINT) 0, (BYTE *) &nullable);
    dbbind(ctx->dbproc, 8, INTBIND, (DBINT) 0, (BYTE *) &ansi);
    dbbind(ctx->dbproc, 9, INTBIND, (DBINT) 0, (BYTE *) &identity);
    dbbind(ctx->dbproc, 10, STRINGBIND,
	   dbcollen(ctx->dbproc, 10), (BYTE *) typename_buf);
    dbbind(ctx->dbproc, 11, STRINGBIND,
	   dbcollen(ctx->dbproc, 11), (BYTE *) seed_val);
    dbbind(ctx->dbproc, 12, STRINGBIND,
	   dbcollen(ctx->dbproc, 12), (BYTE *) inc_val);
    dbbind(ctx->dbproc, 13, INTBIND, (DBINT) 0, (BYTE *) &not4repl);
  
    int rowcode;
    struct sqlfs_ms_obj * obj = NULL;
    while (!terr && (rowcode = dbnextrow(ctx->dbproc)) != NO_MORE_ROWS) {
      switch(rowcode) {
      case REG_ROW:
	obj = g_try_new0(struct sqlfs_ms_obj, 1);

	obj->object_id = col_id_buf;
	obj->name = g_strdup(trimwhitespace(colname_buf));
	obj->type = R_COL;
	obj->parent_id = tid;
	
	obj->column = g_try_new0(struct sqlfs_ms_column, 1);     
	obj->column->column_id = col_id_buf;
	obj->column->systype = type_id_buf;
	obj->column->max_len = mlen;
	obj->column->identity = identity;
	obj->column->scale = scale;
	obj->column->precision = precision;
	obj->column->nullable = nullable;
	obj->column->ansi = ansi;
	obj->column->type_name = g_strdup(trimwhitespace(typename_buf));
	if (identity == TRUE) {
	  obj->column->seed_val = g_strdup(trimwhitespace(seed_val));
	  obj->column->inc_val = g_strdup(trimwhitespace(inc_val));
	  obj->column->not4repl = not4repl;
	}
	obj->column->def = make_column_def(obj);
	obj->len = strlen(obj->column->def);	
      
	reslist = g_list_append(reslist, obj);
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
    
    g_free(typename_buf);
    g_free(colname_buf);
    g_free(seed_val);
    g_free(inc_val);
  
  }
  close_sql(ctx);
  g_string_free(sql, TRUE);

  if (terr != NULL)
    g_propagate_error(error, terr);

  return reslist;
}

GList * fetch_modules(int tid, const char *name, GError **error)
{
  GError *terr = NULL;
  GList *list = NULL;
  GString *sql = g_string_new(NULL);
  g_string_truncate(sql, 0);
  g_string_append(sql, "SELECT so.name, so.object_id, so.type");
  g_string_append(sql, ", DATEDIFF(second, {d '1970-01-01'}, so.create_date)");
  g_string_append(sql, ", DATEDIFF(second, {d '1970-01-01'}, so.modify_date)");
  g_string_append(sql, ", DATALENGTH(sm.definition) ");
  g_string_append(sql, " FROM sys.objects so INNER JOIN sys.sql_modules sm");
  g_string_append(sql, "   ON so.object_id = sm.object_id");
  g_string_append_printf(sql, " WHERE so.parent_object_id = %d", tid);

  if (name)
    g_string_append_printf(sql, " AND so.name = '%s'", name);

  msctx_t *ctx = exec_sql(sql->str, &terr);
  if (!terr) {
    DBINT trg_id_buf;
    DBCHAR type_buf[2];
    DBINT def_len_buf;
    char *trgname_buf = g_malloc0_n(dbcollen(ctx->dbproc, 1) + 1, sizeof(char ));
    DBINT cdate_buf, mdate_buf;

    dbbind(ctx->dbproc, 1, STRINGBIND,
	   dbcollen(ctx->dbproc, 1), (BYTE *) trgname_buf);
    dbbind(ctx->dbproc, 2, INTBIND, (DBINT) 0, (BYTE *) &trg_id_buf);
    dbbind(ctx->dbproc, 3, STRINGBIND, (DBINT) 0, (BYTE *) type_buf);
    dbbind(ctx->dbproc, 4, INTBIND, (DBINT) 0, (BYTE *) &cdate_buf);
    dbbind(ctx->dbproc, 5, INTBIND, (DBINT) 0, (BYTE *) &mdate_buf);
    dbbind(ctx->dbproc, 6, INTBIND, (DBINT) 0, (BYTE *) &def_len_buf);
  
    int rowcode;
    struct sqlfs_ms_obj * trgobj = NULL;
  
    while (!terr && (rowcode = dbnextrow(ctx->dbproc)) != NO_MORE_ROWS) {
      switch(rowcode) {
      case REG_ROW:
	trgname_buf = trimwhitespace(trgname_buf);
	trgobj = g_try_new0(struct sqlfs_ms_obj, 1);
	trgobj->object_id = trg_id_buf;
	trgobj->name = g_strdup(trgname_buf);
	trgobj->type = str2mstype(trimwhitespace(type_buf));
	trgobj->parent_id = tid;
	trgobj->ctime = cdate_buf;
	trgobj->mtime = mdate_buf;
	trgobj->len = def_len_buf;
            
	trgobj->cached_time = g_get_monotonic_time();
	list = g_list_append(list, trgobj);
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
    
    g_free(trgname_buf);
    
  }
  close_sql(ctx);
  g_string_free(sql, TRUE);

  if (terr != NULL)
    g_propagate_error(error, terr);
  
  return list;
}

char * make_constraint_def(const struct sqlfs_ms_obj *ctrt, const char *def)
{
  if (!ctrt || !ctrt->clmn_ctrt || !def)
    return NULL;

  char *text = NULL;
  GString *sql = g_string_new(NULL);

  if (ctrt->type == R_D) {
    g_string_append_printf(sql, "DEFAULT (%s) FOR %s",
			   def, ctrt->clmn_ctrt->column_name);
  }
  else
    if (ctrt->type == R_C) {
      if (ctrt->clmn_ctrt->disabled == TRUE)
	g_string_append(sql, "WITH NOCHECK ");
      else
	g_string_append(sql, "WITH CHECK ");
      
      g_string_append_printf(sql, "CONSTRAINT %s CHECK ",
			     ctrt->name);
      
      if (ctrt->clmn_ctrt->not4repl == TRUE)
	g_string_append_printf(sql, "NOT FOR REPLICATION ", def);

      g_string_append_printf(sql, "(%s)", def);
    }

  text = g_strdup(sql->str);
  g_string_free(sql, TRUE);
  
  return text;
}

GList * fetch_constraints(int tid, const char *name, GError **error)
{
  GList *reslist = NULL;

  GError *terr = NULL;
  GString *sql = g_string_new(NULL);

  g_string_append(sql, "SELECT dc.object_id, dc.name, sc.name");
  g_string_append(sql, ", dc.definition, dc.type, 0, 0");
  g_string_append(sql, ", DATEDIFF(second, {d '1970-01-01'}, dc.create_date)");
  g_string_append(sql, ", DATEDIFF(second, {d '1970-01-01'}, dc.modify_date)");
  g_string_append(sql, " FROM sys.columns sc");
  g_string_append(sql, " INNER JOIN sys.default_constraints dc ");
  g_string_append(sql, "  ON dc.parent_column_id = sc.column_id ");
  g_string_append(sql, "    AND dc.parent_object_id = sc.object_id ");
  g_string_append_printf(sql, "WHERE dc.parent_object_id = %d", tid);

  if (name != NULL)
    g_string_append_printf(sql, " AND dc.name = '%s'", name);

  g_string_append(sql, " UNION ALL ");
  g_string_append(sql, "SELECT cc.object_id, cc.name, ''");
  g_string_append(sql, ", cc.definition, cc.type");
  g_string_append(sql, ", cc.is_disabled, cc.is_not_for_replication");
  g_string_append(sql, ", DATEDIFF(second, {d '1970-01-01'}, cc.create_date)");
  g_string_append(sql, ", DATEDIFF(second, {d '1970-01-01'}, cc.modify_date)");
  g_string_append(sql, " FROM sys.check_constraints cc ");
  g_string_append_printf(sql, "WHERE cc.parent_object_id = %d", tid);

  if (name != NULL)
    g_string_append_printf(sql, "AND cc.name = '%s'", name);

  msctx_t *ctx = exec_sql(sql->str, &terr);
  if (terr == NULL) {
    DBINT obj_id;
    DBCHAR type_buf[2];
    DBINT cdate_buf, mdate_buf, disabled, not4repl;
    char *csnt_name = g_malloc0_n(dbcollen(ctx->dbproc, 2) + 1, sizeof(char ));
    char *clmn_name = g_malloc0_n(dbcollen(ctx->dbproc, 3) + 1, sizeof(char ));
    char *def_text  = g_malloc0_n(dbcollen(ctx->dbproc, 4) + 1, sizeof(char ));

    dbbind(ctx->dbproc, 1, INTBIND, (DBINT) 0, (BYTE *) &obj_id);
    dbbind(ctx->dbproc, 2, STRINGBIND,
	   dbcollen(ctx->dbproc, 2), (BYTE *) csnt_name);
    dbbind(ctx->dbproc, 3, STRINGBIND,
	   dbcollen(ctx->dbproc, 3), (BYTE *) clmn_name);
    dbbind(ctx->dbproc, 4, STRINGBIND,
	   dbcollen(ctx->dbproc, 4), (BYTE *) def_text);
    dbbind(ctx->dbproc, 5, STRINGBIND, (DBINT) 0, (BYTE *) type_buf);
    dbbind(ctx->dbproc, 6, INTBIND, (DBINT) 0, (BYTE *) &disabled);
    dbbind(ctx->dbproc, 7, INTBIND, (DBINT) 0, (BYTE *) &not4repl);
    dbbind(ctx->dbproc, 8, INTBIND, (DBINT) 0, (BYTE *) &cdate_buf);
    dbbind(ctx->dbproc, 9, INTBIND, (DBINT) 0, (BYTE *) &mdate_buf);


    int rowcode;
    struct sqlfs_ms_obj *obj = NULL;
    
    while (!terr && (rowcode = dbnextrow(ctx->dbproc)) != NO_MORE_ROWS) {
      switch(rowcode) {
      case REG_ROW:
	obj = g_try_new0(struct sqlfs_ms_obj, 1);
	obj->object_id = obj_id;
	obj->parent_id = tid;
	obj->name = g_strdup(trimwhitespace(csnt_name));
	obj->mtime = mdate_buf;
	obj->ctime = cdate_buf;
	obj->type = str2mstype(trimwhitespace(type_buf));

	obj->clmn_ctrt =  g_try_new0(struct sqlfs_ms_constraint, 1);

	if (obj->type == R_D)
	  obj->clmn_ctrt->column_name = g_strdup(trimwhitespace(clmn_name));
	else {
	  obj->clmn_ctrt->disabled = disabled;
	  obj->clmn_ctrt->not4repl = not4repl;
	}
	
	obj->clmn_ctrt->def = make_constraint_def(obj, trimwhitespace(def_text));
	obj->len = strlen(obj->clmn_ctrt->def);

	obj->cached_time = g_get_monotonic_time();
	reslist = g_list_append(reslist, obj);
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

    g_free(csnt_name);
    g_free(clmn_name);
    g_free(def_text);
    
  }

  close_sql(ctx);
  g_string_free(sql, TRUE);

  if (terr != NULL)
    g_propagate_error(error, terr);
  
  return reslist;
}


char * make_index_def(const struct sqlfs_ms_obj *obj)
{
  if (!obj || !obj->index)
    return NULL;

  return NULL;
}

GList * fetch_indexes(int tid, const char *name, GError **error)
{
  GList *reslist = NULL;
  GError *terr = NULL;
  GString *sql = g_string_new(NULL);

  g_string_append(sql, "SELECT so.object_id, so.type");
  g_string_append(sql, ", DATEDIFF(second, {d '1970-01-01'}, so.create_date)");
  g_string_append(sql, ", DATEDIFF(second, {d '1970-01-01'}, so.modify_date)");
  g_string_append(sql, ", si.name, si.index_id, si.type, si.is_unique");
  g_string_append(sql, ", si.ignore_dup_key, si.is_primary_key");
  g_string_append(sql, ", si.is_unique_constraint, si.fill_factor");
  g_string_append(sql, ", si.is_padded, si.is_disabled, si.is_hypothetical");
  g_string_append(sql, ", si.allow_row_locks, si.allow_page_locks");
  g_string_append(sql, ", si.has_filter, si.filter_definition");
  
  g_string_append(sql, ",((SELECT sc.name ");
  g_string_append(sql, " +  CASE ic.is_descending_key WHEN 1 THEN ' DESC'");
  g_string_append(sql, "     ELSE ' ASC' END + ','");
  g_string_append(sql, "   FROM sys.index_columns ic INNER JOIN sys.columns sc");
  g_string_append(sql, "    ON sc.column_id = ic.column_id ");
  g_string_append(sql, "     AND sc.object_id = ic.object_id");
  g_string_append(sql, "   WHERE ic.index_id = si.index_id");
  g_string_append(sql, "     AND ic.object_id = si.object_id");
  g_string_append(sql, "     AND ic.is_included_column = 0");
  g_string_append(sql, "   FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)') )");

  g_string_append(sql, ",((SELECT sc.name ");
  g_string_append(sql, " +  CASE ic.is_descending_key WHEN 1 THEN ' DESC'");
  g_string_append(sql, "     ELSE ' ASC' END + ','");
  g_string_append(sql, "   FROM sys.index_columns ic INNER JOIN sys.columns sc");
  g_string_append(sql, "    ON sc.column_id = ic.column_id ");
  g_string_append(sql, "     AND sc.object_id = ic.object_id");
  g_string_append(sql, "   WHERE ic.index_id = si.index_id");
  g_string_append(sql, "     AND ic.object_id = si.object_id");
  g_string_append(sql, "     AND ic.is_included_column = 1");
  g_string_append(sql, "   FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)') )");
  
  g_string_append(sql, " FROM sys.objects so INNER JOIN sys.indexes si");
  g_string_append(sql, "   ON si.object_id = so.object_id");
  g_string_append_printf(sql, " WHERE so.object_id = %d", tid);

  if (name)
    g_string_append_printf(sql, " AND si.name = '%s'", name);

  msctx_t *ctx = exec_sql(sql->str, &terr);
  if (!terr) {
    DBINT obj_id, index_id, type_id, is_unique, ignr_dup_key, is_pk, is_uqc;
    DBCHAR type_buf[2];
    char *name_buf = g_malloc0_n(dbcollen(ctx->dbproc, 5) + 1, sizeof(char ));
    char *filter_def = g_malloc0_n(dbcollen(ctx->dbproc, 19) + 1, sizeof(char ));
    char *col_def = g_malloc0_n(dbcollen(ctx->dbproc, 20) + 1, sizeof(char ));
    char *incl_def = g_malloc0_n(dbcollen(ctx->dbproc, 21) + 1, sizeof(char ));
    DBINT cdate_buf, mdate_buf, fill_factor, is_padded, is_disabled, is_hyp;
    DBINT allow_rl, allow_pl, has_filter;

    dbbind(ctx->dbproc, 1, INTBIND, (DBINT) 0, (BYTE *) &obj_id);
    dbbind(ctx->dbproc, 2, STRINGBIND, (DBINT) 0, (BYTE *) type_buf);
    dbbind(ctx->dbproc, 3, INTBIND, (DBINT) 0, (BYTE *) &cdate_buf);
    dbbind(ctx->dbproc, 4, INTBIND, (DBINT) 0, (BYTE *) &mdate_buf);
    dbbind(ctx->dbproc, 5, STRINGBIND,
	   dbcollen(ctx->dbproc, 5), (BYTE *) name_buf);
    dbbind(ctx->dbproc, 6, INTBIND, (DBINT) 0, (BYTE *) &index_id);
    dbbind(ctx->dbproc, 7, INTBIND, (DBINT) 0, (BYTE *) &type_id);
    dbbind(ctx->dbproc, 8, INTBIND, (DBINT) 0, (BYTE *) &is_unique);
    dbbind(ctx->dbproc, 9, INTBIND, (DBINT) 0, (BYTE *) &ignr_dup_key);
    dbbind(ctx->dbproc, 10, INTBIND, (DBINT) 0, (BYTE *) &is_pk);
    dbbind(ctx->dbproc, 11, INTBIND, (DBINT) 0, (BYTE *) &is_uqc);
    dbbind(ctx->dbproc, 12, INTBIND, (DBINT) 0, (BYTE *) &fill_factor);
    dbbind(ctx->dbproc, 13, INTBIND, (DBINT) 0, (BYTE *) &is_padded);
    dbbind(ctx->dbproc, 14, INTBIND, (DBINT) 0, (BYTE *) &is_disabled);
    dbbind(ctx->dbproc, 15, INTBIND, (DBINT) 0, (BYTE *) &is_hyp);
    dbbind(ctx->dbproc, 16, INTBIND, (DBINT) 0, (BYTE *) &allow_rl);
    dbbind(ctx->dbproc, 17, INTBIND, (DBINT) 0, (BYTE *) &allow_pl);
    dbbind(ctx->dbproc, 18, INTBIND, (DBINT) 0, (BYTE *) &has_filter);
    dbbind(ctx->dbproc, 19, STRINGBIND,
	   dbcollen(ctx->dbproc, 19), (BYTE *) filter_def);
    dbbind(ctx->dbproc, 20, STRINGBIND,
	   dbcollen(ctx->dbproc, 20), (BYTE *) col_def);
    dbbind(ctx->dbproc, 21, STRINGBIND,
	   dbcollen(ctx->dbproc, 21), (BYTE *) incl_def);
  
    int rowcode;
    struct sqlfs_ms_obj *obj = NULL;
  
    while (!terr && (rowcode = dbnextrow(ctx->dbproc)) != NO_MORE_ROWS) {
      switch(rowcode) {
      case REG_ROW:
	name_buf = trimwhitespace(name_buf);
	obj = g_try_new0(struct sqlfs_ms_obj, 1);
	obj->object_id = index_id;
	obj->parent_id = obj_id;
	obj->name = g_strdup(name_buf);
	obj->mtime = mdate_buf;
	obj->ctime = cdate_buf;

	struct sqlfs_ms_index *idx = g_try_new0(struct sqlfs_ms_index, 1);
	idx->type_id = type_id;
	idx->is_unique = is_unique;
	idx->ignore_dup_key = ignr_dup_key;
	idx->is_pk = is_pk;
	idx->is_unique_const = is_uqc;
	idx->fill_factor = fill_factor;
	idx->is_padded = is_padded;
	idx->is_disabled = is_disabled;
	idx->is_hyp = is_hyp;
	idx->allow_rl = allow_rl;
	idx->allow_pl = allow_pl;
	idx->has_filter = has_filter;

	if (has_filter)
	  idx->filter_def = g_strdup(filter_def);

	if (col_def)
	  idx->columns_def = g_strdup(col_def);

	if (incl_def)
	  idx->incl_columns_def = g_strdup(incl_def);

	obj->index = idx;
	obj->cached_time = g_get_monotonic_time();

	reslist = g_list_append(reslist, obj);
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
    g_free(filter_def);
    g_free(col_def);
    g_free(incl_def);

  }
  
  close_sql(ctx);
  g_string_free(sql, TRUE);

  if (terr != NULL)
    g_propagate_error(error, terr);
  
  return reslist;
}
