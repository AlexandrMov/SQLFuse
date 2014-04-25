#include <string.h>

#include "mssqlfs.h"
#include "exec.h"
#include "tsqlcheck.h"
#include "tsql.tab.h"

struct cache {
  GHashTable *objtypes, *datatypes;
  
  int multithreads;  
};

static struct cache cache;

#define ADD_OBJTYPE(s,d)						\
  g_hash_table_insert(cache.objtypes, g_strdup(s), GINT_TO_POINTER(d));


static inline int initobjtypes()
{
  if (!cache.objtypes)
    cache.objtypes = g_hash_table_new(g_str_hash, g_str_equal);
  else
    return 0;
  
  ADD_OBJTYPE("$H", D_SCHEMA);
  ADD_OBJTYPE("$L", R_COL);
  ADD_OBJTYPE("$T", R_TYPE);
  
  ADD_OBJTYPE("IT", D_IT);
  ADD_OBJTYPE("S", D_S);
  ADD_OBJTYPE("TT", D_TT);
  ADD_OBJTYPE("U", D_U);
  ADD_OBJTYPE("V", D_V);
  
  ADD_OBJTYPE("AF", R_AF);
  ADD_OBJTYPE("C", R_C);
  ADD_OBJTYPE("D", R_D);
  ADD_OBJTYPE("F", R_F);
  ADD_OBJTYPE("FN", R_FN);
  ADD_OBJTYPE("FS", R_FS);
  ADD_OBJTYPE("FT", R_FT);
  ADD_OBJTYPE("IF", R_IF);
  ADD_OBJTYPE("P", R_P);
  ADD_OBJTYPE("PC", R_PC);
  ADD_OBJTYPE("PG", R_PG);
  ADD_OBJTYPE("PK", R_PK);
  ADD_OBJTYPE("RF", R_RF);
  ADD_OBJTYPE("SN", R_SN);
  ADD_OBJTYPE("SO", R_SO);
  ADD_OBJTYPE("SQ", R_SQ);
  ADD_OBJTYPE("TA", R_TA);
  ADD_OBJTYPE("TR", R_TR);
  ADD_OBJTYPE("TF", R_TF);
  ADD_OBJTYPE("UQ", R_UQ);
  ADD_OBJTYPE("X", R_X);

  return 0;
}

static int str2mstype(char * type)
{
  if(!type)
    return 0;
  
  gpointer r = g_hash_table_lookup(cache.objtypes, type);
  if(!r)
    return 0;
  
  return GPOINTER_TO_INT(r);
}

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

static char *trimwhitespace(char *str)
{
  char *end;
  
  while(isspace(*str)) str++;
  
  if(*str == 0)
    return str;
  
  end = str + strlen(str) - 1;
  while(end > str && isspace(*end)) end--;
  
  *(end+1) = 0;
  
  return str;
}

int init_msctx(struct sqlctx *ctx)
{
  int error = init_context(ctx, err_handler, msg_handler);
  initobjtypes();
  init_checker();
  
  return error;
}

static int load_datatypes()
{
  int error = 0;

  if (!cache.datatypes)
    cache.datatypes = g_hash_table_new_full(g_str_hash, g_str_equal,
					    g_free, free_ms_obj);
  
  g_hash_table_remove_all(cache.datatypes);
  
  RETCODE erc;
  GString * sql = g_string_new(NULL);
  g_string_append(sql, "SELECT name, system_type_id, user_type_id");
  g_string_append(sql, ", max_length, precision, scale, collation_name");
  g_string_append(sql, ", is_nullable, is_table_type");
  g_string_append(sql, " FROM sys.types");

  msctx_t *ctx = NULL;
  error = exec_sql(sql->str, &ctx);

  if (!error) {
    DBINT systype_id, usrtype_id, maxlen;
    DBINT prec, scale, nullable, tbltype;
    char *name_buf = g_malloc0_n(dbcollen(ctx->dbproc, 1) + 1,
				 sizeof(char *));
    char *cltname = g_malloc0_n(dbcollen(ctx->dbproc, 7) + 1,
				sizeof(char *));
    dbbind(ctx->dbproc, 1, STRINGBIND,
	   dbcollen(ctx->dbproc, 2), (BYTE *) name_buf);
    dbbind(ctx->dbproc, 2, INTBIND, (DBINT) 0, (BYTE *) &systype_id);
    dbbind(ctx->dbproc, 3, INTBIND, (DBINT) 0, (BYTE *) &usrtype_id);
    dbbind(ctx->dbproc, 4, INTBIND, (DBINT) 0, (BYTE *) &maxlen);
    dbbind(ctx->dbproc, 5, INTBIND, (DBINT) 0, (BYTE *) &prec);
    dbbind(ctx->dbproc, 6, INTBIND, (DBINT) 0, (BYTE *) &scale);
    dbbind(ctx->dbproc, 7, STRINGBIND, (DBINT) 0, (BYTE *) cltname);
    dbbind(ctx->dbproc, 8, INTBIND, (DBINT) 0, (BYTE *) &nullable);
    dbbind(ctx->dbproc, 9, INTBIND, (DBINT) 0, (BYTE *) &tbltype);

    struct sqlfs_ms_obj *obj = NULL;
    int rowcode;
    while (!error && (rowcode = dbnextrow(ctx->dbproc)) != NO_MORE_ROWS) {
      switch(rowcode) {
      case REG_ROW:
	obj = g_try_new0(struct sqlfs_ms_obj, 1);
	obj->name = g_strdup(trimwhitespace(name_buf));
	obj->type = R_TYPE;
	
	obj->mstype = g_try_new0(struct sqlfs_ms_type, 1);
	obj->mstype->sys_type_id = systype_id;
	obj->mstype->user_type_id = usrtype_id;
	obj->mstype->max_length = maxlen;
	obj->mstype->precision = prec;
	obj->mstype->scale = scale;
	obj->mstype->collation_name = g_strdup(trimwhitespace(cltname));
	obj->mstype->nullable = nullable;
	obj->mstype->table_type = tbltype;
	
	obj->len = 0;

	g_hash_table_insert(cache.datatypes, g_strdup(obj->name), obj);
	break;
      case BUF_FULL:
	break;
      case FAIL:
	g_printerr("%d: dbresults failed\n", __LINE__);
	error = EERES;
	break;
      }
    }
    g_free(name_buf);
    g_free(cltname);
  }

  close_sql(ctx);
  g_string_free(sql, TRUE);
  
  return error;
}

int find_ms_object(const struct sqlfs_ms_obj *parent,
		   const char *name, struct sqlfs_ms_obj *obj)
{
  int error = 0;

  if (!name || !obj)
    return EENULL;

  RETCODE erc;
  GString * sql = g_string_new(NULL);
  if (!parent) {
    g_string_append(sql, "SELECT sh.schema_id, sh.name, '$H' as type");
    g_string_append(sql, ", NULL as create_date");
    g_string_append(sql, ", NULL as modify_date");
    g_string_append(sql, ", 0 as len");
    g_string_append(sql, " FROM sys.schemas sh");
    g_string_append_printf(sql, " WHERE name = '%s'", name);
  }
  else {
    g_string_append(sql, "SELECT so.object_id, so.name, so.type");
    g_string_append(sql, ", DATEDIFF(second, {d '1970-01-01'}, so.create_date)");
    g_string_append(sql, ", DATEDIFF(second, {d '1970-01-01'}, so.modify_date)");
    g_string_append(sql, ", LEN(ISNULL(sm.definition, 0)) ");
    g_string_append(sql, " FROM sys.objects so");
    g_string_append(sql, " LEFT JOIN sys.sql_modules sm");
    g_string_append(sql, "   ON sm.object_id = so.object_id");
    g_string_append_printf(sql, " WHERE name = '%s'", name);
    if (parent->type == D_SCHEMA) {
      g_string_append(sql, " AND so.parent_object_id = 0");
      g_string_append_printf(sql, " AND so.schema_id = %d", parent->schema_id);
    } else {
      g_string_append_printf(sql, " AND so.parent_object_id = %d",
			     parent->object_id);
    }
    
    if (parent->type == D_S || parent->type == D_U
	|| parent->type == D_V) {
      g_string_append(sql, " UNION ALL ");
      g_string_append(sql, "SELECT sc.colid, sc.name, '$L'");
      g_string_append(sql, ", NULL as create_date");
      g_string_append(sql, ", NULL as modify_date");
      g_string_append(sql, ", 0");
      g_string_append(sql, " FROM syscolumns sc");
      g_string_append_printf(sql, " WHERE sc.id = %d", parent->object_id);
      g_string_append_printf(sql, " AND name = '%s'", name);
    }
  }

  msctx_t *ctx = NULL;
  error = exec_sql(sql->str, &ctx);

  if (!error) {
    DBINT obj_id_buf;
    DBINT def_len_buf;
    char *name_buf = g_malloc0_n(dbcollen(ctx->dbproc, 2) + 1,
				 sizeof(char *));
    DBCHAR type_buf[2];
    DBINT cdate_buf, mdate_buf;
    
    dbbind(ctx->dbproc, 1, INTBIND, (DBINT) 0, (BYTE *) &obj_id_buf);
    dbbind(ctx->dbproc, 2, STRINGBIND,
	   dbcollen(ctx->dbproc, 2), (BYTE *) name_buf);
    dbbind(ctx->dbproc, 3, STRINGBIND, (DBINT) 0, (BYTE *) type_buf);
    dbbind(ctx->dbproc, 4, INTBIND, (DBINT) 0, (BYTE *) &cdate_buf);
    dbbind(ctx->dbproc, 5, INTBIND, (DBINT) 0, (BYTE *) &mdate_buf);
    dbbind(ctx->dbproc, 6, INTBIND, (DBINT) 0, (BYTE *) &def_len_buf);
    
    int rowcode = dbnextrow(ctx->dbproc);
    if (rowcode != NO_MORE_ROWS) {
      switch(rowcode) {
      case REG_ROW:
	obj->name = g_strdup(trimwhitespace(name_buf));
	obj->type = str2mstype(trimwhitespace(type_buf));
	if (obj->type == D_SCHEMA)
	  obj->schema_id = obj_id_buf;
	obj->object_id = obj_id_buf;
	obj->ctime = cdate_buf;
	obj->mtime = mdate_buf;

	if(obj->type == R_P || obj->type == D_V ||
	   obj->type == R_FN || obj->type == R_TF ||
	   obj->type == R_TR || obj->type == R_COL) {
	  obj->len = def_len_buf;
	}

	obj->cached_time = g_get_monotonic_time();
	break;
      case BUF_FULL:
	break;
      case FAIL:
	g_printerr("%d: dbresults failed\n",  __LINE__);
	error = EERES;
	break;
      }
    }
    
    g_free(name_buf);
  }

  close_sql(ctx);
  g_string_free(sql, TRUE);
  
  return error;
}

int write_ms_object(const char *schema, const struct sqlfs_ms_obj *parent,
		    const char *text, struct sqlfs_ms_obj *obj)
{
  if (!obj || !obj->name || !text)
    return EENULL;

  int error = 0;
  start_checker();

  yy_scan_string(text);
  error = yyparse();
  objnode_t *node = NULL;
  if (!error && ((node = get_node()) != NULL)) {

    char *wrktext = NULL;
    GString *sql = g_string_new(NULL);
    if (node->type == COLUMN) {
      if (!parent || !parent->name) {
	error = EENULL;
      }
      else {
	wrktext = g_strdup(text + node->first_column - 1);
	g_string_append_printf(sql, "ALTER TABLE %s.%s", schema, parent->name);
	if (obj->object_id) {
	  g_string_append(sql, " ALTER COLUMN ");
	}
	else {
	  g_string_append(sql, " ADD ");
	}
	g_string_append_printf(sql, "%s ", obj->name);
      }
    }
    else {
      wrktext = g_strdup(text);
    }
      

    if (!error) {
      g_string_append_printf(sql, "%s", wrktext);
    }
    
    msctx_t *ctx = NULL;
    error = exec_sql(sql->str, &ctx);
    if (!error) {
      
      switch(node->type) {
      case COLUMN:
	obj->type = R_COL;
	if (!obj->column)
	  obj->column = g_try_new0(struct sqlfs_ms_column, 1);
	
	obj->column->def = g_strdup(text);
	obj->len = strlen(text);
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
	obj->type = R_P;
      }
    }
    
    close_sql(ctx);
    g_free(wrktext);
    g_string_free(sql, TRUE);
    
  }
  
  end_checker();

  return error;
}
  
int remove_ms_object(const char *schema, const char *parent,
		     struct sqlfs_ms_obj *obj)
{
  if (!obj || !schema)
    return EENULL;
  
  int error = 0;
  GString *sql = g_string_new(NULL);
  if (obj->type == R_COL) {
    g_string_append_printf(sql, "ALTER TABLE %s.%s DROP COLUMN %s",
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
    default:
      error = EENOTSUP;
    }
    
    g_string_append_printf(sql, " %s.%s", schema, obj->name);
  }

  if (!error) {
    msctx_t *ctx = NULL;
    error = exec_sql(sql->str, &ctx);
    close_sql(ctx);
  }

  g_string_free(sql, TRUE);

  return error;
}

int load_module_text(const char *parent, struct sqlfs_ms_obj *obj, char **text)
{
  if (!obj || !parent)
    return EENULL;

  int error = 0;
  char *def = NULL;
  if (obj->type == R_COL && obj->column) {
    def = g_strdup(obj->column->def);
  }
  else {
    RETCODE erc;
    GString *sql = g_string_new(NULL);
    
    g_string_append_printf(sql, "EXEC sp_helptext '%s.%s'", parent, obj->name);
    msctx_t *ctx = NULL;
    error = exec_sql(sql->str, &ctx);

    if (!error) {
      int rowcode;
      g_string_truncate(sql, 0);
    
      struct sqlfs_ms_module * module = NULL;
      DBCHAR def_buf[256];
      dbbind(ctx->dbproc, 1, NTBSTRINGBIND, (DBINT) 0, (BYTE *) def_buf);
    
      while (!error && (rowcode = dbnextrow(ctx->dbproc)) != NO_MORE_ROWS) {
	switch(rowcode) {
	case REG_ROW:
	  g_string_append(sql, def_buf);
	  break;
	case BUF_FULL:
	  break;
	case FAIL:
	  g_printerr("%d: dbresults failed\n", __LINE__);
	  error = EERES;
	  break;
	}
      }
      
      if (!error) {
	module = g_try_new0(struct sqlfs_ms_module, 1);
	module->def = g_strdup(sql->str);
	obj->sql_module = module;
	def = g_strdup(sql->str);
      }
      
    }
    
    close_sql(ctx);
    g_string_free(sql, TRUE);
  }

  *text = def;
  
  return error;
}

static void make_column_def(const struct sqlfs_ms_obj *obj, char **text)
{
  if (!obj || !obj->column)
    return ;

  struct sqlfs_ms_column *col = obj->column;
  GString *def = g_string_new(NULL);
  g_string_append_printf(def, "COLUMN %s %s", obj->name, col->type_name);
  
  if (!g_strcmp0(col->type_name, "float"))
    g_string_append_printf(def, "(%d)", col->precision);

  if (!g_strcmp0(col->type_name, "numeric")
      || !g_strcmp0(col->type_name, "decimal"))
    g_string_append_printf(def, "(%d,%d)", col->precision, col->scale);

  if (g_str_has_suffix(col->type_name, "char")
      || g_str_has_suffix(col->type_name, "binary")) {
    if (col->max_len < 0)
      g_string_append(def, "(max)");
    else
      g_string_append_printf(def, "(%d)", col->max_len);
  }

  if (!col->nullable)
    g_string_append(def, " NOT NULL");
  
  *text = g_strdup(def->str);

  g_string_free(def, TRUE);
}

int load_table_obj(const struct sqlfs_ms_obj *tbl, GList **obj_list)
{
  if (!tbl)
    return EENULL;

  int error = 0;
  RETCODE erc;
  GString *sql = g_string_new(NULL);
  g_string_append(sql, "SELECT sc.column_id, sc.name");
  g_string_append(sql, ", sc.system_type_id, sc.max_length");
  g_string_append(sql, ", sc.precision, sc.scale, sc.is_nullable");
  g_string_append(sql, ", sc.is_ansi_padded, sc.is_identity, st.name");
  g_string_append(sql, " FROM sys.columns sc INNER JOIN sys.types st");
  g_string_append(sql, " ON st.user_type_id = sc.user_type_id");
  g_string_append_printf(sql, " WHERE sc.object_id = %d", tbl->object_id);

  msctx_t *ctx = NULL;
  error = exec_sql(sql->str, &ctx);

  DBINT col_id_buf, type_id_buf, mlen, precision, scale, nullable,
    ansi, identity;
  char *colname_buf = g_malloc0_n(dbcollen(ctx->dbproc, 2) + 1, sizeof(char *));
  char *typename_buf = g_malloc0_n(dbcollen(ctx->dbproc, 10) + 1, sizeof(char *));

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
  
  int rowcode;
  char *deftext = NULL;
  struct sqlfs_ms_obj * obj = NULL;
  while (!error && (rowcode = dbnextrow(ctx->dbproc)) != NO_MORE_ROWS) {
    switch(rowcode) {
    case REG_ROW:
      obj = g_try_new0(struct sqlfs_ms_obj, 1);

      obj->object_id = col_id_buf;
      obj->name = g_strdup(trimwhitespace(colname_buf));
      obj->type = R_COL;

      obj->parent_id = tbl->object_id;
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

      make_column_def(obj, &deftext);
      obj->column->def = g_strdup(deftext);
      obj->len = strlen(deftext);
      *obj_list = g_list_append(*obj_list, obj);
      
      g_free(deftext);
      break;
    case BUF_FULL:
      break;
    case FAIL:
      g_printerr("%d: dbresults failed\n", __LINE__);
      error = EERES;
      break;
    }
  }

  close_sql(ctx);
  g_free(typename_buf);
  g_free(colname_buf);

  if (error) {
    g_string_free(sql, TRUE);
    return error;
  }

  g_string_truncate(sql, 0);
  g_string_append(sql, "SELECT so.name, so.object_id, so.type");
  g_string_append(sql, ", DATEDIFF(second, {d '1970-01-01'}, so.create_date)");
  g_string_append(sql, ", DATEDIFF(second, {d '1970-01-01'}, so.modify_date)");
  g_string_append(sql, ", LEN(ISNULL(sm.definition, 0)) ");
  g_string_append(sql, ", LEN(ISNULL(dc.definition, 0)) ");
  g_string_append(sql, ", LEN(ISNULL(cc.definition, 0)) ");
  g_string_append(sql, " FROM sys.objects so");
  g_string_append(sql, " LEFT JOIN sys.sql_modules sm");
  g_string_append(sql, "  ON sm.object_id = so.object_id");
  g_string_append(sql, " LEFT JOIN sys.default_constraints dc");
  g_string_append(sql, "  ON dc.object_id = so.object_id");
  g_string_append(sql, " LEFT JOIN sys.check_constraints cc");
  g_string_append(sql, "  ON cc.object_id = so.object_id");
  g_string_append_printf(sql, " WHERE so.parent_object_id = %d", tbl->object_id);

  ctx = NULL;
  error = exec_sql(sql->str, &ctx);

  DBINT trg_id_buf;
  DBCHAR type_buf[2];
  DBINT def_len_buf, def_cc_buf, def_dc_buf;
  char * trgname_buf = g_malloc0_n(dbcollen(ctx->dbproc, 1) + 1, sizeof(char *));
  DBINT cdate_buf, mdate_buf;

  dbbind(ctx->dbproc, 1, STRINGBIND,
	 dbcollen(ctx->dbproc, 1), (BYTE *) trgname_buf);
  dbbind(ctx->dbproc, 2, INTBIND, (DBINT) 0, (BYTE *) &trg_id_buf);
  dbbind(ctx->dbproc, 3, STRINGBIND, (DBINT) 0, (BYTE *) type_buf);
  dbbind(ctx->dbproc, 4, INTBIND, (DBINT) 0, (BYTE *) &cdate_buf);
  dbbind(ctx->dbproc, 5, INTBIND, (DBINT) 0, (BYTE *) &mdate_buf);
  dbbind(ctx->dbproc, 6, INTBIND, (DBINT) 0, (BYTE *) &def_len_buf);
  dbbind(ctx->dbproc, 7, INTBIND, (DBINT) 0, (BYTE *) &def_dc_buf);
  dbbind(ctx->dbproc, 8, INTBIND, (DBINT) 0, (BYTE *) &def_cc_buf);
  struct sqlfs_ms_obj * trgobj = NULL;
  while (!error && (rowcode = dbnextrow(ctx->dbproc)) != NO_MORE_ROWS) {
    switch(rowcode) {
    case REG_ROW:
      trgname_buf = trimwhitespace(trgname_buf);
      trgobj = g_try_new0(struct sqlfs_ms_obj, 1);
      trgobj->object_id = trg_id_buf;
      trgobj->name = g_strdup(trgname_buf);
      trgobj->type = str2mstype(trimwhitespace(type_buf));
      trgobj->parent_id = tbl->object_id;
      trgobj->ctime = cdate_buf;
      trgobj->mtime = mdate_buf;
      if(trgobj->type == R_P || trgobj->type == D_V ||
	 trgobj->type == R_FN || trgobj->type == R_TF ||
	 trgobj->type == R_TR) {
	trgobj->len = def_len_buf;
      } else if (trgobj->type == R_C) {
	trgobj->len = def_cc_buf;
      } else if (trgobj->type == R_D) {
	trgobj->len = def_dc_buf;
      }
            
      trgobj->cached_time = g_get_monotonic_time();
      *obj_list = g_list_append(*obj_list, trgobj);
      break;
    case BUF_FULL:
      break;
    case FAIL:
      g_printerr("%d: dbresults failed\n", __LINE__);
      error = EERES;
      break;
    }
  }

  close_sql(ctx);
  g_free(trgname_buf);
  g_string_free(sql, TRUE);
  
  return error;
}

int load_schema_obj(const struct sqlfs_ms_obj *sch, GList **obj_list)
{
  if (!sch)
    return EENULL;

  int error = 0;
  RETCODE erc;
  GString * sql = g_string_new(NULL);
  g_string_append(sql, "SELECT so.object_id, so.name, so.type");
  g_string_append(sql, ", DATEDIFF(second, {d '1970-01-01'}, so.create_date)");
  g_string_append(sql, ", DATEDIFF(second, {d '1970-01-01'}, so.modify_date) ");
  g_string_append(sql, ", LEN(sm.definition) ");
  g_string_append(sql, "FROM sys.objects so LEFT JOIN sys.sql_modules sm");
  g_string_append(sql, " ON sm.object_id = so.object_id");
  g_string_append_printf(sql, " WHERE schema_id = %d", sch->schema_id);
  g_string_append(sql, " AND parent_object_id = 0");

  msctx_t *ctx = NULL;
  error = exec_sql(sql->str, &ctx);

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
  GList *lst = NULL;
  while (!error && (rowcode = dbnextrow(ctx->dbproc)) != NO_MORE_ROWS) {
    switch(rowcode) {
    case REG_ROW:
      name_buf = trimwhitespace(name_buf);
      obj = g_try_new0(struct sqlfs_ms_obj, 1);
      obj->name = g_strdup(name_buf);
      obj->type = str2mstype(trimwhitespace(type_buf));
      obj->schema_id = sch->schema_id;
      obj->object_id = obj_id_buf;
      obj->ctime = cdate_buf;
      obj->mtime = mdate_buf;
      obj->cached_time = g_get_monotonic_time();

      if (obj->type == R_P || obj->type == D_V ||
	 obj->type == R_FN || obj->type == R_TF ||
	 obj->type == R_COL) {
	obj->len = def_len_buf;
      }
      lst = g_list_append(lst, obj);
      break;
    case BUF_FULL:
      break;
    case FAIL:
      g_printerr("%d: dbresults failed\n", __LINE__);
      error = EERES;
      break;
    }
  }

  *obj_list = lst;
  
  close_sql(ctx);
  g_string_free(sql, TRUE);
  g_free(name_buf);

  return error;
}

int load_schemas(GList **obj_list)
{
  int error = 0;
  RETCODE erc;
  gchar * sql = "SELECT schema_id, name FROM sys.schemas";

  msctx_t *ctx = NULL;
  error = exec_sql(sql, &ctx);

  int rowcode;
  DBINT schid_buf;
  char * schname_buf = g_malloc0_n(dbcollen(ctx->dbproc, 2) + 1, sizeof(char ));
  dbbind(ctx->dbproc, 1, INTBIND, (DBINT) 0, (BYTE *) &schid_buf);
  dbbind(ctx->dbproc, 2, STRINGBIND, dbcollen(ctx->dbproc, 2) + 1,
	 (BYTE *) schname_buf);

  GList *lst = NULL;
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
      g_printerr("%d: dbresults failed\n", __LINE__);
      error = EERES;
      break;
    }
  }

  *obj_list = lst;
  
  close_sql(ctx);
  g_free(schname_buf);
  
  return error;
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
    if (obj->sql_module)
      g_free(obj->sql_module->def);
    
    g_free(obj->sql_module);
    break;
  case R_COL:
    if (obj->column) {
      g_free(obj->column->type_name);
      g_free(obj->column->def);
    }
    
    g_free(obj->column);
    break;
  case R_TYPE:
    if (obj->mstype)
      g_free(obj->mstype->collation_name);
    
    g_free(obj->mstype);
    break;
  }

  g_free(obj->name);
  g_free(obj);
}

int close_msctx()
{
  int error = 0;

  close_checker();
  error = close_context();
  dbexit();
  
  return error;
}
