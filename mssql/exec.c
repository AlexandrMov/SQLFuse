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

#include "exec.h"
#include <conf/keyconf.h>
#include <string.h>


typedef struct {
  GSList *ctxlist;
  gchar *to_codeset, *from_codeset;
} exectx_t;

exectx_t *ectx;

static inline char * get_npw_sql()
{
  char *result;
  GString *sql = g_string_new(NULL);
  
  g_string_append(sql, "SET ANSI_NULLS ON;\n");
  g_string_append(sql, "SET ANSI_PADDING ON;\n");
  g_string_append(sql, "SET ANSI_WARNINGS ON;\n");
  g_string_append(sql, "SET QUOTED_IDENTIFIER ON;\n");
  g_string_append(sql, "SET CONCAT_NULL_YIELDS_NULL ON;\n");

  result = g_strdup(sql->str);

  g_string_free(sql, TRUE);

  return result;
}

static gboolean do_exec_sql(const char *sql, const msctx_t *ctx, GError **err)
{
  gchar *sqlconv = NULL;
  if (ectx->to_codeset != NULL && ectx->from_codeset != NULL) {
    sqlconv = g_convert(sql, strlen(sql),
			ectx->to_codeset, ectx->from_codeset,
			NULL, NULL, err);
  }
  else {
    sqlconv = g_strdup(sql);
  }
  
  if ((dbcmd(ctx->dbproc, sqlconv) == FAIL)) {
    g_set_error(err, EECMD, EECMD,
		"%d: dbcmd() failed\n", __LINE__);
    return FALSE;
  }
  
  if ((dbsqlexec(ctx->dbproc) == FAIL)) {
    g_set_error(err, EEXEC, EEXEC,
		"%d: dbsqlexec() failed\n", __LINE__);
    return FALSE;
  }
  
  if ((dbresults(ctx->dbproc) == FAIL)) {
    g_set_error(err, EERES, EERES,
		"%d: dbresults() failed\n", __LINE__);
    return FALSE;
  }

  g_free(sqlconv);

  return TRUE;
}

void init_context(gpointer err_handler, gpointer msg_handler, GError **error)
{
  GError *terr = NULL;  
  sqlctx_t *sqlctx = fetch_context(TRUE, &terr);
  
  if (!sqlctx->appname || !sqlctx->servername
      || !sqlctx->username || !sqlctx->dbname) {
    return ;
  }
  
  RETCODE erc;
  if (dbinit() == FAIL) {
    g_set_error(&terr, EEINIT, EEINIT, "%s:%d: dbinit() failed\n",
		sqlctx->appname, __LINE__);
    return ;
  }

  dberrhandle(err_handler);
  dbmsghandle(msg_handler);

  if (!ectx) {
    ectx = g_try_new0(exectx_t, 1);
  }
  
  if (terr == NULL) {
    if (sqlctx->to_codeset != NULL && sqlctx->from_codeset != NULL) {
      ectx->to_codeset = g_strdup(sqlctx->to_codeset);
      ectx->from_codeset = g_strdup(sqlctx->from_codeset);
    }
    
    int i;
    char *npw_sql = get_npw_sql();
    for (i = 0; i < sqlctx->maxconn; i++) {
      msctx_t *msctx = g_try_new0(msctx_t, 1);
    
      if ((msctx->login = dblogin()) == NULL) {
	g_set_error(&terr, EELOGIN, EELOGIN,
		    "%s:%d: unable to allocate login structure\n",
		    sqlctx->appname, __LINE__);
      }

      if (terr == NULL) {
	if (ectx->to_codeset != NULL)
	  DBSETLCHARSET(msctx->login, ectx->to_codeset);
	
	DBSETLUSER(msctx->login, sqlctx->username);
	DBSETLPWD(msctx->login, sqlctx->password);
	DBSETLAPP(msctx->login, sqlctx->appname);

	if ((msctx->dbproc = dbopen(msctx->login, sqlctx->servername)) == NULL) {
	  g_set_error(&terr, EECONN, EECONN, "%s:%d: unable to connect to %s as %s\n",
		      sqlctx->appname, __LINE__,
		      sqlctx->servername, sqlctx->username);
	}

	if (terr == NULL && sqlctx->dbname
	    && (erc = dbuse(msctx->dbproc, sqlctx->dbname)) == FAIL) {
	  g_set_error(&terr, EEUSE, EEUSE, "%s:%d: unable to use to database %s\n",
		      sqlctx->appname, __LINE__, sqlctx->dbname);
	}
      }

      if (terr == NULL && sqlctx->ansi_npw == TRUE)
	do_exec_sql(npw_sql, msctx, &terr);

      if (terr == NULL) {
	g_mutex_init(&msctx->lock);
	ectx->ctxlist = g_slist_append(ectx->ctxlist, msctx);
      }
      else {
	g_free(msctx);
      }
    }
    
    g_free(npw_sql);
    
  }
  
  if (terr != NULL)
    g_propagate_error(error, terr);
}

msctx_t * exec_sql(const char *sql, GError **error)
{
  gboolean lock;
  int i, len = g_slist_length(ectx->ctxlist);
  GSList *list = ectx->ctxlist;
  msctx_t *wrkctx = NULL;
  GError *terr = NULL;
  RETCODE erc;
  char *npw_sql = get_npw_sql();
	
  for (i = 0; i < len; i++) {
    wrkctx = list->data;
    lock = g_mutex_trylock(&wrkctx->lock);

    if (lock && wrkctx) {
      
      if (dbdead(wrkctx->dbproc)) {
	g_message("isdead: reconnect...\n");
	dbclose(wrkctx->dbproc);
	
	sqlctx_t *sqlctx = fetch_context(TRUE, &terr);
	
	if ((wrkctx->dbproc = dbopen(wrkctx->login, sqlctx->servername)) == NULL) {
	  g_set_error(&terr, EECONN, EECONN,
		      "%s:%d: unable to connect to %s as %s\n",
		      sqlctx->appname, __LINE__,
		      sqlctx->servername, sqlctx->username);
	}
	
	if (!terr && sqlctx->dbname
	    && (erc = dbuse(wrkctx->dbproc, sqlctx->dbname)) == FAIL) {
	  g_set_error(&terr, EEUSE, EEUSE,
		      "%s:%d: unable to use to database %s\n",
		      sqlctx->appname, __LINE__,
		      sqlctx->dbname);
	}

	if (sqlctx->ansi_npw == TRUE && terr == NULL
	    && do_exec_sql(npw_sql, wrkctx, &terr)) {
	  g_set_error(&terr, EEXEC, EEXEC,
		      "%s:%d: unable sets init params NPW\n",
		      sqlctx->appname, __LINE__);
	}

	clear_context();
	
      }
      
      if (terr == NULL && do_exec_sql(sql, wrkctx, &terr) == FALSE
	  && !dbdead(wrkctx->dbproc)) {
	g_clear_error(&terr);
	g_set_error(&terr, EECONN, EECONN, "SQL Error: %s\n", sql);
      }

      if (terr != NULL) {	
	if (!dbdead(wrkctx->dbproc)) {
	  g_mutex_unlock(&wrkctx->lock);
	  break;
	}
	
	g_clear_error(&terr);
	g_mutex_unlock(&wrkctx->lock);
      }
      else
	break;  
    }

    list = g_slist_next(list);
  }

  if (!wrkctx || !lock) {
    list = ectx->ctxlist;
    
    if (list && list->data) {
      wrkctx = list->data;
      g_mutex_lock(&wrkctx->lock);
      do_exec_sql(sql, wrkctx, &terr);
    }

  }

  g_free(npw_sql);

  if (terr != NULL)
    g_propagate_error(error, terr);

  return wrkctx;
}

void close_sql(msctx_t *context)
{
  if (!context)
    return ;

  g_mutex_unlock(&context->lock);
}

void close_context(GError **error)
{
  GError *terr = NULL;
  int i, len = g_slist_length(ectx->ctxlist);
  msctx_t *wrkctx = NULL;
  GSList *list = ectx->ctxlist;
  gboolean lock;
  
  for (i = 0; i < len; i++) {
    if ((wrkctx = list->data) != NULL) {
      
      lock = g_mutex_trylock(&wrkctx->lock);
      if (lock) {
	dbclose(wrkctx->dbproc);
      }
      else {
	g_set_error(&terr, EEBUSY, EEBUSY, NULL);
      }
      
    }
    list = g_slist_remove(list, wrkctx);
  }

  if (ectx->to_codeset != NULL)
    g_free(ectx->to_codeset);

  if (ectx->from_codeset != NULL)
    g_free(ectx->from_codeset);

  if (terr != NULL)
    g_propagate_error(error, terr);
}
