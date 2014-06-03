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


typedef struct {
  struct sqlctx *ctx;
  
  GSList *ctxlist;
} exectx_t;

exectx_t *ectx;

static gboolean do_exec_sql(const char *sql, const msctx_t *ctx, GError **err)
{
  if ((dbcmd(ctx->dbproc, sql) == FAIL)) {
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

  return TRUE;
}

int init_context(const struct sqlctx *sqlctx,
		 gpointer err_handler, gpointer msg_handler)
{
  if (!sqlctx)
    return EENULL;
  
  if (!sqlctx->appname || !sqlctx->servername
      || !sqlctx->username || !sqlctx->dbname) {
    return EENULL;
  }
  
  RETCODE erc;
  if (dbinit() == FAIL) {
    g_printerr("%s:%d: dbinit() failed\n", sqlctx->appname, __LINE__);
    return EEINIT;
  }

  dberrhandle(err_handler);
  dbmsghandle(msg_handler);

  if (!ectx) {
    ectx = g_try_new0(exectx_t, 1);
  }
  
  int error = 0;

  if (ectx) {
    ectx->ctx = g_memdup(sqlctx, sizeof(*sqlctx));
  }
  else
    error = EEMEM;

  if (!error) {

    int i;
    msctx_t *msctx = NULL;
    for (i = 0; i < sqlctx->maxconn; i++) {
      msctx = g_try_new0(msctx_t, 1);
    
      if ((msctx->login = dblogin()) == NULL) {
	g_printerr("%s:%d: unable to allocate login structure\n",
		   sqlctx->appname, __LINE__);
	error = EELOGIN;
      }

      if (!error) {
	//DBSETLNATLANG(msctx->login, "english");
	DBSETLCHARSET(msctx->login, "CP1251");
	DBSETLUSER(msctx->login, sqlctx->username);
	DBSETLPWD(msctx->login, sqlctx->password);
	DBSETLAPP(msctx->login, sqlctx->appname);

	if ((msctx->dbproc = dbopen(msctx->login, sqlctx->servername)) == NULL) {
	  g_printerr("%s:%d: unable to connect to %s as %s\n",
		     sqlctx->appname, __LINE__,
		     sqlctx->servername, sqlctx->username);
	  error = EECONN;
	}

	if (!error && sqlctx->dbname
	    && (erc = dbuse(msctx->dbproc, sqlctx->dbname)) == FAIL) {
	  g_printerr("%s:%d: unable to use to database %s\n",
		     sqlctx->appname, __LINE__, sqlctx->dbname);
	  error = EEUSE;
	}
      }

      if (!error) {
	g_mutex_init(&msctx->lock);
	ectx->ctxlist = g_slist_append(ectx->ctxlist, msctx);
      }
      else {
	g_free(msctx);
      }
    }
    
  }

  return error;
}

msctx_t * exec_sql(const char *sql, GError **error)
{
  gboolean lock;
  int i, len = g_slist_length(ectx->ctxlist);
  GSList *list = ectx->ctxlist;
  msctx_t *wrkctx = NULL;
  GError *terr = NULL;
  RETCODE erc;
	
  for (i = 0; i < len; i++) {
    wrkctx = list->data;
    lock = g_mutex_trylock(&wrkctx->lock);

    if (lock && wrkctx) {
      
      if (dbdead(wrkctx->dbproc)) {
	g_message("isdead: reconnect...\n");
	dbclose(wrkctx->dbproc);
	
	if ((wrkctx->dbproc = dbopen(wrkctx->login,
				     ectx->ctx->servername)) == NULL) {
	  g_set_error(&terr, EECONN, EECONN,
		      "%s:%d: unable to connect to %s as %s\n",
		      ectx->ctx->appname, __LINE__,
		      ectx->ctx->servername, ectx->ctx->username);
	}
	
	if (!terr && ectx->ctx->dbname
	    && (erc = dbuse(wrkctx->dbproc, ectx->ctx->dbname)) == FAIL) {
	  g_set_error(&terr, EEUSE, EEUSE,
		      "%s:%d: unable to use to database %s\n",
		      ectx->ctx->appname, __LINE__,
		      ectx->ctx->dbname);
	}
	
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

int close_context()
{
  int error = 0;
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
	error = EEBUSY;
      }
      
    }
    list = g_slist_remove(list, wrkctx);
  }

  return error;
}
