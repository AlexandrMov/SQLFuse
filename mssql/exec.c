#include "exec.h"

typedef struct {
  struct sqlctx *ctx;
  
  GSList *ctxlist;
} exectx_t;

exectx_t *ectx;

static int do_exec_sql(const char *sql, const msctx_t *ctx)
{
  if (!sql || !ctx)
    return EENULL;

  int error = 0;
  
  if (!error && (dbcmd(ctx->dbproc, sql) == FAIL)) {
    g_printerr("%d: dbcmd() failed\n", __LINE__);
    error = EECMD;
  }
  
  if (!error && (dbsqlexec(ctx->dbproc) == FAIL)) {
    g_printerr("%d: dbsqlexec() failed\n", __LINE__);
    error = EEXEC;
  }
  
  if (!error && (dbresults(ctx->dbproc) == FAIL)) {
    g_printerr("%d: dbresults() failed\n", __LINE__);
    error = EERES;
  }

  return error;
}

int init_context(const struct sqlctx *sqlctx, gpointer err_handler,
		 gpointer msg_handler)
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

  if (!ectx) {
    ectx = g_try_new0(exectx_t, 1);
    ectx->ctx = g_try_new0(struct sqlctx, 1);
  }
  
  int error = 0;

  if (ectx && ectx->ctx) {
    ectx->ctx = g_memdup(sqlctx, sizeof(*sqlctx));
  }
  else
    error = EEMEM;

  int i;
  msctx_t *msctx = NULL;
  for (i = 0; i < sqlctx->maxconn; i++) {
    msctx = g_try_new0(msctx_t, 1);
    
    if ((msctx->login = dblogin()) == NULL) {
      g_printerr("%s:%d: unable to allocate login structure\n",
		 ctx->appname, __LINE__);
      error = EELOGIN;
    }

    if (!error) {
      DBSETLNATLANG(msctx->login, "russian");
      DBSETLUSER(msctx->login, sqlctx->username);
      DBSETLPWD(msctx->login, sqlctx->password);
      DBSETLAPP(msctx->login, sqlctx->appname);

      if ((msctx->dbproc = dbopen(msctx->login, msctx->servername)) == NULL) {
	g_printerr("%s:%d: unable to connect to %s as %s\n",
		   msctx->appname, __LINE__,
		   msctx->servername, msctx->username);
	error = EECONN;
      }

      if (!error && msctx->dbname
	  && (erc = dbuse(msctx->dbproc, sqlctx->dbname)) == FAIL) {
	g_printerr("%s:%d: unable to use to database %s\n",
		   sqlctx->appname, __LINE__, sqlctx->dbname);
	error = EEUSE;
      }
    }

    if (!error) {
      g_mutex_init(&msctx->lock);
      cache.ctxlist = g_slist_append(ectx->ctxlist, msctx);
    }
  }

  return error;
}

int exec_sql(const char *sql, msctx_t *msctx)
{
  int error = 0;

  gboolean lock;
  int i, len = g_slist_length(ectx->ctxlist);
  GSList *list = ectx->ctxlist;
  msctx_t *wrkctx = NULL;
  RETCODE erc;
	
  for (i = 0; i < len; i++) {
    error = 0;
    wrkctx = list->data;
    lock = g_mutex_trylock(&wrkctx->lock);

    if (lock && wrkctx) {
      
      if (dbdead(wrkctx->dbproc)) {
	g_message("isdead: reconnect...\n");
	dbclose(wrkctx->dbproc);
	
	if ((wrkctx->dbproc = dbopen(wrkctx->login,
				     ectx->ctx->servername)) == NULL) {
	  g_printerr("%s:%d: unable to connect to %s as %s\n",
		     ectx->ctx->appname, __LINE__,
		     ectx->ctx->servername, ectx->ctx->username);
	  error = EECONN;
	}
	
	if (!error && ectx->ctx->dbname
	    && (erc = dbuse(ctx->dbproc, ectx->ctx->dbname)) == FAIL) {
	  g_printerr("%s:%d: unable to use to database %s\n",
		     ectx->ctx->appname, __LINE__,
		     ectx->ctx->dbname);
	  error = EEUSE;
	}
	
      }

      if (!error && !do_exec_sql(sql, wrkctx))
	break;
      
      g_message("Error #%d: %s, %d", error, sql, i);
      g_mutex_unlock(&wrkctx->lock);
      
    }

    list = g_slist_next(list);
  }

  if (!wrkctx || !lock) {
    list = ectx->ctxlist;
    if (list && list->data) {
      wrkctx = list->data;
      g_mutex_lock(&wrkctx->lock);
    } else {
      g_printerr("no connections!");
    }
  }
  
  return error;
}

void close_sql(const msctx_t *context)
{
}

void close_context()
{

}
