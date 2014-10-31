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
#include <string.h>
#include "exec.h"

typedef struct {
  GAsyncQueue *aqueue;
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

static gint sort_contexts(gconstpointer a, gconstpointer b, gpointer user_data)
{
  if (a != NULL && b != NULL) {
    msctx_t *ctx1 = (msctx_t *) a, *ctx2 = (msctx_t *) b;
    int a1 = dbdead(ctx1->dbproc), b1 = dbdead(ctx2->dbproc);

    if (a1 && !b1)
      return 1;

    if (!a1 && b1)
      return -1;
  }

  return 0;
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
    
    ectx->aqueue = g_async_queue_new();

    int i;
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

	msctx->dbproc = NULL;
      }

      if (terr == NULL) {
	g_async_queue_push(ectx->aqueue, msctx);
      }
      else {
	break;
      }
      
    }

  }

  clear_context();
  
  if (terr != NULL)
    g_propagate_error(error, terr);
}

int get_count_free_contexts()
{
  return g_async_queue_length(ectx->aqueue);
}

static void reconnect(msctx_t *wrkctx, GError **error)
{
  GError *terr = NULL;
  RETCODE erc;

  char *npw_sql = get_npw_sql();

  if (wrkctx->dbproc)
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
      && !do_exec_sql(npw_sql, wrkctx, &terr)) {
    g_set_error(&terr, EEXEC, EEXEC,
		"%s:%d: unable sets init params NPW\n",
		sqlctx->appname, __LINE__);
  }

  clear_context();
  g_free(npw_sql);

  if (terr != NULL)
    g_propagate_error(error, terr);
}

msctx_t * get_msctx(GError **error)
{
  msctx_t *wrkctx = NULL;
  GError *terr = NULL;
  
  wrkctx = g_async_queue_pop(ectx->aqueue);

  if (dbdead(wrkctx->dbproc)) {
#ifdef SQLDEBUG
    g_message("isdead: reconnect...\n");
#endif
    reconnect(wrkctx, &terr);
  }

  if (terr != NULL)
    g_propagate_error(error, terr);

  return wrkctx;
}

void exec_sql_cmd(const char *sql, msctx_t *msctx, GError **error)
{
  GError *terr = NULL;
  int i;
  for (i = 0; i < 2; i++) {

    if (terr != NULL)
      g_clear_error(&terr);

    gboolean result = do_exec_sql(sql, msctx, &terr);

    if (!result) {
      if (dbdead(msctx->dbproc)) {
	// попробовать восстановить подключение
	g_usleep(1000000);

	g_clear_error(&terr);
	reconnect(msctx, &terr);
      }
      else {
	// закончить выполнение с ошибкой
	break;
      }
    }
    else {
      // выполнение запроса успешно
      break;
    }
    
  }

  if (terr != NULL)
    g_propagate_error(error, terr);
}

msctx_t * exec_sql(const char *sql, GError **error)
{
  GError *terr = NULL;
  msctx_t *wrkctx = get_msctx(&terr);

  if (terr == NULL)
    exec_sql_cmd(sql, wrkctx, &terr);

  if (terr != NULL)
    g_propagate_error(error, terr);

  return wrkctx;
}

void close_sql(msctx_t *context)
{
  if (!context)
    return ;

  dbfreebuf(context->dbproc);

  // не устанавливать дополнительных подключений без необходимости
  g_async_queue_push_sorted(ectx->aqueue, context, &sort_contexts, NULL);
}

void close_context(GError **error)
{
  GError *terr = NULL;
  int i = 0, maxconn = get_context()->maxconn;
  msctx_t *wrkctx = NULL;

  while (i < maxconn) {

    // подождать для закрытия всех подключений
    wrkctx = g_async_queue_pop(ectx->aqueue);
    if (wrkctx->dbproc) {
      dbclose(wrkctx->dbproc);
    }

    if (wrkctx->login) {
      dbloginfree(wrkctx->login);
    }

    g_free(wrkctx);

    i++;

  }

  if (ectx->to_codeset != NULL)
    g_free(ectx->to_codeset);

  if (ectx->from_codeset != NULL)
    g_free(ectx->from_codeset);

  g_async_queue_unref(ectx->aqueue);

  if (terr != NULL)
    g_propagate_error(error, terr);
}
