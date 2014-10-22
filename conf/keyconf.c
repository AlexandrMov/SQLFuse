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

#include "keyconf.h"

struct context {
  sqlctx_t *sqlctx;
  char *filename;
  char *authfn;
};

struct context *keyctx;

#define ADD_KEYVAR(v, p, t)				\
  if (g_key_file_has_key(keyfile, group, p, &terr))	\
    v = g_key_file_get_##t(keyfile, group, p, &terr);	\
    
#define ADD_KEYVAL(v, p)				\
  if (g_key_file_has_key(keyfile, group, p, &terr)) {	\
    if (v != NULL)					\
      g_free(v);					\
    v = g_key_file_get_value(keyfile, group, p, &terr);	\
  }

#define ADD_KEYBOOL(v, p) ADD_KEYVAR(v, p, boolean)
#define ADD_KEYINT(v, p) ADD_KEYVAR(v, p, integer)

static void load_from_file(GKeyFile *keyfile, const char *group, GError **error)
{
  GError *terr = NULL;
  sqlctx_t *sqlctx = keyctx->sqlctx;

  ADD_KEYVAL(sqlctx->appname, "appname");

  ADD_KEYINT(sqlctx->maxconn, "maxconn");

  ADD_KEYVAL(sqlctx->servername, "servername");
  ADD_KEYVAL(sqlctx->dbname, "dbname");
  
  ADD_KEYVAL(sqlctx->to_codeset, "to_codeset");
  ADD_KEYVAL(sqlctx->from_codeset, "from_codeset");

  ADD_KEYBOOL(sqlctx->ansi_npw, "ansi_npw");

  ADD_KEYINT(sqlctx->depltime, "deploy_time");

  if (g_key_file_has_key(keyfile, group, "auth", &terr))
    sqlctx->auth = g_key_file_get_value(keyfile, group, "auth", &terr);
  else {
    ADD_KEYVAL(sqlctx->username, "username");
    ADD_KEYVAL(sqlctx->password, "password");
  }

  ADD_KEYVAL(sqlctx->filter, "filter");
  
  if (g_key_file_has_key(keyfile, group, "exclude_schemas", &terr))
    sqlctx->excl_sch = g_key_file_get_string_list(keyfile, group, "exclude_schemas",
  						  NULL, &terr);
  
  if (terr != NULL)
    g_propagate_error(error, terr);
  
}

void init_keyfile(const char *profile, GError **error)
{
  GError *terr = NULL;
  keyctx = g_try_new0(struct context, 1);
  gchar *curdir = g_get_current_dir();
  keyctx->filename = g_strconcat(g_get_user_config_dir(), "/sqlfuse/sqlfuse.conf", NULL);
  keyctx->authfn = g_strconcat(g_get_user_config_dir(), "/sqlfuse/sqlfuse.auth.conf", NULL);

  GKeyFile *keyfile = g_key_file_new();
  g_key_file_load_from_file(keyfile, keyctx->filename, G_KEY_FILE_NONE, &terr);
  
  keyctx->sqlctx = g_try_new0(sqlctx_t, 1);
  if (terr == NULL) {
    
    if (g_key_file_has_group(keyfile, "global")) {
      load_from_file(keyfile, "global", &terr);
    }

    if (terr == NULL) {
      if (g_key_file_has_group(keyfile, profile)) {
	load_from_file(keyfile, profile, &terr);
      }
      else {
	g_set_error(&terr, EENOTFOUND, EENOTFOUND,
		    "%s:%d: Profile not found in config",
		    __FILE__, __LINE__);
      }
    }
    
  }
  
  g_key_file_free(keyfile);
  g_free(curdir);
  
  if (terr != NULL)
    g_propagate_error(error, terr);
}

sqlctx_t * get_context() {
  return keyctx->sqlctx;
}

sqlctx_t * fetch_context(gboolean load_auth, GError **error)
{
  sqlctx_t *sqlctx = keyctx->sqlctx;

  if (!sqlctx->appname)
    sqlctx->appname = g_strdup("sqlfuse");

  if (sqlctx->maxconn < 1)
    sqlctx->maxconn = 1;

  if (load_auth && sqlctx->auth) {
    GError *terr = NULL;
    GKeyFile *keyfile = g_key_file_new();
    g_key_file_load_from_file(keyfile, keyctx->authfn, G_KEY_FILE_NONE, &terr);
    
    if (g_key_file_has_group(keyfile, sqlctx->auth)) {

      if (g_key_file_has_key(keyfile, sqlctx->auth, "username", &terr))
	sqlctx->username = g_key_file_get_value(keyfile, sqlctx->auth,
						"username", &terr);
      
      if (g_key_file_has_key(keyfile, sqlctx->auth, "password", &terr))
	sqlctx->password = g_key_file_get_value(keyfile, sqlctx->auth,
						"password", &terr);
    }

    g_key_file_free(keyfile);

    if (terr != NULL)
      g_propagate_error(error, terr);
  }

  return keyctx->sqlctx;
}

void clear_context()
{
  if (keyctx->sqlctx->username != NULL) {
    g_free(keyctx->sqlctx->username);
    keyctx->sqlctx->username = NULL;
  }

  if (keyctx->sqlctx->password != NULL) {
    g_free(keyctx->sqlctx->password);
    keyctx->sqlctx->password = NULL;
  }
}

void close_keyfile()
{
  clear_context();

  if (keyctx->authfn != NULL)
    g_free(keyctx->authfn);

  if (keyctx->filename != NULL)
    g_free(keyctx->filename);

  sqlctx_t *sqlctx = keyctx->sqlctx;
  if (sqlctx != NULL) {
    if (sqlctx->appname != NULL)
      g_free(sqlctx->appname);
    
    if (sqlctx->servername != NULL)
      g_free(sqlctx->servername);

    if (sqlctx->dbname != NULL)
      g_free(sqlctx->dbname);

    if (sqlctx->to_codeset != NULL)
      g_free(sqlctx->to_codeset);

    if (sqlctx->from_codeset != NULL)
      g_free(sqlctx->from_codeset);

    if (sqlctx->auth != NULL)
      g_free(sqlctx->auth);
    
    if (sqlctx->excl_sch != NULL && g_strv_length(sqlctx->excl_sch) > 0)
      g_strfreev(sqlctx->excl_sch);

    if (sqlctx->filter != NULL)
      g_free(sqlctx->filter);
    
    g_free(sqlctx);
  }

  if (keyctx != NULL)
    g_free(keyctx);

}
