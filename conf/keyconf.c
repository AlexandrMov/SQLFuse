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

static void load_from_file(GKeyFile *keyfile, const char *group, GError **error)
{
  GError *terr = NULL;
  sqlctx_t *sqlctx = keyctx->sqlctx;
  if (g_key_file_has_key(keyfile, group, "appname", &terr))
    sqlctx->appname = g_key_file_get_value(keyfile, group, "appname", &terr);

  if (g_key_file_has_key(keyfile, group, "maxconn", &terr))
    sqlctx->maxconn = g_key_file_get_integer(keyfile, group, "maxconn", &terr);

  if (g_key_file_has_key(keyfile, group, "servername", &terr))
    sqlctx->servername = g_key_file_get_value(keyfile, group, "servername", &terr);

  if (g_key_file_has_key(keyfile, group, "dbname", &terr))
    sqlctx->dbname = g_key_file_get_value(keyfile, group, "dbname", &terr);

  if (g_key_file_has_key(keyfile, group, "to_codeset", &terr))
    sqlctx->to_codeset = g_key_file_get_value(keyfile, group, "to_codeset", &terr);

  if (g_key_file_has_key(keyfile, group, "from_codeset", &terr))
    sqlctx->from_codeset = g_key_file_get_value(keyfile, group,
						"from_codeset", &terr);

  if (g_key_file_has_key(keyfile, group, "auth", &terr))
    sqlctx->auth = g_key_file_get_value(keyfile, group, "auth", &terr);
  else {
    if (g_key_file_has_key(keyfile, group, "username", &terr))
      sqlctx->username = g_key_file_get_value(keyfile, group, "username", &terr);
    
    if (g_key_file_has_key(keyfile, group, "password", &terr))
      sqlctx->password = g_key_file_get_value(keyfile, group, "password", &terr);
  }

  if (terr != NULL)
    g_propagate_error(error, terr);
  
}

void init_keyfile(const char *profile, GError **error)
{
  GError *terr = NULL;
  keyctx = g_try_new0(struct context, 1);
  gchar *curdir = g_get_current_dir();
  keyctx->filename = g_strconcat(curdir, "/conf/sqlfuse.conf", NULL);
  keyctx->authfn = g_strconcat(curdir, "/conf/sqlfuse.auth.conf", NULL);

  GKeyFile *keyfile = g_key_file_new();
  g_key_file_load_from_file(keyfile, keyctx->filename, G_KEY_FILE_NONE, &terr);
  
  keyctx->sqlctx = g_try_new0(sqlctx_t, 1);
  if (terr == NULL && g_key_file_has_group(keyfile, "global")) {
    load_from_file(keyfile, "global", &terr);
  }

  if (terr == NULL && g_key_file_has_group(keyfile, profile)) {
    load_from_file(keyfile, profile, &terr);
  }

  g_key_file_free(keyfile);
  g_free(curdir);
  
  if (terr != NULL)
    g_propagate_error(error, terr);
}

sqlctx_t * fetch_context(gboolean load_auth, GError **error)
{
  sqlctx_t *sqlctx = keyctx->sqlctx;

  if (!sqlctx->appname)
    sqlctx->appname = g_strdup("sqlfuse");

  if (!sqlctx->maxconn)
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
  if (keyctx->sqlctx->username != NULL)
    g_free(keyctx->sqlctx->username);

  if (keyctx->sqlctx->password != NULL)
    g_free(keyctx->sqlctx->password);
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

    if (sqlctx->from_codeset != NULL)
      g_free(sqlctx->auth);

    
    g_free(sqlctx);
  }

  if (keyctx != NULL)
    g_free(keyctx);

}
