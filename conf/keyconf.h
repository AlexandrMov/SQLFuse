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

#ifndef KEYFILE_H
#define KEYFILE_H

#include <glib.h>

typedef struct {
  char *appname;
  char *servername, *dbname, *auth;
  char *username, *password;
  char *from_codeset, *to_codeset;

  gboolean ansi_npw, merge_names;

  char *defcol;
  
  int maxconn, debug;
} sqlctx_t;

/*
 * Инициализировать конфигурационный файл
 */
void init_keyfile(const char *profile, GError **error);

/*
 * Загрузить контекст
 */
sqlctx_t * fetch_context(int load_auth, GError **error);

/*
 * Очистить контекст
 */
void clear_context();

/*
 * Закрыть конфигурационный файл
 */
void close_keyfile();

#endif
