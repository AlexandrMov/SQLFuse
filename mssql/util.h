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

#ifndef UTIL_H
#define UTIL_H

#include <string.h>
#include "msctx.h"

#define ADD_OBJTYPE(s,d)						\
  g_hash_table_insert(cache.objtypes, g_strdup(s), GINT_TO_POINTER(d)); \
  g_hash_table_insert(cache.objtypenames, GINT_TO_POINTER(d), g_strdup(s));


int initobjtypes();

int str2mstype(char * type);

char * mstype2str(int type);

#endif
