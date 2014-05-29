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

/*
 * Вернёт SQL-запрос, создающий новое поле в таблице
 */
char * create_column_def(const char *schema, const char *table,
			 struct sqlfs_ms_obj *obj, const char *def);

char * make_column_def(const struct sqlfs_ms_obj *obj);

char * make_index_def(const struct sqlfs_ms_obj *obj);

/*
 * Вернёт SQL-запрос, создающий новое ограничение в таблице
 */
char * create_constr_def(const char *schema, const char *table,
			 struct sqlfs_ms_obj *obj, const char *def);

char * make_constraint_def(const struct sqlfs_ms_obj *obj, const char *def);

GList * fetch_columns(int table_id, const char *name, GError **err);

GList * fetch_modules(int table_id, const char *name, GError **err);

GList * fetch_indexes(int table_id, const char *name, GError **err);

GList * fetch_constraints(int table_id, const char *name, GError **err);
