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

#ifndef CACHE_H
#define CACHE_H

#include <glib.h>

#define SF_DIR 0x01
#define SF_REG 0x02

struct sqlfs_object {
  gchar *name;
  int object_id;
  unsigned int type;
  
  unsigned int len;
  gchar *def;
  
  time_t ctime;
  time_t mtime;
  time_t cached_time;
};


/*
 * Инициализировать кэш. Вызывается однажды.
 */
void init_cache(GError **error);


/*
 * Найти объект
 */
struct sqlfs_object * find_object(const char *pathfile, GError **error);


/*
 * Получить список объектов директории
 */
GList * fetch_dir_objects(const char *pathdir, GError **error);


/*
 * Создать директорию
 */
void create_dir(const char *pathdir, GError **error);


/*
 * Создать пустой модуль
 */
void create_node(const char *pathfile, GError **error);


/*
 * Получить текст определения модуля
 */
char * fetch_object_text(const char *path, GError **error);


/*
 * Записать модуль
 */
void write_object(const char *path, struct sqlfs_object *object,
		  int mode, GError **error);


/*
 * Переименовать модуль
 */
void rename_object(const char *oldpath, const char *newpath, GError **error);


/*
 * Удалить объект
 */
void remove_object(const char *path, GError **error);


/*
 * Освободить память, занимаемую кэшем. Вызывается однажды.
 */
void destroy_cache(GError **error);


#endif
