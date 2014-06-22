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

#ifndef TSQLCHECK_H
#define TSQLCHECK_H

#define TOKEN_POS(t) int first_column##t, int first_line##t, \
    int last_column##t, int last_line##t
#define TOKEN_POS_T(t) int first_column##t, first_line##t, \
    last_column##t, last_line##t

typedef struct module_mode {
  unsigned int make_type;

  TOKEN_POS_T(m);
} mod_node_t;


typedef struct check_node {
  unsigned int check;
  
} check_node_t;

/*
 * Лист синтаксического дерева
 */
typedef struct objnode {
  unsigned int type;
  
  char *objname;
  char *schema;

  union {
    mod_node_t *module_node;
    check_node_t *check_node;
  };
  
  TOKEN_POS_T();
} objnode_t;


/*
 *
 * Инициализация парсера, вызывается однажды
 *
 */
void init_checker();

/*
 *
 * Подготовка парсера к работе. После окончания парсинга,
 * должна вызываться end_checker()
 *
 */
void start_checker();

/*
 *
 * Вызывается из парсера, - вставляет найденный
 * токен в синтаксическое дерево
 *
 */
void put_node(unsigned int type, char *schema, char *objname, TOKEN_POS());


void put_column(char *schema, char *objname, TOKEN_POS());


void put_module(unsigned int make_type, TOKEN_POS(m),
		unsigned int module_type, char *schema, char *objname,
		TOKEN_POS(t));


void put_default(char *schema, char *objname, TOKEN_POS());


void put_check(unsigned int check, TOKEN_POS());

/*
 *
 * Вернёт синтаксическое дерево
 *
 */
objnode_t * get_node();

/*
 *
 * Завершение парсинга
 *
 */
void end_checker();

/*
 *
 * Освобожение ресурсов, занимаемых парсером. Вызывается однажды.
 *
 */
void close_checker();

#endif
