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

#ifndef MSEXEC_H
#define MSEXEC_H

#include <sybdb.h>
#include <sybfront.h>

#include <glib.h>

typedef struct {
  LOGINREC *login;
  DBPROCESS *dbproc;
  
} msctx_t;


/*
 * Инициализация контекста
 */
void init_context(gpointer err_handler, gpointer msg_handler, GError **error);


/*
 * Количество свободных подключений. 
 * Отрицательное значение, когда есть ожидающие процессы.
 */
int get_free_count_contexts();


/*
 * Найти свободный контекст и захватить блокировку
 */
msctx_t * get_msctx(GError **error);


/*
 * Выполнить SQL-запрос на основе контекста
 */
void exec_sql_cmd(const char *sql, msctx_t *msctx, GError **error);


/*
 * Выполнить SQL-запрос на основе контекста
 */
msctx_t * exec_sql(const char *sql, GError **err);


/*
 * Закончить выполнение SQL-запроса
 */
void close_sql(msctx_t *context);


/*
 * Закрытие контекста
 */
void close_context(GError **error);

#endif
