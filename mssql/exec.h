#include "mssqlfs.h"

typedef struct {
  LOGINREC *login;
  DBPROCESS *dbproc;
  
  GMutex lock;
  
} msctx_t;

/*
 *
 * Инициализация контекста
 *
 */
int init_context(const struct sqlctx *sqlctx, gpointer err_handler,
		 gpointer msg_handler);

/*
 *
 * Выполнить SQL-запрос на основе контекста
 *
 */
int exec_sql(const char *sql, msctx_t **context);

/*
 *
 * Закончить выполнение SQL-запроса
 *
 */
void close_sql(msctx_t *context);

/*
 *
 * Закрытие контекста
 *
 */
int close_context();
