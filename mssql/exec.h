#include "mssql.h"

#define EENULL 0x01
#define EEMEM 0x09
#define EELOGIN 0x10
#define EECONN 0x11
#define EEUSE 0x12
#define EEINIT 0x13
#define EECMD 0x21
#define EEXEC 0x22
#define EERES 0x23

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
int exec_sql(const char *sql, msctx_t *context);

/*
 *
 * Закончить выполнение SQL-запроса
 *
 */
void close_sql(const msctx_t *context);

/*
 *
 * Закрытие контекста
 *
 */
void close_context();
