#ifndef TSQLCHECK_H
#define TSQLCHECK_H

/*
 * Лист синтаксического дерева
 */
typedef struct objnode {
  unsigned int type;
  
  char *objname;
  char *schema;
  
  int first_column;
  int first_line;
  int last_column;
  int last_line;
  
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
int start_checker();

/*
 *
 * Вызывается из парсера, - вставляет найденный
 * токен в синтаксическое дерево
 *
 */
int put_node(unsigned int type, char *schema, char *objname,
	     int fc, int fl, int lc, int ll);

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
int end_checker();

/*
 *
 * Освобожение ресурсов, занимаемых парсером. Вызывается однажды.
 *
 */
void close_checker();

#endif
