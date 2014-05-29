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
  unsigned int not4repl;
  
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


void put_check(unsigned int check, unsigned int not4repl, TOKEN_POS());

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
