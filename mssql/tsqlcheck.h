#ifndef TSQLCHECK_H
#define TSQLCHECK_H

struct tsql_node {
  unsigned int type;
  char *schema;
  char *objname;
};

int start_checker();

int put_node(unsigned int type, char *schema, char *objname);

struct tsql_node * get_node();

int end_checker();

#endif
