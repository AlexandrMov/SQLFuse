#include <glib.h>

#include "tsqlcheck.h"

struct tsql_checker {
  struct tsql_node *node;
  GMutex lock;
};

struct tsql_checker checker;

int start_checker()
{
  g_mutex_init(&checker.lock);
  g_mutex_lock(&checker.lock);
}

int put_node(unsigned int type, char *schema, char *objname)
{
  if (!checker.node)
    checker.node = g_try_new0(struct tsql_node, 1);

  if (checker.node) {
    if (schema)
      checker.node->schema = g_strdup(schema);
    
    checker.node->objname = g_strdup(objname);
    checker.node->type = type;
  }

  return 0;
}

struct tsql_node *get_node() {
  return checker.node;
}

int end_checker()
{
  if (checker.node->schema)
    g_free(checker.node->schema);
  
  g_free(checker.node->objname);
  g_mutex_unlock(&checker.lock);
  g_mutex_clear(&checker.lock);

  return 0;
}
