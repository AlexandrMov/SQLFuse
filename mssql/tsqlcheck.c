#include <glib.h>

#include "tsqlcheck.h"

struct tsql_checker {
  objnode_t *node;
  GMutex lock;
};

struct tsql_checker *checker;

void init_checker()
{
  if (!checker) {
    checker = g_try_new0(struct tsql_checker, 1);
    g_mutex_init(&checker->lock);
  }
}

int start_checker()
{
  if (!checker->node)
    checker->node = g_try_new0(objnode_t, 1);
  
  g_mutex_lock(&checker->lock);
}

int put_node(unsigned int type, char *schema, char *objname,
	     int fc, int fl, int lc, int ll)
{
  if (checker->node == NULL) {
    checker->node = g_try_new0(objnode_t, 1);
  }

  if (checker->node != NULL) {
    checker->node->first_line = fl;
    checker->node->first_column = fc;
    checker->node->last_line = ll;
    checker->node->last_column = lc;
    
    if (schema != NULL)
      checker->node->schema = g_strdup(schema);
    
    checker->node->objname = g_strdup(objname);
    checker->node->type = type;
  }

  return 0;
}

objnode_t * get_node() {
  return checker->node;
}

int end_checker()
{
  if (checker->node != NULL) {
    if (checker->node->schema != NULL)
      g_free(checker->node->schema);

    if (checker->node->objname != NULL)
      g_free(checker->node->objname);
    
    g_free(checker->node);
  }

  g_mutex_unlock(&checker->lock);

  return 0;
}

void close_checker()
{
  g_mutex_clear(&checker->lock);
  g_free(checker);
}
