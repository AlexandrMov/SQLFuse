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

#include <glib.h>

#include "tsqlcheck.h"
#include "tsql.tab.h"

#define TOKEN_POS_A(t) first_column##t, first_line##t, \
    last_column##t, last_line##t

#define TOKEN_POS_ASSIGN(dest) TOKEN_POS_ASSIGN_N(dest, )

#define TOKEN_POS_ASSIGN_N(dest, t) \
  dest->first_line##t = first_line##t; \
  dest->first_column##t = first_column##t; \
  dest->last_line##t = last_line##t; \
  dest->last_column##t = last_column##t;

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

void start_checker()
{
  if (!checker->node)
    checker->node = g_try_new0(objnode_t, 1);
  
  g_mutex_lock(&checker->lock);
}

void put_node(unsigned int type, char *schema, char *objname, TOKEN_POS())
{
  if (checker->node == NULL) {
    checker->node = g_try_new0(objnode_t, 1);
  }

  if (checker->node != NULL) {
    TOKEN_POS_ASSIGN(checker->node);
    
    if (schema != NULL)
      checker->node->schema = g_strdup(schema);

    if (objname != NULL)
      checker->node->objname = g_strdup(objname);
    
    checker->node->type = type;
  }

  reset_column();
}

void put_column(char *schema, char *objname, TOKEN_POS())
{
  put_node(COLUMN, schema, objname, TOKEN_POS_A());
}

void put_module(unsigned int make_type, TOKEN_POS(m),
		unsigned int module_type, char *schema, char *objname,
		TOKEN_POS(t))
{
  put_node(module_type, schema, objname, TOKEN_POS_A(t));
  
  checker->node->module_node = g_try_new0(mod_node_t , 1);
  checker->node->module_node->make_type = make_type;

  TOKEN_POS_ASSIGN_N(checker->node->module_node, m);
}

void put_default(char *schema, char *objname, TOKEN_POS())
{
  put_node(DEFAULT, schema, objname, TOKEN_POS_A());
}


void put_check(unsigned int check, unsigned int check_type, TOKEN_POS())
{
  put_node(check_type, NULL, NULL, TOKEN_POS_A());
  checker->node->check_node = g_try_new0(check_node_t, 1);
  checker->node->check_node->check = check;
  checker->node->check_node->check_type = check_type;
}


objnode_t * get_node() {
  return checker->node;
}

void end_checker()
{
  /*if (checker->node != NULL) {
    if (checker->node->schema != NULL)
      g_free(checker->node->schema);

    if (checker->node->objname != NULL)
      g_free(checker->node->objname);

    if (checker->node->module_node != NULL)
      g_free(checker->node->module_node);

    if (checker->node->check_node != NULL)
      g_free(checker->node->check_node);
    
    g_free(checker->node);
    }*/

  g_mutex_unlock(&checker->lock);
}

void close_checker()
{
  g_mutex_clear(&checker->lock);
  g_free(checker);
}
