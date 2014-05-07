#ifndef UTIL_H
#define UTIL_H

#include <string.h>
#include "mssqlfs.h"

#define ADD_OBJTYPE(s,d)						\
  g_hash_table_insert(cache.objtypes, g_strdup(s), GINT_TO_POINTER(d));


int initobjtypes();

int str2mstype(char * type);

char * trimwhitespace(char *str);

#endif
