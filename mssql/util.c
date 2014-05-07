#include "util.h"

struct cache_util {
  GHashTable *objtypes;
};

static struct cache_util cache;

int initobjtypes()
{
  if (!cache.objtypes)
    cache.objtypes = g_hash_table_new(g_str_hash, g_str_equal);
  else
    return 0;
  
  ADD_OBJTYPE("$H", D_SCHEMA);
  ADD_OBJTYPE("$L", R_COL);
  ADD_OBJTYPE("$T", R_TYPE);
  
  ADD_OBJTYPE("IT", D_IT);
  ADD_OBJTYPE("S", D_S);
  ADD_OBJTYPE("TT", D_TT);
  ADD_OBJTYPE("U", D_U);
  ADD_OBJTYPE("V", D_V);
  
  ADD_OBJTYPE("AF", R_AF);
  ADD_OBJTYPE("C", R_C);
  ADD_OBJTYPE("D", R_D);
  ADD_OBJTYPE("F", R_F);
  ADD_OBJTYPE("FN", R_FN);
  ADD_OBJTYPE("FS", R_FS);
  ADD_OBJTYPE("FT", R_FT);
  ADD_OBJTYPE("IF", R_IF);
  ADD_OBJTYPE("P", R_P);
  ADD_OBJTYPE("PC", R_PC);
  ADD_OBJTYPE("PG", R_PG);
  ADD_OBJTYPE("PK", R_PK);
  ADD_OBJTYPE("RF", R_RF);
  ADD_OBJTYPE("SN", R_SN);
  ADD_OBJTYPE("SO", R_SO);
  ADD_OBJTYPE("SQ", R_SQ);
  ADD_OBJTYPE("TA", R_TA);
  ADD_OBJTYPE("TR", R_TR);
  ADD_OBJTYPE("TF", R_TF);
  ADD_OBJTYPE("UQ", R_UQ);
  ADD_OBJTYPE("X", R_X);

  return 0;
}

int str2mstype(char * type)
{
  if (!type)
    return 0;

  gpointer r = g_hash_table_lookup(cache.objtypes, type);
  if (!r)
    return 0;
  
  return GPOINTER_TO_INT(r);
}

char * trimwhitespace(char *str)
{
  char *end;
  
  while(isspace(*str)) str++;
  
  if(*str == 0)
    return str;
  
  end = str + strlen(str) - 1;
  while(end > str && isspace(*end)) end--;
  
  *(end+1) = 0;
  
  return str;
}
