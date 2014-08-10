#include "cache.h"

#include "mssqlfs.h"
#include <conf/keyconf.h>


struct sqlcache {
  GMutex m;
  
  GHashTable *cache_table, *temp_table, *action_table;
};

enum action {
  CREP,
  DROP,
  RENAME
};

struct cache_oper {
  action cache_action;
  char *path;
  unsigned int objtype;
  char *buffer;
};

static struct sqlcache cache;

static struct sqlfs_ms_object * find_cache_obj(const char *pathfile, GError **error)
{
  if (!g_path_is_absolute(pathfile))
    return NULL;

  GList *list = NULL;
  
  const gchar * pn = g_path_skip_root(pathname);
  if (pn == NULL)
    pn = pathname;
  
  gchar **tree = g_strsplit(pn, G_DIR_SEPARATOR_S, -1);
  if (tree == NULL)
    return 0;

  GError *terr = NULL;
  GString * path = g_string_new(NULL);
  guint i = 0;
  
  struct sqlfs_ms_obj *obj = g_hash_table_lookup(cache.temp_table, pathname);
  if (!obj)
    obj = g_hash_table_lookup(cache.cache_table, pathname);
  
  if (!obj) {
    
    while(*tree && !terr) {
      g_string_append_printf(path, "%s%s", G_DIR_SEPARATOR_S, *tree);
      obj = g_hash_table_lookup(cache.cache_table, path->str);
      
      if (!obj) {
	if (i == 0) {
	  obj = find_ms_object(NULL, *tree, &terr);
	}
	if (i > 0) {
	  obj = find_ms_object(g_list_last(list)->data, *tree, &terr);
	}
      }
      
      if (!terr) {
	
	if (obj && obj->name) {
	  list = g_list_append(list, obj);
	  if (!g_hash_table_contains(cache.cache_table, path->str)) {
	    g_mutex_lock(&cache.m);
	    g_hash_table_insert(cache.cache_table, g_strdup(path->str), obj);
	    g_mutex_unlock(&cache.m);
	  }
	}
	else {
	  g_set_error(&terr, EENOTFOUND, EENOTFOUND,
		      "%d: Object not found\n", __LINE__);
	  break;
	}
	
      } else {
	g_set_error(&terr, EERES, EERES,
		    "%d: Find object failed\n", __LINE__);
	break;
      }
      
      tree++;
      i++;
    }
    
    GList *last = g_list_last(list);
    if (last)
      obj = last->data;
    
    g_list_free(list);
    
  }

  tree -= i;
  g_strfreev(tree);
  g_string_free(path, TRUE);

  if (terr != NULL)
    g_propagate_error(error, terr); 
  
  return obj;
}

struct sqlfs_object * find_object(const char *pathfile, GError **error)
{
  GError *terr;
  struct sqlfs_object *result = NULL;
  struct sqlfs_ms_obj *obj = find_cache_obj(pathfile, &terr);

  if (terr == NULL) {
    result = g_try_new0(struct sqlfs_object, 1);
    
    result->object_id = obj->object_id;
    result->name = g_strdup(obj->name);

    if (obj->type < 0x08)
      result->type = SF_DIR;
    else {
      result->type = SF_REG;
      
      result->len = obj->len;
      result->def = g_strdup(obj->def);
    }

    result->ctime = obj->ctime;
    result->mtime = obj->mtime;
    result->cached_time = g_get_monotonic_time();
  }
  
  if (terr != NULL)
    g_propagate_error(error, terr); 
  
  return result;
}


GList * fetch_dir_objects(const char *pathdir, GError **error)
{
  GList *reslist = NULL;
  GError *terr = NULL;
  struct sqlfs_ms_obj *object = NULL;
  
  if (!g_strcmp0(path, G_DIR_SEPARATOR_S)) {
    reslist = fetch_schemas(NULL, &terr);
  } else {
    object = find_cache_obj(path, &terr);
    if (terr == NULL) {
      if (object->type == D_SCHEMA) 
	reslist = fetch_schema_obj(object->schema_id, NULL, &terr);
      else
	if (object->type == D_U || object->type == D_V) {
	  reslist = fetch_table_obj(object->schema_id, object->object_id,
				    NULL, &terr);
	}
	else
	  g_set_error(&terr, EENOTSUP, EENOTSUP,
		      "%d: Operation not supported", __LINE__);
    }
  }
  
  if (terr != NULL)
    g_propagate_error(error, terr);

  return reslist;
}

char * fetch_object_text(const char *path, GError **error)
{
  char *text;

  GError *terr;
  struct sqlfs_ms_obj *object = find_cache_obj(path, &terr);

  if (terr == NULL) {
    gchar **schema = g_strsplit(g_path_skip_root(path), G_DIR_SEPARATOR_S, -1);

    if (object->object_id != 0) {
      if (!g_hash_table_contains(cache.temp_table, path))
	text = load_module_text(*schema, object, &terr);
      else
	text = object->def;
    }
    else {
      text = g_strdup("\0");
    }

    g_strfreev(schema); 
  }
  
  if (terr != NULL)
    g_propagate_error(error, terr);
  
  return text;
}
