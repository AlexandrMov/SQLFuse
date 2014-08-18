#include <sqlfuse.h>
#include <conf/keyconf.h>
#include "msctx.h"

#include <string.h>

struct sqlcache {
  GMutex m;
  
  GHashTable *db_table, *app_table;
};

struct sqldeploy {
  GCond cond;
  GMutex lock;
  GTimer *timer;
  volatile int run;
  GThread *thread;
  GSequence *sql_seq;
};


enum action {
  CREP,
  DROP,
  RENAME
};

struct sql_cmd {
  enum action act;
  char *path;
  unsigned int objtype;
  char *buffer;
};

#define SAFE_REMOVE_ALL(p)						\
  g_mutex_lock(&cache.m);						\
  if (g_hash_table_contains(cache.db_table, p))				\
    g_hash_table_remove(cache.db_table, p);				\
  if (g_hash_table_contains(cache.app_table, p))			\
    g_hash_table_remove(cache.app_table, p);				\
  g_mutex_unlock(&cache.m);

static struct sqlcache cache;
static struct sqldeploy deploy;

static struct sqlfs_ms_obj * find_cache_obj(const char *pathname, GError **error)
{
  if (!g_path_is_absolute(pathname))
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
  
  struct sqlfs_ms_obj *obj = g_hash_table_lookup(cache.app_table, pathname);
  if (!obj)
    obj = g_hash_table_lookup(cache.db_table, pathname);
  
  if (!obj) {
    
    while(*tree && !terr) {
      g_string_append_printf(path, "%s%s", G_DIR_SEPARATOR_S, *tree);
      obj = g_hash_table_lookup(cache.db_table, path->str);
      
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
	  if (!g_hash_table_contains(cache.db_table, path->str)) {
	    g_mutex_lock(&cache.m);
	    g_hash_table_insert(cache.db_table, g_strdup(path->str), obj);
	    g_mutex_unlock(&cache.m);
	  }
	}
	else {
	  g_set_error(&terr, EENOTFOUND, EENOTFOUND,
		      "%d: Object not found\n", __LINE__);
	  break;
	}
	
      } else {
	g_clear_error(&terr);
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

static struct sqlfs_object * ms2sqlfs(struct sqlfs_ms_obj *src)
{
  struct sqlfs_object *result = g_try_new0(struct sqlfs_object, 1);

  result->object_id = src->object_id;
  result->name = g_strdup(src->name);
  
  if (src->type < 0x08)
    result->type = SF_DIR;
  else {
    result->type = SF_REG;
    
    result->len = src->len;
    result->def = g_strdup(src->def);
  }
  
  result->ctime = src->ctime;
  result->mtime = src->mtime;
  result->cached_time = g_get_monotonic_time();
  
  return result;
}

static void do_rename(const char *oldname, const char *newname,
		      gchar **schemaold, gchar **schemanew, GError **error)
{
  GError *terr = NULL;
  gchar *ppold = g_path_get_dirname(oldname);
  
  struct sqlfs_ms_obj
    *ppobj_old = find_cache_obj(ppold, &terr),
    *obj_old = find_cache_obj(oldname, &terr),
    *obj_new = g_try_new0(struct sqlfs_ms_obj, 1);
  
  if (terr == NULL) {
    obj_old->name = g_path_get_basename(oldname);
    obj_new->name = g_path_get_basename(newname);
    
    if (!g_hash_table_contains(cache.app_table, oldname)) {
      rename_ms_object(*schemaold, *schemanew, obj_old, obj_new,
		       ppobj_old, &terr);
      
      if (terr == NULL) {
	SAFE_REMOVE_ALL(oldname);
      }
      
    } else {
      obj_old->name = g_strdup(obj_new->name);
      g_mutex_lock(&cache.m);
      g_hash_table_steal(cache.app_table, oldname);
      g_hash_table_insert(cache.app_table, g_strdup(newname), obj_old);
      g_mutex_unlock(&cache.m);
    }
    
  }
  
  if (terr != NULL)
    g_propagate_error(error, terr);
  
  g_free(ppold);
  free_ms_obj(obj_new);
}

static void add2deploy(enum action act, const char *path,
		       unsigned int objtype, const char *buffer)
{
  g_mutex_lock(&deploy.lock);
  
  struct sql_cmd *cmd = g_try_new0(struct sql_cmd, 1);
  cmd->path = g_strdup(path);
  cmd->objtype = objtype;
  
  if (buffer != NULL)
    cmd->buffer = g_strdup(buffer);

  cmd->act = act;
  g_sequence_append(deploy.sql_seq, cmd);

  g_timer_start(deploy.timer);

  g_cond_signal(&deploy.cond);
  g_mutex_unlock(&deploy.lock);
}

static inline void do_deploy_sql()
{
  GSequenceIter *iter = g_sequence_get_begin_iter(deploy.sql_seq);
  while(!g_sequence_iter_is_end(iter)) {
    struct sql_cmd *cmd = g_sequence_get(iter);
    g_message("Deploy: %s", cmd->path);
    iter = g_sequence_iter_next(iter);
  }

  g_sequence_remove_range(g_sequence_get_begin_iter(deploy.sql_seq),
			  g_sequence_get_end_iter(deploy.sql_seq));
}


static gpointer deploy_thread(gpointer data) {
  while (deploy.run) {
    g_mutex_lock(&deploy.lock);
    
    while(!g_sequence_get_length(deploy.sql_seq) && deploy.run) {
      g_cond_wait(&deploy.cond, &deploy.lock);
    }

    if (!deploy.run) {
      g_mutex_unlock(&deploy.lock);
      break;
    }

    gdouble tm = g_timer_elapsed(deploy.timer, NULL);
    if (tm > get_context()->depltime) {
      do_deploy_sql();
      g_timer_stop(deploy.timer);
      g_mutex_unlock(&deploy.lock);
    }
    else {
      g_mutex_unlock(&deploy.lock);
      g_usleep((get_context()->depltime - tm) * 1000000);
    }
  }

  return 0;
}

static void free_sqlcmd_object(gpointer object)
{
  if (object == NULL)
    return ;

  struct sql_cmd *obj = (struct sql_cmd *) object;

  if (obj->path != NULL)
    g_free(obj->path);

  if (obj->buffer != NULL)
    g_free(obj->buffer);

  if (obj != NULL)
    g_free(obj);
}


void init_cache(GError **error)
{
  GError *terr = NULL;

  g_mutex_init(&cache.m);
  cache.db_table = g_hash_table_new_full(g_str_hash, g_str_equal,
					 g_free, free_ms_obj);
  cache.app_table = g_hash_table_new_full(g_str_hash, g_str_equal,
					  g_free, free_ms_obj);

  g_mutex_init(&deploy.lock);
  g_cond_init(&deploy.cond);
  deploy.sql_seq = g_sequence_new(&free_sqlcmd_object);
  deploy.timer = g_timer_new();
  deploy.run = 1;
  deploy.thread = g_thread_new(NULL, &deploy_thread, NULL);
  
  init_msctx(&terr);
  
  if (terr != NULL)
    g_propagate_error(error, terr);
}


struct sqlfs_object * find_object(const char *pathfile, GError **error)
{
  GError *terr = NULL;
  struct sqlfs_object *result;
  struct sqlfs_ms_obj *obj = find_cache_obj(pathfile, &terr);
  
  if (terr == NULL) {
    result = ms2sqlfs(obj);
  }
  
  if (terr != NULL)
    g_propagate_error(error, terr); 
  
  return result;
}


GList * fetch_dir_objects(const char *pathdir, GError **error)
{
  GList *reslist = NULL, *wrk = NULL;
  GError *terr = NULL;
  struct sqlfs_ms_obj *object = NULL;
  int nschema = g_strcmp0(pathdir, G_DIR_SEPARATOR_S);
  
  // получить объекты в соответствии с уровнем
  if (!nschema) {
    wrk = fetch_schemas(NULL, &terr);
  } else {
    object = find_cache_obj(pathdir, &terr);
    if (terr == NULL) {
      if (object->type == D_SCHEMA) 
	wrk = fetch_schema_obj(object->schema_id, NULL, &terr);
      else
	if (object->type == D_U || object->type == D_V) {
	  wrk = fetch_table_obj(object->schema_id, object->object_id,
				NULL, &terr);
	}
	else
	  g_set_error(&terr, EENOTSUP, EENOTSUP,
		      "%d: Operation not supported", __LINE__);
    }
  }

  if (terr == NULL) {
    
    // добавить объекты в кэш DB, если их нет в кэше APP
    wrk = g_list_first(wrk);
    gchar * str = NULL;
    while (wrk) {
      object = wrk->data;
      str = (nschema) ? g_strjoin(G_DIR_SEPARATOR_S, pathdir, object->name, NULL)
	: g_strconcat(pathdir, object->name, NULL);
      if (!g_hash_table_contains(cache.app_table, str)) {
	if (!g_hash_table_contains(cache.db_table, str)) {
	  g_mutex_lock(&cache.m);
	  g_hash_table_insert(cache.db_table, g_strdup(str), object);
	  g_mutex_unlock(&cache.m);
	}
	reslist = g_list_append(reslist, ms2sqlfs(object));
	g_free(str);
      }
      wrk = g_list_next(wrk);
    }
    g_list_free(wrk);

    // дополнить список объектов из APP кэша
    GHashTableIter iter;
    gpointer key, value;
    
    g_mutex_lock(&cache.m);
    g_hash_table_iter_init(&iter, cache.app_table);
    while (g_hash_table_iter_next(&iter, &key, &value)) {
      gchar *keydir = g_path_get_dirname(key);
      if (!g_strcmp0(keydir, pathdir)) {
	object = (struct sqlfs_ms_obj *) value;
	reslist = g_list_append(reslist, ms2sqlfs(object));
      }
      g_free(keydir);
    }
    g_mutex_unlock(&cache.m);

  }
  
  if (terr != NULL)
    g_propagate_error(error, terr);

  return reslist;
}

char * fetch_object_text(const char *path, GError **error)
{
  char *text;

  GError *terr = NULL;
  struct sqlfs_ms_obj *object = find_cache_obj(path, &terr);

  if (terr == NULL) {
    gchar **schema = g_strsplit(g_path_skip_root(path), G_DIR_SEPARATOR_S, -1);

    if (object->object_id != 0) {
      if (!g_hash_table_contains(cache.app_table, path))
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

void create_dir(const char *pathdir, GError **error)
{
  GError *terr = NULL;
  
  gchar **parent = g_strsplit(g_path_skip_root(pathdir), G_DIR_SEPARATOR_S, -1);
  if (parent == NULL)
    g_set_error(&terr, EENULL, EENULL,
		"%d: parent is not defined!", __LINE__);
  
  if (terr == NULL) {
    int level = g_strv_length(parent);
  
    if (level == 1) {
      add2deploy(CREP, pathdir, D_SCHEMA, NULL);
      create_schema(g_path_get_basename(pathdir), &terr);
    }
    else
      if (level == 2) {
	add2deploy(CREP, pathdir, D_U, NULL);
	create_table(*parent, g_path_get_basename(pathdir), &terr);
      }
      else
	g_set_error(&terr, EENOTSUP, EENOTSUP,
		    "%d: Operation not supported", __LINE__);
  
  }
  
  if (g_strv_length(parent) > 0) {
    g_strfreev(parent);
  }

  if (terr != NULL)
    g_propagate_error(error, terr);
}

void create_node(const char *pathfile, GError **error)
{
  GError *terr = NULL;
  
  gchar **parent = g_strsplit(g_path_skip_root(pathfile), G_DIR_SEPARATOR_S, -1);
  if (parent == NULL)
    g_set_error(&terr, EENULL, EENULL,
		"%d: parent is not defined!", __LINE__);
  
  if (terr == NULL) {
    struct sqlfs_ms_obj *tobj = g_try_new0(struct sqlfs_ms_obj, 1);
    tobj->object_id = 0;
    tobj->name = g_path_get_basename(pathfile);
    tobj->type = R_TEMP;
    
    g_mutex_lock(&cache.m);
    g_hash_table_insert(cache.app_table, g_strdup(pathfile), tobj);
    g_mutex_unlock(&cache.m);
  }
  
  if (g_strv_length(parent) > 0) {
    g_strfreev(parent);
  }

  if (terr != NULL)
    g_propagate_error(error, terr);
  
}


void write_object(const char *path, const char *buffer, GError **error)
{
  GError *terr = NULL;
  
  gchar **schema = g_strsplit(g_path_skip_root(path), G_DIR_SEPARATOR_S, -1);
  if (schema == NULL)
    g_set_error(&terr, EENULL, EENULL,
		"%d: parent is not defined!", __LINE__);
  
  if (terr == NULL) {
    gchar *pp = g_path_get_dirname(path);
    struct sqlfs_ms_obj *pobj = find_cache_obj(pp, &terr),
      *object = find_cache_obj(path, &terr);

    if (terr != NULL && terr->code == EENOTFOUND) {
      g_clear_error(&terr);
      object->name = g_path_get_basename(path);
    }

    if (object && buffer && strlen(buffer) > 0) {
      add2deploy(CREP, path, object->type, buffer);
      write_ms_object(*schema, pobj, buffer, object, &terr);
    }

    SAFE_REMOVE_ALL(path);

    g_free(pp);
  }

  if (schema != NULL)
    g_strfreev(schema);

  if (terr != NULL)
    g_propagate_error(error, terr);
  
}

void truncate_object(const char *path, off_t offset, GError **error)
{
  GError *terr = NULL;

  struct sqlfs_ms_obj *obj = g_hash_table_lookup(cache.app_table, path);
  if (!obj) {
    obj = find_cache_obj(path, &terr);
    if (terr == NULL) {
      g_hash_table_steal(cache.db_table, path);
    }
  }

  if (obj != NULL && terr == NULL) {
    gchar **schema = g_strsplit(g_path_skip_root(path), G_DIR_SEPARATOR_S, -1);
    char *def = NULL;
    if (obj->object_id != 0) {
      def = g_strndup(load_module_text(*schema, obj, &terr), offset);
    }
    else
      if (obj->def != NULL) {
	def = g_strndup(obj->def, offset);
      }

    if (obj->def != NULL)
      g_free(obj->def);

    if (offset > 0 && def != NULL) {
      obj->def = def;
      obj->len = strlen(obj->def);
    }
    else {
      obj->def = g_strdup("\0");
      obj->len = 0;
    }

    if (!g_hash_table_contains(cache.app_table, path)) {
      g_mutex_lock(&cache.m);
      g_hash_table_insert(cache.app_table, g_strdup(path), obj);
      g_mutex_unlock(&cache.m);
    }

    add2deploy(CREP, path, obj->type, obj->def);

    if (g_strv_length(schema) > 0) {
      g_strfreev(schema);
    }
  }

  if (terr != NULL)
    g_propagate_error(error, terr);
}


void remove_object(const char *path, GError **error)
{
  GError *terr = NULL;

  gchar **schema = g_strsplit(g_path_skip_root(path), G_DIR_SEPARATOR_S, -1);
  if (schema == NULL)
    g_set_error(&terr, EENULL, EENULL,
		"%d: parent is not defined!", __LINE__);

  if (terr == NULL) {
    struct sqlfs_ms_obj *object = find_cache_obj(path, &terr);
    if (terr == NULL) {
      add2deploy(DROP, path, object->type, NULL);
      remove_ms_object(*schema, *(schema + 1), object, &terr);      
    }
    
    SAFE_REMOVE_ALL(path);

  }

  if (g_strv_length(schema) > 0) {
    g_strfreev(schema);
  }

  if (terr != NULL)
    g_propagate_error(error, terr);
  
}


void rename_object(const char *oldname, const char *newname, GError **error)
{
  GError *terr = NULL;
  
  gchar **schemaold = g_strsplit(g_path_skip_root(oldname),
				 G_DIR_SEPARATOR_S, -1);
  gchar **schemanew = g_strsplit(g_path_skip_root(newname),
				 G_DIR_SEPARATOR_S, -1);
  
  if (g_strv_length(schemanew) == 1)
    g_set_error(&terr, EENOTSUP, EENOTSUP,
		"%d: Operation not supported", __LINE__);
  
  if (g_strv_length(schemaold) != g_strv_length(schemanew))
    g_set_error(&terr, EENOTSUP, EENOTSUP,
		"%d: Operation not supported", __LINE__);
  
  if (g_strv_length(schemanew) > 2
      && g_strcmp0(*(schemanew + 1), *(schemaold + 1)))
    g_set_error(&terr, EENOTSUP, EENOTSUP,
		"%d: Operation not supported", __LINE__);
  
  if (terr == NULL) {
    add2deploy(RENAME, oldname, 0, newname);
    struct sqlfs_ms_obj *obj_new = find_cache_obj(newname, &terr);
    if (terr == NULL) {
      if (g_hash_table_contains(cache.app_table, newname)) {
	g_hash_table_remove(cache.app_table, newname);
	do_rename(oldname, newname, schemaold, schemanew, &terr);
      }
      else {
	struct sqlfs_ms_obj *obj_old = find_cache_obj(oldname, &terr);
	if (g_hash_table_contains(cache.app_table, oldname)) {
	  obj_old->name = g_strdup(obj_new->name);
	  g_mutex_lock(&cache.m);
	  g_hash_table_steal(cache.app_table, oldname);
	  g_hash_table_insert(cache.app_table, g_strdup(newname), obj_old);
	  g_mutex_unlock(&cache.m);
	}
	else {	    
	  char *def = load_module_text(*schemaold, obj_old, &terr);
	  if (terr == NULL) {
	    gchar *ppnew = g_path_get_dirname(newname);
	    
	    g_free(obj_old->name);
	    obj_old->name = g_strdup(obj_new->name);
	    
	    struct sqlfs_ms_obj *ppobj_new = find_cache_obj(ppnew, &terr);
	    write_ms_object(*schemanew, ppobj_new, def, obj_old, &terr);
	    g_free(ppnew);
	  }
	}
	
	SAFE_REMOVE_ALL(oldname);
      }
    }
    else {
      g_clear_error(&terr);
      
      if (g_hash_table_contains(cache.app_table, newname))
	g_hash_table_remove(cache.app_table, newname);

      do_rename(oldname, newname, schemaold, schemanew, &terr);
      
    }
  }
  
  g_strfreev(schemanew);
  g_strfreev(schemaold);

  if (terr != NULL)
    g_propagate_error(error, terr);
}


void free_sqlfs_object(gpointer object)
{
  if (object == NULL)
    return ;

  struct sqlfs_object *obj = (struct sqlfs_object *) object;

  if (obj->name != NULL)
    g_free(obj->name);

  if (obj->def != NULL)
    g_free(obj->def);

  if (obj != NULL)
    g_free(obj);
}


void destroy_cache(GError **error)
{
  GError *terr = NULL;

  g_mutex_lock(&deploy.lock);
  deploy.run = 0;
  g_cond_signal(&deploy.cond);
  g_mutex_unlock(&deploy.lock);

  g_thread_join(deploy.thread);
  
  close_msctx(&terr);
  
  g_hash_table_destroy(cache.db_table);
  g_hash_table_destroy(cache.app_table);
  g_sequence_free(deploy.sql_seq);
  g_timer_destroy(deploy.timer);
  
  g_mutex_clear(&cache.m);
  g_mutex_clear(&deploy.lock);
  g_cond_clear(&deploy.cond);
  
  
  if (terr != NULL)
    g_propagate_error(error, terr);
}
