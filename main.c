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

#include "mssql/mssqlfs.h"
#include <conf/keyconf.h>

struct sqlcache {
  GMutex m;
  GHashTable *cache_table, *temp_table, *open_table;
};

struct sqlprofile {
  char *profile;
};

typedef struct {
  gchar *buffer;
  int flush;
} sqlfs_file_t;

#define FUSE_USE_VERSION 26
#include <fuse.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>

static struct sqlcache cache;

#define SQLFS_OPT(t, p, v) { t, offsetof(struct sqlprofile, p), v }

#define SAFE_REMOVE_ALL(p) g_mutex_lock(&cache.m); \
  g_hash_table_remove(cache.cache_table, p);	   \
  g_hash_table_remove(cache.temp_table, p);	   \
  g_mutex_unlock(&cache.m);

static struct fuse_opt sqlfs_opts[] = {
  SQLFS_OPT("profilename=%s", profile, 0),

  FUSE_OPT_END
};

static int sqlfs_opt_proc(void * data, const char * arg, int key,
			  struct fuse_args * outargs)
{
  (void) data;
  (void) outargs;
  switch(key) {
  case FUSE_OPT_KEY_OPT:
    return 1;
  case FUSE_OPT_KEY_NONOPT:
    return 1;
  default:
    exit(1);
  }
}

static struct sqlfs_ms_obj * find_object(const char *pathname, GError **error)
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
  
  struct sqlfs_ms_obj *obj = g_hash_table_lookup(cache.cache_table, pathname);
  if (!obj)
    obj = g_hash_table_lookup(cache.temp_table, pathname);
  
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

static int sqlfs_getattr(const char *path, struct stat *stbuf)
{
  int err = 0;
  memset(stbuf, 0, sizeof(struct stat));
  
  if (g_strcmp0(path, G_DIR_SEPARATOR_S) == 0) {
    stbuf->st_mode = S_IFDIR | 0755;
    stbuf->st_nlink = 2;
  } else {
    GError *terr = NULL;
    struct sqlfs_ms_obj *object = find_object(path, &terr);
    if (terr != NULL) {
      err = -ENOENT;
    }
    else {
      
      if (object->type < 0x08) {
	stbuf->st_mode = S_IFDIR | 0755;
	stbuf->st_nlink = 2;
      } else {
	stbuf->st_mode = S_IFREG | 0666;
	stbuf->st_nlink = 1;
	stbuf->st_size = object->len;
      }
      stbuf->st_mtime = object->mtime;
      stbuf->st_ino = object->object_id;
      
    }
  }

  return err;
}

static int sqlfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
			 off_t offset, struct fuse_file_info *fi)
{
  filler(buf, ".", NULL, 0);
  filler(buf, "..", NULL, 0);

  int nschema = g_strcmp0(path, G_DIR_SEPARATOR_S);
  GList *wrk = NULL;
  GError *terr = NULL;
  int err = 0;
  struct sqlfs_ms_obj *object = NULL;
  
  if (!nschema) {
    wrk = fetch_schemas(NULL, &terr);
  } else {
    object = find_object(path, &terr);
    if (!object || !object->name)
      return -ENOENT;

    if (object->type == D_SCHEMA) 
      wrk = fetch_schema_obj(object->schema_id, NULL, &terr);
    else if (object->type == D_U || object->type == D_V) {
      wrk = fetch_table_obj(object->schema_id, object->object_id, NULL, &terr);
    }
    else
      return -ENOENT;
  }

  if (!terr && wrk) {
    wrk = g_list_first(wrk);
    gchar * str = NULL;
    while (wrk) {
      object = wrk->data;
      filler(buf, object->name, NULL, 0);
      str = (nschema) ? g_strjoin(G_DIR_SEPARATOR_S, path, object->name, NULL)
	: g_strconcat(path, object->name, NULL);
      if (!g_hash_table_contains(cache.cache_table, str)) {
	g_mutex_lock(&cache.m);
	g_hash_table_insert(cache.cache_table, g_strdup(str), object);
	g_mutex_unlock(&cache.m);
      }
      wrk = g_list_next(wrk);
      g_free(str);
    }
    g_list_free(wrk);
  
    gchar *dir = g_path_get_dirname(path);
    GHashTableIter iter;
    gpointer key, value;

    g_mutex_lock(&cache.m);
    g_hash_table_iter_init(&iter, cache.temp_table);
    while (g_hash_table_iter_next(&iter, &key, &value)) {
      if (g_strcmp0(key, dir) == 0 && value) {
	object = (struct sqlfs_ms_obj *) value;
	filler(buf, object->name, NULL, 0);
      }
    }
    g_mutex_unlock(&cache.m);
    g_free(dir);
  }
  else
    if (terr != NULL && terr->code == EERES) {
      err = -ECONNABORTED;
    }
  
  return err;
}

static int sqlfs_read(const char *path, char *buf, size_t size,
		      off_t offset, struct fuse_file_info *fi)
{
  sqlfs_file_t *fsfile = g_hash_table_lookup(cache.open_table, &(fi->fh));
  if (!fsfile || !fsfile->buffer)
    return 0;
  
  size_t len = strlen(fsfile->buffer);
  if (len < 1)
    return 0;
  
  if (offset < len) {
    if (offset + size > len)
      size = len - offset;
    memcpy(buf, fsfile->buffer + offset, size);
  } else
    size = 0;

  return size;
}

static int sqlfs_chmod(const char *path, mode_t mode)
{
  int err = 0;

  return err;
}

static int sqlfs_chown(const char *path, uid_t uid, gid_t gid)
{
  int err = 0;

  return err;
}

static int sqlfs_utime(const char *path, struct utimbuf* time)
{
  (void) path;
  (void) time;
  
  return 0;
}

static int sqlfs_mkdir(const char *path, mode_t mode)
{
  int err = 0;
  
  if (S_ISDIR(mode))
    return -EPERM;

  gchar **parent = g_strsplit(g_path_skip_root(path), G_DIR_SEPARATOR_S, -1);
  if (parent == NULL)
    err = -EFAULT;

  if (!err && find_object(path, NULL) == NULL) {
    err = -EEXIST;
  }
  else {
    int level = g_strv_length(parent);
    if (level == 1) {
      g_message("CREATE SCHEMA");
    }
    else
      if (level == 2)
	g_message("CREATE TABLE");
      else
	err = -ENOTSUP;

  }
  
  return err;
}

static int sqlfs_mknod(const char *path, mode_t mode, dev_t rdev)
{
  int err = 0;

  if ((mode & S_IFMT) != S_IFREG)
    return -EPERM;
  
  gchar **parent = g_strsplit(g_path_skip_root(path), G_DIR_SEPARATOR_S, -1);
  if (parent == NULL)
    err = -EFAULT;

  if (!err) {
    if (find_object(path, NULL) == NULL) {
      err = -EEXIST;
    }
    else {
      struct sqlfs_ms_obj *tobj = g_try_new0(struct sqlfs_ms_obj, 1);
      tobj->object_id = 0;
      tobj->name = g_path_get_basename(path);
      tobj->type = R_TEMP;
      
      g_mutex_lock(&cache.m);
      g_hash_table_insert(cache.temp_table, g_strdup(path), tobj);
      g_mutex_unlock(&cache.m);
      
      sqlfs_chmod(path, mode);
      
      err = 0;
    }
  }
  
  if (g_strv_length(parent) > 0) {
    g_strfreev(parent);
  }
  
  return err;
}

static int sqlfs_open(const char *path, struct fuse_file_info *fi)
{
  int err = 0;

  struct sqlfs_ms_obj *object = find_object(path, NULL);
  if (object == NULL) {
    err = -EIO;
  }
  
  if ((fi->flags & O_ACCMODE) == O_RDONLY) {
    if (fi->flags & O_CREAT) {
      err = sqlfs_mknod(path, 07777 | S_IFREG, 0);
    }
  } else if ((fi->flags & O_ACCMODE) == O_RDWR
	     || (fi->flags & O_ACCMODE) == O_WRONLY)
    {
      if (fi->flags & O_APPEND) {
	err = -ENOTSUP;
      }
      
      if ((fi->flags & O_EXCL) && object->object_id)
	  err = -EACCES;
    }
  else {
    err = -EIO;
  }

  if (!err) {
    fi->fh = object->object_id;
    gchar **schema = g_strsplit(g_path_skip_root(path), G_DIR_SEPARATOR_S, -1);
    if (schema == NULL)
      err = -EFAULT;

    char *def = NULL;
    if (object->object_id != 0) {
      GError *terr = NULL;
      def = load_module_text(*schema, object, &terr);
    }
    else {
      def = g_strdup("\0");
    }
    
    if (def != NULL) {
      sqlfs_file_t *fsfile = g_try_new0(sqlfs_file_t, 1);
      uint64_t *pfh = g_malloc0(sizeof(uint64_t));
      *pfh = fi->fh;
      if (def) {
	fsfile->buffer = def;
      }
      g_hash_table_insert(cache.open_table, pfh, fsfile);
    }

    g_strfreev(schema);
  }
  
  return err;
}

static int sqlfs_write(const char *path, const char *buf, size_t size,
		       off_t offset, struct fuse_file_info *fi)
{
  int err = 0;
  sqlfs_file_t *fsfile = g_hash_table_lookup(cache.open_table, &(fi->fh));
  if (fsfile && fsfile->buffer) {
    size_t len = strlen(fsfile->buffer);
    if (len < size + offset)
      fsfile->buffer = g_realloc_n(fsfile->buffer, size + offset + 1,
				   sizeof(gchar ));
    g_strlcpy(fsfile->buffer + offset, buf, size + 1);
    err = size;
    fsfile->flush = TRUE;
  }
  else
    err = -ENOENT;

  return err;
}

static int sqlfs_flush(const char *path, struct fuse_file_info *fi)
{
  int err = 0;
  sqlfs_file_t *fsfile = g_hash_table_lookup(cache.open_table, &(fi->fh));
  if (!fsfile)
    return -ENOENT;
  
  gchar **schema = g_strsplit(g_path_skip_root(path), G_DIR_SEPARATOR_S, -1);
  if (schema == NULL)
    err = -EFAULT;

  if (!err) {
    GError *terr = NULL;
    gchar *pp = g_path_get_dirname(path);
    struct sqlfs_ms_obj *pobj = find_object(pp, &terr),
      *obj = find_object(path, &terr);

    if (!obj)
      obj->name = g_path_get_basename(path);


    if (fsfile->flush == TRUE && strlen(fsfile->buffer) > 0) {
      write_ms_object(*schema, pobj, fsfile->buffer, obj, &terr);
      fsfile->flush = FALSE;
    }

    if (terr != NULL)
      err = -EFAULT;
    
    g_free(pp);
  }

  if (schema != NULL)
    g_strfreev(schema);
  
  return err;
}

static int sqlfs_release(const char *path, struct fuse_file_info *fi)
{
  int err = 0;

  sqlfs_file_t *fsfile = g_hash_table_lookup(cache.open_table, &(fi->fh));
  if (!fsfile)
    err = -ENOENT;

  g_hash_table_remove(cache.open_table, &(fi->fh));
  SAFE_REMOVE_ALL(path);  
  
  return err;
}

static int sqlfs_unlink(const char *path)
{
  int err = 0;

  gchar **schema = g_strsplit(g_path_skip_root(path), G_DIR_SEPARATOR_S, -1);
  if (schema == NULL)
    err = -EFAULT; 

  if (!err) {
    GError *terr = NULL;
    struct sqlfs_ms_obj *object = find_object(path, &terr);
    if (object != NULL && !object->object_id) {
	err = -ENOENT;
    }

    remove_ms_object(*schema, *(schema + 1), object, &terr);
    
    if (terr != NULL)
      err = -EFAULT;

    SAFE_REMOVE_ALL(path);
  }

  if (g_strv_length(schema) > 0) {
    g_strfreev(schema);
  }

  return err;
}


static int sqlfs_truncate(const char *path, off_t offset)
{
  int err = 0;

  if (offset == 0) {
    err = sqlfs_unlink(path);
    if (!err)
      err = sqlfs_mknod(path, 07777 | S_IFREG, 0);
  }
  else
    err = -EPERM;
  
  return err;
}

static int sqlfs_rename(const char *oldname, const char *newname)
{
  int err = 0;

  gchar **schemaold = g_strsplit(g_path_skip_root(oldname), G_DIR_SEPARATOR_S, -1);
  if (schemaold == NULL)
    err = -EFAULT;

  gchar **schemanew = g_strsplit(g_path_skip_root(newname), G_DIR_SEPARATOR_S, -1);
  if (schemanew == NULL)
    err = -EFAULT;

  if (!err) {
    GError *terr = NULL;
    gchar *ppold = g_path_get_dirname(oldname),
      *ppnew = g_path_get_dirname(newname);
    
    struct sqlfs_ms_obj
      *ppobj_old = find_object(ppold, &terr),
      *obj_old = find_object(oldname, &terr),
      *ppobj_new = find_object(ppnew, &terr),
      *obj_new = g_try_new0(struct sqlfs_ms_obj, 1);

    if (!obj_old)
      obj_old->name = g_path_get_basename(oldname);

    obj_new->name = g_path_get_basename(newname);

    if (!g_strcmp0(ppold, ppnew))
      rename_ms_object(*schemanew, ppobj_new, obj_old, obj_new, &terr);
    else
      err = -ENOTSUP;
    
    if (terr != NULL && err == 0)
      g_hash_table_remove(cache.cache_table, oldname);
  }
  
  return err;
}

static void free_sqlfs_file(gpointer pointer)
{
  sqlfs_file_t *fsfile = (sqlfs_file_t *) pointer;
  if (fsfile) {
    if (fsfile->buffer)
      g_free(fsfile->buffer);
    
    g_free(fsfile); 
  }
}

static struct fuse_operations sqlfs_oper = {
  .getattr = sqlfs_getattr,
  .readdir = sqlfs_readdir,
  .read = sqlfs_read,
  .open = sqlfs_open,
  .mkdir = sqlfs_mkdir,
  .mknod = sqlfs_mknod,
  .write = sqlfs_write,
  .rename = sqlfs_rename,
  .utime = sqlfs_utime,
  .chown = sqlfs_chown,
  .chmod = sqlfs_chmod,
  .unlink = sqlfs_unlink,
  .truncate = sqlfs_truncate,
  .flush = sqlfs_flush,
  .release = sqlfs_release,
};

static int sqlfs_fuse_main(struct fuse_args *args)
{
#if FUSE_VERSION >= 26
  return fuse_main(args->argc, args->argv, &sqlfs_oper, NULL);
#else
  return fuse_main(args->argc, args->argv, &sqlfs_oper);
#endif
}

int main (int argc, char **argv)
{
  int res;
  struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
  struct sqlprofile *profile = g_try_new0(struct sqlprofile, 1);
  g_mutex_init(&cache.m);
  cache.cache_table = g_hash_table_new_full(g_str_hash, g_str_equal,
				      g_free, free_ms_obj);
  cache.temp_table = g_hash_table_new_full(g_str_hash, g_str_equal,
				      g_free, free_ms_obj);
  cache.open_table = g_hash_table_new_full(g_int64_hash, g_int64_equal,
					   g_free, free_sqlfs_file);

  if (fuse_opt_parse(&args, profile, sqlfs_opts, sqlfs_opt_proc) == -1)
    exit(1);
    
  GError *terr = NULL;

  init_keyfile(profile->profile, &terr);
  if (terr != NULL)
    exit(1);
  
  init_msctx(&terr);
  if (terr != NULL) {
    exit(1);
  }
  
  g_free(profile->profile);
  g_free(profile);
  
  res = sqlfs_fuse_main(&args);
  fuse_opt_free_args(&args);

  close_msctx(&terr);
  g_mutex_clear(&cache.m);

  close_keyfile();
  
  if (terr != NULL) {
    exit(2);
  }
  
  return res;
}
