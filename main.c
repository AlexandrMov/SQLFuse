#include "mssql/mssqlfs.h"

struct sqlcache {
  GMutex m;
  GHashTable *cache_table, *temp_table, *open_table;
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

struct sqlctx *msctx;
static struct sqlcache cache;

#define SQLFS_OPT(t, p, v) { t, offsetof(struct sqlctx, p), v }

#define SAFE_REMOVE_ALL(p) g_mutex_lock(&cache.m); \
  g_hash_table_remove(cache.cache_table, p);	   \
  g_hash_table_remove(cache.temp_table, p);	   \
  g_mutex_unlock(&cache.m);

static struct fuse_opt sqlfs_opts[] = {
  SQLFS_OPT("servername=%s", servername, 0),
  SQLFS_OPT("username=%s", username, 0),
  SQLFS_OPT("password=%s", password, 0),
  SQLFS_OPT("dbname=%s", dbname, 0),
  SQLFS_OPT("maxconn=%d", maxconn, 1),

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

static int find_object(const char *pathname, struct sqlfs_ms_obj **object)
{
  if (!g_path_is_absolute(pathname))
    return 1;

  GList *list = NULL;
  
  const gchar * pn = g_path_skip_root(pathname);
  if (pn == NULL)
    pn = pathname;
  
  gchar **tree = g_strsplit(pn, G_DIR_SEPARATOR_S, -1);
  if (tree == NULL)
    return 0;

  int err = 0;
  GString * path = g_string_new(NULL);
  guint i = 0;
  
  struct sqlfs_ms_obj *obj = g_hash_table_lookup(cache.cache_table, pathname);
  if (!obj)
    obj = g_hash_table_lookup(cache.temp_table, pathname);
  
  if (!obj) {
    
    while(*tree && !err) {
      g_string_append_printf(path, "%s%s", G_DIR_SEPARATOR_S, *tree);
      err = 0;
      obj = g_hash_table_lookup(cache.cache_table, path->str);
      
      if (!obj) {
	obj = g_try_new0(struct sqlfs_ms_obj, 1);
	if (i == 0) {
	  err = find_ms_object(NULL, *tree, obj);
	}
	if (i > 0) {
	  err = find_ms_object(g_list_last(list)->data, *tree, obj);
	}
      }
      
      if (!err && obj && obj->name) {

	list = g_list_append(list, obj);
	if (!g_hash_table_contains(cache.cache_table, path->str)) {
	  g_mutex_lock(&cache.m);
	  g_hash_table_insert(cache.cache_table, g_strdup(path->str), obj);
	  g_mutex_unlock(&cache.m);
	}

      } else {
	free_ms_obj(obj);
	err = EERES;
      }
      tree++;
      i++;
    }
    
    GList *last = g_list_last(list);
    if (last)
      obj = last->data;
    
    g_list_free(list);
    
  }

  if (!err)
    *object = obj;

  tree -= i;
  g_strfreev(tree);
  g_string_free(path, TRUE);
  
  return err;
}

static int sqlfs_getattr(const char *path, struct stat *stbuf)
{
  int err = 0;
  memset(stbuf, 0, sizeof(struct stat));
  if (g_strcmp0(path, G_DIR_SEPARATOR_S) == 0) {
    stbuf->st_mode = S_IFDIR | 0755;
    stbuf->st_nlink = 2;
  } else {
    struct sqlfs_ms_obj *object = NULL;
    err = find_object(path, &object);
    if (err)
      err = -ENOENT;

    if (!err) {
      if (object->type < 0x08) {
	stbuf->st_mode = S_IFDIR | 0755;
	stbuf->st_nlink = 2;
      } else {
	stbuf->st_mode = S_IFREG | 0666;
	stbuf->st_nlink = 1;
	stbuf->st_size = object->len;
      }
      stbuf->st_mtime = object->mtime;
    }
  }

  return err;
}

static int sqlfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
			 off_t offset, struct fuse_file_info *fi)
{
  filler(buf, ".", NULL, 0);
  filler(buf, "..", NULL, 0);

  int err = 0;
  int nschema = g_strcmp0(path, G_DIR_SEPARATOR_S);
  GList *wrk = NULL;
  struct sqlfs_ms_obj *object = NULL;
  if (!nschema) {
    err = load_schemas(&wrk);
  } else {
    err = find_object(path, &object);
    if (!err && !object->name)
      return -ENOENT;

    if (object->type == D_SCHEMA) 
      err = load_schema_obj(object, &wrk);
    else if (object->type == D_U || object->type == D_V) {
      err = load_table_obj(object, &wrk);
    }
    else
      return -ENOENT;
  }

  if (!err) {
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
  else if (err == EERES)
    err = -ECONNABORTED;
  
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

static int sqlfs_mknod(const char *path, mode_t mode, dev_t rdev)
{
  int err = 0;

  if ((mode & S_IFMT) != S_IFREG)
    return -EPERM;
  
  gchar **parent = g_strsplit(g_path_skip_root(path), G_DIR_SEPARATOR_S, -1);
  if (parent == NULL)
    err = -EFAULT;

  if (!err) {
    struct sqlfs_ms_obj *object = NULL;
    err = find_object(path, &object);
    if (!err)
      err = -EEXIST;

    if (err > 0) {
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
    g_free(object);
  }

  if (g_strv_length(parent) > 0) {
    g_strfreev(parent);
  }

  return err;
}

static int sqlfs_open(const char *path, struct fuse_file_info *fi)
{
  int err = 0;

  struct sqlfs_ms_obj *object = NULL;
  err = find_object(path, &object);
  if (err) {
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
    if (!err && !load_module_text(*schema, object, &def)) {
      sqlfs_file_t *fsfile = g_try_new0(sqlfs_file_t, 1);
      uint64_t *pfh = g_malloc0(sizeof(uint64_t));
      *pfh = fi->fh;
      if (def) {
	fsfile->buffer = g_strdup(def);
      }
      g_hash_table_insert(cache.open_table, pfh, fsfile);
      g_free(def);
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
				   sizeof(gchar));
    g_strlcpy(fsfile->buffer + offset, buf, size + 1);
    fsfile->flush = 1;
    err = size;
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
    err = -ENOENT;
  
  if (!err && !fsfile->flush) {
    g_hash_table_remove(cache.open_table, &(fi->fh));
    return 0;
  }
    
  gchar **schema = g_strsplit(g_path_skip_root(path), G_DIR_SEPARATOR_S, -1);
  if (schema == NULL)
    err = -EFAULT;

  if (!err) {
    struct sqlfs_ms_obj *pobj = NULL;
    gchar *pp = g_path_get_dirname(path);
    err = find_object(pp, &pobj);

    struct sqlfs_ms_obj *obj = NULL;
    err = find_object(path, &obj);
    if (!obj)
      obj->name = g_path_get_basename(path);

    if (!fsfile->buffer)
      err = -EFAULT;
    else {
      err = write_ms_object(*schema, pobj, fsfile->buffer, obj);
      SAFE_REMOVE_ALL(path);
    }
    
    g_free(pp);
  }

  g_hash_table_remove(cache.open_table, &(fi->fh));
  g_strfreev(schema);
  
  return err;
}

static int sqlfs_unlink(const char *path)
{
  int err = 0;

  gchar **schema = g_strsplit(g_path_skip_root(path), G_DIR_SEPARATOR_S, -1);
  if (schema == NULL)
    err = -EFAULT;

  if (!err) {
    struct sqlfs_ms_obj *object = NULL;
    err = find_object(path, &object);
    if (err) {
      err = -EIO;
    }

    if (!err && !object->object_id)
      err = -ENOENT;

    if (remove_ms_object(*schema, *(schema + 1), object))
      err = -EIO;

    if (!err) {
      SAFE_REMOVE_ALL(path);
    }
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

static void free_sqlfs_file(gpointer pointer)
{
  sqlfs_file_t *fsfile = (sqlfs_file_t *) pointer;
  g_free(fsfile->buffer);
  g_free(fsfile);
}

static struct fuse_operations sqlfs_oper = {
  .getattr = sqlfs_getattr,
  .readdir = sqlfs_readdir,
  .read = sqlfs_read,
  .open = sqlfs_open,
  .mknod = sqlfs_mknod,
  .write = sqlfs_write,
  .utime = sqlfs_utime,
  .chown = sqlfs_chown,
  .chmod = sqlfs_chmod,
  .unlink = sqlfs_unlink,
  .truncate = sqlfs_truncate,
  .flush = sqlfs_flush,
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
  msctx = g_try_new0(struct sqlctx, 1);
  g_mutex_init(&cache.m);
  cache.cache_table = g_hash_table_new_full(g_str_hash, g_str_equal,
				      g_free, free_ms_obj);
  cache.temp_table = g_hash_table_new_full(g_str_hash, g_str_equal,
				      g_free, free_ms_obj);
  cache.open_table = g_hash_table_new_full(g_int64_hash, g_int64_equal,
					   g_free, free_sqlfs_file);
  msctx->appname = g_strdup("sqlfs");

  if (fuse_opt_parse(&args, msctx, sqlfs_opts, sqlfs_opt_proc) == -1)
    exit(1);

  if (init_msctx(msctx)) {
    exit(1);
  }
  g_free(msctx->appname);
  g_free(msctx);
  
  res = sqlfs_fuse_main(&args);
  fuse_opt_free_args(&args);

  close_msctx();
  g_mutex_clear(&cache.m);
  
  return res;
}
