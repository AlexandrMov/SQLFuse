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

#include <sqlfuse.h>
#include <conf/keyconf.h>

struct sqlcache {
  GMutex m;
  GHashTable *open_table;
};

struct sqlprofile {
  char *profile;
  uid_t uid;
  gid_t gid;
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
#include <stdlib.h>

static struct sqlcache cache;
static struct sqlprofile *sqlprofile;

#define SQLFS_OPT(t, p, v) { t, offsetof(struct sqlprofile, p), v }


static struct fuse_opt sqlfs_opts[] = {
  SQLFS_OPT("profilename=%s", profile, 0),
  SQLFS_OPT("uid=%d", uid, 0),
  SQLFS_OPT("gid=%d", gid, 0),

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

static int sqlfs_getattr(const char *path, struct stat *stbuf)
{
  int err = 0;

  char *filter = get_context()->filter;
  if (filter != NULL && g_regex_match_simple(filter, path, 0, 0)) {
    return -ENOENT;
  }

  memset(stbuf, 0, sizeof(struct stat));
  
  if (g_strcmp0(path, G_DIR_SEPARATOR_S) == 0) {
    stbuf->st_mode = S_IFDIR | 0755;
    stbuf->st_nlink = 2;
  } else {
    GError *terr = NULL;
    struct sqlfs_object *object = find_object(path, &terr);
    if (terr != NULL) {
      err = -ENOENT;
    }
    else {
      if (object->type == SF_DIR) {
	stbuf->st_mode = S_IFDIR | 0755;
	stbuf->st_nlink = 2;
      } else {
	stbuf->st_mode = S_IFREG | 0666;
	stbuf->st_nlink = 1;
	stbuf->st_size = object->len;
      }
      stbuf->st_mtime = object->mtime;
      stbuf->st_ino = object->object_id;
      
      if (object != NULL)
	free_sqlfs_object(object);
    }
    
    if (terr != NULL)
      g_error_free(terr);
  }

  stbuf->st_uid = sqlprofile->uid;
  stbuf->st_gid = sqlprofile->gid;

  return err;
}

static int sqlfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
			 off_t offset, struct fuse_file_info *fi)
{
  filler(buf, ".", NULL, 0);
  filler(buf, "..", NULL, 0);

  GError *terr = NULL;
  int err = 0;

  GList *wrk = fetch_dir_objects(path, &terr);
  if (terr == NULL && wrk) {
    wrk = g_list_first(wrk);
    while (wrk) {
      struct sqlfs_object *object = wrk->data;
      filler(buf, object->name, NULL, 0);
      wrk = g_list_next(wrk);
    }
    wrk = g_list_first(wrk);
    g_list_free_full(wrk, &free_sqlfs_object);
  }
  else
    if (terr != NULL && terr->code == EERES) {
      err = -ECONNABORTED;
    }
  
  if (terr != NULL)
    g_error_free(terr);
  
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
  
  GError *terr = NULL;
  
  create_dir(path, &terr);
  
  if (terr != NULL) {
    if (terr->code == EENOTSUP)
      err = -ENOTSUP;
    else
      err = -EFAULT;
  }
  
  if (terr != NULL)
    g_error_free(terr);

  return err;
}

static int sqlfs_rmdir(const char *path)
{
  int err = 0;
  GError *terr = NULL;

  remove_object(path, &terr);

  if (terr != NULL)
    err = -EFAULT;

  if (terr != NULL)
    g_error_free(terr);

  return err;
}

static int sqlfs_mknod(const char *path, mode_t mode, dev_t rdev)
{
  int err = 0;

  if ((mode & S_IFMT) != S_IFREG)
    return -EPERM;

  GError *terr = NULL;
  create_node(path, &terr);

  sqlfs_chmod(path, mode);

  if (terr != NULL)
    err = -EFAULT;

  if (terr != NULL)
    g_error_free(terr);
    
  return err;
}

static int sqlfs_open(const char *path, struct fuse_file_info *fi)
{
  int err = 0;

  GError *terr = NULL;
  struct sqlfs_object *object = find_object(path, &terr);
  if (terr != NULL && terr->code == EENOTFOUND) {
    err = -ENOENT;
  }
  
  if ((fi->flags & O_ACCMODE) == O_RDONLY) {
    if (fi->flags & O_CREAT) {
      err = sqlfs_mknod(path, 07777 | S_IFREG, 0);
    }
  } else if ((fi->flags & O_ACCMODE) == O_RDWR
	     || (fi->flags & O_ACCMODE) == O_WRONLY)
    {
      /*if (fi->flags & O_APPEND) {
	err = -ENOTSUP;
	}*/
      
      if ((fi->flags & O_EXCL) && object->object_id)
	  err = -EACCES;
    }
  else {
    err = -EIO;
  }

  if (!err) {
    fi->fh = object->object_id;

    char *def = fetch_object_text(path, &terr);
    if (def != NULL) {
      sqlfs_file_t *fsfile = g_try_new0(sqlfs_file_t, 1);
      uint64_t *pfh = g_malloc0(sizeof(uint64_t));
      *pfh = fi->fh;
      if (def) {
	fsfile->buffer = g_strdup(def);
      }
      g_hash_table_insert(cache.open_table, pfh, fsfile);
    }

    if (object != NULL)
      free_sqlfs_object(object);
  }

  if (terr != NULL)
    g_error_free(terr);
  
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
    return err;

  if (!err) {
    GError *terr = NULL;

    if (fsfile->flush == TRUE && strlen(fsfile->buffer) > 0) {
      write_object(path, fsfile->buffer, &terr);
      fsfile->flush = FALSE;
    }

    if (terr != NULL)
      err = -EFAULT;

    if (terr != NULL)
      g_error_free(terr);
  }

  return err;
}

static int sqlfs_release(const char *path, struct fuse_file_info *fi)
{
  int err = 0;

  if (g_hash_table_contains(cache.open_table, &(fi->fh)))
    g_hash_table_remove(cache.open_table, &(fi->fh));

  return err;
}

static int sqlfs_unlink(const char *path)
{
  int err = 0;
  GError *terr = NULL;
  remove_object(path, &terr);
  if (terr != NULL) {
    if (terr->code == EENOTFOUND) {
      err = -ENOENT;
    }
    else {
      err = -EFAULT;
    }
  }
  
  if (terr != NULL)
    g_error_free(terr);
  
  return err;
}


static int sqlfs_truncate(const char *path, off_t offset)
{
  int err = 0;
  GError *terr = NULL;

  truncate_object(path, offset, &terr);
  
  if (terr != NULL) {
    if (terr->code == EENOTFOUND) {
      err = -ENOENT;
    }
    else {
      err = -EFAULT;
    }
  }
  
  if (terr != NULL)
    g_error_free(terr);
  
  return err;
}


static int sqlfs_rename(const char *oldname, const char *newname)
{
  GError *terr = NULL;
  int err = 0;
  
  rename_object(oldname, newname, &terr);
  if (terr != NULL) {
    switch(terr->code) {
    case EENOTFOUND:
      err = -ENOENT;
      break;
    case EENOTSUP:
      err = -ENOTSUP;
      break;
    default:
      err = -EFAULT;
    }
  }
 
  if (terr != NULL)
    g_error_free(terr);
  
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
  .rmdir = sqlfs_rmdir,
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
  int res = 0;
  struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
  sqlprofile = g_try_new0(struct sqlprofile, 1);
  g_mutex_init(&cache.m);
  
  cache.open_table = g_hash_table_new_full(g_int64_hash, g_int64_equal,
					   g_free, free_sqlfs_file);

  if (fuse_opt_parse(&args, sqlprofile, sqlfs_opts, sqlfs_opt_proc) == -1)
    res = 1;

  if (!res) {
    GError *terr = NULL;
    
    init_keyfile(sqlprofile->profile, &terr);
    if (terr != NULL)
      res = 1;
    
    if (!res) {
      init_cache(&terr);
      
      if (terr != NULL)
	res = 2;
    }

    if (!res) {
      res = sqlfs_fuse_main(&args);
      fuse_opt_free_args(&args);

      if (terr != NULL)
	res = 3;
    }

    if (!res || res > 2)
      destroy_cache(&terr);

    if (!res || res > 1)
      close_keyfile();

    if (sqlprofile != NULL) {
      if (sqlprofile->profile != NULL)
	g_free(sqlprofile->profile);

      g_free(sqlprofile);
    }


    if (terr != NULL)
      g_error_free(terr);
  }

  g_hash_table_destroy(cache.open_table);
  
  g_mutex_clear(&cache.m);
  
  return res;
}
