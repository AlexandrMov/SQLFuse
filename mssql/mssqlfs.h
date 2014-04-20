#ifndef MSSQLFS_H
#define MSSQLFS_H

#include <stdlib.h>

#include <sybdb.h>
#include <sybfront.h>

#include <glib.h>

#define D_SCHEMA 0x01
#define D_IT 0x02
#define D_S  0x03
#define D_TT 0x04
#define D_U 0x05
#define D_V 0x06

#define R_AF 0x20
#define R_C 0x21
#define R_D 0x22
#define R_FN 0x23
#define R_FS 0x24
#define R_IF 0x25
#define R_P 0x27
#define R_PC 0x28
#define R_PG 0x29
#define R_PK 0x2A
#define R_F 0x2B
#define R_RF 0x2C
#define R_SN 0x2D
#define R_SO 0x2E
#define R_SQ 0x2F
#define R_TA 0x30
#define R_TF 0x31
#define R_TR 0x32
#define R_UQ 0x33
#define R_X 0x34
#define R_FT 0x35
#define R_COL 0x40
#define R_TYPE 0x41

#define D_TEMP 0x11
#define R_TEMP 0x99

#define EENULL 0x101
#define EENOTSUP 0x102
#define EEMEM 0x109
#define EELOGIN 0x110
#define EECONN 0x111
#define EEUSE 0x112
#define EEINIT 0x113
#define EEBUSY 0x114
#define EECMD 0x121
#define EEXEC 0x122
#define EERES 0x123

struct sqlctx {
  char *appname;
  char *servername;
  char *dbname;
  char *username;
  char *password;
  int maxconn;
  int debug;
};

struct sqlfs_ms_type {
  int sys_type_id;
  int user_type_id;
  int max_length;
  int precision;
  int scale;
  char *collation_name;
  int nullable;
  int table_type;
};

struct sqlfs_ms_column {
  int column_id;
  int systype;
  char *type_name;
  int nullable;
  int identity;
  int max_len;
  int precision;
  int scale;
  int ansi;
  char *def;
};

struct sqlfs_ms_module {
  char *def;
  int uses_ansi_nulls;
  int uses_database_collation;
  int is_recompiled;
};

struct sqlfs_ms_fk {
  int delact;
  int updact;
};

struct sqlfs_ms_obj {
  int object_id;
  int parent_id;
  int schema_id;
  char *name;
  unsigned int type;
  union {
    struct sqlfs_ms_module *sql_module;
    struct sqlfs_ms_column *column;
    struct sqlfs_ms_fk *foreign_ctrt;
    struct sqlfs_ms_type *mstype;
  };
  unsigned int len;
  time_t ctime;
  time_t mtime;
  time_t cached_time;
};

int init_msctx(struct sqlctx *ctx);

int find_ms_object(const struct sqlfs_ms_obj *parent,
		   const char *name, struct sqlfs_ms_obj **obj);

int load_schemas(GList **obj_list);

int load_schema_obj(const struct sqlfs_ms_obj *sch, GList **obj_list);

int load_table_obj(const struct sqlfs_ms_obj *tbl, GList **obj_list);

int load_module_text(const char *parent, struct sqlfs_ms_obj *obj, char **text);

int write_ms_object(const char *schema, const struct sqlfs_ms_obj *parent,
		    const char *text, struct sqlfs_ms_obj *obj);

int remove_ms_object(const char *schema, const char *parent,
		     struct sqlfs_ms_obj *obj);

void free_ms_obj(gpointer msobj);

int close_msctx();
  
#endif
