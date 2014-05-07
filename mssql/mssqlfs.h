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
#define EENOTFOUND 0x221

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
  int max_len;
  int precision;
  int scale;
  int ansi;

  int identity;
  int not4repl;
  char *seed_val;
  char *inc_val;

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

struct sqlfs_ms_index {
  int type_id;
  int is_unique;
  int ignore_dup_key;
  int is_pk, has_filter;
  int is_unique_const;
  int fill_factor, is_padded;
  int is_disabled, is_hyp;
  int allow_rl, allow_pl;

  char *filter_def;
  char *columns_def;
  char *incl_columns_def;
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
    struct sqlfs_ms_index *index;
  };
  unsigned int len;
  time_t ctime;
  time_t mtime;
  time_t cached_time;
};


/*
 * Инициализировать контекст
 */
int init_msctx(struct sqlctx *ctx);

/*
 * Найти объект
 */
struct sqlfs_ms_obj * find_ms_object(const struct sqlfs_ms_obj *parent,
				     const char *name, GError **error);

/*
 * Список объектов корневого уровня
 */
GList * fetch_schemas(const char *name, GError **error);


/*
 * Список объектов уровня схемы
 */
GList * fetch_schema_obj(int schema_id, const char *name,
			 GError **error);

/*
 * Вернёт список объектов уровня таблицы
 */
GList * fetch_table_obj(int schema_id, int table_id, const char *name,
			GError **error);

/*
 * Загрузить полный программный текст модуля
 */
char * load_module_text(const char *parent, struct sqlfs_ms_obj *obj,
			GError **error);

/*
 * Создать/записать объект
 */
int write_ms_object(const char *schema, const struct sqlfs_ms_obj *parent,
		    const char *text, struct sqlfs_ms_obj *obj);

/*
 * Удалить объект
 */
int remove_ms_object(const char *schema, const char *parent,
		     struct sqlfs_ms_obj *obj);

/*
 * Убрать за объектом
 */
void free_ms_obj(gpointer msobj);

/*
 * Закончить работу с контекстом
 */
int close_msctx();

  
#endif
