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

#ifndef MSSQLFS_H
#define MSSQLFS_H

#include <sybdb.h>
#include <sybfront.h>

#include <glib.h>

#include <sqlfuse.h>
#include "exec.h"

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

#define GRANT "G"
#define GRANTW "W"
#define REVOKE "R"
#define DENY "D"


struct sqlfs_ms_acl {
  char *state;
  char *perm;
  char *principals;
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
};

struct sqlfs_ms_constraint {
  union {
    char *column_name;
    int disabled;
  };
  int not4repl;
};

struct sqlfs_ms_module {
  char *def;
  int uses_ansi_nulls;
  int uses_database_collation;
  int is_recompiled;
};

struct sqlfs_ms_fk {
  int delact, updact;
  int not4repl, disabled;

  char *columns_def;
  char *ref_object_def;
  char *ref_columns_def;
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
  char *data_space;
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
    struct sqlfs_ms_constraint *clmn_ctrt;
    
    int is_disabled; //<! Trigger
  };
  
  char *def;
  unsigned int len;
  
  time_t ctime;
  time_t mtime;
};


/*
 * Инициализировать контекст
 */
void init_msctx(GError **error);

/*
 * Найти объект
 */
struct sqlfs_ms_obj * find_ms_object(struct sqlfs_ms_obj *parent,
				     const char *name, GError **error);

/*
 * Список объектов корневого уровня
 */
GList * fetch_schemas(const char *name, msctx_t *ctx, int astart,
		      GError **error);

/*
 * Список объектов уровня схемы
 */
GList * fetch_schema_obj(int schema_id, const char *name, msctx_t *ctx,
			 GError **error);

/*
 * Вернёт список объектов уровня таблицы
 */
GList * fetch_table_obj(int schema_id, int table_id, const char *name,
			msctx_t *ctx, GError **error);

/*
 * Список прав для объекта
 */
GList * fetch_acl(int class_id, int major_id, int minor_id,
		  msctx_t *ctx, GError **error);


/*
 * Загрузить полный программный текст модуля
 */
char * load_module_text(const char *parent, struct sqlfs_ms_obj *obj,
			GError **error);


/*
 * Создать/записать объект
 */
char * write_ms_object(const char *schema, struct sqlfs_ms_obj *parent,
		       const char *text, struct sqlfs_ms_obj *obj, GError **error);

/*
 * Переименовать/переместить объект
 */
char * rename_ms_object(const char *schema_old, const char *schema_new,
			struct sqlfs_ms_obj *obj_old, struct sqlfs_ms_obj *obj_new,
			struct sqlfs_ms_obj *parent, GError **error);

/*
 * Удалить объект
 */
char * remove_ms_object(const char *schema, const char *parent,
			struct sqlfs_ms_obj *obj, GError **error);

/*
 * Убрать за объектом
 */
void free_ms_obj(gpointer msobj);


/*
 * Закончить работу с контекстом
 */
void close_msctx(GError **error);

  
#endif
