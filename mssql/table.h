#include "mssqlfs.h"

/*
 * Вернёт SQL-запрос, создающий новое поле в таблице
 */
char * create_column_def(const char *schema, const char *table,
			 struct sqlfs_ms_obj *obj, const char *def);

char * make_column_def(const struct sqlfs_ms_obj *obj);

char * make_index_def(const struct sqlfs_ms_obj *obj);

/*
 * Вернёт SQL-запрос, создающий новое ограничение в таблице
 */
char * create_constr_def(const char *schema, const char *table,
			 struct sqlfs_ms_obj *obj, const char *def);

char * make_constraint_def(const struct sqlfs_ms_obj *obj, const char *def);

GList * fetch_columns(int table_id, const char *name, GError **err);

GList * fetch_modules(int table_id, const char *name, GError **err);

GList * fetch_indexes(int table_id, const char *name, GError **err);

GList * fetch_constraints(int table_id, const char *name, GError **err);
