#include "mssqlfs.h"

char * make_column_def(const struct sqlfs_ms_obj *obj);

char * make_index_def(const struct sqlfs_ms_obj *obj);

char * make_constraint_def(const struct sqlfs_ms_obj *obj);

GList * fetch_columns(int table_id, const char *name, GError **err);

GList * fetch_modules(int table_id, const char *name, GError **err);

GList * fetch_indexes(int table_id, const char *name, GError **err);

GList * fetch_constraints(int table_id, const char *name, GError **err);
