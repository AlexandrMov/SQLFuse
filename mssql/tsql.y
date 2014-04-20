%code top {
  #include <stdio.h>
  #include <string.h>

  #include "tsqlcheck.h"

  struct node_obj {
  	 char *dbname;
  	 char *schema;
	 char *objname;
  };
}

%union {
       int ival;
       char *sval;
       struct node_obj *nodeobj;
}

%{
  int wrapRet = 1;
  int yylex(void);
  int yywrap( void ) {
     return wrapRet;
  }  
%}
%locations

%token<sval> NAME
%token<ival> INTNUM

%left NOT

%token<ival> CREATE ALTER INDEX UNIQUE FUNCTION
%token<ival> CLUSTERED NONCLUSTERED TYPE VIEW SCHEMA TRIGGER
%token<ival> FILESTREAM COLLATE NULLX ROWGUIDCOL SPARSE
%token<ival> IDENTITY DOCUMENT CONTENT MAX FOR
%token<ival> ONX NOT_FOR_REPLICATION COLUMN
%token<ival> PROCEDURE PROC

%type<nodeobj> index_def obj_name data_type clust_idx_def

%start input

%%
input: column_def
        | proc_def
	| func_def
	| index_def
{
	put_node(INDEX, $1->schema, $1->objname,
		 @1.first_column, @1.first_line,
	      	 @1.last_column, @1.last_line);
	YYACCEPT;
}
	| trg_def
	| view_def
	| schema_def
	| type_def
;

proc_def: mk_def PROC obj_name
{
	put_node(PROC, $3->schema, $3->objname,
		 @3.first_column, @3.first_line,
	      	 @3.last_column, @3.last_line);
	YYACCEPT;
}
;

func_def: mk_def FUNCTION obj_name
{
	put_node(FUNCTION, $3->schema, $3->objname,
		 @3.first_column, @3.first_line,
	      	 @3.last_column, @3.last_line);
	YYACCEPT;
}
;

trg_def: mk_def TRIGGER obj_name ONX obj_name
{
	put_node(TRIGGER, $3->schema, $3->objname,
		 @3.first_column, @3.first_line,
	      	 @3.last_column, @3.last_line);
	YYACCEPT;
}
;

view_def: mk_def VIEW obj_name
{
	put_node(VIEW, $3->schema, $3->objname,
		 @3.first_column, @3.first_line,
	      	 @3.last_column, @3.last_line);
	YYACCEPT;
}
;

schema_def: mk_def SCHEMA NAME
{
	put_node(SCHEMA, NULL, $3,
		 @3.first_column, @3.first_line,
	      	 @3.last_column, @3.last_line);
	YYACCEPT;
}
;

type_def: mk_def TYPE obj_name
{
	put_node(TYPE, $3->schema, $3->objname,
		 @3.first_column, @3.first_line,
	      	 @3.last_column, @3.last_line);
	YYACCEPT;
}
;

index_def: mk_def UNIQUE clust_idx_def
{
	$$ = $3;
}
	| mk_def clust_idx_def
{
	$$ = $2;
}
;

clust_idx_def: INDEX NAME ONX obj_name
{
	$$ = $4;
}
	| CLUSTERED clust_idx_def
{
	$$ = $2;
}
	| NONCLUSTERED clust_idx_def
{
	$$ = $2;
}
;

mk_def: CREATE
	| ALTER
;

column_def: COLUMN data_type column_def_opt_list
{
	put_node(COLUMN, $2->schema, $2->objname,
		 @2.first_column, @2.first_line,
	      	 @2.last_column, @2.last_line);
		 
	YYACCEPT;
}
;

data_type: obj_name
	| obj_name '(' INTNUM ')'
	| obj_name '(' MAX ')'
	| obj_name '(' INTNUM ',' INTNUM ')'
;

obj_name: NAME
{
	$$->objname = $1;
}
	| NAME '.' NAME
{
	$$->schema = $1;
	$$->objname = $3;
}	
	| NAME '.' NAME '.' NAME
{
	$$->dbname = $1;
	$$->schema = $3;
	$$->objname = $5;
}
;

column_def_opt_list: /* empty */
	| column_def_opt_list FILESTREAM
{
}
	| column_def_opt_list NOT NULLX
{
}
	| column_def_opt_list NULLX
{
}
	| column_def_opt_list identity_def
{
}
	| column_def_opt_list ROWGUIDCOL
	| column_def_opt_list SPARSE
;
identity_def: IDENTITY
	| identity_def '(' INTNUM ',' INTNUM ')'
	| identity_def NOT_FOR_REPLICATION
;
%%
