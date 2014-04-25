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

%token<sval> NAME STRING
%token<ival> INTNUM

%left '+' '-'
%left '*' '/'
%left COMPARISON

%token<ival> CREATE ALTER INDEX UNIQUE FUNCTION CONSTRAINT
%token<ival> CLUSTERED NONCLUSTERED TYPE VIEW SCHEMA TRIGGER
%token<ival> FILESTREAM COLLATE NULLX ROWGUIDCOL SPARSE
%token<ival> IDENTITY DOCUMENT CONTENT MAX FOR WITH_CHECK
%token<ival> ONX NOT_FOR_REPLICATION COLUMN NOT WITH_NOCHECK
%token<ival> PROCEDURE PROC DEFAULT CHECK WITH APPROXNUM
%token<ival> FOREIGN_KEY REFERENCES PRIMARY_KEY

%type<nodeobj> index_def obj_name data_type clust_idx_def

%start input

%%
input: column_def
        | proc_def
	| func_def
	| constraint_def index_def
{
	put_node(INDEX, $2->schema, $2->objname,
		 @2.first_column, @2.first_line,
	      	 @2.last_column, @2.last_line);
	YYACCEPT;
}
	| trg_def
	| view_def
	| schema_def
	| type_def
	| constraint_def default_def
	| constraint_def check_def
{
	put_node(CHECK, NULL, NULL,
		 @2.first_column, @2.first_line,
		 @2.last_column, @2.last_line);
	YYACCEPT;
}
	| constraint_def foreign_def
	| constraint_def primary_def
;

constraint_def: %empty
	| CONSTRAINT NAME
;

primary_def: PRIMARY_KEY
{
	put_node(PRIMARY_KEY, NULL, NULL,
		 @1.first_column, @1.first_line,
		 @1.last_column, @1.last_column);
	YYACCEPT;
}

foreign_def: FOREIGN_KEY '(' NAME ')'
	     REFERENCES obj_name '(' NAME ')'
{
	put_node(FOREIGN_KEY, NULL, $3,
		 @3.first_column, @3.first_line,
		 @3.last_column, @3.last_line);
	YYACCEPT;
}
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

index_def: UNIQUE clust_idx_def
{
	$$ = $2;
}
	| clust_idx_def
{
	$$ = $1;
}
;

clust_idx_def: INDEX NAME ONX obj_name
{
	$$ = $4;
}
	| NONCLUSTERED clust_idx_def
{
	$$ = $2;
}
;

mk_def: CREATE
	| ALTER
;

check_def: CHECK comparison_expr
{
	@$ = @2;
}
	| CHECK NOT_FOR_REPLICATION comparison_expr
{
	@$ = @3;
}
	| WITH_CHECK CONSTRAINT obj_name CHECK comparison_expr
{
	@$ = @5;
}
	| WITH_NOCHECK CONSTRAINT obj_name CHECK comparison_expr
{
	@$ = @5;
}
;


default_def: DEFAULT const_expr FOR obj_name
{
	put_node(DEFAULT, $4->schema, $4->objname,
		 @4.first_column, @4.first_line,
		 @4.last_column, @4.last_line);
	YYACCEPT;
}

comparison_expr: const_expr COMPARISON const_expr
		 | '(' comparison_expr ')'
;

const_expr: scalar_exp
	    | '(' const_expr ')'
;

atom: INTNUM
      | STRING
      | APPROXNUM
;

scalar_exp: scalar_exp '+' scalar_exp
	    | scalar_exp '-' scalar_exp
	    | scalar_exp '*' scalar_exp
	    | scalar_exp '/' scalar_exp
	    | '+' scalar_exp
	    | '-' scalar_exp
	    | atom
	    | obj_name
	    | func_def
;

func_def: obj_name '(' scalar_exp_commalist ')'
	  | obj_name '(' ')'
;

scalar_exp_commalist: scalar_exp
		| scalar_exp_commalist ',' scalar_exp
;

column_def: COLUMN obj_name data_type column_def_opt_list
{
	put_node(COLUMN, $3->schema, $3->objname,
		 @3.first_column, @3.first_line,
		 @3.last_column, @3.last_line);
		 
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

column_def_opt_list: %empty
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
