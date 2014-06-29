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

%{
  #include <stdio.h>
  #include <string.h>
  #include "tsqlcheck.h"
%}

%code requires{
      struct node_obj {
      	 char *dbname;
	 char *schema;
	 char *objname;
      };
}

%union {
       int ival;
       char *sval;
       struct node_obj nodeobj;
}

%{
  int yycolumn = 1;
  int yylex(void);

  void reset_column() {
    yycolumn = 1;
  }
%}

%locations

%token<sval> NAME STRING
%token<ival> INTNUM

%left '+' '-'
%left '*' '/'

%token<ival> CREATE ALTER INDEX UNIQUE FUNCTION CONSTRAINT
%token<ival> CLUSTERED NONCLUSTERED TYPE VIEW SCHEMA TRIGGER
%token<ival> FILESTREAM COLLATE NULLX ROWGUIDCOL SPARSE
%token<ival> IDENTITY MAX FOR WITH_CHECK
%token<ival> ONX NOT_FOR_REPLICATION COLUMN NOT WITH_NOCHECK
%token<ival> PROCEDURE PROC DEFAULT CHECK WITH APPROXNUM
%token<ival> FOREIGN_KEY PRIMARY_KEY

%type<nodeobj> index_def obj_name data_type clust_idx_def
%type<ival> mk_def check_def
%type<sval> var_def

%start input

%%
input: column_def
        | proc_def
	| func_def
	| index_def
{
	put_node(INDEX, $1.schema, $1.objname,
		 @1.first_column, @1.first_line,
	      	 @1.last_column, @1.last_line);
	YYACCEPT;
}
	| trg_def
	| view_def
	| schema_def
	| type_def
	| constraint_def default_def
	| with_check_def
	| constraint_def primary_def
	| constraint_def unique_def
;

constraint_def: CONSTRAINT var_def
;

primary_def: PRIMARY_KEY 
{
	put_node(PRIMARY_KEY, NULL, NULL,
		 @1.first_column, @1.first_line,
		 @1.last_column, @1.last_column);
	YYACCEPT;
}

unique_def: UNIQUE NONCLUSTERED
{
	put_node(UNIQUE, NULL, NULL,
		 @2.first_column, @2.first_line,
		 @2.last_column, @2.last_column);
	YYACCEPT;
}
;

proc_def: mk_def PROC obj_name
{
	put_module($1, @1.first_column, @1.first_line,
	      	   @1.last_column, @1.last_line,
		   PROC, $3.schema, $3.objname,
		   @3.first_column, @3.first_line,
	      	   @3.last_column, @3.last_line);
	YYACCEPT;
}
;

func_def: mk_def FUNCTION obj_name
{
	put_module($1, @1.first_column, @1.first_line,
	      	   @1.last_column, @1.last_line,
		   FUNCTION, $3.schema, $3.objname,
		   @3.first_column, @3.first_line,
	      	   @3.last_column, @3.last_line);
	YYACCEPT;
}
;

trg_def: mk_def TRIGGER obj_name ONX obj_name
{
	put_module($1, @1.first_column, @1.first_line,
	      	   @1.last_column, @1.last_line,
		   TRIGGER, $3.schema, $3.objname,
		   @3.first_column, @3.first_line,
	      	   @5.last_column, @5.last_line);
	YYACCEPT;
}
;

view_def: mk_def VIEW obj_name
{
	put_module($1, @1.first_column, @1.first_line,
	      	   @1.last_column, @1.last_line,
		   VIEW, $3.schema, $3.objname,
		   @3.first_column, @3.first_line,
	      	   @3.last_column, @3.last_line);
	YYACCEPT;
}
;

schema_def: mk_def SCHEMA var_def
{
	put_node(SCHEMA, NULL, $3,
		 @3.first_column, @3.first_line,
	      	 @3.last_column, @3.last_line);
	YYACCEPT;
}
;

type_def: mk_def TYPE obj_name
{
	put_node(TYPE, $3.schema, $3.objname,
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

clust_idx_def: INDEX var_def ONX obj_name
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

check_def: constraint_def CHECK
{
	@$ = @2;
	$$ = CHECK;
}
	| constraint_def FOREIGN_KEY
{
	@$ = @2;
	$$ = FOREIGN_KEY;
}
;

with_check_def: check_def
{
	put_check(WITH_CHECK, $1, @1.first_column, @1.first_line,
	      	  @1.last_column, @1.last_line);
	YYACCEPT;
}
	| WITH_CHECK check_def
{
	put_check(WITH_CHECK, $2, @2.first_column, @2.first_line,
	      	  @2.last_column, @2.last_line);
	YYACCEPT;
}
	| WITH_NOCHECK check_def
{
	put_check(WITH_NOCHECK, $2, @2.first_column, @2.first_line,
	      	  @2.last_column, @2.last_line);
	YYACCEPT;
}
;

default_def: DEFAULT const_expr FOR obj_name
{
	put_default($4.schema, $4.objname,
		    @2.first_column, @2.first_line,
		    @2.last_column, @2.last_line);
	YYACCEPT;
}

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
	put_column($3.schema, $3.objname,
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

obj_name: var_def
{
	$$.objname = $1;
}
	| var_def '.' var_def
{
	$$.schema = $1;
	$$.objname = $3;
	@$.first_column = @1.first_column;
	@$.first_line = @1.first_line;
	@$.last_column = @3.last_column;
	@$.last_line = @3.last_line;
}	
	| var_def '.' var_def '.' var_def
{
	$$.dbname = $1;
	$$.schema = $3;
	$$.objname = $5;
	@$.first_column = @1.first_column;
	@$.first_line = @1.first_line;
	@$.last_column = @5.last_column;
	@$.last_line = @5.last_line;
}
;

var_def: NAME
	| '[' NAME ']'
{
	$$ = $2;
	@$.first_column = @1.first_column;
	@$.first_line = @1.first_line;
	@$.last_column = @3.first_column;
	@$.last_line = @3.last_line;
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
