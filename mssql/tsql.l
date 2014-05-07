%{
#include <stdio.h>
#include <string.h>

#include "tsql.tab.h"

int yycolumn = 1;

#define YY_USER_ACTION yylloc.first_line = yylloc.last_line = yylineno; \
    yylloc.first_column = yycolumn; yylloc.last_column = yycolumn + yyleng - 1; \
    yycolumn += yyleng;

void yyerror(char *s);
%}

%option yylineno

%%

COLUMN		{ return COLUMN; }
CONTENT		{ return CONTENT; }
DOCUMENT	{ return DOCUMENT; }
FILESTREAM	{ return FILESTREAM; }
COLLATE		{ return COLLATE; }
NOT		{ return NOT; }
NULL		{ return NULLX; }
MAX		{ return MAX; }
ROWGUIDCOL	{ return ROWGUIDCOL; }
SPARSE		{ return SPARSE; }
IDENTITY	{ return IDENTITY; }
FOR		{ return FOR; }
NOT[ \t\n]+FOR[ \t\n]+REPLICATION	{ return NOT_FOR_REPLICATION; }
WITH[ \t\n]+NOCHECK 			{ return WITH_NOCHECK; }
WITH[ \t\n]+CHECK   			{ return WITH_CHECK; }
PRIMARY[ \t\n]+KEY			{ return PRIMARY_KEY; }
FOREIGN[ \t\n]+KEY			{ return FOREIGN_KEY; }
CREATE		{ return CREATE;}
ALTER		{ return ALTER; }
PROC|PROCEDURE	{ return PROC; }
TRIGGER		{ return TRIGGER; }
VIEW		{ return VIEW; }
SCHEMA		{ return SCHEMA; }
TYPE		{ return TYPE; }
INDEX		{ return INDEX; }
UNIQUE		{ return UNIQUE; }
CLUSTERED	{ return CLUSTERED; }
NONCLUSTERED	{ return NONCLUSTERED; }
FUNCTION	{ return FUNCTION; }
ON		{ return ONX; }
DEFAULT		{ return DEFAULT; }
CHECK		{ return CHECK; }
WITH		{ return WITH; }
CONSTRAINT	{ return CONSTRAINT; }
REFERENCES	{ return REFERENCES; }

"="		|
"<>"		|
"<"		|
">"		|
"<="		|
">="		|
"!="		{ yylval.sval = strdup(yytext); return COMPARISON; }

[0-9]+ 		|
[0-9]+"."[0-9]*	|
"."[0-9]*		{ yylval.ival = atoi(yytext); return INTNUM; }

[0-9]+[eE][+-]?[0-9]+	      	|
[0-9]+"."[0-9]*[eE][+-]?[0-9]+ 	|
"."[0-9]*[eE][+-]?[0-9]+		{ return APPROXNUM; }

'[^'\n]*'	{ yylval.sval = strdup(yytext); return STRING; }
[-+*/(),.;]	return yytext[0];
"--".*$		{ yycolumn = 1; /* skip comment */ }
[A-Za-z][A-Za-z0-9_]*	{ yylval.sval = strdup(yytext); return NAME; }
\/\*([^*]|\*[^/])*\*+\/ ; /* skip comment */
'[^'\n]*$		{ yyerror("Unterminated string"); }
\n		       	{ yycolumn = 1; }
[ \t\r]+	       	; /* skip whitespaces */
<<EOF>>			{ yycolumn = 1; yyterminate(); }
%%

void
yyerror(char *s)
{
	printf("== FLEX: %s at %s\n", s, yytext);
}
