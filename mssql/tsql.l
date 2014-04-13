%{
#include <stdio.h>
#include <string.h>

#include "tsql.tab.h"

void yyerror(char *s);

%}
%%

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

[0-9]+		{ yylval.ival = atoi(yytext); return INTNUM; }

[-+*/(),.;]	return yytext[0];
"--".*$		{
		/* skip comment */
		}
[A-Za-z][A-Za-z0-9_]*	{ yylval.sval = strdup(yytext); return NAME; }
\/\*([^*]|\*[^/])*\*+\/ ; /* skip comment */
'[^'\n]*$		{ yyerror("Unterminated string"); }
\n		       	;
[ \t\r]+	       	; /* skip whitespaces */
%%

void
yyerror(char *s)
{
	printf("%s at %s\n", s, yytext);
}
