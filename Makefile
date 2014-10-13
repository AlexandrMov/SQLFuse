#  Copyright (C) 2013, 2014 Movsunov A.N.
#  
#  This file is part of SQLFuse
#
#  SQLFuse is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  SQLFuse is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with SQLFuse.  If not, see <http://www.gnu.org/licenses/>.

PROGRAM 	:= sqlfuse
CC		:= gcc
YACC		:= bison -dvl
LEX		:= flex -L
MODULES		:= .

SRC_FILES	:= main.c
OBJ_FILES	:= main.o

# KEYCONF
KC_PREFIX	:= ./conf/
KC_FILES	:= keyconf.c keyconf.h
KC_OBJS		:= keyconf.o
SRC_FILES	+= $(addprefix $(KC_PREFIX), $(KC_FILES))
OBJ_FILES	+= $(addprefix $(KC_PREFIX), $(KC_OBJS))
MODULES		+= conf

# MSSQL
MSSQL_PREFIX	:= ./mssql/
MSSQL_FILES	:= msctx.c tsqlcheck.c exec.c table.c util.c mssql.c
MSSQL_GEN_FILES	:= tsql.tab.c tsql.parser.c tsql.tab.h tsql.parser.h
MSSQL_OBJS	:= tsql.tab.o tsql.parser.o msctx.o tsqlcheck.o
MSSQL_OBJS	+= exec.o table.o util.o mssql.o
SRC_FILES	+= $(addprefix $(MSSQL_PREFIX), $(MSSQL_FILES))
OBJ_FILES	+= $(addprefix $(MSSQL_PREFIX), $(MSSQL_OBJS))
MODULES		+= mssql


CFLAGS 	+= -g -lsybdb $(shell pkg-config --cflags glib-2.0 fuse) -I.
LDFLAGS += -g
LIBS 	+= -lsybdb $(shell pkg-config --libs glib-2.0 fuse)

SRC_DIRS 	:= $(MODULES)
VPATH 		:= $(SRC_DIRS)

all: $(PROGRAM)

debug: CC += -DSQLDEBUG
debug: $(PROGRAM)

clean:
	rm -f $(PROGRAM)
	rm -f $(OBJ_FILES)
	rm -f $(addprefix $(MSSQL_PREFIX), $(MSSQL_GEN_FILES) *~)
	rm -f $(addprefix $(KC_PREFIX), $(KC_OBJS) *~)
	rm -f *~
	rm -f *.d *.output

$(PROGRAM): $(OBJ_FILES)
	$(CC) $^ -o $@ $(LIBS) -pipe

main.o: main.c
	$(CC) $< -c $(CFLAGS) $(LDFLAGS) $(LIBS) -MD

# MSSQL
mssql/tsql.%.o: tsql.%.c
	$(CC) $< -c -o $@ -pipe

mssql/tsql.parser.c: tsql.l
	$(LEX) --outfile=$(addprefix $(MSSQL_PREFIX), tsql.parser.c) --header-file=$(addprefix $(MSSQL_PREFIX), tsql.parser.h) $<

mssql/tsql.tab.c: tsql.y
	$(YACC) $<
	@mv tsql.tab.c $(addprefix $(MSSQL_PREFIX), tsql.tab.c)
	@mv tsql.tab.h $(addprefix $(MSSQL_PREFIX), tsql.tab.h)

