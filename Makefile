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

SRC_FILES	:= main.c keyconf.c keyconf.h
OBJ_FILES	:= main.o keyconf.o

# MSSQL
MSSQL_PREFIX	:= ./mssql/
MSSQL_FILES	:= mssqlfs.c tsqlcheck.c exec.c table.c util.c
MSSQL_GEN_FILES	:= tsql.tab.c tsql.parser.c tsql.tab.h tsql.parser.h
MSSQL_OBJS	:= tsql.tab.o tsql.parser.o mssqlfs.o tsqlcheck.o
MSSQL_OBJS	+= exec.o table.o util.o
SRC_FILES	+= $(addprefix $(MSSQL_PREFIX), $(MSSQL_FILES))
OBJ_FILES	+= $(addprefix $(MSSQL_PREFIX), $(MSSQL_OBJS))
MODULES		+= mssql


CFLAGS 	+= -g -lsybdb $(shell pkg-config --cflags glib-2.0 fuse) -I`pwd`
LDFLAGS += -g
LIBS 	+= -lsybdb $(shell pkg-config --libs glib-2.0 fuse)

SRC_DIRS 	:= $(MODULES)
VPATH 		:= $(SRC_DIRS)

all: $(PROGRAM)

clean:
	rm -f $(PROGRAM)
	rm -f $(OBJ_FILES)
	rm -f $(addprefix $(MSSQL_PREFIX), $(MSSQL_GEN_FILES) *~)
	rm -f *~
	rm -f *.d *.output

$(PROGRAM): $(OBJ_FILES)
	$(CC) $^ -o $@ $(LIBS) -pipe

main.o: main.c
	$(CC) $< -c $(CFLAGS) $(LDFLAGS) $(LIBS) -MD

keyconf.o: keyconf.c
	$(CC) $< -c $(shell pkg-config --cflags glib-2.0)

# MSSQL
mssql/tsql.%.o: tsql.%.c
	$(CC) $< -c -o $@ -pipe

mssql/tsql.parser.c: tsql.l
	$(LEX) --outfile=$(addprefix $(MSSQL_PREFIX), tsql.parser.c) --header-file=$(addprefix $(MSSQL_PREFIX), tsql.parser.h) $<

mssql/tsql.tab.c: tsql.y
	$(YACC) $<
	@mv tsql.tab.c $(addprefix $(MSSQL_PREFIX), tsql.tab.c)
	@mv tsql.tab.h $(addprefix $(MSSQL_PREFIX), tsql.tab.h)

