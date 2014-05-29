PROGRAM 	:= sqlfuse
CC		:= gcc
YACC		:= bison -dvl
LEX		:= flex -L
MODULES		:= .

SRC_FILES	:= main.c
OBJ_FILES	:= main.o

# MSSQL
MSSQL_PREFIX	:= ./mssql/
MSSQL_FILES	:= mssqlfs.c tsqlcheck.c exec.c table.c util.c
MSSQL_GEN_FILES	:= tsql.tab.c tsql.parser.c tsql.tab.h tsql.parser.h
MSSQL_OBJS	:= tsql.tab.o tsql.parser.o mssqlfs.o tsqlcheck.o
MSSQL_OBJS	+= exec.o table.o util.o
SRC_FILES	+= $(addprefix $(MSSQL_PREFIX), $(MSSQL_FILES))
OBJ_FILES	+= $(addprefix $(MSSQL_PREFIX), $(MSSQL_OBJS))
MODULES		+= mssql


CFLAGS 	+= -g -lsybdb $(shell pkg-config --cflags glib-2.0 fuse)
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

# MSSQL
mssql/tsql.%.o: tsql.%.c
	$(CC) $< -c -o $@ -pipe

mssql/tsql.parser.c: tsql.l
	$(LEX) --outfile=$(addprefix $(MSSQL_PREFIX), tsql.parser.c) --header-file=$(addprefix $(MSSQL_PREFIX), tsql.parser.h) $<

mssql/tsql.tab.c: tsql.y
	$(YACC) $<
	@mv tsql.tab.c $(addprefix $(MSSQL_PREFIX), tsql.tab.c)
	@mv tsql.tab.h $(addprefix $(MSSQL_PREFIX), tsql.tab.h)

