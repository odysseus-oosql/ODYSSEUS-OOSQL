flex -t8 OQL.lex.l > lex.yy.c
bison -d OQL.yacc.y
move OQL.yacc.tab.c y.tab.c
move OQL.yacc.tab.h y.tab.h
copy y.tab.h ..\include
