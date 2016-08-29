%{

/*
 * LEX specification for ODMG-93 OQL
 *
 * LOG:
 */

#include "OQL.lex.h"
#include "OQL_AST.h"
#include <malloc.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#define SINGLEQUOTE 0
#define DOUBLEQUOTE 1

int yywrap() { return(1); }     /* return 1 to report the end of file */

int   LINE_NUM = 1;
int   COLM_NUM = 0;
int   NEW_COLM_NUM = 0;
char* LINE;
int   LINE_COLM_NUM = 0;
int   LINE_SIZE = 2000;

#undef  YY_INPUT
#define YY_INPUT(b, r, ms) (r = oql_yyinput(b, ms))

%}

newline         [\n]
space           [ \t]
letter          [A-Za-z\_]
digit           [0-9] 
identifier      [A-Za-z\_]([A-Za-z\_]|[0-9])*
bind_arg        [\$]([0-9]+)[icrsko]
integer         [+\-]?[0-9]+
real            [+\-]?[0-9]+(\.[0-9]+)?((E|e)[+\-]?[0-9]+)?
sqString		[\']([^\']*(\'\')?[^\']*)+[\']
dqString		[\"]([^\"]*(\"\")?[^\"]*)+[\"]


%% 

{newline}       { puttoken(); putline(); }
{space}         { puttoken(); }
"("             { puttoken(); return(L_PAR);}
")"             { puttoken(); return(R_PAR);}
"["             { puttoken(); return(L_BRACKET);}
"]"             { puttoken(); return(R_BRACKET);}
"{"				{ puttoken(); return(L_BRACE);}
"}"				{ puttoken(); return(R_BRACE);}
"+"             { puttoken(); return(PLUS); }
"-"             { puttoken(); return(MINUS); }
"*"             { puttoken(); return(ASTERISK); }
"#"             { puttoken(); return(SHARP); }
"/"             { puttoken(); return(SLASH); }
"|"             { puttoken(); return(VERT_BAR); }
"||"            { puttoken(); return(STR_CAT); }
"="             { puttoken(); return(EQ); }
"!="            { puttoken(); return(NE); }
"<>"            { puttoken(); return(NE); }
">"             { puttoken(); return(GT); }
"<"             { puttoken(); return(LT); }
">="            { puttoken(); return(GE); }
"<="            { puttoken(); return(LE); }
"."             { puttoken(); return(DOT); } 
","             { puttoken(); return(COMMA); } 
".."            { puttoken(); return(RANGE); }
"->"            { puttoken(); return(R_ARROW); } 
":"             { puttoken(); return(COLON); } 
";"             { puttoken(); return(SEMICOLON); } 
"&"             { puttoken(); return(AMPER); }
"~"             { puttoken(); return(TILD); }
"^"             { puttoken(); return(CAROT); }

between			{ puttoken(); return(BETWEEN); }
BETWEEN			{ puttoken(); return(BETWEEN); }
OBJECT			{ puttoken(); return(OBJECT); }
object			{ puttoken(); return(OBJECT); }
IMMEDIATE		{ puttoken(); return(IMMEDIATE); }
immediate		{ puttoken(); return(IMMEDIATE); }
DEFERRED		{ puttoken(); return(DEFERRED); }
deferred		{ puttoken(); return(DEFERRED); }
TEXT			{ puttoken(); return(TEXT); }
text			{ puttoken(); return(TEXT); }
"?"				{ puttoken(); return(QUESTIONVALUE); }
TEMPORARY		{ puttoken(); return(TEMPORARY); }
temporary		{ puttoken(); return(TEMPORARY); }
NULL			{ puttoken(); return(NULLVALUE); }
null			{ puttoken(); return(NULLVALUE); }
CREATE			{ puttoken(); return(CREATE); }
create			{ puttoken(); return(CREATE); }
ALTER			{ puttoken(); return(ALTER); }
alter			{ puttoken(); return(ALTER); }
CLASS			{ puttoken(); return(CLASS); }
class			{ puttoken(); return(CLASS); }
SUBCLASS		{ puttoken(); return(SUBCLASS); }
subclass		{ puttoken(); return(SUBCLASS); }
SEQUENCE		{ puttoken(); return(SEQUENCE); }
sequence		{ puttoken(); return(SEQUENCE); }
START			{ puttoken(); return(START); }
start			{ puttoken(); return(START); }
WITH			{ puttoken(); return(WITH); }
with			{ puttoken(); return(WITH); }
.CURRVAL		{ puttoken(); return(DOT_CURRVAL); }
.currval		{ puttoken(); return(DOT_CURRVAL); }
.NEXTVAL		{ puttoken(); return(DOT_NEXTVAL); }
.nextval		{ puttoken(); return(DOT_NEXTVAL); }
OF				{ puttoken(); return(OF); }
of				{ puttoken(); return(OF); }
IS				{ puttoken(); return(IS); }
is				{ puttoken(); return(IS); }
UNDER			{ puttoken(); return(UNDER); }
under			{ puttoken(); return(UNDER); }
TABLE			{ puttoken(); return(TABLE); }
table			{ puttoken(); return(TABLE); }
PRIMARY			{ puttoken(); return(PRIMARY); }
primary			{ puttoken(); return(PRIMARY); }
INDEX			{ puttoken(); return(INDEX); }
index			{ puttoken(); return(INDEX); }
ON				{ puttoken(); return(ON); }
on				{ puttoken(); return(ON); }
CLUSTER			{ puttoken(); return(CLUSTER); }
cluster			{ puttoken(); return(CLUSTER); }
DROP			{ puttoken(); return(DROP); }
drop			{ puttoken(); return(DROP); }
UPDATE			{ puttoken(); return(UPDATE); }
update			{ puttoken(); return(UPDATE); }
INSERT			{ puttoken(); return(INSERT); }
insert			{ puttoken(); return(INSERT); }
INTO			{ puttoken(); return(INTO); }
into			{ puttoken(); return(INTO); }
VALUES			{ puttoken(); return(VALUES); }
values			{ puttoken(); return(VALUES); }
DELETE			{ puttoken(); return(DELETE); }
delete			{ puttoken(); return(DELETE); }
DEFINE          { puttoken(); return(DEFINE); } 
define          { puttoken(); return(DEFINE); } 
AS              { puttoken(); return(AS); }
as              { puttoken(); return(AS); }
COLUMN			{ puttoken(); return(COLUMN); }
column			{ puttoken(); return(COLUMN); }
ADD				{ puttoken(); return(ADD); }
add				{ puttoken(); return(ADD); }
MODIFY			{ puttoken(); return(MODIFY); }
modify			{ puttoken(); return(MODIFY); }

FUNCTION		{ puttoken(); return(FUNCTION); }
METHOD			{ puttoken(); return(METHOD); }
RETURNS			{ puttoken(); return(RETURNS); }
LOCATOR			{ puttoken(); return(LOCATOR); }
SPECIFIC		{ puttoken(); return(SPECIFIC); }
EXTERNAL		{ puttoken(); return(EXTERNAL); }
NAME			{ puttoken(); return(NAME); }
DETERMINISTIC	{ puttoken(); return(DETERMINISTIC); }
ACTION			{ puttoken(); return(ACTION); }
FENCED			{ puttoken(); return(FENCED); }
CALL			{ puttoken(); return(CALL); }
LANGUAGE		{ puttoken(); return(LANGUAGE); }
PARAMETER		{ puttoken(); return(PARAMETER); }
STYLE			{ puttoken(); return(STYLE); }
NO				{ puttoken(); return(NO); }
SCRATCHPAD		{ puttoken(); return(SCRATCHPAD); }
FINAL			{ puttoken(); return(FINAL); }
ALLOW			{ puttoken(); return(ALLOW); }
PARALLEL		{ puttoken(); return(PARALLEL); }
DISALLOW		{ puttoken(); return(DISALLOW); }
DBINFO			{ puttoken(); return(DBINFO); }
PROCEDURE		{ puttoken(); return(PROCEDURE); }
RESULT			{ puttoken(); return(RESULT); }
SETS			{ puttoken(); return(SETS); }

NIL             { puttoken(); return(VAL_NIL); }
nil             { puttoken(); return(VAL_NIL); }
TRUE            { puttoken(); return(VAL_TRUE); }
true            { puttoken(); return(VAL_TRUE); }
FALSE           { puttoken(); return(VAL_FALSE); }
false           { puttoken(); return(VAL_FALSE); }

DATE            { puttoken(); return(DATE); }
TIME            { puttoken(); return(TIME); }
TIMESTAMP       { puttoken(); return(TIMESTAMP); }
INTERVAL        { puttoken(); return(INTERVAL); }
TO              { puttoken(); return(TO); }
YEAR            { puttoken(); return(YEAR); }
MONTH           { puttoken(); return(MONTH); }
DAY             { puttoken(); return(DAY); }
HOUR            { puttoken(); return(HOUR); }
MINUTE          { puttoken(); return(MINUTE); }
SECOND          { puttoken(); return(SECOND); }

MOD             { puttoken(); return(MOD); }
mod             { puttoken(); return(MOD); }
ABS             { puttoken(); return(ABS); }
abs             { puttoken(); return(ABS); }
LIKE            { puttoken(); return(LIKE); }
like            { puttoken(); return(LIKE); }
NOT             { puttoken(); return(NOT); }
not             { puttoken(); return(NOT); }
AND             { puttoken(); return(AND); }
and             { puttoken(); return(AND); }
OR              { puttoken(); return(OR); }
or              { puttoken(); return(OR); }

struct          { puttoken(); return(STRUCT); }
STRUCT          { puttoken(); return(STRUCT); }
set             { puttoken(); return(SET); }
SET             { puttoken(); return(SET); }
bag             { puttoken(); return(BAG); }
BAG             { puttoken(); return(BAG); }
list            { puttoken(); return(LIST); }
LIST            { puttoken(); return(LIST); }
array           { puttoken(); return(ARRAY); }
ARRAY           { puttoken(); return(ARRAY); }
odmg_set        { puttoken(); return(ODMG_SET); }
ODMG_SET        { puttoken(); return(ODMG_SET); }
odmg_bag        { puttoken(); return(ODMG_BAG); }
ODMG_BAG        { puttoken(); return(ODMG_BAG); }
odmg_list       { puttoken(); return(ODMG_LIST); }
ODMG_LIST       { puttoken(); return(ODMG_LIST); }
odmg_array      { puttoken(); return(ODMG_ARRAY); }
ODMG_ARRAY      { puttoken(); return(ODMG_ARRAY); }

first           { puttoken(); return(FIRST); }
last            { puttoken(); return(LAST); }

FOR             { puttoken(); return(FOR); }
for             { puttoken(); return(FOR); }
ALL             { puttoken(); return(ALL); }
all             { puttoken(); return(ALL); }
IN              { puttoken(); return(IN); }
in              { puttoken(); return(IN); }
OUT				{ puttoken(); return(OUT); }
out				{ puttoken(); return(OUT); }
INOUT			{ puttoken(); return(INOUT); }
inout			{ puttoken(); return(INOUT); }
EXISTS          { puttoken(); return(EXISTS); }
exists          { puttoken(); return(EXISTS); }
UNIQUE          { puttoken(); return(UNIQUE); }
unique          { puttoken(); return(UNIQUE); }
SOME            { puttoken(); return(SOME); }
some            { puttoken(); return(SOME); }
ANY             { puttoken(); return(ANY); }
any             { puttoken(); return(ANY); }
COUNT           { puttoken(); return(COUNT); }
count           { puttoken(); return(COUNT); }
SUM             { puttoken(); return(SUM); }
sum             { puttoken(); return(SUM); }
MIN             { puttoken(); return(MIN); }
min             { puttoken(); return(MIN); }
MAX             { puttoken(); return(MAX); }
max             { puttoken(); return(MAX); }
AVG             { puttoken(); return(AVG); }
avg             { puttoken(); return(AVG); }

DISTINCT        { puttoken(); return(DISTINCT); }
distinct        { puttoken(); return(DISTINCT); }
SELECT          { puttoken(); return(SELECT); }
select          { puttoken(); return(SELECT); }
FROM            { puttoken(); return(FROM); }
from            { puttoken(); return(FROM); }
WHERE           { puttoken(); return(WHERE); }
where           { puttoken(); return(WHERE); }
GROUP           { puttoken(); return(GROUP); }
group           { puttoken(); return(GROUP); }
BY              { puttoken(); return(BY); }
by              { puttoken(); return(BY); }
HAVING          { puttoken(); return(HAVING); }
having          { puttoken(); return(HAVING); }
ORDER           { puttoken(); return(ORDER); }
order           { puttoken(); return(ORDER); }
ASC             { puttoken(); return(ASC); }
asc             { puttoken(); return(ASC); }
DESC            { puttoken(); return(DESC); }
desc            { puttoken(); return(DESC); }
LIMIT			{ puttoken(); return(LIMIT); }
limit           { puttoken(); return(LIMIT); }

INTERSECT       { puttoken(); return(INTERSECT); }
intersect       { puttoken(); return(INTERSECT); }
UNION           { puttoken(); return(UNION); }
union           { puttoken(); return(UNION); }
DIFFERENCE      { puttoken(); return(EXCEPT); }
difference      { puttoken(); return(EXCEPT); }
EXCEPT          { puttoken(); return(EXCEPT); }
except          { puttoken(); return(EXCEPT); }

listtoset       { puttoken(); return(LISTTOSET); }
element         { puttoken(); return(ELEMENT); }
flatten         { puttoken(); return(FLATTEN); }

NMATCH          { puttoken(); return(NMATCH); }
nmatch          { puttoken(); return(NMATCH); }
MATCH           { puttoken(); return(MATCH); }
match           { puttoken(); return(MATCH); }
WEIGHT          { puttoken(); return(WEIGHT); }
weight          { puttoken(); return(WEIGHT); }
FORWARD			{ puttoken(); return(FORWARDORDER); }
forward			{ puttoken(); return(FORWARDORDER); }
BACKWARD		{ puttoken(); return(BACKWARDORDER); }
backward		{ puttoken(); return(BACKWARDORDER); }

mlgf			{ puttoken(); return(MLGF); }
MLGF			{ puttoken(); return(MLGF); }

GeometryFromText           { puttoken(); return(OGIS_GEOMETRYFROMTEXT); }
PointFromText              { puttoken(); return(OGIS_POINTFROMTEXT); }
LineStringFromText         { puttoken(); return(OGIS_LINESTRINGFROMTEXT); }
PolygonFromText            { puttoken(); return(OGIS_POLYGONFROMTEXT); }
MultiPointFromText         { puttoken(); return(OGIS_MULTIPOINTFROMTEXT); }
MultiLineStringFromText    { puttoken(); return(OGIS_MULTILINESTRINGFROMTEXT); }
MultiPolygonFromText       { puttoken(); return(OGIS_MULTIPOLYGONFROMTEXT); }
GeometryCollectionFromText { puttoken(); return(OGIS_GEOMETRYCOLLECTIONFROMTEXT); }
GeometryFromWKB            { puttoken(); return(OGIS_GEOMETRYFROMWKB); }
PointFromWKB               { puttoken(); return(OGIS_POINTFROMWKB); }
LineStringFromWKB          { puttoken(); return(OGIS_LINESTRINGFROMWKB); }
PolygonFromWKB             { puttoken(); return(OGIS_POLYGONFROMWKB); }
MultiPointFromWKB          { puttoken(); return(OGIS_MULTIPOINTFROMWKB); }
MultiLineFromWKB           { puttoken(); return(OGIS_MULTILINEFROMWKB); }
MultiPolygonFromWKB        { puttoken(); return(OGIS_MULTIPOLYGONFROMWKB); }
GeometryCollectionFromWKB  { puttoken(); return(OGIS_GEOMETRYCOLLECTIONFROMWKB); }
AsText                     { puttoken(); return(OGIS_ASTEXT); }
AsBinary                   { puttoken(); return(OGIS_ASBINARY); }
Dimension                  { puttoken(); return(OGIS_DIMENSION); }
GeometryType               { puttoken(); return(OGIS_GEOMETRYTYPE); }
SRID                       { puttoken(); return(OGIS_SRID); }
Boundary                   { puttoken(); return(OGIS_BOUNDARY); }
OGIS_Length                { puttoken(); return(OGIS_LENGTH); }
OGIS_X                     { puttoken(); return(OGIS_X); }
OGIS_Y                     { puttoken(); return(OGIS_Y); }
Area                       { puttoken(); return(OGIS_AREA); }
NumGeometries              { puttoken(); return(OGIS_NUMGEOMETRIES); }
NumPoints                  { puttoken(); return(OGIS_NUMPOINTS); }
NumInteriorRings           { puttoken(); return(OGIS_NUMINTERIORRINGS); }
IsEmpty                    { puttoken(); return(OGIS_ISEMPTY); }
IsSimple                   { puttoken(); return(OGIS_ISSIMPLE); }
IsClosed                   { puttoken(); return(OGIS_ISCLOSED); }
IsRing                     { puttoken(); return(OGIS_ISRING); }
Contains                   { puttoken(); return(OGIS_CONTAINS); }
Crosses                    { puttoken(); return(OGIS_CROSSES); }
Disjoint                   { puttoken(); return(OGIS_DISJOINT); }
Equals                     { puttoken(); return(OGIS_EQUALS); }
Intersects                 { puttoken(); return(OGIS_INTERSECTS); }
Overlaps                   { puttoken(); return(OGIS_OVERLAPS); }
Related                    { puttoken(); return(OGIS_RELATED); }
Touches                    { puttoken(); return(OGIS_TOUCHES); }
Within                     { puttoken(); return(OGIS_WITHIN); }
Difference                 { puttoken(); return(OGIS_DIFFERENCE); }
Intersection               { puttoken(); return(OGIS_INTERSECTION); }
SymDifference              { puttoken(); return(OGIS_SYMDIFFERENCE); }
Union                      { puttoken(); return(OGIS_UNION); }
Distance                   { puttoken(); return(OGIS_DISTANCE); }
Envelope                   { puttoken(); return(OGIS_ENVELOPE); }
Buffer                     { puttoken(); return(OGIS_BUFFER); }
ConvexHull                 { puttoken(); return(OGIS_CONVEXHULL); }
ExteriorRing               { puttoken(); return(OGIS_EXTERIORRING); }
InteriorRingN              { puttoken(); return(OGIS_INTERIORRINGN); }
Centroid                   { puttoken(); return(OGIS_CENTROID); }
StartPoint                 { puttoken(); return(OGIS_STARTPOINT); }
EndPoint                   { puttoken(); return(OGIS_ENDPOINT); }
PointOnSurface             { puttoken(); return(OGIS_POINTONSURFACE); }
PointN                     { puttoken(); return(OGIS_POINTN); }
GeometryN                  { puttoken(); return(OGIS_GEOMETRYN); }


{identifier}            { IDInstall(); puttoken(); return(ID); } 
{bind_arg}              { BindArgInstall(); puttoken(); return(BINDARG); } 
{integer}               { IntInstall(); puttoken(); return(INTEGER); }
{real}                  { RealInstall(); puttoken(); return(REAL); } 
{sqString}              { StrInstall(SINGLEQUOTE); puttoken(); return(STRING); }
{dqString}              { StrInstall(DOUBLEQUOTE); puttoken(); return(STRING); }

%%


/*********************************************************************
 *      Functions that install the ID's, strings, and constants.
 *         These functions install the ID's, strings, and constants to
 *       the corespond tables.
 *********************************************************************/

/*********************************************************************
 *      Function IDInstall
 *        This function inserts identifier to table "StrPool"
 *********************************************************************/
void    IDInstall()
{
    int index, i;
    char *temp;
    
#ifdef  TRACE  
    printf("\nIDInstall()");
#endif
    
    while( StringTop + yyleng >= MAXSTR ) {
        temp = (char *)malloc(sizeof(char)*MAXSTR*2);
        for( i = 0; i < MAXSTR; i++ )
            *(temp+i) = *(StrPool+i);
        free(StrPool);
        StrPool = temp;
        MAXSTR = MAXSTR * 2;
    }
    
#ifdef  TRACE   
    printf("\nyytext = %s", yytext);
    printf("\nyyleng = %d", yyleng);
#endif
    
    strcpy (&StrPool[StringTop],yytext);
    index = Enter(yyleng);
    
    yylval = ast_newNode(ID, AST_NULL);
    AST(yylval).tokenVal = index;
}

/*********************************************************************
 *      Function BindArgInstall
 *        This function inserts binding argument to table "StrPool"
 *********************************************************************/
void    BindArgInstall()
{
    int index, i;
    char *temp;
    
    if( StringTop + yyleng >= MAXSTR ) {
        temp = (char *)malloc(sizeof(char)*MAXSTR*2);
        for( i = 0; i < MAXSTR; i++ )
            *(temp+i) = *(StrPool+i);
        free(StrPool);
        StrPool = temp;
        MAXSTR = MAXSTR * 2;
    }
    
    strcpy (&StrPool[StringTop],yytext);
    index = Enter(yyleng);
    
    yylval = ast_newNode(BINDARG, AST_NULL);
    AST(yylval).tokenVal = index;
}

/*********************************************************************
 *      Function StrInstall
 *        This installs the string to table "StrPool"
 *********************************************************************/
void    StrInstall(int quotationType)
{
    int index, i;
    char *temp;
    
#ifdef  TRACE  
    printf("\nStrInstall()");
#endif
    
    while( StringTop + yyleng >= MAXSTR ) {
        temp = (char *)malloc(sizeof(char) * MAXSTR*2);
        for( i = 0; i < MAXSTR; i++ )
            *( temp + i ) = *( StrPool + i );
        free(StrPool);
        StrPool = temp;
        MAXSTR = MAXSTR * 2;
    }
    
#ifdef TRACE
    printf("\nyytext : %s\n",yytext);
#endif
    
    /* get rid of qutation mark from string */
	for(index = 0, i = 1; i < yyleng - 1;)
	{
		if(yytext[i] == '\'' && yytext[i + 1] == '\'' && quotationType == SINGLEQUOTE)
		{
			StrPool[StringTop + index] = '\'';
			i     += 2;
			index ++;
		}
		else if(yytext[i] == '"' && yytext[i + 1] == '"' && quotationType == DOUBLEQUOTE)
		{
			StrPool[StringTop + index] = '"';
			i     += 2;
			index ++;
		}
		else
		{
			StrPool[StringTop + index] = yytext[i];
			i     ++;
			index ++;
		}
	}
			
    index = Enter(index);
    
    yylval = ast_newNode(STRING, AST_NULL);
    AST(yylval).tokenVal = index;
}

/*********************************************************************
 *      Function IntInstall
 *        This function inserts integer to table "IntPool"
 *********************************************************************/
void    IntInstall()
{
    long *temp; 
	int i;
    
    if( IntTop + 1 >= maxInt ) 
	{
        temp = (long *) malloc(maxInt*2);
        for( i = 0; i < maxInt; i++ )
            temp[i] = IntPool[i];
        free (IntPool);
        IntPool = temp;
        maxInt = maxInt * 2;
    }
    
#ifndef _LP64
    IntPool[IntTop] = atol(yytext);
#else
    IntPool[IntTop] = atoll(yytext);
#endif
    
    yylval = ast_newNode(INTEGER, AST_NULL);
    AST(yylval).tokenVal = IntTop;

    IntTop++;
    IntEntryCnt++;
}

/*********************************************************************
 *      Function RealInstall
 *        This function inserts real number to table "RealPool"
 *********************************************************************/
void    RealInstall()
{
    float *temp;
    int i;
    
    if( RealTop + 1 >= MAXREAL )
    {
        temp = (float *)malloc(sizeof(float)*MAXREAL*2);
        for( i = 0; i < MAXREAL; i++ )
            *( temp + i ) = *( RealPool + i );
        free(RealPool);
        RealPool = temp;
        MAXREAL = MAXREAL * 2;
    }
    
    RealPool[RealTop] = atoff(yytext);
    
    yylval = ast_newNode(REAL, AST_NULL);
    AST(yylval).tokenVal = RealTop;
    
    RealTop++;
    RealEntryCnt++;
}

/*********************************************************************
 *      Function atoff()
 *        This function convert real number string to real number
 *********************************************************************/
# define RIGHT  0
# define LEFT   1

float atoff(str)
char str[];
{
    int i,j;
    int leng;
    float realnum;
    int locate;
    int rtlen;
    float tmp;
    int minusFlag;


    minusFlag = 0;
    
    leng = strlen(str);
    realnum = 0;
    locate = LEFT;
    rtlen = 0;
    
    for(i=0;i<leng;i++)
    {
        if( str[i] == '-' && i == 0)
            minusFlag = 1;
        else if( str[i] == '.' )
            locate = RIGHT;
        else if( locate == LEFT )
            realnum = realnum * 10 + str[i] - '0';
        else if( locate == RIGHT )
        {
            rtlen++;
            tmp = str[i] - '0';
            for(j=0;j<rtlen;j++)
                tmp = tmp /10;
            realnum = realnum + tmp;
        }
    }
    if(minusFlag == 1)
        realnum = -realnum;

    return(realnum);
}

/*********************************************************************
 *      Function putline()
 *        This function prints a line number to be read
 *********************************************************************/
void    putline()
{
    int i;
   
    LINE_NUM++;
	NEW_COLM_NUM = 0;
}

/*********************************************************************
 *      Function puttoken()
 *        This function insert token string to line pool
 *        and print the token as output
 *********************************************************************/
void    puttoken()
{
    int i,j;
    char *tmp;

    while(LINE_COLM_NUM + strlen(yytext) + 1 > LINE_SIZE)
    {
        tmp = (char*)malloc(sizeof(char)*LINE_SIZE * 2);

        memcpy(tmp, LINE, LINE_SIZE);

        free(LINE);
        LINE = tmp;
        LINE_SIZE *= 2;
    }

    strcpy(&LINE[LINE_COLM_NUM],yytext);
    
#ifdef TRACE
    printf("%s", yytext);       
#endif
    
    LINE_COLM_NUM = LINE_COLM_NUM + yyleng;
	COLM_NUM = NEW_COLM_NUM;
	NEW_COLM_NUM = NEW_COLM_NUM + yyleng;
}

/* 
        input() support routine
        
        we must check the size of SQL query string.
*/
/* define input() */
#undef  input()

#define input() (((yytchar = yysptr > yysbuf ? U(*--yysptr):oql_getc(yyin)) == 10?(yylineno++,yytchar):yytchar)==EOF?0:yytchar)


char    *oql_query_buffer;              /* buffer to store oql query */
int      oql_query_idx;                 /* buffer index */
int      oql_query_buffer_size;         /* buffer size */
/*
        copy query to OQL query buffer
*/
void oql_query_buffer_init( query)
char *query;
{
    oql_query_buffer = (char*)malloc( strlen(query)+1);
    strcpy( oql_query_buffer, query);
    oql_query_idx = 0;
    oql_query_buffer_size = strlen(query);
    
    LINE = (char*)malloc(sizeof(char)*LINE_SIZE);
}

void    oql_query_buffer_final()
{
        if (oql_query_buffer != NULL) free(oql_query_buffer);

        if (LINE != NULL) free(LINE);
        LINE = NULL;
        LINE_SIZE = 2000;
        LINE_NUM = 1;
		COLM_NUM = 0;
        NEW_COLM_NUM = 0;
        LINE_COLM_NUM = 0;
}

/*
        return a character from buffer
*/
int     oql_getc( fp)
FILE *fp;
{
    char c;
    
    c = oql_query_buffer[oql_query_idx];
    if ( c == NULL ) return EOF;
    oql_query_idx ++;
    return c;           
}

/*
        input function for flex 
*/
#ifndef min
#define min(x,y) ((x)<(y))?(x):(y)
#endif

int oql_yyinput(buf, max_size)
char* buf; 
int max_size;
{
    int n = min(max_size, oql_query_buffer_size - oql_query_idx);
    
    if(n > 0)
    {
        memcpy(buf, &oql_query_buffer[oql_query_idx], n);
        oql_query_idx += n;
    }
    
    return n;
}

