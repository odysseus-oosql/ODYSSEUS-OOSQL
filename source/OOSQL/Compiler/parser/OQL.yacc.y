%{
/*
 * MODULE: OQL.yacc.y
 *
 * DESCRIPTION:
 *      YACC specifications for ODMG-93 OQL
 *
 */
#include <malloc.h>
#include "OQL_Err.h"
#include "OQL.yacc.h"
#include "OQL_AST.h"
#include "ctype.h"

#include <string.h>
extern char* yytext;
extern void IDInstall();

/* variable declarations */
ASTNodeIdx      root, first;
%}

%token  ID
%token  BINDARG
%token  CHARACTER
%token  STRING
%token  INTEGER
%token  REAL

%token  L_PAR
%token  R_PAR
%token  L_BRACKET
%token  R_BRACKET
%token  L_BRACE
%token  R_BRACE
%token  PLUS
%token  MINUS
%token  ASTERISK
%token  SHARP
%token  SLASH
%token  VERT_BAR
%token  STR_CAT
%token  EQ
%token  NE
%token  GT
%token  LT
%token  GE
%token  LE
%token  DOT
%token  COMMA
%token  RANGE
%token  R_ARROW
%token  COLON
%token  SEMICOLON
%token  AMPER
%token  CAROT
%token  TILD

%token  BETWEEN
%token  OBJECT
%token	IMMEDIATE
%token	DEFERRED
%token	TEXT
%token	QUESTIONVALUE
%token	TEMPORARY
%token  NULLVALUE
%token	CREATE
%token  ALTER
%token	SEQUENCE
%token	START
%token	WITH
%token  ADD
%token  DOT_CURRVAL
%token  DOT_NEXTVAL
%token  COLUMN
%token  MODIFY
%token	TABLE
%token  CLASS
%token	SUBCLASS
%token	OF
%token  IS
%token	UNDER
%token	PRIMARY
%token	INDEX
%token	ON
%token	CLUSTER
%token	DROP
%token	UPDATE
%token	INSERT
%token	INTO
%token  VALUES
%token  DELETE
%token  DEFINE
%token  AS
%token  VAL_NIL
%token  VAL_TRUE
%token  VAL_FALSE
%token  MOD
%token  ABS
%token  LIKE
%token  NOT
%token  AND
%token  OR
%token  STRUCT
%token  SET
%token  BAG
%token  LIST
%token  ARRAY
%token  ODMG_SET
%token  ODMG_BAG
%token  ODMG_LIST
%token  ODMG_ARRAY
%token  FIRST
%token  LAST
%token  FOR
%token  ALL
%token  IN
%token  OUT
%token  INOUT
%token  EXISTS
%token  UNIQUE
%token  SOME
%token  ANY
%token  COUNT
%token  SUM
%token  MIN
%token  MAX
%token  AVG
%token  DISTINCT
%token  SELECT
%token  FROM
%token  WHERE
%token  GROUP
%token  BY
%token  HAVING
%token  ORDER
%token  ASC
%token  DESC
%token	LIMIT
%token  INTERSECT
%token  UNION   
%token  EXCEPT
%token  LISTTOSET
%token  ELEMENT
%token  FLATTEN

%token  NMATCH
%token  MATCH
%token  WEIGHT
%token	FORWARDORDER
%token	BACKWARDORDER
%token  DATE
%token  TIME
%token  TIMESTAMP
%token  INTERVAL
%token  TO
%token  YEAR
%token  MONTH
%token  DAY
%token  HOUR
%token  MINUTE
%token  SECOND

%token  FUNCTION
%token  METHOD
%token  RETURNS
%token  LOCATOR
%token  SPECIFIC
%token  EXTERNAL
%token  NAME
%token  DETERMINISTIC
%token  ACTION
%token  FENCED
%token  CALL
%token  LANGUAGE
%token  PARAMETER
%token  STYLE
%token  NO
%token  SCRATCHPAD
%token  FINAL
%token  ALLOW
%token  PARALLEL
%token  DISALLOW
%token  DBINFO
%token  PROCEDURE
%token  RESULT
%token  SETS
%token	MLGF

%token	OGIS_GEOMETRYFROMTEXT
%token	OGIS_POINTFROMTEXT
%token	OGIS_LINESTRINGFROMTEXT
%token	OGIS_POLYGONFROMTEXT
%token	OGIS_MULTIPOINTFROMTEXT
%token	OGIS_MULTILINESTRINGFROMTEXT
%token	OGIS_MULTIPOLYGONFROMTEXT
%token	OGIS_GEOMETRYCOLLECTIONFROMTEXT
%token	OGIS_GEOMETRYFROMWKB
%token	OGIS_POINTFROMWKB
%token	OGIS_LINESTRINGFROMWKB
%token	OGIS_POLYGONFROMWKB
%token	OGIS_MULTIPOINTFROMWKB
%token	OGIS_MULTILINEFROMWKB
%token	OGIS_MULTIPOLYGONFROMWKB
%token	OGIS_GEOMETRYCOLLECTIONFROMWKB
%token	OGIS_ASTEXT
%token	OGIS_ASBINARY
%token	OGIS_DIMENSION
%token	OGIS_GEOMETRYTYPE
%token	OGIS_SRID
%token	OGIS_BOUNDARY
%token	OGIS_LENGTH
%token	OGIS_X
%token	OGIS_Y
%token	OGIS_AREA
%token	OGIS_NUMGEOMETRIES
%token	OGIS_NUMPOINTS
%token	OGIS_NUMINTERIORRINGS
%token	OGIS_ISEMPTY
%token	OGIS_ISSIMPLE
%token	OGIS_ISCLOSED
%token	OGIS_ISRING
%token	OGIS_CONTAINS
%token	OGIS_CROSSES
%token	OGIS_DISJOINT
%token	OGIS_EQUALS
%token	OGIS_INTERSECTS
%token	OGIS_OVERLAPS
%token	OGIS_RELATED
%token	OGIS_TOUCHES
%token	OGIS_WITHIN
%token	OGIS_DIFFERENCE
%token	OGIS_INTERSECTION
%token	OGIS_SYMDIFFERENCE
%token	OGIS_UNION
%token	OGIS_DISTANCE
%token	OGIS_ENVELOPE
%token	OGIS_BUFFER
%token	OGIS_CONVEXHULL
%token	OGIS_EXTERIORRING
%token	OGIS_INTERIORRINGN
%token	OGIS_CENTROID
%token	OGIS_STARTPOINT
%token	OGIS_ENDPOINT
%token	OGIS_POINTONSURFACE
%token	OGIS_POINTN
%token	OGIS_GEOMETRYN

%nonassoc       EMPTY_RULE
%nonassoc       DISTINCT_FUNCTION
%nonassoc       BASIC_QUERY 
%nonassoc       IR_BOOL_PRIMARY
%nonassoc       ASC DESC
%nonassoc       SELECT
%nonassoc       FROM
%nonassoc       WHERE
%nonassoc       GROUP
%nonassoc       HAVING
%nonassoc       ORDER
%nonassoc		LIMIT
%right          TYPE_CAST
%left           COMMA
%nonassoc       RANGE COLON AS
%left           OR
%left           AND EXISTS FOR ALL
%left           EQ NE LIKE
%left           LT GT LE GE QUANTIFIED_COMP

%left           PLUS MINUS UNION EXCEPT STR_CAT
%left           TEXTIR_ACCUMULATE
%left           TEXTIR_OR
%left           TEXTIR_AND
%left           TEXTIR_MINUS
%left           ASTERISK SLASH MOD INTERSECT
%nonassoc       FROM_CLTN_STAR
%left           TEXTIR_MAX TEXTIR_MULTIPLY TEXTIR_THRESHOLD
%left           TEXTIR_NEAR
%left           IN
%left           NOT UMINUS UPLUS
%nonassoc       L_PAR L_BRACKET
%left           DOT R_ARROW ID
%nonassoc       R_PAR R_BRACKET

%%

/* %nonassoc       FIRST LAST COUNT SUM MIN MAX AVG LISTTOSET ELEMENT DISTINCT FLATTEN */
/************************************************************************
 *      Production rules & their corresponding actions for OQL BNF,     *
 *      which is defined in ODMG-93 Release 1.2.                        *
 ************************************************************************/

/************************
 *      Axiom           *
 ************************/

query_program   
        : opt_define_query_list query { 
                first = ast_nodeCat( $1, $2 );
                root = ast_newNode( QuProg, first );
                }
		| opt_define_query_list delete_query { 
                first = ast_nodeCat( $1, $2 );
                root = ast_newNode( QuProg, first );
                }
		| opt_define_query_list insert_query { 
                first = ast_nodeCat( $1, $2 );
                root = ast_newNode( QuProg, first );
                }
		| opt_define_query_list update_query { 
                first = ast_nodeCat( $1, $2 );
                root = ast_newNode( QuProg, first );
                }
		| opt_define_query_list create_table { 
                first = ast_nodeCat( $1, $2 );
                root = ast_newNode( QuProg, first );
                }
		| opt_define_query_list drop_table { 
                first = ast_nodeCat( $1, $2 );
                root = ast_newNode( QuProg, first );
                }
		| opt_define_query_list create_index { 
                first = ast_nodeCat( $1, $2 );
                root = ast_newNode( QuProg, first );
                }
		| opt_define_query_list drop_index { 
                first = ast_nodeCat( $1, $2 );
                root = ast_newNode( QuProg, first );
                }
		| opt_define_query_list alter_table { 
                first = ast_nodeCat( $1, $2 );
                root = ast_newNode( QuProg, first );
                }
		| opt_define_query_list create_sequence { 
                first = ast_nodeCat( $1, $2 );
                root = ast_newNode( QuProg, first );
                }
		| opt_define_query_list drop_sequence { 
                first = ast_nodeCat( $1, $2 );
                root = ast_newNode( QuProg, first );
                }
		| opt_define_query_list create_function { 
                first = ast_nodeCat( $1, $2 );
                root = ast_newNode( QuProg, first );
                }
		| opt_define_query_list create_procedure { 
                first = ast_nodeCat( $1, $2 );
                root = ast_newNode( QuProg, first );
                }
		| opt_define_query_list drop_function { 
                first = ast_nodeCat( $1, $2 );
                root = ast_newNode( QuProg, first );
                }
		| opt_define_query_list drop_procedure { 
                first = ast_nodeCat( $1, $2 );
                root = ast_newNode( QuProg, first );
                }
		| opt_define_query_list call_procedure { 
                first = ast_nodeCat( $1, $2 );
                root = ast_newNode( QuProg, first );
                }
        ;

opt_define_query_list
        : { $$ = ast_newNode(null, AST_NULL); }
        | define_query_list { $$ = $1; }
        ;

define_query_list
        : define_query SEMICOLON  { $$ = $1; }
        | define_query SEMICOLON define_query_list { $$ = ast_nodeCat( $1, $3 ); }
        ;

define_query
        : DEFINE ID AS query { 
                first = ast_nodeCat( $2, $4 ); 
                $$ = ast_newNode( QuDef, first );
                }
        ;

delete_query 
		: DELETE FROM ID opt_where_clause {
				first = ast_nodeCat($3, ast_newNode(null, AST_NULL));
				first = ast_newNode(FrCltn, first);
				first = ast_newNode(ClFr, first);
				first = ast_nodeCat(first, $4);
				$$ = ast_newNode(QuDel, first);	}
		| DELETE FROM OBJECT object_id {
				first = ast_nodeCat($4, AST_NULL);
				first = ast_newNode(FrObject, first);
				first = ast_newNode(ClFr, first);
				$$ = ast_newNode(QuDel, ast_nodeCat(first, ast_newNode(null, AST_NULL)));	}
		| DEFERRED DELETE FROM ID opt_where_clause {
				first = ast_nodeCat($4, ast_newNode(null, AST_NULL));
				first = ast_newNode(FrCltn, first);
				first = ast_newNode(ClFr, first);
				first = ast_nodeCat(first, $5);
				$$ = ast_newNode(QuDeferredDel, first);	}
		| DEFERRED DELETE FROM OBJECT object_id {
				first = ast_nodeCat($5, AST_NULL);
				first = ast_newNode(FrObject, first);
				first = ast_newNode(ClFr, first);
				$$ = ast_newNode(QuDeferredDel, ast_nodeCat(first, ast_newNode(null, AST_NULL)));	}
		;

insert_query
		: INSERT INTO ID L_PAR field_list R_PAR icond {
				first = ast_nodeCat($3, ast_newNode(null, AST_NULL));
				first = ast_newNode(FrCltn, first);
				first = ast_newNode(ClFr, first);
				first = ast_nodeCat(first, ast_nodeCat(ast_newNode(QuInsCol, $5), $7));
				$$ = ast_newNode(QuIns, first); }
		| INSERT INTO ID icond {
				first = ast_nodeCat($3, ast_newNode(null, AST_NULL));
				first = ast_newNode(FrCltn, first);
				first = ast_newNode(ClFr, first);
				first = ast_nodeCat(first, $4);
				$$ = ast_newNode(QuIns, first); }
		;

create_table
		: CREATE class_or_table ID superclasses primary_key L_PAR table_member_list R_PAR {
				first = ast_nodeCat($5, ast_newNode(TblAttrList, $7));
				first = ast_nodeCat($3, ast_nodeCat($4, first));
				$$ = ast_newNode(QuCreateTbl, first); }
		| CREATE TEMPORARY class_or_table ID primary_key L_PAR table_member_list R_PAR {
				first = ast_nodeCat($5, ast_newNode(TblAttrList, $7));
				first = ast_nodeCat($4, ast_nodeCat(ast_newNode(SuperClasses, ast_newNode(null, AST_NULL)), first));
				$$ = ast_newNode(QuCreateTempTbl, first); }

class_or_table
		: CLASS
		| TABLE

superclasses
		:									{ $$ = ast_newNode(SuperClasses, ast_newNode(null, AST_NULL)); }
		| UNDER superclass_list				{ $$ = ast_newNode(SuperClasses, $2); }
		| AS SUBCLASS OF superclass_list	{ $$ = ast_newNode(SuperClasses, $4); }

superclass_list
		: ID								{ $$ = $1; }
		| superclass_list COMMA ID			{ $$ = ast_nodeCat($1, $3); }

primary_key
		:									{ $$ = ast_newNode(TblKeyDef, ast_newNode(null, AST_NULL)); }
		| PRIMARY L_PAR primary_keys R_PAR	{ $$ = ast_newNode(TblKeyDef, $3); }

primary_keys
		: ID								{ $$ = $1; }
		| primary_keys COMMA ID				{ $$ = ast_nodeCat($1, $3); }

table_member_list
		:												{ $$ = ast_newNode(null, AST_NULL); }
		| table_attribute								{ $$ = $1; }
		| table_method									{ $$ = $1; }
		| table_member_list COMMA table_attribute		{ $$ = ast_nodeCat($1, $3); }

table_method
		: METHOD func_name L_PAR func_arg_list R_PAR RETURNS func_arg specific_name
		  external_name deterministic external_action fenced null_call language parameter_style
		  scratchpad finalcall parallel dbinfo
			{
				first = ast_nodeCat($18, $19);
				first = ast_nodeCat($17, first);
				first = ast_nodeCat($16, first);
				first = ast_nodeCat($15, first);
				first = ast_nodeCat($14, first);
				first = ast_nodeCat($13, first);
				first = ast_nodeCat($12, first);
				first = ast_nodeCat($11, first);
				first = ast_nodeCat($10, first);
				first = ast_nodeCat($9, first);
				first = ast_nodeCat($8, first);
				first = ast_nodeCat(ast_newNode(FuncReturns, $7), first);
				first = ast_nodeCat(ast_newNode(FuncArgList, $4), first);
				first = ast_nodeCat($2, first);
				$$ = ast_newNode(QuCreateMethod, first);
			}
		;

table_attribute
		: table_attribute_name table_attribute_domain {
				first = ast_nodeCat(ast_newNode(TblAttrName, $1), $2);
				$$ = ast_newNode(TblAttr, first); }

table_attribute_name
		: ID						{ $$ = $1; }

table_attribute_domain
		: table_attribute_domain_name {
				$$ = ast_newNode(TblAttrDomain, $1); }
		| table_attribute_complex_domain L_PAR table_attribute_domain_name R_PAR {
				first = ast_nodeCat($1, $3);
				$$ = ast_newNode(TblAttrDomain, first); }
		| table_attribute_complex_domain L_PAR table_attribute_domain_name L_PAR INTEGER R_PAR R_PAR {
				first = ast_nodeCat($3, $5);
				first = ast_nodeCat($1, first);
				$$ = ast_newNode(TblAttrDomain, first); }
		| table_attribute_domain_name L_PAR INTEGER R_PAR {
				first = ast_nodeCat($1, $3);
				$$ = ast_newNode(TblAttrDomain, first); }
        | table_attribute_domain_name L_PAR ID R_PAR {
				first = ast_nodeCat($1, $3);
				$$ = ast_newNode(TblAttrDomain, first); }
		| table_attribute_domain_name L_PAR domain_option_list R_PAR {
				first = ast_nodeCat($1, $3);
				$$ = ast_newNode(TblAttrDomain, first); }

table_attribute_complex_domain
		: SET		 { strcpy((char*)yytext, (char*)"set");			IDInstall(); yyval = yylval; }
		| BAG		 { strcpy((char*)yytext, (char*)"bag");			IDInstall(); yyval = yylval; }
		| LIST		 { strcpy((char*)yytext, (char*)"list");		IDInstall(); yyval = yylval; }
		| ODMG_SET	 { strcpy((char*)yytext, (char*)"odmg_set");	IDInstall(); yyval = yylval; }
		| ODMG_BAG	 { strcpy((char*)yytext, (char*)"odmg_bag");	IDInstall(); yyval = yylval; }
		| ODMG_LIST	 { strcpy((char*)yytext, (char*)"odmg_list");	IDInstall(); yyval = yylval; }
		| ODMG_ARRAY { strcpy((char*)yytext, (char*)"odmg_array");  IDInstall(); yyval = yylval; }

domain_option_list
		:												{ $$ = ast_newNode(null, AST_NULL); }
		| domain_option									{ $$ = $1; }
		| domain_option_list COMMA domain_option		{ $$ = ast_nodeCat($1, $3); }

domain_option
		: INTEGER							/* example : a INTEGER(10, 20) */
				{	$$ = $1; }
		| ID L_PAR VAL_TRUE R_PAR			/* example : a TEXT(CONTAIN_TUPLEID(TRUE)) */
				{   
					first = ast_nodeCat($1, ast_newNode(VaTr, AST_NULL));
					$$ = ast_newNode(QuUserFunction, first); 
				}
		| ID L_PAR VAL_FALSE R_PAR			/* example : a TEXT(CONTAIN_TUPLEID(FALSE)) */
				{   
					first = ast_nodeCat($1, ast_newNode(VaFls, AST_NULL));
					$$ = ast_newNode(QuUserFunction, first); 
				}
		| ID L_PAR attributes_list R_PAR	/* example : a TEXT(EMBED_ATTRIBUTES(b, c, e)) */
				{   
					first = ast_nodeCat($1, $3);
					$$ = ast_newNode(QuUserFunction, first); 
				}
		;

attributes_list
		:												{ $$ = ast_newNode(null, AST_NULL); }
		| ID											{ $$ = $1; }
		| attributes_list COMMA ID						{ $$ = ast_nodeCat($1, $3); }
		;

table_attribute_domain_name
		: ID		{ $$ = $1; }
		| TEXT		{ strcpy((char*)yytext, (char*)"text"); IDInstall(); yyval = yylval; }
		| DATE		{ strcpy((char*)yytext, (char*)"date"); IDInstall(); yyval = yylval; }
		| TIME		{ strcpy((char*)yytext, (char*)"time"); IDInstall(); yyval = yylval; }
		| TIMESTAMP { strcpy((char*)yytext, (char*)"timestamp"); IDInstall(); yyval = yylval; }
		| INTERVAL	{ strcpy((char*)yytext, (char*)"interval"); IDInstall(); yyval = yylval; }
		| ID ID		{ $$ = ast_nodeCat($1, $2); }
		;

alter_table
		: ALTER class_or_table ID alter_table_action_list {
				first = ast_newNode(AltActList, $4);
				first = ast_nodeCat($3, first);
				$$ = ast_newNode(QuAlterTbl, first); }

alter_table_action_list
		: alter_table_action									{ $$ = $1; }
		| alter_table_action_list COMMA alter_table_action		{ $$ = ast_nodeCat($1, $3); }

alter_table_action
		: ADD L_PAR column_declar_list R_PAR {
				$$ = ast_newNode(AltActAddCol, $3); }
		| DROP COLUMN column_name {
				$$ = ast_newNode(AltActDropCol, $3); }
		| DROP L_PAR column_name_list R_PAR {
				$$ = ast_newNode(AltActDropColList, $3); }

column_declar_list
		: column_declar								{ $$ = $1; }
		| column_declar_list COMMA column_declar	{ $$ = ast_nodeCat($1, $3); }

column_declar
		: table_attribute	{ $$ = $1; }

column_name_list
		: column_name								{ $$ = $1; }
		| column_name_list COMMA column_name		{ $$ = ast_nodeCat($1, $3); }

column_name
		: ID				{ $$ = $1; }
		
create_sequence
		: CREATE SEQUENCE ID create_seq_opt_list {
				first = ast_newNode(SeqOptList, $4);
				first = ast_nodeCat($3, first);
				$$ = ast_newNode(QuCreateSeq, first); }

create_seq_opt_list
		: create_seq_opt									{ $$ = $1; }
		| create_seq_opt_list COMMA create_seq_opt			{ $$ = ast_nodeCat($1, $3); }

create_seq_opt
		: START WITH integer_literal {
				$$ = ast_newNode(SeqStartWith, $3); }
		|

drop_sequence
		: DROP SEQUENCE ID	{ $$ = ast_newNode(QuDropSeq, $3); }

create_index
		: CREATE index_type ID ON ID L_PAR index_key_list R_PAR {
				first = ast_newNode(IdxKeyList, $7);
				first = ast_nodeCat($2, ast_nodeCat($3, ast_nodeCat($5, first)));
				$$ = ast_newNode(QuCreateIdx, first); }

index_type
		: INDEX					{ $$ = ast_newNode(IdxTypeNormal, AST_NULL); }
		| UNIQUE INDEX			{ $$ = ast_newNode(IdxTypeUnique, AST_NULL); }
		| CLUSTER INDEX			{ $$ = ast_newNode(IdxTypeCluster, AST_NULL); }
		| UNIQUE CLUSTER INDEX	{ $$ = ast_newNode(IdxTypeUniqueCluster, AST_NULL); }
		| MLGF INDEX					{ $$ = ast_newNode(IdxTypeMlgf, AST_NULL); }
		| UNIQUE MLGF INDEX				{ $$ = ast_newNode(IdxTypeUniqueMlgf, AST_NULL); }
		| CLUSTER MLGF INDEX			{ $$ = ast_newNode(IdxTypeClusterMlgf, AST_NULL); }
		| UNIQUE CLUSTER MLGF INDEX		{ $$ = ast_newNode(IdxTypeUniqueClusterMlgf, AST_NULL); }
		;

index_key_list
		: ID						{ $$ = $1; }
		| index_key_list COMMA ID	{ $$ = ast_nodeCat($1, $3); }

drop_table
		: DROP TABLE ID { $$ = ast_newNode(QuDropTbl, $3); }
		| DROP CLASS ID { $$ = ast_newNode(QuDropTbl, $3); }

drop_index
		: DROP INDEX ID { $$ = ast_newNode(QuDropIdx, $3); }

field_list
		: field						{ $$ = $1;}
		| field_list COMMA field	{ $$ = ast_nodeCat($1, $3); }
		;

field	: ID						{ $$ = $1; }
		;

icond	: VALUES L_PAR const_list R_PAR { $$ = ast_newNode(QuInsValue, $3); }
		| select_expr_query				{ $$ = $1; }
		;

const_list
		:							{ $$ = ast_newNode(null, AST_NULL); }
		| const						{ $$ = $1; }
		| const_list COMMA const	{ $$ = ast_nodeCat($1, $3); }
		;

const	: integer_literal			{ $$ = $1; }
		| float_literal				{ $$ = $1; }
		| character_literal			{ $$ = $1; }
		| string_literal			{ $$ = $1; }
		| date_literal				{ $$ = $1; }
		| time_literal				{ $$ = $1; }
		| timestamp_literal			{ $$ = $1; }
		| interval_literal			{ $$ = $1; }
		| text_literal				{ $$ = $1; }
		| complex_literal			{ $$ = $1; }
		| NULLVALUE					{ $$ = ast_newNode(VaNil, AST_NULL); }
		| QUESTIONVALUE				{ $$ = ast_newNode(VaParam, AST_NULL); }
		| ID DOT_CURRVAL			{ $$ = ast_newNode(VaSeqCurr, $1); }
		| ID DOT_NEXTVAL			{ $$ = ast_newNode(VaSeqNext, $1); }
		| ogis_query				{ $$ = $1; }
		;

update_query
		: UPDATE ID SET set_elements opt_where_clause {
				first = ast_nodeCat($2, ast_newNode(null, AST_NULL));
				first = ast_newNode(FrCltn, first);
				first = ast_newNode(ClFr, first);
				first = ast_nodeCat(first, ast_nodeCat(ast_newNode(QuUpdSetList, $4), $5));
				$$ = ast_newNode(QuUpd, first); }
		| UPDATE OBJECT object_id SET set_elements {
				first = ast_nodeCat($3, AST_NULL);
				first = ast_newNode(FrObject, first);
				first = ast_newNode(ClFr, first);
				first = ast_nodeCat(first, ast_nodeCat(ast_newNode(QuUpdSetList, $5), ast_newNode(null, AST_NULL)));
				$$ = ast_newNode(QuUpd, first); }
		;

object_id
		: string_literal			{ $$ = $1; }

set_elements
		: set_element						{ $$ = $1; }
		| set_elements COMMA set_element	{ $$ = ast_nodeCat($1, $3); }
		;
set_element
		: ID EQ query { 
				$$ = ast_newNode(QuUpdSet, ast_nodeCat($1, $3)); }
		| ID EQ NULLVALUE {
				first = ast_newNode(VaNil, AST_NULL);
				$$ = ast_newNode(QuUpdSet, ast_nodeCat($1, first)); }
		| ID EQ QUESTIONVALUE {
				first = ast_newNode(VaParam, AST_NULL);
				$$ = ast_newNode(QuUpdSet, ast_nodeCat($1, first)); }
		| ID EQ text_literal {
				$$ = ast_newNode(QuUpdSet, ast_nodeCat($1, $3)); }
		| ID EQ ID DOT_CURRVAL { 
				first = ast_newNode(VaSeqCurr, $3);				
				$$ = ast_newNode(QuUpdSet, ast_nodeCat($1, first)); }
		| ID EQ ID DOT_NEXTVAL { 
				first = ast_newNode(VaSeqNext, $3);				
				$$ = ast_newNode(QuUpdSet, ast_nodeCat($1, first)); }
		;

text_literal
		: TEXT update_mode string_literal	{
				first = ast_nodeCat($2, $3);
				$$ = ast_newNode(VaText, first); }
		| TEXT update_mode QUESTIONVALUE	{
				first = ast_newNode(VaParam, AST_NULL);
				first = ast_nodeCat($2, first);
				$$ = ast_newNode(VaText, first); }
		;

update_mode
		:						{ $$ = ast_newNode(TextUpdModeImmediate, AST_NULL); }
		| IMMEDIATE				{ $$ = ast_newNode(TextUpdModeImmediate, AST_NULL); }
		| DEFERRED				{ $$ = ast_newNode(TextUpdModeDeferred, AST_NULL); }
		;

create_function
		: CREATE FUNCTION func_name L_PAR func_arg_list R_PAR RETURNS func_arg specific_name
		  external_name deterministic external_action fenced null_call language parameter_style
		  scratchpad finalcall parallel dbinfo
			{
				first = ast_nodeCat($19, $20);
				first = ast_nodeCat($18, first);
				first = ast_nodeCat($17, first);
				first = ast_nodeCat($16, first);
				first = ast_nodeCat($15, first);
				first = ast_nodeCat($14, first);
				first = ast_nodeCat($13, first);
				first = ast_nodeCat($12, first);
				first = ast_nodeCat($11, first);
				first = ast_nodeCat($10, first);
				first = ast_nodeCat($9, first);
				first = ast_nodeCat(ast_newNode(FuncReturns, $8), first);
				first = ast_nodeCat(ast_newNode(FuncArgList, $5), first);
				first = ast_nodeCat($3, first);
				$$ = ast_newNode(QuCreateFunc, first);
			}
		;

func_name
		: ID							{ $$ = $1; }
		;

func_arg_list
		:								{ $$ = ast_newNode(null, AST_NULL); }
		| func_arg						{ $$ = $1; }
		| func_arg_list COMMA func_arg	{ $$ = ast_nodeCat($1, $3); }
		;

func_arg
		: func_arg_type					
			{ 
				$$ = ast_newNode(FuncArg, $1); 
			}
		| func_arg_type AS LOCATOR		
			{ 
				first = ast_nodeCat($1, ast_newNode(FucnArgAsLocator, AST_NULL));
				$$ = ast_newNode(FuncArg, first); 
			}
		;

func_arg_type
		: ID							{ $$ = $1; }
		;

specific_name
		:	{
				$$ = ast_newNode(FuncSpecific, AST_NULL);
			}
		| SPECIFIC ID
			{
				$$ = ast_newNode(FuncSpecific, $2);
			}
		;

external_name
		: EXTERNAL NAME STRING
			{
				$$ = ast_newNode(FuncExternalName, $3);
			}
		;

deterministic
		:	{
				first = ast_newNode(VaTr, AST_NULL);
				$$ = ast_newNode(FuncDeterministic, first);
			}
		| DETERMINISTIC
			{
				first = ast_newNode(VaTr, AST_NULL);
				$$ = ast_newNode(FuncDeterministic, first);
			}
		| NOT DETERMINISTIC
			{
				first = ast_newNode(VaFls, AST_NULL);
				$$ = ast_newNode(FuncDeterministic, first);
			}
		;

external_action
		:	{
				first = ast_newNode(VaFls, AST_NULL);
				$$ = ast_newNode(FuncExternalAction, first);
			}
		| NO EXTERNAL ACTION
			{
				first = ast_newNode(VaFls, AST_NULL);
				$$ = ast_newNode(FuncExternalAction, first);
			}
		| EXTERNAL ACTION
			{
				first = ast_newNode(VaTr, AST_NULL);
				$$ = ast_newNode(FuncExternalAction, first);
			}
		;
		 
fenced
		:	{
				first = ast_newNode(VaTr, AST_NULL);
				$$ = ast_newNode(FuncFenced, first);
			}
		| FENCED
			{
				first = ast_newNode(VaTr, AST_NULL);
				$$ = ast_newNode(FuncFenced, first);
			}
		| NOT FENCED
			{
				first = ast_newNode(VaFls, AST_NULL);
				$$ = ast_newNode(FuncFenced, first);
			}
		;

null_call
		:	{
				first = ast_newNode(VaFls, AST_NULL);
				$$ = ast_newNode(FuncNullCall, first);
			}
		| NOT NULLVALUE CALL
			{
				first = ast_newNode(VaFls, AST_NULL);
				$$ = ast_newNode(FuncNullCall, first);
			}
		| NULLVALUE CALL
			{
				first = ast_newNode(VaTr, AST_NULL);
				$$ = ast_newNode(FuncNullCall, first);
			}
		;

language
		:	{
				strcpy((char*)yytext, (char*)"C"); IDInstall(); first = yylval;
				$$ = ast_newNode(FuncLanguage, first);
			}
		| LANGUAGE ID
			{
				$$ = ast_newNode(FuncLanguage, $2);
			}
		;

parameter_style
		:	{
				strcpy((char*)yytext, (char*)"OOSQL"); IDInstall(); first = yylval;
				$$ = ast_newNode(FuncParameterStyle, first);
			}
		| PARAMETER STYLE ID
			{
				$$ = ast_newNode(FuncParameterStyle, $3);
			}
		;

scratchpad
		:	{
				first = ast_newNode(VaFls, AST_NULL);
				$$ = ast_newNode(FuncScratchpad, first);
			}
		| NO SCRATCHPAD
			{
				first = ast_newNode(VaFls, AST_NULL);
				$$ = ast_newNode(FuncScratchpad, first);
			}
		| SCRATCHPAD
			{
				first = ast_newNode(VaTr, AST_NULL);
				$$ = ast_newNode(FuncScratchpad, first);
			}
		;

finalcall
		:	{
				first = ast_newNode(VaFls, AST_NULL);
				$$ = ast_newNode(FuncFinalCall, first);
			}
		| NO FINAL CALL
			{
				first = ast_newNode(VaFls, AST_NULL);
				$$ = ast_newNode(FuncFinalCall, first);
			}
		| FINAL CALL
			{
				first = ast_newNode(VaTr, AST_NULL);
				$$ = ast_newNode(FuncFinalCall, first);
			}
		;

parallel
		:	{
				first = ast_newNode(VaTr, AST_NULL);
				$$ = ast_newNode(FuncParallel, first);
			}
		| ALLOW PARALLEL
			{
				first = ast_newNode(VaTr, AST_NULL);
				$$ = ast_newNode(FuncParallel, first);
			}
		| DISALLOW PARALLEL
			{
				first = ast_newNode(VaFls, AST_NULL);
				$$ = ast_newNode(FuncParallel, first);
			}
		;

dbinfo
		:	{
				first = ast_newNode(VaFls, AST_NULL);
				$$ = ast_newNode(FuncDbinfo, first);
			}
		| NO DBINFO
			{
				first = ast_newNode(VaFls, AST_NULL);
				$$ = ast_newNode(FuncDbinfo, first);
			}
		| DBINFO
			{
				first = ast_newNode(VaTr, AST_NULL);
				$$ = ast_newNode(FuncDbinfo, first);
			}
		;

create_procedure
		: CREATE PROCEDURE proc_name L_PAR proc_arg_list R_PAR specific_name
		  result_set external_name language null_call parameter_style deterministic fenced
			{
				first = ast_nodeCat($13, $14);
				first = ast_nodeCat($12, first);
				first = ast_nodeCat($11, first);
				first = ast_nodeCat($10, first);
				first = ast_nodeCat($9, first);
				first = ast_nodeCat($8, first);
				first = ast_nodeCat($7, first);
				first = ast_nodeCat(ast_newNode(ProcArgList, $5), first);
				first = ast_nodeCat($3, first);
				$$ = ast_newNode(QuCreateProc, first);
			}
		;

proc_name
		: ID							{ $$ = $1; }
		;

proc_arg_list
		:								{ $$ = ast_newNode(null, AST_NULL); }
		| proc_arg						{ $$ = $1; }
		| proc_arg_list COMMA proc_arg	{ $$ = ast_nodeCat($1, $3); }
		;

proc_arg
		: proc_arg_mode proc_arg_name proc_arg_type
			{
				first = ast_nodeCat($2, $3);
				first = ast_nodeCat($1, first);
				$$ = ast_newNode(ProcArg, first);
			}
		;

proc_arg_mode
		: IN							{ $$ = ast_newNode(ProcArgModeIn, AST_NULL); }
		| OUT							{ $$ = ast_newNode(ProcArgModeOut, AST_NULL); }
		| INOUT							{ $$ = ast_newNode(ProcArgModeInOut, AST_NULL); }
		;

proc_arg_name
		: ID							{ $$ = $1; }
		;

proc_arg_type
		: ID							{ $$ = $1; }
		;

result_set
		:								{ $$ = ast_newNode(ProcResultSet, AST_NULL); }
		| RESULT SETS '0'				{ $$ = ast_newNode(ProcResultSet, AST_NULL); }
		| RESULT SETS INTEGER			{ $$ = ast_newNode(ProcResultSet, $3); }
		;

drop_function
		: DROP FUNCTION func_name
			{
				$$ = ast_newNode(QuDropFunc, $3); 
			}
		| DROP FUNCTION func_name L_PAR func_arg_list R_PAR
			{	
				first = ast_nodeCat($3, ast_newNode(FuncArgList, $5));
				$$ = ast_newNode(QuDropFunc, first); 
			}
		| DROP SPECIFIC FUNCTION func_name
			{
				$$ = ast_newNode(QuDropSpecFunc, $4); 
			}
		;

drop_procedure
		: DROP PROCEDURE proc_name
			{
				$$ = ast_newNode(QuDropProc, $3); 
			}
		| DROP SPECIFIC PROCEDURE proc_name
			{
				$$ = ast_newNode(QuDropSpecProc, $4); 
			}
		;

call_procedure
		: CALL proc_name L_PAR arg_list R_PAR
			{
				first = ast_nodeCat($2, $4);
				$$ = ast_newNode(QuCallProc, first)
			}
		;

arg_list
		:								{ $$ = ast_newNode(null, AST_NULL); }
		| query							{ $$ = $1; }
		| arg_list COMMA query			{ $$ = ast_nodeCat($1, $3); }
		;

query
        : basic_query           %prec BASIC_QUERY { $$ = $1; }
        | sequence_query		%prec BASIC_QUERY { $$ = $1; }
        | simple_expr_query     %prec BASIC_QUERY { $$ = $1; }
        | comparison_query      %prec BASIC_QUERY { $$ = $1; }
        | bool_expr_query       %prec BASIC_QUERY { $$ = $1; }
        | constructor_query     %prec BASIC_QUERY { $$ = $1; }
        | accessor_query        %prec BASIC_QUERY { $$ = $1; }
        | collection_expr_query %prec BASIC_QUERY { $$ = $1; }
        | select_expr_query     %prec BASIC_QUERY { $$ = $1; }
        | set_expr_query        %prec BASIC_QUERY { $$ = $1; }
        | conversion_query      %prec BASIC_QUERY { $$ = $1; }
        | ogis_query            %prec BASIC_QUERY { $$ = $1; }
        | ir_query              %prec BASIC_QUERY { $$ = $1; }
        ;

/************************
 *      Basic           *
 ************************/

basic_query
        : VAL_NIL               %prec BASIC_QUERY { $$ = ast_newNode(VaNil, AST_NULL); }
        | VAL_TRUE              %prec BASIC_QUERY { $$ = ast_newNode(VaTr, AST_NULL); }
        | VAL_FALSE             %prec BASIC_QUERY { $$ = ast_newNode(VaFls, AST_NULL); }
        | integer_literal       %prec BASIC_QUERY { $$ = $1; }
        | float_literal         %prec BASIC_QUERY { $$ = $1; }
        | character_literal     %prec BASIC_QUERY { $$ = $1; }
        | string_literal        %prec BASIC_QUERY { $$ = $1; }
        | date_literal          %prec BASIC_QUERY { $$ = $1; }
        | time_literal          %prec BASIC_QUERY { $$ = $1; }
        | timestamp_literal     %prec BASIC_QUERY { $$ = $1; }
        | interval_literal      %prec BASIC_QUERY { $$ = $1; }
		| complex_literal		%prec BASIC_QUERY { $$ = $1; }
        | ID                    %prec BASIC_QUERY { $$ = $1; }
        | bind_argument         %prec BASIC_QUERY { $$ = $1; }
        | L_PAR query R_PAR     %prec BASIC_QUERY { $$ = $2; }
        ;

integer_literal
        : INTEGER       { $$ = $1; }
        ;
float_literal
        : REAL { $$ = $1; }
        ;
character_literal
        : CHARACTER { $$ = $1; }
        ;
string_literal
        : STRING { $$ = $1; }
        ;
date_literal
        : DATE STRING { $$ = ast_newNode( VaLtDate, $2 ); }
        ;
time_literal
        : TIME STRING { $$ = ast_newNode( VaLtTime, $2 ); }
        ;
timestamp_literal
        : TIMESTAMP STRING { $$ = ast_newNode( VaLtTimestamp, $2 ); }
        ;
interval_literal
        : INTERVAL STRING interval_precision { 
                first = ast_nodeCat( $2, $3 );
                $$ = ast_newNode( VaLtInterval, first ); 
                }
        ;
interval_precision
        : interval_precision_unit { $$ = $1; }
        | interval_precision_unit TO interval_precision_unit {
                $$ = ast_nodeCat( $1, $3 );
                }
        ;
interval_precision_unit
        : YEAR          { $$ = ast_newNode( IntervalYear, $1 ); }
        | MONTH         { $$ = ast_newNode( IntervalMonth, $1 ); }
        | DAY           { $$ = ast_newNode( IntervalDay, $1 ); }
        | HOUR          { $$ = ast_newNode( IntervalHour, $1 ); }
        | MINUTE        { $$ = ast_newNode( IntervalMinute, $1 ); }
        | SECOND        { $$ = ast_newNode( IntervalSecond, $1 ); }
        ;
complex_literal
		: L_BRACE literal_list R_BRACE	{ $$ = ast_newNode(VaLtComplex, $2) }
		;
literal_list
        :								{ $$ = ast_newNode(null, AST_NULL); }
		| literal						{ $$ = $1; }
		| literal_list COMMA literal	{ $$ = ast_nodeCat($1, $3); }
		;
literal : integer_literal				{ $$ = $1; }
        | float_literal					{ $$ = $1; }
        | character_literal				{ $$ = $1; }
        | string_literal				{ $$ = $1; }
        | date_literal					{ $$ = $1; }
        | time_literal					{ $$ = $1; }
        | timestamp_literal				{ $$ = $1; }
        | interval_literal				{ $$ = $1; }
		;

bind_argument
        : BINDARG { $$ = $1; }
        ;

/********************************
 *      Sequence                *
 ********************************/
sequence_query
		: ID DOT_CURRVAL			%prec BASIC_QUERY { $$ = ast_newNode(VaSeqCurr, $1); }
		| ID DOT_NEXTVAL			%prec BASIC_QUERY { $$ = ast_newNode(VaSeqNext, $1); }
		;


/********************************
 *      Simple Expression       *
 ********************************/

simple_expr_query
        : query PLUS query      %prec PLUS {
                first = ast_nodeCat( $1, $3 );
                first = ast_nodeCat( ast_newNode(OpBiPlu, AST_NULL), first );
                $$ = ast_newNode( QuSmp, first ); 
                }
        | query MINUS query {
                first = ast_nodeCat( $1, $3 );
                first = ast_nodeCat( ast_newNode(OpBiMin, AST_NULL), first );
                $$ = ast_newNode( QuSmp, first ); 
                }
        | query integer_float_literal {      
                if(yytext[0] == '+' || yytext[0] == '-')
                {
                    first = ast_nodeCat( $1, $2 );
                    first = ast_nodeCat( ast_newNode(OpBiPlu, AST_NULL), first );
                    $$ = ast_newNode( QuSmp, first ); 
                }
                else
                {
                    YYERROR;
                }
                }
        | query ASTERISK query {
                first = ast_nodeCat( $1, $3 );
                first = ast_nodeCat( ast_newNode(OpBiMul, AST_NULL), first );
                $$ = ast_newNode( QuSmp, first ); 
                }
        | query SLASH query {
                first = ast_nodeCat( $1, $3 );
                first = ast_nodeCat( ast_newNode(OpBiDiv, AST_NULL), first );
                $$ = ast_newNode( QuSmp, first ); 
                }
        | MINUS query           %prec UMINUS {
                first = ast_nodeCat( ast_newNode(OpUnMin, AST_NULL), $2 );
                $$ = ast_newNode( QuSmp, first ); 
                }
        | query MOD query {
                first = ast_nodeCat( $1, $3 );
                first = ast_nodeCat( ast_newNode(OpBiMod, AST_NULL), first );
                $$ = ast_newNode( QuSmp, first ); 
                }
        | ABS L_PAR query R_PAR {
                first = ast_nodeCat( ast_newNode(OpUnAbs, AST_NULL), $3 );
                $$ = ast_newNode( QuSmp, first ); 
                }
        | query STR_CAT query {
                first = ast_nodeCat( $1, $3 );
                first = ast_nodeCat( ast_newNode(OpBiStrcat, AST_NULL), first );
                $$ = ast_newNode( QuSmp, first ); 
                }
        ;

integer_float_literal : integer_literal { $$ = $1 }
                     | float_literal    { $$ = $1 }
                     
/************************
 *      Comparison      *
 ************************/

comparison_query
        : query EQ query { 
                first = ast_nodeCat( $1, $3 );
                first = ast_nodeCat( ast_newNode(OpCmpEq, AST_NULL), first );
                $$ = ast_newNode( QuCmp, first ); 
                }
        | query NE query { 
                first = ast_nodeCat( $1, $3 );
                first = ast_nodeCat( ast_newNode(OpCmpNe, AST_NULL), first );
                $$ = ast_newNode( QuCmp, first ); 
                }
        | query GT query { 
                first = ast_nodeCat( $1, $3 );
                first = ast_nodeCat( ast_newNode(OpCmpGt, AST_NULL), first );
                $$ = ast_newNode( QuCmp, first ); 
                }
        | query LT query { 
                first = ast_nodeCat( $1, $3 );
                first = ast_nodeCat( ast_newNode(OpCmpLt, AST_NULL), first );
                $$ = ast_newNode( QuCmp, first ); 
                }
        | query GE query { 
                first = ast_nodeCat( $1, $3 );
                first = ast_nodeCat( ast_newNode(OpCmpGe, AST_NULL), first );
                $$ = ast_newNode( QuCmp, first ); 
                }
        | query LE query { 
                first = ast_nodeCat( $1, $3 );
                first = ast_nodeCat( ast_newNode(OpCmpLe, AST_NULL), first );
                $$ = ast_newNode( QuCmp, first ); 
                }
        | query LIKE string_literal             %prec LIKE {
                first = ast_nodeCat( $1, $3 );
                first = ast_nodeCat( ast_newNode(OpCmpLike, AST_NULL), first );
                $$ = ast_newNode( QuCmp, first ); 
                }
		| query IS NULLVALUE {
				first = ast_nodeCat( ast_newNode(OpCmpIsNull, AST_NULL), $1 );
				$$ = ast_newNode( QuCmp, first );
				}
		| query IS NOT NULLVALUE {
				first = ast_nodeCat( ast_newNode(OpCmpIsNotNull, AST_NULL), $1 );
				$$ = ast_newNode( QuCmp, first );
				}
        ;

comparison_op
        : EQ { $$ = ast_newNode( OpCmpEq, AST_NULL ); }
        | NE { $$ = ast_newNode( OpCmpNe, AST_NULL ); }
        | GT { $$ = ast_newNode( OpCmpGt, AST_NULL ); }
        | LT { $$ = ast_newNode( OpCmpLt, AST_NULL ); }
        | GE { $$ = ast_newNode( OpCmpGe, AST_NULL ); }
        | LE { $$ = ast_newNode( OpCmpLe, AST_NULL ); }
        ;

/********************************
 *      Boolean Expression      *
 ********************************/

bool_expr_query
        : NOT query {
                first = ast_nodeCat( ast_newNode(OpBlnNot, AST_NULL), $2 );
                $$ = ast_newNode( QuBln, first ); 
                }
        | query AND query {
                first = ast_nodeCat( $1, $3 );
                first = ast_nodeCat( ast_newNode(OpBlnAnd, AST_NULL), first );
                $$ = ast_newNode( QuBln, first ); 
                }
        | query OR query {
                first = ast_nodeCat( $1, $3 );
                first = ast_nodeCat( ast_newNode(OpBlnOr, AST_NULL), first );
                $$ = ast_newNode( QuBln, first ); 
                }
        ;

/************************
 *      Constructor     *
 ************************/

constructor_query
        : type_cons     { $$ = $1; }
        | struct_cons   { $$ = $1; }
        | set_cons      { $$ = $1; }
        | bag_cons      { $$ = $1; }
        | list_cons     { $$ = $1; }
        | array_cons    { $$ = $1; }
        ;

type_cons
        : ID L_PAR type_arg_list R_PAR {
                first = ast_nodeCat($1, $3);
                $$ = ast_newNode(QuCnsObj, first); 
                } ;

struct_cons
        : STRUCT L_PAR type_arg_list R_PAR {
                $$ = ast_newNode(QuCnsStruct, $3); 
                } ;

set_cons
        : SET L_PAR opt_query_list R_PAR {
                $$ = ast_newNode(QuCnsSet, $3); 
                } ;

bag_cons
        : BAG L_PAR opt_query_list R_PAR {
                $$ = ast_newNode(QuCnsBag, $3); 
                } ;

list_cons
        : LIST L_PAR opt_query_list R_PAR {
                $$ = ast_newNode(QuCnsLst, $3); 
                }
        | L_PAR query COMMA query_list R_PAR {
                first = ast_nodeCat( $2, $4);
                $$ = ast_newNode( QuCnsLst, first ); 
                }
        | L_PAR query RANGE query R_PAR {
                first = ast_nodeCat( $2, $4 );
                $$ = ast_newNode( QuCnsLstrn, first ); 
                }
        | LIST L_PAR query RANGE query R_PAR {
                first = ast_nodeCat( $3, $5 );
                $$ = ast_newNode( QuCnsLstrn, first ); 
                } ;

array_cons
        : ARRAY L_PAR opt_query_list R_PAR {
                $$ = ast_newNode(QuCnsArr, $3); 
                } ;

type_arg_list
        : ID COLON query {
                first = ast_nodeCat( $1, $3 ); 
                }
        | ID COLON query COMMA type_arg_list {
                first = ast_nodeCat( $1, $3 );
                $$ = ast_nodeCat( first, $5 ); 
                } ;

query_list
        : query { $$ = $1; }
        | query COMMA query_list {
                $$ = ast_nodeCat( $1, $3 ); 
                } 
        ;

opt_query_list
        :               %prec EMPTY_RULE { $$ = ast_newNode(null, AST_NULL); }
        | query_list    { $$ = $1; } 
        ;


/************************
 *      Accessor        *
 ************************/

accessor_query
        /*  extended for 'extended path expression' (home-class substitution 
         *      and domain substitution) using typing expression of OQL. 
         *      NOTE: an example of typing expression: (Student)(person).name
         */
        : query dot ID                                  %prec DOT {
                first = ast_nodeCat( $1, $3 );
                $$ = ast_newNode( QuAccPaex, first ); 
                }
        | query dot ASTERISK                            %prec DOT {
                $$ = ast_newNode( QuAccPaexAll, $1 );
                }
        /* extended path expression: substituting with a class */
        | query L_BRACKET ID R_BRACKET dot ID           %prec DOT {
                first = ast_newNode( QuCnvType, ast_nodeCat($3, $1) );
                first = ast_nodeCat( first, $6 );
                $$ = ast_newNode( QuAccPaex, first ); 
                }
        /* extended path expression: substituting with a class hierarchy */
        | query L_BRACKET ID  ASTERISK R_BRACKET dot ID         %prec DOT {
                first = ast_newNode( QuCnvTypeStar, ast_nodeCat($3, $1) );
                first = ast_nodeCat( first, $7 );
                $$ = ast_newNode( QuAccPaex, first ); 
                }
        | query dot ID L_PAR opt_query_list R_PAR       %prec DOT {
                first = ast_nodeCat( $3, $5 );
                first = ast_nodeCat( $1, ast_newNode(Method, first) );
                $$ = ast_newNode( QuAccPaex, first ); 
                }
        /* extended path expression: substituting with a class */
        | query L_BRACKET ID R_BRACKET dot ID L_PAR opt_query_list R_PAR        %prec DOT {
                first = ast_newNode( QuCnvType, ast_nodeCat($3, $1) );
                first = ast_nodeCat( first, ast_newNode( Method, ast_nodeCat($6, $8) ) );
                $$ = ast_newNode( QuAccPaex, first ); 
                }
        /* extended path expression: substituting with a class hierarchy */
        | query L_BRACKET ID ASTERISK R_BRACKET dot ID L_PAR opt_query_list R_PAR       %prec DOT {
                first = ast_newNode( QuCnvTypeStar, ast_nodeCat($3, $1) );
                first = ast_nodeCat( first, ast_newNode( Method, ast_nodeCat($7, $9) ) );
                $$ = ast_newNode( QuAccPaex, first ); 
                }
        | ASTERISK query                                %prec UMINUS {
                $$ = ast_newNode(QuAccDref, $2); }
        | query L_BRACKET query R_BRACKET               {
                first = ast_nodeCat( $1, $3 );
                $$ = ast_newNode( QuAccIcltnElem, first ); 
                }
        | query L_BRACKET query COLON query R_BRACKET   {
                first = ast_nodeCat( $3, $5 );
                first = ast_nodeCat( $1, first );
                $$ = ast_newNode( QuAccIcltnSub, first ); 
                }
        | FIRST L_PAR query R_PAR {
                $$ = ast_newNode( QuAccIcltnFr, $3 ); }
        | LAST L_PAR query R_PAR {
                $$ = ast_newNode( QuAccIcltnLs, $3 ); }
        | ID L_PAR opt_query_list R_PAR {
                first = ast_nodeCat( $1, $3 );
                $$ = ast_newNode( FnOrCltnobj, first ); }
        ; 

dot
        : DOT { $$ = $1; }
        | R_ARROW { $$ = $1; }
        ;


/****************************************
 *      Collection Expression           *
 ****************************************/

collection_expr_query
        : FOR ALL ID IN query COLON query               %prec FOR {
                first = ast_nodeCat( $3, $5 );
                first = ast_nodeCat( first, $7 );
                $$ = ast_newNode( QuCltnAll, first ); 
                }
        | EXISTS ID IN query COLON query                %prec EXISTS {
                first = ast_nodeCat( $2, $4 );
                first = ast_nodeCat( first, $6 );
                $$ = ast_newNode( QuCltnEx, first ); 
                }
        | EXISTS L_PAR query R_PAR                      %prec EXISTS {
                $$ = ast_newNode( QuCltnExany, $3 ); 
                }
        | UNIQUE L_PAR query R_PAR                      %prec EXISTS {
                $$ = ast_newNode( QuCltnUni, $3 ); 
                }
        | query IN query                        {
                first = ast_nodeCat( $1, $3 );
                $$ = ast_newNode( QuCltnIn, first ); 
                }
		| query NOT IN query					{
				first = ast_nodeCat( $1, $4 );
				first = ast_newNode( QuCltnIn, first );
				first = ast_nodeCat( ast_newNode(OpBlnNot, AST_NULL), first );
				$$ = ast_newNode( QuBln, first );
				}
		| query comparison_op SOME query        %prec QUANTIFIED_COMP {
                first = ast_nodeCat( $1, $4 );
                $$ = ast_newNode( QuCltnCmpSome, ast_nodeCat($2, first) );
                }
        | query comparison_op ANY query         %prec QUANTIFIED_COMP { 
                first = ast_nodeCat( $1, $4 );
                $$ = ast_newNode( QuCltnCmpAny, ast_nodeCat($2, first) ); 
                }
        | query comparison_op ALL query         %prec QUANTIFIED_COMP { 
                first = ast_nodeCat( $1, $4 );
                $$ = ast_newNode( QuCltnCmpAll, ast_nodeCat($2, first) ); 
                }
        | COUNT L_PAR ASTERISK R_PAR {
                first = ast_newNode( FnAggCntall, AST_NULL );
                $$ = ast_newNode( QuCltnAgg, first ); 
                }
        | COUNT L_PAR DISTINCT ASTERISK R_PAR {
                first = ast_newNode( FnAggCntall, AST_NULL );
                $$ = ast_newNode( QuCltnAggDist, first ); 
                }
        | COUNT L_PAR query R_PAR {
                first = ast_nodeCat( ast_newNode(FnAggCnt, AST_NULL), $3 );
                $$ = ast_newNode( QuCltnAgg, first ); 
                }
        | COUNT L_PAR ALL query R_PAR {
                first = ast_nodeCat( ast_newNode(FnAggCnt, AST_NULL), $4 );
                $$ = ast_newNode( QuCltnAgg, first ); 
                }
        | COUNT L_PAR DISTINCT query R_PAR {
                first = ast_nodeCat( ast_newNode(FnAggCnt, AST_NULL), $4 );
                $$ = ast_newNode( QuCltnAggDist, first ); 
                }
        | aggregate_func L_PAR query R_PAR {
                first = ast_nodeCat( $1, $3 );
                $$ = ast_newNode( QuCltnAgg, first ); 
                }
        | aggregate_func L_PAR ALL query R_PAR {
                first = ast_nodeCat( $1, $4 );
                $$ = ast_newNode( QuCltnAgg, first ); 
                }
        | aggregate_func L_PAR DISTINCT query R_PAR {
                first = ast_nodeCat( $1, $4 );
                $$ = ast_newNode( QuCltnAggDist, first ); 
                }
        ;

aggregate_func
/* Though 'count' is an aggregate function, we exclude it from the following rule
 *      to eliminate the shift/reduce conflict between 'count(*)' and 'count([all | distinct] ..)'.
 */
        : SUM { $$ = ast_newNode(FnAggSum, AST_NULL); }
        | MIN { $$ = ast_newNode(FnAggMin, AST_NULL); }
        | MAX { $$ = ast_newNode(FnAggMax, AST_NULL); }
        | AVG { $$ = ast_newNode(FnAggAvg, AST_NULL); }
        ;


/********************************
 *      Select Expression       *
 ********************************/
select_expr_query
        : SELECT projection_list
          FROM from_var_declaration_list
          opt_where_clause
          opt_group_by_clause
          opt_having_clause
          opt_order_by_clause
          opt_limit_clause                           %prec SELECT {
                first = ast_newNode( ClSelAll, $2 );
                first = ast_nodeCat( first, ast_newNode(ClFr, $4) );
                first = ast_nodeCat( first, $5 );
                first = ast_nodeCat( first, $6 );
                first = ast_nodeCat( first, $7 );
                first = ast_nodeCat( first, $8 );
                first = ast_nodeCat( first, $9 );
                $$ = ast_newNode(QuSel, first); 
                }
        | SELECT projection_list
		  FROM OBJECT object_id							%prec SELECT {
				first = ast_nodeCat($5, AST_NULL);
				first = ast_newNode(FrObject, first);
				first = ast_newNode(ClFr, first);
				first = ast_nodeCat(ast_newNode(ClSelAll, $2), 
											ast_nodeCat(first, 
												ast_nodeCat(ast_newNode(null, AST_NULL), /* where */
													ast_nodeCat(ast_newNode(null, AST_NULL), /* group by */
														ast_nodeCat(ast_newNode(null, AST_NULL), /* having */
															ast_nodeCat(ast_newNode(null, AST_NULL), /* order by */
																ast_newNode(null, AST_NULL)  /* limit */
															)
														)
													)
												)
											)
									);
				$$ = ast_newNode(QuSel, first); 
				}
        | SELECT DISTINCT projection_list
          FROM from_var_declaration_list
          opt_where_clause
          opt_group_by_clause
          opt_having_clause
          opt_order_by_clause
          opt_limit_clause                           %prec SELECT {
                first = ast_newNode( ClSelDist, $3 );
                first = ast_nodeCat( first, ast_newNode(ClFr, $5) );
                first = ast_nodeCat( first, $6 );
                first = ast_nodeCat( first, $7 );
                first = ast_nodeCat( first, $8 );
                first = ast_nodeCat( first, $9 );
                first = ast_nodeCat( first, $10 );
                $$ = ast_newNode(QuSel, first); 
                } 
        ;

projection_list
        : projection                            %prec SELECT { $$ = $1; }
        | projection COMMA projection_list { $$ = ast_nodeCat( $1, $3 ); }
        ;

projection
        : ASTERISK                              {
                            $$ = ast_newNode(ProAll, AST_NULL);
                                            }
        | SHARP                                 {
                            $$ = ast_newNode(ProAllLogicalID, AST_NULL);
                                            }
        | query                         %prec SELECT {
                first = $1;
                $$ = ast_newNode(ProSmp, first); 
                }
        | ID COLON query                %prec COLON {   
                first = ast_nodeCat( $1, $3 );
                $$ = ast_newNode( ProAs, first ); 
                }
        | query AS ID                   %prec AS {
                first = ast_nodeCat( $3, $1 );
                $$ = ast_newNode( ProAs, first ); 
                }
        ;

from_var_declaration_list
        : variable_declaration                          %prec FROM { $$ = $1; }
        | variable_declaration COMMA from_var_declaration_list  { $$ = ast_nodeCat( $1, $3 ); }
        ;

variable_declaration
        : query opt_correlation { 
                first = ast_nodeCat( $1, $2 );
                $$ = ast_newNode( FrCltn, first );
                }
        | query ASTERISK opt_correlation        %prec FROM_CLTN_STAR { 
                first = ast_nodeCat( $1, $3);
                $$ = ast_newNode( FrCltnStar, first );
                }
        | query ASTERISK ID     %prec FROM_CLTN_STAR { 
                first = ast_nodeCat( $1, $3);
                $$ = ast_newNode( FrCltnStar, first );
                }
        ;

opt_correlation
        :               %prec EMPTY_RULE { $$ = ast_newNode(null, AST_NULL); }
        | ID            { $$ = $1; }
        | AS ID         { $$ = $2; }
        ;

opt_where_clause
        :               %prec EMPTY_RULE { $$ = ast_newNode(null, AST_NULL); }
        | WHERE query {
                first = $2;
                $$ = ast_newNode(ClWh, first); 
                }
        ;

opt_group_by_clause
        :                                       %prec EMPTY_RULE { 
                $$ = ast_newNode(null, AST_NULL); 
                }
        | GROUP BY partition_attributes         %prec GROUP {
                first = $3;
                $$ = ast_newNode(ClGrp, first); 
                }
        ;

opt_having_clause
        :                                       %prec EMPTY_RULE { 
                $$ = ast_newNode(null, AST_NULL); 
                }
        | HAVING query {
                first = $2;
                $$ = ast_newNode(ClHav, first); 
                }
        ;

opt_order_by_clause
        :                                       %prec EMPTY_RULE { 
                $$ = ast_newNode(null, AST_NULL); 
                }
        | ORDER BY sort_criterion_list          %prec ORDER {
                first = $3;
                $$ = ast_newNode(ClOrd, first); 
                }
        ;

opt_limit_clause
        :                                       %prec EMPTY_RULE { 
                $$ = ast_newNode(null, AST_NULL); 
                }
        | LIMIT integer_literal {
                first = $2;
                $$ = ast_newNode(ClLim, first); 
                }
        ;

partition_attributes
        : projection_list { $$ = $1; }
        ;

sort_criterion_list
        : sort_criterion                                %prec ORDER { $$ = $1; }
        | sort_criterion COMMA sort_criterion_list { $$ = ast_nodeCat( $1, $3 ); }
        ;

sort_criterion 
        : query opt_ordering            %prec ORDER { $$ = ast_nodeCat( $1, $2 ); }
        ;

opt_ordering
        :       %prec EMPTY_RULE { $$ = ast_newNode( OrdAsc, AST_NULL ); }
        | ASC   { $$ = ast_newNode( OrdAsc, AST_NULL ); }
        | DESC  { $$ = ast_newNode( OrdDesc, AST_NULL ); }
        ;

/********************************
 *      Set Expression  *
 ********************************/

set_expr_query
        : query INTERSECT query {
                first = ast_nodeCat( $1, $3 );
                first = ast_nodeCat( ast_newNode(OpSetInt, AST_NULL), first );
                $$ = ast_newNode( QuSet, first ); 
                }
        | query UNION query {
                first = ast_nodeCat( $1, $3 );
                first = ast_nodeCat( ast_newNode(OpSetUni, AST_NULL), first );
                $$ = ast_newNode( QuSet, first ); 
                }
        | query EXCEPT query {
                first = ast_nodeCat( $1, $3 );
                first = ast_nodeCat( ast_newNode(OpSetExc, AST_NULL), first );
                $$ = ast_newNode( QuSet, first ); 
                }
        ;

/********************************
 *      Conversion              *
 ********************************/

conversion_query
        : LISTTOSET L_PAR query R_PAR           {
                first = $3;
                $$ = ast_newNode(QuCnvL2s, first); 
                }
        | ELEMENT L_PAR query R_PAR             {
                first = $3;
                $$ = ast_newNode(QuCnvElem, first); 
                }
        | DISTINCT L_PAR query R_PAR            %prec DISTINCT_FUNCTION {
                first = $3;
                $$ = ast_newNode(QuCnvDist, first); 
                }
        | FLATTEN L_PAR query R_PAR             {
                first = $3;
                $$ = ast_newNode(QuCnvFlat, first); 
                }
        | L_PAR ID R_PAR query                  %prec TYPE_CAST {
                first = ast_nodeCat( $2, $4 );
                $$ = ast_newNode( QuCnvTypeStar, first ); 
                }
        ;


/****************************************
 *      IR                              *
 ****************************************/

ir_query
        : ir_function { $$ = $1; }
        ;

ir_function
		: NMATCH L_PAR query COMMA STRING R_PAR {
                first = ast_nodeCat( $3, $5 );
                $$ = ast_newNode( QuIrFnNmatch, first );
                }
        | NMATCH L_PAR query COMMA STRING COMMA INTEGER R_PAR {
                first = ast_nodeCat( $3, $5 );
                first = ast_nodeCat( first, $7 );
                $$ = ast_newNode( QuIrFnNmatch, first );
                }
        | MATCH L_PAR query COMMA ir_bool_expr R_PAR {
                first = ast_nodeCat( $3, $5 );
                first = ast_nodeCat( first, ast_newNode(null, AST_NULL) );
                first = ast_nodeCat( first, ast_newNode(null, AST_NULL) );
                $$ = ast_newNode( QuIrFnMatch, first );
                }
        | MATCH L_PAR query COMMA ir_bool_expr COMMA ir_scan_direction R_PAR {
                first = ast_nodeCat( $3, $5 );
                first = ast_nodeCat( first, ast_newNode(null, AST_NULL) );
                first = ast_nodeCat( first, $7 );
                $$ = ast_newNode( QuIrFnMatch, first );
                }
        | MATCH L_PAR query COMMA ir_bool_expr COMMA INTEGER R_PAR {
                first = ast_nodeCat( $3, $5 );
                first = ast_nodeCat( first, $7 );
                first = ast_nodeCat( first, ast_newNode(null, AST_NULL) );
                $$ = ast_newNode( QuIrFnMatch, first );
                }
        | MATCH L_PAR query COMMA ir_bool_expr COMMA INTEGER COMMA ir_scan_direction R_PAR {
                first = ast_nodeCat( $3, $5 );
                first = ast_nodeCat( first, $7 );
                first = ast_nodeCat( first, $9 );
                $$ = ast_newNode( QuIrFnMatch, first );
                }
        | WEIGHT L_PAR R_PAR {
                $$ = ast_newNode( QuIrFnWeight, AST_NULL );
                }
        | WEIGHT L_PAR INTEGER R_PAR {
                $$ = ast_newNode( QuIrFnWeight, $3 );
                }
        ;

ir_bool_expr
        : ir_bool_or_term { $$ = $1; }
        | ir_bool_expr PLUS ir_bool_or_term     %prec TEXTIR_ACCUMULATE {
                first = ast_nodeCat( $1, $3 );
                $$ = ast_newNode( OpIrBlnAccum, first );
                }
        ;

ir_bool_or_term
        : ir_bool_and_term { $$ = $1; }
        | ir_bool_or_term VERT_BAR ir_bool_and_term     %prec TEXTIR_OR {
                first = ast_nodeCat( $1, $3 );
                $$ = ast_newNode( OpIrBlnOr, first );
                }
        ;

ir_bool_and_term
        : ir_bool_minus_term { $$ = $1; }
        | ir_bool_and_term AMPER ir_bool_minus_term     %prec TEXTIR_AND {
                first = ast_nodeCat( $1, $3 );
                $$ = ast_newNode( OpIrBlnAnd, first );
                }
        ;

ir_bool_minus_term
        : ir_bool_factor { $$ = $1; }
        | ir_bool_minus_term MINUS ir_bool_factor       %prec TEXTIR_MINUS {
                first = ast_nodeCat( $1, $3 );
                $$ = ast_newNode( OpIrBlnMinus, first );
                }
        ;

ir_bool_factor
        : ir_bool_near_factor { $$ = $1; }
        | ir_bool_in_factor { $$ = $1; }
        | ir_bool_factor GT ir_bool_near_factor         %prec TEXTIR_THRESHOLD {
                first = ast_nodeCat( $1, $3 );
                $$ = ast_newNode( OpIrBlnThreshold, first );
                }
        | ir_bool_factor ASTERISK ir_bool_near_factor   %prec TEXTIR_MULTIPLY {
                first = ast_nodeCat( $1, $3 );
                $$ = ast_newNode( OpIrBlnMultiply, first );
                }
        | ir_bool_factor COLON ir_bool_near_factor      %prec TEXTIR_MAX {
                first = ast_nodeCat( $1, $3 );
                $$ = ast_newNode( OpIrBlnMax, first );
                }
        ;

ir_bool_near_factor
        : ir_bool_primary { $$ = $1; }
        | ir_bool_near_factor TILD INTEGER ir_bool_near_factor  %prec TEXTIR_NEAR {
                first = ast_nodeCat( $1, $4 );
                first = ast_nodeCat( first, $3 );
                $$ = ast_newNode( OpIrBlnNear, first );
                }
        | ir_bool_near_factor CAROT INTEGER ir_bool_near_factor %prec TEXTIR_NEAR {
                first = ast_nodeCat( $1, $4 );
                first = ast_nodeCat( first, $3 );
                $$ = ast_newNode( OpIrBlnNearWithOrder, first );
                }
        ;

ir_bool_in_factor
        : ir_bool_primary { $$ = $1; }
        | ir_bool_primary L_BRACKET basic_query COMMA basic_query COMMA basic_query COMMA basic_query R_BRACKET %prec TEXTIR_NEAR {
                first = ast_nodeCat( $1, $3 );
                first = ast_nodeCat( first, $5 );
                first = ast_nodeCat( first, $7 );
                first = ast_nodeCat( first, $9 );
                $$ = ast_newNode( OpIrBlnUnIn, first );
                }
        ;

ir_bool_primary
    : ir_bool_literal    %prec IR_BOOL_PRIMARY { $$ = $1; }
		| ir_keyword_between				{ $$ = $1; }
        | L_PAR ir_bool_expr R_PAR      %prec IR_BOOL_PRIMARY { $$ = $2; }
        ;

ir_bool_literal
    : integer_literal
    | float_literal
    | character_literal
    | string_literal
    ;



ir_keyword_between
		: BETWEEN L_PAR STRING COMMA STRING R_PAR {
				first = ast_nodeCat($3, $5);
				$$ = ast_newNode(OpIrBetween, first);
				}
		;
		
ir_scan_direction
		/* to read postings in reverse order */
		: FORWARDORDER		{ $$ = ast_newNode( OpIrScanForward, AST_NULL ); }
		| BACKWARDORDER		{ $$ = ast_newNode( OpIrScanBackward, AST_NULL ); }
		;
				
/****************************************
 *      OpenGIS                         *
 ****************************************/

ogis_query
		: ogis_translatable_operator	{ $$ = ast_newNode(QuOgisTranslatableOp, $1); }
		| ogis_geometric_operator		{ $$ = ast_newNode(QuOgisGeometricOp, $1); }
		| ogis_relational_operator		{ $$ = ast_newNode(QuOgisRelationalOp, $1); }
		| ogis_miscellaneous_operator	{ $$ = ast_newNode(QuOgisMiscellaneousOp, $1); }
		;


/* Operator1 */
ogis_translatable_operator
		: OGIS_GEOMETRYFROMTEXT L_PAR query COMMA query R_PAR {
				first = ast_nodeCat($3, $5);
				$$ = ast_newNode(QuOgisTrGeometryFT, first);
				}
		| OGIS_POINTFROMTEXT L_PAR query COMMA query R_PAR {
				first = ast_nodeCat($3, $5);
				$$ = ast_newNode(QuOgisTrPointFT, first);
				}
		| OGIS_LINESTRINGFROMTEXT L_PAR query COMMA query R_PAR {
				first = ast_nodeCat($3, $5);
				$$ = ast_newNode(QuOgisTrLineStringFT, first);
				}
		| OGIS_POLYGONFROMTEXT L_PAR query COMMA query R_PAR {
				first = ast_nodeCat($3, $5);
				$$ = ast_newNode(QuOgisTrPolygonFT, first);
				}
		| OGIS_MULTIPOINTFROMTEXT L_PAR query COMMA query R_PAR {
				first = ast_nodeCat($3, $5);
				$$ = ast_newNode(QuOgisTrMultiPointFT, first);
				}
		| OGIS_MULTILINESTRINGFROMTEXT L_PAR query COMMA query R_PAR {
				first = ast_nodeCat($3, $5);
				$$ = ast_newNode(QuOgisTrMultiLineStringFT, first);
				}
		| OGIS_MULTIPOLYGONFROMTEXT L_PAR query COMMA query R_PAR {
				first = ast_nodeCat($3, $5);
				$$ = ast_newNode(QuOgisTrMultiPolygonFT, first);
				}
		| OGIS_GEOMETRYCOLLECTIONFROMTEXT L_PAR query COMMA query R_PAR {
				first = ast_nodeCat($3, $5);
				$$ = ast_newNode(QuOgisTrGeometryCollectionFT, first);
				}
		| OGIS_GEOMETRYFROMWKB L_PAR query COMMA query R_PAR {
				first = ast_nodeCat($3, $5);
				$$ = ast_newNode(QuOgisTrGeometryFB, first);
				}
		| OGIS_POINTFROMWKB L_PAR query COMMA query R_PAR {
				first = ast_nodeCat($3, $5);
				$$ = ast_newNode(QuOgisTrPointFB, first);
				}
		| OGIS_LINESTRINGFROMWKB L_PAR query COMMA query R_PAR {
				first = ast_nodeCat($3, $5);
				$$ = ast_newNode(QuOgisTrLineStringFB, first);
				}
		| OGIS_POLYGONFROMWKB L_PAR query COMMA query R_PAR {
				first = ast_nodeCat($3, $5);
				$$ = ast_newNode(QuOgisTrPolygonFB, first);
				}
		| OGIS_MULTIPOINTFROMWKB L_PAR query COMMA query R_PAR {
				first = ast_nodeCat($3, $5);
				$$ = ast_newNode(QuOgisTrMultiPointFB, first);
				}
		| OGIS_MULTILINEFROMWKB L_PAR query COMMA query R_PAR {
				first = ast_nodeCat($3, $5);
				$$ = ast_newNode(QuOgisTrMultiLineStringFB, first);
				}
		| OGIS_MULTIPOLYGONFROMWKB L_PAR query COMMA query R_PAR {
				first = ast_nodeCat($3, $5);
				$$ = ast_newNode(QuOgisTrMultiPolygonFB, first);
				}
		| OGIS_GEOMETRYCOLLECTIONFROMWKB L_PAR query COMMA query R_PAR {
				first = ast_nodeCat($3, $5);
				$$ = ast_newNode(QuOgisTrGeometryCollectionFB, first);
				}
		| OGIS_ASTEXT L_PAR query R_PAR {
				first = $3;
				$$ = ast_newNode(QuOgisTrAsText, first);
				}
		| OGIS_ASBINARY L_PAR query R_PAR {
				first = $3;
				$$ = ast_newNode(QuOgisTrAsBinary, first);
				}
		;

/* Operator2 */
ogis_geometric_operator
		: OGIS_DIMENSION L_PAR query R_PAR {
				first = $3;
				$$ = ast_newNode(QuOgisGeDimension, first);
				}
		| OGIS_GEOMETRYTYPE L_PAR query R_PAR {
				first = $3;
				$$ = ast_newNode(QuOgisGeGeometryType, first);
				}
		| OGIS_SRID L_PAR query R_PAR {
				first = $3;
				$$ = ast_newNode(QuOgisGeSRID, first);
				}
		| OGIS_BOUNDARY L_PAR query R_PAR {
				first = $3;
				$$ = ast_newNode(QuOgisGeBoundary, first);
				}
		| OGIS_LENGTH L_PAR query R_PAR {
				first = $3;
				$$ = ast_newNode(QuOgisGeLength, first);
				}
		| OGIS_X L_PAR query R_PAR {
				first = $3;
				$$ = ast_newNode(QuOgisGeX, first);
				}
		| OGIS_Y L_PAR query R_PAR {
				first = $3;
				$$ = ast_newNode(QuOgisGeY, first);
				}
		| OGIS_AREA L_PAR query R_PAR {
				first = $3;
				$$ = ast_newNode(QuOgisGeArea, first);
				}
		| OGIS_NUMGEOMETRIES L_PAR query R_PAR {
				first = $3;
				$$ = ast_newNode(QuOgisGeNumGeometries, first);
				}
		| OGIS_NUMPOINTS L_PAR query R_PAR {
				first = $3;
				$$ = ast_newNode(QuOgisGeNumPoints, first);
				}
		| OGIS_NUMINTERIORRINGS L_PAR query R_PAR {
				first = $3;
				$$ = ast_newNode(QuOgisGeNumInteriorRings, first);
				}
		;

/* Operator3 */
ogis_relational_operator
		: OGIS_ISEMPTY L_PAR query R_PAR {
				first = $3;
				$$ = ast_newNode(QuOgisReIsEmpty, first);
				}
		| OGIS_ISSIMPLE L_PAR query R_PAR {
				first = $3;
				$$ = ast_newNode(QuOgisReIsSimple, first);
				}
		| OGIS_ISCLOSED L_PAR query R_PAR {
				first = $3;
				$$ = ast_newNode(QuOgisReIsClosed, first);
				}
		| OGIS_ISRING L_PAR query R_PAR {
				first = $3;
				$$ = ast_newNode(QuOgisReIsRing, first);
				}
		| OGIS_CONTAINS L_PAR query COMMA query R_PAR {
				first = ast_nodeCat($3, $5);
				$$ = ast_newNode(QuOgisReContains, first);
				}
		| OGIS_CROSSES L_PAR query COMMA query R_PAR {
				first = ast_nodeCat($3, $5);
				$$ = ast_newNode(QuOgisReCrosses, first);
				}
		| OGIS_DISJOINT L_PAR query COMMA query R_PAR {
				first = ast_nodeCat($3, $5);
				$$ = ast_newNode(QuOgisReDisjoint, first);
				}
		| OGIS_EQUALS L_PAR query COMMA query R_PAR {
				first = ast_nodeCat($3, $5);
				$$ = ast_newNode(QuOgisReEquals, first);
				}
		| OGIS_INTERSECTS L_PAR query COMMA query R_PAR {
				first = ast_nodeCat($3, $5);
				$$ = ast_newNode(QuOgisReIntersects, first);
				}
		| OGIS_OVERLAPS L_PAR query COMMA query R_PAR {
				first = ast_nodeCat($3, $5);
				$$ = ast_newNode(QuOgisReOverlaps, first);
				}
		| OGIS_RELATED L_PAR query COMMA query COMMA query R_PAR {
				first = ast_nodeCat($3, $5);
				first = ast_nodeCat(first, $7);
				$$ = ast_newNode(QuOgisReRelated, first);
				}
		| OGIS_TOUCHES L_PAR query COMMA query R_PAR {
				first = ast_nodeCat($3, $5);
				$$ = ast_newNode(QuOgisReTouches, first);
				}
		| OGIS_WITHIN L_PAR query COMMA query R_PAR {
				first = ast_nodeCat($3, $5);
				$$ = ast_newNode(QuOgisReWithin, first);
				}
		;

/* Operator4 */
ogis_miscellaneous_operator
		: OGIS_DIFFERENCE L_PAR query COMMA query R_PAR {
				first = ast_nodeCat($3, $5);
				$$ = ast_newNode(QuOgisMiDifference, first);
				}
		| OGIS_INTERSECTION L_PAR query COMMA query R_PAR {
				first = ast_nodeCat($3, $5);
				$$ = ast_newNode(QuOgisMiIntersection, first);
				}
		| OGIS_SYMDIFFERENCE L_PAR query COMMA query R_PAR {
				first = ast_nodeCat($3, $5);
				$$ = ast_newNode(QuOgisMiSymDifference, first);
				}
		| OGIS_UNION L_PAR query COMMA query R_PAR {
				first = ast_nodeCat($3, $5);
				$$ = ast_newNode(QuOgisMiUnion, first);
				}
		| OGIS_DISTANCE L_PAR query COMMA query R_PAR {
				first = ast_nodeCat($3, $5);
				$$ = ast_newNode(QuOgisMiDistance, first);
				}
		| OGIS_ENVELOPE L_PAR query R_PAR {
				first = $3;
				$$ = ast_newNode(QuOgisMiEnvelope, first);
				}
		| OGIS_BUFFER L_PAR query COMMA float_literal R_PAR {
				first = ast_nodeCat($3, $5);
				$$ = ast_newNode(QuOgisMiBuffer, first);
				}
		| OGIS_CONVEXHULL L_PAR query R_PAR {
				first = $3;
				$$ = ast_newNode(QuOgisMiConvexHull, first);
				}
		| OGIS_EXTERIORRING L_PAR query R_PAR {
				first = $3;
				$$ = ast_newNode(QuOgisMiExteriorRing, first);
				}
		| OGIS_INTERIORRINGN L_PAR query R_PAR {
				first = $3;
				$$ = ast_newNode(QuOgisMiInteriorRingN, first);
				}
		| OGIS_CENTROID L_PAR query R_PAR {
				first = $3;
				$$ = ast_newNode(QuOgisMiCentriod, first);
				}
		| OGIS_STARTPOINT L_PAR query R_PAR {
				first = $3;
				$$ = ast_newNode(QuOgisMiStartPoint, first);
				}
		| OGIS_ENDPOINT L_PAR query R_PAR {
				first = $3;
				$$ = ast_newNode(QuOgisMiEndPoint, first);
				}
		| OGIS_POINTONSURFACE L_PAR query R_PAR {
				first = $3;
				$$ = ast_newNode(QuOgisMiPointOnSurface, first);
				}
		| OGIS_POINTN L_PAR query COMMA query R_PAR {
				first = ast_nodeCat($3, $5);
				$$ = ast_newNode(QuOgisMiPointN, first);
				}
		| OGIS_GEOMETRYN L_PAR query COMMA query R_PAR {
				first = ast_nodeCat($3, $5);
				$$ = ast_newNode(QuOgisMiGeometryN, first);
				}
		;

%%


extern int yychar;
extern int LINE_NUM;
extern int COLM_NUM;

yyerror(char* s)
{
    int i;

    if(yychar <= 999)
	{
        while (yychar != 0) yychar = yylex();

        return eSYNTAX_ERROR_OQL;
    }
    return ePARSE_ERROR_OQL;
}

#include "lex.yy.c"
#include "hash.c"
#include "OQL_AST.c"
#include "print_ast.c" 
