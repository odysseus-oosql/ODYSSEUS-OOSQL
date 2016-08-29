/******************************************************************************/
/*                                                                            */
/*    Copyright (c) 1990-2016, KAIST                                          */
/*    All rights reserved.                                                    */
/*                                                                            */
/*    Redistribution and use in source and binary forms, with or without      */
/*    modification, are permitted provided that the following conditions      */
/*    are met:                                                                */
/*                                                                            */
/*    1. Redistributions of source code must retain the above copyright       */
/*       notice, this list of conditions and the following disclaimer.        */
/*                                                                            */
/*    2. Redistributions in binary form must reproduce the above copyright    */
/*       notice, this list of conditions and the following disclaimer in      */
/*       the documentation and/or other materials provided with the           */
/*       distribution.                                                        */
/*                                                                            */
/*    3. Neither the name of the copyright holder nor the names of its        */
/*       contributors may be used to endorse or promote products derived      */
/*       from this software without specific prior written permission.        */
/*                                                                            */
/*    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS     */
/*    "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT       */
/*    LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS       */
/*    FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE          */
/*    COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,    */
/*    INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,    */
/*    BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;        */
/*    LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER        */
/*    CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT      */
/*    LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN       */
/*    ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE         */
/*    POSSIBILITY OF SUCH DAMAGE.                                             */
/*                                                                            */
/******************************************************************************/
/******************************************************************************/
/*                                                                            */
/*    ODYSSEUS/OOSQL DB-IR-Spatial Tightly-Integrated DBMS                    */
/*    Version 5.0                                                             */
/*                                                                            */
/*    Developed by Professor Kyu-Young Whang et al.                           */
/*                                                                            */
/*    Advanced Information Technology Research Center (AITrc)                 */
/*    Korea Advanced Institute of Science and Technology (KAIST)              */
/*                                                                            */
/*    e-mail: odysseus.oosql@gmail.com                                        */
/*                                                                            */
/*    Bibliography:                                                           */
/*    [1] Whang, K., Lee, J., Lee, M., Han, W., Kim, M., and Kim, J., "DB-IR  */
/*        Integration Using Tight-Coupling in the Odysseus DBMS," World Wide  */
/*        Web, Vol. 18, No. 3, pp. 491-520, May 2015.                         */
/*    [2] Whang, K., Lee, M., Lee, J., Kim, M., and Han, W., "Odysseus: a     */
/*        High-Performance ORDBMS Tightly-Coupled with IR Features," In Proc. */
/*        IEEE 21st Int'l Conf. on Data Engineering (ICDE), pp. 1104-1105     */
/*        (demo), Tokyo, Japan, April 5-8, 2005. This paper received the Best */
/*        Demonstration Award.                                                */
/*    [3] Whang, K., Park, B., Han, W., and Lee, Y., "An Inverted Index       */
/*        Storage Structure Using Subindexes and Large Objects for Tight      */
/*        Coupling of Information Retrieval with Database Management          */
/*        Systems," U.S. Patent No.6,349,308 (2002) (Appl. No. 09/250,487     */
/*        (1999)).                                                            */
/*    [4] Whang, K., Lee, J., Kim, M., Lee, M., Lee, K., Han, W., and Kim,    */
/*        J., "Tightly-Coupled Spatial Database Features in the               */
/*        Odysseus/OpenGIS DBMS for High-Performance," GeoInformatica,        */
/*        Vol. 14, No. 4, pp. 425-446, Oct. 2010.                             */
/*    [5] Whang, K., Lee, J., Kim, M., Lee, M., and Lee, K., "Odysseus: a     */
/*        High-Performance ORDBMS Tightly-Coupled with Spatial Database       */
/*        Features," In Proc. 23rd IEEE Int'l Conf. on Data Engineering       */
/*        (ICDE), pp. 1493-1494 (demo), Istanbul, Turkey, Apr. 16-20, 2007.   */
/*                                                                            */
/******************************************************************************/

#include	<stdio.h>
#include	"OQL.lex.h"
#include	"OQL.yacc.h"
#include	"OOSQL_Error.h"

EntryPtr HashTable[MAXHASHBUCKET];	/* hash table                   */
int *StrIndex;                  	/* pointer of string index table*/
int StrIdxTop;                  	/* top of string index table    */
int MAXIDX;                     	/* maximum size of index table  */

/* For ID String Table */

char *StrPool;						/* pointer if string symbol table */
int StringTop;						/* top of string table            */
int StrEntryCnt;					/* the number of element of table */
int MAXSTR;							/* maximum size of table          */

/* For Integer Table */

long *IntPool;						/* integer symbol table           */
int IntTop;							/* top of table                   */
int IntEntryCnt;					/* the number of element of table */
int maxInt;							/* maximum size of table          */

/* For Real Table */

float *RealPool;					/* real symbol table            */
int RealTop;						/* top of table                 */
int RealEntryCnt;					/* the number of element        */
int MAXREAL;						/* maximum size of table        */

extern char* oql_query_buffer;	/* extern varaible */

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
ePARSE_ERROR_OOSQL  
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
int c_parser(
	char*		str,	/* IN */
	ASTNodeIdx*	node	/* OUT parse tree의 root node */
)
{
	int i;

	/* initialize memory to save input query */
	oql_query_buffer_init(str);

    InitHash();         /* initialize hash table before parsing */

	/* initialize AST node pool */
	initAstNodePool();

   	i = yyparse();      /* return 0 if worked,
                           1, if syntax error not recovered from. */
	if (i == 1) return ePARSE_ERROR_OOSQL;	

#ifdef	AST_DEBUG	
	printf("\n");
	print_ast1( root, 0, 0); 
	//file_out( root );	// file로 쓸생각이 없다면 계속 닫아 둔다.
#endif

	*node = root;

	return eNOERROR;
}

/*
	free memory which allocated in parser routine
*/
/****************************************************************************
DESCRIPTION:

RETURN VALUE:
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
int free_parser()
{
	/* free buffers 
	 * free memory for StrIndex
	 */
	FinalHash();

	/* free ast tree */
	if (root != NULL) free_ast(root);
	root = NULL;

	/* deallocate query buffer */
	oql_query_buffer_final();

	return 0;
}

/*
	free ast tree recursively
*/
/****************************************************************************
DESCRIPTION:

RETURN VALUE:
< eNOERROR         

IMPLEMENTATION:
****************************************************************************/
int free_ast()
{
	finalAstNodePool();

	return 0;
}


