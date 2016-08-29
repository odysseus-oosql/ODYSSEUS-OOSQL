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

/*--------------------------------------------------------*/
/*    print_ast is a routine which print AST whose root	  */
/*	node is given.					  */
/*    -  parameter					  */
/*	 which : this tree is the eldest brother or not	  */
/*       level : the level of current root		  */
/*	     these are needed to recursive call	  	  */
/*    -  print_ast1 : prints node name as defined AST	  */
/*			print_dummy.h			  */
/*    -  print_ast2 : prints node name as readable form	  */
/*			print_ast.h			  */
/*--------------------------------------------------------*/

#include "OQL_AST.h"

void print_ast1( r, which, level)
 ASTNodeIdx r;
 int which, level;
{
    ASTNodeIdx t, b;
    void tabs(), printname();
    
    t = r;
    
    switch (AST(t).nodeName) {
#include "print_ast.h"
    }
    
    /*------------------------------------------------------*/
    /*     This part is a recursive call			*/
    /*               for son and brother part		*/
    /*------------------------------------------------------*/
    if (AST(t).son != AST_NULL)
	print_ast1(AST(t).son, 1, level+1);
    else
	printf("\n");
    
    if (AST(t).brother != AST_NULL)
	print_ast1(AST(t).brother, 1, level);
}

ASTNodeIdx	astQueue[1000];
int		front = 0, rear = 0;
int		queueUniueNumber = 0;

void AddQueue( node )
ASTNodeIdx node;
{
	if (rear < 1000) {
		AST(node).uniqueNum = queueUniueNumber++;
		astQueue[rear++] = node;
	}
	else {
		printf("\n[in OQL Parser] AST queue is full...\n");
		exit(1);
	}
}

ASTNodeIdx	DeleteQueue()
{
	if (front < rear) {
		return astQueue[front++];
	}
	else {
		printf("\n[in OQL Parser] AST queue is empty...\n");
		return AST_NULL;
	}
}

int	IsEmpty()
{
	return (front == rear);
}

void printnametofile(out,i)
 FILE *out;
 int i;
{
    char k, j, l;
    
    j = StrIndex[i];
    l = StrIndex[i+1];
    
    for (k = j; (k < l && k <= j+10); k++)
	fputc(StrPool[k], out);
}

void file_out(r)
    ASTNodeIdx r;
{
    FILE	*out;

    ASTNodeIdx t, b;
    ASTNodeIdx first_sibling;
    void tabs(), printname();

    /* change file name from "ast.out" to "/tmp/ast.out" */
    out = fopen("/tmp/ast.out", "w+");
    if (out == NULL) {
    	printf("\n[in OQL Parser] file creation error...\n");
    	exit(1);
    }

    AddQueue(r);

    while(!IsEmpty()) {

		t = DeleteQueue();
		switch (AST(t).nodeName) {
#include "print_ast.h_for_file"
		}
		fprintf(out,"%d",AST(t).uniqueNum);
		fprintf(out,":");

		for(b = AST(t).son; b != AST_NULL; b = AST(b).brother) {
			ASTNodeIdx t = b;
			switch (AST(b).nodeName) {
#include "print_ast.h_for_file"
			}
			AddQueue(b);
			fprintf(out,"_%d",AST(b).uniqueNum);

			if(AST(b).brother != AST_NULL) fprintf(out,",");

		}
		fprintf(out,";\n");
    }

    fprintf(out,"\n");

    fclose(out);
}

void print_ast2( r, which, level)
 ASTNodeIdx r;
 int which, level;
{
    ASTNodeIdx t, b;
    void tabs(), printname();

    t = r;
    
    switch (AST(t).nodeName) {
#include "print_ast.h"
    };
    
    if (AST(t).son != AST_NULL)
	print_ast2(AST(t).son, 0, level+1);
    else
	printf("\n");

    if (AST(t).brother != AST_NULL)
	print_ast2(AST(t).brother, 1, level);
}

/*--------------------------------------------------------------*/
/*	This routine prints a name of ID or string  		*/
/*        StrIndex: an array of index for StrPool 		*/
/*		    parameter strIdx is the start index. 	*/
/*	  StrPool:  is an array of char. sequencially		*/
/*		    store all names and strings.     		*/
/*        StrPool[startIdx to endIdx-1] is the current name     */
/*        StrPool[endIdx to ...] is the next name		*/
/*--------------------------------------------------------------*/
void	printname(strIdx)
int	strIdx;
{
	char	i, startIdx, endIdx;

    	/* set start index to string pool for the current string */
	startIdx = StrIndex[strIdx];
	/* set start index to string pool for the next string */
	endIdx = StrIndex[strIdx+1];

	for (i = startIdx; i < endIdx; i++)
	    printf("%c", StrPool[i]);
}


/*----------------------------------------------*/
/*     This is a ruotine for the indentation.	*/
/*----------------------------------------------*/
void tabs(l)
 int l;
{
    int i;
    
    for (i = 0; i < l; i++)
	putchar('\t');
}



