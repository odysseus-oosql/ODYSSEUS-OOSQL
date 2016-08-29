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

/*

	Format Converter...

	This program converts noun lists in to 
	unin noun list and location info....

Infile Format
   noun list with the .SYMBOL mark which indicates sentence boundary

Outfile Format
   noun location_pair location_noun sentence_no

			made by ycpark  Mar 23 1997

*/


#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <ndbm.h>
#include <fcntl.h>
#include "LOM_Internal.h"
#include "LOM.h"
#include "Catalog.h"
#include "Catalog_Internal.h"


#define MAXTERM 2000  /* Maximum number of index term in a document */

typedef struct  {
	char main_entry[200];
        int  tf;
        int  part_tf;
	int  location;
	int  noun_number[MAXTERM];
	int  sentence_number[MAXTERM];
	int  sub_noun_number;
	char sub_noun[20][100]; } Term;

Term    *index_term;
/*
Term    *index_term[MAXTERM];
*/
int     index_term_number;

int Stem();
void getKeyword(); 

int convertf(infile,outfile)
char *infile,*outfile;
{
	FILE *rawin,*rawout,*in,*out;
	char command[1000],tmpfile[L_tmpnam];
	char buffer[100];
	char sub_buffer[20][100];
	int  sub_number;
	int  mode;
	int  noun_position;
	int  sentence_position,i;

	char *irPath = getenv("IR_SYSTEM_PATH");
	if(irPath == NULL) return(eCONFIGURATION_LOM);
	
	startIR(); 	

	tmpnam(tmpfile);
#ifdef TRACE
	printf("Command Executing: %s\n", buffer);
#endif

	/*
	getKeyword(infile, tmpfile);
	*/

#define EXECUTE_KEYWORDEXTRACTOR "/opt/home/library/KeywordExtractor/extractor"
	sprintf(command, "%s %s %s", EXECUTE_KEYWORDEXTRACTOR, infile, tmpfile);
	system(command);

	if ( (in=Util_fopen(tmpfile,"r")) == NULL )  {
		fprintf(stderr,"Input file ERROR\n");
		return(eINTERNAL_LOM);
	}
	if ( (out=Util_fopen(outfile,"w")) == NULL )  {
		fprintf(stderr,"Output file ERROR\n");
		return(eINTERNAL_LOM);
	}
	
	index_term_number= 0;
	noun_position = sentence_position = 1;
	index_term = malloc(sizeof(Term)*MAXTERM);
	if(index_term == NULL) return(eOUTOFMEMORY_LOM);

	while (feof(in) == NULL)
	{		
		strcpy(buffer,"------");
		Util_fscanf(in,"%s",buffer);
		if (strcmp(buffer,".SYMBOL") == 0)  {
			sentence_position++;
			noun_position = 1;
			continue;
		}
		/* Stemization */
		upper_to_lower(buffer);
		Stem(buffer);

		encode_to_global(buffer,noun_position,sentence_position);  
		noun_position++;
	}

	print_result_final(out);  
	Util_fclose(out);
	Util_fclose(in);

	free(index_term);
	unlink(tmpfile);

	endIR(); 	
}


int print_result_final(out)
FILE *out;
{

	int i,j,k;

	for (i=0; i < index_term_number; i++)
	{
		Util_fprintf(out,"%s %d ",index_term[i].main_entry,index_term[i].location);
		for (j=0; j < index_term[i].location;j++)
			Util_fprintf(out,"%d %d  ",index_term[i].sentence_number[j],
					 index_term[i].noun_number[j]);
		Util_fprintf(out,"\n");
	}	
}


/* encode document statistics */
int encode_to_global(cnoun,noun_pos,sentence_pos)
char *cnoun;
int  noun_pos,sentence_pos;
{

	int i,j,k,FLAG;

	if (strcmp(cnoun,"------")==0) return;
	for (i=0; i < index_term_number; i++)
	{
		if ( strcmp(cnoun,index_term[i].main_entry) == 0)
		{
			j = index_term[i].location;
			index_term[i].noun_number[j] = noun_pos;
			index_term[i].sentence_number[j] = sentence_pos;
			index_term[i].location++;
			return;
		}
	}	
	strcpy(index_term[index_term_number].main_entry,cnoun);
	index_term[index_term_number].location = 1;
	index_term[i].noun_number[0] = noun_pos;
	index_term[i].sentence_number[0] = sentence_pos;
	index_term_number++;
}


/* In english keywords, converts to lower case 
	english keyword : return 1
	else return 0;
*/

int upper_to_lower(buffer)
char *buffer;
{
	int i,result;
	
	result = 1;
	for (i=0; i< strlen(buffer); i++)
	{
		if ( !isalpha(buffer[i]))  { result=0; break;}
		if ( (buffer[i] & 0x80) ) { result=0; break; }
	}

	if (result) 
	{
		for (i=0;i<strlen(buffer); i++) 
			buffer[i] = tolower(buffer[i]);

		return 1;
	}
	return 0;
}
	
		
