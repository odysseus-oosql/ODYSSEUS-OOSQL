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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include "OOSQL_APIs.h"

#define ERROR_CHECK(e) \
do { \
if (e < 0) { \
    printf("File:%s Line:%d\n", __FILE__, __LINE__); fflush(stdout); \
	return e; \
}\
} while(0);

#define ERROR_CHECK_XCT(systemHandle, e, xactId) \
do { \
if (e < 0) { \
	char errorMessage[4096]; \
	OOSQL_GetErrorName(systemHandle, e, errorMessage, sizeof(errorMessage)); \
	printf("OOSQL ERROR(%s) : ", errorMessage); \
    OOSQL_GetErrorMessage(systemHandle, e, errorMessage, sizeof(errorMessage)); \
    puts(errorMessage); \
	if((xactId) != NULL) (int) OOSQL_TransAbort(systemHandle, xactId); \
        (int) OOSQL_DestroySystemHandle(systemHandle, procIndex); \
    if (1) exit(1); \
}\
} while(0);

Four procIndex;
char mode[255];

#define MAXVARSTRINGLEN 	2048
#define MAXQUERYSTRLEN		4096
#define HEADER_SIZE        	100
#define BODY_SIZE          	200 * 1024

typedef struct paperStruct {
	int id;
	char title[MAXVARSTRINGLEN];
	char author[MAXVARSTRINGLEN];
	char affiliation[MAXVARSTRINGLEN];
	char keyword[MAXVARSTRINGLEN];
	char abstract[MAXVARSTRINGLEN];
	char language[MAXVARSTRINGLEN];
	char journal[MAXVARSTRINGLEN];
	int year;
	int volume;
	int number;
	char pages[MAXVARSTRINGLEN];
} paperStruct;

enum {
    HEADER_ID = 1,
    HEADER_TITLE,
    HEADER_AUTHOR,
    HEADER_AFFILIATION,
    HEADER_LANGUAGE,
    HEADER_JOURNAL,
    HEADER_YEAR,
    HEADER_VOLUME,
    HEADER_NUMBER,
    HEADER_PAGES,
    HEADER_ABSTRACT,
    HEADER_KEYWORD
};

enum {
    TITLE_COLNO = 0,
    AUTHOR_COLNO,
    AFFILIATION_COLNO,
	LANGUAGE_COLNO,
	JOURNAL_COLNO,
	PAGES_COLNO,
    ABSTRACT_COLNO,
    KEYWORD_COLNO
};

struct HeaderToHeaderID {
    char* header;
    Four  id;
};

struct HeaderToHeaderID headerToHeaderID_Table[] = {
    {"id",          HEADER_ID},
    {"title",       HEADER_TITLE},
    {"author",      HEADER_AUTHOR},
    {"affiliation", HEADER_AFFILIATION},
    {"language",    HEADER_LANGUAGE},
    {"journal",     HEADER_JOURNAL},
    {"year",        HEADER_YEAR},
    {"volume",      HEADER_VOLUME},
    {"number",      HEADER_NUMBER},
    {"pages",       HEADER_PAGES},
    {"abstract",    HEADER_ABSTRACT},
    {"keywords",    HEADER_KEYWORD},
    {NULL,          -1}
};

Four currentID;
Four readRecord(FILE* fp, char* header, char* body)
{
    static char lineBuffer[HEADER_SIZE + BODY_SIZE];
    Four   i;
    
    /* assume each record consist of one line */
    /* each line has following form
       <header> body */

    if(fgets(lineBuffer, sizeof(lineBuffer), fp) == NULL)
        return -1;

    for(i = 1; lineBuffer[i] != '>'; i++)
    {
    	if(i > strlen(lineBuffer) || i > 30)
    	{
    	    fprintf(stderr, "ID<%d> Header error >> %s", currentID, lineBuffer);
    	    *header = '\0';
    	    header ++;
    	    break;
    	}

        *header = lineBuffer[i];
        header++;
    }
    *header = '\0';
    
    for(i++; i < strlen(lineBuffer); i++)
    {
        *body = lineBuffer[i];
        if(*body == '\n')
            *body = '\0';
        body++;
    }
    *body = '\0';
    
    return eNOERROR;
}

Four getHeaderType(char* header)
{
    Four i;

    for(i = 0; headerToHeaderID_Table[i].header != NULL; i++)
    {
        if(!strcmp(headerToHeaderID_Table[i].header, header))
            return headerToHeaderID_Table[i].id;
    }

    return -1;
}

void makeLowerCase(char* s)
{
    while(*s != '\0')
    {
        if(isupper(*s))
            *s = tolower(*s);
        s++;
    }
}

Four insertData(OOSQL_SystemHandle* systemHandle, Four volID, FILE* fp, paperStruct* paper)
{
	Four          	e;
	Four          	headerType;
	Four          	nObjects;
	Four			nLines;
	static char   	header[HEADER_SIZE];
	static char   	body[BODY_SIZE];
	char 			queryStr1[MAXQUERYSTRLEN];
	char 			queryStr2[MAXQUERYSTRLEN];
	char 			queryString[MAXQUERYSTRLEN];
	char 			buf[MAXVARSTRINGLEN];
	OOSQL_Handle	handle;

	strcpy(queryStr1, "insert into Paper\n");
	strcat(queryStr1, "(id, title, author, affiliation, language, journal, "); 
	strcat(queryStr1, "year, volume, number, pages, abstract, keyword)\n");
	strcat(queryStr1, "values\n");
    
    nObjects = nLines = 0;

	memset(paper->title, 0, MAXVARSTRINGLEN);
	memset(paper->author, 0, MAXVARSTRINGLEN);
	memset(paper->affiliation, 0, MAXVARSTRINGLEN);
	memset(paper->keyword, 0, MAXVARSTRINGLEN);
	memset(paper->abstract, 0, MAXVARSTRINGLEN);
	memset(paper->language, 0, MAXVARSTRINGLEN);
	memset(paper->journal, 0, MAXVARSTRINGLEN);
	memset(paper->pages, 0, MAXVARSTRINGLEN);

	for (;;)
    {
		/* read data file and construct colliststruct */
		e = readRecord(fp, header, body);

		if (e == -1) break;

		makeLowerCase(header);
        
		headerType = getHeaderType(header);
        
		switch(headerType)
		{
			case HEADER_YEAR:
				paper->year = atoi(body);
				break;
			case HEADER_VOLUME:
				paper->volume = atoi(body);
				break;
			case HEADER_NUMBER:
				paper->number = atoi(body);
				break;
			case HEADER_ID:
				paper->id = atoi(body);
				break;
            
			case HEADER_TITLE:
				strcpy(paper->title, body);
				break;
			case HEADER_AUTHOR:
				strcpy(paper->author, body);
				break;
			case HEADER_AFFILIATION:
				strcpy(paper->affiliation, body);
				break;
			case HEADER_ABSTRACT:
				strcpy(paper->abstract, body);
				break;
			case HEADER_KEYWORD:
				strcpy(paper->keyword, body);
				break;
            
			case HEADER_LANGUAGE:
				strcpy(paper->language, body);
				break;
			case HEADER_JOURNAL: 
				strcpy(paper->journal, body);
				break;
			case HEADER_PAGES: 
				strcpy(paper->pages, body);
				break;
		} 

		nLines++;

		if (nLines % 12 == 0)  
		{
			sprintf(buf, "(%d, ", paper->id);
			strcpy(queryStr2, buf);							/* id */
			sprintf(buf, "%s ?, ", mode);
			strcat(queryStr2, buf);							/* title */
			sprintf(buf, "%s ?, ", mode);
			strcat(queryStr2, buf);							/* author */
			sprintf(buf, "%s ?, ", mode);
			strcat(queryStr2, buf);							/* affiliation */
			strcat(queryStr2, "?, ");						/* language */
			strcat(queryStr2, "?, ");						/* journal */
			sprintf(buf, "%d, ", paper->year);
			strcat(queryStr2, buf);							/* year */
			sprintf(buf, "%d, ", paper->volume);
			strcat(queryStr2, buf);							/* volume */
			sprintf(buf, "%d, ", paper->number);
			strcat(queryStr2, buf);							/* number */
			strcat(queryStr2, "?, ");						/* pages */
			sprintf(buf, "%s ?, ", mode);
			strcat(queryStr2, buf);							/* abstract */
			sprintf(buf, "%s ?)", mode);
			strcat(queryStr2, buf);							/* keyword */

			strcpy(queryString, queryStr1);
			strcat(queryString, queryStr2);

			e = OOSQL_AllocHandle(systemHandle, volID, &handle);
			ERROR_CHECK(e);

			e = OOSQL_Prepare(systemHandle, handle, queryString, NULL);
			if (e < eNOERROR)  {
				e = OOSQL_FreeHandle(systemHandle, handle);
				ERROR_CHECK(e);
			}

			e = OOSQL_Execute(systemHandle, handle);
			if (e < eNOERROR)  {
				e = OOSQL_FreeHandle(systemHandle, handle);
				ERROR_CHECK(e);
			}

			e = OOSQL_PutData(systemHandle, handle, TITLE_COLNO, 0, &paper->title, strlen(paper->title)+1);  
			ERROR_CHECK(e);

			e = OOSQL_PutData(systemHandle, handle, AUTHOR_COLNO, 0, &paper->author, strlen(paper->author)+1);  
			ERROR_CHECK(e);

			e = OOSQL_PutData(systemHandle, handle, AFFILIATION_COLNO, 0, &paper->affiliation, strlen(paper->affiliation)+1);  
			ERROR_CHECK(e);

			e = OOSQL_PutData(systemHandle, handle, LANGUAGE_COLNO, 0, &paper->language, strlen(paper->language)+1);  
			ERROR_CHECK(e);

			e = OOSQL_PutData(systemHandle, handle, JOURNAL_COLNO, 0, &paper->journal, strlen(paper->journal)+1);  
			ERROR_CHECK(e);

			e = OOSQL_PutData(systemHandle, handle, PAGES_COLNO, 0, &paper->pages, strlen(paper->pages)+1);  
			ERROR_CHECK(e);

			e = OOSQL_PutData(systemHandle, handle, ABSTRACT_COLNO, 0, &paper->abstract, strlen(paper->abstract)+1);  
			ERROR_CHECK(e);

			e = OOSQL_PutData(systemHandle, handle, KEYWORD_COLNO, 0, &paper->keyword, strlen(paper->keyword)+1);  
			ERROR_CHECK(e);

			memset(paper->title, 0, MAXVARSTRINGLEN);
			memset(paper->author, 0, MAXVARSTRINGLEN);
			memset(paper->affiliation, 0, MAXVARSTRINGLEN);
			memset(paper->keyword, 0, MAXVARSTRINGLEN);
			memset(paper->abstract, 0, MAXVARSTRINGLEN);
			memset(paper->language, 0, MAXVARSTRINGLEN);
			memset(paper->journal, 0, MAXVARSTRINGLEN);
			memset(paper->pages, 0, MAXVARSTRINGLEN);

			e = OOSQL_FreeHandle(systemHandle, handle);
			ERROR_CHECK(e);

			nObjects ++;
			
			printf("Count : %d\n", nObjects);

		}
	}

	printf("Total %d objects are inserted\n", nObjects); 

	return eNOERROR;
}

Four insert(OOSQL_SystemHandle* systemHandle, Four volID, char* filename)
{
    FILE *fp;
    Four e;
	paperStruct paper;
    
    /* class Paper
       {
       public:
          char hasParent;
          int  id;
          Text title;
          Text author;
          Text affiliation;
          char language[32];
          char journal[128];
          int  year;
          int  volume;
          int  number;
          char pages[32];
          Text abstract;
          Text keyword;
       }
    */

    /* open data file */
    fp = fopen(filename, "r");
    if(fp == NULL)
    {
        printf("Can't open %s\n", filename);
        return eNOERROR;
    }

	e = insertData(systemHandle, volID, fp, &paper);
	ERROR_CHECK(e);

    return eNOERROR;
}

int main(int argc, char* argv[])
{
    char DBPATH[256];
	Four databaseID;
    Four volID;
    XactID xactID;
    Four e;
	OOSQL_SystemHandle systemHandle;

	if(argc < 3)
	{
		printf("USAGE : insert <volume name> <data file name> [-deferred]\n");
		exit(1);
	}

    printf("OOSQL_CreateSystemHandle\n");
    e = OOSQL_CreateSystemHandle(&systemHandle, &procIndex);
    ERROR_CHECK_XCT(&systemHandle, e, NULL);

	strcpy(DBPATH, argv[1]);

    printf("OOSQL_MountDB\n");
    e = OOSQL_MountDB(&systemHandle, DBPATH, &databaseID);
    ERROR_CHECK_XCT(&systemHandle, e, NULL);

	e = OOSQL_GetUserDefaultVolumeID(&systemHandle, databaseID, &volID);
    ERROR_CHECK_XCT(&systemHandle, e, NULL);
    
    printf("OOSQL_TransBegin\n");
    e = OOSQL_TransBegin(&systemHandle, &xactID, X_RR_RR);
    ERROR_CHECK_XCT(&systemHandle, e, &xactID);

	if (argc == 4)  {
		if (!strcmp(argv[3], "-deferred")) strcpy(mode, "text deferred");
		else strcpy(mode, "text");
	}
	else
		strcpy(mode, "text");

	e = insert(&systemHandle, volID, argv[2]);
	ERROR_CHECK_XCT(&systemHandle, e, &xactID);

    printf("OOSQL_TransCommit\n");
    e = OOSQL_TransCommit(&systemHandle, &xactID);
    ERROR_CHECK_XCT(&systemHandle, e, &xactID);

    printf("OOSQL_DismountDB\n");
    e = OOSQL_DismountDB(&systemHandle, databaseID);
    ERROR_CHECK_XCT(&systemHandle, e, NULL);
    
    printf("OOSQL_DestroySystemHandle\n");
    e = OOSQL_DestroySystemHandle(&systemHandle, procIndex);
    ERROR_CHECK_XCT(&systemHandle, e, NULL);
    
    return eNOERROR;
}
