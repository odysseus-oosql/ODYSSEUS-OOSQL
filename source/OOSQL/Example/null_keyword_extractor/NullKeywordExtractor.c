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
#include <string.h>
#include <malloc.h>
#include <stdlib.h>
#include <ctype.h>
#include "OOSQL_APIs.h"

/* error handling macro definition */
#define CHECK_ERROR(handle, e) if (e < eNOERROR) { printf("Error occurs from keyword extractor (internal error code=%d)\n", e); exit(-1); }

/* various constants definition */
#define INITIALBUFFERLEN	(1024 * 64)
#define MAXTOKENLENGTH		(128)
#define MAXPOSITIONLENGTH	(1024 * 8)
#define MAXKEYWORDNUM		(500 * 16)
#define MAXHANDLENUM		(100)
#define MAXBUFFERLEN		(1024 * 64 * 16)

/* return code definition for tokenizer */
#define TOK_DONE			1
#define TOK_ID				0

/* data structure definition for keyword extractor handler */
typedef struct KeywordExtractorHandle {
	Boolean		isUsed;
	Four		offset;
	Four		bufferSize;
	char*		buffer;
} KeywordExtractorHandle;

/* data structure definition for keyword buffer */
typedef struct KeywordBuffer {
	char*		keyword;
	Four		nPositions;
	char*		positionList;
} KeywordBuffer;

KeywordExtractorHandle* keywordExtractorHandle = NULL;
Four nHandles = 0;

KeywordBuffer* keywordBuffer = NULL;
Four nKeywords = 0;

/* function prototypes */
void initKeywordExtractor();
int isinvisible(char ch);
void eliminateInvisibleChar(char* str);
void convertSymbolCharToSpace(char* str);
int findFirstFreeSlot();
int getToken(char **pStr, int tokenlength, char *token, Four* sentenceNo, Four* wordNo);
int storeToken(char *token, Four sentenceNo, Four wordNo);
int MakeLowercase(char *buffer);

/*
 * IMPORTANT: Three functions must be provided to interface with ODYSSEUS/OOSQL.
 *   1) openAndExecuteKeywordExtractor()
 *   2) getAndNextKeywordExtractor()
 *   3) closeKeywordExtractor()
 */


/*
 * 1) openAndExecuteKeywordExtractor()
 *    This function extracts keywords from contents, and stores the results in the internal memory.
 *    The results, stored in the internal memory, can be identified with resultHandle and
 *    returned through getAndNextKeywordExtractor().
 */

int openAndExecuteKeywordExtractor(
    Four				locationOfContent,		/* IN  flag whether input is file, db, or memory */
    OOSQL_SystemHandle*	handle,					/* IN  OOSQL handle */
    Four				volId,					/* IN  volume id */
    char*				className,				/* IN  class name */
    OID*				oid,					/* IN  object id */
    Two					colNo,					/* IN  column from which keywords are extracted */
    char*				inFileOrContent,		/* IN  input file name if input is file, or content string if input is memory */
    Four*				resultHandle			/* OUT handler to deal with current keyword extractor */
)
{
	FILE*			fp;
	Four			firstFreeSlot;
	char*			ptrToData; 
	static char		token[MAXTOKENLENGTH];
	Four			sentenceNo, wordNo;
	Four			dataLength;
	Four			stringLength;
	Four			returnLength;
	Four			e;

	initKeywordExtractor();

	nKeywords = 0;
	nHandles ++;

	firstFreeSlot = findFirstFreeSlot();
	CHECK_ERROR(handle, firstFreeSlot);

	keywordExtractorHandle[firstFreeSlot].isUsed = TRUE;
	keywordExtractorHandle[firstFreeSlot].offset = 0;
	keywordExtractorHandle[firstFreeSlot].bufferSize = INITIALBUFFERLEN;
	if(keywordExtractorHandle[firstFreeSlot].buffer == NULL)
		keywordExtractorHandle[firstFreeSlot].buffer = (char *)malloc(INITIALBUFFERLEN);

	if(locationOfContent == OOSQL_TEXT_IN_FILE)
	{
		/* get input string from temporary file */
		if((fp = fopen(inFileOrContent, "r")) == NULL)
		{
			fprintf(stderr, "Can not open file %s\n", inFileOrContent);
			exit(1);
		}

		/* read line from input string */
		ptrToData  = keywordExtractorHandle[firstFreeSlot].buffer;
		dataLength = keywordExtractorHandle[firstFreeSlot].bufferSize;

		while(1)
		{
			if(fgets(ptrToData, dataLength, fp) == NULL)
				break;

			stringLength = strlen(ptrToData);
			if(stringLength >= keywordExtractorHandle[firstFreeSlot].bufferSize - 1)
			{
				/* there is more data to read */
				/* doubling input buffer */
				keywordExtractorHandle[firstFreeSlot].bufferSize *= 2;
				keywordExtractorHandle[firstFreeSlot].buffer = realloc(keywordExtractorHandle[firstFreeSlot].buffer, keywordExtractorHandle[firstFreeSlot].bufferSize);

				ptrToData  = &keywordExtractorHandle[firstFreeSlot].buffer[stringLength];
				dataLength = keywordExtractorHandle[firstFreeSlot].bufferSize - stringLength;
			}
			else
				break;
		}

		ptrToData = keywordExtractorHandle[firstFreeSlot].buffer;
		eliminateInvisibleChar(ptrToData);
		convertSymbolCharToSpace(ptrToData);

		if(ptrToData[0] != 0)
		{
			/* REPLACE THIS PART WITH YOUR FUNCTION */

			/* tokenize input string */
			while((e = getToken(&ptrToData, sizeof(token), token, &sentenceNo, &wordNo)) != TOK_DONE)
			{
				if(e < eNOERROR)
				{
					printf("Error occured during extracting keywords from :\n");
					puts(ptrToData);
				}
				CHECK_ERROR(handle, e);

				/* store each extracted keyword into keyword buffer */
				e = storeToken(token, sentenceNo, wordNo);
				if(e < eNOERROR)
				{
					printf("Error occured during storing keywords from :\n");
					puts(ptrToData);
				}
				CHECK_ERROR(handle, e);
			}
		}
		
		fclose(fp);
	}
	else if(locationOfContent == OOSQL_TEXT_IN_MEMORY)
	{
		/* get input string from memory variable */
		ptrToData = inFileOrContent;
		eliminateInvisibleChar(ptrToData);
		convertSymbolCharToSpace(ptrToData);

		if(ptrToData[0] != 0)
		{
			/* REPLACE THIS PART WITH YOUR FUNCTION */

			/* tokenize input string */
			while((e = getToken(&ptrToData, sizeof(token), token, &sentenceNo, &wordNo)) != TOK_DONE)
			{
				if(e < eNOERROR)
				{
					printf("Error occured during extracting keywords from :\n");
					puts(ptrToData);
				}
				CHECK_ERROR(handle, e);

				/* store each extracted keyword into keyword buffer */
				e = storeToken(token, sentenceNo, wordNo);
				if(e < eNOERROR)
				{
					printf("Error occured during storing keywords from :\n");
					puts(ptrToData);
				}
				CHECK_ERROR(handle, e);
			}
		}
	}
	else
	{
		/* get input string from database record */
		ptrToData    = keywordExtractorHandle[firstFreeSlot].buffer;
		dataLength   = keywordExtractorHandle[firstFreeSlot].bufferSize;
		stringLength = 0;
		
		while(1)
		{
			e = OOSQL_Text_FetchContent(handle, volId, className, colNo, oid, keywordExtractorHandle[firstFreeSlot].bufferSize, keywordExtractorHandle[firstFreeSlot].buffer, &returnLength);
			CHECK_ERROR(handle, e);
			stringLength += returnLength;

			if(returnLength == keywordExtractorHandle[firstFreeSlot].bufferSize)
			{
				/* there is more data to read */
				/* doubling input buffer */
				keywordExtractorHandle[firstFreeSlot].bufferSize *= 2;
				keywordExtractorHandle[firstFreeSlot].buffer = realloc(keywordExtractorHandle[firstFreeSlot].buffer, keywordExtractorHandle[firstFreeSlot].bufferSize);

				ptrToData  = &keywordExtractorHandle[firstFreeSlot].buffer[returnLength];
				dataLength = keywordExtractorHandle[firstFreeSlot].bufferSize - returnLength;
			}
			else
				break;
		}

		ptrToData = keywordExtractorHandle[firstFreeSlot].buffer;
		ptrToData[stringLength] = '\0';
		eliminateInvisibleChar(ptrToData);
		convertSymbolCharToSpace(ptrToData);

		if(ptrToData[0] != 0)
		{
			/* REPLACE THIS PART WITH YOUR FUNCTION */

			/* tokenize input string */
			while((e = getToken(&ptrToData, sizeof(token), token, &sentenceNo, &wordNo)) != TOK_DONE)
			{
				if(e < eNOERROR)
				{
					printf("Error occured during extracting keywords from :\n");
					puts(ptrToData);
				}
				CHECK_ERROR(handle, e);

				/* store each extracted keyword into keyword buffer */
				e = storeToken(token, sentenceNo, wordNo);
				if(e < eNOERROR)
				{
					printf("Error occured during storing keywords from :\n");
					puts(ptrToData);
				}
				CHECK_ERROR(handle, e);
			}
		}
	}

	*resultHandle = firstFreeSlot;

	return eNOERROR;
}


/* 
 * 2) getAndNextKeywordExtractor
 *    This function returns the keyword extracted contents, such as keyword strings and position informations of the keywords.
 *    In case all keywords have been read, OOSQL_TEXT_DONE is returned.
 *    In the other case, eNOERROR is returned.
 */

int getAndNextKeywordExtractor(
	Four				handle,					/* IN  handler to deal with current keyword extractor */
	char*				keyword,				/* OUT extracted keyword */
	Four*				nPositions,				/* OUT number of positions for this keyword */
	char*				positionList			/* OUT (setence no, word no) list for this keyword */
)
{
	Four offset;

	while(1)
	{
		offset = keywordExtractorHandle[handle].offset;

		/* get one of extracted keywords from keyword buffer */
		if(keywordBuffer[offset].keyword)
			strcpy(keyword, keywordBuffer[offset].keyword);
		else
			strcpy(keyword, "");

		/* get position information of this keyword */
		/* number of position, list of (sentence no, word no) */
		*nPositions = keywordBuffer[offset].nPositions;
		if(keywordBuffer[offset].positionList)
			memcpy(positionList, keywordBuffer[offset].positionList, keywordBuffer[offset].nPositions * sizeof(Four) * 2);
			
		keywordExtractorHandle[handle].offset++;
		
		if(strlen(keyword) > 0) break;		/* not to return null keyword */
		if(offset >= nKeywords) break;
	}

	if(offset < nKeywords) return eNOERROR;
	else return OOSQL_TEXT_DONE;
}


/*
 * 3) closeKeywordExtractor
 *    This function is called in case the contents, from which the keyword has been extracted, are thoroughly read.
 *    This function returns the resources that have been obtained by openAndExecuteKeywordExtractor().
 */

int closeKeywordExtractor(
	Four 				handle					/* IN  handler to deal with current keyword extractor */
)
{
	keywordExtractorHandle[handle].isUsed = FALSE;
	keywordExtractorHandle[handle].offset = 0;
	if(keywordExtractorHandle[handle].buffer)
		free(keywordExtractorHandle[handle].buffer);
	keywordExtractorHandle[handle].buffer = NULL;

	nHandles --;

	return eNOERROR;
}

void initKeywordExtractor()
{
	int i;

	if(keywordExtractorHandle == NULL)
	{
		keywordExtractorHandle = (KeywordExtractorHandle*)malloc(sizeof(KeywordExtractorHandle) * MAXHANDLENUM);
		if(keywordExtractorHandle == NULL)
		{
			fprintf(stderr, "Can not allocate memory in initKeywordExtractor, size = %d\n", sizeof(KeywordExtractorHandle) * MAXHANDLENUM);
			exit(1);
		}
		for(i = 0; i < MAXHANDLENUM; i++)
		{
			keywordExtractorHandle[i].isUsed = FALSE;
			keywordExtractorHandle[i].buffer = NULL;
		}
	}

	if(keywordBuffer == NULL)
	{
		keywordBuffer = (KeywordBuffer*)malloc(sizeof(KeywordBuffer) * MAXKEYWORDNUM);
		if(keywordBuffer == NULL)
		{
			printf("%d\n", sizeof(KeywordBuffer));
			fprintf(stderr, "Can not allocate memory in initKeywordExtractor, size = %d\n", sizeof(KeywordBuffer) * MAXKEYWORDNUM);
			exit(1);
		}
		for(i = 0; i < MAXKEYWORDNUM; i++)
		{
			keywordBuffer[i].keyword = NULL;
			keywordBuffer[i].positionList = NULL;
		}
	}	
}

int isinvisible(char ch)
{
	if(ch >= 1 && ch <= 27)
		return 1;
	else
		return 0;
}

void eliminateInvisibleChar(char* str)
{
	while(*str)
	{
		if((unsigned char)*str > (unsigned char)0x80)
			str += 2;
		else if(isinvisible(*str))
			*str = ' ';
		else
			str ++;
	}
}

void convertSymbolCharToSpace(char* str)
{
	while(*str)
	{
		if((unsigned char)*str == (unsigned char)0xa1 && (unsigned char)*(str + 1) == (unsigned char)0xa1)
		{
			*str = ' ';
			*(str + 1) = ' ';
			str += 2;
		}
		else if((unsigned char)*str == (unsigned char)0xa1 && (unsigned char)*(str + 1) == (unsigned char)0xa4)
		{
			*str = ' ';
			*(str + 1) = ' ';
			str += 2;
		}
		else if((unsigned char)*str == (unsigned char)0xa1 && (unsigned char)*(str + 1) == (unsigned char)0xde)
		{
			*str = ' ';
			*(str + 1) = ' ';
			str += 2;
		}
		else if((unsigned char)*str == (unsigned char)0xa1 && (unsigned char)*(str + 1) == (unsigned char)0xe2)
		{
			*str = ' ';
			*(str + 1) = ' ';
			str += 2;
		}
		else if((unsigned char)*str == (unsigned char)0xa1 && (unsigned char)*(str + 1) == (unsigned char)0xe3)
		{
			*str = ' ';
			*(str + 1) = ' ';
			str += 2;
		}
		else if((unsigned char)*str == (unsigned char)0xa1 && (unsigned char)*(str + 1) == (unsigned char)0xe4)
		{
			*str = ' ';
			*(str + 1) = ' ';
			str += 2;
		}
		else if((unsigned char)*str > (unsigned char)0x80)
		{
			str += 2;
		}
		else if(isalpha(*str) || isdigit(*str) || *str == '.')
			str ++;
		else
		{
			*str = ' ';
			str ++;
		}
	}
}

int findFirstFreeSlot()
{
	int i;

	if(nHandles >= MAXHANDLENUM) 
	{
		printf("From findFirstFreeSlot, MAXHANDLENUM needs to be increased\n");
		return -1;
	}

	for(i = 0; i < MAXHANDLENUM; i++)
		if (keywordExtractorHandle[i].isUsed == FALSE)
			break;

	return i;
}

int getToken(char **pStr, int tokenlength, char *token, Four* sentenceNo, Four* wordNo)
{
	char *str = *pStr;
	int  i = 0;
	static Four curSentenceNo = 0;
	static Four curWordNo = 0;

	if(*str == '\0')
	{
		curSentenceNo = curWordNo = 0;
		return TOK_DONE;
	}

	while(*str) 
	{
		if((unsigned)*str > (unsigned)0x80)
		{
			/* handle hangul character */
			token[i] = *str; i++; str++;
			token[i] = *str; i++; str++;
		}
		else
		{
			/* handle non hangul chararter */
			/* separate token by space or dot character */
			if(isspace(*str) || *str == '.')
				break;

			token[i] = *str; i++; str++;
		}

		if(tokenlength <= i)
		{
			printf("FROM NullKeywordExtractor\n");
			printf("Token size is exceeded. Please modify MAXTOKENLENGTH and recompile it.\n");
			return -1;
		}
	}

	token[i] = '\0';

	/* set sentence no. and word no. */
	*sentenceNo = curSentenceNo;
	*wordNo     = curWordNo;

	/* increase sentence no. and word no. */
	if(*str == '.')
	{
		/* begin new sentence */
		curSentenceNo ++;
		curWordNo = 0;
	}
	else
		curWordNo ++;

	/* skip remaining spaces */
	while((unsigned)*str <= (unsigned)0x80 && (isspace(*str) || *str == '.') && *str != 0) str++;
	*pStr = str;

	return TOK_ID;
}

int storeToken(char *token, Four sentenceNo, Four wordNo)
{
	Four nPositions;
	char* positionList;
	int i;

	if(nKeywords >= MAXKEYWORDNUM)
	{
		printf("Increase the size of keyword buffer.\n");
		return -1;
	}

	MakeLowercase(token);
	
	for(i = 0; i < nKeywords; i++)
		if(!strcmp(keywordBuffer[i].keyword, token))
			break;

	if(i >= nKeywords)
	{
		/* if same keyword has not been extracted before */

		if(keywordBuffer[nKeywords].keyword == NULL)
		{
			keywordBuffer[nKeywords].keyword = (char*)malloc(MAXTOKENLENGTH);
			if(keywordBuffer[nKeywords].keyword == NULL)
			{
				fprintf(stderr, "Can not allocate memory in storeToken, size = %d\n", MAXTOKENLENGTH);
				exit(1);
			}
		}

		if(keywordBuffer[nKeywords].positionList == NULL)
		{
			keywordBuffer[nKeywords].positionList = (char*)malloc(MAXPOSITIONLENGTH);
			if(keywordBuffer[nKeywords].positionList == NULL)
			{
				fprintf(stderr, "Can not allocate memory in storeToken, size = %d\n", MAXPOSITIONLENGTH);
				exit(1);
			}
		}

		/* store extracted keyword into keyword buffer */
		strncpy(keywordBuffer[nKeywords].keyword, token, MAXTOKENLENGTH - 1);
		keywordBuffer[nKeywords].nPositions = 0;

		nKeywords ++;
	}

	/* increase number of position */
	nPositions   = keywordBuffer[i].nPositions ++;
	positionList = keywordBuffer[i].positionList + nPositions * 8;
	
	if(nPositions * 8 >= MAXPOSITIONLENGTH)
	{
		printf("From storeToken, MAXPOSITIONLENGTH needs to be increased and recompiled.\n");
		return -1;
	}

	/* store position information into keyword buffer */
	memcpy(positionList, (char *)&sentenceNo, sizeof(sentenceNo));
	memcpy(positionList + sizeof(sentenceNo), (char *)&wordNo, sizeof(wordNo));

	return eNOERROR;
}

int MakeLowercase(char *buffer)
{
	int i, j;
	int length;

	/* make lower case */
	length = strlen(buffer);
	for(i = 0; i < length; i++) 
	{
		if((unsigned char)buffer[i] > (unsigned char)0x80)
			i++;
		else
			buffer[i] = tolower(buffer[i]);
	}
	
	/* strip left spaces */
	for(i = 0; i < length; i++)
	{
		if(!isspace(buffer[i]) || (unsigned char)buffer[i] > (unsigned char)0x80)
			break;
	}

	if(i != 0)
	{
		for(j = 0; i < length; i++, j++)
			buffer[j] = buffer[i];
		buffer[j] = '\0';
	}

	/* strip right spaces */
	length = strlen(buffer);
	for(i = length - 1; i >= 0; i--)
	{
		if(i > 0 && (unsigned char)buffer[i - 1] > (unsigned char)0x80)
			break;
		if(!isspace(buffer[i]))
			break;
	}
	
	if(i != length - 1)
		buffer[i] = '\0';	

	return eNOERROR;
}

