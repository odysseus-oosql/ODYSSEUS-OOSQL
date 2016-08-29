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

#define CHECK_ERROR(systemHandle, e) \
do { \
if(e < 0) \
{ \
	char errorMessage[4096]; \
	OOSQL_GetErrorName(systemHandle, e, errorMessage, sizeof(errorMessage)); \
	printf("OOSQL ERROR(%s) : ", errorMessage); \
	OOSQL_GetErrorMessage(systemHandle, e, errorMessage, sizeof(errorMessage)); \
	puts(errorMessage); \
    return e; \
}\
} while(0);

#define TOK_ID			1

#define MAXHANDLENUM		(100)
#define INITIALBUFFERLEN	(1024 * 16)
#define MAXNKEYWORDS		4096

typedef struct KeywordExtractorHandle {
	Boolean		isUsed;
	Four		offset;
	Four		bufferSize;
	char*		buffer;
} KeywordExtractorHandle;

#define MAXKEYWORDLENGTH 128

typedef struct KeywordBuffer {
	char	keyword[MAXKEYWORDLENGTH];
	int		length;						
	int		stringcode;				
								
} KeywordBuffer;

KeywordBuffer keywordBuffer[MAXNKEYWORDS];
Four nKeywords = 0;

KeywordExtractorHandle keywordExtractorHandle[MAXHANDLENUM];
Four nHandles = 0;

int getToken(char **pStr, int tokenlength, char *token);
int findFirstFreeSlot();
int storeToken(char *token);
void eliminateInvisibleChar(char* str);
void convertSymbolCharToSpace(char* str);

int openAndExecuteKeywordExtractor(
	Four locationOfContent,
	OOSQL_SystemHandle *handle,
	Four volId,
	char *className,
	OID *oid,
	Two colNo,
	char *inFileOrContent,
	Four *resultHandle)
{
	FILE *fp;
	Four freeSlot;
	Four e;
	static char token[INITIALBUFFERLEN];
	char *ptrToData; 
	Four dataLength;
	Four returnLength;
	Four stringLength;
	Four eof;

	nKeywords = 0;
	nHandles ++;

	freeSlot = findFirstFreeSlot();
	CHECK_ERROR(handle, freeSlot);

	keywordExtractorHandle[freeSlot].isUsed = TRUE;
	keywordExtractorHandle[freeSlot].offset = 0;
	keywordExtractorHandle[freeSlot].bufferSize = INITIALBUFFERLEN;
	keywordExtractorHandle[freeSlot].buffer = (char*)malloc(INITIALBUFFERLEN);

	if(locationOfContent == OOSQL_TEXT_IN_FILE)
	{
		if((fp = fopen(inFileOrContent, "rb")) == NULL)
		{
			fprintf(stderr, "Can not open file %s\n", inFileOrContent);
			exit(1);
		}

		eof = 0;
		while(!eof)
		{
			/* read line from input string */
			ptrToData  = keywordExtractorHandle[freeSlot].buffer;
			dataLength = keywordExtractorHandle[freeSlot].bufferSize;
			while(1)
			{
				if(fgets(ptrToData, dataLength, fp) == NULL)
				{
					eof = 1;
					break;
				}

				stringLength = strlen(ptrToData);
				if(stringLength >= keywordExtractorHandle[freeSlot].bufferSize - 1)
				{
					/* there is more data to read */
					/* doubling input buffer */
					keywordExtractorHandle[freeSlot].bufferSize *= 2;
					keywordExtractorHandle[freeSlot].buffer = realloc(keywordExtractorHandle[freeSlot].buffer, keywordExtractorHandle[freeSlot].bufferSize);

					ptrToData  = &keywordExtractorHandle[freeSlot].buffer[stringLength];
					dataLength = keywordExtractorHandle[freeSlot].bufferSize - stringLength;
				}
				else
					break;
			}

			/* extract keyword from buffer */
			ptrToData = keywordExtractorHandle[freeSlot].buffer;
			eliminateInvisibleChar(ptrToData);
			convertSymbolCharToSpace(ptrToData);
			while(getToken(&ptrToData, sizeof(token), token) != -1)
			{
				storeToken(token);
			}
		}

		fclose(fp);
	}
	else if(locationOfContent == OOSQL_TEXT_IN_MEMORY)
	{
		ptrToData = inFileOrContent;
		eliminateInvisibleChar(ptrToData);
		convertSymbolCharToSpace(ptrToData);
		while(getToken(&ptrToData, sizeof(token), token) != -1)
		{
			storeToken(token);
		}
	}
	else if(locationOfContent == OOSQL_TEXT_IN_DB)
	{
		/* read line from input string */
		ptrToData    = keywordExtractorHandle[freeSlot].buffer;
		dataLength   = keywordExtractorHandle[freeSlot].bufferSize;
		stringLength = 0;
		while(1)
		{
			e = OOSQL_Text_FetchContent(handle, volId, className, colNo, oid, keywordExtractorHandle[freeSlot].bufferSize, keywordExtractorHandle[freeSlot].buffer, &returnLength);
			CHECK_ERROR(handle, e);
			stringLength += returnLength;

			if(returnLength == keywordExtractorHandle[freeSlot].bufferSize)
			{
				/* there is more data to read */
				/* doubling input buffer */
				keywordExtractorHandle[freeSlot].bufferSize *= 2;
				keywordExtractorHandle[freeSlot].buffer = realloc(keywordExtractorHandle[freeSlot].buffer, keywordExtractorHandle[freeSlot].bufferSize);

				ptrToData  = &keywordExtractorHandle[freeSlot].buffer[returnLength];
				dataLength = keywordExtractorHandle[freeSlot].bufferSize - returnLength;
			}
			else
				break;
		}

		ptrToData = keywordExtractorHandle[freeSlot].buffer;
		ptrToData[stringLength] = '\0';
		eliminateInvisibleChar(ptrToData);
		convertSymbolCharToSpace(ptrToData);
		while(getToken(&ptrToData, sizeof(token), token) != -1)
		{
			storeToken(token);
		}
	}
	else
	{
		printf("FROM ExampleKeywordExtractor\n");
		printf("Unknown input method is used. Since input string is now given, abandon extracting keywords\n");
		return -1;
	}

	*resultHandle = freeSlot;

	return eNOERROR;
}

int getAndNextKeywordExtractor(
	Four handle,
	char *keyword,
	Four *nPositions,
	char *positionList
)
{
	typedef struct {
		Four	sentence;
		Four	word;
	} PositionListStruct;
	Four offset;

	static PositionListStruct position = {0, 0};

	offset = keywordExtractorHandle[handle].offset;

	strncpy(keyword, keywordBuffer[offset].keyword, MAXKEYWORDLENGTH - 1);
	keywordExtractorHandle[handle].offset++;

	*nPositions = 0;
	memcpy(positionList, &position, sizeof(position));

	if(offset < nKeywords) return eNOERROR;
	else return OOSQL_TEXT_DONE;
}

int closeKeywordExtractor(
	Four handle
)
{
	keywordExtractorHandle[handle].isUsed = FALSE;
	keywordExtractorHandle[handle].offset = 0;
	free(keywordExtractorHandle[handle].buffer);

	nHandles --;

	return eNOERROR;
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
		if((unsigned)*str > (unsigned)0x80)
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
			*str       = ' ';
			*(str + 1) = ' ';
			str += 2;
		}
		else if((unsigned)*str > (unsigned)0x80)
			str += 2;
		else if(isalpha(*str) || isdigit(*str) || *str == '.')
			str ++;
		else
		{
			*str = ' ';
			str ++;
		}
	}
}

int getToken(char **pStr, int tokenlength, char *token)
{
	char *str = *pStr;
	int  i = 0;

	if(*str == '\0')
		return -1;

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
			if(isspace(*str))
				break;

			token[i] = *str; i++; str++;
		}

		if(tokenlength <= i)
		{
			printf("FROM ExampleKeywordExtractor\n");
			printf("Token size is exceeded. Please modify KeywordExtractor.c and recompile it.\n", MAXHANDLENUM);
			return -1;
		}
	}

	token[i] = '\0';

	/* skip remaining space */
	while((unsigned)*str <= (unsigned)0x80 && isspace(*str) && *str != 0) str++;
	*pStr = str;

	return TOK_ID;
}

static int MakeLowercase(char *buffer)
{
	int i, j;
	int length;

	length = strlen(buffer);
	for(i = 0; i < length; i++) 
	{
		if((unsigned char)buffer[i] > (unsigned char)0x80)
			i++;
		else
			buffer[i] = tolower(buffer[i]);
	}
	
	for(i = 0; i < length; i++)
	{
		if(!isspace(buffer[i]) || (unsigned)buffer[i] > (unsigned)0x80)
			break;
	}

	if(i != 0)
	{
		for(j = 0; i < length; i++, j++)
			buffer[j] = buffer[i];
		buffer[j] = '\0';
	}

	length = strlen(buffer);
	for(i = length - 1; i >= 0; i--)
	{
		if(i > 0 && (unsigned)buffer[i - 1] > (unsigned)0x80)
			break;
		if(!isspace(buffer[i]))
			break;
	}
	
	if(i != length - 1)
		buffer[i] = '\0';	

	return eNOERROR;
}

int calculateStringCode(char* string)
{
	int stringcode;
	int length;
	int i;

	stringcode = 0;
	length = strlen(string);

	for(i = 0; i < length; i++)
		stringcode += (int)string[i] + i * 256;
	return stringcode;
}

int storeToken(char *token)
{
	int i;
	int tokenLength;
	int tokenStringcode;

	if(nKeywords >= MAXNKEYWORDS)
		return eNOERROR;

	MakeLowercase(token);
	
	tokenStringcode = calculateStringCode(token);

	tokenLength = strlen(token);
	for(i = 0; i < nKeywords; i++)
	{
		if(tokenLength == keywordBuffer[i].length && tokenStringcode == keywordBuffer[i].stringcode)
			if(!strcmp(keywordBuffer[i].keyword, token)) 
				return eNOERROR;
	}

	strncpy(keywordBuffer[nKeywords].keyword, token, MAXKEYWORDLENGTH - 1);
	if (tokenLength >= MAXKEYWORDLENGTH)
	{
		keywordBuffer[nKeywords].keyword[MAXKEYWORDLENGTH - 1] = '\0';
		keywordBuffer[nKeywords].length = MAXKEYWORDLENGTH;
	}
	else
		keywordBuffer[nKeywords].length = tokenLength;
	keywordBuffer[nKeywords].stringcode = tokenStringcode;

	nKeywords++;

	return eNOERROR;
}

int findFirstFreeSlot()
{
	int i;

	if(nHandles >= MAXHANDLENUM) 
	{
		printf("FROM ExampleKeywordExtractor\n");
		printf("More than %d keyword extractors are opened. Please modify KeywordExtractor.c and recompile it.\n", MAXHANDLENUM);
		return -1;
	}

	for(i=0; i<MAXHANDLENUM; i++)
		if(keywordExtractorHandle[i].isUsed == FALSE)
			break;

	return i;
}

typedef struct {
	Four			sentence;
	Two				noun;
} Position;

#define MAXLENGTHOFKEYWORD			1000
#define DEFAULTPOSTINGBUFFERSIZE    1024 * 160

int convertf (
	char 		*inFile,
	char		*outFile)
{
	Four			e;
	Four			i;
	Four			handle;
	FILE			*fpOut;
    Four        	nPositions;
    Four            bufferOffset;
	Position		position;
    unsigned char   keyword[MAXLENGTHOFKEYWORD];
    char            positionListBuffer[DEFAULTPOSTINGBUFFERSIZE];
    int             chk;


	
	if ((fpOut=fopen(outFile, "w")) == NULL)
	{
		fprintf(stderr, "Can't open %s output file\n", outFile);
		exit(-1);
	}

    e = openAndExecuteKeywordExtractor(OOSQL_TEXT_IN_FILE, NULL, 0, NULL, NULL, 0, inFile, &handle);
	if (e < eNOERROR)
	{
		fprintf(stderr, "EngKeywordExtractor Initialization Fail\n");
		exit(-1);
	}

    chk = getAndNextKeywordExtractor(handle, (char *)keyword, &nPositions, positionListBuffer);
    while (chk != OOSQL_TEXT_DONE)
    {
        e = fprintf(fpOut, "%s %d", keyword, nPositions);
        if (e < 0)
        {
            fprintf(stderr, "Can't write posting information to output file\n");
            exit(-1);
        }

        bufferOffset = 0;
        for (i = 0; i < nPositions; i++)
        {
            memcpy(&position, &positionListBuffer[bufferOffset], sizeof(Position));
            bufferOffset += sizeof(Position);

            e = fprintf(fpOut, " %d %d", position.sentence, position.noun);
            if (e < 0)
            {
                fprintf(stderr, "Can't write posting information to output file\n");
                exit(-1);
            }
        }
        e = fprintf(fpOut, "\n");
        if (e < 0)
        {
            fprintf(stderr, "Can't write posting information to output file\n");
            exit(-1);
        }

        chk = getAndNextKeywordExtractor(handle, (char *)keyword, &nPositions, positionListBuffer);
    }


	e = closeKeywordExtractor(handle);
	if (e < eNOERROR)
	{
		fprintf(stderr, "EngKeywordExtractor Finalization Fail\n");
		exit(-1);
	}


	if (fclose(fpOut) == -1)
	{
		fprintf(stderr, "Can't close %s output file\n", outFile);
		exit(-1);
	}

    return eNOERROR;
}
