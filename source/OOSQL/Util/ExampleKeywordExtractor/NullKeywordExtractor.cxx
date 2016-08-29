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
#include <sys/types.h>
#include <fcntl.h>
#include "OOSQL_APIs.h"

// STL header for constructing cache and managing internal data structures

#include <string>
#include <vector>
#include <iterator>
#include <map>
using namespace std;

#define INITIALBUFFERLEN	(1024 * 64)
#define MAXTOKENLENGTH		(1024)
#define MAXKEYWORDLENGTH	(128)

#define CHECK_ERROR(e) if (e < eNOERROR) { fprintf(stderr, "Error occurs from keyword extractor (internal error code = %ld)\n", e); exit(-1); }

struct CacheElement {
	string 			token;
	vector<string> 	keywords;
};

struct Position {
	Four		sentense;
	Four		word;
};

struct KeywordBuffer {
	string			 keyword;
	vector<Position> positionList;
};

struct KeywordExtractor {
	bool 					isUsed;		
	vector<KeywordBuffer>	keywords;		
	map<string, int>		mapKeywordToKeywordsVectorIndex;	
	vector<KeywordBuffer>::iterator keywordsIteratorForGetNext;	
	
	Four					sentenseOffset;	
	Four					wordOffset;		
};

vector<KeywordExtractor>*		keywordExtractor = NULL;	
Four							contentBufferSize;		
char*							contentBuffer;			

#define KEYWORDEXTRACTOR (*keywordExtractor)

static int				findFreeKeywordExtractorInstance();
static int				storeKeyword();
static int				storeToken(Four handle, char *token);
static int				getNextOffset(char *string, int num);
static void				eliminateInvisibleChar(char* str);
static void				convertSymbolCharToSpace(char* str);
static bool				isInvalidKeyword(const char* str);
static void				cache_initCache();
static void				cache_addToCache(const CacheElement& cacheElement, bool syncFlag = true);
static CacheElement*	cache_lockupCache(char* token);
static void				cache_syncCacheFile();

extern "C" {
	int openAndExecuteKeywordExtractor(Four, OOSQL_SystemHandle*, Four, char*, OID*, Two, char*, Four*);
	int getAndNextKeywordExtractor(Four, char*, Four*, char*);
	int closeKeywordExtractor(Four);
}

void initKeywordExtractor()
{
	if(keywordExtractor == NULL)
	{
		keywordExtractor = new vector<KeywordExtractor>;
		if(keywordExtractor == NULL)
		{
			fprintf(stderr, "Can not create keywordExtractor instance\n");
			exit(1);
		}
	}
	
	if(contentBuffer == NULL)
	{
		contentBufferSize = INITIALBUFFERLEN;
		contentBuffer     = (char*)malloc(contentBufferSize);
		if(contentBuffer == NULL)
		{
			fprintf(stderr, "Can not allocate memory for contentBuffer, size = %ld\n", contentBufferSize);
			exit(1);
		}
	}
	
	cache_initCache();
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
			*str       = ' ';
			*(str + 1) = ' ';
			str += 2;
		}
		else if((unsigned char)*str == (unsigned char)0xa1 && (unsigned char)*(str + 1) == (unsigned char)0xa4)
		{
			*str       = ' ';
			*(str + 1) = ' ';
			str += 2;
		}
		
		else if((unsigned char)*str > (unsigned char)0x80)
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

#define TOK_ID			1

int getToken(Four handle, char **pStr, int tokenlength, char *token)
{
	char *str = *pStr;
	int  i = 0;

	if(*str == '\0')
		return -1;

	while(*str) 
	{
		if((unsigned char)*str > (unsigned char)0x80)
		{
			/* handle hangul character */
			token[i] = *str; i++; str++;
			token[i] = *str; i++; str++;
		}
		else
		{
			if(*str == '.')
			{
				KEYWORDEXTRACTOR[handle].sentenseOffset ++;
				KEYWORDEXTRACTOR[handle].wordOffset     = 0;
			}
			
			/* handle non hangul chararter */
			if(isspace(*str))
				break;

			if(i < MAXTOKENLENGTH - 1)
			{
				token[i] = *str; 
				i++;
			}
			str++;
		}

		if(tokenlength <= i)
		{
			fprintf(stderr, "FROM ExampleKeywordExtractor\n");
			fprintf(stderr, "Token size is exceeded. Please modify KeywordExtractor.c and recompile it.\n");
			return -1;
		}
	}

	token[i] = '\0';

	/* skip remaining space or comma */
	while((unsigned char)*str <= (unsigned char)0x80 && (isspace(*str) || *str == '.') && *str != 0) str++;
	*pStr = str;

	KEYWORDEXTRACTOR[handle].wordOffset ++;
	
	return TOK_ID;
}

int openAndExecuteKeywordExtractor(
    Four				locationOfContent,		/* IN  flag whether input is file, db, or memory */
    OOSQL_SystemHandle	*systemHandle,			/* IN  OOSQL system handle */
    Four				volId,					/* IN  volume id */
    char				*className,				/* IN  class name */
    OID					*oid,					/* IN  object id */
    Two					colNo,					/* IN  column from which keywords are extracted */
    char				*inFileOrContent,		/* IN  input file name if input is file, or content string if input is memory */
    Four				*resultHandle)			/* OUT handler to deal with current keyword extractor */
{
	FILE			*fp;
	Four			handle;
	Four			e;
	char			*ptrToData; 
	Four			dataLength;
	Four			stringLength;
	Four			returnLength;
	char 			token[MAXTOKENLENGTH];

	initKeywordExtractor();

	handle = findFreeKeywordExtractorInstance();
	CHECK_ERROR(handle);

	KEYWORDEXTRACTOR[handle].isUsed = true;
	KEYWORDEXTRACTOR[handle].keywords.clear();
	KEYWORDEXTRACTOR[handle].mapKeywordToKeywordsVectorIndex.clear();
	KEYWORDEXTRACTOR[handle].sentenseOffset = 0;
	KEYWORDEXTRACTOR[handle].wordOffset = 0;

	if (locationOfContent == OOSQL_TEXT_IN_FILE)
	{
		if((fp = Util_fopen(inFileOrContent, "r")) == NULL)
		{
			fprintf(stderr, "Can not open file %s\n", inFileOrContent);
			exit(1);
		}

		/* read line from input string */
		ptrToData  = contentBuffer;
		dataLength = contentBufferSize;

		while(1)
		{
			if(Util_fgets(ptrToData, dataLength, fp) == NULL)
				break;
			if(contentBuffer[contentBufferSize - 1] != '\0')	// make null terminated string
				contentBuffer[contentBufferSize - 1] = 0;

			stringLength = strlen(ptrToData);
			if(stringLength >= contentBufferSize - 1)
			{
				/* there is more data to read */
				/* doubling input buffer */
				contentBufferSize *= 2;
				contentBuffer = (char*)realloc(contentBuffer, contentBufferSize);
				if(contentBuffer == NULL)
				{
					fprintf(stderr, "Can not allocate memory for contentBuffer, size = %ld\n", contentBufferSize);
					exit(1);
				}
				
				ptrToData  = &contentBuffer[stringLength];
				dataLength = contentBufferSize - stringLength;
			}
			else
				break;
		}
		Util_fclose(fp);

		ptrToData = contentBuffer;
		eliminateInvisibleChar(ptrToData);
		convertSymbolCharToSpace(ptrToData);

		while(getToken(handle, &ptrToData, sizeof(token), token) != -1)
		{
			storeToken(handle, token);
		}
	}
	else if(locationOfContent == OOSQL_TEXT_IN_MEMORY)
	{
		ptrToData = inFileOrContent;
		eliminateInvisibleChar(ptrToData);
		convertSymbolCharToSpace(ptrToData);

		while(getToken(handle, &ptrToData, sizeof(token), token) != -1)
		{
			storeToken(handle, token);
		}
	}
	else
	{
		/* read line from input string */
		ptrToData    = contentBuffer;
		dataLength   = contentBufferSize;
		while(1)
		{
			e = OOSQL_Text_FetchContent(systemHandle, volId, className, colNo, oid, dataLength, ptrToData, &returnLength);
			CHECK_ERROR(e);

			if(returnLength >= contentBufferSize - 1)
			{
				/* there is more data to read */
				/* doubling input buffer */
				contentBufferSize *= 2;
				contentBuffer = (char*)realloc(contentBuffer, contentBufferSize);
				if(contentBuffer == NULL)
				{
					fprintf(stderr, "Can not allocate memory for contentBuffer, size = %ld\n", contentBufferSize);
					exit(1);
				}

				ptrToData  = contentBuffer;
				dataLength = contentBufferSize;
			}
			else
				break;
		}

		ptrToData = contentBuffer;
		ptrToData[returnLength] = '\0';
		eliminateInvisibleChar(ptrToData);
		convertSymbolCharToSpace(ptrToData);
		while(getToken(handle, &ptrToData, sizeof(token), token) != -1)
		{
			storeToken(handle, token);
		}
	}

	KEYWORDEXTRACTOR[handle].keywordsIteratorForGetNext = KEYWORDEXTRACTOR[handle].keywords.begin();
	*resultHandle = handle;

	return eNOERROR;
}

int getAndNextKeywordExtractor(
	Four handle,
	char *keyword,
	Four *nPositions,
	char *positionList
)
{
	Four i;

	if(KEYWORDEXTRACTOR[handle].keywordsIteratorForGetNext == KEYWORDEXTRACTOR[handle].keywords.end())
		return OOSQL_TEXT_DONE;

	vector<KeywordBuffer>::iterator& iterator = KEYWORDEXTRACTOR[handle].keywordsIteratorForGetNext;

	// copy keyword string
	strncpy(keyword, iterator->keyword.c_str(), MAXKEYWORDLENGTH);
	keyword[MAXKEYWORDLENGTH - 1] = '\0';

	// copy position information
	*nPositions = iterator->positionList.size();

	if(*nPositions > OOSQL_TEXT_MAXPOSITIONLISTLENGTH)
		*nPositions = OOSQL_TEXT_MAXPOSITIONLISTLENGTH;

	for(i = 0; i < *nPositions; i++)
	{
		memcpy(positionList, &iterator->positionList[i].sentense, sizeof(Four));	positionList += sizeof(Four);
		memcpy(positionList, &iterator->positionList[i].word, sizeof(Four));		positionList += sizeof(Four);
	}
	
	KEYWORDEXTRACTOR[handle].keywordsIteratorForGetNext ++;

	return eNOERROR;
}

int closeKeywordExtractor(
	Four handle
)
{
	KEYWORDEXTRACTOR[handle].isUsed = false;

	return eNOERROR;
}

int findFreeKeywordExtractorInstance()
{
	int i;
	int size = KEYWORDEXTRACTOR.size();

	for(i = 0; i < size; i++)
	{
		if(KEYWORDEXTRACTOR[i].isUsed == false)
			break;
	}

	if(i == size)
	{
		KeywordExtractor newKeywordExtractor;
		KEYWORDEXTRACTOR.push_back(newKeywordExtractor);
	}
	return i;
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
		if((unsigned char)buffer[i] > (unsigned)0x80 || !isspace(buffer[i]))
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
		if(i > 0 && (unsigned char)buffer[i - 1] > (unsigned char)0x80)
			break;
		if(!isspace(buffer[i]))
			break;
	}
	
	if(i != length - 1)
		buffer[i] = '\0';	

	return eNOERROR;
}

int isSpaceCheckForInvalidKeyword(int ch)
{
	if(ch == ' ' || ch == '\t' || ch == '\n' || (ch >= 1 && ch <= 27))
	{
		return true;
	}
	else
	{
		return false;
	}
	
}

bool isInvalidKeyword(const char* str)
{
    const char* s = str;

    while(*s)
    {
        if(isSpaceCheckForInvalidKeyword(*s))
           return true;
        s ++;
    }
	return false;
}

static int calculateStringCode(const char* string)
{
	int stringcode;
	int length;
	int i;

	stringcode = 0;
	length = strlen(string);

	for(i = 0; i < length; i++)
		stringcode += (unsigned int)((unsigned char)string[i]) + i * 256;
	return stringcode;
}

int storeSplittedToken(Four handle, string token)
{
	int tokenLength;
	int tokenStringcode;
	int index;
	
	tokenStringcode = calculateStringCode(token.c_str());
	tokenLength     = token.size();
	
	if(KEYWORDEXTRACTOR[handle].mapKeywordToKeywordsVectorIndex.find(token) == KEYWORDEXTRACTOR[handle].mapKeywordToKeywordsVectorIndex.end())
	{
		KeywordBuffer newKeyword;
		Position	  newPosition;
		
		newKeyword.keyword = token;

		newPosition.sentense = KEYWORDEXTRACTOR[handle].sentenseOffset;
		newPosition.word     = KEYWORDEXTRACTOR[handle].wordOffset;
		newKeyword.positionList.push_back(newPosition);

		index = KEYWORDEXTRACTOR[handle].keywords.size();

		KEYWORDEXTRACTOR[handle].keywords.push_back(newKeyword);

		KEYWORDEXTRACTOR[handle].mapKeywordToKeywordsVectorIndex[token] = index;
	}
	else
	{
		index = KEYWORDEXTRACTOR[handle].mapKeywordToKeywordsVectorIndex[token];
		Position newPosition;
		
		newPosition.sentense = KEYWORDEXTRACTOR[handle].sentenseOffset;
		newPosition.word     = KEYWORDEXTRACTOR[handle].wordOffset;

		KEYWORDEXTRACTOR[handle].keywords[index].positionList.push_back(newPosition);
	}

	return eNOERROR;
}

bool checkIfEmptyString(char* string)
{
	while(*string)
	{
		if(*string != ' ')
			return false;
		string ++;
	}
	return true;
}

int extractKeyword(string token, vector<string>& extractedKeywords)
{
	extractedKeywords.clear();
	extractedKeywords.push_back(token);

	return 0;
}

int storeToken(Four handle, char* token)
{
	int				i;
	int				e;
	CacheElement*	cacheElement;

	if((unsigned char)token[MAXKEYWORDLENGTH - 2] > (unsigned char)0x80)
		token[MAXKEYWORDLENGTH - 2] = 0;
	token[MAXKEYWORDLENGTH - 1] = 0;
	
	MakeLowercase(token);
	
	if(checkIfEmptyString(token) == true)
		return eNOERROR;
		
	if(isInvalidKeyword(token) == true)
		return eNOERROR;
	
	cacheElement = cache_lockupCache(token);
	if(cacheElement)
	{
		vector<string>::iterator iterator;
		vector<string>& keywords = cacheElement->keywords;
		
		for(iterator = keywords.begin(); iterator != keywords.end(); iterator ++)
		{
			e = storeSplittedToken(handle, (char*)iterator->c_str());
			CHECK_ERROR(e);
		}
	}
	else
	{
		CacheElement	newCacheElement;
		string			newKeyword;
		vector<string>	extractedKeywords;
		int				nExtractedKeywords;

		newCacheElement.token = token;
		
		e = extractKeyword(token, extractedKeywords);
		CHECK_ERROR(e);

		nExtractedKeywords = extractedKeywords.size();

		for(i = 0; i < nExtractedKeywords; i++)
		{
			if(isInvalidKeyword(extractedKeywords[i].c_str()) == false)
			{
				newKeyword = extractedKeywords[i];
				newCacheElement.keywords.push_back(newKeyword);
				
				e = storeSplittedToken(handle, newKeyword);
				CHECK_ERROR(e);
			}
			else
				printf("invalid keyword %s\n", token);
		}
		cache_addToCache(newCacheElement);
	}
	
	return eNOERROR;
}


struct HashBucket {
	map<string, CacheElement> elements;
};

map<int, HashBucket>*	cacheHashTable;						
int						cacheHashTableSize;					
char					cacheFilename[1024];			
FILE*					cacheFile;							
int						lastSyncCacheHashTableSize;		

#define HASHTABLE_SIZE 5000

#define HASHFUNC(i) ((i) % HASHTABLE_SIZE)

void cache_initCache()
{
	char* envString;
	
#ifdef DISABLE_CACHE
	return;
#else
	
	if(cacheHashTable == NULL)
	{
		// global cacheHashTable을 생성한다.
		cacheHashTable             = new map<int, HashBucket>;
		cacheHashTableSize         = 0;
		lastSyncCacheHashTableSize = 0;
		
		envString = getenv("NIB_CACHE_FILE");
		if(envString != NULL)
		{
			strcpy(cacheFilename, envString);
			printf("Using cache file %s\n", cacheFilename);
			
			cacheFile = Util_fopen(cacheFilename, "r+");	// open file for updating
			if(cacheFile == NULL)
			{
				cacheFile = Util_fopen(cacheFilename, "w+");	// open file for creating
				if(cacheFile == NULL)
					fprintf(stderr, "cache file을 초기화 하는데 실패하였습니다.");
			}
		}
		
		cache_syncCacheFile();
	}
#endif
}

void cache_syncCacheFile()
{
	int 		cacheSize;
#ifndef WIN32
	flock_t     flock;        			/* flock structure */
#endif
	static char	token[INITIALBUFFERLEN];
	static char keyword[INITIALBUFFERLEN];
	int			nKeywords;
	int			i, j;
	int			nFlushed;

#ifdef DISABLE_CACHE
	return;
#else

	if(cacheFile)
	{
#ifdef VERBOSE
		puts("start sync");
#endif
#ifndef WIN32
#ifdef VERBOSE		
		puts("get filelock");
#endif	
		flock.l_type   = F_WRLCK;		// exclusive lock
    	flock.l_start  = 0;         	// from start of file
    	flock.l_whence = SEEK_SET; 		// absolute offset
    	flock.l_len    = 0;           	// extends to end of file

    	if(fcntl(fileno(cacheFile), F_SETLKW, &flock) == -1)
    		return;	
#endif

#ifdef VERBOSE		
		puts("get cache file size");
#endif
		Util_fseek(cacheFile, 0, SEEK_SET);
		if(Util_fscanf(cacheFile, "%d", &cacheSize) != 1)
			cacheSize = 0;
		
#ifdef VERBOSE		
		printf("sync cache file and in-memory cache (cacheSize = %ld, lastSyncCacheHashTableSize = %ld)\n", cacheSize, lastSyncCacheHashTableSize);
#endif
		
		if(cacheSize != lastSyncCacheHashTableSize)
		{
			for(i = 0; i < cacheSize; i++)
			{
				if(Util_fscanf(cacheFile, "%s %d", token, &nKeywords) != 2)
				{
					printf("exit reading loop early %s %d\n", token, nKeywords);
					break;
				}
					
				if(cache_lockupCache(token) == NULL)
				{		
					// construct new cache element and add it
					CacheElement newCacheElement;
					string       newKeyword;
		
					newCacheElement.token = token;
					
					for(j = 0; j < nKeywords; j++)
					{
						Util_fscanf(cacheFile, "%s", keyword);
						
						newKeyword = keyword;
						newCacheElement.keywords.push_back(newKeyword);
					}
					cache_addToCache(newCacheElement, false);
				}
				else
				{
					// skip keyword
					for(j = 0; j < nKeywords; j++)
					{
						Util_fscanf(cacheFile, "%s", keyword);
					}
				}
			}
		}
		
#ifdef VERBOSE		
		printf("flush in memory cache (cacheSize = %ld, cacheHashTableSize = %ld)\n", cacheSize, cacheHashTableSize);
#endif
		nFlushed = 0;
		if(cacheSize != cacheHashTableSize)
		{
			Util_fseek(cacheFile, 0, SEEK_SET);
			Util_fprintf(cacheFile, "%ld\n", cacheHashTableSize);
			
			map<int, HashBucket>::iterator 		cacheHashTableIterator;
			map<string, CacheElement>::iterator cacheElementIterator;
			HashBucket*							hashBucket;	
			CacheElement* 						cacheElement;
			int									nKeywords;
			
			for(cacheHashTableIterator = cacheHashTable->begin(); cacheHashTableIterator != cacheHashTable->end(); cacheHashTableIterator ++)
			{
				hashBucket = &cacheHashTableIterator->second;
				
				for(cacheElementIterator = hashBucket->elements.begin(); cacheElementIterator != hashBucket->elements.end(); cacheElementIterator++)
				{
					cacheElement = &cacheElementIterator->second;
		
					nKeywords = cacheElement->keywords.size();
					Util_fprintf(cacheFile, "%s %ld", cacheElement->token.c_str(), nKeywords);
					for(i = 0; i < nKeywords; i++)
					{
						Util_fprintf(cacheFile, " %s", cacheElement->keywords[i].c_str());
					}
					Util_fprintf(cacheFile, "\n");
					nFlushed ++;
				}
			}
		}
#ifdef VERBOSE		
		printf("total %ld tokens are flushed\n", nFlushed);
#endif
		lastSyncCacheHashTableSize = cacheHashTableSize;
		
		// flush file
		Util_fflush(cacheFile);
		
#ifndef WIN32
#ifdef VERBOSE				
		puts("release filelock");
#endif		
	    flock.l_type   = F_UNLCK;		// unlock
    	flock.l_start  = 0;         	// from start of file
    	flock.l_whence = SEEK_SET; 		// absolute offset
    	flock.l_len    = 0;           	// extends to end of file

    	fcntl(fileno(cacheFile), F_SETLKW, &flock);
#endif

#ifdef VERBOSE				
		puts("end sync");
#endif		
	}
#endif
}

void cache_addToCache(const CacheElement& cacheElement, bool syncFlag)
{
	int 		stringcode;
	int 		hashvalue;
	HashBucket* bucket;
	HashBucket 	newBucket;
	
#ifdef DISABLE_CACHE
	return;
#else
	if(cacheHashTableSize > 10000000)
		return;						
	
	stringcode = calculateStringCode(cacheElement.token.c_str());
	
	hashvalue = HASHFUNC(stringcode);
	
	if(cacheHashTable->find(hashvalue) == cacheHashTable->end())
	{
		(*cacheHashTable)[hashvalue] = newBucket;
		
		bucket = &(*cacheHashTable)[hashvalue];
	}
	else
	{
		bucket = &(*cacheHashTable)[hashvalue];
	}
	
	bucket->elements[cacheElement.token] = cacheElement;

	cacheHashTableSize ++;
	
	if((cacheHashTableSize % 100000) == 0 && syncFlag)
		cache_syncCacheFile();
#endif
}

CacheElement* cache_lockupCache(char* token)
{
	int 		stringcode;
	int 		hashvalue;
	HashBucket* bucket;

#ifdef DISABLE_CACHE
	return NULL;
#else
	stringcode = calculateStringCode(token);
	
	hashvalue = HASHFUNC(stringcode);
	
	if(cacheHashTable->find(hashvalue) == cacheHashTable->end())
	{
		return NULL;
	}
	else
	{
		string tokenString = token;
		
		bucket = &(*cacheHashTable)[hashvalue];
		
		if(bucket->elements.find(tokenString) == bucket->elements.end())
		{
			return NULL;
		}
		else
		{
			return &(bucket->elements[tokenString]);
		}
	}
#endif
}
