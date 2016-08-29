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

#ifndef _KORKEYWORDEXTRACTOR_H_
#define _KORKEYWORDEXTRACTOR_H_

#define TRUE  1
#define FALSE 0

#define MAXBUFFERSIZE 4*1024*1024
#define MAXSENTENCESIZE 1024
#define MAXWORDSIZE 128
#define MAXKEYWORDSIZE 128


#define MAXNUMOFHANDLES   100
#define MAXNUMOFKEYWORDS 10000
#define MAXNUMOFPOSITIONS 20


#define MAXLENGTHOFKEYWORD 1000
#define MAXLENGTHOFBUFFER 1024*100


#define NGRAMSIZE 2  				/* n-gram size */
#define ENGNGRAMSIZE    3           /* n-gram size of english keywords */
#define HASHSIZE 997
#define DEFAULTBUFFERSIZE 32
#define ONENOUNLENGTH 2

#define ENABLESTEM FALSE			/* enable to stemize */

#define eBADPARAMETER -1

#define ENDOFBUFFER -100
#define ENDOFSENTENCE -101


#define eFILEOPEN -200
#define eMAXBUFFERSIZE -201
#define eMAXSENTENCESIZE -202
#define eJOSAINIT -203
#define eEOMIINIT -204
#define eKORSTOPWORDINIT -205
#define eENGSTOPWORDINIT -206
#define eONENOUNWORDINIT -207
#define eONEPRONOUNWORDINIT -208
#define eSPECIALNOUNINIT -209

#define eJOSAEOMIINIT  -1000
#define eSTOPWORDINIT -1002
#define eONENOUNINIT -1003
#define eONEPRONOUNINIT -1004
#define eJOSAEOMITABLEINIT -1005
#define eSTOPWORDTABLEINIT -1006
#define eONENOUNTABLEINIT -1008
#define eSPECIALNOUNTABLEINIT -1009
#define eJOSATABLEINIT -1010

#define eCHANGENGRAMQUERYMODE -1100



#define NOTFOUND -1

#define MAXJOSAEOMISIZE  20  		/* the buffer size of josa and eomi */
#define MAXSTOPWORDSIZE  20 		/* the buffer size of stopword */
#define MAXONENOUNWORDSIZE  20 		/* the buffer size of onenoun word */
#define MAXSPECIALNOUNSIZE  20 		/* the buffer size of specialnoun */

#define TOOLONGKEYWORDLEN   30 

/* if NGRAMQUERYEXACTMATCH is defined - exact match (each n-gram keywords will be connected by '^0'). */
/* if NGRAMQUERYEXACTMATCH not defiend - partial match (each n-gram keywords will be connected by '|'). */
#define NGRAMQUERYEXACTMATCH



/* Dictionary file difinition	*/

#define JOSAFILE "JOSA.TXT"
#define EOMIFILE "EOMI.TXT"
#define KORSTOPWORDFILE "KORSTOPWORD.TXT"
#define ENGSTOPWORDFILE "ENGSTOPWORD.TXT"
#define ONENOUNFILE "ONENOUN.TXT"
#define ONEPRONOUNFILE "ONEPRONOUN.TXT"
#define SPECIALNOUNFILE1 "SPECIALNOUN_PART1.TXT"
#define SPECIALNOUNFILE2 "SPECIALNOUN_PART2.TXT"

/***********************************************/
/*   structure definition about dictionaries   */
/***********************************************/


typedef struct {
	int		id;
	char	*old_end;
	char 	*new_end;
	int		old_offset;
	int		new_offset;
	int		min_root_size;
	int		(*condition)();
	} RuleList;




/***********************************************/
/*   structure definition about dictionaries   */
/***********************************************/

typedef struct {
	char* JosaEomiTable[HASHSIZE];
	char* StopwordTable[HASHSIZE];
	char* OnenounTable[HASHSIZE];
	char* OnepronounTable[HASHSIZE];
	char* SpecialnounTable[HASHSIZE];		
	char* JosaTable[HASHSIZE];			
} Dictionary;




/********************************************/
/*   structure difinition about keyword		*/
/********************************************/


typedef struct {
		Four sentence;
		Four noun;
} Position;

typedef struct {
	unsigned char *keyword;	
	
	Position *positionList;
	Four nPositions;
	
	Four maxPositionNum;
} KeywordBuffer;


typedef struct {
	Boolean isUsed;

	/*
	 * Add ngramQueryMode field to extract keywords for query
	 * Value of ngramQueryMode field means following:
	 * if ngramQueryMode = TRUE, extract n-gram keywords for query.
	 *    keywords are not sorted, and same keywords are duplicated
	 * else, extract keywords for building text index
	 *    keywords are sorted, and same keywords are not duplicated. 
	 *
	 * Default value of ngramQueryMode is FALSE 
	 * Value of ngramQueryMode can be changed by calling function setNgramQueryMode()
	 */
	Boolean ngramQueryMode;
	
	char* readBuffer;
	Four readBufferSize;

	KeywordBuffer* keywordBuffer;
	Four nKeywords;		
	Four maxKeywordNum;
	
	Four keywordOffset;
} KorKeywordExtractorHandle;




/********************************************/
/*   		function declaration			*/
/********************************************/



/* 	functions for stem	 */

static int WordSize();
static int ContainsVowel();
static int AddAnE();
static int RemoveAnE();
static int ReplaceEnd();




/* Functions about dictoinary */

char* InitJosaEomiTable(void);
char* InitStopwordTable(void);
char* InitOnenounTable(void);
char* InitOnepronounTable(void);
char* InitSpecialnounTable(void);		
char* InitJosaTable(void);			
Four AddStringToHashTable(char** table, char* word);
Four FindStringInHashTable(char** table, char* word);
void PrintHashTable(char** tablen ,int hashValue);



/* Functions about Hash table */
Four GetHashValue (char* word);



/* Functions about the Dictionary */
int InitJosaEomiDic(FILE* fp_josafile, FILE* fp_eomifile);
int InitStopwordDic(FILE* fp_korstopfile, FILE* fp_engstopfile);
int InitOnenounDic(FILE* fp_onenounfile);
int InitOnepronounDic(FILE* fp_onenounfile);
int InitSpecialnounDic(FILE* fp_specialnounfile1, FILE* fp_specialnounfile2);	
int InitJosaDic(FILE* fp_josafile);	

/*  Functions abount processing hangle and special characters */
int ConvertHanjaToHangle( char* ptr_buffer);
int Remove_SpecialChar( char* buffer);
int Remove_SpecialCharForQuery( char* buffer);
int Remove_SpecialCharForHighlight( char* buffer);
int IsInvisible(char ch);
void EliminateInvisibleChar(char* str);
void ConvertSymbolCharToSpace(char* str);
void ConvertSymbolCharExceptAsteriskToSpace(char* str);
void ConvertAsteriskSymbolCharToSpace(char* str);
void EliminateTooLongKeyword(char* str);

int Read_Sentence( char* buffer, char* 	sentence, Four 	str_idx_buffer);
int Read_Word( char*	sentence, char*	word, Four 	str_idx_sentence );
int IsInvisibleChar(char* ptr_buffer);
int IsWordKorean( char* word );
int IsWordEnglish( char* word);



/* Functions about saving keywords */
Four strcmp2( unsigned char* str1, unsigned char* str2);
Four InsertKeyword(Four handle, unsigned char* word, Four sentence, Four noun);
Four InsertPosition(Four handle, Four idx, unsigned char* word, Four sentence, Four noun);
Four SearchKeyword(Four handle, unsigned char* word);
Four StoreKeyword(Four handle, unsigned char* word, Four sentence, Four noun);


/*    Functions about the handle     */
int FindFirstFreeHandle(void);
int InitKorKeywordExtractor(void);
int InitDictionaryHashTable(void);
int InitKorKeywordExtractHandle(void);
int InitKorKeywordExtractBuffer(Four handle);
int setNgramQueryMode(Four handle, Boolean ngramQueryMode);



/*     Extract the keywords form memory		*/
void CheckAndAppendStopword(char* word, char* ptrToNewStopwords);
void RemoveStopword(char* word);
void RemoveJosaEomi(char* word);
void IsOneNoun(char* word);
int IsSpecialNoun(char* word);
int IsWordLengthOne(char* word);

int StopwordExtract(Four handle, char* ptrToData, char* ptrToStopwords, Boolean ngramMode);

int KeywordExtract(Four	handle, char* ptrToData, Boolean ngramMode);


/* Main functions to extract the keywords */
int KorExtractor_Init( Four locationOfContent, OOSQL_SystemHandle* handle, Four	volId, char* className,	OID* oid, Two colNo, char* inFileOrContent,	Four* resultHandle );
int KorExtractor_Next( Four  handle, char* keyword, Four *nPositions, char* positionList);
int KorExtractor_Final(Four handle);

/* Make NGRAM Query in a form of  external function */
void RemoveEmptyParenthesis(char* word);
int IsCorrectNumOfParenthesis(char* word);
int MakeNgramKeywords(char* in, char* out);
int StemizeKeyword(char* in, char* out);
int MakeHighLightKeywords(char* in, char* out, char* out2);

#endif
