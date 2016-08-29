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
 *	Include header file 
 */
#include "EngKeywordExtractor.h"
#include "memory.h"



/* 
 * 	Table	
 */
CharClassType char_class[256] = {
	/* ^@ */ EOS_CH,		/* ^A */ OTHER_CH,		/* ^B */ OTHER_CH,		/* ^C */ OTHER_CH,
	/* ^D */ OTHER_CH,		/* ^E */ OTHER_CH,		/* ^F */ OTHER_CH,		/* ^G */ OTHER_CH,
	/* ^H */ WHITE_CH,		/* ^I */ WHITE_CH,		/* ^J */ WHITE_CH,		/* ^K */ WHITE_CH,
	/* ^L */ WHITE_CH,		/* ^M */ WHITE_CH,		/* ^N */ OTHER_CH,		/* ^O */ OTHER_CH,
	/* ^P */ OTHER_CH,		/* ^Q */ OTHER_CH,		/* ^R */ OTHER_CH,		/* ^S */ OTHER_CH,
	/* ^T */ OTHER_CH,		/* ^U */ OTHER_CH,		/* ^V */ OTHER_CH,		/* ^W */ OTHER_CH,
	/* ^X */ OTHER_CH,		/* ^Y */ OTHER_CH,		/* ^Z */ OTHER_CH,		/* ^[ */ OTHER_CH,
	/* ^\ */ OTHER_CH,		/* ^] */ OTHER_CH,		/* ^^ */ OTHER_CH,		/* ^_ */ OTHER_CH,
	/*    */ WHITE_CH,		/*  ! */ OTHER_CH,		/*  " */ OTHER_CH,		/*  # */ OTHER_CH,
	/*  $ */ OTHER_CH,		/*  % */ OTHER_CH,		/*  & */ OTHER_CH,		/*  ' */ OTHER_CH,
	/*  ( */ OTHER_CH,		/*  ) */ OTHER_CH,		/*  * */ OTHER_CH,		/*  + */ OTHER_CH,
	/*  , */ OTHER_CH,		/*  - */ OTHER_CH,		/*  . */ DOT_CH,		/*  / */ OTHER_CH,
	/*  0 */ DIGIT_CH,		/*  1 */ DIGIT_CH,		/*  2 */ DIGIT_CH,		/*  3 */ DIGIT_CH,
	/*  4 */ DIGIT_CH,		/*  5 */ DIGIT_CH,		/*  6 */ DIGIT_CH,		/*  7 */ DIGIT_CH,
	/*  8 */ DIGIT_CH,		/*  9 */ DIGIT_CH,		/*  : */ OTHER_CH,		/*  ; */ OTHER_CH,
	/*  < */ OTHER_CH,		/*  = */ OTHER_CH,		/*  > */ OTHER_CH,		/*  ? */ OTHER_CH,
	/*  @ */ OTHER_CH,		/*  A */ LETTER_CH,		/*  B */ LETTER_CH,		/*  C */ LETTER_CH,
	/*  D */ LETTER_CH,		/*  E */ LETTER_CH,		/*  F */ LETTER_CH,		/*  G */ LETTER_CH,
	/*  H */ LETTER_CH,		/*  I */ LETTER_CH,		/*  J */ LETTER_CH,		/*  K */ LETTER_CH,
	/*  L */ LETTER_CH,		/*  M */ LETTER_CH,		/*  N */ LETTER_CH,		/*  O */ LETTER_CH,
	/*  P */ LETTER_CH,		/*  Q */ LETTER_CH,		/*  R */ LETTER_CH,		/*  S */ LETTER_CH,
	/*  T */ LETTER_CH,		/*  U */ LETTER_CH,		/*  V */ LETTER_CH,		/*  W */ LETTER_CH,
	/*  X */ LETTER_CH,		/*  Y */ LETTER_CH,		/*  Z */ LETTER_CH,		/*  [ */ OTHER_CH,
	/*  \ */ OTHER_CH,		/*  ] */ OTHER_CH,		/*  ^ */ OTHER_CH,		/*  _ */ OTHER_CH,
	/*  ` */ OTHER_CH,		/*  a */ LETTER_CH,		/*  b */ LETTER_CH,		/*  c */ LETTER_CH,
	/*  d */ LETTER_CH,		/*  e */ LETTER_CH,		/*  f */ LETTER_CH,		/*  g */ LETTER_CH,
	/*  h */ LETTER_CH,		/*  i */ LETTER_CH,		/*  j */ LETTER_CH,		/*  k */ LETTER_CH,
	/*  l */ LETTER_CH,		/*  m */ LETTER_CH,		/*  n */ LETTER_CH,		/*  o */ LETTER_CH,
	/*  p */ LETTER_CH,		/*  q */ LETTER_CH,		/*  r */ LETTER_CH,		/*  s */ LETTER_CH,
	/*  t */ LETTER_CH,		/*  u */ LETTER_CH,		/*  v */ LETTER_CH,		/*  w */ LETTER_CH,
	/*  x */ LETTER_CH,		/*  y */ LETTER_CH,		/*  z */ LETTER_CH,		/*  { */ OTHER_CH,
	/*  | */ OTHER_CH,		/*  } */ OTHER_CH,		/*  ~ */ OTHER_CH,		/* ^? */ OTHER_CH,		
			 OTHER_CH,				 OTHER_CH,				 OTHER_CH,				 OTHER_CH,
			 OTHER_CH,				 OTHER_CH,				 OTHER_CH,				 OTHER_CH,
			 OTHER_CH,				 OTHER_CH,				 OTHER_CH,				 OTHER_CH,
			 OTHER_CH,				 OTHER_CH,				 OTHER_CH,				 OTHER_CH,
			 OTHER_CH,				 OTHER_CH,				 OTHER_CH,				 OTHER_CH,
			 OTHER_CH,				 OTHER_CH,				 OTHER_CH,				 OTHER_CH,
			 OTHER_CH,				 OTHER_CH,				 OTHER_CH,				 OTHER_CH,
			 OTHER_CH,				 OTHER_CH,				 OTHER_CH,				 OTHER_CH,
			 OTHER_CH,				 OTHER_CH,				 OTHER_CH,				 OTHER_CH,
			 OTHER_CH,				 OTHER_CH,				 OTHER_CH,				 OTHER_CH,
			 OTHER_CH,				 OTHER_CH,				 OTHER_CH,				 OTHER_CH,
			 OTHER_CH,				 OTHER_CH,				 OTHER_CH,				 OTHER_CH,
			 OTHER_CH,				 OTHER_CH,				 OTHER_CH,				 OTHER_CH,
			 OTHER_CH,				 OTHER_CH,				 OTHER_CH,				 OTHER_CH,
			 OTHER_CH,				 OTHER_CH,				 OTHER_CH,				 OTHER_CH,
			 OTHER_CH,				 OTHER_CH,				 OTHER_CH,				 OTHER_CH,
			 OTHER_CH,				 OTHER_CH,				 OTHER_CH,				 OTHER_CH,
			 OTHER_CH,				 OTHER_CH,				 OTHER_CH,				 OTHER_CH,
			 OTHER_CH,				 OTHER_CH,				 OTHER_CH,				 OTHER_CH,
			 OTHER_CH,				 OTHER_CH,				 OTHER_CH,				 OTHER_CH,
			 OTHER_CH,				 OTHER_CH,				 OTHER_CH,				 OTHER_CH,
			 OTHER_CH,				 OTHER_CH,				 OTHER_CH,				 OTHER_CH,
			 OTHER_CH,				 OTHER_CH,				 OTHER_CH,				 OTHER_CH,
			 OTHER_CH,				 OTHER_CH,				 OTHER_CH,				 OTHER_CH,
			 OTHER_CH,				 OTHER_CH,				 OTHER_CH,				 OTHER_CH,
			 OTHER_CH,				 OTHER_CH,				 OTHER_CH,				 OTHER_CH,
			 OTHER_CH,				 OTHER_CH,				 OTHER_CH,				 OTHER_CH,
			 OTHER_CH,				 OTHER_CH,				 OTHER_CH,				 OTHER_CH,
			 OTHER_CH,				 OTHER_CH,				 OTHER_CH,				 OTHER_CH,
			 OTHER_CH,				 OTHER_CH,				 OTHER_CH,				 OTHER_CH,
			 OTHER_CH,				 OTHER_CH,				 OTHER_CH,				 OTHER_CH,
			 OTHER_CH,				 OTHER_CH,				 OTHER_CH,				 OTHER_CH		};


unsigned char convert_case[256] = {
	/* ^@ */ 	  0,		/* ^A */ 	  0,		/* ^B */ 	  0,		/* ^C */ 	  0,
	/* ^D */ 	  0,		/* ^E */ 	  0,		/* ^F */ 	  0,		/* ^G */ 	  0,
	/* ^H */ 	  0,		/* ^I */ 	  0,		/* ^J */ 	  0,		/* ^K */ 	  0,
	/* ^L */ 	  0,		/* ^M */ 	  0,		/* ^N */ 	  0,		/* ^O */ 	  0,
	/* ^P */ 	  0,		/* ^Q */ 	  0,		/* ^R */ 	  0,		/* ^S */ 	  0,
	/* ^T */ 	  0,		/* ^U */ 	  0,		/* ^V */ 	  0,		/* ^W */ 	  0,
	/* ^X */ 	  0,		/* ^Y */ 	  0,		/* ^Z */ 	  0,		/* ^[ */ 	  0,
	/* ^\ */ 	  0,		/* ^] */ 	  0,		/* ^^ */ 	  0,		/* ^_ */ 	  0,
	/*    */ 	' ',		/*  ! */    '!',		/*  " */    '"',		/*  # */    '#',
	/*  $ */ 	'$',		/*  % */    '%',		/*  & */    '&',		/*  ' */   '\'',
	/*  ( */ 	'(',		/*  ) */    ')',		/*  * */    '*',		/*  + */    '+',
	/*  , */  	',',		/*  - */    '-',		/*  . */    '.',		/*  / */    '/',
	/*  0 */ 	'0',		/*  1 */    '1',		/*  2 */    '2',		/*  3 */    '3',
	/*  4 */    '4',		/*  5 */    '5',		/*  6 */    '6',		/*  7 */    '7',
	/*  8 */    '8',		/*  9 */    '9',		/*  : */    ':',		/*  ; */    ';',
	/*  < */    '<',		/*  = */    '=',		/*  > */    '>',		/*  ? */    '?',
	/*  @ */    '@',		/*  A */    'a',		/*  B */    'b',		/*  C */    'c',
	/*  D */    'd',		/*  E */    'e',		/*  F */    'f',		/*  G */    'g',
	/*  H */    'h',		/*  I */    'i',		/*  J */    'j',		/*  K */    'k',
	/*  L */    'l',		/*  M */    'm',		/*  N */    'n',		/*  O */    'o',
	/*  P */    'p',		/*  Q */    'q',		/*  R */    'r',		/*  S */    's',
	/*  T */    't',		/*  U */    'u',		/*  V */    'v',		/*  W */    'w',
	/*  X */    'x',		/*  Y */    'y',		/*  Z */    'z',		/*  [ */    '[',
	/*  \ */   '\\',		/*  ] */    ']',		/*  ^ */    '^',		/*  _ */    '_',
	/*  ` */    '`',		/*  a */    'a',		/*  b */    'b',		/*  c */    'c',
	/*  d */    'd',		/*  e */    'e',		/*  f */    'f',		/*  g */    'g',
	/*  h */    'h',		/*  i */    'i',		/*  j */    'j',		/*  k */    'k',
	/*  l */    'l',		/*  m */    'm',		/*  n */    'n',		/*  o */    'o',
	/*  p */    'p',		/*  q */    'q',		/*  r */    'r',		/*  s */    's',
	/*  t */    't',		/*  u */    'u',		/*  v */    'v',		/*  w */    'w',
	/*  x */    'x',		/*  y */    'y',		/*  z */    'z',		/*  { */    '{',
	/*  | */    '|',		/*  } */    '}',		/*  ~ */    '~',		/* ^? */ 	  0,
				128,					129,					130,					131,
				132,					133,					134,					135,
				136,					137,					138,					139,
				140,					141,					142,					143,
				144,					145,					146,					147,
				148,					149,					150,					151,
				152,					153,					154,					155,
				156,					157,					158,					159,
				160,					161,					162,					163,
				164,					165,					166,					167,
				168,					169,					170,					171,
				172,					173,					174,					175,
				176,					177,					178,					179,
				180,					181,					182,					183,
				184,					185,					186,					187,
				188,					189,					190,					191,
				192,					193,					194,					195,
				196,					197,					198,					199,
				200,					201,					202,					203,
				204,					205,					206,					207,
				208,					209,					210,					211,
				212,					213,					214,					215,
				216,					217,					218,					219,
				220,					221,					222,					223,
				224,					225,					226,					227,
				228,					229,					230,					231,
				232,					233,					234,					235,
				236,					237,					238,					239,
				240,					241,					242,					243,
				244,					245,					246,					247,
				248,					249,					250,					251,
				252,					253,					254,					255		};



char *stopList1[] = 		/* Oracle8's stop list */
{
	/* A */	"a", "about", "after", "all", "also", "an", "any", "and", "are", "as", "at", 
	/* B */	"be", "because", "been", "but", "by",
	/* C */ "can", "co", "corp", "could", 
	/* D */	
	/* E */
	/* F */	"for", "from", 
	/* G */
	/* H */	"had", "has", "have", "he", "her", "his", 
	/* I */	"if", "in", "inc", "into", "is", "it", "its", 
	/* J */
	/* K */
	/* L */	"last", 
	/* M */	"more", "most", "mr", "mrs", "ms", "mz",
	/* N */	"no", "not", 
	/* O */	"only", "of", "on", "one", "or", "other", "out", "over", 
	/* P */
	/* Q */
	/* R */
	/* S */	"s", "so", "says", "she", "some", "such", 
	/* T */	"than", "that", "the", "their", "there", "they", "this", "to", 
	/* U */	"up",
	/* V */
	/* W */	"was", "we", "were", "when", "which", "who", "will", "with", "would",
	/* X */
	/* Y */
	/* Z */
	"-"
};


char *stopList2[] = 		/* BRS/Search's stop list */
{
	/* A */	"a", "about", "all", "among", "an", "and", "are", "as", "at", 
	/* B */	"be", "been", "between", "both", "but", "by", 
	/* C */ 
	/* D */	"do", "during", 
	/* E */	"each", "either", 
	/* F */	"for", "found", "from", "further", 
	/* G */
	/* H */	"has", "have", "however", 
	/* I */	"if", "in", "into", "is", "it", "its", 
	/* J */
	/* K */
	/* L */	
	/* M */	"made", "make", "many", "more", "most", "must",
	/* N */	"no", "not", 
	/* O */	"of", "on", "or", 
	/* P */
	/* Q */
	/* R */
	/* S */ "same", "several", "some", "such", 
	/* T */	"than", "that", "the", "their", "these", "they", "this", "those", "through", "to", "toward",  
	/* U */	"upon", "used", "using", 
	/* V */
	/* W */	"was", "were", "what", "which", "while", "who", "will", "with", "within", "would",
	/* X */
	/* Y */
	/* Z */
	"-"
};




/* 
 *	Global variables 	
 */
int	startIRFlag = 0;

QuickFitMM_Handle				staticMMHandle;	
QuickFitMM_Handle				dynamicMMHandle;	

EngKeywordExtractorHandle	*engKeywordExtractorHandle;
Four						maxHandleNum;
Four						nHandle = 0;

StopwordList				stopwordTable[NUMOFALPHABET];

int findFirstFreeHandle();

int EngKeywordExtractor_Init (
    Four			locationOfContent,		/* IN  flag whether input is file, db, or memory */
    LOM_Handle		*handle,				/* IN  LOM handle */
    Four			volId,					/* IN  volume id */
    char			*className,				/* IN  class name */
    OID				*oid,					/* IN  object id */
    Two				colNo,					/* IN  column from which keywords are extracted */
    char			*inFileOrContent,		/* IN  input file name if input is file, or content string if input is memory */
    Four			*resultHandle)			/* OUT handler to deal with current keyword extractor */
{
	Four				e;
	Four				i, j, k;
	FILE				*fp;
	Four				ocn;
	TextColStruct		tclist[1];
	LOM_TextDesc		textDesc;
	unsigned char		*ptrToData;
	unsigned char		*ptrToEnd;
	unsigned char		*ptr;
	Four				idx;
	unsigned char		token[MAXLENGTHOFKEYWORD];
	Four				memSize;
	Four				firstFreeHandle;
	Four				readByte;
	KeywordTokenType	tokType;
	Four				sentence_position=1, noun_position=1;
	char				**tempList;	

	/* FOR DEBUG */
	/*
	printf("EngKeywordExtractor_Init : locationOfContent = %d, handle = %X, volId = %d, className = %X, oid = %X, colNo = %d, inFileOrContent = %X, resultHandle = %X\n", 
		locationOfContent, handle, volId, className, oid, colNo, inFileOrContent, resultHandle);
	*/

	/*
	 *	I. Initialize static memory pool for EngKeywordExtractorHandle
	 */
	if (startIRFlag == 0)
	{
		/* initialize memory handle */
		e = QuickFitMM_Init(&staticMMHandle, MAXBUFFERSIZE);
		if (e < 0)	QUICKFITMM_ERROR(e);

		e = QuickFitMM_Init(&dynamicMMHandle, MAXBUFFERSIZE);
		if (e < 0)	QUICKFITMM_ERROR(e);

		/* alloc memory for engKeywordExtractorHandle */
		memSize = sizeof(EngKeywordExtractorHandle) * MAXNUMOFHANDLE;
		engKeywordExtractorHandle = (EngKeywordExtractorHandle*)QuickFitMM_Alloc(&staticMMHandle, memSize); 

		/* set value of maxHandleNum with MAXNUMOFHANDLE */
		maxHandleNum = MAXNUMOFHANDLE;

		/* initialize isUsed field of engKeywordExtractorHandle array */
		for (i = 0; i < maxHandleNum; i++)
		{
			engKeywordExtractorHandle[i].isUsed  		= SM_FALSE;
			engKeywordExtractorHandle[i].readBuffer  	= NULL;
			engKeywordExtractorHandle[i].keywordBuffer  = NULL;
			engKeywordExtractorHandle[i].maxKeywordNum	= 0;
			engKeywordExtractorHandle[i].nKeyword 		= 0;
			engKeywordExtractorHandle[i].keywordOffset  = 0;
		}


		/* initialize stopword table */
		for (i = 0; i < NUMOFALPHABET; i++)
		{
			memSize = sizeof(unsigned char *) * MAXNUMOFSTOPWORD;
			stopwordTable[i].wordList = (unsigned char **)QuickFitMM_Alloc(&staticMMHandle, memSize); 

			memSize = sizeof(Four) * MAXNUMOFSTOPWORD;
			stopwordTable[i].lenList = (Four *)QuickFitMM_Alloc(&staticMMHandle, memSize); 

			stopwordTable[i].maxStopwordNum = MAXNUMOFSTOPWORD;
			stopwordTable[i].nStopword 		= 0;
		}

		/* NOTE : tempTable must be stopword set with lower case letter */ 
		/* load stopword table */
		i = 0;
		tempList = stopList1; 
		while(tempList[i][0] != '-')
		{
			j = tempList[i][0] - 'a';
			k = stopwordTable[j].nStopword;
			memSize = (sizeof(tempList[i]) + 1) * sizeof(unsigned char);
			stopwordTable[j].wordList[k] = (unsigned char *)QuickFitMM_Alloc(&staticMMHandle, memSize); 
			strcpy((char *)stopwordTable[j].wordList[k], (const char *)tempList[i]);
			stopwordTable[j].lenList[k] = strlen(tempList[i]);
			stopwordTable[j].nStopword++;

			if (stopwordTable[j].nStopword == stopwordTable[j].maxStopwordNum)
			{
				memSize = sizeof(unsigned char *) * stopwordTable[j].maxStopwordNum * 2;
				e = QuickFitMM_ReAlloc(&staticMMHandle, (void**)&stopwordTable[i].wordList, memSize); 
				if (e < 0)	QUICKFITMM_ERROR(e);

				memSize = sizeof(Four *) * stopwordTable[j].maxStopwordNum * 2;
				e = QuickFitMM_ReAlloc(&staticMMHandle, (void**)&stopwordTable[i].lenList, memSize); 
				if (e < 0)	QUICKFITMM_ERROR(e);

				stopwordTable[j].maxStopwordNum = stopwordTable[j].maxStopwordNum * 2;
			}

			i++;
		}


		/* set startIRflag */
		startIRFlag = 1;
	}
	
	/*
	 *	II. Find first free handle of engKeywordExtractorHandle  
	 */
	nHandle++;
	firstFreeHandle = findFirstFreeHandle();
	LOM_CHECK_ERROR(handle, firstFreeHandle); 

	*resultHandle = firstFreeHandle;

	/*
	 *	III. Initialize selected handle 
	 */
	engKeywordExtractorHandle[firstFreeHandle].isUsed  		 = SM_TRUE;
	engKeywordExtractorHandle[firstFreeHandle].readBuffer  	 = (unsigned char *)QuickFitMM_Alloc(&dynamicMMHandle, MAXLENGTHOFBUFFER);

	memSize = sizeof(KeywordBuffer) * MAXNUMOFKEYWORD;
	engKeywordExtractorHandle[firstFreeHandle].keywordBuffer = (KeywordBuffer*)QuickFitMM_Alloc(&dynamicMMHandle, memSize); 

	memset(engKeywordExtractorHandle[firstFreeHandle].keywordBuffer, 0, memSize);

	engKeywordExtractorHandle[firstFreeHandle].maxKeywordNum = MAXNUMOFKEYWORD;
	
	engKeywordExtractorHandle[firstFreeHandle].nKeyword 	 = 0;
	engKeywordExtractorHandle[firstFreeHandle].keywordOffset = 0;

	/*
	 *	IV. Extract keyword 
	 */
	if (locationOfContent == TEXT_IN_FILE)
	{
		/* 
		 *	1. Open input flie 
		 */
		if ((fp = fopen(inFileOrContent, "rb")) == NULL)
		{
			fprintf(stderr, "Can't open %s input-file in EngKeywordExtractor_Init\n", inFileOrContent);
			exit(-1);
		}

	
		/* 
		 *	2. Extract keywords from input file  
		 */
		readByte = fread(engKeywordExtractorHandle[firstFreeHandle].readBuffer, 1, MAXLENGTHOFBUFFER, fp);

		idx = 0;
		while (readByte == MAXLENGTHOFBUFFER-idx)
		{
			/* 2-1. Remove last incomplete token in buffer */
			ptr = ptrToEnd = &engKeywordExtractorHandle[firstFreeHandle].readBuffer[MAXLENGTHOFBUFFER-1];
			while (char_class[*ptr] != WHITE_CH)	ptr--;
			*ptr = '\0';	

			/* 2-2. Get token and store token */ 
			ptrToData = engKeywordExtractorHandle[firstFreeHandle].readBuffer;
			eliminateInvisibleChar((char*)ptrToData);
			convertSymbolCharToSpace((char*)ptrToData);

			tokType = getToken(&ptrToData, token);
			while (tokType != END_TOKEN)
			{
				if (tokType == DOT_TOKEN)
				{
					sentence_position++;
					noun_position = 1;
				}
				else 
				{
					if (isStopword(token) != 1)
					{
						storeToken(firstFreeHandle, token, sentence_position, noun_position);
						noun_position++;
					}
				}
				tokType = getToken(&ptrToData, token);
			}
			
			/* 2-3. Move last incomplete token into start position of buffer */
			idx = 0;
			while (ptr != ptrToEnd) 
			{
				ptr++;
				engKeywordExtractorHandle[firstFreeHandle].readBuffer[idx] = *ptr;	
				idx++;
			}

			/* 2-4. Read bytes from input-file stream */ 
			readByte = fread(&engKeywordExtractorHandle[firstFreeHandle].readBuffer[idx], 1, MAXLENGTHOFBUFFER-idx, fp);
		}
	
		engKeywordExtractorHandle[firstFreeHandle].readBuffer[idx + readByte] = '\0';
		ptrToData = engKeywordExtractorHandle[firstFreeHandle].readBuffer;
		eliminateInvisibleChar((char*)ptrToData);
		convertSymbolCharToSpace((char*)ptrToData);

		tokType = getToken(&ptrToData, token);
		while (tokType != END_TOKEN)
		{
			if (tokType == DOT_TOKEN)
			{
				sentence_position++;
				noun_position = 1;
			}
			else 
			{
				if (isStopword(token) != 1)
				{
					storeToken(firstFreeHandle, token, sentence_position, noun_position);
					noun_position++;
				}
			}
			tokType = getToken(&ptrToData, token);
		}
	

		/* 
		 *	3. Close input flie 
		 */
		if ((fclose(fp)) == EOF)
		{
			fprintf(stderr, "Can't close %s input-file in EngKeywordExtractor_Init\n", inFileOrContent);
			exit(-1);
		}
	}
	else if(locationOfContent == TEXT_IN_MEMORY)
	{
		ptrToData = (unsigned char*)inFileOrContent;
		eliminateInvisibleChar((char*)ptrToData);
		convertSymbolCharToSpace((char*)ptrToData);

		tokType = getToken(&ptrToData, token);
		while (tokType != END_TOKEN)
		{
			if (tokType == DOT_TOKEN)
			{
				sentence_position++;
				noun_position = 1;
			}
			else 
			{
				if (isStopword(token) != 1)
				{
					storeToken(firstFreeHandle, token, sentence_position, noun_position);
					noun_position++;
				}
			}
			tokType = getToken(&ptrToData, token);
		}
	}
	else if(locationOfContent == TEXT_IN_DB)
	{
		/* 
		 *	1. Open class and Initialize text col-struct & text descriptor
		 */
		ocn = LOM_OpenClass(handle, volId, className);		
		LOM_CHECK_ERROR(handle, ocn);
		
        e = LOM_Text_GetDescriptor(handle, ocn, SM_FALSE, oid, colNo, &textDesc); 
        LOM_CHECK_ERROR(handle, e);


		/* 
		 *	2. Extract keywords from input file  
		 */
		tclist[0].start 	 = 0;
		tclist[0].length     = MAXLENGTHOFBUFFER;
		tclist[0].dataLength = MAXLENGTHOFBUFFER;
		tclist[0].data		 = engKeywordExtractorHandle[firstFreeHandle].readBuffer;

       	e = LOM_Text_FetchContent(handle, ocn, SM_FALSE, oid, colNo, &tclist[0], &textDesc);
       	LOM_CHECK_ERROR(handle, e);
		((char*)tclist[0].data)[tclist[0].retLength] = '\0';

		idx = 0;
		while(tclist[0].retLength == MAXLENGTHOFBUFFER-idx)
		{
			/* 2-1. Remove last incomplete token in buffer */
			ptr = ptrToEnd = &engKeywordExtractorHandle[firstFreeHandle].readBuffer[MAXLENGTHOFBUFFER-1];
			while (char_class[*ptr] != WHITE_CH)	ptr--;
			*ptr = '\0';	

			/* 2-2. Get token and store token */ 
			ptrToData = engKeywordExtractorHandle[firstFreeHandle].readBuffer;
			eliminateInvisibleChar((char*)ptrToData);
			convertSymbolCharToSpace((char*)ptrToData);

			tokType = getToken(&ptrToData, token);
			while (tokType != END_TOKEN)
			{
				if (tokType == DOT_TOKEN)
				{
					sentence_position++;
					noun_position = 1;
				}
				else 
				{
					if (isStopword(token) != 1)
					{
						storeToken(firstFreeHandle, token, sentence_position, noun_position);
						noun_position++;
					}
				}
				tokType = getToken(&ptrToData, token);
			}

			/* 2-3. Move last incomplete token into start position of buffer */
			idx = 0;
			while (ptr != ptrToEnd)
			{
				ptr++;
				engKeywordExtractorHandle[firstFreeHandle].readBuffer[idx] = *ptr;	
				idx++;
			}

			/* 2-4. Read bytes from text content */ 
            tclist[0].start 	+= tclist[0].retLength;
			tclist[0].length     = MAXLENGTHOFBUFFER - idx;
			tclist[0].dataLength = MAXLENGTHOFBUFFER - idx;
			tclist[0].data		 = &engKeywordExtractorHandle[firstFreeHandle].readBuffer[idx];

       		e = LOM_Text_FetchContent(handle, ocn, SM_FALSE, oid, colNo, &tclist[0], &textDesc);
       		LOM_CHECK_ERROR(handle, e);
		}

		engKeywordExtractorHandle[firstFreeHandle].readBuffer[idx + tclist[0].retLength] = '\0';
		ptrToData = engKeywordExtractorHandle[firstFreeHandle].readBuffer;
		eliminateInvisibleChar((char*)ptrToData);
		convertSymbolCharToSpace((char*)ptrToData);

		tokType = getToken(&ptrToData, token);
		while (tokType != END_TOKEN)
		{
			if (tokType == DOT_TOKEN)
			{
				sentence_position++;
				noun_position = 1;
			}
			else 
			{
				if (isStopword(token) != 1)
				{
					storeToken(firstFreeHandle, token, sentence_position, noun_position);
					noun_position++;
				}
			}
			tokType = getToken(&ptrToData, token);
		}


		/* 
		 *	3. Close class
		 */
		e = LOM_CloseClass(handle, ocn);
		LOM_CHECK_ERROR(handle, e);
	}
	else
	{
		fprintf(stderr, "Unknown input method is used. Since input string is now given, abandon extracting keywords\n");
		exit(-1);
	}

	return eNOERROR;
}


int EngKeywordExtractor_Next (
    Four			handle,					/* IN  handler to deal with current keyword extractor */
    char			*keyword,				/* OUT current keyword */
    Four			*nPositions,			/* OUT # of position */	
    char			*positionList)			/* OUT position list of the current keyword */
{
    Four            i;
    Four            remain;
    Four            bufferOffset = 0;
    Four            keywordOffset;
    PositionList    *pList;

	keywordOffset = engKeywordExtractorHandle[handle].keywordOffset;
    if (keywordOffset >= engKeywordExtractorHandle[handle].nKeyword)
        return TEXT_DONE;

    strcpy(keyword, (char *)engKeywordExtractorHandle[handle].keywordBuffer[keywordOffset].keyword);
    *nPositions = engKeywordExtractorHandle[handle].keywordBuffer[keywordOffset].nPosition;

    pList = engKeywordExtractorHandle[handle].keywordBuffer[keywordOffset].list;
    while (pList->next != NULL)
    {
        for (i = 0; i < MAXNUMOFPOSITIONPERLIST; i++)
        {
            memcpy(&positionList[bufferOffset], &pList->posArray[i], sizeof(Position));
            bufferOffset += sizeof(Position);
        }
        pList = pList->next;
    }
    remain = *nPositions % MAXNUMOFPOSITIONPERLIST;
    if (remain == 0)    remain = MAXNUMOFPOSITIONPERLIST;
    for (i = 0; i < remain; i++)
    {
        memcpy(&positionList[bufferOffset], &pList->posArray[i], sizeof(Position));
        bufferOffset += sizeof(Position);
    }

    engKeywordExtractorHandle[handle].keywordOffset++;

	return eNOERROR;
}


int EngKeywordExtractor_Final (
    Four			handle)					/* IN  handler to deal with current keyword extractor */
{
	Four			e;

	/*
	 *	I. Finalize given handle 
	 */
	engKeywordExtractorHandle[handle].isUsed  		= SM_FALSE;
	nHandle--;


	/*
	 *	II. Finalize dynamic memory pool
	 */
	e = QuickFitMM_ReInit(&dynamicMMHandle);
	if (e < 0)	QUICKFITMM_ERROR(e);

	return eNOERROR;
}



int findFirstFreeHandle()
{
	Four			i;
	Four			e;
	Four			memSize;


	if (nHandle >= maxHandleNum)
	{
		memSize = sizeof(EngKeywordExtractorHandle) * maxHandleNum * 2;
		e = QuickFitMM_ReAlloc(&staticMMHandle, (void**)&engKeywordExtractorHandle, memSize); 
		if (e < 0)	QUICKFITMM_ERROR(e);

		/* initialize isUsed field of engKeywordExtractorHandle array */
		for (i = maxHandleNum; i < maxHandleNum*2; i++)
		{
			engKeywordExtractorHandle[i].isUsed  		= SM_FALSE;
			engKeywordExtractorHandle[i].readBuffer  	= NULL;
			engKeywordExtractorHandle[i].keywordBuffer  = NULL;
			engKeywordExtractorHandle[i].maxKeywordNum	= 0;
			engKeywordExtractorHandle[i].nKeyword 		= 0;
			engKeywordExtractorHandle[i].keywordOffset  = 0;
		}

		maxHandleNum = maxHandleNum * 2;
	}


	for (i = 0; i < maxHandleNum; i++)
	{	
		if (engKeywordExtractorHandle[i].isUsed == SM_FALSE)
			break;
	}


	return i;
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
		if((unsigned)*str > (unsigned)0x80)
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

KeywordTokenType getToken (
	unsigned char	**pStr,
	unsigned char	*token)
{
	unsigned char	*str = *pStr;
	Four			idx = 0;


	while (char_class[*str] == WHITE_CH || char_class[*str] == OTHER_CH)	str++;


	while (char_class[*str] == DIGIT_CH || char_class[*str] == LETTER_CH)
	{
		token[idx] = convert_case[*str];
		idx++;
		str++;
	}
	token[idx] 	= '\0';
	*pStr 		= str;

	if (idx != 0)	
		return ((KeywordTokenType)TERM_TOKEN);	

	if (char_class[*str] == DOT_CH)	
	{
		*pStr = str+1;
		return ((KeywordTokenType)DOT_TOKEN);		
	}

	if (char_class[*str] == EOS_CH)	
		return ((KeywordTokenType)END_TOKEN);		

	return -1;
}


Four isStopword (
	unsigned char 	*token)
{
	Four			i;
	Four			idx;
	Four			tokenLength;

	idx = token[0] - 'a';
	if (idx < 0 || idx >= NUMOFALPHABET)
		return 0;

	tokenLength = strlen((char*)token);
	for (i = 0; i < stopwordTable[idx].nStopword; i++)
	{
		if (tokenLength == stopwordTable[idx].lenList[i])
		{
			if (memcmp(token, stopwordTable[idx].wordList[i], stopwordTable[idx].lenList[i]) == 0) 
				return 1;
		}
	}

	return 0;
}


Four storeToken (
	Four			handle,
	unsigned char	*token,
	Four			sentence_position,
	Four			noun_position)
{
	Four			e;
	Four			i;
	Four			idx;
	Four			memSize;
	Four			maxKeywordNum;
	
	maxKeywordNum = engKeywordExtractorHandle[handle].maxKeywordNum; 
	if (engKeywordExtractorHandle[handle].nKeyword >= maxKeywordNum) 
	{
		/* Doubling keyword buffer */
        memSize = sizeof(KeywordBuffer) * maxKeywordNum * 2;
        e = QuickFitMM_ReAlloc(&dynamicMMHandle, (void**)&engKeywordExtractorHandle[handle].keywordBuffer, memSize);
        if (e < 0)  QUICKFITMM_ERROR(e);

		/* Initialize keyword buffer */ 
		memset(&engKeywordExtractorHandle[handle].keywordBuffer[maxKeywordNum], 0, maxKeywordNum);

		engKeywordExtractorHandle[handle].maxKeywordNum = maxKeywordNum * 2; 
	}

	idx = searchKeyword(handle, token);

	if (idx == NOT_FOUND)
	{
		insertKeyword(handle, token, sentence_position, noun_position);
	}
	else 
	{
		insertPosition(handle, idx, token, sentence_position, noun_position);
	}


	return eNOERROR;
}


Four strcmp2 (
	unsigned char 	*s1,
	unsigned char 	*s2)
{
	Four			len;

	len = (strlen((const char *)s1) < strlen((const char *)s2)) ? 
		  strlen((const char *)s1)+1 : 
		  strlen((const char *)s2)+1; 

	return memcmp(s1, s2, len); 
}


Four searchKeyword (
	Four			handle,
	unsigned char	*keyword)
{
	Four			m;
	Four			l = 0;
	Four			u = engKeywordExtractorHandle[handle].nKeyword;	
	Four			result;

	if (u == 0)	
		return NOT_FOUND;

	while (l <= u)
	{	
		m = (l + u) / 2;

		if (m >= u)
			return NOT_FOUND;


		result = strcmp2(engKeywordExtractorHandle[handle].keywordBuffer[m].keyword, keyword);

		if (result == 0)	
			return m;
		else if (result < 0)
			l = m + 1;
		else 
			u = m - 1;
	}

	return NOT_FOUND;
}


PositionList *newPositionList (
	Four			sentence_position,
	Four			noun_position)
{
	PositionList	*pList;

	pList = (PositionList*)QuickFitMM_Alloc(&dynamicMMHandle, sizeof(PositionList));
	pList->posArray[0].sentence	= sentence_position;
	pList->posArray[0].noun		= noun_position; 
	pList->next					= NULL;

	return pList;
}

	
Four insertKeyword (
	Four			handle,
	unsigned char	*token,
	Four			sentence_position,
	Four			noun_position)
{
	Four			idx;	
	KeywordBuffer	*keywordBuffer;
	unsigned char	*tmpBuffer;
	unsigned char	*keyword;
	Four			nPosition;
	PositionList	*list;


	tmpBuffer = (unsigned char *)QuickFitMM_Alloc(&dynamicMMHandle, strlen((char*)token) * sizeof(unsigned char));
	strcpy((char *)tmpBuffer, (const char *)token);
	

	idx 	      = engKeywordExtractorHandle[handle].nKeyword;
	keywordBuffer = engKeywordExtractorHandle[handle].keywordBuffer;
	keywordBuffer[idx].keyword   = tmpBuffer;
	keywordBuffer[idx].list      = newPositionList(sentence_position, noun_position); 
	keywordBuffer[idx].nPosition = 1; 
	engKeywordExtractorHandle[handle].nKeyword++;

	
	keyword   = keywordBuffer[idx].keyword;
	nPosition = keywordBuffer[idx].nPosition;
	list 	  = keywordBuffer[idx].list;

	while (idx > 0)
	{
		if (strcmp2(keyword, keywordBuffer[idx-1].keyword)>0)
			break;
		else
		{
			idx--;
			keywordBuffer[idx+1].keyword 	= keywordBuffer[idx].keyword; 
			keywordBuffer[idx+1].nPosition  = keywordBuffer[idx].nPosition; 
			keywordBuffer[idx+1].list		= keywordBuffer[idx].list; 
		}
	}

	keywordBuffer[idx].keyword	 = keyword;
	keywordBuffer[idx].nPosition = nPosition;
	keywordBuffer[idx].list		 = list;


	return eNOERROR;
}


Four insertPosition (
	Four			handle,
	Four			idx,
	unsigned char	*token,
	Four			sentence_position,
	Four				noun_position)
{
	Four			posOffset;
	PositionList	*pList;


	pList = engKeywordExtractorHandle[handle].keywordBuffer[idx].list;
	while (pList->next != NULL)
		pList = pList->next;


	posOffset = engKeywordExtractorHandle[handle].keywordBuffer[idx].nPosition % MAXNUMOFPOSITIONPERLIST;	
	if (posOffset == 0)
	{
		pList->next = newPositionList(sentence_position, noun_position); 
	}
	else
	{
		pList->posArray[posOffset].sentence = sentence_position;
		pList->posArray[posOffset].noun		= noun_position;
	}
	engKeywordExtractorHandle[handle].keywordBuffer[idx].nPosition++;


	return eNOERROR;
}


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

    e = EngKeywordExtractor_Init(TEXT_IN_FILE, NULL, 0, NULL, NULL, 0, inFile, &handle);
	if (e < eNOERROR)
	{
		fprintf(stderr, "EngKeywordExtractor Initialization Fail\n");
		exit(-1);
	}

    chk = EngKeywordExtractor_Next(handle, (char *)keyword, &nPositions, positionListBuffer);
    while (chk != TEXT_DONE)
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

        chk = EngKeywordExtractor_Next(handle, (char *)keyword, &nPositions, positionListBuffer);
    }


	e = EngKeywordExtractor_Final(handle);
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
