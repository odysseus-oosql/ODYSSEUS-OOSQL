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

#ifndef _ENGKEYWORDEXTRACTOR_H_
#define _ENGKEYWORDEXTRACTOR_H_

/* 
 *	Include header file 
 */
#include <stdio.h>
#include <string.h>
#include "dblablib.h"
#include "cosmos_r.h"
#include "LOM.h"
#include "Catalog.h"
#include "QuickFitMM.hxx"


/*
 *	MACRO definition
 */
#define LOM_CHECK_ERROR(handle, e)		if (e < eNOERROR) exit(-1)
#define QUICKFITMM_ERROR(e)				if (e < eNOERROR) exit(-1)



/*
 * Constant definition	
 */
#define TEXT_IN_DB							0
#define TEXT_IN_FILE						1
#define TEXT_IN_MEMORY						2
#define TEXT_DONE							1

#define MAXBUFFERSIZE       	4 * 1024 * 1024
#define DEFAULTPOSTINGBUFFERSIZE    1024 * 160

#define MAXNUMOFHANDLE			100
#define MAXLENGTHOFKEYWORD		1000
#define MAXLENGTHOFBUFFER		1024 * 100
#define MAXNUMOFPOSITIONPERLIST	20
#define MAXNUMOFKEYWORD			10000	
#define MAXNUMOFSTOPWORD		10
#define NUMOFALPHABET			26


#define NOT_FOUND				-1


/*
 *	Type definition
 */
typedef enum {
	WHITE_CH,			/* whilespace characters */
	DIGIT_CH,			/* the digits */
	LETTER_CH,			/* upper and lower case */
	EOS_CH,				/* the end of string character */
	OTHER_CH,			/* catch-all for everything else */
	DOT_CH				/* the end of sentense */
} CharClassType;

typedef enum {
	TERM_TOKEN	= 1,	/* a search term */
	END_TOKEN	= 2, 	/* end of the query */
	DOT_TOKEN	= 3		/* end of sentense */
} KeywordTokenType;

typedef struct {
	Four			sentence;
	Four			noun;
} Position;

typedef struct PositionList {
	Position			posArray[MAXNUMOFPOSITIONPERLIST];
	struct PositionList	*next;
} PositionList;

typedef struct {
	unsigned char	*keyword;
	Four			nPosition;
	PositionList	*list;
} KeywordBuffer;

typedef struct {
	Boolean			isUsed;
	unsigned char	*readBuffer;
	KeywordBuffer   *keywordBuffer;
	Four            maxKeywordNum;
	Four            nKeyword;
	Four			keywordOffset;
} EngKeywordExtractorHandle;

typedef struct {
	unsigned char	**wordList;
	Four			*lenList;
	Four			maxStopwordNum;
	Four			nStopword;
} StopwordList;



/*
 * Function declaration 
 */

int EngKeywordExtractor_Init (Four, LOM_Handle*, Four, char*, OID*, Two, char*, Four*);
int EngKeywordExtractor_Next (Four, char*, Four*, char*);
int EngKeywordExtractor_Final (Four);  

KeywordTokenType getToken (unsigned char **, unsigned char *);
Four storeToken(Four, unsigned char *, Four, Four);
Four searchKeyword(Four, unsigned char *);
Four insertKeyword(Four, unsigned char *, Four, Four);
Four insertPosition(Four, Four, unsigned char *, Four, Four);
PositionList *newPositionList(Four, Four);
Four strcmp2(unsigned char *, unsigned char *);
Four isStopword(unsigned char *);
void eliminateInvisibleChar(char* str);
void convertSymbolCharToSpace(char* str);

#endif	/* _ENGKEYWORDEXTRACTOR_H_ */
