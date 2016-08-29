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
#include <ctype.h>

#define FALSE 0
#define TRUE 1

#define HIGHLIGHT_START "<b>"
#define HIGHLIGHT_END "</b>"
#define MAX_LENGTH 102400

// F_DEBUG: debug message, 0 is off, 1 is on
#define F_DEBUG 0
#define IJKIM_DEBUG 0


typedef unsigned char UCHAR;
struct hjtrans
{
	UCHAR hangul[2];
};

typedef struct hjtrans  HJTRANS;
#define  HJ_COUNTER  4888	

UCHAR *conv2hg (UCHAR *hjsrc);
UCHAR  *str2hg (UCHAR *src);

UCHAR *conv2hg (UCHAR *hjsrc)
{
	int		tindex;
	UCHAR	*resultp = (UCHAR *)0;

	static HJTRANS *hj_ptr = (HJTRANS*)"������������������������������������������������������������������������������������������������������������������������\
������������������������������������������������������������������������������������������������������������������������\
���������������������������������������������������������������ŰŰŰŰŰŰŰŰŰŰŰŰŰŰŰŰŰǰǰǰǰǰǰǰǰǰǰǰ�\
�ɰɰɰɰ˰˰˰˰˰˰˰̰̰̰԰԰԰ݰݰݰݰݰݰݰ߰߰߰߰߰߰߰߰߰߰߰�������������������������\
�������������������������������������������������������������\
�����������������������������������������������������������������\
������������������������������������������������������������������������������������������������������������������������\
������������������������������������������������������������������������������������������������������������������������\
�����������������������������������������������������������������������������������������������������������ññññññ�\
�ǱǱǱǱǱǱǱǱǱȱȱȱȱȱ˱˱˱˱˱˱ͱͱͱͱͱͱԱԱԱԱԱԱԱԱԱԱԱԱԱԱԱձձձձձձձֱرررررررٱٱٱ�\
�ٱٱٱٱٱٱٱٱٱٱٱ۱ݱݱݱݱݱݱݱݱݱݱݱݱݱݱޱޱޱޱޱޱޱ���������������������������\
�����������������������������������������������賣������������������������\
���������������������������������������������������������������������������������������������������ó����������\
���������������������������������������������������������������������������ƴɴɴɴɴɴ�\
�ϴϴддٴٴܴܴܴܴܴܴܴܴܴܴܴܴܴܴܴܴܴܴܴܴ޴޴޴޴޴�����������������������������\
���������������������������������������������������������������������������������������������������\
�������������������������������������������������������������������������������������εεεεεεεεεεεееееее�\
������������������������������������������������������������������������������������������������������\
���������������������������������������������������������������������������·······÷÷÷÷÷÷÷÷÷÷÷÷ķķķ�\
�ķķŷŷŷŷŷƷɷɷɷɷɷɷɷɷɷɷɷɷɷɷɷɷɷɷʷʷʷʷʷηηηηηηηηηηηηηηηηηηϷϷϷϷϷϷϷзշշ�\
�շշշշڷڷڷڷڷڷڷڷ���������������������������������������������������������������������\
������������������������������������������������������������������������������������������������������������������������\
�������������������������������������������������������������������������������������������������������ŸŸŸŸŸŸŸŸ�\
�ŸŸŸŸŸƸƸƸƸƸ͸͸͸͸͸͸��������������������������������������������\
������������������������������������������������������������������������������������������������������\
�����������������������������̹̹̹̹̹̹̹̹̹̹̹̹̹̹̹̹̹̹̹ιιιιιιιιιιιιιйййڹڹڹڹڹڹڹڹڹڹ�\
�ڹڹڹڹڹڹڹڹݹݹݹݹݹݹݹݹݹݹݹݹݹݹݹݹݹݹݹݹݹݹݹݹݹ߹߹߹߹߹߹߹߹߹߹߹����������������\
��������������������������������������������������������������������������������\
������������������������������������������������������������������������������������������������������������������������\
�����������������������������������������������������������������������κκκκκκκκκκκκκκκκκκκκκκκκ�\
�κκκκκκκκκκκκκκκκκκϺкккккккккккккккккккҺҺҺҺҺغغغغغغ�����������\
�����������������������������������������������������������������\
�������������������������������������������������������������\
������������������������������������������������������������������\
������������������������������������������������������������������������������������������������������������������������\
������������������������������������������������������������������������������������������������������������������������\
���������������������������������ҼҼҼҼҼҼҼҼҼҼҼҼҼҼҼҼҼҼҼҼҼҼҼҼҼҼҼҼҼҼҼҼҼҼҼҼҼӼӼӼӼӼӼ�\
�ӼӼռռռռռռּۼۼۼۼۼۼۼۼ������������������������������������������������������������������������������\
������������������������������������������������������������������������������������������������������������������������\
���������������������������������������½½½½½½½½½½ýýýýýýýýýýýýýýýýýýýýýýýýýýýýĽĽ�\
�ĽĽĽĽĽĽĽĽĽĽĽĽŽŽŽŽŽŽŽŽŽŽŽŽŽŽŽŽŽŽŽŽŽŽŽŽǽǽǽǽɽɽɽɽɽɽɽɽɽɽʽʽʽ־��ƾƾƾƾ�\
�ƾƾƾƾƾƾƾƾƾƾƾƾƾǾǾǾǾǾǾǾǾǾǾǾǾǾǾȾȾȾȾȾȾȾȾȾȾ˾˾˾˾ϾϾϾϾϾϾϾϾооооӾӾӾӾӾӾ�\
�־־־־־־־־־־־׾׾׾׾׾׾׾޾޾޾޾߾߾߾߾߾߾߾߾߾߾߾���������������������������\
������������������������������������������������������������������������������������\
������������������������������������������������������������������������������������������������������������������������\
������������������������������������������������������������������������������������������������������������������������\
������������������������������������������������������������������������������������������������������������������������\
���������������������������¿¿¿¿¿¿ÿ˿˿˿˿˿˿˿˿˿ͿͿͿͿͿͿͿͿϿϿϿϿϿϿϿϿϿϿϿϿϿϿϿϿϿϿпտտտ�\
�տֿֿֿֿܿܿܿܿܿ��������������������������������������������������\
�������������������������������������������������������������\
���������������������������������������������������������������������������������������������������������\
������������������������������������������������������������������������������������������������������������������������\
������������������������������������������������������������������������������������������������������������������������\
������������������������������������������������������������������������������������������������������������������������\
������������������������������������������������������������������������������������������������������������������������\
������������������������������������������������������������������������������������������������������������������������\
������������������������������������������������������������������������������������������������������������������������\
������������������������������������������������������������������������������������������������������������������������\
������������������������������������������������������������������������������������������������������������������������\
������������������������������������������������������������������������������������������������������������������������\
������������������������������������������������������������������������������������������������������������������������\
������������������������������������������������������������������������������������������������������������������������\
������������������������������������������������������������������������������������������������������������������������\
������������������������������������������������������������������������������������������������������������������������\
��������������������������������¡¡¡����������������������������������������������������������������������������������\
����������������������ââââââââââââââââââââââääääääääääääååååóóóóôôôôôôô\
ôôôôôôôôõõõõõõõõõõõõõõõõõõõöööööööööö÷÷÷÷÷÷÷÷÷÷øøøøøøøøøøûûû\
ûûûûûüüüüüüüüüü������������������������������������������������������������������������������������������\
������������������������������������������������������������������������������������������������������������������������\
����������������������������������ġġġġġġġġġġġġġġġġġġġġġġġġĢĢĢģĥĥĥħħħħħħħħħĨĪĪ\
��ŸŸŸŸŸŸŸŸŸŸŸŸŸŸŹŹŹŹŹŹŹŹŹŹŹŹŹŹŹŹźźźźźźźźźźŻŻŽŽŽŽžžž��������������������\
��������������������������������������������������������������������������ƯƯƴ����������������������������������������\
������������������������������������������������������������������������������������������������������������������������\
������������������������������������������������ǥǥǥǥǥǥǥǥǥǥǥǥǥǥǰǰǳǳǳǳǳ������������������������������\
������������������������������������������������������������������������������������������������������������������������\
������������������������������������������������������������������������������������������������������������������������\
������������������������������������������������������������������������������������������������������������������������\
����������������������ȣȣȣȣȣȣȣȣȣȣȣȣȣȣȣȣȣȣȣȣȣȣȣȣȣȣȣȣȣȣȣȣȣȣȣȣȣȣȣȣȣȤȤȤȥȥȥȥȥ\
ȥȦȦȦȫȫȫȫȫȫȫȫȫȫȭȭȭȭȭȭȭȭȭȭȭȭȭȭȮȮȮȮȮȮȯȯȯȯȯȯȯȯȯȯȯȯȯȯȯȯȯȰȰȰȰȰȲȲȲȲ\
ȲȲȲȲȲȲȲȲȲȲȲȲȲȲȲȲȲȲȲȲȸȸȸȸȸȸȸȸȸȸȸȸȸȸȸȸȸȸȸȸȹȹȾȾȾȿȿȿȿȿȿȿȿȿȿȿȿȿ����\
������������������������������������������������������������������������������������������������������������������������\
��������������������������������������������������������";
	if (hj_ptr != NULL)
	{
		tindex = -1;
		if ((*hjsrc >= 0xCA) && (*(hjsrc+1) >= 0xA1))
			tindex = (*hjsrc - 0xCA) * 0x5E + (*(hjsrc+1) - 0xA1);
		if ((tindex >= 0) && (tindex < HJ_COUNTER))
			resultp = (hj_ptr + tindex)->hangul;
	}

	return (resultp);
}


UCHAR  *str2hg (UCHAR *src)
{
	UCHAR *cp, *maxcp, *hgptr;

	if (src == NULL)	return (NULL);

	maxcp = src + strlen(src) - 1;  
	cp	= src;

	while (cp < maxcp)
	if (*cp >= 0x80)
	{
		hgptr = conv2hg(cp);
		if (hgptr != NULL)	strncpy(cp, hgptr, 2);
		cp += 2;
	}
	else  cp++;

	cp = src;

	return (cp);
}

int isalphabet(char c)
{
	if((c >= 'a') && (c <= 'z')) return 1;
	if((c >= 'A') && (c <= 'Z')) return 1;
	return 0;
}
int ishangul(char c)
{
	if((c & 0x80) == 0x80) return 1;
	return 0;
}

int ext_CompareChar(char pattern, char input)
{
	if(input >= 'A' && input <= 'Z')
	{
		input += ('a' - 'A');
		return (pattern == input);
	}
	else
		return (pattern == input);
}


char *ext3_FindEndOfWord(char *target_string)
{
	char *tchar = target_string;

	for(; *tchar; tchar++)
	{
		if(*tchar == ' ' || *tchar == '!' || *tchar == '\"' || *tchar == '#' || *tchar == '$' ||
		*tchar == '%' || *tchar == '&' || *tchar == '\'' || *tchar == '`' || *tchar == '(' ||
		*tchar == ')' || *tchar == '+' || *tchar == '.' || *tchar == ',' || *tchar == '-' || 
		*tchar == '/' || *tchar == ':' || *tchar == ';' || *tchar == '<' || *tchar == '=' || 
		*tchar == '>' || *tchar == '?' || *tchar == '@' || *tchar == '[' || *tchar == ']' || 
		*tchar == '\\' || *tchar == '^' || *tchar == '_' || *tchar == '{' || *tchar == '}' || 
		*tchar == '|' || *tchar == '~' || *tchar == '\n' || *tchar == '\0')
		{
			if(tchar == target_string)
				return tchar;
			else
				return (tchar-1);
		}
	}

	if(tchar == target_string)
		return tchar;
	else
		return (tchar-1);
}

/* find start postition of word */
char *ext3_FindStartOfWord(char *target_string, char* boundary)
{
	char *tchar = target_string;

	for(; *tchar && tchar > boundary; tchar--)
	{
		if(*tchar == ' ' || *tchar == '!' || *tchar == '\"' || *tchar == '#' || *tchar == '$' ||
		*tchar == '%' || *tchar == '&' || *tchar == '\'' || *tchar == '`' || *tchar == '(' ||
		*tchar == ')' || *tchar == '+' || *tchar == '.' || *tchar == ',' || *tchar == '-' || 
		*tchar == '/' || *tchar == ':' || *tchar == ';' || *tchar == '<' || *tchar == '=' || 
		*tchar == '>' || *tchar == '?' || *tchar == '@' || *tchar == '[' || *tchar == ']' || 
		*tchar == '\\' || *tchar == '^' || *tchar == '_' || *tchar == '{' || *tchar == '}' || 
		*tchar == '|' || *tchar == '~' || *tchar == '\n' || *tchar == '\0')
			return (++tchar);
	}
	
	if (tchar == boundary) return tchar;
	else if (!(*tchar)) return (++tchar);

	return tchar;
}

/* Function : int findMatchPointFromRight(char* target_start_ptr, char* target_end_ptr,
 *                char* pattern_start_ptr, char* pattern_end_ptr,
 *                char** match_start_ptr, char** match_end_ptr)
 *
 * Description : 
 *
 * Return :	1 if there is a match point
 *			0 if there is no match point
 */

int findMatchPointFromRight(
	char* target_start_ptr, 
	char* target_end_ptr, 
	char* pattern_start_ptr, 
	char* pattern_end_ptr, 
	char** match_start_ptr, 
	char** match_end_ptr 
)
{
	char* target_check_ptr; 
	char* cur_match_end_ptr; 

	char* pattern_check_ptr; 
	int pattern_length; 

	int isMatched; 


	char target_string[100];
	char pattern_string[100];
	char match_string[100];

	if(IJKIM_DEBUG) fprintf(stderr, "findMatchPointFromRight\n");

	isMatched = FALSE;

	if (target_start_ptr > target_end_ptr) 
		return FALSE;

	if (pattern_start_ptr > pattern_end_ptr) 
		return FALSE;

	if(IJKIM_DEBUG) {
		strncpy(target_string,target_start_ptr, target_end_ptr - target_start_ptr + 1);
		strncpy(pattern_string,pattern_start_ptr, pattern_end_ptr - pattern_start_ptr + 1);
		target_string[target_end_ptr - target_start_ptr + 1] = '\0';
		pattern_string[ pattern_end_ptr - pattern_start_ptr + 1] = '\0';

		fprintf(stderr, "target_string = [%s], pattern_string = [%s]\n", target_string, pattern_string);
	}

	pattern_length = pattern_end_ptr - pattern_start_ptr + 1;


	cur_match_end_ptr = target_end_ptr;


	while(cur_match_end_ptr - target_start_ptr + 1 >= pattern_length)
	{
		target_check_ptr = cur_match_end_ptr;
		pattern_check_ptr = pattern_end_ptr;

		isMatched = TRUE;
		while(pattern_check_ptr >= pattern_start_ptr)
		{
			if(ext_CompareChar(*pattern_check_ptr, *target_check_ptr))
			{
				pattern_check_ptr--;
				target_check_ptr--;
			}
			else
			{
				isMatched = FALSE;
				break;
			}

		}

		if(isMatched)
		{
			*match_start_ptr = target_check_ptr + 1;
			*match_end_ptr = cur_match_end_ptr;
			break;
		}

		cur_match_end_ptr--;
	}

	if(IJKIM_DEBUG) 
	{
		if (isMatched)
		{
			strncpy(match_string,*match_start_ptr, *match_end_ptr - *match_start_ptr + 1);
			match_string[ *match_end_ptr - *match_start_ptr + 1] = '\0';
			
			fprintf(stderr, "  - mathed: match_string = [%s]\n", match_string);
		}
		else
		{
			fprintf(stderr, "  - not mathed\n");
		}
	}


	return isMatched;
}


/* Function : int findMatchPointFromLeft(char* target_start_ptr, char* target_end_ptr,
 *                char* pattern_start_ptr, char* pattern_end_ptr,
 *                char** match_start_ptr, char** match_end_ptr)
 *
 * Description : 
 *
 * Return :	1 if there is a match point
 *			0 if there is no match point
 */

int findMatchPointFromLeft(
	char* target_start_ptr, 
	char* target_end_ptr, 
	char* pattern_start_ptr, 
	char* pattern_end_ptr, 
	char** match_start_ptr,
	char** match_end_ptr 
)
{
	char* target_check_ptr; 
	char* cur_match_start_ptr; 

	char* pattern_check_ptr; 
	int pattern_length; 

	int isMatched;

	char target_string[100];
	char pattern_string[100];
	char match_string[100];

	if(IJKIM_DEBUG) fprintf(stderr, "findMatchPointFromLeft\n");

	if(IJKIM_DEBUG) {
		strncpy(target_string,target_start_ptr, target_end_ptr - target_start_ptr + 1);
		strncpy(pattern_string,pattern_start_ptr, pattern_end_ptr - pattern_start_ptr + 1);
		target_string[target_end_ptr - target_start_ptr + 1] = '\0';
		pattern_string[ pattern_end_ptr - pattern_start_ptr + 1] = '\0';
		fprintf(stderr, "target_string = [%s], pattern_string = [%s]\n", target_string, pattern_string);
	}
	
	isMatched = FALSE;

	if (target_start_ptr > target_end_ptr) 
		return FALSE;

	if (pattern_start_ptr > pattern_end_ptr) 
		return FALSE;


	pattern_length = pattern_end_ptr - pattern_start_ptr + 1;

	cur_match_start_ptr = target_start_ptr;


	while(target_end_ptr - cur_match_start_ptr + 1 >= pattern_length)
	{
		target_check_ptr = cur_match_start_ptr;
		pattern_check_ptr = pattern_start_ptr;

		isMatched = TRUE;
		while(pattern_check_ptr <= pattern_end_ptr)
		{
			if(ext_CompareChar(*pattern_check_ptr, *target_check_ptr))
			{
				pattern_check_ptr++;
				target_check_ptr++;
			}
			else
			{
				isMatched = FALSE;
				break;
			}

		}

		if(isMatched)
		{
			*match_start_ptr = cur_match_start_ptr;
			*match_end_ptr = target_check_ptr - 1;
			break;
		}

		cur_match_start_ptr++;
	}

	if(IJKIM_DEBUG) 
	{
		if (isMatched)
		{
			strncpy(match_string,*match_start_ptr, *match_end_ptr - *match_start_ptr + 1);
			match_string[ *match_end_ptr - *match_start_ptr + 1] = '\0';
			
			fprintf(stderr, "  - mathed: match_string = [%s]\n", match_string);
		}
		else
		{
			fprintf(stderr, "  - not mathed\n");
		}
	}

	return isMatched;
}

void compact_multiple_asterisk(char* in, char* out)
{
	int i, j;
	int input_string_length;
	int asteriskFound;

	input_string_length = strlen(in);
	asteriskFound = 0;

	j=0;
	for(i = 0; i < input_string_length; i++)
	{
		if(asteriskFound)
		{
			if(in[i] == '*')
			{
				;
			}
			else
			{
				out[j] = in[i];
				j++;
				asteriskFound = 0;
			}
		}
		else
		{
			out[j] = in[i];
			j++;

			if(in[i] == '*') asteriskFound = 1;
		}
	}
	out[j] = '\0';
}

/* Function : int ext_FindHighlightMatchPoint(char *target_string, 
 *                  char* core_pattern, char* orig_pattern, 
 *                  char** match_start_ptr, char** match_end_ptr)
 *
 * Description :
 *
 * Return :	TRUE if target_string is matched with pattern
 *			FALSE if target_string is not matched with pattern
 */

int ext_FindHighlightMatchPoint(
	char* target_string,
	char* core_pattern,		
	char* orig_pattern,	
	char** match_start_ptr,	
	char** match_end_ptr
)
{
	char* word_end_ptr;
	char* target_str_check_ptr; 

	/* variables to matching with core pattern */
	int core_pattern_length; 
	char core_pattern_string[MAX_LENGTH];  
	int num_keywords_core_pattern; 
	int is_first_keyword_match; 

	/* variables for matching with original pattern */
	char orig_pattern_string[MAX_LENGTH];  
	int orig_pattern_string_length; 
	int orig_pattern_string_check_start_idx; 
	int orig_pattern_string_check_end_idx; 

	int found_same_substring_core_pattern_match;  

	int found_left_truncation; 
	int found_middle_truncation; 
	int found_right_truncation; 

	int orig_pattern_string_matched; 

	char* tmp_match_start_ptr;
	char* tmp_match_end_ptr; 

	char* keyword_start_ptr; 
	char* keyword_end_ptr; 
	int current_middle_asterisk_idx; 
	int next_middle_asterisk_idx; 

	/* variables for processing english word */
	char target_orig_string[MAX_LENGTH]; 	
	char stem_target_orig_string[MAX_LENGTH];

	/* temporoary variables */
	char* substring_start_ptr; 
	char* substring_end_ptr; 
	char* matched_substring_start_ptr; 
	char* matched_substring_end_ptr; 
	int	i, j; 


	/* input parameter checking */
	if(target_string == NULL || core_pattern == NULL || orig_pattern == NULL)
		return FALSE;


	word_end_ptr = ext3_FindEndOfWord(target_string);

	*match_start_ptr = target_string;
	*match_end_ptr = target_string;

	if (IJKIM_DEBUG) fprintf(stderr, "==== ext_FindHighlightMatchPoint - start ==== \n");
	if (IJKIM_DEBUG) fprintf(stderr, "(ijkim debug) target_string = [%s]\n", target_string);
	if (IJKIM_DEBUG) fprintf(stderr, "(ijkim debug) core_pattern= [%s]\n", core_pattern);
	if (IJKIM_DEBUG) fprintf(stderr, "(ijkim debug) orig_pattern= [%s]\n", orig_pattern);

	core_pattern_length = strlen(core_pattern);
	num_keywords_core_pattern = 0;
	for (i = 0; i < core_pattern_length; i++)
	{
		if(core_pattern[i] == ' ')
		{
			core_pattern_string[i] = '\0';
			num_keywords_core_pattern++;
		}
		else
		{
			core_pattern_string[i] = core_pattern[i];
		}
	}

	if (core_pattern_length > 0) num_keywords_core_pattern++;

	core_pattern_string[i] = '\0';

	substring_start_ptr = target_string;
	substring_end_ptr = word_end_ptr;

	keyword_start_ptr = &core_pattern_string[0];

	is_first_keyword_match = 1;
	for (i=0; i < num_keywords_core_pattern; i++)
	{
		keyword_end_ptr = keyword_start_ptr + strlen(keyword_start_ptr) - 1;

		if( findMatchPointFromLeft(substring_start_ptr, substring_end_ptr, \
					keyword_start_ptr, keyword_end_ptr, \
					&matched_substring_start_ptr, &matched_substring_end_ptr) )
		{
			if (is_first_keyword_match == 1)
			{
				*match_start_ptr = matched_substring_start_ptr;
			}

			substring_start_ptr = matched_substring_end_ptr + 1;
			substring_end_ptr = word_end_ptr;
		}
		else
		{
			return FALSE;
		}
		
		keyword_start_ptr =  keyword_start_ptr + strlen(keyword_start_ptr) + 1;
	}

	*match_end_ptr = matched_substring_end_ptr;

	/* Here, core pattern is matched */

	if (*match_start_ptr == target_string && *match_end_ptr == word_end_ptr)
		return TRUE;


	compact_multiple_asterisk(orig_pattern, orig_pattern_string);

	if(strcmp(core_pattern, orig_pattern_string) != 0)
	{
		if (IJKIM_DEBUG) fprintf(stderr, "core_pattern = [%s], orig_pattern_string = [%s]\n", core_pattern,orig_pattern_string);

		found_left_truncation = 0; 
		found_right_truncation = 0; 
		found_middle_truncation = 0;

		orig_pattern_string_matched = 1;

		orig_pattern_string_length = strlen(orig_pattern_string);

		orig_pattern_string_check_start_idx = 0;
		orig_pattern_string_check_end_idx = orig_pattern_string_length-1;


		if (orig_pattern_string[orig_pattern_string_check_start_idx] == '*')
		{
			found_left_truncation = 1;
			orig_pattern_string_check_start_idx++;
		}

		if (orig_pattern_string[orig_pattern_string_check_end_idx] == '*')
		{
			found_right_truncation = 1;
			orig_pattern_string_check_end_idx--;
		}

		for (i = orig_pattern_string_check_start_idx; i <= orig_pattern_string_check_end_idx; i++)
		{
			if (orig_pattern_string[i]=='*')
			{
				current_middle_asterisk_idx = i;
				found_middle_truncation = 1;
				break;
			}
		}

		if (IJKIM_DEBUG) {
			if(found_left_truncation)
				fprintf(stderr, "left_truncation is found\n");

			if(found_right_truncation)
				fprintf(stderr, "right_truncation is found\n");

			if(found_middle_truncation)
				fprintf(stderr, "middle_truncation is found\n");
		}


		if (!found_middle_truncation)
		{
			target_str_check_ptr = *match_start_ptr;

			tmp_match_start_ptr = *match_start_ptr;
			tmp_match_end_ptr = *match_start_ptr - 1;

			for (i = orig_pattern_string_check_start_idx; i <= orig_pattern_string_check_end_idx; i++)
			{
				if(ext_CompareChar(orig_pattern_string[i], *target_str_check_ptr)) 
				{
					target_str_check_ptr++;
					tmp_match_end_ptr++;
				}
				else 
				{
					orig_pattern_string_matched = 0;
					break;
				}
			}

			if (orig_pattern_string_matched)
			{
				if(found_left_truncation)
					tmp_match_start_ptr = target_string;

				if(found_right_truncation)
					tmp_match_end_ptr = word_end_ptr; 
			}
		}
		else 
		{
			keyword_start_ptr = &orig_pattern_string[orig_pattern_string_check_start_idx];
			keyword_end_ptr = &orig_pattern_string[current_middle_asterisk_idx-1];

			substring_start_ptr = target_string;
			substring_end_ptr = word_end_ptr;

			if( findMatchPointFromLeft(substring_start_ptr, substring_end_ptr, \
					keyword_start_ptr, keyword_end_ptr, \
					&matched_substring_start_ptr, &matched_substring_end_ptr) )
			{
				tmp_match_start_ptr = matched_substring_start_ptr;


				while(1)
				{
					/* find next middle asterisk */
					next_middle_asterisk_idx = -1;
					for (i=current_middle_asterisk_idx + 1; i <= orig_pattern_string_check_end_idx; i++)
					{
						if(orig_pattern_string[i] == '*')
						{
							next_middle_asterisk_idx = i;
							break;
						}
					}

					if (next_middle_asterisk_idx == -1)
						break;

					keyword_start_ptr = &orig_pattern_string[current_middle_asterisk_idx + 1];
					keyword_end_ptr = &orig_pattern_string[next_middle_asterisk_idx - 1];

					substring_start_ptr = matched_substring_end_ptr + 1;
					substring_end_ptr = word_end_ptr;

					if( findMatchPointFromLeft(substring_start_ptr, substring_end_ptr, \
							keyword_start_ptr, keyword_end_ptr, \
							&matched_substring_start_ptr, &matched_substring_end_ptr) )
					{
					}
					else
					{
						orig_pattern_string_matched = 0;
						break;
					}

					current_middle_asterisk_idx = next_middle_asterisk_idx;
				}

				if (orig_pattern_string_matched) 
				{
					keyword_start_ptr = &orig_pattern_string[current_middle_asterisk_idx+1];
					keyword_end_ptr = &orig_pattern_string[orig_pattern_string_check_end_idx];

					substring_start_ptr = matched_substring_end_ptr + 1;
					substring_end_ptr = word_end_ptr;

					if( findMatchPointFromRight(substring_start_ptr, substring_end_ptr, \
							keyword_start_ptr, keyword_end_ptr, \
							&matched_substring_start_ptr, &matched_substring_end_ptr) )
					{
						tmp_match_end_ptr = matched_substring_end_ptr;
					}
					else 
					{
						orig_pattern_string_matched = 0;
					}
				}
			} 
			else 
			{
				orig_pattern_string_matched = 0;
			} 


			if (orig_pattern_string_matched)
			{
				if(found_left_truncation)
					tmp_match_start_ptr = target_string;

				if(found_right_truncation)
					tmp_match_end_ptr = word_end_ptr; 
			}
		}

		if (orig_pattern_string_matched) 
		{
			*match_start_ptr = tmp_match_start_ptr;
			*match_end_ptr = tmp_match_end_ptr;
		}
	} 
	else { 
		orig_pattern_string_matched = 1;
	} 

	if (IJKIM_DEBUG) fprintf(stderr, "1\n");
	if (!orig_pattern_string_matched)
	{
		if (IJKIM_DEBUG) fprintf(stderr, "12\n");
		if (!found_left_truncation && !found_right_truncation && !found_middle_truncation)
		{
			j = 0;
			for(i = 0 ; i <= (word_end_ptr - target_string); i++)
			{
				if(target_string[i] >= 'A' && target_string[i] <= 'Z')
					target_orig_string[j++] = target_string[i] + ('a' - 'A');
				else
					target_orig_string[j++] = target_string[i];
			}
			target_orig_string[j] = '\0';

			if (IJKIM_DEBUG) fprintf(stderr, "123\n");

			if(IsWordEnglish(target_orig_string))
			{
				if (IJKIM_DEBUG) fprintf(stderr, "234\n");
				strcpy(stem_target_orig_string, target_orig_string);
				Stem(stem_target_orig_string);

				if (IJKIM_DEBUG) fprintf(stderr, "check with stem word\n");
				if (IJKIM_DEBUG) fprintf(stderr, "  - target_orig_string = [%s], stem_target_orig_string = [%s], core_pattern[%s]\n", target_orig_string, stem_target_orig_string, core_pattern);

				if(strcmp(stem_target_orig_string, core_pattern)==0)
				{
					if (IJKIM_DEBUG) fprintf(stderr, "  - matched!\n");

					*match_start_ptr = target_string;
					*match_end_ptr = word_end_ptr;
				}
			} 
		}
	} 

	if (IJKIM_DEBUG) fprintf(stderr, "===== ext_FindHighlightMatchPoint - end ===== \n");
	if (IJKIM_DEBUG) fprintf(stderr, "\n");

	return TRUE;
}

char* swig_web_highlight_ngram(char* result_val, char* pat_val) 
{
	static char returnVal[MAX_LENGTH]; 
	char* start_ptr;
	char* current_ptr;
	char* word_end_ptr;

	char core_pattern[MAX_LENGTH]; /* patterns without josa or postfix and asterisk */
	char orig_pattern[MAX_LENGTH]; /* patterns with josa or postfix and asterisk */

	char real_result[MAX_LENGTH]; /* buffer for storing original input result */

	char* tmp_core_pattern;
	char* tmp_orig_pattern;

	int num_of_core_patterns = 0;
	int num_of_orig_patterns = 0;
	int num_of_char;
	int i, j;
	int err;

	char* match_start_ptr;
	char* match_end_ptr;

	char* max_match_start_ptr;
	char* max_match_end_ptr;

	int bMatch = FALSE;
	int mpos = 0;

	char* result;
	char* pat;
	char* resultStr;

	if (F_DEBUG) fprintf(stderr, "highlight------------1\n");
	if (IJKIM_DEBUG) fprintf(stderr, "(ijkim debug) input = [%s]\n", result_val);
	if (IJKIM_DEBUG) fprintf(stderr, "(ijkim debug) pattern = [%s]\n", pat_val);

	result = result_val;
	pat = pat_val;
	resultStr = returnVal;

	strcpy(resultStr, "");

	if(result == NULL)
	{
		returnVal[0] = 0;
		return;
	}

	strcpy(real_result, result);
	str2hg(result);

	if(pat == NULL || *pat == '\0') 
	{
		strcpy(resultStr, real_result);
		return;
	}

	/* make highlight keywords(patterns) */
	err = MakeHighLightKeywords(pat, core_pattern, orig_pattern);
	if (err != 0)
	{
		strcpy(resultStr, real_result);
		return;
	}

	if (IJKIM_DEBUG) fprintf(stderr, "(ijkim debug) highlight keywords\n");
	if (IJKIM_DEBUG) fprintf(stderr, "(ijkim debug) core pattern = [%s]\n", core_pattern);
	if (IJKIM_DEBUG) fprintf(stderr, "(ijkim debug) orig pattern = [%s]\n", orig_pattern);

	/* change ' ' to '\0' in core pattern and calculate number of patterns */
	num_of_char = strlen(core_pattern);
	for (i = 0; i < num_of_char; i++)
	{
		if (core_pattern[i] == '|')
		{
			core_pattern[i] = '\0';
			num_of_core_patterns++;
		}
	}
	if (num_of_char > 0) num_of_core_patterns++;

	/* change ' ' to '\0' in original pattern */
	num_of_char = strlen(orig_pattern);
	for (i = 0; i < num_of_char; i++)
	{
		if (orig_pattern[i] == '|')
		{
			orig_pattern[i] = '\0';
			num_of_orig_patterns++;
		}
	}

	if (num_of_char > 0) num_of_orig_patterns++;


	if (IJKIM_DEBUG) fprintf(stderr, "(ijkim debug) # of core pattern = [%d]\n", num_of_core_patterns);
	if (IJKIM_DEBUG) fprintf(stderr, "(ijkim debug) # of orig pattern = [%d]\n", num_of_orig_patterns);


	if (F_DEBUG) fprintf(stderr, "highlight------------5\n");

	start_ptr = current_ptr = result;

	while( *current_ptr != '\0') 
	{

		tmp_core_pattern = &core_pattern[0];	
		tmp_orig_pattern = &orig_pattern[0];

		max_match_start_ptr = NULL;
		max_match_end_ptr = NULL;

		word_end_ptr = ext3_FindEndOfWord(current_ptr);

		bMatch = FALSE;

		for(i = 0; i < num_of_core_patterns; i++)
		{
			int comp;

			comp = ext_FindHighlightMatchPoint(current_ptr, tmp_core_pattern, tmp_orig_pattern,
					&match_start_ptr, &match_end_ptr);

			if(comp == 1) /* pattern is matched */
			{

				if (bMatch)
				{
					if (match_start_ptr < max_match_start_ptr)
					{
						max_match_start_ptr = match_start_ptr;
						max_match_end_ptr = match_end_ptr;
					}
					else if ( match_start_ptr ==  max_match_start_ptr)
					{
						if (max_match_end_ptr < match_end_ptr)
							max_match_end_ptr = match_end_ptr;
					}
				} 
				else
				{
					max_match_start_ptr = match_start_ptr;
					max_match_end_ptr = match_end_ptr;
				}

				bMatch = TRUE;

				if (max_match_start_ptr == current_ptr && max_match_end_ptr == word_end_ptr)
					break;
			}

			tmp_core_pattern = tmp_core_pattern + strlen(tmp_core_pattern) + 1;
			tmp_orig_pattern = tmp_orig_pattern + strlen(tmp_orig_pattern) + 1;
		}
		
		if (bMatch)
		{
			/* pattern is matched */
			if((max_match_start_ptr - start_ptr) > 0) {
				strncat(resultStr, &(real_result[start_ptr-result]), max_match_start_ptr - start_ptr);
			}

			strncat(resultStr, HIGHLIGHT_START, strlen(HIGHLIGHT_START));
			strncat(resultStr, &(real_result[max_match_start_ptr-result]), max_match_end_ptr - max_match_start_ptr + 1);
			strncat(resultStr, HIGHLIGHT_END, strlen(HIGHLIGHT_END));

			start_ptr =  max_match_end_ptr + 1;
			current_ptr = start_ptr;
		}
		else  
		{
			current_ptr = word_end_ptr+1;
		}

		if(*current_ptr == ' ') current_ptr++;
	}

	if((current_ptr - start_ptr) > 0)
		strncat(resultStr, &(real_result[start_ptr-result]), current_ptr - start_ptr);
	if (F_DEBUG) fprintf(stderr, "highlight------------6\n");


	if (IJKIM_DEBUG) fprintf(stderr, "(ijkim debug) output = [%s] == \n", resultStr);
	if (IJKIM_DEBUG) fprintf(stderr, "\n", resultStr);

	return returnVal;
}

char * swig_get_stopwords(char* input)
{
	static char returnVal[MAX_LENGTH];
	GetStopwords(input, returnVal);
	return returnVal;
}


char * swig_make_n_gram(char* input)
{
	static int wordCount;
	static char returnVal[MAX_LENGTH]; 

	wordCount = MakeNgramKeywords(input, returnVal);
	return returnVal;	
}


char* swig_cut_string_by_query (const char *src, const char *pat_val, int max_len)
{
	static char returnVal[MAX_LENGTH]; 
	char* start_ptr;
	char* current_ptr;
	char* word_end_ptr;

	char core_pattern[MAX_LENGTH]; /* patterns without josa or postfix and asterisk */
	char orig_pattern[MAX_LENGTH]; /* patterns with josa or postfix and asterisk */

	char real_result[MAX_LENGTH]; /* buffer for storing original input result */

	char* tmp_core_pattern;
	char* tmp_orig_pattern;

	int num_of_core_patterns = 0;
	int num_of_orig_patterns = 0;
	int num_of_char;
	int i, j;
	int err;

	char* match_start_ptr;
	char* match_end_ptr;

	char* max_match_start_ptr;
	char* max_match_end_ptr;

	char* cut_start_ptr;
	char* cut_end_ptr;

	int bMatch = FALSE;
	int mpos = 0;

	char* result;
	char* pat;
	char* resultStr;

	result = src;
	pat = pat_val;
	resultStr = returnVal;

	strcpy(resultStr, "");

	if(src == NULL)
	{
		returnVal[0] = 0;
		return returnVal;
	}

	if (max_len == -1 || strlen(result) <= max_len)
	{
		strcpy(returnVal, result);
		return returnVal;
	}

	strcpy(real_result, result);
	str2hg(result);

	if(pat == NULL || *pat == '\0') 
	{
		strcpy(resultStr, real_result);
		return returnVal;
	}

	/* make highlight keywords(patterns) */
	err = MakeHighLightKeywords(pat, core_pattern, orig_pattern);
	if (err != 0)
	{
		strcpy(resultStr, real_result);
		return returnVal;
	}

	/* change ' ' to '\0' in core pattern and calculate number of patterns */
	num_of_char = strlen(core_pattern);
	for (i = 0; i < num_of_char; i++)
	{
		if (core_pattern[i] == '|')
		{
			core_pattern[i] = '\0';
			num_of_core_patterns++;
		}
	}
	if (num_of_char > 0) num_of_core_patterns++;

	/* change ' ' to '\0' in original pattern */
	num_of_char = strlen(orig_pattern);
	for (i = 0; i < num_of_char; i++)
	{
		if (orig_pattern[i] == '|')
		{
			orig_pattern[i] = '\0';
			num_of_orig_patterns++;
		}
	}

	if (num_of_char > 0) num_of_orig_patterns++;

	start_ptr = current_ptr = result;

	while( *current_ptr != '\0') 
	{

		tmp_core_pattern = &core_pattern[0];	
		tmp_orig_pattern = &orig_pattern[0];

		max_match_start_ptr = NULL;
		max_match_end_ptr = NULL;

		word_end_ptr = ext3_FindEndOfWord(current_ptr);

		bMatch = FALSE;

		for(i = 0; i < num_of_core_patterns; i++)
		{
			int comp;

			comp = ext_FindHighlightMatchPoint(current_ptr, tmp_core_pattern, tmp_orig_pattern,
					&match_start_ptr, &match_end_ptr);

			if(comp == 1) /* pattern is matched */
			{

				if (bMatch)
				{
					if (match_start_ptr < max_match_start_ptr)
					{
						max_match_start_ptr = match_start_ptr;
						max_match_end_ptr = match_end_ptr;
					}
					else if ( match_start_ptr ==  max_match_start_ptr)
					{
						if (max_match_end_ptr < match_end_ptr)
							max_match_end_ptr = match_end_ptr;
					}
				} 
				else 
				{
					max_match_start_ptr = match_start_ptr;
					max_match_end_ptr = match_end_ptr;
				}

				bMatch = TRUE;

				if (max_match_start_ptr == current_ptr && max_match_end_ptr == word_end_ptr)
					break;
			}

			tmp_core_pattern = tmp_core_pattern + strlen(tmp_core_pattern) + 1;
			tmp_orig_pattern = tmp_orig_pattern + strlen(tmp_orig_pattern) + 1;
		}
		
		if (bMatch)
		{
			/* pattern is matched */
			/* find cut start ptr */
			/* if letters between start of target string and end of mached keyword are less than half max_len */
			if((max_match_end_ptr - start_ptr) < max_len/2) {
				cut_start_ptr = start_ptr;
			}
			else {
				cut_start_ptr = ext3_FindStartOfWord(max_match_end_ptr - max_len/2, start_ptr);
			}

			/* find cut end ptr */
			/* if letters between start of target string and end of mached keyword are less than half max_len */
			if((start_ptr + strlen(start_ptr)) - max_match_end_ptr < max_len/2) {
				cut_end_ptr = start_ptr + strlen(start_ptr);
			}
			else {
				cut_end_ptr = ext3_FindEndOfWord(max_match_end_ptr + max_len/2);
			}

			break;
		}
		else  
		{
			current_ptr = word_end_ptr+1;
		}

		if(*current_ptr == ' ') current_ptr++;
	}

	if(bMatch)
	{
		strncpy(resultStr, &(real_result[cut_start_ptr - start_ptr]), cut_end_ptr - cut_start_ptr + 1);
		resultStr[cut_end_ptr - cut_start_ptr + 1] = '\0';
	}

	if (IJKIM_DEBUG) fprintf(stderr, "(ijkim debug) output = [%s] == \n", resultStr);
	if (IJKIM_DEBUG) fprintf(stderr, "\n", resultStr);

	return returnVal;
}


