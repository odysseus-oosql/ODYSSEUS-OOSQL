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

static int WordSize();
static int ContainsVowel();
static int AddAnE();
static int RemoveAnE();
static int ReplaceEnd();

#define FALSE		0
#define TRUE		1
#define EOS			'\0'

#define IsVowel(c) ('a'==(c) || 'e'==(c) || 'i'==(c) || 'o'==(c) || 'u'==(c))

typedef struct {
	int		id;
	char	*old_end;
	char 	*new_end;
	int		old_offset;
	int		new_offset;
	int		min_root_size;
	int		(*condition)();
	} RuleList;

static char LAMBDA[1] = "";
static char *end;

static RuleList step1a_rules[] =
	{
		101,	"sses",		"ss",	3,	1,	-1,	NULL,
		102,	"ies",		"i",	2,	0,	-1,	NULL,
		103,	"ss",		"ss",	1,	1,	-1,	NULL,
		104,	"s",		LAMBDA,	0,	-1,	-1,	NULL,
		000,	NULL,		NULL,	0,	0,	0,	NULL
	};

static RuleList step1b_rules[] =
	{
		105,	"eed",		"ee",	2,	1,	0,	NULL,
		106,	"ed",		LAMBDA,	1,	-1,	-1,	ContainsVowel,
		107,	"ing",		LAMBDA,	2,	-1,	-1,	ContainsVowel,
		000,	NULL,		NULL,	0,	0,	0,	NULL
	};


static RuleList step1b1_rules[] =
	{
		108, 	"at",	"ate",	1,	2,	-1,	NULL,
		109,	"bl",	"ble",	1,	2,	-1,	NULL,
		110,	"iz",	"ize",	1,	2,	-1,	NULL,
		111,	"bb",	"b",	1,	0,	-1,	NULL,
		112,	"dd",	"d",	1,	0,	-1,	NULL,
		113,	"ff",	"f",	1,	0,	-1,	NULL,
		114,	"gg",	"g",	1,	0,	-1,	NULL,
		115,	"mm",	"m",	1,	0,	-1,	NULL,
		116,	"nn",	"n",	1,	0,	-1,	NULL,
		117,	"pp",	"p",	1,	0,	-1,	NULL,
		118,	"rr",	"r",	1,	0,	-1,	NULL,
		119,	"tt",	"t",	1,	0,	-1,	NULL,
		120,	"ww",	"w",	1,	0,	-1,	NULL,
		121,	"xx",	"x",	1,	0,	-1,	NULL,
		122,	LAMBDA, "e",   -1,      0,      -1,    AddAnE,
		000,    NULL,   NULL,   0,      0,       0,     NULL,
	      };

static RuleList step1c_rules[] =
              {
                123,    "y",    "i",    0,      0,      -1, ContainsVowel,
		000,	NULL,	NULL,	0,	0,	0,  NULL,
	      };

static RuleList step2_rules[] =
	      {
		203,	"ational", "ate",  6,   2,	0,	NULL,
		204,	"tional",	"tion", 5,	3,	0,	NULL,
		205,	"enci",	"ence", 3,	3,	0, 	NULL, 
		206, 	"anci",	"ance", 3,	3,	0,	NULL,
		207,	"izer",	"ize",  3,	2,	0,	NULL,
		208, 	"abli",	"able", 3,	3,	0, 	NULL,
		209,	"alli",	"al",	  3,	1, 	0,	NULL,
		210,	"entli",	"ent",	  4, 	2, 	0,	NULL,
		211,	"eli",		"e",	  2,	0,	0,	NULL,
		213, 	"ousli",	"ous",	  4, 	2,	0,	NULL,
		214,	"ization",	"ize",	  6,	2,	0,	NULL,
		215,	"ation",	"ate",	  4,	2,	0,	NULL,
		216,	"ator",	"ate",	  3,	2,	0,	NULL,
		217,	"alism",	"al",	  4, 	1,	0,	NULL,
		218,	"iveness",	"ive",	  6,	2,	0,	NULL,
		219,	"fulnes",	"ful",	  5, 	2,	0,	NULL,
		220,	"ousness",	"ous",	  6, 	2,	0,	NULL,
		221,	"aliti",	"al", 	  4,	1, 	0,	NULL,
		222,	"iviti",	"ive",	  4,	2,	0,	NULL,
		223,	"biliti",	"ble",	  5,	2,	0,	NULL,
		000,	NULL,		NULL,	  0, 	0,	0,	NULL,
           };

static RuleList step3_rules[] = 
	     {
		301,	"icate",	"ic",	  4,	1,	0,	NULL,
		302,	"ative",	LAMBDA,  4,	-1,	0,	NULL,
		303,	"alize",	"al",	  4,	1,	0,	NULL,
		304,	"iciti",	"ic",	  4,	1,	0,	NULL,
		305,	"ical",	"ic",	  3,	1,	0,	NULL,
		308,	"ful",		LAMBDA, 2,	-1,	0,	NULL,
		309,	"ness",	LAMBDA, 3,	-1,	0,	NULL,
		000,	NULL,		NULL,	  0,	0,	0,	NULL,
	     };

static RuleList step4_rules[] =
	     {	
		401,	"al",		LAMBDA,  1,	-1,	1,	NULL,
		402,	"ance",	LAMBDA,  3,	-1,	1,	NULL,
		403,	"ence",	LAMBDA,  3,	-1,	1,	NULL,
		405,	"er",		LAMBDA,  1,	-1,	1,	NULL,
		406,	"ic",		LAMBDA,  1,	-1,	1,	NULL,
		407,	"able",	LAMBDA,  3,	-1,	1,	NULL,
		408,	"ible",	LAMBDA,  3,	-1,	1,	NULL,
		409,	"ant",		LAMBDA,  2,	-1,	1,	NULL,
		410,	"ement",	LAMBDA,  4,	-1,	1,	NULL,
		411,	"ment",	LAMBDA,  3,	-1,	1,	NULL,
		412,	"ent",		LAMBDA,  2,	-1,	1,	NULL,
		423,	"sion",	"s",  	3,	0,	1,	NULL,
		424,	"tion",	"t",  3,	0,	1,	NULL,
		415,	"ou",		LAMBDA,  1,	-1,	1,	NULL,
		416,	"ism",		LAMBDA,  2,	-1,	1,	NULL,
		417,	"ate",		LAMBDA,  2,	-1,	1,	NULL,
		418,	"iti",		LAMBDA,  2,	-1,	1,	NULL,
		419,	"ous",		LAMBDA,  2,	-1,	1,	NULL,
		420,	"ive",		LAMBDA,  2,	-1,	1,	NULL,
		421,	"ize",		LAMBDA,  2,	-1,	1,	NULL,
		000,	NULL,		NULL,  0,	0,	0,	NULL,
	     };                                

static RuleList step5a_rules[] =
	     {	
		501,	"e",		LAMBDA,  0, 	-1,	1,	NULL,
		502,	"e",		LAMBDA,  0, 	-1,   -1,	RemoveAnE,
		000,	NULL,		NULL,  0,	0,	0,	NULL,    
	     };

static RuleList step5b_rules[] = 
	     {
		503,   "ll",		"l",	1, 	0,	1,	NULL,
		000,	NULL,		NULL,	0,	0,	0,	NULL,
	     };

static int
WordSize(word)
	register char *word;
	{
	register int result;
	register int state;

	result = 0;
	state = 0;

	while (EOS != *word)
	{
	switch(state)
		{
		case 0: state = (IsVowel(*word)) ? 1 : 2;
			  break;
		case 1: state = (IsVowel(*word)) ? 1 : 2;
			  if (2 == state) result++;
			  break;                   
		case 2: state = (IsVowel(*word) || ('y' == *word)) ? 1 : 2;
			  break;
		}
	word++;
	}
	
	return(result);
}

static int 
ContainsVowel(word)
	register char *word;
	{
	
	if (EOS == *word)
		return(FALSE);
	else
		return(IsVowel(*word) || (NULL != strpbrk(word+1, "aeiouy")));
	}

static int
EndsWithCVC(word)
	register char *word;
	{
	int length;

	if ((length = strlen(word)) < 2)
		return(FALSE);
	else
		{
		end = word + length - 1;
		return(  (NULL == strchr("aeiouwxy", *end--))
			&& (NULL != strchr("aeiouy", *end--))
			&& (NULL == strchr("aeiou", *end)));
		}
	}

static int
AddAnE(word)
	register char *word;
	{
	
	return((1 == WordSize(word)) && EndsWithCVC(word));
	}

static int
RemoveAnE(word)
	register char *word;
	{
	
	return((1 == WordSize(word)) && !EndsWithCVC(word));
	}

static int
ReplaceEnd(word, rule)
	register char *word;
	RuleList *rule;
	{
	register char *ending;
	char tmp_ch;

	while (0 != rule->id)
		{
		ending = end - rule->old_offset;
		if (word != ending)
			if (0 == strcmp(ending, rule->old_end))
				{
				tmp_ch = *ending;
				*ending = EOS;
				if (rule->min_root_size < WordSize(word))
					if (!rule->condition || (*rule->condition)(word))
						{
						(void)strcat(word, rule->new_end);
						end = ending + rule->new_offset;
						break;
						}
					*ending = tmp_ch;
				}
			rule++;
		}
	return(rule->id);
	}
	
int
Stem(word)
	register char *word;
	{
	int rule;

	for (end = word; *end != EOS; end++)
		if (!isalpha(*end)) return(FALSE);
	end--;

	ReplaceEnd(word, step1a_rules);
	rule = ReplaceEnd(word, step1b_rules);
	if ((106 == rule) || (107 == rule))
		ReplaceEnd(word, step1b1_rules);
	ReplaceEnd(word, step1c_rules);	      

	ReplaceEnd(word, step2_rules);	      

	ReplaceEnd(word, step3_rules);	      

	ReplaceEnd(word, step4_rules);	      

	ReplaceEnd(word, step5a_rules);	      

	ReplaceEnd(word, step5b_rules);	      

	return(TRUE);
	}

