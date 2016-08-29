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

/****************************************************************************
*
* Description:	Member functions for the string class, an abstract base
*				class for representing strings.
*
****************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include "OOSQL_String.hxx"
#include "OOSQL_Techlib.hxx"

//------------------------- Compatability Functions -----------------------//

#ifdef	NO_STRUPR

#include <ctype.h>

void strupr(char *s)
{
	for (int i = strlen(s); i; i--) {
		*s = toupper(*s);
		s++;
		}
}

void strlwr(char *s)
{
	for (int i = strlen(s); i; i--) {
		*s = tolower(*s);
		s++;
		}
}

#endif

//----------------------------- Member functions --------------------------//

UFour OOSQL_TCString::split(OOSQL_TCSubStr*& array,const char *fs) const
/****************************************************************************
*
* Function:		OOSQL_TCString::split
* Parameters:	array	- Pointer to array of substrings to fill
*				fs		- Set of field separator characters
* Returns:		Number of strings in array.
*
* Description:	Breaks the string up into a number of substrings separated
*				by characters in the field separator string. No memory
*				is actually allocated for the data in the strings, since
*				the substrings will index directly into the original
*				dynamic string (so don't kill the original string before
*				doing something with them).
*
*				If memory allocation fails during the operation, 'array'
*				will be set to NULL.
*
****************************************************************************/
{
	char			*p = text;
	OOSQL_TCSubStr	*temp;
	int				num,j;
	UFour			pos,count;

	// Determine the number of tokens to be parsed

	CHECK(valid());
	num = 0;
	while ((j = strcspn(p,fs)) != 0) {
		p += strspn(p += j,fs);
		num++;
		}

	// Allocate memory for the array

	if ((temp = new OOSQL_TCSubStr[num]) == NULL)
		goto Done;

	for (j = 0, pos = 0; j < num; j++) {
		count = strcspn(text + pos,fs);
		temp[j] = OOSQL_TCSubStr(*this,pos,count);
		pos += count;
		pos += strspn(text + pos,fs);
		}

Done:
	array = temp;
	return num;
}

#if		defined(__16BIT__)
#define	NBITS_IN_UNSIGNED		16
#else
#define	NBITS_IN_UNSIGNED		32
#endif
#define SEVENTY_FIVE_PERCENT	((int)(NBITS_IN_UNSIGNED * .75))
#define	TWELVE_PERCENT			((int)(NBITS_IN_UNSIGNED * .125))
#define	HIGH_BITS				( ~( (unsigned)(~0) >> TWELVE_PERCENT) )

UFour OOSQL_TCString::hash() const
/****************************************************************************
*
* Function:		OOSQL_TCString::hash
* Returns:		Hash value for the OOSQL_TCString.
*
* Description:	Calculates the hash value for a string. This is done using
*				a shift-and-XOR strategy. The main iteration of the loop
*				shifts the accumulated hash value to the left by a few bits
*				and adds in the current character. When the number gets too
*				large, it is randomized by XORing it with a shifted version
*				of itself. Note that we set up a few macros to determine the
*				number of bits to shift independant of the machine word
*				size.
*
****************************************************************************/
{
	UFour	h = 0,g;

	CHECK(valid());
	for (UFour i = 0; i < length(); i++) {
		h = (h << TWELVE_PERCENT) + text[i];
		if ( (g = h & HIGH_BITS) != 0)
			h = (h ^ (g >> SEVENTY_FIVE_PERCENT)) & ~HIGH_BITS;
		}

	return h;
}

/****************************************************************************
*
* The following conversion routines work correctly, given that the strings
* will always be null terminated. If the string is a OOSQL_TCSubStr, which
* indexes into a DynStr, then we may possibly be saving the null terminator
* and putting it back.
*
****************************************************************************/

int OOSQL_TCString::asInt() const
/****************************************************************************
*
* Function:		OOSQL_TCString::asInt
* Returns:		Representation of the string as an integer
*
* Description:	Converts the string to an integer. If the conversion fails,
*				the value will be set to zero.
*
****************************************************************************/
{
	CHECK(valid());
	char save = text[length()];
	int	result = atoi(text);
	text[length()] = save;
	return result;
}

unsigned int OOSQL_TCString::asUInt() const
/****************************************************************************
*
* Function:		OOSQL_TCString::asUInt
* Returns:		Representation of the string as an unsigned integer
*
* Description:	Converts the string to an unsigned integer. If the
*				conversion fails, the value will be set to zero.
*
****************************************************************************/
{
	CHECK(valid());
	char save = text[length()];
	UFour result;
	if (sscanf(text,"%u\n",&result) != 1)
		result = 0;
	text[length()] = save;
	return result;
}

long OOSQL_TCString::asLong() const
/****************************************************************************
*
* Function:		OOSQL_TCString::asLong
* Returns:		Representation of the string as a long integer
*
* Description:	Converts the string to an integer. If the conversion fails,
*				the value will be set to zero.
*
****************************************************************************/
{
	CHECK(valid());
	char save = text[length()];
	long result = atol(text);
	text[length()] = save;
	return result;
}

unsigned long OOSQL_TCString::asULong() const
/****************************************************************************
*
* Function:		OOSQL_TCString::asULong
* Returns:		Representation of the string as an unsigned long
*
* Description:	Converts the string to an unsigned long. If the
*				conversion fails, the value will be set to zero.
*
****************************************************************************/
{
	CHECK(valid());
	char save = text[length()];
	UFour result;
	if (sscanf(text,"%lu\n",&result) != 1)
		result = 0;
	text[length()] = save;
	return result;
}

float OOSQL_TCString::asFloat() const
/****************************************************************************
*
* Function:		OOSQL_TCString::asFloat
* Returns:		Representation of the string as a float
*
* Description:	Converts the string to a floating point number. If the
*				conversion fails, the value will be set to zero.
*
****************************************************************************/
{
	CHECK(valid());
	char save = text[length()];
	float result = (float)atof(text);
	text[length()] = save;
	return result;
}

double OOSQL_TCString::asDouble() const
/****************************************************************************
*
* Function:		OOSQL_TCString::asDouble
* Returns:		Representation of the string as a double
*
* Description:	Converts the string to a floating point number. If the
*				conversion fails, the value will be set to zero.
*
****************************************************************************/
{
	CHECK(valid());
	char save = text[length()];
	double result;
	if (sscanf(text,"%lf\n",&result) != 1)
		result = 0;
	text[length()] = save;
	return result;
}
