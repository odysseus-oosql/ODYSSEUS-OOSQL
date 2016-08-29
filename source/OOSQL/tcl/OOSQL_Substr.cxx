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
* Description:	Member functions for the OOSQL_TCSubStr class, a class for
*				representing substrings of dynamically sized strings or
*				c style strings.
*
****************************************************************************/

#include <limits.h>
#include "OOSQL_String.hxx"
#include "OOSQL_Techlib.hxx"

//------------------------- Global initialisations ------------------------//

static char empty[] = "Empty OOSQL_TCSubStr";		// Empty substring string

//----------------------------- Member functions --------------------------//

OOSQL_TCSubStr::OOSQL_TCSubStr()
/****************************************************************************
*
* Function:		OOSQL_TCSubStr::OOSQL_TCSubStr
*
* Description:	Constructor given no parameters. All we do here point
*				the OOSQL_TCSubStr at the empty string above.
*
****************************************************************************/
{
	len = sizeof(empty) + 1;		// Length includes the null terminator!
	text = empty;
}

OOSQL_TCSubStr::OOSQL_TCSubStr(const OOSQL_TCString& str,UFour pos,UFour count)
/****************************************************************************
*
* Function:		OOSQL_TCSubStr::OOSQL_TCSubStr
* Parameters:	str		- OOSQL_TCString to copy from
*				pos		- Starting position in the string
*				count	- Number of characters to copy
*
* Description:	Constructs a OOSQL_TCSubStr from another string, starting at the
*				position 'pos' and including 'count' characters.
*
****************************************************************************/
{
	CHECK(str.valid());
	if (pos > str.length())
		pos = str.length();
	if (count > str.length() - pos)
		count = str.length() - pos;
	len = count+1;
	text = (char *)((const char *)str + pos);
}

OOSQL_TCSubStr::OOSQL_TCSubStr(const char *cstr,UFour pos,UFour count)
/****************************************************************************
*
* Function:		OOSQL_TCSubStr::OOSQL_TCSubStr
* Parameters:	cstr	- C style string to copy from
*				pos		- Starting position in the string
*				count	- Number of characters to copy
*
* Description:	Constructs a OOSQL_TCSubStr from a C string, starting at the
*				position 'pos' and including 'count' characters.
*
*				The count and position are assumed to be valid if count
*				is not set to UINT_MAX.
*
****************************************************************************/
{
	CHECK(cstr != NULL);
	if (count == UINT_MAX) {
		len = strlen(cstr) + 1;
		if (pos >= len)
			pos = len-1;
		if (count >= len - pos)
			count = len-1 - pos;
		}
	len = count+1;
	text = (char*)cstr + pos;
}

OOSQL_TCSubStr& OOSQL_TCSubStr::operator = (const OOSQL_TCString& str)
/****************************************************************************
*
* Function:		OOSQL_TCSubStr::operator =
* Parameters:	str	- OOSQL_TCString to assign from
* Returns:		Reference to the newly created string.
*
* Description:	Assignment operator given a string.
*
****************************************************************************/
{
	CHECK(str.valid());
	len = str.length()+1;
	text = (char*)((const char *)str);
	return *this;
}

OOSQL_TCSubStr& OOSQL_TCSubStr::operator = (const char *cstr)
/****************************************************************************
*
* Function:		OOSQL_TCSubStr::operator =
* Parameters:	cstr	- C style string to assign
* Returns:		Reference to the newly allocated string.
*
* Description:	Assignment operator given a C style string.
*
****************************************************************************/
{
	CHECK(cstr != NULL);
	len = strlen(cstr)+1;
	text = (char*)cstr;
	return *this;
}

OOSQL_TCSubStr::operator OOSQL_TCDynStr () const
/****************************************************************************
*
* Function:		OOSQL_TCSubStr::operator OOSQL_TCDynStr
* Returns:		A newly constructed dynamic string.
*
* Description:	This routine casts a OOSQL_TCSubStr to a dynamically allocated. We
*				do this by constructing a new dynamic string and returning
*				this to the user.
*
****************************************************************************/
{
	return OOSQL_TCDynStr(*this,0,len-1);
}

OOSQL_TCSubStr& OOSQL_TCSubStr::left(UFour count)
/****************************************************************************
*
* Function:		OOSQL_TCSubStr::left
* Parameters:	count	- Number of characters to keep
* Returns:		Reference to the new substring.
*
* Description:	Converts the substring to represent only the left count
*				characters.
*
****************************************************************************/
{
	len = MIN(count,length()) + 1;
	return *this;
}

OOSQL_TCSubStr& OOSQL_TCSubStr::right(UFour count)
/****************************************************************************
*
* Function:		OOSQL_TCSubStr::right
* Parameters:	count	- Number of characters to keep
* Returns:		Reference to the new substring.
*
* Description:	Converts the substring to represent only the right count
*				characters.
*
****************************************************************************/
{
	if (count < length()) {
		text += length() - count;
		len = count+1;
		}
	return *this;
}

OOSQL_TCSubStr& OOSQL_TCSubStr::mid(UFour pos,UFour count)
/****************************************************************************
*
* Function:		OOSQL_TCSubStr::mid
* Parameters:	pos		- Position to start at
*				count	- Number of characters to keep
* Returns:		Reference to the new substring.
*
* Description:	Converts the substring to represent the middle count
*				characters starting at pos.
*
****************************************************************************/
{
	if (pos > length())
		pos = length();
	if (count > length() - pos)
		count = length() - pos;
	len = count+1;
	text += pos;
	return *this;
}

std::ostream& operator << (std::ostream& o,const OOSQL_TCSubStr& s)
/****************************************************************************
*
* Function:		operator <<
* Parameters:	o	- Stream to dump string to
*				s	- OOSQL_TCSubStr to dump
* Returns:		Stream to dump data to
*
****************************************************************************/
{
	UFour	i;
	char	*p;

	for (i = 0,p = s.text; i < s.length(); i++,p++)
		o << *p;
	return o;
}
