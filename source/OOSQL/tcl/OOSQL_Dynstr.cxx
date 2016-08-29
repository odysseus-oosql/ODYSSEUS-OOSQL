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
* Description:	Member functions for the OOSQL_TCDynStr class, a class for
*				representing dynamically sized strings.
*
****************************************************************************/

#include <limits.h>
#include "OOSQL_String.hxx"
#include "OOSQL_Techlib.hxx"

//------------------------- Global initialisations ------------------------//

UFour	OOSQL_TCDynStr::granularity = 8;			// Default granularity

//----------------------------- Member functions --------------------------//

void OOSQL_TCDynStr::shrink()
/****************************************************************************
*
* Function:		OOSQL_TCDynStr::shrink
*
* Description:	Shrinks the size of the allocated memory down to the
*				smallest possible amount.
*
****************************************************************************/
{
	char	*temp;

	CHECK(valid());
	if ((size - len) > granularity) {

		// Determine the smallest possible size (in increments of
		// granularity and allocate the new memory

		size = computeSize(len);

		if(pMemoryManager)
			temp = (char*)pMemoryManager->Alloc(size);
		else
			temp = new char[size];

		// Copy the text and delete the old text

		if (temp)
			memcpy(temp,text,len);
		if(pMemoryManager)
			pMemoryManager->Free(text);
		else
			delete [] text;
		text = temp;
		}
}

void OOSQL_TCDynStr::resize(UFour newresize)
/****************************************************************************
*
* Function:		OOSQL_TCDynStr::resize
*
* Description:	Resizes the size of the allocated memory to the
*				determined amount of size.
*
****************************************************************************/
{
	UFour	newsize,newlen;
	char	*temp;

	CHECK(valid());
	PRECONDITION(newresize < UINT_MAX);

	newlen = newresize + 1;
	newsize = computeSize(newlen);

	if (newsize != size) {
		size = newsize;
		if(pMemoryManager)
			temp = (char*)pMemoryManager->Alloc(size);
		else
			temp = new char[size];
		if (temp != NULL) {
			temp[newlen-1] = '\0';				// Null terminate the string
			}
		if(pMemoryManager)
			pMemoryManager->Free(text);
		else
			delete [] text;
		text = temp;
	}
	len = newlen;
}

OOSQL_TCDynStr::OOSQL_TCDynStr(OOSQL_MemoryManager* memoryManager)
/****************************************************************************
*
* Function:		OOSQL_TCDynStr::OOSQL_TCDynStr
*
* Description:	Constructor given no parameters. All we do here is allocate
*				the smallest amount of space possible, which is equal to
*				the current setting of the granularity, and set the value
*				of the string to empty.
*
****************************************************************************/
{
	len = 1;					// Length includes the null terminator!
	size = granularity;

	pMemoryManager = memoryManager;
	if(pMemoryManager)
		text = (char*)pMemoryManager->Alloc(size);
	else
		text = new char[size];

	if (text != NULL)
		text[0] = '\0';			// Null terminate the string
}

OOSQL_TCDynStr::OOSQL_TCDynStr(const OOSQL_TCDynStr& str)
/****************************************************************************
*
* Function:		OOSQL_TCDynStr::OOSQL_TCDynStr
* Parameters:	str		- OOSQL_TCString to copy from
*
* Description:	Standard copy constructor. This is required for some
*				routines to work (the compiler gets confused about the next
*				constructor).
*
****************************************************************************/
{
	CHECK(str.valid());
	len            = str.len;
	size           = str.size;
	pMemoryManager = str.pMemoryManager;
	if(pMemoryManager)
		text = (char*)pMemoryManager->Alloc(size);
	else
		text = new char[size];
	if (text != NULL)
		memcpy(text,str.text,len);
}

OOSQL_TCDynStr::OOSQL_TCDynStr(const OOSQL_TCString& str,OOSQL_MemoryManager* memoryManager,UFour pos,UFour count)
/****************************************************************************
*
* Function:		OOSQL_TCDynStr::OOSQL_TCDynStr
* Parameters:	str		- OOSQL_TCString to copy from
*				pos		- Starting position in the string
*				count	- Number of characters to copy
*
* Description:	Constructs a string from another string, starting at the
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
	size = computeSize(len);
	pMemoryManager = memoryManager;
	if(pMemoryManager)
		text = (char*)pMemoryManager->Alloc(size);
	else
		text = new char[size];
	if (text != NULL) {
		memcpy(text,(const char *)str+pos,len);
		text[count] = '\0';				// Null terminate the string
		}
}

OOSQL_TCDynStr::OOSQL_TCDynStr(const char *cstr,OOSQL_MemoryManager* memoryManager,UFour pos,UFour count)
/****************************************************************************
*
* Function:		OOSQL_TCDynStr::OOSQL_TCDynStr
* Parameters:	cstr	- C style string to copy from
*				pos		- Starting position in the string
*				count	- Number of characters to copy
*
* Description:	Constructs a string from a C string, starting at the
*				position 'pos' and including 'count' characters.
*
****************************************************************************/
{
	CHECK(cstr != NULL);
	len = strlen(cstr) + 1;
	if (pos >= len)
		pos = len-1;
	if (count >= len - pos)
		count = len-1 - pos;
	len = count+1;
	size = computeSize(len);
	pMemoryManager = memoryManager;
	if(pMemoryManager)
		text = (char*)pMemoryManager->Alloc(size);
	else
		text = new char[size];
	if (text != NULL) {
		memcpy(text,cstr+pos,count);
		text[count] = '\0';				// Null terminate the string
		}
}

OOSQL_TCDynStr::OOSQL_TCDynStr(char ch,UFour count,OOSQL_MemoryManager* memoryManager)
/****************************************************************************
*
* Function:		OOSQL_TCDynStr::OOSQL_TCDynStr
* Parameters:	ch		- Character to fill with
*				count	- Number of characters to fill
*
* Description:	Constructs the string with 'count' replications of 'ch'.
*
****************************************************************************/
{
	len = count + 1;
	size = computeSize(len);
	pMemoryManager = memoryManager;
	if(pMemoryManager)
		text = (char*)pMemoryManager->Alloc(size);
	else
		text = new char[size];
	if (text != NULL) {
		memset(text,ch,count);
		text[len-1] = '\0';
		}
}

OOSQL_TCDynStr::~OOSQL_TCDynStr()
/****************************************************************************
*
* Function:		OOSQL_TCDynStr::~OOSQL_TCDynStr
*
* Description:	Destructor for the OOSQL_TCString class. Simply delete the memory
*				allocated to the string.
*
****************************************************************************/
{
	if(pMemoryManager)
		pMemoryManager->Free(text);
	else
		delete [] text;
}

OOSQL_TCDynStr& OOSQL_TCDynStr::operator = (const OOSQL_TCDynStr& str)
/****************************************************************************
*
* Function:		OOSQL_TCDynStr::operator =
* Parameters:	str	- OOSQL_TCString to assign from
* Returns:		Reference to the newly created string.
*
* Description:	Assignment operator given a string. We delete the old text
*				and then create a new string.
*
****************************************************************************/
{
	// If the amount of memory required for the new string is the same as
	// for the old string, we do not need to delete the memory allocated
	// to the text!

	CHECK(valid() && str.valid());
	UFour newsize = computeSize(str.length()+1);
	char *temp = text;
	if (size != newsize)
	{
		if(pMemoryManager)
			temp = (char*)pMemoryManager->Alloc(newsize);
		else
			temp = new char[newsize];
	}

	if (temp) {
		len = str.length()+1;
		memcpy(temp,str,len);
		if (size != newsize) {
			size = newsize;
			if(pMemoryManager)
				pMemoryManager->Free(text);
			else
				delete [] text;
			text = temp;
			}
		text[len-1] = '\0';					// Null Terminate string
		}

	return *this;
}

OOSQL_TCDynStr& OOSQL_TCDynStr::operator = (const OOSQL_TCString& str)
/****************************************************************************
*
* Function:		OOSQL_TCDynStr::operator =
* Parameters:	str	- OOSQL_TCString to assign from
* Returns:		Reference to the newly created string.
*
* Description:	Assignment operator given a string. We delete the old text
*				and then create a new string.
*
****************************************************************************/
{
	// If the amount of memory required for the new string is the same as
	// for the old string, we do not need to delete the memory allocated
	// to the text!

	CHECK(valid() && str.valid());
	UFour newsize = computeSize(str.length()+1);
	char *temp = text;
	if (size != newsize)
	{
		if(pMemoryManager)
			temp = (char*)pMemoryManager->Alloc(newsize);
		else
			temp = new char[newsize];
	}

	if (temp) {
		len = str.length()+1;
		memcpy(temp,str,len);
		if (size != newsize) {
			size = newsize;
			if(pMemoryManager)
				pMemoryManager->Free(text);
			else
				delete [] text;
			text = temp;
			}
		text[len-1] = '\0';					// Null Terminate string
		}

	return *this;
}

OOSQL_TCDynStr& OOSQL_TCDynStr::operator = (const char *cstr)
/****************************************************************************
*
* Function:		OOSQL_TCDynStr::operator =
* Parameters:	cstr	- C style string to assign
* Returns:		Reference to the newly allocated string.
*
* Description:	Assignment operator given a C style string. We delete the
*				old text and copy the C string into a new string.
*
****************************************************************************/
{
	CHECK(valid() && cstr != NULL);
	return *this = OOSQL_TCSubStr(cstr);
}

OOSQL_TCDynStr& OOSQL_TCDynStr::operator += (const OOSQL_TCString& str)
/****************************************************************************
*
* Function:		OOSQL_TCDynStr::operator +=
* Parameters:	str	- OOSQL_TCString to concatenate
* Returns:		Reference to the concatenated string.
*
* Description:	Concatenation operator. Here we copy the second string onto
*				the end of the first string, so no new string is created.
*				We may need to reallocate the memory though.
*
****************************************************************************/
{
	UFour	newsize,newlen;
	char	*temp;

	CHECK(valid() && str.valid());
	PRECONDITION((UFour)(len + str.length()) < UINT_MAX);

	newlen = length() + str.length() + 1;
	newsize = computeSize(newlen);

	// If the amount of memory already allocated for the string is enough
	// we do not need to re-allocate the memory!

	if (newsize != size) {
		size = newsize;
		if(pMemoryManager)
			temp = (char*)pMemoryManager->Alloc(size);
		else
			temp = new char[size];
		if (temp != NULL) {
			memcpy(temp,text,len-1);
			memcpy(&temp[len-1],str,str.length()+1);
			}
		if(pMemoryManager)
			pMemoryManager->Free(text);
		else
			delete [] text;
		text = temp;
		}
	else
		memcpy(&text[len-1],str,str.length()+1);

	if (valid()) {
		len = newlen;
		text[len-1] = '\0';				// Null terminate the string
		}
	return *this;
}

OOSQL_TCDynStr& OOSQL_TCDynStr::operator += (const char* cstr)
/****************************************************************************
*
* Function:		OOSQL_TCDynStr::operator +=
* Parameters:	cstr	- OOSQL_TCString to concatenate
* Returns:		Reference to the concatenated string.
*
* Description:	Concatenation operator. Here we copy the second string onto
*				the end of the first string, so no new string is created.
*				We may need to reallocate the memory though.
*
****************************************************************************/
{
	CHECK(valid() && cstr != NULL);
	return *this += OOSQL_TCSubStr(cstr);
}

OOSQL_TCDynStr& OOSQL_TCDynStr::operator += (const char ch)
/****************************************************************************
*
* Function:		OOSQL_TCDynStr::operator +=
* Parameters:	ch	- Character to concatenate.
* Returns:		Reference to the concatenated string.
*
****************************************************************************/
{
	char	temp[2];

	temp[0] = ch;
	temp[1] = '\0';
	CHECK(valid());
	return *this += OOSQL_TCSubStr(temp,0,1);
}

OOSQL_TCDynStr& OOSQL_TCDynStr::operator *= (UFour count)
/****************************************************************************
*
* Function:		OOSQL_TCDynStr::operator *=
* Parameters:	count	- Number of times to replicate string
* Returns:		Reference to the replicated string.
*
* Description:	Replicates a string 'count' times.
*
****************************************************************************/
{
	UFour	newsize,newlen;
	char	*temp,*p;

	PRECONDITION((UFour)(length() + length()*(count-1) + 1) < UINT_MAX);

	newlen = length() + length()*(count-1) + 1;
	newsize = computeSize(newlen);

	// If the amount of memory already allocated for the string is enough
	// we do not need to re-allocate the memory!

	if (newsize != size) {
		if(pMemoryManager)
			temp = (char*)pMemoryManager->Alloc(size);
		else
			temp = new char[size];
		if (temp  != NULL)
			memcpy(temp,text,length());
		}
	else
		temp = text;

	if (temp) {
		p = temp + length();
		for (UFour i = 1; i < count; i++,p += length())
			memcpy(p,temp,length());
		temp[newlen-1] = '\0';
		}

	if (newsize != size) {
		size = newsize;
		if(pMemoryManager)
			pMemoryManager->Free(text);
		else
			delete [] text;
		text = temp;
		}

	len = newlen;
	return *this;
}

// Slower concatenation/replication routines in terms of the faster ones

OOSQL_TCDynStr operator + (const OOSQL_TCString& s1,const OOSQL_TCString& s2)
{
	CHECK(s1.valid() && s2.valid());
	OOSQL_TCDynStr	s(s1);
	return s += s2;
}

OOSQL_TCDynStr operator + (const OOSQL_TCString& s1,const char *s2)
{
	CHECK(s1.valid() && s2 != NULL);
	OOSQL_TCDynStr	s(s1);
	return s += s2;
}

OOSQL_TCDynStr operator + (const char *s1,const OOSQL_TCString& s2)
{
	CHECK(s1 != NULL && s2.valid());
	OOSQL_TCDynStr	s(s1);
	return s += s2;
}

OOSQL_TCDynStr operator * (const OOSQL_TCString& s1,UFour count)
{
	CHECK(s1.valid());
	OOSQL_TCDynStr	s(s1);
	return s *= count;
}

OOSQL_TCDynStr& OOSQL_TCDynStr::del(UFour pos,UFour count,bool shrinkIt)
/****************************************************************************
*
* Function:		OOSQL_TCDynStr::del
* Parameters:	pos			- Position to delete from
*				count		- Number of characters to delete
*				shrinkIt	- True if string should be shrunk to minimum
* Returns:		Reference to the new string.
*
* Description:	Deletes a substring from the string, given the position
*				and number of characters to delete.
*
****************************************************************************/
{
	UFour	copyfrom;

	CHECK(valid());
	if (pos >= length() || count == 0)
		return *this;

	copyfrom = (UFour)pos + (UFour)count;

	if (copyfrom >= length()) {
		text[pos] = '\0';			// Just null terminate the string
		len = pos+1;
		}
	else {
		memmove(&text[pos],&text[copyfrom],(size_t)(len-copyfrom));
		len -= count;
		}

	if (shrinkIt)
		shrink();					// Shrink the size of the string

	return *this;
}

OOSQL_TCDynStr& OOSQL_TCDynStr::insert(UFour pos,const char ch)
/****************************************************************************
*
* Function:		OOSQL_TCDynStr::insert
* Parameters:	pos	- Character position to insert before
*				ch	- Character to insert
* Returns:		Reference to the new string.
*
* Description:	Inserts a character into the string before the character at
*				position 'pos'. If 'pos' is set to the length of the string,
*				the string is simply concatenated onto the end.
*
****************************************************************************/
{
	char	*temp;

	CHECK(valid());
	if (pos > length())
		return *this;

	// Expand the size of the string if necessary

	if (len == size) {
		size += granularity;
		if(pMemoryManager)
			temp = (char*)pMemoryManager->Alloc(size);
		else
			temp = new char[size];
		if (temp != NULL)
			memcpy(temp,text,len);
		if(pMemoryManager)
			pMemoryManager->Free(text);
		else
			delete [] text;
		text = temp;
		}

	// Move the text and insert the character

	if (valid()) {
		memmove(&text[pos+1],&text[pos],len-pos);
		text[pos] = ch;
		len++;
		text[len-1] = '\0';			// Null terminate the expanded string
		}

	return *this;
}

OOSQL_TCDynStr& OOSQL_TCDynStr::insert(UFour pos,const OOSQL_TCString& str)
/****************************************************************************
*
* Function:		OOSQL_TCDynStr::insert
* Parameters:	pos	- Character position to insert before
*				str	- OOSQL_TCString to insert
* Returns:		Reference to the new string.
*
* Description:	Inserts a string into the string before the character at
*				position 'pos'. If 'pos' is set to the length of the string,
*				the string is simply concatenated onto the end.
*
****************************************************************************/
{
	char	*temp;
	UFour	newlen,newsize;

	CHECK(valid());
	if (pos > length())
		return *this;

	// Expand the size of the string if necessary

	newlen = length() + str.length() + 1;
	newsize = ((newlen + granularity) / granularity) * granularity;

	if (newsize != size) {
		size = newsize;
		if(pMemoryManager)
			temp = (char*)pMemoryManager->Alloc(size);
		else
			temp = new char[size];
		if (temp != NULL)
			memcpy(temp,text,len);
		if(pMemoryManager)
			pMemoryManager->Free(text);
		else
			delete [] text;
		text = temp;
		}

	// Move the text and insert the new string

	if (valid()) {
		memmove(&text[pos+str.length()],&text[pos],len-pos);
		memcpy(&text[pos],str,str.length());
		len = newlen;
		text[len-1] = '\0';			// Null terminate the expanded string
		}

	return *this;
}

OOSQL_TCDynStr& OOSQL_TCDynStr::insert(UFour pos,const char *cstr)
/****************************************************************************
*
* Function:		OOSQL_TCDynStr::insert
* Parameters:	pos		- Character position to insert before
*				cstr	- C style string to insert
* Returns:		Reference to the new string.
*
* Description:	Inserts a string into the string before the character at
*				position 'pos'.
*
****************************************************************************/
{
	return insert(pos,OOSQL_TCSubStr(cstr));
}

OOSQL_TCDynStr& OOSQL_TCDynStr::replace(UFour pos,const char ch)
/****************************************************************************
*
* Function:		OOSQL_TCDynStr::replace
* Parameters:	pos	- Position to replace character at
*				ch	- Character to replace with
* Returns:		Reference to the new string
*
* Description:
*
****************************************************************************/
{
	char	*temp;

	CHECK(valid());
	if (pos < length())
		text[pos] = ch;
	else if (pos == length()) {

		// Expand the size of the string if necessary

		if (len == size) {
			size += granularity;
			if(pMemoryManager)
				temp = (char*)pMemoryManager->Alloc(size);
			else
				temp = new char[size];
			if (temp != NULL)
				memcpy(temp,text,len);
			if(pMemoryManager)
				pMemoryManager->Free(text);
			else
				delete [] text;
			text = temp;
			}

		// Insert the character

		if (valid()) {
			text[pos] = ch;
			len++;
			text[len-1] = '\0';		// Null terminate the expanded string
			}
		}
	return *this;
}

OOSQL_TCDynStr& OOSQL_TCDynStr::replace(UFour pos,const OOSQL_TCString& str)
/****************************************************************************
*
* Function:		OOSQL_TCDynStr::replace
* Parameters:	pos	- Position to replace string at
*				str	- OOSQL_TCString to replace with
* Returns:		Reference to the new string.
*
* Description:	Overwrites the string 'str' with the new string. If the new
*				string runs off the end of the old string, this string
*				will be re-sized to fit.
*
*				If pos is set to the length of the string, it will simply
*				be concatenated onto the end of the string.
*
****************************************************************************/
{
	char	*temp;
	UFour	newlen,newsize;

	CHECK(valid());
	if (pos > length())
		return *this;

	// Expand the size of the string if necessary

	newlen = pos + str.length() + 1;
	newlen = MAX(len,newlen);
	newsize = ((newlen + granularity) / granularity) * granularity;

	if (newsize != size) {
		size = newsize;
		if(pMemoryManager)
			temp = (char*)pMemoryManager->Alloc(size);
		else
			temp = new char[size];
		if (temp != NULL)
			memcpy(temp,text,len);
		if(pMemoryManager)
			pMemoryManager->Free(text);
		else
			delete [] text;
		text = temp;
		if (valid())
			text[newlen-1] = '\0';		// Null terminate the expanded string
		}

	// Overwrite the new text in the string

	if (valid()) {
		memcpy(&text[pos],str,str.length());
		len = newlen;
		}

	return *this;
}

OOSQL_TCDynStr& OOSQL_TCDynStr::replace(UFour pos,const char *cstr)
/****************************************************************************
*
* Function:		OOSQL_TCDynStr::replace
* Parameters:	pos		- Position to replace string at
*				cstr	- OOSQL_TCString to replace with
* Returns:		Reference to the new string.
*
* Description:	Overwrites the string 'str' with the new string. If the new
*				string runs off the end of the old string, this string
*				will be re-sized to fit.
*
*				If pos is set to the length of the string, it will simply
*				be concatenated onto the end of the string.
*
****************************************************************************/
{
	CHECK(valid());
	return replace(pos,OOSQL_TCSubStr(cstr));
}

OOSQL_TCDynStr& OOSQL_TCDynStr::justify(int mode,UFour count,bool clip,bool shrinkIt)
/****************************************************************************
*
* Function:		OOSQL_TCDynStr::justify
* Parameters:	mode		- Justification mode (left,center,right)
*				len			- Length of space to justify within
*				clip		- Clip to justification space?
*				shrinkIt	- True if the string should be shrunk
* Returns:		Reference to the newly allocated string.
*
* Description:
*
****************************************************************************/
{
	CHECK(valid());
	trim(center,0,false);

	if (!clip && len > count) {				// Check for out of bounds
		if (shrinkIt)
			shrink();
		return *this;
		}

	if (clip && len >= count) {				// Clip the string to bounds
		OOSQL_TCSubStr	s(*this);

		switch (mode) {
			case left:
				*this = s.left(count);
				break;
			case center:
				*this = s.mid((length()-count)/2,count);
				break;
			case right:
				*this = s.right(count);
				break;
			}
		}
	else {
		switch (mode) {
			case left:
				*this += OOSQL_TCDynStr(' ',count - length());
				break;
			case center:
				*this = OOSQL_TCDynStr(' ',(count - length())/2)
					+ *this
					+ OOSQL_TCDynStr(' ',count - (count+length())/2);
				break;
			case right:
				*this = OOSQL_TCDynStr(' ',count - length()) + *this;
				break;
			}
		}

	return *this;
}

OOSQL_TCDynStr& OOSQL_TCDynStr::trim(int mode,const char ch,bool shrink)
/****************************************************************************
*
* Function:		OOSQL_TCDynStr::trim
* Parameters:	mode	- Trimming mode (left,center,right)
*				ch		- Character's to trim (0 = whitespace).
* Returns:		Reference to the trimmed string.
*
* Description:	Trims the string of leading and/or trailing characters. If
*				ch is set to 0, we trim whitespace as defined by the
*				standard 'isspace' function.
*
****************************************************************************/
{
	int		i;
	char	*whitespace,p[2];

	CHECK(valid());
	if (ch != 0) {
		whitespace = p;
		p[0] = ch;
		p[1] = '\0';
		}
	else whitespace = " \t\n\v\f\r";

	if (mode == left || mode == center)
		del(0,strspn(text,whitespace),false);

	if (mode == right || mode == center) {
		i = len-1;
		while (strchr(whitespace,text[i]) != NULL)
			i--;
		del(i+1,(UFour)UINT_MAX,false);
		}

	if (shrink)
		this->shrink();

	return *this;
}
