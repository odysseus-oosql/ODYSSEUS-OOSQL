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
* Description:  Header file for a the OOSQL_TCString, OOSQL_TCDynStr and OOSQL_TCSubStr classes.
*               The OOSQL_TCDynStr class is a class for representing dynamically
*               sized strings, and the OOSQL_TCSubStr class is a class for
*               representing finite sub strings that index directly into
*               an allocated dynamic string or a normal C style string.
*
*               To avoid lots of data copying you can set the granularity
*               for a OOSQL_TCDynStr to a large value, say 1k rather than the
*               default 8 bytes. Since the granularity is global to all
*               string's, you should reset the granularity to the previous
*               value when you are finished. ie:
*
*               myfunc()
*               {
*                   UFour   old_granularity = OOSQL_TCString::setGranularity(1024);
*
*                   ... do stuff with 1k strings ...
*
*                   OOSQL_TCString::setGranularity(old_granularity);
*               }
*
*               Remember that if you convert a OOSQL_TCSubStr to a (const char *)
*               the string may not necessarily be null terminated, so
*               you need to only look at 'length()' number of characters.
*
*
****************************************************************************/

#ifndef _OOSQL_TCL_STR_H_
#define _OOSQL_TCL_STR_H_

#include "OOSQL_Common.h"
#include <iostream>
#include <string.h>
#include <limits.h>
#include "OOSQL_MemoryManager.hxx"
#include "OOSQL_MemoryManagedObject.hxx"

/*------------------------ Compatability Definitions ----------------------*/

#if defined(__LINUX__) || defined(_SUNCPP_V4_)
#include <ctype.h>
#undef toupper
#undef tolower

inline void strupr(char *s)
{ while (*s) { *s = toupper(*s); s++; } }

inline void strlwr(char *s)
{ while (*s) { *s = tolower(*s); s++; } }

#endif

#ifndef MIN
#define MIN(x, y) (((x)<(y))?(x):(y))
#endif

#ifndef MAX
#define MAX(x, y) (((x)>(y))?(x):(y))
#endif

/*--------------------------- Class Definitions ---------------------------*/

class OOSQL_TCSubStr;
class OOSQL_TCDynStr;

//---------------------------------------------------------------------------
// The OOSQL_TCString class is an abstract base class for the dynamically allocated
// string class and substring class.
//
// The only methods and operations available on the base string class
// are ones that do not modify the internal representation.
//---------------------------------------------------------------------------

class OOSQL_TCString {
protected:
    UFour       len;                // Length of the string
    char        *text;              // Pointer to the string data

public:
            // Returns the length of the string
            UFour length() const        { return len-1; };

            // Overloaded cast to a (const char *)
            operator const char * () const  { return text; };

            // Return a const char *
            const char * str() const    { return text; };

            // Retrieve the ith character from the string
            char operator [] (UFour i) const
                { return i > len ? '\0' : text[i]; };

            // Equality operator
    friend  int operator == (const OOSQL_TCString& s1,const OOSQL_TCString& s2)
                { return strncmp(s1,s2,MIN(s1.length(),s2.length())) == 0; };
    friend  int operator == (const OOSQL_TCString& s1,const char* s2)
                { return strncmp(s1,s2,s1.length()) == 0; };
    friend  int operator == (const char *s1,const OOSQL_TCString& s2)
                { return strncmp(s1,s2,s2.length()) == 0; };

            // Inequality operator
    friend  int operator != (const OOSQL_TCString& s1,const OOSQL_TCString& s2)
                { return strncmp(s1,s2,MIN(s1.length(),s2.length())) != 0; };
    friend  int operator != (const OOSQL_TCString& s1,const char* s2)
                { return strncmp(s1,s2,s1.length()) != 0; };
    friend  int operator != (const char *s1,const OOSQL_TCString& s2)
                { return strncmp(s1,s2,s2.length()) != 0; };

            // Less than operator
    friend  int operator < (const OOSQL_TCString& s1,const OOSQL_TCString& s2)
                { return strncmp(s1,s2,MIN(s1.length(),s2.length())) < 0; };
    friend  int operator < (const OOSQL_TCString& s1,const char* s2)
                { return strncmp(s1,s2,s1.length()) < 0; };
    friend  int operator < (const char *s1,const OOSQL_TCString& s2)
                { return strncmp(s1,s2,s2.length()) < 0; };

            // Greater than operator
    friend  int operator > (const OOSQL_TCString& s1,const OOSQL_TCString& s2)
                { return strncmp(s1,s2,MIN(s1.length(),s2.length())) > 0; };
    friend  int operator > (const OOSQL_TCString& s1,const char* s2)
                { return strncmp(s1,s2,s1.length()) > 0; };
    friend  int operator > (const char *s1,const OOSQL_TCString& s2)
                { return strncmp(s1,s2,s2.length()) > 0; };

            // Less than or equal to operator
    friend  int operator <= (const OOSQL_TCString& s1,const OOSQL_TCString& s2)
                { return strncmp(s1,s2,MIN(s1.length(),s2.length())) <= 0; };
    friend  int operator <= (const OOSQL_TCString& s1,const char* s2)
                { return strncmp(s1,s2,s1.length()) <= 0; };
    friend  int operator <= (const char *s1,const OOSQL_TCString& s2)
                { return strncmp(s1,s2,s2.length()) <= 0; };

            // Greater than or equal to operator
    friend  int operator >= (const OOSQL_TCString& s1,const OOSQL_TCString& s2)
                { return strncmp(s1,s2,MIN(s1.length(),s2.length())) >= 0; };
    friend  int operator >= (const OOSQL_TCString& s1,const char* s2)
                { return strncmp(s1,s2,s1.length()) >= 0; };
    friend  int operator >= (const char *s1,const OOSQL_TCString& s2)
                { return strncmp(s1,s2,s2.length()) >= 0; };

            // Convert routines for strings
            int asInt() const;
            short asShort() const   { return (short)asInt(); };
            long asLong() const;
            unsigned int asUInt() const;
            unsigned short asUShort() const { return (UTwo)asUInt(); };
            unsigned long asULong() const;
            float asFloat() const;
            double asDouble() const;

            // Split the string into an array of substrings
            UFour split(OOSQL_TCSubStr*& array,const char *fs) const;

            // Compute a hash value for the string
            UFour hash() const;

            // Returns true if the string is valid
            bool valid() const      { return (text != NULL)?true:false; };
    };

//---------------------------------------------------------------------------
// The DynString class is a class for maintaining dynamically sized strings.
// Storage for the string is maintained dynamically on the heap.
//
// Note that the string class is maintained as a 'C' style string
// internally, with an integer count.
//---------------------------------------------------------------------------

class OOSQL_TCDynStr : public OOSQL_TCString {
protected:
    UFour       size;               // Amount of memory allocated
	OOSQL_MemoryManager* pMemoryManager;
	bool	memoryManagedObjectFlag;

    static UFour    granularity;        // Granularity of memory allocation

            UFour computeSize(UFour len)
            {
                return ((len + granularity) / granularity) * granularity;
            };

public:
            // Enumerations for the string class
    enum    { left, center, right };

            // Default constructor
            OOSQL_TCDynStr(OOSQL_MemoryManager* memoryManager = NULL);

            // Copy constructor
            OOSQL_TCDynStr(const OOSQL_TCDynStr& str);

            // Constructor given another string and counts etc
            OOSQL_TCDynStr(const OOSQL_TCString& str,OOSQL_MemoryManager* memoryManager = NULL,UFour pos = 0,UFour count = (UFour)UINT_MAX);

            // Constructor using a normal C type string
            OOSQL_TCDynStr(const char *cstr,OOSQL_MemoryManager* memoryManager = NULL,UFour pos = 0,UFour count = (UFour)UINT_MAX);

            // Constructor given a character and a count value
            OOSQL_TCDynStr(char c,UFour count = 1U,OOSQL_MemoryManager* memoryManager = NULL);

            // Assignment operator given a OOSQL_TCString
            OOSQL_TCDynStr& operator = (const OOSQL_TCString& str);
            OOSQL_TCDynStr& operator = (const OOSQL_TCDynStr& str);

            // Assignment operator given a C type string
            OOSQL_TCDynStr& operator = (const char *cstr);

            // Destructor
            ~OOSQL_TCDynStr();

            // Convert the string to all upper case
            OOSQL_TCDynStr& toupper()       { strupr(text); return *this; };

            // Convert the string to all lower case
            OOSQL_TCDynStr& tolower()       { strlwr(text); return *this; };

            // Substring deletion method
            OOSQL_TCDynStr& del(UFour pos,UFour count = (UFour)UINT_MAX,
                bool shrink = true);

            // Substring Insertion method given another string
            OOSQL_TCDynStr& insert(UFour pos,const OOSQL_TCString& str);

            // Substring Insertion method given a C type string
            OOSQL_TCDynStr& insert(UFour pos,const char *cstr);

            // Character Insertion method
            OOSQL_TCDynStr& insert(UFour pos,const char ch);

            // Substring replacement method given another string
            OOSQL_TCDynStr& replace(UFour pos,const OOSQL_TCString& str);

            // Substring replacement method given a C type string
            OOSQL_TCDynStr& replace(UFour pos,const char *cstr);

            // Character replacement method
            OOSQL_TCDynStr& replace(UFour pos,const char ch);

            // Justify the string according to the mode
            OOSQL_TCDynStr& justify(int mode,UFour len,bool clip = false,
                bool shrink = true);

            // Trim the string of leading/trailing characters
            OOSQL_TCDynStr& trim(int mode = center,const char ch = 0,
                bool shrink = true);

            //---------------------------------------------------------------
            // Fast efficient operators (mimimum copying).
            //---------------------------------------------------------------

            // Concatenation operators
            OOSQL_TCDynStr& operator += (const OOSQL_TCString& str);
            OOSQL_TCDynStr& operator += (const char *str);
            OOSQL_TCDynStr& operator += (const char);

            // Replicate a string a number of times
            OOSQL_TCDynStr& operator *= (UFour count);

            //---------------------------------------------------------------
            // Slower but more flexible operators (lots of construction etc).
            //---------------------------------------------------------------

            // Concatenation operators
    friend  OOSQL_TCDynStr operator + (const OOSQL_TCString& s1,const OOSQL_TCString& s2);
    friend  OOSQL_TCDynStr operator + (const OOSQL_TCString& s1,const char *s2);
    friend  OOSQL_TCDynStr operator + (const char *s1,const OOSQL_TCString& s2);

            // Replicate a string a number of times
    friend  OOSQL_TCDynStr operator * (const OOSQL_TCString& s1,UFour count);

            // Shrink the string to the minimum memory allocation
            void shrink();
			void resize(UFour newresize);

            // Member functions to set and obtain the granularity
    static  UFour setGranularity(UFour gran)
                { UFour old = granularity; granularity = gran; return old; };
    static  UFour getGranularity()
                { return granularity; };

            // Method to dump a OOSQL_TCDynStr to a stream
    friend  std::ostream& operator << (std::ostream& o,const OOSQL_TCDynStr& s)
                { return o << (const char *)s; };
    };

//---------------------------------------------------------------------------
// The OOSQL_TCSubStr class is a class for maintaining sub strings that are indexed
// into the storage of an allocated dynamic string, or a c style string.
// No storage is owned by the class, so none is deleted when it is killed.
//
// Thus the class does not allow you to modify the string it represents.
//---------------------------------------------------------------------------

class OOSQL_TCSubStr : public OOSQL_TCString {
public:
            // Default constructor
            OOSQL_TCSubStr();

            // Constructor given another string
            OOSQL_TCSubStr(const OOSQL_TCString& str,UFour pos = 0,UFour count = (UFour)UINT_MAX);

            // Constructor using a normal C type string
            OOSQL_TCSubStr(const char *cstr,UFour pos = 0,UFour count = (UFour)UINT_MAX);

            // Assignment operator given a OOSQL_TCString
            OOSQL_TCSubStr& operator = (const OOSQL_TCString& str);

            // Assignment operator given a C type string
            OOSQL_TCSubStr& operator = (const char *cstr);

            // Overloaded cast to a DynString
            operator OOSQL_TCDynStr () const;

            // Convert to leftmost count characters
            OOSQL_TCSubStr& left(UFour count);

            // Convert to middle count characters starting at pos
            OOSQL_TCSubStr& mid(UFour pos,UFour count);

            // Convert to rightmost count characters
            OOSQL_TCSubStr& right(UFour count);

            // Method to dump a OOSQL_TCSubStr to a stream
    friend  std::ostream& operator << (std::ostream& o,const OOSQL_TCSubStr& s);
    };

#endif  // _OOSQL_TCL_STR_H_
