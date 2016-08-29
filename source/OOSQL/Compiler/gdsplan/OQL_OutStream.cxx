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
#include <malloc.h>
#include <string.h>
#include "OQL_OutStream.hxx"

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& OQL_OutStream::flush()
{
    fflush(stdout);

    return *this;
}

/****************************************************************************
DESCRIPTION:


RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& OQL_OutStream::operator<<(OQL_OutStream& (*_f)(OQL_OutStream&))
{
    (*_f)(*this); 

    return *this;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& OQL_OutStream::operator<<(const char * s)
{
    printf("%s", s);

    return *this;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& OQL_OutStream::operator<<(const unsigned char *s)
{
    return operator<<((const char *) s);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& OQL_OutStream::operator<<(const signed char *s)
{
    return operator<<((const char *) s);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& OQL_OutStream::operator<<(char c)
{
    return operator<<((unsigned char) c);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& OQL_OutStream::operator<<(unsigned char c)
{
    printf("%c", c);

    return *this;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& OQL_OutStream::operator<<(signed char c)
{
    return operator<<((unsigned char) c);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& OQL_OutStream::operator<<(short d)
{
    return operator<<((int) d);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& OQL_OutStream::operator<<(unsigned short d)
{
    return operator<<((unsigned int) d);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& OQL_OutStream::operator<<(int d)
{
    printf("%ld", d);

    return *this;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& OQL_OutStream::operator<<(unsigned int d)
{
    printf("%ud", d);

    return *this;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& OQL_OutStream::operator<<(long d)
{
    return operator<<((int) d);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& OQL_OutStream::operator<<(unsigned long d)
{
    return operator<<((unsigned int) d);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& OQL_OutStream::operator<<(float f)
{
    return operator<<((double) f);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& OQL_OutStream::operator<<(double f)
{
    printf("%f", f);

    return *this;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& OQL_OutStream::operator<<(long double f)
{
    return operator<<((double) f);

    return *this;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& OQL_OutStream::operator<<(const void * p)
{
    printf("%X", p);

    return *this;
}



/****************************************************************************
DESCRIPTION:
****************************************************************************/
OQL_StringOutStream::OQL_StringOutStream()		
{ 
	stringBuffer = NULL; 
}
OQL_StringOutStream::~OQL_StringOutStream()		
{ 
	if(stringBuffer) free(stringBuffer);
}
OQL_OutStream& OQL_StringOutStream::flush()
{
	return *this;
}
OQL_OutStream& OQL_StringOutStream::operator<<(OQL_OutStream& (*_f)(OQL_OutStream&))
{
	(*_f)(*this); 
    return *this;
}
OQL_OutStream& OQL_StringOutStream::operator<<(const char *s)
{
	AddToString((const char*)s);
    return *this;
}
OQL_OutStream& OQL_StringOutStream::operator<<(const unsigned char *s)
{
	AddToString((const char*)s);
    return *this;
}
OQL_OutStream& OQL_StringOutStream::operator<<(const signed char *s)
{
	AddToString((const char*)s);
    return *this;
}
OQL_OutStream& OQL_StringOutStream::operator<<(char c)
{
	char temp_string[32];
	sprintf(temp_string, "%c", c);
	AddToString(temp_string);
    return *this;
}
OQL_OutStream& OQL_StringOutStream::operator<<(unsigned char c)
{
	char temp_string[32];
	sprintf(temp_string, "%c", c);
	AddToString(temp_string);
    return *this;
}
OQL_OutStream& OQL_StringOutStream::operator<<(signed char c)
{
	char temp_string[32];
	sprintf(temp_string, "%c", c);
	AddToString(temp_string);
    return *this;
}
OQL_OutStream& OQL_StringOutStream::operator<<(short s)
{
	char temp_string[32];
	sprintf(temp_string, "%ld", (int)s);
	AddToString(temp_string);
    return *this;
}
OQL_OutStream& OQL_StringOutStream::operator<<(unsigned short s)
{
	char temp_string[32];
	sprintf(temp_string, "%ld", (unsigned int)s);
	AddToString(temp_string);
    return *this;
}
OQL_OutStream& OQL_StringOutStream::operator<<(int i)
{
	char temp_string[32];
	sprintf(temp_string, "%ld", i);
	AddToString(temp_string);
    return *this;
}
OQL_OutStream& OQL_StringOutStream::operator<<(unsigned int i)
{
	char temp_string[32];
	sprintf(temp_string, "%ld", i);
	AddToString(temp_string);
    return *this;
}
OQL_OutStream& OQL_StringOutStream::operator<<(long l)
{
	char temp_string[32];
	sprintf(temp_string, "%ld", (int)l);
	AddToString(temp_string);
    return *this;
}
OQL_OutStream& OQL_StringOutStream::operator<<(unsigned long l)
{
	char temp_string[32];
	sprintf(temp_string, "%ld", (unsigned int)l);
	AddToString(temp_string);
    return *this;
}
OQL_OutStream& OQL_StringOutStream::operator<<(float f)
{
	char temp_string[32];
	sprintf(temp_string, "%f", (double)f);
	AddToString(temp_string);
    return *this;
}
OQL_OutStream& OQL_StringOutStream::operator<<(double d)
{
	char temp_string[32];
	sprintf(temp_string, "%f", (double)d);
	AddToString(temp_string);
    return *this;
}
OQL_OutStream& OQL_StringOutStream::operator<<(long double ld)
{
	char temp_string[32];
	sprintf(temp_string, "%ld", (double)ld);
	AddToString(temp_string);
    return *this;
}
OQL_OutStream& OQL_StringOutStream::operator<<(const void * v)
{
	char temp_string[32];
	sprintf(temp_string, "%X", v);
	AddToString(temp_string);
    return *this;
}
void OQL_StringOutStream::AddToString(const char* s)
{
	if(stringBuffer == NULL)
	{
		stringBufferSize = 1024;
		stringBuffer     = (char*)malloc(stringBufferSize);
		if(stringBuffer == NULL)	
			return;
		strcpy(stringBuffer, "");
	}
	while(1) 
	{
		int sLength      = strlen(s);
		int stringLength = strlen(stringBuffer);
		if(stringLength + sLength >= stringBufferSize)
		{
			stringBufferSize *= 2;
			stringBuffer      = (char*)realloc(stringBuffer, stringBufferSize);
		}
		else
			break;
	}
	strcat(stringBuffer, s);
}
char* OQL_StringOutStream::GetString()
{
	if(stringBuffer)
		return stringBuffer; 
	else
		return "";
}
