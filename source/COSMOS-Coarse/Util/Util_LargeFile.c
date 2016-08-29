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
/*    ODYSSEUS/COSMOS General-Purpose Large-Scale Object Storage System --    */
/*    Coarse-Granule Locking (Volume Lock) Version                            */
/*    Version 3.0                                                             */
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
 * Module : Util_LargeFile.c
 *
 * Description :
 *  Support files that are larger than 2GB.
 *  Solaris 2.7 or above supports large files.
 *  AIX 4.2 or above supports large files.
 *
 * Exports :
 *  FILE *Util_fopen(const char *, const char *);
 *  Four  Util_fseek(FILE *, Eight, Four);
 *  Eight Util_ftell(FILE *);
 *
 */

#include <stdio.h>
#include "common.h"
#include "Util.h"
#include "perThreadDS.h"
#include "perProcessDS.h"

#ifdef USE_LARGE_FILE

FILE *Util_fopen(const char *pathname, const char *type)
{
#if (defined(AIX64) || defined(SOLARIS64) || defined(LINUX64)) && !defined(EIGHT_NOT_DEFINED)
	return fopen64(pathname, type);
#elif (defined(WIN64) || defined(WIN32)) && !defined(EIGHT_NOT_DEFINED)
	return fopen(pathname, type);
#else
	return fopen(pathname, type);
#endif
}

Four Util_fclose(FILE* stream)
{
#if (defined(AIX64) || defined(SOLARIS64) || defined(LINUX64)) && !defined(EIGHT_NOT_DEFINED)
	return fclose(stream);
#elif (defined(WIN64) || defined(WIN32)) && !defined(EIGHT_NOT_DEFINED)
	return fclose(stream);
#else
	return fclose(stream);
#endif
}

Four Util_fseek(FILE *stream, filepos_t offset, Four whence)
{
#if (defined(AIX64) || defined(SOLARIS64) || defined(LINUX64)) && !defined(EIGHT_NOT_DEFINED)
	return fseeko64(stream, offset, whence);
#elif (defined(WIN64) || defined(WIN32)) && !defined(EIGHT_NOT_DEFINED)
	fpos_t offset2;
	fpos_t pos;
	switch (whence) {
		case SEEK_CUR:
			if (fgetpos(stream, &pos) != 0)
				return -1;
			offset += pos;
			break;
		case SEEK_END:
			/* do a "no-op" seek first to sync the buffering so that
			   the low-level tell() can be used correctly */
			if (fseek(stream, 0, SEEK_END) != 0)
				return -1;
			if ((pos = _telli64(fileno(stream))) == -1L)
				return -1;
			offset += pos;
			break;
		/* case SEEK_SET: break; */
	}
	offset2 = offset;
	return fsetpos(stream, &offset2);
#else
	return fseek(stream, (Four_Invariable)offset, whence);
#endif
}

filepos_t Util_ftell(FILE *stream)
{
#if (defined(AIX64) || defined(SOLARIS64) || defined(LINUX64)) && !defined(EIGHT_NOT_DEFINED)
	return ftello64(stream);
#elif (defined(WIN64) || defined(WIN32)) && !defined(EIGHT_NOT_DEFINED)
	fpos_t pos;
	if (fgetpos(stream, &pos) != 0)
		return -1;
	return (filepos_t)pos;
#else
	return ftell(stream);
#endif
}

#else	/* USE_LARGE_FILE */

FILE *Util_fopen(const char *pathname, const char *type)
{
	return fopen(pathname, type);
}

Four Util_fclose(Four handle, FILE* stream)
{
	return fclose(stream);
}

Four Util_fseek(Four handle, FILE *stream, filepos_t offset, Four whence)
{
	return fseek(stream, (Four_Invariable)offset, whence);
}

filepos_t Util_ftell(handle, FILE *stream)
{
	return ftell(stream);
}

#endif

