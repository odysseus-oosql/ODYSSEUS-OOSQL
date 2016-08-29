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
#ifndef __DBLABLIB_H__
#define __DBLABLIB_H__

#include "param.h"

#ifdef  __cplusplus
extern "C" {
#endif

/*
 * Type definitions for the basic types
 *
 * Note: it is used not to COSMOS but to ODYSSEUS.
 */

#if defined(_LP64) && defined(SUPPORT_LARGE_DATABASE2) 

/* one byte data type (in fact, it is a two byte data type) */
typedef short                   One;
typedef unsigned short          UOne;

/* two bytes data type (in fact, it is a four byte data type) */
typedef int                     Two;
typedef unsigned int            UTwo;

/* four bytes data type (in fact, it is a eight byte data type) */
typedef long                    Four;
typedef unsigned long           UFour;

/* eight bytes data type */
typedef long                    Eight;
typedef unsigned long           UEight;

/* invarialbe size data type */
typedef char                    One_Invariable;
typedef unsigned char           UOne_Invariable;
typedef One                     Two_Invariable;
typedef UOne                    UTwo_Invariable;
typedef Two                     Four_Invariable;
typedef UTwo                    UFour_Invariable;
typedef Four                    Eight_Invariable;
typedef UFour                   UEight_Invariable;

/* data & memory align type */
typedef Eight_Invariable		ALIGN_TYPE;
typedef Eight_Invariable 		MEMORY_ALIGN_TYPE;

#elif defined(_LP64) && !defined(SUPPORT_LARGE_DATABASE2)

/* one byte data type */
typedef char                    One;
typedef unsigned char           UOne;

/* two bytes data type */
typedef short                   Two;
typedef unsigned short          UTwo;

/* four bytes data type */
typedef int                     Four;
typedef unsigned int            UFour;

/* eight bytes data type */
typedef long                    Eight;
typedef unsigned long           UEight;

/* invarialbe size data type */
typedef char                    One_Invariable;
typedef unsigned char           UOne_Invariable;
typedef Two                     Two_Invariable;
typedef UTwo                    UTwo_Invariable;
typedef Four                    Four_Invariable;
typedef UFour                   UFour_Invariable;
typedef Eight                   Eight_Invariable;
typedef UEight                  UEight_Invariable;

/* data & memory align type */
typedef Four_Invariable        	ALIGN_TYPE;
typedef Eight_Invariable 		MEMORY_ALIGN_TYPE;

#elif !defined(_LP64) && defined(SUPPORT_LARGE_DATABASE2)

/* one byte data type (in fact, it is a two byte data type) */
typedef short                   One;
typedef unsigned short          UOne;

/* two bytes data type (in fact, it is a four byte data type) */
typedef long                    Two;
typedef unsigned long           UTwo;

/* four bytes data type (in fact, it is a eight byte data type) */
#if defined(AIX64) || defined(SOLARIS64) || defined(LINUX64)
typedef long long               Four;
typedef unsigned long long      UFour;
#elif defined(WIN64) || defined(WIN32)
typedef __int64                 Four;
typedef unsigned __int64        UFour;
#else
#define EIGHT_NOT_DEFINED
#endif /* defined(AIX64) || defined(SOLARIS64) || defined(LINUX64) */

/* eight bytes data type */
#if defined(AIX64) || defined(SOLARIS64) || defined(LINUX64)
typedef long long               Eight;
typedef unsigned long long      UEight;
#elif defined(WIN64) || defined(WIN32)
typedef __int64                 Eight;
typedef unsigned __int64        UEight;
#else
#define EIGHT_NOT_DEFINED
#endif /* defined(AIX64) || defined(SOLARIS64) || defined(LINUX64) */
 
/* invarialbe size data type */
typedef char                    One_Invariable;
typedef unsigned char           UOne_Invariable;
typedef One                     Two_Invariable;
typedef UOne                    UTwo_Invariable;
typedef Two                     Four_Invariable;
typedef UTwo                    UFour_Invariable;
#if defined(AIX64) || defined(SOLARIS64) || defined(LINUX64) || defined(WIN64) || defined(WIN32)
typedef Four                    Eight_Invariable;
typedef UFour                   UEight_Invariable;
#endif

/* data & memory align type */
typedef Eight_Invariable        ALIGN_TYPE;
typedef Four_Invariable         MEMORY_ALIGN_TYPE;

#elif !defined(_LP64) && !defined(SUPPORT_LARGE_DATABASE2) 

/* one byte data type */
typedef char                    One;
typedef unsigned char           UOne;

/* two bytes data type */
typedef short                   Two;
typedef unsigned short          UTwo;

/* four bytes data type */
typedef long                    Four;
typedef unsigned long           UFour;
 
/* eight bytes data type */
#if defined(AIX64) || defined(SOLARIS64) || defined(LINUX64)
typedef long long               Eight;
typedef unsigned long long      UEight;
#elif defined(WIN64) || defined(WIN32)
typedef __int64                 Eight;
typedef unsigned __int64        UEight;
#else
#define EIGHT_NOT_DEFINED
#endif /* defined(AIX64) || defined(SOLARIS64) || defined(LINUX64) */
 
/* invarialbe size data type */
typedef char                    One_Invariable;
typedef unsigned char           UOne_Invariable;
typedef Two                     Two_Invariable;
typedef UTwo                    UTwo_Invariable;
typedef Four                    Four_Invariable;
typedef UFour                   UFour_Invariable;
#if defined(AIX64) || defined(SOLARIS64) || defined(LINUX64) || defined(WIN64) || defined(WIN32)
typedef Eight                   Eight_Invariable;
typedef UEight                  UEight_Invariable;
#endif

/* data & memory align type */
typedef Four_Invariable         ALIGN_TYPE;
typedef Four_Invariable         MEMORY_ALIGN_TYPE;

#endif 


#ifdef __cplusplus
}
#endif

#endif /* __DBLABLIB_H__ */
