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
/*    Fine-Granule Locking Version                                            */
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
#ifndef __PRIMITIVE_TYPES_H__
#define __PRIMITIVE_TYPES_H__

#include "param.h"

#ifndef SUPPORT_LARGE_DATABASE2
#include "primitivetypes-NormalDatabase.h"
#else
#include "primitivetypes-LargeDatabase2.h"
#endif


/*
 * Type Definition
 */
#if (defined(SOLARIS64) && !defined(_LARGEFILE64_SOURCE))
typedef off_t           devOffset_t;            /* 32bit architecture, raw-device(2G) & 64bit architecture, raw-device(8T) */
#elif (defined(SOLARIS64) && defined(_LARGEFILE64_SOURCE))
typedef off64_t         devOffset_t;            /* 32bit architecture, raw-device(8T), solaris */
#elif (defined(LINUX64) && !defined(_LARGEFILE64_SOURCE))
typedef off_t           devOffset_t;            /* 32bit architecture, raw-device(2G) & 64bit architecture, raw-device(8T) */
#elif (defined(LINUX64) && defined(_LARGEFILE64_SOURCE))
typedef __off64_t       devOffset_t;            /* 32bit architecture, raw-device(8T), linux */
#elif (defined(WIN32))
typedef long            devOffset_t;            /* 32bit architecture, raw-device(2G), Windows */
#endif


/*
 * MACRO
 */
#define SM_SERIAL               ((sizeof(Serial) == sizeof(Four_Invariable)) ? (SM_LONG) : (SM_LONG_LONG))
#define SM_SERIAL_SIZE          ((sizeof(Serial) == sizeof(Four_Invariable)) ? (SM_LONG_SIZE) : (SM_LONG_LONG_SIZE))

#define SM_VOLNO                ((sizeof(VolNo) == sizeof(Two_Invariable)) ? (SM_SHORT) : (SM_LONG))
#define SM_VOLNO_SIZE           ((sizeof(VolNo) == sizeof(Two_Invariable)) ? (SM_SHORT_SIZE) : (SM_LONG_SIZE))

#if !defined(_LP64) && !defined(_LARGEFILE64_SOURCE)
#define MAX_RAW_DEVICE_OFFSET   (2147483647L)
#elif !defined(_LP64) && defined(_LARGEFILE64_SOURCE)
#define MAX_RAW_DEVICE_OFFSET   (9223372036854775807LL)
#elif defined(_LP64)
#define MAX_RAW_DEVICE_OFFSET   (9223372036854775807L)
#endif

#if defined(_LP64)
#define MAX_VOLUME_NUMBER       ((sizeof(VolNo) > sizeof(Two_Invariable)) ? (2147483647L) : (32767))
#define MAX_PAGES_IN_VOLUME     ((sizeof(PageNo) > sizeof(Four_Invariable)) ? (9223372036854775807L) : (2147483647L))
#else
#define MAX_VOLUME_NUMBER       ((sizeof(VolNo) > sizeof(Two_Invariable)) ? (2147483647L) : (32767))
#if defined(WIN32)
#define MAX_PAGES_IN_VOLUME     ((sizeof(PageNo) > sizeof(Four_Invariable)) ? (9223372036854775807L) : (2147483647L))
#else
#define MAX_PAGES_IN_VOLUME     ((sizeof(PageNo) > sizeof(Four_Invariable)) ? (9223372036854775807LL) : (2147483647L))
#endif
#endif

#define STTWO(_handle, _two, _p)   	perThreadTable[_handle].lrdsDS.lrdsformat_tmpTwo = _two; \
                                	memcpy((char*)(_p), (char*)&(perThreadTable[_handle].lrdsDS.lrdsformat_tmpTwo), \
					       sizeof(Two_Invariable))
#define STFOUR(_handle, _four, _p)      perThreadTable[_handle].lrdsDS.lrdsformat_tmpFour = _four; \
                                	memcpy((char*)(_p), (char*)&(perThreadTable[_handle].lrdsDS.lrdsformat_tmpFour), \
					       sizeof(Four_Invariable))
#define STEIGHT(_handle, _eight, _p)    perThreadTable[_handle].lrdsDS.lrdsformat_tmpEight = _eight; \
                                	memcpy((char*)(_p), (char*)&(perThreadTable[_handle].lrdsDS.lrdsformat_tmpEight), \
					       sizeof(Eight_Invariable))

#define ST_TWO_FOUR_EIGHT_INVARIABLE(_handle, _v, _p, _t) \
	if (sizeof(_t) == sizeof(Two_Invariable)) { STTWO(_handle, _v, _p); } \
        else if (sizeof(_t) == sizeof(Four_Invariable)) { STFOUR(_handle, _v, _p); } \
        else if (sizeof(_t) == sizeof(Eight_Invariable)) { STEIGHT(_handle, _v, _p); } \

#define CONVERT_TO_SM_TYPE(_type) \
	((sizeof(_type) == sizeof(Two_Invariable)) ? (SM_SHORT) : \
                                                     (sizeof(_type) == sizeof(Four_Invariable)) ? (SM_LONG) : \
                                                                                                  (SM_LONG_LONG)) \

#define ASSIGN_BASIC_TYPE_VALUE_TO_COL_LIST_STRUCT(_handle, _clist, _value, _type) \
	do { \
		if (sizeof(_type) == sizeof(Two_Invariable)) { \
			perThreadTable[_handle].lrdsDS.lrdsformat_tmpTwo = _value; \
			memcpy(&((_clist).data), &(perThreadTable[_handle].lrdsDS.lrdsformat_tmpTwo), sizeof(Two_Invariable)); \
		} \
		else if (sizeof(_type) == sizeof(Four_Invariable)) { \
			perThreadTable[_handle].lrdsDS.lrdsformat_tmpFour = _value; \
			memcpy(&((_clist).data), &(perThreadTable[_handle].lrdsDS.lrdsformat_tmpFour), sizeof(Four_Invariable)); \
		} \
		else if (sizeof(_type) == sizeof(Eight_Invariable)) { \
			perThreadTable[_handle].lrdsDS.lrdsformat_tmpEight = _value; \
			memcpy(&((_clist).data), &(perThreadTable[_handle].lrdsDS.lrdsformat_tmpEight), sizeof(Eight_Invariable)); \
		} \
	} while (0) \

#define GET_BASIC_TYPE_VALUE_FROM_COL_LIST_STRUCT(_clist, _type) \
	((sizeof(_type) == sizeof(Two_Invariable)) ? ((_clist).data.s) : \
                                                     ((sizeof(_type) == sizeof(Four_Invariable)) ? ((_clist).data.l) : \
												   ((_clist).data.ll))) \

#if defined(SUPPORT_LARGE_DATABASE2)
#if defined(_LP64)
#define CONSTANT_CASTING_TYPE           long
#define CONSTANT_UNSIGNED_CASTING_TYPE  unsigned long
#define CONSTANT_ONE                    1L
#else
#define CONSTANT_CASTING_TYPE           long long
#define CONSTANT_UNSIGNED_CASTING_TYPE  unsigned long long
#define CONSTANT_ONE                    1LL
#endif
#else /* defined(SUPPORT_LARGE_DATABASE2) */
#define CONSTANT_CASTING_TYPE           int
#define CONSTANT_UNSIGNED_CASTING_TYPE  unsigned int
#define CONSTANT_ONE                    1
#endif

#define CONSTANT_ALL_BITS_SET(_size)    ((sizeof(_size) == 1) ? (0xff) : \
					((sizeof(_size) == 2) ? (0xffff) : \
					((sizeof(_size) == 4) ? (0xffffffff) : (0xffffffffffffffff)))) \

#endif /* __PRIMITIVE_TYPES_H__ */
