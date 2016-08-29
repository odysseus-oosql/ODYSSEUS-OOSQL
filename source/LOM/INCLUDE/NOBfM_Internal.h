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

#ifndef __OBFM_INTERNAL_H_
#define __OBFM_INTERNAL_H_

#ifndef EXTERN 
#define EXTERN extern
#endif

#include <stdio.h>
#include <malloc.h>
#include "OBfM_Err.h"

#ifndef PRIMITIVE
#define PRIMITIVE

#endif



/* Define STATEMEMT */
#define MAXDATASIZE_OBFM	1000
#define MAXOBJECTPOOLSIZE 1024*256 /* 256k byes */
#define MAXNUMOFUSERDESCRIPTOR 1024*4 /* 4k RODs */
#define ALIGNMENTLENGTH 8
#define LENGTH_FIELD	8
#define TOTALNUMCATALOGS 8
#define MINCOUNT_TTREE 1
#define MAXCOUNT_TTREE 10
#define eNOERROR	0
#define OBFM_OBJECT 1
#define OBFM_VALUE 0


/* 50 % of MAXDATAPARTITIONSIZE*/
#define COMPACTTONED_FREE_DATA_SIZE (MAXOBJECTPOOLSIZE)/2 
#define ISHALFSIZEFREE(p) ((p)->totalFreeSize) >= (MAXOBJECTPOOLSIZE)/2

/*
 * Typedef for ObjectDescriptor
 *
 * Descriptor for accessing object in object buffer
 *
 */

typedef struct {
	unsigned mark:1;
	unsigned dirty:1;
	unsigned referenced:1;
	unsigned hasParent:1;
/*
	OID oid;
*/
}objectDescriptor;


/*
 * Typedef for User Descriptor
 *
 * Descriptor for accessing object in user application
*/

typedef struct {
	unsigned isSwappedOut:1;
	unsigned isValid:1;
	unsigned referenceCount:16; 
	objectDescriptor *objPtr;
}userDescriptor;

/*
 * Typedef for OBfM_ObjectPool
 *
 * Pool for objects and their values' allocation
*/

typedef struct {
	char data[MAXOBJECTPOOLSIZE];
	char *freeAdr; /* contiguous free addree of this partition */
	Four totalFreeSize; /* total free bytes */
}OBfM_ObjectPool;

/*
 * Typedef for OBfM_UserDescriptorPool
 *
 * Pool for user descriptors' allocation
*/

typedef struct {
	userDescriptor usrDesc[MAXNUMOFUSERDESCRIPTOR];
	userDescriptor *firstFree; /* first free ROD */
	userDescriptor *contiguousFree; /* contiguous free ROD after this, all rods are free*/
}OBfM_UserDescriptorPool;

#define OBFM_MALLOC(m) malloc(m);

/* Error Handling */
#define OBFM_ERROR(e) \
do { \
printf("Error Code %ld: %s\n", e, OBfM_Err(e)); \
printf("File:%s Line:%ld\n", __FILE__, __LINE__); \
if (1) return(e);  \
} while(0)


/* global extern variables */

extern OBfM_ObjectPool *obfm_objectPool; /* for object and value allocation */
extern char *startAddrOfOBfMObjectPool; /* start address of mm_objectPool */
extern char *endAddrOfOBfMObjectPool; /* end address of mm_objectPool */

#endif /* __OBFM_INTERNAL_H_ */
