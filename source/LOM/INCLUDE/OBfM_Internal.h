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
#include "ODYS_common.h"
#include "OBfM_Err.h"


/* Define STATEMEMT */
#define MAXDATASIZE_OBFM	4096
#define MAXOBJECTPOOLSIZE 1024*256 /* 256k byes */
#define MAXNUMOFUSERDESCRIPTOR 1024*4 /* 4k RODs */
#define ALIGNMENTLENGTH 8
#define LENGTH_FIELD	4
#define TOTALNUMCATALOGS 8
#define OBFM_VALUE 0
#define OBFM_OBJECT 1
#define PATHDOT "."
#define MINCOUNT_TTREE 1
#define MAXCOUNT_TTREE 10
#define OBFM_MAXRELATIONSHIPNAME 60


/* 50 % of MAXDATAPARTITIONSIZE*/
#define COMPACTTONED_FREE_DATA_SIZE (MAXOBJECTPOOLSIZE)/2 
#define ISHALFSIZEFREE(p) ((p)->totalFreeSize) >= (MAXOBJECTPOOLSIZE)/2

/*
 * Typedef for obfm_Set
 *
 * This is primary structure used for set interface for set attribute
 *
 */

typedef struct {
	Four setScanID;
	unsigned isNew : 1;
	unsigned isFixedSize : 1;
	unsigned elemSize : 24;
	char *data;
}obfm_Set;

typedef enum obfm_type {
	OBFM_TYPE_NULL = -1,
	OBFM_TYPE_VOID = -1,
	OBFM_TYPE_SHORT = SM_SHORT,
	OBFM_TYPE_INTEGER = SM_INT,
	OBFM_TYPE_LONG = SM_LONG,
	OBFM_TYPE_FLOAT = SM_FLOAT,
	OBFM_TYPE_DOUBLE = SM_DOUBLE,
	OBFM_TYPE_STRING = SM_STRING,
	OBFM_TYPE_VARSTRING = SM_VARSTRING,
	OBFM_TYPE_OID = SM_OID,
	OBFM_TYPE_SET = 15,
	OBFM_TYPE_POINTER = 16              /* method arguments only */
} obfm_Type;

/*
 * Typedef for domain_info
 *
*/

typedef struct {
	unsigned char is_null;  /* check if the coresponding attribute is null */
	obfm_Type	 type;
	Four domain_id;
}obfm_DomainInfo;


typedef union {
	Two     s; 
	int     i;
	Four    l;
	float   f;
	double  d;
	userDescriptor *ud; /* pointer to userDescriptor */
	void   *ptr;        /* pointer to data */
	obfm_Set *set;

	/* INTERNAL USE ONLY */
	objectDescriptor *od;	/* pointer to objectDescriptor */ 
	OID     oid;        /* INTERNAL USE ONLY */
	/* large data manipulation */
	/*
	obfm_Stream   *ld;   
	*/
}obfm_Data;

typedef struct {
	obfm_DomainInfo   domainInfo;
	obfm_Data         data;
}obfm_Value;

/*
 * Typedef for OBfM_MemoryHeader
 *
 *
*/
typedef struct {
	Four size;
}obfm_MemoryHeader;


/*
 * Typedef for OBfM_ObjectPool
 *
 * Pool for objects and their values' allocation
*/

typedef struct {
	char data[MAXOBJECTPOOLSIZE];
	char *freeAdr; /* contiguous free addree of this partition */
	Four totalFreeSize; /* total free bytes */
	Four contiguousFreeSize; /* contiguous free bytes */
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

/*
 * Typedef for obfm_SysIndexesOverlay
 *
 * catalog information on lrds sysIndexes
*/

typedef struct {
	IndexID iid;
	union {
		KeyDesc btree;
		MLGF_KeyDesc mlgf;
	}kdesc;
	Two colNo[8];
	One		indexType;	
}obfm_SysIndexesOverlay;

/*
 * Typedef for obfm_SysInheritanceOverlay
 *
 * catalog information on lom sysInheritance
*/

typedef struct {
	Four    superclassId;       /* superclass id */
} obfm_SysInheritanceOverlay;

typedef struct {
	Four    fromClassId;       /* superclass id */
	Four	toClassId;
	Four	relationshipID;
	Two	fromAttrNum;
	Two	toAttrNum;
	One	direction;
	One	cardinality;
	char	relationshipName[OBFM_MAXRELATIONSHIPNAME];
} obfm_SysRelationShipOverlay;

/*
 * Typedef for obfm_SysAttributesOverlay
 *
 * catalog information on lom SysAttributes
*/

typedef struct obfm_SysAttributesOverlay{
	Two		complexType;		/* SM_COMPLEX_BASIC, SM_COMPLEX_SET */
	Two		type;				/* column type */
	Two		varColNo;			/* variable column Number */
	Four    offset;             /* offset of this column of a object */
	Four	length;				/* length of this column */
	Four    inheritedFrom;      /* class id which herites this column */
	Four    domain;      		/* domain id */
	char    name[MAXATTRNAME];  /* column name */
} obfm_SysAttributesOverlay;

/*
 * Typedef for obfm_SysMethodsOverlay
 *
 * catalog information on lom SysMethods
*/

typedef struct obfm_SysMethodsOverlay{
	char    dirPath[MAXDIRPATH];        /* path where this method is implemented */
	char    name[MAXMETHODNAME];            /* Method name */
	char    functionName[MAXFUNCTIONNAME];	/* Function name */
	Two     nArguments;                     /* # of arguments */
	Four    argumentsList[MAXNARGUMENT];    /* list of arguments */
	Four    inheritedFrom;					/* indicated where this method is inherited from */
	Four    returnType;                     /* return type */
} obfm_SysMethodsOverlay;

/*
 * Typedef for obfm_SysClassesOverlay
 *
 * catalog information on lom SysClasses
*/

typedef struct obfm_SysClassesOverlay{
	Two		nIndexes;			/* the number of indexes */
	Two		nCols;				/* the number of columns */
	Two		nVarCols;			/* the number of variable columns */
	Two		nSuperclasses;				/* the number of superclasses */
	Two		nMethods;				/* the number of methods */
	Four	size;				/* the size of object */
	Four    classId;            /* class id */
	Four	ocn;				/* open class number */
	obfm_SysAttributesOverlay *adesc;	/* attributes information */
	obfm_SysInheritanceOverlay *inheritanceInfo; /* inheritance information */
	obfm_SysMethodsOverlay *methodInfo; /* inheritance information */
	obfm_SysIndexesOverlay *ii;			/* index information */
	struct obfm_SysClassesOverlay *next; /* next bucket of the same hash value */
	char    name[MAXCLASSNAME]; /* class name */
} obfm_SysClassesOverlay;

/*
 * Typedef for OBFM_HashTbl
 * table for looking up the sysclasses
 */
typedef struct {
	obfm_SysClassesOverlay *next;
}OBfM_HashTbl;

#define EXTRACTCLASSID(h) (h).classId

#ifndef	_OOSQL_	
/* not to redefine OIDHASH and CLASSIDHASH macros
 * 	which are already defined in Catalog_Internal.h
 */
/*
#define OIDHASH(h) (EXTRACTCLASSID((h))) % (obfm_nClasses*2)
#define CLASSIDHASH(h) (h)<0 ? ((h) *(-1)) % (obfm_nClasses*2) : (h) % (obfm_nClasses*2)
*/
#endif	_OOSQL_

#define OBFM_MALLOC(m) malloc(m);
#define OBFM_FREE(m) free(m);

/* Error Handling */
#define OBFM_ERROR(e) \
BEGIN_MACRO \
printf("Error Code %ld: %s\n", e, OBfM_Err(e)); \
printf("File:%s Line:%ld\n", __FILE__, __LINE__); \
if (1) return(e);  \
END_MACRO

#define CATA_FREE(c)	if((c)->nIndexes > 0 ) free((c)->ii); \
						if((c)->nCols > 0) free((c)->adesc); \
						if((c)->nMethods > 0) free((c)->methodInfo); \
						if((c)->nSuperclasses > 0) free((c)->inheritanceInfo);

/* global extern variables */

extern OBfM_ObjectPool *obfm_objectPool; /* for object and value allocation */
extern char *startAddrOfOBfMObjectPool; /* start address of mm_objectPool */
extern char *endAddrOfOBfMObjectPool; /* end address of mm_objectPool */
extern OBfM_UserDescriptorPool  *obfm_userDescriptorPool;
extern char *startAddrOfOBfMUserDescriptorPool;
extern char *endAddrOfOBfMUserDescriptorPool;
extern OBfM_HashTbl *obfm_cataHashTblHead;
extern Four obfm_nClasses;


#endif /* __OBFM_INTERNAL_H_ */
