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

#ifndef __CATALOG_INTERNAL_H_
#define __CATALOG_INTERNAL_H_

#ifdef  __cplusplus
extern "C" {
#endif

#ifndef EXTERN 
#define EXTERN extern
#endif

#include <stdio.h>
#include <malloc.h>
#include "ODYS_common.h"
#include "Catalog_Err.h"

#ifndef TOTALNUMLOMCATALOGS
#define TOTALNUMLOMCATALOGS 8
#endif

#define MAXCLASSINFOTBLESIZE		60
#define MAXATTRINFOTBLESIZE		180
#define MAXMETHODINFOTBLESIZE		180
#define MAXINDEXINFOTBLESIZE		180
#define MAXSUPERCLASSINFOTBLESIZE	180
#define MAXSUBCLASSINFOTBLESIZE		180
#define MAXRELATIONSHIPINFOTBLSIZE      60 /* For Relationship */

typedef enum { CATALOG_CLASSINFOTBL, 
	   CATALOG_ATTRINFOTBL, 
	   CATALOG_METHODINFOTBL,
	   CATALOG_INDEXINFOTBL, 
	   CATALOG_SUPERCLASSINFOTBL, 
	   CATALOG_SUBCLASSINFOTBL,
	   CATALOG_RELATIONSHIPINFOTBL /* For Relationship */
} CatalogEntryType;

/*
 * Typedef for catalog_SysSuperClassesOverlay
 *
*/
typedef struct {
	Four superClassId;
	One inUse;
}catalog_SysSuperClassesOverlay;

/*
 * Typedef for catalog_SysSubClassesOverlay
 *
*/
typedef struct {
	Four subClassId;
	One inUse;
}catalog_SysSubClassesOverlay;

typedef struct {
	Boolean isContainingTupleID;
	Boolean isContainingSentenceAndWordNum;
	Boolean isContainingByteOffset;
	Two nEmbeddedAttributes;
	Two embeddedAttrNo[MAX_NUM_EMBEDDEDATTRIBUTES];
	Two embeddedAttrOffset[MAX_NUM_EMBEDDEDATTRIBUTES];
	Two embeddedAttrVarColNo[MAX_NUM_EMBEDDEDATTRIBUTES];
}InMemory_PostingStructureInfo;

/*
 * Typedef for InvertedIndexDesc invertedIndex
 *
 */
typedef struct {
	IndexID keywordIndex;	/* btree index on keyword attribute of inverted index table */
	IndexID	reverseKeywordIndex; /* SM_TEXT index on reverse-keyword attribtue of inverted index table */
	IndexID docIdIndex;	/* SM_TEXT index on document id of document-id index table */
	char invertedIndexName[LOM_MAXINDEXNAME];
	char docIdIndexTableName[LOM_MAXINDEXNAME];
	InMemory_PostingStructureInfo postingInfo;
}InvertedIndexDesc;
	
/*
 * Typedef for catalog_SysIndexesOverlay
 *
 * catalog information on lrds sysIndexes
*/

typedef struct {
	LOM_IndexID iid;
	union {
		KeyDesc btree;
		MLGF_KeyDesc mlgf;
		InvertedIndexDesc invertedIndex;
	}kdesc;
	Two colNo[MAXNUMKEYPARTS];
	One	indexType;	
	char 	inUse;		/* validity */
	char	name[LOM_MAXINDEXNAME];
}catalog_SysIndexesOverlay;

/*
 * Typedef for catalog_SysAttributesOverlay
 *
 * catalog information on lom SysAttributes
*/

typedef struct catalog_SysAttributesOverlay{
	char 	inUse;			/* validity */
	Two	colNo;		/* column number */
	Two	complexType;	/* SM_COMPLEX_BASIC, SM_COMPLEX_SET */
	Two	type;		/* column type */
	Two	varColNo;	/* variable column Number */
	Four    offset;		/* offset of this column of a object */
	Four	length;		/* length of this column */
	Four    inheritedFrom;	/* class id which herites this column */
	Four    domain;		/* domain id */
	char    name[MAXATTRNAME];  /* column name */
	Two		activeColNo;		/* active column number except deleted columns */
} catalog_SysAttributesOverlay;

/*
 * Typedef for catalog_SysMethodsOverlay
 *
 * catalog information on lom SysMethods
*/

typedef struct catalog_SysMethodsOverlay{
	char    dirPath[MAXDIRPATH];/* path where this method is implemented */
	char    name[MAXMETHODNAME];	/* Method name */
	char    functionName[MAXFUNCTIONNAME];	/* Function name */
	Two     nArguments;                     /* # of arguments */
	Four    argumentsList[MAXNARGUMENT];    /* list of arguments */
	Four    inheritedFrom;	/* indicated where this method is inherited from */
	Four    returnType;		/* return type */
	void	*fhandle;		/* handle for dynamic linking */
	void	(*ptrToFunction)();	/* ptr to function */
	char inUse;			/* validity */
} catalog_SysMethodsOverlay;

/*
 * Typedef for catalog_SysClassesOverlay
 *
 * catalog information on lom SysClasses
*/

typedef struct catalog_SysClassesOverlay{
	Two	nIndexes;	/* the number of indexes */
	Two	nCols;		/* the number of columns */
	Two	nVarCols;	/* the number of variable columns */
	Two	nSuperclasses;	/* the number of superclasses */
	Two	nSubclasses;	/* the number of subclasses */
	Two	nMethods;	/* the number of methods */
	Two 	nRelationships; /* For Relationship, the number of relationships */
	Two	nActiveCols;	/* the number of columns except deleted columns */
	Four	objectSize;	/* the size of object */
	Four    classId;	/* class id */
	Four	numOfOpen;	/* number of Open this class */
	Four	attrInfoIndex;	/* attributes information */
	Four 	superClassInfoIndex; /* super class information */
	Four 	subClassInfoIndex; /* subclass information */
	Four 	methodInfoIndex; /* method information */
	Four 	indexInfoIndex;	/* index information */
	Four	relationshipInfoIndex; /* For Relationship, relationship information */
	char    name[MAXCLASSNAME]; /* class name */
	char	inUse;	/* validity */
} catalog_SysClassesOverlay;

/* class, attribute, method , index, and inheritance information table */
#define CATALOG_GET_CLASSINFOTBL_SIZE(handle, v) ((lom_GDSInstance*)(lomGDSInstanceTable.ptr))[handle->instanceId].catalogMountTable[v].classInfoTbl.nEntries
#define CATALOG_GET_CLASSINFOTBL(handle, v)	((catalog_SysClassesOverlay *)(((lom_GDSInstance*)(lomGDSInstanceTable.ptr))[handle->instanceId].catalogMountTable[v].classInfoTbl.ptr))

#define CATALOG_GET_ATTRINFOTBL_SIZE(handle, v)	((lom_GDSInstance*)(lomGDSInstanceTable.ptr))[handle->instanceId].catalogMountTable[v].attrInfoTbl.nEntries
#define CATALOG_GET_ATTRINFOTBL(handle, v)	((catalog_SysAttributesOverlay *)(((lom_GDSInstance*)(lomGDSInstanceTable.ptr))[handle->instanceId].catalogMountTable[v].attrInfoTbl.ptr))
#define CATALOG_GET_ATTRINFOTBL_INDEX(pClass)	(pClass)->attrInfoIndex

#define CATALOG_GET_METHODINFOTBL_SIZE(handle, v)	((lom_GDSInstance*)(lomGDSInstanceTable.ptr))[handle->instanceId].catalogMountTable[v].methodInfoTbl.nEntries
#define CATALOG_GET_METHODINFOTBL(handle, v)	((catalog_SysMethodsOverlay *)(((lom_GDSInstance*)(lomGDSInstanceTable.ptr))[handle->instanceId].catalogMountTable[v].methodInfoTbl.ptr))
#define CATALOG_GET_METHODINFOTBL_INDEX(pClass)	(pClass)->methodInfoIndex
#define CATALOG_GET_INDEXINFOTBL_SIZE(handle, v)	((lom_GDSInstance*)(lomGDSInstanceTable.ptr))[handle->instanceId].catalogMountTable[v].indexInfoTbl.nEntries
#define CATALOG_GET_INDEXINFOTBL(handle, v)	((catalog_SysIndexesOverlay *)(((lom_GDSInstance*)(lomGDSInstanceTable.ptr))[handle->instanceId].catalogMountTable[v].indexInfoTbl.ptr))
#define CATALOG_GET_INDEXINFOTBL_INDEX(pClass)	(pClass)->indexInfoIndex

#define CATALOG_GET_SUPERCLASSINFOTBL_SIZE(handle, v)	((lom_GDSInstance*)(lomGDSInstanceTable.ptr))[handle->instanceId].catalogMountTable[v].superClassInfoTbl.nEntries
#define CATALOG_GET_SUPERCLASSINFOTBL(handle, v)	((catalog_SysSuperClassesOverlay *)(((lom_GDSInstance*)(lomGDSInstanceTable.ptr))[handle->instanceId].catalogMountTable[v].superClassInfoTbl.ptr))
#define CATALOG_GET_SUPERCLASSINFOTBL_INDEX(pClass)	(pClass)->superClassInfoIndex

#define CATALOG_GET_SUBCLASSINFOTBL_SIZE(handle, v)	((lom_GDSInstance*)(lomGDSInstanceTable.ptr))[handle->instanceId].catalogMountTable[v].subClassInfoTbl.nEntries
#define CATALOG_GET_SUBCLASSINFOTBL(handle, v)	((catalog_SysSubClassesOverlay *)(((lom_GDSInstance*)(lomGDSInstanceTable.ptr))[handle->instanceId].catalogMountTable[v].subClassInfoTbl.ptr))
#define CATALOG_GET_SUBCLASSINFOTBL_INDEX(pClass)	(pClass)->subClassInfoIndex

/* For Relationship ... */
#define CATALOG_GET_RELATIONSHIPINFOTBL(handle, v)    ((catalog_SysRelationshipOverlay*)(((lom_GDSInstance*)(lomGDSInstanceTable.ptr))[handle->instanceId].catalogMountTable[v].relationshipInfoTbl.ptr))
#define CATALOG_GET_RELATIONSHIPINFOTBL_SIZE(handle, v)         ((lom_GDSInstance*)(lomGDSInstanceTable.ptr))[handle->instanceId].catalogMountTable[v].relationshipInfoTbl.nEntries
#define CATALOG_GET_RELATIONSHIPINFOTBL_INDEX(pClass)   (pClass)->relationshipInfoIndex 

#define EXTRACTCLASSID(h) (h).classId
#define OIDHASH(v, h) CLASSIDHASH((v), (EXTRACTCLASSID((h))))
#define CATALOG_MALLOC(m) malloc((m));
#define CATALOG_FREE(m) free((m));
#define CATALOG_GET_INDEXNUM(sysCatalog)		(sysCatalog)->nIndexes
#define CATALOG_GET_ATTRNUM(sysCatalog)		(sysCatalog)->nCols
#define CATALOG_GET_VARATTRNUM(sysCatalog)	(sysCatalog)->nVarCols
#define CATALOG_GET_ACTIVEATTRNUM(sysCatalog)		(sysCatalog)->nActiveCols
#define CATALOG_GET_SUPERCLASSNUM(sysCatalog)		(sysCatalog)->nSuperclasses
#define CATALOG_GET_SUBCLASSNUM(sysCatalog)		(sysCatalog)->nSubclasses
#define CATALOG_GET_METHODNUM(sysCatalog)	(sysCatalog)->nMethods
#define CATALOG_GET_RELATIONSHIPNUM(sysCatalog) (sysCatalog)->nRelationships	/* For Relationship */
#define CATALOG_GET_OBJECTSIZE(sysCatalog)	(sysCatalog)->objectSize
#define CATALOG_GET_CLASSID(sysCatalog)		(sysCatalog)->classId
#define CATALOG_GET_OCN(sysCatalog)			(sysCatalog)->ocn
#define CATALOG_GET_OSN(sysCatalog)			(sysCatalog)->osn
#define CATALOG_GET_CLASSNAME(sysCatalog)	(sysCatalog)->name
#define CATALOG_GET_INDEXID(ii)				(ii)->iid
#define CATALOG_GET_NUMOFOPEN(sysCatalog)	(sysCatalog)->numOfOpen

/* macro definitions for access superClassInfoIndex and
   subClassInfoIndex */
#define CATALOG_GET_SUPERCLASSINFOINDEX(sysCatalog)	(sysCatalog)->superClassInfoIndex
#define CATALOG_GET_SUBCLASSINFOINDEX(sysCatalog)	(sysCatalog)->subClassInfoIndex

/* macro definitions for accessing (struct catalog_SysAttributesOverlay*) 
 *	c.f. Naming Rule:
 *		CATALOG_GET + ATTR + attribute_name_of_catalog_SysAttributesOverlay 
 *	     Exception:
 *		CATALOG_GET + RELATIONSHIPINFO	
 */
#define CATALOG_GET_ATTRCOLNO(sysAttr)		(sysAttr)->colNo
#define	CATALOG_GET_ATTRCOMPLEXTYPE(sysAttr)	(sysAttr)->complexType
#define	CATALOG_GET_ATTRTYPE(sysAttr)		(sysAttr)->type
#define	CATALOG_GET_ATTRVARCOLNO(sysAttr)	(sysAttr)->varColNo
#define	CATALOG_GET_ATTROFFSET(sysAttr)		(sysAttr)->offset
#define	CATALOG_GET_ATTRLENGTH(sysAttr)		(sysAttr)->length
#define	CATALOG_GET_ATTRINHERITEDFROM(sysAttr)	(sysAttr)->inheritedFrom
#define	CATALOG_GET_ATTRDOMAIN(sysAttr)		(sysAttr)->domain
#define	CATALOG_GET_ATTRNAME(sysAttr)		(sysAttr)->name
#define CATALOG_GET_ACTIVEATTRCOLNO(sysAttr)		(sysAttr)->activeColNo

/* macro definitions for accessing (struct catalog_SysIndexesOverlay*) 
 *	c.f. Naming Rule:
 *		CATALOG_GET + INDEX + attribute-name-of-catalog_SysIndexesOverlay 
 *	     Exception:
 *		CATALOG_GET + INDEX + TYPE	
 */
#define	CATALOG_GET_INDEXKEYDESC(sysIndex)	(sysIndex)->kdesc
#define	CATALOG_GET_INDEXCOLNO(sysIndex)		(sysIndex)->colNo
#define	CATALOG_GET_INDEXTYPE(sysIndex)		(sysIndex)->indexType
#define CATALOG_GET_INDEXNAME(sysIndex)         (sysIndex)->name

/*  macro definitions for accessing (struct catalog_SysMethodsOverlay*) 
 *	c.f. Naming Rule:
 *		CATALOG_GET + METHOD + attribute-name-of-catalog_SysMethodsOverlay 
 */
#define CATALOG_GET_METHODDIRPATH(sysMethod)	(sysMethod)->dirPath
#define CATALOG_GET_METHODNAME(sysMethod)	(sysMethod)->name
#define CATALOG_GET_METHODFUNCTIONNAME(sysMethod)	(sysMethod)->functionName
#define CATALOG_GET_METHODNARGUMENTS(sysMethod)	(sysMethod)->nArguments
#define CATALOG_GET_METHODARGUMENTLIST(sysMethod)	(sysMethod)->argumentsList
#define CATALOG_GET_METHODINHERITEDFROM(sysMethod)	(sysMethod)->inheritedFrom
#define CATALOG_GET_METHODRETURNTYPE(sysMethod)	(sysMethod)->returnType
#define CATALOG_GET_METHODPTRTOFUNCTION(sysMethod)	(sysMethod)->ptrToFunction
#define CATALOG_GET_METHODHANDLE(sysMethod)	(sysMethod)->fhandle

/*
 *
 */
#define CATALOG_GET_SUPERCLASSEID(sysSuperClass) (sysSuperClass)->superClassId

#define CATALOG_GET_SUBCLASSEID(sysSubClass) (sysSubClass)->subClassId

/* For Relationship ... */
/*
 * Typedef for catalog_SysRelationshipOverlay
 *
 * catalog information on lom SysRelationship
 */
typedef struct {
    Four fromClassId;       
    Four toClassId;
    Four relationshipID;
    Two	 fromAttrNum;
    Two	 toAttrNum;
    One	 direction;
    One  cardinality;
    char relationshipName[MAXRELATIONSHIPNAME];
    One  inUse;
} catalog_SysRelationshipOverlay;

#define CATALOG_GET_RELATIONSHIPFROMCLASSID(sysRelationship) (sysRelationship)->fromClassId
#define CATALOG_GET_RELATIONSHIPTOCLASSID(sysRelationship) (sysRelationship)->toClassId
#define CATALOG_GET_RELATIONSHIPFROMATTRNUM(sysRelationship) (sysRelationship)->fromAttrNum
#define CATALOG_GET_RELATIONSHIPTOATTRNUM(sysRelationship) (sysRelationship)->toAttrNum
#define CATALOG_GET_RELATIONSHIPDIRECTION(sysRelationship) (sysRelationship)->direction
#define CATALOG_GET_RELATIONSHIPCARDINALITY(sysRelationship) (sysRelationship)->cardinality
#define CATALOG_GET_RELATIONSHIPRELATIONSHIPNAME(sysRelationship) (sysRelationship)->relationshipName
#define CATALOG_GET_RELATIONSHIPRELATIONSHIPID(sysRelationship) (sysRelationship)->relationshipID

/* Error Handling */
#define CATALOG_ERROR(handle, e) \
BEGIN_MACRO \
Util_ErrorLog_Printf("Error : %ld(%s) in %s:%ld\n", (e), "Catalog", __FILE__, __LINE__); \
if (1) return(e);  \
END_MACRO

#ifdef  __cplusplus
}
#endif

#endif /* __CATALOG_INTERNAL_H_ */
