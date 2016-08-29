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

#ifndef _CATALOG_H
#define _CATALOG_H

#include "Catalog_Internal.h"
#include "LOM_Internal.h"

#ifdef  __cplusplus
extern "C" {
#endif

Four Catalog_InitLocalDS(LOM_Handle *handle);

Four Catalog_FinalLocalDS(LOM_Handle *handle);

char *Catalog_Err(LOM_Handle *handle, Four errCode);

Four Catalog_Dismount(LOM_Handle *handle, Four volId);

Four Catalog_Mount(LOM_Handle *handle, Four volId); /* volumn Id */

Four Catalog_FlushAll(LOM_Handle *handle); 
Four catalog_FlushAll(LOM_Handle *handle, Four volId); /* volumn Id */

Four Catalog_GetAttrInfo(
	LOM_Handle *handle, 
	Four volId,             /* IN : volumn ID */
	Four classId,           /* IN : class ID */
	Four attrNum,           /* IN : attribute number */
	Four *attrInfoTblIndex  /* OUT: index for attribute info.table */
);
Four Catalog_GetAttrNumIncludingDroppedCol(
	LOM_Handle *handle, 
	Four volId,             /* IN : volumn ID */
	Four classId,           /* IN : class ID */
	Four attrNum,           /* IN : attribute number */
	Four *attrNum2			/* OUT: index for attribute info.table */
);
Four Catalog_GetAttrInfoByName(
	LOM_Handle *handle, 
	Four volId,             /* IN : volumn ID */
	Four classId,           /* IN : class ID */
	char *attrName,         /* IN : attribute name */
	Four *attrInfoTblIndex  /* OUT: index for attribute info.table */
);

Four Catalog_GetClassInfo(
	LOM_Handle *handle, 
	Four volId,
	Four classId,			/* IN: class Id */
	Four *freeClassEntryIndex	/* IN: class info. number */
);

Four catalog_GetClassInfo(
	LOM_Handle *handle, 
	Four volId,             /* IN: volume id */
	Four classId,           /* IN: class Id */
	Four *freeClassEntryIndex       /* IN: class info. number */
);

Four Catalog_CreateInheritanceInfo(
	LOM_Handle *handle,
	Four volId,
	Four classId,
	Four nSuperclasss, /* IN number of superclasss */
	ClassID *superClasses /* IN superclases id */
);

Four Catalog_GetTemporaryClassId(
	LOM_Handle *handle,
	Four volId,
	char *className,
	ClassID *classId
);

Four Catalog_CreateTemporaryClass(
	LOM_Handle *handle,
	Four volId,
	char *className,
	char *indexName,
	LOM_IndexDesc *idesc,
	Four nAttrs,
	AttrInfo *ainfo,
	Four nMethods,
	MethodInfo *minfo,
	ClassID classId
);

/* index information */
Four Catalog_GetIndexInfoByAttrNum(
	LOM_Handle *handle, 
	Four volId,         /* IN : volumn ID */
	Four classId,       /* IN: class Id */
	Four attrNum,       /* IN: attribute number */
	Four *numOfReturnedIndex,   /* number Of Returned Index */
	Four *indexInfoEntryList /* OUT : corresponding index info. table entry list*/
);

Four Catalog_GetIndexInfoByAttrName(
	LOM_Handle *handle, 
	Four volId,         /* IN : volumn ID */
	Four classId,       /* IN: class Id */
	char *attrName,       /* IN: attribute Name */
	Four *numOfReturnedIndex,   /* number Of Returned Index */
	Four *indexInfoEntryList /* OUT : corresponding index info. table entry list*/
);

Four Catalog_GetIndexInfoByLOM_IndexDesc(
	LOM_Handle *handle, 
	Four volId,         /* IN : volumn ID */
	Four classId,       /* IN: class Id */
	LOM_IndexDesc   *idesc, /* IN: lom_index desc */
	Four *indexInfoEntryIndex /* OUT : corresponding index info. table entry list*/
);

Four Catalog_GetIndexInfoByIndexID(
	LOM_Handle *handle, 
	Four volId,	/* IN : volumn ID */
	Four classId,	/* IN: class Id */
	LOM_IndexID *indexId,	/* IN: index id */
	Four *indexInfoTblEntryIndex /* OUT : corresponding index info. table entry list */
);

Four catalog_ConvertKeyInfoToKeyDesc(
	LOM_Handle *handle,
	Four volId,
	Four classId,
	KeyInfo *kinfo,
	KeyDesc *kdesc
);

Four catalog_ConvertMLGF_KeyInfoToMLGF_KeyDesc(
	LOM_Handle *handle,
	Four volId,
	Four classId,
	MLGF_KeyInfo *kinfo,
	MLGF_KeyDesc *kdesc
);

Four Catalog_GetClassIdAndIndexInfoByTemporaryIndexName(
	LOM_Handle *handle,
	Four volId,             /* volumn id */
	char *indexName,
	ClassID *classId,
	Four *idxForIndexInfo
);

/* method information */
Four Catalog_GetMethodInfo(
	LOM_Handle *handle, 
	Four volId,             /* IN : volumn ID */
	Four classId,           /* IN : class ID */
	Four methodNum,         /* IN : method number */
	Four *methodInfoTblIndex        /* OUT: index for method info.table */
);

Four Catalog_RemoveClassInfo(
	LOM_Handle *handle, 
	Four volId,		/* IN : volumn ID */
	Four classId 		/* IN : Class ID */
);


Four Catalog_GetObjectSize(
	LOM_Handle *handle, 
	Four volId,         /* IN : volumn ID */
	Four classId,       /* IN: class Id */
	Four *objectSize	/* OUT: object size */
);

Four Catalog_GetOpenClassNum(
	LOM_Handle *handle, 
	Four volId,                                     /* IN: volumn ID */
	catalog_SysClassesOverlay *sysCatalog           /* IN: catalog entry */
);

Four Catalog_GetOpenClassNumByClassID(
	LOM_Handle *handle, 
	Four volId,                                     /* IN: volumn ID */
	Four classID         				  /* IN: class ID  */
);

Four Catalog_GetDomainID(
	LOM_Handle *handle, 
	Four volId, 					/* IN: volumn ID */
	char *domainName				/* IN: domain Name */
);

Four Catalog_CloseClass(
	LOM_Handle *handle, 
	Four classNum
);

Four Catalog_GetMountTableInfo(
	LOM_Handle *handle, 
	Four volId,					/* IN: volumn ID */
	Four *mountTableEntryIndex			/* OUT: mount table entry index */
);

Four catalog_getFreeEntry(
	LOM_Handle *handle, 
	Four mountTableVolumnIndex,/* IN: mount table array number about volumn ID */
	CatalogEntryType entryType,             /* IN: kind of entry */
	Four *freeEntryIndex    /* OUT: free entry */
);


Four catalog_getFreeEntries(
	LOM_Handle *handle, 
	Four mountTableVolumnIndex,/* IN: mount table array number about volumn ID */
	CatalogEntryType entryType,             /* IN: kind of entry */
	Four    numOfFreeEntries,               /* IN: the number of free entrie */
	Four    *freeEntryIndex /* OUT: free entry */
);

Four Catalog_GetVolIndex(LOM_Handle *handle, Four volId);

Four Catalog_AddIndex(
	LOM_Handle *handle, 
	Four volId, 
	Four classId, 
	char* indexName,
	LOM_IndexDesc *idesc, 
	LOM_IndexID *iid
);

Four Catalog_DropIndex(
	LOM_Handle *handle, 
	Four volId,
	Four classId,
	LOM_IndexID *iid
);

Four Catalog_RemoveClassInfo(
	LOM_Handle *handle, 
	Four volId,                     /* IN : volumn ID */
	Four classId);                   /* IN : Class ID */

/* For Relationship ... */
Four Catalog_Relationship_CreateClass(
    LOM_Handle* handle,
    Four    volId,							/* IN volume in which the relation will be placed */
    Four    classId,						/* IN class id has been created */
    Four    nSuperclasss,					/* IN number of superclasss */
    char    (*superclassList)[MAXCLASSNAME] /* IN names of superclasss */
);
Four Catalog_Relationship_DestroyClass(
    LOM_Handle* handle,
    Four    volId,              /* IN volume in which the relation will be placed */
    Four    classId             /* IN class id has been created */
);
Four Catalog_Relationship_CreateRelationship(
    LOM_Handle* handle,
    Four volId,             /* IN volume ID */
    char *relationshipName, /* IN relationship name */
    Four fromClassId,       /* IN from class ID */
    Two  fromAttrNum,       /* IN from attribute number */
    Four toClassId,         /* IN to calss ID */
    Two  toAttrNum,         /* IN to attribute number */
    One  cardinality,       /* IN cardinality */
    One  direction,         /* IN uni or bi-directional */
    Four relationshipId     /* IN relationship id */
);
Four Catalog_Relationship_DestroyRelationship(
    LOM_Handle* handle,
    Four  volId,               /* volume id */
    char* relationshipName     /* relationship name */
);
Four Catalog_GetRelationshipInfo(
    LOM_Handle* handle,
    Four                volId,
    Four                fromClassId,
    Four                toClassId,
    Four                relationshipId,
    Four                *relationshipInfoTblIndex);

Four Catalog_CreateInheritanceInfo(
	LOM_Handle *handle,
	Four volId,
	Four classId,
	Four nSuperclasss, /* IN number of superclasss */
	Four *superClasses /* IN superclases id */
);

Four Catalog_DestroyInheritanceInfo(
	LOM_Handle *handle,
	Four volId,
	Four classId,
	Four nSuperclasss, /* IN number of superclasss */
	Four *superClasses /* IN superclases id */
);

Four Catalog_Text_DefinePostingStructure(
	LOM_Handle *handle, 
	Four volId,     /* volumn id */
	Four classId,   /* class id */
	Two colNo,
	PostingStructureInfo *postingInfo
);

Four Catalog_CreateClassCatalog(
	LOM_Handle	*handle,
	Four    volId,			/* IN volume in which the relation will be placed */
	char    *className,			/* IN relation name to create */
	char	*indexName,			/* IN index name to create */
	LOM_IndexDesc *idesc,				/* IN key info for the culustering index */
	Four    nAttrs,				/* IN number of atrributes */
	AttrInfo *ainfo,			/* IN attribute information */
	Four	nSuperclasses,		/* IN number of superclasss */
	char	(*superclassList)[LOM_MAXCLASSNAME],	/* IN names of superclasss */
	Four	nMethods,			/* IN number of methods	*/
	MethodInfo	*methodinfo,	/* IN method information */
	Boolean		tmpClassFlag,		/* IN flag to indicate if this class is temporary or not */
	Four	classId); 			/* IN class id */		

Four Catalog_DestroyClassCatalog(
	LOM_Handle *handle, 
	Four volId,			/* IN volume where the relation was placed */
	char *className);		/* IN class to destroy */

Four Catalog_CreateIndexCatalog(
	LOM_Handle		*handle,
	Four			volId,			/* IN volume where the relation is placed */
	char			*className,     /* IN class name */
	char			*indexName,		/* IN index name */
	LOM_IndexDesc	*idesc,			/* IN index information */
	IndexID			*physical_iid	/* IN physical index id */
);

Four Catalog_DestroyIndexCatalog(
	LOM_Handle		*handle, 
	Four			volId,		/* IN volume where the relation was placed */
	char			*indexName	/* IN index to drop */	
);

Four Catalog_IsLRDS_RelationBasedClass(
	LOM_Handle* handle,
	Four		volId,
	Four		classId
);

Four Catalog_CreateLOMCatalogBasedOnLRDSCatalog(
	LOM_Handle*		handle,
	Four			volId, 
	char*			className,
	Four			nAttrs,
	char**			attrNames,
	Four			classId
);

#ifdef  __cplusplus
}
#endif

#endif
