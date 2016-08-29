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

/*
 * Module: Catalog_GetIndexInfo.c
 *
 * Description:
 *
 *
 *
 * Imports:
 *
 * Exports:
 * 		Four Catalog_GetIndexInfoByAttrNum(LOM_Handle *handle, Four volId,
 *		Four classId, Four attrNum, catalog_SysIndexesOverlay **node)
 *
*/

#include "LOM_Internal.h"
#include "Catalog_Internal.h"
#include "Catalog.h"


Four Catalog_GetIndexInfoByAttrNum(
	LOM_Handle *handle, 
	Four volId,         /* IN : volumn ID */
	Four classId,       /* IN: class Id */
	Four attrNum,       /* IN: attribute number */
	Four *numOfReturnedIndex,	/* INOUT : number Of Returned Index */
	Four *indexInfoTblEntryList  /* OUT : corresponding index info. table entry list */
)
{
	Four e;
	catalog_SysClassesOverlay *ptrSysclasses;
	catalog_SysIndexesOverlay *tmpIndex;
	Four numOfFoundIndex;
	Four i, v;
	Four j;
	Boolean found;

	/* Check whether the volume is already mounted. */
	for (v = 0; v < CATALOG_MAXNUMOFVOLS; v++)
		if (LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].volId == volId) {
			break;
		}
	if (v == CATALOG_MAXNUMOFVOLS) return eVOLUMNNOTMOUNTED_CATALOG;

	found = SM_FALSE;
	for(i = 0; i < CATALOG_GET_CLASSINFOTBL_SIZE(handle, v); i++) {
		if(CATALOG_GET_CLASSINFOTBL(handle, v)[i].classId == classId) {
			found = SM_TRUE;
		break;
		}
	}

	if(!found) {
		e = Catalog_GetClassInfo(handle, volId, classId, &i);
		if( e < 0) CATALOG_ERROR(handle, e);
	}


	numOfFoundIndex = 0;
	/* number of returned index */

	ptrSysclasses = &(CATALOG_GET_CLASSINFOTBL(handle, v)[i]);
	tmpIndex = &(CATALOG_GET_INDEXINFOTBL(handle, v)[CATALOG_GET_INDEXINFOTBL_INDEX(ptrSysclasses)]);

	for(i = 0, j = 0 ; i < CATALOG_GET_INDEXNUM(ptrSysclasses) && j < *numOfReturnedIndex; i++) {
		/* if the first column is equal to attrNum */
		
		if(tmpIndex[i].indexType != LOM_INDEXTYPE_MLGF)
		{
			if(tmpIndex[i].colNo[0] == GET_SYSTEMLEVEL_COLNO(attrNum)) 
			{
				numOfFoundIndex++;
				indexInfoTblEntryList[j] = i + CATALOG_GET_INDEXINFOTBL_INDEX(ptrSysclasses);
				j++;
			}
		}
		else
		{
			Four k;
            Boolean found = SM_FALSE;

            for(k = 0; k < tmpIndex[i].kdesc.mlgf.nKeys; k++)
            {
                if(tmpIndex[i].colNo[k] == GET_SYSTEMLEVEL_COLNO(attrNum))
                {
                    found = SM_TRUE;
                }
            }

            if(found == SM_TRUE)
            {
                numOfFoundIndex++;
                indexInfoTblEntryList[j] = i + CATALOG_GET_INDEXINFOTBL_INDEX(ptrSysclasses);
                j++;
            }
		}
	}
	
	*numOfReturnedIndex = numOfFoundIndex;

	return eNOERROR;
}

Four Catalog_GetIndexInfoByAttrName(
	LOM_Handle *handle, 
	Four volId,         /* IN : volumn ID */
	Four classId,       /* IN: class Id */
	char *attrName,       /* IN: attribute Name */
	Four *numOfReturnedIndex,	/* number Of Returned Index */
	Four *indexInfoTblEntryList  /* OUT : corresponding index info. table entry list */
)
{
	Four e;
	catalog_SysClassesOverlay *ptrSysclasses;
	catalog_SysIndexesOverlay *tmpIndex;
	Four attrInfoTblIndex;
	Four i, v;
	Four j;

	/* Check whether the volume is already mounted. */
	for (v = 0; v < CATALOG_MAXNUMOFVOLS; v++)
		if (LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].volId == volId) {
			break;
		}
	if (v == CATALOG_MAXNUMOFVOLS) return eVOLUMNNOTMOUNTED_CATALOG;

	e = Catalog_GetAttrInfoByName(handle, volId, classId, attrName, &attrInfoTblIndex);
	if(e < 0) CATALOG_ERROR(handle, e);

	/* get attribute info by attribute number */
	e = Catalog_GetIndexInfoByAttrNum(handle, volId, classId, CATALOG_GET_ATTRINFOTBL(handle, v)[attrInfoTblIndex].colNo, numOfReturnedIndex, indexInfoTblEntryList);
	if(e < 0) CATALOG_ERROR(handle, e);
	
	return eNOERROR;
}

Four Catalog_GetIndexInfoByLOM_IndexDesc(
	LOM_Handle *handle, 
	Four volId,         /* IN : volumn ID */
	Four classId,       /* IN: class Id */
	LOM_IndexDesc	*idesc,	/* IN: lom_index desc */
	Four *indexInfoEntryIndex /* OUT : found index */
)
{
	Four e;
	catalog_SysClassesOverlay *ptrSysclasses;
	catalog_SysIndexesOverlay *tmpIndex;
	Four i;
	Four j;
	Four v;
	Boolean found;

	/* Check whether the volume is already mounted. */
	for (v = 0; v < CATALOG_MAXNUMOFVOLS; v++)
		if (LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].volId == volId) {
			break;
		}
	if (v == CATALOG_MAXNUMOFVOLS) return eVOLUMNNOTMOUNTED_CATALOG;

	found = SM_FALSE;
	for(i = 0; i < CATALOG_GET_CLASSINFOTBL_SIZE(handle, v); i++) {
		if(CATALOG_GET_CLASSINFOTBL(handle, v)[i].classId == classId) {
			found = SM_TRUE;
		break;
		}
	}

	if(!found) {
		e = Catalog_GetClassInfo(handle, volId, classId, &i);
		if( e < 0) CATALOG_ERROR(handle, e);
	}


	ptrSysclasses = &(CATALOG_GET_CLASSINFOTBL(handle, v)[i]);
	tmpIndex = &(CATALOG_GET_INDEXINFOTBL(handle, v)[CATALOG_GET_INDEXINFOTBL_INDEX(ptrSysclasses)]);

	for(i = 0; i < CATALOG_GET_INDEXNUM(ptrSysclasses); i++) {
		if(idesc->indexType == tmpIndex[i].indexType) {
			/* if index type is btree */
			if(idesc->indexType == LOM_INDEXTYPE_BTREE) {
				/* first check if the number of columns defined for index is equal to the number of columns specified in parameter */
				if(idesc->kinfo.btree.nColumns == tmpIndex[i].kdesc.btree.nparts) {
					found = SM_TRUE;
					/* compare column information */
					for(j = 0; j< idesc->kinfo.btree.nColumns ; j++) {
						if(idesc->kinfo.btree.columns[j].colNo != tmpIndex[i].colNo[j]) {
							found = SM_FALSE;
							continue;
						}
					}
					if(found) {
						/* exact match found */
						*indexInfoEntryIndex = i + CATALOG_GET_INDEXINFOTBL_INDEX(ptrSysclasses);
						return eNOERROR;
					}
				}
			}
			/* mlgf index */
			else {
				
				/* first check if the number of columns defined for index is equal to the number of columns specified in parameter */
				if(idesc->kinfo.mlgf.nColumns == tmpIndex[i].kdesc.mlgf.nKeys) {
					found = SM_TRUE;
					for(j = 0; j< idesc->kinfo.mlgf.nColumns ; j++) {
						if(idesc->kinfo.mlgf.colNo[j] != tmpIndex[i].colNo[j]) {
							found = SM_FALSE;
							continue;
						}
					}
					if( found) {
						/* exact match found */
						*indexInfoEntryIndex = i + CATALOG_GET_INDEXINFOTBL_INDEX(ptrSysclasses);
						return eNOERROR;
					}
				} /* if */		
			} /* else */
		} /* if */
	} /* for */
	
	*indexInfoEntryIndex = -1;
	return eNOERROR;
}

Four Catalog_GetIndexInfoByIndexID(
	LOM_Handle *handle, 
	Four volId,         /* IN : volumn ID */
	Four classId,       /* IN: class Id */
	LOM_IndexID *indexId,       /* IN: index id */
	Four *indexInfoTblEntryIndex  /* OUT : corresponding index info. table entry list */
)
{

	Four e;
	catalog_SysClassesOverlay *ptrSysclasses;
	catalog_SysIndexesOverlay *tmpIndex;
	Four i;
	Four j;
	Four v;
	Boolean found;

	/* Check whether the volume is already mounted. */
	for (v = 0; v < CATALOG_MAXNUMOFVOLS; v++)
		if (LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].volId == volId) {
			break;
		}
	if (v == CATALOG_MAXNUMOFVOLS) return eVOLUMNNOTMOUNTED_CATALOG;

	found = SM_FALSE;
	for(i = 0; i < CATALOG_GET_CLASSINFOTBL_SIZE(handle, v); i++) {
		if(CATALOG_GET_CLASSINFOTBL(handle, v)[i].classId == classId) {
			found = SM_TRUE;
		break;
		}
	}

	if(!found) {
		e = Catalog_GetClassInfo(handle, volId, classId, &i);
		if( e < 0) CATALOG_ERROR(handle, e);
	}

	ptrSysclasses = &(CATALOG_GET_CLASSINFOTBL(handle, v)[i]);
	tmpIndex = &(CATALOG_GET_INDEXINFOTBL(handle, v)[CATALOG_GET_INDEXINFOTBL_INDEX(ptrSysclasses)]);

	for(i = 0; i < CATALOG_GET_INDEXNUM(ptrSysclasses); i++) {
		if(indexId->isLogical == SM_TRUE) {
			if((indexId->isLogical == tmpIndex[i].iid.isLogical) &&
			(indexId->index.logical_iid.pageNo == tmpIndex[i].iid.index.logical_iid.pageNo) &&
			(indexId->index.logical_iid.volNo == tmpIndex[i].iid.index.logical_iid.volNo) &&
			(indexId->index.logical_iid.slotNo == tmpIndex[i].iid.index.logical_iid.slotNo) &&
			(indexId->index.logical_iid.unique == tmpIndex[i].iid.index.logical_iid.unique)) {
				*indexInfoTblEntryIndex = (Four)(i + CATALOG_GET_INDEXINFOTBL_INDEX(ptrSysclasses));
				return eNOERROR;
			}
		}
		else {
			if((indexId->isLogical == tmpIndex[i].iid.isLogical) &&
				memcmp(&indexId->index.physical_iid, &tmpIndex[i].iid.index.physical_iid, 
				       sizeof(tmpIndex[i].iid.index.physical_iid)) == 0) {
					*indexInfoTblEntryIndex = i + CATALOG_GET_INDEXINFOTBL_INDEX(ptrSysclasses);
					return eNOERROR;
			}
		}
	}
	*indexInfoTblEntryIndex = -1;
	return eNOERROR;
}

Four Catalog_GetClassIdAndIndexInfoByTemporaryIndexName(
	LOM_Handle *handle, 
	Four volId,		/* volumn id */
	char *indexName,
	ClassID *classId,
	Four *idxForIndexInfo
)
{
	Four v;
	Four e;
	Four i,j;
	catalog_SysClassesOverlay *tmp_sysClasses;
	catalog_SysIndexesOverlay *tmp_ii;
	Boolean found;

	found = SM_FALSE;
	v = Catalog_GetVolIndex(handle, volId);
	if(v < eNOERROR) CATALOG_ERROR(handle, v);

	for (i = 0; i < CATALOG_GET_CLASSINFOTBL_SIZE(handle, v); i++) {
		if(CATALOG_GET_CLASSINFOTBL(handle, v)[i].inUse == SM_TRUE)
		/* check if this class is temporary or not */
		if(CATALOG_GET_CLASSINFOTBL(handle, v)[i].classId < 0) {
			tmp_sysClasses = &(CATALOG_GET_CLASSINFOTBL(handle, v)[i]);
			tmp_ii = &(CATALOG_GET_INDEXINFOTBL(handle, v)[CATALOG_GET_INDEXINFOTBL_INDEX(tmp_sysClasses)]);
			for(j = 0; j < CATALOG_GET_INDEXNUM(tmp_sysClasses); j++) {
				if(!strcmp(tmp_ii[j].name, indexName)) {
					found = SM_TRUE;
					*classId = CATALOG_GET_CLASSINFOTBL(handle, v)[i].classId;
					*idxForIndexInfo = CATALOG_GET_INDEXINFOTBL_INDEX(tmp_sysClasses) + j;
					break;
				}
			}
		}
	}

	if(!found) 
		return eNOSUCHINDEXFOUND_CATALOG;

	return eNOERROR;
}

Four Catalog_AddIndex(
	LOM_Handle *handle, 
	Four volId,				/* volumn id */
	Four classId,			/* class id */
	char* indexName,		/* index name */
	LOM_IndexDesc *idesc,	/* index desc. */
	LOM_IndexID *iid		/* index id */
)
{

	Four classInfoTblEntryIndex = -1;
	Four indexInfoTblEntryIndex = -1;
	Four newIndexInfoTblEntryIndex = -1;
	Four e;
	Four v;
	Four i;
	catalog_SysClassesOverlay *ptrSysclasses;
	catalog_SysIndexesOverlay *ptrSysindexes;
	catalog_SysIndexesOverlay *addedIndex;
	Four orn;
	lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */

	/* user level column no -> system level */
	if(idesc->indexType == SM_INDEXTYPE_BTREE)
		for(i = 0; i< idesc->kinfo.btree.nColumns; i++) {
			idesc->kinfo.btree.columns[i].colNo++;
		}
	else 
		for(i = 0; i< idesc->kinfo.mlgf.nColumns; i++) {
			idesc->kinfo.mlgf.colNo[i]++;
		}

	/* get class info. */
	e = catalog_GetClassInfo(handle, volId, classId, &classInfoTblEntryIndex);
	if(e < eNOERROR) CATALOG_ERROR(handle, e);
	if(classInfoTblEntryIndex == -1) return eNOERROR;

	/* get index info. by the given index id */
	e = Catalog_GetIndexInfoByIndexID(handle, 
		volId, 
		classId, 
		iid, 
		&indexInfoTblEntryIndex);
	if(e < eNOERROR) CATALOG_ERROR(handle, e);
	if(indexInfoTblEntryIndex < 0) {

		v = Catalog_GetVolIndex(handle, volId);
		if(v < eNOERROR) CATALOG_ERROR(handle, v);

		ptrSysclasses = &(CATALOG_GET_CLASSINFOTBL(handle, v)[classInfoTblEntryIndex]);
		ptrSysindexes = &(CATALOG_GET_INDEXINFOTBL(handle, v)[ptrSysclasses->indexInfoIndex]);

		/* get free entries */
		e = catalog_getFreeEntries(handle, 
			v,
			CATALOG_INDEXINFOTBL,
			CATALOG_GET_INDEXNUM(ptrSysclasses) + 1,
			&newIndexInfoTblEntryIndex);
		if(e < eNOERROR) CATALOG_ERROR(handle, e);

		/* copy old info.s into new allocated entries */
		for(i = 0; i < CATALOG_GET_INDEXNUM(ptrSysclasses); i++) {
			CATALOG_GET_INDEXINFOTBL(handle, v)[CATALOG_GET_INDEXINFOTBL_INDEX(ptrSysclasses) + i].inUse = 0;
			bcopy(&(CATALOG_GET_INDEXINFOTBL(handle, v)[CATALOG_GET_INDEXINFOTBL_INDEX(ptrSysclasses) + i]),
				&(CATALOG_GET_INDEXINFOTBL(handle, v)[newIndexInfoTblEntryIndex + i]),
				sizeof(catalog_SysIndexesOverlay));
		}

		/* newly added index information */
		addedIndex = &CATALOG_GET_INDEXINFOTBL(handle, v)[newIndexInfoTblEntryIndex + CATALOG_GET_INDEXNUM(ptrSysclasses)];
		/* index id */
		addedIndex->iid.index.physical_iid = iid->index.physical_iid;
	
		/* is Logical field */
		addedIndex->iid.isLogical = SM_FALSE;
	
		/* key description */
		if(idesc->indexType == LOM_INDEXTYPE_BTREE) {
			e = catalog_ConvertKeyInfoToKeyDesc(
				handle, 
				volId, 
				classId, 
				&(idesc->kinfo.btree),
				&(addedIndex->kdesc.btree));
		}
		else {
			e = catalog_ConvertMLGF_KeyInfoToMLGF_KeyDesc(
				handle, 
				volId, 
				classId, 
				&(idesc->kinfo.mlgf),
				&(addedIndex->kdesc.mlgf));
		}
		if(e < eNOERROR) CATALOG_ERROR(handle, e);

		/* colNo[MAXNUMKEYPARTS] */
		if(idesc->indexType == SM_INDEXTYPE_BTREE)
			for(i = 0; i< idesc->kinfo.btree.nColumns; i++) {
				addedIndex->colNo[i] = idesc->kinfo.btree.columns[i].colNo;
			}
		else 
			for(i = 0; i< idesc->kinfo.mlgf.nColumns; i++) {
				addedIndex->colNo[i] = idesc->kinfo.mlgf.colNo[i]--;
			}

		/* indexType */
		addedIndex->indexType = idesc->indexType;

		/* name */
		strncpy(addedIndex->name, indexName, LOM_MAXINDEXNAME - 1);

		CATALOG_GET_INDEXNUM(ptrSysclasses)++;
	
		CATALOG_GET_INDEXINFOTBL_INDEX(ptrSysclasses) = newIndexInfoTblEntryIndex;
	}

	/* system level column no -> user level */
	if(idesc->indexType == SM_INDEXTYPE_BTREE)
		for(i = 0; i< idesc->kinfo.btree.nColumns; i++) {
			idesc->kinfo.btree.columns[i].colNo--;
		}
	else 
		for(i = 0; i< idesc->kinfo.mlgf.nColumns; i++) {
			idesc->kinfo.mlgf.colNo[i]--;
		}
	
	return eNOERROR;
}

Four catalog_ConvertKeyInfoToKeyDesc(
	LOM_Handle *handle,
	Four volId,
	Four classId,
	KeyInfo *kinfo,
	KeyDesc *kdesc
)
{
	Four e;
	Four v;
	Four classEntryIndex;
	catalog_SysClassesOverlay *ptrSysClass;
	catalog_SysAttributesOverlay *ptrSysAttribute;
	Four i;

	v = Catalog_GetVolIndex(handle, volId);
	if(v < eNOERROR) CATALOG_ERROR(handle, v);

	e = Catalog_GetClassInfo(handle, volId, classId, &classEntryIndex);
	if(e < eNOERROR) CATALOG_ERROR(handle, e);

	ptrSysClass = &(CATALOG_GET_CLASSINFOTBL(handle, v)[classEntryIndex]);

	ptrSysAttribute = &(CATALOG_GET_ATTRINFOTBL(handle, v)[CATALOG_GET_ATTRINFOTBL_INDEX(ptrSysClass)]);

	kdesc->flag = kinfo->flag;
	kdesc->nparts = kinfo->nColumns;

	if (ptrSysAttribute[kinfo->columns[0].colNo].type == SM_TEXT) {
		kdesc->kpart[0].type = SM_VARSTRING;
		kdesc->kpart[0].length = sizeof(Two) + MAXKEYWORDLEN;
	}
	else {
		for (i = 0; i < kinfo->nColumns; i++) {             
			kdesc->kpart[i].type = ptrSysAttribute[kinfo->columns[i].colNo].type;
			kdesc->kpart[i].length = ptrSysAttribute[kinfo->columns[i].colNo].length;
		}
	}

	return eNOERROR;

}

//old#define MLGF_KEYDESC_SET_MINTYPE(kdesc, i) \
//	((kdesc).minMaxTypeVector |= (unsigned)0x80000000 >> (i))

//old#define MLGF_KEYDESC_SET_MAXTYPE(kdesc, i) \
//	((kdesc).minMaxTypeVector &= ~((unsigned)0x80000000 >> (i)))

#define MLGF_KEYDESC_SET_MINTYPE(kdesc, i) \
((kdesc).minMaxTypeVector |= (((MLGF_HashValue)1)<<(sizeof(MLGF_HashValue)*CHAR_BIT-1)) >> (i))

#define MLGF_KEYDESC_SET_MAXTYPE(kdesc, i) \
((kdesc).minMaxTypeVector &= ~((((MLGF_HashValue)1)<<(sizeof(MLGF_HashValue)*CHAR_BIT-1)) >> (i)))

#define MLGF_KEYDESC_CLEAR_MINMAXTYPEVECTOR(kdesc) \
	((kdesc).minMaxTypeVector = 0)

Four catalog_ConvertMLGF_KeyInfoToMLGF_KeyDesc(
	LOM_Handle *handle,
	Four volId,
	Four classId,
	MLGF_KeyInfo *kinfo,
	MLGF_KeyDesc *kdesc
)
{
	Four e;
	Four v;
	Four k;
	Four classEntryIndex;
	catalog_SysClassesOverlay *ptrSysClass;
	catalog_SysAttributesOverlay *ptrSysAttribute;

	v = Catalog_GetVolIndex(handle, volId);
	if(v < eNOERROR) CATALOG_ERROR(handle, v);

	e = Catalog_GetClassInfo(handle, volId, classId, &classEntryIndex);
	if(e < eNOERROR) CATALOG_ERROR(handle, e);

	ptrSysClass = &(CATALOG_GET_CLASSINFOTBL(handle, v)[classEntryIndex]);

	ptrSysAttribute = &(CATALOG_GET_ATTRINFOTBL(handle, v)[CATALOG_GET_ATTRINFOTBL_INDEX(ptrSysClass)]);

	kdesc->flag = kinfo->flag;
	kdesc->extraDataLen = kinfo->extraDataLen;

	if( ptrSysAttribute[kinfo->colNo[0]].type == LOM_MBR ||
		ptrSysAttribute[kinfo->colNo[0]].type == LOM_OGIS_GEOMETRY ||
		ptrSysAttribute[kinfo->colNo[0]].type == LOM_OGIS_POINT ||
		ptrSysAttribute[kinfo->colNo[0]].type == LOM_OGIS_LINESTRING ||
		ptrSysAttribute[kinfo->colNo[0]].type == LOM_OGIS_POLYGON ||
		ptrSysAttribute[kinfo->colNo[0]].type == LOM_OGIS_GEOMETRYCOLLECTION ||
		ptrSysAttribute[kinfo->colNo[0]].type == LOM_OGIS_MULTIPOINT ||
		ptrSysAttribute[kinfo->colNo[0]].type == LOM_OGIS_MULTILINESTRING ||
		ptrSysAttribute[kinfo->colNo[0]].type == LOM_OGIS_MULTIPOLYGON)
    {
        kdesc->nKeys = MBR_NUM_PARTS;
	    for (k = 0; k < kdesc->nKeys/2; k++)
		{
            MLGF_KEYDESC_SET_MINTYPE(*kdesc, k);
		}
        for ( ; k < kdesc->nKeys; k++)
		{
            MLGF_KEYDESC_SET_MAXTYPE(*kdesc, k);
		}
	}
    else
    {
        kdesc->nKeys = kinfo->nColumns;
		MLGF_KEYDESC_CLEAR_MINMAXTYPEVECTOR(*kdesc);
   	} 
	return eNOERROR;
}

Four Catalog_DropIndex(
	LOM_Handle *handle, 
	Four volId,		/* volumn id */
	Four classId,	/* class id */
	LOM_IndexID *iid		/* index id */
)
{

	Four classInfoTblEntryIndex = -1;
	Four indexInfoTblEntryIndex = -1;
	Four newIndexInfoTblEntryIndex = -1;
	Four e;
	Four v;
	catalog_SysClassesOverlay *ptrSysclasses;
	catalog_SysIndexesOverlay *ptrSysindexes;
	Four i;
	Four j;
	Four nOldIndexes;
	Four oldIndexInfoTblEntryIndex;

	/* get class info. */
	e = catalog_GetClassInfo(handle, volId, classId, &classInfoTblEntryIndex);
	if(e < eNOERROR) CATALOG_ERROR(handle, e);
	if(classInfoTblEntryIndex == -1) return eNOERROR;

	/* get index info. by the given index id */
	e = Catalog_GetIndexInfoByIndexID(
		handle, 
		volId, 
		classId, 
		iid, 
		&indexInfoTblEntryIndex);
	if(e < eNOERROR) CATALOG_ERROR(handle, e);
	if(indexInfoTblEntryIndex < 0) CATALOG_ERROR(handle, eINTERNAL_CATALOG);

	v = Catalog_GetVolIndex(handle, volId);
	if(v < eNOERROR) CATALOG_ERROR(handle, v);

	ptrSysclasses = &(CATALOG_GET_CLASSINFOTBL(handle, v)[classInfoTblEntryIndex]);
	ptrSysindexes = &(CATALOG_GET_INDEXINFOTBL(handle, v)[indexInfoTblEntryIndex]);

	nOldIndexes = CATALOG_GET_INDEXNUM(ptrSysclasses);
	oldIndexInfoTblEntryIndex = CATALOG_GET_INDEXINFOTBL_INDEX(ptrSysclasses);

	/* get free entries */
	e = catalog_getFreeEntries(
		handle, 
		v,
		CATALOG_INDEXINFOTBL,
		nOldIndexes-1,
		&newIndexInfoTblEntryIndex);
	if(e < eNOERROR) CATALOG_ERROR(handle, e);

	/* copy old info.s into new allocated entries */
	for(i = 0, j = 0; i < nOldIndexes;	i++) 
	{
		if(oldIndexInfoTblEntryIndex + i != indexInfoTblEntryIndex)
		{
			bcopy(&(CATALOG_GET_INDEXINFOTBL(handle, v)[oldIndexInfoTblEntryIndex+i]),
				  &(CATALOG_GET_INDEXINFOTBL(handle, v)[newIndexInfoTblEntryIndex+j]),
				  sizeof(catalog_SysIndexesOverlay));
			j++;
		}
	}

	/* reset */
	CATALOG_GET_INDEXINFOTBL_INDEX(ptrSysclasses) = newIndexInfoTblEntryIndex;
	for(i = 0; i < nOldIndexes;	i++)
	{
		CATALOG_GET_INDEXINFOTBL(handle, v)[oldIndexInfoTblEntryIndex+i].inUse = 0;
	}
	
	/* decrement the number of index */
	CATALOG_GET_INDEXNUM(ptrSysclasses)--;

	return eNOERROR;
}


