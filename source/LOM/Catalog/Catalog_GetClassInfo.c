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
 * Module: Catalog_GetClassInfo.c
 *
 * Description:
 *
 *
 *
 * Imports:
 *		Four LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), );
 *		Four LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), );
 *		Four lom_GetClassName(LOM_Handle *handle, );
 *		Four LRDS_OpenSeqScan(LOM_GET_LRDS_HANDLE(handle), );
 *		Four LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), );
 *		Four LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), );
 *		Four LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), );
 *		Four LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), );
 *
 * Exports:
 *		Four Catalog_GetClassInfo(LOM_Handle *handle, );
 *
*/

#include <stdio.h>
#include <string.h>
#include "LOM_Internal.h"
#include "LOM.h"
#include "Catalog_Internal.h"
#include "Catalog.h"

Four catalog_Text_ConvertToInMemoryPostingStructure(
	LOM_Handle *handle, 
	Four indexEntryIndex,
	catalog_SysIndexesOverlay *tmpIndex,
	catalog_SysAttributesOverlay *tmp_Adesc,
	PostingStructureInfo *postingInfo
);

Four Catalog_GetVolIndex(
	LOM_Handle *handle, 
	Four volId
)
{
	Four v;

	for (v = 0; v < CATALOG_MAXNUMOFVOLS; v++)
		if (LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].volId == volId) {
			break;
		}

	if (v == CATALOG_MAXNUMOFVOLS) return eVOLUMNNOTMOUNTED_CATALOG;

	return v;

}

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
)
{

	Four v;
	lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */
	catalog_SysClassesOverlay *tmp_sysClasses;
	catalog_SysAttributesOverlay *tmp_Adesc;
	catalog_SysIndexesOverlay *tmp_ii;
	Four indexNum;
	Four orn;
	Four freeClassEntryIndex;
	Four e;
	Four i,j;

	if(indexName == NULL) indexNum = 0;
	else indexNum = 1;

	v = Catalog_GetVolIndex(handle, volId);
	if(v < eNOERROR) CATALOG_ERROR(handle, v);

	/* get free entry for class information */
	e = catalog_getFreeEntry(handle, v, CATALOG_CLASSINFOTBL, &freeClassEntryIndex);
	if (e < 0) CATALOG_ERROR(handle, e);

	tmp_sysClasses = &(CATALOG_GET_CLASSINFOTBL(handle, v)[freeClassEntryIndex]);

	strcpy(CATALOG_GET_CLASSNAME(tmp_sysClasses), className);

	/* class id */
	CATALOG_GET_CLASSID(tmp_sysClasses) = classId;

	/* the number of attributes */
	/* including logical column */
	CATALOG_GET_ATTRNUM(tmp_sysClasses) = nAttrs + 1;

	CATALOG_GET_ACTIVEATTRNUM(tmp_sysClasses) = nAttrs + 1;

	/* the number of superclass number */
	CATALOG_GET_SUPERCLASSNUM(tmp_sysClasses) = 0;

	/* the number of subclass number */
	CATALOG_GET_SUBCLASSNUM(tmp_sysClasses) = 0;

	/* the number of method number */
	CATALOG_GET_METHODNUM(tmp_sysClasses) = 0;

	/* the number of indexes */
	CATALOG_GET_INDEXNUM(tmp_sysClasses) = indexNum;

	/* the number of relationships */
	CATALOG_GET_RELATIONSHIPNUM(tmp_sysClasses) = 0;

	if(CATALOG_GET_ATTRNUM(tmp_sysClasses) > 0) {
		/* get free entries */
		e = catalog_getFreeEntries(handle, v, CATALOG_ATTRINFOTBL, CATALOG_GET_ATTRNUM(tmp_sysClasses), &(CATALOG_GET_ATTRINFOTBL_INDEX(tmp_sysClasses)));
		if (e < 0) CATALOG_ERROR(handle, e);

		/* COLUMNS INFORMATION */
		/* temporary copy */
		tmp_Adesc = &(CATALOG_GET_ATTRINFOTBL(handle, v)[CATALOG_GET_ATTRINFOTBL_INDEX(tmp_sysClasses)]);

		/* logical column */
		tmp_Adesc[0].complexType = SM_COMPLEXTYPE_BASIC;
		tmp_Adesc[0].colNo = 0;
		tmp_Adesc[0].activeColNo = 0;
		tmp_Adesc[0].type = SM_STRING;
		tmp_Adesc[0].length = 1;
		tmp_Adesc[0].inheritedFrom = -1;
		tmp_Adesc[0].domain = SM_STRING;
		strcpy(tmp_Adesc[0].name, LOM_LOGICALID_COLNAME);

		for(j = 1; j < CATALOG_GET_ATTRNUM(tmp_sysClasses); j++) {
			/* copy into client's catalog */
			tmp_Adesc[j].complexType = ainfo[j-1].complexType;
			tmp_Adesc[j].colNo = j;
            tmp_Adesc[j].activeColNo = j;
			tmp_Adesc[j].type = ainfo[j-1].type;
			tmp_Adesc[j].length = ainfo[j-1].length;
			tmp_Adesc[j].inheritedFrom = -1;
			tmp_Adesc[j].domain = ainfo[j-1].domain;
			strcpy(tmp_Adesc[j].name, ainfo[j-1].name);
		}
	}

	/* INDEX INFORMATION */
	if(CATALOG_GET_INDEXNUM(tmp_sysClasses) > 0) {
		/* get free entries */
		e = catalog_getFreeEntries(handle, v, CATALOG_INDEXINFOTBL, CATALOG_GET_INDEXNUM(tmp_sysClasses), &(CATALOG_GET_INDEXINFOTBL_INDEX(tmp_sysClasses)));
		if (e < 0) CATALOG_ERROR(handle, e);

		orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, className);
		if( orn < 0) CATALOG_ERROR(handle, orn);

		relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);

		/* INDEX INFORMATION */
		/* temporary copy */
		tmp_ii = &(CATALOG_GET_INDEXINFOTBL(handle, v)[CATALOG_GET_INDEXINFOTBL_INDEX(tmp_sysClasses)]);

		for(j = 0; j < CATALOG_GET_INDEXNUM(tmp_sysClasses); j++) {
			/* think of index type */
			switch(idesc->indexType) {
				case LOM_INDEXTYPE_BTREE:
					bcopy(&((LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[j].kdesc.btree), &(tmp_ii[j].kdesc.btree), sizeof(KeyDesc));
					break;
				case LOM_INDEXTYPE_MLGF:
					bcopy(&((LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[j].kdesc.mlgf), &(tmp_ii[j].kdesc.mlgf), sizeof(MLGF_KeyDesc));
					break;
				default: CATALOG_ERROR(handle, eBADPARAMETER_CATALOG);
			}

			bcopy(&((LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[j].colNo), &(tmp_ii[j].colNo), sizeof((LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[j].colNo));
			tmp_ii[j].iid.index.physical_iid = (LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[j].iid;
			tmp_ii[j].iid.isLogical = SM_FALSE;
			tmp_ii[j].indexType = idesc->indexType;
			strcpy(tmp_ii[j].name, indexName);
		}/* for */
		e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
		if(e < 0) CATALOG_ERROR(handle, e);
	} /* if */

	return eNOERROR;
}

Four Catalog_GetTemporaryClassId(
	LOM_Handle *handle,
	Four volId,
	char *className,
	ClassID *classId
)
{
	Four v;
	Four e;
	Four i;
	Boolean found;

	v = Catalog_GetVolIndex(handle, volId);
	if(v < eNOERROR) CATALOG_ERROR(handle, v);

	found = SM_FALSE;
	for (i = 0; i < CATALOG_GET_CLASSINFOTBL_SIZE(handle, v); i++) {
		if(CATALOG_GET_CLASSINFOTBL(handle, v)[i].inUse == SM_TRUE)
		/* check if this class is temporary or not */
		if(CATALOG_GET_CLASSINFOTBL(handle, v)[i].classId < 0) {
			if(!strcmp(CATALOG_GET_CLASSNAME((&CATALOG_GET_CLASSINFOTBL(handle, v)[i])), className)) {
				*classId = CATALOG_GET_CLASSINFOTBL(handle, v)[i].classId;
				found = SM_TRUE;
				break;
			}
		}
	}
	if(!found) 
		return eNOSUCHCLASSFOUND_CATALOG;

	return eNOERROR;
}

Four Catalog_GetClassInfo(
	LOM_Handle *handle, 
	Four volId,
	Four classId,				/* IN: class Id */
	Four *freeClassEntryIndex			/* IN: class info. number */
) 
{
	Four ocn;
	Four ocn2;
	Four orn3;
	Four catScanId; /* catalog scan Id */
	Four catScanId2;
	char className[LOM_MAXCLASSNAME];
	catalog_SysClassesOverlay *tmp_sysClasses;
	catalog_SysAttributesOverlay *tmp_Adesc;
	catalog_SysIndexesOverlay *tmp_ii;
	catalog_SysSuperClassesOverlay *tmp_superClasses;
	catalog_SysSubClassesOverlay *tmp_subClasses;
	catalog_SysMethodsOverlay *tmp_Methods;
	BoundCond boundCond;        /* boundary condition */
	LockParameter lockup;
	ColListStruct clist[20];    /* column list */
	char colName[LOM_MAXATTRNAME];
	Four domain;
	Four nCols; /* number of columns */
	Four curSuperclassOrder;
	Four nSuperclasses; /* number of superclass */
	Four nSubclasses; /* number of superclass */
	Two keyLen;
	Two curColNo;
	Four i, j;
	Four e;
	Four v;
	lrds_RelTableEntry	*relTableEntry;
	lrds_RelTableEntry	*relTableEntry2;
	Boolean found;
	char invertedIndexName[LOM_MAXINDEXNAME];
	/* For Relationship ... */
	Four							orn4;
	catalog_SysRelationshipOverlay*	tmp_Relationships; 
	char							relationshipName[MAXRELATIONSHIPNAME]; 
	Four							nRelationships; 
	lrds_RelTableEntry*				relTableEntry4; 
	One								direction;   
    One								cardinality; 

	PostingStructureInfo postingInfo;

	/* Check whether the volume is already mounted. */
	for (v = 0; v < CATALOG_MAXNUMOFVOLS; v++)
		if (LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].volId == volId) {
			break;
		}

	if (v == CATALOG_MAXNUMOFVOLS) return eVOLUMNNOTMOUNTED_CATALOG;

	found = SM_FALSE;
	for (i = 0; i < CATALOG_GET_CLASSINFOTBL_SIZE(handle, v); i++) {
		if(CATALOG_GET_CLASSINFOTBL(handle, v)[i].inUse == SM_TRUE)
		if(CATALOG_GET_CLASSINFOTBL(handle, v)[i].classId == classId) {
			found = SM_TRUE;
			break;
		}
	}
	
	if(!found) {

		/* we get class name for this classId */
		e = lom_GetClassName(handle, volId, classId, &className[0]);
		if (e < 0) 
			CATALOG_ERROR(handle, e);


		/* we open class here */
		ocn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, &className[0]);
		if (ocn < 0) CATALOG_ERROR(handle, ocn);

		/* set lock up parameters */
		/* just for reading */
		lockup.mode = L_IX;
		lockup.duration = L_COMMIT;

		relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), ocn);

		/* get free entry for class information */
		e = catalog_getFreeEntry(handle, v, CATALOG_CLASSINFOTBL, freeClassEntryIndex);
		if (e < 0) CATALOG_ERROR(handle, e);

		tmp_sysClasses = &(CATALOG_GET_CLASSINFOTBL(handle, v)[*freeClassEntryIndex]);
		if(tmp_sysClasses==(catalog_SysClassesOverlay*)NULL) CATALOG_ERROR(handle, eSHORTOFMEMORY_CATALOG);

		/* class id */
		CATALOG_GET_CLASSID(tmp_sysClasses) = classId;

		/* catalog information */
		strcpy(CATALOG_GET_CLASSNAME(tmp_sysClasses),relTableEntry->ri.relName);
		CATALOG_GET_ATTRNUM(tmp_sysClasses) = relTableEntry->ri.nColumns;
		CATALOG_GET_VARATTRNUM(tmp_sysClasses) = relTableEntry->ri.nVarColumns;
		CATALOG_GET_SUPERCLASSNUM(tmp_sysClasses) = 0; /* initially set to 0 */
		CATALOG_GET_METHODNUM(tmp_sysClasses) = 0; /* initially set to 0 */
		CATALOG_GET_ACTIVEATTRNUM(tmp_sysClasses) = relTableEntry->ri.nColumns;

		if(CATALOG_GET_ATTRNUM(tmp_sysClasses) > 0) {

			/* get free entries */
			e = catalog_getFreeEntries(handle, v, CATALOG_ATTRINFOTBL, CATALOG_GET_ATTRNUM(tmp_sysClasses), &(CATALOG_GET_ATTRINFOTBL_INDEX(tmp_sysClasses)));
			if (e < 0) CATALOG_ERROR(handle, e);

			/* columns description */
			/* temporary copy */
			tmp_Adesc = &(CATALOG_GET_ATTRINFOTBL(handle, v)[CATALOG_GET_ATTRINFOTBL_INDEX(tmp_sysClasses)]); 

			/* open LOM_SYSCOLUMNS */
			ocn2 = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId,LOM_SYSCOLUMNS_CLASSNAME);
			if(ocn2 < 0) CATALOG_ERROR(handle, ocn2);


			relTableEntry2 = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), ocn2);

			boundCond.op = SM_EQ;
			boundCond.key.len = sizeof(Four);
			bcopy(&classId,&(boundCond.key.val[0]),sizeof(Four));

			/* set lock up parameters */
			/* just for reading */
			lockup.mode = L_IS;
			lockup.duration = L_COMMIT;

			/* index scan of LOM_SYSCOLUMNS */
			catScanId = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), ocn2,
			    &(LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry2))[0].iid,
			    &boundCond, &boundCond, 0, NULL, &lockup);
			if (catScanId < 0) CATALOG_ERROR(handle, catScanId);

			/* column information */
			for (i = 0; i < LOM_SYSCOLUMNS_NUM_COLS; i++) {
				clist[i].colNo = i;
				clist[i].start = ALL_VALUE;
				clist[i].length = LOM_MAXCLASSNAME;
				clist[i].dataLength = LOM_MAXCLASSNAME;
			}
			/* For ODMG Collection */
			clist[LOM_SYSCOLUMNS_COLOFFSET_COLNO].dataLength = sizeof(Four);

			clist[LOM_SYSCOLUMNS_COLNAME_COLNO].data.ptr = &colName[0];

			nCols = CATALOG_GET_ATTRNUM(tmp_sysClasses); /* temporary copy */
			while((e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, (TupleID*)NULL, NULL))!=EOS) {
				if (e < 0) CATALOG_ERROR(handle, e);

				e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, SM_TRUE, NULL, LOM_SYSCOLUMNS_NUM_COLS, &clist[0]);
				if (e < 0) CATALOG_ERROR(handle, e);

				/* currenct column number */
				curColNo = GET_VALUE_FROM_COL_LIST(clist[LOM_SYSCOLUMNS_COLUMNNO_COLNO], sizeof(curColNo));
				tmp_Adesc[curColNo].colNo = curColNo; /* for speeding up */
				tmp_Adesc[curColNo].offset = GET_VALUE_FROM_COL_LIST(clist[LOM_SYSCOLUMNS_COLOFFSET_COLNO], sizeof(tmp_Adesc[curColNo].offset));
				tmp_Adesc[curColNo].inheritedFrom = GET_VALUE_FROM_COL_LIST(clist[LOM_SYSCOLUMNS_INHERITEDFROM_COLNO], sizeof(tmp_Adesc[curColNo].inheritedFrom));
				bcopy(&colName[0],tmp_Adesc[curColNo].name,clist[LOM_SYSCOLUMNS_COLNAME_COLNO].retLength);
				tmp_Adesc[curColNo].name[clist[LOM_SYSCOLUMNS_COLNAME_COLNO].retLength] = '\0';
				tmp_Adesc[curColNo].type = GET_VALUE_FROM_COL_LIST(clist[LOM_SYSCOLUMNS_COLTYPE_COLNO], sizeof(tmp_Adesc[curColNo].type));
				tmp_Adesc[curColNo].varColNo = (LRDS_GET_COLDESC_FROM_RELTABLE_ENTRY(relTableEntry))[curColNo].varColNo;
				tmp_Adesc[curColNo].length = (LRDS_GET_COLDESC_FROM_RELTABLE_ENTRY(relTableEntry))[curColNo].length;
				tmp_Adesc[curColNo].domain = GET_VALUE_FROM_COL_LIST(clist[LOM_SYSCOLUMNS_DOMAINID_COLNO], sizeof(tmp_Adesc[curColNo].domain));
				tmp_Adesc[curColNo].activeColNo = GET_VALUE_FROM_COL_LIST(clist[LOM_SYSCOLUMNS_ACTIVECOLUMNNO_COLNO], sizeof(tmp_Adesc[curColNo].activeColNo));
				if (tmp_Adesc[curColNo].activeColNo == NIL)
					CATALOG_GET_ACTIVEATTRNUM(tmp_sysClasses)--;
				/* For ODMG Collection */
				tmp_Adesc[curColNo].complexType = GET_VALUE_FROM_COL_LIST(clist[LOM_SYSCOLUMNS_COMPLEXTYPE_COLNO], sizeof(tmp_Adesc[curColNo].complexType));
				if(tmp_Adesc[curColNo].domain < 0 ) CATALOG_ERROR(handle, tmp_Adesc[curColNo].domain);
				nCols--;
			}

			/* if nCols != 0 , error...*/
			if(nCols!=0) CATALOG_ERROR(handle, eINVALIDCATALOG_CATALOG);

			/* close index scan of LOM_SYSCOLUMNS */
			e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), catScanId);
			if(e < 0) CATALOG_ERROR(handle, e);

			/* close LOM_SYSCOLUMNS */
			e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), ocn2);
			if(e < 0) CATALOG_ERROR(handle, e);
		}
		else CATALOG_GET_ATTRINFOTBL_INDEX(tmp_sysClasses) = -1;

		/* the size of object */
		if(tmp_Adesc[tmp_sysClasses->nCols-1].type==SM_VARSTRING) 
			CATALOG_GET_OBJECTSIZE(tmp_sysClasses) = 
				tmp_Adesc[tmp_sysClasses->nCols-1].offset + sizeof(Four);
		/* consider if the last column is ref or link type */				
		else if(tmp_Adesc[tmp_sysClasses->nCols-1].type==ODYS_REF|| tmp_Adesc[tmp_sysClasses->nCols-1].type==ODYS_LINK)
			CATALOG_GET_OBJECTSIZE(tmp_sysClasses) = 
				tmp_Adesc[tmp_sysClasses->nCols-1].offset + sizeof(objectRef);
		else CATALOG_GET_OBJECTSIZE(tmp_sysClasses) = 
			tmp_Adesc[tmp_sysClasses->nCols-1].offset + tmp_Adesc[tmp_sysClasses->nCols-1].length;

		/* index information */
		/* we here count the number of logical index */
		ocn2 = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSINDEXES_CLASSNAME);
		if(ocn2 < eNOERROR) CATALOG_ERROR(handle, ocn2);

		relTableEntry2 = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), ocn2);

		/* make bound information */
		boundCond.op = SM_EQ;
		keyLen = strlen(className);
		bcopy(&keyLen, &(boundCond.key.val[0]), sizeof(Two));
		bcopy(className, &(boundCond.key.val[sizeof(Two)]), keyLen);

		lockup.mode = L_IS;
		lockup.duration = L_COMMIT;
			
		/* index scan of LOM_SYSINDEXES */
		catScanId = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), ocn2,
					&(LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry2))[1].iid,
					&boundCond, &boundCond, 0, NULL, &lockup);
		if (catScanId < 0) CATALOG_ERROR(handle, catScanId);

		
		/* We here count of superclass of this class id */
		CATALOG_GET_INDEXNUM(tmp_sysClasses) = 0;
		while((e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, (TupleID*)NULL, NULL))!=EOS) {
			if (e < 0) CATALOG_ERROR(handle, e);
			CATALOG_GET_INDEXNUM(tmp_sysClasses)++; 
		}

		e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), catScanId);
		if (e < 0) CATALOG_ERROR(handle, e);

		/* index id */
		clist[0].colNo = LOM_SYSINDEXES_INDEXID_COLNO;
		clist[0].start = ALL_VALUE;
		clist[0].length = sizeof(LOM_IndexID);
		clist[0].dataLength = sizeof(LOM_IndexID);

		/* index name */
		clist[1].colNo = LOM_SYSINDEXES_INDEXNAME_COLNO;
		clist[1].start = ALL_VALUE;
		clist[1].length = LOM_MAXINDEXNAME;
		clist[1].dataLength = LOM_MAXINDEXNAME;

		/* initialize local variable j */
		j = 0;

		if(CATALOG_GET_INDEXNUM(tmp_sysClasses) > 0 ) {

			catScanId = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), ocn2,
							&(LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry2))[1].iid,
							&boundCond, &boundCond, 0, NULL, &lockup);
			if (catScanId < 0) CATALOG_ERROR(handle, catScanId);
			    
			/* get free entries */
			e = catalog_getFreeEntries(handle, v, CATALOG_INDEXINFOTBL, CATALOG_GET_INDEXNUM(tmp_sysClasses), &(CATALOG_GET_INDEXINFOTBL_INDEX(tmp_sysClasses)));
			if (e < 0) CATALOG_ERROR(handle, e);

			/* temporary pointer to index information table entry */
			tmp_ii = &(CATALOG_GET_INDEXINFOTBL(handle, v)[CATALOG_GET_INDEXINFOTBL_INDEX(tmp_sysClasses)]);

			if(relTableEntry->ri.nIndexes == CATALOG_GET_INDEXNUM(tmp_sysClasses)) {
			/* it means no logical index built on this class */
				for(i = 0; i < CATALOG_GET_INDEXNUM(tmp_sysClasses); i++) {
					switch((LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[i].indexType) {
						case LOM_INDEXTYPE_BTREE :
							bcopy(&((LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[i].kdesc.btree), &(tmp_ii[i].kdesc.btree), sizeof(KeyDesc));
							break;
						case LOM_INDEXTYPE_MLGF :
							bcopy(&((LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[i].kdesc.mlgf), &(tmp_ii[i].kdesc.mlgf), sizeof(KeyDesc));
							break;
						default : CATALOG_ERROR(handle, eINTERNAL_CATALOG);
					}
					bcopy(&((LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[i].colNo), &(tmp_ii[i].colNo), sizeof((LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[i].colNo));
					tmp_ii[i].indexType = (LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[i].indexType;
					/* store index id to memory catalog */
					tmp_ii[i].iid.index.physical_iid = (LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[i].iid;
					tmp_ii[i].iid.isLogical = SM_FALSE;

						
					clist[1].data.ptr = tmp_ii[j].name;

					e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, (TupleID*)NULL, NULL);
					if( e < eNOERROR) CATALOG_ERROR(handle, e);

					e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, SM_TRUE, NULL, 1, &clist[1]);
					if( e < eNOERROR) CATALOG_ERROR(handle, e);
				}

				j = relTableEntry->ri.nIndexes;
			}
			else {
				for(i = 0;i < relTableEntry->ri.nIndexes; i++) {
					switch((LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[i].indexType) {
						case LOM_INDEXTYPE_BTREE :
							bcopy(&((LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[i].kdesc.btree), &(tmp_ii[i].kdesc.btree), sizeof(KeyDesc));
							break;
						case LOM_INDEXTYPE_MLGF :
							bcopy(&((LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[i].kdesc.mlgf), &(tmp_ii[i].kdesc.mlgf), sizeof(KeyDesc));
							break;
						default : CATALOG_ERROR(handle, eINTERNAL_CATALOG);
					}
					bcopy(&((LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[i].colNo), &(tmp_ii[i].colNo), sizeof((LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[i].colNo));
					tmp_ii[i].indexType = (LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[i].indexType;
					/* store index id to memory catalog */
					tmp_ii[i].iid.index.physical_iid = (LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[i].iid;
					tmp_ii[i].iid.isLogical = SM_FALSE;
				}

				i = 0;
				j = relTableEntry->ri.nIndexes;

				orn3 = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSTEXTINDEXES_CLASSNAME);
				if(orn3 < eNOERROR) CATALOG_ERROR(handle, orn3);

				lockup.mode = L_IS;
				lockup.duration = L_COMMIT;

				/* inverted index name */
				clist[2].colNo = LOM_SYSTEXTINDEXES_INVERTEDINDEXNAME_COLNO;
				clist[2].start = ALL_VALUE;
				clist[2].length = LOM_MAXINDEXNAME;
				clist[2].dataLength = LOM_MAXINDEXNAME;

				/* index id on keyword field */
				clist[3].colNo = LOM_SYSTEXTINDEXES_KEYWORDINDEXID_COLNO;
				clist[3].start = ALL_VALUE;
				clist[3].length = SM_INDEXID_SIZE;
				clist[3].dataLength = SM_INDEXID_SIZE;

				/* index id on reverse keyword field */
				clist[4].colNo = LOM_SYSTEXTINDEXES_REVKEYWORDINDEXID_COLNO;
				clist[4].start = ALL_VALUE;
				clist[4].length = SM_INDEXID_SIZE;
				clist[4].dataLength = SM_INDEXID_SIZE;

				/* index id on doc-id field */
				clist[5].colNo = LOM_SYSTEXTINDEXES_DOCIDINDEXID_COLNO;
				clist[5].start = ALL_VALUE;
				clist[5].length = SM_INDEXID_SIZE;
				clist[5].dataLength = SM_INDEXID_SIZE;

				/* doc-id table name */
				clist[6].colNo = LOM_SYSTEXTINDEXES_DOCIDINDEXTABLENAME_COLNO;
				clist[6].start = ALL_VALUE;
				clist[6].length = LOM_MAXINDEXNAME;
				clist[6].dataLength = LOM_MAXINDEXNAME;

				/* column number */
				clist[7].colNo = LOM_SYSTEXTINDEXES_COLUMNNO_COLNO;
				clist[7].start = ALL_VALUE;
				clist[7].length = LOM_SHORT_SIZE_VAR;
				clist[7].dataLength = LOM_SHORT_SIZE_VAR;

				/* posting structure info. */
				clist[8].colNo = LOM_SYSTEXTINDEXES_POSTINGSTRUCTUREINFO_COLNO;
				clist[8].start = ALL_VALUE;
				clist[8].length = sizeof(PostingStructureInfo);
				clist[8].dataLength = sizeof(PostingStructureInfo);

				while((e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, (TupleID*)NULL, NULL))!=EOS) {
					clist[0].data.ptr = &(tmp_ii[j].iid);
					clist[2].data.ptr = tmp_ii[j].kdesc.invertedIndex.invertedIndexName;
					clist[6].data.ptr = tmp_ii[j].kdesc.invertedIndex.docIdIndexTableName;
					clist[8].data.ptr = &(postingInfo);

					e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, SM_TRUE, NULL, 1, clist);
					if( e < eNOERROR) CATALOG_ERROR(handle, e);

					if(tmp_ii[j].iid.isLogical == SM_TRUE)
						clist[1].data.ptr = tmp_ii[j].name;
					else
					{
						clist[1].data.ptr = tmp_ii[i].name;
						i ++;
					}

					e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, SM_TRUE, NULL, 1, &clist[1]);
					if( e < eNOERROR) CATALOG_ERROR(handle, e);

					if(tmp_ii[j].iid.isLogical == SM_TRUE) {
						e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), orn3, SM_FALSE, (TupleID *)&tmp_ii[j].iid.index.logical_iid, 7, &clist[2]);
						if(e < eNOERROR) CATALOG_ERROR(handle, e);

						/* inverted index name */
						tmp_ii[j].kdesc.invertedIndex.invertedIndexName[clist[2].retLength] = 0;

						/* keyword index */
						tmp_ii[j].kdesc.invertedIndex.keywordIndex = clist[3].data.iid;

						/* reverse keyword index */
						tmp_ii[j].kdesc.invertedIndex.reverseKeywordIndex = clist[4].data.iid;

						/* doc-id index */
						tmp_ii[j].kdesc.invertedIndex.docIdIndex = clist[5].data.iid;

						/* doc-id index table name */
						tmp_ii[j].kdesc.invertedIndex.docIdIndexTableName[clist[6].retLength] = 0;

						/* column number */
						tmp_ii[j].colNo[0] = GET_VALUE_FROM_COL_LIST(clist[7], sizeof(tmp_ii[j].colNo[0]));

						tmp_ii[j].indexType = LOM_INDEXTYPE_TEXT;

						e  = catalog_Text_ConvertToInMemoryPostingStructure(
							handle, 
	 						j,
							tmp_ii,
							tmp_Adesc,
							&postingInfo);
						if(e < eNOERROR) CATALOG_ERROR(handle, e);

						j++;
					}

				} /* while */

				e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn3);
				if(e < eNOERROR) CATALOG_ERROR(handle, e);

				/* close relation */
				e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), ocn2);
				if(e < eNOERROR) CATALOG_ERROR(handle, e);
	
			} /* else */
			e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), catScanId);
			if(e < eNOERROR) CATALOG_ERROR(handle, e);
	
		} /* if */
		else CATALOG_GET_INDEXINFOTBL_INDEX(tmp_sysClasses) = -1;

		if(j != CATALOG_GET_INDEXNUM(tmp_sysClasses)) CATALOG_ERROR(handle, eINTERNAL_CATALOG);

		/* inheritance information */
		/* open LOM_SYSINHERITANCE */
		ocn2 = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_INHERITANCE_CLASSNAME);
		if(ocn2 < 0) CATALOG_ERROR(handle, ocn2);

		relTableEntry2 = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), ocn2);

		/* SUPERCLASS INFORMATION */
		boundCond.op = SM_EQ;
		boundCond.key.len = sizeof(Four);
		bcopy(&classId,&(boundCond.key.val[0]),sizeof(Four));

		/* set lock up parameters */
		/* just for reading */
		lockup.mode = L_IS;
		lockup.duration = L_COMMIT;

		/* index scan of LOM_SYSINHERITANCE */
		catScanId = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), ocn2,
					&(LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry2))[0].iid,
					&boundCond, &boundCond, 0, NULL, &lockup);
		if (catScanId < 0) CATALOG_ERROR(handle, catScanId);

		/* collist setting */
		for (i = 0; i < LOM_INHERITANCE_NUM_COLS; i++) {
			clist[i].colNo = i;
			clist[i].start = ALL_VALUE;
		}
	    
		/* We here count of superclass of this class id */
		CATALOG_GET_SUPERCLASSNUM(tmp_sysClasses) = 0;
		while((e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, (TupleID*)NULL, NULL))!=EOS) {
			if (e < 0) CATALOG_ERROR(handle, e);
			CATALOG_GET_SUPERCLASSNUM(tmp_sysClasses)++; 
		}

		e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), catScanId);
		if (e < 0) CATALOG_ERROR(handle, e);

		/* SUBCLASS INFORMATION */

		boundCond.op = SM_EQ;
		boundCond.key.len = sizeof(Four);
		bcopy(&classId,&(boundCond.key.val[0]),sizeof(Four));

		/* set lock up parameters */
		/* just for reading */
		lockup.mode = L_IS;
		lockup.duration = L_COMMIT;

		/* index scan of LOM_SYSINHERITANCE */
		catScanId = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), ocn2,
				&(LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry2))[1].iid,
				&boundCond, &boundCond, 0, NULL, &lockup);
		if (catScanId < 0) CATALOG_ERROR(handle, catScanId);

		/* We here count of superclass of this class id */
		CATALOG_GET_SUBCLASSNUM(tmp_sysClasses) = 0;
		while((e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, (TupleID*)NULL, NULL))!=EOS) {
			if (e < 0) CATALOG_ERROR(handle, e);
			CATALOG_GET_SUBCLASSNUM(tmp_sysClasses)++; 
		}

		e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), catScanId);
		if (e < 0) CATALOG_ERROR(handle, e);

		/* get Superclass Information */
		if(CATALOG_GET_SUPERCLASSNUM(tmp_sysClasses) > 0) {

			catScanId = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), ocn2,
					&(LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry2))[0].iid,
					&boundCond, &boundCond, 0, NULL, &lockup);
			if (catScanId < 0) CATALOG_ERROR(handle, catScanId);
			    
			/* get free entries */
			e = catalog_getFreeEntries(handle, v, CATALOG_SUPERCLASSINFOTBL, CATALOG_GET_SUPERCLASSNUM(tmp_sysClasses), &(CATALOG_GET_SUPERCLASSINFOTBL_INDEX(tmp_sysClasses)));
			if( e < 0) CATALOG_ERROR(handle, eSHORTOFMEMORY_CATALOG);

			nSuperclasses = CATALOG_GET_SUPERCLASSNUM(tmp_sysClasses);

			while((e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, (TupleID*)NULL, NULL))!=EOS) {
				if (e < 0) CATALOG_ERROR(handle, e);

				e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, SM_TRUE, NULL, LOM_INHERITANCE_NUM_COLS, &clist[0]);
				if (e < 0) CATALOG_ERROR(handle, e);

				curSuperclassOrder = GET_VALUE_FROM_COL_LIST(clist[LOM_INHERITANCE_ORDER_COLNO], sizeof(curSuperclassOrder));

			    /* temporary pointer to superclass information table entry */
			    tmp_superClasses = &(CATALOG_GET_SUPERCLASSINFOTBL(handle, v)[CATALOG_GET_SUPERCLASSINFOTBL_INDEX(tmp_sysClasses)]);
			    tmp_superClasses[curSuperclassOrder].superClassId = GET_VALUE_FROM_COL_LIST(clist[LOM_INHERITANCE_SUPERCLASSID_COLNO], sizeof(tmp_superClasses[curSuperclassOrder].superClassId));
			    nSuperclasses--;
		    }

		    /* if nSuperclasses != 0 , error...*/
		    if(nSuperclasses!=0) CATALOG_ERROR(handle, eINVALIDCATALOG_CATALOG);

		    e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), catScanId);
		    if (e < 0) CATALOG_ERROR(handle, e);

		}
		else CATALOG_GET_SUPERCLASSINFOTBL_INDEX(tmp_sysClasses) = -1;

		/* get Subclass Information */
		if(CATALOG_GET_SUBCLASSNUM(tmp_sysClasses) > 0) {

			catScanId = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), ocn2,
					&(LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry2))[1].iid,
					&boundCond, &boundCond, 0, NULL, &lockup);
			if (catScanId < 0) CATALOG_ERROR(handle, catScanId);
			    
			/* get free entries */
			e = catalog_getFreeEntries(handle, v, CATALOG_SUBCLASSINFOTBL, CATALOG_GET_SUBCLASSNUM(tmp_sysClasses), &(CATALOG_GET_SUBCLASSINFOTBL_INDEX(tmp_sysClasses)));
			if( e < 0) CATALOG_ERROR(handle, eSHORTOFMEMORY_CATALOG);

			nSubclasses = CATALOG_GET_SUBCLASSNUM(tmp_sysClasses);

			i = 0;
			while((e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, (TupleID*)NULL, NULL))!=EOS) {
				if (e < 0) CATALOG_ERROR(handle, e);

				e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, SM_TRUE, NULL, LOM_INHERITANCE_NUM_COLS, &clist[0]);
				if (e < 0) CATALOG_ERROR(handle, e);

				/* temporary pointer to superclass information table entry */
				tmp_subClasses = &(CATALOG_GET_SUBCLASSINFOTBL(handle, v)[CATALOG_GET_SUBCLASSINFOTBL_INDEX(tmp_sysClasses)]);
				tmp_subClasses[i].subClassId = GET_VALUE_FROM_COL_LIST(clist[LOM_INHERITANCE_CLASSID_COLNO], sizeof(tmp_subClasses[i].subClassId));
				nSubclasses--;
				i++;
			}

			/* if nSubclasses != 0 , error...*/
			if(nSubclasses!=0) CATALOG_ERROR(handle, eINVALIDCATALOG_CATALOG);

			e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), catScanId);
			if (e < 0) CATALOG_ERROR(handle, e);

		}
		else CATALOG_GET_SUBCLASSINFOTBL_INDEX(tmp_sysClasses) = -1;

		/* we here close relation */
		e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), ocn2);
		if (e < 0) CATALOG_ERROR(handle, e);

		    
		/* methods description */
		ocn2 = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId,LOM_SYSMETHODS_CLASSNAME);
		if(ocn2 < 0) CATALOG_ERROR(handle, ocn2);

		relTableEntry2 = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), ocn2);

		boundCond.op = SM_EQ;
		boundCond.key.len = sizeof(Four);
		bcopy(&classId, &(boundCond.key.val[0]), boundCond.key.len);

		/* set lock up parameters */
		/* just for reading */
		lockup.mode = L_IS;
		lockup.duration = L_COMMIT;

		catScanId = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), ocn2,
				&(LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry2))[0].iid,
				&boundCond, &boundCond, 0, NULL, &lockup);
		if (catScanId < 0) CATALOG_ERROR(handle, catScanId);

		for (i = 0; i < LOM_SYSMETHODS_NUM_COLS; i++) {
			clist[i].colNo = i;
			clist[i].start = ALL_VALUE;
			clist[i].dataLength = MAXDIRPATH;
		}
		    
		while((e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, (TupleID*)NULL, NULL))!=EOS) {
			if (e < 0) CATALOG_ERROR(handle, e);
			CATALOG_GET_METHODNUM(tmp_sysClasses)++; 
		}

		e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), catScanId);
		if (e < 0) CATALOG_ERROR(handle, e);


		/* If methods informations exist, */
		if(CATALOG_GET_METHODNUM(tmp_sysClasses) > 0) {

			catScanId = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), ocn2,
						&(LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry2))[0].iid,
						&boundCond, &boundCond, 0, NULL, &lockup);
			if (catScanId < 0) CATALOG_ERROR(handle, catScanId);
				    
			/* get free entries */
			e = catalog_getFreeEntries(handle, v, CATALOG_METHODINFOTBL, CATALOG_GET_METHODNUM(tmp_sysClasses), &(CATALOG_GET_METHODINFOTBL_INDEX(tmp_sysClasses)));
			if( e < 0) CATALOG_ERROR(handle, eSHORTOFMEMORY_CATALOG);

			tmp_Methods = &(CATALOG_GET_METHODINFOTBL(handle, v)[CATALOG_GET_METHODINFOTBL_INDEX(tmp_sysClasses)]);

			i = 0;
			while((e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, (TupleID*)NULL, NULL))!=EOS) {
				if (e < 0) CATALOG_ERROR(handle, e);

				/* make column struct list */
				clist[LOM_SYSMETHODS_DIRPATH_COLNO].data.ptr = tmp_Methods[i].dirPath;
				clist[LOM_SYSMETHODS_FUNCTIONNAME_COLNO].data.ptr = tmp_Methods[i].functionName;
				clist[LOM_SYSMETHODS_METHODNAME_COLNO].data.ptr = tmp_Methods[i].name;
				clist[LOM_SYSMETHODS_ARGUMENTLIST_COLNO].data.ptr = tmp_Methods[i].argumentsList;

				e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, SM_TRUE, NULL, LOM_SYSMETHODS_NUM_COLS, &clist[0]);
				if (e < 0) CATALOG_ERROR(handle, e);

				/* dir path */
				if( clist[LOM_SYSMETHODS_DIRPATH_COLNO].retLength > 0) {
					tmp_Methods[i].dirPath[clist[LOM_SYSMETHODS_DIRPATH_COLNO].retLength] = '\0';
				}
				else tmp_Methods[i].dirPath[0] = '\0';

				/* method name */
				if( clist[LOM_SYSMETHODS_METHODNAME_COLNO].retLength > 0) {
					tmp_Methods[i].name[clist[LOM_SYSMETHODS_METHODNAME_COLNO].retLength] = '\0';
				}
				else tmp_Methods[i].name[0] ='\0';

				/* function name */
				if( clist[LOM_SYSMETHODS_FUNCTIONNAME_COLNO].retLength > 0) {
					tmp_Methods[i].functionName[clist[LOM_SYSMETHODS_FUNCTIONNAME_COLNO].retLength] = '\0';
				}
				else tmp_Methods[i].functionName[0] ='\0';

				/* nArguments */
				tmp_Methods[i].nArguments = GET_VALUE_FROM_COL_LIST(clist[LOM_SYSMETHODS_NARGUMENTS_COLNO], sizeof(tmp_Methods[i].nArguments));

				/* argument list */
				if ( clist[LOM_SYSMETHODS_ARGUMENTLIST_COLNO].retLength <= 0) 
					tmp_Methods[i].argumentsList[0] = -1;

				/* inherited from */
				tmp_Methods[i].inheritedFrom = GET_VALUE_FROM_COL_LIST(clist[LOM_SYSMETHODS_INHERITEDFROM_COLNO], sizeof(tmp_Methods[i].inheritedFrom));

				/* return type */
				tmp_Methods[i].returnType = GET_VALUE_FROM_COL_LIST(clist[LOM_SYSMETHODS_RETURNTYPE_COLNO], sizeof(tmp_Methods[i].returnType));

				/* function ptr */
				tmp_Methods[i].ptrToFunction = NULL;

				/* function ptr */
				tmp_Methods[i].fhandle = NULL;

				/* increment index i */
				i++;
			}

			/* close scan */
			e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), catScanId);
			if (e < 0) CATALOG_ERROR(handle, e);

		} 
		else CATALOG_GET_METHODINFOTBL_INDEX(tmp_sysClasses) = -1;

		/* For Relationship ... */
		/* RELATIONSHIP INFORMATION */
		orn4 = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSRELATIONSHIP_CLASSNAME);
		if(orn4 < 0) CATALOG_ERROR(handle, orn4);

		relTableEntry4 = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn4);

		/* set bound condition for scanning sysRelationshipInfoTbl */
		boundCond.op = SM_EQ;
		boundCond.key.len = sizeof(Four);
		bcopy(&classId, &(boundCond.key.val[0]), sizeof(Four));

		/* set lock up parameters */
		/* just for reading */
		lockup.mode = L_IS;
		lockup.duration = L_COMMIT;

		/* index scan of LOM_SYSRELATIONSHIP */
		catScanId = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), orn4, &(LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry4))[0].iid,
				&boundCond, &boundCond, 0, NULL, &lockup);
		if(catScanId < 0) CATALOG_ERROR(handle, catScanId);

		/* count number of relationship infos */
		CATALOG_GET_RELATIONSHIPNUM(tmp_sysClasses) = 0;
		while((e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, (TupleID*)NULL, NULL)) != EOS)
		{
			if(e < 0) CATALOG_ERROR(handle, e);
			CATALOG_GET_RELATIONSHIPNUM(tmp_sysClasses) ++;
		}
		
		e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), catScanId);
		if(e < eNOERROR) CATALOG_ERROR(handle, e);

		/* get Relationship information */
		if(CATALOG_GET_RELATIONSHIPNUM(tmp_sysClasses) > 0)
		{
			catScanId = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), orn4, &(LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry4))[0].iid,
                                           &boundCond, &boundCond,
                                           0, NULL, &lockup);
			if(catScanId < 0) CATALOG_ERROR(handle, catScanId);

			/* get free entries */
			e = catalog_getFreeEntries(handle, v, CATALOG_RELATIONSHIPINFOTBL,
                                       CATALOG_GET_RELATIONSHIPNUM(tmp_sysClasses),
                                       &(CATALOG_GET_RELATIONSHIPINFOTBL_INDEX(tmp_sysClasses)));
			if(e < 0) CATALOG_ERROR(handle, eSHORTOFMEMORY_CATALOG);
			tmp_Relationships = &(CATALOG_GET_RELATIONSHIPINFOTBL(handle, v)[CATALOG_GET_RELATIONSHIPINFOTBL_INDEX(tmp_sysClasses)]);

			nRelationships = CATALOG_GET_RELATIONSHIPNUM(tmp_sysClasses);

			/* construct clist info */
			clist[LOM_SYSRELATIONSHIP_FROMCLASSID_COLNO].colNo = LOM_SYSRELATIONSHIP_FROMCLASSID_COLNO;
			clist[LOM_SYSRELATIONSHIP_FROMCLASSID_COLNO].start = ALL_VALUE;
			clist[LOM_SYSRELATIONSHIP_FROMCLASSID_COLNO].length = LOM_LONG_SIZE_VAR;
			clist[LOM_SYSRELATIONSHIP_FROMCLASSID_COLNO].dataLength = LOM_LONG_SIZE_VAR;

			clist[LOM_SYSRELATIONSHIP_TOCLASSID_COLNO].colNo = LOM_SYSRELATIONSHIP_TOCLASSID_COLNO;
			clist[LOM_SYSRELATIONSHIP_TOCLASSID_COLNO].start = ALL_VALUE;
			clist[LOM_SYSRELATIONSHIP_TOCLASSID_COLNO].length = LOM_LONG_SIZE_VAR;
			clist[LOM_SYSRELATIONSHIP_TOCLASSID_COLNO].dataLength = LOM_LONG_SIZE_VAR;

			clist[LOM_SYSRELATIONSHIP_FROMATTRNUM_COLNO].colNo = LOM_SYSRELATIONSHIP_FROMATTRNUM_COLNO;
			clist[LOM_SYSRELATIONSHIP_FROMATTRNUM_COLNO].start = ALL_VALUE;
			clist[LOM_SYSRELATIONSHIP_FROMATTRNUM_COLNO].length = LOM_SHORT_SIZE_VAR;
			clist[LOM_SYSRELATIONSHIP_FROMATTRNUM_COLNO].dataLength = LOM_SHORT_SIZE_VAR;

			clist[LOM_SYSRELATIONSHIP_TOATTRNUM_COLNO].colNo = LOM_SYSRELATIONSHIP_TOATTRNUM_COLNO;
			clist[LOM_SYSRELATIONSHIP_TOATTRNUM_COLNO].start = ALL_VALUE;
			clist[LOM_SYSRELATIONSHIP_TOATTRNUM_COLNO].length = LOM_SHORT_SIZE_VAR;
			clist[LOM_SYSRELATIONSHIP_TOATTRNUM_COLNO].dataLength = LOM_SHORT_SIZE_VAR;

			clist[LOM_SYSRELATIONSHIP_CARDINALITY_COLNO].colNo = LOM_SYSRELATIONSHIP_CARDINALITY_COLNO;
			clist[LOM_SYSRELATIONSHIP_CARDINALITY_COLNO].start = ALL_VALUE;
			clist[LOM_SYSRELATIONSHIP_CARDINALITY_COLNO].length = 1;
			clist[LOM_SYSRELATIONSHIP_CARDINALITY_COLNO].dataLength = 1;
			clist[LOM_SYSRELATIONSHIP_CARDINALITY_COLNO].data.ptr = &cardinality;

			clist[LOM_SYSRELATIONSHIP_DIRECTION_COLNO].colNo = LOM_SYSRELATIONSHIP_DIRECTION_COLNO;
			clist[LOM_SYSRELATIONSHIP_DIRECTION_COLNO].start = ALL_VALUE;
			clist[LOM_SYSRELATIONSHIP_DIRECTION_COLNO].length = 1;
			clist[LOM_SYSRELATIONSHIP_DIRECTION_COLNO].dataLength = 1;
			clist[LOM_SYSRELATIONSHIP_DIRECTION_COLNO].data.ptr = &direction;

			clist[LOM_SYSRELATIONSHIP_RELATIONSHIPNAME_COLNO].colNo = LOM_SYSRELATIONSHIP_RELATIONSHIPNAME_COLNO;
			clist[LOM_SYSRELATIONSHIP_RELATIONSHIPNAME_COLNO].start = ALL_VALUE;
			clist[LOM_SYSRELATIONSHIP_RELATIONSHIPNAME_COLNO].length = MAXRELATIONSHIPNAME;
			clist[LOM_SYSRELATIONSHIP_RELATIONSHIPNAME_COLNO].dataLength = MAXRELATIONSHIPNAME;
			clist[LOM_SYSRELATIONSHIP_RELATIONSHIPNAME_COLNO].data.ptr = relationshipName;

			clist[LOM_SYSRELATIONSHIP_RELATIONSHIPID_COLNO].colNo = LOM_SYSRELATIONSHIP_RELATIONSHIPID_COLNO;
			clist[LOM_SYSRELATIONSHIP_RELATIONSHIPID_COLNO].start = ALL_VALUE;
			clist[LOM_SYSRELATIONSHIP_RELATIONSHIPID_COLNO].length = LOM_LONG_SIZE_VAR;
			clist[LOM_SYSRELATIONSHIP_RELATIONSHIPID_COLNO].dataLength = LOM_LONG_SIZE_VAR;

			i = 0;
			while((e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, (TupleID*)NULL, NULL))!=EOS)
			{
				if (e < 0) CATALOG_ERROR(handle, e);
                
				e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, SM_TRUE, NULL,
				    LOM_SYSRELATIONSHIP_NUM_COLS, clist);
                
				tmp_Relationships[i].fromClassId = GET_VALUE_FROM_COL_LIST(clist[LOM_SYSRELATIONSHIP_FROMCLASSID_COLNO], sizeof(tmp_Relationships[i].fromClassId));
				tmp_Relationships[i].fromAttrNum = GET_VALUE_FROM_COL_LIST(clist[LOM_SYSRELATIONSHIP_FROMATTRNUM_COLNO], sizeof(tmp_Relationships[i].fromAttrNum));
				tmp_Relationships[i].toClassId = GET_VALUE_FROM_COL_LIST(clist[LOM_SYSRELATIONSHIP_TOCLASSID_COLNO], sizeof(tmp_Relationships[i].toClassId));
				tmp_Relationships[i].toAttrNum = GET_VALUE_FROM_COL_LIST(clist[LOM_SYSRELATIONSHIP_TOATTRNUM_COLNO], sizeof(tmp_Relationships[i].toAttrNum));
				bcopy(clist[LOM_SYSRELATIONSHIP_DIRECTION_COLNO].data.ptr,
					&tmp_Relationships[i].direction, 1);
				bcopy(clist[LOM_SYSRELATIONSHIP_CARDINALITY_COLNO].data.ptr,
					&tmp_Relationships[i].cardinality, 1);                
				bcopy(clist[LOM_SYSRELATIONSHIP_RELATIONSHIPNAME_COLNO].data.ptr,
					&tmp_Relationships[i].relationshipName,
				clist[LOM_SYSRELATIONSHIP_RELATIONSHIPNAME_COLNO].retLength);
				tmp_Relationships[i].relationshipID =
					GET_VALUE_FROM_COL_LIST(clist[LOM_SYSRELATIONSHIP_RELATIONSHIPID_COLNO], sizeof(tmp_Relationships[i].relationshipID));

				nRelationships--;
				i++;
			}
			if(nRelationships != 0)
				CATALOG_ERROR(handle, eINVALIDCATALOG_CATALOG);

			e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), catScanId);
			if (e < 0) CATALOG_ERROR(handle, e);
		}
		else
			CATALOG_GET_RELATIONSHIPINFOTBL_INDEX(tmp_sysClasses) = -1;
            
		e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn4);
		if(e < 0) CATALOG_ERROR(handle, e);

		/* we here close relation */
		e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), ocn2);
		if (e < 0) CATALOG_ERROR(handle, e);

		e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), ocn);
		if (e < 0) CATALOG_ERROR(handle, e);

	} /* if the given class is not found in in-memory catalog */
	else {
		*freeClassEntryIndex = i;
	}

	return eNOERROR;

}

Four catalog_GetClassInfo(
	LOM_Handle *handle, 
	Four volId,		/* IN: volume id */
	Four classId,		/* IN: class Id */
	Four *freeClassEntryIndex	/* IN: class info. number */
)
{
	Boolean found;
	Four i, v;
	/* Check whether the volume is already mounted. */
	for (v = 0; v < CATALOG_MAXNUMOFVOLS; v++)
		if (LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].volId == volId) {
			break;
		}

	if (v == CATALOG_MAXNUMOFVOLS) return eVOLUMNNOTMOUNTED_CATALOG;

	found = SM_FALSE;
	for (i = 0; i < CATALOG_GET_CLASSINFOTBL_SIZE(handle, v); i++) {
		if(CATALOG_GET_CLASSINFOTBL(handle, v)[i].inUse == SM_TRUE)
		if(CATALOG_GET_CLASSINFOTBL(handle, v)[i].classId == classId) {
			found = SM_TRUE;
			break;
		}
	}
	
	if(!found) *freeClassEntryIndex = -1;
	else *freeClassEntryIndex = i;

	return eNOERROR;
}

Four Catalog_CreateInheritanceInfo(
	LOM_Handle *handle,
	Four volId,
	Four classId,
	Four nSuperclasss, /* IN number of superclasss */
	ClassID *superClasses /* IN superclases id */
)
{
	Four e;
	Four i,j;
	Four idx;
	catalog_SysClassesOverlay *ptrSysclasses;
	Four oldSubClassInfoTblEntryIndex;
	Four newSubClassInfoTblEntryIndex;
	Four v;

	/* Check whether the volume is already mounted. */
	for (v = 0; v < CATALOG_MAXNUMOFVOLS; v++)
		if (LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].volId == volId) {
			break;
		}

	if (v == CATALOG_MAXNUMOFVOLS) return eVOLUMNNOTMOUNTED_CATALOG;

	for(i = 0; i < nSuperclasss; i++) {
		e = catalog_GetClassInfo(handle, volId, superClasses[i], &idx);
		if(e < eNOERROR) CATALOG_ERROR(handle, e);
		else {
			if(idx < 0) continue;
			else {
				ptrSysclasses = &CATALOG_GET_CLASSINFOTBL(handle, v)[idx];
				oldSubClassInfoTblEntryIndex = CATALOG_GET_SUBCLASSINFOTBL_INDEX(ptrSysclasses);
	
				/* get free entries */
				e = catalog_getFreeEntries(handle, 
					v,
					CATALOG_SUBCLASSINFOTBL,
					CATALOG_GET_SUBCLASSNUM(ptrSysclasses) + 1, 
					&newSubClassInfoTblEntryIndex);
				if(e < eNOERROR) CATALOG_ERROR(handle, e);
	
				for(j = 0; j < CATALOG_GET_SUBCLASSNUM(ptrSysclasses); j++) {
					CATALOG_GET_SUBCLASSINFOTBL(handle, v)[CATALOG_GET_SUBCLASSINFOTBL_INDEX(ptrSysclasses) + j].inUse = 0;
					bcopy(&(CATALOG_GET_SUBCLASSINFOTBL(handle, v)[CATALOG_GET_SUBCLASSINFOTBL_INDEX(ptrSysclasses) + j]),
					&(CATALOG_GET_SUBCLASSINFOTBL(handle, v)[newSubClassInfoTblEntryIndex + j]), sizeof(catalog_SysSubClassesOverlay));
				}
				CATALOG_GET_SUBCLASSINFOTBL_INDEX(ptrSysclasses) = newSubClassInfoTblEntryIndex;
	
				CATALOG_GET_SUBCLASSINFOTBL(handle, v)[newSubClassInfoTblEntryIndex + j].subClassId = classId; 
				CATALOG_GET_SUBCLASSNUM(ptrSysclasses)++;
			}
		}
	}	

	return eNOERROR;
}

Four Catalog_DestroyInheritanceInfo(
	LOM_Handle *handle,
	Four volId,
	Four classId,
	Four nSuperclasss, /* IN number of superclasss */
	ClassID *superClasses /* IN superclases id */
)
{
	Four e;
	Four i,j,k;
	Four idx;
	catalog_SysClassesOverlay *ptrSysclasses;
	Four oldSubClassInfoTblEntryIndex;
	Four v;

	/* Check whether the volume is already mounted. */
	for (v = 0; v < CATALOG_MAXNUMOFVOLS; v++)
		if (LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].volId == volId) {
			break;
		}

	if (v == CATALOG_MAXNUMOFVOLS) return eVOLUMNNOTMOUNTED_CATALOG;

	for(i = 0; i < nSuperclasss; i++) {
		e = catalog_GetClassInfo(handle, volId, superClasses[i], &idx);
		if(e < eNOERROR) CATALOG_ERROR(handle, e);
		else {
			if(idx < 0) continue; 
			ptrSysclasses = &CATALOG_GET_CLASSINFOTBL(handle, v)[idx];
			oldSubClassInfoTblEntryIndex = CATALOG_GET_SUBCLASSINFOTBL_INDEX(ptrSysclasses);

			for(j = 0; j < CATALOG_GET_SUBCLASSNUM(ptrSysclasses); j++) {
				if(CATALOG_GET_SUBCLASSINFOTBL(handle, v)[oldSubClassInfoTblEntryIndex + j].subClassId == classId) {
					break;
				}
			}
			if(j == CATALOG_GET_SUBCLASSNUM(ptrSysclasses)) {
				CATALOG_ERROR(handle, eINTERNAL_CATALOG);
			}
			for(k = j + 1;k < CATALOG_GET_SUBCLASSNUM(ptrSysclasses); k++) {
				bcopy(&(CATALOG_GET_SUBCLASSINFOTBL(handle, v)[CATALOG_GET_SUBCLASSINFOTBL_INDEX(ptrSysclasses) + k]),
				&(CATALOG_GET_SUBCLASSINFOTBL(handle, v)[CATALOG_GET_SUBCLASSINFOTBL_INDEX(ptrSysclasses)+ k - 1]), sizeof(catalog_SysSubClassesOverlay));
			}
			CATALOG_GET_SUBCLASSINFOTBL(handle, v)[CATALOG_GET_SUBCLASSINFOTBL_INDEX(ptrSysclasses) + CATALOG_GET_SUBCLASSNUM(ptrSysclasses) - 1].inUse = 0;
			CATALOG_GET_SUBCLASSNUM(ptrSysclasses)--;
		}
	}

	return eNOERROR;
}

Four Catalog_Text_DefinePostingStructure(
	LOM_Handle *handle, 
	Four volId,     /* volumn id */
	Four classId,   /* class id */
	Two colNo,
	PostingStructureInfo *postingInfo
)
{

	Four e;
	Four v;
	Four classEntryIndex;
	catalog_SysClassesOverlay *ptrSysclasses;
	catalog_SysIndexesOverlay *tmpIndex;
	catalog_SysAttributesOverlay *tmp_Adesc;
	Four nReturnedIndexes, indexEntryIndex;

	/* get in-memory catalog */
	e = catalog_GetClassInfo(handle, volId, classId, &classEntryIndex);
	if(e < eNOERROR) CATALOG_ERROR(handle, e);

	if(classEntryIndex == -1) return eNOERROR;

	e = Catalog_GetIndexInfoByAttrNum(handle, volId, classId, colNo, &nReturnedIndexes, &indexEntryIndex);
	if(e < eNOERROR) CATALOG_ERROR(handle, e);

	v = Catalog_GetVolIndex(handle, volId);
	ptrSysclasses = &(CATALOG_GET_CLASSINFOTBL(handle, v)[classEntryIndex]);
	tmp_Adesc = &(CATALOG_GET_ATTRINFOTBL(handle, v)[CATALOG_GET_ATTRINFOTBL_INDEX(ptrSysclasses)]);
	tmpIndex = &(CATALOG_GET_INDEXINFOTBL(handle, v)[CATALOG_GET_INDEXINFOTBL_INDEX(ptrSysclasses)]);

	e  = catalog_Text_ConvertToInMemoryPostingStructure(
			handle, 
			indexEntryIndex,
			tmpIndex,
			tmp_Adesc,
			postingInfo);
	if(e < eNOERROR) CATALOG_ERROR(handle, e);

	return eNOERROR;
}

Four catalog_Text_ConvertToInMemoryPostingStructure(
	LOM_Handle *handle, 
	Four indexEntryIndex,
	catalog_SysIndexesOverlay *tmpIndex,
	catalog_SysAttributesOverlay *tmp_Adesc,
	PostingStructureInfo *postingInfo
)
{

	Four e;
	Four v;
	Four classEntryIndex;
	Four nVarCols;
	Four i;
	Four firstVarColOffset;
	Four offset;
	Four varColNo;

	/* copy elements */
	tmpIndex[indexEntryIndex].kdesc.invertedIndex.postingInfo.isContainingTupleID = postingInfo->isContainingTupleID;
	tmpIndex[indexEntryIndex].kdesc.invertedIndex.postingInfo.isContainingSentenceAndWordNum = postingInfo->isContainingSentenceAndWordNum;
	tmpIndex[indexEntryIndex].kdesc.invertedIndex.postingInfo.isContainingByteOffset = postingInfo->isContainingByteOffset;
	tmpIndex[indexEntryIndex].kdesc.invertedIndex.postingInfo.nEmbeddedAttributes = postingInfo->nEmbeddedAttributes;
	for(i = 0; i < postingInfo->nEmbeddedAttributes; i++) {
		tmpIndex[indexEntryIndex].kdesc.invertedIndex.postingInfo.embeddedAttrNo[i] = postingInfo->embeddedAttrNo[i];
	}
	

	nVarCols = 0;
	for(i = 0; i< postingInfo->nEmbeddedAttributes; i++) {
		if(CATALOG_GET_ATTRTYPE(&tmp_Adesc[postingInfo->embeddedAttrNo[i]]) != LOM_VARSTRING) {
			firstVarColOffset += CATALOG_GET_ATTRLENGTH(&tmp_Adesc[postingInfo->embeddedAttrNo[i]]);
		}
		else nVarCols++;
	}

	offset = 0;
	varColNo = 0;
	for(i = 0; i < postingInfo->nEmbeddedAttributes; i++) {
		if(CATALOG_GET_ATTRTYPE(&tmp_Adesc[postingInfo->embeddedAttrNo[i]]) != LOM_VARSTRING) {
			tmpIndex[indexEntryIndex].kdesc.invertedIndex.postingInfo.embeddedAttrOffset[i] = offset;
			tmpIndex[indexEntryIndex].kdesc.invertedIndex.postingInfo.embeddedAttrVarColNo[i] = NIL;
			offset += CATALOG_GET_ATTRLENGTH(&tmp_Adesc[postingInfo->embeddedAttrNo[i]]);
		}
	}

	if(nVarCols > 0) {
		for(i = 0; i < postingInfo->nEmbeddedAttributes; i++) {
			if(CATALOG_GET_ATTRTYPE(&tmp_Adesc[postingInfo->embeddedAttrNo[i]]) == LOM_VARSTRING) {
				tmpIndex[indexEntryIndex].kdesc.invertedIndex.postingInfo.embeddedAttrVarColNo[i] = varColNo++;
			}
		}
	} 

	return eNOERROR;
}

