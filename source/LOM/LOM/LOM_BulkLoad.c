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

#include <malloc.h>
#include <string.h>
#include <stdlib.h>
#include <signal.h>
#ifndef WIN32
#include <sys/wait.h>
#endif
#include "LOM_Internal.h"
#include "LOM.h"
#include "Catalog.h"
#include "Catalog_Internal.h"

Four LOM_OpenClassBulkLoad(
	LOM_Handle*		handle,				/* IN LOM system handle */
	Four			volId,				/* IN volume id */
	Four			temporaryVolId,		/* IN id for temporary volume used in sorting and etc. */
	char*			className,			/* IN class name that bulk load will be processed */
	Boolean			isNeedSort,			/* IN flag which indicates input data must be sorted by clustering index key */
	Boolean         isIndexBulkLoad,    /* IN flag which indicates build index by bulkloading facility */
	Two				pff,				/* IN Page fill factor */
	Two				eff,				/* IN Extent fill factor */
	LockParameter*	lockup				/* IN Lock parameter */
)
{
	Four							e, i, v, index;
	catalog_SysClassesOverlay*		ptrToSysClasses;
	catalog_SysAttributesOverlay*	ptrToSysAttributes;
	lrds_RelTableEntry*				relTableEntry; 
	Four							idxForClassInfo;
	Four							lrdsBulkLoadId;
	Four							bulkLoadId;
	Four							classId;
	Four							ocn;
	char							contentTableName[LOM_MAXCLASSNAME];
	Boolean							textColumnExist;

	/* allocate new lom level bulk loading id */
	for(bulkLoadId = 0; bulkLoadId < LOM_BULKLOADTABLE_ENTRIES(handle); bulkLoadId++)
		if(LOM_BULKLOADTABLE(handle)[bulkLoadId].lrdsBulkLoadId == NIL) break;

	/* scan table is full */
	if(bulkLoadId == LOM_BULKLOADTABLE_ENTRIES(handle)) 
	{
		/* There are no empty entries */
		e = LRDS_doublesizeVarArray(LOM_GET_LRDS_HANDLE(handle), LOM_BULKLOADTABLE_PTR(handle), sizeof(lom_BulkLoadTableEntry));
		if(e < eNOERROR) LOM_ERROR(handle, e);

		/* Initialize the newly allocated entries */
		for (i = bulkLoadId; i < LOM_BULKLOADTABLE_ENTRIES(handle); i++) 
			LOM_BULKLOADTABLE(handle)[i].lrdsBulkLoadId = NIL;
	}

	/* init lrds level bulk loading */
	lrdsBulkLoadId = LRDS_InitRelationBulkLoad(LOM_GET_LRDS_HANDLE(handle), volId, temporaryVolId, className, isNeedSort, isIndexBulkLoad, pff, eff, lockup);
	if(lrdsBulkLoadId < eNOERROR) LOM_ERROR(handle, lrdsBulkLoadId);

	/* open class and get class information */
	ocn = LOM_OpenClass(handle, volId, className);
	if(ocn < eNOERROR) LOM_ERROR(handle, ocn);

	relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), ocn);

	/* get class id and other informations */
	e = LOM_GetClassID(handle, volId, className, &classId);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	e = Catalog_GetClassInfo(handle, relTableEntry->ri.fid.volNo, classId, &idxForClassInfo);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	v = Catalog_GetVolIndex(handle, relTableEntry->ri.fid.volNo);
	if(v < eNOERROR) LOM_ERROR(handle, e);

	ptrToSysClasses    = &CATALOG_GET_CLASSINFOTBL(handle, v)[idxForClassInfo];
	ptrToSysAttributes = &CATALOG_GET_ATTRINFOTBL(handle, v)[CATALOG_GET_ATTRINFOTBL_INDEX(ptrToSysClasses)];

	textColumnExist = SM_FALSE;
	for(i = 0, index = 0; i < CATALOG_GET_ATTRNUM(ptrToSysClasses); i++) 
	{ 
		if(ptrToSysAttributes[i].type == LOM_TEXT)
			textColumnExist = SM_TRUE;
	}

	if(textColumnExist)
	{
		/* make content table name */
		sprintf(contentTableName, "_%s_Content", className);

		e = LRDS_InitRelationBulkLoad(LOM_GET_LRDS_HANDLE(handle), volId, temporaryVolId, contentTableName, SM_FALSE, SM_FALSE, 100, 100, lockup);
		if(e < eNOERROR) LOM_ERROR(handle, e);

		LOM_BULKLOADTABLE(handle)[bulkLoadId].lrdsTextBulkLoadId = e;
	}

	LOM_BULKLOADTABLE(handle)[bulkLoadId].lrdsBulkLoadId  = lrdsBulkLoadId;
	LOM_BULKLOADTABLE(handle)[bulkLoadId].textColumnExist = textColumnExist;
	LOM_BULKLOADTABLE(handle)[bulkLoadId].classId         = classId;
	LOM_BULKLOADTABLE(handle)[bulkLoadId].ocn             = ocn;
	LOM_BULKLOADTABLE(handle)[bulkLoadId].relTableEntry   = relTableEntry;
	LOM_BULKLOADTABLE(handle)[bulkLoadId].newlyCreatedObjectFlag = SM_TRUE;
	LOM_BULKLOADTABLE(handle)[bulkLoadId].userGivenLogicalId = -1;

	return eNOERROR;
}

Four LOM_CloseClassBulkLoad(
	LOM_Handle*		handle,				/* IN LOM system handle */
	Four            bulkLoadId			/* IN Bulk Load Id */
)
{
	Four e;

	if(LOM_BULKLOADTABLE(handle)[bulkLoadId].textColumnExist)
	{
		e = LRDS_FinalRelationBulkLoad(LOM_GET_LRDS_HANDLE(handle), LOM_BULKLOADTABLE(handle)[bulkLoadId].lrdsTextBulkLoadId);
		if(e < eNOERROR) LOM_ERROR(handle, e);
	}

	e = LRDS_FinalRelationBulkLoad(LOM_GET_LRDS_HANDLE(handle), LOM_BULKLOADTABLE(handle)[bulkLoadId].lrdsBulkLoadId);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	e = LOM_CloseClass(handle, LOM_BULKLOADTABLE(handle)[bulkLoadId].ocn);
	if(e < eNOERROR) LOM_ERROR(handle, e);


	LOM_BULKLOADTABLE(handle)[bulkLoadId].lrdsBulkLoadId = NIL;

	return eNOERROR;
}

Four LOM_NextClassBulkLoad(
	LOM_Handle*			handle,			/* IN LOM system handle */
	Four				bulkLoadId,		/* IN Bulk Load Id */
    Four				nCols,			/* IN number of columns */
    LOM_ColListStruct*	clist,			/* IN data of the created columns */
    Boolean				endOfTuple,		/* IN flag which indicates given clist is end of object or not */
	Four*				logicalId,		/* OUT logical id of the object */
	OID*				oid				/* OUT physical id of the object */
)
{
	Four							e, i, v;
	ColListStruct					lrdsColListStruct[LOM_MAXNUMOFATTRIBUTE];
	lrds_RelTableEntry*				relTableEntry; 
	Four							idxForClassInfo;
	catalog_SysClassesOverlay*		ptrToSysClasses;
	catalog_SysAttributesOverlay*	ptrToSysAttributes;

	relTableEntry = LOM_BULKLOADTABLE(handle)[bulkLoadId].relTableEntry;

	lrdsColListStruct[0].colNo = 0;
	lrdsColListStruct[0].start = ALL_VALUE;
	lrdsColListStruct[0].length = sizeof(Four);
	lrdsColListStruct[0].dataLength = sizeof(Four);
	lrdsColListStruct[0].nullFlag = SM_FALSE;

	e = Catalog_GetClassInfo(handle, relTableEntry->ri.fid.volNo, LOM_BULKLOADTABLE(handle)[bulkLoadId].classId, &idxForClassInfo);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	v = Catalog_GetVolIndex(handle, relTableEntry->ri.fid.volNo);
	if(v < eNOERROR) LOM_ERROR(handle, e);

	ptrToSysClasses    = &CATALOG_GET_CLASSINFOTBL(handle, v)[idxForClassInfo];
	ptrToSysAttributes = &CATALOG_GET_ATTRINFOTBL(handle, v)[CATALOG_GET_ATTRINFOTBL_INDEX(ptrToSysClasses)];

	for(i = 0; i < nCols; i++) {
		bcopy(&(clist[i]), &(lrdsColListStruct[i+1]), sizeof(ColListStruct));

		/* conversion between LOM type and LRDS type */
		switch(ptrToSysAttributes[GET_SYSTEMLEVEL_COLNO(clist[i].colNo)].type) 
		{
		case LOM_OCTET:
			lrdsColListStruct[i+1].data.ptr = &(clist[i].data.o);
			break;
		case LOM_BOOLEAN:
			ASSIGN_VALUE_TO_COL_LIST(lrdsColListStruct[i+1], clist[i].data.b, sizeof(Four));
			break;
		case LOM_USHORT:
			ASSIGN_VALUE_TO_COL_LIST(lrdsColListStruct[i+1], clist[i].data.us, sizeof(Two_Invariable));
			break;
		case LOM_ULONG:
			ASSIGN_VALUE_TO_COL_LIST(lrdsColListStruct[i+1], clist[i].data.ul, sizeof(Four_Invariable));
			break;
		case LOM_DATE :
			ASSIGN_VALUE_TO_COL_LIST(lrdsColListStruct[i+1], clist[i].data.date, sizeof(Four_Invariable));
			break;
		case LOM_TIME :
			lrdsColListStruct[i+1].data.ptr = &(clist[i].data.time);
			break;
		case LOM_TIMESTAMP :
			lrdsColListStruct[i+1].data.ptr = &(clist[i].data.timestamp);
			break;
		case LOM_INTERVAL :
			lrdsColListStruct[i+1].data.d = (double)clist[i].data.interval;
			break;
		}

		lrdsColListStruct[i+1].colNo ++;
	}

	if(LOM_BULKLOADTABLE(handle)[bulkLoadId].textColumnExist) 
	{
		if(LOM_BULKLOADTABLE(handle)[bulkLoadId].newlyCreatedObjectFlag)
		{
            if(LOM_BULKLOADTABLE(handle)[bulkLoadId].userGivenLogicalId >= 0)
			{
				ASSIGN_VALUE_TO_COL_LIST(lrdsColListStruct[0], LOM_BULKLOADTABLE(handle)[bulkLoadId].userGivenLogicalId, sizeof(Four));
				LOM_BULKLOADTABLE(handle)[bulkLoadId].userGivenLogicalId = -1;
			}
			else
				ASSIGN_VALUE_TO_COL_LIST(lrdsColListStruct[0], lom_Text_GetAndIncrementLogicalId(handle, LOM_BULKLOADTABLE(handle)[bulkLoadId].ocn), sizeof(Four));

			LOM_BULKLOADTABLE(handle)[bulkLoadId].logicalId = GET_VALUE_FROM_COL_LIST(lrdsColListStruct[0], sizeof(LOM_BULKLOADTABLE(handle)[bulkLoadId].logicalId));

			e = LRDS_NextRelationBulkLoad(LOM_GET_LRDS_HANDLE(handle), LOM_BULKLOADTABLE(handle)[bulkLoadId].lrdsBulkLoadId, nCols + 1,
				                          &lrdsColListStruct[0], endOfTuple, (TupleID*)oid);
			if (e < 0) LOM_ERROR(handle, e);
		}
		else
		{
			e = LRDS_NextRelationBulkLoad(LOM_GET_LRDS_HANDLE(handle), LOM_BULKLOADTABLE(handle)[bulkLoadId].lrdsBulkLoadId, nCols,
				                          &lrdsColListStruct[1], endOfTuple, (TupleID*)oid);
			if (e < 0) LOM_ERROR(handle, e);
		}
	}
	else 
	{
		if(LOM_BULKLOADTABLE(handle)[bulkLoadId].newlyCreatedObjectFlag)
		{
			lrdsColListStruct[0].nullFlag = SM_TRUE;
			LOM_BULKLOADTABLE(handle)[bulkLoadId].logicalId = NIL;

			e = LRDS_NextRelationBulkLoad(LOM_GET_LRDS_HANDLE(handle), LOM_BULKLOADTABLE(handle)[bulkLoadId].lrdsBulkLoadId, nCols + 1,
				                          &lrdsColListStruct[0], endOfTuple, (TupleID*)oid);
			if (e < 0) LOM_ERROR(handle, e);
		}
		else
		{
			e = LRDS_NextRelationBulkLoad(LOM_GET_LRDS_HANDLE(handle), LOM_BULKLOADTABLE(handle)[bulkLoadId].lrdsBulkLoadId, nCols,
				                          &lrdsColListStruct[1], endOfTuple, (TupleID*)oid);
			if (e < 0) LOM_ERROR(handle, e);
		}
	}

	if(endOfTuple)
	{
		LOM_BULKLOADTABLE(handle)[bulkLoadId].newlyCreatedObjectFlag = SM_TRUE;
		if(logicalId)
			*logicalId = LOM_BULKLOADTABLE(handle)[bulkLoadId].logicalId;
		if(oid)
			oid->classID = LOM_BULKLOADTABLE(handle)[bulkLoadId].classId;
	}
	else
	{
		LOM_BULKLOADTABLE(handle)[bulkLoadId].newlyCreatedObjectFlag = SM_FALSE;
		if(logicalId)
			*logicalId = LOM_BULKLOADTABLE(handle)[bulkLoadId].logicalId;
		if(oid)
			oid->classID = LOM_BULKLOADTABLE(handle)[bulkLoadId].classId;
	}

	return eNOERROR;
}

Four LOM_Text_CreateContentBulkLoad(
	LOM_Handle*		handle,				/* IN LOM system handle */
	Four			bulkLoadId,			/* IN Bulk load Id */
	Two				colNum,				/* IN column number */
	TextColStruct*	text,				/* IN text column struct */
	LOM_TextDesc*	textDesc, 			/* INOUT: text descriptor */
	Boolean			endOfTuple,			/* IN flag which indicates this ordered set is end of tuple or not */
	Four*			logicalId,			/* OUT logical id of the object */
	OID*			oid					/* OUT physical id of the object */
)
{
	ColListStruct	clist[2];			/* collist struct */
	Four			e;					/* error code */
	Four			orn;				/* open class number for the given class */
	Two				colNo;				/* column number : given by system not user */
	LOM_TextDesc	tmpTextDesc;
	TupleID 		tid;

#ifdef COMPRESSION
    char *compressedData;
    Four compressedDataLength;  
    FILE *fp;
#endif

	/* check parameter */
	if(text == NULL < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if(colNum < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if(textDesc == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if(text->dataLength < 0)  LOM_ERROR(handle, eBADPARAMETER_LOM);

	if(!DOES_NOCONTENT_EXIST_TEXTDESC(*textDesc)) LOM_ERROR(handle, eBADPARAMETER_LOM);

	orn = LOM_BULKLOADTABLE(handle)[bulkLoadId].ocn;
	
	/* convert user-known column number to system column number */
	colNo = GET_SYSTEMLEVEL_COLNO(colNum);

#ifdef COMPRESSION
    compressedDataLength = compressBound(text->dataLength);
    compressedData = (char*)malloc(compressedDataLength + sizeof(char));
    if(compressedData == NULL) LOM_ERROR(handle, eOUTOFMEMORY_LOM);
    
    e = lom_Text_Compression(handle, text->data, text->dataLength, compressedData, &compressedDataLength);
    if(e < eNOERROR) 
	{
		if(compressedData != NULL) free(compressedData);
		LOM_ERROR(handle, e);
	}
#endif

	/* create content data */
	/* construct column struct list */
	clist[0].colNo      = LOM_CONTENT_CONTENT_COLNO;
	clist[0].start      = ALL_VALUE;

#ifdef COMPRESSION
	clist[0].dataLength = compressedDataLength;
	clist[0].data.ptr   = compressedData;
#else
	clist[0].dataLength = text->dataLength;
	clist[0].data.ptr   = text->data;
#endif

	clist[0].nullFlag   = SM_FALSE;

	/* create content */
	e = LRDS_NextRelationBulkLoad(LOM_GET_LRDS_HANDLE(handle), LOM_BULKLOADTABLE(handle)[bulkLoadId].lrdsTextBulkLoadId,
			                      1, &clist[0], SM_TRUE, &tid);

#ifdef COMPRESSION
	if (e < 0)
	{
		if(compressedData != NULL) free(compressedData);
		LOM_ERROR(handle, e);
	}
	if(compressedData != NULL) free(compressedData);
#else
	if (e < 0) LOM_ERROR(handle, e);
#endif


	textDesc->size = text->dataLength;

	tmpTextDesc.size = textDesc->size;
	memcpy(&tmpTextDesc.contentTid, &tid, sizeof(TupleID));
	memcpy(&textDesc->contentTid, &tid, sizeof(TupleID));
	if(text->indexMode == LOM_IMMEDIATE_MODE) {
		tmpTextDesc.isIndexed = SM_TRUE;
		tmpTextDesc.hasBeenIndexed = SM_TRUE;
	}
	else {
		tmpTextDesc.isIndexed = textDesc->isIndexed;
		tmpTextDesc.hasBeenIndexed = textDesc->hasBeenIndexed;
	}

	/* store text descriptor information */
	clist[1].colNo = colNo; 
	clist[1].start = ALL_VALUE;
	clist[1].length = sizeof(LOM_TextDesc);
	clist[1].dataLength = sizeof(LOM_TextDesc);
	clist[1].data.ptr = &tmpTextDesc;
	clist[1].nullFlag = SM_FALSE;

	/* if newly created object, then make logical id field */
	if(LOM_BULKLOADTABLE(handle)[bulkLoadId].newlyCreatedObjectFlag)
	{
		clist[0].colNo = 0;
		clist[0].start = ALL_VALUE;
		clist[0].length = sizeof(Four);
		clist[0].dataLength = sizeof(Four);
		clist[0].nullFlag = SM_FALSE;

		if(LOM_BULKLOADTABLE(handle)[bulkLoadId].userGivenLogicalId > 0)
		{
			ASSIGN_VALUE_TO_COL_LIST(clist[0], LOM_BULKLOADTABLE(handle)[bulkLoadId].userGivenLogicalId, sizeof(Four));
			LOM_BULKLOADTABLE(handle)[bulkLoadId].userGivenLogicalId = -1;
		}
		else
			ASSIGN_VALUE_TO_COL_LIST(clist[0], lom_Text_GetAndIncrementLogicalId(handle, LOM_BULKLOADTABLE(handle)[bulkLoadId].ocn), sizeof(Four));

		LOM_BULKLOADTABLE(handle)[bulkLoadId].logicalId = GET_VALUE_FROM_COL_LIST(clist[0], sizeof(LOM_BULKLOADTABLE(handle)[bulkLoadId].logicalId));
	}

	/* update text descriptor */
	if(LOM_BULKLOADTABLE(handle)[bulkLoadId].newlyCreatedObjectFlag)
	{
		e = LRDS_NextRelationBulkLoad(LOM_GET_LRDS_HANDLE(handle), LOM_BULKLOADTABLE(handle)[bulkLoadId].lrdsBulkLoadId, 2, &clist[0], endOfTuple, (TupleID*)oid);
		if (e < 0) LOM_ERROR(handle, e);
	}
	else
	{
		e = LRDS_NextRelationBulkLoad(LOM_GET_LRDS_HANDLE(handle), LOM_BULKLOADTABLE(handle)[bulkLoadId].lrdsBulkLoadId, 1, &clist[1], endOfTuple, (TupleID*)oid);
		if (e < 0) LOM_ERROR(handle, e);
	}

	textDesc->isIndexed = SM_FALSE;

	if(endOfTuple)
	{
		LOM_BULKLOADTABLE(handle)[bulkLoadId].newlyCreatedObjectFlag = SM_TRUE;
		if(logicalId)
			*logicalId = LOM_BULKLOADTABLE(handle)[bulkLoadId].logicalId;
		if(oid)
			oid->classID = LOM_BULKLOADTABLE(handle)[bulkLoadId].classId;
	}
	else
	{
		LOM_BULKLOADTABLE(handle)[bulkLoadId].newlyCreatedObjectFlag = SM_FALSE;
		if(logicalId)
			*logicalId = LOM_BULKLOADTABLE(handle)[bulkLoadId].logicalId;
		if(oid)
			oid->classID = LOM_BULKLOADTABLE(handle)[bulkLoadId].classId;
	}

	return eNOERROR;
}

Four LOM_NextClassBulkLoad_OrderedSetBulkLoad(
	LOM_Handle*		handle,				/* IN LOM system handle */
	Four			temporaryVolId,		/* IN id for temporary volume used in sorting and etc. */
    Four			bulkLoadId,			
    Four			colNo,				
	Four            nElements,          /* IN # of elements to append */
    Four            elementsBufSize,    /* IN buffer size */
    char*           elementsBuf,        /* IN elements to append */
    Boolean			endOfTuple,			
	Four*			logicalId,			/* OUT logical id of the object */
	OID*			oid					/* OUT physical id of the object */
)
{
	Four e;

#ifndef COMPRESSION
	e = LRDS_NextRelationBulkLoad_OrderedSetBulkLoad(LOM_GET_LRDS_HANDLE(handle), bulkLoadId, temporaryVolId, GET_SYSTEMLEVEL_COLNO(colNo),
		                                             nElements, elementsBufSize, elementsBuf, endOfTuple, (TupleID*)oid);
#else
	VolNo volNo;
	char *buffer = NULL;
	Boolean	encodeDocIdFlag = SM_FALSE;
	Four	lastDocId;

	e = LRDS_NextRelationBulkLoad_OrderedSetBulkLoad(LOM_GET_LRDS_HANDLE(handle), bulkLoadId, temporaryVolId, GET_SYSTEMLEVEL_COLNO(colNo),
		                                             nElements, elementsBufSize, elementsBuf, endOfTuple, (TupleID*)oid, buffer, volNo, lastDocId);
#endif
	if(e < eNOERROR) LOM_ERROR(handle, e);

	if(endOfTuple)
	{
		LOM_BULKLOADTABLE(handle)[bulkLoadId].newlyCreatedObjectFlag = SM_TRUE;
		*logicalId = LOM_BULKLOADTABLE(handle)[bulkLoadId].logicalId;
	}
	else
	{
		LOM_BULKLOADTABLE(handle)[bulkLoadId].newlyCreatedObjectFlag = SM_FALSE;
		*logicalId = NIL;
	}

	return eNOERROR;
}


Four LOM_OrderedSetAppendBulkLoad(
	LOM_Handle*		handle,				/* IN LOM system handle */
	Four			temporaryVolId,		/* IN id for temporary volume used in sorting and etc. */
	Four			ocnOrScanId,		/* IN open class number of scan id */
	Boolean			useScanFlag,		/* IN use scan flag */
    OID*			oid,				
    Four            colNo,              /* IN column number of ordered set type */
    Four            nElements,          /* IN # of elements to append */
    Four            elementsBufSize,    /* IN buffer size */
    char*           elementsBuf,        /* IN elements to append */
	LockParameter*  lockup				/* IN lock up parameter */
)
{
	Four e;
	Four lrdsOrnOrScanId;

	if(useScanFlag)
		lrdsOrnOrScanId = LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId;
	else
		lrdsOrnOrScanId = ocnOrScanId;

	e = LRDS_OrderedSetAppendBulkLoad(LOM_GET_LRDS_HANDLE(handle), temporaryVolId, lrdsOrnOrScanId, useScanFlag, (TupleID*)oid,
		                              GET_SYSTEMLEVEL_COLNO(colNo), 
                                      nElements, elementsBufSize, elementsBuf, lockup);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}

Four LOM_SetUserGivenLogicalID_BulkLoad(
	LOM_Handle*			handle,			/* IN LOM system handle */
	Four				bulkLoadId,		/* IN Bulk Load Id */
	Four				logicalId		/* IN New logical id used in bulkloading */
)
{
	LOM_BULKLOADTABLE(handle)[bulkLoadId].userGivenLogicalId = logicalId;
	return eNOERROR;
}
