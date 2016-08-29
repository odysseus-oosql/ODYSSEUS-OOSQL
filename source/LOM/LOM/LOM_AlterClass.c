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
 * Module: LOM_AlterClass.c
 *
 * Description:
 *  Alter a Class.
 *
 * Imports:
 *  
 *
 * Exports:
 *
 * Returns:
 *  Error code
 *    some errors caused by function calls
 *
 */


#include <string.h>
#include <malloc.h>
#include "LOM.h"
#include "ODYS_common.h"
#include "Catalog_Internal.h"
#include "Catalog.h"


Four catalog_RefreshClassInfoForAlterClass(LOM_Handle *handle, Four volId, Four classId, char *className, catalog_SysClassesOverlay* tmp_sysClasses);

Four LOM_AlterClass(
	LOM_Handle		*handle,
	Four			volId,			/* IN volume in which the relation will be placed */
	char			*className,		/* IN relation name to create */
	Four			classId, 		/* IN class id */		
	Four			nAddCol,		/* IN number of add columns */
	AttrInfo		*addColInfo,	/* IN information of add columns */
	Four			nDropCol,		/* IN number of drop columns */
	AttrInfo		*dropColInfo)	/* IN information of drop columns */
{
	Two				activeColNo;
	Four			v;
	Four			e;								/* error number */
	Four			i,j;							/* index variable */
	Four			idxForClassInfo;
	Four			newAttrInfoTblEntryIndex = -1;
	Four			ocn;
	Four			osn;							/* catalog scan Id */
	Four			orn;							/* open relation number */
	Four			orn2;							/* open relation number */
	Four			nOldCol;						
	Four			nOldIndex;						
	Four			offset;							/* offset of the corresponging attribute of an object */
    Four            scanId;                     /* scan identifier */
	Four			nTextAttr;
	Four			colNo;
	char			invertedIndexTableName[LOM_MAXCLASSNAME]; /* content class name */
	char			logicalIdIndexName[LOM_MAXCLASSNAME];
	char			colName[LOM_MAXATTRNAME];
	FileID			fid;				/* SM level file identifier */
	LOM_IndexID		iid;
	ColInfo			colInfo;						/* ColInfo */
	TupleID			tid;
	LOM_IndexID		logical_id_index;				/* index built on logical-id column */
	LockParameter	lockup;      
	Boolean			isFirstTextAttr;
	Boolean			isDeleted;
	BoolExp			boolexp[1];
	BoundCond						boundCond;        /* boundary condition */
	lrds_RelTableEntry				*relTableEntry;
	ColListStruct	clist[LOM_SYSCOLUMNS_NUM_COLS];	/* column list */
	catalog_SysClassesOverlay *tmp_sysClasses;
	catalog_SysAttributesOverlay *tmp_Adesc;



	/* 1. check parameters */
	if (volId < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);
    
	if (className == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);

	if (nAddCol < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);

	if (addColInfo == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);

	if (nDropCol < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);

	if (dropColInfo == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);

	ocn = LOM_GetOpenClassNum(handle, volId, className);
	if(ocn < 0) LOM_ERROR(handle, ocn);

	e = LOM_CloseClass(handle, ocn);
	if(e < eNOERROR) LOM_ERROR(handle, e);


	/* 
		2. update lrdsSysTables catalog info using LRDS_AddColumn 
	*/
	for(i = 0; i< nAddCol; i++) {
		/* For ODMG Collection */
		if(addColInfo[i].complexType == LOM_COMPLEXTYPE_ODMG_COLLECTIONSET ||
		   addColInfo[i].complexType == LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG ||
		   addColInfo[i].complexType == LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST ||
		   addColInfo[i].complexType == LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY)
		{
			colInfo.complexType = SM_COMPLEXTYPE_BASIC;
			colInfo.type        = SM_STRING;
			colInfo.length      = sizeof(ODMG_CollectionDesc);
		}
		else
		{
			colInfo.complexType = addColInfo[i].complexType;
			switch(addColInfo[i].type) {
				case ODYS_ULONG:
					colInfo.type = ODYS_LONG;
					colInfo.length = ODYS_LONG_SIZE;
					break;
				case ODYS_USHORT:
					colInfo.type = ODYS_SHORT;
					colInfo.length = ODYS_SHORT_SIZE;
					break;
				case ODYS_OCTET:
					colInfo.type = ODYS_STRING;
					colInfo.length = addColInfo[i].length;
					break;
				case ODYS_BOOLEAN:
					colInfo.type = ODYS_LONG;
					colInfo.length = ODYS_BOOLEAN_SIZE;
					break;
				case ODYS_REF :
				case ODYS_LINK :
					colInfo.type = SM_OID;
					colInfo.length = addColInfo[i].length;
					break;
				case LOM_TEXT:
					colInfo.type = SM_STRING; 			
					colInfo.length = sizeof(LOM_TextDesc);
					break;
				/* added ODMG Literal Type into LOM layer */
				case LOM_DATE:
					colInfo.type = SM_LONG;
					colInfo.length = SM_LONG_SIZE;
					break;
				case LOM_TIME:
					colInfo.type = SM_STRING;
					colInfo.length = addColInfo[i].length;
					break;
				case LOM_TIMESTAMP:
					colInfo.type = SM_STRING;
					colInfo.length = addColInfo[i].length;
					break;
				case LOM_INTERVAL:
					colInfo.type = SM_DOUBLE;
					colInfo.length = SM_DOUBLE_SIZE;
					break;
				default:
					colInfo.type = addColInfo[i].type;
					colInfo.length = addColInfo[i].length;
					break;
			}
		}

		/* add column in LRDS */
		e = LRDS_AddColumn(LOM_GET_LRDS_HANDLE(handle), volId, className, &colInfo);
		if (e < 0) LOM_ERROR(handle, e);
	}




	/* 
		3. update lomSysClasses catalog info 
	*/
	/* 
		3-1. Construct 'clist'. 
	*/
	clist[LOM_SYSCOLUMNS_CLASSID_COLNO].colNo = LOM_SYSCOLUMNS_CLASSID_COLNO;
	clist[LOM_SYSCOLUMNS_CLASSID_COLNO].start = ALL_VALUE;
	clist[LOM_SYSCOLUMNS_CLASSID_COLNO].dataLength = sizeof(Four);
	ASSIGN_VALUE_TO_COL_LIST(clist[LOM_SYSCOLUMNS_CLASSID_COLNO], classId, sizeof(Four));
	clist[LOM_SYSCOLUMNS_CLASSID_COLNO].nullFlag = SM_FALSE;

	clist[LOM_SYSCOLUMNS_COMPLEXTYPE_COLNO].colNo = LOM_SYSCOLUMNS_COMPLEXTYPE_COLNO;
	clist[LOM_SYSCOLUMNS_COMPLEXTYPE_COLNO].start = ALL_VALUE;
	clist[LOM_SYSCOLUMNS_COMPLEXTYPE_COLNO].dataLength = sizeof(Two);
	clist[LOM_SYSCOLUMNS_COMPLEXTYPE_COLNO].nullFlag = SM_FALSE;

	clist[LOM_SYSCOLUMNS_COLTYPE_COLNO].colNo = LOM_SYSCOLUMNS_COLTYPE_COLNO;
	clist[LOM_SYSCOLUMNS_COLTYPE_COLNO].start = ALL_VALUE;
	clist[LOM_SYSCOLUMNS_COLTYPE_COLNO].dataLength = sizeof(Two);
	clist[LOM_SYSCOLUMNS_COLTYPE_COLNO].nullFlag = SM_FALSE;

	clist[LOM_SYSCOLUMNS_COLUMNNO_COLNO].colNo = LOM_SYSCOLUMNS_COLUMNNO_COLNO;
	clist[LOM_SYSCOLUMNS_COLUMNNO_COLNO].start = ALL_VALUE;
	clist[LOM_SYSCOLUMNS_COLUMNNO_COLNO].dataLength = sizeof(Two);
	clist[LOM_SYSCOLUMNS_COLUMNNO_COLNO].nullFlag = SM_FALSE;

	clist[LOM_SYSCOLUMNS_COLNAME_COLNO].colNo = LOM_SYSCOLUMNS_COLNAME_COLNO;
	clist[LOM_SYSCOLUMNS_COLNAME_COLNO].start = ALL_VALUE;
	clist[LOM_SYSCOLUMNS_COLNAME_COLNO].nullFlag = SM_FALSE;
	clist[LOM_SYSCOLUMNS_COLNAME_COLNO].data.ptr = &colName[0];
	clist[LOM_SYSCOLUMNS_COLNAME_COLNO].dataLength = LOM_MAXATTRNAME;
	/* length is set where the offet is calculated */

	clist[LOM_SYSCOLUMNS_COLOFFSET_COLNO].colNo = LOM_SYSCOLUMNS_COLOFFSET_COLNO;
	clist[LOM_SYSCOLUMNS_COLOFFSET_COLNO].start = ALL_VALUE;
	clist[LOM_SYSCOLUMNS_COLOFFSET_COLNO].dataLength =  sizeof(Four);
	clist[LOM_SYSCOLUMNS_COLOFFSET_COLNO].nullFlag =  SM_FALSE;

	clist[LOM_SYSCOLUMNS_INHERITEDFROM_COLNO].colNo = LOM_SYSCOLUMNS_INHERITEDFROM_COLNO;
	clist[LOM_SYSCOLUMNS_INHERITEDFROM_COLNO].start = ALL_VALUE;
	clist[LOM_SYSCOLUMNS_INHERITEDFROM_COLNO].dataLength =  sizeof(Four);
	clist[LOM_SYSCOLUMNS_INHERITEDFROM_COLNO].nullFlag =  SM_FALSE;

	clist[LOM_SYSCOLUMNS_DOMAINID_COLNO].colNo = LOM_SYSCOLUMNS_DOMAINID_COLNO;
	clist[LOM_SYSCOLUMNS_DOMAINID_COLNO].start = ALL_VALUE;
	clist[LOM_SYSCOLUMNS_DOMAINID_COLNO].nullFlag = SM_FALSE;

	clist[LOM_SYSCOLUMNS_ACTIVECOLUMNNO_COLNO].colNo = LOM_SYSCOLUMNS_ACTIVECOLUMNNO_COLNO;
	clist[LOM_SYSCOLUMNS_ACTIVECOLUMNNO_COLNO].start = ALL_VALUE;
	clist[LOM_SYSCOLUMNS_ACTIVECOLUMNNO_COLNO].dataLength = sizeof(Two);
	clist[LOM_SYSCOLUMNS_ACTIVECOLUMNNO_COLNO].nullFlag = SM_FALSE;
	

	nOldCol = 0;
	offset = 0;
	isFirstTextAttr = SM_TRUE;
	activeColNo = 0;
	nTextAttr = 0;

	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSCOLUMNS_CLASSNAME);
	if (orn < 0) LOM_ERROR(handle, orn);


	/* 
		3-2. calculate offset using LOM_SYSCOLUMNS_CLASSNAME index scan 
	*/
	relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);

	boundCond.op = SM_EQ;
	boundCond.key.len = sizeof(Four);
	bcopy(&classId,&(boundCond.key.val[0]),sizeof(Four));

	/* set lock up parameters */
	/* just for reading */
	lockup.mode = L_IS;
	lockup.duration = L_COMMIT;

	/* index scan of LOM_SYSCOLUMNS */
	osn = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), orn, &(LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[0].iid, &boundCond, &boundCond, 0, NULL, &lockup);
	if (osn < 0) LOM_ERROR(handle, osn);


	while((e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), osn, &tid, NULL))!=EOS) {
		if (e < 0) LOM_ERROR(handle, e);

		e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), osn, SM_TRUE, &tid, LOM_SYSCOLUMNS_NUM_COLS, &clist[0]);
		if (e < 0) CATALOG_ERROR(handle, e);

		if (GET_VALUE_FROM_COL_LIST(clist[LOM_SYSCOLUMNS_ACTIVECOLUMNNO_COLNO], sizeof(Two)) != NIL)
			activeColNo++;

		if (GET_VALUE_FROM_COL_LIST(clist[LOM_SYSCOLUMNS_COLTYPE_COLNO], sizeof(Two)) == SM_STRING) {
			offset += GET_VALUE_FROM_COL_LIST(clist[LOM_SYSCOLUMNS_COLOFFSET_COLNO], sizeof(offset));
		}
		else if (GET_VALUE_FROM_COL_LIST(clist[LOM_SYSCOLUMNS_COLTYPE_COLNO], sizeof(Two)) == LOM_TEXT) {
			offset += sizeof(LOM_IndexDesc);
			if (GET_VALUE_FROM_COL_LIST(clist[LOM_SYSCOLUMNS_ACTIVECOLUMNNO_COLNO], sizeof(Two)) != NIL)
			{
				isFirstTextAttr = SM_FALSE;
				nTextAttr++;
			}
		}
		else if(GET_VALUE_FROM_COL_LIST(clist[LOM_SYSCOLUMNS_COMPLEXTYPE_COLNO], sizeof(Two)) == LOM_COMPLEXTYPE_ODMG_COLLECTIONSET ||
				GET_VALUE_FROM_COL_LIST(clist[LOM_SYSCOLUMNS_COMPLEXTYPE_COLNO], sizeof(Two)) == LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG ||
				GET_VALUE_FROM_COL_LIST(clist[LOM_SYSCOLUMNS_COMPLEXTYPE_COLNO], sizeof(Two)) == LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST ||
				GET_VALUE_FROM_COL_LIST(clist[LOM_SYSCOLUMNS_COMPLEXTYPE_COLNO], sizeof(Two)) == LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY) 
		{
			offset += sizeof(ODMG_CollectionDesc);
		}
		else if (GET_VALUE_FROM_COL_LIST(clist[LOM_SYSCOLUMNS_COLTYPE_COLNO], sizeof(Two)) == SM_SHORT) {
			if(offset % SM_SHORT_SIZE) offset = offset + SM_SHORT_SIZE - (offset%SM_SHORT_SIZE);
			offset += SM_SHORT_SIZE;
		}
		else if (GET_VALUE_FROM_COL_LIST(clist[LOM_SYSCOLUMNS_COLTYPE_COLNO], sizeof(Two)) == SM_INT) {
			if(offset % SM_INT_SIZE) offset = offset + SM_INT_SIZE - (offset%SM_INT_SIZE);
			offset += SM_INT_SIZE;
		}
		else if (GET_VALUE_FROM_COL_LIST(clist[LOM_SYSCOLUMNS_COLTYPE_COLNO], sizeof(Two)) == SM_LONG) {
			if(offset % SM_LONG_SIZE) offset = offset + SM_LONG_SIZE - (offset%SM_LONG_SIZE);
			offset += SM_LONG_SIZE;
		}
		else if (GET_VALUE_FROM_COL_LIST(clist[LOM_SYSCOLUMNS_COLTYPE_COLNO], sizeof(Two)) == SM_LONG_LONG) {
			if(offset % SM_LONG_LONG_SIZE) offset = offset + SM_LONG_LONG_SIZE - (offset%SM_LONG_LONG_SIZE);
			offset += SM_LONG_LONG_SIZE;
		}
		else if (GET_VALUE_FROM_COL_LIST(clist[LOM_SYSCOLUMNS_COLTYPE_COLNO], sizeof(Two)) == SM_FLOAT) {
			if(offset % SM_FLOAT_SIZE) offset = offset + SM_FLOAT_SIZE - (offset%SM_FLOAT_SIZE);
			offset += SM_FLOAT_SIZE;
		}
		else if (GET_VALUE_FROM_COL_LIST(clist[LOM_SYSCOLUMNS_COLTYPE_COLNO], sizeof(Two)) == SM_DOUBLE) {
			if(offset % SM_DOUBLE_SIZE) offset = offset + SM_DOUBLE_SIZE - (offset%SM_DOUBLE_SIZE);
			offset += SM_DOUBLE_SIZE;
		}
		else if (GET_VALUE_FROM_COL_LIST(clist[LOM_SYSCOLUMNS_COLTYPE_COLNO], sizeof(Two)) == SM_MBR) {
			if(offset % SM_MBR_SIZE) offset = offset + SM_MBR_SIZE - (offset%SM_MBR_SIZE);
			offset += SM_MBR_SIZE;
		}
		else if (GET_VALUE_FROM_COL_LIST(clist[LOM_SYSCOLUMNS_COLTYPE_COLNO], sizeof(Two)) == SM_OID || 
				GET_VALUE_FROM_COL_LIST(clist[LOM_SYSCOLUMNS_COLTYPE_COLNO], sizeof(Two)) == ODYS_REF || 
				GET_VALUE_FROM_COL_LIST(clist[LOM_SYSCOLUMNS_COLTYPE_COLNO], sizeof(Two)) == ODYS_LINK) {
			if(offset % SM_LONG_SIZE) offset = offset + SM_LONG_SIZE - (offset%SM_LONG_SIZE);
			offset += LOM_OBJREF_SIZE;
		}
		else {    
			if(offset % sizeof(void *)) offset = offset + sizeof(void *) - (offset%sizeof(void *));
			offset += sizeof(void *);
		}
		nOldCol++;
	}


	/* close scan */
	e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), osn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	/*
		3-3. add column info into LOM_SYSCOLUMNS_CLASSNAME table
	*/
	for (i = 0; i < nAddCol; i++) {
		ASSIGN_VALUE_TO_COL_LIST(clist[LOM_SYSCOLUMNS_COLUMNNO_COLNO], nOldCol + i, sizeof(Two));
		ASSIGN_VALUE_TO_COL_LIST(clist[LOM_SYSCOLUMNS_COMPLEXTYPE_COLNO], addColInfo[i].complexType, sizeof(Two));
		ASSIGN_VALUE_TO_COL_LIST(clist[LOM_SYSCOLUMNS_COLTYPE_COLNO], addColInfo[i].type, sizeof(Two));
		clist[LOM_SYSCOLUMNS_COLNAME_COLNO].dataLength = strlen(addColInfo[i].name) +1;
		clist[LOM_SYSCOLUMNS_COLNAME_COLNO].data.ptr = addColInfo[i].name;
		ASSIGN_VALUE_TO_COL_LIST(clist[LOM_SYSCOLUMNS_COLOFFSET_COLNO], offset, sizeof(Four));
		clist[LOM_SYSCOLUMNS_DOMAINID_COLNO].dataLength = LOM_LONG_SIZE_VAR;
		ASSIGN_VALUE_TO_COL_LIST(clist[LOM_SYSCOLUMNS_DOMAINID_COLNO], addColInfo[i].domain, sizeof(Four));
		ASSIGN_VALUE_TO_COL_LIST(clist[LOM_SYSCOLUMNS_ACTIVECOLUMNNO_COLNO], activeColNo, sizeof(Two));
		activeColNo++;
		if (GET_VALUE_FROM_COL_LIST(clist[LOM_SYSCOLUMNS_COLTYPE_COLNO], sizeof(Two)) == LOM_TEXT)
			nTextAttr++;

		if (addColInfo[i].inheritedFrom == LOM_INHERITEDFROM_THIS_CLASS) 
			ASSIGN_VALUE_TO_COL_LIST(clist[LOM_SYSCOLUMNS_INHERITEDFROM_COLNO], classId, sizeof(Four));
		else
			ASSIGN_VALUE_TO_COL_LIST(clist[LOM_SYSCOLUMNS_INHERITEDFROM_COLNO], addColInfo[i].inheritedFrom, sizeof(Four));

		/* calcultate the offset of next attribute */
		if (addColInfo[i].type == SM_STRING) {
			offset += addColInfo[i].length;
		}
		else if (addColInfo[i].type == LOM_TEXT) {
			offset += sizeof(LOM_TextDesc);
		}
		else if(addColInfo[i].complexType == LOM_COMPLEXTYPE_ODMG_COLLECTIONSET ||
				addColInfo[i].complexType == LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG ||
				addColInfo[i].complexType == LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST ||
				addColInfo[i].complexType == LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY) 
		{
			offset += sizeof(ODMG_CollectionDesc);
		}
		else if (addColInfo[i].type == SM_SHORT) {
			if(offset % SM_SHORT_SIZE) offset = offset + SM_SHORT_SIZE - (offset%SM_SHORT_SIZE);
			ASSIGN_VALUE_TO_COL_LIST(clist[LOM_SYSCOLUMNS_COLOFFSET_COLNO], offset, sizeof(Four));
			offset += SM_SHORT_SIZE;
		}
		else if (addColInfo[i].type == SM_INT) {
			if(offset % SM_INT_SIZE) offset = offset + SM_INT_SIZE - (offset%SM_INT_SIZE);
			offset += SM_INT_SIZE;
		}
		else if (addColInfo[i].type == SM_LONG) {
			if(offset % SM_LONG_SIZE) offset = offset + SM_LONG_SIZE - (offset%SM_LONG_SIZE);
			offset += SM_LONG_SIZE;
		}
		else if (addColInfo[i].type == SM_LONG_LONG) {
			if(offset % SM_LONG_LONG_SIZE) offset = offset + SM_LONG_LONG_SIZE - (offset%SM_LONG_LONG_SIZE);
			offset += SM_LONG_LONG_SIZE;
		}
		else if (addColInfo[i].type == SM_FLOAT) {
			if(offset % SM_FLOAT_SIZE) offset = offset + SM_FLOAT_SIZE - (offset%SM_FLOAT_SIZE);
			offset += SM_FLOAT_SIZE;
		}
		else if (addColInfo[i].type == SM_DOUBLE) {
			if(offset % SM_DOUBLE_SIZE) offset = offset + SM_DOUBLE_SIZE - (offset%SM_DOUBLE_SIZE);
			offset += SM_DOUBLE_SIZE;
		}
		else if (addColInfo[i].type == SM_MBR) {
			if(offset % SM_MBR_SIZE) offset = offset + SM_MBR_SIZE - (offset%SM_MBR_SIZE);
			ASSIGN_VALUE_TO_COL_LIST(clist[LOM_SYSCOLUMNS_COLOFFSET_COLNO], offset, sizeof(Four));
			offset += SM_MBR_SIZE;
		}
		else if (addColInfo[i].type == SM_OID || addColInfo[i].type == ODYS_REF || addColInfo[i].type == ODYS_LINK) {
			if(offset % SM_LONG_SIZE) offset = offset + SM_LONG_SIZE - (offset%SM_LONG_SIZE);
			ASSIGN_VALUE_TO_COL_LIST(clist[LOM_SYSCOLUMNS_COLOFFSET_COLNO], offset, sizeof(Four));
			offset += LOM_OBJREF_SIZE;
		}
		else {      /* SM_VARSTRING */ /* i.e. pointer to data */
			if(offset % sizeof(void *)) offset = offset + sizeof(void *) - (offset%sizeof(void *));
			ASSIGN_VALUE_TO_COL_LIST(clist[LOM_SYSCOLUMNS_COLOFFSET_COLNO], offset, sizeof(Four));
			offset += sizeof(void *);
		}

		e = LRDS_CreateTuple(LOM_GET_LRDS_HANDLE(handle), orn, SM_FALSE, LOM_SYSCOLUMNS_NUM_COLS, &(clist[0]), (TupleID*)&tid);
		if (e < 0) LOM_ERROR(handle, e);

		if(addColInfo[i].type == LOM_TEXT) {
			if(isFirstTextAttr) {
				LOM_IndexDesc idesc;

				/* create content table for storing text type attribute */
				e = lom_Text_CreateContentTable(handle, volId, className);
				if(e < eNOERROR) LOM_ERROR(handle, e);

				/* create index on 'logical id' column */
				idesc.indexType = SM_INDEXTYPE_BTREE;
				idesc.kinfo.btree.flag = 0;
				/*
				KEYFLAG_UNIQUE;
				*/
				idesc.kinfo.btree.nColumns = 1;
				idesc.kinfo.btree.columns[0].colNo = -1; /* logical id column */
				idesc.kinfo.btree.columns[0].flag = KEYINFO_COL_ASC;

				lom_Text_MakeLogicalIdIndexName(handle, volId, className, logicalIdIndexName);

				/* add index on logical-id column */
				e = LOM_AddIndex(handle, volId, className, logicalIdIndexName, &idesc, &logical_id_index);
				if( e < eNOERROR) LOM_ERROR(handle, e);

				/* create logical counter */
				e = lom_Text_CreateCounter(handle, volId, className, 0);
				if( e < eNOERROR) LOM_ERROR(handle, e);

				/* make isFirstTextAttr be false */
				isFirstTextAttr = SM_FALSE;
			}

			/* create inverted index table */
			e = lom_Text_CreateInvertedIndexTable(handle, volId, className, addColInfo[i].name, i+nOldCol);
			if(e < eNOERROR) LOM_ERROR(handle, e);

			/* create docId index table */
			e = lom_Text_CreateDocIdIndexTable(handle, volId, className, addColInfo[i].name, i+nOldCol); 
			if(e < eNOERROR) LOM_ERROR(handle, e);

			/* add inverted index info. to catalog */
			e = lom_Text_AddIndexInfoIntoCatalog(handle, volId, className, addColInfo[i].name, i+nOldCol);
			if(e < eNOERROR) LOM_ERROR(handle, e);
		}
	}

	/* Close the data file */
	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if (e < 0) LOM_ERROR(handle, e);

	for(i = 0; i< nAddCol; i++) {
		if(addColInfo[i].type == LOM_TEXT) {

			e = LOM_Text_SetFilter(handle, volId, classId, GET_USERLEVEL_COLNO(i+nOldCol), LOM_DEFAULT_FILTER_NO);
			if(e < eNOERROR) LOM_ERROR(handle, e);

			e = LOM_Text_SetKeywordExtractor(handle, volId, classId, GET_USERLEVEL_COLNO(i+nOldCol), LOM_DEFAULT_KEYWORDEXTRACTOR_NO);
			if(e < eNOERROR) LOM_ERROR(handle, e);

			e = LOM_Text_SetStemizer(handle, volId, classId, GET_USERLEVEL_COLNO(i+nOldCol), LOM_DEFAULT_STEMIZER_NO);
			if(e < eNOERROR) LOM_ERROR(handle, e);
		}
	}



	/*
		4. Delete columns 
	*/
	if (nDropCol > 0)
	{
		orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSCOLUMNS_CLASSNAME);
		if (orn < 0) LOM_ERROR(handle, orn);

		relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);

		boundCond.op = SM_EQ;
		boundCond.key.len = sizeof(Four);
		bcopy(&classId,&(boundCond.key.val[0]),sizeof(Four));

		/* set lock up parameters */
		/* just for reading */
		lockup.mode = L_IX;
		lockup.duration = L_COMMIT;

		/* index scan of LOM_SYSCOLUMNS */
		osn = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), orn, &(LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[0].iid, &boundCond, &boundCond, 0, NULL, &lockup);
		if (osn < 0) LOM_ERROR(handle, osn);

		activeColNo = 0;
		clist[LOM_SYSCOLUMNS_COLNAME_COLNO].data.ptr = &colName[0];
		clist[LOM_SYSCOLUMNS_COLNAME_COLNO].dataLength = LOM_MAXATTRNAME;

		while((e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), osn, &tid, NULL))!=EOS) {
			if (e < 0) LOM_ERROR(handle, e);

			e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), osn, SM_TRUE, &tid, LOM_SYSCOLUMNS_NUM_COLS, &clist[0]);
			if (e < 0) CATALOG_ERROR(handle, e);

			isDeleted = SM_FALSE;
			for (j = 0; j < nDropCol; j++)
			{
				if (GET_VALUE_FROM_COL_LIST(clist[LOM_SYSCOLUMNS_ACTIVECOLUMNNO_COLNO], sizeof(Two)) != NIL &&
					strncmp((char*)(clist[LOM_SYSCOLUMNS_COLNAME_COLNO].data.ptr), 
							dropColInfo[j].name, LOM_MAXATTRNAME) == 0)
				{
					isDeleted = SM_TRUE;
					break;
				}
			}

			if (isDeleted == SM_TRUE)
			{
				/* mark deleted column */
				ASSIGN_VALUE_TO_COL_LIST(clist[LOM_SYSCOLUMNS_ACTIVECOLUMNNO_COLNO], NIL, sizeof(Two));

				if(GET_VALUE_FROM_COL_LIST(clist[LOM_SYSCOLUMNS_COLTYPE_COLNO], sizeof(Two)) == LOM_TEXT) 
				{
					if(nTextAttr == 1) 
					{
						/* destroy content table for storing text type attribute */
						e = lom_Text_DestroyContentTable(handle, volId, className);
						if(e < eNOERROR) LOM_ERROR(handle, e);

						/* destroy logical counter */
						e = lom_Text_DestroyCounter(handle, volId, className);
						if(e < eNOERROR) LOM_ERROR(handle, e);
					}
					nTextAttr--;

					/* DESTROY INVERTED-INDEX TABLE */
					e = lom_Text_DestroyInvertedIndexTable(handle, volId, className, dropColInfo[j].name);
					if(e < eNOERROR) LOM_ERROR(handle, e);

					/* DESTROY DOC-ID INDEX TABLE */
					e = lom_Text_DestroyDocIdIndexTable(handle, volId, className, dropColInfo[j].name);
					if(e < eNOERROR) LOM_ERROR(handle, e);

					/* DROP INERTED-INDEX FROM CATALOG */
					e = lom_Text_DropIndexInfoFromCatalog(handle, volId, className, dropColInfo[j].name);
					if(e < eNOERROR) LOM_ERROR(handle, e);

					colNo = GET_USERLEVEL_COLNO(clist[LOM_SYSCOLUMNS_COLUMNNO_COLNO].data.s);

					/* Reset filter entry */
					e = LOM_Text_ResetFilter(handle, volId, classId, colNo);
					if(e != eNOSUCHFILTEREXIST_LOM && e < eNOERROR) LOM_ERROR(handle, e);

					/* Reset keyword extractor entry */
					e = LOM_Text_ResetKeywordExtractor(handle, volId, classId, colNo);
					if(e != eNOSUCHKEYWORDEXTRACTOREXIST_LOM && e < eNOERROR) LOM_ERROR(handle, e);

					/* Reset stemizer entry */
					e = LOM_Text_ResetStemizer(handle, volId, classId, colNo);
					if(e != eNOSUCHSTEMIZEREXIST_LOM && e < eNOERROR) LOM_ERROR(handle, e);
				}
			}
			else if (GET_VALUE_FROM_COL_LIST(clist[LOM_SYSCOLUMNS_ACTIVECOLUMNNO_COLNO], sizeof(Two)) == NIL) 
			{
				;
			}
			else
			{
				/* assign active column no */
				ASSIGN_VALUE_TO_COL_LIST(clist[LOM_SYSCOLUMNS_ACTIVECOLUMNNO_COLNO], activeColNo, sizeof(Two));
				activeColNo++;
			}

			e = LRDS_UpdateTuple(LOM_GET_LRDS_HANDLE(handle), osn, SM_TRUE, &tid, 1, &clist[LOM_SYSCOLUMNS_ACTIVECOLUMNNO_COLNO]);
			if(e < eNOERROR) LOM_ERROR(handle, e);
		}


		/* close scan */
		e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), osn);
		if(e < eNOERROR) LOM_ERROR(handle, e);

		/* close relation */
		e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
		if(e < eNOERROR) LOM_ERROR(handle, e);
	}


	/* 
		5. update LOM-level in-memory catalog info 
	*/
	v = Catalog_GetVolIndex(handle, volId);
	if(v < eNOERROR) CATALOG_ERROR(handle, v);

	e = Catalog_GetClassInfo(handle, volId, classId, &idxForClassInfo);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	tmp_sysClasses = &CATALOG_GET_CLASSINFOTBL(handle, v)[idxForClassInfo];

	e = catalog_RefreshClassInfoForAlterClass(handle, volId, classId, className, tmp_sysClasses);
	if(e < eNOERROR) LOM_ERROR(handle, e);


	return(eNOERROR);
} /* LOM_AlterClass() */



Four catalog_RefreshClassInfoForAlterClass(
	LOM_Handle						*handle, 
	Four							volId,
	Four							classId,				/* IN: class Id */
	char							*className,				/* IN relation name to create */
	catalog_SysClassesOverlay		*tmp_sysClasses)
{
	Two								keyLen;
	Two								curColNo;
	Four							i, j;
	Four							e;
	Four							v;
	Four							ocn;
	Four							ocn2;
	Four							orn3;
	Four							catScanId; /* catalog scan Id */
	Four							nCols; /* number of columns */
	char							colName[LOM_MAXATTRNAME];
	LockParameter					lockup;
	ColListStruct					clist[20];    /* column list */
	BoundCond						boundCond;        /* boundary condition */
	lrds_RelTableEntry				*relTableEntry;
	lrds_RelTableEntry				*relTableEntry2;
	catalog_SysAttributesOverlay	*tmp_Adesc;
	catalog_SysIndexesOverlay		*tmp_ii;
	PostingStructureInfo			postingInfo;


	/* Check whether the volume is already mounted. */
	for (v = 0; v < CATALOG_MAXNUMOFVOLS; v++)
		if (LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].volId == volId) {
			break;
		}

	if (v == CATALOG_MAXNUMOFVOLS) return eVOLUMNNOTMOUNTED_CATALOG;


	/* we open class here */
	ocn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, &className[0]);
	if (ocn < 0) CATALOG_ERROR(handle, ocn);

	/* set lock up parameters */
	/* just for reading */
	lockup.mode = L_IX;
	lockup.duration = L_COMMIT;

	relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), ocn);

	CATALOG_GET_ATTRNUM(tmp_sysClasses) = relTableEntry->ri.nColumns;
	CATALOG_GET_VARATTRNUM(tmp_sysClasses) = relTableEntry->ri.nVarColumns;
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
		clist[LOM_SYSCOLUMNS_COLOFFSET_COLNO].dataLength = sizeof(Four);

		clist[LOM_SYSCOLUMNS_COLNAME_COLNO].data.ptr = &colName[0];

		nCols = CATALOG_GET_ATTRNUM(tmp_sysClasses); /* temporary copy */
		while((e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, (TupleID*)NULL, NULL))!=EOS) {
			if (e < 0) CATALOG_ERROR(handle, e);

			e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, SM_TRUE, NULL, LOM_SYSCOLUMNS_NUM_COLS, &clist[0]);
			if (e < 0) CATALOG_ERROR(handle, e);

			/* currenct column number */
			curColNo = GET_VALUE_FROM_COL_LIST(clist[LOM_SYSCOLUMNS_COLUMNNO_COLNO], sizeof(curColNo));
			tmp_Adesc[curColNo].colNo = curColNo; 
			tmp_Adesc[curColNo].offset = GET_VALUE_FROM_COL_LIST(clist[LOM_SYSCOLUMNS_COLOFFSET_COLNO], sizeof(tmp_Adesc[curColNo].offset));
			tmp_Adesc[curColNo].inheritedFrom = GET_VALUE_FROM_COL_LIST(clist[LOM_SYSCOLUMNS_INHERITEDFROM_COLNO], sizeof(tmp_Adesc[curColNo].inheritedFrom));
			bcopy(&colName[0],tmp_Adesc[curColNo].name,clist[LOM_SYSCOLUMNS_COLNAME_COLNO].retLength);
			tmp_Adesc[curColNo].name[clist[LOM_SYSCOLUMNS_COLNAME_COLNO].retLength] = '\0';
			tmp_Adesc[curColNo].type = GET_VALUE_FROM_COL_LIST(clist[LOM_SYSCOLUMNS_COLTYPE_COLNO], sizeof(tmp_Adesc[curColNo].type));
			tmp_Adesc[curColNo].varColNo = (LRDS_GET_COLDESC_FROM_RELTABLE_ENTRY(relTableEntry))[curColNo].varColNo;
			tmp_Adesc[curColNo].length =  (LRDS_GET_COLDESC_FROM_RELTABLE_ENTRY(relTableEntry))[curColNo].length;
			tmp_Adesc[curColNo].domain = GET_VALUE_FROM_COL_LIST(clist[LOM_SYSCOLUMNS_DOMAINID_COLNO], sizeof(tmp_Adesc[curColNo].domain));
			tmp_Adesc[curColNo].activeColNo = GET_VALUE_FROM_COL_LIST(clist[LOM_SYSCOLUMNS_ACTIVECOLUMNNO_COLNO], sizeof(tmp_Adesc[curColNo].activeColNo));
			if (tmp_Adesc[curColNo].activeColNo == NIL)
				CATALOG_GET_ACTIVEATTRNUM(tmp_sysClasses)--;
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

		} /* else */
		e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), catScanId);
		if(e < eNOERROR) CATALOG_ERROR(handle, e);

	} /* if */
	else CATALOG_GET_INDEXINFOTBL_INDEX(tmp_sysClasses) = -1;

	/* close relation */
	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), ocn2);
	if(e < eNOERROR) CATALOG_ERROR(handle, e);

	if(j != CATALOG_GET_INDEXNUM(tmp_sysClasses)) CATALOG_ERROR(handle, eINTERNAL_CATALOG);

	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), ocn);
	if (e < 0) CATALOG_ERROR(handle, e);


	return eNOERROR;
}



Four lom_ReloadClassForAlterClass(
	LOM_Handle *handle, 
	Four volId,         /* IN volume ID */
	char *className)      /* IN classname to open */
{
	Four e;
	Four orn;
	Four i;
	Four idxForClassInfo;
	catalog_SysClassesOverlay *ptrToSysClasses;
	catalog_SysAttributesOverlay *ptrToSysAttributes;
	Four numOfTextAttrs;
	Boolean isFirstTextAttr;
	Four j;
	Four v;
	LockParameter lockup;

	j = 0;

	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId,className);
	if(orn < eNOERROR) LOM_ERROR(handle, orn);

	/* check if this class is temporary or not */
	if(LRDS_IS_TEMPORARY_RELATION(LOM_GET_LRDS_HANDLE(handle), orn))
	{
		e = Catalog_GetTemporaryClassId(handle, volId, className, &(LOM_USEROPENCLASSTABLE(handle)[orn].classID));
		if(e<0) LOM_ERROR(handle, e);
	}
	else 
	{
		/* 
		   check if this class has alread been opened
		*/
		if(LOM_USEROPENCLASSTABLE(handle)[orn].count == 0) 
		{
			e = lom_GetClassId(handle, volId, className, &(LOM_USEROPENCLASSTABLE(handle)[orn].classID));
			if(e<0) LOM_ERROR(handle, e);
		}
	}

	/* increase count */
	/* LOM_USEROPENCLASSTABLE(handle)[orn].count++; */

	/* if(LOM_USEROPENCLASSTABLE(handle)[orn].count > 1) return orn; */

	/* if this class is temporary class */
	if(LOM_USEROPENCLASSTABLE(handle)[orn].classID == 0) return eNOERROR;

	/* check if text attributes exist */
	e = Catalog_GetClassInfo(handle, volId, LOM_USEROPENCLASSTABLE(handle)[orn].classID, &idxForClassInfo);
	if( e < eNOERROR) LOM_ERROR(handle, e);

	v = Catalog_GetVolIndex(handle, volId);
    if(v < eNOERROR) LOM_ERROR(handle, v);

	ptrToSysClasses = &CATALOG_GET_CLASSINFOTBL(handle, v)[idxForClassInfo];

	ptrToSysAttributes =  &CATALOG_GET_ATTRINFOTBL(handle, v)[CATALOG_GET_ATTRINFOTBL_INDEX(ptrToSysClasses)];

	numOfTextAttrs = 0;
	isFirstTextAttr = SM_TRUE;
	j = 0;

	/* set numOfTextAttrs variable to 0 */
	LOM_USEROPENCLASSTABLE(handle)[orn].numOfTextAttrs				  = 0;
	LOM_USEROPENCLASSTABLE(handle)[orn].numOfodmgCollAttrs			  = 0; 
	LOM_USEROPENCLASSTABLE(handle)[orn].lrdsScanIdForTextScan		  = NIL;
	LOM_USEROPENCLASSTABLE(handle)[orn].contentTableScanIdForTextScan = NIL;
	for(i = 0; i < CATALOG_GET_ATTRNUM(ptrToSysClasses); i++) 
	{ 
		if(ptrToSysAttributes[i].type == LOM_TEXT) 
		{
			LOM_USEROPENCLASSTABLE(handle)[orn].numOfTextAttrs++;

			if(isFirstTextAttr) 
			{
				/* open content table */
				LOM_USEROPENCLASSTABLE(handle)[orn].ornForContentTable = 
					e = lom_Text_OpenContentClass(handle, volId, className);
				if(e < eNOERROR) LOM_ERROR(handle, e);

				/* get counter id */
				e = lom_Text_GetCounterId(handle, volId, className, &(LOM_USEROPENCLASSTABLE(handle)[orn].counterId));
				if(e < eNOERROR) LOM_ERROR(handle, e);

				/* open lrds level sequential scan and content table */
				lockup.mode     = L_IS;
				lockup.duration = L_COMMIT;
				LOM_USEROPENCLASSTABLE(handle)[orn].lrdsScanIdForTextScan = 
					e = LRDS_OpenSeqScan(LOM_GET_LRDS_HANDLE(handle), orn, FORWARD, 0, NULL, &lockup);
				if(e < eNOERROR) LOM_ERROR(handle, e);

				LOM_USEROPENCLASSTABLE(handle)[orn].contentTableScanIdForTextScan = 
					e = LRDS_OpenSeqScan(LOM_GET_LRDS_HANDLE(handle), LOM_USEROPENCLASSTABLE(handle)[orn].ornForContentTable, FORWARD, 0, NULL, &lockup);
				if(e < eNOERROR) LOM_ERROR(handle, e);
			}
			if(LOM_USEROPENCLASSTABLE(handle)[orn].ornForContentTable < eNOERROR) LOM_ERROR(handle, LOM_USEROPENCLASSTABLE(handle)[orn].ornForContentTable);
			isFirstTextAttr = SM_FALSE;

			LOM_USEROPENCLASSTABLE(handle)[orn].textColNo[j] = (Two)i;

			/* open inverted index table */
			LOM_USEROPENCLASSTABLE(handle)[orn].ornForInvertedIndexTable[j] = 
				lom_Text_OpenInvertedIndexTable(handle, volId, className, ptrToSysAttributes[i].name);

			/* open inverted index table */
			LOM_USEROPENCLASSTABLE(handle)[orn].ornForDocIdIndexTable[j] = 
				lom_Text_OpenDocIdIndexTable(handle, volId, className, ptrToSysAttributes[i].name);

			/* we donot link a keyword shared object dynamically until it will be actually used */
#ifdef PRELOAD_KEYWORDEXTRACTOR
			/* get a function pointer to keyword extractor */
			e = LOM_Text_OpenHandleForKeywordExtractor(handle, 
				volId, 
				LOM_USEROPENCLASSTABLE(handle)[orn].classID,
				GET_USERLEVEL_COLNO(i),	/* user-level function call */
				&LOM_USEROPENCLASSTABLE(handle)[orn].handleForDLOfKeywordExtractor[j],
				&LOM_USEROPENCLASSTABLE(handle)[orn].fptrToKeywordExtractor[j],
				&LOM_USEROPENCLASSTABLE(handle)[orn].fptrToGettingNextPostingInfo[j],
				&LOM_USEROPENCLASSTABLE(handle)[orn].fptrToFinalizeKeywordExtraction[j]);
			if(e < eNOERROR) LOM_ERROR(handle, e);
#else
			LOM_USEROPENCLASSTABLE(handle)[orn].handleForDLOfKeywordExtractor[j] = NULL;
			LOM_USEROPENCLASSTABLE(handle)[orn].fptrToKeywordExtractor[j] = NULL;
#endif

			/* we donot link a filter shared object dynamically until it will be actually used */
#ifdef PRELOAD_FILTER
			/* get a function pointer to filter */
			e = LOM_Text_OpenHandleForFilter(handle, 
				volId, 
				LOM_USEROPENCLASSTABLE(handle)[orn].classID,
				GET_USERLEVEL_COLNO(i),	/* user-level function call */
				&LOM_USEROPENCLASSTABLE(handle)[orn].handleForDLOfFilter[j],
				&LOM_USEROPENCLASSTABLE(handle)[orn].fptrToFilter[j]);
			if(e < eNOERROR) LOM_ERROR(handle, e);
#else
			LOM_USEROPENCLASSTABLE(handle)[orn].handleForDLOfFilter[j] = NULL;
			LOM_USEROPENCLASSTABLE(handle)[orn].fptrToFilter[j] = NULL;
#endif

#ifdef PRELOAD_STEMIZER
			/* get a function pointer to filter */
			e = LOM_Text_OpenHandleForStemizer(handle, 
				volId, 
				LOM_USEROPENCLASSTABLE(handle)[orn].classID,
				GET_USERLEVEL_COLNO(i),	/* user-level function call */
				&LOM_USEROPENCLASSTABLE(handle)[orn].handleForDLOfStemizer[j],
				&LOM_USEROPENCLASSTABLE(handle)[orn].fptrToStemizer[j]);
			if(e < eNOERROR) 
			{
				LOM_PTRERROR(handle, e);
				LOM_USEROPENCLASSTABLE(handle)[orn].handleForDLOfStemizer[j] = NULL;
				LOM_USEROPENCLASSTABLE(handle)[orn].fptrToStemizer[j] = NULL;
			}
#else
			LOM_USEROPENCLASSTABLE(handle)[orn].handleForDLOfStemizer[j] = NULL;
			LOM_USEROPENCLASSTABLE(handle)[orn].fptrToStemizer[j] = NULL;
#endif
			j++;

		}
        else if(ptrToSysAttributes[i].complexType == LOM_COMPLEXTYPE_ODMG_COLLECTIONSET ||
                ptrToSysAttributes[i].complexType == LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG ||
                ptrToSysAttributes[i].complexType == LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST ||
                ptrToSysAttributes[i].complexType == LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY)
        {
			int   index;
            		char  dataRelationName[LOM_MAXCLASSNAME];
            
            		index = LOM_USEROPENCLASSTABLE(handle)[orn].numOfodmgCollAttrs;

			switch(ptrToSysAttributes[i].complexType)
			{
			case LOM_COMPLEXTYPE_ODMG_COLLECTIONSET:
				strcpy(dataRelationName, LOM_ODMG_COLLECTION_SET_PREFIX); 
				break;
			case LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG:
				strcpy(dataRelationName, LOM_ODMG_COLLECTION_BAG_PREFIX); 
				break;
			case LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST:
				strcpy(dataRelationName, LOM_ODMG_COLLECTION_LIST_PREFIX); 
				break;
			case LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY:
				strcpy(dataRelationName, LOM_ODMG_COLLECTION_VARARRAY_PREFIX); 
				break;
			}

			switch(ptrToSysAttributes[i].type)
            {
            case LOM_SHORT:
				strcat(dataRelationName, LOM_ODMG_COLLECTION_SHORT_POSTFIX);
				break;
			case LOM_INT:
				strcat(dataRelationName, LOM_ODMG_COLLECTION_INT_POSTFIX);
				break;
			case LOM_LONG:
				strcat(dataRelationName, LOM_ODMG_COLLECTION_LONG_POSTFIX);
				break;
			case LOM_LONG_LONG:
				strcat(dataRelationName, LOM_ODMG_COLLECTION_LONG_LONG_POSTFIX);
				break;
			case LOM_FLOAT:
				strcat(dataRelationName, LOM_ODMG_COLLECTION_FLOAT_POSTFIX);
				break;
			case LOM_DOUBLE:
				strcat(dataRelationName, LOM_ODMG_COLLECTION_DOUBLE_POSTFIX);
				break;
			case LOM_STRING:
				strcat(dataRelationName, LOM_ODMG_COLLECTION_STRING_POSTFIX);
				break;
			case LOM_VARSTRING:
				strcat(dataRelationName, LOM_ODMG_COLLECTION_VARSTRING_POSTFIX);
				break;
			case LOM_PAGEID:
				strcat(dataRelationName, LOM_ODMG_COLLECTION_PAGEID_POSTFIX);
				break;
			case LOM_FILEID:
				strcat(dataRelationName, LOM_ODMG_COLLECTION_FILEID_POSTFIX);
				break;
			case LOM_INDEXID:
				strcat(dataRelationName, LOM_ODMG_COLLECTION_INDEXID_POSTFIX);
				break;
			case LOM_OID:
				strcat(dataRelationName, LOM_ODMG_COLLECTION_OID_POSTFIX);
				break;
			case LOM_MBR:
				strcat(dataRelationName, LOM_ODMG_COLLECTION_MBR_POSTFIX);
				break;
			case LOM_REF:
				strcat(dataRelationName, LOM_ODMG_COLLECTION_REF_POSTFIX);
				break;
			case LOM_LINK:
				strcat(dataRelationName, LOM_ODMG_COLLECTION_LINK_POSTFIX);
				break;
			case LOM_TEXT:
				strcat(dataRelationName, LOM_ODMG_COLLECTION_TEXT_POSTFIX);
				break;
			case LOM_DATE:
				strcat(dataRelationName, LOM_ODMG_COLLECTION_DATE_POSTFIX);
				break;
			case LOM_TIME:
				strcat(dataRelationName, LOM_ODMG_COLLECTION_TIME_POSTFIX);
				break;
			case LOM_TIMESTAMP:
				strcat(dataRelationName, LOM_ODMG_COLLECTION_TIMESTAMP_POSTFIX);
				break;
			case LOM_INTERVAL:
				strcat(dataRelationName, LOM_ODMG_COLLECTION_INTERVAL_POSTFIX);
				break;
            }            
            LOM_USEROPENCLASSTABLE(handle)[orn].numOfodmgCollAttrs++;
            LOM_USEROPENCLASSTABLE(handle)[orn].odmgCollColNo[index] = i;
            LOM_USEROPENCLASSTABLE(handle)[orn].odmgCollDataOrn[index] = e = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, dataRelationName);
            if(e < 0) LOM_ERROR(handle, e);
        }
	}/* for */

	LOM_USEROPENCLASSTABLE(handle)[orn].textColNo[LOM_USEROPENCLASSTABLE(handle)[orn].numOfTextAttrs] = NIL;
	LOM_USEROPENCLASSTABLE(handle)[orn].volId = (VolID)volId;

	/* open deletion list table and get tid of it's first tuple -> lom_AppendObjectToDeferredDeletionList에서 open한다. */
	LOM_USEROPENCLASSTABLE(handle)[orn].ornForDeletionListTable = NIL;


	orn = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if(orn < eNOERROR) LOM_ERROR(handle, orn);

	return eNOERROR;
}

