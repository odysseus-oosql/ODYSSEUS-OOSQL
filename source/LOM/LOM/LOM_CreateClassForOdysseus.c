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
 * Module: LOM_CreateClass.c
 *
 * Description:
 *  Create a Class.
 *
 * Imports:
 *  SM_CreateFile()
 *
 * Exports:
 *  Four LOM_CreateClass( Four ,char *, LRDS_IndexDesc *, Four ,AttrInfo *, Four,	
 *  char **, Four , MethodInfo *, Four)
 *
 * Returns:
 *  Error code
 *    eBADPARAMETER_LOM
 *    eVOLUMENOTMOUNTED_LOM
 *    eRELATIONDUPLICATED_LOM
 *    some errors caused by function calls
 *
 */


#include <string.h>
#include <malloc.h>
#include "LOM.h"
#include "ODYS_common.h"
#include "Catalog_Internal.h"
#include "Catalog.h"


Four LOM_CreateClass(
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
	Four	classId) 			/* IN class id */		
{
	Two  keyLen;			/* should be Two to store key length */
	Four e;					/* error number */
	Four i,j;					/* index variable */
	Four v;					/* index on LRDS mount table */
	Four catScanId;			/* scan id for a catalog table access */
	Four catScanId2;		/* scan id for a catalog table access */
	Four maxTupleLen;		/* maximum tuple length */
	FileID fid;				/* SM level file identifier */
	FileID superclassfid;	/* SM level file identifier */
	LOM_IndexID iid;			/* index used for clustering */
	LOM_IndexID logical_id_index;	/* index built on logical-id column */
	Boolean found;			/* TRUE if the relation is already defined */
	BoundCond bound;		/* boundary condition */
	ColListStruct clist[LOM_SYSCOLUMNS_NUM_COLS];	/* column list */
	ColListStruct clist_for_classid[LOM_CLASSID_NUM_COLS]; /* column list */
	LockParameter lockup;      
	Four orn;		/* open relation number */
	Four orn2;		/* open relation number */
	Four offset;	/* offset of the corresponging attribute of an object */
	BoolExp boolexp[1];
	TupleID	tupleid;
	TupleID tid;
	Four alignmentoffset;
	ColInfo	colInfo[LOM_MAXNUMOFATTRIBUTE];	/* ColInfo list */
	AttrInfo attrinfo[LOM_MAXNUMOFATTRIBUTE];	/* ColInfo list */
	Boolean isFirstTextAttr;
	lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */
	lrds_RelTableEntry *relTableEntry2; /* pointer to an entry of relation table */
	char invertedIndexTableName[LOM_MAXCLASSNAME];
	char logicalIdIndexName[LOM_MAXCLASSNAME];
	LockParameter lockup2;      
	Four superclassId;
	Four fromClassId;
	Two  fromAttrNum;
	Four toClassId;
	Two  toAttrNum;
	One  direction;
	One  cardinality;
	One  reverseCardinality;
	Four relationshipId;
	char relationshipName[LOM_MAXRELATIONSHIPNAME];
	ColListStruct clist_for_rel[LOM_SYSRELATIONSHIP_NUM_COLS];
	ClassID superClassIdList[LOM_MAXSUPERCLASSNUM];

	/* check parameters */
	if (volId < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);
    
	if (className == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);

	if ((indexName == NULL && idesc != NULL) || (indexName != NULL && idesc == NULL)) LOM_ERROR(handle, eBADPARAMETER_LOM);

	if (nAttrs < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);

	if (ainfo == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);

	if ( tmpClassFlag ) {
		if(nSuperclasses!=0 || nMethods != 0) LOM_ERROR(handle, eBADPARAMETER_LOM);
	}

	/* 0th column : logical id */
	if(tmpClassFlag) {
		attrinfo[0].complexType = SM_COMPLEXTYPE_BASIC;
		attrinfo[0].type = SM_STRING;
		attrinfo[0].length = 1;
		attrinfo[0].inheritedFrom = -1;
		attrinfo[0].domain = SM_STRING;
		strcpy(attrinfo[0].name, LOM_LOGICALID_COLNAME);
	}
	else {
		attrinfo[0].complexType = SM_COMPLEXTYPE_BASIC;
		attrinfo[0].type = LOM_LONG_VAR;
		attrinfo[0].length = sizeof(Four);
		attrinfo[0].inheritedFrom = -1;
		attrinfo[0].domain = LOM_LONG_VAR;
		strcpy(attrinfo[0].name, LOM_LOGICALID_COLNAME);
	}

	/* copy to temporay attribute informations */
	for( i = 0; i < nAttrs; i++) 
		bcopy(&(ainfo[i]), &(attrinfo[i + 1]), sizeof(AttrInfo));

	/* the first column is for logical id */
	nAttrs++;

	/* copy attribute info */
	for(i = 0; i< nAttrs; i++) {
		/* For ODMG Collection */
		if(attrinfo[i].complexType == LOM_COMPLEXTYPE_ODMG_COLLECTIONSET ||
		   attrinfo[i].complexType == LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG ||
		   attrinfo[i].complexType == LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST ||
		   attrinfo[i].complexType == LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY)
		{
			colInfo[i].complexType = SM_COMPLEXTYPE_BASIC;
			colInfo[i].type        = SM_STRING;
			colInfo[i].length      = sizeof(ODMG_CollectionDesc);
		}
		else
		{
			colInfo[i].complexType = attrinfo[i].complexType;
			switch(attrinfo[i].type) {
				case ODYS_ULONG:
					colInfo[i].type = ODYS_LONG;
					colInfo[i].length = ODYS_LONG_SIZE;
					break;
				case ODYS_USHORT:
					colInfo[i].type = ODYS_SHORT;
					colInfo[i].length = ODYS_SHORT_SIZE;
					break;
				case ODYS_OCTET:
					colInfo[i].type = ODYS_STRING;
					colInfo[i].length = attrinfo[i].length;
					break;
				case ODYS_BOOLEAN:
					colInfo[i].type = ODYS_LONG;
					colInfo[i].length = ODYS_BOOLEAN_SIZE;
					break;
				case ODYS_REF :
				case ODYS_LINK :
					colInfo[i].type = SM_OID;
					colInfo[i].length = attrinfo[i].length;
					break;
				case LOM_TEXT:
					colInfo[i].type = SM_STRING; 			
					colInfo[i].length = sizeof(LOM_TextDesc);
					break;
				/* added ODMG Literal Type into LOM layer
				 */
				case LOM_DATE:
					colInfo[i].type = SM_LONG;
					colInfo[i].length = SM_LONG_SIZE;
					break;
				case LOM_TIME:
					colInfo[i].type = SM_STRING;
					colInfo[i].length = attrinfo[i].length;
					break;
				case LOM_TIMESTAMP:
					colInfo[i].type = SM_STRING;
					colInfo[i].length = attrinfo[i].length;
					break;
				case LOM_INTERVAL:
					colInfo[i].type = SM_DOUBLE;
					colInfo[i].length = SM_DOUBLE_SIZE;
					break;
				case LOM_OGIS_GEOMETRY:
					colInfo[i].type = SM_VARSTRING;
					colInfo[i].length = LOM_MAXLARGEOBJECTSIZE;
					break;
				case LOM_OGIS_POINT:
					colInfo[i].type = SM_VARSTRING;
					colInfo[i].length = LOM_MAXLARGEOBJECTSIZE;
					break;
				case LOM_OGIS_LINESTRING:
					colInfo[i].type = SM_VARSTRING;
					colInfo[i].length = LOM_MAXLARGEOBJECTSIZE;
					break;
				case LOM_OGIS_POLYGON:
					colInfo[i].type = SM_VARSTRING;
					colInfo[i].length = LOM_MAXLARGEOBJECTSIZE;
					break;
				case LOM_OGIS_GEOMETRYCOLLECTION:
					colInfo[i].type = SM_VARSTRING;
					colInfo[i].length = LOM_MAXLARGEOBJECTSIZE;
					break;
				case LOM_OGIS_MULTIPOINT:
					colInfo[i].type = SM_VARSTRING;
					colInfo[i].length = LOM_MAXLARGEOBJECTSIZE;
					break;
				case LOM_OGIS_MULTILINESTRING:
					colInfo[i].type = SM_VARSTRING;
					colInfo[i].length = LOM_MAXLARGEOBJECTSIZE;
					break;
				case LOM_OGIS_MULTIPOLYGON:
					colInfo[i].type = SM_VARSTRING;
					colInfo[i].length = LOM_MAXLARGEOBJECTSIZE;
					break;
				default:
					colInfo[i].type = attrinfo[i].type;
					colInfo[i].length = attrinfo[i].length;
					break;
			}
		}
	}

	if(idesc != NULL) {
		if(idesc->indexType == SM_INDEXTYPE_BTREE)
			for(i = 0; i< idesc->kinfo.btree.nColumns; i++) {
				idesc->kinfo.btree.columns[i].colNo++;
			}
		else 
			for(i = 0; i< idesc->kinfo.btree.nColumns; i++) {
				idesc->kinfo.mlgf.colNo[i]++;
			}
	}

	/* create relation in LRDS */
	e = LRDS_CreateRelation(LOM_GET_LRDS_HANDLE(handle), volId, className, (LRDS_IndexDesc *)idesc, nAttrs, colInfo, tmpClassFlag);
	if ( e < 0) {
		if(idesc != NULL) {
			if(idesc->indexType == SM_INDEXTYPE_BTREE)
				for(i = 0; i< idesc->kinfo.btree.nColumns; i++) {
					idesc->kinfo.btree.columns[i].colNo--;
				}
			else 
				for(i = 0; i< idesc->kinfo.btree.nColumns; i++) {
					idesc->kinfo.mlgf.colNo[i]--;
				}
		}
	    LOM_ERROR(handle, e);
    }

	if(idesc != NULL) {
		if(idesc->indexType == SM_INDEXTYPE_BTREE)
			for(i = 0; i< idesc->kinfo.btree.nColumns; i++) {
				idesc->kinfo.btree.columns[i].colNo--;
			}
		else 
			for(i = 0; i< idesc->kinfo.btree.nColumns; i++) {
				idesc->kinfo.mlgf.colNo[i]--;
			}
	}

	if(tmpClassFlag) {
		e = Catalog_CreateTemporaryClass(	
			handle, 
			volId, 
			className, 
			indexName, idesc, 
			nAttrs - 1, ainfo, 
			nMethods, methodinfo, 
			classId);
		if(e < eNOERROR) LOM_ERROR(handle, e);
		return eNOERROR; /* if this class is temporary, we here return */
	}

	/*
		open LRDS_SYSTABLES and get DFileID
	*/
	e = LRDS_GetFileIdOfRelation(LOM_GET_LRDS_HANDLE(handle), volId, className, &fid);
	if ( e < 0) LOM_ERROR(handle, e);
	
	/*
		LOM Catalogs Update
	*/

	/* START : LOM_SYSCLASSES */ /* THINK */

	/* set the lockup parameter */
	lockup.mode = L_IX;
	lockup.duration = L_COMMIT;

	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSCLASSES_CLASSNAME);
	if (orn < 0) LOM_ERROR(handle, orn);

	/* Construct 'clist'. */
	clist[0].colNo = LOM_SYSCLASSES_CLASSNAME_COLNO;
	clist[0].start = ALL_VALUE;
	clist[0].dataLength = strlen(className);
	clist[0].data.ptr = className;
	clist[0].nullFlag = SM_FALSE;
	
	clist[1].colNo = LOM_SYSCLASSES_CLASSID_COLNO;
	clist[1].start = ALL_VALUE;
	clist[1].dataLength = sizeof(Four);
	ASSIGN_VALUE_TO_COL_LIST(clist[1], classId, sizeof(Four));
	clist[1].nullFlag = SM_FALSE;
	
	clist[2].colNo = LOM_SYSCLASSES_DATAFILEID_COLNO;
	clist[2].start = ALL_VALUE;
	clist[2].dataLength = sizeof(FileID);
	clist[2].data.fid = fid; 
	clist[2].nullFlag = SM_FALSE;

	e = LRDS_CreateTuple(LOM_GET_LRDS_HANDLE(handle), orn, SM_FALSE, LOM_SYSCLASSES_NUM_COLS, &(clist[0]), (TupleID*)NULL);
	if (e < 0) LOM_ERROR(handle, e);
	
	/* Close the data file */
	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if (e < 0) LOM_ERROR(handle, e);

	/* END : LOM_SYSCLASSES */ 

	/* open relation number for LOM_SYSCLASSES_CLASSNAME */
	orn2 = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId,LOM_SYSCLASSES_CLASSNAME);
	if (orn2 < 0) LOM_ERROR(handle, orn2);

	/* START: LOM_INHERITANCE */ /* THINK */

	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId,LOM_INHERITANCE_CLASSNAME);
	if (orn < 0) LOM_ERROR(handle, orn);

	/* set the lockup parameter */
	lockup.mode = L_IX;
	lockup.duration = L_COMMIT;

	for(i=0;i < nSuperclasses ; i++) {
		/* Construct 'clist'. */
		clist[0].colNo = LOM_INHERITANCE_CLASSID_COLNO;
		clist[0].start = ALL_VALUE;
		clist[0].dataLength = sizeof(Four);
		ASSIGN_VALUE_TO_COL_LIST(clist[0], classId, sizeof(Four));
		clist[0].nullFlag = SM_FALSE;

		/* set lock up parameters */
		lockup.mode = L_IS;
		lockup.duration = L_COMMIT;

		/* get class id of superclasss. */
		boolexp[0].op = SM_EQ;
		boolexp[0].colNo = LOM_SYSCLASSES_CLASSNAME_COLNO;
		boolexp[0].length = strlen(superclassList[i]);
		bcopy(superclassList[i],boolexp[0].data.str,boolexp[0].length);

		/* Open for finding superclasss */
		catScanId2 = LRDS_OpenSeqScan(LOM_GET_LRDS_HANDLE(handle), orn2, FORWARD, 1, &boolexp[0], &lockup);
		if (catScanId2 < 0) LOM_ERROR(handle, catScanId2);
    
		e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), catScanId2, &tupleid, NULL);
		if (e < 0) LOM_ERROR(handle, e);

		if (e == EOS) LOM_ERROR(handle, eRELATIONNOTFOUND_LOM);

		clist_for_classid[0].colNo = LOM_SYSCLASSES_CLASSID_COLNO;
		clist_for_classid[0].start = ALL_VALUE;
     
		e =  LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), catScanId2, SM_TRUE, &tupleid, 1, &clist_for_classid[0]);
		if (e < 0) LOM_ERROR(handle, e);

		e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), catScanId2);
		if (e < 0) LOM_ERROR(handle, e); 

		superClassIdList[i] = GET_VALUE_FROM_COL_LIST(clist_for_classid[0], sizeof(superClassIdList[i]));

		clist[1].colNo = LOM_INHERITANCE_SUPERCLASSID_COLNO;
		clist[1].start = ALL_VALUE;
		clist[1].dataLength = sizeof(Four);
#if defined(SUPPORT_LARGE_DATABASE2)
		clist[1].data.ll = clist_for_classid[0].data.ll;
#else
		clist[1].data.l = clist_for_classid[0].data.l;
#endif
		clist[1].nullFlag = SM_FALSE;
	
		clist[2].colNo = LOM_INHERITANCE_ORDER_COLNO;
		clist[2].start = ALL_VALUE;
		clist[2].dataLength = sizeof(Four);
		ASSIGN_VALUE_TO_COL_LIST(clist[2], i, sizeof(Four));
		clist[2].nullFlag = SM_FALSE;

		e = LRDS_CreateTuple(LOM_GET_LRDS_HANDLE(handle), orn, SM_FALSE, LOM_INHERITANCE_NUM_COLS, &(clist[0]), (TupleID*)NULL);
		if (e < 0) LOM_ERROR(handle, e);
	}
    
	/* Close the data file */
	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if (e < 0) LOM_ERROR(handle, e);

	/* END: LOM_INHERITANCE */ 

	/* START : LOM_SYSCOLUMNS */ /* THINK */

	/* set the lockup parameter */
	lockup.mode = L_IX;
	lockup.duration = L_COMMIT;

	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId,LOM_SYSCOLUMNS_CLASSNAME);
	if (orn < 0) LOM_ERROR(handle, orn);

	/* Construct 'clist'. */
	clist[LOM_SYSCOLUMNS_CLASSID_COLNO].colNo = LOM_SYSCOLUMNS_CLASSID_COLNO;
	clist[LOM_SYSCOLUMNS_CLASSID_COLNO].start = ALL_VALUE;
	clist[LOM_SYSCOLUMNS_CLASSID_COLNO].dataLength = sizeof(Four);
	ASSIGN_VALUE_TO_COL_LIST(clist[LOM_SYSCOLUMNS_CLASSID_COLNO], classId, sizeof(Four));
	clist[LOM_SYSCOLUMNS_CLASSID_COLNO].nullFlag = SM_FALSE;

	/* For ODMG Collection */	
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

	offset = 0;

	isFirstTextAttr = SM_TRUE;

	for (i = 0; i < nAttrs; i++) {
		ASSIGN_VALUE_TO_COL_LIST(clist[LOM_SYSCOLUMNS_COLUMNNO_COLNO], i, sizeof(Two));
		ASSIGN_VALUE_TO_COL_LIST(clist[LOM_SYSCOLUMNS_COMPLEXTYPE_COLNO], attrinfo[i].complexType, sizeof(Two));
		ASSIGN_VALUE_TO_COL_LIST(clist[LOM_SYSCOLUMNS_COLTYPE_COLNO], attrinfo[i].type, sizeof(Two));
		clist[LOM_SYSCOLUMNS_COLNAME_COLNO].dataLength = strlen(attrinfo[i].name) +1;	/* insert NULL character */
		clist[LOM_SYSCOLUMNS_COLNAME_COLNO].data.ptr = attrinfo[i].name;
		ASSIGN_VALUE_TO_COL_LIST(clist[LOM_SYSCOLUMNS_COLOFFSET_COLNO], offset, sizeof(Four));
		clist[LOM_SYSCOLUMNS_DOMAINID_COLNO].dataLength = LOM_LONG_SIZE_VAR;
		ASSIGN_VALUE_TO_COL_LIST(clist[LOM_SYSCOLUMNS_DOMAINID_COLNO], attrinfo[i].domain, sizeof(Four));
		ASSIGN_VALUE_TO_COL_LIST(clist[LOM_SYSCOLUMNS_ACTIVECOLUMNNO_COLNO], i, sizeof(Two));

		if ( attrinfo[i].inheritedFrom == LOM_INHERITEDFROM_THIS_CLASS) 
			ASSIGN_VALUE_TO_COL_LIST(clist[LOM_SYSCOLUMNS_INHERITEDFROM_COLNO], classId, sizeof(Four));
		else
			ASSIGN_VALUE_TO_COL_LIST(clist[LOM_SYSCOLUMNS_INHERITEDFROM_COLNO], attrinfo[i].inheritedFrom, sizeof(Four));

		/* calcultate the offset of next attribute */
		if (attrinfo[i].type == SM_STRING) {
			offset += attrinfo[i].length;
		}
		else if ( attrinfo[i].type == LOM_TEXT) { 
			offset += sizeof(LOM_TextDesc);
		}
		/* For ODMG Collection */
		else if(attrinfo[i].complexType == LOM_COMPLEXTYPE_ODMG_COLLECTIONSET ||
			attrinfo[i].complexType == LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG ||
			attrinfo[i].complexType == LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST ||
			attrinfo[i].complexType == LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY) 
		{
			offset += sizeof(ODMG_CollectionDesc);
		}
		else if (attrinfo[i].type == SM_SHORT) {
			if(offset % SM_SHORT_SIZE) offset = offset + SM_SHORT_SIZE - (offset%SM_SHORT_SIZE);
			ASSIGN_VALUE_TO_COL_LIST(clist[LOM_SYSCOLUMNS_COLOFFSET_COLNO], offset, sizeof(Four));
			offset += SM_SHORT_SIZE;
		}
		else if (attrinfo[i].type == SM_LONG) {
			if(offset % SM_LONG_SIZE) offset = offset + SM_LONG_SIZE - (offset%SM_LONG_SIZE);
			ASSIGN_VALUE_TO_COL_LIST(clist[LOM_SYSCOLUMNS_COLOFFSET_COLNO], offset, sizeof(Four));
			offset += SM_LONG_SIZE;
		}
		else if (attrinfo[i].type == SM_LONG_LONG) {
			if(offset % SM_LONG_LONG_SIZE) offset = offset + SM_LONG_LONG_SIZE - (offset%SM_LONG_LONG_SIZE);
			ASSIGN_VALUE_TO_COL_LIST(clist[LOM_SYSCOLUMNS_COLOFFSET_COLNO], offset, sizeof(Four));
			offset += SM_LONG_LONG_SIZE;
		}
		else if (attrinfo[i].type == SM_MBR) {
			if(offset % SM_MBR_SIZE) offset = offset + SM_MBR_SIZE - (offset%SM_MBR_SIZE);
			ASSIGN_VALUE_TO_COL_LIST(clist[LOM_SYSCOLUMNS_COLOFFSET_COLNO], offset, sizeof(Four));
			offset += SM_MBR_SIZE;
		}
		else if (attrinfo[i].type == SM_OID || attrinfo[i].type == ODYS_REF || attrinfo[i].type == ODYS_LINK) {
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

		if( attrinfo[i].type == LOM_TEXT) {
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
			e = lom_Text_CreateInvertedIndexTable(handle, volId, className, attrinfo[i].name, i);
			if(e < eNOERROR) LOM_ERROR(handle, e);

			/* create docId index table */
			e = lom_Text_CreateDocIdIndexTable(handle, volId, className, attrinfo[i].name, i); 
			if(e < eNOERROR) LOM_ERROR(handle, e);

			/* add inverted index info. to catalog */
			e = lom_Text_AddIndexInfoIntoCatalog(handle, volId, className, attrinfo[i].name, i);
			if(e < eNOERROR) LOM_ERROR(handle, e);
		}
	}

	/* Close the data file */
	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if (e < 0) LOM_ERROR(handle, e);


	/* END : LOM_SYSCOLUMNS */ 

	/* START: LOM_SYSMETHODS */ /* THINK */

	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId,LOM_SYSMETHODS_CLASSNAME);
	if (orn < 0) LOM_ERROR(handle, orn);

	/* set the lockup parameter */
	lockup.mode = L_IX;
	lockup.duration = L_COMMIT;

	/* Construct 'clist'. */
	clist[LOM_SYSMETHODS_CLASSID_COLNO].colNo = LOM_SYSMETHODS_CLASSID_COLNO;
	clist[LOM_SYSMETHODS_CLASSID_COLNO].start = ALL_VALUE;
	clist[LOM_SYSMETHODS_CLASSID_COLNO].dataLength = sizeof(Four);
	ASSIGN_VALUE_TO_COL_LIST(clist[LOM_SYSMETHODS_CLASSID_COLNO], classId, sizeof(Four));
	clist[LOM_SYSMETHODS_CLASSID_COLNO].nullFlag = SM_FALSE;
	
	clist[LOM_SYSMETHODS_DIRPATH_COLNO].colNo = LOM_SYSMETHODS_DIRPATH_COLNO;
	clist[LOM_SYSMETHODS_DIRPATH_COLNO].start = ALL_VALUE;
	clist[LOM_SYSMETHODS_DIRPATH_COLNO].nullFlag = SM_FALSE;

	clist[LOM_SYSMETHODS_METHODNAME_COLNO].colNo = LOM_SYSMETHODS_METHODNAME_COLNO;
	clist[LOM_SYSMETHODS_METHODNAME_COLNO].start = ALL_VALUE;
	clist[LOM_SYSMETHODS_METHODNAME_COLNO].nullFlag = SM_FALSE;

	clist[LOM_SYSMETHODS_FUNCTIONNAME_COLNO].colNo = LOM_SYSMETHODS_FUNCTIONNAME_COLNO;
	clist[LOM_SYSMETHODS_FUNCTIONNAME_COLNO].start = ALL_VALUE;
	clist[LOM_SYSMETHODS_FUNCTIONNAME_COLNO].nullFlag = SM_FALSE;

	clist[LOM_SYSMETHODS_NARGUMENTS_COLNO].colNo = LOM_SYSMETHODS_NARGUMENTS_COLNO;
	clist[LOM_SYSMETHODS_NARGUMENTS_COLNO].start = ALL_VALUE;
	clist[LOM_SYSMETHODS_NARGUMENTS_COLNO].dataLength = sizeof(Two);
	clist[LOM_SYSMETHODS_NARGUMENTS_COLNO].nullFlag = SM_FALSE;

	clist[LOM_SYSMETHODS_ARGUMENTLIST_COLNO].colNo = LOM_SYSMETHODS_ARGUMENTLIST_COLNO;
	clist[LOM_SYSMETHODS_ARGUMENTLIST_COLNO].start = ALL_VALUE;
	clist[LOM_SYSMETHODS_ARGUMENTLIST_COLNO].nullFlag = SM_FALSE;

	clist[LOM_SYSMETHODS_RETURNTYPE_COLNO].colNo = LOM_SYSMETHODS_RETURNTYPE_COLNO;
	clist[LOM_SYSMETHODS_RETURNTYPE_COLNO].start = ALL_VALUE;
	clist[LOM_SYSMETHODS_RETURNTYPE_COLNO].dataLength = sizeof(Four);
	clist[LOM_SYSMETHODS_RETURNTYPE_COLNO].nullFlag = SM_FALSE;
	
	clist[LOM_SYSMETHODS_INHERITEDFROM_COLNO].colNo = LOM_SYSMETHODS_INHERITEDFROM_COLNO;
	clist[LOM_SYSMETHODS_INHERITEDFROM_COLNO].start = ALL_VALUE;
	clist[LOM_SYSMETHODS_INHERITEDFROM_COLNO].dataLength = sizeof(Four);
	clist[LOM_SYSMETHODS_INHERITEDFROM_COLNO].nullFlag = SM_FALSE;

	for(i=0;i < nMethods; i++) {
		clist[LOM_SYSMETHODS_DIRPATH_COLNO].dataLength = strlen(methodinfo[i].dirPath);
		clist[LOM_SYSMETHODS_DIRPATH_COLNO].data.ptr = methodinfo[i].dirPath;

		clist[LOM_SYSMETHODS_FUNCTIONNAME_COLNO].dataLength = strlen(methodinfo[i].functionName);
		clist[LOM_SYSMETHODS_FUNCTIONNAME_COLNO].data.ptr = methodinfo[i].functionName;
		
		clist[LOM_SYSMETHODS_METHODNAME_COLNO].dataLength = strlen(methodinfo[i].name);
		clist[LOM_SYSMETHODS_METHODNAME_COLNO].data.ptr = methodinfo[i].name;

		ASSIGN_VALUE_TO_COL_LIST(clist[LOM_SYSMETHODS_NARGUMENTS_COLNO], methodinfo[i].nArguments, sizeof(Two));

		clist[LOM_SYSMETHODS_ARGUMENTLIST_COLNO].dataLength = sizeof(Four)*methodinfo[i].nArguments;
		clist[LOM_SYSMETHODS_ARGUMENTLIST_COLNO].data.ptr = methodinfo[i].ArgumentsList;

		ASSIGN_VALUE_TO_COL_LIST(clist[LOM_SYSMETHODS_RETURNTYPE_COLNO], methodinfo[i].returnType, sizeof(Four));

		ASSIGN_VALUE_TO_COL_LIST(clist[LOM_SYSMETHODS_INHERITEDFROM_COLNO], methodinfo[i].inheritedFrom, sizeof(Four));

		e = LRDS_CreateTuple(LOM_GET_LRDS_HANDLE(handle), orn, SM_FALSE, LOM_SYSMETHODS_NUM_COLS, &(clist[0]), (TupleID*)NULL);
		if (e < 0) LOM_ERROR(handle, e);
	}
		
	/* Close the data file */
	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if (e < 0) LOM_ERROR(handle, e);

	/* END: LOM_SYSMETHODS */ 

	/* Close LOM_SYSCLASSES */
	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn2);
	if ( e < 0) LOM_ERROR(handle, e);

	/* START: LOM_INDEX */
	if(indexName != NULL) {
		orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, className);
		if (orn < 0) LOM_ERROR(handle, orn);

		relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);
		/* The number of the existing index on this class is only one */

		orn2 = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId,LOM_SYSINDEXES_CLASSNAME);
		if (orn2 < 0) LOM_ERROR(handle, orn2);

		iid.isLogical = SM_FALSE;
		iid.index.physical_iid = (LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[0].iid;

		/* Construct 'clist'. */
		clist[LOM_SYSINDEXES_INDEXNAME_COLNO].colNo = LOM_SYSINDEXES_INDEXNAME_COLNO;
		clist[LOM_SYSINDEXES_INDEXNAME_COLNO].start = ALL_VALUE;
		clist[LOM_SYSINDEXES_INDEXNAME_COLNO].dataLength = strlen(indexName);
		clist[LOM_SYSINDEXES_INDEXNAME_COLNO].data.ptr = indexName;
		clist[LOM_SYSINDEXES_INDEXNAME_COLNO].nullFlag = SM_FALSE;
		   
		clist[LOM_SYSINDEXES_CLASSNAME_COLNO].colNo = LOM_SYSINDEXES_CLASSNAME_COLNO;
		clist[LOM_SYSINDEXES_CLASSNAME_COLNO].start = ALL_VALUE;
		clist[LOM_SYSINDEXES_CLASSNAME_COLNO].dataLength = strlen(className);
		clist[LOM_SYSINDEXES_CLASSNAME_COLNO].data.ptr = className;
		clist[LOM_SYSINDEXES_CLASSNAME_COLNO].nullFlag = SM_FALSE;
		   
		clist[LOM_SYSINDEXES_INDEXID_COLNO].colNo = LOM_SYSINDEXES_INDEXID_COLNO;
		clist[LOM_SYSINDEXES_INDEXID_COLNO].start = ALL_VALUE;
		clist[LOM_SYSINDEXES_INDEXID_COLNO].dataLength = sizeof(LOM_IndexID);
		clist[LOM_SYSINDEXES_INDEXID_COLNO].data.ptr = &iid;
		clist[LOM_SYSINDEXES_INDEXID_COLNO].nullFlag = SM_FALSE;

		e = LRDS_CreateTuple(LOM_GET_LRDS_HANDLE(handle), orn2, SM_FALSE, LOM_SYSINDEXES_NUM_COLS, &(clist[0]), (TupleID*)NULL);
		if (e < 0) LOM_ERROR(handle, e);

		/* Close the LOM_SYSINDEXES file */
		e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn2);
		if (e < 0) LOM_ERROR(handle, e);

		e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
		if ( e < 0) LOM_ERROR(handle, e);

	} /* END : LOM_SYSINDEXES */


	/* For Relationship ... */
	/* START: LOM_SYSRELATIONSHIP */
	/* open LOM_SYSRELATIONSHIP and open scan on it */
	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSRELATIONSHIP_CLASSNAME);
	if(orn < 0) LOM_ERROR(handle, orn);

	lockup.mode     = L_IX;
	lockup.duration = L_COMMIT;

	catScanId = LRDS_OpenSeqScan(LOM_GET_LRDS_HANDLE(handle), orn, FORWARD, 0, NULL, &lockup);
	if(catScanId < 0) LOM_ERROR(handle, catScanId);

	for(i = 0; i < nSuperclasses; i++)
	{
        e = LOM_GetClassID(handle, volId, superclassList[i], &superclassId);
	    if(e < 0) LOM_ERROR(handle, e);

		lockup2.mode     = L_IS;
		lockup2.duration = L_COMMIT;

		bound.op         = SM_EQ;
		bound.key.len    = sizeof(superclassId);
		bcopy(&superclassId, &(bound.key.val[0]), sizeof(superclassId));

		relTableEntry2 = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);
		catScanId2 = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), orn, &((LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry2))[0].iid),
                                        &bound, &bound, 0, NULL, &lockup2);
		if(catScanId2 < 0) LOM_ERROR(handle, catScanId2);
    
		/* read superclass relationship infos and
		   inherites all relationships of the superclass */
		clist_for_rel[0].colNo = LOM_SYSRELATIONSHIP_FROMCLASSID_COLNO;
		clist_for_rel[0].start = ALL_VALUE;
		clist_for_rel[0].dataLength = LOM_LONG_SIZE_VAR;
		clist_for_rel[0].length = LOM_LONG_SIZE_VAR;
		clist_for_rel[0].nullFlag = SM_FALSE;
        
		clist_for_rel[1].colNo = LOM_SYSRELATIONSHIP_FROMATTRNUM_COLNO;
		clist_for_rel[1].start = ALL_VALUE;
		clist_for_rel[1].dataLength = LOM_LONG_SIZE_VAR;
		clist_for_rel[1].length = LOM_LONG_SIZE_VAR;
		clist_for_rel[1].nullFlag = SM_FALSE;
        
		clist_for_rel[2].colNo = LOM_SYSRELATIONSHIP_TOCLASSID_COLNO;
		clist_for_rel[2].start = ALL_VALUE;
		clist_for_rel[2].dataLength = LOM_LONG_SIZE_VAR;
		clist_for_rel[2].length = LOM_LONG_SIZE_VAR;
		clist_for_rel[2].nullFlag = SM_FALSE;
        
		clist_for_rel[3].colNo = LOM_SYSRELATIONSHIP_TOATTRNUM_COLNO;
		clist_for_rel[3].start = ALL_VALUE;
		clist_for_rel[3].dataLength = LOM_LONG_SIZE_VAR;
		clist_for_rel[3].length = LOM_LONG_SIZE_VAR;
		clist_for_rel[3].nullFlag = SM_FALSE;
        
		clist_for_rel[4].colNo = LOM_SYSRELATIONSHIP_CARDINALITY_COLNO;
		clist_for_rel[4].start = ALL_VALUE;
		clist_for_rel[4].dataLength = sizeof(One);
		clist_for_rel[4].length = sizeof(One);
		clist_for_rel[4].data.ptr = &cardinality;
		clist_for_rel[4].nullFlag = SM_FALSE;
        
		clist_for_rel[5].colNo = LOM_SYSRELATIONSHIP_RELATIONSHIPNAME_COLNO;
		clist_for_rel[5].start = ALL_VALUE;
		clist_for_rel[5].dataLength = sizeof(relationshipName);
		clist_for_rel[5].length = sizeof(relationshipName);
		clist_for_rel[5].data.ptr = relationshipName;
		clist_for_rel[5].nullFlag = SM_FALSE;

		clist_for_rel[6].colNo = LOM_SYSRELATIONSHIP_RELATIONSHIPID_COLNO;
		clist_for_rel[6].start = ALL_VALUE;
		clist_for_rel[6].dataLength = sizeof(Four);
		clist_for_rel[6].length = sizeof(Four);
		clist_for_rel[6].nullFlag = SM_FALSE;
        
		clist_for_rel[7].colNo = LOM_SYSRELATIONSHIP_DIRECTION_COLNO;
		clist_for_rel[7].start = ALL_VALUE;
		clist_for_rel[7].dataLength = sizeof(One);
		clist_for_rel[7].length = sizeof(One);
		clist_for_rel[7].data.ptr = &direction;
		clist_for_rel[7].nullFlag = SM_FALSE;

	    while((e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), catScanId2, &tid, NULL)) != EOS)
		{
			if(e < 0) LOM_ERROR(handle, e);

			/* get relationship info from catalog */
			e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), catScanId2, SM_TRUE, &tid, 8, clist_for_rel);
			if(e < 0) LOM_ERROR(handle, e);

			fromClassId      = classId;
			fromAttrNum = GET_VALUE_FROM_COL_LIST(clist_for_rel[1], sizeof(fromAttrNum));
			toClassId = GET_VALUE_FROM_COL_LIST(clist_for_rel[2], sizeof(toClassId));
			toAttrNum = GET_VALUE_FROM_COL_LIST(clist_for_rel[3], sizeof(toAttrNum));
			relationshipId = GET_VALUE_FROM_COL_LIST(clist_for_rel[6], sizeof(relationshipId));
			/* cardinality, direction, relationshipName are automatically
			   loaded by LRDS_FetchTuple */
            
			/* generate reverse cardinality */
			switch(cardinality)
			{
				case LOM_RELATIONSHIP_ONE_TO_ONE:
				case LOM_RELATIONSHIP_MANY_TO_MANY:
					reverseCardinality = cardinality;
					break;
				case LOM_RELATIONSHIP_MANY_TO_ONE:
					reverseCardinality = LOM_RELATIONSHIP_ONE_TO_MANY;
					break;
				case LOM_RELATIONSHIP_ONE_TO_MANY:
					reverseCardinality = LOM_RELATIONSHIP_MANY_TO_ONE;
					break;
			}
            
			/* adjust from-attr-num caused by multiple inheritance */
			if(nSuperclasses > 1)
			{
				e = lom_AdjustAttrNum(handle, volId, superclassId, fromAttrNum, classId, &fromAttrNum);
				if(e < 0) LOM_ERROR(handle, e);
			}

			e = lom_AddRelationship(handle, catScanId, SM_TRUE, 
				fromClassId, fromAttrNum,
				toClassId, toAttrNum,
				direction, cardinality,
				relationshipId, relationshipName);
			if(e < 0) LOM_ERROR(handle, e);
            
			if(direction == LOM_RELATIONSHIP_BIDIRECTIONAL)
			{
				e = lom_AddRelationship(handle, catScanId, SM_TRUE, toClassId, toAttrNum,
					fromClassId, fromAttrNum,
                                        direction, reverseCardinality,
                                        relationshipId, relationshipName);
				if(e < 0) LOM_ERROR(handle, e);
			}
        	}
		e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), catScanId2);
		if(e < 0) LOM_ERROR(handle, e);
	}

	e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), catScanId);
	if(e < 0) LOM_ERROR(handle, e);

	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if(e < 0) LOM_ERROR(handle, e);

	/* Update inheritance informations */
	e = Catalog_CreateInheritanceInfo(handle, volId, classId, nSuperclasses, superClassIdList);
	if (e < 0) LOM_ERROR(handle, e);

	/* update in-memory catalog */
	e = Catalog_Relationship_CreateClass(handle, volId, classId, 
                                         nSuperclasses, superclassList);
	if(e < 0) LOM_ERROR(handle, e);
    
	/* END : LOM_SYSRELATIONSHIP */

	for(i = 0; i< nAttrs; i++) {
		if(attrinfo[i].type == LOM_TEXT) {

			e = LOM_Text_SetFilter(handle, volId, classId, GET_USERLEVEL_COLNO(i), LOM_DEFAULT_FILTER_NO);
			if(e < eNOERROR) LOM_ERROR(handle, e);

			e = LOM_Text_SetKeywordExtractor(handle, volId, classId, GET_USERLEVEL_COLNO(i), LOM_DEFAULT_KEYWORDEXTRACTOR_NO);
			if(e < eNOERROR) LOM_ERROR(handle, e);

			e = LOM_Text_SetStemizer(handle, volId, classId, GET_USERLEVEL_COLNO(i), LOM_DEFAULT_STEMIZER_NO);
			if(e < eNOERROR) LOM_ERROR(handle, e);
		}
	}

	/* create deferred deletion list table */
	e = lom_CreateDeferredDeletionListTable(handle, volId, className);
	if( e < eNOERROR) LOM_ERROR(handle, e);

	return(eNOERROR);

} /* LOM_CreateClass() */


