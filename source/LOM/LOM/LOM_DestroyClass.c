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
 * Module: LOM_DestroyClass.c
 *
 * Description:
 *  Destroy a class. We also remove the related indexes. And then
 *  update the catalog tables including LRDS catalog and LOM catalog.
 *
 * Imports:
 *  SM_DropFile()
 *
 * Exports:
 *  Four LOM_DestroyClass(handle, Four, char*)
 *
 * Returns:
 *  Error code
 *    eBADPARAMETER_LRDS
 *    eVOLUMENOTMOUNTED_LRDS
 *    eOPENEDRELATION_LRDS
 *    eRELATIONNOTFOUND_LRDS
 *    some errors caused by function calls
 */


#include <string.h>
#include "LOM_Internal.h"
#include "LOM.h"
#include "Catalog.h"



Four LOM_DestroyClass(
	LOM_Handle *handle, 
	Four volId,			/* IN volume where the relation was placed */
	char *className)		/* IN class to destroy */
{
	Two  keyLen;		/* should be Two to store key length */
	Four e;			/* error number */
	Four v;			/* index on LRDS mount table */
	Four orn;			/* open relation number */
	Four catScanId;		/* scan id on a catalog table */
	Boolean notFound;		/* TRUE if the given relation exist */
	TupleID catalogEntry;	/* tuple id of catalog tuple in LRDS_SYSTABLES */    
	BoundCond bound;		/* boundary condition of a range scan */
	ColListStruct clist[4];	/* a column list structure */
	LockParameter lockup;	/* lockup for SM_Fetch Tuple */
	PageID catalogPid;
	BoolExp boolexp[1];     /* Boolean Expression */
	TupleID tid;			/* tuple id for catalog entry in LOM level catalog */
	Four classId;			/* class Id */
	lrds_RelTableEntry *relTableEntry;
	lrds_RelTableEntry *relTableEntry2; 
	Four counterId;
	Boolean isFirstTextAttr;	/* flag to indicate thie text attribute is the first text attribute defined on class */
	char attrName[LOM_MAXATTRNAME];
	Boolean flagToIndicateThisClassHasSubclasses = SM_FALSE;
	Two colNo;
	ClassID superClassIdList[LOM_MAXSUPERCLASSNUM];
	Four idx;
	catalog_SysClassesOverlay *ptrSysclasses;
	catalog_SysSuperClassesOverlay *ptrSysSuperclasses;
	Four i;
	Four nSuperclasses;
	Boolean tmpFlag;

	/* check parameters */
	if(volId < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);

	if(className == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);

	/* open relation */
	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId,className);
	if(orn < eNOERROR) LOM_ERROR(handle, orn);

	if(LRDS_IS_TEMPORARY_RELATION(LOM_GET_LRDS_HANDLE(handle), orn)) {
		tmpFlag = SM_TRUE;
		e = Catalog_GetTemporaryClassId(handle, volId, className, &classId);
	}
	else {
		tmpFlag = SM_FALSE;
		e = lom_GetClassId(handle, volId, className, &classId);
		if(e < eNOERROR) LOM_ERROR(handle, e);
	}

	/* close relation */
	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	if(LOM_USEROPENCLASSTABLE(handle)[orn].count > 0 &&
		LOM_USEROPENCLASSTABLE(handle)[orn].openedByLOM_GetOpenClassNum == SM_TRUE) {
		e = LOM_CloseClass(handle, orn);
		if(e < eNOERROR) LOM_ERROR(handle, e);
		LOM_USEROPENCLASSTABLE(handle)[orn].openedByLOM_GetOpenClassNum = SM_FALSE;
	}

	if(tmpFlag) {
		e = Catalog_RemoveClassInfo(handle, volId, classId);
		if(e < eNOERROR) LOM_ERROR(handle, e);

		e = LRDS_DestroyRelation(LOM_GET_LRDS_HANDLE(handle), volId, className);
		if(e < 0) LOM_ERROR(handle, e);

		return eNOERROR;
	}

	/* For Relationship ... */
	/* if the given class has any subclasses,
	   it cannot be destroyded before destroying all its subclasses 
	 */

	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_INHERITANCE_CLASSNAME);
	if( orn < 0 ) LOM_ERROR(handle, orn);

	/* Temporary copy */
	relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);

	bound.op = SM_EQ;
	bound.key.len = sizeof(Four);
	bcopy(&classId,&(bound.key.val[0]),sizeof(Four));


	/* set lock up parameters */
	/* just for reading */
	lockup.mode = L_IS;
	lockup.duration = L_COMMIT;

	catScanId = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), orn,
					&(LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[1].iid,
					&bound,&bound,0,(BoolExp*)NULL,&lockup);
	if (catScanId < 0) LOM_ERROR(handle, catScanId);

	/* intially set to 0 */
	flagToIndicateThisClassHasSubclasses = SM_FALSE;

	while((e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, &tid, NULL))!=EOS) {
		if (e < 0) LOM_ERROR(handle, e);

		flagToIndicateThisClassHasSubclasses = SM_TRUE;
		break;
	}

	e =  LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), catScanId);
	if (e < 0) LOM_ERROR(handle, e);

	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if (e < 0) LOM_ERROR(handle, e);

	if(flagToIndicateThisClassHasSubclasses)
		LOM_ERROR(handle, eSUBCLASSEXIST_LOM);

	v = Catalog_GetVolIndex(handle, volId);
	if(v < eNOERROR) LOM_ERROR(handle, e);

	e = Catalog_GetClassInfo(handle, volId, classId, &idx);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	ptrSysclasses = &CATALOG_GET_CLASSINFOTBL(handle, v)[idx];
	ptrSysSuperclasses = &CATALOG_GET_SUPERCLASSINFOTBL(handle, v)[CATALOG_GET_SUPERCLASSINFOTBL_INDEX(ptrSysclasses)];

	nSuperclasses = CATALOG_GET_SUPERCLASSNUM(ptrSysclasses);
	for(i = 0; i < nSuperclasses; i++) {
		superClassIdList[i] = ptrSysSuperclasses[i].superClassId;
	}

	/*
	** LOM_SYSCLASSES
	*/

	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId,LOM_SYSCLASSES_CLASSNAME);
	if( orn < 0 ) LOM_ERROR(handle, orn);

	bound.op = SM_EQ;
	keyLen = strlen(className);
	bound.key.len = sizeof(Two) + keyLen;
	bcopy(&keyLen,&(bound.key.val[0]),sizeof(Two));
	bcopy(className,&(bound.key.val[sizeof(Two)]),keyLen);

	lockup.mode = L_IX;
	lockup.duration = L_COMMIT;

	relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);

	catScanId = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), orn,
			 &(LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[0].iid,
			 &bound,&bound,0,(BoolExp*)NULL,&lockup);
	if (catScanId < 0) LOM_ERROR(handle, catScanId);

	e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, &tid, NULL);
	if (e < 0) LOM_ERROR(handle, e);

	clist[0].colNo = LOM_SYSCLASSES_CLASSID_COLNO;
	clist[0].start = ALL_VALUE;

	e =  LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, SM_TRUE, &tid,1,&clist[0]);
	if (e < 0) LOM_ERROR(handle, e);

	classId = GET_VALUE_FROM_COL_LIST(clist[0], sizeof(classId));

	e = LRDS_DestroyTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, SM_TRUE, &tid);
	if (e < 0) LOM_ERROR(handle, e);

	e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), catScanId);
	if (e < 0) LOM_ERROR(handle, e);

	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if( e < 0 ) LOM_ERROR(handle, e);

	/*
	** LOM_SYSINHERITANCE
	*/
    
	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId,LOM_INHERITANCE_CLASSNAME);
	if( orn < 0 ) LOM_ERROR(handle, orn);

	bound.op = SM_EQ;
	bound.key.len = sizeof(Four);
	bcopy(&classId,&(bound.key.val[0]),sizeof(Four));

	lockup.mode = L_IX;
	lockup.duration = L_COMMIT;

	relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);

	/* index scan of LOM_SYSINHERITANCE */
	catScanId = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), orn,
					&(LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[0].iid,
					&bound, &bound, 0, NULL, &lockup);
	if (catScanId < 0) LOM_ERROR(handle, catScanId);

	e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, &tid, NULL);
	if (e < 0) LOM_ERROR(handle, e);

	while (e != EOS) {
		e = LRDS_DestroyTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, SM_TRUE, (TupleID*)NULL);
		if (e < 0) LOM_ERROR(handle, e);

		e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, &tid, NULL);
		if (e < 0) LOM_ERROR(handle, e);
	}

	e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), catScanId);
	if (e < 0) LOM_ERROR(handle, e);

	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if( e < 0 ) LOM_ERROR(handle, e);

	/*
	** LOM_SYSCOLUMNS
	*/
    
	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId,LOM_SYSCOLUMNS_CLASSNAME);
	if( orn < 0 ) LOM_ERROR(handle, orn);

	lockup.mode = L_IX;
	lockup.duration = L_COMMIT;

	relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);

	/* index scan of LOM_SYSINHERITANCE */
	catScanId = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), orn,
					&(LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[0].iid,
					&bound, &bound, 0, NULL, &lockup);
	if (catScanId < 0) LOM_ERROR(handle, catScanId);

	e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, &tid, NULL);
	if (e < 0) LOM_ERROR(handle, e);

	clist[0].colNo = LOM_SYSCOLUMNS_COLTYPE_COLNO;
	clist[0].start = ALL_VALUE;
	clist[0].length  = LOM_SHORT_SIZE_VAR;
	clist[0].dataLength = LOM_SHORT_SIZE_VAR;

	clist[1].colNo = LOM_SYSCOLUMNS_COLNAME_COLNO;
	clist[1].start = ALL_VALUE;
	clist[1].length = LOM_MAXATTRNAME;
	clist[1].dataLength = LOM_MAXATTRNAME;
	clist[1].data.ptr = attrName;

	clist[2].colNo = LOM_SYSCOLUMNS_COLUMNNO_COLNO;
	clist[2].start = ALL_VALUE;
	clist[2].length  = LOM_SHORT_SIZE_VAR;
	clist[2].dataLength = LOM_SHORT_SIZE_VAR;

	clist[3].colNo = LOM_SYSCOLUMNS_ACTIVECOLUMNNO_COLNO;
	clist[3].start = ALL_VALUE;
	clist[3].length  = LOM_SHORT_SIZE_VAR;
	clist[3].dataLength = LOM_SHORT_SIZE_VAR;

	isFirstTextAttr = SM_TRUE;

	while (e != EOS) {
		e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, SM_TRUE, NULL, 4, clist);
		if(e < eNOERROR) LOM_ERROR(handle, e);

		if(GET_VALUE_FROM_COL_LIST(clist[0], sizeof(Two)) == LOM_TEXT && 
			GET_VALUE_FROM_COL_LIST(clist[3], sizeof(Two)) != NIL) 
		{
			if(isFirstTextAttr) {
				/* destroy content table for storing text type attribute */
				e = lom_Text_DestroyContentTable(handle, volId, className);
				if(e < eNOERROR) LOM_ERROR(handle, e);

				/* destroy logical counter */
				e = lom_Text_DestroyCounter(handle, volId, className);
				if(e < eNOERROR) LOM_ERROR(handle, e);

				isFirstTextAttr = SM_FALSE;
			}

			/* make string */
			attrName[clist[1].retLength] = 0;

			/* DESTROY INVERTED-INDEX TABLE */
			e = lom_Text_DestroyInvertedIndexTable(handle, volId, className, attrName);
			if(e < eNOERROR) LOM_ERROR(handle, e);

			/* DESTROY DOC-ID INDEX TABLE */
			e = lom_Text_DestroyDocIdIndexTable(handle, volId, className, attrName);
			if(e < eNOERROR) LOM_ERROR(handle, e);

			/* DROP INERTED-INDEX FROM CATALOG */
			e = lom_Text_DropIndexInfoFromCatalog(handle, volId, className, attrName);
			if(e < eNOERROR) LOM_ERROR(handle, e);

			colNo = GET_USERLEVEL_COLNO(GET_VALUE_FROM_COL_LIST(clist[2], sizeof(Two)));

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

		/* destroy column information tuple */
		e = LRDS_DestroyTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, SM_TRUE, (TupleID*)NULL);
		if (e < 0) LOM_ERROR(handle, e);

		/* move cursor next */
		e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, &tid, NULL);
		if (e < 0) LOM_ERROR(handle, e);
	}

	/* close scan */
	e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), catScanId);
	if (e < 0) LOM_ERROR(handle, e);

	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if( e < 0 ) LOM_ERROR(handle, e);

	/*
	** LOM_SYSMETHODS
	*/
    
	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId,LOM_SYSMETHODS_CLASSNAME);
	if( orn < 0 ) LOM_ERROR(handle, orn);

	lockup.mode = L_IX;
	lockup.duration = L_COMMIT;

	relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);

	/* index scan of LOM_SYSINHERITANCE */
	catScanId = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), orn,
					&(LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[0].iid,
					&bound, &bound, 0, NULL, &lockup);
	if (catScanId < 0) LOM_ERROR(handle, catScanId);

	e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, (TupleID*)NULL, NULL);
	if (e < 0) LOM_ERROR(handle, e);

	while (e != EOS) {
		e = LRDS_DestroyTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, SM_TRUE, (TupleID*)NULL);
		if (e < 0) LOM_ERROR(handle, e);

		e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, (TupleID*)NULL, NULL);
		if (e < 0) LOM_ERROR(handle, e);
	}

	e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), catScanId);
	if (e < 0) LOM_ERROR(handle, e);

	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if( e < 0 ) LOM_ERROR(handle, e);

	/* 
	   delete index-related information from LOM_SYSINDEXES
	*/
	/*
	** LOM_SYSINDEXES
	*/
    
	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId,LOM_SYSINDEXES_CLASSNAME);
	if( orn < 0 ) LOM_ERROR(handle, orn);

	lockup.mode = L_IX;
	lockup.duration = L_COMMIT;

	relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);

	bound.op = SM_EQ;
	keyLen = strlen(className);
	bound.key.len = sizeof(Two) + keyLen;
	bcopy(&keyLen,&(bound.key.val[0]),sizeof(Two));
	bcopy(className,&(bound.key.val[sizeof(Two)]),keyLen);

	/* index scan of LOM_SYSINDEXES */
	/* we here shold use in-memory catalog manager to get index id 
	*/
	if((LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[1].colNo[0] == LOM_SYSINDEXES_CLASSNAME_COLNO)
		catScanId = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), orn,
						&(LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[1].iid,
						&bound, &bound, 0, NULL, &lockup);
	else
		catScanId = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), orn,
						&(LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[1].iid,
						&bound, &bound, 0, NULL, &lockup);

	if (catScanId < 0) LOM_ERROR(handle, catScanId);

	e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, (TupleID*)NULL, NULL);
	if (e < 0) LOM_ERROR(handle, e);

	while (e != EOS) {
		e = LRDS_DestroyTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, SM_TRUE, (TupleID*)NULL);
		if (e < 0) LOM_ERROR(handle, e);

		e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, (TupleID*)NULL, NULL);
		if (e < 0) LOM_ERROR(handle, e);
	}

	e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), catScanId);
	if (e < 0) LOM_ERROR(handle, e);

	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if( e < 0 ) LOM_ERROR(handle, e);

    /*
     * LOM_SYSRELATIONSHIP
     */
    /* Update disk relationship catalog */
    orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId,LOM_SYSRELATIONSHIP_CLASSNAME);
    if(orn < 0) LOM_ERROR(handle, orn);

    /* delete relationship info whose toclassid is classId */
    bound.op = SM_EQ;
    bound.key.len = LOM_LONG_SIZE_VAR;
    bcopy(&classId, &(bound.key.val[0]), LOM_LONG_SIZE_VAR);
    
    /* set lock up parameters */
    /* just for reading */
    lockup.mode = L_IX;
    lockup.duration = L_COMMIT;
    
    relTableEntry2 = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);
    catScanId = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), orn, &((LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry2))[1].iid),
                                   &bound, &bound, 0, (BoolExp*)NULL, &lockup);
    if (catScanId < 0) LOM_ERROR(handle, catScanId);

    while((e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, &tid, NULL)) != EOS)
    {
        if (e < 0) LOM_ERROR(handle, e);

        e = LRDS_DestroyTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, SM_TRUE, &tid);
        if (e < 0) LOM_ERROR(handle, e);
    }
    
    e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), catScanId);
    if (e < 0) LOM_ERROR(handle, e);
	
    /* delete relationship info whose fromclassid is classId */
    bound.op = SM_EQ;
    bound.key.len = LOM_LONG_SIZE_VAR;
    bcopy(&classId, &(bound.key.val[0]), LOM_LONG_SIZE_VAR);
    
    /* set lock up parameters */
    /* just for reading */
    lockup.mode     = L_IX;
    lockup.duration = L_COMMIT;
    
    relTableEntry2 = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);
    catScanId = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), orn, &((LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry2))[0].iid),
                                   &bound, &bound, 0, (BoolExp*)NULL, &lockup);
    if (catScanId < 0) LOM_ERROR(handle, catScanId);
    
    while((e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, &tid, NULL)) != EOS)
    {
        if (e < 0) LOM_ERROR(handle, e);
        
        e = LRDS_DestroyTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, SM_TRUE, &tid);
        if (e < 0) LOM_ERROR(handle, e);
    }
    
    e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), catScanId);
    if (e < 0) LOM_ERROR(handle, e);

    e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
    if(e < 0) LOM_ERROR(handle, e);

	/* destroy deferred deletion list */
	e = lom_DestroyDeferredDeletionListTable(handle, volId, className);
	if(e < 0) LOM_ERROR(handle, e);

	/* Update in-memory catalog */
	/* In in-memory catalog, destroy relationship info whose to classid is classid */
	e = Catalog_Relationship_DestroyClass(handle, volId, classId);
	if(e < 0) LOM_ERROR(handle, e);

	e = Catalog_DestroyInheritanceInfo(handle, volId, classId, nSuperclasses, superClassIdList);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	e = Catalog_RemoveClassInfo(handle, volId, classId);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	e = LRDS_DestroyRelation(LOM_GET_LRDS_HANDLE(handle), volId, className);
	if(e < 0) LOM_ERROR(handle, e);

	return(eNOERROR);

} /* LOM_DestroyClass(handle, ) */
	     

