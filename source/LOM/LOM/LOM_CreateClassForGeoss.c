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
 *    eBADPARAMETER_LRDS
 *    eVOLUMENOTMOUNTED_LRDS
 *    eRELATIONDUPLICATED_LRDS
 *    some errors caused by function calls
 *
 */


#include <string.h>
#include <malloc.h>
#include "LOM_Internal.h"
#include "LOM.h"


Four LOM_CreateClass(
	LOM_Handle *handle,
	Four    volId,			/* IN volume in which the relation will be placed */
	char    *className,			/* IN relation name to create */
	LRDS_IndexDesc *idesc,				/* IN key info for the culustering index */
	Four    nAttrs,				/* IN number of atrributes */
	AttrInfo *attrinfo,			/* IN attribute information */
	Four	nSuperclasss,		/* IN number of superclasss */
	char	(*superclassList)[MAXCLASSNAME],	/* IN names of superclasss */
	Four	nMethods,			/* IN number of methods	*/
	MethodInfo	*methodinfo,	/* IN method information */
	Boolean		tmpClassFlag,		/* IN flag to indicate if this class is temporary or not */
	Four	*classId) 			/* OUT class id */		
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
	IndexID iid;			/* a Btree index used for clustering */
	Boolean found;			/* TRUE if the relation is already defined */
	BoundCond bound;		/* boundary condition */
	ColListStruct clist[6];	/* column list */
	ColListStruct clist_for_classid[1]; /* column list */
	LockParameter lockup;      
	Four orn;		/* open relation number */
	Four orn2;		/* open relation number */
	Four offset;	/* offset of the corresponging attribute of an object */
	BoolExp boolexp[1];
	TupleID	tupleid;
	TupleID tid;
	Four alignmentoffset;
	AttrInfo headerInfo[1];
	Two domainID;
	ColInfo	*colInfo;	/* ColInfo list */


    /* check parameters */
    if (volId < 0) LOM_ERROR(eBADPARAMETER_LRDS);
    
    if (className == NULL) LOM_ERROR(eBADPARAMETER_LRDS);

    if (nAttrs <= 0) LOM_ERROR(eBADPARAMETER_LRDS);

    if (attrinfo == NULL) LOM_ERROR(eBADPARAMETER_LRDS);

	if ( tmpClassFlag ) {
		if(nSuperclasss!=0 || nMethods != 0) LOM_ERROR(eBADPARAMETER_LRDS);
	}

	/* make colInfo list */
	colInfo = (ColInfo*)malloc( sizeof(ColInfo) * nAttrs);
	if ( colInfo == NULL ) LOM_ERROR( eMEMORYALLOCERR_LRDS);

	/* copy attribute info */
	for( i=0; i< nAttrs; i++) {
		colInfo[i].complexType = attrinfo[i].complexType;
		colInfo[i].type = attrinfo[i].type;
		colInfo[i].length = attrinfo[i].length;
	}

	/* create relation in LRDS */
	e = LRDS_CreateRelation(LOM_GET_LRDS_HANDLE(handle), volId, className, idesc, nAttrs, colInfo, tmpClassFlag);
	if ( e < 0) LOM_ERROR(e);

	/* free colInfo list */
	e = free( colInfo);
	if ( e == 0) LOM_ERROR( eMEMORYFREEERR_LOM);

	if(tmpClassFlag) {
		*classId = 0;
		return eNOERROR; /* if this class is temporary, we here return */
	}

	/*
		open LRDS_SYSTABLES and get DFileID
	*/
	e = LRDS_GetFileIdOfRelation(LOM_GET_LRDS_HANDLE(handle), volId, className, &fid);
	if ( e < 0) LOM_ERROR(e);
	
	/*
		LOM Catalogs Update
	*/

	/* Notice!!! Here we need codes to get classId for this class */

	*classId = lom_GetAndIncrementLastClassId(volId);

    /* START : LOM_SYSCLASSES */ /* THINK */

	/* set the lockup parameter */
	lockup.mode = L_IX;
	lockup.duration = L_COMMIT;

	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId,LOM_SYSCLASSES_CLASSNAME);
	if (orn < 0) LOM_ERROR(orn);

	catScanId = LRDS_OpenSeqScan(LOM_GET_LRDS_HANDLE(handle), orn, FORWARD , 0, (BoolExp*)NULL, &lockup);
	if (catScanId < 0) LOM_ERROR(catScanId);

	/* Construct 'clist'. */
	clist[0].colNo = LOM_SYSCLASSES_CLASSNAME_COLNO;
	clist[0].start = ALL_VALUE;
	clist[0].dataLength = strlen(className);
	clist[0].data.ptr = className;
	clist[0].nullFlag = SM_FALSE;
	
	clist[1].colNo = LOM_SYSCLASSES_CLASSID_COLNO;
	clist[1].start = ALL_VALUE;
	clist[1].dataLength = sizeof(Four);
	ASSIGN_VALUE_TO_COL_LIST(clist[1], *classId, sizeof(Four));
	clist[1].nullFlag = SM_FALSE;
	
	clist[2].colNo = LOM_SYSCLASSES_DATAFILEID_COLNO;
	clist[2].start = ALL_VALUE;
	clist[2].dataLength = sizeof(FileID);
	clist[2].data.fid = fid; 
	clist[2].nullFlag = SM_FALSE;

	e = LRDS_CreateTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, LOM_SYSCLASSES_NUM_COLS, &(clist[0]), (TupleID*)NULL);
	if (e < 0) LOM_ERROR(e);
	
	/* Close scan on the data file */
	e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), catScanId);
	if (e < 0) LOM_ERROR(e);

	/* Close the data file */
	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if (e < 0) LOM_ERROR(e);

	/* END : LOM_SYSCLASSES */ 

	/* open relation number for LOM_SYSCLASSES_CLASSNAME */
	orn2 = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId,LOM_SYSCLASSES_CLASSNAME);
    if (orn2 < 0) LOM_ERROR(orn2);

    /* START: LOM_INHERITANCE */ /* THINK */

	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId,LOM_INHERITANCE_CLASSNAME);
    if (orn < 0) LOM_ERROR(orn);

    /* set the lockup parameter */
    lockup.mode = L_IX;
    lockup.duration = L_COMMIT;

	catScanId = LRDS_OpenSeqScan(LOM_GET_LRDS_HANDLE(handle), orn, FORWARD, 0, (BoolExp*)NULL, &lockup);
	if (catScanId < 0) LOM_ERROR(catScanId);


	for(i=0;i < nSuperclasss ; i++) {
		/* Construct 'clist'. */
		clist[0].colNo = LOM_INHERITANCE_CLASSID_COLNO;
		clist[0].start = ALL_VALUE;
		clist[0].dataLength = sizeof(Four);
		ASSIGN_VALUE_TO_COL_LIST(clist[0], *classId, sizeof(Four));

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
		if (catScanId2 < 0) LOM_ERROR(catScanId2);
    
		e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), catScanId2, &tupleid);
		if (e < 0) LOM_ERROR(e);

		if (e == EOS) LOM_ERROR(eRELATIONNOTFOUND_LRDS);

		clist_for_classid[0].colNo = LOM_SYSCLASSES_CLASSID_COLNO;
		clist_for_classid[0].start = ALL_VALUE;
     
		e =  LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), catScanId2,&tupleid,1,&clist_for_classid[0]);
		if (e < 0) LOM_ERROR(e);

		e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), catScanId2);

		clist[1].colNo = LOM_INHERITANCE_SUPERCLASSID_COLNO;
		clist[1].start = ALL_VALUE;
		clist[1].dataLength = sizeof(Four);
#if defined(SUPPORT_LARGE_DATABASE2)
		clist[1].data.ll = clist_for_classid[0].data.ll;
#else
		clist[1].data.l = clist_for_classid[0].data.l;
#endif
	
		clist[2].colNo = LOM_INHERITANCE_ORDER_COLNO;
		clist[2].start = ALL_VALUE;
		clist[2].dataLength = sizeof(Four);
		ASSIGN_VALUE_TO_COL_LIST(clist[2], i, Four);

		e = LRDS_CreateTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, LOM_INHERITANCE_NUM_COLS, &(clist[0]), (TupleID*)NULL);
		if (e < 0) LOM_ERROR(e);
	}
    
	/* Close scan on the data file */
	e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), catScanId);
	if (e < 0) LOM_ERROR(e);

	/* Close the data file */
	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if (e < 0) LOM_ERROR(e);

    /* END: LOM_INHERITANCE */ 

    /* START : LOM_SYSCOLUMNS */ /* THINK */

    /* set the lockup parameter */
    lockup.mode = L_IX;
    lockup.duration = L_COMMIT;

    orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId,LOM_SYSCOLUMNS_CLASSNAME);
    if (orn < 0) LOM_ERROR(orn);

    catScanId = LRDS_OpenSeqScan(LOM_GET_LRDS_HANDLE(handle), orn, FORWARD, 0, (BoolExp*)NULL, &lockup);
    if (catScanId < 0) LOM_ERROR(catScanId);

    /* Construct 'clist'. */
    clist[0].colNo = LOM_SYSCOLUMNS_CLASSID_COLNO;
    clist[0].start = ALL_VALUE;
    clist[0].dataLength = sizeof(Four);
    ASSIGN_VALUE_TO_COL_LIST(clist[0], *classId, sizeof(Four));

    clist[1].colNo = LOM_SYSCOLUMNS_COLUMNNO_COLNO;
    clist[1].start = ALL_VALUE;
    clist[1].dataLength = sizeof(Two);

    clist[2].colNo = LOM_SYSCOLUMNS_COLNAME_COLNO;
    clist[2].start = ALL_VALUE;
    /* length is set where the offet is calculated */

    clist[3].colNo = LOM_SYSCOLUMNS_COLOFFSET_COLNO;
    clist[3].start = ALL_VALUE;
    clist[3].dataLength =  sizeof(Four);

    clist[4].colNo = LOM_SYSCOLUMNS_INHERITEDFROM_COLNO;
    clist[4].start = ALL_VALUE;
    clist[4].dataLength =  sizeof(Four);


    offset = LOM_OBJDESC_SIZE;

    for (i = 0; i < nAttrs; i++) {
        ASSIGN_VALUE_TO_COL_LIST(clist[1], i, size(Two));
        clist[2].dataLength = strlen(attrinfo[i].name) +1;	/* insert NULL character */
        clist[2].data.ptr = attrinfo[i].name;
        ASSIGN_VALUE_TO_COL_LIST(clist[3], offset - LOM_OBJDESC_SIZE, sizeof(Four));

		if ( attrinfo[i].inheritedFrom == LOM_INHERITEDFROM_THIS_CLASS) 
			ASSIGN_VALUE_TO_COL_LIST(clist[4], *classId, sizeof(Four));
		else
			ASSIGN_VALUE_TO_COL_LIST(clist[4], attrinfo[i].inheritedFrom, sizeof(Four));

        /* calcultate the offset of next attribute */
        if (attrinfo[i].type == SM_STRING) {
            offset = offset + attrinfo[i].length;
        }
        else if (attrinfo[i].type == SM_SHORT) {
            if(offset % SM_SHORT_SIZE) offset = offset + SM_SHORT_SIZE - (offset%SM_SHORT_SIZE);
            ASSIGN_VALUE_TO_COL_LIST(clist[3], offset - LOM_OBJDESC_SIZE, sizeof(Four));
            offset += SM_SHORT_SIZE;
        }
        else if (attrinfo[i].type == SM_LONG) {
            if(offset % SM_LONG_SIZE) offset = offset + SM_LONG_SIZE - (offset%SM_LONG_SIZE);
            ASSIGN_VALUE_TO_COL_LIST(clist[3], offset - LOM_OBJDESC_SIZE, sizeof(Four));
            offset += SM_LONG_SIZE;
        }
        else if (attrinfo[i].type == SM_LONG_LONG) {
            if(offset % SM_LONG_LONG_SIZE) offset = offset + SM_LONG_LONG_SIZE - (offset%SM_LONG_LONG_SIZE);
            ASSIGN_VALUE_TO_COL_LIST(clist[3], offset - LOM_OBJDESC_SIZE, sizeof(Four));
            offset += SM_LONG_LONG_SIZE;
        }
        else if (attrinfo[i].type == SM_MBR) {
            if(offset % LOM_OBJREF_SIZE) offset = offset + SM_MBR_SIZE - (offset%SM_MBR_SIZE);
            ASSIGN_VALUE_TO_COL_LIST(clist[3], offset - LOM_OBJDESC_SIZE, sizeof(Four));
            offset += SM_MBR_SIZE;
        }
        else if (attrinfo[i].type == SM_OID) {
            if(offset % LOM_OBJREF_SIZE) offset = offset + LOM_OBJREF_SIZE - (offset%LOM_OBJREF_SIZE);
            ASSIGN_VALUE_TO_COL_LIST(clist[3], offset - LOM_OBJDESC_SIZE, sizeof(Four));
            offset += LOM_OBJREF_SIZE;
        }
        else {      /* SM_VARSTRING */ /* i.e. pointer to data */
            if(offset % sizeof(void *)) offset = offset + sizeof(void *) - (offset%sizeof(void *));
            ASSIGN_VALUE_TO_COL_LIST(clist[3], offset - LOM_OBJDESC_SIZE, sizeof(Four));
            offset += sizeof(void *);
        }

        e = LRDS_CreateTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, 5, &(clist[0]), (TupleID*)&tid);
        if (e < 0) LOM_ERROR(e);

        #ifdef TRACE
        printf("Column Offset: %ld Inherited_From: %ld\n",clist[3].data.l,clist[4].data.l);
        #endif
    }
    /* Close scan on the data file */
    e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), catScanId);
    if (e < 0) LOM_ERROR(e);

    /* Close the data file */
    e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
    if (e < 0) LOM_ERROR(e);


    /* END : LOM_SYSCOLUMNS */ 

    /* START: LOM_SYSMETHODS */ /* THINK */

	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId,LOM_SYSMETHODS_CLASSNAME);
    if (orn < 0) LOM_ERROR(orn);

    /* set the lockup parameter */
    lockup.mode = L_IX;
    lockup.duration = L_COMMIT;

	catScanId = LRDS_OpenSeqScan(LOM_GET_LRDS_HANDLE(handle), orn, FORWARD, 0, (BoolExp*)NULL, &lockup);
	if (catScanId < 0) LOM_ERROR(catScanId);


	/* Construct 'clist'. */
	clist[0].colNo = LOM_SYSMETHODS_CLASSID_COLNO;
	clist[0].start = ALL_VALUE;
	clist[0].dataLength = sizeof(Four);
	ASSIGN_VALUE_TO_COL_LIST(clist[0], *classId, sizeof(Four));
	
	clist[1].colNo = LOM_SYSMETHODS_DIRPATH_COLNO;
	clist[1].start = ALL_VALUE;

	clist[2].colNo = LOM_SYSMETHODS_METHODNAME_COLNO;
	clist[2].start = ALL_VALUE;

    clist[3].colNo = LOM_SYSMETHODS_NARGUMENTS_COLNO;
    clist[3].start = ALL_VALUE;
    clist[3].dataLength = sizeof(Two);

	clist[4].colNo = LOM_SYSMETHODS_ARGUMENTLIST_COLNO;
	clist[4].start = ALL_VALUE;

	clist[5].colNo = LOM_SYSMETHODS_RETURNTYPE_COLNO;
	clist[5].start = ALL_VALUE;
    clist[5].dataLength = sizeof(Four);
	
	clist[6].colNo = LOM_SYSMETHODS_INHERITEDFROM_COLNO;
	clist[6].start = ALL_VALUE;
	clist[6].dataLength = sizeof(Four);

	for(i=0;i < nMethods; i++) {
		clist[1].dataLength = strlen(methodinfo[i].dirPath);
		clist[1].data.ptr = methodinfo[i].dirPath;
		
		clist[2].dataLength = strlen(methodinfo[i].name);
		clist[2].data.ptr = methodinfo[i].name;

		ASSIGN_VALUE_TO_COL_LIST(clist[3], methodinfo[i].nArguments, sizeof(Two));

		clist[4].dataLength = sizeof(Four)*methodinfo[i].nArguments;
		clist[4].data.ptr = methodinfo[i].ArgumentsList;

		ASSIGN_VALUE_TO_COL_LIST(clist[5], methodinfo[i].returnType, sizeof(Four));

		ASSIGN_VALUE_TO_COL_LIST(clist[6], methodinfo[i].inheritedFrom, sizeof(Four));

		e = LRDS_CreateTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, LOM_SYSMETHODS_NUM_COLS, &(clist[0]), (TupleID*)NULL);
		if (e < 0) LOM_ERROR(e);
	}
		
	/* Close scan on the data file */
	e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), catScanId);
	if (e < 0) LOM_ERROR(e);

	/* Close the data file */
	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if (e < 0) LOM_ERROR(e);

    /* END: LOM_SYSMETHODS */ 

	/* Close LOM_SYSCLASSES */
	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn2);
	if ( e < 0) LOM_ERROR(e);

    return(eNOERROR);

} /* LOM_CreateClass() */


