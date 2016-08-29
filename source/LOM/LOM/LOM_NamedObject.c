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
#include "LOM_Internal.h"
#include "LOM.h"
#include "Catalog.h"
#include "Catalog_Internal.h"

Four lom_CreateNamedObjectTable(
	LOM_Handle *handle,
	Four volId		/* volumn id */ /* system volume */
)
{
	
	ColInfo lomSysNamedObject[LOM_NAMEDOBJECTTABLE_NUM_COLS]; /* lomSysNamedObject */
	Four e;		/* error code */
	LRDS_IndexDesc idesc;	/* index descriptor */
	IndexID iid;

	/* object name */
	lomSysNamedObject[LOM_NAMEDOBJECTTABLE_OBJNAME_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysNamedObject[LOM_NAMEDOBJECTTABLE_OBJNAME_COLNO].type = SM_VARSTRING;
	lomSysNamedObject[LOM_NAMEDOBJECTTABLE_OBJNAME_COLNO].length = LOM_MAXOBJECTNAME;

	/* object id */
	lomSysNamedObject[LOM_NAMEDOBJECTTABLE_OBJECTID_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysNamedObject[LOM_NAMEDOBJECTTABLE_OBJECTID_COLNO].type = SM_OID;
	lomSysNamedObject[LOM_NAMEDOBJECTTABLE_OBJECTID_COLNO].length = SM_OID_SIZE;

	idesc.indexType = SM_INDEXTYPE_BTREE;
	idesc.kinfo.btree.flag = KEYFLAG_CLUSTERING | KEYFLAG_UNIQUE;
	idesc.kinfo.btree.nColumns = 1;
	idesc.kinfo.btree.columns[0].colNo = LOM_NAMEDOBJECTTABLE_OBJNAME_COLNO;
	idesc.kinfo.btree.columns[0].flag = KEYINFO_COL_ASC;

	/* create sysnamedobject */
	e = LRDS_CreateRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSNAMEDOBJECT_CLASSNAME, &idesc, LOM_NAMEDOBJECTTABLE_NUM_COLS, &lomSysNamedObject[0], SM_FALSE);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	/* add index */
	idesc.indexType = SM_INDEXTYPE_BTREE;
	idesc.kinfo.btree.flag = KEYFLAG_UNIQUE;
	idesc.kinfo.btree.nColumns = 1;
	idesc.kinfo.btree.columns[0].colNo = LOM_NAMEDOBJECTTABLE_OBJECTID_COLNO;
	idesc.kinfo.btree.columns[0].flag = KEYINFO_COL_ASC;

	/* add index on the LOM_SYSNAMEDOBJECT */
	e = LRDS_AddIndex(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSNAMEDOBJECT_CLASSNAME, &idesc, &iid);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}


/*
 * Function: Four lom_DestroyNamedObjectTable(Four)
 *
 * Description:
 *  destroy named object table 
 *
 * Retuns:
 *  error code
 */
Four lom_DestroyNamedObjectTable(
	LOM_Handle *handle,
	Four volId		/* volumn id */ /* system volume */
)
{
	Four e;

	/* destroy sysnamedobject table */
	e = LRDS_DestroyRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSNAMEDOBJECT_CLASSNAME);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}

/*
 * Function: Four LOM_OpenNamedObjectTable(Four)
 *
 * Description:
 *  open named object table 
 *
 * Retuns:
 *  error code
 */
Four LOM_OpenNamedObjectTable(
	LOM_Handle *handle,
	Four volId
)
{
	Four e;

	e = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSNAMEDOBJECT_CLASSNAME);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return e;
}

/*
 * Function: Four LOM_CloseNamedObjectTable(handle, Four)
 *
 * Description:
 *  close named object table 
 *
 * Retuns:
 *  error code
 */
Four LOM_CloseNamedObjectTable(
	LOM_Handle *handle, 
	Four ocn
)
{
	Four e;

	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), ocn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}


/*
 * Function: Four LOM_SetObjectName(handle, Four, char *, OID *)
 *
 * Description:
 *  assign name to the given object
 *
 * Retuns:
 *  error code
 *  eNAMEDOBJECTDUPLICATED_LOM
 */
Four LOM_SetObjectName(
	LOM_Handle *handle, 
	Four ocn,
	char *objectName,
	OID *oid
)
{
	Four e;
	lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation tale */
	LockParameter lockup;
	Four osn;
	BoundCond bound;
	Two keyLen;
	Four e2;
	ColListStruct clist[2];

	/* check parameter */
	if(objectName == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if(ocn < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if(oid == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);

	/* get pointer to an entry of relation table */
	relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), ocn);

	/* make bound condition */
	bound.op = SM_EQ;
	keyLen = strlen(objectName);
	bound.key.len = sizeof(Two) + keyLen;
	bcopy(&keyLen, &(bound.key.val[0]), sizeof(Two));
	bcopy(objectName, &(bound.key.val[sizeof(Two)]), keyLen);

	lockup.mode = L_IX;
	lockup.duration = L_COMMIT;

	/* open index scan */
	osn = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), ocn, &((LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[0].iid), &bound, &bound , 0, NULL, &lockup);
	if(osn < eNOERROR) LOM_ERROR(handle, osn);

	/* check if the given object named is already in namedObject table */
	e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), osn, NULL, NULL);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	if(e != EOS) {
		/* close scan */
		e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), osn);
		if(e < eNOERROR) LOM_ERROR(handle, e);

		return eNAMEDOBJECTDUPLICATED_LOM;
	}

	/* make column struct list */
	clist[0].colNo = LOM_NAMEDOBJECTTABLE_OBJNAME_COLNO;
	clist[0].start = ALL_VALUE;
	clist[0].dataLength = strlen(objectName);
	clist[0].data.ptr = objectName;
	clist[0].nullFlag = SM_FALSE;

	clist[1].colNo = LOM_NAMEDOBJECTTABLE_OBJECTID_COLNO;
	clist[1].start = ALL_VALUE;
	clist[1].dataLength = SM_OID_SIZE;
	clist[1].data.oid = *oid;
	clist[1].nullFlag = SM_FALSE;

	/* create entry */
	e2 = LRDS_CreateTuple(LOM_GET_LRDS_HANDLE(handle), osn, SM_TRUE, LOM_NAMEDOBJECTTABLE_NUM_COLS, clist, NULL);
	if(e2 < eNOERROR) {
		
		e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), osn);
		if(e < eNOERROR) LOM_ERROR(handle, e);

		LOM_ERROR(handle, e2);
	}

	/* close scan */
	e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), osn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}

/*
 * Function: Four LOM_LookUpNamedObject(handle, Four, char *, OID *)
 *
 * Description:
 *  assign name to the given object
 *
 * Retuns:
 *  error code
 *  eNAMEDOBJECTNOTFOUND_LOM
 */
Four LOM_LookUpNamedObject(
	LOM_Handle *handle, 
	Four ocn,
	char *objectName,
	OID *oid
)
{
	Four e;
	lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation tale */
	LockParameter lockup;
	Four osn;
	BoundCond bound;
	Two keyLen;
	Four e2;
	ColListStruct clist[1];

	/* check parameter */
	if(objectName == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if(ocn < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if(oid == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);

	/* get pointer to an entry of relation table */
	relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), ocn);

	/* make bound condition */
	bound.op = SM_EQ;
	keyLen = strlen(objectName);
	bound.key.len = sizeof(Two) + keyLen;
	bcopy(&keyLen, &(bound.key.val[0]), sizeof(Two));
	bcopy(objectName, &(bound.key.val[sizeof(Two)]), keyLen);

	lockup.mode = L_IX;
	lockup.duration = L_COMMIT;

	/* open index scan */
	osn = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), ocn, &((LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[0].iid), &bound, &bound , 0, NULL, &lockup);
	if(osn < eNOERROR) LOM_ERROR(handle, osn);

	/* check if the given object named is already in namedObject table */
	e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), osn, NULL, NULL);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	if(e == EOS) {
		/* close relation */
		e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), osn);
		if(e < eNOERROR) LOM_ERROR(handle, e);

		return eNAMEDOBJECTNOTFOUND_LOM;
	}

	clist[0].colNo = LOM_NAMEDOBJECTTABLE_OBJECTID_COLNO;
	clist[0].start = ALL_VALUE;
	clist[0].dataLength = SM_OID_SIZE;
	clist[0].length = SM_OID_SIZE;

	/* create entry */
	e2 = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), osn, SM_TRUE, NULL, 1, clist);
	if(e2 < eNOERROR) {
		
		e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), osn);
		if(e < eNOERROR) LOM_ERROR(handle, e);

		LOM_ERROR(handle, e2);
	}

	/* close scan */
	e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), osn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	/* copy to result */
	*oid = clist[0].data.oid;

	return eNOERROR;
}

/* Function: Four LOM_GetObjectName(handle, Four, char *, OID *)
 *
 * Description:
 *  assign name to the given object
 *
 * Retuns:
 *  error code
 *  eNAMEDOBJECTNOTFOUND_LOM
 */
Four LOM_GetObjectName(
	LOM_Handle *handle, 
	Four ocn,
	OID *oid,
	char *objectName
)
{
	Four e;
	lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation tale */
	LockParameter lockup;
	Four osn;
	BoundCond bound;
	Two keyLen;
	Four e2;
	ColListStruct clist[1];

	/* check parameter */
	if(objectName == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if(ocn < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if(oid == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);

	/* get pointer to an entry of relation table */
	relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), ocn);

	/* make bound condition */
	bound.op = SM_EQ;
	keyLen = sizeof(OID);
	bcopy(oid, &(bound.key.val[0]), keyLen);
	bound.key.len = keyLen;

	lockup.mode = L_IX;
	lockup.duration = L_COMMIT;

	/* open index scan */
	osn = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), ocn, &((LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[1].iid), &bound, &bound , 0, NULL, &lockup);
	if(osn < eNOERROR) LOM_ERROR(handle, osn);

	/* check if the given object named is already in namedObject table */
	e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), osn, NULL, NULL);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	if(e == EOS) {
		/* close relation */
		e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), osn);
		if(e < eNOERROR) LOM_ERROR(handle, e);

		return eNAMEDOBJECTNOTFOUND_LOM;
	}

	clist[0].colNo = LOM_NAMEDOBJECTTABLE_OBJNAME_COLNO;
	clist[0].start = ALL_VALUE;
	clist[0].dataLength = LOM_MAXOBJECTNAME;
	clist[0].length = LOM_MAXOBJECTNAME;
	clist[0].data.ptr = objectName;

	/* create entry */
	e2 = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), osn, SM_TRUE, NULL, 1, clist);
	if(e2 < eNOERROR) {
		
		e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), osn);
		if(e < eNOERROR) LOM_ERROR(handle, e);

		LOM_ERROR(handle, e2);
	}

	/* close scan */
	e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), osn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	objectName[clist[0].retLength] = '\0';

	return eNOERROR;
}

/*
 * Function: Four LOM_ResetObjectName(handle, Four, char *, OID *)
 *
 * Description:
 *  reset given object
 *
 * Retuns:
 *  error code
 *  eNAMEDOBJECTNOTFOUND_LOM
 */
Four LOM_ResetObjectName(
	LOM_Handle *handle, 
	Four ocn,
	char *objectName
)
{
	Four e;
	lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation tale */
	LockParameter lockup;
	Four osn;
	BoundCond bound;
	Two keyLen;
	Four e2;
	ColListStruct clist[1];

	/* check parameter */
	if(objectName == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if(ocn < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);

	/* get pointer to an entry of relation table */
	relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), ocn);

	/* make bound condition */
	bound.op = SM_EQ;
	keyLen = strlen(objectName);
	bound.key.len = sizeof(Two) + keyLen;
	bcopy(&keyLen, &(bound.key.val[0]), sizeof(Two));
	bcopy(objectName, &(bound.key.val[sizeof(Two)]), keyLen);

	lockup.mode = L_IX;
	lockup.duration = L_COMMIT;

	/* open index scan */
	osn = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), ocn, &((LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[0].iid), &bound, &bound , 0, NULL, &lockup);
	if(osn < eNOERROR) LOM_ERROR(handle, osn);

	/* check if the given object named is already in namedObject table */
	e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), osn, NULL, NULL);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	if(e == EOS) {
		/* close relation */
		e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), osn);
		if(e < eNOERROR) LOM_ERROR(handle, e);

		return eNAMEDOBJECTNOTFOUND_LOM;
	}

	/* destroy entry */
	e2 = LRDS_DestroyTuple(LOM_GET_LRDS_HANDLE(handle), osn, SM_TRUE, NULL);
	if(e2 < eNOERROR) {
		
		e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), osn);
		if(e < eNOERROR) LOM_ERROR(handle, e);

		LOM_ERROR(handle, e2);
	}

	/* close scan */
	e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), osn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}

/*
 * Function: Four LOM_RenameNamedObject(handle, Four, char *, OID *)
 *
 * Description:
 *  rename the given object
 *
 * Retuns:
 *  error code
 */
Four LOM_RenameNamedObject(
	LOM_Handle *handle, 
	Four ocn,
	char *oldName,
	char *newName
)
{
	Four e;
	OID oid;

	/* destroy and create */
	e = LOM_ResetObjectName(handle, ocn, oldName);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	/* assign name to object */
	e = LOM_SetObjectName(handle, ocn, newName, &oid);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}


