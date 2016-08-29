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

#ifndef SLIMDOWN_TEXTIR

#include <malloc.h>
#include <string.h>
#include <stdlib.h>
#include <dlfcn.h>
#include "LOM_Internal.h"
#include "LOM.h"
#include "Catalog.h"
#include "Catalog_Internal.h"


Four LOM_Text_AddStemizer(
	LOM_Handle *handle, 
	Four volId,
	char *stemizerName,
	Four version,
	char *stemizerFilePath,
	char *stemizerFunctionName,
	Four *stemizerNo
)
{
	LockParameter lockup;
	Four e;
	Four orn;
	Four osn;
	ColListStruct clist[LOM_SYSTEXTSTEMIZERINFO_NUM_COLS];

	if(stemizerName == NULL || version < 0 || stemizerFilePath == NULL ||
	stemizerFunctionName == NULL || stemizerNo == NULL)
		LOM_ERROR(handle, eBADPARAMETER_LOM);

	/* open text-stemizer-info. table */
	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSTEXTSTEMIZERINFO_CLASSNAME);
	if(orn < eNOERROR) LOM_ERROR(handle, orn);

	/* make column struct list */
	/* stemizer name */
	clist[LOM_SYSTEXTSTEMIZERINFO_STEMIZERNAME_COLNO].colNo = LOM_SYSTEXTSTEMIZERINFO_STEMIZERNAME_COLNO;
	clist[LOM_SYSTEXTSTEMIZERINFO_STEMIZERNAME_COLNO].start = ALL_VALUE;
	clist[LOM_SYSTEXTSTEMIZERINFO_STEMIZERNAME_COLNO].dataLength = strlen(stemizerName);
	clist[LOM_SYSTEXTSTEMIZERINFO_STEMIZERNAME_COLNO].length = strlen(stemizerName);
	clist[LOM_SYSTEXTSTEMIZERINFO_STEMIZERNAME_COLNO].data.ptr = stemizerName;
	clist[LOM_SYSTEXTSTEMIZERINFO_STEMIZERNAME_COLNO].nullFlag = SM_FALSE;

	/* version */
	clist[LOM_SYSTEXTSTEMIZERINFO_VERSION_COLNO].colNo = LOM_SYSTEXTSTEMIZERINFO_VERSION_COLNO;
	clist[LOM_SYSTEXTSTEMIZERINFO_VERSION_COLNO].start = ALL_VALUE;
	clist[LOM_SYSTEXTSTEMIZERINFO_VERSION_COLNO].dataLength = LOM_LONG_SIZE_VAR;
	clist[LOM_SYSTEXTSTEMIZERINFO_VERSION_COLNO].length = LOM_LONG_SIZE_VAR;
	ASSIGN_VALUE_TO_COL_LIST(clist[LOM_SYSTEXTSTEMIZERINFO_VERSION_COLNO], version, sizeof(Four));
	clist[LOM_SYSTEXTSTEMIZERINFO_VERSION_COLNO].nullFlag = SM_FALSE;

	/* stemizer file path */
	clist[LOM_SYSTEXTSTEMIZERINFO_STEMIZERFILEPATH_COLNO].colNo = LOM_SYSTEXTSTEMIZERINFO_STEMIZERFILEPATH_COLNO;
	clist[LOM_SYSTEXTSTEMIZERINFO_STEMIZERFILEPATH_COLNO].start = ALL_VALUE;
	clist[LOM_SYSTEXTSTEMIZERINFO_STEMIZERFILEPATH_COLNO].dataLength = strlen(stemizerFilePath);
	clist[LOM_SYSTEXTSTEMIZERINFO_STEMIZERFILEPATH_COLNO].length = strlen(stemizerFilePath);
	clist[LOM_SYSTEXTSTEMIZERINFO_STEMIZERFILEPATH_COLNO].data.ptr = stemizerFilePath;
	clist[LOM_SYSTEXTSTEMIZERINFO_STEMIZERFILEPATH_COLNO].nullFlag = SM_FALSE;

	/* stemizer function name */
	clist[LOM_SYSTEXTSTEMIZERINFO_STEMIZERFUNCTIONNAME_COLNO].colNo = LOM_SYSTEXTSTEMIZERINFO_STEMIZERFUNCTIONNAME_COLNO;
	clist[LOM_SYSTEXTSTEMIZERINFO_STEMIZERFUNCTIONNAME_COLNO].start = ALL_VALUE;
	clist[LOM_SYSTEXTSTEMIZERINFO_STEMIZERFUNCTIONNAME_COLNO].dataLength = strlen(stemizerFunctionName);
	clist[LOM_SYSTEXTSTEMIZERINFO_STEMIZERFUNCTIONNAME_COLNO].length = strlen(stemizerFunctionName);
	clist[LOM_SYSTEXTSTEMIZERINFO_STEMIZERFUNCTIONNAME_COLNO].data.ptr = stemizerFunctionName;
	clist[LOM_SYSTEXTSTEMIZERINFO_STEMIZERFUNCTIONNAME_COLNO].nullFlag = SM_FALSE;

	/* get stemizer number */
	*stemizerNo = lom_Text_GetAndIncrementLastStemizerNo(handle, volId);	
	if(*stemizerNo < 0) LOM_ERROR(handle, *stemizerNo);

	clist[LOM_SYSTEXTSTEMIZERINFO_STEMIZERNO_COLNO].colNo = LOM_SYSTEXTSTEMIZERINFO_STEMIZERNO_COLNO;
	clist[LOM_SYSTEXTSTEMIZERINFO_STEMIZERNO_COLNO].start = ALL_VALUE;
	clist[LOM_SYSTEXTSTEMIZERINFO_STEMIZERNO_COLNO].dataLength = LOM_LONG_SIZE_VAR;
	clist[LOM_SYSTEXTSTEMIZERINFO_STEMIZERNO_COLNO].length = LOM_LONG_SIZE_VAR;
	ASSIGN_VALUE_TO_COL_LIST(clist[LOM_SYSTEXTSTEMIZERINFO_STEMIZERNO_COLNO], *stemizerNo, sizeof(Four));
	clist[LOM_SYSTEXTSTEMIZERINFO_STEMIZERNO_COLNO].nullFlag = SM_FALSE;

	/* create entry for the give stemizer information */
	e = LRDS_CreateTuple(LOM_GET_LRDS_HANDLE(handle), orn, SM_FALSE, LOM_SYSTEXTSTEMIZERINFO_NUM_COLS, &clist[0], NULL);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}

Four LOM_Text_AddDefaultStemizer(
	LOM_Handle *handle, 
	Four volId,
	char *stemizerName,
	Four version,
	char *stemizerFilePath,
	char *stemizerFunctionName
)
{
	LockParameter lockup;
	Four e;
	Four orn;
	ColListStruct clist[LOM_SYSTEXTSTEMIZERINFO_NUM_COLS];

	if(stemizerName == NULL || version < 0 || stemizerFilePath == NULL ||
	stemizerFunctionName == NULL)
		LOM_ERROR(handle, eBADPARAMETER_LOM);

	/* open text-stemizer-info. table */
	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSTEXTSTEMIZERINFO_CLASSNAME);
	if(orn < eNOERROR) LOM_ERROR(handle, orn);

	/* make column struct list */
	/* stemizer name */
	clist[LOM_SYSTEXTSTEMIZERINFO_STEMIZERNAME_COLNO].colNo = LOM_SYSTEXTSTEMIZERINFO_STEMIZERNAME_COLNO;
	clist[LOM_SYSTEXTSTEMIZERINFO_STEMIZERNAME_COLNO].start = ALL_VALUE;
	clist[LOM_SYSTEXTSTEMIZERINFO_STEMIZERNAME_COLNO].dataLength = strlen(stemizerName);
	clist[LOM_SYSTEXTSTEMIZERINFO_STEMIZERNAME_COLNO].length = strlen(stemizerName);
	clist[LOM_SYSTEXTSTEMIZERINFO_STEMIZERNAME_COLNO].data.ptr = stemizerName;
	clist[LOM_SYSTEXTSTEMIZERINFO_STEMIZERNAME_COLNO].nullFlag = SM_FALSE;

	/* version */
	clist[LOM_SYSTEXTSTEMIZERINFO_VERSION_COLNO].colNo = LOM_SYSTEXTSTEMIZERINFO_VERSION_COLNO;
	clist[LOM_SYSTEXTSTEMIZERINFO_VERSION_COLNO].start = ALL_VALUE;
	clist[LOM_SYSTEXTSTEMIZERINFO_VERSION_COLNO].dataLength = LOM_LONG_SIZE_VAR;
	clist[LOM_SYSTEXTSTEMIZERINFO_VERSION_COLNO].length = LOM_LONG_SIZE_VAR;
	ASSIGN_VALUE_TO_COL_LIST(clist[LOM_SYSTEXTSTEMIZERINFO_VERSION_COLNO], version, sizeof(Four));
	clist[LOM_SYSTEXTSTEMIZERINFO_VERSION_COLNO].nullFlag = SM_FALSE;

	/* stemizer file path */
	clist[LOM_SYSTEXTSTEMIZERINFO_STEMIZERFILEPATH_COLNO].colNo = LOM_SYSTEXTSTEMIZERINFO_STEMIZERFILEPATH_COLNO;
	clist[LOM_SYSTEXTSTEMIZERINFO_STEMIZERFILEPATH_COLNO].start = ALL_VALUE;
	clist[LOM_SYSTEXTSTEMIZERINFO_STEMIZERFILEPATH_COLNO].dataLength = strlen(stemizerFilePath);
	clist[LOM_SYSTEXTSTEMIZERINFO_STEMIZERFILEPATH_COLNO].length = strlen(stemizerFilePath);
	clist[LOM_SYSTEXTSTEMIZERINFO_STEMIZERFILEPATH_COLNO].data.ptr = stemizerFilePath;
	clist[LOM_SYSTEXTSTEMIZERINFO_STEMIZERFILEPATH_COLNO].nullFlag = SM_FALSE;

	/* stemizer function name */
	clist[LOM_SYSTEXTSTEMIZERINFO_STEMIZERFUNCTIONNAME_COLNO].colNo = LOM_SYSTEXTSTEMIZERINFO_STEMIZERFUNCTIONNAME_COLNO;
	clist[LOM_SYSTEXTSTEMIZERINFO_STEMIZERFUNCTIONNAME_COLNO].start = ALL_VALUE;
	clist[LOM_SYSTEXTSTEMIZERINFO_STEMIZERFUNCTIONNAME_COLNO].dataLength = strlen(stemizerFunctionName);
	clist[LOM_SYSTEXTSTEMIZERINFO_STEMIZERFUNCTIONNAME_COLNO].length = strlen(stemizerFunctionName);
	clist[LOM_SYSTEXTSTEMIZERINFO_STEMIZERFUNCTIONNAME_COLNO].data.ptr = stemizerFunctionName;
	clist[LOM_SYSTEXTSTEMIZERINFO_STEMIZERFUNCTIONNAME_COLNO].nullFlag = SM_FALSE;

	/* get stemizer number */
	clist[LOM_SYSTEXTSTEMIZERINFO_STEMIZERNO_COLNO].colNo = LOM_SYSTEXTSTEMIZERINFO_STEMIZERNO_COLNO;
	clist[LOM_SYSTEXTSTEMIZERINFO_STEMIZERNO_COLNO].start = ALL_VALUE;
	clist[LOM_SYSTEXTSTEMIZERINFO_STEMIZERNO_COLNO].dataLength = LOM_LONG_SIZE_VAR;
	clist[LOM_SYSTEXTSTEMIZERINFO_STEMIZERNO_COLNO].length = LOM_LONG_SIZE_VAR;
	ASSIGN_VALUE_TO_COL_LIST(clist[LOM_SYSTEXTSTEMIZERINFO_STEMIZERNO_COLNO], 0, sizeof(Four));
	clist[LOM_SYSTEXTSTEMIZERINFO_STEMIZERNO_COLNO].nullFlag = SM_FALSE;

	/* create entry for the give stemizer information */
	e = LRDS_CreateTuple(LOM_GET_LRDS_HANDLE(handle), orn, SM_FALSE, LOM_SYSTEXTSTEMIZERINFO_NUM_COLS, &clist[0], NULL);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}

Four LOM_Text_DropStemizer(
	LOM_Handle *handle, 
	Four volId,
	char *stemizerName,
	Four version
)
{
	Four e;
	BoundCond bound;
	Two keyLen;
	Two stemizerNameLength;
	Four orn;
	Four osn;
	lrds_RelTableEntry *relTableEntry;
	TupleID tid;
	ColListStruct clist[1];
	LockParameter lockup;

	stemizerNameLength = strlen(stemizerName);

	/* make bound condition */
	bound.op = SM_EQ;
	keyLen = 0;
	bcopy(&stemizerNameLength, &(bound.key.val[0]), sizeof(Two));
	keyLen += sizeof(Two);
	bcopy(stemizerName, &(bound.key.val[keyLen]), stemizerNameLength);
	keyLen += stemizerNameLength;
	bcopy(&version, &(bound.key.val[keyLen]), sizeof(Four));
	keyLen += sizeof(Four);
	bound.key.len = keyLen;

	/* open text-stemizer-info. table */
	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSTEXTSTEMIZERINFO_CLASSNAME);
	if(orn < eNOERROR) LOM_ERROR(handle, orn);

	/* Get the relation table entry. */
	relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);

	lockup.mode = L_IX;
	lockup.duration = L_COMMIT;

	/* open index scan */
	osn = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), orn, &((LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[1].iid), &bound, &bound,
					0, NULL, &lockup);
	if(osn < eNOERROR) LOM_ERROR(handle, osn);

	e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), osn, &tid, NULL);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	if(e == EOS) LOM_ERROR(handle, eNOSUCHSTEMIZEREXIST_LOM);

	/* destroy entry */
	e = LRDS_DestroyTuple(LOM_GET_LRDS_HANDLE(handle), osn, SM_TRUE, &tid);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), osn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}

Four LOM_Text_GetStemizerNo(
	LOM_Handle *handle, 
	Four volId,
	char *stemizerName,
	Four version,
	Four *stemizerNo
)
{
	Four e;
	BoundCond bound;
	Two keyLen;
	Two stemizerNameLength;
	Four orn;
	Four osn;
	lrds_RelTableEntry *relTableEntry;
	TupleID tid;
	ColListStruct clist[1];
	LockParameter lockup;

	stemizerNameLength = strlen(stemizerName);

	/* make bound condition */
	bound.op = SM_EQ;
	keyLen = 0;
	bcopy(&stemizerNameLength, &(bound.key.val[0]), sizeof(Two));
	keyLen += sizeof(Two);
	bcopy(stemizerName, &(bound.key.val[keyLen]), stemizerNameLength);
	keyLen += stemizerNameLength;
	bcopy(&version, &(bound.key.val[keyLen]), sizeof(Four));
	keyLen += sizeof(Four);
	bound.key.len = keyLen;

	/* open text-stemizer-info. table */
	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSTEXTSTEMIZERINFO_CLASSNAME);
	if(orn < eNOERROR) LOM_ERROR(handle, orn);

	/* Get the relation table entry. */
	relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);

	lockup.mode = L_IS;
	lockup.duration = L_COMMIT;

	/* open index scan */
	osn = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), orn, &((LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[1].iid), &bound, &bound,
					0, NULL, &lockup);
	if(osn < eNOERROR) LOM_ERROR(handle, osn);

	e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), osn, &tid, NULL);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	if(e == EOS) LOM_ERROR(handle, eNOSUCHSTEMIZEREXIST_LOM);

	clist[0].colNo = LOM_SYSTEXTSTEMIZERINFO_STEMIZERNO_COLNO;
	clist[0].start = ALL_VALUE;

	/* fetch entry */
	e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), osn, SM_TRUE, &tid, 1, &clist[0]);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	/* set stemizer number */
	*stemizerNo = GET_VALUE_FROM_COL_LIST(clist[0], sizeof(*stemizerNo));

	/* close scan */
	e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), osn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	/* close relation */
	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;

}

Four LOM_Text_SetStemizer(
	LOM_Handle *handle, 
	Four volId,
	Four classID,
	Four colNo,
	Four stemizerNo
)
{
	Four e;
	Four osn;
	Four orn;
	BoundCond bound;
	Two keyLen;
	Four systemLevelColNo;
	lrds_RelTableEntry *relTableEntry;
	TupleID tid;
	ColListStruct clist[3];
	LockParameter lockup;

	systemLevelColNo = GET_SYSTEMLEVEL_COLNO(colNo);

	/* open relation */
	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSTEXTPREFERENCES_CLASSNAME);
	if(orn < eNOERROR) LOM_ERROR(handle, orn);

	/* open index scan */
	bound.op = SM_EQ;
	keyLen = 0;
	bcopy(&classID, &(bound.key.val[keyLen]), sizeof(Four));
	keyLen += sizeof(Four);
	bcopy(&systemLevelColNo, &(bound.key.val[keyLen]), sizeof(Four));
	keyLen += sizeof(Four);
	bound.key.len = keyLen;

	/* Get the relation table entry. */
	relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);

	lockup.mode = L_IX;
	lockup.duration = L_COMMIT;

	/* open index scan */
	osn = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), orn, &((LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[0].iid), &bound, &bound,
					0, NULL, &lockup);
	if(osn < eNOERROR) LOM_ERROR(handle, osn);

	/* move cursor */
	e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), osn, &tid, NULL);
	if(e < eNOERROR) {
		e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), osn);
		if(e < eNOERROR) LOM_ERROR(handle, e);

		e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
		if(e < eNOERROR) LOM_ERROR(handle, e);

		LOM_ERROR(handle, e);
	}

	clist[0].colNo = LOM_SYSTEXTPREFERENCES_CLASSID_COLNO;
	clist[0].start = ALL_VALUE;
	clist[0].length = clist[0].dataLength = LOM_LONG_SIZE_VAR;
	ASSIGN_VALUE_TO_COL_LIST(clist[0], classID, sizeof(Four));
	clist[0].nullFlag = SM_FALSE;

	clist[1].colNo = LOM_SYSTEXTSTEMIZERINFO_VERSION_COLNO;
	clist[1].start = ALL_VALUE;
	clist[1].length = clist[1].dataLength = LOM_LONG_SIZE_VAR;
	ASSIGN_VALUE_TO_COL_LIST(clist[1], systemLevelColNo, sizeof(Four));
	clist[1].nullFlag = SM_FALSE;

	clist[2].colNo = LOM_SYSTEXTSTEMIZERINFO_STEMIZERNO_COLNO;
	clist[2].start = ALL_VALUE;
	clist[2].length = clist[2].dataLength = LOM_LONG_SIZE_VAR;
	ASSIGN_VALUE_TO_COL_LIST(clist[2], stemizerNo, sizeof(Four));
	clist[2].nullFlag = SM_FALSE;

	if(e == EOS) {
		e = LRDS_CreateTuple(LOM_GET_LRDS_HANDLE(handle), osn, SM_TRUE, 3, &clist[0], &tid);
		if(e < eNOERROR) LOM_ERROR(handle, e);
	}	
	else {
		e = LRDS_UpdateTuple(LOM_GET_LRDS_HANDLE(handle), osn, SM_TRUE, &tid, 1, &clist[2]);
		if(e < eNOERROR) LOM_ERROR(handle, e);
	}

	/* close scan */
	e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), osn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	/* close relation */
	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}

Four LOM_Text_ResetStemizer(
	LOM_Handle *handle, 
	Four volId,
	Four classID,
	Four colNo
)
{
	Four e;
	Four osn;
	Four orn;
	BoundCond bound;
	Two keyLen;
	Four systemLevelColNo;
	lrds_RelTableEntry *relTableEntry;
	TupleID tid;
	ColListStruct clist[3];
	LockParameter lockup;

	systemLevelColNo = GET_SYSTEMLEVEL_COLNO(colNo);

	/* open relation */
	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSTEXTPREFERENCES_CLASSNAME);
	if(orn < eNOERROR) LOM_ERROR(handle, orn);

	/* open index scan */
	bound.op = SM_EQ;
	keyLen = 0;
	bcopy(&classID, &(bound.key.val[keyLen]), sizeof(Four));
	keyLen += sizeof(Four);
	bcopy(&systemLevelColNo, &(bound.key.val[keyLen]), sizeof(Four));
	keyLen += sizeof(Four);
	bound.key.len = keyLen;

	/* Get the relation table entry. */
	relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);

	lockup.mode = L_IX;
	lockup.duration = L_COMMIT;

	/* open index scan */
	osn = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), orn, &((LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[0].iid), &bound, &bound,
					0, NULL, &lockup);
	if(osn < eNOERROR) LOM_ERROR(handle, osn);

	/* move cursor */
	e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), osn, &tid, NULL);
	if(e < eNOERROR) {
		e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), osn);
		if(e < eNOERROR) LOM_ERROR(handle, e);

		e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
		if(e < eNOERROR) LOM_ERROR(handle, e);

		LOM_ERROR(handle, e);
	}

	if(e == EOS) { 
    	if(e < eNOERROR) LOM_ERROR(handle, eNOSUCHSTEMIZEREXIST_LOM);
	}
	else {
	    e = LRDS_DestroyTuple(LOM_GET_LRDS_HANDLE(handle), osn, SM_TRUE, &tid);
	    if(e < eNOERROR) LOM_ERROR(handle, e);
	}

	/* close scan */
	e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), osn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	/* close relation */
	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}

Four LOM_Text_GetStemizerInfo(
	LOM_Handle *handle, 
	Four volId,
	Four stemizerNo,
	char *stemizerFilePath,
	char *stemizerFunctionName
)
{
	Four e;
	BoundCond bound;
	Two keyLen;
	Four orn;
	Four osn;
	lrds_RelTableEntry *relTableEntry;
	TupleID tid;
	ColListStruct clist[LOM_SYSTEXTSTEMIZERINFO_NUM_COLS];
	LockParameter lockup;
	Four count;

	/* make bound condition */
	bound.op = SM_EQ;
	keyLen = 0;
	bcopy(&stemizerNo, &(bound.key.val[0]), sizeof(Four));
	keyLen += sizeof(Four);
	bound.key.len = keyLen;

	/* open text-stemizer-info. table */
	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSTEXTPREFERENCES_CLASSNAME);
	if(orn < eNOERROR) LOM_ERROR(handle, orn);

	/* Get the relation table entry. */
	relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);

	lockup.mode = L_IS;
	lockup.duration = L_COMMIT;

	/* open index scan */
	osn = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), orn, &((LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[0].iid), &bound, &bound,
					0, NULL, &lockup);
	if(osn < eNOERROR) LOM_ERROR(handle, osn);

	e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), osn, &tid, NULL);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	if(e == EOS) LOM_ERROR(handle, eNOSUCHSTEMIZEREXIST_LOM);

	count = 0;
	clist[count].colNo = LOM_SYSTEXTSTEMIZERINFO_STEMIZERFILEPATH_COLNO;
	clist[count].start = ALL_VALUE;
	clist[count].length = clist[count].dataLength = LOM_MAXSTEMIZERFILEPATHNAME;
	clist[count].data.ptr = stemizerFilePath;
	count++;

	clist[count].colNo = LOM_SYSTEXTSTEMIZERINFO_STEMIZERFUNCTIONNAME_COLNO;
	clist[count].start = ALL_VALUE;
	clist[count].length = clist[count].dataLength = LOM_MAXSTEMIZERFUNCTIONNAME;
	clist[count].data.ptr = stemizerFunctionName;
	count++;

	e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), osn, SM_TRUE, &tid, count, &clist[0]);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), osn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	stemizerFilePath[clist[0].retLength] = '\0';
	stemizerFunctionName[clist[1].retLength] = '\0';

	return eNOERROR;
}

/* pass a function pointer as output parameter instead of return value */
Four lom_Text_GetStemizerFPtr(
	LOM_Handle			*handle,
	Four				ocn,
	Four				colNo,
	lom_FptrToStemizer	*stemizerFPtr
)
{
	Four j;

	for(j = 0; j < LOM_USEROPENCLASSTABLE(handle)[ocn].numOfTextAttrs; j++) {
		if(LOM_USEROPENCLASSTABLE(handle)[ocn].textColNo[j] == colNo) {
#ifndef PRELOAD_STEMIZER
			if(!(LOM_USEROPENCLASSTABLE(handle)[ocn].fptrToStemizer[j])) {
				lrds_RelTableEntry *relTableEntry;
				Four e;
				relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), ocn);
				/* get a function pointer to filter */
				e = LOM_Text_OpenHandleForStemizer(handle,
					relTableEntry->ri.fid.volNo,
					LOM_USEROPENCLASSTABLE(handle)[ocn].classID,
					GET_USERLEVEL_COLNO(colNo),
					&LOM_USEROPENCLASSTABLE(handle)[ocn].handleForDLOfStemizer[j],
					&LOM_USEROPENCLASSTABLE(handle)[ocn].fptrToStemizer[j]);
				if(e < eNOERROR)
				{
					*stemizerFPtr = NULL;
					LOM_ERROR(handle, e);
				}
			}
#endif
		}

		*stemizerFPtr = LOM_USEROPENCLASSTABLE(handle)[ocn].fptrToStemizer[j];
		return eNOERROR;
	}

	*stemizerFPtr = NULL;
	return eNOERROR;
}

Four lom_Text_CreateStemizerInfoCatalog(
	LOM_Handle *handle, 
	Four volId
)
{
	ColInfo lomSysStemizerInfo[LOM_SYSTEXTSTEMIZERINFO_NUM_COLS];
	LRDS_IndexDesc idesc;
	IndexID iid;
	Four e;

	/* stemizer name */
	lomSysStemizerInfo[LOM_SYSTEXTSTEMIZERINFO_STEMIZERNAME_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysStemizerInfo[LOM_SYSTEXTSTEMIZERINFO_STEMIZERNAME_COLNO].type = SM_VARSTRING;
	lomSysStemizerInfo[LOM_SYSTEXTSTEMIZERINFO_STEMIZERNAME_COLNO].length = LOM_MAXKEYWORDEXTRACTORNAME;

	/* version no */
	lomSysStemizerInfo[LOM_SYSTEXTSTEMIZERINFO_VERSION_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysStemizerInfo[LOM_SYSTEXTSTEMIZERINFO_VERSION_COLNO].type = LOM_LONG_VAR;
	lomSysStemizerInfo[LOM_SYSTEXTSTEMIZERINFO_VERSION_COLNO].length = LOM_LONG_SIZE_VAR;

	/* stemizer file path */
	lomSysStemizerInfo[LOM_SYSTEXTSTEMIZERINFO_STEMIZERFILEPATH_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysStemizerInfo[LOM_SYSTEXTSTEMIZERINFO_STEMIZERFILEPATH_COLNO].type = SM_VARSTRING;
	lomSysStemizerInfo[LOM_SYSTEXTSTEMIZERINFO_STEMIZERFILEPATH_COLNO].length = LOM_MAXKEYWORDEXTRACTORFILEPATHNAME;

	/* stemizer function name */
	lomSysStemizerInfo[LOM_SYSTEXTSTEMIZERINFO_STEMIZERFUNCTIONNAME_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysStemizerInfo[LOM_SYSTEXTSTEMIZERINFO_STEMIZERFUNCTIONNAME_COLNO].type = SM_VARSTRING;
	lomSysStemizerInfo[LOM_SYSTEXTSTEMIZERINFO_STEMIZERFUNCTIONNAME_COLNO].length = LOM_MAXKEYWORDEXTRACTORFUNCTIONNAME;

	/* stemizer no */
	lomSysStemizerInfo[LOM_SYSTEXTSTEMIZERINFO_STEMIZERNO_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysStemizerInfo[LOM_SYSTEXTSTEMIZERINFO_STEMIZERNO_COLNO].type = LOM_LONG_VAR;
	lomSysStemizerInfo[LOM_SYSTEXTSTEMIZERINFO_STEMIZERNO_COLNO].length = LOM_LONG_SIZE_VAR;

	idesc.indexType = SM_INDEXTYPE_BTREE;
	idesc.kinfo.btree.flag = KEYFLAG_UNIQUE | KEYFLAG_CLUSTERING;
	idesc.kinfo.btree.nColumns = 1;
	idesc.kinfo.btree.columns[0].colNo = LOM_SYSTEXTSTEMIZERINFO_STEMIZERNO_COLNO;
	idesc.kinfo.btree.columns[0].flag = KEYINFO_COL_ASC;

	/* create sysTextFilterInfo */
	e = LRDS_CreateRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSTEXTSTEMIZERINFO_CLASSNAME, &idesc,
		LOM_SYSTEXTPREFERENCES_NUM_COLS, &lomSysStemizerInfo[0], SM_FALSE);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	idesc.indexType = SM_INDEXTYPE_BTREE;
	idesc.kinfo.btree.flag = KEYFLAG_UNIQUE;
	idesc.kinfo.btree.nColumns = 2;
	idesc.kinfo.btree.columns[0].colNo = LOM_SYSTEXTSTEMIZERINFO_STEMIZERNAME_COLNO;
	idesc.kinfo.btree.columns[0].flag = KEYINFO_COL_ASC;
	idesc.kinfo.btree.columns[1].colNo = LOM_SYSTEXTSTEMIZERINFO_VERSION_COLNO;
	idesc.kinfo.btree.columns[1].flag = KEYINFO_COL_ASC;

	/* add index on stemizer name + version */
	e = LRDS_AddIndex(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSTEXTSTEMIZERINFO_CLASSNAME, &idesc, &iid);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}

Four lom_Text_DestroyStemizerInfoCatalog(
	LOM_Handle *handle, 
	Four volId
)
{
	Four e;

	e = LRDS_DestroyRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSTEXTSTEMIZERINFO_CLASSNAME);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}

#else /* SLIMDOWN_TEXTIR */

#include "LOM_Internal.h"
#include "LOM.h"
#include "Catalog.h"
#include "Catalog_Internal.h"


Four LOM_Text_AddStemizer(
	LOM_Handle *handle, 
	Four volId,
	char *stemizerName,
	Four version,
	char *stemizerFilePath,
	char *stemizerFunctionName,
	Four *stemizerNo
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four LOM_Text_AddDefaultStemizer(
	LOM_Handle *handle, 
	Four volId,
	char *stemizerName,
	Four version,
	char *stemizerFilePath,
	char *stemizerFunctionName
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four LOM_Text_DropStemizer(
	LOM_Handle *handle, 
	Four volId,
	char *stemizerName,
	Four version
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four LOM_Text_GetStemizerNo(
	LOM_Handle *handle, 
	Four volId,
	char *stemizerName,
	Four version,
	Four *stemizerNo
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four LOM_Text_SetStemizer(
	LOM_Handle *handle, 
	Four volId,
	Four classID,
	Four colNo,
	Four stemizerNo
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four LOM_Text_ResetStemizer(
	LOM_Handle *handle, 
	Four volId,
	Four classID,
	Four colNo
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four LOM_Text_GetStemizerInfo(
	LOM_Handle *handle, 
	Four volId,
	Four stemizerNo,
	char *stemizerFilePath,
	char *stemizerFunctionName
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_GetStemizerFPtr(
	LOM_Handle			*handle,
	Four				ocn,
	Four				colNo,
	lom_FptrToStemizer	*stemizerFPtr
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_CreateStemizerInfoCatalog(
	LOM_Handle *handle, 
	Four volId
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_DestroyStemizerInfoCatalog(
	LOM_Handle *handle, 
	Four volId
)
{
	return eTEXTIR_NOTENABLED_LOM;
}

#endif /* SLIMDOWN_TEXTIR */
