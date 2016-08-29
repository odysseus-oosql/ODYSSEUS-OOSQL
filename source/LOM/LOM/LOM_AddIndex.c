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
 * Module: LOM_AddIndex.c
 *
 * Description:
 *  Add Index .
 *
 * Imports:
 *  LRDS_AddIndex(LOM_GET_LRDS_HANDLE(handle), )
 *
 * Exports:
 *  Four LOM_AddIndex( Four ,char *, char *, LOM_IndexDesc *, IndexID *)
 *
 * Returns:
 *  Error code
 *    eBADPARAMETER_LOM
 *    eVOLUMENOTMOUNTED_LOM
 *    eINDEXDUPLICATED_LOM
 *    eMEMORYALLOCERR_LOM
 *    some errors caused by function calls
 *
 */


#include <string.h>
#include <malloc.h>
#include "LOM_Internal.h"
#include "LOM.h"
#include "Catalog_Internal.h"
#include "Catalog.h"


Four LOM_AddIndex(
	LOM_Handle *handle,
	Four volId,         /* IN volume where the relation is placed */
	char *className,      /* IN class name */
	char *indexName,	/* IN index name */
	LOM_IndexDesc *idesc,  /* IN index information */
	LOM_IndexID *iid)       /* OUT IndexID of the newly added index */
{

	LockParameter lockup;   /* relName will be locked commit duration */
	Four orn;           /* open relation number */
	Four e;
	ColListStruct	clist[LOM_SYSINDEXES_NUM_COLS + 1];
	Four classId;
	Four indexInfoTblEntryIndex;
	Four newIndexInfoTblEntryIndex;
	catalog_SysClassesOverlay *tmpNode;
	catalog_SysIndexesOverlay *tmpIndex;
	Four i;
	Four v;
	Boolean tmpFlag;

	/* check parameters */
	if (volId < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);
    
	if (className == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);

	if (indexName == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);

	if (idesc == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);

	if (iid == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);

	if(idesc->indexType == SM_INDEXTYPE_BTREE)
		for(i = 0; i< idesc->kinfo.btree.nColumns; i++) {
			idesc->kinfo.btree.columns[i].colNo++;
		}
	else 
		for(i = 0; i< idesc->kinfo.btree.nColumns; i++) {
			idesc->kinfo.mlgf.colNo[i]++;
		}

	e = LRDS_AddIndex(LOM_GET_LRDS_HANDLE(handle), volId, className, (LRDS_IndexDesc *)idesc, &iid->index.physical_iid);
	if ( e < 0) LOM_ERROR(handle, e);

	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, className);
	if(orn < eNOERROR) LOM_ERROR(handle, orn);

	if(LRDS_IS_TEMPORARY_RELATION(LOM_GET_LRDS_HANDLE(handle), orn)) tmpFlag = SM_TRUE;
	else tmpFlag = SM_FALSE;

	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if ( e < 0) LOM_ERROR(handle, e);

	if(tmpFlag) {
		e = Catalog_GetTemporaryClassId(handle, volId, className, &classId);
		/* reflect in memory catalog */
		e = Catalog_AddIndex(handle, volId, classId, indexName, idesc, iid);
		if( e < 0 ) LOM_ERROR(handle, e);

		return eNOERROR;
	}

	if(idesc->indexType == SM_INDEXTYPE_BTREE)
		for(i = 0; i< idesc->kinfo.btree.nColumns; i++) {
			idesc->kinfo.btree.columns[i].colNo--;
		}
	else 
		for(i = 0; i< idesc->kinfo.btree.nColumns; i++) {
			idesc->kinfo.mlgf.colNo[i]--;
		}

	iid->isLogical = SM_FALSE;

	/* START : LOM_SYSINDEXES */ /* THINK */

	/* set the lockup parameter */
	lockup.mode = L_IX;
	lockup.duration = L_COMMIT;

	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId,LOM_SYSINDEXES_CLASSNAME);
	if (orn < 0) LOM_ERROR(handle, orn);

	/* Construct 'clist'. */
	/* index name */
	clist[LOM_SYSINDEXES_INDEXNAME_COLNO].colNo = LOM_SYSINDEXES_INDEXNAME_COLNO;
	clist[LOM_SYSINDEXES_INDEXNAME_COLNO].start = ALL_VALUE;
	clist[LOM_SYSINDEXES_INDEXNAME_COLNO].dataLength = strlen(indexName);
	clist[LOM_SYSINDEXES_INDEXNAME_COLNO].data.ptr = indexName;
	clist[LOM_SYSINDEXES_INDEXNAME_COLNO].nullFlag = SM_FALSE;
	
	/* class name */
	clist[LOM_SYSINDEXES_CLASSNAME_COLNO].colNo = LOM_SYSINDEXES_CLASSNAME_COLNO;
	clist[LOM_SYSINDEXES_CLASSNAME_COLNO].start = ALL_VALUE;
	clist[LOM_SYSINDEXES_CLASSNAME_COLNO].dataLength = strlen(className);
	clist[LOM_SYSINDEXES_CLASSNAME_COLNO].data.ptr = className;
	clist[LOM_SYSINDEXES_CLASSNAME_COLNO].nullFlag = SM_FALSE;
	
	/* index id */
	clist[LOM_SYSINDEXES_INDEXID_COLNO].colNo = LOM_SYSINDEXES_INDEXID_COLNO;
	clist[LOM_SYSINDEXES_INDEXID_COLNO].start = ALL_VALUE;
	clist[LOM_SYSINDEXES_INDEXID_COLNO].dataLength = sizeof(LOM_IndexID);
	clist[LOM_SYSINDEXES_INDEXID_COLNO].data.ptr = iid; 
	clist[LOM_SYSINDEXES_INDEXID_COLNO].nullFlag = SM_FALSE;

	e = LRDS_CreateTuple(LOM_GET_LRDS_HANDLE(handle), orn, SM_FALSE, LOM_SYSINDEXES_NUM_COLS, &(clist[0]), (TupleID*)NULL);
	if (e < 0) LOM_ERROR(handle, e);
	
	/* Close the data file */
	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if (e < 0) LOM_ERROR(handle, e);

    /* END : LOM_SYSINDEXES */ 

	e = lom_GetClassId(handle, volId, className, &classId);
	if( e < 0 ) LOM_ERROR(handle, e);

	/* reflect in memory catalog */
	e = Catalog_AddIndex(handle, volId, classId, indexName, idesc, iid);
	if( e < 0 ) LOM_ERROR(handle, e);

	return(eNOERROR);

} /* LOM_AddIndex() */


