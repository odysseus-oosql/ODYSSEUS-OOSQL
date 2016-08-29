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
 * Module: LOM_DropIndex.c
 *
 * Description:
 * 		Drop index using index name, not index ID
 *
 * Imports:
 *  LRDS_DropIndex(LOM_GET_LRDS_HANDLE(handle), )
 *
 * Exports:
 *  Four LOM_DropIndex(handle, Four, char *, char*)
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



Four LOM_DropIndex(
	LOM_Handle *handle, 
	Four volId,	/* IN volume where the relation was placed */
	char *indexName /* IN index to drop */	
)
{
	/* check parameters */

	LOM_IndexID		iid;
	Four orn;		/* open relation number */
	BoolExp boolexp[1];     /* Boolean Expression */
	Four catScanId;     /* catalog scan number */
	Four e;
	ColListStruct   clist[LOM_SYSINDEXES_NUM_COLS + 1];
	LockParameter lockup;   /* lockup for SM_Fetch Tuple */
	TupleID tid;            /* tuple id for catalog entry in LOM level catalog */
	Four classID;
	Four indexInfoTblEntryIndex;
	catalog_SysClassesOverlay *tmpNode;
	catalog_SysIndexesOverlay *tmpIndex;
	Four i;
	Four classId;
	Four v;
	lrds_RelTableEntry *relTableEntry;
	Two keyLen;
	BoundCond bound;
	char className[LOM_MAXCLASSNAME];
	Four idxForClassInfo;
	Four idxForIndexInfo;
	Boolean tmpFlag;

	/* parameter check */
	if ( indexName == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);

	tmpFlag = SM_TRUE;

	/* get class id */
	e = Catalog_GetClassIdAndIndexInfoByTemporaryIndexName(handle, volId, indexName, &classId, &idxForIndexInfo);
	if(e < 0) {
		if(e == eNOSUCHINDEXFOUND_CATALOG) tmpFlag = SM_FALSE;
		else LOM_ERROR(handle, e);
	}

	if(tmpFlag) {
		/* get class name */
		e = Catalog_GetClassInfo(handle, volId, classId, &idxForClassInfo);
		if(e < eNOERROR) LOM_ERROR(handle, e);

		v = Catalog_GetVolIndex(handle, volId);
		if(v < eNOERROR) LOM_ERROR(handle, v);

		tmpNode = &CATALOG_GET_CLASSINFOTBL(handle, v)[idxForClassInfo];
		strcpy(className, tmpNode->name);

		tmpIndex = &(CATALOG_GET_INDEXINFOTBL(handle, v)[idxForIndexInfo]);

		e = LRDS_DropIndex(LOM_GET_LRDS_HANDLE(handle), volId, className, &(tmpIndex->iid.index.physical_iid));
		if(e < 0) LOM_ERROR(handle, e);

		/* reflect in memory catalog */
		e = Catalog_DropIndex(handle, volId, classId, &(tmpIndex->iid));
		if( e < 0 ) LOM_ERROR(handle, e);

		return eNOERROR;
	}

	/*
	** LOM_SYSINDEXES
	*/

	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSINDEXES_CLASSNAME);
	if( orn < 0 ) LOM_ERROR(handle, orn);

	relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);

	bound.op = SM_EQ;
	keyLen = strlen(indexName);
	bound.key.len = sizeof(Two) + keyLen;
	bcopy(&keyLen, &(bound.key.val[0]), sizeof(Two));
	bcopy(indexName, &(bound.key.val[sizeof(Two)]), keyLen);

	lockup.mode = L_IX;
	lockup.duration = L_COMMIT;

	catScanId = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), orn, &(LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[0].iid, &bound, &bound, 0, NULL, &lockup);
	if (catScanId < 0) LOM_ERROR(handle, catScanId);

	e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, &tid, NULL);
	if (e < 0) LOM_ERROR(handle, e);
	if(e == EOS) LOM_ERROR(handle, eINDEXNOTFOUND_LOM);

	clist[0].colNo = LOM_SYSINDEXES_INDEXID_COLNO;
	clist[0].start = ALL_VALUE;
	clist[0].dataLength = sizeof(LOM_IndexID);
	clist[0].data.ptr  = &iid;

	clist[1].colNo = LOM_SYSINDEXES_CLASSNAME_COLNO;
	clist[1].start = ALL_VALUE;
	clist[1].dataLength = LOM_MAXCLASSNAME;
	clist[1].data.ptr = className;

	e =  LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, SM_TRUE, &tid, 2, &clist[0]);
	if (e < 0) LOM_ERROR(handle, e);

	className[clist[1].retLength] = '\0';

	/* remove LRDS catalog */
	e = LRDS_DropIndex(LOM_GET_LRDS_HANDLE(handle), volId, className, &(iid.index.physical_iid));
	if(e < 0) LOM_ERROR(handle, e);

	e = LRDS_DestroyTuple(LOM_GET_LRDS_HANDLE(handle), catScanId, SM_TRUE, &tid);
	if (e < 0) LOM_ERROR(handle, e);

	e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), catScanId);
	if (e < 0) LOM_ERROR(handle, e);

	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if( e < 0 ) LOM_ERROR(handle, e);

	e = lom_GetClassId(handle, volId, className, &classId);
	if( e < 0 ) LOM_ERROR(handle, e);

	e = Catalog_DropIndex(handle, volId, classId, &iid);
	if( e < 0 ) LOM_ERROR(handle, e);

	return eNOERROR;

} /* LOM_DropIndex(handle, ) */
	     


