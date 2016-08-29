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

#include "LOM_Internal.h"
#include "LOM.h"
#include "Catalog.h"
#include "Catalog_Internal.h"

Four LOM_MLGF_OpenIndexScan(
	LOM_Handle		*handle, 
    Four			ocn,			/* IN open relation number */
    LOM_IndexID		*iid,			/* IN MLGF index to be used for this scan */
    MLGF_HashValue	lowerBounds[],	/* IN lower bounds of region */
    MLGF_HashValue	upperBounds[],	/* IN upper bounds of region */
    Four			nBools,			/* IN number of boolean expressions */
    BoolExp			bool[],			/* IN array of boolean expressions */
    LockParameter	*lockup)		/* IN lock mode & duration */
{
	Four				e;
	lrds_RelTableEntry*	relTableEntry; /* pointer to an entry of relation table */
	Four				scanId;
	Four				i;

	/* Find the empty scan table entry. */
	scanId = e = lom_ScanTableAllocEntry(handle);
	if (e < 0) LOM_ERROR(handle, e);

	/* set open class num */
	LOM_SCANTABLE(handle)[scanId].ocn = ocn;

	/* adjust bool[i].colNo to system level colno */
	for(i = 0; i < nBools; i++)
		bool[i].colNo = GET_SYSTEMLEVEL_COLNO(bool[i].colNo);

	/* open lrds-level scan */
	LOM_SCANTABLE(handle)[scanId].lrdsScanId = LRDS_MLGF_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), ocn, &(iid->index.physical_iid), lowerBounds, upperBounds, nBools, bool, lockup);
	if(LOM_SCANTABLE(handle)[scanId].lrdsScanId < 0) LOM_ERROR(handle, LOM_SCANTABLE(handle)[scanId].lrdsScanId);

	/* adjust bool[i].colNo to user level colno */
	for(i = 0; i < nBools; i++)
		bool[i].colNo = GET_USERLEVEL_COLNO(bool[i].colNo);

	/* set scan type */
	LOM_SCANTABLE(handle)[scanId].scanType = LOM_GENERAL_SCAN;

	/* copy lock mode and lock duration */
	LOM_SCANTABLE(handle)[scanId].lockup.duration = lockup->duration;
	LOM_SCANTABLE(handle)[scanId].lockup.mode = lockup->mode;

	/* open content table scan */
	if(LOM_USEROPENCLASSTABLE(handle)[ocn].numOfTextAttrs > 0) {
		relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), ocn);
		LOM_SCANTABLE(handle)[scanId].contentTableScanId = 
			LRDS_OpenSeqScan(LOM_GET_LRDS_HANDLE(handle), LOM_USEROPENCLASSTABLE(handle)[ocn].ornForContentTable, FORWARD, 0, NULL, lockup);
		if(LOM_SCANTABLE(handle)[scanId].contentTableScanId < 0) LOM_ERROR(handle, LOM_SCANTABLE(handle)[scanId].contentTableScanId);
	}

    if(LOM_USEROPENCLASSTABLE(handle)[ocn].numOfodmgCollAttrs > 0)
    {
        for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ocn].numOfodmgCollAttrs; i++)
        {
            LOM_SCANTABLE(handle)[scanId].odmgCollDataScanId[i] = e =
                LRDS_OpenSeqScan(LOM_GET_LRDS_HANDLE(handle), LOM_USEROPENCLASSTABLE(handle)[ocn].odmgCollDataOrn[i],
                                 FORWARD, 0, NULL, lockup);
            if(e < 0) LOM_ERROR(handle, e);
        }
        for(; i < LOM_MAXNUMOFODMGCOLLCOLUMN; i++)
        {
            LOM_SCANTABLE(handle)[scanId].odmgCollDataScanId[i] = NIL;
        }
    }

	return scanId;
}

Four LOM_MLGF_SearchNearTuple(
	LOM_Handle		*handle,
    Four			ocn,			/* IN open relation number */
    LOM_IndexID		*iid,			/* IN the used index */
    MLGF_HashValue	kval[],			/* IN hash values of the given object */
    OID				*oid,			/* OUT TupleID of the near tuple */
    LockParameter	*lockup)		/* IN request lock or not */
{
	TupleID *tid;
	Four    e;

	e = LRDS_MLGF_SearchNearTuple(LOM_GET_LRDS_HANDLE(handle), ocn, &(iid->index.physical_iid), kval, tid, lockup);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	memcpy(oid, tid, sizeof(TupleID));
	oid->classID = LOM_USEROPENCLASSTABLE(handle)[ocn].classID;

	return eNOERROR;
}
