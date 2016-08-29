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
#include "Catalog_Internal.h"
#include "Catalog.h"
#include "LOM_ODMG_Collection.h"

Four LOM_CreateObjectByColList(
	LOM_Handle	*handle,
	Four ocnOrScanId,        /* IN ocn of scan_id */
	Boolean useScanFlag,	/* ocn or scan_id */
	Four    nCols,      /* IN number of columns to fetch */
	LOM_ColListStruct *clist,  /* INOUT columns to fetch */
	OID *oid)       /* OUT tuple to fetch */
{
	
	Four e; /* error code */
	Four e2; /* error code */
	Four orn;
	Four classId;
	lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */
	ColListStruct lrdsColListStruct[LOM_MAXNUMOFATTRIBUTE];
	catalog_SysClassesOverlay *ptrToSysClasses;
    catalog_SysAttributesOverlay *ptrToSysAttributes;
    Boolean textColumnExist;
	Four scanId;
	Four idxForClassInfo;
	Four i;
	Four v;
	char ctag[LOM_MAXNUMOFATTRIBUTE];
	char itag[LOM_MAXNUMOFINDEX];


	if(useScanFlag) {
		scanId = LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId;

		if (!LRDS_VALID_SCANID(LOM_GET_LRDS_HANDLE(handle), scanId)) LOM_ERROR(handle, eBADPARAMETER_LOM);

		orn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[scanId].orn;
	}
	else orn = ocnOrScanId;

	lrdsColListStruct[0].colNo = 0;
	lrdsColListStruct[0].start = ALL_VALUE;
	lrdsColListStruct[0].length = sizeof(Four);
	lrdsColListStruct[0].dataLength = sizeof(Four);
	lrdsColListStruct[0].nullFlag = SM_FALSE;

	relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);
	
	e = Catalog_GetClassInfo(handle, relTableEntry->ri.fid.volNo, LOM_USEROPENCLASSTABLE(handle)[orn].classID, &idxForClassInfo);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	v = Catalog_GetVolIndex(handle, relTableEntry->ri.fid.volNo);
	if(v < eNOERROR) LOM_ERROR(handle, e);

	ptrToSysClasses = &CATALOG_GET_CLASSINFOTBL(handle, v)[idxForClassInfo];

    ptrToSysAttributes =  &CATALOG_GET_ATTRINFOTBL(handle, v)[CATALOG_GET_ATTRINFOTBL_INDEX(ptrToSysClasses)];

	if(LOM_USEROPENCLASSTABLE(handle)[orn].numOfTextAttrs > 0 ) textColumnExist = SM_TRUE;
	else textColumnExist = SM_FALSE;

	for(i = 0; i < nCols; i++) {

		bcopy(&(clist[i]), &(lrdsColListStruct[i+1]), sizeof(ColListStruct));

		/* conversion between LOM type and LRDS type */
		switch(ptrToSysAttributes[GET_SYSTEMLEVEL_COLNO(clist[i].colNo)].type) {
            case LOM_BOOLEAN:
                            lrdsColListStruct[i+1].data.l = clist[i].data.b;
                            break;
            case LOM_USHORT:
                            lrdsColListStruct[i+1].data.s = clist[i].data.us;
                            break;
            case LOM_ULONG:
                            lrdsColListStruct[i+1].data.l = clist[i].data.ul;
                            break;
            case LOM_DATE :
                            lrdsColListStruct[i+1].data.l = clist[i].data.date;
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

	if(textColumnExist) {
		/* get logical id */
		ASSIGN_VALUE_TO_COL_LIST(lrdsColListStruct[0], lom_Text_GetAndIncrementLogicalId(handle, orn), sizeof(Four));

		if(useScanFlag)
			e = LRDS_CreateTuple(LOM_GET_LRDS_HANDLE(handle), scanId, useScanFlag, nCols + 1, lrdsColListStruct,(TupleID*)oid);
		else
			e = LRDS_CreateTuple(LOM_GET_LRDS_HANDLE(handle), ocnOrScanId, useScanFlag, nCols + 1, &lrdsColListStruct[0], (TupleID*)oid);
		if (e < 0) LOM_ERROR(handle, e);
	}
	else {
		if(useScanFlag)
			e = LRDS_CreateTuple(LOM_GET_LRDS_HANDLE(handle), scanId, useScanFlag, nCols, &lrdsColListStruct[1], (TupleID *)oid);
		else
			e = LRDS_CreateTuple(LOM_GET_LRDS_HANDLE(handle), ocnOrScanId, useScanFlag, nCols, &lrdsColListStruct[1], (TupleID*)oid);
		if (e < 0) LOM_ERROR(handle, e);
	}

	/* For ODMG Collection */
	for(i = 0; i < CATALOG_GET_ATTRNUM(ptrToSysClasses); i++) 
	{
		if(ptrToSysAttributes[i].complexType == LOM_COMPLEXTYPE_ODMG_COLLECTIONSET ||
	       ptrToSysAttributes[i].complexType == LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG ||
       	   ptrToSysAttributes[i].complexType == LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST ||
       	   ptrToSysAttributes[i].complexType == LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY)
		{
			/* create initial column value */
			ODMG_CollectionDesc collDesc;
			collDesc.nElements = 0;
			SET_NILTUPLEID(collDesc.dataTid);

			e = LOM_ODMG_Collection_SetDescriptor(handle, ocnOrScanId, useScanFlag, oid, ptrToSysAttributes[i].colNo - 1, &collDesc);
			if (e < 0) LOM_ERROR(handle, e);
		}
	}

	if(oid != (OID *)NULL) {
		oid->classID =  LOM_USEROPENCLASSTABLE(handle)[orn].classID;
	}

	return eNOERROR;
}

