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


Four LOM_UpdateObjectByColList(
	LOM_Handle *handle, 
	Four ocnOrScanId,        /* IN ocn or scan_id */
	Boolean useScanFlag,	/* IN flag */
	OID *oid,       /* IN tuple to fetch */
	Four    nCols,      /* IN number of columns to fetch */
	LOM_ColListStruct clist[])  /* INOUT columns to fetch */
{
	Four scanId; 
	Four i;
	Four e;
	Four idxForClassInfo;
	lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */
	Four v;
	Four orn;
	catalog_SysClassesOverlay *ptrToSysClasses;     /* pointer to sysclasses */
	catalog_SysAttributesOverlay *ptrToSysAttributes; /* pointer to sysattributes */
	ColListStruct lrdsColListStruct[LOM_MAXNUMOFATTRIBUTE];
	char ctag[LOM_MAXNUMOFATTRIBUTE];
	char itag[LOM_MAXNUMOFINDEX];
	Boolean isEmbeddedAttributeUpdated;

	Four j;
	Four logicalDocId;
	Four classEntryIndex;
	catalog_SysClassesOverlay *classInfo;
	catalog_SysIndexesOverlay *indexInfo;
	catalog_SysAttributesOverlay *attrInfo;
	InMemory_PostingStructureInfo *postingInfo;
	Four nColsForUpdatingEmbeddedAttrs;
	Two embeddedAttrsForUpdate[LOM_MAXNUMOFATTRIBUTE];

	if(useScanFlag) {
		scanId = LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId;

		if (!LRDS_VALID_SCANID(LOM_GET_LRDS_HANDLE(handle), scanId)) LOM_ERROR(handle, eBADPARAMETER_LOM);

		orn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[scanId].orn;
	}
	else orn = ocnOrScanId;

	relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);

	e = Catalog_GetClassInfo(handle, relTableEntry->ri.fid.volNo, LOM_USEROPENCLASSTABLE(handle)[orn].classID, &idxForClassInfo);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	v = Catalog_GetVolIndex(handle, relTableEntry->ri.fid.volNo);
	if(v < eNOERROR) LOM_ERROR(handle, e);

	ptrToSysClasses = &CATALOG_GET_CLASSINFOTBL(handle, v)[idxForClassInfo];
	ptrToSysAttributes =  &CATALOG_GET_ATTRINFOTBL(handle, v)[CATALOG_GET_ATTRINFOTBL_INDEX(ptrToSysClasses)];

	/* conversion between LOM type and LRDS type */
	for(i = 0; i < nCols; i++) {
		/* copy access information */
		lrdsColListStruct[i].colNo = GET_SYSTEMLEVEL_COLNO(clist[i].colNo);
		lrdsColListStruct[i].start = clist[i].start;
		lrdsColListStruct[i].length = clist[i].length;
		lrdsColListStruct[i].dataLength = clist[i].dataLength;
		lrdsColListStruct[i].nullFlag = clist[i].nullFlag;
		memcpy(&(lrdsColListStruct[i].data), &(clist[i].data), sizeof(clist[i].data));

		switch(ptrToSysAttributes[GET_SYSTEMLEVEL_COLNO(clist[i].colNo)].type) {
            case LOM_BOOLEAN:
                lrdsColListStruct[i].data.l = clist[i].data.b;
                break;
            case LOM_USHORT:
                lrdsColListStruct[i].data.s = clist[i].data.us;
                break;
            case LOM_ULONG:
                lrdsColListStruct[i].data.l = clist[i].data.ul;
                break;
            case LOM_DATE :
                lrdsColListStruct[i].data.l = clist[i].data.date;
                break;
			case LOM_TIME :
				lrdsColListStruct[i].data.ptr = &(clist[i].data.time);
				break;
			case LOM_TIMESTAMP :
				lrdsColListStruct[i].data.ptr = &(clist[i].data.timestamp);
				break;
			case LOM_INTERVAL :
				lrdsColListStruct[i].data.d = clist[i].data.interval;
				break;
		}
	}

	/* update object */
	if(useScanFlag)
		e = LRDS_UpdateTuple(LOM_GET_LRDS_HANDLE(handle), scanId, useScanFlag, (TupleID *)oid, nCols, lrdsColListStruct);
	else
		e = LRDS_UpdateTuple(LOM_GET_LRDS_HANDLE(handle), ocnOrScanId, useScanFlag, (TupleID *)oid, nCols, lrdsColListStruct);
	if(e < eNOERROR) LOM_ERROR(handle, e);


	/* START: automatic index for embedded attributes in text index */

	v = Catalog_GetVolIndex(handle, relTableEntry->ri.fid.volNo);

	if((e = Catalog_GetClassInfo(handle, relTableEntry->ri.fid.volNo, LOM_USEROPENCLASSTABLE(handle)[orn].classID, &classEntryIndex)) < eNOERROR)
		LOM_ERROR(handle, e);

	classInfo = &CATALOG_GET_CLASSINFOTBL(handle, v)[classEntryIndex];
	attrInfo = &CATALOG_GET_ATTRINFOTBL(handle, v)[CATALOG_GET_ATTRINFOTBL_INDEX(classInfo)];
	indexInfo = &(CATALOG_GET_INDEXINFOTBL(handle, v)[CATALOG_GET_INDEXINFOTBL_INDEX(classInfo)]);

	for(i = 0; i < CATALOG_GET_ATTRNUM(classInfo); i++) ctag[i] = SM_FALSE;
	for(i = 0; i < nCols; i++) ctag[GET_SYSTEMLEVEL_COLNO(clist[i].colNo)] = SM_TRUE;

	nColsForUpdatingEmbeddedAttrs = 0;
	for(i = 0; i < nCols; i++) {
		if(ctag[GET_SYSTEMLEVEL_COLNO(clist[i].colNo)]) {
			embeddedAttrsForUpdate[nColsForUpdatingEmbeddedAttrs++] = i;
		}
	}

	isEmbeddedAttributeUpdated = SM_FALSE;
	for(i = 0; i < CATALOG_GET_INDEXNUM(classInfo); i++) 
	{
		itag[i] = SM_FALSE;

		if(indexInfo[i].indexType == LOM_INDEXTYPE_TEXT) 
		{
			postingInfo = &(indexInfo[i].kdesc.invertedIndex.postingInfo);
			for(j = 0; j < postingInfo->nEmbeddedAttributes; j++) 
			{
				if(ctag[postingInfo->embeddedAttrNo[j]]) 
				{
					itag[i] = SM_TRUE;
					isEmbeddedAttributeUpdated = SM_TRUE;
				}
			}
		}
	}

	if(isEmbeddedAttributeUpdated)
	{
		logicalDocId = lom_Text_GetLogicalId(handle, ocnOrScanId, useScanFlag, oid);
		if(e < eNOERROR) LOM_ERROR(handle, e);
	}
	else
	{
		logicalDocId = -1;
	}

	for(i = 0; i < CATALOG_GET_INDEXNUM(classInfo); i++) {
		if(!itag[i]) continue;

		e = lom_Text_UpdateEmbeddedAttrs(handle, ocnOrScanId, useScanFlag, oid, logicalDocId, indexInfo[i].colNo[0]);
		if(e < eNOERROR) LOM_ERROR(handle, e);
	}

	/* END: automatic index for embedded attributes in text index */

	return eNOERROR;
}

