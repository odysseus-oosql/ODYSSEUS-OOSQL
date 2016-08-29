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


Four LOM_FetchObjectByColList(
	LOM_Handle *handle,
	Four ocnOrScanId,      /* IN ocn or scanId*/
	Boolean useScanFlag,	/* ocn or scan id */
	OID *oid,       /* IN tuple to fetch */
	Four    nCols,      /* IN number of columns to fetch */
	LOM_ColListStruct clist[])  /* INOUT columns to fetch */
{
	Four scanId;	/* scan id */
	Four i;
	Four e;		/* error code */
	lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */
	Four idxForClassInfo;
	Four v;
	Four orn;
	catalog_SysClassesOverlay *ptrToSysClasses;	/* pointer to sysclasses */
	catalog_SysAttributesOverlay *ptrToSysAttributes; /* pointer to sysattributes */
	ColListStruct lrdsColListStruct[LOM_MAXNUMOFATTRIBUTE];
	Four volId;

	if(useScanFlag) {
		scanId = LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId;

		if (!LRDS_VALID_SCANID(LOM_GET_LRDS_HANDLE(handle), scanId))
			LOM_ERROR(handle, eBADPARAMETER_LOM);

		orn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[scanId].orn;
	}
	else orn = ocnOrScanId;

	relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);

	volId = relTableEntry->ri.fid.volNo;

	e = Catalog_GetClassInfo(handle, volId, LOM_USEROPENCLASSTABLE(handle)[orn].classID, &idxForClassInfo);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	v = Catalog_GetVolIndex(handle, volId);
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
		bcopy(&(clist[i].data), &(lrdsColListStruct[i].data), sizeof(clist[i].data));

		switch(ptrToSysAttributes[GET_SYSTEMLEVEL_COLNO(clist[i].colNo)].type) {
			case LOM_USHORT:
					ASSIGN_VALUE_TO_COL_LIST(lrdsColListStruct[i], clist[i].data.us, sizeof(Two));
					break;
			case LOM_ULONG:
					ASSIGN_VALUE_TO_COL_LIST(lrdsColListStruct[i], clist[i].data.ul, sizeof(Four));
					break;
			case LOM_DATE :
					ASSIGN_VALUE_TO_COL_LIST(lrdsColListStruct[i], clist[i].data.date, sizeof(Four));
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
			case LOM_OCTET:
					break;
			case LOM_BOOLEAN:
					ASSIGN_VALUE_TO_COL_LIST(lrdsColListStruct[i], clist[i].data.b, sizeof(Four));
					break;
		}
	}
	
	/* fetch object by using LRDS interface */
	if(useScanFlag)
		e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), scanId, useScanFlag, (TupleID *)oid, nCols, lrdsColListStruct);
	else
		e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), ocnOrScanId, useScanFlag, (TupleID *)oid, nCols, lrdsColListStruct);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	/* conversion between LOM type and LRDS type */
	for(i = 0; i < nCols; i++) {
		clist[i].retLength = lrdsColListStruct[i].retLength;
		clist[i].nullFlag = lrdsColListStruct[i].nullFlag;
		switch(ptrToSysAttributes[GET_SYSTEMLEVEL_COLNO(clist[i].colNo)].type) {
			case LOM_OCTET:
					break;
			case LOM_BOOLEAN:
					clist[i].data.b = lrdsColListStruct[i].data.l;
					break;
			case LOM_USHORT:
					clist[i].data.us = lrdsColListStruct[i].data.s;
					break;
			case LOM_ULONG:
					clist[i].data.ul = lrdsColListStruct[i].data.l;
					break;
			case LOM_DATE :
					clist[i].data.date = lrdsColListStruct[i].data.l;
					break;
			case LOM_INTERVAL :	
					clist[i].data.interval = lrdsColListStruct[i].data.d;
					break;
			case LOM_TIME:
			case LOM_TIMESTAMP:
			case LOM_STRING:
			case LOM_VARSTRING:
					break;
			default:
					bcopy(&(lrdsColListStruct[i].data), &(clist[i].data), sizeof(lrdsColListStruct[i].data));
					break;
		}
	}
	
	return eNOERROR;
}

