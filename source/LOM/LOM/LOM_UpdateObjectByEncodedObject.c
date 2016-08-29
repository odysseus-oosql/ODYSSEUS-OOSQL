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

Four LOM_UpdateObjectByEncodedObject(
	LOM_Handle *handle,
	Four ocnOrScanId,
	Boolean useScanFlag,
	OID *oid,
	Four nCols,
	Four encodedObjectLength,
	char *encodedObject
)
{
	Four e;
	Four orn;
	lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relatin table */
	LOM_ColListStruct *clist;
	Two colNum;
	Four i;
	Four offset;
	char nullFlag;
	char *ptrCol;
	catalog_SysClassesOverlay *ptrToSysClasses;
        catalog_SysAttributesOverlay *ptrToSysAttributes;
        Four idxForClassInfo;
        Four v;
        Four scanId;

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

	/* memory allocation */
	clist = (LOM_ColListStruct *)malloc(sizeof(LOM_ColListStruct) * nCols);
	if(clist == NULL) LOM_ERROR(handle, eOUTOFMEMORY_LOM);

	/* point to encoded object */
	offset = 0;
	ptrCol = encodedObject;

	/* decoding encoded object */
	for(i = 0; i < nCols; i++) {

		/* get column number */
		bcopy(&ptrCol[offset], (void*)&colNum, sizeof(Two));
		offset += sizeof(Two);
		clist[i].colNo = colNum;

		/* is null field */
		bcopy(&ptrCol[offset], &nullFlag, sizeof(char));
		offset += sizeof(char);
		if(nullFlag) {
			clist[i].nullFlag = SM_TRUE;
			continue;
		}
		clist[i].nullFlag = SM_FALSE;

		/* start field */
		bcopy(&ptrCol[offset], &clist[i].start, sizeof(Four));
		offset += sizeof(Four);

		/* length field */
		bcopy(&ptrCol[offset], &clist[i].length, sizeof(Four));
		offset += sizeof(Four);

		/* dataLength field */
		bcopy(&ptrCol[offset], &clist[i].dataLength, sizeof(Four));
		offset += sizeof(Four);

		if((ptrToSysAttributes[GET_SYSTEMLEVEL_COLNO(clist[i].colNo)].complexType == LOM_COMPLEXTYPE_ODMG_COLLECTIONSET) ||
		   (ptrToSysAttributes[GET_SYSTEMLEVEL_COLNO(clist[i].colNo)].complexType == LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG) ||
		   (ptrToSysAttributes[GET_SYSTEMLEVEL_COLNO(clist[i].colNo)].complexType == LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST) ||
		   (ptrToSysAttributes[GET_SYSTEMLEVEL_COLNO(clist[i].colNo)].complexType == LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY)
		) {
			clist[i].data.ptr = &ptrCol[offset];
			offset += clist[i].dataLength;
			continue;
		}

		switch(ptrToSysAttributes[GET_SYSTEMLEVEL_COLNO(clist[i].colNo)].type) {
			case LOM_SHORT:
				bcopy((void *)&ptrCol[offset], 
					(void*)&(clist[i].data.s), LOM_SHORT_SIZE);
				offset += LOM_SHORT_SIZE;
				break;
			case LOM_USHORT:
				bcopy((void *)&ptrCol[offset], 
					(void*)&(clist[i].data.us), LOM_USHORT_SIZE);
				offset += LOM_USHORT_SIZE;
				break;
			case LOM_INT:
				bcopy((void*)&ptrCol[offset],
					(void*)&(clist[i].data.i), LOM_INT_SIZE);
				offset += LOM_INT_SIZE;
				break;
			case LOM_LONG:
				bcopy((void*)&ptrCol[offset],
					(void*)&(clist[i].data.l), LOM_LONG_SIZE);
				offset += LOM_LONG_SIZE;
				break;
			case LOM_LONG_LONG:
				bcopy((void*)&ptrCol[offset],
					(void*)&(clist[i].data.ll), LOM_LONG_LONG_SIZE);
				offset += LOM_LONG_LONG_SIZE;
				break;
			case LOM_ULONG:
				bcopy((void*)&ptrCol[offset],
					(void*)&(clist[i].data.ul), LOM_ULONG_SIZE);
				offset += LOM_ULONG_SIZE;
				break;
			case LOM_FLOAT:
				bcopy((void*)&ptrCol[offset],
					(void*)&(clist[i].data.f), LOM_FLOAT_SIZE);
				offset += LOM_FLOAT_SIZE;
				break;
			case LOM_DOUBLE:
				bcopy((void*)&ptrCol[offset],
					(void*)&(clist[i].data.d),LOM_DOUBLE_SIZE);
				offset += LOM_DOUBLE_SIZE;
				break;
			case LOM_OID:
			case LOM_REF:
				bcopy((void*)&ptrCol[offset],
					(void*)&(clist[i].data.oid), LOM_OID_SIZE);
				offset += LOM_OID_SIZE;
				break;
			case LOM_OCTET:
				clist[i].data.ptr = &ptrCol[offset];
				offset += clist[i].dataLength;
				break;
			case LOM_BOOLEAN:
				bcopy((void*)&ptrCol[offset],
					(void*)&(clist[i].data.b), LOM_BOOLEAN_SIZE);
				offset += LOM_BOOLEAN_SIZE;
				break;
			case LOM_STRING:
			case LOM_TEXT:
				clist[i].data.ptr = &ptrCol[offset];
				offset += clist[i].dataLength;
				break;
			case LOM_VARSTRING:
				/* data */
				clist[i].data.ptr = &ptrCol[offset];

				/* advance pointer to encoded object */
				offset += clist[i].dataLength;
				break;
			/* START: Literal */
			case LOM_DATE:
				bcopy((void*)&ptrCol[offset],
					(void*)&(clist[i].data.date), LOM_DATE_SIZE);
				offset += LOM_DATE_SIZE;
				break;
			case LOM_TIME:
				bcopy((void*)&ptrCol[offset],
					(void*)&(clist[i].data.time), LOM_TIME_SIZE);
				offset += LOM_TIME_SIZE;
				break;
			case LOM_TIMESTAMP:
				bcopy((void*)&ptrCol[offset],
					(void*)&(clist[i].data.timestamp), LOM_TIMESTAMP_SIZE);
				offset += LOM_TIMESTAMP_SIZE;
				break;
			case LOM_INTERVAL:
				bcopy((void*)&ptrCol[offset],
					(void*)&(clist[i].data.interval), LOM_INTERVAL_SIZE);
				offset += LOM_INTERVAL_SIZE;
				break;
			/* END: Literal */
		}
	}

	e = LOM_UpdateObjectByColList(handle, ocnOrScanId, useScanFlag, oid, nCols, &clist[0]);
	if(e < 0) {
		/*free clist */
		free((char*)clist);
		LOM_ERROR(handle, e);
	}

	/* free clist */
	free((char*)clist);

	return eNOERROR;
}

