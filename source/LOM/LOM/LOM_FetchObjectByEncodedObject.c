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

Four LOM_FetchObjectByEncodedObject(
	LOM_Handle *handle,
	Four ocnOrScanId,
	Boolean useScanFlag,
	OID *oid,
	Four nCols,
	Four toFetchColInfoLength,
	char *toFetchColInfo,
	Four *fetchedEncodedObjectLength,
	char **fetchedEncodedObject
)
{
	Four e;
	Four v;
	Four i,j;
	Four scanId;    /* scan id */
	Four orn;
	lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */
	Four idxForClassInfo;
	char *ptrCol;
	Four offset;
	Four length;
	Four offsetToActualColVal;
	LOM_ColListStruct *clist;
	catalog_SysClassesOverlay *ptrToSysClasses;	/* pointer to sysclasses */
	catalog_SysAttributesOverlay *ptrToSysAttributes; /* pointer to sysattributes */
	Four encodedObject_len;
	Four numOfVarLengthAttrs;
	Four variableLengthAttributes_len;
	ColLengthInfoListStruct *varColLengthInfoList;
	char *encodedObject;
	char nullFlag;

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

	/* ptr to encoded object */
	ptrCol = toFetchColInfo;
	offset = 0;
	
	/* memory allocation */
	clist = (LOM_ColListStruct *)malloc(sizeof(LOM_ColListStruct) * nCols);
	if(clist == NULL) LOM_ERROR(handle, eOUTOFMEMORY_LOM);

	varColLengthInfoList = (ColLengthInfoListStruct *)malloc(sizeof(ColLengthInfoListStruct) * nCols);
	if(varColLengthInfoList == NULL) LOM_ERROR(handle, eOUTOFMEMORY_LOM);

	/* initialize the length of encoded object */
	*fetchedEncodedObjectLength = 0;


	/* think the type of to-be-fetched columns is variable-length */
	encodedObject_len = 0;
	numOfVarLengthAttrs = 0;
	j = 0;
	for(i = 0; i < nCols; i++) {

		/* colNo */
		bcopy((void*)&ptrCol[offset], (void*)&clist[i].colNo, sizeof(Two));
		offset += sizeof(Two); 

		/* start */
		bcopy((void*)&ptrCol[offset], (void*)&clist[i].start, sizeof(Four));
		offset += sizeof(Four); 

		/* length */
		bcopy((void*)&ptrCol[offset], (void*)&clist[i].length, sizeof(Four));
		offset += sizeof(Four); 

		/* dataLength */
		bcopy((void*)&ptrCol[offset], (void*)&clist[i].dataLength, sizeof(Four));
		offset += sizeof(Four); 

		if(clist[i].length == TO_END) clist[i].length = ptrToSysAttributes[GET_SYSTEMLEVEL_COLNO(clist[i].colNo)].length;

		/* count the number of variable-length attributes */
		if((ptrToSysAttributes[GET_SYSTEMLEVEL_COLNO(clist[i].colNo)].type == LOM_VARSTRING)) {
			varColLengthInfoList[j].colNo = clist[i].colNo;
			numOfVarLengthAttrs++;
			j++;
			continue;
		}

		if(clist[i].start == ALL_VALUE) {
			length = MIN(clist[i].dataLength, ptrToSysAttributes[GET_SYSTEMLEVEL_COLNO(clist[i].colNo)].length);
			length = MIN(length, clist[i].length);
			encodedObject_len += sizeof(char) /* nullFlag */
								+ sizeof(Four) /* retLength */
								+ length;
		}
		else {
			encodedObject_len += sizeof(char) + sizeof(Four)  + MIN(clist[i].length, clist[i].dataLength);
		}

	}

	variableLengthAttributes_len = 0;

	/* memory allocation for values for variable-length attributes */
	if(numOfVarLengthAttrs > 0) {

		e = LOM_FetchColLength(
			handle, 
			ocnOrScanId, 
			useScanFlag, 
			oid, 
			numOfVarLengthAttrs, 
			&varColLengthInfoList[0]);
		if( e < 0) {
			free((char*)clist);
			LOM_ERROR(handle, e);
		}

		for(i = 0; i < numOfVarLengthAttrs; i++) {
			variableLengthAttributes_len += sizeof(char) /* nullFlag */ +
				sizeof(Four) /* retLength */ +
				varColLengthInfoList[i].length;
		}

		for(i = 0, j = 0; i < nCols; i++) {
			if((ptrToSysAttributes[GET_SYSTEMLEVEL_COLNO(clist[i].colNo)].type == LOM_VARSTRING)) {
				clist[i].length = clist[i].dataLength = varColLengthInfoList[j++].length;
			}
		}
	}

	/* allocate reply buffer */
	encodedObject_len += variableLengthAttributes_len;
	*fetchedEncodedObjectLength = encodedObject_len;
	*fetchedEncodedObject = (char *)malloc(encodedObject_len);
	if(*fetchedEncodedObject == NULL)
		LOM_ERROR(handle, eOUTOFMEMORY_LOM);

	encodedObject = *fetchedEncodedObject;

	offsetToActualColVal = 0;
	/* set clist[] for VARSTRING or STRING data type */
	for(i = 0; i < nCols; i++) {
		offsetToActualColVal += sizeof(char); /* nullFlag */
		offsetToActualColVal += sizeof(Four); /* retLength */
		clist[i].data.ptr = &encodedObject[offsetToActualColVal];
		offsetToActualColVal += MIN(clist[i].length, clist[i].dataLength);
	}

	e = LOM_FetchObjectByColList( handle, ocnOrScanId, useScanFlag, oid, nCols, &clist[0]);
	if( e < 0) {
		free((char*)clist);
		LOM_ERROR(handle, e);
	}

	offset = 0;
	offsetToActualColVal = 0;
	nullFlag = 0;

	/* encoding object */
	for(i = 0; i < nCols; i++) {
		nullFlag = clist[i].nullFlag;
		bcopy(&nullFlag, &encodedObject[offsetToActualColVal], sizeof(char));
		offsetToActualColVal += sizeof(char);
		if(clist[i].nullFlag) {
			offsetToActualColVal += sizeof(Four); /* retLength */
			offsetToActualColVal += clist[i].dataLength; /* data */
			continue;
		}

		if((ptrToSysAttributes[GET_SYSTEMLEVEL_COLNO(clist[i].colNo)].complexType == LOM_COMPLEXTYPE_ODMG_COLLECTIONSET)
	   	|| (ptrToSysAttributes[GET_SYSTEMLEVEL_COLNO(clist[i].colNo)].complexType == LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG)
	   	|| (ptrToSysAttributes[GET_SYSTEMLEVEL_COLNO(clist[i].colNo)].complexType == LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST)
	   	|| (ptrToSysAttributes[GET_SYSTEMLEVEL_COLNO(clist[i].colNo)].complexType == LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY)) {
			/* retLength */
			bcopy((void*)&(clist[i].retLength), (void*)&encodedObject[offsetToActualColVal], sizeof(Four));
			offsetToActualColVal += sizeof(Four);

			/* advance pointer to encoded object */
			offsetToActualColVal += MIN(clist[i].length, clist[i].dataLength);
			continue;
	   	}

		switch(ptrToSysAttributes[GET_SYSTEMLEVEL_COLNO(clist[i].colNo)].type) {
			case LOM_SHORT:
				/* retLength */
				bcopy((void*)&(clist[i].retLength), (void*)&encodedObject[offsetToActualColVal], sizeof(Four));
				offsetToActualColVal += sizeof(Four);

				bcopy((void*)&(clist[i].data.s), (void*)&encodedObject[offsetToActualColVal], LOM_SHORT_SIZE);
				offsetToActualColVal += LOM_SHORT_SIZE;
				break;
			case LOM_USHORT:
				/* retLength */
				bcopy((void*)&(clist[i].retLength), (void*)&encodedObject[offsetToActualColVal], sizeof(Four));
				offsetToActualColVal += sizeof(Four);

				bcopy((void*)&(clist[i].data.us), (void*)&encodedObject[offsetToActualColVal], LOM_USHORT_SIZE);
				offsetToActualColVal += LOM_USHORT_SIZE;
				break;
			case LOM_INT:
				/* retLength */
				bcopy((void*)&(clist[i].retLength), (void*)&encodedObject[offsetToActualColVal], sizeof(Four));
				offsetToActualColVal += sizeof(Four);

				bcopy((void*)&(clist[i].data.i), (void*)(void*)&encodedObject[offsetToActualColVal], LOM_INT_SIZE);
				offsetToActualColVal += LOM_INT_SIZE;
				break;
			case LOM_LONG:
				/* retLength */
				bcopy((void*)&(clist[i].retLength), (void*)&encodedObject[offsetToActualColVal], sizeof(Four));
				offsetToActualColVal += sizeof(Four);

				bcopy((void*)&(clist[i].data.l), (void*)(void*)&encodedObject[offsetToActualColVal], LOM_LONG_SIZE);
				offsetToActualColVal += LOM_LONG_SIZE;
				break;
			case LOM_LONG_LONG:
				/* retLength */
				bcopy((void*)&(clist[i].retLength), (void*)&encodedObject[offsetToActualColVal], sizeof(Eight_Invariable));
				offsetToActualColVal += sizeof(Eight_Invariable);

				bcopy((void*)&(clist[i].data.ll), (void*)(void*)&encodedObject[offsetToActualColVal], LOM_LONG_LONG_SIZE);
				offsetToActualColVal += LOM_LONG_LONG_SIZE;
				break;
			case LOM_ULONG:
				/* retLength */
				bcopy((void*)&(clist[i].retLength), (void*)&encodedObject[offsetToActualColVal], sizeof(Four));
				offsetToActualColVal += sizeof(Four);

				bcopy((void*)&(clist[i].data.ul), (void*)(void*)&encodedObject[offsetToActualColVal], LOM_ULONG_SIZE);
				offsetToActualColVal += LOM_ULONG_SIZE;
				break;
			case LOM_FLOAT:
				/* retLength */
				bcopy((void*)&(clist[i].retLength), (void*)&encodedObject[offsetToActualColVal], sizeof(Four));
				offsetToActualColVal += sizeof(Four);

				bcopy((void*)&(clist[i].data.f), (void*)(void*)&encodedObject[offsetToActualColVal], LOM_FLOAT_SIZE);
				offsetToActualColVal += LOM_FLOAT_SIZE;
				break;
			case LOM_DOUBLE:
				/* retLength */
				bcopy((void*)&(clist[i].retLength), (void*)&encodedObject[offsetToActualColVal], sizeof(Four));
				offsetToActualColVal += sizeof(Four);

				bcopy((void*)&(clist[i].data.d), (void*)(void*)&encodedObject[offsetToActualColVal], LOM_DOUBLE_SIZE);
				offsetToActualColVal += LOM_DOUBLE_SIZE;
				break;
			case LOM_OID:
			case LOM_REF:
				/* retLength */
				bcopy((void*)&(clist[i].retLength), (void*)&encodedObject[offsetToActualColVal], sizeof(Four));
				offsetToActualColVal += sizeof(Four);

				bcopy((void*)&(clist[i].data.oid), (void*)(void*)&encodedObject[offsetToActualColVal], LOM_OID_SIZE);
				offsetToActualColVal += LOM_OID_SIZE;
				break;
			case LOM_OCTET:
				/* retLength */
				bcopy((void*)&(clist[i].retLength), (void*)&encodedObject[offsetToActualColVal], sizeof(Four));
				offsetToActualColVal += sizeof(Four);

				offsetToActualColVal += MIN(clist[i].length,clist[i].dataLength);
				break;
			case LOM_BOOLEAN:
				/* retLength */
				bcopy((void*)&(clist[i].retLength), (void*)&encodedObject[offsetToActualColVal], sizeof(Four));
				offsetToActualColVal += sizeof(Four);
				/* advance pointer to encoded object */
				offsetToActualColVal += MIN(clist[i].length, clist[i].dataLength);
				break;
			case LOM_STRING:
			case LOM_TEXT:
				/* retLength */
				bcopy((void*)&(clist[i].retLength), (void*)&encodedObject[offsetToActualColVal], sizeof(Four));
				offsetToActualColVal += sizeof(Four);
				/* advance pointer to encoded object */
				offsetToActualColVal += MIN(clist[i].length, clist[i].dataLength);
				break;
			case LOM_VARSTRING:
				/* retLength */
				bcopy((void*)&(clist[i].retLength), (void*)&encodedObject[offsetToActualColVal], sizeof(Four));
				offsetToActualColVal += sizeof(Four);

				offsetToActualColVal += MIN(clist[i].length, clist[i].dataLength);
				break;

			case LOM_DATE:
				/* retLength */
				bcopy((void*)&(clist[i].retLength), (void*)&encodedObject[offsetToActualColVal], sizeof(Four));
				offsetToActualColVal += sizeof(Four);

				bcopy((void*)&(clist[i].data.date),
					(void*)&encodedObject[offsetToActualColVal], LOM_DATE_SIZE);
				offsetToActualColVal += LOM_DATE_SIZE;
				break;
			case LOM_TIME:
				/* retLength */
				bcopy((void*)&(clist[i].retLength), (void*)&encodedObject[offsetToActualColVal], sizeof(Four));
				offsetToActualColVal += sizeof(Four);

				bcopy((void*)&(clist[i].data.time),
					(void*)&encodedObject[offsetToActualColVal], LOM_TIME_SIZE);
				offsetToActualColVal += LOM_TIME_SIZE;
				break;
			case LOM_TIMESTAMP:
				/* retLength */
				bcopy((void*)&(clist[i].retLength), (void*)&encodedObject[offsetToActualColVal], sizeof(Four));
				offsetToActualColVal += sizeof(Four);

				bcopy((void*)&(clist[i].data.timestamp),
					(void*)&encodedObject[offsetToActualColVal], LOM_TIMESTAMP_SIZE);
				offsetToActualColVal += LOM_TIMESTAMP_SIZE;
				break;
			case LOM_INTERVAL:
				/* retLength */
				bcopy((void*)&(clist[i].retLength), (void*)&encodedObject[offsetToActualColVal], sizeof(Four));
				offsetToActualColVal += sizeof(Four);

				bcopy((void*)&(clist[i].data.interval),
					(void*)&encodedObject[offsetToActualColVal], LOM_INTERVAL_SIZE);
				offsetToActualColVal += LOM_INTERVAL_SIZE;
				break;
		}
	}

	/* free clist */
	free((char*)clist);

	free((char*)varColLengthInfoList);

	return eNOERROR;
}

