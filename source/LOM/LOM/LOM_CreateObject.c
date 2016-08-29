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
 * Module: LOM_CreateObject(handle, )
 *
 * Description:
 *  Create an object in the given class. The class is specified by scanId.
 *
 * Imports:
 *  LRDS_CreateTuple(LOM_GET_LRDS_HANDLE(handle), )
 *
 * Exports:
 *  Four LOM_CreateObject(handle, )
 *
 * Returns:
 *  Error code
 *     eBADPARAMETER_LRDS
 *     eCOLUMNVALUEEXPECTED_LRDS
 *     eWRONGCOLUMNVALUE_LRDS
 *     some errors caused by function calls
 *
 * Assumption:
 */


#include <malloc.h>
#include <string.h>
#include "LOM_Internal.h"
#include "LOM.h"
#include "Catalog.h"
#include "Catalog_Internal.h"



Four LOM_CreateObject(
	LOM_Handle *handle,
	Four ocnOrScanId,		/* IN ocr or ScanId */
	Boolean useScanFlag,
	char *pObj,	/* IN data of the created columns */
	OID *oid)		/* OUT TupleID of the newly created tuple */
{
	Four orn;				/* open relation number */
	Two nCols;			/* number of columns */
	ColListStruct *clist; /* converted column structure list */
	Four i,j;					/* temporary variable */
	Four e;					/* error number variable */
	Four catScanId;         /* scan id for a catalog table access */
	BoolExp boolexp[1];		/* Boolean Expression */
	char *startaddress;		/* start address of attribute */
	Four	isallocated[5];		/* to deallocate the memory for column list struct */
	ColListStruct tmp;		/* temporary data to be converted */
	Four classId;			/* class Id */
	Four numNotNullCols;
	Boolean nullFlag[LOM_MAXNUMOFATTRIBUTE];
	Four offset;
	unsigned char *nullVector;
	One hasParent;
	Four totalSize;
	lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */
	catalog_SysClassesOverlay *sysClass;
	catalog_SysAttributesOverlay *sysAttr;
	objectRef *or;
	Four scanId;
	Four v;

	if(useScanFlag) {
		scanId = LOM_SCANTABLE(handle->instanceId)[ocnOrScanId].lrdsScanId;

		if (!LRDS_VALID_SCANID(LOM_GET_LRDS_HANDLE(handle), scanId)) LOM_ERROR(handle, eBADPARAMETER_LRDS);
	}

	if (pObj == NULL) LOM_ERROR(handle, eBADPARAMETER_LRDS);


	/* Copy Open Relation Number to a local variable. */
	if(useScanFlag) 
		orn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[scanId].orn;
	else orn = ocnOrScanId;

	/* Temporary copy */
	relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);
	
	nCols = relTableEntry->ri.nColumns - 1; /* exclude logical-id column */

	e = Catalog_SearchClassInfo(handle,  relTableEntry->ri.fid.volNo, LOM_USEROPENCLASSTABLE(handle->instanceId)[orn].classID, &sysClass);
	if ( e < 0) LOM_ERROR(handle, e);


	/* get attribute information from catalog manager */
	sysAttr = CATALOG_GET_ATTRINFO(sysClass);

	/* get total size of object exept bit vectors */
	totalSize = CATALOG_GET_OBJECTSIZE(sysClass);

	startaddress = pObj;
	nullVector = (unsigned char *)((char *)startaddress + totalSize);

	numNotNullCols = 0;
	/* count the number of non-null column */
	for(i = 0;i < nCols; i++)
		if((nullVector[i/8] & (0x80>>(i%8))))	nullFlag[i]=TRUE; 
		else {
			nullFlag[i]=FALSE;
			numNotNullCols++;
		}
			
	/* memory allocation for collist struct */
	clist = (ColListStruct *)malloc(sizeof(ColListStruct)*numNotNullCols);

	/* change object format into column list struct */
	for(i = 0, j = 0;i < nCols ; i++) {
		if(nullFlag[i]==FALSE) {
			clist[j].colNo = i + 1;
			clist[j].start = ALL_VALUE;
			/* 
				if type is SM_VARSTRING, we change the length of dat
			*/
			clist[j].dataLength = (LRDS_GET_COLDESC_FROM_RELTABLE_ENTRY(relTableEntry))[i].length;
			offset = sysAttr[i].offset;

			switch((LRDS_GET_COLDESC_FROM_RELTABLE_ENTRY(relTableEntry))[i].type) {
				case SM_SHORT:
								bcopy(&startaddress[offset], &clist[j].data.s,SM_SHORT_SIZE);
								break;
				case SM_INT:
								bcopy(&startaddress[offset], &clist[j].data.i,SM_INT_SIZE);
								break;
				case SM_LONG:
								bcopy(&startaddress[offset], &clist[j].data.l,SM_LONG_SIZE);
								break;
				case SM_LONG_LONG:
								bcopy(&startaddress[offset], &clist[j].data.ll,SM_LONG_LONG_SIZE);
								break;
				case SM_FLOAT:
								bcopy(&startaddress[offset], &clist[j].data.f,SM_FLOAT_SIZE);
								break;
				case SM_DOUBLE:
								bcopy(&startaddress[offset], &clist[j].data.d,SM_DOUBLE_SIZE);
								break;
				case SM_STRING:
								clist[j].data.ptr = &startaddress[offset];
								break;
				case SM_VARSTRING:
                               bcopy(&startaddress[offset], &(clist[i].data.ptr),sizeof(void *));
								if( strlen(clist[i].data.ptr) <= clist[i].dataLength)
									clist[i].dataLength = strlen(clist[i].data.ptr);
								else LOM_ERROR(handle, eBADPARAMETER_LRDS);
								break;
				case SM_OID     :
								or = (objectRef *)&startaddress[offset];
								or->oid = clist[j].data.oid;
								or->od = NULL;
								break;
				case SM_PAGEID:
								bcopy(&startaddress[offset],
									&(clist[j].data.pid),SM_PAGEID_SIZE);
								break;
				case SM_FILEID:
								bcopy(&startaddress[offset],
									&(clist[j].data.fid),SM_FILEID_SIZE);
								break;
				case SM_INDEXID:
								bcopy(&startaddress[offset],
									&(clist[j].data.iid),SM_INDEXID_SIZE);
								break;
				default	: break;
			}
			j++;
		}
	}

	if(useScanFlag)
		e = LRDS_CreateTuple(LOM_GET_LRDS_HANDLE(handle), scanId, useScanFlag, numNotNullCols, &clist[0], (TupleID*)oid);
	else
		e = LRDS_CreateTuple(LOM_GET_LRDS_HANDLE(handle), ocnOrScanId, useScanFlag, numNotNullCols, &clist[0], (TupleID*)oid);
	if (e < 0) LOM_ERROR(handle, e);

	if(oid != (OID *)NULL) {
		oid->classID = LOM_USEROPENCLASSTABLE(handle->instanceId)[orn].classID;
	}
	if (e < 0) LOM_ERROR(handle, e);

	/* free clist */
	free(clist);

	return(eNOERROR);

} /* LRDS_CreateTuple(LOM_GET_LRDS_HANDLE(handle), ) */


