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
 * Module: LOM_FetchObject.c
 *
 * Description:
 *  Fetch the given object.
 *
 * Imports:
 *  SM_FetchObject()
 *
 * Exports:
 *  Four LOM_FetchObject(handle, Four, OID*, void*)
 *
 * Returns:
 *  Error code
 *    eBADPARAMETER_LRDS
 *    eFETCHERROR_LRDS
 *    some errors caused by function calls
 *
 * Side effects:
 */

#include <malloc.h>
#include <string.h>
#include "LOM_Internal.h"
#include "LOM.h"
#include "Catalog_Internal.h"
#include "Catalog.h"


Four LOM_FetchObject(
	LOM_Handle *handle, 
	Four ocnOrScanId,	/* IN used scan */
	Boolean useScanFlag,	/* IN useScanFlag */
	OID *oid,		/* IN object id to fetch */
	char *pObj)		/* INOUT columns to fetch */
{
	Four e;			/* error code */
	Four i;			/* index variable */
	Four j;			/* index variable */
	Four orn;		/* open relation number */
	Four size;		/* size of tuple header */
	Four varColNo;		/* column number of variable-length columns */
	Four start;		/* starting offset of fetch */
	Four length;		/* amount of data to fetch */
	ColDesc *cdesc;		/* pointer to the current column descriptor */
	TupleHdr tupHdr;	/* a tuple header */
	unsigned char *nullVector;	/* bit array of null flags */
	unsigned char *updateVector; /* update vector */
	LockParameter lockup;		/* lockup for SM_Fetch Tuple */
	Four nCols;					/* number of columns */
	BoolExp boolexp[1]; 	    /* Boolean Expression */
	Four classId;				/* class Id */
	char *startaddress;			/* start addresss of object */
	Four offset;				/* offset */
	ColListStruct *clist;		
	char **tmpptr;
	long varoffset;
	One hasParent;
	Four totalSize;
	objectRef *or;
	char *ptr;
	lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */
	catalog_SysClassesOverlay *sysClass;
	catalog_SysAttributesOverlay *sysAttr;
	void *ptrVLA;
	Four scanId;

	if(useScanFlag) {
		scanId = LOM_SCANTABLE[ocnOrScanId].lrdsScanId;

		if (!LRDS_VALID_SCANID(LOM_GET_LRDS_HANDLE(handle), scanId)) LOM_ERROR(handle, eBADPARAMETER_LRDS);

		orn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[scanId].orn;
	}
	else orn = ocnOrScanId;

	if (pObj == NULL) LOM_ERROR(handle, eBADPARAMETER_LRDS);

	relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);

	nCols = relTableEntry->ri.nColumns;
	
	startaddress = pObj;

	clist = (ColListStruct *)malloc(sizeof(ColListStruct)*nCols);

	e = Catalog_SearchClassInfo(handle,  relTableEntry->ri.fid.volNo, LOM_USEROPENCLASSTABLE[orn].classID, &sysClass);
	if ( e < 0) CATALOG_ERROR(e);

	/* get attribute information from catalog manager */
	sysAttr = CATALOG_GET_ATTRINFO(sysClass);

	/* get total size of object exept bit vectors */
	totalSize = CATALOG_GET_OBJECTSIZE(sysClass);

	/* the position of null vector */
	nullVector = (unsigned char *)(startaddress + totalSize);

	/* the position of update vector */
	updateVector = (unsigned char *)(startaddress + totalSize + LENGTH_NULLVECTOR(nCols));

	/* For string and variable string attribute fetch */
	for(i = 0;i < nCols; i++) {
		clist[i].colNo = i;
		clist[i].start = ALL_VALUE;
		clist[i].dataLength = (LRDS_GET_COLDESC_FROM_RELTABLE_ENTRY(relTableEntry))[i].length;
		if((LRDS_GET_COLDESC_FROM_RELTABLE_ENTRY(relTableEntry))[i].type == SM_VARSTRING) {
			 bcopy(&startaddress[sysAttr[i].offset],&(clist[i].data.ptr)
									 ,sizeof(void *));
		}		
		if((LRDS_GET_COLDESC_FROM_RELTABLE_ENTRY(relTableEntry))[i].type == SM_STRING) clist[i].data.ptr = &startaddress[sysAttr[i].offset];
	}

	if(useScanFlag)
		e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), scanId,useScanFlag, (TupleID *)oid,nCols,&clist[0]);
	else
		e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), ocnOrScanId,useScanFlag, (TupleID *)oid,nCols,&clist[0]);
	if(e < 0) {
		free(clist);
		LOM_ERROR(handle, e);
	}


#ifdef TRACE_FETCHOBJECT
	for(i = 0;i < nCols; i++) {
		printf("Column %ld \n",i);
		if((LRDS_GET_COLDESC_FROM_RELTABLE_ENTRY(relTableEntry))[i].type  == SM_VARSTRING || (LRDS_GET_COLDESC_FROM_RELTABLE_ENTRY(relTableEntry))[i].type  == SM_STRING) {
			printf("Data : %s\n",clist[i].data.ptr);
		}
		else {
		if(attrinfo[i].type == SM_SHORT) printf("Data : %ld\n",clist[i].data.s);
		else printf("Data : %ld\n",clist[i].data.l);
		}
	}
#endif
	/*
	 fixed Column copy
	*/


	for(j = 0;j < nCols; j++) {
			if(clist[j].nullFlag) {
				nullVector[j/8] |= (unsigned char)(0x80 >> (j % 8));
				continue;
			}
			else {
				nullVector[j/8] &= ~((unsigned char)0x80 >> (unsigned char)(j % 8));
			}
			offset = sysAttr[j].offset;
		switch((LRDS_GET_COLDESC_FROM_RELTABLE_ENTRY(relTableEntry))[j].type) {
			case SM_SHORT   : 
				bcopy(&clist[j].data.s, &startaddress[offset], SM_SHORT_SIZE);
				break;
			case SM_INT     :
				bcopy(&clist[j].data.i, &startaddress[offset], SM_INT_SIZE);
				break;
			case SM_LONG    :
				bcopy(&clist[j].data.l, &startaddress[offset], SM_LONG_SIZE);
				break;
			case SM_LONG_LONG :
				bcopy(&clist[j].data.ll, &startaddress[offset], SM_LONG_LONG_SIZE);
				break;
			case SM_FLOAT   :
				bcopy(&clist[j].data.l, &startaddress[offset], SM_FLOAT_SIZE);
				break;
			case SM_DOUBLE  :
				bcopy(&clist[j].data.l, &startaddress[offset], SM_DOUBLE_SIZE);
				break;
			case SM_STRING  :
				break;
			case SM_VARSTRING  :
				ptr = (char *)clist[j].data.ptr;
				ptr[clist[j].retLength] = '\0';
				break;
			case SM_OID		:
				or = (objectRef *)&startaddress[offset];
				or->oid = clist[j].data.oid;
				or->od = NULL;
				break;
			case SM_PAGEID  :
				bcopy(&clist[j].data.pid,&startaddress[offset],SM_PAGEID_SIZE);
				break;
			case SM_FILEID  :
				bcopy(&clist[j].data.fid,&startaddress[offset],SM_FILEID_SIZE);
				break;
			case SM_INDEXID :
				bcopy(&clist[j].data.iid,&startaddress[offset],SM_INDEXID_SIZE);
				break;
			default : break;
		}
	}

	/* update Vector */
	for(i = 0 ;i < LENGTH_UPDATEVECTOR(nCols) ; i++)
		updateVector[i] = (char)0;

	free(clist);

	return(eNOERROR);
} /* LOM_FetchObject(handle, ) */
    
