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


Four LOM_Text_BatchInvertedIndexBuildWithSortedData(
	LOM_Handle *handle, 
	Four volId,
	char *className,
	Two colNo,
	char *sortedFileName
)
{
	Four e, e2;
	Four ocn;
	Four idxForClassInfo;		/* index for class information */
	Four i;
	catalog_SysIndexesOverlay *ptrToSysIndexes;	
	catalog_SysClassesOverlay *ptrToSysClasses;
	catalog_SysAttributesOverlay *ptrToSysAttributes;
	LOM_IndexID iid;
	BoundCond startBound;		/* start bound */
	BoundCond stopBound;		/* stop bound */
	Four lomScanNum;
	LockParameter lockup;
	Boolean tagToSortFile[LOM_MAXNUMOFATTRIBUTE];
	char *tempName2;
	OID oid;
	LOM_TextDesc textDesc;
	Four docLogicalId;
	Four v;
	ColListStruct clist[1];
#ifdef OROM
#include <time.h>
	time_t t1, t2;
	Four count = 0;
#endif

	/* open class */
	ocn = LOM_OpenClass(handle, volId, className);
	if( ocn < eNOERROR) LOM_ERROR(handle, ocn);

	/* get class information */
	e = Catalog_GetClassInfo(handle, volId, LOM_USEROPENCLASSTABLE[ocn].classID, &idxForClassInfo);
	if( e < eNOERROR) LOM_ERROR(handle, e);

	v = Catalog_GetVolIndex(handle, volId);
        if(v < eNOERROR) LOM_ERROR(handle, v);

	/* set memory pointer */
	ptrToSysClasses = &CATALOG_GET_CLASSINFOTBL(v)[idxForClassInfo];

	ptrToSysIndexes =  &CATALOG_GET_INDEXINFOTBL(v)[CATALOG_GET_INDEXINFOTBL_INDEX(ptrToSysClasses)];

	ptrToSysAttributes =  &CATALOG_GET_ATTRINFOTBL(v)[CATALOG_GET_ATTRINFOTBL_INDEX(ptrToSysClasses)];

	for(i = 0; i < CATALOG_GET_INDEXNUM(ptrToSysClasses); i++) {
		if(CATALOG_GET_INDEXCOLNO(ptrToSysIndexes)[0] == LOM_LOGICALID_COLNO)  {
			iid = ptrToSysIndexes->iid;
			break;
		}
	}

	if(i == CATALOG_GET_INDEXNUM(ptrToSysClasses)) LOM_ERROR(handle, eINTERNAL_LOM);

	startBound.op = SM_BOF;
	stopBound.op = SM_EOF;

	/* lock parameter */
	/* we here donot release file-level lock until commit */
	lockup.mode = L_X;
	lockup.duration = L_COMMIT;

	/* open index scan */
	lomScanNum = LOM_OpenIndexScan(handle, ocn, &iid, &startBound, &stopBound, 0, (BoolExp *)NULL, &lockup);
	if(lomScanNum < eNOERROR) LOM_ERROR(handle, lomScanNum);

	/* add inverted index entry from temproary posting file */
	e = lom_Text_AddInvertedIndexEntryFromTempPostingFile(handle, lomScanNum, GET_SYSTEMLEVEL_COLNO(colNo), sortedFileName);
	if(e < eNOERROR) {
 		LOM_ERROR(handle, e);
	}

#ifdef OROM
	t2 = time(NULL);

	printf("index-building time: %ld", t2 - t1);
#endif

 	e = LOM_CloseScan(handle, lomScanNum);
 	if(e < eNOERROR) LOM_ERROR(handle, e);

 	e = LOM_CloseClass(handle, ocn);
 	if(e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}
