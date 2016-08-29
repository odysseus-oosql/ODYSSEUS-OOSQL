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


Four LOM_CloseClass(
	LOM_Handle *handle,
	Four ocn)
{
	Four e;
	Four i;

	if( ocn < 0) 
		LOM_ERROR(handle, eBADPARAMETER_LOM);

	if(LOM_USEROPENCLASSTABLE(handle)[ocn].count == 0) 
		LOM_ERROR(handle, eBADPARAMETER_LOM);

	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), ocn);
	if(e<0) LOM_ERROR(handle, e);

	LOM_USEROPENCLASSTABLE(handle)[ocn].count--;

	if(LOM_USEROPENCLASSTABLE(handle)[ocn].count == 0) {

		if(LOM_USEROPENCLASSTABLE(handle)[ocn].numOfTextAttrs >0 ) {
			/* check if the class has text-type attributes */
			if(LOM_USEROPENCLASSTABLE(handle)[ocn].ornForContentTable >= 0) {
				/* close lrds scan */
				if(LOM_USEROPENCLASSTABLE(handle)[ocn].lrdsScanIdForTextScan >= 0) {
					e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), LOM_USEROPENCLASSTABLE(handle)[ocn].lrdsScanIdForTextScan);
					if( e < eNOERROR) LOM_ERROR(handle, e);
				}

				/* close conetet scan */
				if(LOM_USEROPENCLASSTABLE(handle)[ocn].contentTableScanIdForTextScan >= 0) {
					e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), LOM_USEROPENCLASSTABLE(handle)[ocn].contentTableScanIdForTextScan);
					if( e < eNOERROR) LOM_ERROR(handle, e);
				}

				/* close content table */
				e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), LOM_USEROPENCLASSTABLE(handle)[ocn].ornForContentTable);
				if( e < eNOERROR) LOM_ERROR(handle, e);

				/* close inverted index table and posting table */
				for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ocn].numOfTextAttrs; i++) {
					if(LOM_USEROPENCLASSTABLE(handle)[ocn].textColNo[i] != NIL) {
						/* close inverted index table */
						if(LOM_USEROPENCLASSTABLE(handle)[ocn].ornForInvertedIndexTable[i] >= 0) {
							e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), LOM_USEROPENCLASSTABLE(handle)[ocn].ornForInvertedIndexTable[i]);
							if(e < eNOERROR) LOM_ERROR(handle, e);
						}

						/* close doc-id index table */
						if(LOM_USEROPENCLASSTABLE(handle)[ocn].ornForDocIdIndexTable[i] >= 0) {
							e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), LOM_USEROPENCLASSTABLE(handle)[ocn].ornForDocIdIndexTable[i]);
							if(e < eNOERROR) LOM_ERROR(handle, e);
						}
					}

					/* close handle for keyword extractor */
					if(LOM_USEROPENCLASSTABLE(handle)[ocn].handleForDLOfKeywordExtractor[i]) {
						e = LOM_Text_CloseHandle(handle, LOM_USEROPENCLASSTABLE(handle)[ocn].handleForDLOfKeywordExtractor[i]);
						if(e < eNOERROR) LOM_ERROR(handle, e);
						LOM_USEROPENCLASSTABLE(handle)[ocn].handleForDLOfKeywordExtractor[i] = NULL;
					}

					/* close handle for filter */
					if(LOM_USEROPENCLASSTABLE(handle)[ocn].handleForDLOfFilter[i]) {
						e = LOM_Text_CloseHandle(handle, LOM_USEROPENCLASSTABLE(handle)[ocn].handleForDLOfFilter[i]);
						if(e < eNOERROR) LOM_ERROR(handle, e);
						LOM_USEROPENCLASSTABLE(handle)[ocn].handleForDLOfFilter[i] = NULL;
					}

					/* close handle for stemizer */
					if(LOM_USEROPENCLASSTABLE(handle)[ocn].handleForDLOfStemizer[i]) {
						e = LOM_Text_CloseHandle(handle, LOM_USEROPENCLASSTABLE(handle)[ocn].handleForDLOfStemizer[i]);
						if(e < eNOERROR) LOM_ERROR(handle, e);
						LOM_USEROPENCLASSTABLE(handle)[ocn].handleForDLOfStemizer[i] = NULL;
					}
				}
			}
			else LOM_ERROR(handle, eINTERNAL_LOM);
		}

		if(LOM_USEROPENCLASSTABLE(handle)[ocn].numOfodmgCollAttrs > 0)
		{
			for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ocn].numOfodmgCollAttrs; i++)
			{
				if(LOM_USEROPENCLASSTABLE(handle)[ocn].odmgCollDataOrn[i] >= 0)
				{
					e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), LOM_USEROPENCLASSTABLE(handle)[ocn].odmgCollDataOrn[i]);
					if(e < eNOERROR) LOM_ERROR(handle, e);
				}
			}
		}

		if(LOM_USEROPENCLASSTABLE(handle)[ocn].ornForDeletionListTable != NIL)
		{
			e = lom_CloseDeferredDeletionListTable(handle, LOM_USEROPENCLASSTABLE(handle)[ocn].ornForDeletionListTable);
			if(e < eNOERROR) LOM_ERROR(handle, e);
		}
	}

	return eNOERROR;
}


