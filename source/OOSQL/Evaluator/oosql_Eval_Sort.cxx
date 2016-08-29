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
    MODULE:
        oosql_Eval_Sort.cxx

    DESCRIPTION:

    IMPORTS:

    EXPORTS:

*/

#include "OOSQL_Evaluator.hxx"
#include "OOSQL_SortStream.hxx"

Four    OOSQL_Evaluator::sorting()
/*
    Function:

    Side effect:

    Referenced member varialbes:
        m_currPlanIndex

    Return value:
*/
{
    AP_ProjectionListPoolElements projList;
    OOSQL_TempFileInfo *tempFileInfo;
    Four scanId;
    OOSQL_StorageManager::LockParameter lockup;
    Four i;
    Four e;


#ifdef  OOSQL_DEBUG
    /* check if sort is prepared */
    if (m_evalStatus.prepareAndSortStatus != EVALSTATUS_END)
        OOSQL_ERR(eSORT_NOT_PREPARED_OOSQL);
#endif

    projList = ACCESSPLAN.getProjectionListPool(m_currPlanIndex);

    /* for each projection element that needs sorting, 
     * sort the temporary file appointed to by that 
     */
    for (i = 0; i < GET_POOLSIZE(projList); i++) 
	{
        /* get information about the temporary file to sort */
        tempFileInfo = EVAL_TEMPFILEINFOTABLEELEMENTS[ projList[i].tempFileNum ];

		if(tempFileInfo->sortStream)
		{
			e = tempFileInfo->sortStream->Sort();
			OOSQL_CHECK_ERR(e);
		}
		else
		{
			/* check if scan is opened for the temporary file */
			if (tempFileInfo->osn >= 0) {
				/* close the scan */
				e = m_storageManager->CloseScan( tempFileInfo->osn );
				OOSQL_CHECK_ERR(e);

				/* reset the open scan number */
				RESET_OPENSCANNUM(tempFileInfo->osn);
			}

			/* sort temporary file */
			e = sortTempFile(&projList[i]);
			OOSQL_CHECK_ERR(e);

			/* prepare lockup parameter to open scan */
			lockup.mode = OOSQL_StorageManager::L_S;
			lockup.duration = OOSQL_StorageManager::L_COMMIT;

			/* 
			 * open sequential scan for the sorted temporary file 
			 */
			scanId = m_storageManager->OpenSeqScan(tempFileInfo->ocn, FORWARD, 0, NULL, &lockup);
			OOSQL_CHECK_ERR(e);

			tempFileInfo->osn = scanId;
		}
    } /* end of for */

    /* reset m_evalStatus.prepareAndSortStatus */
    m_evalStatus.prepareAndSortStatus = EVALSTATUS_INIT;

    /* return */
    return(eNOERROR);
}


Four    OOSQL_Evaluator::sortTempFile(
    AP_ProjectionListElement *projInfo          /* IN: */
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    OOSQL_TempFileInfo *tempFileInfo;
    OOSQL_StorageManager::LockParameter lockup;
    static Four unique = 0;
    Four i;
    Four e;


	if(projInfo->nSortKeys == 0)	
		return eNOERROR;

    /* check input parameter */
#ifdef  OOSQL_DEBUG
    if (projInfo == NULL) 
        OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

    /* get information about the temporary file to sort */
    tempFileInfo = EVAL_TEMPFILEINFOTABLEELEMENTS[ projInfo->tempFileNum ];

    OOSQL_StorageManager::BTreeKeyInfo     sortKeyInfo;

    /* prepare sort key information */
    sortKeyInfo.flag = 0;       /* non-unique */
    sortKeyInfo.nColumns = projInfo->nSortKeys;
    for (i = 0; i < projInfo->nSortKeys; i++) 
	{
        sortKeyInfo.columns[i].colNo = projInfo->sortKeys[i];
        if (projInfo->sortAscDesc[i] == SORTORDER_DESC)
            sortKeyInfo.columns[i].flag = KEYINFO_COL_DESC;
        else
            sortKeyInfo.columns[i].flag = KEYINFO_COL_ASC;
    }

    /* prepare lock parameter */
    lockup.mode = OOSQL_StorageManager::L_X;
    lockup.duration = OOSQL_StorageManager::L_COMMIT;

    e = m_storageManager->SortRelation(m_sortBufferInfo.diskInfo.sortVolID, m_sortBufferInfo.diskInfo.sortVolID, tempFileInfo->name, &sortKeyInfo, SM_FALSE, NULL, SM_TRUE, &lockup);
    if (e < eNOERROR) 
        OOSQL_ERR(e);

    /* return */
    return(eNOERROR);
}

