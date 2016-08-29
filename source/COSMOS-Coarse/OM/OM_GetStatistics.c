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
/*    ODYSSEUS/COSMOS General-Purpose Large-Scale Object Storage System --    */
/*    Coarse-Granule Locking (Volume Lock) Version                            */
/*    Version 3.0                                                             */
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
 * Module: OM_GetStatistics.c
 *
 * Description:
 *  Get Statistics Information
 */


#include "common.h"
#include "trace.h"
#include "OM_Internal.h"
#include "BfM.h"
#include "perThreadDS.h"
#include "perProcessDS.h"

Four OM_GetStatistics_DataFilePageInfo(
    Four handle,
    PageID*         curPid,                /* INOUT */
    Four*           numPinfoArray,         /* INOUT */
    sm_PageInfo*    pinfoArray)            /* OUT */
{
    Four            		e;
    Four            		i;
    PageNo          		nextPno;
    SlottedPage*    		aPage;
    SlottedPage*    		catPage; 		/* pointer to buffer containing the catalog */
    sm_CatOverlayForData* 	catEntry;       /* pointer to data file catalog information */

    TR_PRINT(TR_OM, TR1,
             ("OM_GetStatistics_DataFilePageInfo(handle, curPid=%P, numPinfoArray=%P, pinfoArray=%P)",
               curPid, numPinfoArray, pinfoArray));


    /* errorCheck */
    if(curPid == NULL) ERR(handle, eBADPARAMETER);

    /* for each page, get page infomation */
    for( i=0; i<*numPinfoArray && curPid->pageNo!=NIL; i++ ) {
        
        /* fix current page */
        e = BfM_GetTrain(handle, (TrainID*)curPid, (char **)&aPage, PAGE_BUF);
        if(e < 0) ERR(handle, e);
                
        /* set pinfoArray */ 
        pinfoArray[i].type = aPage->header.flags & PAGE_TYPE_VECTOR_MASK;
        pinfoArray[i].nSlots = aPage->header.nSlots;
        pinfoArray[i].free = aPage->header.free;
        pinfoArray[i].unused = aPage->header.unused;
        printf("[%ld] nSlots = %ld, free space = %ld\n", curPid->pageNo, pinfoArray[i].nSlots, SP_FREE(aPage));

        /* get 'nextPno' */
        nextPno = aPage->header.nextPage;
        
        /* unfix current page */
        e = BfM_FreeTrain(handle, (TrainID*)curPid, PAGE_BUF);
        if (e < 0) ERR(handle, e);

        /* update 'curPid' */
        curPid->pageNo = nextPno;
    }

    /* update 'numPinfoArray' if needed */
    if(i < *numPinfoArray) *numPinfoArray = i;


    return eNOERROR;
}
