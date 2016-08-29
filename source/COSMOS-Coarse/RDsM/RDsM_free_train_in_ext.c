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
 * Module: RDsM_free_train_in_ext.c
 *
 * Description:
 *
 * Exports:
 *  Four RDsM_free_train_in_ext(VolumeTable*, Four, PageID*, Two)
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "RDsM_Internal.h"
#include "BfM.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * RDsM_free_train_in_ext()
 *================================*/
/*
 * Function: Four RDsM_free_train_in_ext(VolumeTable*, Four, PageID*, Two)
 *
 * Description:
 *  free a train into the extent that the train belongs to
 *
 * Returns:
 *  error code
 */
Four RDsM_free_train_in_ext(
    Four 			handle,
    VolumeTable 	*v,				/* IN pointer to the entry in the volume table */
    Four        	extNum,			/* IN extent number in question */
    PageID			*trainId,		/* IN pointer to a page identifier */
    Two 			sizeOfTrain)	/* IN size of a train */
{
    Four			e;				/* returned error code */
    PageID			bmPageId;		/* page identifier */
    PageType		*bmPage;		/* pointer to a page */
    Four			pos;			/* ith bit position in a page */
    Four        	devNo;          /* number of device in which given extent is located */
    Four        	pageOffset;     /* offset of given extent (unit = # of page) */
    Four        	extOffset;      /* offset of given extent (unit = # of extent) */


    TR_PRINT(TR_RDSM, TR1,
             ("RDsM_free_train_in_ext(handle, v=%P, extNum=%lD, trainId=%P, sizeOfTrain=%lD)", v, extNum, trainId, sizeOfTrain));


    /*
     *  Get physical information about given train
     */
    e = rdsm_GetPhysicalInfo(handle, v, trainId->pageNo, &devNo, &pageOffset);
    if (e < eNOERROR) ERR(handle, e);

    extOffset = pageOffset / v->sizeOfExt;


    /*
     *  Get the bitmap page of the extent
     */

    /* find the PageMap page of the extent */
    bmPageId.volNo = DEVINFO_ARRAY(v->devInfo)[devNo].bitMapPageId.volNo;
    bmPageId.pageNo = DEVINFO_ARRAY(v->devInfo)[devNo].bitMapPageId.pageNo + extOffset / ((BITMAP_USABLEBYTESPERPAGE*BITSPERBYTE)/v->sizeOfExt);

    /*@ get a page for a page map page */
    e = BfM_GetTrain(handle, &bmPageId, (char**)&bmPage, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e); 


    /*
     *  Set bitmap
     */

    /* calculate page position pos : the 1st page is 0-th position */
    pos = pageOffset % (((BITMAP_USABLEBYTESPERPAGE*BITSPERBYTE)/v->sizeOfExt) * v->sizeOfExt);

    /* test whether SizeOfTrain bits from the pos-th bit of bmPage are set */
    e = RDsM_test_n_bits_set(handle, bmPage, pos, sizeOfTrain);
    if (e < eNOERROR) ERR(handle, e);

    /* set SizeOfTrain bits from the pos-th bit of bmPage */
    RDsM_set_bits(handle, bmPage, pos, sizeOfTrain);


    /*@
     *  Set dirty bit
     */
    e = BfM_SetDirty(handle, &bmPageId, PAGE_BUF);
    if (e < eNOERROR) ERRB1(handle, e, &bmPageId, PAGE_BUF);


    /*@
     *  Free this page
     */
    e = BfM_FreeTrain(handle, &bmPageId, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);


    return(eNOERROR);

} /* RDsM_free_train_in_ext() */
