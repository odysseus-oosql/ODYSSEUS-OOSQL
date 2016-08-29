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
 * Module: RDsM_eff.c
 *
 * Description:
 *
 * Exports:
 *  Four RDsM_check_eff(VolumeTable*, Four, Two*)
 *  Four RDsM_change_eff(VolumeTable*, Four, Two)
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "RDsM_Internal.h"
#include "BfM.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * RDsM_check_eff()
 *================================*/
/*
 * Function: Four RDsM_check_eff(VolumeTable*, Four, Two*)
 *
 * Description:
 *  check the extent fill factor of the extent in question.
 *
 * Returns:
 *  error code
 */
Four RDsM_check_eff(
    Four 			handle,
    VolumeTable		*v,				/* IN pointer to the entry in the volume table */
    Four			extNum,			/* IN extent number in question */
    Two 			*eff)			/* OUT an extent fill factor */
{
    Four			e;				/* returned error code */
    PageID			pageId;			/* page identifier */
    ExtEntry		*extEntry;		/* pointer to an extent link element */
    Two 			index;			/* index of an entry in an extent array page */
    PageType		*aPage;			/* pointer to a page */
    Four        	devNo;          /* number of device in which given extent is located */
    Four        	pageOffset;     /* offset of given extent (unit = # of page) */
    Four        	extOffset;      /* offset of given extent (unit = # of extent) */


    TR_PRINT(TR_RDSM, TR1, ("RDsM_check_eff(handle, v=%P, extNum=%lD, eff=%P)", v, extNum, eff));


    /*
     *  Get physical information about given extNum
     */
    e = rdsm_GetPhysicalInfo(handle, v, extNum*v->sizeOfExt, &devNo, &pageOffset);
    if (e < eNOERROR) ERR(handle, e);

    extOffset = pageOffset / v->sizeOfExt;


    /*
     *  Set the page identifier of the page of the extent entry array
     */
    pageId.volNo = DEVINFO_ARRAY(v->devInfo)[devNo].extEntryArrayPageId.volNo;
    pageId.pageNo = DEVINFO_ARRAY(v->devInfo)[devNo].extEntryArrayPageId.pageNo + extOffset/EXTENTRYPERPAGE;
    index = extOffset % EXTENTRYPERPAGE;


    /*@
     *  Get a page for a page of the extent entry array
     */
    e = BfM_GetTrain(handle, &pageId, (char**)&aPage, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e); 


    /*
     * find the entry in question
     */
    extEntry = aPage->el.el; 
    *eff = extEntry[index].eff;


    /*@
     *  Free this page
     */
    e = BfM_FreeTrain(handle, &pageId, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e); 


    return(eNOERROR);

} /* RDsM_check_eff() */



/*@================================
 * RDsM_change_eff()
 *================================*/
/*
 * Function: Four RDsM_change_eff(VolumeTable*, Four, Two)
 *
 * Description:
 *  change the extent fill factor of the extent in question.
 *
 * Returns:
 *  error code
 */
Four RDsM_change_eff(
    Four 			handle,
    VolumeTable		*v,				/* IN pointer to the entry in the volume table */
    Four    		extNum,			/* IN extent number in question */
    Two 			eff)			/* IN extent fill factor to be changed */
{
    Four        	e;				/* returned error code */
    PageID      	pageId;			/* page identifier */
    ExtEntry		*extEntry;		/* pointer to an extent link element */
    Two         	index;			/* index of an entry in a the extent link page */
    PageType    	*aPage;			/* pointer to a page */
    Four        	devNo;          /* number of device in which given extent is located */
    Four        	pageOffset;     /* offset of given extent (unit = # of page) */
    Four        	extOffset;      /* offset of given extent (unit = # of extent) */


    TR_PRINT(TR_RDSM, TR1, ("RDsM_chage_eff(v=%P, extNum=%lD, eff=%lD)", v, extNum, eff));


    /*
     *  Get physical information about given extNum
     */
    e = rdsm_GetPhysicalInfo(handle, v, extNum*v->sizeOfExt, &devNo, &pageOffset);
    if (e < eNOERROR) ERR(handle, e);

    extOffset = pageOffset / v->sizeOfExt;


    /*
     *  Set the page identifier of the page of the extent entry array
     */
    pageId.volNo = DEVINFO_ARRAY(v->devInfo)[devNo].extEntryArrayPageId.volNo;
    pageId.pageNo = DEVINFO_ARRAY(v->devInfo)[devNo].extEntryArrayPageId.pageNo + extOffset/EXTENTRYPERPAGE;
    index = extOffset%EXTENTRYPERPAGE;


    /*@
     *  Get a page of the extent entry array
     */
    e = BfM_GetTrain(handle, &pageId, (char**)&aPage, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e); 


    /*
     *  Find the entry in question
     */
    extEntry = aPage->el.el; 
    extEntry[index].eff = eff;


    /*@
     *  Set dirty bit
     */
    e = BfM_SetDirty(handle, &pageId, PAGE_BUF);
    if (e < eNOERROR) ERRB1(handle, e, &pageId, PAGE_BUF);


    /*@
     *  Free this page
     */
    e = BfM_FreeTrain(handle, &pageId, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);


    return(eNOERROR);

} /* RDsM_change_eff() */
