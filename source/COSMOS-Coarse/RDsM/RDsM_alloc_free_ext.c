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
 * Module: RDsM_alloc_free.c
 *
 * Description:
 *
 * Exports:
 *  Four RDsM_alloc_ext(VolumeTable*, Four, Four*)
 *  Four RDsM_free_ext(VolumeTable*, Four, Four*, Four)
 *  Four RDsM_chage_NumOfExts_FirstFreeExt(VolumeTable*, Four, Four)
 */


#include <assert.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "RDsM_Internal.h"
#include "BfM.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * RDsM_alloc_ext()
 *================================*/
/*
 * Function: Four RDsM_alloc_ext(VolumeTable*, Four, Four*)
 *
 * Description:
 *  allocate an extent from the free extent entry list
 *
 * Returns:
 *  error code
 *    eNODISKSPACE - no disk space for this extent allocation
 */
Four RDsM_alloc_ext(
    Four 			handle,
    VolumeTable		*v,				/* IN pointer to an entry of VolTable */
    Four			firstExt,		/* IN first extent number in an extent list of a segment */
    Four			*newExt)		/* OUT new extent number to be allocated */
{
    Four			e;				/* returned error code */
    Four			prevext;		/* previous extent number */
    Four			nextext;		/* next extent number */
    VolInfoPage 	volInfoPage; 	/* volume information page */ 


    TR_PRINT(TR_RDSM, TR1,
             ("RDsM_alloc_ext(handle, v=%P, firstExt=%lD, newExt=%lD)", v, firstExt, newExt));


    if (v->numOfFreeExts == NOT_ASSIGNED || v->firstFreeExt == NOT_ASSIGNED) {

        /* read volume information page */
        e = rdsm_ReadTrain(handle, DEVINFO_ARRAY(v->devInfo)[0].devAddr, v->volInfoPageId.pageNo, &volInfoPage, PAGESIZE2);
        if (e < eNOERROR) ERR(handle, e);

        /* set numOfFreeExts & firstFreeExt */
        v->numOfFreeExts = volInfoPage.numOfFreeExts;
        v->firstFreeExt = volInfoPage.firstFreeExt;
    }

    /*@ if there is no extents in this volume */
    if (v->firstFreeExt == NIL) ERR(handle, eNODISKSPACE_RDSM);

    /* find the allocated extent number from the free extent list */
    *newExt = v->firstFreeExt;

    /* get the extent number next to the v->FirstFreeExt in the segment extent list */
    e = RDsM_get_prev_next_ext(handle, v, v->firstFreeExt, &prevext, &nextext);
    if (e < eNOERROR) ERR(handle, e); 

    /* set all bits in the bit map of the corresponding extent */
    e = RDsM_set_page_map(handle, v, *newExt);
    if (e < eNOERROR) ERR(handle, e); 

    /*@ set extent fill factor of the ExtNum */
    e = RDsM_change_eff(handle, v, *newExt, 0);
    if (e < eNOERROR) ERR(handle, e); 

    /*@ assign a new first free extent to v->FirstFreeExt and decrement NumOfFreeExts */
    v->firstFreeExt = nextext;
    v->numOfFreeExts--;
    assert (v->numOfFreeExts >= 0);

    /* reflect the changes of NumOfFreeExts & FirstFreeExt in the VolInfoPage */
    e = RDsM_change_NumOfFreeExts_FirstFreeExt(handle, v, v->numOfFreeExts, v->firstFreeExt);
    if (e < eNOERROR) ERR(handle, e); 

    /*@ if the v->FirstFreeExt is not NIL */
    if (v->firstFreeExt != NIL) {

	/* set the previous extent number of the v->FirstFreeExt to NIL */
	e = RDsM_set_prev_next_ext(handle, v, v->firstFreeExt, NIL, NULL);
	if (e < eNOERROR) ERR(handle, e); 
    }

    /* if the NewExt becomes the first extent of this segment */
    if (firstExt == NIL) {

	/* set the previous and next extent numbers of NewExt to NIL */
	e = RDsM_set_prev_next_ext(handle, v, *newExt, NIL, NIL);
	if (e < eNOERROR) ERR(handle, e); 

	return(eNOERROR);

    }	/* end if */

    /*@ get the extent number next to the FirstExt in the segment extent list */
    e = RDsM_get_prev_next_ext(handle, v, firstExt, &prevext, &nextext);
    if (e < eNOERROR) ERR(handle, e); 

    /*@ set the previous and next extent of the new allocated extent */
    e = RDsM_set_prev_next_ext(handle, v, *newExt, firstExt, nextext);
    if (e < eNOERROR) ERR(handle, e); 

    /*@ if the extent number next to the FirstExt is not NIL */
    if (nextext != NIL) {

	/* set the previous extent of the extent next to the new allocated extent */
	e = RDsM_set_prev_next_ext(handle, v, nextext, *newExt, NULL);
	if (e < eNOERROR) ERR(handle, e); 

    }	/* end if */

    /*@ set the previous and next extent of the FirstExt */
    e = RDsM_set_prev_next_ext(handle, v, firstExt, NULL, *newExt);
    if (e < eNOERROR) ERR(handle, e); 

    return(eNOERROR);

} /* RDsM_alloc_ext() */



/*@================================
 * RDsM_free_ext()
 *================================*/
/*
 * Function: Four RDsM_free_ext(VolumeTable*, Four, Four*, Four)
 *
 * Description:
 *  free an extent to the free extent entry list
 *
 * Returns:
 *  error code
 */
Four	RDsM_free_ext(
    Four 			handle,
    VolumeTable 	*v,             /* IN pointer to an entry of VolTable */
    Four        	extNum,         /* IN extent to be freed */
    Four        	*newFirstExt,   /* OUT extent that is next to ExtNum in the segment extent list */
    Four        	caller)	    	/* IN the caller calling this routine */
{
    Four			e;          	/* returned error code */
    PageID      	pageid;     	/* page identifier */
    Four        	prevext;    	/* extent number previous to the extent ExtNum */
    Four        	nextext;    	/* extent number next to the extent ExtNum */
    PageType    	*aPage;     	/* pointer a buffer page */
    ExtEntry    	*extentry;  	/* pointer to an entry in the extent link page */
    VolInfoPage 	volInfoPage; 	/* volume information page */ 


    TR_PRINT(TR_RDSM, TR1,
             ("RDsM_free_ext(handle, v=%P, extNum=%lD, newFirstExt=%P, caller=%lD)", v, extNum, newFirstExt, caller));

    if (v->numOfFreeExts == NOT_ASSIGNED || v->firstFreeExt == NOT_ASSIGNED) {

        /* read volume information page */
        e = rdsm_ReadTrain(handle, DEVINFO_ARRAY(v->devInfo)[0].devAddr, v->volInfoPageId.pageNo, &volInfoPage, PAGESIZE2);
        if (e < eNOERROR) ERR(handle, e);

        /* set numOfFreeExts & firstFreeExt */
        v->numOfFreeExts = volInfoPage.numOfFreeExts;
        v->firstFreeExt = volInfoPage.firstFreeExt;
    }

    /*& get the extent numbers previous & next to the ExtNum in the segment extent list */
    e = RDsM_get_prev_next_ext(handle, v, extNum, &prevext, &nextext);
    if (e < eNOERROR) ERR(handle, e); 

    /* if this is the first extent of the segment, and
       the caller is RDsM_FreeTrain, do nothing */
    if (prevext == NIL && caller == FREE) {
	return(eNOERROR);
    }

    /* change extent fill factor of the ExtNum */
    e = RDsM_change_eff(handle, v, extNum, NIL);
    if (e < eNOERROR) ERR(handle, e);

    /* free the ExtNum into the free extent list */
    e = RDsM_set_prev_next_ext(handle, v, extNum, NIL, v->firstFreeExt);
    if (e < eNOERROR) ERR(handle, e); 

    /*@ change the first free extent number to the ExtNum */
    v->firstFreeExt = extNum;

    if (prevext != NIL && nextext != NIL) {

	e = RDsM_set_prev_next_ext(handle, v, prevext, NULL, nextext);
	if (e < eNOERROR) ERR(handle, e);

	e = RDsM_set_prev_next_ext(handle, v, nextext, prevext, NULL);
	if (e < eNOERROR) ERR(handle, e);

	*newFirstExt = NULL;

    } /* end if */

    else if (nextext != NIL) {

	/* previous extent does not exist */

	e = RDsM_set_prev_next_ext(handle, v, nextext, NIL, NULL);
	if (e < eNOERROR) ERR(handle, e);

	/* new first extent number in a segment is returned */
	*newFirstExt = nextext;

    } /* end else if */

    else if (prevext != NIL) {

	/* next extent does not exist */
	e = RDsM_set_prev_next_ext(handle, v, prevext, NULL, NIL);
	if (e < eNOERROR) ERR(handle, e);

	*newFirstExt = NULL;
    }

    else /* next & previous extents do not exist */

	*newFirstExt = NIL; /* new first extent number in a segment is returned */

    v->numOfFreeExts++;

    /* reflect the changes of NumOfFreeExts & FirstFreeExt in the VolInfoPage for recovery purposes */
    e = RDsM_change_NumOfFreeExts_FirstFreeExt(handle, v, v->numOfFreeExts, v->firstFreeExt);
    if (e < eNOERROR) ERR(handle, e);


    return(eNOERROR);

} /* RDsM_free_ext() */



/*@================================
 * RDsM_chage_NumOfExts_FirstFreeExt()
 *================================*/
/*
 * Function: Four RDsM_chage_NumOfExts_FirstFreeExt(VolumeTable*, Four, Four)
 *
 * Description
 *  reflect two fields (# of free extents & the first free extent) in the
 * volume information in the disk for the purpose of the fast recovery at the
 * time system crash happens
 *
 * Returns:
 *  error code
 */
Four	RDsM_change_NumOfFreeExts_FirstFreeExt(
    Four 			handle,
    VolumeTable		*v,				/* IN pointer to an entry in VolTable */
    Four			numOfFreeExts,	/* IN # of free extents in this volume */
    Four			firstFreeExt)	/* IN the first free extent number in the free extent list */
{
    PageType		*aPage;			/* pointer to a buffer page */
    Four			e;				/* returned error code */


    TR_PRINT(TR_RDSM, TR1,
             ("RDsM_chage_NumOfExts_FirstFreeExt(v=%P, numOfFreeExts=%lD, firstFreeExt=%lD)",
	      v, numOfFreeExts, firstFreeExt));


    /*@ allocate a page for volume information management */
    e = BfM_GetTrain(handle, &(v->volInfoPageId), (char**)&aPage, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e); 

    /* store NumOfFreeExts and FirstFreeExt in the volume information page */
    aPage->vi.numOfFreeExts = v->numOfFreeExts;
    aPage->vi.firstFreeExt = v->firstFreeExt;

    /*@ set dirty bit */
    e = BfM_SetDirty(handle, &(v->volInfoPageId), PAGE_BUF);
    if (e < eNOERROR) ERRB1(handle, e, &(v->volInfoPageId), PAGE_BUF);

    /*@ free this page */
    e = BfM_FreeTrain(handle, &(v->volInfoPageId), PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);


    return(eNOERROR);

} /* RDsM_chage_NumOfExts_FirstFreeExt() */
