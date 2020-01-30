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
/*    Fine-Granule Locking Version                                            */
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
 * Module: LOT_DropObject.c
 *
 * Description:
 *  Drop the specified large object tree.
 *
 * Exports:
 *  Four LOT_DestroyObject(Four, XactTableEntry_T*, DataFileInfo*, char*, Boolean, LogParameter_T*)
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "TM.h"
#include "RDsM.h"
#include "BfM.h"
#include "LOT.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*@================================
 * LOT_DestroyObject( )
 *================================*/
/*
 * Function: Four LOT_DestroyObject(Four, XactTableEntry_T*, DataFileInfo*, char*, Boolean, LogParameter_T*)
 *
 * Description:
 *  Drop the specified large object tree.
 *
 * Returns:
 *  Error codes
 *    eBADPAGEID
 *    some errors caused by function calls
 *
 * Side Effects:
 *  The large object tree is dropped.
 */
Four LOT_DestroyObject(
    Four handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    DataFileInfo *finfo,	/* IN file information */
    char *anodeOrRootPageNo,    /* IN anode or root page no */
    Boolean rootWithHdr_Flag,   /* IN TRUE if root is with header */
    LogParameter_T *logParam)   /* IN log parameter */
{
    Four e;			/* error nubmer */
    Four i;			/* index variable */
    L_O_T_INode *anode;		/* pointer to the root node */
    Four len;			/* length of internal node in slotted page */
    PageID root;		/* the root of the large object tree */
    PageID child;		/* child PageID */


    TR_PRINT(handle, TR_LOT, TR1, ("LOT_DestroyObject()"));


    if (rootWithHdr_Flag) {
	anode = (L_O_T_INode*)anodeOrRootPageNo;

        for (i = 0; i < anode->header.nEntries; i++) {

            /* construct the child's PageID */
            MAKE_PAGEID(child, finfo->fid.volNo, anode->entry[i].spid);

            if (anode->header.height == 1) {	/* the deepest internal node */

                /* deallocate the leaf data page */
                e = RDsM_FreeTrain(handle, xactEntry, &child, TRAINSIZE2, finfo->tmpFileFlag, logParam);
                if (e < eNOERROR) ERR(handle, e);

            } else {		/* the internal node except the deepest internal node */

                if (finfo->tmpFileFlag) {
                    /*@ recursive call to drop the subtree */
                    e = lot_DropTree(handle, xactEntry, &child, TRUE, logParam);
                } else {
                    e = TM_XT_AddToDeallocList(handle, xactEntry, &child, NULL, NULL, DL_LRGOBJ); 
                }
                if (e < eNOERROR) ERR(handle, e);
            }
	}

    } else {
	/*@ get root PageID */
	MAKE_PAGEID(root, finfo->fid.volNo, *((ShortPageID *)anodeOrRootPageNo));


        if (finfo->tmpFileFlag) {
            /*@ recursive call to drop the subtree */
            e = lot_DropTree(handle, xactEntry, &root, TRUE, logParam);
        } else {
            e = TM_XT_AddToDeallocList(handle, xactEntry, &root, NULL, NULL, DL_LRGOBJ); 
        }
        if (e < eNOERROR) ERR(handle, e);
    }

    return(eNOERROR);

} /* LOT_DestroyObject() */


