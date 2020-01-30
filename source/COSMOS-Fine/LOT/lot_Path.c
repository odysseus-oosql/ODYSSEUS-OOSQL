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
 * Module: lot_Path.c
 *
 * Description:
 *  Handles the L_O_T_Path data structure.
 *
 * Exports:
 *  void lot_InitPath(Four, L_O_T_Path*)
 *  Boolean lot_EmptyPath(Four, L_O_T_Path*)
 *  Four lot_PushPath(Four, L_O_T_Path*, PageID*, Four, L_O_T_INode*, Four, Boolean)
 *  Four lot_PopPath(Four, L_O_T_Path*, PageID*, Four, L_O_T_INode**, Four*, Boolean*)
 *  Four lot_ReplaceTop(Four, L_O_T_Path*, PageID*, Four)
 *  void lot_FinalPath(Four, L_O_T_Path*)
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "BfM.h"
#include "LOT.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*@================================
 * lot_InitPath( )
 *================================*/
/*
 * Function: void lot_InitPath(Four, L_O_T_Path*)
 *
 * Description:
 *  Initialize the path structure
 *
 * Returns:
 *   None
 */
void lot_InitPath(
    Four handle,
    L_O_T_Path *path)		/* INOUT cut path */
{
    TR_PRINT(handle, TR_LOT, TR1, ("lot_InitPath(path=%P)", path));

    path->top = -1;

} /* lot_InitPath() */



/*@================================
 * lot_EmptyPath( )
 *================================*/
/*
 * Function: Boolean lot_EmptyPath(Four, L_O_T_Path*)
 *
 * Description:
 *  Return TRUE if path reaches the bottom.
 *
 * Returns:
 */
Boolean lot_EmptyPath(
    Four handle,
    L_O_T_Path *path)		/* IN cut path */
{
    TR_PRINT(handle, TR_LOT, TR1, ("lot_EmptyPath()"));

    return ((path->top == -1) ? TRUE:FALSE);

} /* lot_EmptyPath() */



/*@================================
 * lot_PushPath( )
 *================================*/
/*
 * Function: Four lot_PushPath(Four, L_O_T_Path*, PageID*, Four,
 *                             L_O_T_INode*, Four, Boolean)
 *
 * Description:
 *  Push the current node information into the path
 *
 * Returns:
 *  Error codes
 *    eEXCEEDMAXDEPTH_LOT
 *
 * Side effects:
 */
Four lot_PushPath(
    Four handle,
    L_O_T_Path  *path,		/* INOUT cut path */
    Buffer_ACC_CB *page_BCBP,   /* IN buffer access control block of the current node */
    L_O_T_INode *node,          /* IN pointer to the node when root is with header */
    Four        c_idx,		/* IN entry index of the child node in path */
    Boolean     c_uf)		/* IN underflow flag of the child node */
{
    TR_PRINT(handle, TR_LOT, TR1, ("lot_PushPath()"));

    if (path->top == LOT_MAXDEPTH) /* top is greater than MAXDEPTH */
	ERR(handle, eEXCEEDMAXDEPTH_LOT);

    path->top++;
    path->item[path->top].page_BCBP = page_BCBP;
    path->item[path->top].node = node;
    path->item[path->top].c_idx = c_idx;
    path->item[path->top].c_uf = c_uf;

    return(eNOERROR);

} /* lot_PushPath( ) */



/*@================================
 * lot_PopPath( )
 *================================*/
/*
 * Function: Four lot_PopPath(Four, L_O_T_Path*, PageID*, Four*,
 *                            L_O_T_INode**, Four*, Boolean*)
 *
 * Description:
 *  Pop the current node information from the path
 *
 * Returns:
 *  Error codes
 *    eEMPTYPATH_LOT
 *    some errors caused by function calls
 *
 * Side effects:
 */
Four lot_PopPath(
    Four handle,
    L_O_T_Path *path,		/* INOUT cut path */
    Buffer_ACC_CB **page_BCBP,	/* OUT buffer access control block of the current node */ 
    L_O_T_INode **node,         /* OUT pointer to node when root is with header */
    Four        *c_idx,		/* OUT entry index of the child node in path */
    Boolean     *c_uf)		/* OUT underflow flag of the child node */
{
    TR_PRINT(handle, TR_LOT, TR1, ("lot_PopPath()"));

    if (path->top < 0)		/* top has reached bottom already */
	ERR(handle, eEMPTYPATH_LOT);

    *page_BCBP = path->item[path->top].page_BCBP; 
    *node = path->item[path->top].node;
    *c_idx = path->item[path->top].c_idx;
    *c_uf = path->item[path->top].c_uf;
    path->top--;

    return(eNOERROR);

} /* lot_PopPath() */



/*@================================
 * lot_ReplaceTop( )
 *================================*/
/*
 * Function: Four lot_ReplaceTop(Four, L_O_T_Path*, PageID*, Four)
 *
 * Description:
 *  Change the node information in top node
 *
 * Returns:
 *  Error codes
 *    some errors caused by function calls
 *
 * Side effects:
 */
Four lot_ReplaceTop(
    Four handle,
    L_O_T_Path *path,		/* IN cut path */
    PageID     *pid,		/* IN PageID of the node to be replaced with */
    Four       c_idx_inc)	/* IN increment of index */
{
    Four e;			/* error number */
    Buffer_ACC_CB *apage_BCBP;  /* buffer access control block of the node to be replaced with */
    L_O_T_INodePage *apage;
    L_O_T_INode *anode;		/* pointer to the buffer */

    TR_PRINT(handle, TR_LOT, TR1, ("lot_ReplaceTop(path=%P, pid=%P, c_idx_inc=%ld)",
			   path, pid, c_idx_inc));

    if (path->top < 0)		/* There is no top entry. */
	ERR(handle, eEMPTYPATH_LOT);

    if (pid != (PageID *)NULL) {
	e = BfM_unfixBuffer(handle, path->item[path->top].page_BCBP, PAGE_BUF);
	if (e < eNOERROR) ERR(handle, e);

	e = BfM_getAndFixBuffer(handle, pid, M_FREE, &apage_BCBP, PAGE_BUF);
	if (e < eNOERROR) ERR(handle, e);

	apage = (L_O_T_INodePage *)apage_BCBP->bufPagePtr;
        anode = &apage->node;

	path->item[path->top].page_BCBP = apage_BCBP;
	path->item[path->top].c_idx += anode->header.nEntries + c_idx_inc;

    } else {
	path->item[path->top].c_idx += c_idx_inc;
    }

    return(eNOERROR);

} /* lot_ReplaceTop( ) */



/*@================================
 * lot_FinalPath( )
 *================================*/
/*
 * Function: Four lot_FinalPath(handle, L_O_T_Path*)
 *
 * Description:
 *  Finalize the path. This routine will be called when some errors are
 *  reached during the processing. So this routine ignore the errors and
 *  unfix all the fixed buffers.
 *
 * Returns:
 *  None
 */
void lot_FinalPath(
    Four handle,
    L_O_T_Path *path)		/* INOUT cut path */
{
    Four e;

    TR_PRINT(handle, TR_LOT, TR1, ("lot_FinalPath(path=%P)"));

    while (path->top >= 0) {
	(Four)BfM_unfixBuffer(handle, path->item[path->top].page_BCBP, PAGE_BUF);
	path->top --;
    }
} /* lot_FinalPath( ) */
