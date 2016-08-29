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
 * Module: lot_Path.c
 *
 * Description:
 *  Handles the L_O_T_Path data structure.
 *
 * Exports:
 *  void lot_InitPath(L_O_T_Path*)
 *  Boolean lot_EmptyPath(L_O_T_Path*)
 *  Four lot_PushPath(L_O_T_Path*, PageID*, Two, L_O_T_INode*, Two, Boolean)
 *  Four lot_PopPath(L_O_T_Path*, PageID*, Two, L_O_T_INode**, Two*, Boolean*)
 *  Four lot_ReplaceTop(L_O_T_Path*, PageID*, Two)
 *  void lot_FinalPath(L_O_T_Path*)
 */


#include "common.h"
#include "trace.h"
#include "BfM.h"
#include "LOT_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * lot_InitPath()
 *================================*/
/*
 * Function: void lot_InitPath(L_O_T_Path*)
 *
 * Description:
 *  Initialize the path structure
 *
 * Returns:
 *   None
 */
void lot_InitPath(
    Four       handle, 
    L_O_T_Path *path)		/* INOUT cut path */
{
    TR_PRINT(TR_LOT, TR1, ("lot_InitPath(handle, path=%P)", path));

    path->top = -1;
    
} /* lot_InitPath() */



/*@================================
 * lot_EmptyPath()
 *================================*/
/*
 * Function: Boolean lot_EmptyPath(L_O_T_Path*)
 *
 * Description:
 *  Return TRUE if path reaches the bottom.
 *
 * Returns:
 *  Return TRUE if path reaches the bottom.
 */
Boolean lot_EmptyPath(
    Four handle,
    L_O_T_Path *path)		/* IN cut path */
{
    TR_PRINT(TR_LOT, TR1, ("lot_EmptyPath(handle, path=%P)", path));

    return ((path->top == -1) ? TRUE:FALSE);
    
} /* lot_EmptyPath() */



/*@================================
 * lot_PushPath()
 *================================*/
/*
 * Function: Four lot_PushPath(L_O_T_Path*, PageID*, Two, L_O_T_INode*, Two, Boolean)
 *
 * Description:
 *  Push the current node information into the path
 *
 * Returns:
 *  error code
 *    eEXCEEDMAXDEPTH_LOT
 */
Four lot_PushPath(
    Four handle,
    L_O_T_Path  *path,		/* INOUT cut path */
    PageID      *pid,		/* IN PageID of the current node */
    Two         rootSlotNo,	/* IN slot no of root if root is with header */
    L_O_T_INode *node,		/* IN pointer to the buffer holding the node */
    Two         c_idx,		/* IN entry index of the child node in path */
    Boolean     c_uf)		/* IN underflow flag of the child node */
{
    TR_PRINT(TR_LOT, TR1,
             ("lot_PushPath(path=%P, pid=%P, rootSlotNo=%ld, node=%P, c_idx=%ld, c_uf=%ld",
	      path, pid, rootSlotNo, node, c_idx, c_uf));

    if (path->top == LOT_MAXDEPTH) /* top is greater than MAXDEPTH */
	ERR(handle, eEXCEEDMAXDEPTH_LOT);

    path->top++;
    path->item[path->top].pid = *pid;
    path->item[path->top].rootSlotNo = rootSlotNo;
    path->item[path->top].node = node;
    path->item[path->top].c_idx = c_idx;
    path->item[path->top].c_uf = c_uf;

    return(eNOERROR);

} /* lot_PushPath() */



/*@================================
 * lot_PopPath()
 *================================*/
/*
 * Function: Four lot_PopPath(L_O_T_Path*, PageID*,Two*, L_O_T_INode**, Two*, Boolean*)
 *
 * Description:
 *  Pop the current node information from the path
 *
 * Returns:
 *  error code
 *    eEMPTYPATH_LOT
 *    some errors caused by function calls
 */
Four lot_PopPath(
    Four handle,
    L_O_T_Path *path,		/* INOUT cut path */
    PageID     *pid,		/* OUT PageID of the current node */
    Two        *rootSlotNo,	/* OUT slot no of root if root is with header */
    L_O_T_INode **node,		/* OUT pointer to the buffer holding the node */
    Two         *c_idx,		/* OUT entry index of the child node in path */
    Boolean     *c_uf)		/* OUT underflow flag of the child node */
{
    TR_PRINT(TR_LOT, TR1,
             ("lot_PopPath(path=%P, pid=%P, rootSlotNo=%P, node=%P, c_idx=%ld, c_uf=%ld",
	      path, pid, rootSlotNo, node, c_idx, c_uf));

    if (path->top < 0)		/* top has reached bottom already */
	ERR(handle, eEMPTYPATH_LOT);

    *pid = path->item[path->top].pid;
    *rootSlotNo = path->item[path->top].rootSlotNo;
    *node = path->item[path->top].node;
    *c_idx = path->item[path->top].c_idx;
    *c_uf = path->item[path->top].c_uf;
    path->top--;

    return(eNOERROR);
    
} /* lot_PopPath() */
    


/*@================================
 * lot_ReplaceTop()
 *================================*/
/*
 * Function: Four lot_ReplaceTop(L_O_T_Path*, PageID*, Two)
 *
 * Description:
 *  Change the node information in top node
 *
 * Returns:
 *  error code
 *    some errors caused by function calls
 */
Four lot_ReplaceTop(
    Four handle,
    L_O_T_Path *path,		/* IN cut path */
    PageID     *pid,		/* IN PageID of the node to be replaced with */
    Two        c_idx_inc)	/* IN increment of index */
{
    Four e;			/* error number */
    L_O_T_INode *anode;		/* pointer to the buffer */
    
    TR_PRINT(TR_LOT, TR1, ("lot_ReplaceTop(handle, path=%P, pid=%P, c_idx_inc=%ld)",
			   path, pid, c_idx_inc));

    if (path->top < 0)		/* There is no top entry. */
	ERR(handle, eEMPTYPATH_LOT);
	
    if (pid != (PageID *)NULL) {
        /* The page to be freed was merged with the given page and deallocated.
         * The page was already freed by BfM_RemoveTrain() function when it
         * was merged and deallocated.
         * This bug was caused by adding BfM_RemoveTrain(), which was added
         * for fixing another bug.
         */
	
	e = BfM_GetTrain(handle, pid, (char **)&anode, PAGE_BUF);
	if (e < 0) ERR(handle, e);

	path->item[path->top].pid = *pid;
	path->item[path->top].node = anode;
	path->item[path->top].c_idx += anode->header.nEntries + c_idx_inc;
	
    } else {
	path->item[path->top].c_idx += c_idx_inc;
    }

    return(eNOERROR);

} /* lot_ReplaceTop() */



/*@================================
 * lot_FinalPath()
 *================================*/
/*
 * Function: void lot_FinalPath(L_O_T_Path*)
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
    Four       handle, 
    L_O_T_Path *path)		/* INOUT cut path */
{
    TR_PRINT(TR_LOT, TR1, ("lot_FinalPath(handle, path=%P)"));

    while (path->top >= 0) {
	BfM_FreeTrain(handle, &path->item[path->top].pid, PAGE_BUF);
	path->top --;
    }
    
} /* lot_FinalPath() */
    
