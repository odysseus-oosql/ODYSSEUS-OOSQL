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
 * Module: btm_Path.c
 *
 * Description:
 *  This module contains the functions related to the Btree Traverse Path Stack.
 *
 * Exports:
 *  void btm_InitPath(Four, btm_TraversePath*, LATCH_TYPE*)
 *  Four btm_FinalPath(Four, btm_TraversePath*)
 *  Boolean btm_IsEmptyPath(Four, btm_TraversePath*)
 *  Four btm_PushElemIntoPath(Four, btm_TraversePath*, Buffer_ACC_CB*, LSN*)
 *  Four btm_PopElemFromPath(Four, btm_TraversePath*, Buffer_ACC_CB**, LSN*)
 *  Four btm_ReadTopElemFromPath(Four, btm_TraversePath*, Buffer_ACC_CB**, LSN*)
 *  Four btm_GetTreeLatchInPath(Four, btm_TraversePath*, Four, LatchConditional)
 *  Four btm_ReleaseTreeLatchInPath(Four, btm_TraversePath*)
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "BfM.h"
#include "BtM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*
 * Function: void btm_InitPath(Four, btm_TraversePath*)
 *
 * Description:
 *  Initialize the Btree traverse path stack.
 *
 * Returns:
 *  None
 */
void btm_InitPath(
    Four 		handle,
    btm_TraversePath 	*path,		/* OUT traverse path to initialize */
    LATCH_TYPE 		*treeLatchPtr)	/* IN pointer to the tree latch */
{
    TR_PRINT(handle, TR_BTM, TR1, ("btm_InitPath(path=%P, treeLatchPtr=%P)", path, treeLatchPtr));


    path->treeLatchPtr = treeLatchPtr;
    path->top = -1;

} /* btm_InitPath( ) */



/*
 * Function: void btm_IsEmptyPath(handle, btm_TraversePath*)
 *
 * Description:
 *  Check if the traverse path stack is empty.
 *
 * Returns:
 *  TRUE if the path stack is empty, FALSE otherwise
 */
Boolean btm_IsEmptyPath(
    Four 		handle,
    btm_TraversePath 	*path)		/* IN traverse path to initialize */
{
    TR_PRINT(handle, TR_BTM, TR1, ("btm_IsEmptyPath(path=%P)", path));

    return((path->top == -1) ? TRUE:FALSE);

} /* btm_IsEmptyPath( ) */



/*
 * Function: Four btm_PushElemIntoPath(Four, btm_TraversePath*, Buffer_ACC_CB*, Lsn_T*)
 *
 * Description:
 *  Push information for the current node into the traverse path stack.
 *
 * Returns:
 *  Error code
 *    eEXCEEDMAXDEPTH_BTM
 */
Four btm_PushElemIntoPath(
    Four 		handle,
    btm_TraversePath 	*path,		/* INOUT traverse path stack */
    Buffer_ACC_CB 	*bcb,		/* IN buffer control block for current page */
    Lsn_T 		*lsn)		/* IN LSN of current page */
{
    TR_PRINT(handle, TR_BTM, TR1,
	     ("btm_PushElemIntoPath(path=%P, bcb=%P, lsn=%P)", path, bcb, lsn));


    if (++path->top >= BTM_MAXDEPTH) return(eEXCEEDMAXDEPTHOFBTREE_BTM);


    path->elem[path->top].bcbPtr = bcb;
    path->elem[path->top].lsn = *lsn;

    return(eNOERROR);

} /* btm_PushElemIntoPath() */



/*
 * Function: Four btm_PopElemFromPath(Four, btm_TraversePath*, Buffer_ACC_CB**, LSN*)
 *
 * Description:
 *  Pop the information for the node in the top of the traverse path stack.
 *
 * Returns:
 *  error code
 *    eTRAVERSEPATH_BTM
 */
Four btm_PopElemFromPath(
    Four 		handle,
    btm_TraversePath 	*path,		/* INOUT traverse path stack */
    Buffer_ACC_CB 	**bcb,		/* OUT buffer control block of page of stack top */
    Lsn_T 		*lsn)		/* OUT LSN of page of stack top */
{
    TR_PRINT(handle, TR_BTM, TR1,
	     ("btm_PopElemFromPath(handle, path=%P, bcb=%P, lsn=%P)", path, bcb, lsn));

    if (path->top < 0) ERR(handle, eTRAVERSEPATH_BTM);

    *bcb = path->elem[path->top].bcbPtr;
    *lsn = path->elem[path->top].lsn;

    path->top--;

    return(eNOERROR);

} /* btm_PopElemFromPath() */



/*
 * Function: Four btm_ReadTopElemFromPath(Four, btm_TraversePath*, Buffer_ACC_CB**, Lsn_T*)
 *
 * Description:
 *  Read the information for the node in the top of the traverse path stack.
 *
 * Returns:
 *  error code
 *    eTRAVERSEPATH_BTM
 */
Four btm_ReadTopElemFromPath(
    Four 		handle,
    btm_TraversePath 	*path,		/* IN traverse path stack */
    Buffer_ACC_CB 	**bcb,		/* OUT buffer control block of page of stack top */
    Lsn_T 		*lsn)		/* OUT LSN of page of stack top */
{
    TR_PRINT(handle, TR_BTM, TR1,
	     ("btm_ReadTopElemFromPath(handle, path=%P, bcb=%P, lsn=%P)", path, bcb, lsn));

    if (path->top < 0) ERR(handle, eTRAVERSEPATH_BTM);

    *bcb = path->elem[path->top].bcbPtr;
    *lsn = path->elem[path->top].lsn;

    return(eNOERROR);

} /* btm_ReadTopElemFromPath() */



/*
 * Function: Four btm_GetTreeLatchInPath(Four, btm_TraversePath*, Four, Four)
 *
 * Description:
 *  Get the tree latch in the given mode.
 *
 * Returns:
 *  error code
 *    some errors caused by function calls
 */
Four btm_GetTreeLatchInPath(
    Four 		handle,
    btm_TraversePath 	*path,		/* IN traverse path stack */
    Four 		mode,		/* IN required latch mode */
    LatchConditional 	cond)      	/* IN required latch condition */
{
    Four 		e;		/* error code */


    TR_PRINT(handle, TR_BTM, TR1,
	     ("btm_GetTreeLatchInPath(handle, path=%P, mode=%ld, cond=%ld)", path, mode, cond));


    e = SHM_getLatch(handle, path->treeLatchPtr, procIndex, mode, cond, NULL);
    if (e < eNOERROR) ERR(handle, e);

    return(e);

} /* btm_GetTreeLatchInPath() */



/*
 * Function: Four btm_ReleaseTreeLatchInPath(Four, btm_TraversePath*)
 *
 * Description:
 *  Release the tree latch.
 *
 * Returns:
 *  error code
 *    some errors caused by function calls
 */
Four btm_ReleaseTreeLatchInPath(
    Four 		handle,
    btm_TraversePath 	*path)		/* IN traverse path stack */
{
    Four 		e;		/* error code */


    TR_PRINT(handle, TR_BTM, TR1, ("btm_ReleaseTreeLatchInPath(path=%P)", path));


    e = SHM_releaseLatch(handle, path->treeLatchPtr, procIndex);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* btm_ReleaseTreeLatchInPath() */



/*
 * Function: Four btm_FinalPath(Four, btm_TraversePath*)
 *
 * Description:
 *  Finalize the Btree Traverse Path Stack.
 *  We unfix the buffers.
 *
 * Returns:
 *  error code
 *    eTRAVERSEPATH_BTM
 */
Four btm_FinalPath(
    Four 		handle,
    btm_TraversePath 	*path)		/* INOUT traverse path stack */
{
    Four 		e;		/* error code */


    TR_PRINT(handle, TR_BTM, TR1, ("btm_finalPath(path=%P)", path));

    if (path->top < -1) ERR(handle, eTRAVERSEPATH_BTM);

    while (path->top >= 0) {
	e = BfM_unfixBuffer(handle, path->elem[path->top].bcbPtr, PAGE_BUF);
	if (e < eNOERROR) ERR(handle, e);

	path->top --;
    }

    return(eNOERROR);

} /* btm_FinalPath() */





