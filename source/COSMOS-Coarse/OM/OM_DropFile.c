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
 * Module :	OM_DropFile.c
 *
 * Description : 
 *  Drop the given data file.
 *
 * Exports:
 *  Four OM_DropFile(FileID*, Pool*, DeallocListElem*)
 */


#include "common.h"
#include "trace.h"
#include "Util.h"
#include "OM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * OM_DropFile()
 *================================*/
/* 
 * Function: Four OM_DropFile(FileID*, Pool*, DeallocListElem*)
 *
 * Description : 
 *  Drop the given data file. We don't have to access pages in the file.
 *  It is enough to call RDsM_DropSegment(). But this operation is delayed
 *  until the volume will be dismouted. And the pages of the file should be
 *  inserted into the dealloc list. Instead of inserting all pages
 *  we insert the FileID into the dealloc list with the 'pid' field
 *  set to NILPAGEID to differtiate the page and file. Before inserting
 *  the volId, we eleminate the previously deallocated pages of the file from
 *  the dealloc list.
 *
 * Returns:
 *  error code
 *    eBADPARAMETER_OM
 *    some errors caused by function calls
 *
 * Side Effects:
 *  1) parameter dlHead
 *     The nodes for the pages of the dropped file are deleted from the list
 *     and a node for the file is inserted.
 */
Four OM_DropFile(
    Four handle,
    PhysicalFileID 	*pFid,		/* IN File ID for the file to be deleted */ 
    Pool   			*dlPool,	/* INOUT pool of dealloc list elements */
    DeallocListElem *dlHead)    /* INOUT head of dealloc list */
{
    Four 			e;				/* for the error number */
    DeallocListElem *prevdlElem; 	/* previous element of the list */
    DeallocListElem *dlElem;     	/* current element of the list */

    
    TR_PRINT(TR_OM, TR1,
             ("OM_DropFile(handle, pFid=%P, dlPool=%P, dlHead=%P)",
	      pFid, dlPool, dlHead));

    
    /*@ Check parameters. */
    if (pFid == NULL || dlPool == NULL || dlHead == NULL)
	ERR(handle, eBADPARAMETER_OM);

    /*
    ** Insert a new node for the dropped file.
    */    
    e = Util_getElementFromPool(handle, dlPool, &dlElem);
    if (e < 0) ERR(handle, e);
    
    dlElem->type = DL_FILE;
    dlElem->elem.pFid = *pFid;    	/* save the file identifier */
    dlElem->next = dlHead->next; 	/* insert to the list */
    dlHead->next = dlElem;       	/* new first element of the list */

    return(eNOERROR);
    
} /* OM_DropFile() */
