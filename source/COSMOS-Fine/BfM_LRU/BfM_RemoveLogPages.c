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
 * Module: BfM_RemoveLogPages.c
 *
 * Description :
 *  Remove the log pages from the buffer pool.
 */


#include <assert.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "SHM.h"
#include "BfM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*@================================
 * BfM_RemoveLogPages()
 *================================*/
/*
 * Function: Four BfM_RemoveLogPages(void)
 *
 * Description :
 *  Remove log pages from the buffer pool
 *
 * Returns:
 *  error code
 */
Four BfM_RemoveLogPages(
    Four    handle)
{
    Four e;			/* error */
    Four type;			/* buffer type */
    BufTBLEntry *anEntry;	/* a Buffer Table Entry to be flushed */
    BfMHashKey localKey;	
    Four i;			/* loop index */


    TR_PRINT(handle, TR_BFM, TR1, ("BfM_RemoveLogPages()"));


    /* For each buffer pool */
    for (type = 0; type < NUM_BUF_TYPES; type++) {

	/*@ flush dirty buffer */
	/* Flush out the dirty buffer with same volume number
	 * in the current buffer pool. */
	for( anEntry = PHYSICAL_PTR(BI_BUFTABLE(type)), i = 0;
	     i < BI_NBUFS(type); anEntry++, i++ )  {

	    if (!IS_NILBFMHASHKEY(anEntry->key) && GET_PAGE_TYPE(BI_BUFFER(type, anEntry)) == LOG_PAGE_TYPE) {
		/* This buffer holds a log page. */

		/* mutex begin :: for update Buffer Table Entry */
		localKey = anEntry->key;
		ERROR_PASS(handle, bfm_lock(handle, (TrainID *)&localKey, type));

		if ( !EQUALKEY(&anEntry->key, &localKey) ) {
		    ERROR_PASS(handle, bfm_unlock(handle, (TrainID *)&localKey, type));
		    continue;
		}
		/* This buffer is an unfixed buffer. */
		if (anEntry->fixed > 0) {
#ifdef TRACE
		    printf("BfM_dismount : There is a buffer of which 'fixed' is not 0. pid=(%ld,%ld)\n", anEntry->key.volNo, anEntry->key.pageNo);
#endif
		    ERROR_PASS(handle, bfm_unlock(handle, (TrainID *)&localKey, type));
		    ERR(handle, eFLUSHFIXEDBUF_BFM);
		}

                assert(anEntry->dirtyFlag == 0);

		/*@ delete the victim */
		/* Delete the victim from the hash table */
		e = bfm_delete(handle,  anEntry, type );
		if (e < eNOERROR) ERR(handle, e);

		/*@ clear the information */
		/* clear the infomation because a train in the dismounted
		   volume will not be used near future*/
                 /* Set anEntry->fixed by 1 not to select as victim
                 during the initializing anEntry->key to NIL*/
                anEntry->fixed = 1;
                anEntry->invalidFlag = TRUE;
		SET_NILBFMHASHKEY(anEntry->key);
                anEntry->fixed = 0;

		/*@ release latch */
		/* mutex begin :: for update Buffer Table Entry */
		ERROR_PASS(handle, bfm_unlock(handle, (TrainID *)&localKey, type));

	    }
	}
    }
    return( eNOERROR );

}  /* BfM_RemoveLogPages */

