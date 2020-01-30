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
 * Module: shm_processTable.c
 *
 * Description:
 *  Process Table  Initialization and finalization
 *
 * Exports:
 *	shm_initProcessTable()
 *	shm_allocAndInitProcessTableEntry()
 *	shm_freeProcessTableEntry()
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "SHM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*@================================
 * shm_initProcessTable( )
 *================================*/
/* shm_processTable_init :: initialize processTable at initialization time */
Four shm_initProcessTable(
    Four 	handle
)
{

    Four 	e, i;

    /*@ initialize */
    /* initialize process table and trasaction table */

    for (i = 0; i < MAXPROCS; i++) {

        /* this part is executed by TM
         *
         * SET_NIL_XACTID(tm_shmPtr->xactTable[i].xactID);
         * tm_shmPtr->xactTable[i].dlHead.next = NULL;
         */

	tm_shmPtr->processTable[i].pID = NIL;
    }

    tm_shmPtr->procCounter = 0;

    return (eNOERROR);
}



/*@================================
 * shm_allocAndInitProcessTableEntry( )
 *================================*/
Four shm_allocAndInitProcessTableEntry(
    Four 	handle,
    Four 	*pindex)
{
    Four 	i;			/* loop index */

    for ( i = 0; i < MAXPROCS; i++ ){
	if ( tm_shmPtr->processTable[i].pID == NIL ) { /* semID => semID */
            tm_shmPtr->processTable[i].pID = i;
	    *pindex = i;
            tm_shmPtr->procCounter++;
	    return(eNOERROR);
	}
    }


    ERR(handle, eFULLPROCTABLE_SHM);

}



/*@================================
 * shm_freeProcessTableEntry( )
 *================================*/
Four shm_freeProcessTableEntry(
    Four 	handle,
    Four 	pindex
)
{

     Four 	e;

     /*
      * e = Util_finalPool(handle, &(tm_shmPtr->TCB_Pool));
      * if (e < eNOERROR) ERR(handle, e);
      */

    /* empty the corresponding entry of the processTable */
    tm_shmPtr->processTable[pindex].pID = NIL; /* semID => semID */
    tm_shmPtr->procCounter --;

    return (eNOERROR);
}

