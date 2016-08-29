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
 * Module: bfm_AllocTrain.c
 *
 * Description : 
 *  Allocate a new buffer from the buffer pool.
 *
 * Exports:
 *  Four bfm_AllocTrain(Four)
 */

#include <errno.h>
#include "common.h"
#include "trace.h"
#include "BfM_Internal.h"
#include "perThreadDS_Internal.h"
#include "perProcessDS.h"


extern CfgParams_T sm_cfgParams;


#ifdef USE_SHARED_MEMORY_BUFFER	

/*@================================
 * bfm_AllocTrain()
 *================================*/
/*
 * Function: Four bfm_AllocTrain(Four)
 *
 * Description :
 *  Allocate a new buffer from the buffer pool.
 *  The used buffer pool is specified by the parameter 'type'.
 *  This routine uses the second chance buffer replacement algorithm
 *  to select a victim.  That is, if the reference bit of current checking
 *  entry (indicated by BI_NEXTVICTIM(type), macro for
 *  bufInfo[type].nextVictim) is set, then simply clear
 *  the bit for the second chance and proceed to the next entry, otherwise
 *  the current buffer indicated by BI_NEXTVICTIM(type) is selected to be
 *  returned.
 *  Before return the buffer, if the dirty bit of the victim is set, it
 *  must be force out to the disk.
 *
 * Note:
 *  This function is taken from bfm_allocBuffer of COSMOS-CC.
 *
 * Returns :
 *  1) An index of a new buffer from the buffer pool
 *  2) Error codes: Negative value means error code.
 *     some errors caused by fuction calls
 */
Four bfm_AllocTrain(
    Four handle,
    Four 	type)                   /* IN type of buffer (PAGE or TRAIN) */
{
    UFour  	 		j;                  /* loop index */
    Four 	 		e;                  /* for error */
    BfMHashKey 	 	localKey;        	/* local key to handle replaced entry */
    Four	 		victim;
    BufferTable* 	victimEntry;
    LockMode	 	volumeLockMode;	

    /* THINK :: exit condition of for loop is needed ?? */
    /* starting from the NextVictim, find the victim */

    for  (j = BI_NEXTVICTIM(type)++;; j = BI_NEXTVICTIM(type)++) {

	victim = j % BI_NBUFS(type);
	/* 
	 * Because of performance, use victimEntry pointer 
	 * instead of BI_... macros defined in BfM_Internal.h
	 */ 
	victimEntry = &BI_BUFTABLE_ENTRY(type, victim);

        if ( victimEntry->fixed <= 0 && bfm_CheckBufPageInMountedVolume(handle, BFM_PER_THREAD_DS(handle).mountedVolNos, 
	     BFM_PER_THREAD_DS(handle).nMountedVols, &(victimEntry->key)) ) {

            /* if the reference bit is set, simple clear it */
            if ( victimEntry->bits & REFER ) 
		victimEntry->bits ^= REFER;

            else { /* The bit is 0 */

		/* 
		 * Case for Bulk Loading 
		 *
		 * Assume the following codes operates in stand-alone process.
		 * Must not shared buffer wish other processes.
		 */
		if (sm_cfgParams.useBulkFlush && victimEntry->bits & DIRTY) {
		    e = bfm_FlushTrains(handle, type);
		    if( e < 0 ) ERR(handle,  e );

		    break;
		}

                localKey = victimEntry->key; 

                /* Invalid case */
                if ( IS_NILBFMHASHKEY(localKey) ) { 

#ifdef USE_MUTEX
		    /* Mutex Invalid Begin */
		    e = MUTEX_TRYLOCK(&(victimEntry->mutex));

                    /* do not wait for acquiring latch
                     * and try next entry
                     */
		    if (e == EBUSY) continue;
#else
		    /* Mutex INVALID Begin */
                    e = bfm_GetLatch(handle, &(victimEntry->latch), procIndex, M_EXCLUSIVE, M_CONDITIONAL,  NULL);
		    if (e < eNOERROR) ERR(handle, e);

                    /* do not wait for acquiring latch
                     * and try next entry
                     */
                    if (e == BFM_BUSYLATCH) continue;
#endif


                    /* control the process arrived lately */
                    /* if ( !(*victimEntry)->invalidFlag ) { */
                    if ( victimEntry->fixed > 0 )  {
#ifdef USE_MUTEX
			ERROR_PASS(handle, MUTEX_UNLOCK(&(victimEntry->mutex)));
#else
                        ERROR_PASS(handle, bfm_ReleaseLatch(handle, &(victimEntry->latch), procIndex));
#endif
                        continue;
                    }

                    victimEntry->fixed = 1; 	/* set fix value quickly */
		    victimEntry->bits = ALL_0;
		    victimEntry->bits |= VALID; 

#ifdef USE_MUTEX
                    /* Mutex INVALID End */
                    ERROR_PASS(handle, MUTEX_UNLOCK(&(victimEntry->mutex)));
#else
                    /* Mutex INVALID End */
                    ERROR_PASS(handle, bfm_ReleaseLatch(handle, &(victimEntry->latch), procIndex));
#endif
		
                    return(victim);
                }

                /* Now, Valid case */

                /* Mutex VALID Begin */
                ERROR_PASS(handle, bfm_Lock(handle, (TrainID* )&localKey, type));

                if (victimEntry->fixed > 0) {
                    /* perhaps this entry(key) was already replaced by other key */
                    ERROR_PASS(handle, bfm_Unlock(handle, (TrainID *)&localKey, type));
                }
                else {

                    /* caused by dismount/remove/discard operation */
                    if (IS_NILBFMHASHKEY(victimEntry->key)) {

                        /* Mutex INVALID Begin */
                        ERROR_PASS(handle, bfm_Unlock(handle, (TrainID *)&localKey, type));

#ifdef USE_MUTEX
                        /* Mutex INVALID Begin */
                        e = MUTEX_TRYLOCK(&(victimEntry->mutex));
                        if (e < eNOERROR) ERR(handle, e);

                        /* do not wait for acquiring latch
                         * and try next entry
                         */
                        if (e == EBUSY) continue;
#else
                        /* Mutex INVALID Begin */
                        e = bfm_GetLatch(handle, &(victimEntry->latch), procIndex, M_EXCLUSIVE, M_CONDITIONAL,  NULL);
			if (e < eNOERROR) ERR(handle, e);

                        /* do not wait for acquiring latch
                         * and try next entry
                         */
                        if (e == BFM_BUSYLATCH) continue;
#endif
                        /* control the process arrived lately */
                        if ( victimEntry->fixed > 0  ) {
#if USE_MUTEX
                            ERROR_PASS(handle, MUTEX_UNLOCK(&(victimEntry->mutex)));
#else
                            ERROR_PASS(handle, bfm_ReleaseLatch(handle, &(victimEntry->latch), procIndex));
#endif
                            continue;
                        }

                        victimEntry->fixed = 1; /* set fix value quickly */
			victimEntry->bits = ALL_0;
                        victimEntry->bits |= VALID;

#if USE_MUTEX
                        /* Mutex INVALID End */
                        ERROR_PASS(handle, MUTEX_UNLOCK(&(victimEntry->mutex)));
#else
                        /* Mutex INVALID End */
                        ERROR_PASS(handle, bfm_ReleaseLatch(handle, &(victimEntry->latch), procIndex));
#endif
			
                        return(victim);
                    }

                    /* check the above bfm_lock operation is correct */
                    if ( !EQUALKEY(&(victimEntry->key), &localKey)
                         || victimEntry->fixed > 0) {
                        ERROR_PASS(handle, bfm_Unlock(handle, (TrainID *)&localKey, type));
                        continue;
                    }

                    victimEntry->fixed = 1; /* set fix value quickly */

                    /* if the dirty bit is set, force out to the disk */
#ifdef DBLOCK
                    if( (victimEntry->bits & DIRTY) )  {
			volumeLockMode = RDsM_GetVolumeLockMode(handle, (victimEntry->key).volNo);
			if (volumeLockMode == L_X || volumeLockMode == L_IX || volumeLockMode == L_SIX) { 
                            e = bfm_FlushTrain(handle, (TrainID* )&(victimEntry->key), type);
                            if( e < eNOERROR ) {
                               ERROR_PASS(handle, bfm_Unlock(handle, (TrainID* )&(victimEntry->key), type));
                               ERR(handle, e);
                            }
			}
			else {
			    victimEntry->fixed = 0;
                            ERROR_PASS(handle, bfm_Unlock(handle, (TrainID* )&(victimEntry->key), type));
			    continue;
			}
                    }
#else
                    if( (victimEntry->bits & DIRTY) ) {
                        e = bfm_FlushTrain(handle, (TrainID* )&(victimEntry->key), type);
                        if( e < eNOERROR ) {
                           ERROR_PASS(handle, bfm_Unlock(handle, (TrainID* )&(victimEntry->key), type));
                           ERR(handle, e);
			}
		    }
#endif

                    /* Delete the victim from the hash table */
                    e = bfm_Delete(handle, &(victimEntry->key), type );
                    if( e < eNOERROR ) {
                        ERROR_PASS(handle, bfm_Unlock(handle, (TrainID* )&(victimEntry->key), type));
                        ERR(handle, e);
                    }

 
		    /* Clear all bits. */
                    victimEntry->bits = ALL_0;

                    /* Mutex VALID End */
                    ERROR_PASS(handle, bfm_Unlock(handle, (TrainID* )&localKey, type));
		

                    return(victim);
                }               /* else  */

            }                   /* else reference bit == 0 */

        }   /* BI_NFIXED(type, j) <= 0 */
    } /* for */


    /* by victimEntry parameter, return victim */
    return(victim);

}  /* bfm_allocBuffer */

#else

/*@================================
 * bfm_AllocTrain()
 *================================*/
/*
 * Function: Four bfm_AllocTrain(Four)
 *
 * Description : 
 *  Allocate a new buffer from the buffer pool.
 *  The used buffer pool is specified by the parameter 'type'.
 *  This routine uses the second chance buffer replacement algorithm
 *  to select a victim.  That is, if the reference bit of current checking
 *  entry (indicated by BI_NEXTVICTIM(type), macro for
 *  bufInfo[type].nextVictim) is set, then simply clear
 *  the bit for the second chance and proceed to the next entry, otherwise
 *  the current buffer indicated by BI_NEXTVICTIM(type) is selected to be
 *  returned.
 *  Before return the buffer, if the dirty bit of the victim is set, it 
 *  must be force out to the disk.
 *
 * Returns;
 *  1) An index of a new buffer from the buffer pool
 *  2) Error codes: Negative value means error code.
 *     eNOUNFIXEDBUF_BFM - There is no unfixed buffer.
 *     some errors caused by fuction calls
 */
Four bfm_AllocTrain(
    Four handle,
    Four 	type)			/* IN type of buffer (PAGE or TRAIN) */
{
    Four 	e;				/* for error */
    Four 	victim;			/* return value */
    Four 	i;
    
    TR_PRINT(TR_BFM, TR1, ("bfm_AllocTrain(handle, type=%ld)", type));

    /* starting from the NextVictim, find the victim  */    
    /* If there is a unfixed buffer, we can find it at most 2 loops */
    victim = BI_NEXTVICTIM(type);
    for(i=0; i < 2*BI_NBUFS(type); i++, victim = (victim+1) % BI_NBUFS(type))
	
	if( BI_NFIXED(type, victim) <= 0 ) {
	    
	    /* if the reference bit is set, simply clear it for the second chance */
	    if( BI_BITS(type, victim) & REFER ) 
            	BI_BITS(type, victim) ^= REFER;   /* clear the bit */
    
	    else  {  /* The bit is 0 */
		
		if(sm_cfgParams.useBulkFlush && BI_BITS(type, victim) & DIRTY)
		{
		    e = bfm_FlushTrains(handle, type);
		    if( e < 0 ) ERR(handle,  e );
		}
		else
		{
            	    /* if the dirty bit is set, force out to the disk */
            	    if( BI_BITS(type, victim) & DIRTY )  {
			e = bfm_FlushTrain(handle, (TrainID *)&BI_KEY(type, victim), type);
			if( e < 0 ) ERR(handle,  e );
            	    }
		    
              	    /* Delete the victim from the hash table */
		    if(!IS_NILBFMHASHKEY(BI_KEY(type, victim))) {
			e = bfm_Delete(handle,  &BI_KEY(type, victim), type );
			if( e < 0 ) ERR(handle,  e );
		    }

		    BI_BITS(type, victim) = ALL_0;
		}

		/* The next buffer is the next victim. */
		BI_NEXTVICTIM(type) = (victim + 1) % BI_NBUFS(type);
            	
		break;  /*@ escape from the loop */
	    }
	    
	}

    if (i == 2*BI_NBUFS(type))
	ERR(handle, eNOUNFIXEDBUF_BFM);
    
    return( victim );
    
}  /* bfm_AllocTrain */
#endif

#ifdef USE_SHARED_MEMORY_BUFFER
/*@================================
 * bfm_CheckBufPageInMountedVolume()
 *================================*/
/*
 * Function: bfm_CheckBufPageInMountedVolume(Four*, Four, TrainID* Four)
 *
 * Description : 
 *  Check the given buffer page is in mounted volumes.
 *  Buffer page is described with type and index arguments.
 *
 * Returns;
 *  true:  if the given buffer page is in mounted volume.
 *  false: if the given buffer page is not in mounted volume. 
 */
Four bfm_CheckBufPageInMountedVolume(
    Four handle,
    Four 			*volNos,
    Four 			nMountedVols,
    BfMHashKey		*key)
{
    Four 		i;
    VolNo 		volNo;
    
    TR_PRINT(TR_BFM, TR1, ("bfm_CheckBufPageInMountedVolume"));

    /*
     * Utilities such as OOSQL_CreateDB call BfM_GetTrain() function without mounting volumes.
     * So, there are no mounted volumes and this function always returns FALSE. It occurs eNOUNFIXEDBUF_BFM error.
     * 
     * To prevent eNOUNFIXEDBUF_BFM error, if there are no mounted volumes, return TRUE.
     */
    if (nMountedVols == -1) {
	return TRUE;
    }

    volNo  = key->volNo;
    
    for (i = 0; i < nMountedVols; i++) { 
	if (volNo == volNos[i] || IS_NILBFMHASHKEY(*key)) return TRUE;
    }
    
    return FALSE;
} /* bfm_CheckBufPageInMountedVolume() */
#endif
