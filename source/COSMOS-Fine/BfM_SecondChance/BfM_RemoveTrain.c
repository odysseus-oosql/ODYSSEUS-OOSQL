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
 * Module: BfM_RemoveTrain.c
 *
 * Description :
 *  Return a buffer which has the disk content indicated by `trainId'.
 *
 * Exports:
 *  Four BfM_RemoveTrain(Four, TrainID *, Four, Buffer_ACC_CB *, Four)
 */


#include <string.h>  /* for memcpy */
#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "SHM.h"
#include "BfM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*@================================
 * BfM_RemoveTrain( )
 *================================*/
/*
 * Function: Four BfM_RemoveTrain(Four, TrainID *, Four, Boolean)
 *
 * Description :
 *  Remove a buffer.
 *  If 'writeFlag' is TRUE, flush out the buffer before removing
 *
 * Returns :
 *  error code
 *    eBADBUFFER_BFM - Invalid Buffer
 *    eNOTFOUND_BFM  - The key isn't in the hash table
 *    some errors caused by function calls
 *
 * Side effects:
 */
Four BfM_RemoveTrain(
    Four handle,
    TrainID     *trainId,	/* IN  train to be used */
    Four        type,		/* IN  buffer type */
    Boolean     writeFlag)      /* IN flag which indicates flush-out is needed */
{
    Four        status;		/* for returned message */
    Four        e;		/* for resource release error */
    BufTBLEntry *anEntry;	/* a buffer Table Entry */


    TR_PRINT(handle, TR_BFM, TR1,("BfM_RemoveTrain(trainId=%P, type=%ld, writeFlag=%ld)", trainId, type, writeFlag));


    /*@
     * Check the validity of given parameters
     */

    /* Is the buffer type valid? */
    if (IS_BAD_BUFFERTYPE(type)) ERR(handle, eBADBUFFERTYPE_BFM);


    /*@
     * get latch
     */

    /* Mutex Begin : mutual exclusively fix the trainID page */
    ERROR_PASS(handle, bfm_lock(handle, trainId, type));


    /*
     * if given train isn't in buffer, skip!!
     */
    status = bfm_lookUp(handle, (BfMHashKey *)trainId, type, &anEntry);
    if (status == NOTFOUND_IN_HTABLE) {
	ERROR_PASS(handle, bfm_unlock(handle, trainId, type));
	return eNOERROR;
    } else if (status  < eNOERROR) {
	ERROR_PASS(handle, bfm_unlock(handle, trainId, type));
	ERR(handle,  status );
    }


    /*
     * If the buffer is dirty, write into the disk
     */
    if (writeFlag == TRUE && anEntry->dirtyFlag) {
        e = bfm_flushBuffer(handle, anEntry, type);
        if( e < 0 ) {
            ERROR_PASS(handle, bfm_unlock(handle, trainId, type));
            ERR(handle,  e );
        }
    }


    /*
     * Delete the victim from the hash table
     */
    e = bfm_delete(handle, anEntry, type);
    if( e < 0 ) {
        ERROR_PASS(handle, bfm_unlock(handle, trainId, type));
        ERR(handle,  e );
    }


     /* Set anEntry->fixed by 1 not to select as victim
      during the initializing anEntry->key to NIL */
    anEntry->fixed = 1;
    anEntry->invalidFlag = TRUE; 
    SET_NILBFMHASHKEY(anEntry->key);

    /* fixed counter is reset to 0. */
    anEntry->fixed = 0;


    /*@
     * release latch
     */

    /* Mutex End : mutual exclusively fix the trainID page */
    ERROR_PASS(handle, bfm_unlock(handle, trainId, type));


    return( eNOERROR );   /* No error */

}  /* BfM_RemoveTrain */
