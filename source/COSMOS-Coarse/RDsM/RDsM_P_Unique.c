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
 * Module: RDsM_P_Unique.c
 *
 * Description:
 *
 *
 * Exports:
 *  Four RDsM_P_Unique(Four)
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "BfM.h"
#include "RDsM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * RDsM_P_Unique()
 *================================*/
/*
 * Function: Four RDsM_P_Unique(Four)
 *
 * Description:
 *  print all the unique numbers
 *
 * Returns:
 *  error code
 */
Four RDsM_P_Unique(
    Four 			handle,
    Four			volNo)		/* IN volume number */
{
    Four			e;			/* returned error code */
    VolumeTable		*v;			/* pointer an entry in volume table */
    PageID			pid;		/* a page identifier */
    PageType		*unPage;	/* pointer to a buffer page */
    Unique			*unique;	/* pointer to a unique number */
    Two             idx;
    Two             k, last;    /* last unique element */
    Four            i;
	Four            j;          /* loop index */
    Four			count;		/* number of unique numbers */


    TR_PRINT(TR_RDSM, TR1, ("RDsM_P_Unique(handle, volNo=%lD)", volNo));


    /*
     * get the corresponding volume table entry via searching the VolTable
     */
    for (idx = 0; idx < MAXNUMOFVOLS; idx++) {
        if (RDSM_PER_THREAD_DS(handle).volTable[idx].volNo == volNo) break; 
    }
    if (idx >= MAXNUMOFVOLS) ERR(handle, eVOLNOTMOUNTED_RDSM);


    v = &(RDSM_PER_THREAD_DS(handle).volTable[idx]); 


    /*
     *  For each devices in given volume, print unique numbers
     */
    for (i = 0; i < v->numDevices; i++) {

        /*@ unique count */
        count = 0;

        /* initialize page of the unique numbers */
        pid = DEVINFO_ARRAY(v->devInfo)[i].uniqNumPageId;

        /*@ for all the unique number pages */
        for (j = 0; j < DEVINFO_ARRAY(v->devInfo)[i].uniqNumSize; j++, pid.pageNo++) {

            /*@ allocate a page for volume information management */
            e = BfM_GetTrain(handle, &pid, (char **) &unPage, PAGE_BUF);
            if (e < eNOERROR) ERR(handle, e); 

            /* initialize a unique number page */
            unique = (Unique *) unPage;

            /* if this is the last of the unique number page */
            if (j == DEVINFO_ARRAY(v->devInfo)[i].uniqNumSize-1)
                last = ((v->numOfExts*v->sizeOfExt-1)/UNIQUEPARTITIONSIZE+1)%NUMUNIQUESPERPAGE;
            else
                last = NUMUNIQUESPERPAGE;

            for (k = 0; k < last; k++, count++) {

                printf("%ldth partition's value of %ldth device: %ld\n", count, i, unique[k]);

            } /* end for */

            /*@ free this page */
            e = BfM_FreeTrain(handle, &pid, PAGE_BUF);
            if (e < eNOERROR) ERR(handle, e);

        } /* end for j */

    } /* end for i */


    return(eNOERROR);

} /* RDsM_P_Unique() */
