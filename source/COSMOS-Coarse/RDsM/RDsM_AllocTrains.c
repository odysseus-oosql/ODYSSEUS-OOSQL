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
 * Module: RDsM_AllocTrains.c
 *
 * Description:
 *  allocate trains and return their train identifiers in "TrainIds".
 *  If NearPID is specified, allocate trains close to it if possible.
 *
 * Exports:
 *  Four RDsM_AllocTrains(Four, Four, PageID*, Two, Four, Two, PageID*)
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "RDsM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * RDsM_AllocTrains()
 *================================*/
/*
 * Function: Four RDsM_AllocTrains(Four, Four, PageID*, Two, Four, Two, PageID*)
 *
 * Description:
 *  allocate trains and return their train identifiers in "TrainIds".
 *  If NearPID is specified, allocate trains close to it if possible.
 *
 * Returns:
 *  error code
 *    eINVALIDFIRSTEXTRDsM - invalid first extent number
 *    eNULLPIDPTRRDsM - null pointer to page identifier
 *    eINVALIDEFFRDsM - invalid extent fill factor
 *    eVOLNOTMOUNTEDRDsM - volume not mounted
 *
 * Side Effects:
 *  additional extents are allocated if necessary
 */
Four RDsM_AllocTrains(
    Four handle,
    Four volNo,			/* IN volume number in question */
    Four firstExt,		/* IN first extent number of the segment */
    PageID *nearPID,		/* IN allocate as near this pid as possible */
    Two  eff,			/* IN number of pages in an extent to keep filled */
    Four numOfTrains,		/* IN number of trains to be allocated */
    Two  sizeOfTrain,		/* IN size of a train to be allocated */
    PageID *trainIDs)		/* OUT pointer to train identifiers(first page identifier of each train) for the allocated trains */
{
    VolumeTable	*v;		/* pointer to an entry in shmPtr->volTable */
    Two 	idx;		/* loop index */
    Four	i;		/* loop index */
    Four	e;		/* returned error code */
    Two 	curEff;         /* extent fill factor of the current extent */
    Four	numofallocs;	/* number of trains allocated */
    Four	newext;		/* extent number to be allocated */
    Four	extnum;		/* extent number */
    Four	prev;		/* extent number previous to extnum: not used in this procedure */
    Four	next;		/* extent number next to extnum */


    TR_PRINT(TR_RDSM, TR1,
             ("RDsM_AllocTrains(handle, volNo=%lD, firstExt=%lD, nearPID=%P, eff=%lD, numOfTrains=%lD, sizeOfTrain=%lD, trainIDs=%P)",
	      volNo, firstExt, nearPID, eff, numOfTrains, sizeOfTrain, trainIDs));


    /* get the corresponding volume table entry via searching the shmPtr->volTable */
    for (idx = 0; idx < MAXNUMOFVOLS; idx++) {
        if (RDSM_PER_THREAD_DS(handle).volTable[idx].volNo == volNo) break; 
    }
    if (idx >= MAXNUMOFVOLS) ERR(handle, eVOLNOTMOUNTED_RDSM);

    v = &(RDSM_PER_THREAD_DS(handle).volTable[idx]);

    /*
     *	NOTE:
     *	conversion of the extent fill factor in the number of pages
     *	the unit of the extent fill factor is the number of pages in RDsM
     *	while the unit of the eff, input parameter is % in the upper layer
     */
    eff = (eff * v->sizeOfExt) / 100;

    /*@ check input parameters */
    if (firstExt < NIL ||firstExt >= v->numOfExts) ERR(handle, eINVALIDFIRSTEXT_RDSM);
    if (trainIDs == NULL) ERR(handle, eBADPARAMETER);
    if (eff < 0 || eff > v->sizeOfExt) ERR(handle, eINVALIDEFF_RDSM);
    if (sizeOfTrain != PAGESIZE2 && sizeOfTrain != TRAINSIZE2) ERR(handle, eINVALIDTRAINSIZE_RDSM);

    /*
     *  I. if NearPID is given, allocate trains in the extent that the NearPID belongs to
     */
    if (nearPID != NULL) {

	/* validate NearPID */
	if (nearPID->volNo != volNo || nearPID->pageNo < 0 || nearPID->pageNo >= v->numOfExts*v->sizeOfExt)
	    ERR(handle, eINVALIDPID_RDSM);

	/*@ check the extent fill factor of the extent */
	e = RDsM_check_eff(handle, v, nearPID->pageNo/v->sizeOfExt, &curEff);
	if (e < eNOERROR) ERR(handle, e); 

	/* if there are sufficient pages in the extent in which the NearPID is included */
	if (curEff+sizeOfTrain <= eff) {

	    /* allocate those */
	    e = RDsM_alloc_trains_in_ext(handle, v, eff-curEff, nearPID->pageNo/v->sizeOfExt, trainIDs, sizeOfTrain, numOfTrains, &numofallocs);
	    if (e < eNOERROR) ERR(handle, e); 

	    /* if more than one train are allocated */
	    if (numofallocs > 0) {

		/* forward the pointer to the array of the TrainIDs and decrement the NumOfTrains */
		numOfTrains -= numofallocs;
		trainIDs += numofallocs;

		/*@ updates a new extent fill factor in the corresponding extent entry */
		e = RDsM_change_eff(handle, v, nearPID->pageNo/v->sizeOfExt, curEff+(numofallocs*sizeOfTrain));
		if (e < eNOERROR) ERR(handle, e); 

	    }	/* end if */

	    /*@ if all trains are allocated, return */
	    if (numOfTrains == 0) return(eNOERROR);

	} /* end if */

    } /* end if */


    /*
     * II. if size of allocated train is too large, new extent allocation is required
     */
    if (eff <= numOfTrains*sizeOfTrain) {

	/* for necessary extents */
	for (i=numOfTrains/(eff/sizeOfTrain); i>0 ; i--) {

	    /* allocate an extent */
		e = RDsM_alloc_ext(handle, v, (nearPID != NULL) ? nearPID->pageNo/v->sizeOfExt : firstExt, &newext);
	    if (e < eNOERROR) ERR(handle, e); 

	    /* allocate all possible trains in this extent */
	    e = RDsM_alloc_trains_in_ext(handle, v, eff, newext, trainIDs, sizeOfTrain, numOfTrains, &numofallocs);
	    if (e < eNOERROR) ERR(handle, e); 

	    /*@
	     * decrement the NumOfTrains by numofallocs,
	     * increment the pointer to TrainIDs by numofallocs
	     */
	    numOfTrains -= numofallocs;
	    trainIDs += numofallocs;

	    /*@ updates a new extent fill factor in the corresponding extent entry */
	    e = RDsM_change_eff(handle, v, newext, numofallocs*sizeOfTrain);
	    if (e < eNOERROR) ERR(handle, e); 

	    /*@ if all trains are allocated, return */
	    if (numOfTrains == 0) return(eNOERROR);

	} /* end for */

    } /* end if */

    /*
     * III. allocate from next & previous extent
     */
    if (nearPID != NULL) {

    /* III-1. allocate trains from previous & next extent of nearPid */
	/* get the previous & next extent number */
	e = RDsM_get_prev_next_ext(handle, v, nearPID->pageNo/v->sizeOfExt, &prev, &next);
	if (e < eNOERROR) ERR(handle, e); 
    }
    else {

        /* III-2. check next extent of first extent */
        /* Note!! next extent of first extent is the newest allocated extent when nearPid == NULL */

	/* get the previous & next extent number */
	e = RDsM_get_prev_next_ext(handle, v, firstExt, &prev, &next);
	if (e < eNOERROR) ERR(handle, e);
    }

    /* first, allocate from next extent */
    if (next != NIL) {

        /*@ check the extent fill factor of the extent */
        e = RDsM_check_eff(handle, v, next, &curEff);
        if (e < eNOERROR) ERR(handle, e); 

        /* if there are sufficient pages in the extent in which the NearPID is included */
        if (curEff+sizeOfTrain <= eff) {

            /* allocate those */
            e = RDsM_alloc_trains_in_ext(handle, v, eff-curEff, next, trainIDs, sizeOfTrain, numOfTrains, &numofallocs);
            if (e < eNOERROR) ERR(handle, e); 

            /* if more than one train are allocated */
            if (numofallocs > 0) {

                /* forward the pointer to the array of the TrainIDs and decrement the NumOfTrains */
                numOfTrains -= numofallocs;
                trainIDs += numofallocs;

                /*@ updates a new extent fill factor in the corresponding extent entry */
                e = RDsM_change_eff(handle, v, next, curEff+(numofallocs*sizeOfTrain));
                if (e < eNOERROR) ERR(handle, e); 

            } /* end if */

            /*@ if all trains are allocated, return */
            if (numOfTrains == 0) return(eNOERROR);

        } /* end if */

    } /* end if */


    /* second, allocate from previous extent */
    if (prev != NIL) {

        /*@ check the extent fill factor of the extent */
        e = RDsM_check_eff(handle, v, prev, &curEff);
        if (e < eNOERROR) ERR(handle, e); 

        /* if there are sufficient pages in the extent in which the NearPID is included */
        if (curEff+sizeOfTrain <= eff) {

            /* allocate those */
            e = RDsM_alloc_trains_in_ext(handle, v, eff-curEff, prev, trainIDs, sizeOfTrain, numOfTrains, &numofallocs);
            if (e < eNOERROR) ERR(handle, e); 

            /* if more than one train are allocated */
            if (numofallocs > 0) {

                /* forward the pointer to the array of the TrainIDs and decrement the NumOfTrains */
                numOfTrains -= numofallocs;
                trainIDs += numofallocs;

                /*@ updates a new extent fill factor in the corresponding extent entry */
                e = RDsM_change_eff(handle, v, prev, curEff+(numofallocs*sizeOfTrain));
                if (e < eNOERROR) ERR(handle, e); 

            } /* end if */

            /*@ if all trains are allocated, return */
            if (numOfTrains == 0) return(eNOERROR);

        } /* end if */

    } /* end if */

    /*
     *  IV. allocate a new extent
     */
    e = RDsM_alloc_ext(handle, v, (nearPID != NULL) ? nearPID->pageNo/v->sizeOfExt : firstExt, &newext);
    if (e < eNOERROR) ERR(handle, e); 

    /*@ allocate trains in it */
    e = RDsM_alloc_trains_in_ext(handle, v, eff, newext, trainIDs, sizeOfTrain, numOfTrains, &numofallocs);
    if (e < eNOERROR) ERR(handle, e);

    /*@ update extent fill factor */
    e = RDsM_change_eff(handle, v, newext, numofallocs*sizeOfTrain);
    if (e < eNOERROR) ERR(handle, e); 


    return(eNOERROR);

} /* RDsM_AllocTrains() */
