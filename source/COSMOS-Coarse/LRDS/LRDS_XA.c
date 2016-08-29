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
 * Module: LRDS_XA.c
 *
 * Description:
 *  LRDS level implementation of X/OPEN XA Interface 
 *  
 * Exports:
 *  
 *  
 *
 * Returns:
 *  Error code
 *   some erros caused by function calls
 *
 */

#include <stdlib.h> 	 /* for malloc & free */
#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "Util.h"
#include "SM_Internal.h" 
#include "LRDS.h"
#include "LRDS_XA.h" 
#include "perThreadDS.h"
#include "perProcessDS.h"

/*@================================
 * Four LRDS_XA_Open()
 *================================*/
/*
 * Function: Four LRDS_XA_Open(char* xa_info, int rmid, long flags)
 *
 * Description:
 *
 * Return values:
 *  Error codes
 *    some errors cased by function calls
 */

Four LRDS_XA_Open(
    Four 		handle,		       /* IN a handle for multi threading */
    Four 		numDevices,            /* IN # of devices in the volume to be mounted */
    char 		**devNames,            /* IN devices' name in the volume to be mounted */
    Four 		*volId,                /* OUT mounted volume's volume id */
    int 		rmid,
    long 		flags)
{

    Four e;                     /* error code */

    /***********************/
    /* I. parameter check */
    /***********************/

    if (numDevices <= 0) ERR(handle, eBADPARAMETER);
    if (devNames == NULL) ERR(handle, eBADPARAMETER);
    if (volId == NULL) ERR(handle, eBADPARAMETER);

    if (flags != LRDS_XA_TMNOFLAGS) return (eXAER_INVAL_LRDS_XA);

    /**************************/
    /* II. initialize COSMOS */
    /**************************/
	
    /* Initialize the global variables for LRDS_XA */
    LRDS_XA_SCANSTATUS(handle)   = LRDS_XA_SCANENDED;
    LRDS_XA_PREPAREDNUM(handle)  = 0;
    LRDS_XA_PREPAREDLIST(handle) = NULL;

    /* Initialize the LRDS */
    e = LRDS_Init(handle);
    if ( e != eNOERROR) return (eXAER_RMERR_LRDS_XA);

    /* Mount the volume. */
    e = LRDS_Mount(handle, numDevices, devNames, &LRDS_XA_VOLID(handle));
    if ( e != eNOERROR) return (eXAER_RMERR_LRDS_XA);

    /* set return variable */
    *volId = LRDS_XA_VOLID(handle); 


    return(eNOERROR);


} /* LRDS_XA_Open() */


/* role moved to XA Thin Layer */
/*
 * Function: Four lrds_XA_GetInitInfo(char* xa_info, int rmid, long flags)
 *
 * Return values:
 *  Error codes
 *    some errors cased by function calls
 */

Four lrds_XA_GetInitInfo(
    Four handle,
    char		*openString, 
    Four		*numDevices, 
    char		***devNames)
{

    /* openstring format
       COSMOS_XA+{required_fields}

       required_fields :

          NumDevices=n
          DevNames=string;string;string;...

       example : 

          COSMOS_XA+NumDevices=2+DevName=/dev/rdsk/c0t1d0s1/;/dev/rdsk/c0t1d0s1/
    */


    char  buffer[LRDS_XA_MAXOPENSTRINGLEN];
    char* seperator = "+=;";
    char* strPtr;
    Four  i;


    /* check parameter */ 
    if ( strlen(openString) > LRDS_XA_MAXOPENSTRINGLEN ) 
          return (eINVALIDOPENSTRING_LRDS_XA);

    /* get information */
    strPtr = strtok(openString, seperator);

    /* check header */
    if ( strcmp(strPtr, LRDS_XA_OPENSTRINGHEADER ) != 0 ) {
          return (eINVALIDOPENSTRING_LRDS_XA);
    }

    while( (strPtr = strtok(NULL, seperator)) != NULL ) {

        /* get numDevices */
        if ( strcmp( strPtr, "NumDevices" ) == 0 ) {

            strPtr = strtok(NULL, seperator);
            *numDevices = atol(strPtr);

        /* get devNames */
        } else if ( strcmp( strPtr, "DevNames" ) == 0 ) {

            if (*numDevices == 0) return (eINVALIDOPENSTRING_LRDS_XA);

            *devNames = (char**)(malloc(sizeof(char*) * (*numDevices)));

            for(i = 0; i < *numDevices; i++) {

                strPtr = strtok(NULL, seperator);
                if (strPtr == NULL) return (eINVALIDOPENSTRING_LRDS_XA);

                (*devNames)[i] = (char*) malloc(strlen(strPtr) * sizeof(char) + 1);
                strcpy( (*devNames)[i], strPtr);

            }


        } else {

            return (eINVALIDOPENSTRING_LRDS_XA);

        } 

    }

    /* result check */

    if ( numDevices == 0 || *devNames == NULL)
                  return (eINVALIDOPENSTRING_LRDS_XA);


    return(eNOERROR);

}


/*@================================
 * Four LRDS_XA_Close()
 *================================*/
/*
 * Function: Four LRDS_XA_Close(char *xa_info, int rmid, long flags)
 *
 * Description:
 *
 * Return values:
 *  Error codes
 *    some errors cased by function calls
 */

Four LRDS_XA_Close(
    Four handle,
   char *xa_info, 
   int rmid, 
   long flags)
{

    Four e;                     /* error code */


    /**********************/
    /* I. parameter check */
    /**********************/

    if (xa_info == NULL) return (eXAER_INVAL_LRDS_XA);

    if (rmid < 0) return (eXAER_INVAL_LRDS_XA);

    if (flags != LRDS_XA_TMNOFLAGS) return (eXAER_INVAL_LRDS_XA);


    /***********************/
    /* II. finalize COSMOS */
    /***********************/

    e = LRDS_Dismount(handle, LRDS_XA_VOLID(handle));
    if ( e != eNOERROR) return (eXAER_RMERR_LRDS_XA);

    e = LRDS_Final(handle);
    if ( e != eNOERROR) return (eXAER_RMERR_LRDS_XA);

    return(eNOERROR);


} /* LRDS_XA_Close() */





/*@================================
 * Four LRDS_XA_Start()
 *================================*/
/*
 * Function: Four LRDS_XA_Start(LRDS_XA_XID *xid, int rmid, long flags)
 *
 * Description:
 *
 * Return values:
 *  Error codes
 *    some errors cased by function calls
 */

Four LRDS_XA_Start(
    Four handle,
   LRDS_XA_XID *xid, 
   int rmid, 
   long flags)
{

    Four e;                     /* error code */
    XactID xactId;              /* local transaction ID */
    GlobalXactID globalXactId;  /* global transaction ID */

    /**********************/
    /* I. parameter check */
    /**********************/

    if (xid == NULL) return (eXAER_INVAL_LRDS_XA);

    if (rmid < 0) return (eXAER_INVAL_LRDS_XA);

    if (flags != LRDS_XA_TMNOFLAGS) return (eXAER_INVAL_LRDS_XA);


    /********************************/
    /* II. start global transaction */
    /********************************/

    /* begin local transaction */
    e = SM_BeginTransaction(handle, &xactId, X_RR_RR);
    if ( e != eNOERROR) return (eXAER_RMERR_LRDS_XA); 

    /* associate global transaction with local transaction */

    memset(&globalXactId, 0, MAX_GLOBAL_XACTID_LEN);
    memcpy(&globalXactId, xid, sizeof(LRDS_XA_XID)); 

    e = SM_EnterTwoPhaseCommit(handle, &xactId, &globalXactId);

    /* global transaction ID is duplicated */ 
    if ( e == eDUPLICATEDGTID_SM ) return (eDUPLICATEDGTID_LRDS_XA);

    /* other error occur */ 
    if ( e != eNOERROR) return (eXAER_RMERR_LRDS_XA); 
 

    return(eNOERROR);

} /* LRDS_XA_Start() */



/*@================================
 * Four LRDS_XA_End()
 *================================*/
/*
 * Function: Four LRDS_XA_End(LRDS_XA_XID *xid, int rmid, long flags)
 *
 * Description:
 *
 * Return values:
 *  Error codes
 *    some errors cased by function calls
 */

Four LRDS_XA_End(
    Four handle,
    LRDS_XA_XID *xid, 
    int rmid, 
    long flags)
{

    Four e;                     /* error code */


    /**********************/
    /* I. parameter check */
    /**********************/

    if (xid == NULL) return (eXAER_INVAL_LRDS_XA);

    if (rmid < 0) return (eXAER_INVAL_LRDS_XA);

    if (flags != LRDS_XA_TMSUCCESS) return (eXAER_INVAL_LRDS_XA);


    /******************************/
    /* II. end COSMOS transaction */
    /******************************/

    /* Do nothing in COSMOS */


    return(eNOERROR);

} /* LRDS_XA_End() */



/*@================================
 * Four LRDS_XA_Prepare()
 *================================*/
/*
 * Function: Four LRDS_XA_Prepare(LRDS_XA_XID *xid , int rmid, long flags)
 *
 * Description:
 *
 * Return values:
 *  Error codes
 *    some errors cased by function calls
 */

Four LRDS_XA_Prepare(
    Four handle,
    LRDS_XA_XID *xid , 
    int rmid, 
    long flags)
{

    Four e;                     /* error code */
    XactID xactId;              /* local transaction ID */
    GlobalXactID globalXactId;  /* global transaction ID */

    Boolean flag;               /* TRUE if read-only; FALSE otherwis */


    /**********************/
    /* I. parameter check */
    /**********************/

    if (xid == NULL) return (eXAER_INVAL_LRDS_XA);

    if (rmid < 0) return (eXAER_INVAL_LRDS_XA);

    if (flags != LRDS_XA_TMNOFLAGS) return (eXAER_INVAL_LRDS_XA);


    /**********************************/
    /* II. prepare COSMOS transaction */
    /**********************************/

    /* associate global transaction with local transaction */
    memset(&globalXactId, 0, MAX_GLOBAL_XACTID_LEN);
    memcpy(&globalXactId, xid, sizeof(LRDS_XA_XID));

    e = SM_RecoverTwoPhaseCommit(handle, &globalXactId, &xactId);
    if ( e != eNOERROR) {

        return (eXAER_RMERR_LRDS_XA);

    }

    e =  SM_PrepareTransaction(handle, &xactId);
    if ( e != eNOERROR) {

        return (eXAER_RMERR_LRDS_XA);

    }


    /***************************************/
    /* III. check if read-only transaction */
    /***************************************/

    /************************/
    /* IV. normally return  */
    /************************/

    return(eNOERROR);


} /* LRDS_XA_Prepare() */



/*@================================
 * Four LRDS_XA_Commit()
 *================================*/
/*
 * Function: Four LRDS_XA_Commit(LRDS_XA_XID *xid, int rmid, long flags)
 *
 * Description:
 *
 * Return values:
 *  Error codes
 *    some errors cased by function calls
 */

Four LRDS_XA_Commit(
    Four handle,
    LRDS_XA_XID *xid, 
    int rmid, 
    long flags)
{

    Four e;                     /* error code */
    XactID xactId;              /* local transaction ID */
    GlobalXactID globalXactId;  /* global transaction ID */


    /**********************/
    /* I. parameter check */
    /**********************/

    if (xid == NULL) return (eXAER_INVAL_LRDS_XA);  

    if (rmid < 0) return (eXAER_INVAL_LRDS_XA);

    if (flags != LRDS_XA_TMNOFLAGS) return (eXAER_INVAL_LRDS_XA);


    /*********************************/
    /* II. commit COSMOS transaction */
    /*********************************/

    /* associate global transaction with local transaction */
    memset(&globalXactId, 0, MAX_GLOBAL_XACTID_LEN);
    memcpy(&globalXactId, xid, sizeof(LRDS_XA_XID));

    e = SM_RecoverTwoPhaseCommit(handle, &globalXactId, &xactId);
    if ( e != eNOERROR) {

        return (eXAER_RMERR_LRDS_XA);

    }
    
    e =  SM_CommitTransaction(handle, &xactId);
    if ( e != eNOERROR) {

        /* TTT : rollback 처리 필요 */
        /* TTT : It needs a roll back management */
        return (eXAER_RMERR_LRDS_XA);

    }

    return(eNOERROR);

} /* LRDS_XA_Commit() */



/*@================================
 * Four LRDS_XA_Rollback()
 *================================*/
/*
 * Function: Four LRDS_XA_Rollback(LRDS_XA_XID* xid, int rmid, long flags)
 *
 * Description:
 *
 * Return values:
 *  Error codes
 *    some errors cased by function calls
 */

Four LRDS_XA_Rollback(
    Four handle,
    LRDS_XA_XID* xid, 
    int rmid, 
    long flags)
{

    Four e;                     /* error code */
    XactID xactId;              /* local transaction ID */
    GlobalXactID globalXactId;  /* global transaction ID */

    /**********************/
    /* I. parameter check */
    /**********************/

    if (xid == NULL) return (eXAER_INVAL_LRDS_XA);

    if (rmid < 0) return (eXAER_INVAL_LRDS_XA);

    if (flags != LRDS_XA_TMNOFLAGS) return (eXAER_INVAL_LRDS_XA);


    /*********************************/
    /* II. commit COSMOS transaction */
    /*********************************/

    /* associate global transaction with local transaction */
    memset(&globalXactId, 0, MAX_GLOBAL_XACTID_LEN);
    memcpy(&globalXactId, xid, sizeof(LRDS_XA_XID));

    e = SM_RecoverTwoPhaseCommit(handle, &globalXactId, &xactId);
    if ( e != eNOERROR) {

        return (eXAER_RMERR_LRDS_XA);

    }

    e =  SM_AbortTransaction(handle, &xactId);
    if ( e != eNOERROR) {

        return (eXAER_RMERR_LRDS_XA);

    }


    return(eNOERROR);

} /* LRDS_XA_Rollback() */

 
/*@================================
 * Four LRDS_XA_Recover()
 *================================*/
/*
 * Function: Four LRDS_XA_Recover(LRDS_XA_XID *xids, long count, int rmid, long flags)
 *
 * Description:
 *
 * Return values:
 *  Error codes
 *    some errors cased by function calls
 */

Four LRDS_XA_Recover(
    Four handle,
    LRDS_XA_XID *xids, 
    long count, 
    int rmid, 
    long flags)
{

    Four e;                       /* error code */
    Four i, j;
    GlobalXactID* globalXactID;  


    /***********************/
    /* I. parameter check */
    /***********************/
 
    if (xids == NULL) return (eXAER_INVAL_LRDS_XA);

    if (count <= 0) return (eXAER_INVAL_LRDS_XA);

    if (rmid < 0) return (eXAER_INVAL_LRDS_XA);

    if (flags != LRDS_XA_TMNOFLAGS && flags != LRDS_XA_TMSTARTRSCAN && flags != LRDS_XA_TMENDRSCAN &&
	flags != (long)(LRDS_XA_TMSTARTRSCAN & LRDS_XA_TMENDRSCAN)) 
        return (eXAER_INVAL_LRDS_XA);


    /********************/
    /* II. status check */
    /********************/

    /* scan status */

    if (flags == LRDS_XA_TMENDRSCAN && flags == LRDS_XA_TMNOFLAGS)
	if (LRDS_XA_SCANSTATUS(handle) == LRDS_XA_SCANENDED) 
	    return (eXAER_INVAL_LRDS_XA);

    if (flags == LRDS_XA_TMSTARTRSCAN || flags == (long)(LRDS_XA_TMSTARTRSCAN & LRDS_XA_TMENDRSCAN))
	if (LRDS_XA_SCANSTATUS(handle) == LRDS_XA_SCANSTARTED) 
            return (eXAER_INVAL_LRDS_XA);


    /*********************************************/
    /* III. recover prepared transaction id list */
    /*********************************************/

    /* start new scan */
    if (flags == LRDS_XA_TMSTARTRSCAN) {

        /* get the number of prepared transaction */
        e = SM_GetNumberOfPreparedTransactions(handle, &LRDS_XA_PREPAREDNUM(handle));
        if (e != eNOERROR) return (eXAER_RMERR_LRDS_XA);

        if (LRDS_XA_PREPAREDNUM(handle) > 0) {

            LRDS_XA_PREPAREDLIST(handle) = (LRDS_XA_XID*) ( malloc(sizeof(LRDS_XA_XID) * LRDS_XA_PREPAREDNUM(handle)) ); 
            globalXactID = (GlobalXactID*) ( malloc(sizeof(GlobalXactID) * LRDS_XA_PREPAREDNUM(handle)) );

            /* get all the transaction id of prepared transaction */
            e =  SM_GetPreparedTransactions(handle, LRDS_XA_PREPAREDNUM(handle), globalXactID);
            if (e != eNOERROR) return (eXAER_RMERR_LRDS_XA);

            for (i = 0; i < LRDS_XA_PREPAREDNUM(handle); i++) {
               memcpy(&(LRDS_XA_PREPAREDLIST(handle)[i]), &(globalXactID[i]), sizeof(LRDS_XA_XID));
            }


            /* return the result */
            if (count > LRDS_XA_PREPAREDNUM(handle)) {

                for(i = 0; i < LRDS_XA_PREPAREDNUM(handle); i++) {
                    memcpy(&(xids[i]), &(LRDS_XA_PREPAREDLIST(handle)[i]), sizeof(LRDS_XA_XID));
                }

                return LRDS_XA_PREPAREDNUM(handle);

            } else {

                for(i = 0; i < count; i++) {
                    memcpy(&(xids[i]), &(LRDS_XA_PREPAREDLIST(handle)[i]), sizeof(LRDS_XA_XID));
                }

                LRDS_XA_CURRENTPOS(handle) = i;

                return count;

            }

        } else {

            return LRDS_XA_PREPAREDNUM(handle);

	}

    /* continue current scan */
    } else if (flags == LRDS_XA_TMNOFLAGS) {

        /* return the result */
        if (count > LRDS_XA_PREPAREDNUM(handle) - LRDS_XA_CURRENTPOS(handle)) {

            j = 0;
            for(i = LRDS_XA_CURRENTPOS(handle); i < LRDS_XA_PREPAREDNUM(handle); i++) {
                memcpy(&(xids[j]), &(LRDS_XA_PREPAREDLIST(handle)[i]), sizeof(LRDS_XA_XID)); 
                j++;
            } 

            return LRDS_XA_PREPAREDNUM(handle) - LRDS_XA_CURRENTPOS(handle);

        } else {

            j = 0;
            for(i = LRDS_XA_CURRENTPOS(handle); i < LRDS_XA_CURRENTPOS(handle) + count; i++) {
                memcpy(&(xids[j]), &(LRDS_XA_PREPAREDLIST(handle)[i]), sizeof(LRDS_XA_XID)); 
                j++;
            } 
      
            LRDS_XA_CURRENTPOS(handle) = i; 
 
            return count;

        }

    /* end current scan */
    } else if (flags == LRDS_XA_TMENDRSCAN) {

        /* return the result */
        if (LRDS_XA_CURRENTPOS(handle) == LRDS_XA_PREPAREDNUM(handle)) {

            if (count > LRDS_XA_PREPAREDNUM(handle) - LRDS_XA_CURRENTPOS(handle)) {

                j = 0;
                for(i = LRDS_XA_CURRENTPOS(handle); i < LRDS_XA_PREPAREDNUM(handle); i++) {
                    memcpy(&(xids[j]), &(LRDS_XA_PREPAREDLIST(handle)[i]), sizeof(LRDS_XA_XID));
                    j++;
                }

                /* free data structure */
                free(LRDS_XA_PREPAREDLIST(handle)); 

                return LRDS_XA_PREPAREDNUM(handle) - LRDS_XA_CURRENTPOS(handle);

            } else {

                j = 0;
                for(i = LRDS_XA_CURRENTPOS(handle); i < LRDS_XA_CURRENTPOS(handle) + count; i++) {
                    memcpy(&(xids[j]), &(LRDS_XA_PREPAREDLIST(handle)[i]), sizeof(LRDS_XA_XID));
                    j++;
                }

                LRDS_XA_CURRENTPOS(handle) = i;

                /* free data structure */
                free(LRDS_XA_PREPAREDLIST(handle)); 

                return count;

            }

        }

    /* start new scan and end scan */
    } else if (flags == (long)(LRDS_XA_TMSTARTRSCAN & LRDS_XA_TMENDRSCAN)) {

        /* get the number of prepared transaction */
        e = SM_GetNumberOfPreparedTransactions(handle, &LRDS_XA_PREPAREDNUM(handle));
        if (e != eNOERROR) return (eXAER_RMERR_LRDS_XA);

        if (LRDS_XA_PREPAREDNUM(handle) > 0) {

            LRDS_XA_PREPAREDLIST(handle) = (LRDS_XA_XID*) ( malloc(sizeof(LRDS_XA_XID) * LRDS_XA_PREPAREDNUM(handle)) );
            globalXactID = (GlobalXactID*) ( malloc(sizeof(GlobalXactID) * LRDS_XA_PREPAREDNUM(handle)) );

            /* scan status transition */ 
            LRDS_XA_SCANSTATUS(handle) = LRDS_XA_SCANSTARTED;

            /* get all the transaction id of prepared transaction */
            e =  SM_GetPreparedTransactions(handle, LRDS_XA_PREPAREDNUM(handle), globalXactID);
            if (e != eNOERROR) return (eXAER_RMERR_LRDS_XA);

            for (i = 0; i < LRDS_XA_PREPAREDNUM(handle); i++) {
               memcpy(&(LRDS_XA_PREPAREDLIST(handle)[i]), &(globalXactID[i]), sizeof(LRDS_XA_XID));
            }


            /* return the result */
            if (count > LRDS_XA_PREPAREDNUM(handle)) {

                for(i = 0; i < LRDS_XA_PREPAREDNUM(handle); i++) {
                    memcpy(&(xids[i]), &(LRDS_XA_PREPAREDLIST(handle)[i]), sizeof(LRDS_XA_XID));
                }

                /* free data structure */
                free(globalXactID); 
                free(LRDS_XA_PREPAREDLIST(handle)); 

                return LRDS_XA_PREPAREDNUM(handle);

            } else {

                for(i = 0; i < count; i++) {
                    memcpy(&(xids[i]), &(LRDS_XA_PREPAREDLIST(handle)[i]), sizeof(LRDS_XA_XID));
                }

                /* free data structure */
                free(globalXactID); 
                free(LRDS_XA_PREPAREDLIST(handle)); 

                /* scan status transition */ 
                LRDS_XA_SCANSTATUS(handle) = LRDS_XA_SCANENDED;

                LRDS_XA_CURRENTPOS(handle) = i;

                return count;

            }

        } else {

            return LRDS_XA_PREPAREDNUM(handle);

        }

    }

} /* LRDS_XA_Recover() */


/*@================================
 * Four LRDS_XA_Forget()
 *================================*/
/*
 * Function: Four LRDS_XA_Forget(LRDS_XA_XID *xid, int rmid, long flags)
 *
 * Description:
 *
 * Return values:
 *  Error codes
 *    some errors cased by function calls
 */

Four LRDS_XA_Forget(
    Four handle,
    LRDS_XA_XID *xid, 
    int rmid, 
    long flags)
{

    Four e;                     /* error code */
    XactID xactId;              /* local transaction ID */
    GlobalXactID globalXactId;  /* global transaction ID */


    /**********************/
    /* I. parameter check */
    /**********************/

    if (xid == NULL) return (eXAER_INVAL_LRDS_XA);

    if (rmid < 0) return (eXAER_INVAL_LRDS_XA);

    if (flags != LRDS_XA_TMNOFLAGS) return (eXAER_INVAL_LRDS_XA);


    /*********************************/
    /* II. forget COSMOS transaction */
    /*********************************/

    /* associate global transaction with local transaction */
    memset(&globalXactId, 0, MAX_GLOBAL_XACTID_LEN);
    memcpy(&globalXactId, xid, sizeof(LRDS_XA_XID));

    e = SM_RecoverTwoPhaseCommit(handle, &globalXactId, &xactId);
    if ( e != eNOERROR) {

        return (eXAER_RMERR_LRDS_XA);

    }

    e =  SM_AbortTransaction(handle, &xactId);
    if ( e != eNOERROR) {

        return (eXAER_RMERR_LRDS_XA);

    }

    return(eNOERROR);

} /* LRDS_XA_Forget() */



/*@================================
 * Four LRDS_XA_Complete()
 *================================*/
/*
 * Function: Four LRDS_XA_Complete(int* handle, int *retval, int rmid, long flags)
 *
 * Return values:
 *  Error codes
 *    some errors cased by function calls
 */

Four LRDS_XA_Complete(
    Four handle,
    int* xa_handle, 
    int *retval, 
    int rmid, 
    long flags)
{

    Four e;         /* error code */

    /***********************/
    /* I. parameter check */
    /***********************/

    if (xa_handle == NULL) return (eXAER_INVAL_LRDS_XA); 

    if (retval == NULL) return (eXAER_INVAL_LRDS_XA);

    if (rmid < 0) return (eXAER_INVAL_LRDS_XA);

    if (flags != LRDS_XA_TMNOFLAGS) return (eXAER_INVAL_LRDS_XA);


    /********************/
    /* II. return value */
    /********************/

    return(eXAER_PROTO_LRDS_XA);


} /* LRDS_XA_Complete() */


/*@================================
 * Four LRDS_XA_AxReg()
 *================================*/
/*
 * Function: Four LRDS_XA_AxReg(int rmid, LRDS_XA_XID* xid, long flags)
 *
 * Return values:
 *  Error codes
 *    some errors cased by function calls
 */

Four LRDS_XA_AxReg(
    Four handle,
    int rmid, 
    LRDS_XA_XID *xid,
    long flags)
{

    Four e;                     /* error code */

    /***********************/
    /* I. parameter check */
    /***********************/

    if (rmid < 0) return (eXAER_INVAL_LRDS_XA);

    if (xid == NULL) return (eXAER_INVAL_LRDS_XA);

    if (flags != LRDS_XA_TMNOFLAGS) return (eXAER_INVAL_LRDS_XA);


    /********************/
    /* II. return value */
    /********************/

    return(eXAER_PROTO_LRDS_XA);


} /* LRDS_XA_AxReg() */


/*@================================
 * Four LRDS_XA_AxUnreg()
 *================================*/
/*
 * Function: Four LRDS_XA_AxUnreg(int rmid, long flags)
 *
 * Return values:
 *  Error codes
 *    some errors cased by function calls
 */

Four LRDS_XA_AxUnreg(
    Four handle,
    int rmid, 
    long flags)
{

    Four e;                     /* error code */

    /***********************/
    /* I. parameter check */
    /***********************/

    if (rmid < 0) return (eXAER_INVAL_LRDS_XA);

    if (flags != LRDS_XA_TMNOFLAGS) return (eXAER_INVAL_LRDS_XA);


    /********************/
    /* II. return value */
    /********************/

    return(eXAER_PROTO_LRDS_XA);


} /* LRDS_XA_AxUnreg() */



