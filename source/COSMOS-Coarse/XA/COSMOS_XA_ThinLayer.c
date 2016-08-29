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
 * Module: COSMOS_XA_ThinLayer.c
 *
 * Description:
 *  COSMOS level implementation of X/OPEN XA Interface 
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

#include <stdlib.h>	 /* for malloc & free */
#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util.h"
#include "SM_Internal.h" 
#include "LRDS.h"
#include "LRDS_XA.h" 
#include "COSMOS_XA_ThinLayer.h" 
#include "xa.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


/*@================================
 * int COSMOS_XA_Open()
 *================================*/
/*
 * Function: int COSMOS_XA_Open(char* xa_info, int rmid, long flags)
 *
 * Description:
 *
 * Return values:
 *  Error codes
 *    some errors cased by function calls
 */

int COSMOS_XA_Open(
    Four handle,
    char* xa_info, 
    int rmid,
    long flags)
{

    Four 	e;		/* error code */
    Four 	e1;		/* error code */
    int 	xaErrCode;	/* xa error code */ 

    Four 	numDevices;
    char** 	devNames;
    char 	logDevNames[COSMOS_XA_MAXOPENSTRINGLEN];
    Four 	volId;
    Four 	i;

    char*   name = "cosmosxa";
	Four    len;

    /****************************************/
    /* 0. initialization of global variable */
    /****************************************/

    /* per-process global variables */
    len = strlen(name);
    memcpy(&(XA_PER_PROCESS_DS.cosmosxa.name), name, len);
    XA_PER_PROCESS_DS.cosmosxa.name[len] 	 = '\0';
    XA_PER_PROCESS_DS.cosmosxa.flags 		 = TMNOMIGRATE;
    XA_PER_PROCESS_DS.cosmosxa.version 		 = 0;
    XA_PER_PROCESS_DS.cosmosxa.xa_open_entry 	 = COSMOS_XA_Open;
    XA_PER_PROCESS_DS.cosmosxa.xa_close_entry 	 = COSMOS_XA_Close;
    XA_PER_PROCESS_DS.cosmosxa.xa_start_entry 	 = COSMOS_XA_Start;
    XA_PER_PROCESS_DS.cosmosxa.xa_end_entry 	 = COSMOS_XA_End;
    XA_PER_PROCESS_DS.cosmosxa.xa_rollback_entry = COSMOS_XA_Rollback;
    XA_PER_PROCESS_DS.cosmosxa.xa_prepare_entry  = COSMOS_XA_Prepare;
    XA_PER_PROCESS_DS.cosmosxa.xa_commit_entry 	 = COSMOS_XA_Commit;
    XA_PER_PROCESS_DS.cosmosxa.xa_recover_entry  = COSMOS_XA_Recover;
    XA_PER_PROCESS_DS.cosmosxa.xa_forget_entry 	 = COSMOS_XA_Forget;
    XA_PER_PROCESS_DS.cosmosxa.xa_complete_entry = COSMOS_XA_Complete;
   
    /* per-thread global variables */
    XA_PER_THREAD_DS(handle).cosmos_xa_rmOpenStatus        = COSMOS_XA_UNINITIALIZED;
    XA_PER_THREAD_DS(handle).cosmos_xa_rmAssociationStatus = COSMOS_XA_NOTASSOCIATED;
    XA_PER_THREAD_DS(handle).cosmos_xa_rmTranStatus        = COSMOS_XA_NONTRANSACTION;
    XA_PER_THREAD_DS(handle).cosmos_xa_scanStatus          = COSMOS_XA_SCANENDED;
    XA_PER_THREAD_DS(handle).cosmos_xa_preparedNum         = 0;
    XA_PER_THREAD_DS(handle).cosmos_xa_preparedList 	   = NULL;


    /*******************/
    /* I. status check */
    /*******************/

    /* open status */

    /* repeated call : if it is redundent call, do nothing and return OK */
    if (COSMOS_XA_RMOPENSTATUS(handle) == COSMOS_XA_INITIALIZED) return (XA_OK); /* insert a handle */


    /***********************/
    /* II. parameter check  */
    /***********************/

    if (xa_info == NULL) return (XAER_INVAL);

    if (rmid < 0) return (XAER_INVAL);

    if (flags != TMNOFLAGS) return (XAER_INVAL);


    /**************************/
    /* III. call COSMOS XA API */
    /**************************/

    /* convert open string */
    e = cosmos_xa_GetInitInfo(handle, xa_info, &numDevices, &devNames, logDevNames);
    if ( e != eNOERROR) return (XAER_INVAL);


    e = LRDS_SetCfgParam(handle, "LOG_VOLUME_DEVICE_LIST", logDevNames);

    e = LRDS_XA_Open(handle, numDevices, devNames, &volId, rmid, flags);

    XA_PER_THREAD_DS(handle).cosmos_xa_volId = volId;

    /* free data structure */
    for( i = 0; i < numDevices; i++) {
        free(devNames[i]);
    }

    free (devNames);


    if (e != eNOERROR) {

        /* Error Code Converting (COSMOS error code to XA error code) */
        e1 = cosmos_xa_ErrorConvert(handle,  e, &xaErrCode );
        if ( e1 == eNOERROR ) 
            return (xaErrCode);

        return (XAER_RMERR);

    }

    /*************************/
    /* IV. status transition */
    /*************************/

    COSMOS_XA_RMOPENSTATUS(handle) = COSMOS_XA_INITIALIZED; /* insert a handle */


    return(XA_OK);


} /* COSMOS_XA_Open() */



/*
 * Function: Four cosmos_xa_GetInitInfo(char* openString, Four* numDevices, char*** devNames, char* logDevNames) 
 *
 * Return values:
 *  Error codes
 *    some errors cased by function calls
 */


Four cosmos_xa_GetInitInfo(
    Four handle,
    char* openString, 
    Four* numDevices, 
    char*** devNames,
    char* logDevNames)
{

    /* openstring format
       COSMOS_XA+{required_fields}

       required_fields :

          NumDevices=n
          DevNames=string;string;string;...
          LogDevNames=string;string;string;...

       example : 

          COSMOS_XA+NumDevices=2+DevNames=/dev/rdsk/c0t1d0s1/;/dev/rdsk/c0t1d0s1/+LogDevNames=/dev/rdsk/c0t1d0s2
    */


    char  buffer[COSMOS_XA_MAXOPENSTRINGLEN];
    char* seperator = "+=;";
    char* strPtr;
    Four  inputLen;
    Four  i;


    /* check parameter */ 
    if ( strlen(openString) > COSMOS_XA_MAXOPENSTRINGLEN ) 
          return (eINVALIDOPENSTRING_COSMOS_XA);

    /* get information */
    strPtr = strtok(openString, seperator);

    /* check header */
    if ( strcmp(strPtr, COSMOS_XA_OPENSTRINGHEADER ) != 0 ) {
          return (eINVALIDOPENSTRING_COSMOS_XA);
    }

    while( (strPtr = strtok(NULL, seperator)) != NULL ) {

        /* get numDevices */
        if ( strcmp( strPtr, "NumDevices" ) == 0 ) {

            strPtr = strtok(NULL, seperator);
            *numDevices = atol(strPtr);

        /* get devNames */
        } else if ( strcmp( strPtr, "DevNames" ) == 0 ) {

            if (*numDevices == 0) return (eINVALIDOPENSTRING_COSMOS_XA);

            *devNames = (char**)(malloc(sizeof(char*) * (*numDevices)));

            for(i = 0; i < *numDevices; i++) {

                strPtr = strtok(NULL, seperator);
                if (strPtr == NULL) return (eINVALIDOPENSTRING_COSMOS_XA);

                (*devNames)[i] = (char*) malloc(strlen(strPtr) * sizeof(char) + 1);
                strcpy( (*devNames)[i], strPtr);

            }

        /* the last value of open string */
        } else if ( strcmp( strPtr, "LogDevNames" ) == 0 ) {

            strPtr = strtok(NULL, seperator);
            if (strPtr == NULL) return (eINVALIDOPENSTRING_COSMOS_XA);

            strcpy(logDevNames, strPtr);

            while (1) {

                strPtr = strtok(NULL, seperator);
                if (strPtr == NULL) break;
                strcat(logDevNames, ";");
                strcat(logDevNames, strPtr);

            }


        } else {

            return (eINVALIDOPENSTRING_COSMOS_XA);

        } 

    }

    /* result check */

    if ( numDevices == 0 || *devNames == NULL ) {

        /* free data structure */

        for( i = 0; i < *numDevices; i++) {
            free(*(devNames[i]));
        }

        free (*devNames);

        return (eINVALIDOPENSTRING_COSMOS_XA);

    }

    return(eNOERROR);

}


/*
 * Function: Four cosmos_xa_ErrorConvert(Four cosmosErrCode, int* xaErrCode)
 *
 * Return values:
 *  Error codes
 *    some errors cased by function calls
 */

Four cosmos_xa_ErrorConvert(
    Four handle,
    Four cosmosErrCode, 
    int* xaErrCode) {

    switch (cosmosErrCode) {

        case eXA_RBROLLBACK_LRDS_XA :
            *xaErrCode = XA_RBROLLBACK;
            break; 

        case eXA_RBCOMMFAIL_LRDS_XA :
            *xaErrCode = XA_RBCOMMFAIL;
            break; 

        case eXA_RBDEADLOCK_LRDS_XA :
            *xaErrCode = XA_RBDEADLOCK;
            break; 

        case eXA_RBINTEGRITY_LRDS_XA :
            *xaErrCode = XA_RBINTEGRITY;
            break; 

        case eXA_RBOTHER_LRDS_XA :
            *xaErrCode = XA_RBOTHER;
            break; 

        case eXA_RBPROTO_LRDS_XA :
            *xaErrCode = XA_RBPROTO;
            break; 

        case eXA_RBTIMEOUT_LRDS_XA :
            *xaErrCode = XA_RBTIMEOUT;
            break; 

        case eXA_RBTRANSIENT_LRDS_XA :
            *xaErrCode = XA_RBTRANSIENT;
            break; 

        case eXA_RBEND_LRDS_XA :
            *xaErrCode = XA_RBEND;
            break; 

        case eXA_NOMIGRATE_LRDS_XA :
            *xaErrCode = XA_NOMIGRATE;
            break; 

        case eXA_HEURHAZ_LRDS_XA :
            *xaErrCode = XA_HEURHAZ;
            break; 

        case eXA_HEURCOM_LRDS_XA :
            *xaErrCode = XA_HEURCOM;
            break; 

        case eXA_HEURRB_LRDS_XA :
            *xaErrCode = XA_HEURRB;
            break; 

        case eXA_HEURMIX_LRDS_XA :
            *xaErrCode = XA_HEURMIX;
            break; 

        case eXA_RETRY_LRDS_XA :
            *xaErrCode = XA_RETRY;
            break; 

        case eXA_RDONLY_LRDS_XA :
            *xaErrCode = XA_RDONLY;
            break; 

        case eXAER_ASYNC_LRDS_XA :
            *xaErrCode = XAER_ASYNC;
            break; 

        case eXAER_RMERR_LRDS_XA :
            *xaErrCode = XAER_RMERR;
            break; 

        case eXAER_NOTA_LRDS_XA :
            *xaErrCode = XAER_NOTA;
            break; 

        case eXAER_INVAL_LRDS_XA :
            *xaErrCode = XAER_INVAL;
            break; 

        case eXAER_PROTO_LRDS_XA :
            *xaErrCode = XAER_PROTO;
            break; 

        case eXAER_RMFAIL_LRDS_XA :
            *xaErrCode = XAER_RMFAIL;
            break; 

        case eXAER_DUPID_LRDS_XA :
            *xaErrCode = XAER_DUPID;
            break; 

        case eXAER_OUTSIDE_LRDS_XA :
            *xaErrCode = XAER_OUTSIDE;
            break; 

        default :
            return eINVALIDERRORCODE_COSMOS_XA;

    }

    return eNOERROR;

}


/*@================================
 * int COSMOS_XA_Close()
 *================================*/
/*
 * Function: int COSMOS_XA_Close(char *xa_info, int rmid, long flags)
 *
 * Description:
 *
 * Return values:
 *  Error codes
 *    some errors cased by function calls
 */

int COSMOS_XA_Close(
    Four handle,
   char *xa_info, 
   int rmid, 
   long flags)
{

    Four e;                     /* error code */
    Four e1;                    /* error code */
    int xaErrCode;              /* xa error code */ 

    /*******************/
    /* 0. status check */
    /*******************/

    /* open status */
    /* repeated call : if it is redundent call, do nothing and return OK */
    if (COSMOS_XA_RMOPENSTATUS(handle) == COSMOS_XA_UNINITIALIZED) return (XA_OK); /* insert a handle */

    /* association status */
    if (COSMOS_XA_RMASSOCIATIONSTATUS(handle) != COSMOS_XA_NOTASSOCIATED) return (XAER_PROTO); /* insert a handle */

    /* transaction status */
    if (COSMOS_XA_RMTRANSTATUS(handle) == COSMOS_XA_ACTIVE) return (XAER_PROTO); /* insert a handle */


    /***********************/
    /* I. parameter check */
    /***********************/

    if (xa_info == NULL) return (XAER_INVAL);

    if (rmid < 0) return (XAER_INVAL);

    if (flags != TMNOFLAGS) return (XAER_INVAL);


    /**************************/
    /* II. call COSMOS XA API */
    /**************************/

    e = LRDS_XA_Close(handle, xa_info, rmid, flags);

    if (e != eNOERROR) {

        /* Error Code Converting (COSMOS error code to XA error code) */
        e1 = cosmos_xa_ErrorConvert(handle,  e, &xaErrCode );
        if ( e1 == eNOERROR ) 
            return (xaErrCode);

        return (XAER_RMERR);

    }

    /**************************/
    /* III. status transition */
    /**************************/

    COSMOS_XA_RMOPENSTATUS(handle) 	  = COSMOS_XA_UNINITIALIZED; 
    COSMOS_XA_RMASSOCIATIONSTATUS(handle) = COSMOS_XA_NOTASSOCIATED;
    COSMOS_XA_RMTRANSTATUS(handle)        = COSMOS_XA_NONTRANSACTION;


    return(XA_OK);


} /* COSMOS_XA_Close() */



/*@================================
 * int COSMOS_XA_Start()
 *================================*/
/*
 * Function: int COSMOS_XA_Start(XID *xid, int rmid, long flags)
 *
 * Description:
 *
 * Return values:
 *  Error codes
 *    some errors cased by function calls
 */

int COSMOS_XA_Start(
   Four handle,
   XID *xid, 
   int rmid, 
   long flags)
{

    Four e;                     /* error code */
    Four e1;                    /* error code */
    int xaErrCode;              /* xa error code */ 


    /*******************/
    /* 0. status check */
    /*******************/

    /* open status */
    if (COSMOS_XA_RMOPENSTATUS(handle) == COSMOS_XA_UNINITIALIZED) return (XAER_PROTO); /* insert a handle */

    /* association status */
    if (COSMOS_XA_RMASSOCIATIONSTATUS(handle) == COSMOS_XA_ASSOCIATED) return (XAER_PROTO); /* insert a handle */

    /* transaction status */
    if (COSMOS_XA_RMTRANSTATUS(handle) != COSMOS_XA_NONTRANSACTION && COSMOS_XA_RMTRANSTATUS(handle) != COSMOS_XA_IDLE) 
        return (XAER_PROTO);


    /***********************/
    /* I. parameter check */
    /***********************/

    if (xid == NULL) return (XAER_INVAL);

    if (rmid < 0) return (XAER_INVAL);

    if (flags != TMNOFLAGS) return (XAER_INVAL);


    /**************************/
    /* II. call COSMOS XA API */
    /**************************/

    e = LRDS_XA_Start(handle, (LRDS_XA_XID*) xid, rmid, flags);
    if (e != eNOERROR) {

        /* Error Code Converting (COSMOS error code to XA error code) */
        e1 = cosmos_xa_ErrorConvert(handle,  e, &xaErrCode );
        if ( e1 == eNOERROR ) 
            return (xaErrCode);

        return (XAER_RMERR);

    }

    /**************************/
    /* III. status transition */
    /**************************/

    COSMOS_XA_RMASSOCIATIONSTATUS(handle) = COSMOS_XA_ASSOCIATED;
    COSMOS_XA_RMTRANSTATUS(handle) 	  = COSMOS_XA_ACTIVE;


    return(XA_OK);


} /* COSMOS_XA_Start() */



/*@================================
 * int COSMOS_XA_End()
 *================================*/
/*
 * Function: int COSMOS_XA_End(XID *xid, int rmid, long flags)
 *
 * Description:
 *
 * Return values:
 *  Error codes
 *    some errors cased by function calls
 */

int COSMOS_XA_End(
    Four handle,
    XID *xid, 
    int rmid, 
    long flags)
{

    Four e;                     /* error code */
    Four e1;                    /* error code */
    int xaErrCode;              /* xa error code */ 


    /*******************/
    /* 0. status check */
    /*******************/

    /* open status */
    if (COSMOS_XA_RMOPENSTATUS(handle) != COSMOS_XA_INITIALIZED) return (XAER_PROTO); /* insert a handle */

    /* association status */
    if (COSMOS_XA_RMASSOCIATIONSTATUS(handle) != COSMOS_XA_ASSOCIATED) return (XAER_PROTO); /* insert a handle */

    /* transaction status */
    if (COSMOS_XA_RMTRANSTATUS(handle) != COSMOS_XA_ACTIVE) return (XAER_PROTO); /* insert a handle */


    /***********************/
    /* I. parameter check */
    /***********************/

    if (xid == NULL) return (XAER_INVAL);

    if (rmid < 0) return (XAER_INVAL);

    if (flags != TMSUCCESS) return (XAER_INVAL);


    /**************************/
    /* II. call COSMOS XA API */
    /**************************/

    e = LRDS_XA_End(handle, (LRDS_XA_XID*)xid, rmid, flags);
    if (e != eNOERROR) {

        /* Error Code Converting (COSMOS error code to XA error code) */
        e1 = cosmos_xa_ErrorConvert(handle,  e, &xaErrCode );
        if ( e1 == eNOERROR ) 
            return (xaErrCode);

        return (XAER_RMERR);

    }

    /**************************/
    /* III. status transition */
    /**************************/

    COSMOS_XA_RMASSOCIATIONSTATUS(handle) = COSMOS_XA_NOTASSOCIATED;
    COSMOS_XA_RMTRANSTATUS(handle) = COSMOS_XA_IDLE;



    return(XA_OK);


} /* COSMOS_XA_End() */



/*@================================
 * int COSMOS_XA_Prepare()
 *================================*/
/*
 * Function: int COSMOS_XA_Prepare(XID *xid , int rmid, long flags)
 *
 * Description:
 *
 * Return values:
 *  Error codes
 *    some errors cased by function calls
 */

int COSMOS_XA_Prepare(
    Four handle,
	XID *xid , 
	int rmid, 
	long flags)
{

    Four e;                     /* error code */
    Four e1;                    /* error code */
    int xaErrCode;              /* xa error code */ 

    /*******************/
    /* 0. status check */
    /*******************/

    /* open status */
    if (COSMOS_XA_RMOPENSTATUS(handle) != COSMOS_XA_INITIALIZED) return (XAER_PROTO); /* insert a handle */

    /* association status */
    if (COSMOS_XA_RMASSOCIATIONSTATUS(handle) != COSMOS_XA_NOTASSOCIATED) return (XAER_PROTO); /* insert a handle */

    /* transaction status */
    if (COSMOS_XA_RMTRANSTATUS(handle) != COSMOS_XA_IDLE) return (XAER_PROTO); /* insert a handle */


    /**********************/
    /* I. parameter check */
    /**********************/

    if (xid == NULL) return (XAER_INVAL);

    if (rmid < 0) return (XAER_INVAL);

    if (flags != TMNOFLAGS) return (XAER_INVAL);


    /**************************/
    /* II. call COSMOS XA API */
    /**************************/

    e = LRDS_XA_Prepare(handle, (LRDS_XA_XID*)xid, rmid, flags);

    if (e == eXA_RDONLY_LRDS_XA) {

        /* state transition */
        COSMOS_XA_RMTRANSTATUS(handle) = COSMOS_XA_NONTRANSACTION; /* insert a handle */

        return (XA_RDONLY);

    } else if (e != eNOERROR) {

        /* status transition */
        COSMOS_XA_RMTRANSTATUS(handle) = COSMOS_XA_NONTRANSACTION; /* insert a handle */


        /* Error Code Converting (ODYSSEUS error code to XA error code) */
        e1 = cosmos_xa_ErrorConvert(handle,  e, &xaErrCode );
        if ( e1 == eNOERROR ) 
            return (xaErrCode);

        return (XAER_RMERR);

    }

    /**************************/
    /* III. status transition */
    /**************************/

    COSMOS_XA_RMTRANSTATUS(handle) = COSMOS_XA_PREPARED; /* insert a handle */


    return(XA_OK);

} /* COSMOS_XA_Prepare() */



/*@================================
 * int COSMOS_XA_Commit()
 *================================*/
/*
 * Function: int COSMOS_XA_Commit(XID *xid, int rmid, long flags)
 *
 * Description:
 *
 * Return values:
 *  Error codes
 *    some errors cased by function calls
 */

int COSMOS_XA_Commit(
    Four handle,
	XID *xid, 
	int rmid, 
	long flags)
{

    Four e;                     /* error code */
    Four e1;                    /* error code */
    int xaErrCode;              /* xa error code */ 

    /*******************/
    /* 0. status check */
    /*******************/

    /* open status */
    if (COSMOS_XA_RMOPENSTATUS(handle) != COSMOS_XA_INITIALIZED) return (XAER_PROTO); /* insert a handle */

    /* association status */
    if (COSMOS_XA_RMASSOCIATIONSTATUS(handle) != COSMOS_XA_NOTASSOCIATED) return (XAER_PROTO); /* insert a handle */

    /* transaction status */
    if (COSMOS_XA_RMTRANSTATUS(handle) != COSMOS_XA_IDLE && COSMOS_XA_RMTRANSTATUS(handle) != COSMOS_XA_PREPARED) 
        return (XAER_PROTO);


    /**********************/
    /* I. parameter check */
    /**********************/

    if (xid == NULL) return (XAER_INVAL);  

    if (rmid < 0) return (XAER_INVAL);

    if (flags != TMNOFLAGS) return (XAER_INVAL);


    /**************************/
    /* II. call COSMOS XA API */
    /**************************/

    e = LRDS_XA_Commit(handle, (LRDS_XA_XID*)xid, rmid, flags);
    if (e != eNOERROR) {

	COSMOS_XA_RMTRANSTATUS(handle) = COSMOS_XA_NONTRANSACTION; /* insert a handle */

        /* Error Code Converting (COSMOS error code to XA error code) */
        e1 = cosmos_xa_ErrorConvert(handle,  e, &xaErrCode );
        if ( e1 == eNOERROR ) 
            return (xaErrCode);

        return (XAER_RMERR);

    }

    /**************************/
    /* III. status transition */
    /**************************/

    COSMOS_XA_RMTRANSTATUS(handle) = COSMOS_XA_NONTRANSACTION; /* insert a handle */


    return(XA_OK);


} /* COSMOS_XA_Commit() */



/*@================================
 * int COSMOS_XA_Rollback()
 *================================*/
/*
 * Function: int COSMOS_XA_Rollback(XID* xid, int rmid, long flags)
 *
 * Description:
 *
 * Return values:
 *  Error codes
 *    some errors cased by function calls
 */

int COSMOS_XA_Rollback(
    Four handle,
	XID* xid, 
	int rmid, 
	long flags)
{

    Four e;                     /* error code */
    Four e1;                    /* error code */
    int xaErrCode;              /* xa error code */ 

    /*******************/
    /* 0. status check */
    /*******************/

    /* open status */
    if (COSMOS_XA_RMOPENSTATUS(handle) != COSMOS_XA_INITIALIZED) return (XAER_PROTO); /* insert a handle */

    /* transaction status */
    if (COSMOS_XA_RMTRANSTATUS(handle) != COSMOS_XA_PREPARED && COSMOS_XA_RMTRANSTATUS(handle) != COSMOS_XA_IDLE &&
        COSMOS_XA_RMTRANSTATUS(handle) != COSMOS_XA_ROLLBACKONLY) 
        return (XAER_PROTO);


    /**********************/
    /* I. parameter check */
    /**********************/

    if (xid == NULL) return (XAER_INVAL);

    if (rmid < 0) return (XAER_INVAL);

    if (flags != TMNOFLAGS) return (XAER_INVAL);


    /**************************/
    /* II. call COSMOS XA API */
    /**************************/

    e = LRDS_XA_Rollback(handle, (LRDS_XA_XID*)xid, rmid, flags);
    if (e != eNOERROR) {

        COSMOS_XA_RMTRANSTATUS(handle) = COSMOS_XA_NONTRANSACTION; /* insert a handle */

        /* Error Code Converting (COSMOS error code to XA error code) */
        e1 = cosmos_xa_ErrorConvert(handle,  e, &xaErrCode );
        if ( e1 == eNOERROR ) 
            return (xaErrCode);

        return (XAER_RMERR);

    }

    /**************************/
    /* III. status transition */
    /**************************/

    COSMOS_XA_RMTRANSTATUS(handle) = COSMOS_XA_NONTRANSACTION; /* insert a handle */


    return(XA_OK);

} /* COSMOS_XA_Rollback() */

 

/*@================================
 * int COSMOS_XA_Recover()
 *================================*/
/*
 * Function: int COSMOS_XA_Recover(XID *xids, long count, int rmid, long flags)
 *
 * Description:
 *
 * Return values:
 *  Error codes
 *    some errors cased by function calls
 */

int COSMOS_XA_Recover(
    Four handle,
	XID *xids, 
	long count, 
	int rmid, 
	long flags)
{

    Four e;                     /* error code */
    Four e1;                    /* error code */
    int xaErrCode;              /* xa error code */ 


    /***********************/
    /* I. parameter check */
    /***********************/
 
    if (xids == NULL) return (XAER_INVAL);

    if (count <= 0) return (XAER_INVAL);

    if (rmid < 0) return (XAER_INVAL);

    if (flags != TMNOFLAGS && flags != TMSTARTRSCAN && flags != TMENDRSCAN &&
	flags != (long)(TMSTARTRSCAN & TMENDRSCAN)) 
        return (XAER_INVAL);


    /**************************/
    /* II. call COSMOS XA API */
    /**************************/

    e = LRDS_XA_Recover(handle, (LRDS_XA_XID*)xids, count, rmid, flags);
    if (e != eNOERROR) {

        /* Error Code Converting (COSMOS error code to XA error code) */
        e1 = cosmos_xa_ErrorConvert(handle,  e, &xaErrCode );
        if ( e1 == eNOERROR ) 
            return (xaErrCode);

        return (XAER_RMERR);

    }

    return(XA_OK);


} /* COSMOS_XA_Recover() */


/*@================================
 * int COSMOS_XA_Forget()
 *================================*/
/*
 * Function: int COSMOS_XA_Forget(XID *xid, int rmid, long flags)
 *
 * Description:
 *
 * Return values:
 *  Error codes
 *    some errors cased by function calls
 */

int COSMOS_XA_Forget(
    Four handle,
   XID *xid, 
   int rmid, 
   long flags)
{

    Four e;                     /* error code */
    Four e1;                    /* error code */
    int xaErrCode;              /* xa error code */ 

    /*******************/
    /* 0. status check */
    /*******************/

    /* open status */
    if (COSMOS_XA_RMOPENSTATUS(handle) != COSMOS_XA_INITIALIZED) return (XAER_PROTO); /* insert a handle */

    /* transaction status */
    if (COSMOS_XA_RMTRANSTATUS(handle) != COSMOS_XA_PREPARED && COSMOS_XA_RMTRANSTATUS(handle) != COSMOS_XA_IDLE &&
        COSMOS_XA_RMTRANSTATUS(handle) != COSMOS_XA_ROLLBACKONLY)
        return (XAER_PROTO);


    /***********************/
    /* I. parameter check */
    /***********************/

    if (xid == NULL) return (XAER_INVAL);

    if (rmid < 0) return (XAER_INVAL);

    if (flags != TMNOFLAGS) return (XAER_INVAL);


    /**************************/
    /* II. call COSMOS XA API */
    /**************************/

    e = LRDS_XA_Forget(handle, (LRDS_XA_XID*)xid, rmid, flags);
    if (e != eNOERROR) {

        COSMOS_XA_RMTRANSTATUS(handle) = COSMOS_XA_NONTRANSACTION; /* insert a handle */

        /* Error Code Converting (COSMOS error code to XA error code) */
        e1 = cosmos_xa_ErrorConvert(handle,  e, &xaErrCode );
        if ( e1 == eNOERROR ) 
            return (xaErrCode);

        return (XAER_RMERR);

    }


    return(XA_OK);


} /* COSMOS_XA_Forget() */



/*@================================
 * int COSMOS_XA_Complete()
 *================================*/
/*
 * Function: int COSMOS_XA_Complete(int* handle, int *retval, int rmid, long flags)
 *
 * Return values:
 *  Error codes
 *    some errors cased by function calls
 */

int COSMOS_XA_Complete(
   Four handle,	
   int *xa_handle,	
   int *retval, 
   int rmid, 
   long flags)
{

    Four e;                     /* error code */

    /***********************/
    /* I. parameter check */
    /***********************/
    if (xa_handle == NULL) return (XAER_INVAL);	

    if (retval == NULL) return (XAER_INVAL);

    if (rmid < 0) return (XAER_INVAL);

    if (flags != TMNOFLAGS) return (XAER_INVAL);


    /********************/
    /* II. return value */
    /********************/

    return(XAER_PROTO);


} /* COSMOS_XA_Complete() */


/*@================================
 * int COSMOS_XA_AxReg()
 *================================*/
/*
 * Function: int COSMOS_XA_AxReg(int rmid, XID* xid, long flags)
 *
 * Return values:
 *  Error codes
 *    some errors cased by function calls
 */

int COSMOS_XA_AxReg(
    Four handle,
   int rmid, 
   XID *xid,
   long flags)
{

    Four e;                     /* error code */

    /***********************/
    /* I. parameter check */
    /***********************/

    if (rmid < 0) return (XAER_INVAL);

    if (xid == NULL) return (XAER_INVAL);

    if (flags != TMNOFLAGS) return (XAER_INVAL);


    /********************/
    /* II. return value */
    /********************/

    return(XAER_PROTO);


} /* COSMOS_XA_AxReg() */


/*@================================
 * int COSMOS_XA_AxUnreg()
 *================================*/
/*
 * Function: int COSMOS_XA_AxUnreg(int rmid, long flags)
 *
 * Return values:
 *  Error codes
 *    some errors cased by function calls
 */

int COSMOS_XA_AxUnreg(
    Four handle,
   int rmid, 
   long flags)
{

    Four e;                     /* error code */

    /***********************/
    /* I. parameter check */
    /***********************/

    if (rmid < 0) return (XAER_INVAL);

    if (flags != TMNOFLAGS) return (XAER_INVAL);


    /********************/
    /* II. return value */
    /********************/

    return(XAER_PROTO);


} /* COSMOS_XA_AxUnreg() */
