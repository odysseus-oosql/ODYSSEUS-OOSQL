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
 * Module: SM_Counter.c
 *
 * Description:
 *  Implements a counter.
 *
 * Exports:
 *  Four SM_CreateCounter(Four, Four, char*, Four, CounterID*, LockParameter*)
 *  Four SM_DestroyCounter(Four, Four, char*, LockParameter*)
 *  Four SM_GetCounterId(Four, Four, char*, CounterID*, LockParameter*)
 *  Four SM_SetCounter(Four, Four, CounterID*, Four, LockParameter*)
 *  Four SM_ReadCounter(Four, Four, CounterID*, Four*, LockParameter*)
 *  Four SM_GetCounterValues(Four, Four, CounterID*, Four, Four*, LockParameter*)
 */

#include <assert.h>
#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "TM.h" 
#include "LM.h"
#include "OM.h"
#include "BtM.h"
#include "SM.h"
#include "SHM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


/*
 * Function: Four SM_CreateCounter(Four, Four, char*, Four, CounterID*, LockParameter*)
 *
 * Description:
 *  Creates a counter as the given name.
 *
 * Returns:
 *  error code
 */
Four SM_CreateCounter(
    Four       handle,
    Four       volId,               /* IN volume id */
    char*      cntrName,            /* IN counter name */
    Four       initialValue,        /* IN initialize the counter as this value */
    CounterID* cntrId)              /* OUT counter id */
{
    Four       e;                   /* error code */
    Four       v;                   /* array index on scan manager mount table */
    KeyValue   kval;                /* key value for index on SYSCOUNTERS */
    LockParameter lockup;           /* lockup parameter */
    LogParameter_T logParam;        /* log parameter */
    sm_SysCountersOverlay_T counter;/* overlay for created counter */
    LockParameter fileLockup;	    /* lockup for file */ 
    LockReply	  lockReply;        /* lock reply */ 
    LockMode      oldMode;          /* old lock mode */ 

    TR_PRINT(handle, TR_LRDS, TR1, ("SM_CreateCounter()"));


    /* Check parameters */
    if (volId < 0) ERR(handle, eBADPARAMETER);
    if (cntrName == NULL) ERR(handle, eBADPARAMETER);
    if (strlen(cntrName) > SM_COUNTER_NAME_MAX_LEN) ERR(handle, eBADPARAMETER);
    if (initialValue < 0) ERR(handle, eBADPARAMETER);
    if (cntrId == NULL) ERR(handle, eBADPARAMETER);

    /* find the given volume in the scan manager mount table */
    for (v = 0; v < MAXNUMOFVOLS; v++)
        if (SM_MOUNTTABLE[v].volId == volId) break; /* found */
    if (v == MAXNUMOFVOLS) ERR(handle, eNOTMOUNTEDVOLUME_SM);

    /* set lockup parameter */
    lockup.mode = L_X;
    lockup.duration = L_COMMIT;

    fileLockup.mode = L_IX;
    fileLockup.duration = L_COMMIT;

    /* set logParam */
    SET_LOG_PARAMETER(logParam, common_shmPtr->recoveryFlag, FALSE);

    /* set counter */
    strcpy(counter.counterName, cntrName);
    counter.counterValue = initialValue;

    /* Get file lock first */
    e = LM_getFileLock(handle, &MY_XACTID(handle), &(SM_MOUNTTABLE[v].sysCountersInfo.fid) , fileLockup.mode, fileLockup.duration,
                       L_UNCONDITIONAL, &lockReply, &oldMode);
    if ( e < eNOERROR ) ERR(handle, e);

    if (lockReply == LR_DEADLOCK) ERR(handle, eDEADLOCK);

    /* insert into SYSCOUNTERS */
    e = OM_CreateObject(handle, MY_XACT_TABLE_ENTRY(handle), &(SM_MOUNTTABLE[v].sysCountersInfo),
                        NULL, NULL, sizeof(sm_SysCountersOverlay_T),
                        (char*)&counter, (ObjectID*)cntrId, &lockup, &logParam);
    if (e < eNOERROR) ERR(handle, e);

    /* set kval */
    kval.len = strlen(cntrName);
    memcpy(&kval.val[0], &kval.len, sizeof(Two));
    memcpy(&kval.val[sizeof(Two)], cntrName, kval.len);
    kval.len += sizeof(Two);

    /* insert into index on SYSCOUNTERS */
#ifdef CCPL
    e = BtM_InsertObject(handle, MY_XACT_TABLE_ENTRY(handle),
                         &(SM_MOUNTTABLE[v].sysCountersCounterNameIndexInfo),
			 &(SM_MOUNTTABLE[v].sysCountersInfo.fid), 
                         &SM_SYSCNTR_CNTRNAME_KEYDESC,
                         &kval, (ObjectID*)cntrId, &lockup, &logParam);
    if (e < eNOERROR) ERR(handle, e);
#endif /* CCPL */

#ifdef CCRL
    e = BtM_InsertObject(handle, MY_XACT_TABLE_ENTRY(handle),
                         &(SM_MOUNTTABLE[v].sysCountersCounterNameIndexInfo),
			 &(SM_MOUNTTABLE[v].sysCountersInfo.fid), 
                         &SM_SYSCNTR_CNTRNAME_KEYDESC,
                         &kval, (ObjectID*)cntrId, NULL, &logParam); /* no lock request */
    if (e < eNOERROR) ERR(handle, e);
#endif /* CCRL */


    return(eNOERROR);

} /* SM_CreateCounter() */



/*
 * Function: Four SM_DestroyCounter(Four, Four, char*, LockParameter*)
 *
 * Description:
 *  Destroy the given counter.
 *
 * Returns:
 *  error code
 */
Four SM_DestroyCounter(
    Four        handle,
    Four        volId,              /* IN volume id */
    char*       cntrName)           /* IN counter name */
{
    Four        e;                  /* error code */
    Four        v;                  /* array index on scan manager mount table */
    KeyValue    kval;               /* key value for index on SYSCOUNTERS */
    BtreeCursor cursor;             /* a B+ tree cursor */
    LockParameter lockup;           /* lockup parameter */
    LockParameter indexLockup;      /* lockup parameter for the index lookup */
    LogParameter_T logParam;        /* log parameter */
    LockParameter fileLockup;	    /* lockup for file */ 
    LockReply	  lockReply;        /* lock reply */ 
    LockMode      oldMode;          /* old lock mode */ 

    TR_PRINT(handle, TR_LRDS, TR1, ("SM_DestroyCounter()"));


    /* Check parameters */
    if (volId < 0) ERR(handle, eBADPARAMETER);
    if (cntrName == NULL) ERR(handle, eBADPARAMETER);
    if (strlen(cntrName) > SM_COUNTER_NAME_MAX_LEN) ERR(handle, eBADPARAMETER);

    /* set logParam */
    SET_LOG_PARAMETER(logParam, common_shmPtr->recoveryFlag, FALSE);

    /* find the given volume in the scan manager mount table */
    for (v = 0; v < MAXNUMOFVOLS; v++)
        if (SM_MOUNTTABLE[v].volId == volId) break; /* found */
    if (v == MAXNUMOFVOLS) ERR(handle, eNOTMOUNTEDVOLUME_SM);

    /* set kval */
    kval.len = strlen(cntrName);
    memcpy(&kval.val[0], &kval.len, sizeof(Two));
    memcpy(&kval.val[sizeof(Two)], cntrName, kval.len);
    kval.len += sizeof(Two);

    fileLockup.mode = L_IX;
    fileLockup.duration = L_COMMIT;

    /* Get file lock first */
    e = LM_getFileLock(handle, &MY_XACTID(handle), &(SM_MOUNTTABLE[v].sysCountersInfo.fid) , fileLockup.mode, fileLockup.duration,
                       L_UNCONDITIONAL, &lockReply, &oldMode);
    if ( e < eNOERROR ) ERR(handle, e);

    if (lockReply == LR_DEADLOCK) ERR(handle, eDEADLOCK);

    /* check given counter is exists */
#ifdef CCPL
    indexLockup.mode = L_S;
    indexLockup.duration = L_COMMIT;

    e = BtM_Fetch(handle, MY_XACT_TABLE_ENTRY(handle), &(SM_MOUNTTABLE[v].sysCountersCounterNameIndexInfo),
                  &(SM_MOUNTTABLE[v].sysCountersInfo.fid), 
                  &SM_SYSCNTR_CNTRNAME_KEYDESC,
                  &kval, SM_EQ, &kval, SM_EQ, &cursor, NULL, &indexLockup);
    if (e < eNOERROR) ERR(handle, e);
#endif /* CCPL */

#ifdef CCRL
    e = BtM_Fetch(handle, MY_XACT_TABLE_ENTRY(handle), &(SM_MOUNTTABLE[v].sysCountersCounterNameIndexInfo),
                  &(SM_MOUNTTABLE[v].sysCountersInfo.fid), 
                  &SM_SYSCNTR_CNTRNAME_KEYDESC,
                  &kval, SM_EQ, &kval, SM_EQ, &cursor, NULL, NULL); /* no lock request */
    if (e < eNOERROR) ERR(handle, e);
#endif /* CCRL */

    /* there exist given counter? */
    if (cursor.flag == CURSOR_EOS) ERR(handle, eCOUNTERNOTFOUND_LRDS);

    /* set lockup parameter */
    lockup.mode = L_X;
    lockup.duration = L_COMMIT;

    /* delete from index on SYSCOUNTERS */
#ifdef CCPL
    e = BtM_DeleteObject(handle, MY_XACT_TABLE_ENTRY(handle),
                         &(SM_MOUNTTABLE[v].sysCountersCounterNameIndexInfo),
			 &(SM_MOUNTTABLE[v].sysCountersInfo.fid), 
                         &SM_SYSCNTR_CNTRNAME_KEYDESC,
                         &kval, &cursor.oid, &lockup, &logParam);
    if (e < eNOERROR) ERR(handle, e);
#endif /* CCPL */

#ifdef CCRL
    e = BtM_DeleteObject(handle, MY_XACT_TABLE_ENTRY(handle),
                         &(SM_MOUNTTABLE[v].sysCountersCounterNameIndexInfo),
			 &(SM_MOUNTTABLE[v].sysCountersInfo.fid), 
                         &SM_SYSCNTR_CNTRNAME_KEYDESC,
                         &kval, &cursor.oid, NULL, &logParam); /* no lock request */
    if (e < eNOERROR) ERR(handle, e);
#endif /* CCRL */

    /* delete from SYSCOUNTERS */
    e = OM_DestroyObject(handle, MY_XACT_TABLE_ENTRY(handle), &(SM_MOUNTTABLE[v].sysCountersInfo),
                         &cursor.oid, &lockup, &logParam);
    if (e < eNOERROR) ERR(handle, e);


    return(eNOERROR);

} /* SM_DestroyCounter() */



/*
 * Function: Four SM_GetCounterId(Four, Four, char*, CounterID*, LockParameter*)
 *
 * Description:
 *  Returns the internal id of the given counter.
 *
 * Returns:
 *  error code
 */
Four SM_GetCounterId(
    Four 	handle,		    /* IN handle */
    Four        volId,              /* IN volume id */
    char*       cntrName,           /* IN counter name */
    CounterID*  cntrId)             /* OUT counter id */
{
    Four        e;                  /* error code */
    Four        v;                  /* array index on scan manager mount table */
    KeyValue    kval;               /* key value for index on SYSCOUNTERS */
    BtreeCursor cursor;             /* a B+ tree cursor */
    LockParameter indexLockup;      /* lockup parameter for the index of catalog */
    LockParameter fileLockup;	    /* lockup for file */ 
    LockReply	  lockReply;        /* lock reply */ 
    LockMode      oldMode;          /* old lock mode */ 


    TR_PRINT(handle, TR_LRDS, TR1, ("SM_GetCounterId()"));


    /* Check parameters */
    if (volId < 0) ERR(handle, eBADPARAMETER);
    if (cntrName == NULL) ERR(handle, eBADPARAMETER);
    if (strlen(cntrName) > SM_COUNTER_NAME_MAX_LEN) ERR(handle, eBADPARAMETER);
    if (cntrId == NULL) ERR(handle, eBADPARAMETER);

    /* find the given volume in the scan manager mount table */
    for (v = 0; v < MAXNUMOFVOLS; v++)
        if (SM_MOUNTTABLE[v].volId == volId) break; /* found */
    if (v == MAXNUMOFVOLS) ERR(handle, eNOTMOUNTEDVOLUME_SM);

    /* set kval */
    kval.len = strlen(cntrName);
    memcpy(&kval.val[0], &kval.len, sizeof(Two));
    memcpy(&kval.val[sizeof(Two)], cntrName, kval.len);
    kval.len += sizeof(Two);

    fileLockup.mode = L_IS;
    fileLockup.duration = L_COMMIT;

    /* Get file lock first */
    e = LM_getFileLock(handle, &MY_XACTID(handle), &(SM_MOUNTTABLE[v].sysCountersInfo.fid) , fileLockup.mode, fileLockup.duration,
                       L_UNCONDITIONAL, &lockReply, &oldMode);
    if ( e < eNOERROR ) ERR(handle, e);

    if (lockReply == LR_DEADLOCK) ERR(handle, eDEADLOCK);


    /* check given counter is exsists */
#ifdef CCPL
    indexLockup.mode = L_S;
    indexLockup.duration = L_COMMIT;

    e = BtM_Fetch(handle, MY_XACT_TABLE_ENTRY(handle), &(SM_MOUNTTABLE[v].sysCountersCounterNameIndexInfo),
                  &(SM_MOUNTTABLE[v].sysCountersInfo.fid), 
                  &SM_SYSCNTR_CNTRNAME_KEYDESC,
                  &kval, SM_EQ, &kval, SM_EQ, &cursor, NULL, &indexLockup);
    if (e < eNOERROR) ERR(handle, e);
#endif /* CCPL */

#ifdef CCRL
    e = BtM_Fetch(handle, MY_XACT_TABLE_ENTRY(handle), &(SM_MOUNTTABLE[v].sysCountersCounterNameIndexInfo),
                  &(SM_MOUNTTABLE[v].sysCountersInfo.fid), 
                  &SM_SYSCNTR_CNTRNAME_KEYDESC,
                  &kval, SM_EQ, &kval, SM_EQ, &cursor, NULL, NULL); /* no lock request */
    if (e < eNOERROR) ERR(handle, e);
#endif /* CCRL */

    /* there exist given counter? */
    if (cursor.flag == CURSOR_EOS) ERR(handle, eCOUNTERNOTFOUND_LRDS);

    /* set cntrId */
    *cntrId = cursor.oid;


    return(eNOERROR);

} /* SM_GetCounterId() */



/*
 * Function: Four SM_SetCounter(Four, Four, CounterID*, Four, LockParameter*)
 *
 * Description:
 *  Set the counter to the given value.
 *
 * Returns:
 *  error code
 */
Four SM_SetCounter(
    Four 	handle,		    /* IN handle */
    Four        volId,              /* IN volume id */
    CounterID*  cntrId,             /* IN counter id */
    Four        value)              /* IN set the counter to this value */
{
    Four        e;                  /* error code */
    Four        v;                  /* array index on scan manager mount table */
    LockParameter lockup;           /* lockup parameter */
    LogParameter_T logParam;        /* log parameter */
    LockParameter fileLockup;	    /* lockup for file */ 
    LockReply	  lockReply;        /* lock reply */ 
    LockMode      oldMode;          /* old lock mode */ 

    TR_PRINT(handle, TR_LRDS, TR1, ("SM_SetCounter()"));


    /* Check parameters */
    if (volId < 0) ERR(handle, eBADPARAMETER);
    if (cntrId == NULL) ERR(handle, eBADPARAMETER);
    if (value < 0) ERR(handle, eBADPARAMETER);

    /* find the given volume in the scan manager mount table */
    for (v = 0; v < MAXNUMOFVOLS; v++)
        if (SM_MOUNTTABLE[v].volId == volId) break; /* found */
    if (v == MAXNUMOFVOLS) ERR(handle, eNOTMOUNTEDVOLUME_SM);

    /* set lockup parameter */
    lockup.mode = L_X;
    lockup.duration = L_COMMIT;

    fileLockup.mode = L_IX;
    fileLockup.duration = L_COMMIT;

    /* Get file lock first */
    e = LM_getFileLock(handle, &MY_XACTID(handle), &(SM_MOUNTTABLE[v].sysCountersInfo.fid) , fileLockup.mode, fileLockup.duration,
                       L_UNCONDITIONAL, &lockReply, &oldMode);
    if ( e < eNOERROR ) ERR(handle, e);

    if (lockReply == LR_DEADLOCK) ERR(handle, eDEADLOCK);

    /* set logParam */
    SET_LOG_PARAMETER(logParam, common_shmPtr->recoveryFlag, FALSE);

    /* update counter value */
    e = OM_WriteObject(handle, MY_XACT_TABLE_ENTRY(handle), &(SM_MOUNTTABLE[v].sysCountersInfo),
                       (ObjectID*)cntrId, OFFSET_OF(sm_SysCountersOverlay_T, counterValue),
                       sizeof(Four), (char*) &value, &lockup, &logParam);
    if (e < eNOERROR) ERR(handle, e);


    return(eNOERROR);

} /* SM_SetCounter() */



/*
 * Function: Four SM_ReadCounter(Four, Four, CounterID*, Four*, LockParameter*)
 *
 * Description:
 *  Read the current value from the counter.
 *
 * Returns:
 *  error code
 */
Four SM_ReadCounter(
    Four 	handle,		    /* IN handle */
    Four        volId,              /* IN volume id */
    CounterID*  cntrId,             /* IN counter id */
    Four*       value)              /* OUT the current counter value */
{
    Four        e;                  /* error code */
    Four        v;                  /* array index on scan manager mount table */
    LockParameter lockup;           /* lockup parameter */
    LockParameter fileLockup;	    /* lockup for file */ 
    LockReply	  lockReply;        /* lock reply */ 
    LockMode      oldMode;          /* old lock mode */ 

    TR_PRINT(handle, TR_LRDS, TR1, ("SM_ReadCounter()"));


    /* Check parameters */
    if (volId < 0) ERR(handle, eBADPARAMETER);
    if (cntrId == NULL) ERR(handle, eBADPARAMETER);
    if (value == NULL) ERR(handle, eBADPARAMETER);

    /* find the given volume in the scan manager mount table */
    for (v = 0; v < MAXNUMOFVOLS; v++)
        if (SM_MOUNTTABLE[v].volId == volId) break; /* found */
    if (v == MAXNUMOFVOLS) ERR(handle, eNOTMOUNTEDVOLUME_SM);

    /* set lockup parameter */
    lockup.mode = L_S;
    lockup.duration = L_COMMIT;

    fileLockup.mode = L_IS;
    fileLockup.duration = L_COMMIT;

    /* Get file lock first */
    e = LM_getFileLock(handle, &MY_XACTID(handle), &(SM_MOUNTTABLE[v].sysCountersInfo.fid) , fileLockup.mode, fileLockup.duration,
                       L_UNCONDITIONAL, &lockReply, &oldMode);
    if ( e < eNOERROR ) ERR(handle, e);

    if (lockReply == LR_DEADLOCK) ERR(handle, eDEADLOCK);


    /* read counter value */
    e = OM_ReadObject(handle, MY_XACT_TABLE_ENTRY(handle), &(SM_MOUNTTABLE[v].sysCountersInfo.fid), (ObjectID*)cntrId,
                      OFFSET_OF(sm_SysCountersOverlay_T, counterValue), sizeof(Four), (char *)value, &lockup); 
    if (e < eNOERROR) ERR(handle, e);


    return(eNOERROR);

} /* SM_ReadCounter() */



/*
 * Function: Four SM_GetCounterValues(Four, Four, CounterID*, Four, Four*, LockParameter*)
 *
 * Description:
 *  The counter allocates the given number of numbers to the user.
 *  The numbers start from the startValue and contiguous.
 *
 * Returns:
 *  error code
 */
Four SM_GetCounterValues(
    Four 	handle,		    /* IN handle */
    Four        volId,              /* IN volume id */
    CounterID*  cntrId,             /* IN counter id */
    Four        nValues,            /* IN number of values to be allocated */
    Four*       startValue)         /* OUT allocated numbers start from this value */
{
    Four        e;                  /* error code */
    Four        updateValue;        /* update value of counter */

    TR_PRINT(handle, TR_LRDS, TR1, ("SM_GetCounterValues()"));


    /* get startValue from the counter */
    e = SM_ReadCounter(handle, volId, cntrId, startValue);
    if (e < eNOERROR) ERR(handle, e);

    /* calculate updateValue */
    /* Note!! we use only zero and positive value as counter value */
    /* So, wrap around to zero if counter value is overflowed  */
    updateValue = (*startValue + nValues) & (CONSTANT_ALL_BITS_SET(Four) >> 1);

    /* update counter */
    e = SM_SetCounter(handle, volId, cntrId, updateValue);
    if (e < eNOERROR) ERR(handle, e);


    return(eNOERROR);

} /* SM_GetCounterValues() */
