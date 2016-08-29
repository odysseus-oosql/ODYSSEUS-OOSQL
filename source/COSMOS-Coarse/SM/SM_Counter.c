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
 * Module: SM_Counter.c
 *
 * Description:
 *  Implements a counter.
 *
 * Exports:
 *  Four _SM_CreateCounter(Four, char*, Four, CounterID*)
 *  Four _SM_DestroyCounter(Four, char*)
 *  Four _SM_GetCounterId(Four, char*, CounterID*)
 *  Four _SM_SetCounter(Four, CounterID*, Four)
 *  Four _SM_ReadCounter(Four, CounterID*, Four*)
 *  Four _SM_GetCounterValues(Four, CounterID*, Four, Four*)
 */

#include <assert.h>
#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "OM_Internal.h"	
#include "BtM.h"
#include "SM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


/*
 * Function: Four _SM_CreateCounter(Four, char*, Four, CounterID*)
 *
 * Description:
 *  Creates a counter as the given name.
 *
 * Returns:
 *  error code
 */
Four _SM_CreateCounter(
    Four handle,
    Four       volId,               /* IN volume id */
    char*      cntrName,            /* IN counter name */
    Four       initialValue,        /* IN initialize the counter as this value */
    CounterID* cntrId)              /* OUT counter id */
{
    Four       e;                   /* error code */
    Four       v;                   /* array index on scan manager mount table */
    KeyValue   kval;                /* key value for index on SYSCOUNTERS */
    sm_SysCountersOverlay_T counter;/* overlay for created counter */

    TR_PRINT(TR_LRDS, TR1, ("_SM_CreateCounter(handle)"));


    /* Check parameters */
    if (volId < 0) ERR(handle, eBADPARAMETER);
    if (cntrName == NULL) ERR(handle, eBADPARAMETER);
    if (strlen(cntrName) > SM_COUNTER_NAME_MAX_LEN) ERR(handle, eBADPARAMETER);
    if (cntrId == NULL) ERR(handle, eBADPARAMETER);
    
    /* find the given volume in the scan manager mount table */
    for (v = 0; v < MAXNUMOFVOLS; v++)
        if (SM_PER_THREAD_DS(handle).smMountTable[v].volId == volId) break; /* found */ 
    if (v == MAXNUMOFVOLS) ERR(handle, eNOTMOUNTEDVOLUME_SM);

    /* set counter */
    strcpy(counter.counterName, cntrName);
    counter.counterValue = initialValue;

    /* insert into SYSCOUNTERS */
    e = OM_CreateObject(handle, &(SM_PER_THREAD_DS(handle).smMountTable[v].sysTablesSysCountersEntry), NULL, NULL, 
                        sizeof(sm_SysCountersOverlay_T), (char*)&counter, (ObjectID*)cntrId);
    if (e < eNOERROR) ERR(handle, e);

    /* set kval */
    kval.len = strlen(cntrName);
    memcpy(&kval.val[0], &kval.len, sizeof(Two));
    memcpy(&kval.val[sizeof(Two)], cntrName, kval.len);
    kval.len += sizeof(Two);
    
    /* insert into index on SYSCOUNTERS */
    e = BtM_InsertObject(handle, &(SM_PER_THREAD_DS(handle).smMountTable[v].sysTablesSysCountersEntry), 
                         &(SM_PER_THREAD_DS(handle).smMountTable[v].sysCountersCounterNameIndex),
                         &SM_PER_THREAD_DS(handle).smSysCountersCounterNameIndexKdesc, &kval, 
                         (ObjectID*)cntrId, &SM_PER_THREAD_DS(handle).dlPool, &(SM_PER_THREAD_DS(handle).smMountTable[v].dlHead));
    if (e < eNOERROR) ERR(handle, e);
    

    return(eNOERROR);
    
} /* _SM_CreateCounter() */



/*
 * Function: Four _SM_DestroyCounter(Four, char*)
 *
 * Description:
 *  Destroy the given counter.
 *
 * Returns:
 *  error code
 */
Four _SM_DestroyCounter(
    Four handle,
    Four        volId,              /* IN volume id */
    char*       cntrName)           /* IN counter name */
{
    Four        e;                  /* error code */
    Four        v;                  /* array index on scan manager mount table */
    KeyValue    kval;               /* key value for index on SYSCOUNTERS */
    BtreeCursor cursor;             /* a B+ tree cursor */
    
    TR_PRINT(TR_LRDS, TR1, ("_SM_DestroyCounter(handle)"));


    /* Check parameters */
    if (volId < 0) ERR(handle, eBADPARAMETER);
    if (cntrName == NULL) ERR(handle, eBADPARAMETER);
    if (strlen(cntrName) > SM_COUNTER_NAME_MAX_LEN) ERR(handle, eBADPARAMETER);
    
    /* find the given volume in the scan manager mount table */
    for (v = 0; v < MAXNUMOFVOLS; v++)
        if (SM_PER_THREAD_DS(handle).smMountTable[v].volId == volId) break; /* found */ 
    if (v == MAXNUMOFVOLS) ERR(handle, eNOTMOUNTEDVOLUME_SM);

    /* set kval */
    kval.len = strlen(cntrName);
    memcpy(&kval.val[0], &kval.len, sizeof(Two));
    memcpy(&kval.val[sizeof(Two)], cntrName, kval.len);
    kval.len += sizeof(Two);

    /* check given counter is exsists */
    e = BtM_Fetch(handle, &(SM_PER_THREAD_DS(handle).smMountTable[v].sysCountersCounterNameIndex), 
                  &SM_PER_THREAD_DS(handle).smSysCountersCounterNameIndexKdesc, &kval, SM_EQ, &kval, SM_EQ, &cursor);
    if (e < eNOERROR) ERR(handle, e);
    if (cursor.flag == CURSOR_EOS) ERR(handle, eCOUNTERNOTFOUND_LRDS);

    /* delete from index on SYSCOUNTERS */
    e = BtM_DeleteObject(handle, &(SM_PER_THREAD_DS(handle).smMountTable[v].sysTablesSysCountersEntry),
                         &(SM_PER_THREAD_DS(handle).smMountTable[v].sysCountersCounterNameIndex),
                         &SM_PER_THREAD_DS(handle).smSysCountersCounterNameIndexKdesc, 
                         &kval, &cursor.oid, &SM_PER_THREAD_DS(handle).dlPool, &(SM_PER_THREAD_DS(handle).smMountTable[v].dlHead));
    if (e < eNOERROR) ERR(handle, e);

    /* delete from SYSCOUNTERS */
    e = OM_DestroyObject(handle, &(SM_PER_THREAD_DS(handle).smMountTable[v].sysTablesSysCountersEntry),
                         &cursor.oid, &SM_PER_THREAD_DS(handle).dlPool, &(SM_PER_THREAD_DS(handle).smMountTable[v].dlHead));
    if (e < eNOERROR) ERR(handle, e);

    
    return(eNOERROR);
    
} /* _SM_DestroyCounter() */


    
/*
 * Function: Four _SM_GetCounterId(Four, char*, CounterID*)
 *
 * Description:
 *  Returns the internal id of the given counter.
 *
 * Returns:
 *  error code
 */
Four _SM_GetCounterId(
    Four handle,
    Four        volId,              /* IN volume id */
    char*       cntrName,           /* IN counter name */
    CounterID*  cntrId)             /* OUT counter id */
{
    Four        e;                  /* error code */
    Four        v;                  /* array index on scan manager mount table */
    KeyValue    kval;               /* key value for index on SYSCOUNTERS */
    BtreeCursor cursor;             /* a B+ tree cursor */

    
    TR_PRINT(TR_LRDS, TR1, ("SM_GetCounterId(handle)"));

    
    /* Check parameters */
    if (volId < 0) ERR(handle, eBADPARAMETER);
    if (cntrName == NULL) ERR(handle, eBADPARAMETER);
    if (strlen(cntrName) > SM_COUNTER_NAME_MAX_LEN) ERR(handle, eBADPARAMETER);
    if (cntrId == NULL) ERR(handle, eBADPARAMETER);
    
    /* find the given volume in the scan manager mount table */
    for (v = 0; v < MAXNUMOFVOLS; v++)
        if (SM_PER_THREAD_DS(handle).smMountTable[v].volId == volId) break; /* found */ 
    if (v == MAXNUMOFVOLS) ERR(handle, eNOTMOUNTEDVOLUME_SM);

    /* set kval */
    kval.len = strlen(cntrName);
    memcpy(&kval.val[0], &kval.len, sizeof(Two));
    memcpy(&kval.val[sizeof(Two)], cntrName, kval.len);
    kval.len += sizeof(Two);

    /* check given counter is exsists */
    e = BtM_Fetch(handle, &(SM_PER_THREAD_DS(handle).smMountTable[v].sysCountersCounterNameIndex), 
                  &SM_PER_THREAD_DS(handle).smSysCountersCounterNameIndexKdesc, &kval, SM_EQ, &kval, SM_EQ, &cursor);
    if (e < eNOERROR) ERR(handle, e);
    if (cursor.flag == CURSOR_EOS) ERR(handle, eCOUNTERNOTFOUND_LRDS);
    
    /* set cntrId */
    *cntrId = cursor.oid;


    return(eNOERROR);
    
} /* _SM_GetCounterId() */



/*
 * Function: Four _SM_SetCounter(Four, CounterID*, Four)
 *
 * Description:
 *  Set the counter to the given value.
 *
 * Returns:
 *  error code
 */
Four _SM_SetCounter(
    Four handle,
    Four        volId,              /* IN volume id */
    CounterID*  cntrId,             /* IN counter id */
    Four        value)              /* IN set the counter to this value */
{
    Four        e;                  /* error code */
    
    TR_PRINT(TR_LRDS, TR1, ("_SM_SetCounter(handle)"));

        
    /* Check parameters */
    if (volId < 0) ERR(handle, eBADPARAMETER);
    if (cntrId == NULL) ERR(handle, eBADPARAMETER);
    if (value < 0) ERR(handle, eBADPARAMETER);
    
    /* update counter value */
    e = OM_WriteObject(handle, (ObjectID*)cntrId, OFFSET_OF(sm_SysCountersOverlay_T, counterValue), 
                       sizeof(Four), (char*) &value);
    if (e < eNOERROR) ERR(handle, e);

    
    return(eNOERROR);
    
} /* _SM_SetCounter() */



/*
 * Function: Four _SM_ReadCounter(Four, CounterID*, Four*)
 *
 * Description:
 *  Read the current value from the counter.
 *
 * Returns:
 *  error code
 */
Four _SM_ReadCounter(
    Four handle,
    Four        volId,              /* IN volume id */
    CounterID*  cntrId,             /* IN counter id */
    Four*       value)              /* OUT the current counter value */
{
    Four        e;                  /* error code */
    
    TR_PRINT(TR_LRDS, TR1, ("_SM_ReadCounter(handle)"));


    /* Check parameters */
    if (volId < 0) ERR(handle, eBADPARAMETER);
    if (cntrId == NULL) ERR(handle, eBADPARAMETER);
    if (value == NULL) ERR(handle, eBADPARAMETER);
    
    /* update counter value */
    e = OM_ReadObject(handle, (ObjectID*)cntrId, OFFSET_OF(sm_SysCountersOverlay_T, counterValue),
                      sizeof(Four), (char*) value);
    if (e < eNOERROR) ERR(handle, e);

    
    return(eNOERROR);
    
} /* _SM_ReadCounter() */



/*
 * Function: Four _SM_GetCounterValues(Four, CounterID*, Four, Four*)
 *
 * Description:
 *  The counter allocates the given number of numbers to the user.
 *  The numbers start from the startValue and contiguous.
 *
 * Returns:
 *  error code
 */
Four _SM_GetCounterValues(
    Four handle,
    Four        volId,              /* IN volume id */
    CounterID*  cntrId,             /* IN counter id */
    Four        nValues,            /* IN number of values to be allocated */
    Four*       startValue)         /* OUT allocated numbers start from this value */
{
    Four        e;                  /* error code */
    Four        updateValue;        /* update value of counter */ 
    
    TR_PRINT(TR_LRDS, TR1, ("_SM_GetCounterValues(handle)"));


    /* get startValue from the counter */
    e = _SM_ReadCounter(handle, volId, cntrId, startValue);
    if (e < eNOERROR) ERR(handle, e);

    /* calculate updateValue */
    /* Note!! we use only zero and positive value as counter value */
    /* So, wrap around to zero if counter value is overflowed  */
    updateValue = (*startValue + nValues) & (CONSTANT_ALL_BITS_SET(Four) >> 1);

    /* update counter */
    e = _SM_SetCounter(handle, volId, cntrId, updateValue);
    if (e < eNOERROR) ERR(handle, e);
    
    
    return(eNOERROR);
    
} /* _SM_GetCounterValues() */
