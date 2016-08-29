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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "common.h"
#include "error.h"
#include "OM_Internal.h"
#include "SM_Internal.h"	


#ifdef LRDS_INCLUDED
#include "LRDS.h"
#include "perThreadDS.h"
#include "perProcessDS.h"
#endif


Four SM_FormatDataVolume(
    Four handle,
    Four   numDevices,                    /* IN number of devices in formated volume */
    char   **devNames,                    /* IN array of device name */
    char   *title,                        /* IN volume title */
    Four   volId,                         /* IN volume number */
    Two    extSize,                       /* IN number of pages in an extent */
    Four   *numPagesInDevice,             /* IN array of extents' number */
    Four   segmentSize)                   /* IN # of pages in an segment */
{
    /* check volume id is valid */
    /* Note!! temporary volume use these ids */
    if (volId & TEMP_VOLID_MASK == 1) ERR(handle, eBADPARAMETER);

    return(_SM_FormatDataVolume(handle, numDevices, devNames, title, volId, extSize, numPagesInDevice));
}

Four SM_FormatTempDataVolume(
    Four handle,
    Four   numDevices,                    /* IN number of devices in formated volume */
    char   **devNames,                    /* IN array of device name */
    char   *title,                        /* IN volume title */
    Four   volId,                         /* IN volume number */
    Two    extSize,                       /* IN number of pages in an extent */
    Four   *numPagesInDevice,             /* IN array of extents' number */
    Four   segmentSize)                   /* IN # of pages in an segment */
{
    /* change volId to temporary volId */
    volId |= TEMP_VOLID_MASK;

    return(_SM_FormatDataVolume(handle, numDevices, devNames, title, volId, extSize, numPagesInDevice));
}

Four SM_ExpandDataVolume(
    Four handle,
    Four         volNo,                   /* IN volume number which will be expanded */
    Four         numAddDevices,           /* IN number of added devices */
    char         **addDevNames,           /* IN array of device name */
    Four         *numPagesInAddDevice)    /* IN # of pages in each added devices */
{
    return(_SM_ExpandDataVolume(handle, volNo, numAddDevices, addDevNames, numPagesInAddDevice));
}

Four SM_FormatLogVolume(
    Four handle,
    Four   numDevices,                    /* IN number of devices in formated volume */
    char   **devNames,                    /* IN array of device name */
    char   *title,                        /* IN volume title */
    Four   volId,                         /* IN volume number */
    Two    extSize,                       /* IN number of pages in an extent */
    Four   *numPagesInDevice)             /* IN array of extents' number */
{
    return(_SM_FormatLogVolume(handle, numDevices, devNames, title, volId, extSize, numPagesInDevice));
}

#ifdef USE_COHERENCY_VOLUME
Four SM_FormatCoherencyVolume(
    Four handle,
    char*       devName,        /* IN  name of the coherency volume device */
    char*       title,          /* IN  volume title */
    Four        volNo           /* IN  volume id */
)
{
    Four e;

    e = BfM_FormatCoherencyVolume(handle, devName, title, volNo);
    if(e < eNOERROR) ERR(handle, e);

    return eNOERROR;
}
#endif

Four SM_AddIndex(
    Four handle,
    FileID *fid,
    IndexID *iid,
    LockParameter *lockup)
{
#ifdef DBLOCK
    Four e;

    if (lockup != NULL) {
        if(lockup->mode != L_X && lockup->mode != L_IX) ERR(handle, eEXCLUSIVELOCKREQUIRED_SM);
        if(lockup->duration != L_COMMIT) ERR(handle, eCOMMITDURATIONLOCKREQUIRED_SM);
    }

    e = SM_GetVolumeLock(handle, fid->volNo, L_X);
    if (e < eNOERROR) ERR(handle, e);
#endif

    return(_SM_AddIndex(handle, fid, iid));
}

Four SM_CloseScan(
    Four handle,
    Four scanId)
{
    return(_SM_CloseScan(handle, scanId));
}

Four SM_CloseAllScan(Four handle)
{
    printf("Sorry!! Not Implemented\n");

    ERR(handle, eINTERNAL);
}

Four SM_CreateFile(
    Four handle,
    Four   volId, 
    FileID *fid, 
    Boolean tmpFileFlag,
    LockParameter *lockup)
{
#ifdef DBLOCK
    Four e;

    if (lockup != NULL) {
        if(lockup->mode != L_X && lockup->mode != L_IX) ERR(handle, eEXCLUSIVELOCKREQUIRED_SM);
        if(lockup->duration != L_COMMIT) ERR(handle, eCOMMITDURATIONLOCKREQUIRED_SM);
    }

    e = SM_GetVolumeLock(handle, volId, L_X);
    if (e < eNOERROR) ERR(handle, e);
#endif

    return(_SM_CreateFile(handle, volId, 0, 0, fid, tmpFileFlag));
}

Four SM_CreateObject(
    Four handle,
    Four      scanId,           /* IN scan to use; specify the data file */
    ObjectID  *nearObj,         /* IN create the new object near this object */
    ObjectHdr *objHdr,          /* IN tag value of the new object's header*/
    Four      length,           /* IN amount of data */
    void      *data,            /* IN the initial data of the new object */
    ObjectID  *oid,             /* OUT the created object's ObjectID */
    LockParameter *lockup)      /* IN request lock or not */
{
    return(_SM_CreateObject(handle, scanId, nearObj, objHdr, length, (char *)data, oid));
}

Four SM_dump(Four handle)
{
    printf("Sorry!! Not Implemented\n");

    ERR(handle, eINTERNAL);
}

Four SM_DeleteIndexEntry(
    Four handle,
    IndexID *index,             /* IN index where the entry will be inserted */
    KeyDesc *kdesc,             /* IN key descriptor for B+ tree index */
    KeyValue *kval,             /* IN key value */
    ObjectID *oid,              /* IN object identifier with the given key value */
    LockParameter *lockup)      /* IN request lock or not */
{
#ifdef DBLOCK
    Four e;

    if (lockup != NULL) {
        if(lockup->mode != L_X && lockup->mode != L_IX) ERR(handle, eEXCLUSIVELOCKREQUIRED_SM);
        if(lockup->duration != L_COMMIT) ERR(handle, eCOMMITDURATIONLOCKREQUIRED_SM);
    }

    e = SM_GetVolumeLock(handle, index->volNo, L_X);
    if (e < eNOERROR) ERR(handle, e);
#endif

    return(_SM_DeleteIndexEntry(handle, index, kdesc, kval, oid));
}

Four SM_DeleteMetaDictEntry(
    Four handle,
    Four volId,                 /* IN volume identifier */
    char *name)                 /* IN name of a dictionary entry */
{
#ifdef DBLOCK
    Four e;

    e = SM_GetVolumeLock(handle, volId, L_X);
    if (e < eNOERROR) ERR(handle, e);
#endif

    return(_SM_SetMetaDictEntry(handle, volId, name, NULL, 0));
}

Four SM_DestroyFile(
    Four handle,
    FileID *fid,                /* IN file to destroy */
    LockParameter *lockup)      /* IN request lock or not */
{
#ifdef DBLOCK
    Four e;

    if (lockup != NULL) {
        if(lockup->mode != L_X && lockup->mode != L_IX) ERR(handle, eEXCLUSIVELOCKREQUIRED_SM);
        if(lockup->duration != L_COMMIT) ERR(handle, eCOMMITDURATIONLOCKREQUIRED_SM);
    }

    e = SM_GetVolumeLock(handle, fid->volNo, L_X);
    if (e < eNOERROR) ERR(handle, e);
#endif

    return(_SM_DestroyFile(handle, fid));
}

Four SM_DestroyObject(
    Four handle,
    Four     scanId,            /* IN scan to use */
    ObjectID *oid,              /* IN object to be deleted */
    LockParameter *lockup)      /* IN request lock or not */
{
    return(_SM_DestroyObject(handle, scanId, oid));
}

Four SM_Dismount(
    Four handle,
    Four volId)                 /* IN volume to dismount */
{
    return(_SM_Dismount(handle, volId));
}

Four SM_DropIndex(
    Four handle,
    IndexID *index,             /* IN index to drop */
    LockParameter *lockup)      /* IN request lock or not */
{
#ifdef DBLOCK
    Four e;

    if (lockup != NULL) {
        if(lockup->mode != L_X && lockup->mode != L_IX) ERR(handle, eEXCLUSIVELOCKREQUIRED_SM);
        if(lockup->duration != L_COMMIT) ERR(handle, eCOMMITDURATIONLOCKREQUIRED_SM);
    }

    e = SM_GetVolumeLock(handle, index->volNo, L_X);
    if (e < eNOERROR) ERR(handle, e);
#endif

    return(_SM_DropIndex(handle, index));
}

Four SM_FetchObject(
    Four handle,
    Four scanId,                /* IN scan to use */
    ObjectID *oid,              /* IN object to read */
    Four start,                 /* IN starting offset of data to read */
    Four length,                /* IN amount of data to read */
    char *data,                 /* OUT space to return the read data */
    LockParameter *lockup)      /* IN request lock or not */
{
    return(_SM_FetchObject(handle, scanId, oid, start, length, data));
}

Four SM_FinalLocalDS(Four handle)
{
    return(eNOERROR);
}

Four SM_FinalSharedDS(Four handle)
{
    return(eNOERROR);
}

Four SM_Final()
{
    Four e;

    e = _SM_Final();
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);
}

Four SM_FreeHandle(Four handle)
{
    Four e;

    e = _SM_FreeHandle(handle);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);
}

Four SM_GetMetaDictEntry(
    Four handle,
    Four volId,                 /* IN volume identifier */
    char *name,                 /* IN name of a meta dictionary entry */
    void *data,                 /* OUT value of the meta dictionary entry */
    Four dataLength)            /* IN size of storage 'data' prepared for returning value */
{
    return(_SM_GetMetaDictEntry(handle, volId, name, data, dataLength));
}

Four SM_GetObjectHdr(
    Four handle,
    Four      scanId,           /* IN scan to use */
    ObjectID  *oid,             /* IN object whose header will be returned */
    ObjectHdr *objHdr,          /* OUT returned object header */
    LockParameter *lockup)      /* IN request lock or not */
{
    return(_SM_GetObjectHdr(handle, scanId, oid, objHdr));
}

Four SM_InitLocalDS(Four handle)
{
    return(eNOERROR);
}

Four SM_InitSharedDS(Four handle)
{
    return(eNOERROR);
}

Four SM_Init()
{
    Four e;
     
    /* See _SM_Init() for 'logUseFlag' and 'logVolumeName' */
    e = _SM_Init();
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);
}

Four SM_AllocHandle(Four* handle)
{
    Four e;

    e = _SM_AllocHandle(handle);
    if (e < eNOERROR) ERR(handle, e);

    return eNOERROR;
}

Four SM_InsertIndexEntry(
    Four handle,
    IndexID  *iid,              /* IN B+ tree where the given ObjectID is inserted */
    KeyDesc  *kdesc,            /* IN key descriptor of the given B+ tree */
    KeyValue *kval,             /* IN key value of the inseted ObjectID */
    ObjectID *oid,              /* IN ObjectID to insert */
    LockParameter *lockup)      /* IN request lock or not */
{
#ifdef DBLOCK
    Four e;

    if (lockup != NULL) {
        if(lockup->mode != L_X && lockup->mode != L_IX) ERR(handle, eEXCLUSIVELOCKREQUIRED_SM);
        if(lockup->duration != L_COMMIT) ERR(handle, eCOMMITDURATIONLOCKREQUIRED_SM);
    }

    e = SM_GetVolumeLock(handle, iid->volNo, L_X);
    if (e < eNOERROR) ERR(handle, e);
#endif

    return(_SM_InsertIndexEntry(handle, iid, kdesc, kval, oid));
}

Four SM_Mount(
    Four handle,
    Four numDevices,            /* IN # of devices in the volume to be mounted */
    char **devNames,            /* IN devices' name in the volume to be mounted */
    Four *volId)                /* OUT mounted volume's volume id */
{
    return(_SM_Mount(handle, numDevices, devNames, volId));
}

Four SM_NextObject(
    Four handle,
    Four scanId,                /* IN scan to use */
    ObjectID *oid,              /* OUT the next object */
    ObjectHdr *objHdr,          /* OUT object header of the next object */
    char *extraData,            /* OUT extra data available via index */ 
    Cursor **cursor,            /* OUT the cursor */
    LockParameter *lockup)      /* IN request lock or not */
{
    return(_SM_NextObject(handle, scanId, oid, objHdr, extraData, (SM_Cursor**)cursor)); 
}

Four SM_OpenIndexScan(
    Four handle,
    FileID *fid,                /* IN data file to open */
    IndexID *iid,               /* IN index to use */
    KeyDesc *kdesc,             /* IN key descriptor of the given index */
    BoundCond *startCond,       /* IN start boundary condition of a region */
    BoundCond *stopCond,        /* IN stop boundary condition of a region */
    LockParameter *lockup)      /* IN request lock or not */
{
#ifdef DBLOCK
    Four e;

    if(lockup != NULL) {
        if(lockup->duration != L_COMMIT) ERR(handle, eCOMMITDURATIONLOCKREQUIRED_SM);

        e = SM_GetVolumeLock(handle, fid->volNo, lockup->mode);
        if (e < eNOERROR) ERR(handle, e);
    }
#endif

    return(_SM_OpenIndexScan(handle, fid, iid, kdesc, startCond, stopCond));
}

Four SM_OpenSeqScan(
    Four handle,
    FileID *fid,                /* IN file to open */
    Four   direction,           /* IN direction of the scan */
    LockParameter *lockup)      /* IN request lock or not */
{
#ifdef DBLOCK
    Four e;

    if(lockup != NULL) {
        if(lockup->duration != L_COMMIT) ERR(handle, eCOMMITDURATIONLOCKREQUIRED_SM);

        e = SM_GetVolumeLock(handle, fid->volNo, lockup->mode);
        if (e < eNOERROR) ERR(handle, e);
    }
#endif

    return(_SM_OpenSeqScan(handle, fid, direction));
}

Four SM_SetMetaDictEntry(
    Four handle,
    Four volId,                 /* IN volume identifier */
    char *name,                 /* IN name of a dictionary entry */
    void *data,                 /* IN data of the dictionary entry */
    Four dataLength)            /* IN length of data */
{
#ifdef DBLOCK
    Four e;

    e = SM_GetVolumeLock(handle, volId, L_X);
    if (e < eNOERROR) ERR(handle, e);
#endif

    return(_SM_SetMetaDictEntry(handle, volId, name, data, dataLength));
}

Four SM_SetObjectHdr(
    Four handle,
    Four      scanId,           /* IN scan to use */
    ObjectID  *oid,             /* IN object whose header will be set */
    ObjectHdr *objHdr,          /* IN from which tag field's value is given */
    LockParameter *lockup)      /* IN request lock or not */
{
    return(_SM_SetObjectHdr(handle, scanId, oid, objHdr));
}

Four SM_UpdateObject(
    Four handle,
    Four     scanId,            /* IN scan to use */
    ObjectID *oid,              /* IN object to update */
    Four     start,             /* IN start position of update */
    Four     length,            /* IN amount of data to be updated */
    void     *data,             /* IN new data with which old data is overwritten */
    Four     dataLen,           /* IN amount of new data */
    LockParameter *lockup)      /* IN request lock or not */
{
    return(_SM_UpdateObject(handle, scanId, oid, start, length, data, dataLen));
}

Four SM_MLGF_AddIndex(
    Four handle,
    FileID *fid,                /* IN data file on which an index is created */
    IndexID *iid,               /* OUT index idetifier of the new index */
    MLGF_KeyDesc *kdesc,        /* IN key descriptor of the new index */
    LockParameter *lockup)      /* IN request lock or not */
{
#ifdef DBLOCK
    Four e;

    if (lockup != NULL) {
        if(lockup->mode != L_X && lockup->mode != L_IX) ERR(handle, eEXCLUSIVELOCKREQUIRED_SM);
        if(lockup->duration != L_COMMIT) ERR(handle, eCOMMITDURATIONLOCKREQUIRED_SM);
    }

    e = SM_GetVolumeLock(handle, fid->volNo, L_X);
    if (e < eNOERROR) ERR(handle, e);
#endif

    return(_SM_MLGF_AddIndex(handle, fid, iid, kdesc)); 
}

Four SM_MLGF_DeleteIndexEntry(
    Four handle,
    IndexID *index,             /* IN index where the entry will be inserted */
    MLGF_KeyDesc *kdesc,        /* IN key descriptor for MLGF index */
    MLGF_HashValue *kval,       /* IN hash values */
    ObjectID *oid,              /* IN object identifier with the given key value */
    LockParameter *lockup)      /* IN request lock or not */
{
#ifdef DBLOCK
    Four e;

    if (lockup != NULL) {
        if(lockup->mode != L_X && lockup->mode != L_IX) ERR(handle, eEXCLUSIVELOCKREQUIRED_SM);
        if(lockup->duration != L_COMMIT) ERR(handle, eCOMMITDURATIONLOCKREQUIRED_SM);
    }

    e = SM_GetVolumeLock(handle, index->volNo, L_X);
    if (e < eNOERROR) ERR(handle, e);
#endif

    return(_SM_MLGF_DeleteIndexEntry(handle, index, kdesc, kval, oid)); 
}

Four SM_MLGF_DropIndex(
    Four handle,
    IndexID *index,             /* IN index to drop */
    LockParameter *lockup)       /* IN request lock or not */
{
#ifdef DBLOCK
    Four e;

    if (lockup != NULL) {
        if(lockup->mode != L_X && lockup->mode != L_IX) ERR(handle, eEXCLUSIVELOCKREQUIRED_SM);
        if(lockup->duration != L_COMMIT) ERR(handle, eCOMMITDURATIONLOCKREQUIRED_SM);
    }

    e = SM_GetVolumeLock(handle, index->volNo, L_X);
    if (e < eNOERROR) ERR(handle, e);
#endif


    return(_SM_MLGF_DropIndex(handle, index)); 
}

Four SM_MLGF_InsertIndexEntry(
    Four handle,
    IndexID  *iid,              /* IN MLGF index where the given ObjectID is inserted */
    MLGF_KeyDesc  *kdesc,       /* IN key descriptor of the given MLGF index */
    MLGF_HashValue *kval,       /* IN hash values of the inseted ObjectID */
    ObjectID *oid,              /* IN ObjectID to insert */
    void *data,                 /* IN additional data to store */
    LockParameter *lockup)      /* IN request lock or not */
{
#ifdef DBLOCK
    Four e;

    if (lockup != NULL) {
        if(lockup->mode != L_X && lockup->mode != L_IX) ERR(handle, eEXCLUSIVELOCKREQUIRED_SM);
        if(lockup->duration != L_COMMIT) ERR(handle, eCOMMITDURATIONLOCKREQUIRED_SM);
    }

    e = SM_GetVolumeLock(handle, iid->volNo, L_X);
    if (e < eNOERROR) ERR(handle, e);
#endif


    return(_SM_MLGF_InsertIndexEntry(handle, iid, kdesc, kval, oid, data)); 
}

Four SM_MLGF_OpenIndexScan(
    Four handle,
    FileID *fid,                /* IN data file to open */
    IndexID *iid,               /* IN index to use */
    MLGF_KeyDesc *kdesc,        /* IN key descriptor of the given index */
    MLGF_HashValue *lowerBound, /* IN start boundary condition of a region */
    MLGF_HashValue *upperBound, /* IN stop boundary condition of a region */
    LockParameter *lockup)      /* IN request lock or not */
{
#ifdef DBLOCK
    Four e;

    if(lockup != NULL) {
        if(lockup->duration != L_COMMIT) ERR(handle, eCOMMITDURATIONLOCKREQUIRED_SM);

        e = SM_GetVolumeLock(handle, fid->volNo, lockup->mode);
        if (e < eNOERROR) ERR(handle, e);
    }
#endif

    return(_SM_MLGF_OpenIndexScan(handle, fid, iid, kdesc, lowerBound, upperBound)); 
}

Four SM_MLGF_SearchNearObject(
    Four handle,
    IndexID  *iid,              /* IN MLGF index where the given ObjectID is inserted */
    MLGF_KeyDesc  *kdesc,       /* IN key descriptor of the given MLGF index */
    MLGF_HashValue kval[],      /* IN hash values of the new object */
    ObjectID *oid,              /* OUT ObjectID of the near object */
    LockParameter *lockup)      /* IN request lock or not */
{
#ifdef DBLOCK
    Four e;

    if (lockup != NULL) {
        if (lockup->mode != L_S) ERR(handle, eEXCLUSIVELOCKREQUIRED_SM);
        if(lockup->duration != L_COMMIT) ERR(handle, eCOMMITDURATIONLOCKREQUIRED_SM);

        e = SM_GetVolumeLock(handle, iid->volNo, L_S);
        if (e < eNOERROR) ERR(handle, e);
    }
#endif

    return(_SM_MLGF_SearchNearObject(handle, iid, kdesc, kval, oid)); 
}

Four SM_SetCfgParam(
    Four handle,
    char *name,                 /* IN parameter name */
    char *value)                /* IN parameter value */
{
    return(_SM_SetCfgParam(handle, name, value));
}

char* SM_GetCfgParam(
    Four handle, 
    char *name)                 /* IN parameter name */
{
    return(_SM_GetCfgParam(handle, name));
}

Four SM_SortFile(
    Four handle,
    VolID  tmpVolId,                    /* IN temporary volume in which sort stream is created */ 
    FileID *inFid,                      /* IN file to sort */
    SortKeyDesc *kdesc,                 /* IN sort key description */
    GetKeyAttrsFuncPtr_T getKeyAttrsFn, /* IN tuple analysis function */
    void *schema,                       /* IN schema for analysis function */
    Boolean newFileFlag,                /* IN whether we make new file for sort result */
    Boolean tmpFileFlag,                /* IN new file is a temporary file? */
    FileID *outFid,                     /* OUT new file storing sort result */
    LockParameter *lockup)              /* IN lockup parameter for data volume */
{
#ifdef DBLOCK
    Four e;

    /* locking for data volume */
    if (lockup != NULL) {
        if(lockup->mode != L_X && lockup->mode != L_IX) ERR(handle, eEXCLUSIVELOCKREQUIRED_SM);
        if(lockup->duration != L_COMMIT) ERR(handle, eCOMMITDURATIONLOCKREQUIRED_SM);
    }

    e = SM_GetVolumeLock(handle, inFid->volNo, L_X);
    if (e < eNOERROR) ERR(handle, e);

    /* locking for temporary volume */
    e = SM_GetVolumeLock(handle, tmpVolId, L_X);
    if (e < eNOERROR) ERR(handle, e);
#endif

    return(_SM_SortFile(handle, tmpVolId, inFid, kdesc, getKeyAttrsFn, schema, newFileFlag, tmpFileFlag, outFid)); 
}


Four SM_BeginTransaction(
    Four handle,
    XactID *xactId,             /* OUT transaction id of the newly started transaction */
    ConcurrencyLevel ccLevel)   /* IN concurrency level of this transaction */ 
{
    Four e;                     /* error code */ 
    Boolean rollbackRequiredFlag;
    extern CfgParams_T sm_cfgParams;
    
    xactId = &SM_PER_THREAD_DS(handle).dummy;

#ifdef DBLOCK
    if (ccLevel == X_BROWSE_BROWSE) {
        e = SM_SetUseShareLockFlag(handle, FALSE);
        if (e < eNOERROR) ERR(handle, e);
    }
    else {
        e = SM_SetUseShareLockFlag(handle, TRUE);
        if (e < eNOERROR) ERR(handle, e);
    }
#endif

    rollbackRequiredFlag = (sm_cfgParams.logVolumeDeviceList == NULL || strcmp(sm_cfgParams.logVolumeDeviceList, "") == 0) ? FALSE:TRUE; 
    
    return(_SM_BeginTransaction(handle, rollbackRequiredFlag));
}

Four SM_CommitTransaction(
    Four handle,
    XactID *xactId)             /* IN transaction to commit */
{
    Four e;

    e = _SM_CommitTransaction(handle);
    if (e < eNOERROR) ERR(handle, e);

#ifdef DBLOCK
    e = SM_ReleaseLogVolumeLock(handle);
    if (e < eNOERROR) ERR(handle, e);

    e = SM_ReleaseAllVolumeLock(handle);
    if (e < eNOERROR) ERR(handle, e);
#endif

    /* ShareLockFlag을 원상태로 돌려놓는다. */ 
    /* Restore the ShareLockFlag to original state. */
    e = SM_SetUseShareLockFlag(handle, TRUE);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);
}

Four SM_AbortTransaction(
    Four handle,
    XactID *xactId)             /* IN transaction to abort */
{
    Four e;

    e = _SM_AbortTransaction(handle);
    if (e < eNOERROR) ERR(handle, e);

#ifdef DBLOCK
    e = SM_ReleaseLogVolumeLock(handle);
    if (e < eNOERROR) ERR(handle, e);

    e = SM_ReleaseAllVolumeLock(handle);
    if (e < eNOERROR) ERR(handle, e);
#endif

    /* ShareLockFlag을 원상태로 돌려놓는다. */ 
    /* Restore the ShareLockFlag to original state. */
    e = SM_SetUseShareLockFlag(handle, TRUE);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);
}



Four SM_CreateObjectWithoutScan(
    Four handle,
    FileID    *fid,             /* IN file where the object is inserted */
    ObjectID  *nearObj,         /* IN create the new object near this object */
    ObjectHdr *objHdr,          /* IN tag value of the new object's header*/
    Four      length,           /* IN amount of data */
    char      *data,            /* IN the initial data of the new object */
    ObjectID  *oid,             /* OUT the created object's ObjectID */
    LockParameter *fileLockup,  /* IN request file lock or not */
    LockParameter *objLockup)   /* IN request object lock or not */
{
#ifdef DBLOCK
    Four e;

    if (fileLockup != NULL || objLockup != NULL) {
       
        if (fileLockup != NULL) {
            if(fileLockup->mode != L_X && fileLockup->mode != L_IX && 
               fileLockup->mode != L_SIX) ERR(handle, eEXCLUSIVELOCKREQUIRED_SM);
            if(fileLockup->duration != L_COMMIT) ERR(handle, eCOMMITDURATIONLOCKREQUIRED_SM);
        }
        if (objLockup != NULL) {
            if(objLockup->mode != L_X) ERR(handle, eEXCLUSIVELOCKREQUIRED_SM);
            if(objLockup->duration != L_COMMIT) ERR(handle, eCOMMITDURATIONLOCKREQUIRED_SM);
        }
    }

    e = SM_GetVolumeLock(handle, fid->volNo, L_X);
    if (e < eNOERROR) ERR(handle, e);
#endif

    return (_SM_CreateObjectWithoutScan(handle, fid, nearObj, objHdr, length, data, oid));
}

Four SM_DestroyObjectWithoutScan(
    Four handle,
    FileID    *fid,             /* IN file where the object is inserted */
    ObjectID *oid,              /* IN object to be deleted */
    LockParameter *fileLockup,  /* IN request file lock or not */
    LockParameter *objLockup)   /* IN request object lock or not */
{
#ifdef DBLOCK
    Four e;

    if (fileLockup != NULL || objLockup != NULL) {

        if (fileLockup != NULL) {
            if(fileLockup->mode != L_X && fileLockup->mode != L_IX &&
               fileLockup->mode != L_SIX) ERR(handle, eEXCLUSIVELOCKREQUIRED_SM);
            if(fileLockup->duration != L_COMMIT) ERR(handle, eCOMMITDURATIONLOCKREQUIRED_SM);
        }
        if (objLockup != NULL) {
            if(objLockup->mode != L_X) ERR(handle, eEXCLUSIVELOCKREQUIRED_SM);
            if(objLockup->duration != L_COMMIT) ERR(handle, eCOMMITDURATIONLOCKREQUIRED_SM);
        }
    }

    e = SM_GetVolumeLock(handle, fid->volNo, L_X);
    if (e < eNOERROR) ERR(handle, e);
#endif

    return (_SM_DestroyObjectWithoutScan(handle, fid, oid));
}

Four SM_FetchObjectWithoutScan(
    Four handle,
    FileID    *fid,             /* IN file where the object is inserted */
    ObjectID *oid,              /* IN object to read */
    Four start,                 /* IN starting offset of data to read */
    Four length,                /* IN amount of data to read */
    char *data,                 /* OUT space to return the read data */
    LockParameter *fileLockup,  /* IN request file lock or not */
    LockParameter *objLockup)   /* IN request object lock or not */
{
#ifdef DBLOCK
    Four e;

    if (fileLockup != NULL || objLockup != NULL) {

        if (fileLockup != NULL) {
            if(fileLockup->duration != L_COMMIT) ERR(handle, eCOMMITDURATIONLOCKREQUIRED_SM);
        }
        if (objLockup != NULL) {
            if(objLockup->duration != L_COMMIT) ERR(handle, eCOMMITDURATIONLOCKREQUIRED_SM);
        }

        e = SM_GetVolumeLock(handle, fid->volNo, L_S);
        if (e < eNOERROR) ERR(handle, e);
    }
#endif

    return (_SM_FetchObjectWithoutScan(handle, fid, oid, start, length, data));
}

Four SM_UpdateObjectWithoutScan(
    Four handle,
    FileID    *fid,             /* IN file where the object is inserted */
    ObjectID *oid,              /* IN object to update */
    Four     start,             /* IN start position of update */
    Four     length,            /* IN amount of data to be updated */
    void     *data,             /* IN new data with which old data is overwritten */
    Four     dataLen,           /* IN amount of new data */
    LockParameter *fileLockup,  /* IN request file lock or not */
    LockParameter *objLockup)   /* IN request object lock or not */
{
#ifdef DBLOCK
    Four e;

    if (fileLockup != NULL || objLockup != NULL) {

        if (fileLockup != NULL) {
            if(fileLockup->mode != L_X && fileLockup->mode != L_IX &&
               fileLockup->mode != L_SIX) ERR(handle, eEXCLUSIVELOCKREQUIRED_SM);
            if(fileLockup->duration != L_COMMIT) ERR(handle, eCOMMITDURATIONLOCKREQUIRED_SM);
        }
        if (objLockup != NULL) {
            if(objLockup->mode != L_X) ERR(handle, eEXCLUSIVELOCKREQUIRED_SM);
            if(objLockup->duration != L_COMMIT) ERR(handle, eCOMMITDURATIONLOCKREQUIRED_SM);
        }
    }

    e = SM_GetVolumeLock(handle, fid->volNo, L_X);
    if (e < eNOERROR) ERR(handle, e);
#endif

    return (_SM_UpdateObjectWithoutScan(handle, fid, oid, start, length, data, dataLen));
}

Four SM_WriteObjectRedoOnlyWithoutScan(
    Four handle,
    FileID    *fid,             /* IN file where the object is inserted */
    ObjectID *oid,              /* IN object to read */
    Four start,                 /* IN starting offset of data to read */
    Four length,                /* IN amount of data to read */
    char *data,                 /* IN data to write */
    LockParameter *fileLockup,  /* IN request file lock or not */
    LockParameter *objLockup)   /* IN request object lock or not */
{
#ifdef DBLOCK
    Four e;

    if (fileLockup != NULL || objLockup != NULL) {

        if (fileLockup != NULL) {
            if(fileLockup->mode != L_X && fileLockup->mode != L_IX &&
               fileLockup->mode != L_SIX) ERR(handle, eEXCLUSIVELOCKREQUIRED_SM);
            if(fileLockup->duration != L_COMMIT) ERR(handle, eCOMMITDURATIONLOCKREQUIRED_SM);
        }
        if (objLockup != NULL) {
            if(objLockup->mode != L_X) ERR(handle, eEXCLUSIVELOCKREQUIRED_SM);
            if(objLockup->duration != L_COMMIT) ERR(handle, eCOMMITDURATIONLOCKREQUIRED_SM);
        }
    }

    e = SM_GetVolumeLock(handle, fid->volNo, L_X);
    if (e < eNOERROR) ERR(handle, e);
#endif

    return (_SM_WriteObjectRedoOnlyWithoutScan(handle, fid, oid, start, length, data));
}

Four SM_InsertMetaDictEntry(
    Four handle,
    Four volId,                 /* IN volume identifier */
    char *name,                 /* IN name of a dictionary entry */
    void *data,                 /* IN data of the dictionary entry */
    Four dataLength)            /* IN length of data */
{
#ifdef DBLOCK
    Four e;

    e = SM_GetVolumeLock(handle, volId, L_X);
    if (e < eNOERROR) ERR(handle, e);
#endif

    return (_SM_InsertMetaDictEntry(handle, volId, name, data, dataLength));
}



Four SM_InitDataFileBulkLoad(
    Four handle,
    VolID                  tmpVolId,            /* IN  temporary volume in which sort stream is created */ 
    FileID                 *fid,                /* IN  file that data bulk load is to be processed */
    SortKeyDesc            *kdesc,              /* IN  sort key description */
    GetKeyAttrsFuncPtr_T   getKeyAttrs,         /* IN  object analysis function */
    void                   *schema,             /* IN  schema for analysis function */
    Boolean                isNeedSort,          /* IN  flag indicating input data must be sorted by clustering index key */
    Two                    pff,                 /* IN  Page fill factor */
    Two                    eff,                 /* IN  Extent fill factor */
    PageID                 *firstPageID,        /* OUT first page ID of this bulkload */
    LockParameter          *lockup)             /* IN lockup parameter for data volume */
{
#ifdef DBLOCK
    Four e;

    /* locking for data volume */
    if (lockup != NULL) {
        if(lockup->mode != L_X && lockup->mode != L_IX) ERR(handle, eEXCLUSIVELOCKREQUIRED_SM);
        if(lockup->duration != L_COMMIT) ERR(handle, eCOMMITDURATIONLOCKREQUIRED_SM);
    }

    e = SM_GetVolumeLock(handle, fid->volNo, L_X);
    if (e < eNOERROR) ERR(handle, e);

	/* locking for temporary volume */
    e = SM_GetVolumeLock(handle, tmpVolId, L_X);
    if (e < eNOERROR) ERR(handle, e);
#endif

    return (_SM_InitDataFileBulkLoad(handle, tmpVolId, fid, kdesc, getKeyAttrs, schema, isNeedSort, pff, eff, firstPageID)); 
}

Four SM_NextDataFileBulkLoad(
    Four handle,
    Four                blkLdId,                /* IN  bulkload ID */
    char                *objectBuffer,          /* IN  buffer containing object data */
    Four                objectBufferLen,        /* IN  size of data in buffer */
    Boolean             endOfObject,            /* IN  flag indicating this buffer is end of object or not */
    ObjectID            *oid)                   /* OUT object ID */
{
    return (_SM_NextDataFileBulkLoad(handle, blkLdId, objectBuffer, objectBufferLen, endOfObject, oid));
}

Four SM_NextDataFileBulkLoadWriteLOT(
    Four handle,
    Four                blkLdId,                /* IN  bulkload ID */
    Four                start,                  /* IN  starting offset of read */
    Four                length,                 /* IN  amount of data to read */
    char*               data,                   /* IN  user buffer holding the data */
    Boolean             endOfObject,            /* IN  flag indicating this buffer is end of object */
    ObjectID            *oid)                   /* OUT object ID */
{
    return (_SM_NextDataFileBulkLoadWriteLOT(handle, blkLdId, start, length, data, endOfObject, oid));
}

Four SM_FinalDataFileBulkLoad(
    Four handle,
    Four                blkLdId)                /* IN  bulkload ID */
{
    return (_SM_FinalDataFileBulkLoad(handle, blkLdId));
}


Four SM_InitIndexBulkLoad (
    Four handle,
    VolID               tmpVolId,               /* IN volume ID in which temporary files are allocated */
    KeyDesc             *kdesc)                 /* IN key descriptor of the given B+ tree */
{
#ifdef DBLOCK
    Four e;

	/* locking for temporary volume */
    e = SM_GetVolumeLock(handle, tmpVolId, L_X);
    if (e < eNOERROR) ERR(handle, e);
#endif

    return (_SM_InitIndexBulkLoad(handle, tmpVolId, kdesc));
}

Four SM_NextIndexBulkLoad (
    Four handle,
    Four                blkLdId,                /* IN index bulkload id */
    KeyValue            *key,
    ObjectID            *oid)
{
    return (_SM_NextIndexBulkLoad(handle, blkLdId, key, oid));
}

Four SM_FinalIndexBulkLoad (
    Four handle,
    Four                blkLdId,                /* IN index bulkload id */
    IndexID             *iid,                   /* IN B+ tree where the given ObjectID is inserted */
    Two                 eff,                    /* IN Extent fill factor */
    Two                 pff,                    /* IN Page fill factor */
    LockParameter       *lockup)                /* IN lockup parameter for data volume */
{
#ifdef DBLOCK
    Four e;

    /* locking for data volume */
    if (lockup != NULL) {
        if(lockup->mode != L_X && lockup->mode != L_IX) ERR(handle, eEXCLUSIVELOCKREQUIRED_SM);
        if(lockup->duration != L_COMMIT) ERR(handle, eCOMMITDURATIONLOCKREQUIRED_SM);
    }

    e = SM_GetVolumeLock(handle, iid->volNo, L_X);
    if (e < eNOERROR) ERR(handle, e);
#endif

    return (_SM_FinalIndexBulkLoad(handle, blkLdId, iid, eff, pff));
}


Four SM_InitSortedIndexBulkLoad (
    Four handle,
    IndexID             *iid,                   /* IN B+ tree where the given ObjectID is inserted */
    KeyDesc             *kdesc,                 /* IN key descriptor of the given B+ tree */
    Two                 eff,                    /* IN Extent fill factor */
    Two                 pff,                    /* IN Page fill factor */
    LockParameter       *lockup)                /* IN lockup parameter for data volume */
{
#ifdef DBLOCK
    Four e;

    /* locking for data volume */
    if (lockup != NULL) {
        if(lockup->mode != L_X && lockup->mode != L_IX) ERR(handle, eEXCLUSIVELOCKREQUIRED_SM);
        if(lockup->duration != L_COMMIT) ERR(handle, eCOMMITDURATIONLOCKREQUIRED_SM);
    }

    e = SM_GetVolumeLock(handle, iid->volNo, L_X);
    if (e < eNOERROR) ERR(handle, e);
#endif

    return (_SM_InitSortedIndexBulkLoad(handle, iid, kdesc, eff, pff));
}

Four SM_NextSortedIndexBulkLoad (
    Four handle,
    Four                blkLdId,                /* IN  bulkload ID */
    KeyValue            *key,                   /* IN key value of the inseted ObjectID */
    ObjectID            *oid)                   /* IN ObjectID to insert */
{
    return (_SM_NextSortedIndexBulkLoad(handle, blkLdId, key, oid));
}

Four SM_FinalSortedIndexBulkLoad (
    Four handle,
    Four                blkLdId)                /* IN  bulkload ID */
{
    return (_SM_FinalSortedIndexBulkLoad(handle, blkLdId));
}


Four SM_InitIndexBulkInsert (
    Four handle,
    VolID               tmpVolId,               /* IN volume ID in which temporary files are allocated */
    KeyDesc             *kdesc)                 /* IN key descriptor of the given B+ tree */
{
#ifdef DBLOCK
    Four e;

    /* locking for temporary volume */
    e = SM_GetVolumeLock(handle, tmpVolId, L_X);
    if (e < eNOERROR) ERR(handle, e);
#endif

    return (_SM_InitIndexBulkInsert(handle, tmpVolId, kdesc));
}

Four SM_NextIndexBulkInsert (
    Four handle,
    Four                blkLdId,                /* IN index bulkload id */
    KeyValue            *key,
    ObjectID            *oid)
{
    return (_SM_NextIndexBulkInsert(handle, blkLdId, key, oid));
}

Four SM_FinalIndexBulkInsert (
    Four handle,
    Four                blkLdId,                /* IN index bulkload id */
    IndexID             *iid,                   /* IN B+ tree where the given ObjectID is inserted */
    KeyDesc*            kdesc,                  /* IN Page fill factor */
    LockParameter       *lockup)                /* IN lockup parameter for data volume */
{
#ifdef DBLOCK
    Four e;

    /* locking for data volume */
    if (lockup != NULL) {
        if(lockup->mode != L_X && lockup->mode != L_IX) ERR(handle, eEXCLUSIVELOCKREQUIRED_SM);
        if(lockup->duration != L_COMMIT) ERR(handle, eCOMMITDURATIONLOCKREQUIRED_SM);
    }

    e = SM_GetVolumeLock(handle, iid->volNo, L_X);
    if (e < eNOERROR) ERR(handle, e);
#endif

    return (_SM_FinalIndexBulkInsert(handle, blkLdId, iid, kdesc));
}



Four SM_CreateCounter(
    Four handle,
    Four       volId,               /* IN volume id */
    char*      cntrName,            /* IN counter name */
    Four       initialValue,        /* IN initialize the counter as this value */
    CounterID* cntrId)              /* OUT counter id */
{
    Four e;

#ifdef DBLOCK
    e = SM_GetVolumeLock(handle, volId, L_X);
    if (e < eNOERROR) ERR(handle, e);
#endif

    return(_SM_CreateCounter(handle, volId, cntrName, initialValue, cntrId));
}

Four SM_DestroyCounter(
    Four handle,
    Four        volId,              /* IN volume id */
    char*       cntrName)           /* IN counter name */
{
    Four e;

#ifdef DBLOCK
    e = SM_GetVolumeLock(handle, volId, L_X);
    if (e < eNOERROR) ERR(handle, e);
#endif

    return(_SM_DestroyCounter(handle, volId, cntrName));
}

Four SM_GetCounterId(
    Four handle,
    Four        volId,              /* IN volume id */
    char*       cntrName,           /* IN counter name */
    CounterID*  cntrId)             /* OUT counter id */
{
    return(_SM_GetCounterId(handle, volId, cntrName, cntrId));
}

Four SM_SetCounter(
    Four handle,
    Four        volId,              /* IN volume id */
    CounterID*  cntrId,             /* IN counter id */
    Four        value)              /* IN set the counter to this value */
{
    Four e;

#ifdef DBLOCK
    e = SM_GetVolumeLock(handle, volId, L_X);
    if (e < eNOERROR) ERR(handle, e);
#endif

    return(_SM_SetCounter(handle, volId, cntrId, value));
}

Four SM_ReadCounter(
    Four handle,
    Four        volId,              /* IN volume id */
    CounterID*  cntrId,             /* IN counter id */
    Four*       value)              /* OUT the current counter value */
{
    return(_SM_ReadCounter(handle, volId, cntrId, value));
}

Four SM_GetCounterValues(
    Four handle,
    Four        volId,              /* IN volume id */
    CounterID*  cntrId,             /* IN counter id */
    Four        nValues,            /* IN number of values to be allocated */
    Four*       startValue)         /* OUT allocated numbers start from this value */
{
    Four e;

#ifdef DBLOCK
    e = SM_GetVolumeLock(handle, volId, L_X);
    if (e < eNOERROR) ERR(handle, e);
#endif

    return(_SM_GetCounterValues(handle, volId, cntrId, nValues, startValue));
}

Four SM_EnterTwoPhaseCommit(
    Four handle,
    XactID* xactId, 
    GlobalXactID* globalXactId) {

    return (_SM_EnterTwoPhaseCommit(handle, xactId, globalXactId));
}

Four SM_GetNumberOfPreparedTransactions(
    Four handle,
    Four* num)
{

    return (_SM_GetNumberOfPreparedTransactions(handle, num));
}

Four SM_GetPreparedTransactions(
    Four handle,
    Four num, 
    GlobalXactID globalXactId[])
{

    return (_SM_GetPreparedTransactions(handle, num, globalXactId));
}

Four SM_PrepareTransaction(
    Four handle,
    XactID* xactId) 
{

    return (_SM_PrepareTransaction(handle, xactId));

}

Four SM_RecoverTwoPhaseCommit(
    Four handle,
    GlobalXactID* globalXactId, 
    XactID* xactId)
{

    return (_SM_RecoverTwoPhaseCommit(handle, globalXactId, xactId));

}


Four SM_IsReadOnlyTransaction(
    Four handle,
    XactID* xactId, 
    Boolean* flag)
{

    return (_SM_IsReadOnlyTransaction(handle, xactId, flag));

}


