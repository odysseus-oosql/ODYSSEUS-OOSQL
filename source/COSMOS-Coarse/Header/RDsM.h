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
#ifndef _RDsM_H_
#define _RDsM_H_


#define NUM_EXTS_FOR_MASTER_PAGES 1  


/*
 *  Volume information
 */
typedef struct {
    char                title[MAXVOLTITLE];     /* volume title */
    Four                volNo;                  /* volume number */
    Four                numDevices;             /* number of devices in the volume */
    Four                sizeOfExt;              /* size of a extent (the number of pages) */
    Four                numOfExts;              /* number of extents in this volume */
    Four                numOfFreeExts;          /* number of free extents in this volume */
} RDsM_VolInfo; 


/*@
 * Function Prototypes
 */
/* function prototypes for upper layers */

Four	RDsM_AllocTrains(Four, Four, Four, PageID *, Two, Four, Two, PageID *);
Four    RDsM_AllocContigTrainsInExt(Four, Four, Four, PageID*, Two, Four*, Two, PageID*); 
Four	RDsM_CreateSegment(Four, Four, Four *);
Four    RDsM_DeleteMetaEntry(Four, Four, char*);
Four	RDsM_Dismount(Four, Four);
Four	RDsM_DropSegment(Four, Four, Four);
Four	RDsM_ExtNoToPageId(Four, Four, Four, PageID *); 
Four	RDsM_Finalize(Four);
Four	RDsM_Format(Four, Four, char **, char *, Four, Two, Four *);
Four    RDsM_ExpandVolume(Four, Four, Four, char**, Four *);
Four	RDsM_FreeTrain(Four, PageID *, Two);
Four    RDsM_GetMetaEntry(Four, Four, char*, void*, Two);
Four    RDsM_GetUnique(Four, PageID*, Unique*, Four*);
Four	RDsM_GetVolNo(Four, char *, Four *);
Four	RDsM_GetAllMountedVolNos(Four, Four *);
Four	RDsM_Initialize(Four);
Four    RDsM_InsertMetaEntry(Four, Four, char*, char*, Two);
Four	RDsM_Mount(Four, Four, char**, Four *);
Four	RDsM_PageIdToExtNo(Four, PageID *, Four *);
Four	RDsM_ReadTrain(Four, PageID *, char *, Two);
Four	RDsM_WriteTrain(Four, char *, PageID *, Two);
Four	RDsM_ReadTrainForLogVolume(Four, PageID *, char *, Two);
Four	RDsM_WriteTrainForLogVolume(Four, char *, PageID *, Two);
Four	RDsM_ReadTrainsForLogVolume(Four, PageID*, char*, Four, Two);
Four	RDsM_WriteTrainsForLogVolume(Four, char*, PageID*, Four, Two);
Four	RDsM_ReadTrains(Four, PageID *, char *, Four, Two); 
Four	RDsM_WriteTrains(Four, char *, PageID *, Four, Two); 
Four    RDsM_GetMountedVolumes(Four, Four, char *); 
Four    RDsM_GetStatistics_numExtents(Four, Four, Two*, Four*, Four*);
Four	RDsM_GetStatistics_numPages(Four, Four, sm_NumPages*, Boolean, Boolean); 
Four	RDsM_AbortTransaction(Four); 
Four    RDsM_GetVolumeInfo(Four, Four, RDsM_VolInfo*); 
Four    RDsM_GetSizeOfExt(Four, Four, Two *);

#ifdef DBLOCK
Four RDsM_GetVolumeLock(Four, Four, LockMode, Boolean);
Four RDsM_ReleaseVolumeLock(Four, Four);
Four RDsM_ReleaseAllVolumeLock(Four);
Four RDsM_SetUseShareLockFlag(Four, Boolean);
Boolean RDsM_GetUseShareLockFlag(Four);
Boolean RDsM_CheckVolumeLock(Four, Four, LockMode);
LockMode RDsM_GetVolumeLockMode(Four, Four);
#endif

#endif /* _RDsM_H_ */
