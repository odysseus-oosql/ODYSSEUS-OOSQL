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
#ifndef	__RDSM_INTERNAL_H__
#define	__RDSM_INTERNAL_H__

#include	<fcntl.h>
#include	<stdio.h>

#ifdef COSMOS_MULTITHREAD
#include 	<pthread.h>
#include 	"THM_lock.h"
#endif


#define NUM_EXTS_FOR_MASTER_PAGES 1  


/*@
 * Constant Definitions
 */
/* empty volume entry */
#define	NOVOL		-1		

#undef NULL
/* null value */
#define	NULL	0

/* maximum number of trains which can be read or written in a time */
#define MAXNUMTRAINS  64   

/* Defined for UNIX system calls */
#define	PERM	 0600	/* permission bits for file open */
#define FROM_SET 0     	/* from the starting position */
#define FROM_CUR 1     	/* from the current position */
#define FROM_END 2     	/* from the last position */


#define MASTERPAGESIZE 1
#define VOLINFOSIZE    1
#define METADICTSIZE   1


/*
 * Type Defintions
 */

/* The structure of a volume information page */



/*
 * ExtEntryPage
 */
typedef struct {
    PageID pid;                 /* page id of this page, should be located on the beginning */    
    Four flags;                 /* flag to store page information */
    Four reserved;              /* reserved space to store page information */
} ExtEntryPageHdr;

/* The structure of an extent entry */

typedef	struct	{
    Two		eff;
    Four	prevExt;
    Four	nextExt;
} ExtEntry;

/* number of extent links in a page */
#define EXTENTRYPERPAGE	((PAGESIZE-sizeof(ExtEntryPageHdr))/sizeof(ExtEntry)) 
                         
typedef struct	{
    ExtEntryPageHdr hdr;
    ExtEntry 	el[EXTENTRYPERPAGE];
} ExtEntryPage;


/*
 * The structure of map page
 */
typedef struct {
    PageID pid;                 /* page id of this page, should be located on the beginning */    
    Four flags;                 /* flag to store page information */
    Four reserved;              /* reserved space to store page information */
} MapPageHdr;

#define BITMAP_USABLEBYTESPERPAGE (PAGESIZE-sizeof(MapPageHdr))
                           
typedef struct	{
    MapPageHdr hdr;
    char 	mp[BITMAP_USABLEBYTESPERPAGE];
} MapPage;


/* 
 * The structure for device information in volume information table 
 */

typedef struct {
    char	devName[MAXDEVNAME];	/* device name */
    FileDesc	devAddr;		/* file descriptor for raw device */
    Four        firstExtNo;             /* first extent number of this device */
    Four	bitMapSize;		/* size of page map (in number of pages) */
    Four	extEntryArraySize;	/* size of extent link (in number of pages) */
    Four	uniqNumSize;		/* size of unique number */
    PageID	bitMapPageId;		/* first page identifier of the page allocation map */
    PageID	extEntryArrayPageId;	/* first page identifier of the extent link */
    PageID	uniqNumPageId;		/* first page identifier of the unique number pages */
} DevInfo;

#define DEVINFO_ARRAY(devInfoArray)  ((DevInfo *)((devInfoArray).ptr))

#define NUM_PAGES_IN_DEVICE(v, devNo) ((devNo) >= (v)->numDevices-1) ? \
                                      ((v)->numOfExts - DEVINFO_ARRAY((v)->devInfo)[devNo].firstExtNo)*(v)->sizeOfExt : \
                                      (DEVINFO_ARRAY((v)->devInfo)[devNo+1].firstExtNo - DEVINFO_ARRAY((v)->devInfo)[devNo].firstExtNo)*(v)->sizeOfExt
#define NUM_PAGES_IN_LOG_DEVICE(v, devNo) ((devNo) >= (v)->numDevices-1) ? \
                                          ((v)->numOfExts - DEVINFO_ARRAY((v)->devInfo)[devNo].firstExtNo - NUM_EXTS_FOR_MASTER_PAGES)*(v)->sizeOfExt : \
                                          (DEVINFO_ARRAY((v)->devInfo)[devNo+1].firstExtNo - DEVINFO_ARRAY((v)->devInfo)[devNo].firstExtNo - NUM_EXTS_FOR_MASTER_PAGES)*(v)->sizeOfExt


/* The structure of a volume information table */

typedef struct {
    char	title[MAXVOLTITLE]; 	/* volume title */
    VolNo	volNo;			/* volume number */
    Two 	sizeOfExt;		/* size of a extent (the number of pages) */
    Four	numOfExts;		/* number of extents in this volume */
    Four	numOfFreeExts;		/* number of free extents in this volume */
    Four	firstFreeExt;		/* the first free extent number */
    Two         volInfoSize;            /* size of volume information (in number of pages) */
    Two         metaDicSize;            /* size of meta dictionary (in number of pages) */
    PageID	volInfoPageId;		/* first page identifier of the volume information pages */
    PageID	metaDicPageId;		/* first page identifier of the meta dictionary pages */
#ifdef DBLOCK
    LockMode    lockMode;		/* mode of acquired lock */
#endif
    Four        numDevices;
    VarArray    devInfo;                /* information of each device */ 
#ifdef COSMOS_MULTITHREAD
    cosmos_thread_rwlock_t rwlock;	/* volume lock for threads in a process */
#endif
} VolumeTable;	

/* constant to indicate numOfFreeExts & firstFreeExt isn't assigned yet */
#define NOT_ASSIGNED  -4444


/*
 * Device Master Page which contains information about each device 
 */

typedef struct {
    PageID pid;                 /* page id of this page, should be located on the beginning */    
    Four flags;                 /* flag to store page information */
    Four reserved;              /* reserved space to store page information */
} DevMasterPageHdr;

/* the fixed size of the device master page */
#define DM_FIXED        (sizeof(DevMasterPageHdr) + MAX_VOLUME_TAG_SIZE + \
                         sizeof(Four) + sizeof(Four) + sizeof(Four) + sizeof(Four) + \
                         sizeof(Four) + sizeof(Four) + sizeof(Four) + 4*sizeof(PageID))

#define DEVICE_TAG "### FORMATTED DEVICE ###"

typedef struct {
    DevMasterPageHdr hdr;
    Four        volNo;			
    Four        devNo;
    Four	numOfExtsInDevice;
    Four        volInfoSize;		
    Four	bitMapSize;
    Four	extEntryArraySize;
    Four	uniqNumSize;
    PageID	volInfoPageId;
    PageID	bitMapPageId;	
    PageID	extEntryArrayPageId;
    PageID	uniqNumPageId;
    char        tag[MAX_VOLUME_TAG_SIZE];
    char	dummy[PAGESIZE-DM_FIXED]; /* extra space for filling one page */
} DevMasterPage;


/*
 * Volume Information Page
 */

typedef struct {
    PageID pid;                 /* page id of this page, should be located on the beginning */    
    Four flags;                 /* flag to store page information */
    Four reserved;              /* reserved space to store page information */
} VolInfoPageHdr;

/* the fixed size of the volume information page */
#define VI_FIXED        (sizeof(VolInfoPageHdr) + MAXVOLTITLE + sizeof(Four) + sizeof(Four) + sizeof(Four) + \
                         sizeof(Four) + sizeof(Four) + sizeof(Four) + sizeof(PageID))

typedef struct {
    VolInfoPageHdr 	hdr;
    Four       		volNo;			
    Four        	numDevices;
    Four         	sizeOfExt;		
    Four		numOfFreeExts;
    Four		firstFreeExt;
    Four         	metaDicSize;		
    PageID		metaDicPageId;
    char		title[MAXVOLTITLE];  /* volume title */
    char		dummy[PAGESIZE-VI_FIXED]; /* extra space for filling one page */
} VolInfoPage;	


/*
 * Meta Entry Page
 */

/* constants for meta data */
#define METAENTRYNAMESIZE       50
#define METAENTRYDATASIZE       sizeof(ObjectID)

typedef struct {
    PageID pid;                 /* page id of this page, should be located on the beginning */    
    Four flags;                 /* flag to store page information */
    Four reserved;              /* reserved space to store page information */
} MetaDicPageHdr;

/* structure for a meta data entry */
typedef struct {
    char   name[METAENTRYNAMESIZE];
    char   data[METAENTRYDATASIZE];
}   MetaEntry;

#define NUMMETAENTRIESPERPAGE   ((PAGESIZE-sizeof(MetaDicPageHdr)) / sizeof(MetaEntry))

/* structure of a meta data page */
typedef struct {
    MetaDicPageHdr hdr;
    MetaEntry   metaEntry[NUMMETAENTRIESPERPAGE];
    char        dummy[PAGESIZE-sizeof(MetaDicPageHdr)-NUMMETAENTRIESPERPAGE*sizeof(MetaEntry)];    
}   MetaDicPage;


/*
 * Unique Page
 */

typedef struct {
    PageID pid;                 /* page id of this page, should be located on the beginning */    
    Four flags;                 /* flag to store page information */
    Four reserved;              /* reserved space to store page information */
} UniqNumPageHdr;

#define NUMUNIQUESPERPAGE   ((PAGESIZE-sizeof(UniqNumPageHdr)) / (sizeof(Unique)))
#define UNIQUEPARTITIONSIZE     30
#define NUMALLOCATEDUNIQUES     100

/* structure of a unique page */
typedef struct {
    UniqNumPageHdr hdr;
    Unique      unique[NUMUNIQUESPERPAGE];
}   UniqNumPage;

/*  Page type contains all page types used in RDsM */

typedef union {
    VolInfoPage     vi;
    DevMasterPage   ms;
    ExtEntryPage    el;
    MetaDicPage	    md;
    UniqNumPage	    un;
    MapPage         ch;
} PageType;


#define FREE 1
#define DROP 2


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


/*
 * aligned system read/write buffer 
 */
typedef struct RDsM_ReadWriteBuffer_T_tag {
    void                        *ptr;                   /* start offset of Read/Write Buffer */
    Four                        size;                   /* size of Read/Write Buffer (Unit = byte) */
    void                        *alignedPtr;            /* aligned start offset of Read/Write Buffer */
    Four                        alignedSize;            /* aligned size of Read/Write Buffer (Unit = byte) */
} RDsM_ReadWriteBuffer_T;

#define RDSM_READ_WRITE_BUFFER_PTR(_buffer)             ((_buffer)->alignedPtr)
#define RDSM_READ_WRITE_BUFFER_SIZE(_buffer)            ((_buffer)->alignedSize)
#define RDSM_IS_ALIGNED_READ_WRITE_BUFFER(_ptr)         ((((MEMORY_ALIGN_TYPE)_ptr) & RDSM_READ_WRITE_BUFFER_ALIGN_MASK) ? FALSE : TRUE)

/*@
 * Function Prototypes
 */
/* function prototypes for upper layers */

Four	RDsM_AllocTrains(Four, Four, Four, PageID *, Two, Four, Two, PageID *);
Four    RDsM_AllocContigTrainsInExt(Four, Four, Four, PageID*, Two, Four*, Two, PageID*); 
Four	RDsM_CreateSegment(Four, Four, Four *);
Four	RDsM_Dismount(Four, Four);
Four	RDsM_DropSegment(Four, Four, Four);
Four	RDsM_ExtNoToPageId(Four, Four, Four, PageID *); 
Four	RDsM_Finalize(Four);
Four	RDsM_Format(Four, Four, char **, char *, Four, Two, Four *);
Four	RDsM_ExpandVolume(Four, Four, Four, char**, Four *);
Four	RDsM_FreeTrain(Four, PageID *, Two);
Four	RDsM_GetVolNo(Four, char *, Four *);
Four	RDsM_GetAllMountedVolNos(Four, Four *);
Four	RDsM_Initialize(Four);
Four    RDsM_InsertMetaEntry(Four, Four, char*, char*, Two);
Four	RDsM_Mount(Four, Four, char**, Four *);
Four	RDsM_P_ExtEntryArray(Four, Four);
Four	RDsM_P_PageMap(Four, VolumeTable *, Four);
Four	RDsM_P_VolTable(Four, Two);
Four	RDsM_PageIdToExtNo(Four, PageID *, Four *);
Four	RDsM_ReadTrain(Four, PageID *, PageType *, Two);
Four	RDsM_ReadTrains(Four, PageID *, PageType *, Four, Two);  
Four    RDsM_GetUnique(Four, PageID*, Unique*, Four*);
Four	RDsM_WriteTrain(Four, PageType *, PageID *, Two);
Four	RDsM_WriteTrains(Four, PageType *, PageID *, Four, Two);  
Four    RDsM_RDsM_GetMountedVolumes(Four, char *);           
Four    RDsM_ReadTrainForLogVolume(Four, PageID *, PageType *, Two);
Four    RDsM_WriteTrainForLogVolume(Four, PageType *, PageID *,Two);
Four	RDsM_ReadTrainsForLogVolume(Four, PageID*, PageType*, Four, Two);
Four	RDsM_WriteTrainsForLogVolume(Four, PageType*, PageID*, Four, Two);
Four    RDsM_GetStatistics_numExtents(Four, Four, Two*, Four*, Four*);
Four    RDsM_GetStatistics_numPages(Four, Four, sm_NumPages*, Boolean, Boolean); 
Four    RDsM_AbortTransaction(Four); 


/* function prototypes for the raw disk manager layer */

Four	RDsM_alloc_ext(Four, VolumeTable *, Four, Four *);
Four	RDsM_change_NumOfFreeExts_FirstFreeExt(Four, VolumeTable *, Four, Four);
Four	RDsM_free_ext(Four, VolumeTable *, Four, Four *, Four);
Four    RDsM_get_prev_next_ext(Four, VolumeTable*, Four, Four*, Four*);
Four	RDsM_alloc_trains_in_ext(Four, VolumeTable *, Two, Four, PageID *, Two, Four, Four *);
void	RDsM_clear_bits(Four, PageType*, register Four, Four);
Four    RDsM_test_n_bits_set(Four, PageType*, Four, Four);
Four	RDsM_find_bits(Four, PageType*, Four, Four, Four);
void	RDsM_set_bits(Four, PageType*, Four, Four);
void	RDsM_print_bits(Four, PageType*, Four, Four);
Four	RDsM_check_eff(Four, VolumeTable *, Four, Two *);
Four    RDsM_change_eff(Four, VolumeTable *, Four, Two);
Four	RDsM_free_train_in_ext(Four, VolumeTable *, Four, PageID *, Two);
Four	RDsM_get_prev_next_ext(Four, VolumeTable *, Four, Four *, Four *);
Four	RDsM_set_prev_next_ext(Four, VolumeTable *, Four, Four, Four);
Four	RDsM_set_page_map(Four, VolumeTable *, Four);
Four    RDsM_GetVolumeInfo(Four, Four, RDsM_VolInfo*); 
Four    RDsM_GetSizeOfExt(Four, Four, Two *); 

Four	rdsm_ReadTrain(Four, FileDesc, Four, void*, Two);
Four	rdsm_WriteTrain(Four, FileDesc, Four, void*, Two);
Four	rdsm_GetPhysicalInfo(Four, VolumeTable *, PageNo, Four *, Four *);
Four	rdsm_GetPhysicalInfoForLogVolume(Four, VolumeTable *, PageNo, Four *, Four *); 

#ifdef DBLOCK
Four RDsM_GetVolumeLock(Four, Four, LockMode, Boolean);
Four RDsM_ReleaseVolumeLock(Four, Four);
Four RDsM_ReleaseAllVolumeLock(Four);
Four RDsM_SetUseShareLockFlag(Four, Boolean);
Boolean RDsM_GetUseShareLockFlag(Four);
Boolean RDsM_CheckVolumeLock(Four, Four, LockMode);
LockMode RDsM_GetVolumeLockMode(Four, Four);
#endif

#ifdef READ_WRITE_BUFFER_ALIGN_FOR_LINUX
Four rdsm_InitReadWriteBuffer(Four, RDsM_ReadWriteBuffer_T*);
Four rdsm_reallocReadWriteBuffer(Four, RDsM_ReadWriteBuffer_T*, Four);
Four rdsm_FinalReadWriteBuffer(Four, RDsM_ReadWriteBuffer_T*);
#endif

#endif	/* __RDSM_INTERNAL_H__ */
