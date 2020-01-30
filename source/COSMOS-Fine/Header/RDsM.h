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
#ifndef	__RDSM_H__
#define	__RDSM_H__

#include <fcntl.h>
#include <stdio.h>
#include "Util_heap.h" 
#include "latch.h"
#include "xactTable.h"
#include "BfM.h"

/*
 * Mark that the volume has been formatted as a volume.
 */
#define DEVICE_TAG "### FORMATTED DEVICE ###"

#define RDSM_FIRST_TRAIN_SEED_NAME "FIRST_TRAIN_SEED"
#define FILEID_SEED_INIT_VALUE 1973272912L

#define VOLUME_TYPE_RAW    1
#define VOLUME_TYPE_DATA   2

#define NUM_EXTS_OF_SYS_PAGES_FOR_LOG_VOLUME	1
#define NUM_DEVICE_MASTER_PAGES   		1
#define NUM_VOLUME_INFO_PAGES     		1
#define NUM_VOLUME_METADICT_PAGES 		1
#define NUM_VOLUME_UNIQ_PAGES     		1

#define NUM_SYSTEM_PAGES_IN_NOT_FIRST_DEVICE	(NUM_DEVICE_MASTER_PAGES+NUM_VOLUME_UNIQ_PAGES)
#define NUM_SYSTEM_PAGES_IN_FIRST_DEVICE	(NUM_SYSTEM_PAGES_IN_NOT_FIRST_DEVICE+NUM_VOLUME_INFO_PAGES+NUM_VOLUME_METADICT_PAGES)

/* 
 *	empty volume entry 
 */
#define	NOVOL		-1		

/*
 * Direction & SearchMode Type
 * 	this type is necessary to Page and Train Alloc Algorithm.
 */
typedef enum { RDsM_BACKWARD, RDsM_FORWARD } Direction;
typedef enum { FIRST_SEARCH, SECOND_SEARCH } SearchMode_T;



/*
 * The structure for device information in volume information table
 */

typedef struct {
    char devName[MAX_DEVICE_NAME_SIZE];           /* device name */
    Four numExtsInDevice;                     /* number of extents in this volume */
} RDsM_DevInfo;

typedef struct {
    PageID bitmapTrainId;                 /* first page identifier of the page allocation map trains */
    Four bitmapSize;                      /* size of page map (in number of pages) */

    PageID uniqNumPid;                    /* first page identifier of the unique number pages */
    Four uniqPartitionSize;               /* number of pages in a unique partition */
    Four numUniqNumEntries;               /* number of valid unique number entries in a page */

    PageID extentMapPageId;               /* first page identifier of the extent map pages */
    Four   extentMapPageSize;             /* # of extent map page */
    Four   firstExtentInDevice;           /* first extent No. in a device */

} RDsM_DevInfoForDataVol;


/*
 * The structure for segment information in volume information table
 */

typedef struct {
    Four devNo;
    Four startExtInDevice;
    Four firstExtNo;
} RDsM_SegmentInfo;




/*
 * Volume Information
 */
typedef struct RDsM_VolumeInfo_T_tag {

    /*
     * generic volume information
     */

    char title[MAX_VOLUME_TITLE_SIZE];            /* volume title */
    Four volNo;                                   /* volume number */
    Four type;                                    /* volume type */
    PageID volInfoPageId;                         /* ID of volume information page */


    /*
     * device information
     */

    Four extSize;                                 /* number of pages in an extent */
    Four numExts;                                 /* number of extents in this volume */

    Four numDevices;
    LOGICAL_PTR_TYPE(RDsM_DevInfo *) devInfo; 


    /*
     * data volume information
     */

    struct {

        Four eff;                                 /* extent fill factor */

        /*
         * Bitmap & Unique number information
         */
        Four numExtMapsInTrain;                   /* number of extent maps in a bit map train */
        LOGICAL_PTR_TYPE(RDsM_DevInfoForDataVol *) devInfo; /* array which contains information of bitmap & unique number */

        /*
         * meta dictionary
         */
        PageID metaDictPid;                       /* first page identifier of the meta dictionary pages */
        Four metaDictSize;                        /* size of the meta dictionary (in number of pages) */

    	/* 
     	* extent map information
     	*/
    	LATCH_TYPE	pageAllocDeallocLatch;		/* latch of page alloc & dealloc */

	Four            numExtentMapEntryInPage;        /* number of extent map entry in a extent map page */

    	Four 		freeExtent;			/* No. of 1st free extent */
    	Four 		numOfFreeExtent;		/* count of free extent */

    	Four 		freePageExtent;			/* No. of 1st free page extent */
    	Four 		numOfFreePageExtent;		/* count of free page extent */

    	Four 		freeTrainExtent;		/* No. of 1st free train extent */
    	Four 		numOfFreeTrainExtent;		/* count of free train extent */
    } dataVol;
    
} RDsM_VolumeInfo_T;


/*
 * structure of a volume information table
 */
typedef struct rdsm_VolTableEntry_T_tag {
    RDsM_VolumeInfo_T volInfo;       /* volume information */

    /*
     * From now, main memory data structure starts.
     */
    Four nMounts;               /* # of processes which are mounting this volume */
    LATCH_TYPE latch;		/* support mutual exclusion for this volTable entry */
} rdsm_VolTableEntry_T;


/*
 * MasterPage_T - this page is special page which located first of each device
 */

/* macro for segment's type */
#define PAGE_SEGMENT  1
#define TRAIN_SEGMENT 2
#define FREE_SEGMENT  3

typedef PageHdr_T MasterPageHdr_T;

typedef struct MasterPage_T_tag {
    MasterPageHdr_T hdr;

    char   tag[MAX_DEVICE_TAG_SIZE];   /* device tag for checking volume validation */
    Four   volNo;                      /* volume number */
    PageID volInfoPageId;              /* ID of volume information page */
    Four   devNo;                      /* device number in the volume */
    Four   numExtsInDevice;            /* number of extents in this volume */

    /* data volume specific information */
    struct {

        /* segment information */
        Four numSegments;
        struct {
            Four type;
            Four idx;
            Four startExt;
            Four numExts;
        } segInfo[MAX_NUM_SEGMENT_IN_DEVICE];

        /* bitmap */
        PageID bitmapTrainId;          /* first page identifier of the page allocation map trains */
        Four bitmapSize;               /* size of page map (in number of pages) */

        /* unique numbers */
        PageID uniqNumPid;             /* first page identifier of the unique number pages */
        Four uniqPartitionSize;        /* number of pages in a unique partition */
        Four numUniqNumEntries;        /* number of valid unique number entries in a page */

	/* extent map */
    	PageID extentMapPageId;        /* first page identifier of the extent map pages */
    	Four   extentMapPageSize;      /* # of extent map page */
        Four   firstExtentInDevice;    /* first extent No. in a device */

    } dataVol;

} MasterPage_T;


/*
 * VolInfoPage_T - this page contains information about the volume
 */

typedef PageHdr_T VolInfoPageHdr_T;

typedef struct VolInfoPage_T_tag {
    VolInfoPageHdr_T hdr;

    char title[MAX_VOLUME_TITLE_SIZE];            /* volume title */
    Four volNo;                                   /* volume number */
    Four type;                                    /* volume type */
    Four numDevices;                              /* # of devices in the volume */
    Four extSize;                                 /* number of pages in an extent */

    /* data volume specific information */
    struct {
        Four eff;                                 /* extent fill factor */

        /* meta dictionary */
        PageID metaDictPid;                       /* first page identifier of the meta dictionary pages */
        Four metaDictSize;                        /* size of the meta dictionary (in number of pages) */

	/* free extent list */
    	Four freeExtent;                          /* No. of 1st free extent */
    	Four numOfFreeExtent;                     /* count of free extent */

	/* free page extent list */
    	Four freePageExtent;                      /* No. of 1st free page extent */
    	Four numOfFreePageExtent;                 /* count of free page extent */

	/* free train extent list */
    	Four freeTrainExtent;                     /* No. of 1st free train extent */
    	Four numOfFreeTrainExtent;                /* count of free train extent */

    } dataVol;
} VolInfoPage_T;


/*
 * structure of a meta dictionary entry
 */
typedef	struct MetaDictEntry_T_tag {
    char name[MAX_METADICTENTRY_NAME_SIZE];
    char data[MAX_METADICTENTRY_DATA_SIZE];
} MetaDictEntry_T;


/*
 * structure of a meta dictionary page
 */
typedef PageHdr_T MetaDictPageHdr_T;

#define	NUMMETADICTENTRIESPERPAGE	((PAGESIZE-sizeof(MetaDictPageHdr_T)) / (sizeof(MetaDictEntry_T)))

typedef struct MetaDictPage_T_tag {
    MetaDictPageHdr_T hdr;
    MetaDictEntry_T entries[NUMMETADICTENTRIESPERPAGE]; /* meta dictionary entries */
} MetaDictPage_T;


/*
 * structure of a bit map train
 */
typedef PageHdr_T BitmapTrainHdr_T;

#define USABLE_BYTES_PER_BITMAP_TRAIN (TRAINSIZE - sizeof(BitmapTrainHdr_T))
typedef struct BitmapTrain_T_tag {
    BitmapTrainHdr_T hdr;
    unsigned char bytes[USABLE_BYTES_PER_BITMAP_TRAIN]; /* string of bits */
} BitmapTrain_T;


/* 
 * structure of a unique page 
 */
typedef PageHdr_T UniqNumPageHdr_T;

#define	NUMUNIQUEENTRIESPERPAGE    ((PAGESIZE-sizeof(UniqNumPageHdr_T)) / sizeof(Unique))
#define	NUMALLOCATEDUNIQUES  100

typedef struct UniqNumPage_T_tag {
    UniqNumPageHdr_T hdr;
    Unique uniques[NUMUNIQUEENTRIESPERPAGE]; /* unique numbers */
} UniqNumPage_T;


/* 
 * structure of a bit map page 
 */
typedef PageHdr_T ExtentMapPageHdr_T;

typedef struct ExtentMapEntry_T_tag {
    Four	prevExt;	/* No. of previous extent */
    Four	nextExt;	/* No. of next extent */
} ExtentMapEntry_T;

#define EXTENTMAPENTRIESPERPAGE ((PAGESIZE - 1 - sizeof(ExtentMapPageHdr_T)) / sizeof(ExtentMapEntry_T))

typedef struct ExtentMapPage_T_tag {
    ExtentMapPageHdr_T 		hdr;					/* header of extent map page */
    ExtentMapEntry_T	 	entry[EXTENTMAPENTRIESPERPAGE]; 	/* array of extent map entry */
    char			dummy[PAGESIZE - sizeof(ExtentMapPageHdr_T) - EXTENTMAPENTRIESPERPAGE * sizeof(ExtentMapEntry_T)];
} ExtentMapPage_T;

/*
 * structure of a segment identifier
 */

#define INIT_SEGMENT_ID(_segmentID) { \
    (_segmentID)->volNo = NIL; \
    (_segmentID)->firstExtent = NIL; \
    (_segmentID)->sizeOfTrain = NIL; }

#define IS_SAME_SEGMENT_ID(_segmentID1, _segmentID2) (((_segmentID1)->volNo == (_segmentID2)->volNo && \
						       (_segmentID1)->firstExtent == (_segmentID2)->firstExtent && \
						       (_segmentID1)->sizeOfTrain == (_segmentID2)->sizeOfTrain) ? (TRUE) : (FALSE))

#define IS_NIL_SEGMENT_ID(_segmentID) (((_segmentID)->firstExtent == NIL) ? (TRUE) : (FALSE))
#define GET_VOLNO_FROM_SEGMENT_ID(_segmentID) ((_segmentID)->volNo)

/*
 * structure of new alloc extent info 
 */
typedef struct AllocAndFreeExtentInfo_T_tag {
    Four		extentNo;

    Buffer_ACC_CB	*extentmapPage_BCBP;
    Four                extentmapOffset;

    Buffer_ACC_CB	*bitmapTrain_BCBP;
    Four                bitmapOffset;
} AllocAndFreeExtentInfo_T;

#define RDSM_LATCH_PAGEALLOCDEALLOC(_volInfo) ((_volInfo)->dataVol.pageAllocDeallocLatch)

#define RDSM_EXTENTMAP_BUFFER_ACC_CB(_extent) ((_extent)->extentmapPage_BCBP)
#define RDSM_EXTENTMAP_OFFSET(_extent) ((_extent)->extentmapOffset)
#define RDSM_SETDIRTYBIT_EXTENTMAP_BUFFER(_extent) {(_extent)->extentmapPage_BCBP->dirtyFlag = 1;}

#define RDSM_BITMAP_BUFFER_ACC_CB(_extent) ((_extent)->bitmapTrain_BCBP)
#define RDSM_BITMAP_OFFSET(_extent) ((_extent)->bitmapOffset)
#define RDSM_SETDIRTYBIT_BITMAP_BUFFER(_extent) {(_extent)->bitmapTrain_BCBP->dirtyFlag = 1;}

#define RDSM_FREE_EXTENTMAP_BUFFER(_handle, _extent, e) { \
    BFM_FREEBUFFER(_handle, RDSM_EXTENTMAP_BUFFER_ACC_CB(_extent), PAGE_BUF, e); \
    RDSM_EXTENTMAP_BUFFER_ACC_CB(_extent) = NULL; }

#define RDSM_FREE_BITMAP_BUFFER(_handle, _extent, e) { \
    BFM_FREEBUFFER(_handle, RDSM_BITMAP_BUFFER_ACC_CB(_extent), TRAIN_BUF, e); \
    RDSM_BITMAP_BUFFER_ACC_CB(_extent) = NULL; }

#define NO_OP 		-2
#define SET_BITS	1
#define CLEAR_BITS	0

typedef enum RDsM_FreeExtentListHeader_T_tag {
    RDSM_FREE_EXTENT_HEADER = 0,
    RDSM_FREE_PAGE_EXTENT_HEADER = 1,
    RDSM_FREE_TRAIN_EXTENT_HEADER = 2
} RDsM_FreeExtentListHeader_T;

#define RDSM_ALLOC_AND_FREE_EXTENT_INFO_ARRAY(_extentArray) ((AllocAndFreeExtentInfo_T*)(_extentArray.ptr))


/*
 * Page Type
 *  This type is necessary to read/write in PAGESIZE because other page types
 *  have the size equal to PAGESIZE.
 */
typedef union RDsM_Page_T_tag {
    MasterPage_T ms;
    VolInfoPage_T vi;
    MetaDictPage_T md;
    UniqNumPage_T un;
    char ch[PAGESIZE];
} RDsM_Page_T;



/*
 *  Macro for accessing variable array
 */
#define OPENFILEDESC_ARRAY(openFileDescArray)   ((FileDesc *)((openFileDescArray).ptr))


/*
 * User Volume Mount Table
 */
typedef struct rdsm_UserVolTableEntry_T_tag {
    Four volNo;                                /* volume number */
    Four numDevices;                           /* number of devices in volume */
    VarArray openFileDesc;                     /* open file descriptor for the volume */
} rdsm_UserVolTableEntry_T;

typedef struct RDsM_UserDS_T_tag {
    rdsm_UserVolTableEntry_T volMountTable[MAXNUMOFVOLS];
} RDsM_UserDS_T;

#define RDSM_USERVOLTABLE(_handle) (perThreadTable[_handle].rdsmDS.rdsm_userDS.volMountTable)

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

/*
 * enable/disable macro of an extent map logging flag
 */
#define ENABLE_EXTENT_MAP_LOGGING_FLAG(_logParam) 	(_logParam)->logFlag = (_logParam)->logFlag | LOG_FLAG_EXTENT_MAP_LOGGING
#define DISABLE_EXTENT_MAP_LOGGING_FLAG(_logParam) 	(_logParam)->logFlag = (_logParam)->logFlag & (~LOG_FLAG_EXTENT_MAP_LOGGING)



/*-------------------- BEGIN OF Shared Memory Section -----------------------*/
/*
 * Shared Memory Data Structures
 */
typedef struct {
    rdsm_VolTableEntry_T volMountTable[MAXNUMOFVOLS];
    Heap rdsmDevInfoTableHeap;
    Heap rdsmDevInfoForDataVolTableHeap;
    Heap rdsmSegmentInfoTableHeap;
    LATCH_TYPE latch_volMountTable;
} RDsM_SHM;

extern RDsM_SHM *rdsm_shmPtr;
extern Four procIndex;

#define RDSM_LATCH_VOLTABLE 	(rdsm_shmPtr->latch_volMountTable)
#define RDSM_VOLTABLE	 	(rdsm_shmPtr->volMountTable)

#define RDSM_DEVINFOTABLEHEAP           (rdsm_shmPtr->rdsmDevInfoTableHeap)
#define RDSM_DEVINFOFORDATAVOLTABLEHEAP (rdsm_shmPtr->rdsmDevInfoForDataVolTableHeap)
#define RDSM_SEGMENTINFOTABLEHEAP       (rdsm_shmPtr->rdsmSegmentInfoTableHeap)

/*-------------------- END OF Shared Memory Section -------------------------*/

/* Handle is inserted in function parameter below  */

/*
 * exported function prototypes
 */
Four RDsM_InitSharedDS(Four);
Four RDsM_InitLocalDS(Four);
Four RDsM_finalSharedDS(Four);
Four RDsM_finalLocalDS(Four);
/* Four RDsM_Final(); */

Four RDsM_Format(Four, Four, char**, char*, Four, Four, Four*, Four, Four); 
Four RDsM_ExpandVolume(Four, Four, Four, char**, Four*);

Four RDsM_Mount(Four, Four, char**, Four*, Boolean);
Four rdsm_CheckDevices(Four, Four, char **, PageID *); 
Four RDsM_ExtentReorganization(Four, XactTableEntry_T*, Four); 
Four RDsM_MountWithDeviceListString(Four, char*, Four*, Boolean);
Four RDsM_Dismount(Four, Four, Boolean);
Four RDsM_DismountDataVolumes(Four);
Four RDsM_LogMountedVols(Four);

Four RDsM_CreateSegment(Four, XactTableEntry_T*, Four, SegmentID_T*, Four, LogParameter_T*);
Four RDsM_DropSegment(Four, XactTableEntry_T*, Four, SegmentID_T*, Four, Boolean, LogParameter_T*);

Four RDsM_AllocTrains(Four, XactTableEntry_T*, Four, SegmentID_T*, PageID*, Four, Four, Boolean, PageID*, LogParameter_T*);
Four RDsM_AllocContigTrainsInExt(Four, XactTableEntry_T*, Four, SegmentID_T*, PageID*, Four*, Four, Four, PageID*, LogParameter_T*); 
Four rdsm_AllocContigTrainsInExt(Four, XactTableEntry_T*, Four, SegmentID_T*, PageID*, Four*, Four, Four, PageID*, LogParameter_T*); 
Four RDsM_FreeTrain(Four, XactTableEntry_T*, PageID*, Four, Boolean, LogParameter_T*);

Four RDsM_GetFirstTrainOfSegment(Four, XactTableEntry_T*, Four, SegmentID_T*, PageID*);

Four RDsM_ReadTrain(Four, PageID *, char *, Four);
Four RDsM_WriteTrain(Four, char*, PageID*, Four);
Four RDsM_ReadTrains(Four, PageID *, char *, Four, Four);
Four RDsM_WriteTrains(Four, char *, PageID *, Four, Four);
Four RDsM_CopyExtent(Four, XactTableEntry_T*, Four, Four, Four, LogParameter_T*);

Four RDsM_InsertMetaDictEntry(Four, XactTableEntry_T*, Four , char *, char *, Four, LogParameter_T*);
Four RDsM_DeleteMetaDictEntry(Four, XactTableEntry_T*, Four, char *, LogParameter_T*);
Four RDsM_SetMetaDictEntry(Four, XactTableEntry_T*, Four, char *, char *, Four, LogParameter_T*);
Four RDsM_GetMetaDictEntry(Four, XactTableEntry_T*, Four, char *, char *, Four);

Four RDsM_GetUnique(Four, XactTableEntry_T*, PageID *, Unique *, Four *, LogParameter_T*);

Four RDsM_TrainIdToExtNo(Four, PageID *, Four, Four *); 
Four RDsM_GetVolumeInfo(Four, Four, RDsM_VolumeInfo_T *);

Four RDsM_GetStatistics(Four, Four, Four*, Four*, Four*, Boolean);
Four RDsM_GetStatistics_numExtents(Four, Four, Four*, Four*, Four*);
Four RDsM_GetStatistics_numPages(Four, Four, sm_NumPages*, Boolean, Boolean);

Four RDsM_GetSizeOfExt(Four, VolID, Four*); 


/* 
 * internal function prototypes
 */
Four rdsm_GetNearPid(Four, XactTableEntry_T*, RDsM_VolumeInfo_T*, PageID*, LogParameter_T*);	

Four rdsm_GetVolTableEntryNoByVolNo(Four, Four, Four*);

Four rdsm_ReadTrain(Four, FileDesc, Four, void*, Four);
Four rdsm_WriteTrain(Four, FileDesc, Four, void*, Four);

void rdsm_updateDiskStatistics(Four, FileDesc, Four, Four, char);

Four rdsm_FormatSegment(Four, MasterPage_T*, Four, Four, Four, Four, Boolean);
Four rdsm_AllocSegmentForPage(Four, RDsM_VolumeInfo_T*, Four*);
Four rdsm_AllocSegmentForTrain(Four, RDsM_VolumeInfo_T*, Four*);
Four rdsm_DoubleSegInfo(Four, RDsM_SegmentInfo**, Four*);

Boolean RDsM_TestPageSet(Four, PageID *pageId, Four pageSize); 

Four rdsm_AllocExtent(Four, XactTableEntry_T*, RDsM_VolumeInfo_T*, Four, Four*, Four, LogParameter_T*);
Four rdsm_InitAllocAndFreeExtentInfo(Four, AllocAndFreeExtentInfo_T*, Four);
Four rdsm_GetBitMapInfo(Four, AllocAndFreeExtentInfo_T*, Four, Four, Four*);
Four rdsm_SetBitMapInfo(Four, XactTableEntry_T*, AllocAndFreeExtentInfo_T*, Four, Four, Four, LogParameter_T*);
Four rdsm_InitAllocAndFreeExtentInfo(Four, AllocAndFreeExtentInfo_T*, Four);
Four rdsm_GetExtentMapInfo(Four, AllocAndFreeExtentInfo_T*, Four*, Four*);
Four rdsm_SetExtentMapInfo(Four, XactTableEntry_T*, AllocAndFreeExtentInfo_T*, Four, Four, LogParameter_T*);
Four rdsm_getAndFixExtentMapBuffer(Four, RDsM_VolumeInfo_T*, AllocAndFreeExtentInfo_T*);
Four rdsm_getAndFixBitMapBuffer(Four, RDsM_VolumeInfo_T*, AllocAndFreeExtentInfo_T*);
Four rdsm_GetExtentFromFreeExtentList(Four, RDsM_VolumeInfo_T*, Four*, Four);
Four rdsm_RemoveExtentFromFreeExtentList(Four, XactTableEntry_T*, RDsM_VolumeInfo_T*, AllocAndFreeExtentInfo_T*, Four, LogParameter_T*);
Four rdsm_InsertExtentToFreeExtentList(Four, XactTableEntry_T*, RDsM_VolumeInfo_T*, AllocAndFreeExtentInfo_T*, Four, Four, LogParameter_T*);
Four rdsm_InsertExtentToSegment(Four, XactTableEntry_T*, RDsM_VolumeInfo_T*, AllocAndFreeExtentInfo_T*, AllocAndFreeExtentInfo_T*, AllocAndFreeExtentInfo_T*, LogParameter_T*);
Four rdsm_RemoveExtentFromSegment(Four, XactTableEntry_T*, RDsM_VolumeInfo_T*, AllocAndFreeExtentInfo_T*, AllocAndFreeExtentInfo_T*, AllocAndFreeExtentInfo_T*, LogParameter_T*);
Four rdsm_AllocTrainsInExtent(Four, XactTableEntry_T*, RDsM_VolumeInfo_T*, AllocAndFreeExtentInfo_T*, Four, Four, Four, Four, PageID*, Four*, LogParameter_T*);
Four rdsm_GetPhysicalInfo(Four, RDsM_VolumeInfo_T*, PageNo, Four*, Four*);

#ifdef READ_WRITE_BUFFER_ALIGN_FOR_LINUX
Four rdsm_InitReadWriteBuffer(Four, RDsM_ReadWriteBuffer_T*);
Four rdsm_reallocReadWriteBuffer(Four, RDsM_ReadWriteBuffer_T*, Four);
Four rdsm_FinalReadWriteBuffer(Four, RDsM_ReadWriteBuffer_T*);
#endif

#endif	/* __RDSM_H__ */
