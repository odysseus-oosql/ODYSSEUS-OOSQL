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
#ifndef _PARAM_H_
#define _PARAM_H_

#define COSMOS_RELEASE_STRING "dummy"
#define EMBEDDED_SYSTEM_VERSION_SERIAL_STRING "dummy"
#define ENCODED_EMBEDDED_SYSTEM_VERSION_SERIAL_STRING "dummy"

#define COSMOS_MULTITHREAD

/*
** Global System Configuration Parameters
*/
#define	MAXPROCS 	128
#define MAXTHREADS	128
#define TOTALTHREADS	(MAXPROCS*MAXTHREADS)


/* datafile bulkload write buffer size by page */
#define BLKLD_WRITEBUFFERSIZE           10
#define SIZE_OF_LRDS_TUPLE_BUFFER       PAGESIZE

/* Size in PAGESIZE */
#define PAGESIZE2      1
#define TRAINSIZE2     4
#define LOT_LEAF_SIZE2 TRAINSIZE2

#define PAGESIZE      4096		/* NOTE: PAGESIZE must be a multiple of read/write buffer align size */
#define TRAINSIZE     (TRAINSIZE2*PAGESIZE)
#define LOT_LEAF_SIZE (LOT_LEAF_SIZE2*PAGESIZE)

/* structure of a volume information page */
#define MAXNUMOFVOLS    20       /* maximum number of volumes */
#define MAX_VOLUME_TAG_SIZE 50  /* maximum size of the tag for a volume device */
#define MAXVOLTITLE 50 /* maximum size of the title for a volume device */
#define MAXDEVNAME  256 /* maximum size of the name for a volume device */

#define INITSCAN         20	/* # of possible scans at initial time */
#define INITTMPFILE      10     /* # of possible temporary files at initial time */


/*
** TM
*/
/* the deallocated list of large object trees */
#define INIT_TM_LOT_DL 10


/*
** SHM
*/
#define SHM_LOCKFILE   "/tmp/kaoss_shm"
#define SHM_SHMKEY     7890
#define SHM_SEMKEY     7890     


/*
** RM
*/
/* HASH_TABLE_SIZE_FOR_DIRTY_PAGE_TABLE should be power of 2. */
#define DPT_HASH_TABLE_SIZE 32
#define DPT_INIT_NUM_HASH_TABLE_ENTRIES 100
#define RM_REDO_BUFFER_SIZE	1000			/* Size of the main memory buffer used in rm_Redo() when commit is called */

/*
** RDsM
*/
#define CREATED_VOLUME_PERM  0644

/* Meta Dictionary Entry */
#define METADICTENTRYNAME_MAX 64
#define METADICTENTRYDATA_MAX 16

#define INIT_SIZE_OF_DEVINFO_ARRAY 2 

#define VOLUMELOCK_TIMEOUT_RETRY_WAIT	0.01	/* 10 ms waiting */
#define VOLUMELOCK_TIMEOUT_RETRY_NUMBER	600000	/* 60000 retry -> timeout in sec is 600 sec */

#define PREPARE_TIMEOUT_RETRY_WAIT	0.01	/* 10 ms waiting */
#define PREPARE_TIMEOUT_RETRY_NUMBER	60000	/* 60000 retry -> timeout in sec is 600 sec */

/*
** BfM
*/
#ifdef USE_SHARED_MEMORY_BUFFER	

/* Key to access shared memory buffer */
#define COSMOS_SHARED_MEMORY_BUFFER_KEY 3173

/* File used for buffer file locking */
#define COSMOS_BUFFER_FILE_LOCK_NAME "/tmp/cosmos-lock"
#endif 

/* # of buffers in each buffer pool */
#if defined(WIN32) && defined(_DEBUG)		/* Use small buffer size for debugging */ 
#define NUM_PAGE_BUFS     300	/* old value = 500 */
#define NUM_LOT_LEAF_BUFS 200   /* old value = 100 */
#else
#define NUM_PAGE_BUFS     3000	/* old value = 500 */
#define NUM_LOT_LEAF_BUFS 4000 	/* old value = 100 */
#endif

#define BFM_BULKFLUSH_DISKWRITE_SIZE	256	/* bfm에서 buffer에 있는 내용을 쫓아낼때, disk로 write out하는데 사용되는 크기 */
                                            /* Size of the buffer that is used to flush out the content of the buffer in BfM to the disk */
/* BFM_BULKFLUSH_MAX_BUFFER_SIZE는 PAGE BUFFER혹은 TRAIN BUFFER의 수중, 큰 값을 사용한다. */
/* 이 값은 bfm에서 buffer에 있는 내용을 쫓아낼때, 최대 몇개까지의 buffer를 flush할지를 결정하는 값으로 buffer의 size
   만큼의 크기를 잡는 것이 성능상 좋다. */
/* BFM_BULKFLUSH_MAX_BUFFER_SIZE must be the maximum between PAGE BUFFER and TRAIN BUFFER  
   This value is the number of buffer pages that are flushed to the disk when BfM dismiss the buffer pages.
   For the performance, we recommend using the size of the buffer as this value */
#if NUM_PAGE_BUFS > NUM_LOT_LEAF_BUFS 
#define BFM_BULKFLUSH_MAX_BUFFER_SIZE	NUM_PAGE_BUFS
#else
#define BFM_BULKFLUSH_MAX_BUFFER_SIZE	NUM_LOT_LEAF_BUFS
#endif

/*
** BtM
*/
/* Btree Maximum Key Length */
#define MAXKEYLEN  256

/* Btree Maximum Number of Key Parts */
#define MAXNUMKEYPARTS 8


/*
** LOT
*/
#define INIT_SIZE_LOT_PAGEID_ARRAY   100
#define INIT_SIZE_LOT_ENTRY_ARRAY    100

/*
** MLGF
*/
/* MLGF index constraints */
#define MLGF_MAXNUM_KEYS 10



/*
** SM
*/
/* number of parts or columns which consists in SM_MBR */
#define MBR_NUM_PARTS 4



/*
** LOG
*/
#define NUM_WRITE_LOG_BUFS           10



/*
** Sort
** Note!! SIZE_OF_RUN_ARRAY * SIZE_OF_SORT_IN_BUFFER = SIZE_OF_PNO_ARRAY
*/

/* output buffer's size = extent size*/
#define SIZE_OF_SORT_OUT_BUFFER 16

/* input buffer's size = multiply of extent size */
#define SIZE_OF_SORT_IN_BUFFER 256

/* pno array's default size */
#define SIZE_OF_PNO_ARRAY 1000

/* run array's default size */
#define SIZE_OF_RUN_ARRAY 4

/* Macro which used in merge phase */
#define READ_UNIT       10
#define MAX_NUM_RUN   ((CONSTANT_CASTING_TYPE) (SIZE_OF_SORT_IN_BUFFER/READ_UNIT))



/*
** LRDS
*/
/* Maximum number of temporary relations */
#define MAX_NUM_OF_TMP_RELS 32

/* Maximum number of Entries in Open Relation Table of LRDS */
#define MAXNUMOFOPENRELS 400			

/* Maximum number of Entries in User Open Relation Table of LRDS */
#define MAX_NUM_OF_USEROPENRELS 400		

#define INITSETSCAN       5	/* # of possible set scans at initial time */
#define INITORDEREDSETSCAN       5	/* # of possible ordered set scans at initial time */
#define INITCOLLECTIONSCAN 5    /* # of possible collection scans at initial time */

#define INITBOOL         30	/* Subheap size of Boolean Table Heap */
#define INITCOLUMN       256	/* Subheap size of Column Table Heap */
#define INITINDEX        256	/* Subheap size of Index Table Heap */
#define INIT_SIZE_LRDS_ORDEREDSET_AUXCOLINFO_POOL 30 /* Subpool size of Ordered Set AuxColInfo Pool */
#define INIT_SIZE_LRDS_ORDEREDSET_AUXCOLINFO_LOCALPOOL 30 /* Subpool size of Ordered Set AuxColInfo Local Pool */

#define MAXRELNAME       250	/* maximum length of relation name */
#define MAXNUMOFCOLS     256	/* maximum number of columns */
#define MAXNUMOFINDEXES  128	/* maximum number of indexes */
#define MAXNUMOFBOOLS    20	/* maximum number of boolean expressions */

#define USE_LARGE_FILE		/* use large file interface if possible in the utility module */
#define LINUX64              	/* define compiling platform (SOLARIS64, LINUX64, AIX64, WIN64) */
#undef	SUPPORT_LARGE_DATABASE2	/* support large database */

#define ORDEREDSET_BACKWARD_SCAN /* define LRDS Ordered Set Forward & Backward Scan */
/* Note: Because we must read the offsets of N+1 elements for calculating the lengths of N elements,
         LRDS_ORDEREDSET_ELEMENT_LENGTH_CACHE_SIZE is '4 x LOT's Leaf Page Size + sizeof(OrderedSet_ElementLength)'
         This size(4 x LOT's Leaf Page Size) is related to the elementSizeBufferSize argument of LRDS_OrderedSet_Scan_NextElements().
         We recommend that this size(4 x LOT's Leaf Page Size) should be equal to that argument. */
#define LRDS_ORDEREDSET_ELEMENT_LENGTH_CACHE_SIZE (4 * TRAINSIZE + sizeof(OrderedSet_ElementLength))
#define LRDS_ORDEREDSET_ELEMENT_LENGTH_INIT_CACHE (INITORDEREDSETSCAN)

/* 
 * aligned system read/write buffer for linux raw-device 
 */ 
#define READ_WRITE_BUFFER_ALIGN_FOR_LINUX 
#define RDSM_READ_WRITE_BUFFER_INIT_SIZE            (PAGESIZE * 16)
#define RDSM_READ_WRITE_BUFFER_ALIGN_MASK           (0x00000fff)            /* read/write buffer align size is 4096 */

/*
 * For XA-Interface
 */
#define MAX_GLOBAL_XACTID_LEN 256 /* Maximum length of global transaction ID */

/*
 * For Compression
 */
 
#endif /* _PARAM_H_ */
