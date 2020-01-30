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
 * Module: LOG_Dump.c
 *
 * Description:
 *  Includes the routines for dumping(or printing) data structures used
 *  in log manager.
 *
 * Exports:
 */


#include <stdio.h>
#include "common.h"
#include "trace.h"
#include "error.h"
#include "LOG.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


Four LOG_PrintLogRecordToFile(
    Four    		handle,
    FILE 		*fp,            /* IN file pointer */
    Lsn_T 		*lsn,           /* IN log record LSN */
    LOG_LogRecInfo_T 	*logRecInfo) 	/* IN log record information */
{
    Four 		i;
    Four 		sum;

    if (lsn != NULL) {
        fprintf(fp, "### LSN : (%ld,%ld) ###\n", lsn->wrapCount, lsn->offset);
    }

    /*
     * Print xactId, pid, prevLsn, and undoNextLsn
     */
    fprintf(fp, "xactId=(%ld,%ld) ", logRecInfo->xactId.high, logRecInfo->xactId.low);
    fprintf(fp, "pid=(%ld,%ld) ", logRecInfo->pid.volNo, logRecInfo->pid.pageNo);
    fprintf(fp, "prevLsn=(%ld,%ld) ", logRecInfo->prevLsn.wrapCount, logRecInfo->prevLsn.offset);
    fprintf(fp, "undoNextLsn=(%ld,%ld)\n", logRecInfo->undoNextLsn.wrapCount, logRecInfo->undoNextLsn.offset);

    fprintf(fp, "image sizes = %ld [ ", logRecInfo->nImages);
    sum = 0;
    for (i = 0; i < logRecInfo->nImages; i++) {
        fprintf(fp, "%ld ", logRecInfo->imageSize[i]);
        sum += logRecInfo->imageSize[i];
    }
    fprintf(fp, "] => %ld => %ld\n", sum, log_GetLogRecordLength(handle, logRecInfo));

    /*
     * Print log record type.
     */
    switch (logRecInfo->type) {
      case LOG_TYPE_UPDATE:
        fprintf(fp, "UPDATE");
        break;
      case LOG_TYPE_COMPENSATION:
        fprintf(fp, "COMPENSATION");
        break;
      case LOG_TYPE_TRANSACTION:
        fprintf(fp, "TRANSACTION");
        break;
      case LOG_TYPE_CHECKPOINT:
        fprintf(fp, "CHECKPOINT");
        break;
      case LOG_TYPE_VOLUME:
        fprintf(fp, "VOLUME");
        break;
      default:
        fprintf(fp, "UNKNOWN\n");
        return(eNOERROR);
    }

    /*
     * print log record redo/undo type
     */
    switch (logRecInfo->redoUndo) {
      case LOG_NO_REDO_UNDO:
        fprintf(fp, " NONE");
        break;
      case LOG_REDO_ONLY:
        fprintf(fp, " RO");
        break;
      case LOG_UNDO_ONLY:
        fprintf(fp, " UO");
        break;
      case LOG_REDO_UNDO:
        fprintf(fp, " RU");
        break;
      default:
        fprintf(fp, " UNKNOWN");
        return(eNOERROR);
    }

    /*
     * print log record action
     */
    switch (logRecInfo->action) {
      case LOG_ACTION_DUMMY_CLR:
        fprintf(fp, " DUMMY_CLR");
        break;
      case LOG_ACTION_LOT_DELETE_DATA:
        fprintf(fp, " LOT_DELETE_DATA");
        break;
      case LOG_ACTION_LOT_DELETE_INTERNAL_ENTRIES:
        fprintf(fp, " LOT_DELETE_INTERNAL_ENTRIES");
        break;
      case LOG_ACTION_LOT_INIT_INTERNAL_NODE_PAGE:
        fprintf(fp, " LOT_INIT_INTERNAL_NODE_PAGE");
        break;
      case LOG_ACTION_LOT_INIT_LEAF_NODE:
        fprintf(fp, " LOT_INIT_LEAF_NODE");
        break;
      case LOG_ACTION_LOT_INSERT_DATA:
        fprintf(fp, " LOT_INSERT_DATA");
        break;
      case LOG_ACTION_LOT_REPLACE_INTERNAL_ENTRIES:
        fprintf(fp, " LOT_REPLACE_INTERNAL_ENTRIES");
        break;
      case LOG_ACTION_LOT_UPDATE_COUNT_FIELDS:
        fprintf(fp, " LOT_UPDATE_COUNT_FIELDS");
        break;
      case LOG_ACTION_LOT_WRITE_DATA:
        fprintf(fp, " LOT_WRITE_DATA");
        break;
      case LOG_ACTION_MLGF_CHANGE_LEAF_ENTRY:
        fprintf(fp, " MLGF_CHANGE_LEAF_ENTRY");
        break;
      case LOG_ACTION_MLGF_COPY_PAGE:
        fprintf(fp, " MLGF_COPY_PAGE");
        break;
      case LOG_ACTION_MLGF_DELETE_DIRECTORY_ENTRIES:
        fprintf(fp, " MLGF_DELETE_DIRECTORY_ENTRIES");
        break;
      case LOG_ACTION_MLGF_DELETE_DIRECTORY_ENTRY:
        fprintf(fp, " MLGF_DELETE_DIRECTORY_ENTRY");
        break;
      case LOG_ACTION_MLGF_DELETE_LEAF_ENTRIES:
        fprintf(fp, " MLGF_DELETE_LEAF_ENTRIES");
        break;
      case LOG_ACTION_MLGF_DELETE_LEAF_ENTRY:
        fprintf(fp, " MLGF_DELETE_LEAF_ENTRY");
        break;
      case LOG_ACTION_MLGF_DELETE_OBJECT_FROM_OVERFLOW_PAGE:
        fprintf(fp, " MLGF_DELETE_OBJECT_FROM_OVERFLOW_PAGE");
        break;
      case LOG_ACTION_MLGF_DELETE_OBJECT_FROM_LEAF_ENTRY:
        fprintf(fp, " MLGF_DELETE_OBJECT_FROM_LEAF_ENTRY");
        break;
      case LOG_ACTION_MLGF_DELETE_OBJECT_WITH_LEAF_ENTRY:
        fprintf(fp, " MLGF_DELETE_OBJECT_WITH_LEAF_ENTRY");
        break;
      case LOG_ACTION_MLGF_DELETE_OBJECTS_FROM_OVERFLOW_PAGE:
        fprintf(fp, " MLGF_DELETE_OBJECTS_FROM_OVERFLOW_PAGE");
        break;
      case LOG_ACTION_MLGF_INIT_DIRECTORY_PAGE:
        fprintf(fp, " MLGF_INIT_DIRECTORY_PAGE");
        break;
      case LOG_ACTION_MLGF_INIT_LEAF_PAGE:
        fprintf(fp, " MLGF_INIT_LEAF_PAGE");
        break;
      case LOG_ACTION_MLGF_INIT_OVERFLOW_PAGE:
        fprintf(fp, " MLGF_INIT_OVERFLOW_PAGE");
        break;
      case LOG_ACTION_MLGF_INSERT_DIRECTORY_ENTRIES:
        fprintf(fp, " MLGF_INSERT_DIRECTORY_ENTRIES");
        break;
      case LOG_ACTION_MLGF_INSERT_DIRECTORY_ENTRY:
        fprintf(fp, " MLGF_INSERT_DIRECTORY_ENTRY");
        break;
      case LOG_ACTION_MLGF_INSERT_LEAF_ENTRIES:
        fprintf(fp, " MLGF_INSERT_LEAF_ENTRIES");
        break;
      case LOG_ACTION_MLGF_INSERT_OBJECT_INTO_LEAF_ENTRY:
        fprintf(fp, " MLGF_INSERT_OBJECT_INTO_LEAF_ENTRY");
        break;
      case LOG_ACTION_MLGF_INSERT_OBJECT_INTO_OVERFLOW_PAGE:
        fprintf(fp, " MLGF_INSERT_OBJECT_INTO_OVERFLOW_PAGE");
        break;
      case LOG_ACTION_MLGF_INSERT_OBJECT_WITH_LEAF_ENTRY:
        fprintf(fp, " MLGF_INSERT_OBJECT_WITH_LEAF_ENTRY");
        break;
      case LOG_ACTION_MLGF_INSERT_OBJECTS_INTO_OVERFLOW_PAGE:
        fprintf(fp, " MLGF_INSERT_OBJECTS_INTO_OVERFLOW_PAGE");
        break;
      case LOG_ACTION_MLGF_MODIFY_DIRECTORY_ENTRY:
        fprintf(fp, " MLGF_MODIFY_DIRECTORY_ENTRY");
        break;
      case LOG_ACTION_MLGF_MODIFY_INDEXID_OF_MLGF_PAGE:
        fprintf(fp, " MLGF_MODIFY_INDEXID_OF_MLGF_PAGE");
        break;
      case LOG_ACTION_MLGF_MODIFY_HASH_VALUES_OF_DIRECTORY_ENTRY:
        fprintf(fp, " MLGF_MODIFY_HASH_VALUES_OF_DIRECTORY_ENTRY");
        break;
      case LOG_ACTION_MLGF_MODIFY_NUM_VALID_BITS_OF_DIRECTORY_ENTRY:
        fprintf(fp, " MLGF_MODIFY_NUM_VALID_BITS_OF_DIRECTORY_ENTRY");
        break;
      case LOG_ACTION_MLGF_MODIFY_OVERFLOW_CHAIN_NEXT_LINK:
        fprintf(fp, " MLGF_MODIFY_OVERFLOW_CHAIN_NEXT_LINK");
        break;
      case LOG_ACTION_MLGF_MODIFY_OVERFLOW_CHAIN_PREV_LINK:
        fprintf(fp, " MLGF_MODIFY_OVERFLOW_CHAIN_PREV_LINK");
        break;
      case LOG_ACTION_MLGF_MODIFY_THETA_VALUE_OF_DIRECTORY_ENTRY:
        fprintf(fp, " MLGF_MODIFY_THETA_VALUE_OF_DIRECTORY_ENTRY");
        break;
      case LOG_ACTION_MLGF_ROOT_PAGE_LEVEL_DOWN:
        fprintf(fp, " MLGF_ROOT_PAGE_LEVEL_DOWN");
        break;
      case LOG_ACTION_MLGF_ROOT_PAGE_LEVEL_UP:
      fprintf(fp, " MLGF_ROOT_PAGE_LEVEL_UP");
        break;
      case LOG_ACTION_OM_APPEND_TO_SMALL_OBJECT:
        fprintf(fp, " OM_APPEND_TO_SMALL_OBJECT");
        break;
      case LOG_ACTION_OM_CHANGE_OBJECT:
        fprintf(fp, " OM_CHANGE_OBJECT");
        break;
      case LOG_ACTION_OM_CREATE_SMALL_OBJECT:
        fprintf(fp, " OM_CREATE_SMALL_OBJECT");
        break;
      case LOG_ACTION_OM_DECREASE_OBJECT_HEADER_LENGTH:
        fprintf(fp, " OM_DECREASE_OBJECT_HEADER_LENGTH");
        break;
      case LOG_ACTION_OM_DELETE_FROM_SMALL_OBJECT:
        fprintf(fp, " OM_DELETE_FROM_SMALL_OBJECT");
        break;
      case LOG_ACTION_OM_DESTROY_SMALL_OBJECT:
        fprintf(fp, " OM_DESTROY_SMALL_OBJECT");
        break;
      case LOG_ACTION_OM_GET_UNIQUES:
        fprintf(fp, " OM_GET_UNIQUES");
        break;
      case LOG_ACTION_OM_INCREASE_OBJECT_HEADER_LENGTH:
        fprintf(fp, " OM_INCREASE_OBJECT_HEADER_LENGTH");
        break;
      case LOG_ACTION_OM_INIT_SLOTTED_PAGE:
        fprintf(fp, " OM_INIT_SLOTTED_PAGE");
        break;
      case LOG_ACTION_OM_INSERT_INTO_SMALL_OBJECT:
        fprintf(fp, " OM_INSERT_INTO_SMALL_OBJECT");
        break;
      case LOG_ACTION_OM_MODIFY_LAST_PAGE_IN_CATALOG_ENTRY:
        fprintf(fp, " OM_MODIFY_LAST_PAGE_IN_CATALOG_ENTRY");
        break;
      case LOG_ACTION_OM_MODIFY_PAGE_LIST_NEXT_LINK:
        fprintf(fp, " OM_MODIFY_PAGE_LIST_NEXT_LINK");
        break;
      case LOG_ACTION_OM_MODIFY_PAGE_LIST_PREV_LINK:
        fprintf(fp, " OM_MODIFY_PAGE_LIST_PREV_LINK");
        break;
      case LOG_ACTION_OM_SET_OBJECT_HEADER:
        fprintf(fp, " OM_SET_OBJECT_HEADER");
        break;
      case LOG_ACTION_OM_WRITE_SMALL_OBJECT:
        fprintf(fp, " OM_WRITE_SMALL_OBJECT");
        break;
      case LOG_ACTION_RDSM_ALLOC_TRAINS:
        fprintf(fp, " RDSM_ALLOC_TRAINS");
        break;
      case LOG_ACTION_RDSM_DELETE_METADICTENTRY:
        fprintf(fp, " RDSM_DELETE_METADICTENTRY");
        break;
      case LOG_ACTION_RDSM_FREE_TRAINS:
        fprintf(fp, " RDSM_FREE_TRAINS");
        break;
      case LOG_ACTION_RDSM_GET_UNIQUES:
        fprintf(fp, " RDSM_GET_UNIQUES");
        break;
      case LOG_ACTION_RDSM_INSERT_METADICTENTRY:
        fprintf(fp, " RDSM_INSERT_METADICTENTRY");
        break;
      case LOG_ACTION_RDSM_SET_METADICTENTRY:
        fprintf(fp, " RDSM_SET_METADICTENTRY");
        break;
      case LOG_ACTION_BTM_CHANGE_LEAF_ENTRY:
        fprintf(fp, " BTM_CHANAGE_LEAF_ENTRY");
        break;
      case LOG_ACTION_BTM_COPY_PAGE:
        fprintf(fp, " BTM_COPY_PAGE");
        break;
      case LOG_ACTION_BTM_DELETE_INTERNAL_ENTRIES:
        fprintf(fp, " BTM_DELETE_INTERNAL_ENTRIES");
        break;
      case LOG_ACTION_BTM_DELETE_INTERNAL_ENTRIES_WITHOUT_MORE_UPDATE:
        fprintf(fp, " BTM_DELETE_INTERNAL_ENTRIES_WITHOUT_MORE_UPDATE");
        break;
      case LOG_ACTION_BTM_DELETE_INTERNAL_ENTRY:
        fprintf(fp, " BTM_DELETE_INTERNAL_ENTRY");
        break;
      case LOG_ACTION_BTM_DELETE_LEAF_ENTRIES:
        fprintf(fp, " BTM_DELETE_LEAF_ENTRIES");
        break;
      case LOG_ACTION_BTM_DELETE_LEAF_ENTRIES_WITHOUT_MORE_UPDATE:
        fprintf(fp, " BTM_DELETE_LEAF_ENTRIES_WITHOUT_MORE_UPDATE");
        break;
      case LOG_ACTION_BTM_DELETE_LEAF_ENTRY:
        fprintf(fp, " BTM_DELETE_LEAF_ENTRY");
        break;
      case LOG_ACTION_BTM_DELETE_OID_FROM_LEAF_ENTRY:
        fprintf(fp, " BTM_DELETE_OID_FROM_LEAF_ENTRY");
        break;
      case LOG_ACTION_BTM_DELETE_OID_FROM_OVERFLOW_PAGE:
        fprintf(fp, " BTM_DELETE_OID_FROM_OVERFLOW_PAGE");
        break;
      case LOG_ACTION_BTM_DELETE_OID_WITH_LEAF_ENTRY:
        fprintf(fp, " BTM_DELETE_OID_WITH_LEAF_ENTRY");
        break;
      case LOG_ACTION_BTM_DELETE_OIDS_FROM_OVERFLOW_PAGE:
        fprintf(fp, " BTM_DELETE_OIDS_FROM_OVERFLOW_PAGE");
        break;
      case LOG_ACTION_BTM_REPLACE_ROOT_PAGE_WITH_CHILD_PAGE:
        fprintf(fp, " BTM_REPLACE_ROOT_PAGE_WITH_CHILD_PAGE");
        break;
      case LOG_ACTION_BTM_INIT_INTERNAL_PAGE:
        fprintf(fp, " BTM_INIT_INTERNAL_PAGE");
        break;
      case LOG_ACTION_BTM_INIT_LEAF_PAGE:
        fprintf(fp, " BTM_INIT_LEAF_PAGE");
        break;
      case LOG_ACTION_BTM_INIT_OVERFLOW_PAGE:
        fprintf(fp, " BTM_INIT_OVERFLOW_PAGE");
        break;
      case LOG_ACTION_BTM_INSERT_INTERNAL_ENTRIES:
        fprintf(fp, " BTM_INSERT_INTERNAL_ENTRIES");
        break;
      case LOG_ACTION_BTM_INSERT_INTERNAL_ENTRY:
        fprintf(fp, " BTM_INSERT_INTERNAL_ENTRY");
        break;
      case LOG_ACTION_BTM_INSERT_LEAF_ENTRIES:
        fprintf(fp, " BTM_INSERT_LEAF_ENTRIES");
        break;
      case LOG_ACTION_BTM_INSERT_LEAF_ENTRY:
        fprintf(fp, " BTM_INSERT_LEAF_ENTRY");
        break;
      case LOG_ACTION_BTM_INSERT_OID_INTO_LEAF_ENTRY:
        fprintf(fp, " BTM_INSERT_OID_INTO_LEAF_ENTRY");
        break;
      case LOG_ACTION_BTM_INSERT_OID_INTO_OVERFLOW_PAGE:
        fprintf(fp, " BTM_INSERT_OID_INTO_OVERFLOW_PAGE");
        break;
      case LOG_ACTION_BTM_INSERT_OID_WITH_LEAF_ENTRY:
        fprintf(fp, " BTM_INSERT_OID_WITH_LEAF_ENTRY");
        break;
      case LOG_ACTION_BTM_INSERT_OIDS_INTO_OVERFLOW_PAGE:
        fprintf(fp, " BTM_INSERT_OIDS_INTO_OVERFLOW_PAGE");
        break;
      case LOG_ACTION_BTM_MAKE_ROOT_PAGE:
        fprintf(fp, " BTM_MAKE_ROOT_PAGE");
        break;
      case LOG_ACTION_BTM_MERGE_LEAF_PAGE:
        fprintf(fp, " BTM_MERGE_LEAF_PAGE");
        break;
      case LOG_ACTION_BTM_MODIFY_FIRST_CHILD_OF_INTERNAL_PAGE:
        fprintf(fp, " BTM_MODIFY_FIRST_CHILD_OF_INTERNAL_PAGE");
        break;
      case LOG_ACTION_BTM_MODIFY_INDEXID_OF_BTREE_PAGE:
        fprintf(fp, " BTM_MODIFY_INDEXID_OF_BTREE_PAGE");
        break;
      case LOG_ACTION_BTM_MODIFY_LEAF_CHAIN_NEXT_LINK:
        fprintf(fp, " BTM_MODIFY_LEAF_CHAIN_NEXT_LINK");
        break;
      case LOG_ACTION_BTM_MODIFY_LEAF_CHAIN_PREV_LINK:
        fprintf(fp, " BTM_MODIFY_LEAF_CHAIN_PREV_LINK");
        break;
      case LOG_ACTION_BTM_MODIFY_OVERFLOW_CHAIN_NEXT_LINK:
        fprintf(fp, " BTM_MODIFY_OVERFLOW_CHAIN_NEXT_LINK");
        break;
      case LOG_ACTION_BTM_MODIFY_OVERFLOW_CHAIN_PREV_LINK:
        fprintf(fp, " BTM_MODIFY_OVERFLOW_CHAIN_PREV_LINK");
        break;
      case LOG_ACTION_BTM_REPLACE_INTERNAL_ENTRY_KEY_VALUE:
        fprintf(fp, " BTM_REPLACE_INTERNAL_ENTRY_KEY_VALUE");
        break;
      case LOG_ACTION_BTM_SPLIT_LEAF_PAGE:
        fprintf(fp, " BTM_SPLIT_LEAF_PAGE");
        break;
      case LOG_ACTION_BTM_SPLIT_OVERFLOW_PAGE:
        fprintf(fp, " BTM_SPLIT_OVERFLOW_PAGE");
        break;
      case LOG_ACTION_BTM_UNDO_SPLIT_LEAF_PAGE:
        fprintf(fp, " BTM_UNDO_SPLIT_LEAF_PAGE");
        break;
      case LOG_ACTION_BTM_UNDO_SPLIT_OVERFLOW_PAGE:
        fprintf(fp, " BTM_UNDO_SPLIT_OVERFLOW_PAGE");
        break;
      case LOG_ACTION_XACT_BEGIN_LOGGING_DEALLOC_LIST:
        fprintf(fp, " XACT_BEGIN_LOGGING_DEALLOC_LIST");
        break;
      case LOG_ACTION_XACT_DEALLOC_LIST:
        fprintf(fp, " XACT_DEALLOC_LIST");
        break;
      case LOG_ACTION_XACT_COMMIT_TRANSACTION:
        fprintf(fp, " XACT_COMMIT_TRANSACTION");
        break;
      case LOG_ACTION_XACT_ABORT_TRANSACTION:
        fprintf(fp, " XACT_ABORT_TRANSACTION");
        break;
      case LOG_ACTION_XACT_END_TRANSACTION:
        fprintf(fp, " XACT_END_TRANSACTION");
        break;
      case LOG_ACTION_CHKPT_BEGIN:
        fprintf(fp, " CHKPT_BEGIN");
        break;
      case LOG_ACTION_CHKPT_END:
        fprintf(fp, " CHKPT_END");
        break;
      case LOG_ACTION_CHKPT_ACTIVE_XACTS:
        fprintf(fp, " CHKPT_ACTIVE_XACTS");
        break;
      case LOG_ACTION_CHKPT_DIRTY_PAGES:
        fprintf(fp, " CHKPT_DIRTY_PAGES");
        break;
      case LOG_ACTION_CHKPT_MOUNTED_VOLS:
        fprintf(fp, " CHKPT_MOUNTED_VOLS");
        break;
      case LOG_ACTION_VOL_MOUNT_VOLUME:
        fprintf(fp, " VOL_MOUNT_VOLUME");
        break;
      case LOG_ACTION_VOL_DISMOUNT_VOLUME:
        fprintf(fp, " VOL_DISMOUNT_VOLUME");
        break;
      default:
        fprintf(fp, " UNKNOWN_LOG_ACTION");
        break;
    }
    fprintf(fp, "\n");

    return(eNOERROR);

} /* LOG_PrintLogRecordToFile() */


Four LOG_PrintLogRecord(
    Four 		handle,
    Lsn_T 		*lsn,           /* IN log record LSN */
    LOG_LogRecInfo_T 	*logRecInfo) 	/* IN log record information */
{

    LOG_PrintLogRecordToFile(handle, stdout, lsn, logRecInfo);

    return(eNOERROR);

} /* LOG_PrintLogRecord() */


Four LOG_DumpLogVolumeToFile(
    Four    		handle,
    FILE 		*fp)            /* IN file pointer */
{
    Four 		e;              /* error code */
    Lsn_T 		lsn;            /* log sequence number */
    LOG_LogRecInfo_T 	logRecInfo; 	/* log record information */
    Four 		logRecLen;      /* log record length */
    Four 		image[LOG_MAX_NUM_IMAGES][LOG_MAX_IMAGE_SIZE/sizeof(Four)]; /* space for images, declaration for allignment */
    Four 		i;


    /*
     *  allocate enough memory for log record images
     */
    for (i = 0; i < LOG_MAX_NUM_IMAGES; i++)
	logRecInfo.imageData[i] = image[i];

    /* Set the lsn to the starting lsn. */
    SET_STARTING_LSN(lsn);

    /*
     * Open the log scan
     */
    e = LOG_OpenScan(handle, &lsn);
    if (e < eNOERROR) ERR(handle, e);

    /*
     * read the log record following CHKPT_BEGIN
     */
    e = LOG_NextRecord(handle, &lsn, &logRecInfo, &logRecLen);
    if (e != eENDOFLOG_LOG && e < eNOERROR) ERR(handle, e);


    /*
     *	do the following sequence until the last complete log record is encountered
     */

    while (e == eNOERROR) {

        e = LOG_PrintLogRecordToFile(handle, fp, &lsn, &logRecInfo);
        if (e < eNOERROR) ERR(handle, e);

	/*
	 * read the next log record
	 */
	e = LOG_NextRecord(handle, &lsn, &logRecInfo, &logRecLen);
	if (e != eENDOFLOG_LOG && e < eNOERROR) ERR(handle, e);
    }


    /*
     * Close the scan.
     */
    e = LOG_CloseScan(handle);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* LOG_DumpLogVolumeToFile() */


Four LOG_DumpLogVolume(
    Four    	handle)
{
    Four 	e;

    e = LOG_DumpLogVolumeToFile(handle, stdout);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* LOG_DumpLogVolume() */

Four LOG_DumpLogVolumeInfoToFile(
    FILE 		*fp)            /* IN file pointer */
{
    Four 		e;              /* error code */
    Lsn_T 		lsn;            /* log sequence number */
    LOG_LogRecInfo_T 	logRecInfo; 	/* log record information */
    Four 		logRecLen;      /* log record length */
    Four 		i;


    fprintf(fp, "# of log files = %ld\n", LOG_LOGMASTER.nLogFiles);
    fprintf(fp, "# of bytes in a log file = %ld\n", LOG_LOGMASTER.numBytes);
    fprintf(fp, "# of unused bytes in the current log file = %ld\n", LOG_LOGMASTER.numBytesRemained);
    fprintf(fp, "# of written log records(accumulated) = %ld\n", LOG_LOGMASTER.logRecordCount);
    fprintf(fp, "LSN of the next log record = (%ld,%ld)\n", LOG_LOGMASTER.nextLsn.wrapCount, LOG_LOGMASTER.nextLsn.offset);


    return(eNOERROR);

} /* LOG_DumpLogVolumeInfoToFile() */


