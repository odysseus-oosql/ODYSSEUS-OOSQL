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
 * Module: SM_Dump.c
 *
 * Description:
 *  includes the dump utilities.
 *
 * Exports:
 *  Four SM_DumpMountTable(Four)
 *  Four SM_DumpScanTable(Four)
 */


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

#define PRINT_BOUNDCOND(x, y) \
printf("%s key.len=%ld, key.val=%s, CompOp=0x%ld\n", (x), (y)->key.len, (y)->key.val, (y)->op);

#define PRINT_KEYDESC(x, y)\
{\
Four i;\
printf("%s, flag=%s, nparts=%ld\n", (x), (y)->nparts);\
for ( i = 0; i < (y)->nparts; i++ )\
    printf("part %ld :: type = %ld, offset = %ld, length = %ld\n",\
            i, (y)->kpart[i].type, (y)->kpart[i].offset, (y)->kpart[i].length);\
}

#define PRINT_BTREECURSOR(x,y)\
printf(" %s, flag=%ld", (x), (y)->btree.flag);\
PRINT_OBJECTID("objectID =", &(y)->btree.oid);\
PRINT_PAGEID("pageid of leaf node=", &(y)->btree.leaf);\
PRINT_PAGEID("pageid of overflow node=", &(y)->btree.overflow);\
printf(" slotNo=%ld, oidArrayElemNo=%ld, key.len=%ld, key.val=%s\n",\
(y)->btree.slotNo, (y)->btree.oidArrayElemNo, (y)->btree.key.len, (y)->btree.key.val);

#define PRINT_SEQCURSOR(x,y)\
printf(" %s, flag=%ld", (x), (y)->seq.flag);\
PRINT_OBJECTID("objectID =", &(y)->seq.oid);\

#define PRINT_SM_SYSTABLES_ENTRY(entry) \
PRINT_FILEID("data file id=", &((entry)->data.fid)); \
printf(" data file's last page= %ld", (entry)->data.lastPage);

#define PRINT_DATAFILEINFO(str, finfo) \
BEGIN_MACRO \
printf("*** %s ***\n", str); \
PRINT_FILEID("file id=", &((finfo)->fid)); \
if ((finfo)->tmpFileFlag) { \
    printf(" TEMP FILE\n"); PRINT_SM_SYSTABLES_ENTRY((finfo)->catalog.entry); \
    printf(" NON TEMP FILE\n"); PRINT_OBJECTID("catalog object id=", &((finfo)->catalog.oid)); \
} \
END_MACRO

#define PRINT_SM_SYSINDEXES_ENTRY(entry) \
PRINT_FILEID("data file id=", &((entry)->dataFid)); \
printf(" index's root page= %ld", (entry)->rootPage);

#define PRINT_BTREEINDEXINFO(str, iinfo) \
BEGIN_MACRO \
printf("*** %s ***\n", str); \
PRINT_INDEXID("index id=", &((iinfo)->iid)); \
if ((iinfo)->tmpIndexFlag) { \
	printf(" TEMP FILE\n"); PRINT_SM_SYSINDEXES_ENTRY((iinfo)->catalog.entry); \
	printf(" NON TEMP FILE\n"); PRINT_OBJECTID("catalog object id=", &((iinfo)->catalog.oid)); \
} \
END_MACRO


/*@================================
 * SM_DumpMountTable()
 *================================*/
Four SM_DumpMountTable(Four handle, Four volNo)
{
    Four i;

    for (i = 0; i < MAXNUMOFVOLS; i++)
	if ( SM_MOUNTTABLE[i].volId == volNo ) {

	    printf("+------------------------+\n");
	    printf("| volId | nMount | latch |\n");
	    printf("+------------------------+\n");
	    printf("|%3ld   |%4ld  | %P | %P |\n",
		   SM_MOUNTTABLE[i].volId, SM_MOUNTTABLE[i].nMount,
		   SM_MOUNTTABLE[i].latch);
	    printf("+------------------------+\n");
	    PRINT_DATAFILEINFO("sysTablesInfo ::",
			   &SM_MOUNTTABLE[i].sysTablesInfo);
	    PRINT_BTREEINDEXINFO("\nsysTablesDataFileIdIndexInfo ::", 
			  &SM_MOUNTTABLE[i].sysTablesDataFileIdIndexInfo);

	    PRINT_DATAFILEINFO("\nsysIndexesInfo ::",
			       &SM_MOUNTTABLE[i].sysIndexesInfo);
	    PRINT_BTREEINDEXINFO("\nsysIndexesIndexIdIndexInfo ::", 
			  &SM_MOUNTTABLE[i].sysIndexesIndexIdIndexInfo);
	    /* sysIndexesBtreeFileIdIndex => sysIndexesDataFileIdIndex */
	    PRINT_BTREEINDEXINFO("\nsysIndexesDataFileIdIndexInfo ::", 
			  &SM_MOUNTTABLE[i].sysIndexesDataFileIdIndexInfo);
	    return(eNOERROR);
	}

    if ( i == MAXNUMOFVOLS )
	ERR(handle, eNOTMOUNTEDVOLUME_SM);

}



/*@================================
 * SM_DumpScanTable()
 *================================*/
Four SM_DumpScanTable(
    Four    	handle,
    Four 	scanId)
{

    Four i;

    /* check parameters. */
    if (!VALID_SCANID(handle, scanId)) ERR(handle, eBADPARAMETER);

    PRINT_DATAFILEINFO("finfo", &SM_SCANTABLE(handle)[scanId].finfo);
    switch (SM_SCANTABLE(handle)[scanId].scanType) {
      case NIL:
	printf(" scanType :: NIL\n");
	break;
      case SEQUENTIAL:
	printf(" scanType :: SEQUENTIAL\n");
	printf(" direction :: %c\n", SM_SCANTABLE(handle)[scanId].scanInfo.seq.direction);
	PRINT_SEQCURSOR(" Sequential Cursor ::", &SM_SCANTABLE(handle)[scanId].cursor);
	break;
      case BTREEINDEX:
	printf(" scanType :: BTREEINDEX\n");
	PRINT_INDEXID(" iid ::", &SM_SCANTABLE(handle)[scanId].scanInfo.btree.iinfo.iid); 
	PRINT_KEYDESC(" kdesc ::", &SM_SCANTABLE(handle)[scanId].scanInfo.btree.kdesc);
	PRINT_BOUNDCOND(" startCondition ::", &SM_SCANTABLE(handle)[scanId].scanInfo.btree.startCond);
	PRINT_BOUNDCOND(" stopCondition ::", &SM_SCANTABLE(handle)[scanId].scanInfo.btree.stopCond);
	PRINT_BTREECURSOR(" Btree Cursor ::", &SM_SCANTABLE(handle)[scanId].cursor);
	break;
      case MLGFINDEX:
	printf(" scanType :: MLGFINDEX\n");
	PRINT_INDEXID(" iid ::", &SM_SCANTABLE(handle)[scanId].scanInfo.mlgf.iinfo.iid);
	break;
      default:
	ERR(handle, eINTERNAL);
    }
    printf(" acquiredFileLock :: %ld\n", SM_SCANTABLE(handle)[scanId].acquiredFileLock);
} /* SM_DumpScanTable() */

