#/******************************************************************************/
#/*                                                                            */
#/*    Copyright (c) 1990-2016, KAIST                                          */
#/*    All rights reserved.                                                    */
#/*                                                                            */
#/*    Redistribution and use in source and binary forms, with or without      */
#/*    modification, are permitted provided that the following conditions      */
#/*    are met:                                                                */
#/*                                                                            */
#/*    1. Redistributions of source code must retain the above copyright       */
#/*       notice, this list of conditions and the following disclaimer.        */
#/*                                                                            */
#/*    2. Redistributions in binary form must reproduce the above copyright    */
#/*       notice, this list of conditions and the following disclaimer in      */
#/*       the documentation and/or other materials provided with the           */
#/*       distribution.                                                        */
#/*                                                                            */
#/*    3. Neither the name of the copyright holder nor the names of its        */
#/*       contributors may be used to endorse or promote products derived      */
#/*       from this software without specific prior written permission.        */
#/*                                                                            */
#/*    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS     */
#/*    "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT       */
#/*    LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS       */
#/*    FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE          */
#/*    COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,    */
#/*    INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,    */
#/*    BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;        */
#/*    LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER        */
#/*    CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT      */
#/*    LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN       */
#/*    ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE         */
#/*    POSSIBILITY OF SUCH DAMAGE.                                             */
#/*                                                                            */
#/******************************************************************************/
#/******************************************************************************/
#/*                                                                            */
#/*    ODYSSEUS/COSMOS General-Purpose Large-Scale Object Storage System --    */
#/*    Fine-Granule Locking Version                                            */
#/*    Version 3.0                                                             */
#/*                                                                            */
#/*    Developed by Professor Kyu-Young Whang et al.                           */
#/*                                                                            */
#/*    Advanced Information Technology Research Center (AITrc)                 */
#/*    Korea Advanced Institute of Science and Technology (KAIST)              */
#/*                                                                            */
#/*    e-mail: odysseus.oosql@gmail.com                                        */
#/*                                                                            */
#/*    Bibliography:                                                           */
#/*    [1] Whang, K., Lee, J., Lee, M., Han, W., Kim, M., and Kim, J., "DB-IR  */
#/*        Integration Using Tight-Coupling in the Odysseus DBMS," World Wide  */
#/*        Web, Vol. 18, No. 3, pp. 491-520, May 2015.                         */
#/*    [2] Whang, K., Lee, M., Lee, J., Kim, M., and Han, W., "Odysseus: a     */
#/*        High-Performance ORDBMS Tightly-Coupled with IR Features," In Proc. */
#/*        IEEE 21st Int'l Conf. on Data Engineering (ICDE), pp. 1104-1105     */
#/*        (demo), Tokyo, Japan, April 5-8, 2005. This paper received the Best */
#/*        Demonstration Award.                                                */
#/*    [3] Whang, K., Park, B., Han, W., and Lee, Y., "An Inverted Index       */
#/*        Storage Structure Using Subindexes and Large Objects for Tight      */
#/*        Coupling of Information Retrieval with Database Management          */
#/*        Systems," U.S. Patent No.6,349,308 (2002) (Appl. No. 09/250,487     */
#/*        (1999)).                                                            */
#/*    [4] Whang, K., Lee, J., Kim, M., Lee, M., Lee, K., Han, W., and Kim,    */
#/*        J., "Tightly-Coupled Spatial Database Features in the               */
#/*        Odysseus/OpenGIS DBMS for High-Performance," GeoInformatica,        */
#/*        Vol. 14, No. 4, pp. 425-446, Oct. 2010.                             */
#/*    [5] Whang, K., Lee, J., Kim, M., Lee, M., and Lee, K., "Odysseus: a     */
#/*        High-Performance ORDBMS Tightly-Coupled with Spatial Database       */
#/*        Features," In Proc. 23rd IEEE Int'l Conf. on Data Engineering       */
#/*        (ICDE), pp. 1493-1494 (demo), Istanbul, Turkey, Apr. 16-20, 2007.   */
#/*                                                                            */
#/******************************************************************************/
#
# Makefile for the scan manager
#


.KEEP_STATE:

#include Use ANSI/C compiler
CC = cc
LINT = alint
LINT.c = $(LINT) $(LINTFLAGS) $(CPPFLAGS)

# directory of includeable files
INCLUDE = ../Header

TRACEFLAG = -DTRACE 
#PROF = -p
CPPFLAGS = -I$(INCLUDE) $(TRACEFLAG)
CFLAGS = -DCCRL $(TRACEFLAG) $(PROF) -g -vc -Xc
#CFLAGS = $(TRACEFLAG) $(PROF) -g -vc 
LINTFLAGS = 


INTERFACE = SM_AddIndex.o SM_CloseScan.o SM_CreateFile.o SM_CreateObject.o \
            SM_DeleteIndexEntry.o SM_DestroyFile.o SM_DestroyObject.o \
            SM_Dismount.o SM_DropIndex.o SM_FetchObject.o SM_FinalDS.o \
            SM_Err.o SM_GetMetaDictEntry.o SM_GetObjectHdr.o SM_InitFinal.o SM_InitDS.o\
            SM_InsertIndexEntry.o SM_Mount.o SM_NextObject.o SM_OpenIndexScan.o \
            SM_OpenSeqScan.o SM_SetMetaDictEntry.o SM_SetObjectHdr.o \
            SM_UpdateObject.o \
	    SM_MLGF_AddIndex.o SM_MLGF_DropIndex.o SM_MLGF_InsertIndexEntry.o \
	    SM_MLGF_DeleteIndexEntry.o SM_MLGF_OpenIndexScan.o \
	    SM_MLGF_SearchNearObject.o SM_GetIndexStatistics.o SM_MLGF_GetCursorKeys.o \
	    SM_FormatDataVolume.o SM_FormatTempDataVolume.o SM_FormatLogVolume.o SM_CfgParams.o \
	    SM_InsertMetaDictEntry.o SM_DeleteMetaDictEntry.o SM_GlobalData.o \
	    SM_Transaction.o SM_IndexBulkInsert.o \
	    SM_CreateObjectWithoutScan.o SM_FetchObjectWithoutScan.o \
	    SM_UpdateObjectWithoutScan.o SM_DestroyObjectWithoutScan.o \
	    SM_WriteObjectRedoOnlyWithoutScan.o SM_SortFile.o SM_Action.o \
	    SM_GetStatistics.o SM_ExpandDataVolume.o SM_Savepoint.o SM_Counter.o \
	    SM_InitDataFileBulkLoad.o SM_NextDataFileBulkLoad.o SM_FinalDataFileBulkLoad.o \
	    SM_InitIndexBulkLoad.o SM_NextIndexBulkLoad.o SM_FinalIndexBulkLoad.o \
	    SM_InitSortedIndexBulkLoad.o SM_NextSortedIndexBulkLoad.o SM_FinalSortedIndexBulkLoad.o \
	    SM_Stream.o SM_SortStream.o 


NONINTERFACE = sm_GetCatalogEntryFromDataFileId.o sm_GetCatalogEntryFromIndexId.o \
	       sm_GetNewFileId.o sm_GetNewIndexId.o sm_GetIndexInfoFromIndexId.o sm_Catalog.o \
	       sm_mlgf_GetIndexInfoFromIndexId.o

FORTESTING = SM_Dump.o

OBJECTS= $(INTERFACE) $(NONINTERFACE) $(FORTESTING)
SOURCES= $(OBJECTS:.o=.c)
LINTFILES= $(SOURCES:.c=.ln)

SM.o : $(OBJECTS)
	@echo ld -r ~~~~ -o $@
	@ld -r $(OBJECTS) -o $@ 
	chmod -x $@

.c.ln:
	$(LINT.c) $(OUTPUT_OPTION) -c $<

lint: $(LINTFILES)
	$(LINT) -I$(INCLUDE) $(LINTFILES)

clean: 
	$(RM) -f *.o *.ln *.i TAGS tags

depend:
	$(MAKEDEPEND) -I$(INCLUDE) $(SOURCES)

clear_depend:
	@$(MAKEDEPEND)

#
# DO NOT DELETE THIS LINE -- make depend depends on it.
