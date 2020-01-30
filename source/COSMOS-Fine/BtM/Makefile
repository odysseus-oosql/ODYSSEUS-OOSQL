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
# Makefile for the Btree Manager
#


.KEEP_STATE:

CC=cc
LINT = alint
LINT.c = $(LINT) $(LINTFLAGS) $(CPPFLAGS)

# directory of include files
INCLUDE = ../Header

TRACEFLAG = -DTRACE
#PROF = -p
CPPFLAGS = -I$(INCLUDE) -DCCRL
CFLAGS = $(TRACEFLAG) $(PROF) -O -vc -Xc
LINTFLAGS = 


INTERFACE = BtM_CreateIndex.o BtM_DeleteObject.o BtM_DropIndex.o \
	BtM_Fetch.o BtM_FetchNext.o BtM_Final.o BtM_InitDS.o BtM_InsertObject.o \
	BtM_TreeLatch.o BtM_GetStatistics.o \
	BtM_IsAppendBulkLoad.o BtM_BulkLoad.o BtM_AppendBulkLoad.o \
	BtM_InitSortedBulkLoad.o BtM_NextSortedBulkLoad.o BtM_FinalSortedBulkLoad.o \
	BtM_InitSortedAppendBulkLoad.o BtM_NextSortedAppendBulkLoad.o BtM_FinalSortedAppendBulkLoad.o \
	BtM_BlkLdBtreeDump.o


NONINTERFACE = btm_BinarySearch.o btm_Compact.o btm_Compare.o \
	btm_DeleteLeaf.o btm_DeletePage.o btm_FirstObject.o btm_InsertLeaf.o \
	btm_LastObject.o btm_Overflow.o \
	btm_GetObjectId.o btm_GetNextObjectLock.o \
	btm_ChangeEntrySize.o btm_Path.o btm_IsCorrectPage.o \
	btm_ResetStatusBits.o btm_Search.o \
	btm_FetchForward.o btm_FetchBackward.o btm_FetchNextBwd.o btm_FetchNextFwd.o btm_CheckDuplicatedKey.o btm_Split.o \
	btm_GetRootPid.o \
	btm_BlkLdGetInfo.o btm_BlkLdAlloc.o btm_BlkLdFlush.o \
	btm_BlkLdInitBuffer.o btm_BlkLdFinalBuffer.o \
	btm_BlkLdInsert.o btm_BlkLdOverflow.o btm_BlkLdSplit.o btm_BlkLdEnd.o \
	btm_BlkLdFreeIndex.o \
	btm_GetSegmentID.o btm_IdMapping.o


FORTESTING = btm_DumpPage.o

OBJECTS= $(INTERFACE) $(NONINTERFACE) $(FORTESTING)
SOURCES= $(OBJECTS:.o=.c)
LINTFILES= $(SOURCES:.c=.ln)

BtM.o : $(OBJECTS)
	@echo ld -r ~~~~ -o BtM.o
	@ld -r $(OBJECTS) -o BtM.o 
	chmod -x BtM.o

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
# DO NOT DELETE THIS LINE -- make depend depends on it.
