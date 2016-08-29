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
#/*    Coarse-Granule Locking (Volume Lock) Version                            */
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
# Makefile for the ../BtM directory
#

.KEEP_STATE:

RM = /bin/rm

# dependency list generating utility
MAKEDEPEND = makedepend

# Use ANSI/C compiler
CC=cc
LINT = alint
LINT.c = $(LINT) $(LINTFLAGS) $(CPPFLAGS)

# directory of #include files
INCLUDE = ../Header

TRACEFLAG = -DTRACE
#PROF = -p
CPPFLAGS = -I$(INCLUDE) $(TRACEFLAG)
#CFLAGS = $(PROF) -g -vc -Xc
CFLAGS = $(PROF) -g
LINTFLAGS = 


INTERFACE = BtM_CreateFile.o BtM_CreateIndex.o BtM_DropFile.o BtM_DropIndex.o \
            BtM_Fetch.o BtM_FetchNext.o BtM_InsertObject.o BtM_DeleteObject.o \
            BtM_GetStatistics.o BtM_GetBtreeInfo.o \
			BtM_IsAppendBulkload.o BtM_BulkLoad.o BtM_AppendBulkLoad.o \
			BtM_InitSortedBulkLoad.o BtM_NextSortedBulkLoad.o BtM_FinalSortedBulkLoad.o \
			BtM_InitSortedAppendBulkLoad.o BtM_NextSortedAppendBulkLoad.o BtM_FinalSortedAppendBulkLoad.o \
			BtM_BlkLdBtreeDump.o


NONINTERFACE = btm_AllocPage.o btm_BinarySearch.o btm_Compact.o btm_Compare.o \
               btm_Delete.o btm_FirstObject.o btm_FreePages.o \
               btm_InitPage.o btm_Insert.o btm_LastObject.o btm_Overflow.o \
               btm_Split.o btm_Underflow.o btm_get_objectid.o btm_root.o \
               btm_IsTemporary.o \
				btm_BlkLdGetInfo.o btm_BlkLdAlloc.o btm_BlkLdFlush.o \
				btm_BlkLdInitBuffer.o btm_BlkLdFinalBuffer.o \
				btm_BlkLdInsert.o btm_BlkLdOverflow.o btm_BlkLdSplit.o btm_BlkLdEnd.o \
				btm_BlkLdFreeIndex.o


FORDEBUGING = btm_DumpPage.o

OBJECTS= $(INTERFACE) $(NONINTERFACE) $(FORDEBUGING)
SOURCES= $(OBJECTS:.o=.c)
LINTFILES= $(SOURCES:.c=.ln)


BtM.o : $(OBJECTS)
	@echo ld -r ~~~~ -o $@
	@ld -r $(OBJECTS) -o $@  
	chmod -x $@

.c.ln:
	$(LINT.c) $(OUTPUT_OPTION) -c $<

lint: $(LINTFILES)
	$(LINT) -I$(INCLUDE) $(LINTFILES)

clean: 
	$(RM) -f *.o *.i *.ln TAGS tags

depend:
	$(MAKEDEPEND) -I$(INCLUDE) $(SOURCES)

clear_depend:
	@$(MAKEDEPEND)
#
# DO NOT DELETE THIS LINE -- make depend depends on it.
