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
# Makefile for ../SHM directory ( the Shared Memory Manager)
#


.KEEP_STATE:

#include Use ANSI/C compiler
#CC = acc
CC = cc
#CPP = /usr/lib/cpp
CPP = /usr/ccs/lib/cpp
#CPP = /usr/lib/gcc-lib/i386-linux/2.7.2.1/cpp
#CPP = cpp
LINT = alint
LINT.c = $(LINT) $(LINTFLAGS) $(CPPFLAGS)

# directory of includeable files
INCLUDE = ../Header

#TRACEFLAG = -DTRACE3 
#TRACEFLAG = -DTRACE
#PROF = -p
CPPFLAGS = -I$(INCLUDE) $(TRACEFLAG)
#CFLAGS = -g -vc -Xc $(PROF) -DLRDS_INCLUDED
CFLAGS = -O -vc -Xc $(PROF) -DDEMON_PROCESS -DLRDS
ASFLAGS = -P
LINTFLAGS = 


INTERFACE = SHM_memory.o SHM_process.o \
	SHM_initDS.o SHM_finalDS.o shm_processTable.o

NONINTERFACE =

FORTESTING =

OBJECTS = $(INTERFACE) $(NONINTERFACE) $(FORTESTING)
SOURCES = $(OBJECTS:.o=.c)
LINTFILES = $(SOURCES:.c=.ln)

SHM.o : $(OBJECTS)
	@echo ld -r ~~~ -o $@
	@ld -r $(OBJECTS) -o $@
	chmod -x $@

shm_testandset.o: shm_testandset.S
	$(CPP) $(CPPFLAGS) shm_testandset.S | grep -v '^#' > .testandset.s
	$(AS) -o $@ .testandset.s
	$(RM) .testandset.s

.c.ln:
	$(LINT.c) $(OUTPUT_OPTION) -c $<

lint: $(LINTFILES)
	$(LINT) -I$(INCLUDE) $(LINTFILES)


clean : 
	$(RM) -f *.o *.ln *.i TAGS tags 

depend:
	$(MAKEDEPEND) -I$(INCLUDE) $(SOURCES)

clear_depend:
	@$(MAKEDEPEND)

#
# DO NOT DELETE THIS LINE -- make depend depends on it.

#SHM_latch.o: /usr/include/assert.h ../Header/common.h ../Header/param.h
#SHM_latch.o: ../Header/basictypes.h ../Header/Util_varArray.h
#SHM_latch.o: ../Header/latch.h ../Header/error.h ../Header/errorcodes.h
#SHM_latch.o: ../Header/Util_errorLog.h ../Header/trace.h /usr/include/stdio.h
#SHM_latch.o: /usr/include/sys/feature_tests.h ../Header/SHM.h
#SHM_latch.o: /usr/include/sys/ipc.h /usr/include/sys/types.h
#SHM_latch.o: /usr/include/sys/isa_defs.h /usr/include/sys/machtypes.h
#SHM_latch.o: ../Header/TM.h ../Header/Util.h /usr/include/limits.h
#SHM_latch.o: ../Header/Util_pool.h ../Header/Util_heap.h
#SHM_latch.o: ../Header/xactTable.h ../Header/LM.h ../Header/RDsM.h
#SHM_latch.o: /usr/include/fcntl.h /usr/include/sys/fcntl.h ../Header/BfM.h
#SHM_latch.o: ../Header/OM.h ../Header/LOG.h ../Header/dirtyPageTable.h
#SHM_latch.o: ../Header/logRecTbl.h ../Header/BtM.h ../Header/SM.h
SHM_memory.o: ../Header/common.h ../Header/param.h ../Header/basictypes.h
SHM_memory.o: ../Header/Util_varArray.h ../Header/latch.h ../Header/error.h
SHM_memory.o: ../Header/errorcodes.h ../Header/Util_errorLog.h
SHM_memory.o: ../Header/trace.h /usr/include/stdio.h
SHM_memory.o: /usr/include/sys/feature_tests.h ../Header/SHM.h
SHM_memory.o: /usr/include/sys/ipc.h /usr/include/sys/types.h
SHM_memory.o: /usr/include/sys/isa_defs.h /usr/include/sys/machtypes.h
SHM_memory.o: ../Header/TM.h ../Header/Util.h /usr/include/limits.h
SHM_memory.o: ../Header/Util_pool.h ../Header/Util_heap.h
SHM_memory.o: ../Header/xactTable.h ../Header/LM.h ../Header/RDsM.h
SHM_memory.o: /usr/include/fcntl.h /usr/include/sys/fcntl.h ../Header/BfM.h
SHM_memory.o: ../Header/OM.h ../Header/LOG.h ../Header/dirtyPageTable.h
SHM_memory.o: ../Header/logRecTbl.h ../Header/BtM.h ../Header/SM.h
SHM_process.o: /usr/include/stdio.h /usr/include/sys/feature_tests.h
SHM_process.o: /usr/include/string.h /usr/include/stdlib.h
SHM_process.o: /usr/include/errno.h /usr/include/sys/errno.h
SHM_process.o: ../Header/common.h ../Header/param.h ../Header/basictypes.h
SHM_process.o: ../Header/Util_varArray.h ../Header/latch.h ../Header/error.h
SHM_process.o: ../Header/errorcodes.h ../Header/Util_errorLog.h
SHM_process.o: ../Header/trace.h ../Header/RM.h ../Header/xactTable.h
SHM_process.o: ../Header/dirtyPageTable.h ../Header/Util_pool.h
SHM_process.o: ../Header/BfM.h ../Header/Util.h /usr/include/limits.h
SHM_process.o: ../Header/Util_heap.h ../Header/SHM.h /usr/include/sys/ipc.h
SHM_process.o: /usr/include/sys/types.h /usr/include/sys/isa_defs.h
SHM_process.o: /usr/include/sys/machtypes.h ../Header/TM.h ../Header/LM.h
SHM_process.o: ../Header/RDsM.h /usr/include/fcntl.h /usr/include/sys/fcntl.h
SHM_process.o: ../Header/OM.h ../Header/LOG.h ../Header/logRecTbl.h
SHM_process.o: ../Header/BtM.h ../Header/SM.h
SHM_initDS.o: ../Header/common.h ../Header/param.h ../Header/basictypes.h
SHM_initDS.o: ../Header/Util_varArray.h ../Header/latch.h ../Header/error.h
SHM_initDS.o: ../Header/errorcodes.h ../Header/Util_errorLog.h
SHM_initDS.o: ../Header/SHM.h /usr/include/sys/ipc.h /usr/include/sys/types.h
SHM_initDS.o: /usr/include/sys/feature_tests.h /usr/include/sys/isa_defs.h
SHM_initDS.o: /usr/include/sys/machtypes.h ../Header/TM.h ../Header/Util.h
SHM_initDS.o: /usr/include/limits.h ../Header/Util_pool.h
SHM_initDS.o: ../Header/Util_heap.h ../Header/xactTable.h ../Header/LM.h
SHM_initDS.o: ../Header/RDsM.h /usr/include/fcntl.h /usr/include/sys/fcntl.h
SHM_initDS.o: /usr/include/stdio.h ../Header/BfM.h ../Header/OM.h
SHM_initDS.o: ../Header/LOG.h ../Header/dirtyPageTable.h
SHM_initDS.o: ../Header/logRecTbl.h ../Header/BtM.h ../Header/SM.h
SHM_finalDS.o: ../Header/common.h ../Header/param.h ../Header/basictypes.h
SHM_finalDS.o: ../Header/Util_varArray.h ../Header/latch.h ../Header/error.h
SHM_finalDS.o: ../Header/errorcodes.h ../Header/Util_errorLog.h
SHM_finalDS.o: ../Header/SHM.h /usr/include/sys/ipc.h
SHM_finalDS.o: /usr/include/sys/types.h /usr/include/sys/feature_tests.h
SHM_finalDS.o: /usr/include/sys/isa_defs.h /usr/include/sys/machtypes.h
SHM_finalDS.o: ../Header/TM.h ../Header/Util.h /usr/include/limits.h
SHM_finalDS.o: ../Header/Util_pool.h ../Header/Util_heap.h
SHM_finalDS.o: ../Header/xactTable.h ../Header/LM.h ../Header/RDsM.h
SHM_finalDS.o: /usr/include/fcntl.h /usr/include/sys/fcntl.h
SHM_finalDS.o: /usr/include/stdio.h ../Header/BfM.h ../Header/OM.h
SHM_finalDS.o: ../Header/LOG.h ../Header/dirtyPageTable.h
SHM_finalDS.o: ../Header/logRecTbl.h ../Header/BtM.h ../Header/SM.h
