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
# Makefile for Lock Manager
#


.KEEP_STATE:

# Use ANSI/C compiler
CC = cc
LINT = alint
LINT.c = $(LINT) $(LINTFLAGS) $(CPPFLAGS)

# directory of includeable files
INCLUDE = ../Header 

TRACEFLAG = -DTRACE
#TRACEFLAG = -DTRACE 
#PROF = -p
CPPFLAGS = -I$(INCLUDE) $(TRACEFLAG)
CFLAGS = -g -vc -Xc $(PROF)
LINTFLAGS = 


INTERFACE = LM_flatLock.o LM_initDS.o LM_fileLock.o LM_keyRangeLock.o \
	LM_xactBucket.o LM_finalDS.o LM_xactLock.o LM_pageLock.o LM_action.o LM_objectLock.o LM_locksOfPreparedXact.o

NONINTERFACE =

FORTESTING = 
#FORTESTING = LM_dumpLock.o

OBJECTS = $(INTERFACE) $(NONINTERFACE) $(FORTESTING)
SOURCES = $(OBJECTS:.o=.c)
LINTFILES = $(SOURCES:.c=.ln)

LM.o : $(OBJECTS)
	@echo ld -r ~~~ -o LM.o
	@ld -r $(OBJECTS) -o LM.o 
	chmod -x LM.o

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

LM_flatLock.o: /usr/include/stdio.h /usr/include/sys/feature_tests.h
LM_flatLock.o: /usr/include/stdlib.h ../Header/common.h ../Header/param.h
LM_flatLock.o: ../Header/basictypes.h ../Header/Util_varArray.h
LM_flatLock.o: ../Header/latch.h ../Header/error.h ../Header/errorcodes.h
LM_flatLock.o: ../Header/Util_errorLog.h ../Header/Util.h
LM_flatLock.o: /usr/include/limits.h ../Header/Util_pool.h
LM_flatLock.o: ../Header/Util_heap.h ../Header/TM.h ../Header/xactTable.h
LM_flatLock.o: ../Header/LM.h ../Header/LM_macro.h ../Header/LM_LockMatrix.h
LM_flatLock.o: ../Header/SHM.h /usr/include/sys/ipc.h
LM_flatLock.o: /usr/include/sys/types.h /usr/include/sys/isa_defs.h
LM_flatLock.o: /usr/include/sys/machtypes.h ../Header/RDsM.h
LM_flatLock.o: /usr/include/fcntl.h /usr/include/sys/fcntl.h ../Header/BfM.h
LM_flatLock.o: ../Header/OM.h ../Header/LOG.h ../Header/dirtyPageTable.h
LM_flatLock.o: ../Header/logRecTbl.h ../Header/BtM.h ../Header/SM.h
LM_initDS.o: ../Header/common.h ../Header/param.h ../Header/basictypes.h
LM_initDS.o: ../Header/Util_varArray.h ../Header/latch.h ../Header/error.h
LM_initDS.o: ../Header/errorcodes.h ../Header/Util_errorLog.h
LM_initDS.o: ../Header/trace.h /usr/include/stdio.h
LM_initDS.o: /usr/include/sys/feature_tests.h ../Header/Util.h
LM_initDS.o: /usr/include/limits.h ../Header/Util_pool.h
LM_initDS.o: ../Header/Util_heap.h ../Header/LM.h ../Header/LM_macro.h
LM_initDS.o: ../Header/SHM.h /usr/include/sys/ipc.h /usr/include/sys/types.h
LM_initDS.o: /usr/include/sys/isa_defs.h /usr/include/sys/machtypes.h
LM_initDS.o: ../Header/TM.h ../Header/xactTable.h ../Header/RDsM.h
LM_initDS.o: /usr/include/fcntl.h /usr/include/sys/fcntl.h ../Header/BfM.h
LM_initDS.o: ../Header/OM.h ../Header/LOG.h ../Header/dirtyPageTable.h
LM_initDS.o: ../Header/logRecTbl.h ../Header/BtM.h ../Header/SM.h
LM_fileLock.o: /usr/include/stdio.h /usr/include/sys/feature_tests.h
LM_fileLock.o: /usr/include/stdlib.h ../Header/common.h ../Header/param.h
LM_fileLock.o: ../Header/basictypes.h ../Header/Util_varArray.h
LM_fileLock.o: ../Header/latch.h ../Header/error.h ../Header/errorcodes.h
LM_fileLock.o: ../Header/Util_errorLog.h ../Header/Util.h
LM_fileLock.o: /usr/include/limits.h ../Header/Util_pool.h
LM_fileLock.o: ../Header/Util_heap.h ../Header/TM.h ../Header/xactTable.h
LM_fileLock.o: ../Header/LM.h ../Header/LM_macro.h ../Header/LM_LockMatrix.h
LM_fileLock.o: ../Header/SHM.h /usr/include/sys/ipc.h
LM_fileLock.o: /usr/include/sys/types.h /usr/include/sys/isa_defs.h
LM_fileLock.o: /usr/include/sys/machtypes.h ../Header/RDsM.h
LM_fileLock.o: /usr/include/fcntl.h /usr/include/sys/fcntl.h ../Header/BfM.h
LM_fileLock.o: ../Header/OM.h ../Header/LOG.h ../Header/dirtyPageTable.h
LM_fileLock.o: ../Header/logRecTbl.h ../Header/BtM.h ../Header/SM.h
LM_keyRangeLock.o: /usr/include/stdio.h /usr/include/sys/feature_tests.h
LM_keyRangeLock.o: /usr/include/stdlib.h /usr/include/math.h
LM_keyRangeLock.o: ../Header/common.h ../Header/param.h
LM_keyRangeLock.o: ../Header/basictypes.h ../Header/Util_varArray.h
LM_keyRangeLock.o: ../Header/latch.h ../Header/error.h ../Header/errorcodes.h
LM_keyRangeLock.o: ../Header/Util_errorLog.h ../Header/Util.h
LM_keyRangeLock.o: /usr/include/limits.h ../Header/Util_pool.h
LM_keyRangeLock.o: ../Header/Util_heap.h ../Header/TM.h ../Header/xactTable.h
LM_keyRangeLock.o: ../Header/LM.h ../Header/LM_macro.h
LM_keyRangeLock.o: ../Header/LM_LockMatrix.h ../Header/SHM.h
LM_keyRangeLock.o: /usr/include/sys/ipc.h /usr/include/sys/types.h
LM_keyRangeLock.o: /usr/include/sys/isa_defs.h /usr/include/sys/machtypes.h
LM_keyRangeLock.o: ../Header/RDsM.h /usr/include/fcntl.h
LM_keyRangeLock.o: /usr/include/sys/fcntl.h ../Header/BfM.h ../Header/OM.h
LM_keyRangeLock.o: ../Header/LOG.h ../Header/dirtyPageTable.h
LM_keyRangeLock.o: ../Header/logRecTbl.h ../Header/BtM.h ../Header/SM.h
LM_xactBucket.o: /usr/include/stdio.h /usr/include/sys/feature_tests.h
LM_xactBucket.o: /usr/include/stdlib.h ../Header/common.h ../Header/param.h
LM_xactBucket.o: ../Header/basictypes.h ../Header/Util_varArray.h
LM_xactBucket.o: ../Header/latch.h ../Header/error.h ../Header/errorcodes.h
LM_xactBucket.o: ../Header/Util_errorLog.h ../Header/Util.h
LM_xactBucket.o: /usr/include/limits.h ../Header/Util_pool.h
LM_xactBucket.o: ../Header/Util_heap.h ../Header/TM.h ../Header/xactTable.h
LM_xactBucket.o: ../Header/LM.h ../Header/LM_macro.h
LM_finalDS.o: ../Header/common.h ../Header/param.h ../Header/basictypes.h
LM_finalDS.o: ../Header/Util_varArray.h ../Header/latch.h ../Header/error.h
LM_finalDS.o: ../Header/errorcodes.h ../Header/Util_errorLog.h
LM_finalDS.o: ../Header/trace.h /usr/include/stdio.h
LM_finalDS.o: /usr/include/sys/feature_tests.h ../Header/Util.h
LM_finalDS.o: /usr/include/limits.h ../Header/Util_pool.h
LM_finalDS.o: ../Header/Util_heap.h ../Header/LM.h ../Header/SHM.h
LM_finalDS.o: /usr/include/sys/ipc.h /usr/include/sys/types.h
LM_finalDS.o: /usr/include/sys/isa_defs.h /usr/include/sys/machtypes.h
LM_finalDS.o: ../Header/TM.h ../Header/xactTable.h ../Header/RDsM.h
LM_finalDS.o: /usr/include/fcntl.h /usr/include/sys/fcntl.h ../Header/BfM.h
LM_finalDS.o: ../Header/OM.h ../Header/LOG.h ../Header/dirtyPageTable.h
LM_finalDS.o: ../Header/logRecTbl.h ../Header/BtM.h ../Header/SM.h
LM_xactLock.o: /usr/include/stdio.h /usr/include/sys/feature_tests.h
LM_xactLock.o: /usr/include/stdlib.h ../Header/common.h ../Header/param.h
LM_xactLock.o: ../Header/basictypes.h ../Header/Util_varArray.h
LM_xactLock.o: ../Header/latch.h ../Header/error.h ../Header/errorcodes.h
LM_xactLock.o: ../Header/Util_errorLog.h ../Header/Util.h
LM_xactLock.o: /usr/include/limits.h ../Header/Util_pool.h
LM_xactLock.o: ../Header/Util_heap.h ../Header/TM.h ../Header/xactTable.h
LM_xactLock.o: ../Header/LM.h ../Header/LM_macro.h ../Header/LM_LockMatrix.h
LM_xactLock.o: ../Header/SHM.h /usr/include/sys/ipc.h
LM_xactLock.o: /usr/include/sys/types.h /usr/include/sys/isa_defs.h
LM_xactLock.o: /usr/include/sys/machtypes.h ../Header/RDsM.h
LM_xactLock.o: /usr/include/fcntl.h /usr/include/sys/fcntl.h ../Header/BfM.h
LM_xactLock.o: ../Header/OM.h ../Header/LOG.h ../Header/dirtyPageTable.h
LM_xactLock.o: ../Header/logRecTbl.h ../Header/BtM.h ../Header/SM.h
LM_pageLock.o: /usr/include/stdio.h /usr/include/sys/feature_tests.h
LM_pageLock.o: /usr/include/stdlib.h ../Header/common.h ../Header/param.h
LM_pageLock.o: ../Header/basictypes.h ../Header/Util_varArray.h
LM_pageLock.o: ../Header/latch.h ../Header/error.h ../Header/errorcodes.h
LM_pageLock.o: ../Header/Util_errorLog.h ../Header/Util.h
LM_pageLock.o: /usr/include/limits.h ../Header/Util_pool.h
LM_pageLock.o: ../Header/Util_heap.h ../Header/TM.h ../Header/xactTable.h
LM_pageLock.o: ../Header/LM.h ../Header/LM_macro.h ../Header/LM_LockMatrix.h
LM_pageLock.o: ../Header/SHM.h /usr/include/sys/ipc.h
LM_pageLock.o: /usr/include/sys/types.h /usr/include/sys/isa_defs.h
LM_pageLock.o: /usr/include/sys/machtypes.h ../Header/RDsM.h
LM_pageLock.o: /usr/include/fcntl.h /usr/include/sys/fcntl.h ../Header/BfM.h
LM_pageLock.o: ../Header/OM.h ../Header/LOG.h ../Header/dirtyPageTable.h
LM_pageLock.o: ../Header/logRecTbl.h ../Header/BtM.h ../Header/SM.h
LM_action.o: /usr/include/stdio.h /usr/include/sys/feature_tests.h
LM_action.o: /usr/include/stdlib.h ../Header/common.h ../Header/param.h
LM_action.o: ../Header/basictypes.h ../Header/Util_varArray.h
LM_action.o: ../Header/latch.h ../Header/error.h ../Header/errorcodes.h
LM_action.o: ../Header/Util_errorLog.h ../Header/Util.h /usr/include/limits.h
LM_action.o: ../Header/Util_pool.h ../Header/Util_heap.h ../Header/TM.h
LM_action.o: ../Header/xactTable.h ../Header/LM.h ../Header/LM_macro.h
LM_action.o: ../Header/LM_LockMatrix.h ../Header/SHM.h /usr/include/sys/ipc.h
LM_action.o: /usr/include/sys/types.h /usr/include/sys/isa_defs.h
LM_action.o: /usr/include/sys/machtypes.h ../Header/RDsM.h
LM_action.o: /usr/include/fcntl.h /usr/include/sys/fcntl.h ../Header/BfM.h
LM_action.o: ../Header/OM.h ../Header/LOG.h ../Header/dirtyPageTable.h
LM_action.o: ../Header/logRecTbl.h ../Header/BtM.h ../Header/SM.h
LM_objectLock.o: /usr/include/stdio.h /usr/include/sys/feature_tests.h
LM_objectLock.o: /usr/include/stdlib.h ../Header/common.h ../Header/param.h
LM_objectLock.o: ../Header/basictypes.h ../Header/Util_varArray.h
LM_objectLock.o: ../Header/latch.h ../Header/error.h ../Header/errorcodes.h
LM_objectLock.o: ../Header/Util_errorLog.h ../Header/Util.h
LM_objectLock.o: /usr/include/limits.h ../Header/Util_pool.h
LM_objectLock.o: ../Header/Util_heap.h ../Header/TM.h ../Header/xactTable.h
LM_objectLock.o: ../Header/LM.h ../Header/LM_macro.h
LM_objectLock.o: ../Header/LM_LockMatrix.h ../Header/SHM.h
LM_objectLock.o: /usr/include/sys/ipc.h /usr/include/sys/types.h
LM_objectLock.o: /usr/include/sys/isa_defs.h /usr/include/sys/machtypes.h
LM_objectLock.o: ../Header/RDsM.h /usr/include/fcntl.h
LM_objectLock.o: /usr/include/sys/fcntl.h ../Header/BfM.h ../Header/OM.h
LM_objectLock.o: ../Header/LOG.h ../Header/dirtyPageTable.h
LM_objectLock.o: ../Header/logRecTbl.h ../Header/BtM.h ../Header/SM.h
