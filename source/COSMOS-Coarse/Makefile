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
# Makefile for COSMOS
#

#
# DEFINES
#  SOLAIRS2 : if the OS is Solaris 2.x
#  LINUX : if the OS is Linux
#  DEMONPROCESS : if you want to run the deadlock demon
#  __SYS4 : if the OS is compliant to SVR4 
#  COSMOS_S : use LRDS for cosmos-s
#  _LARGEFILE64_SOURCE=1 : use raw-device that is larger than 2G at 32bit solaris platform
#
#  USE_SHARED_MEMORY_BUFFER : use shared memory buffer
#  USE_LATCH_TESTANDSET, USE_LATCH_MUTEX, USE_MUTEX : use a latch(testandset), a latch(mutex), or a mutex for concurrency control
#    example)
#       cc                                                       : No shared memory
#       cc -DUSE_SHARED_MEMORY_BUFFER -DUSE_LATCH_TESTANDSET     : Shared memory using the latch(testandset)
#       cc -DUSE_SHARED_MEMORY_BUFFER -DUSE_LATCH_MUTEX          : Shared memory using the latch(mutex)
#       cc -DUSE_SHARED_MEMORY_BUFFER -DUSE_MUTEX                : Shared memory using the mutex
#
#  USE_COHERENCY_VOLUME, USE_LOG_COHERENCY_VOLUME : use coherency/log volume for buffer coherency
#

# Basic Combination of Compile Flags 
ifeq ($(shell getconf LONG_BIT),64)
# note) If you want to support large database, define SUPPORT_LARGE_DATABASE2 in Header/param.h
DEFINES = -DLINUX -D_GNU_SOURCE -DDEMON_PROCESS -D__SVR4 -DNDEBUG -DDBLOCK -DUSE_SHARED_MEMORY_BUFFER -DUSE_MUTEX -DUSE_LOGICAL_PTR -DCOSMOS_S -D_LARGEFILE64_SOURCE=1 -D_LP64	# for 64bit 
else
DEFINES = -DLINUX -D_GNU_SOURCE -DDEMON_PROCESS -D__SVR4 -DNDEBUG -DDBLOCK -DUSE_SHARED_MEMORY_BUFFER -DUSE_MUTEX -DUSE_LOGICAL_PTR -DCOSMOS_S -D_LARGEFILE64_SOURCE=1	# for 32bit
endif
#DEFINES = -DLINUX -D_GNU_SOURCE -DDEMON_PROCESS -D__SVR4 -DNDEBUG -DDBLOCK -DUSE_COHERENCY_VOLUME -DCOSMOS_S -D_LARGEFILE64_SOURCE=1
#DEFINES = -DLINUX -D_GNU_SOURCE -DDEMON_PROCESS -D__SVR4 -DNDEBUG -DDBLOCK -DUSE_SHARED_MEMORY_BUFFER -DUSE_MUTEX -DCOSMOS_S -D_LARGEFILE64_SOURCE=1
#DEFINES = -DLINUX -D_GNU_SOURCE -DDEMON_PROCESS -D__SVR4 -DNDEBUG -DDBLOCK -DUSE_SHARED_MEMORY_BUFFER -DUSE_LATCH_MUTEX -DCOSMOS_S -D_LARGEFILE64_SOURCE=1
#DEFINES = -DLINUX -D_GNU_SOURCE -DDEMON_PROCESS -D__SVR4 -DNDEBUG -DDBLOCK -DUSE_SHARED_MEMORY_BUFFER -DUSE_LATCH_MUTEX -DUSE_LOGICAL_PTR -DCOSMOS_S -D_LARGEFILE64_SOURCE=1

# Compile Flag Combinations for Release
#DEFINES = -DLINUX -DDEMON_PROCESS -D__SVR4 -DNDEBUG -DDBLOCK -DUSE_SHARED_MEMORY_BUFFER -DUSE_MUTEX -DCOSMOS_S -D_LARGEFILE64_SOURCE=1
#DEFINES = -DLINUX -DDEMON_PROCESS -D__SVR4 -DNDEBUG -DDBLOCK -DUSE_COHERENCY_VOLUME -DCOSMOS_S -D_LARGEFILE64_SOURCE=1
#DEFINES = -DLINUX -DDEMON_PROCESS -D__SVR4 -DDBLOCK

DEFINES_THREAD = -D_REENTRANT


.KEEP_STATE:

RM = /bin/rm

# dependency list generating utility
MAKEDEPEND = makedepend

#CC = gcc 
CC = gcc -pthread 

CPP = /usr/bin/cpp
#CPP = /usr/lib/cpp
#CPP = /usr/ccs/lib/cpp
LINT = alint
LINT.c = $(LINT) $(LINTFLAGS) $(CPPFLAGS)

AR = ar

# directory of includeable files
INCLUDE = ../Header 


TRACEFLAG = -DTRACE
#PROF = -pg
#DEBUG = -O2
DEBUG = -g 
CPPFLAGS = -I$(INCLUDE) $(TRACEFLAG) $(DEFINES) 
# For creating the shared object, add -KPIC option
ifeq ($(shell getconf LONG_BIT),64)
CFLAGS = $(PROF) -w $(DEBUG) -fsigned-char -fPIC # for 64bit
else
CFLAGS = $(PROF) -w $(DEBUG) -fsigned-char # for 32bit
endif

LINTFLAGS = 

LINTFILES = SM_Format.ln LRDS_Format.ln

MAKE_PASS_MACROS = "CC=$(CC)" "CPP=$(CPP)" "LINT=$(LINT)" "LINT.c=$(LINT.c)" \
	"TRACEFLAG=$(TRACEFLAG)" "PROF=$(PROF)" "INCLUDE=$(INCLUDE)" \
	"CPPFLAGS=$(CPPFLAGS)" "CFLAGS=$(CFLAGS)" "LDFLAGS=$(LDFLAGS)" \
	"LDLIBS=$(LDLIBS)" "LINTFLAGS=$(LINTFLAGS)" "MAKEDEPEND=$(MAKEDEPEND)"


target : cosmos_LRDS


#
# Build the COSMOS_SM
#
COSMOS_SM_CPPFLAGS = $(CPPFLAGS)
COSMOS_SM_SUBDIR = Err Util RDsM BfM LOT OM BtM MLGF SM RM THM
COSMOS_SM_OBJS = ./Err/Err.o ./Util/Util.o ./RDsM/RDsM.o ./BfM/BfM.o ./LOT/LOT.o ./OM/OM.o \
	./BtM/BtM.o ./MLGF/MLGF.o ./SM/SM.o ./RM/RM.o ./THM/THM.o 

cosmos_SM: FORCE
	@$(MAKE) "SUBDIR=$(COSMOS_SM_SUBDIR)" "CPPFLAGS=$(COSMOS_SM_CPPFLAGS)" cosmos_SM_aux

cosmos_SM_aux: cosmos_components
	@echo "### Build the Object cosmos_SM.o ###"
	$(CC) -shared -o cosmos.so.1 $(COSMOS_SM_OBJS) -lm
	@size cosmos.so.1
	$(LD) -r -o cosmos.o $(COSMOS_SM_OBJS) 
	@size cosmos.o
#	for dir in $(COSMOS_SM_SUBDIR); do $(AR) r cosmos_SM.a $${dir}/*.o; done
	@echo "### Build the utilities ###"
	(cd ./Misc; $(MAKE) $(MAKE_PASS_MACROS) cosmos_SM)


#
# Build the COSMOS_LRDS
#
COSMOS_LRDS_CPPFLAGS = $(CPPFLAGS) -DLRDS_INCLUDED
#COSMOS_LRDS_SUBDIR = SM
#COSMOS_LRDS_OBJS = ./SM/SM.o 
COSMOS_LRDS_SUBDIR = Err Util RDsM BfM LOT OM BtM MLGF SM RM LRDS XA THM
COSMOS_LRDS_OBJS = ./Err/Err.o ./Util/Util.o ./RDsM/RDsM.o ./BfM/BfM.o ./LOT/LOT.o \
	./OM/OM.o ./BtM/BtM.o ./MLGF/MLGF.o ./SM/SM.o ./RM/RM.o ./LRDS/LRDS.o \
	./XA/COSMOS_XA.o ./THM/THM.o

cosmos_LRDS: FORCE
	@$(MAKE) "SUBDIR=$(COSMOS_LRDS_SUBDIR)" "CPPFLAGS=$(COSMOS_LRDS_CPPFLAGS)" cosmos_LRDS_aux

cosmos_LRDS_aux: cosmos_components
	@echo "### Build the Object cosmos_LRDS.o ###"
	$(CC) -shared -o cosmos.so.1 $(COSMOS_LRDS_OBJS) -lm
	@size cosmos.so.1
	$(LD) -r -o cosmos.o $(COSMOS_LRDS_OBJS) 
	@size cosmos.o
#	for dir in $(COSMOS_LRDS_SUBDIR); do $(AR) r cosmos.a $${dir}/*.o; done
	@echo "### Build the utilities ###"
	(cd ./Misc; $(MAKE) $(MAKE_PASS_MACROS) cosmos_LRDS)


cosmos_components: cosmos_include_files FORCE
	@echo "### Make the component objects ###"
	for dir in $(SUBDIR); do (cd ./$${dir}; $(MAKE) $(MAKE_PASS_MACROS) ); done

cosmos_include_files: FORCE
	@echo "### Generate the include files ###"
	(cd ./Err; $(MAKE) $(MAKE_PASS_MACROS) $(INCLUDE)/errorcodes.h)


.c.ln:
	$(LINT.c) $(OUTPUT_OPTION) -c $<

lint: $(LINTFILES)
	$(LINT) -I$(INCLUDE) $(LINTFILES)

clean: 
	for dir in $(COSMOS_LRDS_SUBDIR); do ( cd ./$${dir}; $(MAKE) clean); done;
	(cd ./Misc; $(MAKE) clean )
	$(RM) -f ./Header/errorcodes.h
	$(RM) -f cosmos.o cosmos.so.1

depend: cosmos_include_files
	for dir in $(COSMOS_LRDS_SUBDIR); \
	do (cd ./$${dir}; $(MAKE) depend $(MAKE_PASS_MACROS) ); done
	(cd ./Misc; $(MAKE) depend $(MAKE_PASS_MACROS) )

clear_depend:
	for dir in $(COSMOS_LRDS_SUBDIR); \
	do (cd ./$${dir}; $(MAKE) clear_depend $(MAKE_PASS_MACROS) ); done
	(cd ./Misc; $(MAKE) clear_depend $(MAKE_PASS_MACROS) )

FORCE:
