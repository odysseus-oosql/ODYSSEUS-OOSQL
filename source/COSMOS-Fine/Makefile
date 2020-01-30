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
# Makefile for KAOSS
#

#
# DEFINES
#  SOLAIRS2 : if the OS is Solaris 2.x
#  LINUX : if the OS is Linux
#  DEMON_PROCESS : if you want to run the deadlock demon
#  __SYS4 : if the OS is compliant to SVR4 
#  SINGLE_USER : use cosmos-cc for single user version
#  COSMOS_S : use LRDS for cosmos-s
#  USETESTANDSET, USEMUTEX : use testandset or use mutex for concurrency control
#  _LARGEFILE64_SOURCE=1 : use raw-device that is larger than 2G at 32bit platform
#                          (Note: must use 2G raw-devices as log volume at 32bit platform)
#  SIGNAL_HANDLER : install signal handler & exit handler
#

ifeq ($(shell getconf LONG_BIT),64)
# note) If you want to support large database, define SUPPORT_LARGE_DATABASE2 in Header/param.h
DEFINES = -DLINUX -D__SVR4 -DCCRL -DNDEBUG -DDEMON_PROCESS -DUSEMUTEX -D_LARGEFILE64_SOURCE=1 -D_LP64	# for 64bit
else
DEFINES = -DLINUX -D__SVR4 -DCCRL -DNDEBUG -DDEMON_PROCESS -DUSEMUTEX -D_LARGEFILE64_SOURCE=1	# for 32bit
endif
#DEFINES = -DLINUX -D__SVR4 -DDEMON_PROCESS 
#DEFINES = -DLINUX -D__SVR4 -DCCRL -DNDEBUG -DDEMON_PROCESS -DUSEMUTEX -D_LARGEFILE64_SOURCE=1
#DEFINES = -DLINUX -D__SVR4 -DCCRL
#DEFINES = -DLINUX -D__SVR4 -DCCRL 

DEFINES_THREAD = -D_REENTRANT
.KEEP_STATE:

RM = /bin/rm
PERL = /usr/bin/perl
#PERL = perl

# dependency list generating utility
MAKEDEPEND = makedepend

CC = gcc -pthread 

CPP = /usr/bin/cpp
#CPP = /usr/lib/cpp
#CPP = /usr/lib/gcc-lib/i386-linux/2.7.2.1/cpp

LINT = alint
LINT.c = $(LINT) $(LINTFLAGS) $(CPPFLAGS)

AR = ar

# directory of includeable files
INCLUDE = ../Header

#TRACEFLAG = -DTRACE
TRACEFLAG = 
#PROF = -xpg
#DEBUG = -xO2
DEBUG = -O2
#DEBUG = -g
CPPFLAGS = -I$(INCLUDE) $(TRACEFLAG) $(DEFINES) $(DEFINES_THREAD)
# For creating the shared object, add -KPIC option
ifeq ($(shell getconf LONG_BIT),64)
CFLAGS = $(PROF) -w $(DEBUG) -fsigned-char -fPIC	# for 64bit
else
CFLAGS = $(PROF) -w $(DEBUG) -fsigned-char	# for 32bit
endif

LINTFLAGS = 

LINTFILES = SM_Format.ln LRDS_Format.ln

MAKE_PASS_MACROS = "PERL=$(PERL)" "CC=$(CC)" "CPP=$(CPP)" "LINT=$(LINT)" "LINT.c=$(LINT.c)" \
	"TRACEFLAG=$(TRACEFLAG)" "PROF=$(PROF)" "INCLUDE=$(INCLUDE)" \
	"CPPFLAGS=$(CPPFLAGS)" "CFLAGS=$(CFLAGS)" "LDFLAGS=$(LDFLAGS)" \
	"LDLIBS=$(LDLIBS)" "LINTFLAGS=$(LINTFLAGS)" "MAKEDEPEND=$(MAKEDEPEND)" 


target : kaoss_LRDS


#
# Build the KAOSS_SM
#
KAOSS_SM_CPPFLAGS = $(CPPFLAGS)
KAOSS_SM_SUBDIR = Err Util RDsM BfM LOT OM BtM MLGF SM SHM LM TM RM LOG Redo Undo THM
KAOSS_SM_OBJS = ./Err/Err.o ./Util/Util.o ./RDsM/RDsM.o ./BfM/BfM.o ./LOT/LOT.o ./OM/OM.o \
	./BtM/BtM.o ./MLGF/MLGF.o ./SM/SM.o ./SHM/SHM.o ./LM/LM.o ./TM/TM.o \
	./RM/RM.o ./LOG/LOG.o ./Redo/Redo.o ./Undo/Undo.o ./THM/THM.o

kaoss_SM: FORCE
	@$(MAKE) "SUBDIR=$(KAOSS_SM_SUBDIR)" "CPPFLAGS=$(KAOSS_SM_CPPFLAGS)" kaoss_SM_aux

kaoss_SM_aux: kaoss_components
	@echo "### Build the Object kaoss_SM.o ###"
	$(CC) -shared -o kaoss_SM.so.1 $(KAOSS_SM_OBJS) 
	@size kaoss_SM.so.1
#	$(LD) -r -o kaoss_SM.o $(KAOSS_SM_OBJS) 
#	@size kaoss_SM.o
#	for dir in $(KAOSS_SM_SUBDIR); do $(AR) r kaoss_SM.a $${dir}/*.o; done
	@echo "### Build the utilities ###"
	(cd ./Misc; $(MAKE) $(MAKE_PASS_MACROS) kaoss_SM)


#
# Build the KAOSS_LRDS
#
KAOSS_LRDS_CPPFLAGS = $(CPPFLAGS) -DLRDS_INCLUDED
KAOSS_LRDS_SUBDIR = Err Util RDsM BfM LOT OM BtM MLGF SM SHM LM TM RM LOG Redo Undo LRDS THM XA
KAOSS_LRDS_OBJS = ./Err/Err.o ./Util/Util.o ./RDsM/RDsM.o ./BfM/BfM.o ./LOT/LOT.o \
	./OM/OM.o ./BtM/BtM.o ./MLGF/MLGF.o ./SM/SM.o ./SHM/SHM.o ./LM/LM.o \
	./TM/TM.o ./RM/RM.o ./LOG/LOG.o ./Redo/Redo.o ./Undo/Undo.o ./LRDS/LRDS.o ./THM/THM.o ./XA/COSMOS_XA.o

kaoss_LRDS: FORCE
	@$(MAKE) "SUBDIR=$(KAOSS_LRDS_SUBDIR)" "CPPFLAGS=$(KAOSS_LRDS_CPPFLAGS)" kaoss_LRDS_aux

kaoss_LRDS_aux: kaoss_components
	@echo "### Build the Object kaoss_LRDS.o ###"
	$(CC) -shared -o kaoss_LRDS.so.1 $(KAOSS_LRDS_OBJS) 
	$(CC) -shared -o libcosmos.so.1 $(KAOSS_LRDS_OBJS) 
	@size kaoss_LRDS.so.1
	$(LD) -r -o kaoss_LRDS.o $(KAOSS_LRDS_OBJS) 
	@size kaoss_LRDS.o
	\cp kaoss_LRDS.o cosmos.o
#	for dir in $(KAOSS_LRDS_SUBDIR); do $(AR) r kaoss_LRDS.a $${dir}/*.o; done
	@echo "### Build the utilities ###"
	(cd ./Misc; $(MAKE) $(MAKE_PASS_MACROS) kaoss_LRDS)


#
# Build the KAOSS_LRDS_single
#
KAOSS_LRDS_single_CPPFLAGS = $(CPPFLAGS) -DLRDS_INCLUDED -DSINGLE_USER
KAOSS_LRDS_single_SUBDIR = Err Util RDsM BfM LOT OM BtM MLGF SM SHM_Single LM_Single TM RM LOG Redo Undo LRDS THM
KAOSS_LRDS_single_OBJS = ./Err/Err.o ./Util/Util.o ./RDsM/RDsM.o ./BfM/BfM.o ./LOT/LOT.o \
	./OM/OM.o ./BtM/BtM.o ./MLGF/MLGF.o ./SM/SM.o ./SHM_Single/SHM.o ./LM_Single/LM.o \
	./TM/TM.o ./RM/RM.o ./LOG/LOG.o ./Redo/Redo.o ./Undo/Undo.o ./LRDS/LRDS.o ./THM/THM.o

kaoss_LRDS_single: FORCE
	@$(MAKE) "SUBDIR=$(KAOSS_LRDS_single_SUBDIR)" "CPPFLAGS=$(KAOSS_LRDS_single_CPPFLAGS)" kaoss_LRDS_single_aux

kaoss_LRDS_single_aux: kaoss_components
	@echo "### Build the Object kaoss_LRDS.o ###"
	$(CC) -shared -o kaoss_LRDS.so.1 $(KAOSS_LRDS_single_OBJS) 
	chmod 755 kaoss_LRDS.so.1
	$(CC) -shared -o libcosmos.so.1 $(KAOSS_LRDS_single_OBJS) 
	@size kaoss_LRDS.so.1
	chmod 755 libcosmos.so.1
	$(LD) -r -o kaoss_LRDS.o $(KAOSS_LRDS_single_OBJS)
	@size kaoss_LRDS.o
	chmod 755 kaoss_LRDS.o
	\cp kaoss_LRDS.o cosmos.o
#	tar cvf kaoss_LRDS.obj.tar Header $(KAOSS_LRDS_single_OBJS)
#	for dir in $(KAOSS_LRDS_single_SUBDIR); do $(AR) r kaoss_LRDS.a $${dir}/*.o; done
	@echo "### Build the utilities ###"
	(cd ./Misc; $(MAKE) $(MAKE_PASS_MACROS) kaoss_LRDS)


kaoss_components: kaoss_include_files FORCE
	@echo "### Make the component objects ###"
	for dir in $(SUBDIR); do (cd ./$${dir}; $(MAKE) $(MAKE_PASS_MACROS) ); done

kaoss_include_files: FORCE
	@echo "### Generate the include files ###"
	(cd ./Err; $(MAKE) $(MAKE_PASS_MACROS) $(INCLUDE)/errorcodes.h)
	(cd ./Misc; $(MAKE) $(MAKE_PASS_MACROS) sedForCheckDummySize.dat)


.c.ln:
	$(LINT.c) $(OUTPUT_OPTION) -c $<

lint: $(LINTFILES)
	$(LINT) -I$(INCLUDE) $(LINTFILES)

clean: 
	for dir in $(KAOSS_LRDS_SUBDIR); do ( cd ./$${dir}; $(MAKE) clean); done;
	(cd ./Misc; $(MAKE) clean )
	$(RM) -f ./Header/errorcodes.h
	$(RM) -f ./Header/log_LogRecTbl.i
	$(RM) -f kaoss_SM.o kaoss_LRDS.o kaoss_SM.so.1 kaoss_LRDS.so.1 libcosmos.so.1 cosmos.o

clean_single: 
	for dir in $(KAOSS_LRDS_single_SUBDIR); do ( cd ./$${dir}; $(MAKE) clean); done;
	(cd ./Misc; $(MAKE) clean )
	$(RM) -f ./Header/errorcodes.h
	$(RM) -f ./Header/log_LogRecTbl.i
	$(RM) -f kaoss_SM.o kaoss_LRDS.o kaoss_SM.so.1 kaoss_LRDS.so.1 libcosmos.so.1 cosmos.o

depend_SM: kaoss_include_files
	for dir in $(KAOSS_SM_SUBDIR); \
	do (cd ./$${dir}; $(MAKE) depend $(MAKE_PASS_MACROS) ); done
	(cd ./Misc; $(MAKE) depend $(MAKE_PASS_MACROS) )

depend_LRDS: kaoss_include_files
	for dir in $(KAOSS_LRDS_SUBDIR); \
	do (cd ./$${dir}; $(MAKE) depend $(MAKE_PASS_MACROS) ); done
	(cd ./Misc; $(MAKE) depend $(MAKE_PASS_MACROS) )

depend_LRDS_single: kaoss_include_files
	for dir in $(KAOSS_LRDS_single_SUBDIR); \
	do (cd ./$${dir}; $(MAKE) depend $(MAKE_PASS_MACROS) ); done
	(cd ./Misc; $(MAKE) depend $(MAKE_PASS_MACROS) )

clear_depend:
	for dir in $(KAOSS_LRDS_SUBDIR); \
	do (cd ./$${dir}; $(MAKE) clear_depend $(MAKE_PASS_MACROS) ); done
	(cd ./Misc; $(MAKE) clear_depend $(MAKE_PASS_MACROS) )

clear_depend_single:
	for dir in $(KAOSS_LRDS_single_SUBDIR); \
	do (cd ./$${dir}; $(MAKE) clear_depend $(MAKE_PASS_MACROS) ); done
	(cd ./Misc; $(MAKE) clear_depend $(MAKE_PASS_MACROS) )

FORCE:
