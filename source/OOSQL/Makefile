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
#/*    ODYSSEUS/OOSQL DB-IR-Spatial Tightly-Integrated DBMS                    */
#/*    Version 5.0                                                             */
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

#Debugging mode or optimization flag
DEBUGFLAG = -g 
#DEBUGFLAG = -fstrength-reduce -fthread-jumps -fcse-follow-jumps -fcse-skip-blocks -frerun-cse-after-loop -felide-constructors -fexpensive-optimizations -fdelayed-branch
ifneq ($(DEBUGFLAG),-g)
ifneq ($(shell getconf LONG_BIT),64)
DEBUGFLAG += -fschedule-insns -fschedule-insns2
endif
endif

LIBS = -ldl -lm -lstdc++ -lpthread # without shared buffer 

#Trace information flag
#TRACEFLAG =

#Compiler info.
CC = gcc -w -x c++ -fsigned-char
C  = gcc -w -fsigned-char

#Compiler version flag
CC_VER =

C_VER =

#Position independent code information
ifeq ($(shell getconf LONG_BIT),64)
PIC = -fPIC	# for 64bit
else
PIC =	# for 32bit
endif

# dependency list generating utility
MAKEDEPEND = makedepend

#OS info.
OS = -D_LINUX_7x_ -D__LINUX__

#Misc. info
#You should explain all flags below one by one
MISCFLAG = -D_NOTEMPLATEBUG_   # for avoiding template bug
MISCFLAG += -DINTERNAL 		   # for internal compilation(for released odysseus, donot include INTERNAL flag
MISCFLAG += -D__ODMG_93__      # ODMG-93 flag
#MISCFLAG += -DGEOM_INCLUDED  # OLD GIS Functionality (deprecated)
MISCFLAG += -DNDEBUG           # Disable TR_PRINT

#template 
TEMPLATEFLAG = -D_RWSTD_COMPILE_INSTANTIATE # indicate doing compile time instantiatation of STD

#Client flag
#You should explain all flags below one by one
CLIENTFLAG =

#Server flag
#You should explain all flags below one by one
SERVERFLAG = -DSUBINDEX #use sub-index
SERVERFLAG += -DCOSMOS_MULTIVOLUME # use multi-volume
SERVERFLAG += -DLOM_DEFINED # notify to cosmos that LOM is also included
SERVERFLAG += -DLRDSV2 # LRDS has new interface (may be...)
SERVERFLAG += -D_OOSQL_SERVER_ # indicates that we are compiling server
SERVERFLAG += -DUSE_WITHOUT_WEIGHT # text posting list has no weight
SERVERFLAG += -DUSE_SUBINDEX # text posting list's structure is based on SUBINDEX
SERVERFLAG += -DUSE_RPC # use rpc to communicate between client and server
SERVERFLAG += -DLINUX64
ifeq ($(shell getconf LONG_BIT),64)
OS64FLAG = -D_LP64 # for 64bit 
#OS64FLAG += -DSUPPORT_LARGE_DATABASE2 # for Large Database (Should consistent with COSMOS/Header/param.h)
OS64FLAG += $(PIC)
endif
SERVERFLAG += -DUTILFLUSHSTDOUT
#Posting List Compression
#LIBS += -lz
#SERVERFLAG += -DCOMPRESSION
#SERVERFLAG += -DSLIMDOWN_TEXTIR # slimdown for text IR functionality

# GEOM(OPENGIS) has not been made thread-safe yet. In case GIS function is needed, do not use GEOM in multi-thread configuration.
# # use a multi-process version instead; Otherwise exclude GEOM by compiling with "-DSLIMDOWN_OPENGIS" flag below.
SERVERFLAG += -DSLIMDOWN_OPENGIS # slimdown for OpenGIS functionality

#SERVERFLAG += -DENABLE_OPENGIS_CHECKTYPE # (Unrecommended)Enable OpenGIS Type Checking Functionality
SERVERFLAG += -DENABLE_OPENGIS_OPTIMIZATION # (Recommended)Enable OpenGIS Optimization Functionality

#Version Flag
#VERSIONFLAG = ODYS_IR_S_V2.0


####################
# Compiler Options #
####################

INCLUDE = -I$(O_KAOSS)/Header -I$(O_LOM_COMMON)/INCLUDE -I$(O_GEOM)/Header -I$(O_COMMON)/include -I$(O_OOSQL_COMMON)/include -I$(O_OOSQL_SERVER)/include -I$(O_OOSQL_SERVER)/Compiler/include

CCFLAGS = $(DEBUGFLAG) $(PIC) $(INCLUDE) $(TRACEFLAG) $(PROF) $(OS) $(CC_VER) $(SERVERFLAG) $(MISCFLAG) $(TEMPLATEFLAG)

CFLAGS = $(C_DEBUGFLAG) $(PIC) $(INCLUDE) $(TRACEFLAG) $(PROF) $(OS) $(C_VER) $(SERVERFLAG) $(MISCFLAG)

LINT = alint
LINT.c = $(LINT) $(LINTFLAGS) $(CPPFLAGS)

OPTIONS = C_DEBUGFLAG='$(C_DEBUGFLAG)' DEBUGFLAG='$(DEBUGFLAG)' TRACEFLAG='$(TRACEFLAG)' PROF='$(PROF)'\
	EVENTFLAG='$(EVENTFLAG)' CC='$(CC)' CC_VER='$(CC_VER)' C='$(C)' C_VER='$(C_VER)' PIC='$(PIC)' \
	TARGET_ARCH='$(TARGET_ARCH)' OS='$(OS)' MISCFLAG='$(MISCFLAG)' \
	SERVERFLAG='$(SERVERFLAG)' CLIENTFLAG='$(CLIENTFLAG)' VERSIONFLAG='$(VERSIONFLAG)' TEMPLATEFLAG='$(TEMPLATEFLAG)'
	
OPTIONSFOR64 = OS64FLAG='$(OS64FLAG)'
######################
# OOSQL object files #
######################

#OOSQL_SUBDIR = Compiler APIs Evaluator MemoryManager TextIR Tool MinMaxHeap StorageManager Err SDP DBM tcl
OOSQL_SUBDIR = Err Compiler APIs Evaluator MemoryManager TextIR Tool MinMaxHeap StorageManager DBM tcl

TEMPLATE_OBJS = Templates.DB/*.o

OOSQL_OBJS = 	  APIs/*.o \
                  Err/*.o \
                  Compiler/compiler/*.o \
                  Compiler/gdsplan/*.o \
                  Compiler/parser/*.o \
                  Compiler/text/*.o \
                  Evaluator/*.o \
                  MinMaxHeap/*.o \
                  MemoryManager/*.o \
                  StorageManager/*.o \
                  TextIR/*.o \
                  Tool/*.o \
		  		  tcl/*.o \
		          DBM/DBM_API.o 

OOSQL_UTILITY = DBM/ODYS_CreateDB \
		DBM/ODYS_DestroyDB \
		DBM/ODYS_InitDB \
		DBM/ODYS_AddVolume \
		DBM/ODYS_DropVolume \
		DBM/ODYS_InitVolume \
		DBM/ODYS_AddIndex \
		DBM/ODYS_DropIndex \
		DBM/ODYS_FormatLogVolume
 

#################
# MAKE commands #
#################

#target: oosql_SERVER util
target: oosql_SERVER

oosql_SERVER: FORCE
	@$(MAKE) "SUBDIR=$(OOSQL_SUBDIR)" oosql_SERVER_aux

oosql_SERVER_aux: oosql_SERVER_components
	@echo "Build oosqlServer.o"
	$(LD) -r $(OOSQL_OBJS) -o oosql.o
	gcc -shared -o liboosql.so $(O_KAOSS)/cosmos.o $(O_LOM_SERVER)/LOM/LOM.o $(O_LOM_SERVER)/Catalog/catalog.o $(O_GEOM)/geom.o oosql.o $(LIBS)
#	$(LD) -G -o liboosql.so -h liboosql.so oosql.o $(O_KAOSS)/cosmos.o $(O_LOM_SERVER)/LOM/LOM.o $(O_LOM_SERVER)/Catalog/catalog.o $(O_OOSQL_EXPORT)/lib/libC.so.5 /opt/SUNWspro/SC5.0/lib/libCrun.a  /opt/SUNWspro/SC5.0/lib/libCstd.a $(LIBS)
#	$(AR) rcv liboosql.a oosql.o $(O_KAOSS)/cosmos.o $(O_LOM_SERVER)/LOM/LOM.o $(O_LOM_SERVER)/Catalog/catalog.o 
	cp liboosql.so $(O_OOSQL_EXPORT)/lib
	cp include/OOSQL_APIs.h $(O_OOSQL_EXPORT)/include
	cp include/OOSQL_errorcodes.h $(O_OOSQL_EXPORT)/include
	(cd Util/ISQL; make $(OPTIONSFOR64))
	(cd Util/OOSQL_Format; make $(OPTIONSFOR64); make install)
	(cd Util/OOSQL_BatchBuildTools; make $(OPTIONSFOR64); make install)
	(cd Util/OOSQL_LoadDB; make $(OPTIONSFOR64); make install)
	(cd Util/OOSQL_Export; make $(OPTIONSFOR64); make install)
	(cd Util/OOSQL_Import; make $(OPTIONSFOR64); make install)
	(cd Util/PyOOSQL; make $(OPTIONSFOR64); make install)
	(cd Example/keyword_tools; make $(OPTIONSFOR64); make install)
	(cd Example/filter_tools; make $(OPTIONSFOR64); make install)
	(cd Util/EnglishKeywordExtractor; make $(OPTIONSFOR64); make install)
	(cd Util/ExampleKeywordExtractor; make $(OPTIONSFOR64); make install)
	(cd Util/n-gram/src; make $(OPTIONSFOR64); make install)
	(cp Util/n-gram/dict/* $(O_OOSQL_EXPORT)/bin)
	(cp Util/n-gram/tool/* $(O_OOSQL_EXPORT)/bin)

oosql_SERVER_components: FORCE
	for dir in $(SUBDIR); do (cd ./$${dir}; $(MAKE) $(OPTIONS)); done;

util: FORCE
	(cd DBM; $(MAKE) util $(OPTIONS))
	(cd Util; $(MAKE) install $(OPTIONS))
	cp $(OOSQL_UTILITY) $(O_OOSQL_EXPORT)/bin
	cp $(OOSQL_UTILITY) $(O_EXPORT)/bin

clean:
	\rm -rf *.so
	for dir in $(OOSQL_SUBDIR); do (cd ./$${dir}; $(MAKE) clean); done;
	\rm -rf Templates.DB
	\rm -f *.o
	(cd Util/ISQL; \rm -f *.o isql isql_multi)
	(cd Util/OOSQL_Format; \rm -f *.o OOSQL_AddDevice OOSQL_AddVolume OOSQL_CreateDB OOSQL_DestroyDB OOSQL_DropVolume OOSQL_FormatCoherencyVolume OOSQL_FormatLogVolume OOSQL_GetDatabaseStatistics OOSQL_InitDB OOSQL_InitVolume)
	(cd Util/OOSQL_BatchBuildTools; \rm -f *.o OOSQL_BatchDelete OOSQL_BuildTextIndex OOSQL_DeleteTextIndex OOSQL_ExtractKeyword OOSQL_MapPosting OOSQL_SortPosting OOSQL_SortStoredPosting OOSQL_StorePosting OOSQL_UpdateTextDescriptor sort)
	(cd Util/OOSQL_LoadDB; \rm -f *.o OOSQL_CheckDataSyntax OOSQL_LoadDB)
	(cd Util/OOSQL_Export; \rm -f *.o OOSQL_Export)
	(cd Util/OOSQL_Import; \rm -f *.o OOSQL_Import)
	(cd Util/PyOOSQL; make clean)
	(cd Util/EnglishKeywordExtractor; make clean)
	(cd Util/ExampleKeywordExtractor; make clean)
	(cd Util/n-gram/src; make clean)
	(cd Example/keyword_tools; make clean)
	(cd Example/filter_tools; make clean)
	(cd Example/null_keyword_extractor; make clean)

depend_OOSQL: 
	for dir in $(OOSQL_SUBDIR); \
	do (cd ./$${dir}; $(MAKE) depend $(OPTIONS)); done

clear_depend:
	for dir in $(OOSQL_SUBDIR) \
	do (cd ./$${dir}; $(MAKE) clear_depend $(OPTIONS)); done;

FORCE:
# DO NOT DELETE THIS LINE -- make depend depends on it.
