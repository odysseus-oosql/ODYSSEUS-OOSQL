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

#
# Makefile for the LRDS manager
#

#.KEEP_STATE:


COSMOS = $(O_KAOSS)
LOM    = $(O_LOM_COMMON)
OOSQL  = $(O_SERVER)/Server/OOSQL

#COSMOS_VER = -DLRDSV2
#COSMOS_VER = -DLRDSV2 -DCOSMOS_MULTIVOLUME
#LOM_VER    = -DLOM_DEFINED
COSMOS_VER =
LOM_VER    =

#Profile flag
#PROF = -pg
PROF =

#Debugging and optimization flag
DEBUGFLAG =

#Compiler info.
#CC = /opt/SUNWspro/bin/CC
#C = /opt/SUNWspro/bin/cc
CC =
C =

#Compiler version flag
#CC_VER = -D_SUNCPP_V4_ # for compiling by Sun C++ 4.0
#C_VER =
CC_VER =
C_VER =

#Position independent code information
#PIC = -KPIC
PIC =

#OS info.              
#OS = -D_SOLARIS_2x_
OS =

#Misc. info
#You should explain all flags below one by one
#MISCFLAG = -D_NOTEMPLATEBUG_  # for avoiding template bug
#MISCFLAG += -DINTERNAL          # for internal compilation(for released odysseus, donot include INTERNAL flag
#MISCFLAG += -D__ODMG_93__       # ODMG-93 flag
MISCFLAG =

#Client flag
#You should explain all flags below one by one
CLIENTFLAG =

#Server flag
#You should explain all flags below one by one
#SERVERFLAG = -DSUBINDEX #use sub-index
#SERVERFLAG += -DCOSMOS_MULTIVOLUME # use multi-volume
SERVERFLAG =

#Version Flag
#VERSIONFLAG = ODYS_IR_S_V2.0
VERSIONFLAG =

#include directories
#INCLUDE = -I$(O_OOSQL_SERVER)/SDP/include
INCLUDE = -I$(O_KAOSS)/Header -I$(O_LOM_COMMON)/INCLUDE -I$(O_GEOM)/Header -I$(O_COMMON)/include -I$(O_OOSQL_COMMON)/include -I$(O_OOSQL_SERVER)/include -I$(O_OOSQL_SERVER)/Compiler/include

POSTING_FLAG =

DEFINES = -DTEMPLATE_NOT_SUPPORTED -D_LOM_V2_3_ -D_NOT_USE_QPMM_

CCFLAGS = $(PIC) $(INCLUDE) $(TRACEFLAG) $(PROF) $(DEBUGFLAG) $(OS) $(CC_VER) $(CS_FLAGS) $(COSMOS_VER) $(LOM_VER) $(SORT_FLAG) $(POSTING_FLAG) $(DEFINES) $(OS) $(SERVERFLAG) $(MISCFLAG) $(VERSIONFLAG) $(TEMPLATEFLAG)

#COSMOS_LIBS = $(O_EXPORT)/lib/stand-alone/cosmos.o
#LOM_LIBS = $(O_EXPORT)/lib/stand-alone/lom.o
#CATALOG_LIBS = $(O_EXPORT)/lib/stand-alone/catalog.o
OOSQL_LIBS = $(O_OOSQL_EXPORT)/lib/stand-alone/liboosql.a
LIBS = $(OOSQL_LIBS) $(LOM_LIBS) $(COSMOS_LIBS) -ldl

COMMON_INTERFACE = DBM.o DBM_Err.o
INTERFACE = $(COMMON_INTERFACE)
NONINTERFACE = _dbm.o run.o

OBJECTS= $(INTERFACE)
SOURCES= $(OBJECTS:.o=.c)
LINTFILES= $(SOURCES:.c=.ln)

DBM_UTILS = ODYS_CreateDB ODYS_InitDB ODYS_DestroyDB ODYS_AddVolume ODYS_InitVolume ODYS_DropVolume ODYS_AddIndex ODYS_DropIndex ODYS_AddDevice ODYS_FormatLogVolume
DBM_UTILS_OBJS = ODYS_CreateDB.o ODYS_InitDB.o ODYS_DestroyDB.o ODYS_AddVolume.o ODYS_InitVolume.o ODYS_DropVolume.o ODYS_AddIndex.o ODYS_AddDevice.o ODYS_FormatLogVolume.o

.SUFFIXES: .o .c .cxx

.c.o:
	$(CC) -c $*.c $(CCFLAGS)

.cxx.o:
	$(CC) -c $*.cxx $(CCFLAGS)


all: DBM_API.o

util: $(DBM_UTILS)

DBM_API.o : $(INTERFACE) $(NONINTERFACE)
	@echo ld -r $(INTERFACE) $(NONINTERFACE) -o $@
	@ld -r $(INTERFACE) $(NONINTERFACE) -o $@
	chmod -x $@

$(SOURCES): $(O_OOSQL_SERVER)/include/DBM.h

ODYS_CreateDB: DBM_API.o ODYS_CreateDB.o
	$(CC) -o $@ $@.o $(LIBS)

ODYS_InitDB: DBM_API.o ODYS_InitDB.o
	$(CC) -o $@ $@.o $(LIBS)

ODYS_DestroyDB: DBM_API.o ODYS_DestroyDB.o
	$(CC) -o $@ $@.o $(LIBS)

ODYS_AddVolume: DBM_API.o ODYS_AddVolume.o
	$(CC) -o $@ $@.o $(LIBS)

ODYS_InitVolume: DBM_API.o ODYS_InitVolume.o
	$(CC) -o $@ $@.o $(LIBS)

ODYS_DropVolume: DBM_API.o ODYS_DropVolume.o
	$(CC) -o $@ $@.o $(LIBS)

ODYS_AddDevice: DBM_API.o ODYS_AddDevice.o
	$(CC) -o $@ $@.o $(LIBS)

ODYS_FormatLogVolume: DBM_API.o ODYS_FormatLogVolume.o
	$(CC) -o $@ $@.o $(LIBS)

ODYS_AddIndex: DBM_API.o ODYS_AddIndex.o
	$(CC) -o $@ $@.o $(LIBS)

ODYS_DropIndex: DBM_API.o ODYS_DropIndex.o
	$(CC) -o $@ $@.o $(LIBS)

DBM_Err.o : DBM_Err.i

DBM_Err.i : DBM_Err.m4
	m4 Handle.m4 DBM_Err.m4 > DBM_Err.i

$(ODYS_OOSQL_SERVER)/include/DBM_Err.h : DBM_Err.m4
	m4 defs.m4 DBM_Err.m4 > $@

clean: 
	/bin/rm -f *.o *.ln *.i $(DBM_UTILS)
#
