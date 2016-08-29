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

C = gcc -w -fsigned-char 
CC = gcc -w -fsigned-char

LIB =

COSMOS = $(O_KAOSS)
LOM    = $(O_LOM_SERVER)
OOSQL  = $(O_OOSQL_SERVER)

COSMOS_VER = -DLRDSV2 -DCOSMOS_MULTIVOLUME -DUSE_COHERENCY_VOLUME
LOM_VER    = -DLOM_DEFINED

OSTYPE = -D_SOLARIS_2x_
CC_VER = -D_SUNCPP_V4_ -DTEMPLATE_NOT_SUPPORTED
CS_FLAGS = -D_OOSQL_SERVER_ -DLOM_DEFINED

INCLUDE = -I$(COSMOS)/Header -I$(LOM)/INCLUDE -I$(OOSQL)/include -I$(OOSQL)/Compiler/include -I$(O_GEOM)/Header

DEBUG = -g
#DEBUG = -O2
PIC = -fPIC

POSTING_FLAG = -DUSE_WITHOUT_WEIGHT -DUSE_SUBINDEX

CCFLAGS = $(PIC) $(INCLUDE) $(TRACEFLAG) $(PROF) $(DEBUG) $(OSTYPE) $(CC_VER) $(CS_FLAGS) $(COSMOS_VER) $(LOM_VER) $(SORT_FLAG) $(POSTING_FLAG) $(OS64FLAG)
CFLAGS = $(PIC) $(INCLUDE) $(TRACEFLAG) $(PROF) $(DEBUG) $(OSTYPE) $(CC_VER) $(CS_FLAGS) $(COSMOS_VER) $(LOM_VER) $(SORT_FLAG) $(POSTING_FLAG) $(OS64FLAG)

OPTIONS = DEBUG='$(DEBUG)' TRACE='$(TRACE)' PROF='$(PROF)' \
		TRACEFLAG='$(TRACEFLAG)' \
		EVENTFLAG='$(EVENTFLAG)' TARGET_ARCH='$(TARGET_ARCH)' \
		LIBS='$(LIBS)' OS='$(OS)' C='$(C)' CC='$(CC)' PIC='$(PIC)' \
		OSTYPE='$(OSTYPE)' CC_VER='$(CC_VER)' CS_FLAGS='$(CS_FLAGS)' \
		COSMOS_VER='$(COSMOS_VER)' \
		COSMOS='$(COSMOS)' LOM='$(LOM)' OOSQL='$(OOSQL)' \
		CFLAGS='$(CFLAGS)' CCFLAGS='$(CCFLAGS)'

OBJECTS= $(INTERFACE)
SOURCES= $(OBJECTS:.o=.c)
LINTFILES= $(SOURCES:.c=.ln)

LIBS = -L$(O_OOSQL_EXPORT)/lib -loosql -lstdc++
#LIBS = -L$(O_OOSQL_EXPORT)/lib -loosql -lpthread -lrt

DBM_UTILS = OOSQL_CreateDB OOSQL_InitDB OOSQL_DestroyDB OOSQL_AddVolume OOSQL_DropVolume OOSQL_InitVolume OOSQL_AddDevice OOSQL_FormatLogVolume OOSQL_GetDatabaseStatistics OOSQL_FormatCoherencyVolume

.SUFFIXES: .o .c .cxx

.c.o:
	$(CC) $(CCFLAGS) -c $*.c

.cxx.o:
	$(CC) $(CCFLAGS) -c $*.cxx


util: $(DBM_UTILS)

OOSQL_CreateDB: run.o OOSQL_CreateDB.o
	$(CC) -o $@ OOSQL_CreateDB.o $(LIBS) run.o

OOSQL_DestroyDB: run.o OOSQL_DestroyDB.o
	$(CC) -o $@ OOSQL_DestroyDB.o $(LIBS) run.o

OOSQL_InitDB: run.o OOSQL_InitDB.o
	$(CC) -o $@ OOSQL_InitDB.o $(LIBS) run.o

OOSQL_AddVolume: run.o OOSQL_AddVolume.o
	$(CC) -o $@ OOSQL_AddVolume.o $(LIBS) run.o

OOSQL_DropVolume: run.o OOSQL_DropVolume.o
	$(CC) -o $@ OOSQL_DropVolume.o $(LIBS) run.o

OOSQL_InitVolume: run.o OOSQL_InitVolume.o
	$(CC) -o $@ OOSQL_InitVolume.o $(LIBS) run.o

OOSQL_AddDevice: run.o OOSQL_AddDevice.o
	$(CC) -o $@ OOSQL_AddDevice.o $(LIBS) run.o

OOSQL_FormatLogVolume: run.o OOSQL_FormatLogVolume.o
	$(CC) -o $@ OOSQL_FormatLogVolume.o $(LIBS) run.o

OOSQL_GetDatabaseStatistics: run.o OOSQL_GetDatabaseStatistics.o
	$(CC) -o $@ OOSQL_GetDatabaseStatistics.o $(LIBS) run.o

OOSQL_FormatCoherencyVolume: run.o OOSQL_FormatCoherencyVolume.o
	$(CC) -o $@ OOSQL_FormatCoherencyVolume.o $(LIBS) run.o

install:
	cp $(DBM_UTILS) $(O_OOSQL_EXPORT)/bin

clean: 
	/bin/rm -f *.o *.ln *.i $(DBM_UTILS)
#
