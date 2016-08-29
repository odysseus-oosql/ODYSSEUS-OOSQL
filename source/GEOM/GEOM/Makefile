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
# Makefile for the GEOM manager
#

.KEEP_STATE:

#include Use ANSI/C compiler
CC = gcc -w -x c++ -fsigned-char
C = gcc -w -fsigned-char
LINT = alint
LINT.c = $(LINT) $(LINTFLAGS) $(CPPFLAGS)

# directory of includeable files
INCLUDE = -I../Header -I$(O_KAOSS)/Header -I$(O_LOM_COMMON)/INCLUDE -I$(O_OOSQL_COMMON)/include

TRACEFLAG = -DTRACE 
#PROF = -pg -xpg
DEBUG = -g
#DEBUG = -fstrength-reduce -fthread-jumps -fcse-follow-jumps -fcse-skip-blocks -frerun-cse-after-loop -felide-constructors -fexpensive-optimizations -fdelayed-branch
ifneq ($(DEBUG),-g)
ifneq ($(shell getconf LONG_BIT),64)
DEBUG += -fschedule-insns -fschedule-insns2
endif
endif

CPPFLAGS = -I$(INCLUDE) 
LINTFLAGS = 
CFLAGS = $(INCLUDE) -fPIC -DUSE_RPC -D_GEOM_ -D_LINUX_7x_ -D__LINUX__ -DCOSMOS_MULTIVOLUME -D_RWSTD_COMPILE_INSTANTIATE

# GEOM(OPENGIS) has not been made thread-safe yet. In case GIS function is needed, do not use GEOM in multi-thread configuration.
# use a multi-process version instead; Otherwise exclude GEOM by compiling with "-DSLIMDOWN_OPENGIS" flag below.
CFLAGS += -DSLIMDOWN_OPENGIS
CFLAGS += -DENABLE_OPENGIS_OPTIMIZATION
CFLAGS += $(DEBUG)
#CFLAGS += $(TRACEFLAG)
#CFLAGS += $(PROF)
#CFLAGS += $(TRACEFLAG) $(PROF)

INTERFACE = GEO_Init.o Geometry.o geo_Catalog.o geo_UDT.o UDF.o GEO_OIDHashTable.o
 
NONINTERFACE = 

OBJECTS= $(INTERFACE) $(NONINTERFACE)
SOURCES= $(OBJECTS:.o=.c)
LINTFILES= $(SOURCES:.c=.ln)

geom.o : $(INTERFACE) $(NONINTERFACE) 	
	@echo ld -r $(INTERFACE) $(NONINTERFACE) -o $@
	@ld -r $(INTERFACE) $(NONINTERFACE) -o $@ 
	chmod -x $@

#$(SOURCES): $(INCLUDE)/GEO_Err.h
#$(INCLUDE)/GEO.h: $(INCLUDE)/GEO_Err.h
#$(INCLUDE)/GEO_Internal.h: $(INCLUDE)/GEO_Err.h
#
#
#GEO_Err.o : GEO_Err.i	
#
#GEO_Err.i : GEO_Err.m4
#	m4 $(INCLUDE)/Handle.m4 GEO_Err.m4 > GEO_Err.i
#
#$(INCLUDE)/GEO_Err.h : GEO_Err.m4
#	m4 $(INCLUDE)/defs.m4 GEO_Err.m4 > $(INCLUDE)/GEO_Err.h

.SUFFIXES: .C .o .c .cxx

.cxx.o:
	$(CC) -c $*.cxx $(CFLAGS)

.C.o:
	$(CC) -c $*.C $(CFLAGS)

.c.o:
	$(C) -c $*.c $(CPPFLAGS) $(CFLAGS)

.c.ln:
	$(LINT.c) $(OUTPUT_OPTION) -c $<

lint: $(LINTFILES)
	$(LINT) -I$(INCLUDE) $(LINTFILES)

clean: 
	/bin/rm -f *.o *.ln


