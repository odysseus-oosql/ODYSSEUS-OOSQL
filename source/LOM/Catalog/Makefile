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
# Makefile for the OBfM manager
#

.KEEP_STATE:

#include Use ANSI/C compiler
LINT = alint
LINT.c = $(LINT) $(LINTFLAGS) $(CPPFLAGS)

AR = ar

# directory of includeable files
INCLUDE = -I$(O_KAOSS)/Header -I$(O_LOM_COMMON)/INCLUDE

LINTFLAGS = 
CFLAGS = $(C_DEBUGFLAG) $(TRACEFLAG) $(PROF) $(C_VER) $(PIC) $(OS) $(MISCFLAG) $(SERVERFLAG) $(VERSIONFLAG) $(INCLUDE)

OPTIONS = C_DEBUGFLAG='$(C_DEBUGFLAG)' DEBUGFLAG='$(DEBUGFLAG)' TRACEFLAG='$(TRACEFLAG)' PROF='$(PROF)'\
	EVENTFLAG='$(EVENTFLAG)' CC='$(CC)' CC_VER='$(CC_VER)' C='$(C)' C_VER='$(C_VER)' PIC='$(PIC)' \
	TARGET_ARCH='$(TARGET_ARCH)' OS='$(OS)' MISCFLAG='$(MISCFLAG)' \
	SERVERFLAG='$(SERVERFLAG)' CLIENTFLAG='$(CLIENTFLAG)' VERSIONFLAG='$(VERSIONFLAG)'


INTERFACE = Catalog_Mount.o Catalog_Dismount.o Catalog_FlushAll.o\
			Catalog_GetClassInfo.o Catalog_RemoveClassInfo.o \
			Catalog_Err.o  Catalog_GetAttrInfo.o\
			catalog_getFreeEntry.o \
			Catalog_GetIndexInfo.o	Catalog_GetMethodInfo.o \
			Catalog_Util.o Catalog_GetRelationshipInfo.o

OBJECTS= $(INTERFACE)
SOURCES= $(OBJECTS:.o=.c)
LINTFILES= $(SOURCES:.c=.ln)

.c.o:
	$(C) $(CFLAGS) -c $*.c

catalog.o : $(INTERFACE) $(NONINTERFACE) 	
	@echo ld -r ~~~~ -o $@
	@ld -r $(INTERFACE) $(NONINTERFACE) -o $@ 
	chmod -x $@
	@echo ld -G -o libcatalog.so -h libcatalog.so $(INTERFACE) $(NONINTERFACE)
	@ld -G -o libcatalog.so -h libcatalog.so $(INTERFACE) $(NONINTERFACE) 
	chmod -x libcatalog.so
#	cp $@ ../lib
#	cp libcatalog.so ../lib
#	cp catalog.o $(ODYS_EXPORT)/lib/stand-alone
#	cp libcatalog.so $(ODYS_EXPORT)/lib/stand-alone

.c.ln:
	$(LINT.c) $(OUTPUT_OPTION) -c $<

lint: $(LINTFILES)
	$(LINT) $(INCLUDE) $(LINTFILES)

clean: 
	/bin/rm -f *.o *.ln *.so
#
