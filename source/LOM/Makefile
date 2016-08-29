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
# Makefile for the LOM manager
#
#Profile flag
#PROF = -pg

#Debugging mode or optimization flag
DEBUGFLAG = -g
#DEBUGFLAG = -fstrength-reduce -fthread-jumps -fcse-follow-jumps -fcse-skip-blocks -frerun-cse-after-loop -felide-constructors -fexpensive-optimizations -fdelayed-branch
ifneq ($(DEBUGFLAG),-g)
ifneq ($(shell getconf LONG_BIT),64)
DEBUGFLAG += -fschedule-insns -fschedule-insns2
endif
endif
C_DEBUGFLAG = $(DEBUGFLAG)

#Trace information flag
#TRACEFLAG =

#Compiler info.
CC = gcc -w  
C = gcc -w  

#Compiler version flag
CC_VER = -D_SUNCPP_V4_ # for compiling by Sun C++ 4.0

C_VER =

#Position independent code information
PIC = -fPIC

# dependency list generating utility
MAKEDEPEND = makedepend

#OS info.
OS = -D_LINUX_7x_
#OS = -D_SOLARIS_2x_

#Misc. info
#You should explain all flags below one by one
MISCFLAG = -D_NOTEMPLATEBUG_  # for avoiding template bug
MISCFLAG += -DINTERNAL # for internal compilation(for released odysseus, donot include INTERNAL flag
MISCFLAG += -D__ODMG_93__ # ODMG-93 flag
#MISCFLAG += -DTEMPLATE_NOT_SUPPORTED # emulate template by macro (OOSQL-Server)

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
SERVERFLAG += -DUSE_RPC
ifeq ($(shell getconf LONG_BIT),64)
SERVERFLAG += -D_LP64 # for 64bit
#SERVERFLAG += -DSUPPORT_LARGE_DATABASE2 # for Large Database (Should consistent with COSMOS/Header/param.h)
endif
#SERVERFLAG += -DCOMPRESSION
#SERVERFLAG += -DSLIMDOWN_TEXTIR # slimdown for text IR functionality

#Version Flag
#VERSIONFLAG = -DODYS_IR_S_V2.0


####################
# Compiler Options #
####################

INCLUDE = -I$(O_KAOSS)/Header -I$(O_LOM_COMMON)/INCLUDE -I$(O_COMMON)/include 

CCFLAGS = $(PIC) $(INCLUDE) $(TRACEFLAG) $(PROF) $(DEBUGFLAG) $(OS) $(CC_VER) $(SERVERFLAG) $(MISCFLAG)

CFLAGS = $(PIC) $(INCLUDE) $(TRACEFLAG) $(PROF) $(C_DEBUGFAG) $(OS) $(C_VER) $(SERVERFLAG) $(MISCFLAG)

LINT = alint
LINT.c = $(LINT) $(LINTFLAGS) $(CPPFLAGS)

OPTIONS = C_DEBUGFLAG='$(C_DEBUGFLAG)' DEBUGFLAG='$(DEBUGFLAG)' TRACEFLAG='$(TRACEFLAG)' PROF='$(PROF)'\
	EVENTFLAG='$(EVENTFLAG)' CC='$(CC)' CC_VER='$(CC_VER)' C='$(C)' C_VER='$(C_VER)' PIC='$(PIC)' \
	TARGET_ARCH='$(TARGET_ARCH)' OS='$(OS)' MISCFLAG='$(MISCFLAG)' \
	SERVERFLAG='$(SERVERFLAG)' CLIENTFLAG='$(CLIENTFLAG)' VERSIONFLAG='$(VERSIONFLAG)'

.KEEP_STATE:

all: 
	(cd LOM; $(MAKE) $(OPTIONS))
	(cd Catalog; $(MAKE) $(OPTIONS))
	
clean: 
	cd LOM; make clean
	cd Catalog; make clean
