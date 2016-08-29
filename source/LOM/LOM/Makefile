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

.KEEP_STATE:

#include Use ANSI/C compiler
LINT = lint
LINT.c = $(LINT) $(LINTFLAGS) $(CPPFLAGS)

AR = ar

# directory of includeable files
LOM_INCLUDE = $(O_LOM_COMMON)/INCLUDE
COSMOS_INCLUDE = $(O_KAOSS)/Header

OPTIONS = C_DEBUGFLAG='$(C_DEBUGFLAG)' DEBUGFLAG='$(DEBUGFLAG)' TRACEFLAG='$(TRACEFLAG)' PROF='$(PROF)'\
	EVENTFLAG='$(EVENTFLAG)' CC='$(CC)' CC_VER='$(CC_VER)' C='$(C)' C_VER='$(C_VER)' PIC='$(PIC)' \
	TARGET_ARCH='$(TARGET_ARCH)'  OS='$(OS)' MISCFLAG='$(MISCFLAG)' \
	SERVERFLAG='$(SERVERFLAG)' CLIENTFLAG='$(CLIENTFLAG)' VERSIONFLAG='$(VERSIONFLAG)'

LINTFLAGS = 

#CFLAGS = $(C_DEBUGFLAG) $(TRACEFLAG) $(PROF) $(C_VER) $(PIC) $(OS) $(MISCFLAG) $(SERVERFLAG) $(VERSIONFLAG) -I$(COSMOS_INCLUDE) -I$(LOM_INCLUDE)
CFLAGS = $(C_DEBUGFLAG) $(TRACEFLAG) $(PROF) $(C_VER) $(PIC) $(OS) $(MISCFLAG) $(SERVERFLAG) $(VERSIONFLAG) -I$(COSMOS_INCLUDE) -I$(LOM_INCLUDE) -I$(O_DLLSERVER) -I$(O_DLLBROKER)


#COMMON_INTERFACE = LOM_CreateObject.o  LOM_UpdateObject.o LOM_FetchObject.o 
COMMON_INTERFACE += lom_GetClassId.o LOM_GetNewClassId.o\
		lom_GetAndIncrementLastRelationshipId.o \
		LOM_Err.o \
		LOM_DestroyClass.o LOM_OpenClass.o LOM_NextObject.o LOM_CloseClass.o \
		LOM_CloseScan.o LOM_OpenIndexScan.o LOM_CreateObjectByColList.o \
		LOM_OpenSeqScan.o LOM_FetchObjectByColList.o \
		LOM_UpdateObjectByColList.o LOM_DestroyObject.o lom_GetClassName.o \
		lom_GetSuperClass.o lom_GetSubClass.o \
		LOM_Relationship_Create.o LOM_Relationship_Destroy.o LOM_Relationship_CreateInstance.o \
		LOM_Relationship_Traversal.o LOM_Relationship_GetId.o LOM_Relationship_DestroyInstance.o LOM_AddIndex.o LOM_DropIndex.o\
		lom_OpenClassByClassId.o lom_Check.o lom_Util.o \
		LOM_Init.o LOM_Final.o LOM_Mount.o LOM_Dismount.o LOM_Text.o _lom_Text.o LOM_Text_Counter.o \
		lom_Text_KeywordExtract.o stemize.o LOM_Trans.o \
		LOM_NamedObject.o  LOM_Literal.o LOM_FetchColLength.o LOM_Text_Admin.o \
		lom_Text_GetAndIncrementLastKeywordExtractorNo.o lom_Text_GetAndIncrementLastFilterNo.o \
		LOM_FetchObjectByEncodedObject.o LOM_CreateObjectByEncodedObject.o LOM_UpdateObjectByEncodedObject.o\
		LOM_ODMG_Collection.o LOM_Connection.o \
		LOM_Text_Stemizer.o lom_Text_GetAndIncrementLastStemizerNo.o \
		lom_Text_UpdateEmbeddedAttrs.o LOM_MLGF.o LOM_BulkLoad.o lom_Text_Compression.o \
		LOM_Format.o ODYS_Format.o dlfcn.o lom_dlfcn.o LOM_GetOpenClassNum.o LOM_DeferredDestroy.o \
		_lom_ScanTable.o LOM_Statistics.o dllbroker_clnt.o dllbroker_xdr.o dllserver_clnt.o dllserver_xdr.o \
		lom_RPCdlfcn.o lom_CallDll.o LOM_CfgParams.o LOM_Counter.o LOM_AlterClass.o

#KEYWORD_INTERFACE = $(IR_SYSTEM_PATH)/morph.o

INTERFACE = $(COMMON_INTERFACE) LOM_CreateClassForOdysseus.o lom_CheckInheritance.o

.c.o:
	$(C) $(CFLAGS) -c $*.c


OBJECTS= $(INTERFACE)
SOURCES= $(OBJECTS:.o=.c)
LINTFILES= $(SOURCES:.c=.ln)

LOM.o : $(INTERFACE) $(NONINTERFACE) 	
	@echo ld -r *.o $(KEYWORD_INTERFACE) -o $@
	@ld -r $(INTERFACE) $(KEYWORD_INTERFACE) -o $@ 
	chmod -x LOM.o

LOM_Extra.o : $(INTERFACE) $(NONINTERFACE)    
	$(c) -KPIC -c -g $(CPPFLAGS) $(TRACEFLAG) convert.c
	@echo ld -r *.o $(KEYWORD_INTERFACE) -o LOM.o
	@ld -r $(INTERFACE) $(KEYWORD_INTERFACE) -o LOM.o 
	chmod -x LOM.o
	@echo ld -G -o liblom.so -h liblom.so $(INTERFACE) $(KEYWORD_INTERFACE)
	@ld -G -o liblom.so -h liblom.so $(INTERFACE) $(KEYWORD_INTERFACE) 
	chmod -x liblom.so
	cp LOM.o ../lib
	cp liblom.so ../lib

$(SOURCES): $(LOM_INCLUDE)/LOM_Err.h
$(LOM_INCLUDE)/LOM.h: $(LOM_INCLUDE)/LOM_Err.h
$(LOM_INCLUDE)/LOM_Internal.h: $(LOM_INCLUDE)/LOM_Err.h

LOM_Err.o : LOM_Err.i

LOM_Err.i : LOM_Err.m4
	m4 $(LOM_INCLUDE)/Handle.m4 LOM_Err.m4 > LOM_Err.i

$(LOM_INCLUDE)/LOM_Err.h : LOM_Err.m4
	m4 $(LOM_INCLUDE)/defs.m4 LOM_Err.m4 > $(LOM_INCLUDE)/LOM_Err.h

.c.ln:
	$(LINT.c) $(OUTPUT_OPTION) -c $<

lint: $(LINTFILES)
	$(LINT) -I$(LOM_INCLUDE) $(LINTFILES)

clean: 
	/bin/rm -f *.o *.ln
#
