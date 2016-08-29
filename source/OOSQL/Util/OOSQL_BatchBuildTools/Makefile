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

#.KEEP_STATE:


C = gcc -w 
CC = gcc -w -x c++ 

COSMOS = $(O_KAOSS)
LOM    = $(O_LOM_SERVER)
OOSQL  = $(O_OOSQL_SERVER)

COSMOS_VER = -DLRDSV2 -DCOSMOS_MULTIVOLUME
LOM_VER    = -DLOM_DEFINED
OSTYPE = -D_SOLARIS_2x_
CC_VER = -D_SUNCPP_V4_ -DTEMPLATE_NOT_SUPPORTED
CS_FLAGS = -D_OOSQL_SERVER_ -DLOM_DEFINED
#DEBUG = -O2
DEBUG = -g 
PIC = 
POSTING_FLAG = -DUSE_WITHOUT_WEIGHT -DUSE_SUBINDEX

CCFLAGS = $(PIC) $(INCLUDE) $(TRACEFLAG) $(PROF) $(DEBUG) $(OSTYPE) $(CC_VER) $(CS_FLAGS) $(COSMOS_VER) $(LOM_VER) $(SORT_FLAG) $(POSTING_FLAG) $(OS64FLAG)
CFLAGS = $(PIC) $(INCLUDE) $(TRACEFLAG) $(PROF) $(DEBUG) $(OSTYPE) $(CC_VER) $(CS_FLAGS) $(COSMOS_VER) $(LOM_VER) $(SORT_FLAG) $(POSTING_FLAG) $(OS64FLAG)

INCLUDE = -I$(COSMOS)/Header -I$(LOM)/INCLUDE -I$(OOSQL)/include -I$(OOSQL)/Compiler/include -I$(O_GEOM)/Header

LIB     = -L$(O_OOSQL_EXPORT)/lib -loosql -lstdc++
#LIB     = -L$(O_OOSQL_EXPORT)/lib -loosql -lpthread -lrt

LINT = alint
LINT.c = $(LINT) $(LINTFLAGS) $(CPPFLAGS)

OOSQL_GETTEXTCONTENT_OBJ = OOSQL_GetTextContent.o
OOSQL_EXTRACTKEYWORD_OBJ = OOSQL_ExtractKeyword.o
OOSQL_SORTPOSTING_OBJ = OOSQL_SortPosting.o
OOSQL_BUILDTEXTINDEX_OBJ = OOSQL_BuildTextIndex.o
OOSQL_SORTAUXINDEX_OBJ = OOSQL_SortAuxIndex.o
OOSQL_BUILDAUXINDEX_OBJ = OOSQL_BuildAuxIndex.o
OOSQL_GETMAPPING_OBJ = OOSQL_GetMapping.o
OOSQL_MAPPOSTING_OBJ = OOSQL_MapPosting.o
OOSQL_STOREPOSTING_OBJ = OOSQL_StorePosting.o
OOSQL_SORTSTOREDPOSTING_OBJ = OOSQL_SortStoredPosting.o
OOSQL_UPDATETEXTDESCRIPTOR_OBJ = OOSQL_UpdateTextDescriptor.o
OOSQL_DELETETEXTINDEX_OBJ = OOSQL_DeleteTextIndex.o
OOSQL_BATCHDELETE_OBJ = OOSQL_BatchDelete.o

EXEC = OOSQL_BuildTextIndex OOSQL_ExtractKeyword OOSQL_SortPosting OOSQL_MapPosting OOSQL_StorePosting OOSQL_SortStoredPosting OOSQL_UpdateTextDescriptor OOSQL_DeleteTextIndex OOSQL_BatchDelete sort

all: $(EXEC)

OOSQL_GetTextContent: $(OOSQL_GETTEXTCONTENT_OBJ) 
	$(C) -o $@ $(OOSQL_GETTEXTCONTENT_OBJ) $(LIB)

OOSQL_ExtractKeyword: $(OOSQL_EXTRACTKEYWORD_OBJ) 
	$(C) -o $@ $(OOSQL_EXTRACTKEYWORD_OBJ) $(LIB)

OOSQL_SortPosting: $(OOSQL_SORTPOSTING_OBJ) 
	$(C) -o $@ $(OOSQL_SORTPOSTING_OBJ) $(LIB)

OOSQL_BuildTextIndex: $(OOSQL_BUILDTEXTINDEX_OBJ) 
	$(C) -o $@ $(OOSQL_BUILDTEXTINDEX_OBJ) $(LIB)

OOSQL_SortAuxIndex: $(OOSQL_SORTAUXINDEX_OBJ) 
	$(C) -o $@ $(OOSQL_SORTAUXINDEX_OBJ) $(LIB)

OOSQL_BuildAuxIndex: $(OOSQL_BUILDAUXINDEX_OBJ) 
	$(C) -o $@ $(OOSQL_BUILDAUXINDEX_OBJ) $(LIB)

OOSQL_GetMapping: $(OOSQL_GETMAPPING_OBJ) 
	$(C) -o $@ $(OOSQL_GETMAPPING_OBJ) $(LIB)

OOSQL_MapPosting: $(OOSQL_MAPPOSTING_OBJ) 
	$(C) -o $@ $(OOSQL_MAPPOSTING_OBJ) $(LIB)

OOSQL_StorePosting: $(OOSQL_STOREPOSTING_OBJ) 
	$(C) -o $@ $(OOSQL_STOREPOSTING_OBJ) $(LIB)

OOSQL_SortStoredPosting: $(OOSQL_SORTSTOREDPOSTING_OBJ) 
	$(C) -o $@ $(OOSQL_SORTSTOREDPOSTING_OBJ) $(LIB)

OOSQL_UpdateTextDescriptor: $(OOSQL_UPDATETEXTDESCRIPTOR_OBJ) 
	$(C) -o $@ $(OOSQL_UPDATETEXTDESCRIPTOR_OBJ) $(LIB)

OOSQL_DeleteTextIndex: $(OOSQL_DELETETEXTINDEX_OBJ) 
	$(C) -o $@ $(OOSQL_DELETETEXTINDEX_OBJ) $(LIB)

OOSQL_BatchDelete: $(OOSQL_BATCHDELETE_OBJ) 
	$(C) -o $@ $(OOSQL_BATCHDELETE_OBJ) $(LIB)

sort: sort.o
	$(C) -o $@ $@.o

install: $(EXEC)
	cp $(EXEC) OOSQL_MakeTextIndex $(O_OOSQL_EXPORT)/bin

.SUFFIXES: .c .cxx .o
.cxx.o:
	$(CC) $(CCFLAGS) -c $<

.c.o:
	$(C) $(CCFLAGS) -c $<

clean:
	rm -f *.o $(EXEC)
