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
# Makefile for the LRDS manager
#


.KEEP_STATE:

#include Use ANSI/C compiler
CC = cc
#CC = acc
#CC = /opt/SUNWspro/bin/cc
LINT = alint
LINT.c = $(LINT) $(LINTFLAGS) $(CPPFLAGS)

# added directive for Solaris 2.x OS
OSTYPE = -D_SOLARIS_2x_

# directory of includeable files
INCLUDE = ../Header

TRACEFLAG = -DTRACE
#PROF = -p
CPPFLAGS = -I$(INCLUDE) -DLRDS_INCLUDED
LINTFLAGS = 
#CFLAGS = $(TRACEFLAG) $(PROF) -g -vc 
CFLAGS = $(TRACEFLAG) $(PROF) -O -vc -Xc


INTERFACE = LRDS_AddColumn.o LRDS_AddIndex.o LRDS_CloseRelation.o LRDS_CloseScan.o \
	LRDS_CreateRelation.o LRDS_CreateTuple.o LRDS_DestroyRelation.o \
	LRDS_DestroyTuple.o LRDS_Dismount.o LRDS_DropIndex.o LRDS_Err.o \
	LRDS_FetchTuple.o LRDS_Final.o LRDS_InitDS.o LRDS_InitFinal.o LRDS_MLGF_OpenIndexScan.o \
	LRDS_MLGF_SearchNearTuple.o LRDS_Mount.o LRDS_NextTuple.o \
	LRDS_OpenIndexScan.o LRDS_OpenRelation.o LRDS_OpenSeqScan.o LRDS_Set.o \
	LRDS_Text.o LRDS_UpdateTuple.o LRDS_GetFileIdOfRelation.o \
	LRDS_CollectionSet.o LRDS_CollectionBag.o LRDS_CollectionList.o \
	LRDS_OrderedSet.o LRDS_OrderedSet-WithBackwardScan.o LRDS_FetchColLength.o LRDS_Transaction.o LRDS_Counter.o \
	LRDS_SortRelation.o LRDS_CfgParams.o \
	LRDS_FormatDataVolume.o LRDS_FormatLogVolume.o LRDS_FormatTempDataVolume.o \
	LRDS_FormatCoherencyVolume.o LRDS_ExpandDataVolume.o \
	LRDS_InitRelationBulkLoad.o LRDS_NextRelationBulkLoad.o LRDS_FinalRelationBulkLoad.o \
	LRDS_NextRelationBulkLoad_OrderedSet.o LRDS_NextRelationBulkLoad_Collection.o \
	LRDS_Action.o LRDS_Savepoint.o LRDS_NumberOfDiskIO.o LRDS_AllocFreeHandle.o LRDS_XA.o

NONINTERFACE = lrds_Check.o lrds_CloseSharedRelation.o lrds_Util.o lrds_Collection.o lrds_dummy.o

FORTESTING =

OBJECTS= $(INTERFACE) $(NONINTERFACE) $(FORTESTING)
SOURCES= $(OBJECTS:.o=.c)
LINTFILES= $(SOURCES:.c=.ln)

LRDS.o : $(INTERFACE) $(NONINTERFACE) 	
	@echo ld -r ~~~~ -o $@
	@ld -r $(INTERFACE) $(NONINTERFACE) -o $@ 
	chmod -x $@

.c.ln:
	$(LINT.c) $(OUTPUT_OPTION) -c $<

lint: $(LINTFILES)
	$(LINT) -I$(INCLUDE) $(LINTFILES)

clean: 
	$(RM) -f *.o *.ln *.i TAGS tags

depend:
	$(MAKEDEPEND) -I$(INCLUDE) $(SOURCES)

clear_depend:
	@$(MAKEDEPEND)

#
# DO NOT DELETE THIS LINE -- make depend depends on it.
