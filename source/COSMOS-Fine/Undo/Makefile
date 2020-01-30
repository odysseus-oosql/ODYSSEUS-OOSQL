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
# Makefile for ../Undo directory
#


.KEEP_STATE:

# Use ANSI/C compiler
CC = cc
LINT = alint
LINT.c = $(LINT) $(LINTFLAGS) $(CPPFLAGS)

# directory of includeable files
INCLUDE = ../Header

TRACEFLAG = -DTRACE
#PROF = -p
CPPFLAGS = -I$(INCLUDE) $(TRACEFLAG) -DCCRL
#CFLAGS = $(PROF) -g -vc -Xc
LINTFLAGS = 


INTERFACE = Undo_BtM_ChangeLeafEntry.o \
Undo_BtM_CopyPage.o \
Undo_BtM_DeleteInternalEntries.o \
Undo_BtM_DeleteInternalEntriesWithoutMoreUpdate.o \
Undo_BtM_DeleteInternalEntry.o \
Undo_BtM_DeleteLeafEntries.o \
Undo_BtM_DeleteLeafEntriesWithoutMoreUpdate.o \
Undo_BtM_DeleteLeafEntry.o \
Undo_BtM_DeleteOidFromLeafEntry.o \
Undo_BtM_DeleteOidFromOverflowPage.o \
Undo_BtM_DeleteOidWithLeafEntry.o \
Undo_BtM_DeleteOidsFromOverflowPage.o \
Undo_BtM_InitLeafPage.o \
Undo_BtM_InsertInternalEntries.o \
Undo_BtM_InsertInternalEntry.o \
Undo_BtM_InsertLeafEntries.o \
Undo_BtM_InsertOidIntoLeafEntry.o \
Undo_BtM_InsertOidIntoOverflowPage.o \
Undo_BtM_InsertOidWithLeafEntry.o \
Undo_BtM_InsertOidsIntoOverflowPage.o \
Undo_BtM_MakeRootPage.o \
Undo_BtM_MergeLeafPage.o \
Undo_BtM_ModifyFirstChildOfInternalPage.o \
Undo_BtM_ModifyIndexIdOfBtreePage.o \
Undo_BtM_ModifyLeafChainNextLink.o \
Undo_BtM_ModifyLeafChainPrevLink.o \
Undo_BtM_ModifyOverflowChainNextLink.o \
Undo_BtM_ModifyOverflowChainPrevLink.o \
Undo_BtM_ReplaceInternalEntryKeyValue.o \
Undo_BtM_ReplaceRootPageWithChildPage.o \
Undo_BtM_SplitLeafPage.o \
Undo_BtM_SplitOverflowPage.o \
Undo_LOT_DeleteData.o \
Undo_LOT_DeleteInternalEntries.o \
Undo_LOT_InsertData.o \
Undo_LOT_ReplaceInternalEntries.o \
Undo_LOT_UpdateCountFields.o \
Undo_LOT_WriteData.o \
Undo_MLGF_ChangeLeafEntry.o\
Undo_MLGF_DeleteDirectoryEntries.o\
Undo_MLGF_DeleteDirectoryEntry.o\
Undo_MLGF_DeleteLeafEntries.o\
Undo_MLGF_DeleteLeafEntry.o\
Undo_MLGF_DeleteObjectFromLeafEntry.o\
Undo_MLGF_DeleteObjectWithLeafEntry.o\
Undo_MLGF_DeleteObjectFromOverflowPage.o\
Undo_MLGF_DeleteObjectsFromOverflowPage.o\
Undo_MLGF_InsertDirectoryEntries.o\
Undo_MLGF_InsertDirectoryEntry.o\
Undo_MLGF_InsertLeafEntries.o\
Undo_MLGF_InsertObjectIntoLeafEntry.o\
Undo_MLGF_InsertObjectWithLeafEntry.o\
Undo_MLGF_InsertObjectIntoOverflowPage.o\
Undo_MLGF_InsertObjectsIntoOverflowPage.o\
Undo_MLGF_ModifyDirectoryEntry.o \
Undo_MLGF_ModifyHashValuesOfDirectoryEntry.o \
Undo_MLGF_ModifyIndexIdOfMlgfPage.o \
Undo_MLGF_ModifyNumValidBitsOfDirectoryEntry.o \
Undo_MLGF_ModifyOverflowChainNextLink.o \
Undo_MLGF_ModifyOverflowChainPrevLink.o \
Undo_MLGF_ModifyThetaValueOfDirectoryEntry.o \
Undo_MLGF_RootPageLevelDown.o \
Undo_MLGF_RootPageLevelUp.o \
Undo_OM_AppendToSmallObject.o \
Undo_OM_ChangeObject.o \
Undo_OM_CreateSmallObject.o \
Undo_OM_DecreaseObjHdrLength.o \
Undo_OM_DeleteFromSmallObject.o \
Undo_OM_DestroySmallObject.o \
Undo_OM_IncreaseObjHdrLength.o \
Undo_OM_InsertIntoSmallObject.o \
Undo_OM_ModifyLastPageInCatalog.o \
Undo_OM_ModifyTrainSegmentIDInCatalog.o \
Undo_OM_ModifyPageListNextLink.o \
Undo_OM_ModifyPageListPrevLink.o \
Undo_OM_SetObjectHdr.o \
Undo_OM_WriteSmallObject.o \
Undo_RDsM_AllocTrains.o \
Undo_RDsM_FreeTrains.o \
Undo_RDsM_Modify_Extent_Link.o \
Undo_RDsM_Modify_Free_Extent_List_Header.o


NONINTERFACE =

FORTESTING =

OBJECTS= $(INTERFACE) $(NONINTERFACE) $(FORTESTING)
SOURCES= $(OBJECTS:.o=.c)
LINTFILES= $(SOURCES:.c=.ln)

Undo.o : $(OBJECTS)
	@echo ld -r ~~~~ -o $@
	@ld -r $(OBJECTS) -o $@
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
	$(MAKEDEPEND)

#
# DO NOT DELETE THIS LINE -- make depend depends on it.
