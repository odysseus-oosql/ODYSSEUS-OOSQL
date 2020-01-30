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
# Makefile for ../Redo directory
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
CFLAGS = $(PROF) -g -vc -Xc
LINTFLAGS = 


INTERFACE = Redo_BtM_ChangeLeafEntry.o \
Redo_BtM_CopyPage.o \
Redo_BtM_DeleteInternalEntries.o \
Redo_BtM_DeleteInternalEntry.o \
Redo_BtM_DeleteLeafEntries.o \
Redo_BtM_DeleteLeafEntry.o \
Redo_BtM_DeleteOidFromLeafEntry.o \
Redo_BtM_DeleteOidFromOverflowPage.o \
Redo_BtM_DeleteOidWithLeafEntry.o \
Redo_BtM_DeleteOidsFromOverflowPage.o \
Redo_BtM_InitInternalPage.o \
Redo_BtM_InitLeafPage.o \
Redo_BtM_InitOverflowPage.o \
Redo_BtM_InsertInternalEntries.o \
Redo_BtM_InsertInternalEntry.o \
Redo_BtM_InsertLeafEntries.o \
Redo_BtM_InsertLeafEntry.o \
Redo_BtM_InsertOidIntoLeafEntry.o \
Redo_BtM_InsertOidWithLeafEntry.o \
Redo_BtM_InsertOidIntoOverflowPage.o \
Redo_BtM_InsertOidsIntoOverflowPage.o \
Redo_BtM_MakeRootPage.o \
Redo_BtM_MergeLeafPage.o \
Redo_BtM_ModifyFirstChildOfInternalPage.o \
Redo_BtM_ModifyIndexIdOfBtreePage.o \
Redo_BtM_ModifyLeafChainNextLink.o \
Redo_BtM_ModifyLeafChainPrevLink.o \
Redo_BtM_ModifyOverflowChainNextLink.o \
Redo_BtM_ModifyOverflowChainPrevLink.o \
Redo_BtM_ReplaceInternalEntryKeyValue.o \
Redo_BtM_ReplaceRootPageWithChildPage.o \
Redo_BtM_SplitLeafPage.o \
Redo_BtM_SplitOverflowPage.o \
Redo_BtM_UndoSplitLeafPage.o \
Redo_BtM_UndoSplitOverflowPage.o \
Redo_LOT_InitLeafNode.o \
Redo_LOT_DeleteData.o \
Redo_LOT_InsertData.o \
Redo_LOT_WriteData.o \
Redo_LOT_InitInternalNodePage.o \
Redo_LOT_DeleteInternalEntries.o \
Redo_LOT_ReplaceInternalEntries.o \
Redo_LOT_UndoDeleteInternalEntries.o \
Redo_LOT_UpdateCountFields.o \
Redo_MLGF_ChangeLeafEntry.o\
Redo_MLGF_CopyPage.o \
Redo_MLGF_DeleteDirectoryEntries.o\
Redo_MLGF_DeleteDirectoryEntry.o\
Redo_MLGF_DeleteLeafEntries.o\
Redo_MLGF_DeleteLeafEntry.o\
Redo_MLGF_DeleteObjectFromLeafEntry.o\
Redo_MLGF_DeleteObjectWithLeafEntry.o\
Redo_MLGF_DeleteObjectFromOverflowPage.o\
Redo_MLGF_DeleteObjectsFromOverflowPage.o\
Redo_MLGF_InsertDirectoryEntries.o\
Redo_MLGF_InsertDirectoryEntry.o \
Redo_MLGF_InsertLeafEntries.o \
Redo_MLGF_InsertLeafEntry.o \
Redo_MLGF_InsertObjectIntoLeafEntry.o\
Redo_MLGF_InsertObjectWithLeafEntry.o\
Redo_MLGF_InsertObjectIntoOverflowPage.o\
Redo_MLGF_InsertObjectsIntoOverflowPage.o\
Redo_MLGF_InitDirectoryPage.o \
Redo_MLGF_InitLeafPage.o \
Redo_MLGF_InitOverflowPage.o \
Redo_MLGF_ModifyDirectoryEntry.o \
Redo_MLGF_ModifyHashValuesOfDirectoryEntry.o \
Redo_MLGF_ModifyIndexIdOfMlgfPage.o \
Redo_MLGF_ModifyNumValidBitsOfDirectoryEntry.o \
Redo_MLGF_ModifyOverflowChainNextLink.o \
Redo_MLGF_ModifyOverflowChainPrevLink.o \
Redo_MLGF_ModifyThetaValueOfDirectoryEntry.o \
Redo_MLGF_RootPageLevelDown.o \
Redo_MLGF_RootPageLevelUp.o \
Redo_OM_AppendToSmallObject.o \
Redo_OM_ChangeObject.o \
Redo_OM_CreateSmallObject.o \
Redo_OM_DecreaseObjHdrLength.o \
Redo_OM_DeleteFromSmallObject.o \
Redo_OM_DestroySmallObject.o \
Redo_OM_GetUniques.o \
Redo_OM_IncreaseObjHdrLength.o \
Redo_OM_InitSlottedPage.o \
Redo_OM_InsertIntoSmallObject.o \
Redo_OM_ModifyLastPageInCatalog.o \
Redo_OM_ModifyTrainSegmentIDInCatalog.o \
Redo_OM_ModifyPageListNextLink.o \
Redo_OM_ModifyPageListPrevLink.o \
Redo_OM_SetObjectHdr.o \
Redo_OM_WriteSmallObject.o \
Redo_RDsM_AllocTrains.o \
Redo_RDsM_DeleteMetaDictEntry.o \
Redo_RDsM_FreeTrains.o \
Redo_RDsM_FreeTrainsWithDeferment.o \
Redo_RDsM_GetUniques.o \
Redo_RDsM_InsertMetaDictEntry.o \
Redo_RDsM_SetMetaDictEntry.o \
Redo_RDsM_Modify_Extent_Link.o \
Redo_RDsM_Modify_Free_Extent_List_Header.o


NONINTERFACE =

FORTESTING =

OBJECTS= $(INTERFACE) $(NONINTERFACE) $(FORTESTING)
SOURCES= $(OBJECTS:.o=.c)
LINTFILES= $(SOURCES:.c=.ln)

Redo.o : $(OBJECTS)
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
