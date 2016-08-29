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
#/*    Coarse-Granule Locking (Volume Lock) Version                            */
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
# Makefile for ../RDsM directory
#

.KEEP_STATE:

RM = /bin/rm

# dependency list generating utility
MAKEDEPEND = makedepend

# Use ANSI/C compiler
CC = cc
LINT = alint
LINT.c = $(LINT) $(LINTFLAGS) $(CFLAGS) $(CPPFLAGS)

# directory of #include files
INCLUDE = ../Header

TRACEFLAG = -DTRACE 
#PROF = -p
CPPFLAGS = -I$(INCLUDE) $(TRACEFLAG)
CFLAGS = $(PROF) -g -vc -Xc
CFLAGS = $(PROF) -g
LINTFLAGS =


INTERFACE = RDsM_Format.o RDsM_ExpandVolume.o \
	RDsM_ReadTrain.o RDsM_WriteTrain.o RDsM_ReadTrains.o RDsM_WriteTrains.o \
	RDsM_ReadTrainForLogVolume.o RDsM_WriteTrainForLogVolume.o \
	RDsM_Init.o RDsM_Mount.o RDsM_Dismount.o RDsM_Final.o RDsM_GetMetaEntry.o RDsM_GetUnique.o \
	RDsM_FreeTrains.o RDsM_Create.o RDsM_Drop.o RDsM_AllocTrains.o RDsM_GetVolNo.o RDsM_InsertMetaEntry.o \
	RDsM_PageIdToExtNo.o RDsM_ExtNoToPageId.o RDsM_DeleteMetaEntry.o \
	RDsM_GetStatistics.o RDsM_GetMountedVolumes.o RDsM_Transaction.o \
	RDsM_GetVolumeInfo.o RDsM_GetSizeOfExt.o RDsM_AllocContigTrainsInExt.o \
	RDsM_VolumeLock.o RDsM_ReadTrainsForLogVolume.o RDsM_WriteTrainsForLogVolume.o

NONINTERFACE = RDsM_bit.o RDsM_alloc_free_ext.o RDsM_alloc_trains_in_ext.o RDsM_eff.o \
	RDsM_free_train_in_ext.o RDsM_prev_next_ext.o RDsM_set_page_map.o \
	rdsm_ReadTrain.o rdsm_WriteTrain.o rdsm_Mapping.o rdsm_ReadWriteBuffer.o

FORDEBUGING = RDsM_P_ExtEntryArray.o RDsM_P_PageMap.o RDsM_P_Unique.o RDsM_P_UsedPageMap.o

OBJECTS = $(INTERFACE) $(NONINTERFACE) $(FORDEBUGING)
SOURCES = $(OBJECTS:.o=.c)
LINTFILES = $(SOURCES:.c=.ln)


RDsM.o : $(OBJECTS)
	@echo ld -r ~~~ -o $@
	@ld -r $(OBJECTS) -o $@  
	chmod -x $@

.c.ln:
	$(LINT.c) $(OUTPUT_OPTION) -c $<

lint: $(LINTFILES)
	$(LINT) -I$(INCLUDE) $(LINTFILES)

clean : 
	$(RM) -f *.o *.ln *.i TAGS tags

depend:
	$(MAKEDEPEND) -I$(INCLUDE) $(SOURCES)

clear_depend:
	@$(MAKEDEPEND)
#
# DO NOT DELETE THIS LINE -- make depend depends on it.

