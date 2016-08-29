/******************************************************************************/
/*                                                                            */
/*    Copyright (c) 1990-2016, KAIST                                          */
/*    All rights reserved.                                                    */
/*                                                                            */
/*    Redistribution and use in source and binary forms, with or without      */
/*    modification, are permitted provided that the following conditions      */
/*    are met:                                                                */
/*                                                                            */
/*    1. Redistributions of source code must retain the above copyright       */
/*       notice, this list of conditions and the following disclaimer.        */
/*                                                                            */
/*    2. Redistributions in binary form must reproduce the above copyright    */
/*       notice, this list of conditions and the following disclaimer in      */
/*       the documentation and/or other materials provided with the           */
/*       distribution.                                                        */
/*                                                                            */
/*    3. Neither the name of the copyright holder nor the names of its        */
/*       contributors may be used to endorse or promote products derived      */
/*       from this software without specific prior written permission.        */
/*                                                                            */
/*    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS     */
/*    "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT       */
/*    LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS       */
/*    FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE          */
/*    COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,    */
/*    INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,    */
/*    BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;        */
/*    LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER        */
/*    CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT      */
/*    LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN       */
/*    ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE         */
/*    POSSIBILITY OF SUCH DAMAGE.                                             */
/*                                                                            */
/******************************************************************************/
/******************************************************************************/
/*                                                                            */
/*    ODYSSEUS/OOSQL DB-IR-Spatial Tightly-Integrated DBMS                    */
/*    Version 5.0                                                             */
/*                                                                            */
/*    Developed by Professor Kyu-Young Whang et al.                           */
/*                                                                            */
/*    Advanced Information Technology Research Center (AITrc)                 */
/*    Korea Advanced Institute of Science and Technology (KAIST)              */
/*                                                                            */
/*    e-mail: odysseus.oosql@gmail.com                                        */
/*                                                                            */
/*    Bibliography:                                                           */
/*    [1] Whang, K., Lee, J., Lee, M., Han, W., Kim, M., and Kim, J., "DB-IR  */
/*        Integration Using Tight-Coupling in the Odysseus DBMS," World Wide  */
/*        Web, Vol. 18, No. 3, pp. 491-520, May 2015.                         */
/*    [2] Whang, K., Lee, M., Lee, J., Kim, M., and Han, W., "Odysseus: a     */
/*        High-Performance ORDBMS Tightly-Coupled with IR Features," In Proc. */
/*        IEEE 21st Int'l Conf. on Data Engineering (ICDE), pp. 1104-1105     */
/*        (demo), Tokyo, Japan, April 5-8, 2005. This paper received the Best */
/*        Demonstration Award.                                                */
/*    [3] Whang, K., Park, B., Han, W., and Lee, Y., "An Inverted Index       */
/*        Storage Structure Using Subindexes and Large Objects for Tight      */
/*        Coupling of Information Retrieval with Database Management          */
/*        Systems," U.S. Patent No.6,349,308 (2002) (Appl. No. 09/250,487     */
/*        (1999)).                                                            */
/*    [4] Whang, K., Lee, J., Kim, M., Lee, M., Lee, K., Han, W., and Kim,    */
/*        J., "Tightly-Coupled Spatial Database Features in the               */
/*        Odysseus/OpenGIS DBMS for High-Performance," GeoInformatica,        */
/*        Vol. 14, No. 4, pp. 425-446, Oct. 2010.                             */
/*    [5] Whang, K., Lee, J., Kim, M., Lee, M., and Lee, K., "Odysseus: a     */
/*        High-Performance ORDBMS Tightly-Coupled with Spatial Database       */
/*        Features," In Proc. 23rd IEEE Int'l Conf. on Data Engineering       */
/*        (ICDE), pp. 1493-1494 (demo), Istanbul, Turkey, Apr. 16-20, 2007.   */
/*                                                                            */
/******************************************************************************/

#ifndef _LOM_H
#define _LOM_H


#include "LOM_Internal.h"
#include "LOM_ODMG_Collection.h"
#include "LOM_Relationship.h"
#include <stdio.h>
#ifdef COMPRESSION
#include </usr/include/zlib.h>
#endif
#ifdef  __cplusplus
extern "C" {
#endif

/* LOM External Interfaces */
Four LOM_Connect(char *, char *, LOM_Handle *);
Four LOM_Disconnect(LOM_Handle *);
Four lom_Connect(char *, char *, LOM_Handle *);
Four lom_Disconnect(LOM_Handle *);

/* Class Manipulation */
Four LOM_AlterClass(LOM_Handle *, Four,	char*, Four, Four, AttrInfo*, Four,	AttrInfo *);

#ifndef _CS_ODYSSEUS_
Four LOM_CreateClass(LOM_Handle *handle,  Four    , char *, char *, LRDS_IndexDesc *, Four , AttrInfo *, Four ,char (*)[LOM_MAXCLASSNAME], Four , MethodInfo  *, Boolean, Four);
#else
Four LOM_CreateClass(LOM_Handle *handle,  Four    , char *, char *, LRDS_IndexDesc *, Four , AttrInfo *, Four ,char (*)[LOM_MAXCLASSNAME], Four , MethodInfo  *, Boolean, Four);
#endif

Four LOM_CreateDefaultClass(LOM_Handle *handle,  Four    , char *, char *, LRDS_IndexDesc *, Four , AttrInfo *, Four ,char (*)[LOM_MAXCLASSNAME], Four , MethodInfo  *, Boolean, Four );
Four LOM_DestroyClass(LOM_Handle *handle,  Four , char *);
Four LOM_OpenClass(LOM_Handle *handle, Four,char *);
Four LOM_CloseClass(LOM_Handle *handle, Four);
Four LOM_GetOpenClassNum(LOM_Handle *handle, Four volId, char *className);
Four LOM_GetOpenClassNumByClassId(LOM_Handle *handle, Four volId, Four classId);

/* transaction */
Four LOM_TransBegin(LOM_Handle *handle, XactID *, ConcurrencyLevel);
Four LOM_TransCommit(LOM_Handle *handle, XactID *);
Four LOM_TransAbort(LOM_Handle *handle, XactID *);

/* memory manipulations */
#define LOM_initVarArray(handle, varArray, size, number) LRDS_initVarArray(LOM_GET_LRDS_HANDLE(handle), varArray, size, number)
#define LOM_doublesizeVarArray(handle, varArray, size)   LRDS_doublesizeVarArray(LOM_GET_LRDS_HANDLE(handle), varArray, size)
#define LOM_finalVarArray(handle, varArray)              LRDS_finalVarArray(LOM_GET_LRDS_HANDLE(handle), varArray)

/* sort stream manipulations */
#define LOM_OpenSortStream(handle, volId, sortTupleDesc) LRDS_OpenSortStream(LOM_GET_LRDS_HANDLE(handle), volId, sortTupleDesc)
#define LOM_CloseSortStream(handle, streamId)            LRDS_CloseSortStream(LOM_GET_LRDS_HANDLE(handle), streamId)
#define LOM_SortingSortStream(handle, streamId)          LRDS_SortingSortStream(LOM_GET_LRDS_HANDLE(handle), streamId)
#define LOM_PutTuplesIntoSortStream(handle, streamId, numTuples, tuples) \
	LRDS_PutTuplesIntoSortStream(LOM_GET_LRDS_HANDLE(handle), streamId, numTuples, tuples)
#define LOM_GetTuplesFromSortStream(handle, streamId, numTuples, tuples, eof) \
	LRDS_GetTuplesFromSortStream(LOM_GET_LRDS_HANDLE(handle), streamId, numTuples, tuples, eof)
#define LOM_GetNumTuplesInSortStream(handle, streamId)   LRDS_GetNumTuplesInSortStream(LOM_GET_LRDS_HANDLE(handle), streamId)
#define LOM_GetSizeOfSortStream(handle, streamId)        LRDS_GetSizeOfSortStream(LOM_GET_LRDS_HANDLE(handle), streamId)

#define LOM_OpenStream(handle, volId)                    LRDS_OpenStream(LOM_GET_LRDS_HANDLE(handle), volId)
#define LOM_CloseStream(handle, streamId)                LRDS_CloseStream(LOM_GET_LRDS_HANDLE(handle), streamId)
#define LOM_ChangePhaseStream(handle, streamId)          LRDS_ChangePhaseStream(LOM_GET_LRDS_HANDLE(handle), streamId)
#define LOM_PutTuplesIntoStream(handle, streamId, numTuples, tuples) \
	LRDS_PutTuplesIntoStream(LOM_GET_LRDS_HANDLE(handle), streamId, numTuples, tuples)
#define LOM_GetTuplesFromStream(handle, streamId, numTuples, tuples, eof) \
	LRDS_GetTuplesFromStream(LOM_GET_LRDS_HANDLE(handle), streamId, numTuples, tuples, eof)
#define LOM_GetNumTuplesInStream(handle, streamId)       LRDS_GetNumTuplesInStream(LOM_GET_LRDS_HANDLE(handle), streamId)
#define LOM_GetSizeOfStream(handle, streamId)            LRDS_GetSizeOfStream(LOM_GET_LRDS_HANDLE(handle), streamId)


/* mount & dismount */
Four LOM_Mount(LOM_Handle *handle, Four, char **, Four *);
Four LOM_Dismount(LOM_Handle *handle, Four);

#ifndef LOM_CLIENT
Four LOM_FormatDataVolume
(
	LOM_Handle* handle,
	Four        numOfDevices,
	char        **devNameList,
	char        *volName,
	Four        volId,
	Four        extentSize,
	Four        *numPagesInDevice,
	Four        segmentSize
);

Four LOM_FormatTempDataVolume
(
	LOM_Handle* handle,
	Four        numOfDevices,
	char        **devNameList,
	char        *volName,
	Four        volId,
	Four        extentSize,
	Four        *numPagesInDevice,
	Four        segmentSize
);

Four ODYS_Format
(
	LOM_Handle* handle,
	Four        numOfDevices,
	char        **devNameList,
	char        *volName,
	Four        volId,
	Four        extentSize,
	Four        *numPagesInDevice,
	Four        segmentSize
);

#endif 

/* Object Manipulation */
Four LOM_CreateObject(LOM_Handle *handle,  Four , Boolean, char *, OID *);
Four LOM_FetchObject(LOM_Handle *handle,  Four , Boolean, OID *, char *);
Four LOM_UpdateObject(LOM_Handle *handle,  Four , Boolean, OID *, char *);   
#define LOM_CreateObjectByWholeObject	LOM_CreateObject
#define LOM_UpdateObjectByWholeObject	LOM_UpdateObject
#define LOM_FetchObjectByWholeObject	LOM_FetchObject
#ifdef LOM_CLIENT
Four LOM_NextObject(LOM_Handle *handle, Four, OID *);
#else
Four LOM_NextObject(LOM_Handle *handle, Four, OID *, LRDS_Cursor **);
#endif
Four LOM_DestroyObject(LOM_Handle *handle, Four,Boolean, OID *);
Four LOM_CreateObjectByColList(LOM_Handle *handle, Four,Boolean, Four,LOM_ColListStruct*,OID*);
Four LOM_FetchObjectByColList(LOM_Handle *handle, Four,Boolean, OID*,Four,LOM_ColListStruct*);
Four LOM_UpdateObjectByColList(LOM_Handle *handle, Four,Boolean, OID*,Four,LOM_ColListStruct*);
Four LOM_CreateObjectByEncodedObject(LOM_Handle *handle, Four,Boolean, Four, Four, char *,OID*);
Four LOM_FetchObjectByEncodedObject(LOM_Handle *handle, Four,Boolean, OID*,Four,Four, char*, Four*, char**);
Four LOM_UpdateObjectByEncodedObject(LOM_Handle *handle, Four,Boolean, OID*,Four,Four, char*);
Four LOM_FetchColLength(LOM_Handle *handle, Four,Boolean, OID*,Four,ColLengthInfoListStruct*);

Four lom_GetClassId(LOM_Handle *handle, Four, char *, Four *);
#define	LOM_GetClassId	lom_GetClassId
#define	LOM_GetClassID	lom_GetClassId

/* Error */
char *LOM_Err(LOM_Handle *handle, Four);

/* Scan */
Four LOM_CloseScan(LOM_Handle *handle, Four);
Four LOM_OpenIndexScan(LOM_Handle *handle, Four,LOM_IndexID*,BoundCond*,BoundCond*,Four,BoolExp*,LockParameter*);
Four LOM_OpenSeqScan(LOM_Handle *handle, Four,Four,Four,BoolExp*,LockParameter*);

/* LOM Internal Interfaces */
Four LOM_GetNewClassId(LOM_Handle *handle, Four volId, Boolean tmpClassFlag, Four *newClassId);
Four lom_GetAndIncrementLastRelationshipId(LOM_Handle *handle, Four);
Four lom_GetClassName(LOM_Handle *handle, Four,Four,char *);
Four lom_AttrInfomation(LOM_Handle *handle,  Four , Four , AttrInfo *);
Two lom_getTypeID(LOM_Handle *handle, char *);
Four lom_GetSuperClass(LOM_Handle *handle, Four volId, Four classId, Four *nSuperClass, Four *superClassId); /* OUT : superclasses Id */
Four lom_GetSubClass(LOM_Handle *handle, Four volId, Four classId, Four *nSubClass, Four *subClassId); /* OUT : superclasses Id */
Four lom_OpenClassByClassId(LOM_Handle *handle,  Four, Four);

Boolean lom_CheckAttrInfo(LOM_Handle *handle, Four, AttrInfo*);
Boolean lom_CheckKeyInfo(LOM_Handle *handle, Four, AttrInfo*, ColDesc*, KeyInfo*);
Boolean lom_CheckMLGF_KeyInfo(LOM_Handle *handle, Four, AttrInfo*, ColDesc*, MLGF_KeyInfo*);
Boolean lom_CheckIndexDesc(LOM_Handle *handle, Four, AttrInfo*, ColDesc*, LRDS_IndexDesc*);

void lom_KeyInfoToKeyDesc(LOM_Handle *handle, AttrInfo*, ColDesc*, KeyInfo*, KeyDesc*);
void lom_MLGF_KeyInfoToMLGF_KeyDesc(LOM_Handle *handle, AttrInfo*, ColDesc*, MLGF_KeyInfo*, MLGF_KeyDesc*);


Four LOM_AddIndex(LOM_Handle *handle, Four volId, char *className, char *indexName, LOM_IndexDesc *indexDesc, LOM_IndexID *indexID);
Four LOM_DropIndex(LOM_Handle *handle, Four volId, char *indexName);

Four LOM_CreateHandle(LOM_Handle *handle, Four *);
Four LOM_InitLocalDS(LOM_Handle *handle);
Four LOM_DestroyHandle(LOM_Handle *handle, Four);
Four LOM_FinalLocalDS(LOM_Handle *handle);
Four LOM_FlushAll(LOM_Handle *handle);

/* aliasing LRDS set interfaces */
#define LOM_Set_Create(handle, ocnOrScanId, useScanFlag, oid, colNo)	LRDS_Set_Create(LOM_GET_LRDS_HANDLE(handle), ocnOrScanId, useScanFlag, (TupleID*)(oid), GET_SYSTEMLEVEL_COLNO(colNo))
#define LOM_Set_Destroy(handle, ocnOrScanId, useScanFlag, oid, colNo)	LRDS_Set_Destroy(LOM_GET_LRDS_HANDLE(handle), ocnOrScanId, useScanFlag, (TupleID*)(oid), GET_SYSTEMLEVEL_COLNO(colNo))
#define LOM_Set_InsertElements(handle, ocnOrScanId, useScanFlag, oid, colNo, nElements, elements)	LRDS_Set_InsertElements(LOM_GET_LRDS_HANDLE(handle), (ocnOrScanId), (useScanFlag), (TupleID*)(oid), GET_SYSTEMLEVEL_COLNO(colNo), (nElements), (elements))
#define LOM_Set_IsMember(handle, ocnOrScanId, useScanFlag, oid, colNo, elements)	LRDS_Set_IsMember(LOM_GET_LRDS_HANDLE(handle), (ocnOrScanId), (useScanFlag), (TupleID*)(oid), GET_SYSTEMLEVEL_COLNO(colNo), (elements))
#define LOM_Set_DeleteElements(handle, ocnOrScanId, useScanFlag, oid, colNo, nElements, elements)	LRDS_Set_DeleteElements(LOM_GET_LRDS_HANDLE(handle), (ocnOrScanId), (useScanFlag), (TupleID*)(oid), GET_SYSTEMLEVEL_COLNO(colNo), (nElements), (elements))
#define LOM_Set_Scan_Open(handle, ocnOrScanId, useScanFlag, oid, colNo)	LRDS_Set_Scan_Open(LOM_GET_LRDS_HANDLE(handle), (ocnOrScanId), (useScanFlag), (TupleID*)(oid), GET_SYSTEMLEVEL_COLNO(colNo))
#define LOM_Set_Scan_Close(handle, setScanId)		LRDS_Set_Scan_Close(LOM_GET_LRDS_HANDLE(handle), setScanId)
#define LOM_Set_Scan_NextElements(handle, setScanId, nElements, elements)	LRDS_Set_Scan_NextElements(LOM_GET_LRDS_HANDLE(handle), setScanId, nElements, elements)
#define LOM_Set_Scan_InsertElements(handle, setScanId, nElements, elements)	LRDS_Set_Scan_InsertElements(LOM_GET_LRDS_HANDLE(handle), setScanId, nElements, elements)
#define LOM_Set_Scan_DeleteElements(handle, setScanId)	LRDS_Set_Scan_DeleteElements(LOM_GET_LRDS_HANDLE(handle), setScanId)
#define LOM_Set_IsNull(handle, ornOrRelScanId, useScanFlag, oid, colNo) LRDS_Set_IsNull(LOM_GET_LRDS_HANDLE(handle), ornOrRelScanId, useScanFlag, oid, GET_SYSTEMLEVEL_COLNO(colNo))

#define LOM_CreateTemporaryClass(handle, volId, className, idesc, nCols, ainfo) LRDS_CreateRelation(LOM_GET_LRDS_HANDLE(handle), (volId), (className), (LRDS_IndexDesc *)(idesc), nCols, (ColInfo *)ainfo, (Boolean)TRUE)
#define LOM_DestroyTemporaryClass(handle, volId, className) LRDS_DestroyRelation(LOM_GET_LRDS_HANDLE(handle), (volId), (className))
#define LOM_OpenTemporaryClass(handle, volId, className) LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), (volId), (className))
#define LOM_CloseTemporaryClass(handle, ocn) LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), ocn)
/*#endif */

#include "LOM_Collection.h"

/* 
   supported text type in LOM 
   The followings are to be given for supporting text type and inverted files.
 */

Four LOM_Text_CreateContent(
	LOM_Handle *handle, 
	Four ornOrScanId,		/* IN: one of the following open scan numbers : LOM_OpenScan, LOM_OpenIndexScan, LOM_Text_OpenIndexScan, LOM_Text_Scan_Open */
	Boolean useScanFlag,		/* IN: useScanFlag */
	OID *oid,		/* IN: object containging the give text type attribute */
	Two colNum,		/* IN: column number */
	TextColStruct *text,	/* IN: text column struct */
	LOM_TextDesc	*textDesc 	/* INOUT: text descriptor */
);

Four LOM_Text_GetDescriptor(
	LOM_Handle *handle, 
	Four ornOrScanId,		/* IN: one of the following open scan numbers : LOM_OpenScan, LOM_OpenIndexScan, LOM_Text_OpenIndexScan, LOM_Text_Scan_Open */
	Boolean useScanFlag,		/* IN: useScanFlag */
	OID *oid,	/* IN: oid */
	Two colNo,	/* IN: column number */
	LOM_TextDesc	*textDesc	/* OUT: text descriptor */
);
Four LOM_Text_UpdateDescriptor(
	LOM_Handle *handle, 
	Four ocnOrScanId,	/* IN: open scan number */
	Boolean useScanFlag, /* IN: flag */
	OID *oid,	/* IN: oid */
	Two colNo,	/* IN: column number */
	LOM_TextDesc	*textDesc	/* OUT: text descriptor */
);
Four LOM_Text_DestroyContent(
	LOM_Handle *handle, 
	Four ornOrScanId,		/* IN: one of the following open scan numbers : LOM_OpenScan, LOM_OpenIndexScan, LOM_Text_OpenIndexScan, LOM_Text_Scan_Open */
	Boolean useScanFlag,		/* IN: useScanFlag */
	OID *oid,		/* oid */
	Two colNo,		/* column number */
	LOM_TextDesc *textDesc 	/* INOUT: text descriptor */
);	

Four LOM_Text_FetchContent(
	LOM_Handle *handle, 
	Four ornOrScanId,		/* IN: one of the following open scan numbers : LOM_OpenScan, LOM_OpenIndexScan, LOM_Text_OpenIndexScan, LOM_Text_Scan_Open */
	Boolean useScanFlag,		/* IN: useScanFlag */
	OID *oid,		/* oid */
	Two colNo,		/* column number */
	TextColStruct *text,	/* INOUT; text */ /* returned length */
	LOM_TextDesc *textDesc 	/* INOUT: text descriptor */
);	

Four LOM_Text_UpdateContent(
	LOM_Handle *handle, 
	Four ornOrScanId,		/* IN: one of the following open scan numbers : LOM_OpenScan, LOM_OpenIndexScan, LOM_Text_OpenIndexScan, LOM_Text_Scan_Open */
	Boolean useScanFlag,		/* IN: useScanFlag */
	OID *oid,
	Two colNo,
	TextColStruct *text,	/* IN: text column struct */
	LOM_TextDesc *textDesc 	/* INOUT: text descriptor */
);	

Four LOM_Text_OpenIndexScan(
	LOM_Handle *handle, 
	Four ocn,	/* IN: open class number */
	LOM_IndexID *indexId,	/* IN: index id */
	Four keywordKind,	/* IN: kind of keyword */
	BoundCond *keywordStartBound, /* IN: start bound */
	BoundCond *keywordStopBound,	/* IN: stop bound */
	LockParameter *lockup		/* IN: lock parameter */
);
Four LOM_Text_OpenIndexScan_GivenInvertedEntryTupleID(LOM_Handle* handle, Four ocn, Two colNo, TupleID* invertedTableEntryTupleID, LockParameter* lockup);
Four LOM_Text_Scan_Open(
	LOM_Handle *handle, 
	Four ocn,						/* IN: open class number */
	OID *oid,						/* IN: oid */
	Two colNo,						/* IN: column number */
	Four keywordKind,	/* IN: kind of keyword */
	BoundCond *keywordStartBound, 	/* IN: start bound */
	BoundCond *keywordStopBound,	/* IN: stop bound */
	LockParameter *lockup			/* IN: lock parameter */
);

Four LOM_Text_Scan_Close(
	LOM_Handle *handle, 
	Four osn
);

Four lom_Text_GetNPostings(
	LOM_Handle *handle, 
	Four ornOrScanId,		/* IN: one of the following open scan numbers : LOM_OpenScan, LOM_OpenIndexScan, LOM_Text_OpenIndexScan, LOM_Text_Scan_Open */
	Boolean useScanFlag,		/* IN: useScanFlag */
	Two colNo,
	char *keyword
);

Four LOM_Text_Scan_NextPosting(
	LOM_Handle *handle, 
	Four textScan,		/* IN: text scan */
	Four bufferLength,
	char *postingBuffer,
	Four *requiredSize,
	PostingWeight *weight
);

/* for scanDirection argument */
#define LOM_TEXT_SCAN_FORWARD				FORWARD
#define LOM_TEXT_SCAN_BACKWARD				BACKWARD
#define LOM_TEXT_SCAN_BACKWARD_NOORDERING 	BACKWARD_NOORDERING
#define LOM_TEXT_SCAN_BACKWARD_ORDERING		BACKWARD_ORDERING

Four LOM_Text_NextPostings(
	LOM_Handle*	handle,						/* IN: LOM system handle */
	Four		textScan,					/* IN: text scan Id */
	Four		postingLengthBufferSize,	/* IN: size of buffer to hold postings' length */
	char*		postingLengthBuffer,		/* IN: buffer to hold postings' length */
	Four		postingBufferSize,			/* IN: size of buffer to hold postings */
	char*		postingBuffer,				/* IN: buffer to hold postings */
	Four		scanDirection,				/* IN: scan direction to read postings */
	Four		logicalIdHints,				/* IN: logical Id hints to skip postings */
	Four*		nReturnedPosting,			/* OUT: number of read postings */
#ifndef COMPRESSION
	Four*		requiredSize				/* OUT: sufficient size of buffer to hold postings */
#else
	Four*		requiredSize,				/* OUT: sufficient size of buffer to hold postings */
	VolNo*		volNoOfPostingTupleID,
	Four*		lastDocId
#endif
);

Four LOM_Text_GetCursorKeyword(
	LOM_Handle *handle, 
	Four scanId,		/* IN: one of the following open scan numbers : LOM_OpenScan, LOM_OpenIndexScan, LOM_Text_OpenIndexScan, LOM_Text_Scan_Open */
	char *keyword
);

Four LOM_Text_Keyword_Scan_Open(
	LOM_Handle*			handle,			/* IN  lom system handle */
	Four				ocn,			/* IN  open class number of to be scanned */
	Two					colNo,			/* IN  column of of to be scanned */
	char*				keyword			/* IN  a string ended with * which represents keyword range to be scanned (example - KOREA*) */
);
Four LOM_Text_Keyword_Scan_Open_WithBoundCondition(
	LOM_Handle*			handle,			/* IN  lom system handle */
	Four				ocn,			/* IN  open class number of to be scanned */
	Two					colNo,			/* IN  column of of to be scanned */
	Four				keywordKind,	/* IN  kind of keyword, KEYWORD or REVERSEKEYWORD */
	Two					startBoundLen,	/* IN  start bound condition length */
	char*				startBoundVal,	/* IN  start bound condition value */
	Two					stopBoundLen,	/* IN  stop bound condition length */
	char*				stopBoundVal	/* IN  stop bound condition value */
);
Four LOM_Text_Keyword_Scan_Close(
	LOM_Handle*			handle,			/* IN  lom system handle */
	Four				scanId			/* IN  scan id to be closed */
);
Four LOM_Text_Keyword_Scan_Next(
	LOM_Handle*			handle,			/* IN  lom system handle */
	Four				scanId,			/* IN  scan id */
	Four				keywordKind,	/* IN  kind of keyword, KEYWORD or REVERSEKEYWORD */
	char*				keyword,		/* OUT keyword buffer to be returned */
	Four*               nDocuments,		/* OUT number of document associated with the keyword */
	TupleID*			invertedIndexEntryTupleID /* OUT tuple id of inverted index entry */
);
Four LOM_Text_GetNumOfTextObjectsInClass(
	LOM_Handle*			handle,			/* IN  lom system handle */
	Four				ocn,			/* IN  open class number */
	Four*				nObjects		/* OUT number of objects in class */
);

#define LOM_Text_MakeIndex LOM_Text_BatchInvertedIndexBuild

Four LOM_Text_BatchInvertedIndexBuild(
	LOM_Handle *handle, 
	Four volId,
	Four temporaryVolId,
	char *className
);

Four lom_Text_BatchInvetedIndexBuild(
	LOM_Handle *handle, 
	Four ocn,		/* open class number */
	Two colNo		/* system-level column number */
);

Four lom_Text_CreateCatalogTable(
	LOM_Handle *handle, 
	Four volId
); 

void lom_Text_MakeLogicalIdIndexName(
	LOM_Handle *handle, 
	Four volId, 	/* IN: volumn id */
	char *className,/* IN: class name */
	char *logicalIdIndexName /* OUT: content table name */
);

void lom_Text_MakeContentTableName(
	LOM_Handle *handle, 
	Four volId, 	/* IN: volumn id */
	char *className,/* IN: class name */
	char *contentTableName /* OUT: content table name */
);

void lom_Text_MakePostingTableName(
	LOM_Handle *handle, 
	Four volId, 	/* IN: volumn id */
	char *className,/* IN: class name */
	char *postingTableName /* OUT: content table name */
);

void lom_Text_MakeInvertedIndexName(
	LOM_Handle *handle, 
	Four volId, 	/* IN: volumn id */
	char *className, /* IN: class name */
	char *attrName,	/* IN: attribute name */
	char *invertedIndexTableName	/* OUT: inverted index name */
);

Four lom_Text_CreateContentTable(
	LOM_Handle *handle, 	
	Four volId, 	/* IN: volumn id */
	char *className	/* IN: class name */
);

Four lom_Text_CreatePostingTable(
	LOM_Handle *handle, 
	Four volId, 		/* IN: volumn id */
	char *className 	/* IN: class name */
);

Four lom_Text_CreateInvertedIndexTable(
	LOM_Handle *handle, 
	Four volId, 	/* IN: volumn id */
	char *className, /* IN: class name */
	char *attrName,	/* IN: attribute name */
	Two colNo 	/* IN: column number */
);

Four lom_Text_AddInvertedIndex(
	LOM_Handle *handle, 
	Four volId, 	/* IN: volumn id */
	char *className, /* IN: class name */
	char *attrName,	/* IN: attribute name */
	Two colNo 	/* IN: column number */
);

Four lom_Text_DropInvertedIndex(
	LOM_Handle *handle, 
	Four volId, 	/* IN: volumn id */
	char *className, /* IN: class name */
	char *attrName 	/* IN: attribute name */
);

Four lom_Text_DestroyContent(
	LOM_Handle *handle, 
	Four ornOrScanId,		/* IN: one of the following open scan numbers : LOM_OpenScan, LOM_OpenIndexScan, LOM_Text_OpenIndexScan, LOM_Text_Scan_Open */
	Boolean useScanFlag,		/* IN: useScanFlag */
	OID *oid,		/* IN: oid */
	Two colNo,		/* IN: column number */
	LOM_TextDesc *textDesc	/* IN: text descriptor */
);

Four lom_Text_DestroyContentTable(
	LOM_Handle *handle, 
	Four volId, 
	char *className
);

Four lom_Text_DestroyInvertedIndexTable(
	LOM_Handle *handle, 
	Four volId, 		/* IN: volumn id */
	char *className, 	/* IN: class name */
	char *attrName 		/* IN: attribute name */
);

Four lom_Text_DestroyDocIdIndexTable(
	LOM_Handle *handle, 
	Four volId, 		/* IN: volumn id */
	char *className, 	/* IN: class name */
	char *attrName 		/* IN: attribute name */
);

Four lom_Text_DestroyPostingTable(
	LOM_Handle *handle, 
	Four volId, 		/* IN: volumn id */
	char *className  	/* IN: class name */
);

Four lom_Text_OpenContentClass(
	LOM_Handle *handle, 
	Four volId, 		/* IN: volumn id */
	char *className		/* IN: class name */
);

Four lom_Text_OpenInvertedIndexTable(
	LOM_Handle *handle, 
	Four volId, 		/* IN: volumn id */
	char *className, 	/* IN: class name */
	char *attrName		/* IN: attribute name */
);

Four lom_Text_OpenPostingTable(
	LOM_Handle *handle, 
	Four volId, 		/* IN : volumn id */
	char *className  	/* IN : class name */
);

Four lom_Text_OpenContentScan(
	LOM_Handle *handle, 
	Four orn, 		/* IN: open realtion number */
	Four scanDirection, 	/* IN: scan direction */
	LockParameter *lockup	/* IN: lock parameter */
);

Four lom_Text_CreateContentData(
	LOM_Handle *handle, 
	Four ornOrScanId,		/* IN: one of the following open scan numbers : LOM_OpenScan, LOM_OpenIndexScan, LOM_Text_OpenIndexScan, LOM_Text_Scan_Open */
	Boolean useScanFlag,		/* IN: useScanFlag */
	Four dataLength, 	/* IN: length of data */
	char *data, 		/* IN: data */
	TupleID *tid		/* OUT: tuple id */
);

Four lom_Text_UpdateContentData(
	LOM_Handle *handle, 
	Four ornOrScanId,		/* IN: one of the following open scan numbers : LOM_OpenScan, LOM_OpenIndexScan, LOM_Text_OpenIndexScan, LOM_Text_Scan_Open */
	Boolean useScanFlag,		/* IN: useScanFlag */
	TupleID *tid,		/* IN: tuple id */
	Four start,		/* IN: start of data */
	Four length,		/* IN: length of to-be-updated data */
	Four dataLength, 	/* IN: length of data */
	char *data 		/* IN: data */
);

Four lom_Text_FetchContentData(
	LOM_Handle *handle, 
	Four ornOrScanId,		/* IN: one of the following open scan numbers : LOM_OpenScan, LOM_OpenIndexScan, LOM_Text_OpenIndexScan, LOM_Text_Scan_Open */
	Boolean useScanFlag,		/* IN: useScanFlag */
	TupleID *tid,		/* IN: tuple id */
	Four start,		/* IN: start of data */
	Four length,		/* IN: length of to-be-updated data */
	Four dataLength, 	/* IN: length of data */
	char *data 		/* OUT: data */
);


Four lom_Text_GetPostingFromTempFile(
	LOM_Handle								*handle, 
	Four									ocnOrScanId,	/* IN: open class no or scan id */
	Boolean									useScanFlag,	/* IN: flag */
	Two										colNo,			/* IN: column no */
	lom_Text_PostingInfoForReading			*postingInfo,	/* IN: posting info */
	FILE									*fd, 			/* IN: file descriptor */
	lom_PostingBuffer						*postingBuffer	/* IN: posting buffer */
);

Four lom_Text_GetPostingFromTempClass(
	LOM_Handle								*handle, 
	Four									scanIdForTempClass,	/* IN: scanId for temp calss */
	lom_PostingBuffer						*postingBuffer		/* IN: posting buffer */
);

Four lom_Text_OpenTempClassForPosting(
	LOM_Handle								*handle,			/* IN: system handle */
	Four									volId,				/* IN: volume id */
	char									*className,			/* IN */
	char									*attrName,			/* IN */
	Four									*ocn,				/* OUT */
	Four									*scanId				/* OUT */
);

Four lom_Text_CloseTempClassForPosting(
	LOM_Handle								*handle,			/* IN: system handle */
	Four									ocn,				/* IN */
	Four									scanId				/* IN */
);

Four lom_Text_CurrentPostingLengthFromPostingBuffer(
	LOM_Handle *handle, 
	lom_PostingBuffer *postingBuffer	/* IN: posting buffer */
);

void lom_Text_AppendPostingToPostingList(
	LOM_Handle *handle, 
	lom_PostingBuffer *postingListBuffer,
#ifdef COMPRESSION    
	lom_PostingBuffer *compressedPostingListBuffer,
#endif	
	Four postingLength, 
	char *ptrToPosting
);

Four lom_Text_AddInvertedIndexEntryFromTempFile(
	LOM_Handle*	handle, 
	Four		temporaryVolId,			/* IN: id for temporary volume used in sorting and etc. */
	Four		ocnOrScanId,			/* IN: ocn or scanId */
	Boolean		useScanFlag,			/* IN: flag */
	Two			colNo,					/* IN: column number */
	char*		tempPostingFileName,	/* IN: temporary posting file name */
	char*		docIdFileName,			/* INOUT: DocIdFile name */
	Boolean		isLogicalIdOrder,		/* IN: isLogicalIdOrder */
	lom_Text_ConfigForInvertedIndexBuild* config /* IN: index building configurations */
);

char *makeReverseStr(
	char *src,		/* IN: source string */
	char *dest,		/* OUT: reverse string */
	Four length
);

Four lom_Text_AddInvertedIndexEntryFromBuf(
	LOM_Handle*		handle, 
	Four			temporaryVolId,				/* IN: id for temporary volume used in sorting and etc. */
	Four			ocnOrScanId,				/* IN: text open scan number */
	Boolean			useScanFlag,				/* IN: flag */
	Four            lrdsBulkLoadId,				/* IN: lrds level bulk load id */
	Four            lrdsTextBulkLoadId,			/* IN: lrds level text bulk load id */
	Boolean         useBulkLoadFlag,			/* IN: use bulk loading feature */
	Two				colNo,						/* IN: column number */
	char*			keyword,					/* IN: keyword */
#ifdef COMPRESSION
    lom_Text_PostingInfoForReading* postingInfo,        /* IN: posting info */
    char*			ptrToCompressedPostingList,			/* IN: pointer to posting list */
#endif
	Four			nPostings,					/* IN: number of postings */
	Four			lengthOfPostingList,		/* IN: length of posting-list */
	char*			ptrToPostingList,			/* IN: pointer to posting list */
	TupleID*		tid,
	Boolean			isLogicalIdOrder,			/* IN: isLogicalIdOrder */
	Boolean			buildReverseKeywordIndex,	/* IN: build index for reverse keyword in this function */
	Boolean*		newlyRegisteredKeyword		/* OUT: is keyword a newly registered? */
);

Four lom_Text_RemoveInvertedIndexEntry(
	LOM_Handle *handle, 
	Four ornOrScanId,		/* IN: one of the following open scan numbers : LOM_OpenScan, LOM_OpenIndexScan, LOM_Text_OpenIndexScan, LOM_Text_Scan_Open */
	Boolean useScanFlag,		/* IN: useScanFlag */
	Four logicalDocId,			/* IN: document id */
	Two colNo 			/* IN: column number */
);

void lom_Text_MakeDocIdTableName(
	LOM_Handle *handle,
	Four volId,	/* IN: volume id */
	char *className,	/* IN: class name */
	char *attrName,		/* IN: attribute name */
	char *docIdTableName	/* OUT : docId table name */
);

Four lom_Text_AddDocIdIndexEntryFromBuf(
	LOM_Handle*	handle, 
	Four		ocnOrScanId,
	Boolean		useScanFlag,
	Four		lrdsBulkLoadId,
	Four		lrdsTextBulkLoadId,
	Boolean		useBulkLoadFlag,
	Two			colNo,
	Four		logicalDocId, 
	Four		numOfPointers, 
#ifndef COMPRESSION
	TupleID*	pointerBuffer
#else
    char*       pointerBuffer,
    char        *compressedData,
    Four        compressedDataLength
#endif
);

Four lom_Text_AddDocIdIndexEntryFromTempFile(
	LOM_Handle	*handle, 
	Four		temporaryVolId,	/* IN: id for temporary volume used in sorting and etc. */
	Four		ocnOrScanId,	/* IN: ocn or scanId */
	Boolean		useScanFlag,	/* IN: flag */
	Two			colNo,			/* IN: column number */
	char		*docIdFileName	/* INOUT: DocIdFile name */
);

Four lom_Text_AddDocIdIndexEntryIntoTempFile(
	LOM_Handle *handle, 
	FILE *docIdFile,
	Four nPostings,
	char *ptrToPostingBuffer,
	TupleID *tidForInvertedIndexEntry
);

Four lom_Text_GetDocIdAndPointerFromTempFile(
	LOM_Handle *handle, 
	FILE *docIdFile,      /* file descriptor */
	Four *docId,
	TupleID *tidForInvertedIndexEntry
);

Four lom_Text_RemoveDocIdIndexEntry(
	LOM_Handle *handle, 
	Four ornOrScanId,		/* IN: one of the following open scan numbers : LOM_OpenScan, LOM_OpenIndexScan, LOM_Text_OpenIndexScan, LOM_Text_Scan_Open */
	Boolean useScanFlag,		/* IN: useScanFlag */
	Four logicalDocId,	/* IN: document id */
	Two colNo		/* IN: column number */
);

Four lom_Text_SortDocIdFile(LOM_Handle *handle, char *, char *);

Four lom_Text_OpenSortStreamForDocIdIndex(
	LOM_Handle*		handle,						/* IN: LOM system handle */
	Four			volId						/* IN: volume Id */
);
Four lom_Text_CloseSortStreamForDocIdIndex(
	LOM_Handle*		handle,						/* IN: LOM system handle */
	Four			sortStreamId				/* IN: sort stream id */
);
Four lom_Text_AddDocIdIndexEntryIntoSortStream(
	LOM_Handle* handle,						/* IN: LOM system handle */
	Four		sortStreamId,				/* IN: sort stream id */
	Four		nPostings,					/* IN: n of postings to add */
	char		*ptrToPostingList,			/* IN: posting list */
	TupleID		*tidForInvertedIndexEntry	/* IN: tid of index entry for given posting list */
);
Four lom_Text_SortDocIdSortStream(
	LOM_Handle* handle,						/* IN: LOM system handle */
	Four		sortStreamId				/* IN: sort stream id */
);
Four lom_Text_AddDocIdIndexEntryFromSortStream(
	LOM_Handle	*handle, 
	Four		temporaryVolId,	/* IN: id for temporary volume used in sorting and etc. */
	Four		ocnOrScanId,	/* IN: ocn or scanId */
	Boolean		useScanFlag,	/* IN: flag */
	Two			colNo,			/* IN: column number */
	Four        sortStreamId	/* IN: sort stream id */
);
Four lom_Text_GetDocIdAndPointerFromSortStream(
	LOM_Handle		*handle,					/* IN: system handle */
	Four			sortStreamId,				/* IN: sort stream id */
	Four			*docId,						/* OUT: doc id (logical id) */
	TupleID			*tidForInvertedIndexEntry	/* OUT: tid of index entry associated with docid */
);
Four lom_Text_OpenSortStreamForPosting(
	LOM_Handle*		handle,						/* IN: LOM system handle */
	Four			volId						/* IN: volume Id */
);
Four lom_Text_AddPostingIntoSortStream(
	LOM_Handle*			handle,						/* IN: LOM system handle */
	Four				sortStreamId,				/* IN: sort stream id */
	Four                postingBufferLength,		/* IN: posting buffer length */
	char*				postingBuffer				/* IN: posting */
);
Four lom_Text_GetPostingFromSortStream(
	LOM_Handle*			handle,						/* IN: LOM system handle */
	Four				sortStreamId,				/* IN: sort stream id */
	Four				postingBufferLength,		/* IN: posting buffer length */
	char*				postingBuffer				/* OUT: posting */
);
Four lom_Text_SortPostingSortStream(
	LOM_Handle* handle,						/* IN: LOM system handle */
	Four		sortStreamId				/* IN: sort stream id */
);
Four lom_Text_CloseSortStreamForPosting(
	LOM_Handle* handle,						/* IN: LOM system handle */
	Four		sortStreamId				/* IN: sort stream id */
);

Four lom_Text_GetLogicalId(
	LOM_Handle *handle, 
	Four ornOrScanId,		/* IN: one of the following open scan numbers : LOM_OpenScan, LOM_OpenIndexScan, LOM_Text_OpenIndexScan, LOM_Text_Scan_Open */
	Boolean useScanFlag,		/* IN: useScanFlag */
	OID *oid		/* IN: object id */
);

Four lom_Text_SetLogicalId(
	LOM_Handle *handle,
	Four ocnOrScanId,		/* IN: open scan number */
	Boolean useScanFlag,	/* IN: flag */
	OID *oid,				/* IN: object id */
	Four logicalId			/* IN: new logical id */
);

Four lom_Text_GetInvertedIndexTableORN(
	LOM_Handle *handle, 
	Four ornOrScanId,		/* IN: one of the following open scan numbers : LOM_OpenScan, LOM_OpenIndexScan, LOM_Text_OpenIndexScan, LOM_Text_Scan_Open */
	Boolean useScanFlag,		/* IN: useScanFlag */
	Two colNo		/* column number */
);

Four lom_Text_GetDocIdIndexTableORN(
	LOM_Handle *handle, 
	Four ornOrScanId,		/* IN: one of the following open scan numbers : LOM_OpenScan, LOM_OpenIndexScan, LOM_Text_OpenIndexScan, LOM_Text_Scan_Open */
	Boolean useScanFlag,		/* IN: useScanFlag */
	Two colNo
);

Four lom_Text_GetRevKeywordIndex(
	LOM_Handle *handle,
	Four ocnOrScanId,
	Boolean useScanFlag,
	Two colNo,
	IndexID *iid
);

Four lom_Text_GetKeywordIndex(
	LOM_Handle *handle, 
	Four ornOrScanId,		/* IN: one of the following open scan numbers : LOM_OpenScan, LOM_OpenIndexScan, LOM_Text_OpenIndexScan, LOM_Text_Scan_Open */
	Boolean useScanFlag,		/* IN: useScanFlag */
	Two colNo,
	IndexID *iid
);

Four lom_Text_GetReverseKeywordIndex(
	LOM_Handle *handle, 
	Four ornOrScanId,		/* IN: one of the following open scan numbers : LOM_OpenScan, LOM_OpenIndexScan, LOM_Text_OpenIndexScan, LOM_Text_Scan_Open */
	Boolean useScanFlag,		/* IN: useScanFlag */
	Two colNo,
	IndexID *iid
);

Four lom_Text_GetDocIdIndex(
	LOM_Handle *handle, 
	Four ornOrScanId,		/* IN: one of the following open scan numbers : LOM_OpenScan, LOM_OpenIndexScan, LOM_Text_OpenIndexScan, LOM_Text_Scan_Open */
	Boolean useScanFlag,		/* IN: useScanFlag */
	Two colNo,
	IndexID *iid
);

Four lom_Text_GetPostingIndex(
	LOM_Handle *handle, 
	Four ornOrScanId,		/* IN: one of the following open scan numbers : LOM_OpenScan, LOM_OpenIndexScan, LOM_Text_OpenIndexScan, LOM_Text_Scan_Open */
	Boolean useScanFlag,		/* IN: useScanFlag */
	IndexID *iid
);

Four LOM_Text_GetIndexID(
	LOM_Handle *handle, 
	Four ocn,		/* IN: open class number */
	Four colNo,		/* IN: column number */
	LOM_IndexID *iid	/* OUT: index id */
);

Four lom_Text_GetAndIncrementLogicalId(
	LOM_Handle *handle, 
	Four ocn
);

Four lom_Text_CreateCounter(
	LOM_Handle *handle, 
	Four volId,     /* IN: volumn id */
	char *className,        /* IN: class name */
	Four initialValue       /* IN: initial value */
);

Four lom_Text_GetCounterId(
	LOM_Handle *handle, 
	Four volId,             /* IN: volumn id */
	char *className,        /* IN: counter name */
	CounterID *counterId         /* OUT: counter id */
);

Four lom_Text_DestroyCounter(
	LOM_Handle *handle, 
	Four volId,
	char *className
);

Four lom_Text_GetAndIncrementLogicalId(
	LOM_Handle *handle, 
	Four ocn
);


Four LOM_CreateCounter(
	LOM_Handle  *handle,
	Four volId, 	/* IN: volumn id */
	char *counterName,	/* IN: counter name */
	Four initialValue	/* IN: initial value */
);

Four LOM_CheckCounter(
	LOM_Handle  *handle,
	Four volId,
	char *counterName	
);

Four LOM_DestroyCounter(
	LOM_Handle  *handle,
	Four volId,
	char *counterName	
);

Four LOM_SetCounter(
	LOM_Handle  *handle,
	Four volId,
	char *counterName,
	Four value
);

Four LOM_GetCurrCounter(
	LOM_Handle  *handle,
	Four volId,
	char *counterName,
	Four *currValue
);

Four LOM_GetNextCounter(
	LOM_Handle  *handle,
	Four volId,
	char *counterName,
	Four *nextValue
);

#ifdef COMPRESSION
Four lom_Text_Compression
(
    LOM_Handle *handle,
    char *data,                   /* IN: data */
    Four dataLength,              /* IN: length of data */
    char *compressedData,         /* INOUT: compressed data */
    Four *compressedDataLength    /* INOUT: length of compressed data */
); 

Four lom_Text_Uncompression
(
    LOM_Handle *handle,
    char *compressedData,           /* IN: compressed data */
    Four compressedDataLength,      /* IN: length of compressed data */
    char **uncompressedData,        /* OUT: uncompressed data */
    Four *uncompressedDataLength    /* OUT: length of uncompressed data */
);

Four lom_Text_PostingList_Compression
(   
    lom_Text_PostingInfoForReading  *postingInfo,           /* IN */
    Four                            nPostings,              /* IN */
    Four                            *lengthOfPostingList,   /* INOUT */
    char                            *ptrToPostingList,       /* INOUT */
    char                            *ptrToCompressedPostingList,       /* INOUT */
	VolNo							*volNoOfPostingTupleID,
	Boolean							encodeDocIdFlag,
	Four							*lastDocId
);

Four lom_Text_PostingList_Uncompression
(   
    lom_Text_PostingInfoForReading  *postingInfo,               /* IN */
    Four                            nPostings,                  /* IN */
    Boolean                         hasPostingLength,           /* IN */
    Four                            postingListLengthBufferSize,    /* IN */
    char                            *postingListLengthBuffer,       /* INOUT */
    Four                            postingListBufferSize,          /* IN */
    char                            *postingListBuffer,             /* INOUT */
	VolNo							volNoOfPostingTupleID,
	Four							scanDirection,
    Four                            uncompressedPostingListBufferSize,
    char                            *uncompressedPostingListBuffer,
	Boolean							decodeDocIdFlag,
	Four							lastDocId
);

Four lom_Text_GetOffsetOfEmbeddedAttrs
(
    lom_Text_PostingInfoForReading  *postingInfo,           /* IN */
    char                            *posting,               /* IN */
    Four                            *offsetForEmbeddedAttrs /* OUT */
);

#define LOM_VARIABLE_BYTE_DECODING(encodedData, i, decodedData)          \
{                                                                               \
    char tmp = 0x1;                                                             \
    decodedData = 0;                                                            \
                                                                                \
    while((tmp & 0x1) == 0x1)                                    \
    {                                                                           \
        tmp = (encodedData)[i++];                                                 \
        decodedData = (decodedData << 7) + ((tmp >> 1) & 0x7f);                  \
    }                                                                           \
}

#define LOM_VARIABLE_BYTE_ENCODING(data, tempData, encodedData, encodedDataLength)    \
{                                                                                   \
    Two i, j = 0;                                                                       \
                                                                                    \
    if(data > 0)                                                                    \
    {                                                                               \
        encodedDataLength = 0;                                                      \
        for(i = 0; data != 0; i++)                                                  \
        {                                                                           \
            tempData[i] = (data % 128) << 1;                                        \
            data /= 128;                                                            \
            encodedDataLength++;                                                    \
        }                                                                           \
                                                                                    \
        for(i = encodedDataLength -1; i > 0; i--, j++)                       \
        {                                                                           \
            tempData[i] |= 0x1;                                                     \
            (encodedData)[j] = tempData[i];                                           \
        }                                                                           \
                                                                                    \
        tempData[0] |= 0x0;                                                         \
        (encodedData)[j++] = tempData[0];                                             \
    }                                                                               \
    else if(data == 0)                                                              \
    {                                                                               \
        encodedDataLength = 1;                                                      \
        (encodedData)[j++] = 0;                                                       \
    }                                                                               \
}
#endif

/* keyword extractor modules */
int convertf(
	char *infile, 
	char *outfile
);

Four readTextContent(
	Four osn,
	char *buffer,
	TupleID *tid,
	Four start,
	Four length
);

Four makeFile(
	Four ocnOrScanId, 
	Boolean useScanFlag,
	TupleID *contentTid, 
	FILE *fd
);

Four lom_Text_ExtractKeywordFromContentBufferIntoPostingBuffer(
	LOM_Handle *handle, 
	lom_FptrToKeywordExtractor fptrToKeywordExtractor,
	lom_FptrToGetNextPostingInfo fptrToGetNextPostingInfo,
	lom_FptrToFinalizeKeywordExtraction fptrToFinalizeKeywordExtraction,
	lom_FptrToFilter fptrToFilter,
	Four ocnOrScanId,		/* IN: open scan number for content table */
	Boolean useScanFlag,		/* IN flag */
	OID  *docOid,                   /* IN: document oid */
	Four docLogicalId,              /* IN: document logical id */
	Two colNo,			/* IN: system colNo */
	Four PostingBufferLength,       /* IN: length of posting buffer */
	char *postingBuffer,            /* OUT: posting buffer */
	Four *nKeywords                 /* OUT: number of keywords contained in content buffer */
);

Four lom_Text_ExtractKeywordFromContentObjectIntoTempFile(
	LOM_Handle *handle, 
	lom_FptrToKeywordExtractor fptrToKeywordExtractor,
	lom_FptrToGetNextPostingInfo fptrToGetNextPostingInfo,
	lom_FptrToFinalizeKeywordExtraction fptrToFinalizeKeywordExtraction,
	lom_FptrToFilter fptrToFilter,
	Four ocnOrScanId,		/* IN: open scan number for content table */
	Boolean useScanFlag,		/* IN flag */
	TupleID *contentOid,		/* IN: object id for content object */
	OID *docOid,			/* IN: document oid */
	Four docLogicalId,		/* IN: document logical id */
	Two systemLevelColNo,		/* IN: system level column number */
	char *tempPostingFileName	/* IN: temporary file */
);

Four lom_Text_SortPostingFileByKeyword(
	LOM_Handle *handle, 
	char *fileName,
	char *sortedFileName
);

Four lom_Text_CreateTempFile(
	LOM_Handle *handle, 
	char *fileName          /* IN: file name */
);

Four lom_Text_DestroyTempFile(
	LOM_Handle *handle, 
	char *fileName          /* IN: file name */
);

Four lom_Text_OpenTempFile(
	LOM_Handle *handle, 
	char *fileName,                 /* IN: file name */
	char *type,                     /* IN: open type */
	FILE **fd                       /* OUT: file descriptor */
);

Four lom_Text_CloseTempFile(
	LOM_Handle *handle, 
	FILE *fd                        /* IN: file descriptor */
);

Four lom_Text_NextPostingsFromTempFile(
	LOM_Handle *handle, 
	Four volId,											/* IN: volume id */
	ClassID classId,									/* IN: class id */
	Two systemLevelColNo,								/* IN: system level column number */
	lom_Text_PostingInfoForReading* postingInfo,		/* IN: posting info */
	FILE *fd, 											/* IN: file descriptor */
	Four postingBufferLength,							/* IN: length of posting buffer */
	char *postingBuffer,								/* IN: posting buffer */
	Four requestedNumOfPostings,						/* IN: the requested number of posings */
	Four *returnedNumOfPostings,						/* OUT: the returned number of postings */
	Four *requiredPostingBufferLength					/* OUT: required posting buffer length for containing at least one posting */
);

Four lom_Text_PreparePostingInfoForReading(
	LOM_Handle*								handle, 
	Four									volId,				/* IN: volume id */
	ClassID									classId,			/* IN: class id */
	Two										colNo,				/* IN: system level column number */
	lom_Text_PostingInfoForReading*			postingInfo 		/* OUT: posting info */
);

Four lom_Text_OpenDocIdIndexTable(LOM_Handle *, Four, char *, char *);

Four lom_Text_DropIndexInfoFromCatalog(
	LOM_Handle *handle, 
	Four volId, 	/* IN: volumn id */
	char *className, /* IN: class name */
	char *attrName 	/* IN: attribute name */
);

Four LOM_Text_GetNPostingsOfCurrentKeyword(
	LOM_Handle *handle, 
	Four textScan,
	Four *nPostings
);

Four LOM_Text_GetNPostings(
        LOM_Handle *handle, 
        Four ocn,       /* IN: open class number */
        LOM_IndexID *indexId,   /* IN: index id */
        Four keywordKind,       /* IN: kind of keyword */
        BoundCond *keywordStartBound, /* IN: start bound */
        BoundCond *keywordStopBound,    /* IN: stop bound */
        LockParameter *lockup,          /* IN: lock parameter */
        Four *nPostings
);

Four lom_Text_AddIndexInfoIntoCatalog(
	LOM_Handle *handle, 
	Four volId, 	/* IN: volumn id */
	char *className, /* IN: class name */
	char *attrName,	/* IN: attribute name */
	Two colNo 	/* IN: column number */
);

Four lom_Text_AddIndexInfoIntoCatalog(
	LOM_Handle *handle, 
	Four volId, 	/* IN: volumn id */
	char *className, /* IN: class name */
	char *attrName,	/* IN: attribute name */
	Two colNo 	/* IN: column number */
);

Four lom_CreateNamedObjectTable(
	LOM_Handle *handle, 
	Four volId              /* volumn id */ /* system volume */
);

Four lom_DestroyNamedObjectTable(
	LOM_Handle *handle, 
	Four volId              /* volumn id */ /* system volume */
);

Four LOM_OpenNamedObjectTable(
	LOM_Handle *handle, 
	Four volId
);

Four LOM_CloseNamedObjectTable(
	LOM_Handle *handle, 
	Four ocn
);

Four LOM_SetObjectName(
	LOM_Handle *handle, 
	Four ocn,
	char *objectName,
	OID *oid
);

Four LOM_LookUpNamedObject(
	LOM_Handle *handle, 
	Four ocn,
	char *objectName,
	OID *oid
);

Four LOM_ResetObjectName(
	LOM_Handle *handle, 
	Four ocn,
	char *objectName
);

Four LOM_RenameNamedObject(
	LOM_Handle *handle, 
	Four ocn,
	char *oldName,
	char *newName
);

/* get object name */
Four LOM_GetObjectName(
	LOM_Handle *handle, 
	Four ocn,
	OID *oid,
	char *objectName
);

Four LOM_OpenClassBulkLoad(
	LOM_Handle*		handle,				/* IN LOM system handle */
	Four			volId,				/* IN volume id */
	Four			temporaryVolId,		/* IN id for temporary volume used in sorting and etc. */
	char*			className,			/* IN class name that bulk load will be processed */
	Boolean			isNeedSort,			/* IN flag which indicates input data must be sorted by clustering index key */
	Boolean         isIndexBulkLoad,    /* IN flag which indicates build index by bulkloading facility */
	Two				pff,				/* IN Page fill factor */
	Two				eff,				/* IN Extent fill factor */
	LockParameter*	lockup				/* IN Lock parameter */
);

Four LOM_CloseClassBulkLoad(
	LOM_Handle*		handle,				/* IN LOM system handle */
	Four            bulkLoadId			/* IN Bulk Load Id */
);

Four LOM_NextClassBulkLoad(
	LOM_Handle*			handle,			/* IN LOM system handle */
	Four				bulkLoadId,		/* IN Bulk Load Id */
    Four				nCols,			/* IN number of columns */
    LOM_ColListStruct*	clist,			/* IN data of the created columns */
    Boolean				endOfTuple,		/* IN flag which indicates given clist is end of object or not */
	Four*				logicalId,		/* OUT logical id of the object */
	OID*				oid				/* OUT physical id of the object */
);

Four LOM_SetUserGivenLogicalID_BulkLoad(
	LOM_Handle*			handle,			/* IN LOM system handle */
	Four				bulkLoadId,		/* IN Bulk Load Id */
	Four				logicalId		/* IN New logical id used in bulkloading */
);

Four LOM_NextClassBulkLoad_OrderedSetBulkLoad(
	LOM_Handle*		handle,				/* IN LOM system handle */
	Four			temporaryVolId,		/* IN: id for temporary volume used in sorting and etc. */
    Four			bulkLoadId,			/* IN */
    Four			colNo,				/* IN */
	Four            nElements,          /* IN # of elements to append */
    Four            elementsBufSize,    /* IN buffer size */
    char*           elementsBuf,        /* IN elements to append */
    Boolean			endOfTuple,			/* IN */
	Four*			logicalId,			/* OUT logical id of the object */
	OID*			oid					/* OUT physical id of the object */
);

Four LOM_OrderedSetAppendBulkLoad(
	LOM_Handle*		handle,				/* IN LOM system handle */
	Four			temporaryVolId,		/* IN id for temporary volume used in sorting and etc. */
	Four			ocnOrScanId,		/* IN open class number of scan id */
	Boolean			useScanFlag,		/* IN use scan flag */
    OID*			oid,				/* IN */
    Four            colNo,              /* IN column number of ordered set type */
    Four            nElements,          /* IN # of elements to append */
    Four            elementsBufSize,    /* IN buffer size */
    char*           elementsBuf,        /* IN elements to append */
	LockParameter*  lockup				/* IN lock up parameter */
);

Four LOM_Text_CreateContentBulkLoad(
	LOM_Handle*		handle,				/* IN LOM system handle */
	Four			bulkLoadId,			/* IN Bulk load Id */
	Two				colNum,				/* IN: column number */
	TextColStruct*	text,				/* IN: text column struct */
	LOM_TextDesc*	textDesc, 			/* INOUT: text descriptor */
	Boolean			endOfTuple,			/* IN flag which indicates this ordered set is end of tuple or not */
	Four*			logicalId,			/* OUT logical id of the object */
	OID*			oid					/* OUT physical id of the object */
);

/*  
    define direction of relationship
*/
#define LOM_RELATIONSHIP_UNIDIRECTIONAL     0
#define LOM_RELATIONSHIP_BIDIRECTIONAL      1

/*
    define cardinality
*/
#define LOM_RELATIONSHIP_ONE_TO_ONE		0
#define LOM_RELATIONSHIP_ONE_TO_MANY	1
#define LOM_RELATIONSHIP_MANY_TO_ONE	2
#define LOM_RELATIONSHIP_MANY_TO_MANY	3

/* 
	define Inheritence
*/
#define LOM_INHERITEDFROM_THIS_CLASS	-1

#include "LOM_Literal.h"
#include "LOM_Text_Admin.h"

Four lom_Text_GetAndIncrementLastFilterNo(LOM_Handle *handle, Four);
Four lom_Text_GetAndIncrementLastKeywordExtractorNo(LOM_Handle *handle, Four);

Four lom_AddRelationship(
    LOM_Handle*		handle, 
    Four            ocnOrScanId,
	Boolean         useScanFlag,
    Four            fromClassId,         /* IN */
    Two             fromAttrNum,         /* IN */
    Four            toClassId,           /* IN */ 
    Two             toAttrNum,           /* IN */
    One             direction,           /* IN */
    One             cardinality,         /* IN */
    Four            relationshipId,      /* IN */
    char*           relationshipName);   /* IN */
#ifdef LOM_CLIENT
#define lom_GetSubClasses	LOM_GetSubClasses
Four LOM_GetSubClasses(
    LOM_Handle*		handle, 
    Four            volId,              /* mount volume index */
    Four            classId,            /* IN  : class */
    Four            fromNthSubClass,    /* IN  : */
    Four            sizeOfSubClasses,   /* IN  : size of array which will
                                           contain subclasses of the class */
    Four*           subClasses          /* OUT : array of sub classes */
);
#else
Four lom_GetSubClasses(
    LOM_Handle*		handle, 
    Four            volId,              /* mount volume index */
    Four            classId,            /* IN  : class */
    Four            fromNthSubClass,    /* IN  : */
    Four            sizeOfSubClasses,   /* IN  : size of array which will
                                           contain subclasses of the class */
    Four*           subClasses          /* OUT : array of sub classes */
);
#endif
Four lom_AdjustAttrNum(
	LOM_Handle *handle, 
	Four		volId,					/* mount volume index */
	Four		classId,				/* IN  : class */
	Two			attrNum,				/* IN  : class's attr num */
	Four 		subClassId,				/* IN  : sub class id */
	Two*		subClassAttrNum);		/* OUT : adjusted subclass's attr num */

Four Catalog_Relationship_DestroyClass(LOM_Handle *, Four, Four);

Four lom_Text_UpdateEmbeddedAttrs(
	LOM_Handle *handle, 
	Four ocnOrScanId,		/* IN: ocn or scanId */
	Boolean useScanFlag,		/* IN: flag */
	OID *docOid,			/* IN: doc oid */
	Four logicalDocId,		/* IN: document id */
	Two colNo			/* IN: column number */
);

Four LOM_Text_DefinePostingStructure(
	LOM_Handle *handle,
	Four volId,
	char *className,
	char *attrName,
	PostingStructureInfo *postingInfo
);

Four LOM_GetEmbeddedAttrTranslationInfo(
	LOM_Handle*							handle,
	Four								textScanId,
	LOM_EmbeddedAttrTranslationInfo*	embeddedAttrTranslationInfo
);

Four LOM_GetEmbeddedAttrsVal(
	LOM_Handle *handle,
	Four textScanId,
	char *ptrToEmbeddedAttrsBuf,
    Four embeddedAttrSize,
	Four nCols,
	LOM_ColListStruct *clist
);

/* The function parameter is changed */
Four lom_Text_GetStemizerFPtr(
	LOM_Handle			*handle,
	Four				ocn,
	Four				colNo,
	lom_FptrToStemizer	*stemizerFPtr
);

#define LOM_Text_GetStemizerFPtr(handle, ocn, colNo, fPtr) \
	lom_Text_GetStemizerFPtr((handle), (ocn), GET_SYSTEMLEVEL_COLNO((colNo)), (fPtr))

Four LOM_Text_GetOIDFromLogicalDocId(
	LOM_Handle *handle,
	Four ocn,
	Four logicalDocID,
	OID *oid
);

Four lom_EncodeStringVal(
	LOM_ColListStruct *clist,
	char *buf
);

#ifdef LOM_CLIENT
Four lom_ConvertIndexDesc(
	LOM_Handle *handle, 
	LOM_IndexDesc *lom_idesc,
	Server_IndexDesc *rpc_idesc,
	Two directionFlag
);

Four lom_ConvertIndexID(
	LOM_Handle *handle, 
	LOM_IndexID *lom_iid,
	Server_IndexID *rpc_iid,
	Two directionFlag
);

Four lom_ConvertBoundCond(
	LOM_Handle *handle, 
	BoundCond *boundCond,
	Server_BoundCond *rpc_BoundCond,
	Two directionFlag
);

Four lom_ConvertBoolExp(
	LOM_Handle *handle, 
	Four volId,
	Four classId,
	Four nBools,
	BoolExp *bool,
	Server_BoolExp *rpc_bool,
	Two directionFlag
);

Four lom_ConvertLockParameter(
	LOM_Handle *handle, 
	LockParameter *lockup,
	Server_LockParameter *rpc_lockup,
	Two directionFlag
);

Four lom_ConvertTransId(
	LOM_Handle *handle, 
	XactID *transid,
	Server_TransID *rpc_transid,
	Two directionFlag
);

#endif

Four LOM_MLGF_OpenIndexScan(
    LOM_Handle      *handle,
    Four            orn,            /* IN open relation number */
    LOM_IndexID     *iid,           /* IN MLGF index to be used for this scan */
    MLGF_HashValue  lowerBounds[],  /* IN lower bounds of region */
    MLGF_HashValue  upperBounds[],  /* IN upper bounds of region */
    Four            nBools,         /* IN number of boolean expressions */
    BoolExp         boolexp[],         /* IN array of boolean expressions */
    LockParameter   *lockup);       /* IN lock mode & duration */

Four LOM_MLGF_SearchNearTuple(
    LOM_Handle      *handle,
    Four            orn,            /* IN open relation number */
    LOM_IndexID     *iid,           /* IN the used index */
    MLGF_HashValue  kval[],         /* IN hash values of the given object */
    OID             *oid,           /* OUT TupleID of the near tuple */
    LockParameter   *lockup);       /* IN request lock or not */
Four LOM_CreateClassCatalogsForSystemTables(
	LOM_Handle*     handle,     /* IN LOM system handle */
	Four            volId       /* IN Volume ID where the catalog is created */
);

void* lom_dlopen(LOM_Handle *handle, char* pathname, int mode);
int   lom_dlclose(LOM_Handle *handle, void* dlhandle);
void* lom_dlsym(LOM_Handle *handle, void* dlhandle, char *name);
char* lom_dlerror(LOM_Handle *handle);

void* lom_RPCdlopen(LOM_Handle *handle, char *moduleName, int mode);
int   lom_RPCdlclose(LOM_Handle *handle, void *dlhandle);
void* lom_RPCdlsym(LOM_Handle *handle, void *dlhandle, char *name);
char* lom_RPCdlerror(LOM_Handle *handle, void *dlhandle);

Four  lom_CallDll_KeywordExtractorInit(lom_FptrToKeywordExtractor funcPtr, Four locationOfContent, LOM_Handle *handle, Four volId, char *className, OID *oid, Two colNo, char *inFileName, Four *resultHandle);
Four  lom_CallDll_KeywordExtractorNext(lom_FptrToGetNextPostingInfo funcPtr, Four resultHandle, char *keyword, Four *nPositions, char *positionList);
Four  lom_CallDll_KeywordExtractorFinal(lom_FptrToFinalizeKeywordExtraction funcPtr, Four resultHandle);
Four  lom_CallDll_Filter(lom_FptrToStemizer funcPtr, Four locationOfContent, LOM_Handle *handle, Four volId, char *className, OID *oid, Two colNo, char *inFileName, char *outFileName);
Four  lom_CallDll_Stemizer(lom_FptrToStemizer funcPtr, char *keyword, char *stemizedKeyword);

Four  lom_RPCKeywordExtractorInit(lom_FptrToKeywordExtractor funcPtr, Four locationOfContent, LOM_Handle *handle, Four volId, char *className, OID *oid, Two colNo, char *inFileName, Four *resultHandle);
Four  lom_RPCKeywordExtractorNext(lom_FptrToGetNextPostingInfo funcPtr, Four resultHandle, char *keyword, Four *nPositions, char *positionList);
Four  lom_RPCKeywordExtractorFinal(lom_FptrToFinalizeKeywordExtraction funcPtr, Four resultHandle);

Four lom_ConstructEmbeddedAttrTranslationInfo(LOM_Handle *handle,Four textScanId);

Four LOM_DeferredDestroyObject(LOM_Handle* handle, Four ocnOrScanId, Boolean useScanFlag, OID* oid);
Four LOM_BatchDestroyByDeferredDeletionList(LOM_Handle* handle, Four temporaryVolId, Four ocnOrScanId, Boolean useScanFlag);
Four lom_SortDeferredDeletionList(LOM_Handle* handle, Four temporaryVolId, Four ocnOrScanId, Boolean useScanFlag);
Four lom_Text_DeleteInvertedIndexEntryByDeferredDeletionList(LOM_Handle* handle, Four temporaryVolId, Four ocnOrScanId,	Boolean useScanFlag, Two textColNo);
Four lom_Text_DeleteDocIdIndexEntryByDeferredDeletionList(LOM_Handle* handle, Four ocnOrScanId,	Boolean useScanFlag, Two textColNo);
Four lom_Text_DestoryContentByDeferredDeletionList(LOM_Handle* handle, Four ocnOrScanId, Boolean useScanFlag, Two textColNo);
Four lom_CleanDeferredDeletionList(LOM_Handle* handle, Four ocnOrScanId, Boolean useScanFlag);
Four lom_CreateDeferredDeletionListTable(LOM_Handle* handle, Four volId, char* className);
Four lom_DestroyDeferredDeletionListTable(LOM_Handle* handle, Four volId, char* className);
Four lom_OpenDeferredDeletionListTable(LOM_Handle* handle, Four volId, char* className);
Four lom_CloseDeferredDeletionListTable(LOM_Handle* handle, Four orn);
Four lom_AppendObjectToDeferredDeletionList(LOM_Handle* handle, Four ocnOrScanId, Boolean useScanFlag, OID* oid);
Four lom_DeferredDeletionList_Scan_Open(LOM_Handle* handle, Four ocnOrScanId, Boolean useScanFlag);
Four lom_DeferredDeletionList_Scan_Close(LOM_Handle* handle, Four scanId);
Four lom_DeferredDeletionList_Scan_NextElements(LOM_Handle* handle, Four scanId, Four nElements, Four elementsSize, void* elements);
Four lom_GetFirstTupleIdFromDeferredDeletionListTable(LOM_Handle* handle, Four ocnOrScanId, Boolean useScanFlag, TupleID* tid);
Four lom_ShowDeferredDeletionListStatus(LOM_Handle* handle, Four volId, char* className);

Four lom_ScanTableInit(LOM_Handle* handle);
Four lom_ScanTableFinal(LOM_Handle* handle);
Four lom_ScanTableAllocEntry(LOM_Handle* handle);
Four lom_ScanTableFreeEntry(LOM_Handle* handle, Four scanId);
Four lom_RelationshipScanTableInit(LOM_Handle* handle);
Four lom_RelationshipScanTableFinal(LOM_Handle* handle);
Four lom_RelationshipScanTableAllocEntry(LOM_Handle* handle);
Four lom_RelationshipScanTableFreeEntry(LOM_Handle* handle, Four scanId);
Four lom_ODMGcollectionScanTableInit(LOM_Handle* handle);
Four lom_ODMGcollectionScanTableFinal(LOM_Handle* handle);
Four lom_ODMGcollectionScanTableAllocEntry(LOM_Handle* handle);
Four lom_ODMGcollectionScanTableFreeEntry(LOM_Handle* handle, Four scanId);

Four LOM_GetNumObjectsInClass(LOM_Handle* handle, Four volId, char* className, Four* numObjects);
Four LOM_IsTextClass(LOM_Handle* handle, Four volId, char* className);

Four LOM_SetCfgParam(LOM_Handle* handle, char* name, char* value);
char* LOM_GetCfgParam(LOM_Handle* handle, char* name);

#ifdef  __cplusplus
}
#endif

#endif
