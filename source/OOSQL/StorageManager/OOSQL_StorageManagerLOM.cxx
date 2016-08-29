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

#include "OOSQL_StorageSystemHeaders.h"
#include "OOSQL_StorageManager.hxx"
#include "OOSQL_Error.h"
#include <assert.h>

OOSQL_StorageManagerLOM::OOSQL_StorageManagerLOM(LOM_Handle* systemHandle)
{
	m_systemHandle = systemHandle;
	
	// Run time system check...
	assert(sizeof(OOSQL_StorageManager::MBR) == sizeof(::LRDS_MBR));
	assert(sizeof(OOSQL_StorageManager::PageID) == sizeof(::PageID));
	assert(sizeof(OOSQL_StorageManager::ObjectID) == sizeof(::ObjectID));		
	assert(sizeof(OOSQL_StorageManager::OID) == sizeof(::OID));
	assert(sizeof(OOSQL_StorageManager::IndexID) == sizeof(::LOM_IndexID));
	assert(sizeof(OOSQL_StorageManager::BTreeKeyInfo) == sizeof(::KeyInfo));
	assert(sizeof(OOSQL_StorageManager::MLGF_KeyInfo) == sizeof(::MLGF_KeyInfo));
	assert(sizeof(OOSQL_StorageManager::KeyValue) == sizeof(::KeyValue));
	assert(sizeof(OOSQL_StorageManager::AnyCursor) == sizeof(::LRDS_AnyCursor));
	assert(sizeof(OOSQL_StorageManager::DataCursor) == sizeof(::LRDS_DataCursor));
	assert(sizeof(OOSQL_StorageManager::BtreeCursor) == sizeof(::LRDS_BtreeCursor));
	assert(sizeof(OOSQL_StorageManager::MLGF_Cursor) == sizeof(::LRDS_MLGF_Cursor));
	assert(sizeof(OOSQL_StorageManager::Cursor) == sizeof(::LRDS_Cursor));
	assert(sizeof(OOSQL_StorageManager::IndexDesc) == sizeof(::LOM_IndexDesc));
	assert(sizeof(OOSQL_StorageManager::AttrInfo) == sizeof(::AttrInfo));
	assert(sizeof(OOSQL_StorageManager::ColListStruct) == sizeof(::LOM_ColListStruct));
	assert(sizeof(OOSQL_StorageManager::ColLengthInfoListStruct) == sizeof(::ColLengthInfoListStruct));
	assert(sizeof(OOSQL_StorageManager::BoundCond) == sizeof(::BoundCond));
	assert(sizeof(OOSQL_StorageManager::BoolExp) == sizeof(::BoolExp));
	assert(sizeof(OOSQL_StorageManager::TextDesc) == sizeof(::LOM_TextDesc));
	assert(sizeof(OOSQL_StorageManager::TextColStruct) == sizeof(::TextColStruct));
	assert(sizeof(OOSQL_StorageManager::Time) == sizeof(::LOM_Time));
}

OOSQL_StorageManagerLOM::~OOSQL_StorageManagerLOM()
{
}

Four OOSQL_StorageManagerLOM::AlterClass()
{
	return 0;
}

Four OOSQL_StorageManagerLOM::CreateSequence()
{
	return 0;
}

Four OOSQL_StorageManagerLOM::CheckSequence(Four volId, char* seqName)
{
	return 0;
}

Four OOSQL_StorageManagerLOM::DropSequence(Four volId, char* seqName)
{
	return 0;
}

Four OOSQL_StorageManagerLOM::SetSeqVal(Four volId, char* seqName, Four value)
{
	return 0;
}

Four OOSQL_StorageManagerLOM::GetSeqCurrVal(Four volId, char* seqName, Four *currValue)
{
	return 0;
}

Four OOSQL_StorageManagerLOM::GetSeqNextVal(Four volId, char* seqName, Four *nextValue)
{
	return 0;
}



Four OOSQL_StorageManagerLOM::CreateClass(Four volId, char* className, char *indexName, IndexDesc* indexDesc, Four nAttrs, AttrInfo* attrInfo, Four nSuperClasses, char (*superClasses)[MAXCLASSNAME], Four nMethods, MethodInfo* methodInfo, Boolean isTempClass, Four * classId)
{
	Four e;
	Four tmpClassId;
	
	e = LOM_GetNewClassId(m_systemHandle, volId, isTempClass, &tmpClassId);
	OOSQL_CHECK_ERR(e);

	*classId = tmpClassId;

	e = LOM_CreateClass(m_systemHandle, volId, className, indexName, (::LOM_IndexDesc*)indexDesc, nAttrs, (::AttrInfo*)attrInfo,
		                nSuperClasses, superClasses, nMethods, (::MethodInfo*)methodInfo, isTempClass, tmpClassId);

	if(e == eRELATIONEXIST) 
		return eCLASSDUPLICATED_OOSQL;
	else
		return e;
}

Four OOSQL_StorageManagerLOM::DestroyClass(Four volId, char* className)
{
	return LOM_DestroyClass(m_systemHandle, volId, className);
}

Four OOSQL_StorageManagerLOM::OpenClass(Four volId, char* className)
{
	return LOM_OpenClass(m_systemHandle, volId, className);
}

Four OOSQL_StorageManagerLOM::OpenClass(Four volId, Four classId)
{
	Four classInfo;
	Four e;
	Four mv;

	e = Catalog_GetMountTableInfo(m_systemHandle, volId, &mv);
	OOSQL_CHECK_ERR(e);

    e = Catalog_GetClassInfo(m_systemHandle, volId, classId, &classInfo);
    OOSQL_CHECK_ERR(e);

	return LOM_OpenClass(m_systemHandle, volId, CATALOG_GET_CLASSNAME(&(CATALOG_GET_CLASSINFOTBL(m_systemHandle, mv)[classInfo])));
}

Four OOSQL_StorageManagerLOM::CloseClass(Four ocn)
{
	return LOM_CloseClass(m_systemHandle, ocn);
}

Four OOSQL_StorageManagerLOM::GetOpenClassNum(Four volId, char* className)
{
	return LOM_GetOpenClassNum(m_systemHandle, volId, className);
}

Four OOSQL_StorageManagerLOM::GetOpenClassNum(Four volId, Four classId)
{
	Four classInfo;
	Four e;
	Four mv;

	e = Catalog_GetMountTableInfo(m_systemHandle, volId, &mv);
	OOSQL_CHECK_ERR(e);

    e = Catalog_GetClassInfo(m_systemHandle, volId, classId, &classInfo);
    OOSQL_CHECK_ERR(e);

	return LOM_GetOpenClassNum(m_systemHandle, volId, CATALOG_GET_CLASSNAME(&(CATALOG_GET_CLASSINFOTBL(m_systemHandle, mv)[classInfo])));
}	

Four OOSQL_StorageManagerLOM::GetClassID(Four volId, char* className, Four* classId)
{
	return LOM_GetClassID(m_systemHandle, volId, className, classId);
}

// Transaction
Four OOSQL_StorageManagerLOM::TransBegin(XactID* xctId)
{
	return LOM_TransBegin(m_systemHandle, (::XactID*)xctId);
}

Four OOSQL_StorageManagerLOM::TransCommit(XactID* xctId)
{
	return LOM_TransCommit(m_systemHandle, (::XactID*)xctId);
}

Four OOSQL_StorageManagerLOM::TransAbort(XactID* xctId)
{
	return LOM_TransAbort(m_systemHandle, (::XactID*)xctId);
}

// Mount & Dismount
Four OOSQL_StorageManagerLOM::Mount(Four numDevices, char** deviceNames, Four* volumeId)
{
	return LOM_Mount(m_systemHandle, numDevices, deviceNames, volumeId);
}

Four OOSQL_StorageManagerLOM::Dismount(Four volumeId)
{
	return LOM_Dismount(m_systemHandle, volumeId);
}

// Object Manipulation
Four OOSQL_StorageManagerLOM::NextObject(Four scanId, OID* oid, Cursor **cursor)
{
	return LOM_NextObject(m_systemHandle, scanId, (::OID*)oid, (::LRDS_Cursor**)cursor);
}

Four OOSQL_StorageManagerLOM::DestroyObject(Four ocnOrScanId, Boolean useScanFlag, OID* oid)
{
	return LOM_DestroyObject(m_systemHandle, ocnOrScanId, useScanFlag, (::OID*)oid);
}

Four OOSQL_StorageManagerLOM::CreateObjectByColList(Four ocnOrScanId, Boolean useScanFlag, Four nCols, ColListStruct* clist, OID* oid)
{
    ConvertToUserLevelColNoInColListStruct(nCols, clist);
	Four e = LOM_CreateObjectByColList(m_systemHandle, ocnOrScanId, useScanFlag, nCols, (::LOM_ColListStruct*)clist, (::OID*)oid);
    ConvertToSystemLevelColNoInColListStruct(nCols, clist);
    return e;
}

Four OOSQL_StorageManagerLOM::FetchObjectByColList(Four ocnOrScanId, Boolean useScanFlag, OID* oid, Four nCols, ColListStruct* clist)
{
    ConvertToUserLevelColNoInColListStruct(nCols, clist);
	Four e = LOM_FetchObjectByColList(m_systemHandle, ocnOrScanId, useScanFlag, (::OID*)oid, nCols, (::LOM_ColListStruct*)clist);
    ConvertToSystemLevelColNoInColListStruct(nCols, clist);
    return e;
}

Four OOSQL_StorageManagerLOM::UpdateObjectByColList(Four ocnOrScanId, Boolean useScanFlag, OID* oid, Four nCols, ColListStruct* clist)
{
    ConvertToUserLevelColNoInColListStruct(nCols, clist);
	Four e = LOM_UpdateObjectByColList(m_systemHandle, ocnOrScanId, useScanFlag, (::OID*)oid, nCols, (::LOM_ColListStruct*)clist);
    ConvertToSystemLevelColNoInColListStruct(nCols, clist);
    return e;
}

Four OOSQL_StorageManagerLOM::FetchColLength(Four ocnOrScanId, Boolean useScanFlag, OID* oid, Four nCols, ColLengthInfoListStruct* clengthlist)
{
    ConvertToUserLevelColNoInColLengthInfoListStruct(nCols, clengthlist);
	Four e = LOM_FetchColLength(m_systemHandle, ocnOrScanId, useScanFlag, (::OID*)oid, nCols, (::ColLengthInfoListStruct*)clengthlist);
    ConvertToSystemLevelColNoInColLengthInfoListStruct(nCols, clengthlist);
    return e;
}

// Relationship
Four OOSQL_StorageManagerLOM::Relationship_Create(Four volId, char *relationshipName, Four fromClassId, Two fromAttrNum, Four toClassId, Two toAttrNum, One cardinality, One direction, Four* relationshipId)
{
	return LOM_Relationship_Create(m_systemHandle, volId, relationshipName, fromClassId, fromAttrNum, toClassId, toAttrNum, cardinality, direction, relationshipId);
}

Four OOSQL_StorageManagerLOM::Relationship_Destroy(Four volId, char *relationshipName)
{
	return LOM_Relationship_Destroy(m_systemHandle, volId, relationshipName);
}

Four OOSQL_StorageManagerLOM::Relationship_CreateInstance(Four fromOcnOrScanId, Boolean fromUseScanFlag, Four toOcnOrScanId, Boolean toUseScanFlag, Four relationshipId, OID* fromOid, OID* toOid)
{
	return LOM_Relationship_CreateInstance(m_systemHandle, fromOcnOrScanId, fromUseScanFlag, toOcnOrScanId, toUseScanFlag, relationshipId, (::OID*)fromOid, (::OID*)toOid);
}

Four OOSQL_StorageManagerLOM::Relationship_DestroyInstance(Four fromOcnOrScanId, Boolean fromUseScanFlag, Four toOcnOrScanId, Boolean toUseScanFlag, Four relationshipId, OID* fromOid, OID* toOid)
{
	return LOM_Relationship_DestroyInstance(m_systemHandle, fromOcnOrScanId, fromUseScanFlag, toOcnOrScanId, toUseScanFlag, relationshipId, (::OID*)fromOid, (::OID*)toOid);
}

Four OOSQL_StorageManagerLOM::Relationship_OpenScan(Four ocnOrScanId, Boolean useScanFlag, OID* oid, Four relationshipId)
{
	return LOM_Relationship_OpenScan(m_systemHandle, ocnOrScanId, useScanFlag, (::OID*)oid, relationshipId);
}

Four OOSQL_StorageManagerLOM::Relationship_CloseScan(Four relationshipScanId)
{
	return LOM_Relationship_CloseScan(m_systemHandle, relationshipScanId);
}

Four OOSQL_StorageManagerLOM::Relationship_NextInstance(Four relationshipScanId, Four nOIDs, OID* oids)
{
	return LOM_Relationship_NextInstance(m_systemHandle, relationshipScanId, nOIDs, (::OID*)oids);
}

Four OOSQL_StorageManagerLOM::Relationship_GetId(Four volId, char* relationshipName, Four* relationshipId)
{
	return LOM_Relationship_GetId(m_systemHandle, volId, relationshipName, relationshipId);
}


// Error Message
char* OOSQL_StorageManagerLOM::Err(Four errorCode)
{
	return LOM_Err(m_systemHandle, errorCode);
}

// Scan
Four OOSQL_StorageManagerLOM::CloseScan(Four scanId)
{
	return LOM_CloseScan(m_systemHandle, scanId);
}

Four OOSQL_StorageManagerLOM::OpenIndexScan(Four ocn, IndexID* iid, BoundCond* startBound, BoundCond* stopBound, Four nBools, BoolExp* bools, LockParameter* lockup)
{
    ConvertToUserLevelColNoInBoolExp(nBools, bools);
	Four e = LOM_OpenIndexScan(m_systemHandle, ocn, (::LOM_IndexID*)iid, (::BoundCond*)startBound, (::BoundCond*)stopBound, nBools, (::BoolExp*)bools, (::LockParameter*)lockup);
    ConvertToSystemLevelColNoInBoolExp(nBools, bools);
    return e;
}

Four OOSQL_StorageManagerLOM::OpenSeqScan(Four ocn, Four scanDirection, Four nBools, BoolExp* bools, LockParameter* lockup)
{
    ConvertToUserLevelColNoInBoolExp(nBools, bools);
	Four e = LOM_OpenSeqScan(m_systemHandle, ocn, scanDirection, nBools, (::BoolExp*)bools, (::LockParameter*)lockup);
    ConvertToSystemLevelColNoInBoolExp(nBools, bools);
    return e;
}


// Index
Four OOSQL_StorageManagerLOM::AddIndex(Four volId, char *className, char *indexName, IndexDesc *indexDesc, IndexID *indexID)
{
	return LOM_AddIndex(m_systemHandle, volId, className, indexName, (::LOM_IndexDesc*)indexDesc, (::LOM_IndexID*)indexID);
}

Four OOSQL_StorageManagerLOM::DropIndex(Four volId, char *indexName)
{
	return LOM_DropIndex(m_systemHandle, volId, indexName);
}

// Sort Interface
Four OOSQL_StorageManagerLOM::SortRelation(Four volId, Four temporaryVolId, char *inRelName, BTreeKeyInfo *kinfo, Boolean newRelFlag, char *outRelName, Boolean tmpRelFlag, LockParameter *lockup, LockParameter *lockupForTmpVol)
{
    return LRDS_SortRelation(LOM_GET_LRDS_HANDLE(handle), volId, temporaryVolId, inRelName, (::KeyInfo*)kinfo, newRelFlag, outRelName, tmpRelFlag, (::LockParameter*)lockup, (::LockParameter*)lockupForTmpVol);
}

// Text Interface
Four OOSQL_StorageManagerLOM::Text_CreateContent(Four ocnOrScanId, Boolean useScanFlag, OID *oid, Two colNo, TextColStruct *text, TextDesc *textDesc)
{
	return LOM_Text_CreateContent(m_systemHandle, ocnOrScanId, useScanFlag, (::OID*)oid, GET_USERLEVEL_COLNO(colNo), (::TextColStruct*)text, (::LOM_TextDesc*)textDesc);
}

Four OOSQL_StorageManagerLOM::Text_GetDescriptor(Four ocnOrScanId, Boolean useScanFlag, OID *oid, Two colNo, TextDesc *textDesc)
{
	return LOM_Text_GetDescriptor(m_systemHandle, ocnOrScanId, useScanFlag, (::OID*)oid, GET_USERLEVEL_COLNO(colNo), (::LOM_TextDesc*)textDesc);
}

Four OOSQL_StorageManagerLOM::Text_DestroyContent(Four ocnOrScanId, Boolean useScanFlag, OID *oid, Two colNo, TextDesc *textDesc)	
{
	return LOM_Text_DestroyContent(m_systemHandle, ocnOrScanId, useScanFlag, (::OID*)oid, GET_USERLEVEL_COLNO(colNo), (LOM_TextDesc*)textDesc);
}

Four OOSQL_StorageManagerLOM::Text_FetchContent(Four ocnOrScanId, Boolean useScanFlag, OID *oid, Two colNo, TextColStruct *text, TextDesc *textDesc)	
{
	return LOM_Text_FetchContent(m_systemHandle, ocnOrScanId, useScanFlag, (::OID*)oid, GET_USERLEVEL_COLNO(colNo), (::TextColStruct*)text, (::LOM_TextDesc*)textDesc);
}

Four OOSQL_StorageManagerLOM::Text_UpdateContent(Four ocnOrScanId, Boolean useScanFlag, OID *oid, Two colNo, TextColStruct *text, TextDesc *textDesc)	
{
	return LOM_Text_UpdateContent(m_systemHandle, ocnOrScanId, useScanFlag, (::OID*)oid, GET_USERLEVEL_COLNO(colNo), (::TextColStruct*)text, (::LOM_TextDesc*)textDesc);
}

Four OOSQL_StorageManagerLOM::Text_GetNPostingsOfCurrentKeyword(Four textScan, Four *nPostings)
{
	return LOM_Text_GetNPostingsOfCurrentKeyword(m_systemHandle, textScan, nPostings);
}

Four OOSQL_StorageManagerLOM::Text_OpenIndexScan(Four ocn, IndexID *indexId, Four keywordKind, BoundCond *keywordStartBound, BoundCond *keywordStopBound, LockParameter *lockup)
{
	return LOM_Text_OpenIndexScan(m_systemHandle, ocn, (::LOM_IndexID*)indexId,	keywordKind, (::BoundCond*)keywordStartBound, (::BoundCond*)keywordStopBound, (::LockParameter*)lockup);
}

Four OOSQL_StorageManagerLOM::Text_OpenIndexScan_GivenInvertedEntryTupleID(Four ocn, Two colNo, TupleID* invertedTableEntryTupleID, LockParameter* lockup)
{
	return LOM_Text_OpenIndexScan_GivenInvertedEntryTupleID(m_systemHandle, ocn, GET_USERLEVEL_COLNO(colNo), (::TupleID*)invertedTableEntryTupleID, (::LockParameter*)lockup);
}

Four OOSQL_StorageManagerLOM::Text_Scan_Open(Four ocn, OID *oid, Two colNo, Four keywordKind, BoundCond *keywordStartBound, BoundCond *keywordStopBound, LockParameter *lockup)
{
	return LOM_Text_Scan_Open(m_systemHandle, ocn, (::OID*)oid, GET_USERLEVEL_COLNO(colNo), keywordKind, (::BoundCond*)keywordStartBound, (::BoundCond*)keywordStopBound, (::LockParameter*)lockup);
}

Four OOSQL_StorageManagerLOM::Text_Scan_Close(Four osn)
{
	return LOM_Text_Scan_Close(m_systemHandle, osn);
}

Four OOSQL_StorageManagerLOM::Text_GetNPostings(Four ocn, IndexID *indexId, Four keywordKind, BoundCond *keywordStartBound, BoundCond *keywordStopBound, LockParameter *lockup, Four *nPostings)
{
	return LOM_Text_GetNPostings(m_systemHandle, ocn, (::LOM_IndexID*)indexId, keywordKind, (::BoundCond*)keywordStartBound, (::BoundCond*)keywordStopBound, (::LockParameter*)lockup, nPostings);
}

Four OOSQL_StorageManagerLOM::Text_Scan_NextPosting(Four textScan, Four bufferLength, char *postingBuffer, Four *requiredSize, PostingWeight *weight)
{
	return LOM_Text_Scan_NextPosting(m_systemHandle, textScan, bufferLength, postingBuffer, requiredSize, (::PostingWeight*)weight);
}

#ifndef COMPRESSION

Four OOSQL_StorageManagerLOM::Text_NextPostings(Four textScan, Four bufferLength, char *postingBuffer, Four scanDirection, Four logicalIdHints, Four *nReturnedPosting, Four *requiredSize)
{
	return LOM_Text_NextPostings(m_systemHandle, textScan, bufferLength, postingBuffer, scanDirection, logicalIdHints, nReturnedPosting, requiredSize);
}

#else

Four OOSQL_StorageManagerLOM::Text_NextPostings(Four textScan, Four bufferLength, char *postingBuffer, Four scanDirection, Four logicalIdHints, Four *nReturnedPosting, Four *requiredSize, VolNo *volNoOfPostingTupleID, Four *lastDocId)
{
	return LOM_Text_NextPostings(m_systemHandle, textScan, bufferLength, postingBuffer, scanDirection, logicalIdHints, nReturnedPosting, requiredSize, volNoOfPostingTupleID, lastDocId));
}

#endif

Four OOSQL_StorageManagerLOM::Text_GetCursorKeyword(Four textScan, char *keyword)
{
	return LOM_Text_GetCursorKeyword(m_systemHandle, textScan, keyword);
}

Four OOSQL_StorageManagerLOM::Text_MakeIndex(Four volId, char *className)
{
	return LOM_Text_MakeIndex(m_systemHandle, volId, className);
}

Four OOSQL_StorageManagerLOM::Text_BatchInvertedIndexBuild(Four volId, Four temporaryVolId, char *className)
{
	return LOM_Text_BatchInvertedIndexBuild(m_systemHandle, volId, temporaryVolId, className);
}

// Time Related
OOSQL_StorageManager::TimeZone OOSQL_StorageManagerLOM::GetLocalTimeZone()
{
	return (OOSQL_StorageManager::TimeZone)LOM_GetLocalTimeZone(m_systemHandle);
}

void OOSQL_StorageManagerLOM::SetCurTime(Time *_time, TimeZone tz)
{
	LOM_SetCurTime(m_systemHandle, (::LOM_Time*)_time, (::LOM_TimeZone)tz);
}

unsigned short OOSQL_StorageManagerLOM::GetHour(Time *time)
{
	return LOM_GetHour(m_systemHandle, (::LOM_Time*)time);
}

unsigned short OOSQL_StorageManagerLOM::GetMinute(Time *time)
{
	return LOM_GetMinute(m_systemHandle, (::LOM_Time*)time);
}

unsigned short OOSQL_StorageManagerLOM::GetSecond(Time *time)
{
	return LOM_GetSecond(m_systemHandle, (::LOM_Time*)time);
}

long OOSQL_StorageManagerLOM::GetJulDay(unsigned short m, unsigned short d, unsigned short y)
{
	return LOM_GetJulDay(m_systemHandle, m, d, y);
}

void OOSQL_StorageManagerLOM::GetGregorianDate(Date *date, unsigned short *mm, unsigned short *dd, unsigned short *yy)
{
	LOM_GetGregorianDate(m_systemHandle, (::LOM_Date*)date, mm, dd, yy);
}

void OOSQL_StorageManagerLOM::SetCurDate(Date *date)
{
	LOM_SetCurDate(m_systemHandle, (::LOM_Date*)date);
}

void OOSQL_StorageManagerLOM::SetDate(unsigned short year, unsigned short month, unsigned short day, Date *date)
{
	LOM_SetDate(m_systemHandle, year, month, day, (::LOM_Date*)date);
}

int OOSQL_StorageManagerLOM::CompareDate(Date *date1, Date *date2)
{
	return LOM_CompareDate(m_systemHandle, (::LOM_Date*)date1, (::LOM_Date*)date2);
}

unsigned short OOSQL_StorageManagerLOM::GetYear(Date *date)
{
	return LOM_GetYear(m_systemHandle, (::LOM_Date*)date);
}

unsigned short OOSQL_StorageManagerLOM::GetMonth(Date *date)
{
	return LOM_GetMonth(m_systemHandle, (::LOM_Date*)date);
}

unsigned short OOSQL_StorageManagerLOM::GetDay(Date *date)
{
	return LOM_GetDay(m_systemHandle, (::LOM_Date*)date);
}

