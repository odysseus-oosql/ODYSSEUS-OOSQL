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

char* OOSQL_StorageManager::TYPE_TO_DOMAIN_NAME(Four typeId)
{
	switch(typeId)
	{
	case OOSQL_TYPE_SHORT:
		return "short";
	case OOSQL_TYPE_INT:
        return "int";
	case OOSQL_TYPE_LONG:
        return "long";
	case OOSQL_TYPE_LONG_LONG:
        return "long long";
	case OOSQL_TYPE_FLOAT:
        return "float";
	case OOSQL_TYPE_DOUBLE:
        return "double";
	case OOSQL_TYPE_STRING:
        return "char";
	case OOSQL_TYPE_VARSTRING:
        return "varchar";
	case OOSQL_TYPE_OID:
        return "oid";
	case OOSQL_TYPE_TIME:
        return  "time";
	case OOSQL_TYPE_DATE:
        return  "date";
	case OOSQL_TYPE_TIMESTAMP:
        return  "timestamp";
	case OOSQL_TYPE_INTERVAL:
        return  "interval";
	case OOSQL_TYPE_TEXT:
        return  "text";
	default:
		return "undefined";
	}
}

OOSQL_StorageManagerLOM::OOSQL_StorageManagerLOM(OOSQL_SystemHandle* oosqlSystemHandle, LOM_Handle* systemHandle)
: OOSQL_StorageManager(oosqlSystemHandle)
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

Four OOSQL_StorageManagerLOM::AlterClass(Four volId, char* className, Four nAddCol, AttrInfo* addColInfo, Four nDropCol, AttrInfo* dropColInfo)
{
	Four e;
	Four classId;

	e = LOM_GetClassID(m_systemHandle, volId, className, &classId);
	if( e < 0 ) LOM_ERROR(m_systemHandle, e);

	return LOM_AlterClass(m_systemHandle, volId, className, classId, nAddCol, (::AttrInfo*)addColInfo, nDropCol, (::AttrInfo*)dropColInfo);
}

Four OOSQL_StorageManagerLOM::CreateSequence(Four volId, char* sequenceName, Four startWith)
{
	return LOM_CreateCounter(m_systemHandle, volId, sequenceName, startWith);
}

Four OOSQL_StorageManagerLOM::CheckSequence(Four volId, char* seqName)
{
	Four e;

	e = LOM_CheckCounter(m_systemHandle, volId, seqName);

	if(e < eNOERROR)
		return eNOT_FOUND_OOSQL;
	else 
		return eNOERROR;
}

Four OOSQL_StorageManagerLOM::DropSequence(Four volId, char* seqName)
{
	return LOM_DestroyCounter(m_systemHandle, volId, seqName);
}

Four OOSQL_StorageManagerLOM::SetSeqVal(Four volId, char* seqName, Four value)
{
	return LOM_SetCounter(m_systemHandle, volId, seqName, value);
}

Four OOSQL_StorageManagerLOM::GetSeqCurrVal(Four volId, char* seqName, Four *currValue)
{
	return LOM_GetCurrCounter(m_systemHandle, volId, seqName, currValue);
}

Four OOSQL_StorageManagerLOM::GetSeqNextVal(Four volId, char* seqName, Four *nextValue)
{
	return LOM_GetNextCounter(m_systemHandle, volId, seqName, nextValue);
}



Four OOSQL_StorageManagerLOM::CreateClass(Four volId, char* className, char *indexName, IndexDesc* indexDesc, Four nAttrs, AttrInfo* attrInfo, Four nSuperClasses, char (*superClasses)[MAXCLASSNAME], Four nMethods, MethodInfo* methodInfo, Boolean isTempClass, Four * classId)
{
	Four e;
	Four tmpClassId;
	
	e = LOM_GetNewClassId(m_systemHandle, volId, isTempClass, &tmpClassId);
	OOSQL_CHECK_ERR(e);

	*classId = tmpClassId;

    ConvertToUserLevelColNoInIndexDesc(indexDesc);
	e = LOM_CreateClass(m_systemHandle, volId, className, indexName, (::LOM_IndexDesc*)indexDesc, nAttrs, (::AttrInfo*)attrInfo,
		                nSuperClasses, superClasses, nMethods, (::MethodInfo*)methodInfo, isTempClass, tmpClassId);
    ConvertToSystemLevelColNoInIndexDesc(indexDesc);

	if(e == eRELATIONEXIST_LRDS) 
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
Four OOSQL_StorageManagerLOM::TransBegin(XactID* xctId, ConcurrencyLevel ccLevel)
{
	return LOM_TransBegin(m_systemHandle, (::XactID*)xctId, ccLevel);
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

Four OOSQL_StorageManagerLOM::DeferredDestroyObject(Four ocnOrScanId, Boolean useScanFlag, OID* oid)
{
	return LOM_DeferredDestroyObject(m_systemHandle, ocnOrScanId, useScanFlag, (::OID*)oid);
}

Four OOSQL_StorageManagerLOM::CreateObjectByColList(Four ocnOrScanId, Boolean useScanFlag, Four nCols, ColListStruct* clist, OID* oid)
{
    ConvertToUserLevelColNoInColListStruct(nCols, clist);
	Four e = LOM_CreateObjectByColList(m_systemHandle, ocnOrScanId, useScanFlag, nCols, (::LOM_ColListStruct*)clist, (::OID*)oid);
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

Four OOSQL_StorageManagerLOM::OpenMlgfIndexScan(Four ocn, IndexID* iid, MLGF_HashValue lowerBounds[], MLGF_HashValue upperBounds[], Four nBools, BoolExp* bools, LockParameter* lockup) 
{ 
	ConvertToUserLevelColNoInBoolExp(nBools, bools);
	Four e = LOM_MLGF_OpenIndexScan(m_systemHandle, ocn, (::LOM_IndexID*)iid, lowerBounds, upperBounds, nBools, (::BoolExp*)bools, (::LockParameter*)lockup);
    ConvertToSystemLevelColNoInBoolExp(nBools, bools);
    return e;
}


// Index
Four OOSQL_StorageManagerLOM::AddIndex(Four volId, char *className, char *indexName, IndexDesc *indexDesc, IndexID *indexID)
{
	Four e;

    ConvertToUserLevelColNoInIndexDesc(indexDesc);
	e = LOM_AddIndex(m_systemHandle, volId, className, indexName, (::LOM_IndexDesc*)indexDesc, (::LOM_IndexID*)indexID);
    ConvertToSystemLevelColNoInIndexDesc(indexDesc);
	if(e == eINDEXEXIST_LRDS)
		return eINDEXDUPLICATED_OOSQL;
	else
		return e;
}

Four OOSQL_StorageManagerLOM::DropIndex(Four volId, char *indexName)
{
	Four e;

	e = LOM_DropIndex(m_systemHandle, volId, indexName);
	if(e == eINDEXNOTFOUND_LOM)
		return eINDEXNOTDEFINED_OOSQL;
	else
		return e;
}

// Sort Interface
Four OOSQL_StorageManagerLOM::SortRelation(Four volId, Four temporaryVolId, char *inRelName, BTreeKeyInfo *kinfo, Boolean newRelFlag, char *outRelName, Boolean tmpRelFlag, LockParameter *lockup)
{
    return LRDS_SortRelation(LOM_GET_LRDS_HANDLE(m_systemHandle), volId, temporaryVolId, inRelName, (::KeyInfo*)kinfo, newRelFlag, outRelName, tmpRelFlag, (::LockParameter*)lockup);
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

Four OOSQL_StorageManagerLOM::Text_BatchInvertedIndexBuild(Four volId, Four temporaryVolId, char *className)
{
	return LOM_Text_BatchInvertedIndexBuild(m_systemHandle, volId, temporaryVolId, className);
}

Four OOSQL_StorageManagerLOM::Text_DefinePostingStructure(Four volId, char *className, char *attrName, PostingStructureInfo *postingInfo)
{
	return LOM_Text_DefinePostingStructure(m_systemHandle, volId, className, attrName, (::PostingStructureInfo*)postingInfo);
}

/* pass a function pointer as output parameter instead of return value */
Four OOSQL_StorageManagerLOM::Text_GetStemizerFPtr(Four ocn, Two colNo, void** stemizerFPtr)
{
	return LOM_Text_GetStemizerFPtr(m_systemHandle, ocn, GET_USERLEVEL_COLNO(colNo), stemizerFPtr);
}

Four OOSQL_StorageManagerLOM::Text_GetOIDFromLogicalDocId(Four ocn, Four logicalId, OID* oid)
{
	return LOM_Text_GetOIDFromLogicalDocId(m_systemHandle, ocn, logicalId, (::OID*)oid);
}

Four OOSQL_StorageManagerLOM::Text_MakeIndex(Four volId, Four temporaryVolId, char *className)
{
	return LOM_Text_MakeIndex(m_systemHandle, volId, temporaryVolId, className);
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

int OOSQL_StorageManagerLOM::CompareTime(Time* time1, Time* time2)
{
	unsigned short hour1, minute1, second1;
	unsigned short hour2, minute2, second2;

	hour1   = GetHour(time1);
	minute1 = GetMinute(time1);
	second1 = GetSecond(time1);

	hour2   = GetHour(time2);
	minute2 = GetMinute(time2);
	second2 = GetSecond(time2);

	if(hour1 < hour2)
		return -1;
	else if(hour1 > hour2)
		return 1;
	else
	{
		if(minute1 < minute2)
			return -1;
		else if(minute1 > minute2)
			return 1;
		else
		{
			if(second1 < second2)
				return -1;
			else if(second1 > second2)
				return 1;
			else
				return 0;
		}
	}

	return 0;
}
int OOSQL_StorageManagerLOM::CompareTimestamp(Timestamp* timestamp1, Timestamp* timestamp2)
{
	int dateResult;
	int timeResult;

	dateResult = CompareDate(&timestamp1->d, &timestamp2->d);
	timeResult = CompareTime(&timestamp1->t, &timestamp2->t);

	if(dateResult == 0)
		return timeResult;
	else
		return dateResult;
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

Four OOSQL_StorageManagerLOM::CreateCounter(Four volId, char *cntrName, Four initialValue, CounterID *cntrId)
{
	return LRDS_CreateCounter(LOM_GET_LRDS_HANDLE(m_systemHandle), volId, cntrName, initialValue, (::CounterID*)cntrId);
}

Four OOSQL_StorageManagerLOM::DestroyCounter(Four volId, char *cntrName)
{
	return LRDS_DestroyCounter(LOM_GET_LRDS_HANDLE(m_systemHandle), volId, cntrName);
}

Four OOSQL_StorageManagerLOM::GetCounterId(Four volId, char *cntrName, CounterID *cntrId)
{
	return LRDS_GetCounterId(LOM_GET_LRDS_HANDLE(m_systemHandle), volId, cntrName, (::CounterID*)cntrId);
}

Four OOSQL_StorageManagerLOM::SetCounter(Four volId, CounterID *cntrId, Four value)
{
	return LRDS_SetCounter(LOM_GET_LRDS_HANDLE(m_systemHandle), volId, (::CounterID*)cntrId, value);
}

Four OOSQL_StorageManagerLOM::ReadCounter(Four volId, CounterID *cntrId, Four *value)
{
	return LRDS_ReadCounter(LOM_GET_LRDS_HANDLE(m_systemHandle), volId, (::CounterID*)cntrId, value);
}

Four OOSQL_StorageManagerLOM::GetCounterValues(Four volId, CounterID *cntrId, Four nValues, Four *startValue)
{
	return LRDS_GetCounterValues(LOM_GET_LRDS_HANDLE(m_systemHandle), volId, (::CounterID*)cntrId, nValues, startValue);
}

Four OOSQL_StorageManagerLOM::CollectionSet_Create(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four keySize)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionSet_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_FALSE) OOSQL_ERROR(m_systemHandle, eNOTNULLCOLLECTION_OOSQL);

	return LOM_CollectionSet_Create(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo), keySize);
}
Four OOSQL_StorageManagerLOM::CollectionSet_Destroy(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionSet_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

    return LOM_CollectionSet_Destroy(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
}
Four OOSQL_StorageManagerLOM::CollectionSet_AssignElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four nElements, Four* elementSizes, void* elements)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionSet_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

    return LOM_CollectionSet_AssignElements(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo), nElements, elementSizes, elements);
}
Four OOSQL_StorageManagerLOM::CollectionSet_Assign(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four assignedOrnOrScanId, Boolean assignedUseScanFlag, OID* assignedOid, Four assignedColNo)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionSet_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

	return LOM_CollectionSet_Assign(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo), assignedOrnOrScanId, assignedUseScanFlag, (::TupleID*)assignedOid, assignedColNo);
}
Four OOSQL_StorageManagerLOM::CollectionSet_GetN_Elements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four* nElements)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionSet_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

	return LOM_CollectionSet_GetN_Elements(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo), nElements);
}
Four OOSQL_StorageManagerLOM::CollectionSet_InsertElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four nElements, Four* elementSizes, void* elements)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionSet_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

	return LOM_CollectionSet_InsertElements(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo), nElements, elementSizes, elements);
}
Four OOSQL_StorageManagerLOM::CollectionSet_DeleteElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four nElements, Four* elementSizes, void* elements)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionSet_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

	return LOM_CollectionSet_DeleteElements(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo), nElements, elementSizes, elements);
}
Four OOSQL_StorageManagerLOM::CollectionSet_DeleteAll(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionSet_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

	return LOM_CollectionSet_DeleteAll(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
}
Four OOSQL_StorageManagerLOM::CollectionSet_IsMember(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four elementSize, void* element)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionSet_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

	return LOM_CollectionSet_IsMember(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo), elementSize, element);
}
Four OOSQL_StorageManagerLOM::CollectionSet_IsNull(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo)
{
	return LOM_CollectionSet_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
}
Four OOSQL_StorageManagerLOM::CollectionSet_IsEqual(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four comparedOrnOrScanId, Boolean comparedUseScanFlag, OID* comparedOid, Four comparedColNo)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionSet_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

	return LOM_CollectionSet_IsEqual(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo), comparedOrnOrScanId, comparedUseScanFlag, (::TupleID*)comparedOid, comparedColNo);
}
Four OOSQL_StorageManagerLOM::CollectionSet_IsSubset(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four comparedOrnOrScanId, Boolean comparedUseScanFlag, OID* comparedOid, Four comparedColNo)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionSet_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

	return LOM_CollectionSet_IsSubset(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo), comparedOrnOrScanId, comparedUseScanFlag, (::TupleID*)comparedOid, comparedColNo);
}
Four OOSQL_StorageManagerLOM::CollectionSet_GetSizeOfElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four ith, Four nElements, Four* elementsSize)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionSet_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

	return LOM_CollectionSet_GetSizeOfElements(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo), ith, nElements, elementsSize);
}
Four OOSQL_StorageManagerLOM::CollectionSet_RetrieveElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four ith, Four nElements, Four* elementSizes, Four sizeOfElements, void* elements)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionSet_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

	return LOM_CollectionSet_RetrieveElements(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo), ith, nElements, elementSizes, sizeOfElements, elements);
}
Four OOSQL_StorageManagerLOM::CollectionSet_Union(Four ornOrScanIdA, Boolean useScanFlagA, OID* oidA, Four colNoA, Four ornOrScanIdB, Boolean useScanFlagB, OID* oidB, Four colNoB, Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionSet_IsNull(m_systemHandle, ornOrScanIdA, useScanFlagA, (::TupleID*)oidA, GET_USERLEVEL_COLNO(colNoA));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

    isNull = (Boolean)LOM_CollectionSet_IsNull(m_systemHandle, ornOrScanIdB, useScanFlagB, (::TupleID*)oidB, GET_USERLEVEL_COLNO(colNoB));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

    isNull = (Boolean)LOM_CollectionSet_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

	return LOM_CollectionSet_Union(m_systemHandle, ornOrScanIdA, useScanFlagA, (::TupleID*)oidA, colNoA, ornOrScanIdB, useScanFlagB, (::TupleID*)oidB, colNoB, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
}
Four OOSQL_StorageManagerLOM::CollectionSet_Intersect(Four ornOrScanIdA, Boolean useScanFlagA, OID* oidA, Four colNoA, Four ornOrScanIdB, Boolean useScanFlagB, OID* oidB, Four colNoB, Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionSet_IsNull(m_systemHandle, ornOrScanIdA, useScanFlagA, (::TupleID*)oidA, GET_USERLEVEL_COLNO(colNoA));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

    isNull = (Boolean)LOM_CollectionSet_IsNull(m_systemHandle, ornOrScanIdB, useScanFlagB, (::TupleID*)oidB, GET_USERLEVEL_COLNO(colNoB));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

    isNull = (Boolean)LOM_CollectionSet_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

	return LOM_CollectionSet_Intersect(m_systemHandle, ornOrScanIdA, useScanFlagA, (::TupleID*)oidA, colNoA, ornOrScanIdB, useScanFlagB, (::TupleID*)oidB, colNoB, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
}
Four OOSQL_StorageManagerLOM::CollectionSet_Difference(Four ornOrScanIdA, Boolean useScanFlagA, OID* oidA, Four colNoA, Four ornOrScanIdB, Boolean useScanFlagB, OID* oidB, Four colNoB, Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionSet_IsNull(m_systemHandle, ornOrScanIdA, useScanFlagA, (::TupleID*)oidA, GET_USERLEVEL_COLNO(colNoA));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

    isNull = (Boolean)LOM_CollectionSet_IsNull(m_systemHandle, ornOrScanIdB, useScanFlagB, (::TupleID*)oidB, GET_USERLEVEL_COLNO(colNoB));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

    isNull = (Boolean)LOM_CollectionSet_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

	return LOM_CollectionSet_Difference(m_systemHandle, ornOrScanIdA, useScanFlagA, (::TupleID*)oidA, colNoA, ornOrScanIdB, useScanFlagB, (::TupleID*)oidB, colNoB, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
}
Four OOSQL_StorageManagerLOM::CollectionSet_UnionWith(Four ornOrScanIdA, Boolean useScanFlagA, OID* oidA, Four colNoA, Four ornOrScanIdB, Boolean useScanFlagB, OID* oidB, Four colNoB)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionSet_IsNull(m_systemHandle, ornOrScanIdA, useScanFlagA, (::TupleID*)oidA, GET_USERLEVEL_COLNO(colNoA));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

    isNull = (Boolean)LOM_CollectionSet_IsNull(m_systemHandle, ornOrScanIdB, useScanFlagB, (::TupleID*)oidB, GET_USERLEVEL_COLNO(colNoB));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

	return LOM_CollectionSet_UnionWith(m_systemHandle, ornOrScanIdA, useScanFlagA, (::TupleID*)oidA, colNoA, ornOrScanIdB, useScanFlagB, (::TupleID*)oidB, colNoB);
}
Four OOSQL_StorageManagerLOM::CollectionSet_IntersectWith(Four ornOrScanIdA, Boolean useScanFlagA, OID* oidA, Four colNoA, Four ornOrScanIdB, Boolean useScanFlagB, OID* oidB, Four colNoB)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionSet_IsNull(m_systemHandle, ornOrScanIdA, useScanFlagA, (::TupleID*)oidA, GET_USERLEVEL_COLNO(colNoA));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

    isNull = (Boolean)LOM_CollectionSet_IsNull(m_systemHandle, ornOrScanIdB, useScanFlagB, (::TupleID*)oidB, GET_USERLEVEL_COLNO(colNoB));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

	return LOM_CollectionSet_IntersectWith(m_systemHandle, ornOrScanIdA, useScanFlagA, (::TupleID*)oidA, colNoA, ornOrScanIdB, useScanFlagB, (::TupleID*)oidB, colNoB);
}
Four OOSQL_StorageManagerLOM::CollectionSet_DifferenceWith(Four ornOrScanIdA, Boolean useScanFlagA, OID* oidA, Four colNoA, Four ornOrScanIdB, Boolean useScanFlagB, OID* oidB, Four colNoB)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionSet_IsNull(m_systemHandle, ornOrScanIdA, useScanFlagA, (::TupleID*)oidA, GET_USERLEVEL_COLNO(colNoA));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

    isNull = (Boolean)LOM_CollectionSet_IsNull(m_systemHandle, ornOrScanIdB, useScanFlagB, (::TupleID*)oidB, GET_USERLEVEL_COLNO(colNoB));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

	return LOM_CollectionSet_DifferenceWith(m_systemHandle, ornOrScanIdA, useScanFlagA, (::TupleID*)oidA, colNoA, ornOrScanIdB, useScanFlagB, (::TupleID*)oidB, colNoB);
}
Four OOSQL_StorageManagerLOM::CollectionSet_Scan_Open(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionSet_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

	return LOM_CollectionSet_Scan_Open(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
}
Four OOSQL_StorageManagerLOM::CollectionSet_Scan_Close(Four CollectionScanId)
{
	return LOM_CollectionSet_Scan_Close(m_systemHandle, CollectionScanId);
}
Four OOSQL_StorageManagerLOM::CollectionSet_Scan_NextElements(Four CollectionScanId, Four nElements, Four* elementSizes, Four sizeOfElements, void* elements)
{
	return LOM_CollectionSet_Scan_NextElements(m_systemHandle, CollectionScanId, nElements, elementSizes, sizeOfElements, elements);
}
Four OOSQL_StorageManagerLOM::CollectionSet_Scan_GetSizeOfNextElements(Four CollectionScanId, Four nElements, Four* elementsSize)
{
	return LOM_CollectionSet_Scan_GetSizeOfNextElements(m_systemHandle, CollectionScanId, nElements, elementsSize);
}
Four OOSQL_StorageManagerLOM::CollectionSet_Scan_InsertElements(Four CollectionScanId, Four nElements, Four* elementSizes, void* elements)
{
	return LOM_CollectionSet_Scan_InsertElements(m_systemHandle, CollectionScanId, nElements, elementSizes, elements);
}
Four OOSQL_StorageManagerLOM::CollectionSet_Scan_DeleteElements(Four CollectionScanId)
{
	return LOM_CollectionSet_Scan_DeleteElements(m_systemHandle, CollectionScanId);
}

Four OOSQL_StorageManagerLOM::CollectionBag_Create(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four keySize)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionBag_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_FALSE) OOSQL_ERROR(m_systemHandle, eNOTNULLCOLLECTION_OOSQL);

	return LOM_CollectionBag_Create(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo), keySize);
}
Four OOSQL_StorageManagerLOM::CollectionBag_Destroy(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionBag_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

    return LOM_CollectionBag_Destroy(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
}
Four OOSQL_StorageManagerLOM::CollectionBag_AssignElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four nElements, Four* elementSizes, void* elements)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionBag_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

	return LOM_CollectionBag_AssignElements(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo), nElements, elementSizes, elements);
}
Four OOSQL_StorageManagerLOM::CollectionBag_Assign(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four assignedOrnOrScanId, Boolean assignedUseScanFlag, OID* assignedOid, Four assignedColNo)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionBag_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

	return LOM_CollectionBag_Assign(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo), assignedOrnOrScanId, assignedUseScanFlag, (::TupleID*)assignedOid, assignedColNo);
}
Four OOSQL_StorageManagerLOM::CollectionBag_GetN_Elements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four* nElements)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionBag_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

	return LOM_CollectionBag_GetN_Elements(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo), nElements);
}
Four OOSQL_StorageManagerLOM::CollectionBag_InsertElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four nElements, Four* elementSizes, void* elements)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionBag_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

	return LOM_CollectionBag_InsertElements(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo), nElements, elementSizes, elements);
}
Four OOSQL_StorageManagerLOM::CollectionBag_DeleteElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four nElements, Four* elementSizes, void* elements)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionBag_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

	return LOM_CollectionBag_DeleteElements(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo), nElements, elementSizes, elements);
}
Four OOSQL_StorageManagerLOM::CollectionBag_DeleteAll(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionBag_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

	return LOM_CollectionBag_DeleteAll(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
}
Four OOSQL_StorageManagerLOM::CollectionBag_IsMember(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four elementSize, void* element)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionBag_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

	return LOM_CollectionBag_IsMember(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo), elementSize, element);
}
Four OOSQL_StorageManagerLOM::CollectionBag_IsNull(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo)
{
	return LOM_CollectionBag_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
}
Four OOSQL_StorageManagerLOM::CollectionBag_IsEqual(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four comparedOrnOrScanId, Boolean comparedUseScanFlag, OID* comparedOid, Four comparedColNo)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionBag_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

	return LOM_CollectionBag_IsEqual(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo), comparedOrnOrScanId, comparedUseScanFlag, (::TupleID*)comparedOid, comparedColNo);
}
Four OOSQL_StorageManagerLOM::CollectionBag_IsSubset(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four comparedOrnOrScanId, Boolean comparedUseScanFlag, OID* comparedOid, Four comparedColNo)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionBag_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

	return LOM_CollectionBag_IsSubset(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo), comparedOrnOrScanId, comparedUseScanFlag, (::TupleID*)comparedOid, comparedColNo);
}
Four OOSQL_StorageManagerLOM::CollectionBag_GetSizeOfElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four ith, Four nElements, Four* elementsSize)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionBag_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

	return LOM_CollectionBag_GetSizeOfElements(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo), ith, nElements, elementsSize);
}
Four OOSQL_StorageManagerLOM::CollectionBag_RetrieveElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four ith, Four nElements, Four* elementSizes, Four sizeOfElements, void* elements)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionBag_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

	return LOM_CollectionBag_RetrieveElements(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo), ith, nElements, elementSizes, sizeOfElements, elements);
}
Four OOSQL_StorageManagerLOM::CollectionBag_Union(Four ornOrScanIdA, Boolean useScanFlagA, OID* oidA, Four colNoA, Four ornOrScanIdB, Boolean useScanFlagB, OID* oidB, Four colNoB, Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionBag_IsNull(m_systemHandle, ornOrScanIdA, useScanFlagA, (::TupleID*)oidA, GET_USERLEVEL_COLNO(colNoA));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

    isNull = (Boolean)LOM_CollectionBag_IsNull(m_systemHandle, ornOrScanIdB, useScanFlagB, (::TupleID*)oidB, GET_USERLEVEL_COLNO(colNoB));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

    isNull = (Boolean)LOM_CollectionBag_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);
    
    return LOM_CollectionBag_Union(m_systemHandle, ornOrScanIdA, useScanFlagA, (::TupleID*)oidA, colNoA, ornOrScanIdB, useScanFlagB, (::TupleID*)oidB, colNoB, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
}
Four OOSQL_StorageManagerLOM::CollectionBag_Intersect(Four ornOrScanIdA, Boolean useScanFlagA, OID* oidA, Four colNoA, Four ornOrScanIdB, Boolean useScanFlagB, OID* oidB, Four colNoB, Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionBag_IsNull(m_systemHandle, ornOrScanIdA, useScanFlagA, (::TupleID*)oidA, GET_USERLEVEL_COLNO(colNoA));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

    isNull = (Boolean)LOM_CollectionBag_IsNull(m_systemHandle, ornOrScanIdB, useScanFlagB, (::TupleID*)oidB, GET_USERLEVEL_COLNO(colNoB));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

    isNull = (Boolean)LOM_CollectionBag_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

    return LOM_CollectionBag_Intersect(m_systemHandle, ornOrScanIdA, useScanFlagA, (::TupleID*)oidA, colNoA, ornOrScanIdB, useScanFlagB, (::TupleID*)oidB, colNoB, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
}
Four OOSQL_StorageManagerLOM::CollectionBag_Difference(Four ornOrScanIdA, Boolean useScanFlagA, OID* oidA, Four colNoA, Four ornOrScanIdB, Boolean useScanFlagB, OID* oidB, Four colNoB, Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionBag_IsNull(m_systemHandle, ornOrScanIdA, useScanFlagA, (::TupleID*)oidA, GET_USERLEVEL_COLNO(colNoA));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

    isNull = (Boolean)LOM_CollectionBag_IsNull(m_systemHandle, ornOrScanIdB, useScanFlagB, (::TupleID*)oidB, GET_USERLEVEL_COLNO(colNoB));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

    isNull = (Boolean)LOM_CollectionBag_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

    return LOM_CollectionBag_Difference(m_systemHandle, ornOrScanIdA, useScanFlagA, (::TupleID*)oidA, colNoA, ornOrScanIdB, useScanFlagB, (::TupleID*)oidB, colNoB, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
}
Four OOSQL_StorageManagerLOM::CollectionBag_UnionWith(Four ornOrScanIdA, Boolean useScanFlagA, OID* oidA, Four colNoA, Four ornOrScanIdB, Boolean useScanFlagB, OID* oidB, Four colNoB)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionBag_IsNull(m_systemHandle, ornOrScanIdA, useScanFlagA, (::TupleID*)oidA, GET_USERLEVEL_COLNO(colNoA));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

    isNull = (Boolean)LOM_CollectionBag_IsNull(m_systemHandle, ornOrScanIdB, useScanFlagB, (::TupleID*)oidB, GET_USERLEVEL_COLNO(colNoB));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

	return LOM_CollectionBag_UnionWith(m_systemHandle, ornOrScanIdA, useScanFlagA, (::TupleID*)oidA, colNoA, ornOrScanIdB, useScanFlagB, (::TupleID*)oidB, colNoB);
}
Four OOSQL_StorageManagerLOM::CollectionBag_IntersectWith(Four ornOrScanIdA, Boolean useScanFlagA, OID* oidA, Four colNoA, Four ornOrScanIdB, Boolean useScanFlagB, OID* oidB, Four colNoB)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionBag_IsNull(m_systemHandle, ornOrScanIdA, useScanFlagA, (::TupleID*)oidA, GET_USERLEVEL_COLNO(colNoA));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

    isNull = (Boolean)LOM_CollectionBag_IsNull(m_systemHandle, ornOrScanIdB, useScanFlagB, (::TupleID*)oidB, GET_USERLEVEL_COLNO(colNoB));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

    return LOM_CollectionBag_IntersectWith(m_systemHandle, ornOrScanIdA, useScanFlagA, (::TupleID*)oidA, colNoA, ornOrScanIdB, useScanFlagB, (::TupleID*)oidB, colNoB);
}
Four OOSQL_StorageManagerLOM::CollectionBag_DifferenceWith(Four ornOrScanIdA, Boolean useScanFlagA, OID* oidA, Four colNoA, Four ornOrScanIdB, Boolean useScanFlagB, OID* oidB, Four colNoB)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionBag_IsNull(m_systemHandle, ornOrScanIdA, useScanFlagA, (::TupleID*)oidA, GET_USERLEVEL_COLNO(colNoA));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

    isNull = (Boolean)LOM_CollectionBag_IsNull(m_systemHandle, ornOrScanIdB, useScanFlagB, (::TupleID*)oidB, GET_USERLEVEL_COLNO(colNoB));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

    return LOM_CollectionBag_DifferenceWith(m_systemHandle, ornOrScanIdA, useScanFlagA, (::TupleID*)oidA, colNoA, ornOrScanIdB, useScanFlagB, (::TupleID*)oidB, colNoB);
}
Four OOSQL_StorageManagerLOM::CollectionBag_Scan_Open(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionBag_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

	return LOM_CollectionBag_Scan_Open(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
}
Four OOSQL_StorageManagerLOM::CollectionBag_Scan_Close(Four CollectionScanId)
{
	return LOM_CollectionBag_Scan_Close(m_systemHandle, CollectionScanId);
}
Four OOSQL_StorageManagerLOM::CollectionBag_Scan_NextElements(Four CollectionScanId, Four nElements, Four* elementSizes, Four sizeOfElements, void* elements)
{
	return LOM_CollectionBag_Scan_NextElements(m_systemHandle, CollectionScanId, nElements, elementSizes, sizeOfElements, elements);
}
Four OOSQL_StorageManagerLOM::CollectionBag_Scan_GetSizeOfNextElements(Four CollectionScanId, Four nElements, Four* elementsSize)
{
	return LOM_CollectionBag_Scan_GetSizeOfNextElements(m_systemHandle, CollectionScanId, nElements, elementsSize);
}
Four OOSQL_StorageManagerLOM::CollectionBag_Scan_InsertElements(Four CollectionScanId, Four nElements, Four* elementSizes, void* elements)
{
	return LOM_CollectionBag_Scan_InsertElements(m_systemHandle, CollectionScanId, nElements, elementSizes, elements);
}
Four OOSQL_StorageManagerLOM::CollectionBag_Scan_DeleteElements(Four CollectionScanId)
{
	return LOM_CollectionBag_Scan_DeleteElements(m_systemHandle, CollectionScanId);
}

Four OOSQL_StorageManagerLOM::CollectionList_Create(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionList_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_FALSE) OOSQL_ERROR(m_systemHandle, eNOTNULLCOLLECTION_OOSQL);

	return LOM_CollectionList_Create(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
}
Four OOSQL_StorageManagerLOM::CollectionList_Destroy(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionList_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

	return LOM_CollectionList_Destroy(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
}
Four OOSQL_StorageManagerLOM::CollectionList_GetN_Elements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four* nElements)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionList_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

	return LOM_CollectionList_GetN_Elements(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo), nElements);
}
Four OOSQL_StorageManagerLOM::CollectionList_AssignElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four nElements, Four* elementSizes, void* elements)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionList_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

	return LOM_CollectionList_AssignElements(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo), nElements, elementSizes, elements);
}
Four OOSQL_StorageManagerLOM::CollectionList_Assign(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four assignedOrnOrScanId, Boolean assignedUseScanFlag, OID* assignedOid, Four assignedColNo)
{ 
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionList_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

	return LOM_CollectionList_Assign(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo), assignedOrnOrScanId, assignedUseScanFlag, (::TupleID*)assignedOid, assignedColNo);
}
Four OOSQL_StorageManagerLOM::CollectionList_InsertElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four ith, Four nElements, Four* elementSizes, void* elements)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionList_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

	return LOM_CollectionList_InsertElements(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo), ith, nElements, elementSizes, elements);
}
Four OOSQL_StorageManagerLOM::CollectionList_DeleteElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four ith, Four nElements)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionList_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

	return LOM_CollectionList_DeleteElements(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo), ith, nElements);
}
Four OOSQL_StorageManagerLOM::CollectionList_DeleteAll(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionList_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

	return LOM_CollectionList_DeleteAll(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
}
Four OOSQL_StorageManagerLOM::CollectionList_AppendElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four nElements, Four* elementSizes, void* elements)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionList_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

	return LOM_CollectionList_AppendElements(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo), nElements, elementSizes, elements);
}
Four OOSQL_StorageManagerLOM::CollectionList_GetSizeOfElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four ith, Four nElements, Four* elementsSize)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionList_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

	return LOM_CollectionList_GetSizeOfElements(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo), ith, nElements, elementsSize);
}
Four OOSQL_StorageManagerLOM::CollectionList_RetrieveElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four ith, Four nElements, Four* elementSizes, Four sizeOfElements, void* elements)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionList_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

	return LOM_CollectionList_RetrieveElements(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo), ith, nElements, elementSizes, sizeOfElements, elements);
}
Four OOSQL_StorageManagerLOM::CollectionList_UpdateElements(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four ith, Four nElements, Four* elementSizes, void* elements)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionList_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

	return LOM_CollectionList_UpdateElements(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo), ith, nElements, elementSizes, elements);
}
Four OOSQL_StorageManagerLOM::CollectionList_Concatenate(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four concatnatedOrnOrScanId, Boolean concatnatedUseScanFlag, OID* concatnatedOid, Four concatnatedColNo)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionList_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

	return LOM_CollectionList_Concatenate(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo), concatnatedOrnOrScanId, concatnatedUseScanFlag, (::TupleID*)concatnatedOid, concatnatedColNo);
}
Four OOSQL_StorageManagerLOM::CollectionList_Resize(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four size)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionList_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

	return LOM_CollectionList_Resize(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo), size);
}
Four OOSQL_StorageManagerLOM::CollectionList_IsMember(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four elementSize, void* element, Four* pos)
{ 
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionList_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

	return LOM_CollectionList_IsMember(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo), elementSize, element, pos);
}
Four OOSQL_StorageManagerLOM::CollectionList_IsNull(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo)
{ 
	return LOM_CollectionList_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
}
Four OOSQL_StorageManagerLOM::CollectionList_IsEqual(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo, Four comparedOrnOrScanId, Boolean comparedUseScanFlag, OID* comparedOid, Four comparedColNo)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionList_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

	return LOM_CollectionList_IsEqual(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo), comparedOrnOrScanId, comparedUseScanFlag, (::TupleID*)comparedOid, comparedColNo);
}
Four OOSQL_StorageManagerLOM::CollectionList_Scan_Open(Four ornOrScanId, Boolean useScanFlag, OID* oid, Four colNo)
{
    Boolean isNull;

    isNull = (Boolean)LOM_CollectionList_IsNull(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
    if (isNull == SM_TRUE) OOSQL_ERROR(m_systemHandle, eNULLCOLLECTION_OOSQL);

	return LOM_CollectionList_Scan_Open(m_systemHandle, ornOrScanId, useScanFlag, (::TupleID*)oid, GET_USERLEVEL_COLNO(colNo));
}
Four OOSQL_StorageManagerLOM::CollectionList_Scan_Close(Four CollectionScanId)
{
	return LOM_CollectionList_Scan_Close(m_systemHandle, CollectionScanId);
}
Four OOSQL_StorageManagerLOM::CollectionList_Scan_NextElements(Four CollectionScanId, Four nElements, Four* elementSizes, Four sizeOfElements, void* elements)
{
	return LOM_CollectionList_Scan_NextElements(m_systemHandle, CollectionScanId, nElements, elementSizes, sizeOfElements, elements);
}
Four OOSQL_StorageManagerLOM::CollectionList_Scan_GetSizeOfNextElements(Four CollectionScanId, Four nElements, Four* elementsSize)
{
	return LOM_CollectionList_Scan_GetSizeOfNextElements(m_systemHandle, CollectionScanId, nElements, elementsSize);
}
Four OOSQL_StorageManagerLOM::CollectionList_Scan_InsertElements(Four CollectionScanId, Four nElements, Four* elementSizes, void* elements)
{
	return LOM_CollectionList_Scan_InsertElements(m_systemHandle, CollectionScanId, nElements, elementSizes, elements);
}
Four OOSQL_StorageManagerLOM::CollectionList_Scan_DeleteElements(Four CollectionScanId)
{
	return LOM_CollectionList_Scan_DeleteElements(m_systemHandle, CollectionScanId);
}
Four OOSQL_StorageManagerLOM::OpenSortStream(VolID volId, SortTupleDesc* sortTupleDesc)
{
	return LOM_OpenSortStream(m_systemHandle, volId, (::SortTupleDesc*)sortTupleDesc);
}
Four OOSQL_StorageManagerLOM::CloseSortStream(Four streamId)
{
	return LOM_CloseSortStream(m_systemHandle, streamId);
}
Four OOSQL_StorageManagerLOM::SortingSortStream(Four streamId)
{
	return LOM_SortingSortStream(m_systemHandle, streamId);
}
Four OOSQL_StorageManagerLOM::PutTuplesIntoSortStream(Four streamId, Four numTuples, SortStreamTuple* sortStreamTuples)
{
	return LOM_PutTuplesIntoSortStream(m_systemHandle, streamId, numTuples, (::SortStreamTuple*)sortStreamTuples);
}
Four OOSQL_StorageManagerLOM::GetTuplesFromSortStream(Four streamId, Four* numTuples, SortStreamTuple* sortStreamTuples, Boolean* done)
{
	return LOM_GetTuplesFromSortStream(m_systemHandle, streamId, numTuples, (::SortStreamTuple*)sortStreamTuples, done);
}
Four OOSQL_StorageManagerLOM::OpenStream(VolID volId)
{
	return LOM_OpenStream(m_systemHandle, volId);
}
Four OOSQL_StorageManagerLOM::CloseStream(Four streamId)
{
	return LOM_CloseStream(m_systemHandle, streamId);
}
Four OOSQL_StorageManagerLOM::ChangePhaseStream(Four streamId)
{
	return LOM_ChangePhaseStream(m_systemHandle, streamId);
}
Four OOSQL_StorageManagerLOM::PutTuplesIntoStream(Four streamId, Four numTuples, SortStreamTuple* sortStreamTuples)
{
	return LOM_PutTuplesIntoStream(m_systemHandle, streamId, numTuples, (::SortStreamTuple*)sortStreamTuples);
}
Four OOSQL_StorageManagerLOM::GetTuplesFromStream(Four streamId, Four* numTuples, SortStreamTuple* sortStreamTuples, Boolean* done)
{
	return LOM_GetTuplesFromStream(m_systemHandle, streamId, numTuples, (::SortStreamTuple*)sortStreamTuples, done);
}
