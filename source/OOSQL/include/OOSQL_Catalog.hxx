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

#ifndef __OQL_CATALOG_H__
#define __OQL_CATALOG_H__

#include "OOSQL_Common.h"
#include "OOSQL_StorageManager.hxx"
#include "OOSQL_MemoryManager.hxx"
#include "OOSQL_MemoryManagedObject.hxx"

enum ParameterMode {PARM_IN, PARM_OUT, PARM_INOUT};

class OOSQL_Catalog : public OOSQL_MemoryManagedObject {
public:
    OOSQL_Catalog(LOM_Handle* systemHandle, Four volId);
    virtual ~OOSQL_Catalog() {}

    Four class_ClassInfo_to_ClassId(CataClassInfo classInfo, Four& classId);
    Four class_ClassId_to_ClassInfo(Four classId, CataClassInfo& classInfo);
    Four class_GetN_Attrs(CataClassInfo classInfo, Four &nAttrs);
	Four class_GetN_AttrsIncludingDroppedCol(CataClassInfo classInfo, Four &nAttrs);
    Four class_GetN_Methods(CataClassInfo classInfo, Four &nMethods);
    Four class_GetSubClasses(CataClassInfo classInfo, Four fromNthSubClass, Four sizeOfSubClasses, CataClassInfo* subClasses);
    Four class_ClassName_to_CataClassInfo(char* className, CataClassInfo& classInfo);
	Four class_CataClassInfo_to_ClassName(CataClassInfo classInfo, char* className);
	Four class_IsSpatialClass(CataClassInfo classInfo);
	Four class_IsLrdsRelation(CataClassInfo classInfo);

	Four attr_ColNo_to_ColNoIncludingDroppedCol(CataClassInfo classInfo, Two colNo, Two &colNoIncludingDroppedCol);
    Four attr_AttrInfo_to_ColNo(CataClassInfo classInfo, CataAttrInfo attrInfo, Two &colNo);
    Four attr_ColNo_to_AttrInfo(CataClassInfo classInfo, Two colNo, CataAttrInfo &attrInfo);
	Four attr_GetComplexType(CataClassInfo classInfo, CataAttrInfo attrInfo, Four& complexType);
    Four attr_GetTypeID(CataClassInfo classInfo, CataAttrInfo attrInfo, TypeID &typeID);
    Four attr_GetLength(CataClassInfo classInfo, CataAttrInfo attrInfo, Four &length);
	Four attr_GetInheritedFrom(CataClassInfo classInfo, CataAttrInfo attrInfo, Four &inheritedFrom);
    Four attr_GetReferencingClass(CataClassInfo classInfo, CataAttrInfo attrInfo, CataClassInfo &CataClassInfo);
    Four attr_AttrName_to_CataAttrInfo(CataClassInfo classInfo, char* attrName, CataAttrInfo& attrInfo);
	Four attr_CataAttrInfo_to_AttrName(CataClassInfo classInfo, CataAttrInfo attrInfo, char* attrName);
    
    Four method_MethodInfo_to_MethodNo(CataClassInfo classInfo, CataMethodInfo methodInfo, Two &methodNo);
	Four method_MethodNo_to_MethodInfo(CataClassInfo classInfo, Two methodNo, CataMethodInfo& methodInfo);
	Four method_CataMethodInfo_to_MethodName(CataClassInfo classInfo, CataMethodInfo methodInfo, char* methodName);
    Four method_GetReturnTypeID(CataClassInfo classInfo, CataMethodInfo methodInfo, TypeID &typeID);
    Four method_GetReturnTypeLength(CataClassInfo classInfo, CataMethodInfo methodInfo, Four &length);
	Four method_GetReturnTypeName(CataClassInfo classInfo, CataMethodInfo methodInfo, char* typeName);
	Four method_GetReturnParameterMode(CataClassInfo classInfo, CataMethodInfo methodInfo, ParameterMode& parameterMode);
	Four method_GetReturnAsLocatorFlag(CataClassInfo classInfo, CataMethodInfo methodInfo, Boolean& asLocator);
    Four method_GetPtr(CataClassInfo classInfo, CataMethodInfo methodInfo, void* &ptr);
	Four method_GetExternalName(CataClassInfo classInfo, CataMethodInfo methodInfo, char* externalName);
    Four method_GetReferencingClass(CataClassInfo classInfo, CataMethodInfo methodInfo, CataClassInfo& CataClassInfo);
    Four method_GetN_Arguments(CataClassInfo classInfo, CataMethodInfo methodInfo, Four &nArguments);
    Four method_GetNTH_ArgumentTypeID(CataClassInfo classInfo, CataMethodInfo methodInfo, Four nth, TypeID &typeId);
	Four method_GetNTH_ArgumentTypeName(CataClassInfo classInfo, CataMethodInfo methodInfo, Four nth, char* typeName);
	Four method_GetNTH_ArgumentName(CataClassInfo classInfo, CataMethodInfo methodInfo, Four nth, char* argumentName);
	Four method_GetNTH_ArgumentAsLocatorFlag(CataClassInfo classInfo, CataMethodInfo methodInfo, Four nth, Boolean& asLocatorFlag);
	Four method_GetNTH_ArgumentParameterMode(CataClassInfo classInfo, CataMethodInfo methodInfo, Four nth, ParameterMode& parameterMode);
    Four method_MethodName_to_CataMethodInfo(CataClassInfo classInfo, char* methodName, CataMethodInfo& methodInfo);
	Four method_GetInheritedFrom(CataClassInfo classInfo, CataMethodInfo methodInfo, Four &inheritedFrom);
	Four method_GetDeterinisticFlag(CataClassInfo classInfo, CataMethodInfo methodInfo, Boolean& deterministicFlag);
	Four method_GetExternalActionFlag(CataClassInfo classInfo, CataMethodInfo methodInfo, Boolean& externalActionFlag);
	Four method_GetFencedFlag(CataClassInfo classInfo, CataMethodInfo methodInfo, Boolean& fencdedFlag);
	Four method_GetNullCallFlag(CataClassInfo classInfo, CataMethodInfo methodInfo, Boolean& nullCallFlag);
	Four method_GetScratchPadFlag(CataClassInfo classInfo, CataMethodInfo methodInfo, Boolean& scratchPadFlag);
	Four method_GetFinalCallFlag(CataClassInfo classInfo, CataMethodInfo methodInfo, Boolean& finalCallFlag);
	Four method_GetAllowParallelFlag(CataClassInfo classInfo, CataMethodInfo methodInfo, Boolean& allowParallelFlag);
	Four method_GetDbInfoFlag(CataClassInfo classInfo, CataMethodInfo methodInfo, Boolean& dbInfoFlag);
	Four method_GetSpecificName(CataClassInfo classInfo, CataMethodInfo methodInfo, char* specificName);
	Four method_GetLanguage(CataClassInfo classInfo, CataMethodInfo methodInfo, char* language);
	Four method_GetParameterStyle(CataClassInfo classInfo, CataMethodInfo methodInfo, char* parameterStyle);
    
    Four index_isB_TreeIndexExist(CataClassInfo classInfo, CataAttrInfo attrInfo);
    Four index_getB_TreeIndexes(CataClassInfo classInfo, CataAttrInfo attrInfo, Four &nIndexes, OOSQL_StorageManager::IndexID indexes[]);
    Four index_isTextIndexExist(CataClassInfo classInfo, CataAttrInfo attrInfo);
    Four index_getTextIndexes(CataClassInfo classInfo, CataAttrInfo attrInfo, Four &nIndexes, OOSQL_StorageManager::IndexID indexes[]);
    Four index_isMlgfIndexExist(CataClassInfo classInfo, CataAttrInfo attrInfo);
    Four index_getMlgfIndexes(CataClassInfo classInfo, CataAttrInfo attrInfo, Four &nIndexes, OOSQL_StorageManager::IndexID indexes[]);
    
	Four index_GetIndexInfos(CataClassInfo classInfo, CataAttrInfo attrInfo, Four& nIndexes, CataIndexInfo* indexInfos);
	Four index_IndexInfo_to_IndexId(CataClassInfo classInfo, CataIndexInfo indexInfo, OOSQL_StorageManager::IndexID& indexId);
	Four index_IndexId_to_IndexInfo(CataClassInfo classInfo, OOSQL_StorageManager::IndexID indexId, CataIndexInfo& indexInfo);
	Four index_GetIndexDesc(CataClassInfo classInfo, CataIndexInfo indexInfo, OOSQL_StorageManager::KeyDesc& btreeKeyDesc);
	Four index_GetIndexDesc(CataClassInfo classInfo, CataIndexInfo indexInfo, OOSQL_StorageManager::MLGF_KeyDesc& mlgfKeyDesc);
	Four index_GetIndexDesc(CataClassInfo classInfo, CataIndexInfo indexInfo, OOSQL_StorageManager::InvertedIndexDesc& invertedIndexKeyDesc);
	Four index_GetIndexType(CataClassInfo classInfo, CataIndexInfo indexInfo, One& indexType);
	Four index_GetIndexName(CataClassInfo classInfo, CataIndexInfo indexInfo, char* indexName);
	Four index_GetIndexColumns(CataClassInfo classInfo, CataIndexInfo indexInfo, Two colNo[MAXNUMKEYPARTS]);

	Four textindex_GetN_PostingsOfKeyword(CataClassInfo classInfo, CataClassInfo attrInfo, CataIndexInfo indexInfo, char* keyword, Four& nPostings);
	Four textindex_KeywordScan_Open(CataClassInfo classInfo, CataClassInfo attrInfo, CataIndexInfo indexInfo, Four keywordKind, Two startBoundLength, char* startBoundString, Two stopBoundLength, char* stopBoundString);
	Four textindex_KeywordScan_Close(Four scanId);
	Four textindex_KeywordScan_Next(Four scanId, Four keywordKind, char* keyword, Four& nPostings, OOSQL_StorageManager::TupleID& invertedIndexEntryTupleID);

	Four getVolumeID() { return m_volId; }

private:
    Four        m_volId;  // volume id
    Four        m_mv;     // mount volume
	LOM_Handle*	m_systemHandle;
private:
    Four traverse_subclass_tree(CataClassInfo classInfo, Four& index, Four startIndex, 
                                Four endIndex, CataClassInfo*  subClasses); 

};

#endif  /* __OQL_CATALOG_H__ */

