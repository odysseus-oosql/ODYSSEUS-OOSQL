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
#include "OOSQL_Catalog.hxx"

#include <string.h>

static TypeID lomType_to_TypeID(Four typeID)
{
    switch(typeID)
    {
    case LOM_SHORT:
        return TYPEID_SHORT;

    case LOM_INT:
        return TYPEID_INT;

    case LOM_LONG:    
        return TYPEID_LONG;

    case LOM_LONG_LONG:    
        return TYPEID_LONG_LONG;

    case LOM_FLOAT:   
        return TYPEID_FLOAT;

    case LOM_DOUBLE:  
        return TYPEID_DOUBLE;

    case LOM_STRING:  
        return TYPEID_STRING;

    case LOM_VARSTRING: 
        return TYPEID_VARSTRING;

    case LOM_PAGEID:    
        return TYPEID_PAGEID;

    case LOM_FILEID:    
        return TYPEID_FILEID;

    case LOM_INDEXID:   
        return TYPEID_INDEXID;

    case LOM_OID:          
        return TYPEID_OID;

    case LOM_TEXT: 
        return TYPEID_TEXT;

    case LOM_MBR:
        return TYPEID_MBR;

	case SM_TEXT:
		return TYPEID_VARSTRING;

	case LOM_DATE:
		return TYPEID_DATE;

	case LOM_TIME:
		return TYPEID_TIME;

	case LOM_TIMESTAMP:
		return TYPEID_TIMESTAMP;

	case LOM_INTERVAL:
		return TYPEID_INTERVAL;

	case LOM_OGIS_GEOMETRY:
		return TYPEID_OGIS_GEOMETRY;

	case LOM_OGIS_POINT:
		return TYPEID_OGIS_POINT;

	case LOM_OGIS_LINESTRING:
		return TYPEID_OGIS_LINESTRING;

	case LOM_OGIS_POLYGON:
		return TYPEID_OGIS_POLYGON;

	case LOM_OGIS_GEOMETRYCOLLECTION:
		return TYPEID_OGIS_GEOMETRYCOLLECTION;

	case LOM_OGIS_MULTIPOINT:
		return TYPEID_OGIS_MULTIPOINT;

	case LOM_OGIS_MULTILINESTRING:
		return TYPEID_OGIS_MULTILINESTRING;

	case LOM_OGIS_MULTIPOLYGON:
		return TYPEID_OGIS_MULTIPOLYGON;

    default:
        return TYPEID_NONE;
        break;
    }
}

// CataClassInfo  : Four type. used as a classInfoTblIndex
// CataAttrInfo   : Four type, used as a attrInfoTblIndex
// CataMethodInfo : Four type. used as a methodInfoTblIndex

#ifndef MAX
#define MAX(x, y) (((x) > (y))?(x):(y))
#endif
#ifndef MIN
#define MIN(x, y) (((x) < (y))?(x):(y))
#endif

// Constructor
OOSQL_Catalog::OOSQL_Catalog(LOM_Handle* systemHandle, Four avolId)
{
    Four    e;

    // initialize m_volId, mountvolume
    m_volId        = avolId;
	m_systemHandle = systemHandle;

    e = Catalog_GetMountTableInfo(m_systemHandle, m_volId, &m_mv);
	if(e < eNOERROR)
		OOSQL_ERR_EXIT(e);
}

Four OOSQL_Catalog::class_ClassInfo_to_ClassId(CataClassInfo classInfo, Four& classId)
{
    classId = CATALOG_GET_CLASSID(&(CATALOG_GET_CLASSINFOTBL(m_systemHandle, m_mv)[classInfo]));
    return eNOERROR;
}

Four OOSQL_Catalog::class_ClassId_to_ClassInfo(Four classId, CataClassInfo& classInfo)
{
    Four    tblIndex;
    Four    e;

    // get class info tbl index
    e = Catalog_GetClassInfo(m_systemHandle, m_volId, classId, &tblIndex);
    OOSQL_CHECK_ERR(e);

    classInfo = tblIndex;
    return eNOERROR;
}

Four OOSQL_Catalog::class_ClassName_to_CataClassInfo(char* className, CataClassInfo& classInfo)
{

    Four    e;          // error code
    Four    classId;    // class id
    Four    tblIndex;   // table indexbre

    // convert class name to class id
    e = LOM_GetClassID(m_systemHandle, m_volId, className, &classId);
    if(e == eNOSUCHCLASS_LOM)
	{
        e = Catalog_GetTemporaryClassId(m_systemHandle, m_volId, className, &classId);
        if(e == eNOSUCHCLASSFOUND_CATALOG)
            return eNOT_FOUND_OOSQL;
	}
    else
        OOSQL_CHECK_ERR(e);
        
    // get class info tbl index
    e = Catalog_GetClassInfo(m_systemHandle, m_volId, classId, &tblIndex);
    OOSQL_CHECK_ERR(e);
    
    classInfo = tblIndex;
    return eNOERROR;
}

Four OOSQL_Catalog::class_CataClassInfo_to_ClassName(CataClassInfo classInfo, char* className)
{
	char*	name;

	name = CATALOG_GET_CLASSNAME(&(CATALOG_GET_CLASSINFOTBL(m_systemHandle, m_mv)[classInfo]));
	strcpy(className, name);

	return eNOERROR;
}

Four OOSQL_Catalog::traverse_subclass_tree(
    CataClassInfo   classInfo,          // root class 
    Four&           index,              // sub class index
    Four            startIndex,         // start index
    Four            endIndex,           // end index
    CataClassInfo*  subClasses          // subclass array
)
{
    Four          i;    
    Four          e;                    // error code
    Four          nSubClasses;          // number of subclasses of this class
    Four          subClassInfoIndex;    // sub class info table index
    CataClassInfo subClassInfo;         // sub class info
    Four          tblIndex;     
    Four          subClassId;           // sub class id

    subClassInfoIndex = CATALOG_GET_SUBCLASSINFOINDEX(&(CATALOG_GET_CLASSINFOTBL(m_systemHandle, m_mv)[classInfo]));
    nSubClasses       = CATALOG_GET_SUBCLASSNUM(&(CATALOG_GET_CLASSINFOTBL(m_systemHandle, m_mv)[classInfo]));

    for(i = 0;i < nSubClasses; i++)
    {
        subClassId = (CATALOG_GET_SUBCLASSINFOTBL(m_systemHandle, m_mv)[subClassInfoIndex + i]).subClassId;

        // convert class id to class info
        e = Catalog_GetClassInfo(m_systemHandle, m_volId, subClassId, &tblIndex);
        OOSQL_CHECK_ERR(e);

        subClassInfo = tblIndex;

        // if index is between start index and end index, then add to array 
        if(startIndex <= index && index <= endIndex)
        {
            subClasses[index - startIndex] = subClassInfo;
            index ++;
        }

        e = traverse_subclass_tree(subClassInfo, index, startIndex, endIndex, subClasses);
        OOSQL_CHECK_ERR(e);
    }

    return eNOERROR;
}

Four OOSQL_Catalog::class_GetSubClasses(
    CataClassInfo   classInfo,          // IN  : class
    Four            fromNthSubClass,    // IN  : 
    Four            sizeOfSubClasses,   // IN  : size of array which will contain subclasses of the class
    CataClassInfo*  subClasses          // OUT : array of sub classes
)
{
    Four          index;
    Four          e;

    index = 0;
    e = traverse_subclass_tree(classInfo, index, fromNthSubClass, 
                               fromNthSubClass + sizeOfSubClasses - 1, subClasses);
    OOSQL_CHECK_ERR(e);

    return index;
}

Four OOSQL_Catalog::class_GetN_Attrs(CataClassInfo classInfo, Four& nAttrs)
{
    // validate classInfo
	nAttrs = CATALOG_GET_ACTIVEATTRNUM(&(CATALOG_GET_CLASSINFOTBL(m_systemHandle, m_mv)[classInfo]));

    return eNOERROR;
}

Four OOSQL_Catalog::class_GetN_AttrsIncludingDroppedCol(CataClassInfo classInfo, Four& nAttrs)
{
    // validate classInfo
    nAttrs = CATALOG_GET_ATTRNUM(&(CATALOG_GET_CLASSINFOTBL(m_systemHandle, m_mv)[classInfo]));

    return eNOERROR;
}


Four OOSQL_Catalog::class_GetN_Methods(CataClassInfo classInfo, Four& nMethods)
{
    // validate classInfo
    nMethods = CATALOG_GET_METHODNUM(&(CATALOG_GET_CLASSINFOTBL(m_systemHandle, m_mv)[classInfo]));
    return eNOERROR;

}

Four OOSQL_Catalog::attr_GetLength(CataClassInfo classInfo, CataAttrInfo attrInfo, Four &length)
{
	Four	e;
	TypeID	typeID;

    // validate classInfo, attrInfo
	e = attr_GetTypeID(classInfo, attrInfo, typeID);
	OOSQL_CHECK_ERR(e);

	length = CATALOG_GET_ATTRLENGTH(&(CATALOG_GET_ATTRINFOTBL(m_systemHandle, m_mv)[attrInfo]));

    return eNOERROR;
}

Four OOSQL_Catalog::attr_GetInheritedFrom(CataClassInfo classInfo, CataAttrInfo attrInfo, Four &inheritedFrom)
{
	inheritedFrom = CATALOG_GET_ATTRINHERITEDFROM(&(CATALOG_GET_ATTRINFOTBL(m_systemHandle, m_mv)[attrInfo]));
    return eNOERROR;
}

Four OOSQL_Catalog::attr_GetComplexType(CataClassInfo classInfo, CataAttrInfo attrInfo, Four& complexType)
{
    // validate classInfo, attrInfo

    // get type
	complexType = CATALOG_GET_ATTRCOMPLEXTYPE(&(CATALOG_GET_ATTRINFOTBL(m_systemHandle, m_mv)[attrInfo]));
    
    return eNOERROR;
}

Four OOSQL_Catalog::attr_GetTypeID(CataClassInfo classInfo, CataAttrInfo attrInfo, TypeID &typeID)
{
    Four    dTypeID;
    Four    e;
	Four	complexType;

    // validate classInfo, attrInfo

    // get type
    dTypeID = CATALOG_GET_ATTRTYPE(&(CATALOG_GET_ATTRINFOTBL(m_systemHandle, m_mv)[attrInfo]));
    complexType = CATALOG_GET_ATTRCOMPLEXTYPE(&(CATALOG_GET_ATTRINFOTBL(m_systemHandle, m_mv)[attrInfo]));

	typeID = OOSQL_COMPOSE_TYPE(complexType, lomType_to_TypeID(dTypeID));

    if(typeID == TYPEID_NONE)
    {
        CataClassInfo refClass;
        Four          refClassId;

        e = attr_GetReferencingClass(classInfo, attrInfo, refClass);
        OOSQL_CHECK_ERR(e);
        
        e = class_ClassInfo_to_ClassId(refClass, refClassId);

        if(refClassId >= 100)
            typeID = TYPEID_OID;
    }
    return eNOERROR;
}

Four OOSQL_Catalog::attr_GetReferencingClass(CataClassInfo classInfo, CataAttrInfo attrInfo, CataClassInfo &cid)
{
    Four    e;              // error code
    Four    classId;        // class id
    Four    classTblIndex;  // catalog info tbl index

    // validate classInfo, attrInfo
    classId = CATALOG_GET_ATTRDOMAIN(&(CATALOG_GET_ATTRINFOTBL(m_systemHandle, m_mv)[attrInfo]));

    e = Catalog_GetClassInfo(m_systemHandle, m_volId, classId, &classTblIndex);
	if(e == eNOSUCHCLASS_LOM)
		classTblIndex = -1;
	else
		OOSQL_CHECK_ERR(e);

    cid = classTblIndex;

    return eNOERROR;
}

Four OOSQL_Catalog::attr_AttrInfo_to_ColNo(CataClassInfo classInfo, CataAttrInfo attrInfo, Two &colNo)
{
    // validate classInfo, attrInfo
    colNo = CATALOG_GET_ATTRCOLNO(&(CATALOG_GET_ATTRINFOTBL(m_systemHandle, m_mv)[attrInfo]));
    return eNOERROR;
}

Four OOSQL_Catalog::attr_ColNo_to_AttrInfo(CataClassInfo classInfo, Two colNo, CataAttrInfo &attrInfo)
{
    Four    classId;
    Four    tblIndex;
    Four    e;

    // validate classInfo, attrInfo
    classId = CATALOG_GET_CLASSID(&(CATALOG_GET_CLASSINFOTBL(m_systemHandle, m_mv)[classInfo]));
    e = Catalog_GetAttrInfo(m_systemHandle, m_volId, classId, colNo, &tblIndex);
	OOSQL_CHECK_ERR(e);

    attrInfo = tblIndex;

    return eNOERROR;
}


Four OOSQL_Catalog::attr_ColNo_to_ColNoIncludingDroppedCol(CataClassInfo classInfo, Two colNo, Two &colNoIncludingDroppedCol)
{
    Four    classId;
    Four    e;
	Four	colNo2;


    // validate classInfo, attrInfo
    classId = CATALOG_GET_CLASSID(&(CATALOG_GET_CLASSINFOTBL(m_systemHandle, m_mv)[classInfo]));
	e = Catalog_GetAttrNumIncludingDroppedCol(m_systemHandle, m_volId, classId, colNo, &colNo2);
	OOSQL_CHECK_ERR(e);

	colNoIncludingDroppedCol = colNo2;

    return eNOERROR;
}


Four OOSQL_Catalog::attr_AttrName_to_CataAttrInfo(CataClassInfo classInfo, char* attrName, CataAttrInfo& attrInfo)
{
    Four    e;          // error code
    Four    tblIndex;   // table index
    Four    classId;

    classId = CATALOG_GET_CLASSID(&(CATALOG_GET_CLASSINFOTBL(m_systemHandle, m_mv)[classInfo]));
    e = Catalog_GetAttrInfoByName(m_systemHandle, m_volId, classId, attrName, &tblIndex);
    if(e == eNOSUCHATTRIBUTE_CATALOG)
        return eNOT_FOUND_OOSQL;
    else
        OOSQL_CHECK_ERR(e);

    attrInfo = tblIndex;
    return eNOERROR;
}

Four OOSQL_Catalog::attr_CataAttrInfo_to_AttrName(CataClassInfo classInfo, CataAttrInfo attrInfo, char* attrName)
{
	char*	name;

	name = CATALOG_GET_ATTRNAME(&(CATALOG_GET_ATTRINFOTBL(m_systemHandle, m_mv)[attrInfo]));
	strcpy(attrName, name);

    return eNOERROR;
}

Four OOSQL_Catalog::method_GetReturnTypeID(CataClassInfo classInfo, CataMethodInfo methodInfo, TypeID& typeID)
{
    Four    dTypeID;
    // validate classInfo, methodInfo

    dTypeID = CATALOG_GET_METHODRETURNTYPE(&(CATALOG_GET_METHODINFOTBL(m_systemHandle, m_mv)[methodInfo]));
    typeID = lomType_to_TypeID(dTypeID);
    return eNOERROR;
}

Four OOSQL_Catalog::method_GetReferencingClass(CataClassInfo classInfo, CataMethodInfo methodInfo, CataClassInfo &cid)
{
    Four    e;              // error code
    Four    classId;        // class id
    Four    classTblIndex;  // catalog info tbl index

    // validate classInfo, methodInfo


    e = Catalog_GetClassInfo(m_systemHandle, m_volId, classId, &classTblIndex);
    OOSQL_CHECK_ERR(e);

    cid = classTblIndex;

    return eNOERROR;
}

Four OOSQL_Catalog::method_GetN_Arguments(CataClassInfo classInfo, CataMethodInfo methodInfo, Four &nArguments)
{
    // validate classInfo, methodInfo
    nArguments = CATALOG_GET_METHODNARGUMENTS(&(CATALOG_GET_METHODINFOTBL(m_systemHandle, m_mv)[methodInfo]));
    return eNOERROR;
}

Four OOSQL_Catalog::method_GetNTH_ArgumentTypeID(CataClassInfo classInfo, CataMethodInfo methodInfo, Four nth, TypeID &typeID)
{
    Four    dTypeID;

    // validate classInfo, methodInfo
    dTypeID = CATALOG_GET_METHODARGUMENTLIST(&(CATALOG_GET_METHODINFOTBL(m_systemHandle, m_mv)[methodInfo]))[nth];
    typeID = lomType_to_TypeID(dTypeID);
    return eNOERROR;
}

Four OOSQL_Catalog::method_GetNTH_ArgumentName(CataClassInfo classInfo, CataMethodInfo methodInfo, Four nth, char* argumentName)
{
	return eNOERROR;
}
Four OOSQL_Catalog::method_GetNTH_ArgumentTypeName(CataClassInfo classInfo, CataMethodInfo methodInfo, Four nth, char* typeName)
{
	return eNOERROR;
}
Four OOSQL_Catalog::method_GetNTH_ArgumentAsLocatorFlag(CataClassInfo classInfo, CataMethodInfo methodInfo, Four nth, Boolean& asLocatorFlag)
{
	return eNOERROR;
}
Four OOSQL_Catalog::method_GetNTH_ArgumentParameterMode(CataClassInfo classInfo, CataMethodInfo methodInfo, Four nth, ParameterMode& parameterMode)
{
	return eNOERROR;
}
Four OOSQL_Catalog::method_CataMethodInfo_to_MethodName(CataClassInfo classInfo, CataMethodInfo methodInfo, char* methodName)
{
	return eNOERROR;
}
Four OOSQL_Catalog::method_GetInheritedFrom(CataClassInfo classInfo, CataMethodInfo methodInfo, Four &inheritedFrom)
{
	return eNOERROR;
}
Four OOSQL_Catalog::method_MethodInfo_to_MethodNo(CataClassInfo classInfo, CataMethodInfo methodInfo, Two &methodNo)
{
    return eNOERROR;
}

Four OOSQL_Catalog::method_MethodNo_to_MethodInfo(CataClassInfo classInfo, Two methodNo, CataMethodInfo& methodInfo)
{
	return eNOERROR;
}

Four OOSQL_Catalog::method_MethodName_to_CataMethodInfo(CataClassInfo classInfo, char* methodName, CataMethodInfo& methodInfo)
{
    Four    e;          // error code
    Four    tblIndex;   // table index
    Four    classId;

    return eNOT_FOUND_OOSQL;
}

Four OOSQL_Catalog::method_GetReturnTypeLength(CataClassInfo classInfo, CataMethodInfo methodInfo, Four &length)
{
    // validate classInfo, methodInfo

    return eNOERROR;
}

Four OOSQL_Catalog::method_GetPtr(CataClassInfo classInfo, CataMethodInfo methodInfo, void* &ptr)
{
    // validate classInfo, methodInfo
    ptr = (void *)CATALOG_GET_METHODPTRTOFUNCTION(&(CATALOG_GET_METHODINFOTBL(m_systemHandle, m_mv)[methodInfo]));
    return eNOERROR;
}

#define TBLINDEX_ARRAY_SIZE     20
Four OOSQL_Catalog::index_isB_TreeIndexExist(CataClassInfo classInfo, CataAttrInfo attrInfo)
{
    Four    classId;
    Four    attrColNo;
    Four    e;
    Four    i;
    Four    indexEntry[TBLINDEX_ARRAY_SIZE];
    Four    nIndexEntry = TBLINDEX_ARRAY_SIZE;

    classId   = CATALOG_GET_CLASSID(&(CATALOG_GET_CLASSINFOTBL(m_systemHandle, m_mv)[classInfo]));
	attrColNo = CATALOG_GET_ATTRCOLNO(&(CATALOG_GET_ATTRINFOTBL(m_systemHandle, m_mv)[attrInfo]));

    e = Catalog_GetIndexInfoByAttrNum(m_systemHandle, m_volId, classId, GET_USERLEVEL_COLNO(attrColNo), &nIndexEntry, indexEntry);
    OOSQL_CHECK_ERR(e);

    for(i = 0; i < nIndexEntry; i++)
    {
        // check index type (B+Tree Index만 return 한다)
        if(CATALOG_GET_INDEXTYPE(&(CATALOG_GET_INDEXINFOTBL(m_systemHandle, m_mv)[indexEntry[i]])) == LOM_INDEXTYPE_BTREE)
            return SM_TRUE;
    }

    return SM_FALSE;
}


Four OOSQL_Catalog::index_getB_TreeIndexes(CataClassInfo classInfo, CataAttrInfo attrInfo, Four &nIndexes, OOSQL_StorageManager::IndexID indexes[])
{
    Four    classId;
    Four    attrColNo;
    Four    tblIndexes[TBLINDEX_ARRAY_SIZE];
    Four    nTblIndexes = TBLINDEX_ARRAY_SIZE;
    Four    i, index;
    Four    e;

    classId   = CATALOG_GET_CLASSID(&(CATALOG_GET_CLASSINFOTBL(m_systemHandle, m_mv)[classInfo]));
    attrColNo = CATALOG_GET_ATTRCOLNO(&(CATALOG_GET_ATTRINFOTBL(m_systemHandle, m_mv)[attrInfo]));

    e = Catalog_GetIndexInfoByAttrNum(m_systemHandle, m_volId, classId, GET_USERLEVEL_COLNO(attrColNo), &nTblIndexes, tblIndexes);
    OOSQL_CHECK_ERR(e);

	for(i = 0, index = 0; i < nTblIndexes && index < nIndexes; i++)
    {
        if(CATALOG_GET_INDEXTYPE(&(CATALOG_GET_INDEXINFOTBL(m_systemHandle, m_mv)[tblIndexes[i]])) == LOM_INDEXTYPE_BTREE)
        {
			memcpy(&indexes[index], &CATALOG_GET_INDEXID(&(CATALOG_GET_INDEXINFOTBL(m_systemHandle, m_mv)[tblIndexes[i]])), sizeof(OOSQL_StorageManager::IndexID));
            index ++;
        }
    }
    
    // set return value
    nIndexes = index;

    return eNOERROR;
}

Four OOSQL_Catalog::index_isTextIndexExist(CataClassInfo classInfo, CataAttrInfo attrInfo)
{
    Four    classId;
    Four    attrColNo;
    Four    e;
    Four    i;
    Four    indexEntry[TBLINDEX_ARRAY_SIZE];
    Four    nIndexEntry = TBLINDEX_ARRAY_SIZE;

    classId   = CATALOG_GET_CLASSID(&(CATALOG_GET_CLASSINFOTBL(m_systemHandle, m_mv)[classInfo]));
    attrColNo = CATALOG_GET_ATTRCOLNO(&(CATALOG_GET_ATTRINFOTBL(m_systemHandle, m_mv)[attrInfo]));

    e = Catalog_GetIndexInfoByAttrNum(m_systemHandle, m_volId, classId, GET_USERLEVEL_COLNO(attrColNo), &nIndexEntry, indexEntry);
    OOSQL_CHECK_ERR(e);

    for(i = 0; i < nIndexEntry; i++)
    {
        // check index type (text Index만 return 한다)
        if(CATALOG_GET_INDEXTYPE(&(CATALOG_GET_INDEXINFOTBL(m_systemHandle, m_mv)[indexEntry[i]])) == LOM_INDEXTYPE_TEXT)
            return SM_TRUE;
    }

    return SM_FALSE;
}

Four OOSQL_Catalog::index_getTextIndexes(CataClassInfo classInfo, CataAttrInfo attrInfo, Four &nIndexes, OOSQL_StorageManager::IndexID indexes[])
{
    Four    classId;
    Four    attrColNo;
    Four    tblIndexes[TBLINDEX_ARRAY_SIZE];
    Four    nTblIndexes = TBLINDEX_ARRAY_SIZE;
    Four    i, index;
    Four    e;

    classId   = CATALOG_GET_CLASSID(&(CATALOG_GET_CLASSINFOTBL(m_systemHandle, m_mv)[classInfo]));
    attrColNo = CATALOG_GET_ATTRCOLNO(&(CATALOG_GET_ATTRINFOTBL(m_systemHandle, m_mv)[attrInfo]));

    e = Catalog_GetIndexInfoByAttrNum(m_systemHandle, m_volId, classId, GET_USERLEVEL_COLNO(attrColNo), &nTblIndexes, tblIndexes);
    OOSQL_CHECK_ERR(e);

    for(i = 0, index = 0; i < MIN(nTblIndexes, nIndexes); i++)
    {
        if(CATALOG_GET_INDEXTYPE(&(CATALOG_GET_INDEXINFOTBL(m_systemHandle, m_mv)[tblIndexes[i]])) == LOM_INDEXTYPE_TEXT)
        {
			memcpy(&indexes[index], &CATALOG_GET_INDEXID(&(CATALOG_GET_INDEXINFOTBL(m_systemHandle, m_mv)[tblIndexes[i]])), sizeof(OOSQL_StorageManager::IndexID));
            index ++;
        }
    }
    
    // set return value
    nIndexes = index;

    return eNOERROR;
}

Four OOSQL_Catalog::index_isMlgfIndexExist(CataClassInfo classInfo, CataAttrInfo attrInfo)
{
    Four    classId;
    Four    attrColNo;
    Four    e;
    Four    i;
    Four    indexEntry[TBLINDEX_ARRAY_SIZE];
    Four    nIndexEntry = TBLINDEX_ARRAY_SIZE;

    classId   = CATALOG_GET_CLASSID(&(CATALOG_GET_CLASSINFOTBL(m_systemHandle, m_mv)[classInfo]));
    attrColNo = CATALOG_GET_ATTRCOLNO(&(CATALOG_GET_ATTRINFOTBL(m_systemHandle, m_mv)[attrInfo]));

    e = Catalog_GetIndexInfoByAttrNum(m_systemHandle, m_volId, classId, GET_USERLEVEL_COLNO(attrColNo), &nIndexEntry, indexEntry);
    OOSQL_CHECK_ERR(e);

    for(i = 0; i < nIndexEntry; i++)
    {
        if(CATALOG_GET_INDEXTYPE(&(CATALOG_GET_INDEXINFOTBL(m_systemHandle, m_mv)[indexEntry[i]])) == LOM_INDEXTYPE_MLGF)
            return SM_TRUE;
    }

    return SM_FALSE;
}

Four OOSQL_Catalog::index_getMlgfIndexes(CataClassInfo classInfo, CataAttrInfo attrInfo, Four &nIndexes, OOSQL_StorageManager::IndexID indexes[])
{
    Four    classId;
    Four    attrColNo;
    Four    tblIndexes[TBLINDEX_ARRAY_SIZE];
    Four    nTblIndexes = TBLINDEX_ARRAY_SIZE;
    Four    i, index;
    Four    e;

    classId   = CATALOG_GET_CLASSID(&(CATALOG_GET_CLASSINFOTBL(m_systemHandle, m_mv)[classInfo]));
    attrColNo = CATALOG_GET_ATTRCOLNO(&(CATALOG_GET_ATTRINFOTBL(m_systemHandle, m_mv)[attrInfo]));

    e = Catalog_GetIndexInfoByAttrNum(m_systemHandle, m_volId, classId, GET_USERLEVEL_COLNO(attrColNo), &nTblIndexes, tblIndexes);
    OOSQL_CHECK_ERR(e);

	for(i = 0, index = 0; i < nTblIndexes && index < nIndexes; i++)
    {
        if(CATALOG_GET_INDEXTYPE(&(CATALOG_GET_INDEXINFOTBL(m_systemHandle, m_mv)[tblIndexes[i]])) == LOM_INDEXTYPE_MLGF)
        {
            memcpy(&indexes[index], &CATALOG_GET_INDEXID(&(CATALOG_GET_INDEXINFOTBL(m_systemHandle, m_mv)[tblIndexes[i]])), sizeof(OOSQL_StorageManager::IndexID));
            index ++;
        }
    }
    
    // set return value
    nIndexes = index;

    return eNOERROR;
}

Four OOSQL_Catalog::class_IsSpatialClass(CataClassInfo classInfo)
{
	return SM_FALSE;
}

Four OOSQL_Catalog::class_IsLrdsRelation(CataClassInfo classInfo)
{
	Four e;
	Four classId;

	e = class_ClassInfo_to_ClassId(classInfo, classId);
	OOSQL_CHECK_ERR(e);

	e = Catalog_IsLRDS_RelationBasedClass(m_systemHandle, m_volId, classId);
	OOSQL_CHECK_ERR(e);

	if(e)
		return SM_TRUE;
	else
		return SM_FALSE;
}

Four OOSQL_Catalog::index_GetIndexInfos(CataClassInfo classInfo, CataAttrInfo attrInfo, Four& nIndexes, CataIndexInfo* indexInfos)
{
    Four    classId;
    Four    attrColNo;
    Four    tblIndexes[TBLINDEX_ARRAY_SIZE];
    Four    nTblIndexes = TBLINDEX_ARRAY_SIZE;
    Four    i;
    Four    e;

    classId   = CATALOG_GET_CLASSID(&(CATALOG_GET_CLASSINFOTBL(m_systemHandle, m_mv)[classInfo]));
    attrColNo = CATALOG_GET_ATTRCOLNO(&(CATALOG_GET_ATTRINFOTBL(m_systemHandle, m_mv)[attrInfo]));

    e = Catalog_GetIndexInfoByAttrNum(m_systemHandle, m_volId, classId, GET_USERLEVEL_COLNO(attrColNo), &nTblIndexes, tblIndexes);
    OOSQL_CHECK_ERR(e);

	nIndexes = MIN(nTblIndexes, nIndexes);
	for(i = 0; i < nIndexes; i++)
		indexInfos[i] = tblIndexes[i];

	return eNOERROR;
}
Four OOSQL_Catalog::index_IndexInfo_to_IndexId(CataClassInfo classInfo, CataIndexInfo indexInfo, OOSQL_StorageManager::IndexID& indexId)
{
	memcpy(&indexId, &CATALOG_GET_INDEXID(&(CATALOG_GET_INDEXINFOTBL(m_systemHandle, m_mv)[indexInfo])), sizeof(OOSQL_StorageManager::IndexID));

	return eNOERROR;
}
Four OOSQL_Catalog::index_IndexId_to_IndexInfo(CataClassInfo classInfo, OOSQL_StorageManager::IndexID indexId, CataIndexInfo& indexInfo)
{
	Four e;
	Four classId;

	classId   = CATALOG_GET_CLASSID(&(CATALOG_GET_CLASSINFOTBL(m_systemHandle, m_mv)[classInfo]));

	e = Catalog_GetIndexInfoByIndexID(m_systemHandle, m_volId, classId, (::LOM_IndexID*)&indexId, (Four*)&indexInfo);
	OOSQL_CHECK_ERR(e);

	return eNOERROR;
}
Four OOSQL_Catalog::index_GetIndexDesc(CataClassInfo classInfo, CataIndexInfo indexInfo, OOSQL_StorageManager::KeyDesc& btreeKeyDesc)
{
	memcpy(&btreeKeyDesc, &CATALOG_GET_INDEXKEYDESC(&(CATALOG_GET_INDEXINFOTBL(m_systemHandle, m_mv)[indexInfo])).btree, sizeof(btreeKeyDesc));

	return eNOERROR;
}
Four OOSQL_Catalog::index_GetIndexDesc(CataClassInfo classInfo, CataIndexInfo indexInfo, OOSQL_StorageManager::MLGF_KeyDesc& mlgfKeyDesc)
{
	memcpy(&mlgfKeyDesc, &CATALOG_GET_INDEXKEYDESC(&(CATALOG_GET_INDEXINFOTBL(m_systemHandle, m_mv)[indexInfo])).mlgf, sizeof(mlgfKeyDesc));

	return eNOERROR;
}
Four OOSQL_Catalog::index_GetIndexDesc(CataClassInfo classInfo, CataIndexInfo indexInfo, OOSQL_StorageManager::InvertedIndexDesc& invertedIndexKeyDesc)
{
	memcpy(&invertedIndexKeyDesc, &CATALOG_GET_INDEXKEYDESC(&(CATALOG_GET_INDEXINFOTBL(m_systemHandle, m_mv)[indexInfo])).invertedIndex, sizeof(invertedIndexKeyDesc));

	return eNOERROR;
}
Four OOSQL_Catalog::index_GetIndexType(CataClassInfo classInfo, CataIndexInfo indexInfo, One& indexType)
{
	indexType = CATALOG_GET_INDEXTYPE(&(CATALOG_GET_INDEXINFOTBL(m_systemHandle, m_mv)[indexInfo]));

	return eNOERROR;
}
Four OOSQL_Catalog::index_GetIndexName(CataClassInfo classInfo, CataIndexInfo indexInfo, char* indexName)
{
	strcpy(indexName, CATALOG_GET_INDEXNAME(&(CATALOG_GET_INDEXINFOTBL(m_systemHandle, m_mv)[indexInfo])));
	return eNOERROR;
}
Four OOSQL_Catalog::index_GetIndexColumns(CataClassInfo classInfo, CataIndexInfo indexInfo, Two colNo[MAXNUMKEYPARTS])
{
	memcpy(colNo, CATALOG_GET_INDEXCOLNO(&(CATALOG_GET_INDEXINFOTBL(m_systemHandle, m_mv)[indexInfo])), sizeof(Two) * MAXNUMKEYPARTS);

	return eNOERROR;
}

Four OOSQL_Catalog::method_GetReturnTypeName(CataClassInfo classInfo, CataMethodInfo methodInfo, char* typeName)
{
	return eNOERROR;
}
Four OOSQL_Catalog::method_GetReturnParameterMode(CataClassInfo classInfo, CataMethodInfo methodInfo, ParameterMode& parameterMode)
{
	return eNOERROR;
}
Four OOSQL_Catalog::method_GetReturnAsLocatorFlag(CataClassInfo classInfo, CataMethodInfo methodInfo, Boolean& asLocator)
{
	return eNOERROR;
}
Four OOSQL_Catalog::method_GetDeterinisticFlag(CataClassInfo classInfo, CataMethodInfo methodInfo, Boolean& deterministicFlag)
{
	return eNOERROR;
}
Four OOSQL_Catalog::method_GetExternalActionFlag(CataClassInfo classInfo, CataMethodInfo methodInfo, Boolean& externalActionFlag)
{
	return eNOERROR;
}
Four OOSQL_Catalog::method_GetFencedFlag(CataClassInfo classInfo, CataMethodInfo methodInfo, Boolean& fencdedFlag)
{
	return eNOERROR;
}
Four OOSQL_Catalog::method_GetNullCallFlag(CataClassInfo classInfo, CataMethodInfo methodInfo, Boolean& nullCallFlag)
{
	return eNOERROR;
}
Four OOSQL_Catalog::method_GetScratchPadFlag(CataClassInfo classInfo, CataMethodInfo methodInfo, Boolean& scratchPadFlag)
{
	return eNOERROR;
}
Four OOSQL_Catalog::method_GetFinalCallFlag(CataClassInfo classInfo, CataMethodInfo methodInfo, Boolean& finalCallFlag)
{
	return eNOERROR;
}
Four OOSQL_Catalog::method_GetAllowParallelFlag(CataClassInfo classInfo, CataMethodInfo methodInfo, Boolean& allowParallelFlag)
{
	return eNOERROR;
}
Four OOSQL_Catalog::method_GetDbInfoFlag(CataClassInfo classInfo, CataMethodInfo methodInfo, Boolean& dbInfoFlag)
{
	return eNOERROR;
}
Four OOSQL_Catalog::method_GetSpecificName(CataClassInfo classInfo, CataMethodInfo methodInfo, char* specificName)
{
	return eNOERROR;
}
Four OOSQL_Catalog::method_GetLanguage(CataClassInfo classInfo, CataMethodInfo methodInfo, char* language)
{
	return eNOERROR;
}
Four OOSQL_Catalog::method_GetParameterStyle(CataClassInfo classInfo, CataMethodInfo methodInfo, char* parameterStyle)
{
	return eNOERROR;
}
Four OOSQL_Catalog::method_GetExternalName(CataClassInfo classInfo, CataMethodInfo methodInfo, char* externalName)
{
	return eNOERROR;
}

Four OOSQL_Catalog::textindex_GetN_PostingsOfKeyword(CataClassInfo classInfo, CataClassInfo attrInfo, CataIndexInfo indexInfo, char* keyword, Four& nPostings)
{
	Four	e;
	Two		textColNo;
	Four	ocn;
	Four	classId;
	Two		keyLen;
	Four	scanId;
	char	keywordReturned[MAXKEYWORDSIZE];

	e = class_ClassInfo_to_ClassId(classInfo, classId);
	OOSQL_CHECK_ERR(e);

	ocn = e = LOM_GetOpenClassNumByClassId(m_systemHandle, m_volId, classId);
	OOSQL_CHECK_ERR(e);

	e = attr_AttrInfo_to_ColNo(classInfo, attrInfo, textColNo);
	OOSQL_CHECK_ERR(e);

	scanId = e = LOM_Text_Keyword_Scan_Open(m_systemHandle, ocn, GET_USERLEVEL_COLNO(textColNo), keyword);
	OOSQL_CHECK_ERR(e);

	e = LOM_Text_Keyword_Scan_Next(m_systemHandle, scanId, KEYWORD, keywordReturned, &nPostings, NULL);
	OOSQL_CHECK_ERR(e);

	if(e == EOS)		
		nPostings = NIL;
	else if(strcmp(keyword, keywordReturned))
		nPostings = NIL;

	e = LOM_Text_Keyword_Scan_Close(m_systemHandle, scanId);
	OOSQL_CHECK_ERR(e);

	return eNOERROR;
}

Four OOSQL_Catalog::textindex_KeywordScan_Open(CataClassInfo classInfo, CataClassInfo attrInfo, CataIndexInfo indexInfo, 
											   Four keywordKind,
											   Two startBoundLength, char* startBoundString, 
											   Two stopBoundLength, char* stopBoundString)
{
	Four	e;
	Two 	textColNo;
	Four	ocn;
	Four	classId;
	Two		keyLen;
	Four	scanId;
	char	keywordReturned[MAXKEYWORDSIZE];

	e = class_ClassInfo_to_ClassId(classInfo, classId);
	OOSQL_CHECK_ERR(e);

	ocn = e = LOM_GetOpenClassNumByClassId(m_systemHandle, m_volId, classId);
	OOSQL_CHECK_ERR(e);

	e = attr_AttrInfo_to_ColNo(classInfo, attrInfo, textColNo);
	OOSQL_CHECK_ERR(e);

	scanId = e = LOM_Text_Keyword_Scan_Open_WithBoundCondition(	m_systemHandle, ocn, 
																GET_USERLEVEL_COLNO(textColNo), keywordKind,
		                                                       	startBoundLength, startBoundString,
															   	stopBoundLength, stopBoundString);
	OOSQL_CHECK_ERR(e);

	return scanId;
}

Four OOSQL_Catalog::textindex_KeywordScan_Close(Four scanId)
{
	Four	e;

	e = LOM_Text_Keyword_Scan_Close(m_systemHandle, scanId);
	OOSQL_CHECK_ERR(e);

	return eNOERROR;
}

Four OOSQL_Catalog::textindex_KeywordScan_Next(Four scanId, Four keywordKind, char* keyword, Four& nPostings, OOSQL_StorageManager::TupleID& invertedIndexEntryTupleID)
{
	Four	e;

	e = LOM_Text_Keyword_Scan_Next(m_systemHandle, scanId, keywordKind, keyword, &nPostings, (::TupleID*)&invertedIndexEntryTupleID);
	OOSQL_CHECK_ERR(e);

	return e;
}
