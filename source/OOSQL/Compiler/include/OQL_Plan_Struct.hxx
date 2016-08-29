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

#ifndef __OQL_PLAN_STRUCT_H__
#define __OQL_PLAN_STRUCT_H__

#include "OQL_OutStream.hxx"
#include "OOSQL_Common.h"
#include "OQL_Pools.hxx"
#include "OQL_Common_Struct.hxx"
#include "OOSQL_StorageManager.hxx"
#include "OOSQL_Catalog.hxx"

#define IS_NULL_POOLINDEX(poolIdx)      \
        ((poolIdx).startIndex == -1)

#define GET_POOLSTART(poolIdx)  ((poolIdx).startIndex)
#define GET_POOLSIZE(poolIdx)   ((poolIdx).size)

/* 
 *	Forward Structure Declaration
 */
struct AP_ColNoMapElement;
struct AP_UsedColElement;
struct AP_MethodNoMapElement;
struct AP_UsedMethodElement;
struct AP_ProjectionElement;
struct AP_ExprElement;
struct AP_CondListElement;
struct AP_ArgumentElement;
struct AP_AggrFuncElement;
struct AP_ProjectionListElement;
struct AP_TempFileInfoElement;
struct AP_IndexInfoElement;
struct AP_TextIndexCondElement;
struct AP_UpdateValueElement;
struct AP_InsertValueElement;
struct AP_BoolExprElements;
struct CommonAP_Element;
struct ClientAP_Element;

struct AttributeInfoElement;
struct MethodInfoElement;
struct KeyInfoElement;
struct DBCommandElement;
struct SuperClassElement;
struct ArgumentTypeElement;

/*
 *  Declare pool indexes
 */
#ifndef TEMPLATE_NOT_SUPPORTED

#define AP_ColNoMapPoolIndex         PoolIndex<AP_ColNoMapElement>
#define AP_UsedColPoolIndex          PoolIndex<AP_UsedColElement>
#define AP_MethodNoMapPoolIndex      PoolIndex<AP_MethodNoMapElement>
#define AP_UsedMethodPoolIndex       PoolIndex<AP_UsedMethodElement>
#define AP_ProjectionPoolIndex       PoolIndex<AP_ProjectionElement>
#define AP_ExprPoolIndex             PoolIndex<AP_ExprElement>
#define AP_ArgumentPoolIndex         PoolIndex<AP_ArgumentElement>
#define AP_AggrFuncPoolIndex         PoolIndex<AP_AggrFuncElement>
#define AP_ProjectionListPoolIndex   PoolIndex<AP_ProjectionListElement>
#define AP_TempFileInfoPoolIndex     PoolIndex<AP_TempFileInfoElement>
#define AP_TextIndexCondPoolIndex    PoolIndex<AP_TextIndexCondElement>
#define AP_UpdateValuePoolIndex		 PoolIndex<AP_UpdateValueElement>
#define AP_InsertValuePoolIndex		 PoolIndex<AP_InsertValueElement>
#define AP_BoolExprPoolIndex		 PoolIndex<AP_BoolExprElement>
#define CommonAP_PoolIndex           PoolIndex<CommonAP_Element>
#define ClientAP_PoolIndex           PoolIndex<ClientAP_Element>

#define AttributeInfoPoolIndex		 PoolIndex<AttributeInfoElement>
#define MethodInfoPoolIndex			 PoolIndex<MethodInfoElement>
#define KeyInfoPoolIndex			 PoolIndex<KeyInfoElement>
#define DBCommandPoolIndex			 PoolIndex<DBCommandElement>
#define SuperClassPoolIndex			 PoolIndex<SuperClassElement>

#define ArgumentTypePoolIndex		 PoolIndex<ArgumentTypeElement>

#else   /* TEMPLATE_NOT_SUPPORTED */

#define AP_ColNoMapPoolIndex         PoolIndex(AP_ColNoMapElement)
#define AP_UsedColPoolIndex          PoolIndex(AP_UsedColElement)
#define AP_MethodNoMapPoolIndex      PoolIndex(AP_MethodNoMapElement)
#define AP_UsedMethodPoolIndex       PoolIndex(AP_UsedMethodElement)
#define AP_ProjectionPoolIndex       PoolIndex(AP_ProjectionElement)
#define AP_ExprPoolIndex             PoolIndex(AP_ExprElement)
#define AP_ArgumentPoolIndex         PoolIndex(AP_ArgumentElement)
#define AP_AggrFuncPoolIndex         PoolIndex(AP_AggrFuncElement)
#define AP_ProjectionListPoolIndex   PoolIndex(AP_ProjectionListElement)
#define AP_TempFileInfoPoolIndex     PoolIndex(AP_TempFileInfoElement)
#define AP_TextIndexCondPoolIndex    PoolIndex(AP_TextIndexCondElement)
#define AP_UpdateValuePoolIndex		 PoolIndex(AP_UpdateValueElement)
#define AP_InsertValuePoolIndex		 PoolIndex(AP_InsertValueElement)
#define AP_BoolExprPoolIndex		 PoolIndex(AP_BoolExprElement)
#define CommonAP_PoolIndex           PoolIndex(CommonAP_Element)
#define ClientAP_PoolIndex           PoolIndex(ClientAP_Element)

#define AttributeInfoPoolIndex		 PoolIndex(AttributeInfoElement)
#define MethodInfoPoolIndex			 PoolIndex(MethodInfoElement)
#define KeyInfoPoolIndex			 PoolIndex(KeyInfoElement)
#define DBCommandPoolIndex			 PoolIndex(DBCommandElement)
#define SuperClassPoolIndex			 PoolIndex(SuperClassElement)
#define ArgumentTypePoolIndex		 PoolIndex(ArgumentTypeElement)

declare(PoolIndex,AP_ColNoMapElement);
declare(PoolIndex,AP_UsedColElement);
declare(PoolIndex,AP_MethodNoMapElement);
declare(PoolIndex,AP_UsedMethodElement);
declare(PoolIndex,AP_ProjectionElement);
declare(PoolIndex,AP_ExprElement);
declare(PoolIndex,AP_ArgumentElement);
declare(PoolIndex,AP_AggrFuncElement);
declare(PoolIndex,AP_ProjectionListElement);
declare(PoolIndex,AP_TempFileInfoElement);
declare(PoolIndex,AP_TextIndexCondElement);
declare(PoolIndex,AP_UpdateValueElement);
declare(PoolIndex,AP_InsertValueElement);
declare(PoolIndex,AP_BoolExprElement);
declare(PoolIndex,CommonAP_Element);
declare(PoolIndex,ClientAP_Element);

declare(PoolIndex,AttributeInfoElement)
declare(PoolIndex,MethodInfoElement)
declare(PoolIndex,KeyInfoElement)
declare(PoolIndex,DBCommandElement)
declare(PoolIndex,SuperClassElement)
declare(PoolIndex,ArgumentTypeElement)

#endif  /* TEMPLATE_NOT_SUPPORTED */

/* 
 * Aliases
 */
#define AP_PathExprPoolIndex        PathExprPoolIndex
#define AP_StructurePoolIndex       StructurePoolIndex
#define AP_ValuePoolIndex           ValuePoolIndex
#define AP_RealPoolIndex            RealPoolIndex
#define AP_IntegerPoolIndex         IntegerPoolIndex
#define AP_StringPoolIndex          StringPoolIndex
#define AP_FunctionPoolIndex        FunctionPoolIndex
#define AP_DomainPoolIndex          DomainPoolIndex
#define AP_CollectionPoolIndex      CollectionPoolIndex
#define AP_MBRPoolIndex             MBRPoolIndex
#define AP_NilPoolIndex             NilPoolIndex
#define AP_BooleanPoolIndex         BooleanPoolIndex
#define AP_ObjectPoolIndex          ObjectPoolIndex
#define AP_ConstructPoolIndex       ConstructPoolIndex
#define AP_MemberPoolIndex          MemberPoolIndex
#define AP_SubClassPoolIndex        SubClassPoolIndex
#define AP_JoinInfoPoolIndex        JoinInfoPoolIndex
#define AP_PathExprInfoPoolIndex    PathExprInfoPoolIndex
#define AP_PlanPoolIndex            PlanPoolIndex
#define AP_QGNodePoolIndex          QGNodePoolIndex

#define AP_FunctionID               FunctionID
#define AP_AggrFunctionID           AggrFunctionID

#define AP_PathExprPoolElements     PathExprPoolElements
#define AP_StructurePoolElements    StructurePoolElements
#define AP_ValuePoolElements        ValuePoolElements
#define AP_RealPoolElements         RealPoolElements
#define AP_IntegerPoolElements      IntegerPoolElements
#define AP_StringPoolElements       StringPoolElements
#define AP_FunctionPoolElements     FunctionPoolElements
#define AP_DomainPoolElements       DomainPoolElements
#define AP_CollectionPoolElements   CollectionPoolElements
#define AP_MBRPoolElements          MBRPoolElements
#define AP_NilPoolElements          NilPoolElements
#define AP_BooleanPoolElements      BooleanPoolElements
#define AP_ObjectPoolElements       ObjectPoolElements
#define AP_ConstructPoolElements    ConstructPoolElements
#define AP_MemberPoolElements       MemberPoolElements
#define AP_SubClassPoolElements     SubClassPoolElements
#define AP_JoinInfoPoolElements     JoinInfoPoolElements
#define AP_PathExprInfoPoolElements PathExprInfoPoolElements
#define AP_PlanPoolElements         PlanPoolElements
#define AP_QGNodePoolElements       QGNodePoolElements

/*
 *  Declare pool elements
 */
#ifndef TEMPLATE_NOT_SUPPORTED

#define AP_ColNoMapPoolElements         OQL_PoolElements<AP_ColNoMapElement>
#define AP_UsedColPoolElements          OQL_PoolElements<AP_UsedColElement>
#define AP_MethodNoMapPoolElements      OQL_PoolElements<AP_MethodNoMapElement>
#define AP_UsedMethodPoolElements       OQL_PoolElements<AP_UsedMethodElement>
#define AP_ProjectionPoolElements       OQL_PoolElements<AP_ProjectionElement>
#define AP_ExprPoolElements             OQL_PoolElements<AP_ExprElement>
#define AP_CondListPoolElements         OQL_PoolElements<AP_CondListElement>
#define AP_ArgumentPoolElements         OQL_PoolElements<AP_ArgumentElement>
#define AP_AggrFuncPoolElements         OQL_PoolElements<AP_AggrFuncElement>
#define AP_ProjectionListPoolElements   OQL_PoolElements<AP_ProjectionListElement>
#define AP_TempFileInfoPoolElements     OQL_PoolElements<AP_TempFileInfoElement>
#define AP_IndexInfoPoolElements        OQL_PoolElements<AP_IndexInfoElement>
#define AP_TextIndexCondPoolElements    OQL_PoolElements<AP_TextIndexCondElement>
#define AP_UpdateValuePoolElements		OQL_PoolElements<AP_UpdateValueElement>
#define AP_InsertValuePoolElements		OQL_PoolElements<AP_InsertValueElement>
#define AP_BoolExprPoolElements			OQL_PoolElements<AP_BoolExprElement>
#define CommonAP_PoolElements           OQL_PoolElements<CommonAP_Element>
#define ClientAP_PoolElements           OQL_PoolElements<ClientAP_Element>

#define AttributeInfoPoolElements		OQL_PoolElements<AttributeInfoElement>
#define MethodInfoPoolElements			OQL_PoolElements<MethodInfoElement>
#define KeyInfoPoolElements				OQL_PoolElements<KeyInfoElement>
#define DBCommandPoolElements			OQL_PoolElements<DBCommandElement>
#define SuperClassPoolElements			OQL_PoolElements<SuperClassElement>
#define ArgumentTypePoolElements		OQL_PoolElements<ArgumentTypeElement>

#else   /* TEMPLATE_NOT_SUPPORTED */

#define AP_ColNoMapPoolElements         OQL_PoolElements(AP_ColNoMapElement)
#define AP_UsedColPoolElements          OQL_PoolElements(AP_UsedColElement)
#define AP_MethodNoMapPoolElements      OQL_PoolElements(AP_MethodNoMapElement)
#define AP_UsedMethodPoolElements       OQL_PoolElements(AP_UsedMethodElement)
#define AP_ProjectionPoolElements       OQL_PoolElements(AP_ProjectionElement)
#define AP_ExprPoolElements             OQL_PoolElements(AP_ExprElement)
#define AP_CondListPoolElements         OQL_PoolElements(AP_CondListElement)
#define AP_ArgumentPoolElements         OQL_PoolElements(AP_ArgumentElement)
#define AP_AggrFuncPoolElements         OQL_PoolElements(AP_AggrFuncElement)
#define AP_ProjectionListPoolElements   OQL_PoolElements(AP_ProjectionListElement)
#define AP_TempFileInfoPoolElements     OQL_PoolElements(AP_TempFileInfoElement)
#define AP_IndexInfoPoolElements        OQL_PoolElements(AP_IndexInfoElement)
#define AP_TextIndexCondPoolElements    OQL_PoolElements(AP_TextIndexCondElement)
#define AP_UpdateValuePoolElements		OQL_PoolElements(AP_UpdateValueElement)
#define AP_InsertValuePoolElements		OQL_PoolElements(AP_InsertValueElement)
#define AP_BoolExprPoolElements			OQL_PoolElements(AP_BoolExprElement)
#define CommonAP_PoolElements           OQL_PoolElements(CommonAP_Element)
#define ClientAP_PoolElements           OQL_PoolElements(ClientAP_Element)

#define AttributeInfoPoolElements		OQL_PoolElements(AttributeInfoElement)
#define MethodInfoPoolElements			OQL_PoolElements(MethodInfoElement)
#define KeyInfoPoolElements				OQL_PoolElements(KeyInfoElement)
#define DBCommandPoolElements			OQL_PoolElements(DBCommandElement)
#define SuperClassPoolElements			OQL_PoolElements(SuperClassElement)
#define ArgumentTypePoolElements		OQL_PoolElements(ArgumentTypeElement)

#endif  /* TEMPLATE_NOT_SUPPORTED */

/*
 *	AP_OperatorStruct
 */
struct AP_OperatorStruct {
    OperatorID		   operatorId;
    AP_ExprPoolIndex   operand1;
    AP_ExprPoolIndex   operand2;
	AP_ExprPoolIndex   operand3;
    OperatorType       operatorType;
    #ifdef ENABLE_OPENGIS_OPTIMIZATION
    Four                 UDFNo;         /* Unique identifier that is assigned to a user-defined function used in a query */
    Boolean              isConstant;    /* True if the result of this function is constant */
    Boolean              isEvaluated;   /* True if the result of this function has been evaluated */
    AP_ValuePoolIndex    result;        /* The result of this function (used if isConstant is true) */
    #endif
};

OQL_OutStream& operator<<(OQL_OutStream& os, AP_OperatorStruct& object);

/* 
 *  AP_ColAccessInfo 
 */
struct AP_ColAccessInfo {
    Four    planNo;         // plan index from which the column value will be retrieved
    Two     colNo;          // column no.
};

OQL_OutStream& operator<<(OQL_OutStream& os, AP_ColAccessInfo& object);

/* 
 *  AP_MethodAccessInfo
 */
struct AP_MethodAccessInfo {
    Four                   planNo;     // plan index from which the method result will be retrieved
    Two                    methodNo;   // method no
};

OQL_OutStream& operator<<(OQL_OutStream& os, AP_MethodAccessInfo& object);

/*  
 *  AP_FuncEvalInfo
 */
struct AP_FuncEvalInfo {
    AP_FunctionID        functionID;
	Four                 userDefinedFunctionID;
    TypeID               returnType;
    Four                 returnLength;
    AP_ArgumentPoolIndex argument;
};

OQL_OutStream& operator<<(OQL_OutStream& os, AP_FuncEvalInfo& object);

/* 
 *  AP_PathExprAccessInfo
 */
struct AP_PathExprAccessInfo {
    Four    kind;                      // PATHEXPR_KIND_OBJECT(PATHEXPR_KIND_CLASS), 
                                       // PATHEXPR_KIND_ATTR, PATHEXPR_KIND_METHOD
    union {
        Four                   planNo; // plan index from which the object will be retrieved
        AP_ColAccessInfo       col;    // column access info
        AP_MethodAccessInfo    method; // method access info
    };
};

OQL_OutStream& operator<<(OQL_OutStream& os, AP_PathExprAccessInfo& object);

/*
 *  AP_TempFileAccessInfo
 */
struct AP_TempFileAccessInfo {
    Four    tempFileNum;
    Two     colNo;
};

OQL_OutStream& operator<<(OQL_OutStream& os, AP_TempFileAccessInfo& object);

/*
 *  AP_AggrFuncResultAccessInfo
 */
struct AP_AggrFuncResultAccessInfo {
    AP_AggrFunctionID   aggrFunctionID;
    Four                planNo;                 // plan index to which the aggregate function is applied
    Two                 aggrFuncIndex;          // aggregate function index
};

OQL_OutStream& operator<<(OQL_OutStream& os, AP_AggrFuncResultAccessInfo& object);

/* 
 *  AP_FuncResultAccessInfo
 */
struct AP_FuncResultAccessInfo {
    AP_FunctionID   functionID;
    Four            planNo;                 // plan index to which the aggregate function is applied
    Two             funcIndex;              // aggregate function index
};

OQL_OutStream& operator<<(OQL_OutStream& os, AP_FuncResultAccessInfo& object);

/*
 *  AP_ClassInfo
 */
enum {CLASSKIND_NONE, CLASSKIND_PERSISTENT, CLASSKIND_TEMPORARY, CLASSKIND_NULL_AGGRFUNC_ONLY, CLASSKIND_SORTSTREAM};
struct AP_ClassInfo {
    Four            classKind;
    union {
        Four        classId;
        Four        tempFileNum;
    };
};

OQL_OutStream& operator<<(OQL_OutStream& os, AP_ClassInfo& object);

/*
 *  AP_ColNoMapPool
 */
struct AP_ColNoMapElement {
    Two     offset;
};

OQL_OutStream& operator<<(OQL_OutStream& os, AP_ColNoMapElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class  AP_ColNoMapPool       : public OQL_Pool<AP_ColNoMapElement> {
public:
	AP_ColNoMapPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<AP_ColNoMapElement>(memoryManager) {}
    char* name() { return "AP_ColNoMapPool"; }
};

#else


declare(OQL_Pool,AP_ColNoMapElement);
declare(OQL_PoolElements,AP_ColNoMapElement);

class  AP_ColNoMapPool       : public OQL_Pool(AP_ColNoMapElement) {
public:
	AP_ColNoMapPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(AP_ColNoMapElement)(memoryManager) {}
    char* name() { return "AP_ColNoMapPool"; }
};

#endif

/*
 *  AP_UsedColPool
 */
struct AP_UsedColElement {
    TypeID  typeId;         // what type can be applied to this?
    Four    length;
	Four	colNo;
};

OQL_OutStream& operator<<(OQL_OutStream& os, AP_UsedColElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class  AP_UsedColPool       : public OQL_Pool<AP_UsedColElement> {
public:
	AP_UsedColPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<AP_UsedColElement>(memoryManager) {}
    char* name() { return "AP_UsedColPool"; }
};

#else


declare(OQL_Pool,AP_UsedColElement);
declare(OQL_PoolElements,AP_UsedColElement);

class  AP_UsedColPool       : public OQL_Pool(AP_UsedColElement) {
public:
	AP_UsedColPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(AP_UsedColElement)(memoryManager) {}
    char* name() { return "AP_UsedColPool"; }
};

#endif

/*
 *  AP_MethodNoMapPool
 */
struct AP_MethodNoMapElement {
    Two     offset;
};

OQL_OutStream& operator<<(OQL_OutStream& os, AP_MethodNoMapElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class  AP_MethodNoMapPool       : public OQL_Pool<AP_MethodNoMapElement> {
public:
	AP_MethodNoMapPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<AP_MethodNoMapElement>(memoryManager) {}
    char* name() { return "AP_MethodNoMapPool"; }
};

#else


declare(OQL_Pool,AP_MethodNoMapElement);
declare(OQL_PoolElements,AP_MethodNoMapElement);

class  AP_MethodNoMapPool       : public OQL_Pool(AP_MethodNoMapElement) {
public:
	AP_MethodNoMapPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(AP_MethodNoMapElement)(memoryManager) {}
    char* name() { return "AP_MethodNoMapPool"; }
};

#endif

/*
 *  AP_UsedMethodPool
 */
struct AP_UsedMethodElement {
    TypeID  returnType;     // what type can be applied to this?
    Four    returnLength;
    void*   pMethod;
};

OQL_OutStream& operator<<(OQL_OutStream& os, AP_UsedMethodElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class  AP_UsedMethodPool       : public OQL_Pool<AP_UsedMethodElement> {
public:
	AP_UsedMethodPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<AP_UsedMethodElement>(memoryManager) {}
    char* name() { return "AP_UsedMethodPool"; }
};

#else


declare(OQL_Pool,AP_UsedMethodElement);
declare(OQL_PoolElements,AP_UsedMethodElement);

class  AP_UsedMethodPool       : public OQL_Pool(AP_UsedMethodElement) {
public:
	AP_UsedMethodPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(AP_UsedMethodElement)(memoryManager) {}
    char* name() { return "AP_UsedMethodPool"; }
};

#endif

/*
 *  AP_ProjectionListPool
 */
enum SortOrderType {SORTORDER_DESC, SORTORDER_ASC};
enum ProjectionType {PROJECTION_NONE, PROJECTION_UPDATE, PROJECTION_INSERT, PROJECTION_DELETE};

struct AP_InsertInfo {
	AP_ClassInfo				classInfo;
	AP_InsertValuePoolIndex		insertValueList;
};

struct AP_UpdateInfo {
	AP_ClassInfo				classInfo;
	AP_UpdateValuePoolIndex		updateValueList;
};

struct AP_DeleteInfo {
	AP_ClassInfo				classInfo;
	Boolean						isDeferredDelete;
};
	
struct AP_ProjectionListElement {
    AP_ProjectionPoolIndex      projectionInfo;
    Two                         nSortKeys;					// 0 if sort is not used
    Two                         sortKeys[MAX_NUM_KEYS];
    SortOrderType               sortAscDesc[MAX_NUM_KEYS];
    Four                        tempFileNum;				// -1 if not used 
	ProjectionType				projectionType;
	union {
		AP_UpdateInfo			updateInfo;
		AP_InsertInfo			insertInfo;
		AP_DeleteInfo			deleteInfo;
	};

    void init(ProjectionType projectionType = PROJECTION_NONE);
};

OQL_OutStream& operator<<(OQL_OutStream& os, AP_ProjectionListElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class  AP_ProjectionListPool : public OQL_Pool<AP_ProjectionListElement> {
public:
	AP_ProjectionListPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<AP_ProjectionListElement>(memoryManager) {}
    char* name() { return "AP_ProjectionListPool"; }
};

#else

declare(OQL_Pool,AP_ProjectionListElement);
declare(OQL_PoolElements,AP_ProjectionListElement);

class  AP_ProjectionListPool : public OQL_Pool(AP_ProjectionListElement) {
public:
	AP_ProjectionListPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(AP_ProjectionListElement)(memoryManager) {}
    char* name() { return "AP_ProjectionListPool"; }
};

#endif

/*
 *  AP_TempFileInfoPool
 *   used for searching binding element matched with aggrfunc's argument
 */
enum {
    TFS_KIND_NONE, TFS_KIND_PATHEXPR, TFS_KIND_AGGRFUNC, TFS_KIND_FUNC,
    TFS_KIND_VALUE, TFS_KIND_OPER, TFS_KIND_CONS
};

struct AP_TempFileInfoElement {
    TypeID      typeId;         // typeid of column
    Four        length;         // size of column
    Four        kind;
    union {
        PathExprPoolIndex  pathExpr;
        AggrFuncPoolIndex  aggrFunc;
        FunctionPoolIndex  func;
        ValuePoolIndex     value;
        OperatorStruct     oper;
        ConstructPoolIndex cons;
    };
};

OQL_OutStream& operator<<(OQL_OutStream& os, AP_TempFileInfoElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class  AP_TempFileInfoPool : public OQL_Pool<AP_TempFileInfoElement> {
public:
	AP_TempFileInfoPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<AP_TempFileInfoElement>(memoryManager) {}
    char* name() { return "AP_TempFileInfoPool"; }
};

#else

declare(OQL_Pool,AP_TempFileInfoElement);
declare(OQL_PoolElements,AP_TempFileInfoElement);

class  AP_TempFileInfoPool : public OQL_Pool(AP_TempFileInfoElement) {
public:
	AP_TempFileInfoPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(AP_TempFileInfoElement)(memoryManager) {}
    char* name() { return "AP_TempFileInfoPool"; }
};

#endif

/* 
 *  AP_ProjectionPool
 */
enum {PROJECTION_KIND_PATHEXPR,
      PROJECTION_KIND_AGGRFUNCRESULT,
      PROJECTION_KIND_FUNCRESULT,
      PROJECTION_KIND_FUNCEVAL,
      PROJECTION_KIND_VALUE,
      PROJECTION_KIND_OPER,
      PROJECTION_KIND_OID,
	  PROJECTION_KIND_EXPR
};
struct AP_ProjectionElement {
    TypeID  resultType;
    Four    resultLength;
    Four    projectionKind;
    union {
        AP_PathExprAccessInfo       pathExpr;
        AP_AggrFuncResultAccessInfo aggrFuncResult;
        AP_FuncResultAccessInfo     funcResult;
        AP_FuncEvalInfo             funcEval;
        AP_ValuePoolIndex           value;
        AP_OperatorStruct           oper;
        Four                        oid_of_plan;
		AP_ExprPoolIndex			expr;
    };
};

OQL_OutStream& operator<<(OQL_OutStream& os, AP_ProjectionElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class  AP_ProjectionPool       : public OQL_Pool<AP_ProjectionElement> {
public:
	AP_ProjectionPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<AP_ProjectionElement>(memoryManager) {}
    char* name() { return "AP_ProjectionPool"; }
};

#else


declare(OQL_Pool,AP_ProjectionElement);
declare(OQL_PoolElements,AP_ProjectionElement);

class  AP_ProjectionPool       : public OQL_Pool(AP_ProjectionElement) {
public:
	AP_ProjectionPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(AP_ProjectionElement)(memoryManager) {}
    char* name() { return "AP_ProjectionPool"; }
};

#endif

/*
 *  AP_ExprPool
 */
struct AP_ExprElement {
    Two     exprKind;
    TypeID  resultType;
    Four    resultLength;
    union {
        AP_PathExprAccessInfo       pathExpr;
        AP_AggrFuncResultAccessInfo aggrFuncResult;
        AP_FuncResultAccessInfo     funcResult;
        AP_FuncEvalInfo             funcEval;
        AP_ValuePoolIndex           value;
        AP_OperatorStruct           oper;
        AP_ConstructPoolIndex       cons;
    };
};

OQL_OutStream& operator<<(OQL_OutStream& os, AP_ExprElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class  AP_ExprPool       : public OQL_Pool<AP_ExprElement> {
public:
	AP_ExprPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<AP_ExprElement>(memoryManager) {}
    char* name() { return "AP_ExprPool"; }
};

#else

declare(OQL_Pool,AP_ExprElement);
declare(OQL_PoolElements,AP_ExprElement);

class  AP_ExprPool       : public OQL_Pool(AP_ExprElement) {
public:
	AP_ExprPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(AP_ExprElement)(memoryManager) {}
    char* name() { return "AP_ExprPool"; }
};

#endif

/*
 *  AP_CondListPool
 */
struct AP_CondListElement {
    AP_ExprPoolIndex    expr;
    Four                op1_indexType;      
    Four                op2_indexType;     
};

OQL_OutStream& operator<<(OQL_OutStream& os, AP_CondListElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class AP_CondListPool : public OQL_Pool<AP_CondListElement> {
public:
	AP_CondListPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<AP_CondListElement>(memoryManager) {}
    char* name() { return "AP_CondListPool"; }
};

#else

declare(OQL_Pool,AP_CondListElement);
declare(OQL_PoolElements,AP_CondListElement);

class AP_CondListPool : public OQL_Pool(AP_CondListElement) {
public:
	AP_CondListPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(AP_CondListElement)(memoryManager) {}
    char* name() { return "AP_CondListPool"; }
};

#endif

/*
 *  AP_AggrFuncElement
 */
struct AP_AggrFuncElement {
    AggrFunctionID       aggrFunctionID;
    AP_ArgumentPoolIndex argument;

    One                  distinctFlag;
    Four                 tempFileNum;   // meaningful when distinctFlag == TRUE
    AP_UsedColPoolIndex  usedColInfo;   // meaningful when distinctFlag == TRUE

    AggrFuncPoolIndex    srcAggrFunc;   // from what this AP_AggrFuncElement made
                                        // used for searching AP_AggrFuncElement by
                                        // original aggrFunc
};

OQL_OutStream& operator<<(OQL_OutStream& os, AP_AggrFuncElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class  AP_AggrFuncPool       : public OQL_Pool<AP_AggrFuncElement> {
public:
	AP_AggrFuncPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<AP_AggrFuncElement>(memoryManager) {}
    char* name() { return "AP_AggrFuncPool"; }
};

#else

declare(OQL_Pool,AP_AggrFuncElement);
declare(OQL_PoolElements,AP_AggrFuncElement);

class  AP_AggrFuncPool       : public OQL_Pool(AP_AggrFuncElement) {
public:
	AP_AggrFuncPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(AP_AggrFuncElement)(memoryManager) {}
    char* name() { return "AP_AggrFuncPool"; }
};

#endif

/*
 *  AP_BoolExpr
 */
struct AP_BoolExprElement {
	Four						 colNo;						
	TypeID						 type;						
	Four						 length;				
	Boolean						 canReadFromIndexCursor;	
    OOSQL_StorageManager::CompOp op;				
    AP_ExprPoolIndex             key;		
};

OQL_OutStream& operator<<(OQL_OutStream& os, AP_BoolExprElement& object);
OQL_OutStream& operator<<(OQL_OutStream& os, OOSQL_StorageManager::CompOp op);

#ifndef TEMPLATE_NOT_SUPPORTED

class  AP_BoolExprPool       : public OQL_Pool<AP_BoolExprElement> {
public:
	AP_BoolExprPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<AP_BoolExprElement>(memoryManager) {}
    char* name() { return "AP_BoolExprPool"; }
};

#else

declare(OQL_Pool,AP_BoolExprElement);
declare(OQL_PoolElements,AP_BoolExprElement);

class  AP_BoolExprPool       : public OQL_Pool(AP_BoolExprElement) {
public:
	AP_BoolExprPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(AP_BoolExprElement)(memoryManager) {}
    char* name() { return "AP_BoolExprPool"; }
};

#endif

/*
 *  AP_BoundCondInfo
 */
struct AP_BoundCondInfo {
    OOSQL_StorageManager::CompOp op;
    AP_ExprPoolIndex             key;
};

OQL_OutStream& operator<<(OQL_OutStream& os, AP_BoundCondInfo& object);

/*
 *  AP_IndexInfo
 */

struct AP_BtreeIndexCond {
    AP_BoundCondInfo        startBound;
    AP_BoundCondInfo        stopBound;
};
OQL_OutStream& operator<<(OQL_OutStream& os, AP_BtreeIndexCond& object);

struct AP_MlgfIndexCond
{
	Two					nKeys;				/* number of keys */

	AP_ExprPoolIndex	lowerBoundExpr;		/* lower bounds */
	AP_ExprPoolIndex	upperBoundExpr;		/* upper bounds */
	
	MLGF_HashValue		lowerBound[MLGF_MAXNUM_KEYS]; 
    MLGF_HashValue		upperBound[MLGF_MAXNUM_KEYS]; 

	Boolean				lowerBoundExprFlag[MLGF_MAXNUM_KEYS];
	Boolean				upperBoundExprFlag[MLGF_MAXNUM_KEYS];
};

OQL_OutStream& operator<<(OQL_OutStream& os, AP_MlgfIndexCond& object);

enum AP_SpatialOperandType {
    AP_SPATIAL_OPERAND_TYPE_NONE,
    AP_SPATIAL_OPERAND_TYPE_MBR,
    AP_SPATIAL_OPERAND_TYPE_PATHEXPR,
    AP_SPATIAL_OPERAND_TYPE_INDEX
    ,AP_SPATIAL_OPERAND_TYPE_OPER
};

struct AP_MlgfMbrIndexCond {
    OperatorID                  spatialOp;
    AP_SpatialOperandType       operandType;
    union {
        AP_MBRPoolIndex					mbr;
        AP_PathExprAccessInfo			pathExpr;
		OOSQL_StorageManager::IndexID   indexId;
		AP_OperatorStruct				oper;
    };
    AP_MBRPoolIndex             window;
	AP_IndexInfoPoolIndex tidJoinIndexInfo;
};

OQL_OutStream& operator<<(OQL_OutStream& os, AP_MlgfMbrIndexCond& object);

struct AP_TextIndexSubPlan {
    Four                        matchFuncNum;
    AP_TextIndexCondPoolIndex   textIndexCond;
};
OQL_OutStream& operator<<(OQL_OutStream& os, AP_TextIndexSubPlan& object);

enum {OP_INDEX_NONE, OP_INDEX_AND, OP_INDEX_OR};
struct AP_IndexOperatorInfo {
    Four                    operatorID;
    AP_IndexInfoPoolIndex   op1;
    AP_IndexInfoPoolIndex   op2;
};
OQL_OutStream& operator<<(OQL_OutStream& os, AP_IndexOperatorInfo& object);

// INDEXTYPE_NONE, INDEXTYPE_BTREE, INDEXTYPE_TEXT, INDEXTYPE_MLGF are defined in
// OQL_Common_Struct.hxx
struct AP_IndexScanInfo {
	Four							classId;	
	Two								colNo;		
    Four							indexType;
    OOSQL_StorageManager::IndexID   indexId;
    union {
        AP_BtreeIndexCond			btree;
        AP_TextIndexSubPlan			text;
        AP_MlgfIndexCond			mlgf;
        AP_MlgfMbrIndexCond			mlgfmbr;
    };
	AP_BoolExprPoolIndex			boolExprs;
	union {
		OOSQL_StorageManager::KeyDesc				btreeKeyDesc;
		OOSQL_StorageManager::MLGF_KeyDesc			mlgfKeyDesc;
		OOSQL_StorageManager::InvertedIndexDesc		invertedIndexKeyDesc;
	};
	Boolean readObjectValueFromIndexFlag;	
	Four    nCols;					
	struct {
		Four colNo;					
		Four type;								
		Four length;						
	} columns[MAX_NUM_EMBEDDEDATTRIBUTES];	
};
OQL_OutStream& operator<<(OQL_OutStream& os, AP_IndexScanInfo& object);

enum {INDEXINFO_NONE, INDEXINFO_OPERATOR, INDEXINFO_SCAN};
struct AP_IndexInfoElement {
    Four    nodeKind;
    union {
        AP_IndexOperatorInfo    oper;
        AP_IndexScanInfo        scan;
    };
};
OQL_OutStream& operator<<(OQL_OutStream& os, AP_IndexInfoElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class AP_IndexInfoPool : public OQL_Pool<AP_IndexInfoElement> {
public:
	AP_IndexInfoPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<AP_IndexInfoElement>(memoryManager) {}
    char* name() { return "AP_IndexInfoPool"; }
};

#else

declare(OQL_Pool,AP_IndexInfoElement);
declare(OQL_PoolElements,AP_IndexInfoElement);

class AP_IndexInfoPool : public OQL_Pool(AP_IndexInfoElement) {
public:
	AP_IndexInfoPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(AP_IndexInfoElement)(memoryManager) {}
    char* name() { return "AP_IndexInfoPool"; }
};

#endif

/*
 *  AP_TextIndecCondPool
 */
// OP_TEXT_ACCUMULATE, OP_TEXT_OR, OP_TEXT_AND, OP_TEXT_MINUS, 
// OP_TEXT_THRESHOLD, OP_TEXT_MULTPLY, OP_TEXT_MAX, OP_TEXT_NEAR are defined in 
// OQL_Common_Struct.hxx
enum {
	TEXTIR_SCAN_FORWARD = FORWARD,
	TEXTIR_SCAN_BACKWARD = BACKWARD,
	TEXTIR_SCAN_BACKWARD_NOORDERING = BACKWARD_NOORDERING,
	TEXTIR_SCAN_BACKWARD_ORDERING = BACKWARD_ORDERING
};
struct AP_TextIndexOperatorInfo {
    OperatorID                  operatorID;
    AP_TextIndexCondPoolIndex   op1;
    AP_TextIndexCondPoolIndex   op2;
	AP_TextIndexCondPoolIndex   op3;
	Four						scanDirection;	// TEXTIR_SCAN_FORWARD, TEXTIR_SCAN_BACKWARD
	Four						logicalIdHints;	// Skip postings to given logical Id
};
OQL_OutStream& operator<<(OQL_OutStream& os, AP_TextIndexOperatorInfo& object);

enum {KEYWORD_SEQ_SCAN, KEYWORD_IDX_SCAN, REVERSEKEYWORD_IDX_SCAN};
enum {TEXTIR_POSTINGKIND_NONE, TEXTIR_POSTINGKIND_WITHPOSITION, TEXTIR_POSTINGKIND_WITHOUTPOSITION};
struct AP_TextIndexKeywordInfo {
    Four                accessMethod;			// KEYWORD_SEQ_SCAN, KEYWORD_IDX_SCAN, REVERSEKEYWORD_IDX_SCAN
	Four				scanDirection;			// TEXTIR_SCAN_FORWARD, TEXTIR_SCAN_BACKWARD
    AP_StringPoolIndex  startBound;				// Keyword start condition
    AP_StringPoolIndex  stopBound;				// Keyword end condition
    Four                usedPostingKind;		// TEXTIR_POSTINGKIND_NONE, TEXTIR_POSTINGKIND_WITHPOSITION, TEXTIR_POSTINGKIND_WITHOUTPOSITION

    AP_StringPoolIndex  keywordWithWildChar;	

	Boolean				isPostingContainingTupleID;
	Boolean				isPostingContainingSentenceAndWordNum;
	Boolean				isPostingContainingByteOffset;

	Four							nPostings;
	OOSQL_StorageManager::TupleID	invertedIndexEntryTupleID;
};
OQL_OutStream& operator<<(OQL_OutStream& os, AP_TextIndexKeywordInfo& object);

enum {TEXTINDEXCOND_NONE, TEXTINDEXCOND_OPERATOR, TEXTINDEXCOND_KEYWORD,
      TEXTINDEXCOND_CONSTANT};
struct AP_TextIndexCondElement {
    Four    nodeKind;
    union {
        AP_TextIndexOperatorInfo    oper;
        AP_TextIndexKeywordInfo     keyword;
        AP_ValuePoolIndex           constant;
    };
};
OQL_OutStream& operator<<(OQL_OutStream& os, AP_TextIndexCondElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class AP_TextIndexCondPool : public OQL_Pool<AP_TextIndexCondElement> {
public:
	AP_TextIndexCondPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<AP_TextIndexCondElement>(memoryManager) {}
    char* name() { return "AP_TextIndexCondPool"; }
};

#else

declare(OQL_Pool,AP_TextIndexCondElement);
declare(OQL_PoolElements,AP_TextIndexCondElement);

class AP_TextIndexCondPool : public OQL_Pool(AP_TextIndexCondElement) {
public:
	AP_TextIndexCondPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(AP_TextIndexCondElement)(memoryManager) {}
    char* name() { return "AP_TextIndexCondPool"; }
};

#endif

/*
 *  AP_ArgumentPool
 */
struct AP_ArgumentElement {
    Four argumentKind;
    union {
        AP_PathExprAccessInfo       pathExpr;
        AP_AggrFuncResultAccessInfo aggrFuncResult;
        AP_FuncResultAccessInfo     funcResult;
        AP_TempFileAccessInfo       tempFileCol;
        AP_FuncEvalInfo             funcEval;
        AP_ValuePoolIndex           value;
        AP_DomainPoolIndex          domain;
        AP_ExprPoolIndex            expr;
        AP_TextIndexSubPlan         textIndexSubPlan;
    };
};

OQL_OutStream& operator<<(OQL_OutStream& os, AP_ArgumentElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class  AP_ArgumentPool       : public OQL_Pool<AP_ArgumentElement> {
public:
	AP_ArgumentPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<AP_ArgumentElement>(memoryManager) {}
    char* name() { return "AP_ArgumentPool"; }
};

#else

declare(OQL_Pool,AP_ArgumentElement);
declare(OQL_PoolElements,AP_ArgumentElement);

class  AP_ArgumentPool       : public OQL_Pool(AP_ArgumentElement) {
public:
	AP_ArgumentPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(AP_ArgumentElement)(memoryManager) {}
    char* name() { return "AP_ArgumentPool"; }
};

#endif

/*
 *  AP_UpdateValuePool
 */
struct AP_UpdateValueElement {
	Two						colNo;
	Boolean					isParam;	
	AP_ExprPoolIndex		expr;
	TypeID					type;
	Four					length;
	SeqValueType			seqValueType;
};

OQL_OutStream& operator<<(OQL_OutStream& os, AP_UpdateValueElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class AP_UpdateValuePool : public OQL_Pool<AP_UpdateValueElement> {
public:
	AP_UpdateValuePool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<AP_UpdateValueElement>(memoryManager) {}
    char* name() { return "AP_UpdateValuePool"; }
};

#else

declare(OQL_Pool,AP_UpdateValueElement);
declare(OQL_PoolElements,AP_UpdateValueElement);

class AP_UpdateValuePool : public OQL_Pool(AP_UpdateValueElement) {
public:
	AP_UpdateValuePool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(AP_UpdateValueElement)(memoryManager) {}
    char* name() { return "AP_UpdateValuePool"; }
};

#endif

/*
 *  AP_InsertValuePool
 */
struct AP_InsertValueElement {
	Two						colNo;
	Boolean					isParam;	
	AP_ValuePoolIndex		value;
	TypeID					type;
	Four					length;
	SeqValueType			seqValueType;
    AP_ExprPoolIndex           expr;
    PoolType                poolType;
};

OQL_OutStream& operator<<(OQL_OutStream& os, AP_InsertValueElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class AP_InsertValuePool : public OQL_Pool<AP_InsertValueElement> {
public:
	AP_InsertValuePool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<AP_InsertValueElement>(memoryManager) {}
    char* name() { return "AP_InsertValuePool"; }
};

#else

declare(OQL_Pool,AP_InsertValueElement);
declare(OQL_PoolElements,AP_InsertValueElement);

class AP_InsertValuePool : public OQL_Pool(AP_InsertValueElement) {
public:
	AP_InsertValuePool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(AP_InsertValueElement)(memoryManager) {}
    char* name() { return "AP_InsertValuePool"; }
};

#endif

/*
 *  CommonAP_Pool
 */
enum {
      CAP_JOINMETHOD_NONE,
      CAP_JOINMETHOD_CARTESIAN_PRODUCT, 
      CAP_JOINMETHOD_OUTERMOST_CLASS,
      // implicit join methods
      CAP_JOINMETHOD_IMPLICIT_FORWARD, 
      CAP_JOINMETHOD_IMPLICIT_BACKWARD,
      // explicit join methods
      CAP_JOINMETHOD_EXPLICIT_NESTEDLOOP,
      CAP_JOINMETHOD_EXPLICIT_SORTMERGE
};  

enum {CAP_ACCESSMETHOD_NONE, 
      CAP_ACCESSMETHOD_OIDFETCH, 
      CAP_ACCESSMETHOD_SEQSCAN,
      CAP_ACCESSMETHOD_BTREE_INDEXSCAN, 
      CAP_ACCESSMETHOD_TEXT_INDEXSCAN,  
      CAP_ACCESSMETHOD_MLGF_INDEXSCAN,   
	  CAP_ACCESSMETHOD_MLGF_MBR_INDEXSCAN 
};

struct CommonAP_Element {
    // information about the class to access
    AP_ClassInfo                classInfo;

    // join information (both implicit and explicit join
    Four                        joinMethod;         
    Four                        joinClass;          // in plan element number 
    
    One                         isInnermostPlanElem;

    // only for implicit join
    One                         isMethod;           // TRUE or FALSE
    union {
        Two                     implicitJoinColNo;  
        Two                     implicitJoinMethodNo;   
    };

    // access method information
    Four                        accessMethod;
    AP_IndexInfoPoolIndex       indexInfo;
	Boolean						isUseOid;
	OOSQL_StorageManager::OID	oid;	

    // information about the predicates to check
    AP_CondListPoolIndex        condNodes;

    // subclass information for traversing the class hierarchy
    AP_SubClassPoolIndex        subClassInfo;

    // used column(attribute) information
    AP_ColNoMapPoolIndex        colNoMap;
    AP_UsedColPoolIndex         usedColInfo;

    // used method(derived attribute) information
    AP_MethodNoMapPoolIndex     methodNoMap;
    AP_UsedMethodPoolIndex      usedMethodInfo;

    // the number of used match functions in this plan element
    Four                        nUsedFuncMatch;
    
    // aggregate function information
    AP_AggrFuncPoolIndex        aggrFuncInfo;

    // group by and having information
    Two                         nGrpByKeys;
    Two                         grpByKeys[MAX_NUM_KEYS];
    Two                         noHavConds;
    AP_CondListPoolIndex        havCondNodes;

    // projection information
    //  project column information
    //  sort information
    //  output file information
	// or Update, Insert, Delete Information
    AP_ProjectionListPoolIndex  projectionList;

    // select DISTINCT flag
    One                         selDistinctFlag;

	Four						limitStart;
	Four						limitCount;

    void init();
    friend OQL_OutStream& operator<<(OQL_OutStream& os, CommonAP_Element& object);
};

OQL_OutStream& operator<<(OQL_OutStream& os, CommonAP_Element& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class  CommonAP_Pool       : public OQL_Pool<CommonAP_Element> {
public:
	CommonAP_Pool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<CommonAP_Element>(memoryManager) {}
    char* name() { return "CommonAP_Pool"; }
};

#else


declare(OQL_Pool,CommonAP_Element);
declare(OQL_PoolElements,CommonAP_Element);

class  CommonAP_Pool       : public OQL_Pool(CommonAP_Element) {
public:
	CommonAP_Pool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(CommonAP_Element)(memoryManager) {}
    char* name() { return "CommonAP_Pool"; }
};

#endif

/*
 *  ClientAP_Pool
 */
struct ClientAP_Element {
    AP_ClassInfo					classInfo;

    Four							accessMethod;
    OOSQL_StorageManager::IndexID   indexId;

    AP_CondListPoolIndex			condNodes;

    AP_SubClassPoolIndex			subClassInfo;

    AP_ColNoMapPoolIndex			colNoMap;
    AP_UsedColPoolIndex				usedColInfo;

    AP_MethodNoMapPoolIndex			methodNoMap;
    AP_UsedMethodPoolIndex			usedMethodInfo;

    Two								nSortKeys;
    Two								sortKeys[MAX_NUM_KEYS];
    One								sortAscDesc[MAX_NUM_KEYS];

    AP_AggrFuncPoolIndex			aggrFuncInfo;

    Two								nGrpByKeys;
    Two								grpByKeys[MAX_NUM_KEYS];
    Two								noHavConds;
    AP_CondListPoolIndex			havCondNodes;

    AP_ProjectionListPoolIndex		projectionList;

    One								selDistinctFlag;
    Four							tempFileNum;
};

OQL_OutStream& operator<<(OQL_OutStream& os, ClientAP_Element& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class  ClientAP_Pool       : public OQL_Pool<ClientAP_Element> {
public:
	ClientAP_Pool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<ClientAP_Element>(memoryManager) {}
    char* name() { return "ClientAP_Pool"; }
};

#else


declare(OQL_Pool,ClientAP_Element);
declare(OQL_PoolElements,ClientAP_Element);

class  ClientAP_Pool       : public OQL_Pool(ClientAP_Element) {
public:
	ClientAP_Pool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(ClientAP_Element)(memoryManager) {}
    char* name() { return "ClientAP_Pool"; }
};

#endif

/*
 *  AttributeInfoPool
 */

struct AttributeInfoElement {
	StringPoolIndex		attributeName;
	TypeID				domain;
	Four				length;
	Four				inheritedFrom;
	Four				complexType;
	Four				referencingClassId;	
};

OQL_OutStream& operator<<(OQL_OutStream& os, AttributeInfoElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class  AttributeInfoPool : public OQL_Pool<AttributeInfoElement> {
public:
	AttributeInfoPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<AttributeInfoElement>(memoryManager) {}
    char* name() { return "AttributeInfoPool"; }
};

#else

declare(OQL_Pool,AttributeInfoElement);
declare(OQL_PoolElements,AttributeInfoElement);

class  AttributeInfoPool : public OQL_Pool(AttributeInfoElement) {
public:
	AttributeInfoPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(AttributeInfoElement)(memoryManager) {}
    char* name() { return "AttributeInfoPool"; }
};

#endif

/*
 *	ArgumentTypePool (for function, procedure, method definition)
 */

struct ArgumentTypeElement {
	StringPoolIndex		argumentName;
	TypeID				argumentType;
	StringPoolIndex     argumentTypeName;
	Four                argumentTypeLength;
	ParameterMode		parameterMode;
	Boolean				asLocator;
};

OQL_OutStream& operator<<(OQL_OutStream& os, ArgumentTypeElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class ArgumentTypePool : public OQL_Pool<ArgumentTypeElement> {
public:
	ArgumentTypePool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<ArgumentTypeElement>(memoryManager) {}
    char* name() { return "ArgumentTypePool"; }
};

#else


declare(OQL_Pool,ArgumentTypeElement);
declare(OQL_PoolElements,ArgumentTypeElement);

class ArgumentTypePool : public OQL_Pool(ArgumentTypeElement) {
public:
	ArgumentTypePool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(ArgumentTypeElement)(memoryManager) {}
    char* name() { return "ArgumentTypePool"; }
};

#endif

/*
 *  MethodInfoPool
 */

struct MethodInfoElement {
	StringPoolIndex			functionName;
	ArgumentTypePoolIndex   argumentTypeList;
	ArgumentTypeElement		returnType;
	StringPoolIndex			specificName;
	StringPoolIndex			externalName;
	Boolean					deterministic;
	Boolean					externalAction;
	Boolean					fenced;
	Boolean					nullCall;
	StringPoolIndex			language;
	StringPoolIndex			parameterStyle;
	Boolean					scratchPad;
	Boolean					finalCall;
	Boolean					allowParallel;
	Boolean					dbInfo;
};

OQL_OutStream& operator<<(OQL_OutStream& os, MethodInfoElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class  MethodInfoPool : public OQL_Pool<MethodInfoElement> {
public:
	MethodInfoPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<MethodInfoElement>(memoryManager) {}
    char* name() { return "MethodInfoPool"; }
};

#else

declare(OQL_Pool,MethodInfoElement);
declare(OQL_PoolElements,MethodInfoElement);

class  MethodInfoPool : public OQL_Pool(MethodInfoElement) {
public:
	MethodInfoPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(MethodInfoElement)(memoryManager) {}
    char* name() { return "MethodInfoPool"; }
};

#endif

/*
 *  KeyInfoPool
 */

struct KeyInfoElement {
	Four	keyColNo;
};

OQL_OutStream& operator<<(OQL_OutStream& os, KeyInfoElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class  KeyInfoPool : public OQL_Pool<KeyInfoElement> {
public:
	KeyInfoPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<KeyInfoElement>(memoryManager) {}
    char* name() { return "KeyInfoPool"; }
};

#else

declare(OQL_Pool,KeyInfoElement);
declare(OQL_PoolElements,KeyInfoElement);

class  KeyInfoPool : public OQL_Pool(KeyInfoElement) {
public:
	KeyInfoPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(KeyInfoElement)(memoryManager) {}
    char* name() { return "KeyInfoPool"; }
};

#endif

/*
 *	SuperClassPool
 */
struct SuperClassElement {
	StringPoolIndex			className;
};

OQL_OutStream& operator<<(OQL_OutStream& os, SuperClassElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class SuperClassPool : public OQL_Pool<SuperClassElement> {
public:
	SuperClassPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<SuperClassElement>(memoryManager) {}
    char* name() { return "SuperClassPool"; }
};

#else


declare(OQL_Pool,SuperClassElement);
declare(OQL_PoolElements,SuperClassElement);

class SuperClassPool : public OQL_Pool(SuperClassElement) {
public:
	SuperClassPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(SuperClassElement)(memoryManager) {}
    char* name() { return "SuperClassPool"; }
};

#endif


/*
 *  DBCommandPool
 */
enum DBCommandType {
					DBCOMMAND_ALTER_TABLE,
					DBCOMMAND_CREATE_SEQUENCE,
					DBCOMMAND_DROP_SEQUENCE,
					DBCOMMAND_CREATE_TABLE, DBCOMMAND_CREATE_INDEX, 
                    DBCOMMAND_DROP_TABLE, DBCOMMAND_DROP_INDEX,
					DBCOMMAND_DEFINE_POSTING_STRUCTURE,
					DBCOMMAND_CREATE_FUNCTION, DBCOMMAND_DROP_FUNCTION,
					DBCOMMAND_CREATE_PROCEDURE, DBCOMMAND_DROP_PROCEDURE,
					DBCOMMAND_CALL_PROCEDURE
};


struct AlterTableInfo {
	StringPoolIndex			className;
	AttributeInfoPoolIndex	addColList;
	AttributeInfoPoolIndex	dropColList;
};

struct CreateSequenceInfo {
	StringPoolIndex			sequenceName;
	Four					startWith;
};

struct DropSequenceInfo {
	StringPoolIndex			sequenceName;
};

struct CreateTableInfo {
	StringPoolIndex			className;
	SuperClassPoolIndex		superClasses;
	AttributeInfoPoolIndex	attributeList;
	MethodInfoPoolIndex		methodList;
	KeyInfoPoolIndex		keyList;
	Boolean					isTemporary;
};

struct CreateIndexInfo {
	Four					indexType;	// INDEXTYPE_BTREE, INDEXTYPE_MLGF, INDEXTYPE_TEXT
	Boolean					isUnique;
	Boolean					isClustering;
	StringPoolIndex			indexName;
	StringPoolIndex			className;
	KeyInfoPoolIndex		keyList;
};

struct DropTableInfo {
	StringPoolIndex		className;
};

struct DropIndexInfo {
	StringPoolIndex		indexName;
};

struct DefinePostingStructure {
	StringPoolIndex className;
	StringPoolIndex attributeName;
	Boolean			isContainingTupleID;
	Boolean			isContainingSentenceAndWordNum;
	Boolean			isContainingByteOffset;
	Two				nEmbeddedAttributes;
	Two				embeddedAttrNo[MAX_NUM_EMBEDDEDATTRIBUTES];
};

struct CreateFunctionInfo {
	StringPoolIndex			functionName;
	ArgumentTypePoolIndex   argumentTypeList;
	ArgumentTypeElement		returnType;
	StringPoolIndex			specificName;
	StringPoolIndex			externalName;
	Boolean					deterministic;
	Boolean					externalAction;
	Boolean					fenced;
	Boolean					nullCall;
	StringPoolIndex			language;
	StringPoolIndex			parameterStyle;
	Boolean					scratchPad;
	Boolean					finalCall;
	Boolean					allowParallel;
	Boolean					dbInfo;
};

struct DropFunctionInfo {
	StringPoolIndex			functionName;
	ArgumentTypePoolIndex   argumentTypeList;
	Boolean					specific;
};

struct CreateProcedureInfo {
	StringPoolIndex			procedureName;
	ArgumentTypePoolIndex   argumentTypeList;
	StringPoolIndex			specificName;
	StringPoolIndex			externalName;
	int                     resultsets;
	Boolean					deterministic;
	Boolean					fenced;
	Boolean					nullCall;
	StringPoolIndex			language;
	StringPoolIndex			parameterStyle;
};

struct DropProcedureInfo {
	StringPoolIndex			procedureName;
	ArgumentTypePoolIndex   argumentTypeList;
	Boolean					specific;
};

struct CallProcedureInfo {
	StringPoolIndex			procedureName;
	AP_ArgumentPoolIndex    argumentList;
};

struct DBCommandElement {
    DBCommandType	command;
	union {
		AlterTableInfo			alterTableInfo;
		CreateSequenceInfo		createSequenceInfo;
		DropSequenceInfo		dropSequenceInfo;
		CreateTableInfo			createTableInfo;
		CreateIndexInfo			createIndexInfo;
		DropTableInfo			dropTableInfo;
		DropIndexInfo			dropIndexInfo;
		DefinePostingStructure	definePostingStructureInfo;
		CreateFunctionInfo		createFunctionInfo;
		DropFunctionInfo		dropFunctionInfo;
		CreateProcedureInfo		createProcedureInfo;
		DropProcedureInfo		dropProcedureInfo;
		CallProcedureInfo		callProcedureInfo;
	};
};

OQL_OutStream& operator<<(OQL_OutStream& os, DBCommandElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class  DBCommandPool : public OQL_Pool<DBCommandElement> {
public:
	DBCommandPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<DBCommandElement>(memoryManager) {}
    char* name() { return "DBCommandPool"; }
};

#else

declare(OQL_Pool,DBCommandElement);
declare(OQL_PoolElements,DBCommandElement);

class  DBCommandPool : public OQL_Pool(DBCommandElement) {
public:
	DBCommandPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(DBCommandElement)(memoryManager) {}
    char* name() { return "AP_ArgumentPool"; }
};

#endif

#endif  /* __OQL_PLAN_STRUCT_H__ */
