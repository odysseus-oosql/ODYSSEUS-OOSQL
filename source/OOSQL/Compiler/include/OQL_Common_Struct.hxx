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

#ifndef _OQL_COMMON_STRUCT_H_
#define _OQL_COMMON_STRUCT_H_

#include "OOSQL_Common.h"
#include "OQL_OutStream.hxx"
#include "OOSQL_StorageManager.hxx"
#include "OQL_Pools.hxx"

/* 
 *	Forward Structure Declaration
 */
struct PathExprElement;
struct ArgumentElement;
struct StructureElement;
struct ExprElement;
struct ValueElement;
struct AggrFuncElement;
struct FunctionElement;
struct DomainElement;
struct CollectionElement;
struct MBRElement; 
struct ObjectElement;
struct ConstructElement;
struct MemberElement;
struct SubClassElement;
struct JoinInfoElement;
struct PathExprInfoElement;
struct CondListElement;
struct DateElement;
struct TimeElement;
struct TimestampElement;
struct IntervalElement;
struct UpdateValueElement;
struct InsertValueElement;
struct CommonAP_Element;
struct QGNode;
struct AP_IndexInfoElement;
struct AP_CondListElement;
struct LimitClauseElemet;

/*
 *  Declare pool indexes
 */
#define BooleanPoolIndex		 PoolIndexBase
#define NilPoolIndex			 PoolIndexBase

#ifndef TEMPLATE_NOT_SUPPORTED

#define PathExprPoolIndex        PoolIndex<PathExprElement>
#define ArgumentPoolIndex        PoolIndex<ArgumentElement>
#define StructurePoolIndex       PoolIndex<StructureElement>
#define ExprPoolIndex            PoolIndex<ExprElement>
#define ValuePoolIndex           PoolIndex<ValueElement>
#define RealPoolIndex            PoolIndex<float>
#define IntegerPoolIndex         PoolIndex<long>
#define StringPoolIndex          PoolIndex<char>
#define AggrFuncPoolIndex        PoolIndex<AggrFuncElement>
#define FunctionPoolIndex        PoolIndex<FunctionElement>
#define DomainPoolIndex          PoolIndex<DomainElement>
#define CollectionPoolIndex      PoolIndex<CollectionElement>
#define MBRPoolIndex             PoolIndex<MBRElement> 
#define ObjectPoolIndex          PoolIndex<ObjectElement>
#define ConstructPoolIndex       PoolIndex<ConstructElement>
#define MemberPoolIndex          PoolIndex<MemberElement>
#define SubClassPoolIndex        PoolIndex<SubClassElement>
#define JoinInfoPoolIndex        PoolIndex<JoinInfoElement>
#define PathExprInfoPoolIndex    PoolIndex<PathExprInfoElement>
#define CondListPoolIndex        PoolIndex<CondListElement>
#define DatePoolIndex			 PoolIndex<DateElement>
#define TimePoolIndex			 PoolIndex<TimeElement>
#define TimestampPoolIndex		 PoolIndex<TimestampElement>
#define IntervalPoolIndex		 PoolIndex<IntervalElement>
#define UpdateValuePoolIndex	 PoolIndex<UpdateValueElement>
#define InsertValuePoolIndex	 PoolIndex<InsertValueElement>

#define PlanPoolIndex			 PoolIndex<CommonAP_Element>
#define QGNodePoolIndex          PoolIndex<QGNode>
#define AP_IndexInfoPoolIndex    PoolIndex<AP_IndexInfoElement>
#define AP_CondListPoolIndex     PoolIndex<AP_CondListElement>
#define LimitClausePoolIndex     PoolIndex<LimitClauseElement>

#else   /* TEMPLATE_NOT_SUPPORTED */

#define PathExprPoolIndex        PoolIndex(PathExprElement)
#define ArgumentPoolIndex        PoolIndex(ArgumentElement)
#define StructurePoolIndex       PoolIndex(StructureElement)
#define ExprPoolIndex            PoolIndex(ExprElement)
#define ValuePoolIndex           PoolIndex(ValueElement)
#define RealPoolIndex            PoolIndex(float)
#define IntegerPoolIndex         PoolIndex(long)
#define StringPoolIndex          PoolIndex(char)
#define AggrFuncPoolIndex        PoolIndex(AggrFuncElement)
#define FunctionPoolIndex        PoolIndex(FunctionElement)
#define DomainPoolIndex          PoolIndex(DomainElement)
#define CollectionPoolIndex      PoolIndex(CollectionElement)
#define MBRPoolIndex             PoolIndex(MBRElement) 
#define ObjectPoolIndex          PoolIndex(ObjectElement)
#define ConstructPoolIndex       PoolIndex(ConstructElement)
#define MemberPoolIndex          PoolIndex(MemberElement)
#define SubClassPoolIndex        PoolIndex(SubClassElement)
#define JoinInfoPoolIndex        PoolIndex(JoinInfoElement)
#define PathExprInfoPoolIndex    PoolIndex(PathExprInfoElement)
#define CondListPoolIndex        PoolIndex(CondListElement)
#define DatePoolIndex			 PoolIndex(DateElement)
#define TimePoolIndex			 PoolIndex(TimeElement)
#define TimestampPoolIndex		 PoolIndex(TimestampElement)
#define IntervalPoolIndex		 PoolIndex(IntervalElement)
#define UpdateValuePoolIndex	 PoolIndex(UpdateValueElement)
#define InsertValuePoolIndex	 PoolIndex(InsertValueElement)

#define PlanPoolIndex			 PoolIndex(CommonAP_Element)
#define QGNodePoolIndex          PoolIndex(QGNode)
#define AP_IndexInfoPoolIndex    PoolIndex(AP_IndexInfoElement)
#define AP_CondListPoolIndex     PoolIndex(AP_CondListElement)
#define LimitClausePoolIndex     PoolIndex(LimitClauseElement)

declare(PoolIndex,PathExprElement);
declare(PoolIndex,ArgumentElement);
declare(PoolIndex,StructureElement);
declare(PoolIndex,ExprElement);
declare(PoolIndex,ValueElement);
declare(PoolIndex,float);
declare(PoolIndex,int);
declare(PoolIndex,char);
declare(PoolIndex,AggrFuncElement);
declare(PoolIndex,FunctionElement);
declare(PoolIndex,DomainElement);
declare(PoolIndex,CollectionElement);
declare(PoolIndex,MBRElement); 
declare(PoolIndex,ObjectElement);
declare(PoolIndex,ConstructElement);
declare(PoolIndex,MemberElement);
declare(PoolIndex,SubClassElement);
declare(PoolIndex,JoinInfoElement);
declare(PoolIndex,PathExprInfoElement);
declare(PoolIndex,CondListElement);
declare(PoolIndex,DateElement);
declare(PoolIndex,TimeElement);
declare(PoolIndex,TimestampElement);
declare(PoolIndex,IntervalElement);
declare(PoolIndex,UpdateValueElement);
declare(PoolIndex,InsertValueElement);

declare(PoolIndex,CommonAP_Element);
declare(PoolIndex,QGNode);
declare(PoolIndex,AP_IndexInfoElement);
declare(PoolIndex,AP_CondListElement);
declare(PoolIndex,LimitClauseElement);

#endif  /* TEMPLATE_NOT_SUPPORTED */

/*
 *  Declare pool elements
 */
#ifndef TEMPLATE_NOT_SUPPORTED

#define PathExprPoolElements        OQL_PoolElements<PathExprElement>
#define ArgumentPoolElements        OQL_PoolElements<ArgumentElement>
#define StructurePoolElements       OQL_PoolElements<StructureElement>
#define ExprPoolElements            OQL_PoolElements<ExprElement>
#define ValuePoolElements           OQL_PoolElements<ValueElement>
#define RealPoolElements            OQL_PoolElements<float>
#define IntegerPoolElements         OQL_PoolElements<long>
#define StringPoolElements          OQL_PoolElements<char>
#define AggrFuncPoolElements        OQL_PoolElements<AggrFuncElement>
#define FunctionPoolElements        OQL_PoolElements<FunctionElement>
#define DomainPoolElements          OQL_PoolElements<DomainElement>
#define CollectionPoolElements      OQL_PoolElements<CollectionElement>
#define MBRPoolElements             OQL_PoolElements<MBRElement> 
#define ObjectPoolElements          OQL_PoolElements<ObjectElement>
#define ConstructPoolElements       OQL_PoolElements<ConstructElement>
#define MemberPoolElements          OQL_PoolElements<MemberElement>
#define SubClassPoolElements        OQL_PoolElements<SubClassElement>
#define JoinInfoPoolElements        OQL_PoolElements<JoinInfoElement>
#define PathExprInfoPoolElements    OQL_PoolElements<PathExprInfoElement>
#define CondListPoolElements        OQL_PoolElements<CondListElement>
#define DatePoolElements			OQL_PoolElements<DateElement>
#define TimePoolElements			OQL_PoolElements<TimeElement>
#define TimestampPoolElements		OQL_PoolElements<TimestampElement>
#define IntervalPoolElements		OQL_PoolElements<IntervalElement>
#define UpdateValuePoolElements		OQL_PoolElements<UpdateValueElement>
#define InsertValuePoolElements		OQL_PoolElements<InsertValueElement>
#define LimitClausePoolElements     OQL_PoolElements<LimitClauseElement>

#else   /* TEMPLATE_NOT_SUPPORTED */

#define PathExprPoolElements        OQL_PoolElements(PathExprElement)
#define ArgumentPoolElements        OQL_PoolElements(ArgumentElement)
#define StructurePoolElements       OQL_PoolElements(StructureElement)
#define ExprPoolElements            OQL_PoolElements(ExprElement)
#define ValuePoolElements           OQL_PoolElements(ValueElement)
#define RealPoolElements            OQL_PoolElements(float)
#define IntegerPoolElements         OQL_PoolElements(long)
#define StringPoolElements          OQL_PoolElements(char)
#define AggrFuncPoolElements        OQL_PoolElements(AggrFuncElement)
#define FunctionPoolElements        OQL_PoolElements(FunctionElement)
#define DomainPoolElements          OQL_PoolElements(DomainElement)
#define CollectionPoolElements      OQL_PoolElements(CollectionElement)
#define MBRPoolElements             OQL_PoolElements(MBRElement) 
#define ObjectPoolElements          OQL_PoolElements(ObjectElement)
#define ConstructPoolElements       OQL_PoolElements(ConstructElement)
#define MemberPoolElements          OQL_PoolElements(MemberElement)
#define SubClassPoolElements        OQL_PoolElements(SubClassElement)
#define JoinInfoPoolElements        OQL_PoolElements(JoinInfoElement)
#define PathExprInfoPoolElements    OQL_PoolElements(PathExprInfoElement)
#define CondListPoolElements        OQL_PoolElements(CondListElement)
#define DatePoolElements			OQL_PoolElements(DateElement)
#define TimePoolElements			OQL_PoolElements(TimeElement)
#define TimestampPoolElements		OQL_PoolElements(TimestampElement)
#define IntervalPoolElements		OQL_PoolElements(IntervalElement)
#define UpdateValuePoolElements		OQL_PoolElements(UpdateValueElement)
#define InsertValuePoolElements		OQL_PoolElements(InsertValueElement)
#define LimitClausePoolElements     OQL_PoolElements(LimitClauseElement)

#endif  /* TEMPLATE_NOT_SUPPORTED */

// type definition used in OQL_ASTtoGDS
enum PoolType {
    PT_NONE, PT_SELLISTPOOL, PT_TARGETLISTPOOL, PT_EXPRPOOL, PT_GROUPBYLISTPOOL,
    PT_QGNODEPOOL, PT_PATHEXPRPOOL, PT_ARGUMENTPOOL, PT_STRUCTUREPOOL,
    PT_VALUEPOOL, PT_INTPOOL, PT_REALPOOL, PT_STRINGPOOL, PT_LLISTNODEPOOL,
    PT_FROMATTRPOOL, PT_AGGRFUNCPOOL, PT_FUNCPOOL, PT_DOMAINPOOL, PT_COLLECTIONPOOL,
    PT_MBRPOOL, PT_BOOLPOOL, PT_CONSTRUCTPOOL, PT_NILPOOL, PT_OBJECTPOOL
};

enum SeqValueType {
	NO_SEQ,
	SEQ_CURRVAL,
	SEQ_NEXTVAL
};

/* 
 *  CollectionInfo
 */
struct CollectionInfo {
    CataClassInfo   classInfo;
};

OQL_OutStream& operator<<(OQL_OutStream& os, CollectionInfo& object);

/*
 *  ObjectPool
 */
struct ObjectElement {
    CataClassInfo       classInfo;
    StructurePoolIndex  structure;
};

OQL_OutStream& operator<<(OQL_OutStream& os, ObjectElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class  ObjectPool : public OQL_Pool<ObjectElement> {
public:
	ObjectPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<ObjectElement>(memoryManager) {}
    char* name() { return "ObjectPool"; }
};

#else

declare(OQL_Pool,ObjectElement);
declare(OQL_PoolElements,ObjectElement);

class  ObjectPool : public OQL_Pool(ObjectElement) {
public:
	ObjectPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(ObjectElement)(memoryManager) {}
    char* name() { return "ObjectPool"; }
};

#endif
/*
 *  PathExprPool
 */
struct MethodStruct {
    CataMethodInfo    methodInfo;
    ArgumentPoolIndex argument;
};

struct AttrStruct {
    CataAttrInfo      attrInfo;
};

enum {PATHEXPR_KIND_NONE, 
      PATHEXPR_KIND_METHOD, 
      PATHEXPR_KIND_ATTR, 
      PATHEXPR_KIND_FUNC, 
      PATHEXPR_KIND_CLASS, 
      PATHEXPR_KIND_OBJECT, 
      PATHEXPR_KIND_OID,                    // PATHEXPR_KIND_OID type is used in AP_PathExprElement
	  PATHEXPR_KIND_LOGICALID		
};     
                                                   

struct PathExprElement {
    CataClassInfo classInfo;
    Four          fromAttrKind;
    union {
        AttrStruct          attr;
        MethodStruct        method;
        FunctionPoolIndex   func;
        StringPoolIndex     aliasName;
        ObjectPoolIndex     object;
    };
    One             domainSubstFlag;        
    CataClassInfo   substitutedClass;
    One             typeCastingFlag;        
    CataClassInfo   typeCastedClass;
    PlanPoolIndex   planNo;
    QGNodePoolIndex qgNodeNo;
    Four            pathExprKind;
};

OQL_OutStream& operator<<(OQL_OutStream& os, PathExprElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class  PathExprPool       : public OQL_Pool<PathExprElement> {
public:
	PathExprPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<PathExprElement>(memoryManager) {}
    char* name() { return "PathExprPool"; }
};

#else


declare(OQL_Pool,PathExprElement);
declare(OQL_PoolElements,PathExprElement);

class  PathExprPool       : public OQL_Pool(PathExprElement) {
public:
	PathExprPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(PathExprElement)(memoryManager) {}
    char* name() { return "PathExprPool"; }
};

#endif

/*
 *  DomainPool
 */
struct DomainElement {
    CataClassInfo domainClass;
    One           starFlag;
};

OQL_OutStream& operator<<(OQL_OutStream& os, DomainElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class  DomainPool         : public OQL_Pool<DomainElement> {
public:
	DomainPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<DomainElement>(memoryManager) {}
    char* name() { return "DomainPool"; }
};

#else


declare(OQL_Pool,DomainElement);
declare(OQL_PoolElements,DomainElement);

class  DomainPool         : public OQL_Pool(DomainElement) {
public:
	DomainPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(DomainElement)(memoryManager) {}
    char* name() { return "DomainPool"; }
};

#endif

/*
 *  FunctionPool
 */
enum FunctionID {
    FUNCTION_NONE, FUNCTION_GEO_NEAREST, FUNCTION_GEO_BOUNDARY, FUNCTION_GEO_INTERIOR, FUNCTION_GEO_BUFFER, 
    FUNCTION_GEO_UNION, FUNCTION_GEO_INTERSECT, FUNCTION_GEO_DIFFERENCE, FUNCTION_GEO_DISTANCE, 
    FUNCTION_GEO_AREA, FUNCTION_GEO_LENGTH, 
    FUNCTION_TEXTIR_WEIGHT, FUNCTION_TEXTIR_NMATCH, FUNCTION_TEXTIR_MATCH, FUNCTION_USER_DEFINED
};
struct FunctionElement {
    FunctionID        functionID;
	Four              userDefinedFunctionID;
    TypeID            returnType;
    Four              returnLength;
    ArgumentPoolIndex argument;
};


OQL_OutStream& operator<<(OQL_OutStream& os, FunctionElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class  FunctionPool           : public OQL_Pool<FunctionElement> {
public:
	FunctionPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<FunctionElement>(memoryManager) {}
    char* name() { return "FunctionPool"; }
};

#else

declare(OQL_Pool,FunctionElement);
declare(OQL_PoolElements,FunctionElement);

class  FunctionPool           : public OQL_Pool(FunctionElement) {
public:
	FunctionPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(FunctionElement)(memoryManager) {}
    char* name() { return "FunctionPool"; }
};

#endif

/* 
 *  ArgumentPool
 */
enum {
      ARGUMENT_KIND_PATHEXPR, 
      ARGUMENT_KIND_VALUE, 
      ARGUMENT_KIND_FUNC, 
      ARGUMENT_KIND_DOMAIN, 
      ARGUMENT_KIND_AGGRFUNC, 
      ARGUMENT_KIND_EXPR,
      ARGUMENT_KIND_TEMPFILECOL,    // used in AP_ArgumentElement
      ARGUMENT_KIND_AGGRFUNCRESULT, // used in AP_ArgumentElement
      ARGUMENT_KIND_FUNCRESULT,     // used in AP_ArgumentElement
      ARGUMENT_KIND_FUNCEVAL,       // used in AP_ArgumentElement
      ARGUMENT_KIND_TEXTIR_SUBPLAN  // used in AP_ArgumentElement
};
struct ArgumentElement {
    Four argumentKind;
    union {
        PathExprPoolIndex pathExpr;
        ValuePoolIndex    value;
        FunctionPoolIndex func;
        DomainPoolIndex   domain;
        AggrFuncPoolIndex aggrFunc;
        ExprPoolIndex     expr;
    };
	TypeID type;
	Four   length;
};

OQL_OutStream& operator<<(OQL_OutStream& os, ArgumentElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class  ArgumentPool      : public OQL_Pool<ArgumentElement> {
public:
	ArgumentPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<ArgumentElement>(memoryManager) {}
    char* name() { return "ArgumentPool"; }
};

#else


declare(OQL_Pool,ArgumentElement);
declare(OQL_PoolElements,ArgumentElement);

class  ArgumentPool      : public OQL_Pool(ArgumentElement) {
public:
	ArgumentPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(ArgumentElement)(memoryManager) {}
    char* name() { return "ArgumentPool"; }
};

#endif

/*
 *  AggrFuncPool
 */
enum AggrFunctionID {
    AGGRFUNC_NONE,
    AGGRFUNC_COUNTALL,
    AGGRFUNC_COUNT,
    AGGRFUNC_SUM,
    AGGRFUNC_MIN,
    AGGRFUNC_MAX,
    AGGRFUNC_AVG
};

struct AggrFuncElement {
    AggrFunctionID    aggrFunctionID;
    One               distinctFlag;
    ArgumentPoolIndex argument;
};

OQL_OutStream& operator<<(OQL_OutStream& os, AggrFuncElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class  AggrFuncPool      : public OQL_Pool<AggrFuncElement> {
public:
	AggrFuncPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<AggrFuncElement>(memoryManager) {}
    char* name() { return "AggrFuncPool"; }
};

#else


declare(OQL_Pool,AggrFuncElement);
declare(OQL_PoolElements,AggrFuncElement);

class  AggrFuncPool      : public OQL_Pool(AggrFuncElement) {
public:
	AggrFuncPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(AggrFuncElement)(memoryManager) {}
    char* name() { return "AggrFuncPool"; }
};

#endif

/*
 *  StructurePool
 */
enum {
    STRUCTURE_KIND_PATHEXPR, STRUCTURE_KIND_VALUE, STRUCTURE_KIND_FUNC,
    STRUCTURE_KIND_AGGRFUNC
};
struct StructureElement {
    StringPoolIndex   name;
    ArgumentPoolIndex structure;
};

OQL_OutStream& operator<<(OQL_OutStream& os, StructureElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class  StructurePool      : public OQL_Pool<StructureElement> {
public:
	StructurePool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<StructureElement>(memoryManager) {}
    char* name() { return "StructurePool"; }
};

#else


declare(OQL_Pool,StructureElement);
declare(OQL_PoolElements,StructureElement);

class  StructurePool      : public OQL_Pool(StructureElement) {
public:
	StructurePool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(StructureElement)(memoryManager) {}
    char* name() { return "StructurePool"; }
};

#endif

/*
 *  ExprPool
 */
enum OperatorID {
    OP_NONE, OP_UNARY_MINUS, OP_ABS, OP_PLUS, OP_MINUS, OP_MULTIPLY, OP_DIVIDE, OP_MOD, OP_STRING_CONCAT,
    OP_LIKE, OP_EQ, OP_NE, OP_GT, OP_LT, OP_GE, OP_LE,
	OP_ISNULL, OP_ISNOTNULL,
    OP_NOT, OP_AND, OP_OR, OP_INTERSECT, OP_UNION, OP_EXCEPT, OP_IN,
    OP_GEO_NORTH, OP_GEO_SOUTH, OP_GEO_EAST, OP_GEO_WEST, OP_GEO_COVER, OP_GEO_COVERED,
    OP_GEO_CONTAIN, OP_GEO_CONTAINED, OP_GEO_DISJOINT, OP_GEO_EQUAL, OP_GEO_MEET, OP_GEO_OVERLAP,
    OP_TEXTIR_ACCUMULATE, OP_TEXTIR_OR, OP_TEXTIR_AND, OP_TEXTIR_MINUS, OP_TEXTIR_THRESHOLD, OP_TEXTIR_MULTIPLY, 
	OP_TEXTIR_MAX, OP_TEXTIR_NEAR, OP_TEXTIR_NEAR_WITH_ORDER, OP_TEXTIR_BETWEEN,
	OP_OGIS_STARTMARKER,
	OP_OGIS_GEOMETRYFT,
	OP_OGIS_POINTFT,
	OP_OGIS_LINESTRINGFT,
	OP_OGIS_POLYGONFT,
	OP_OGIS_MULTIPOINTFT,
	OP_OGIS_MULTILINESTRINGFT,
	OP_OGIS_MULTIPOLYGONFT,
	OP_OGIS_GEOMETRYCOLLECTIONFT,
	OP_OGIS_GEOMETRYFB,
	OP_OGIS_POINTFB,
	OP_OGIS_LINESTRINGFB,
	OP_OGIS_POLYGONFB,
	OP_OGIS_MULTIPOINTFB,
	OP_OGIS_MULTILINESTRINGFB,
	OP_OGIS_MULTIPOLYGONFB,
	OP_OGIS_GEOMETRYCOLLECTIONFB,
	OP_OGIS_ASTEXT,
	OP_OGIS_ASBINARY,
	OP_OGIS_DIMENSION,
	OP_OGIS_GEOMETRYTYPE,
	OP_OGIS_SRID,
	OP_OGIS_BOUNDARY,
	OP_OGIS_LENGTH,
	OP_OGIS_X,
	OP_OGIS_Y,
	OP_OGIS_AREA,
	OP_OGIS_NUMGEOMETRIES,
	OP_OGIS_NUMPOINTS,
	OP_OGIS_NUMINTERIORRINGS,
	OP_OGIS_ISEMPTY,
	OP_OGIS_ISSIMPLE,
	OP_OGIS_ISCLOSED,
	OP_OGIS_ISRING,
	OP_OGIS_CONTAINS,
	OP_OGIS_CROSSES,
	OP_OGIS_DISJOINT,
	OP_OGIS_EQUALS,
	OP_OGIS_INTERSECTS,
	OP_OGIS_OVERLAPS,
	OP_OGIS_RELATED,
	OP_OGIS_TOUCHES,
	OP_OGIS_WITHIN,
	OP_OGIS_DIFFERENCE,
	OP_OGIS_INTERSECTION,
	OP_OGIS_SYMDIFFERENCE,
	OP_OGIS_UNION,
	OP_OGIS_DISTANCE,
	OP_OGIS_ENVELOPE,
	OP_OGIS_BUFFER,
	OP_OGIS_CONVEXHULL,
	OP_OGIS_EXTERIORRING,
	OP_OGIS_INTERIORRINGN,
	OP_OGIS_CENTRIOD,
	OP_OGIS_STARTPOINT,
	OP_OGIS_ENDPOINT,
	OP_OGIS_POINTONSURFACE,
	OP_OGIS_POINTN,
	OP_OGIS_GEOMETRYN,
	OP_OGIS_ENDMARKER
	,OP_OGIS_KNN
};

enum OperatorType {
    ARITHMETIC_OPERATION, STRING_OPERATION,    
	NULLCHECK_OPERATION,
	COLLECTION_OPERATION, OID_OPERATION, OBJECT_OPERATION, TEXT_OPERATION
};

struct OperatorStruct {
    OperatorID      operatorId;
    ExprPoolIndex   operand1;
    ExprPoolIndex   operand2;
	ExprPoolIndex	operand3;		
    OperatorType    operatorType;
    #ifdef ENABLE_OPENGIS_OPTIMIZATION
    Four            UDFNo;          /* Unique identifier that is assigned to a user-defined function used in a query */
    Boolean         isConstant;     /* True if the result of this function is constant */
    Boolean         isDeterministic;     /* True if the result of this function is deterministic */
    #endif
};

enum OperandType {
    EXPR_OPERANDTYPE_CONTAIN_METHOD,
    EXPR_OPERANDTYPE_CONTAIN_NOMETHOD
};

OQL_OutStream& operator<<(OQL_OutStream& os, TypeID& typeID);

enum ComplexType {
    COMPLEX_TYPE_SIMPLE,
    COMPLEX_TYPE_SET,  COMPLEX_TYPE_BAG, 
    COMPLEX_TYPE_LIST, COMPLEX_TYPE_ARRAY
};
enum {EXPR_KIND_PATHEXPR, 
      EXPR_KIND_AGGRFUNC, 
      EXPR_KIND_FUNCTION,
      EXPR_KIND_OPER, 
      EXPR_KIND_VALUE, 
      EXPR_KIND_CONS,
      EXPR_KIND_AGGRFUNCRESULT, // used in AP_ArgumentElement
      EXPR_KIND_FUNCRESULT,     // used in AP_ArgumentElement
      EXPR_KIND_FUNCEVAL        // used in AP_ArgumentElement
};
struct ExprElement {
    Four exprKind;
    union {
        PathExprPoolIndex  pathExpr;
        AggrFuncPoolIndex  aggrFunc;
        FunctionPoolIndex  func;
        ValuePoolIndex     value;
        OperatorStruct     oper;
        ConstructPoolIndex cons;
    };
    OperandType      operandType;
    Four             refCount;
    TypeID           resultType;
    ComplexType      complexType;
    Four             resultLength;
};

OQL_OutStream& operator<<(OQL_OutStream& os, OperatorStruct& object);
OQL_OutStream& operator<<(OQL_OutStream& os, ExprElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class  ExprPool      : public OQL_Pool<ExprElement> {
public:
	ExprPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<ExprElement>(memoryManager) {}
    char* name() { return "ExprPool"; }
};

#else


declare(OQL_Pool,ExprElement);
declare(OQL_PoolElements,ExprElement);

class  ExprPool      : public OQL_Pool(ExprElement) {
public:
	ExprPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(ExprElement)(memoryManager) {}
    char* name() { return "ExprPool"; }
};

#endif

/*
 *  CollectionPool
 */
enum CollectionTypeID {
    COLTYPE_SET, COLTYPE_BAG, COLTYPE_LIST, COLTYPE_ARRAY
};

enum ColElementTypeID {
    COLELMTYPE_INTEGER, COLELMTYPE_REAL, COLELMTYPE_STRING, COLELMTYPE_BOOL, 
    COLELMTYPE_MBR
};

struct CollectionElement {
    CollectionTypeID  collectionType;
    ColElementTypeID  elementType;
    ValuePoolIndex    value;
};

OQL_OutStream& operator<<(OQL_OutStream& os, CollectionElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class  CollectionPool : public OQL_Pool<CollectionElement> {
public:
	CollectionPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<CollectionElement>(memoryManager) {}
    char* name() { return "CollectionPool"; }
};

#else


declare(OQL_Pool,CollectionElement);
declare(OQL_PoolElements,CollectionElement);

class  CollectionPool : public OQL_Pool(CollectionElement) {
public:
	CollectionPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(CollectionElement)(memoryManager) {}
    char* name() { return "CollectionPool"; }
};

#endif

/*
 *  MBRPool
 */
struct MBRElement {
    IntegerPoolIndex   x1;
    IntegerPoolIndex   y1;
    IntegerPoolIndex   x2;
    IntegerPoolIndex   y2;
};

OQL_OutStream& operator<<(OQL_OutStream& os, MBRElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class  MBRPool        : public OQL_Pool<MBRElement> {
public:
	MBRPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<MBRElement>(memoryManager) {}
    char* name() { return "MBRPool"; }
};

#else


declare(OQL_Pool,MBRElement);
declare(OQL_PoolElements,MBRElement);

class  MBRPool        : public OQL_Pool(MBRElement) {
public:
	MBRPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(MBRElement)(memoryManager) {}
    char* name() { return "MBRPool"; }
};

#endif

/*
 *  ValuePool
 */
enum {VALUE_KIND_STRING, VALUE_KIND_INTEGER, VALUE_KIND_REAL, 
      VALUE_KIND_BOOL, VALUE_KIND_NIL, VALUE_KIND_MBR,
	  VALUE_KIND_DATE, VALUE_KIND_TIME, VALUE_KIND_TIMESTAMP,
	  VALUE_KIND_INTERVAL, VALUE_KIND_TEXT, VALUE_KIND_COMPLEX};
enum {TEXT_UPDATE_MODE_IMMEDIATE, TEXT_UPDATE_MODE_DEFERRED};
struct ValueElement {
    Four valueKind;
    union {
        StringPoolIndex     string;
        IntegerPoolIndex    integer;
        RealPoolIndex       real;
        BooleanPoolIndex    boolean;
        NilPoolIndex        nil;
        MBRPoolIndex        mbr;
		DatePoolIndex		date;
		TimePoolIndex		time;
		TimestampPoolIndex	timestamp;
		IntervalPoolIndex	interval;
		StringPoolIndex     text;
		ValuePoolIndex		complex;
    };
	Four					textUpdateMode;		// IMMEDIATE OR DEFERRED
};

OQL_OutStream& operator<<(OQL_OutStream& os, ValueElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class  ValuePool      : public OQL_Pool<ValueElement> {
public:
	ValuePool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<ValueElement>(memoryManager) {}
    char* name() { return "ValuePool"; }
};

#else


declare(OQL_Pool,ValueElement);
declare(OQL_PoolElements,ValueElement);

class  ValuePool      : public OQL_Pool(ValueElement) {
public:
	ValuePool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(ValueElement)(memoryManager) {}
    char* name() { return "ValuePool"; }
};

#endif

/*
 *  RealPool
 */
#ifndef TEMPLATE_NOT_SUPPORTED

class  RealPool      : public OQL_Pool<float> {
public:
	RealPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<float>(memoryManager) {}
    char* name() { return "RealPool"; }
};

#else


declare(OQL_Pool,float);
declare(OQL_PoolElements,float);

class  RealPool      : public OQL_Pool(float) {
public:
	RealPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(float)(memoryManager) {}
    char* name() { return "RealPool"; }
};

#endif

/* 
 *  IntegerPool
 */
#ifndef TEMPLATE_NOT_SUPPORTED

class  IntegerPool: public OQL_Pool<long> {
public:
	IntegerPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<long>(memoryManager) {}
    char* name() { return "IntegerPool"; }
};
#else

declare(OQL_Pool,long);
declare(OQL_PoolElements,long);

class  IntegerPool: public OQL_Pool(long) {
public:
	IntegerPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(long)(memoryManager) {}
    char* name() { return "IntegerPool"; }
};

#endif

/* 
 *  StringPool
 */
#ifndef TEMPLATE_NOT_SUPPORTED

class  StringPool      : public OQL_Pool<char> {
public:
	StringPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<char>(memoryManager) {}
    char* name() { return "StringPool"; }

    // override OQL_Pool<char>'s <<
    friend OQL_OutStream& operator<<(OQL_OutStream& os, StringPool& object);
};

#else


declare(OQL_Pool,char);
declare(OQL_PoolElements,char);

class  StringPool      : public OQL_Pool(char) {
public:
	StringPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(char)(memoryManager) {}
    char* name() { return "StringPool"; }

    // override OQL_Pool<char>'s <<
    friend OQL_OutStream& operator<<(OQL_OutStream& os, StringPool& object);
};

OQL_OutStream& operator<<(OQL_OutStream& os, StringPool& object);

#endif

/* 
 *  StringIndexPool
 */
#ifndef TEMPLATE_NOT_SUPPORTED

class  StringIndexPool      : public OQL_Pool<int> {
public:
	StringIndexPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<int>(memoryManager) {}
    char* name() { return "StringIndexPool"; }
};

#else

class  StringIndexPool      : public OQL_Pool(int) {
public:
	StringIndexPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(int)(memoryManager) {}
    char* name() { return "StringIndexPool"; }
};

#endif

/*
 *  DatePool
 */
struct DateElement {
	Two_Invariable year;
	Two_Invariable month;
	Two_Invariable day;  
};

OQL_OutStream& operator<<(OQL_OutStream& os, DateElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class  DatePool      : public OQL_Pool<DateElement> {
public:
	DatePool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<DateElement>(memoryManager) {}
    char* name() { return "DatePool"; }
};

#else

declare(OQL_Pool,DateElement);
declare(OQL_PoolElements,DateElement);

class  DatePool      : public OQL_Pool(DateElement) {
public:
	DatePool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(DateElement)(memoryManager) {}
    char* name() { return "DatePool"; }
};

#endif

/*
 *  TimePool
 */
struct TimeElement {
	Two_Invariable   hour;
	Two_Invariable   minute;
	float second;
};

OQL_OutStream& operator<<(OQL_OutStream& os, TimeElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class  TimePool      : public OQL_Pool<TimeElement> {
public:
	TimePool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<TimeElement>(memoryManager) {}
    char* name() { return "TimePool"; }
};

#else

declare(OQL_Pool,TimeElement);
declare(OQL_PoolElements,TimeElement);

class  TimePool      : public OQL_Pool(TimeElement) {
public:
	TimePool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(TimeElement)(memoryManager) {}
    char* name() { return "TimePool"; }
};

#endif

/*
 *  TimestampPool
 */
struct TimestampElement {
	Two_Invariable   year;
	Two_Invariable   month;
	Two_Invariable   day;  
	Two_Invariable   hour;
	Two_Invariable   minute;
	float second;
};

OQL_OutStream& operator<<(OQL_OutStream& os, TimestampElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class  TimestampPool      : public OQL_Pool<TimestampElement> {
public:
	TimestampPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<TimestampElement>(memoryManager) {}
    char* name() { return "TimestampPool"; }
};

#else

declare(OQL_Pool,TimestampElement);
declare(OQL_PoolElements,TimestampElement);

class  TimestampPool      : public OQL_Pool(TimestampElement) {
public:
	TimestampPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(TimestampElement)(memoryManager) {}
    char* name() { return "TimestampPool"; }
};

#endif

/*
 *  IntervalPool
 */
struct IntervalElement {
	Two_Invariable   year;
	Two_Invariable   month;
	Two_Invariable   day;  
	Two_Invariable   hour;
	Two_Invariable   minute;
	float second;
};

OQL_OutStream& operator<<(OQL_OutStream& os, IntervalElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class  IntervalPool      : public OQL_Pool<IntervalElement> {
public:
	IntervalPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<IntervalElement>(memoryManager) {}
    char* name() { return "IntervalPool"; }
};

#else

declare(OQL_Pool,IntervalElement);
declare(OQL_PoolElements,IntervalElement);

class  IntervalPool      : public OQL_Pool(IntervalElement) {
public:
	IntervalPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(IntervalElement)(memoryManager) {}
    char* name() { return "IntervalPool"; }
};

#endif

/*
 *  ConstructPool
 */
enum {CONSTRUCT_KIND_OBJECT, CONSTRUCT_KIND_STRUCTURE, CONSTRUCT_KIND_COLLECTION};
struct ConstructElement {
    Four consKind;
    union {
        ObjectPoolIndex     object;
        StructurePoolIndex  structure;
        CollectionPoolIndex collection;
    };
};

OQL_OutStream& operator<<(OQL_OutStream& os, ConstructElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class ConstructPool : public OQL_Pool<ConstructElement> {
public:
	ConstructPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<ConstructElement>(memoryManager) {}
    char* name() { return "ConstructPool"; }
};

#else


declare(OQL_Pool,ConstructElement);
declare(OQL_PoolElements,ConstructElement);

class ConstructPool : public OQL_Pool(ConstructElement) {
public:
	ConstructPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(ConstructElement)(memoryManager) {}
    char* name() { return "ConstructPool"; }
};

#endif


/*
 *  MemberPool
 */
enum {MEMBER_KIND_NONE, MEMBER_KIND_ATTR, MEMBER_KIND_METHOD};
struct MemberElement {
    Four    memberKind;
    union {
        AttrStruct   attr;
        MethodStruct method;
    };
    MemberPoolIndex nextMember;
};

OQL_OutStream& operator<<(OQL_OutStream& os, MemberElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class MemberPool : public OQL_Pool<MemberElement> {
public:
	MemberPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<MemberElement>(memoryManager) {}
    char* name() { return "MemberPool"; }
};

#else


declare(OQL_Pool,MemberElement);
declare(OQL_PoolElements,MemberElement);

class MemberPool : public OQL_Pool(MemberElement) {
public:
	MemberPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(MemberElement)(memoryManager) {}
    char* name() { return "MemberPool"; }
};

#endif

/*
 *  SubClassElement
 */
struct SubClassElement {
    CataClassInfo           subClassInfo;
    
    Four                    classId;        // used in plan
    Four                    accessMethod;   // used in plan
    AP_IndexInfoPoolIndex   indexInfo;      // used in plan
    AP_CondListPoolIndex    condNodes;      // used in plan
};

OQL_OutStream& operator<<(OQL_OutStream& os, SubClassElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class SubClassPool : public OQL_Pool<SubClassElement> {
public:
	SubClassPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<SubClassElement>(memoryManager) {}
    char* name() { return "SubClassPool"; }
};

#else


declare(OQL_Pool,SubClassElement);
declare(OQL_PoolElements,SubClassElement);

class SubClassPool : public OQL_Pool(SubClassElement) {
public:
	SubClassPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(SubClassElement)(memoryManager) {}
    char* name() { return "SubClassPool"; }
};

#endif

/*
 *  JoinInfoPool
 */
struct JoinInfoElement {
    QGNodePoolIndex     joinClass;
    JoinInfoPoolIndex   nextJoinInfo;
};

OQL_OutStream& operator<<(OQL_OutStream& os, JoinInfoElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class JoinInfoPool : public OQL_Pool<JoinInfoElement> {
public:
	JoinInfoPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<JoinInfoElement>(memoryManager) {}
    char* name() { return "JoinInfoPool"; }
};

#else


declare(OQL_Pool,JoinInfoElement);
declare(OQL_PoolElements,JoinInfoElement);

class JoinInfoPool : public OQL_Pool(JoinInfoElement) {
public:
	JoinInfoPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(JoinInfoElement)(memoryManager) {}
    char* name() { return "JoinInfoPool"; }
};

#endif

/*
 *  PathExprInfoPool
 */
struct PathExprInfoElement {
    PathExprPoolIndex     pathExpr;
    PathExprInfoPoolIndex nextPathExprInfo;
};

OQL_OutStream& operator<<(OQL_OutStream& os, PathExprInfoElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class PathExprInfoPool : public OQL_Pool<PathExprInfoElement> {
public:
	PathExprInfoPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<PathExprInfoElement>(memoryManager) {}
    char* name() { return "PathExprInfoPool"; }
};

#else


declare(OQL_Pool,PathExprInfoElement);
declare(OQL_PoolElements,PathExprInfoElement);

class PathExprInfoPool : public OQL_Pool(PathExprInfoElement) {
public:
	PathExprInfoPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(PathExprInfoElement)(memoryManager) {}
    char* name() { return "PathExprInfoPool"; }
};

#endif

/*
 *  CondListPool
 */
enum { 
	INDEXTYPE_NONE		= 1,
	INDEXTYPE_BTREE		= 2,
	INDEXTYPE_TEXT		= 4,
	INDEXTYPE_MLGF		= 8,   
	INDEXTYPE_MLGF_MBR	= 16
};

struct CondListElement {
    ExprPoolIndex     expr;
    Four              op1_indexType;  
    Four              op2_indexType;
    CondListPoolIndex nextCondInfo;
};

OQL_OutStream& operator<<(OQL_OutStream& os, CondListElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class CondListPool : public OQL_Pool<CondListElement> {
public:
	CondListPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<CondListElement>(memoryManager) {}
    char* name() { return "CondListPool"; }
};

#else

declare(OQL_Pool,CondListElement);
declare(OQL_PoolElements,CondListElement);

class CondListPool : public OQL_Pool(CondListElement) {
public:
	CondListPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(CondListElement)(memoryManager) {}
    char* name() { return "CondListPool"; }
};

#endif

/*
 *  UpdateValuePool
 */
struct UpdateValueElement {
	CataAttrInfo		attrInfo;
	Boolean				isParam;
	ExprPoolIndex		expr;
	TypeID				type;
	Four				length;
	SeqValueType		seqValueType;
};

OQL_OutStream& operator<<(OQL_OutStream& os, UpdateValueElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class UpdateValuePool : public OQL_Pool<UpdateValueElement> {
public:
	UpdateValuePool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<UpdateValueElement>(memoryManager) {}
    char* name() { return "UpdateValuePool"; }
};

#else

declare(OQL_Pool,UpdateValueElement);
declare(OQL_PoolElements,UpdateValueElement);

class UpdateValuePool : public OQL_Pool(UpdateValueElement) {
public:
	UpdateValuePool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(UpdateValueElement)(memoryManager) {}
    char* name() { return "UpdateValuePool"; }
};

#endif

/*
 *  InsertValuePool
 */
struct InsertValueElement {
	CataAttrInfo		attrInfo;
	Boolean				isParam;
	ValuePoolIndex		value;
	TypeID				type;
	Four				length;
	SeqValueType		seqValueType;
	ExprPoolIndex		expr;
	PoolType        	poolType;	
};


OQL_OutStream& operator<<(OQL_OutStream& os, InsertValueElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class InsertValuePool : public OQL_Pool<InsertValueElement> {
public:
	InsertValuePool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<InsertValueElement>(memoryManager) {}
    char* name() { return "InsertValuePool"; }
};

#else

declare(OQL_Pool,InsertValueElement);
declare(OQL_PoolElements,InsertValueElement);

class InsertValuePool : public OQL_Pool(InsertValueElement) {
public:
	InsertValuePool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(InsertValueElement)(memoryManager) {}
    char* name() { return "InsertValuePool"; }
};

#endif

/*
 *  LimitClausePool
 */
struct LimitClauseElement {
	Four	limitStart;
	Four	limitCount;
};

OQL_OutStream& operator<<(OQL_OutStream& os, LimitClauseElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class LimitClausePool : public OQL_Pool<LimitClauseElement> {
public:
	LimitClausePool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<LimitClauseElement>(memoryManager) {}
    char* name() { return "LimitClausePool"; }
};

#else

declare(OQL_Pool,LimitClauseElement);
declare(OQL_PoolElements,LimitClauseElement);

class LimitClausePool : public OQL_Pool(LimitClauseElement) {
public:
	LimitClausePool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(LimitClauseElement)(memoryManager) {}
    char* name() { return "LimitClausePool"; }
};

#endif

#endif  /*_OQL_COMMON_STRUCT_H_*/
