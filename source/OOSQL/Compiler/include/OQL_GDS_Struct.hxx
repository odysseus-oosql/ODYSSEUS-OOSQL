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

#ifndef __OQL_GDS_STRUCT_H__
#define __OQL_GDS_STRUCT_H__

#include "OQL_OutStream.hxx"
#include "OOSQL_Common.h"
#include "OQL_Pools.hxx"
#include "OQL_Common_Struct.hxx"
#include "OOSQL_StorageManager.hxx"

/* 
 *	Forward Structure Declaration
 */
struct SelectQueryElement;
struct UpdateQueryElement;
struct InsertQueryElement;
struct DeleteQueryElement;
struct SelListElement;
struct OrderByListElement;
struct TargetListElement;
struct GroupByListElement;
struct QGNode;

/*
 *  Declare pool indexes
 */
#ifndef TEMPLATE_NOT_SUPPORTED

#define SelectQueryPoolIndex	 PoolIndex<SelectQueryElement>
#define UpdateQueryPoolIndex	 PoolIndex<UpdateQueryElement>
#define InsertQueryPoolIndex	 PoolIndex<InsertQueryElement>
#define DeleteQueryPoolIndex	 PoolIndex<DeleteQueryElement>
#define SelListPoolIndex         PoolIndex<SelListElement>
#define OrderByListPoolIndex     PoolIndex<OrderByListElement>
#define TargetListPoolIndex      PoolIndex<TargetListElement>
#define GroupByListPoolIndex     PoolIndex<GroupByListElement>
#define QGNodePoolIndex          PoolIndex<QGNode>

#else   /* TEMPLATE_NOT_SUPPORTED */

#define SelectQueryPoolIndex	 PoolIndex(SelectQueryElement)
#define UpdateQueryPoolIndex	 PoolIndex(UpdateQueryElement)
#define InsertQueryPoolIndex	 PoolIndex(InsertQueryElement)
#define DeleteQueryPoolIndex	 PoolIndex(DeleteQueryElement)
#define SelListPoolIndex         PoolIndex(SelListElement)
#define OrderByListPoolIndex     PoolIndex(OrderByListElement)
#define TargetListPoolIndex      PoolIndex(TargetListElement)
#define GroupByListPoolIndex     PoolIndex(GroupByListElement)

declare(PoolIndex,SelectQueryElement);
declare(PoolIndex,UpdateQueryElement);
declare(PoolIndex,InsertQueryElement);
declare(PoolIndex,DeleteQueryElement);
declare(PoolIndex,SelListElement);
declare(PoolIndex,OrderByListElement);
declare(PoolIndex,TargetListElement);
declare(PoolIndex,GroupByListElement);

#endif  /* TEMPLATE_NOT_SUPPORTED */

/*
 *  Declare pool elements
 */
#ifndef TEMPLATE_NOT_SUPPORTED

#define SelectQueryPoolElements		OQL_PoolElements<SelectQueryElement>
#define UpdateQueryPoolElements		OQL_PoolElements<UpdateQueryElement>
#define InsertQueryPoolElements		OQL_PoolElements<InsertQueryElement>
#define DeleteQueryPoolElements		OQL_PoolElements<DeleteQueryElement>
#define SelListPoolElements         OQL_PoolElements<SelListElement>
#define OrderByListPoolElements     OQL_PoolElements<OrderByListElement>
#define TargetListPoolElements      OQL_PoolElements<TargetListElement>
#define GroupByListPoolElements     OQL_PoolElements<GroupByListElement>
#define QGNodePoolElements          OQL_PoolElements<QGNode>

#else   /* TEMPLATE_NOT_SUPPORTED */

#define SelectQueryPoolElements		OQL_PoolElements(SelectQueryElement)
#define UpdateQueryPoolElements		OQL_PoolElements(UpdateQueryElement)
#define InsertQueryPoolElements		OQL_PoolElements(InsertQueryElement)
#define DeleteQueryPoolElements		OQL_PoolElements(DeleteQueryElement)
#define SelListPoolElements         OQL_PoolElements(SelListElement)
#define OrderByListPoolElements     OQL_PoolElements(OrderByListElement)
#define TargetListPoolElements      OQL_PoolElements(TargetListElement)
#define GroupByListPoolElements     OQL_PoolElements(GroupByListElement)
#define QGNodePoolElements          OQL_PoolElements(QGNode)

#endif  /* TEMPLATE_NOT_SUPPORTED */

/*
 *  SelectQueryPool
 */
struct SelectQueryElement {
	SelListPoolIndex     selList;
    Four                 selListType;
    TargetListPoolIndex  targetList;
    ExprPoolIndex        whereCond;
    GroupByListPoolIndex groupByList;
    ExprPoolIndex        havingCond;
    OrderByListPoolIndex orderByList;
    QGNodePoolIndex      queryGraph;
	LimitClausePoolIndex limitClause;
};

OQL_OutStream& operator<<(OQL_OutStream& os, SelectQueryElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class  SelectQueryPool : public OQL_Pool<SelectQueryElement> {
public:
	SelectQueryPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<SelectQueryElement>(memoryManager) {}
    char* name() { return "SelectQueryPool"; }
};

#else

declare(OQL_Pool,SelectQueryElement);
declare(OQL_PoolElements,SelectQueryElement);

class  SelectQueryPool : public OQL_Pool(SelectQueryElement) {
public:
	SelectQueryPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(SelectQueryElement)(memoryManager) {}
    char* name() { return "SelectQueryPool"; }
};

#endif

/*
 *  InsertQueryPool
 */
struct InsertQueryElement {
	SelListPoolIndex		selList;
    Four					selListType;
    TargetListPoolIndex		targetList;
    ExprPoolIndex			whereCond;
    GroupByListPoolIndex	groupByList;
    ExprPoolIndex			havingCond;
    OrderByListPoolIndex	orderByList;
	QGNodePoolIndex			queryGraph;
	CataClassInfo			insertTarget;
	InsertValuePoolIndex	insertValueList;
};

OQL_OutStream& operator<<(OQL_OutStream& os, InsertQueryElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class  InsertQueryPool : public OQL_Pool<InsertQueryElement> {
public:
	InsertQueryPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<InsertQueryElement>(memoryManager) {}
    char* name() { return "InsertQueryPool"; }
};

#else

declare(OQL_Pool,InsertQueryElement);
declare(OQL_PoolElements,InsertQueryElement);

class  InsertQueryPool : public OQL_Pool(InsertQueryElement) {
public:
	InsertQueryPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(InsertQueryElement)(memoryManager) {}
    char* name() { return "InsertQueryPool"; }
};

#endif

/*
 *  UpdateQueryPool
 */
struct UpdateQueryElement {
	TargetListPoolIndex		targetList;
	UpdateValuePoolIndex	updateValueList;
	ExprPoolIndex			whereCond;
	QGNodePoolIndex			queryGraph;
};

OQL_OutStream& operator<<(OQL_OutStream& os, UpdateQueryElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class  UpdateQueryPool : public OQL_Pool<UpdateQueryElement> {
public:
	UpdateQueryPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<UpdateQueryElement>(memoryManager) {}
    char* name() { return "UpdateQueryPool"; }
};

#else

declare(OQL_Pool,UpdateQueryElement);
declare(OQL_PoolElements,UpdateQueryElement);

class  UpdateQueryPool : public OQL_Pool(UpdateQueryElement) {
public:
	UpdateQueryPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(UpdateQueryElement)(memoryManager) {}
    char* name() { return "UpdateQueryPool"; }
};

#endif

/*
 *  DeleteQueryPool
 */
struct DeleteQueryElement {
	TargetListPoolIndex  targetList;
	ExprPoolIndex        whereCond;
	QGNodePoolIndex      queryGraph;
	Boolean				 isDeferredDelete;
};

OQL_OutStream& operator<<(OQL_OutStream& os, DeleteQueryElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class  DeleteQueryPool : public OQL_Pool<DeleteQueryElement> {
public:
	DeleteQueryPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<DeleteQueryElement>(memoryManager) {}
    char* name() { return "DeleteQueryPool"; }
};

#else

declare(OQL_Pool,DeleteQueryElement);
declare(OQL_PoolElements,DeleteQueryElement);

class  DeleteQueryPool : public OQL_Pool(DeleteQueryElement) {
public:
	DeleteQueryPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(DeleteQueryElement)(memoryManager) {}
    char* name() { return "DeleteQueryPool"; }
};

#endif

/*
 *  SelListPool
 */
enum {SELLIST_KIND_PATHEXPR, SELLIST_KIND_AGGRFUNC, SELLIST_KIND_FUNC,
      SELLIST_KIND_VALUE, SELLIST_KIND_OPER, SELLIST_KIND_CONS};
struct SelListElement {
    Four selElemKind;
    union {
        PathExprPoolIndex  pathExpr;
        AggrFuncPoolIndex  aggrFunc;
        FunctionPoolIndex  func;
        ValuePoolIndex     value;
        OperatorStruct     oper;
        ConstructPoolIndex cons;
    };
};

OQL_OutStream& operator<<(OQL_OutStream& os, SelListElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class  SelListPool      : public OQL_Pool<SelListElement> {
public:
	SelListPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<SelListElement>(memoryManager) {}
    char* name() { return "SelListPool"; }
};

#else

declare(OQL_Pool,SelListElement);
declare(OQL_PoolElements,SelListElement);

class  SelListPool      : public OQL_Pool(SelListElement) {
public:
	SelListPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(SelListElement)(memoryManager) {}
    char* name() { return "SelListPool"; }
};

#endif

/* 
 *  OrderByListPool
 */
enum {ORDERBYLIST_KIND_PATHEXPR, ORDERBYLIST_KIND_AGGRFUNC, ORDERBYLIST_KIND_FUNC};
struct OrderByListElement {
    One                 asc_or_desc_Flag;
    Four                ordByKeyKind;
    union {
        PathExprPoolIndex pathExpr;
        AggrFuncPoolIndex aggrFunc;
        FunctionPoolIndex func;
    };
};

OQL_OutStream& operator<<(OQL_OutStream& os, OrderByListElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class  OrderByListPool      : public OQL_Pool<OrderByListElement> {
public:
	OrderByListPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<OrderByListElement>(memoryManager) {}
    char* name() { return "OrderByListPool"; }
};

#else

declare(OQL_Pool,OrderByListElement);
declare(OQL_PoolElements,OrderByListElement);

class  OrderByListPool      : public OQL_Pool(OrderByListElement) {
public:
	OrderByListPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(OrderByListElement)(memoryManager) {}
    char* name() { return "OrderByListPool"; }
};

#endif

/*
 *  TargetListPool
 */
struct TargetListElement {
    CollectionInfo				collectionInfo;
    Boolean						starFlag;
    StringPoolIndex				aliasName;
	Boolean						isTargetOid;
	OOSQL_StorageManager::OID	oid;
};

OQL_OutStream& operator<<(OQL_OutStream& os, TargetListElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class  TargetListPool      : public OQL_Pool<TargetListElement> {
public:
	TargetListPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<TargetListElement>(memoryManager) {}
    char* name() { return "TargetListPool"; }
};

#else

declare(OQL_Pool,TargetListElement);
declare(OQL_PoolElements,TargetListElement);

class  TargetListPool      : public OQL_Pool(TargetListElement) {
public:
	TargetListPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(TargetListElement)(memoryManager) {}
    char* name() { return "TargetListPool"; }
};

#endif

/*
 *  GroupByListPool
 */
enum {GROUPBY_KIND_PATHEXPR, GROUPBY_KIND_AGGRFUNC, GROUPBY_KIND_EXPR};
struct GroupByListElement {
    Four grpByKeyKind;
    union {
        PathExprPoolIndex pathExpr;
        AggrFuncPoolIndex aggrFunc;
        ExprPoolIndex     expr;
    };
    StringPoolIndex       aliasName;
};

OQL_OutStream& operator<<(OQL_OutStream& os, GroupByListElement& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class  GroupByListPool      : public OQL_Pool<GroupByListElement> {
public:
	GroupByListPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<GroupByListElement>(memoryManager) {}
    char* name() { return "GroupByListPool"; }
};

#else

declare(OQL_Pool,GroupByListElement);
declare(OQL_PoolElements,GroupByListElement);

class  GroupByListPool      : public OQL_Pool(GroupByListElement) {
public:
	GroupByListPool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(GroupByListElement)(memoryManager) {}
    char* name() { return "GroupByListPool"; }
};

#endif

/*
 *  QGNodePool (QueryGraph)
 */
enum {QGNODE_KIND_FROMNONE, QGNODE_KIND_FROMATTR, QGNODE_KIND_FROMMETHOD};
struct QGNode {
    CataClassInfo				classInfo;
    StringPoolIndex				aliasName;
    Four						implicitJoinAttrKind;
    union {
        CataAttrInfo			fromAttrInfo;
        CataMethodInfo			fromMethodInfo;
    };

    MemberPoolIndex				usedMemberList;
    SubClassPoolIndex			subClasses;
    QGNodePoolIndex				refClassList;
    QGNodePoolIndex				nextRefClass;
    
    JoinInfoPoolIndex			explicitJoinList;
    PathExprInfoPoolIndex		pathExprList;
    CondListPoolIndex			conditionList;
    PlanPoolIndex				planNo;

	Boolean						isTargetOid;
	OOSQL_StorageManager::OID	oid;

    void init();
};

OQL_OutStream& operator<<(OQL_OutStream& os, QGNode& object);

#ifndef TEMPLATE_NOT_SUPPORTED

class QGNodePool : public OQL_Pool<QGNode> {
public:
	QGNodePool(OOSQL_MemoryManager* memoryManager) : OQL_Pool<QGNode>(memoryManager) {}
    char* name() { return "QGNodePool"; }
}; 

#else

declare(OQL_Pool,QGNode);
declare(OQL_PoolElements,QGNode);

class QGNodePool : public OQL_Pool(QGNode) {
public:
	QGNodePool(OOSQL_MemoryManager* memoryManager) : OQL_Pool(QGNode)(memoryManager) {}
    char* name() { return "QGNodePool"; }
}; 

#endif

#endif /* __OQL_GDS_STRUCT_H__ */
