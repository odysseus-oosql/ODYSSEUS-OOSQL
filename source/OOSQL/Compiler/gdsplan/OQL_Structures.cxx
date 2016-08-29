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

#include "OQL_Common_Struct.hxx"
#include "OQL_GDS_Struct.hxx"
#include "OQL_Plan_Struct.hxx"
#include <string.h>

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, Boolean boolean)
{
	if(boolean)
		os << "TRUE";
	else
		os << "FALSE";
	return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, AggrFunctionID aggrFunctionID)
{
    switch(aggrFunctionID)
    {
    case AGGRFUNC_NONE:
        os << "AGGRFUNC_NONE";
        break;
    case AGGRFUNC_COUNTALL:
        os << "AGGRFUNC_COUNTALL";
        break;
    case AGGRFUNC_COUNT:
        os << "AGGRFUNC_COUNT";
        break;
    case AGGRFUNC_SUM:
        os << "AGGRFUNC_SUM";
        break;
    case AGGRFUNC_MIN:
        os << "AGGRFUNC_MIN";
        break;
    case AGGRFUNC_MAX:
        os << "AGGRFUNC_MAX";
        break;
    case AGGRFUNC_AVG:
        os << "AGGRFUNC_AVG";
        break;
    }
    os << endl;

    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, FunctionID id)
{
    switch(id)
    {
    case FUNCTION_NONE:
        os << "FUNCTION_NONE";
        break;

    case FUNCTION_GEO_NEAREST:
        os << "FUNCTION_GEO_NEAREST";
        break;

    case FUNCTION_GEO_BOUNDARY:
        os << "FUNCTION_GEO_BOUNDARY";
        break; 

    case FUNCTION_GEO_INTERIOR:
        os << "FUNCTION_GEO_INTERIOR";
        break; 

    case FUNCTION_GEO_BUFFER: 
        os << "FUNCTION_GEO_BUFFER";
        break; 

    case FUNCTION_GEO_UNION: 
        os << "FUNCTION_GEO_UNION";
        break; 

    case FUNCTION_GEO_INTERSECT:
        os << "FUNCTION_GEO_INTERSECT";
        break;

    case FUNCTION_GEO_DIFFERENCE:
        os << "FUNCTION_GEO_DIFFERENCE";
        break; 

    case FUNCTION_GEO_DISTANCE:
        os << "FUNCTION_GEO_DISTANCE";
        break; 

    case FUNCTION_GEO_AREA: 
        os << "FUNCTION_GEO_AREA";
        break; 

    case FUNCTION_GEO_LENGTH: 
        os << "FUNCTION_GEO_LENGTH";
        break; 

    case FUNCTION_TEXTIR_WEIGHT: 
        os << "FUNCTION_TEXTIR_WEIGHT";
        break; 

    case FUNCTION_TEXTIR_NMATCH:
        os << "FUNCTION_TEXTIR_NMATCH";
        break;

    case FUNCTION_TEXTIR_MATCH:
        os << "FUNCTION_TEXTIR_MATCH";
        break;
	case FUNCTION_USER_DEFINED:
		os << "FUNCTION_USER_DEFINED";
		break;
    default:
        os << "UNKNOWN";
        break;
    }
    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, CollectionInfo& object)
{
    os << object.classInfo;
    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, AP_ColAccessInfo& object)
{
    os << "(planNo=" << object.planNo << ", colNo=" << object.colNo << ")";

    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, AP_MethodAccessInfo& object)
{
    os << "(planNo=" << object.planNo << ", methodNo=" << object.methodNo << ")";
    
    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, AP_PathExprAccessInfo& object)
{
    switch(object.kind)
    {
    case PATHEXPR_KIND_OBJECT:
        os << "(kind=PATHEXPR_KIND_OBJECT, planNo=" << object.planNo << ")";
        break;
    case PATHEXPR_KIND_OID:
        os << "(kind=PATHEXPR_KIND_OID, planNo=" << object.planNo << ")";
        break;
    case PATHEXPR_KIND_CLASS:
        os << "(kind=PATHEXPR_KIND_CLASS, planNo=" << object.planNo << ")";
        break;
    case PATHEXPR_KIND_ATTR:
        os << "(kind=PATHEXPR_KIND_ATTR, col=" << object.col << ")";
        break;
    case PATHEXPR_KIND_METHOD:
        os << "(kind=PATHEXPR_KIND_METHOD, method=" << object.method << ")";
        break;
	case PATHEXPR_KIND_LOGICALID:
		os << "(kind=PATHEXPR_KIND_LOGICALID, planNo=" << object.planNo << ")";
		break;
    default:
        os << "(kind=UNKNOWN)";
        break;
    }
    
    return os;
}


/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, AP_AggrFuncResultAccessInfo& object)
{
    os << "(aggrFunctionID=" << object.aggrFunctionID << ", planNo=" << object.planNo << ", aggrFuncIndex=" << object.aggrFuncIndex << ")";

    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, AP_FuncResultAccessInfo& object)
{
    os << "(functionID=" << object.functionID << ", planNo=" << object.planNo << ", funcIndex=" << object.funcIndex << ")";

    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, AP_ClassInfo& object)
{
    switch(object.classKind)
    {
	case CLASSKIND_NONE:
		os << "(classKind=CLASSKIND_NONE)" << endl;
        break;
    case CLASSKIND_PERSISTENT:
        os << "(classKind=CLASSKIND_PERSISTENT, classId=" << object.classId << ")";
        break;
    case CLASSKIND_TEMPORARY:
        os << "(classKind=CLASSKIND_TEMPORARY, tempFileNum=" << object.tempFileNum << ")";
        break;
	case CLASSKIND_SORTSTREAM:
		os << "(classKind=CLASSKIND_SORTSTREAM, tempFileNum=" << object.tempFileNum << ")";
        break;
    case CLASSKIND_NULL_AGGRFUNC_ONLY:
        os << "(classKind=CLASSKIND_NULL_AGGRFUNC_ONLY)" << endl;
        break;
    }
    
    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, ObjectElement& object)
{
    os << "classInfo " << object.classInfo << endl;
    os << "structure " << object.structure << endl;

    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, PathExprElement& object)
{
    os << "classInfo " << object.classInfo << endl;
    switch(object.fromAttrKind)
    {
    case PATHEXPR_KIND_NONE:
        os << "fromAttrKind PATHEXPR_KIND_NONE" << endl;
        break;
    case PATHEXPR_KIND_METHOD:
        os << "fromAttrKind PATHEXPR_KIND_METHOD" << endl;
        os << "method    " << object.method.methodInfo << "," << object.method.argument << endl;
        break;
    case PATHEXPR_KIND_ATTR:
        os << "fromAttrKind PATHEXPR_KIND_ATTR" << endl;
        os << "attr      " << object.attr.attrInfo << endl;
        break;
    case PATHEXPR_KIND_FUNC:
        os << "fromAttrKind PATHEXPR_KIND_FUNC" << endl;
        os << "func      " << object.func << endl;
        break;
    case PATHEXPR_KIND_CLASS:
        os << "fromAttrKind PATHEXPR_KIND_CLASS" << endl;
        os << "aliasName " << object.aliasName << endl;
        break;
    case PATHEXPR_KIND_OBJECT:
        os << "fromAttrKind PATHEXPR_KIND_OBJECT" << endl;
        os << "object    " << object.object << endl;
        break;
    }
    os << "domainSubstFlag  " << ((object.domainSubstFlag)? "SM_TRUE":"SM_FALSE") << endl;
    os << "substitutedClass " << object.substitutedClass << endl;
    os << "typeCastingFlag  " << ((object.typeCastingFlag)? "SM_TRUE":"SM_FALSE") << endl;
    os << "typeCastedClass  " << object.typeCastedClass << endl;
    os << "planNo           " << object.planNo << endl;
    os << "pathExprKind     " << object.pathExprKind << endl;
    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, DomainElement& object)
{
    os << "domainClass " << object.domainClass << endl;
    os << "starFlag " << ((object.starFlag)? "SM_TRUE":"SM_FALSE") << endl;

    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, FunctionElement& object)
{
    os << "functionID " << object.functionID << endl;
    os << "argument   " << object.argument << endl;

    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, ArgumentElement& object)
{
    switch(object.argumentKind)
    {
    case ARGUMENT_KIND_PATHEXPR:
        os << "argumentKind ARGUMENT_KIND_PATHEXPR" << endl;
        os << "pathExpr" << object.pathExpr << endl;
        break;
    case ARGUMENT_KIND_VALUE:
        os << "argumentKind ARGUMENT_KIND_VALUE" << endl;
        os << "value " << object.value << endl;
        break;
    case ARGUMENT_KIND_FUNC:
        os << "argumentKind ARGUMENT_KIND_FUNC" << endl;
        os << "func " << object.func << endl;
        break;
    case ARGUMENT_KIND_DOMAIN:
        os << "argumentKind ARGUMENT_KIND_DOMAIN" << endl;
        os << "domain " << object.domain << endl;
        break;
    case ARGUMENT_KIND_AGGRFUNC:
        os << "argumentKind ARGUMENT_KIND_AGGRFUNC" << endl;
        os << "aggrFunc " << object.aggrFunc << endl;
        break;
    case ARGUMENT_KIND_EXPR:
        os << "argumentKind ARGUMENT_KIND_EXPR" << endl;
        os << "expr " << object.expr << endl;
        break;
    }

    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, AggrFuncElement& object)
{
    os << "aggrFunctionID " << object.aggrFunctionID << endl;
    os << "distinctFlag   " << ((object.distinctFlag)? "SM_TRUE":"SM_FALSE") << endl;
    os << "argument       " << object.argument << endl;

    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, AP_AggrFuncElement& object)
{
    os << "aggrFunctionID " << object.aggrFunctionID << endl;
    os << "argument       " << object.argument << endl;
    os << "distinctFlag   " << ((object.distinctFlag)? "SM_TRUE":"SM_FALSE") << endl;

    if(object.distinctFlag)
    {
        os << "tempFileNum  " << object.tempFileNum << endl;
        os << "usedColInfo  " << object.usedColInfo << endl;
    }

    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, StructureElement& object)
{   
    os << "name " << object.name << endl;
    os << "structure " << object.structure << endl;

    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, OperatorID& object)
{
    switch(object)
    {
    case OP_NONE:
        os << "OP_NONE";
        break;
    case OP_UNARY_MINUS:
        os << "OP_UNARY_MINUS";
        break;
    case OP_ABS:
        os << "OP_ABS";
        break;
    case OP_PLUS:
        os << "OP_PLUS";
        break;
    case OP_MINUS: 
        os << "OP_MINUS";
        break;
    case OP_MULTIPLY:
        os << "OP_MULTIPLY";
        break;
    case OP_DIVIDE:
        os << "OP_DIVIDE";
        break;
    case OP_MOD:
        os << "OP_MOD";
        break;
    case OP_STRING_CONCAT:
        os << "OP_STRING_CONCAT";
        break;
    case OP_LIKE:
        os << "OP_LIKE";
        break;
    case OP_EQ:
        os << "OP_EQ";
        break;
    case OP_NE:
        os << "OP_NE";
        break;
    case OP_GT:
        os << "OP_GT";
        break;
    case OP_LT:
        os << "OP_LT";
        break;
    case OP_GE:
        os << "OP_GE";
        break;
    case OP_LE:
        os << "OP_LE";
        break;
	case OP_ISNULL:
		os << "OP_ISNULL";
		break;
	case OP_ISNOTNULL:
		os << "OP_ISNOTNULL";
		break;
    case OP_NOT: 
        os << "OP_NOT";
        break;
    case OP_AND: 
        os << "OP_AND";
        break;
    case OP_OR: 
        os << "OP_OR";
        break;
    case OP_INTERSECT:
        os << "OP_INTERSECT";
        break;
    case OP_UNION:
        os << "OP_UNION";
        break;
    case OP_EXCEPT:
        os << "OP_EXCEPT";
        break;
    case OP_IN:
        os << "OP_IN";
        break;
    case OP_GEO_NORTH:
        os << "OP_GEO_NORTH";
        break;
    case OP_GEO_SOUTH: 
        os << "OP_GEO_SOUTH";
        break;
    case OP_GEO_EAST:
        os << "OP_GEO_EAST";
        break;
    case OP_GEO_WEST: 
        os << "OP_GEO_WEST";
        break;
    case OP_GEO_COVER: 
        os << "OP_GEO_COVER";
        break;
    case OP_GEO_COVERED:
        os << "OP_GEO_COVERED";
        break;
    case OP_GEO_CONTAIN: 
        os << "OP_GEO_CONTAIN";
        break;
    case OP_GEO_CONTAINED:
        os << "OP_GEO_CONTAINED";
        break;
    case OP_GEO_DISJOINT:
        os << "OP_GEO_DISJOINT";
        break;
    case OP_GEO_EQUAL:
        os << "OP_GEO_EQUAL";
        break;
    case OP_GEO_MEET: 
        os << "OP_GEO_MEET";
        break;
    case OP_GEO_OVERLAP:
        os << "OP_GEO_OVERLAP";
        break;
    case OP_TEXTIR_ACCUMULATE:
        os << "OP_TEXTIR_ACCUM";
        break;
    case OP_TEXTIR_OR:
        os << "OP_TEXTIR_OR";
        break;
    case OP_TEXTIR_AND:
        os << "OP_TEXTIR_AND";
        break;
    case OP_TEXTIR_MINUS:
        os << "OP_TEXTIR_MINUS";
        break;
    case OP_TEXTIR_THRESHOLD:
        os << "OP_TEXTIR_THRESHOLD";
        break;
    case OP_TEXTIR_MULTIPLY:
        os << "OP_TEXTIR_MULTIPLY";
        break;
    case OP_TEXTIR_MAX:
        os << "OP_TEXTIR_MAX";
        break;
    case OP_TEXTIR_NEAR:
        os << "OP_TEXTIR_NEAR";
        break;
	case OP_TEXTIR_NEAR_WITH_ORDER:
        os << "OP_TEXTIR_NEAR";
        break;
    }

    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, OperatorStruct& object)
{
    os << "operatorId   " << object.operatorId << endl;
    os << "operand1     " << object.operand1 << endl;
    os << "operand2     " << object.operand2 << endl;
	os << "operand3     " << object.operand3 << endl;

    os << "operatorType ";
    switch(object.operatorType)
    {
    case ARITHMETIC_OPERATION:
        os << "ARITHMETIC_OPERATION" << endl;
        break;
    case STRING_OPERATION:
        os << "STRING_OPERATION" << endl;
        break;
	case NULLCHECK_OPERATION:
		os << "NULLCHECK_OPERATION" << endl;
		break;
    case COLLECTION_OPERATION:
        os << "COLLECTION_OPERATION" << endl;
        break;
    case OID_OPERATION:
        os << "OID_OPERATION" << endl;
        break;
    case OBJECT_OPERATION:
        os << "OBJECT_OPERATION" << endl;
        break;
    case TEXT_OPERATION:
        os << "TEXT_OPERATION" << endl;
        break;
    default:
        os << "UNDEFINED" << endl;
        break;
    }
    return os;
}


/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, AP_OperatorStruct& object)
{
    os << "operatorId   " << object.operatorId << endl;
    os << "operand1     " << object.operand1 << endl;
    os << "operand2     " << object.operand2 << endl;
	os << "operand3     " << object.operand3 << endl;

    os << "operatorType ";
    switch(object.operatorType)
    {
    case ARITHMETIC_OPERATION:
        os << "ARITHMETIC_OPERATION" << endl;
        break;
    case STRING_OPERATION:
        os << "STRING_OPERATION" << endl;
        break;
	case NULLCHECK_OPERATION:
		os << "NULLCHECK_OPERATION" << endl;
		break;
    case COLLECTION_OPERATION:
        os << "COLLECTION_OPERATION" << endl;
        break;
    case OID_OPERATION:
        os << "OID_OPERATION" << endl;
        break;
    case OBJECT_OPERATION:
        os << "OBJECT_OPERATION" << endl;
        break;
    case TEXT_OPERATION:
        os << "TEXT_OPERATION" << endl;
        break;
    default:
        os << "UNDEFINED" << endl;
        break;
    }
    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, TypeID& typeID)
{
    switch(typeID)
    {
    case TYPEID_SHORT:
        os << "TYPEID_SHORT";
        break;
    case TYPEID_INT:
        os << "TYPEID_INT";
        break;
    case TYPEID_LONG:
        os << "TYPEID_LONG";
        break;
    case TYPEID_LONG_LONG:
        os << "TYPEID_LONG_LONG";
        break;
    case TYPEID_FLOAT:
        os << "TYPEID_FLOAT";
        break;
    case TYPEID_DOUBLE:
        os << "TYPEID_DOUBLE";
        break;
    case TYPEID_STRING:
        os << "TYPEID_STRING";
        break;
    case TYPEID_VARSTRING:
        os << "TYPEID_VARSTRING";
        break;
    case TYPEID_PAGEID:
        os << "TYPEID_PAGEID";
        break;
    case TYPEID_FILEID:
        os << "TYPEID_FILEID";
        break;
    case TYPEID_INDEXID:
        os << "TYPEID_INDEXID";
        break;
    case TYPEID_OID:
        os << "TYPEID_OID";
        break;
    case TYPEID_TEXT:
        os << "TYPEID_TEXT";
        break;
    case TYPEID_MBR:
        os << "TYPEID_MBR";
        break;
    case TYPEID_NONE:
        os << "TYPEID_NONE";
        break;
    case TYPEID_NULL:
        os << "TYPEID_NULL";
        break;
    case TYPEID_SET:
        os << "TYPEID_SET";
        break;
    case TYPEID_BAG: 
        os << "TYPEID_BAG";
        break;
    case TYPEID_LIST: 
        os << "TYPEID_LIST";
        break;
    case TYPEID_ARRAY:
        os << "TYPEID_ARRAY";
        break;
    case TYPEID_DATE:
        os << "TYPEID_DATE";
        break;
    case TYPEID_TIME: 
        os << "TYPEID_TIME";
        break;
    case TYPEID_TIMESTAMP:
        os << "TYPEID_TIMESTAMP";
        break;
    case TYPEID_INTERVAL:
        os << "TYPEID_INTERVAL";
        break;
    case TYPEID_STRUCTURE:
        os << "TYPEID_STRUCTURE";
        break;
    case TYPEID_NIL:
        os << "TYPEID_NIL";
        break;
    case TYPEID_BOOL:
        os << "TYPEID_BOOL";
        break;
    case TYPEID_ID:
        os << "TYPEID_ID";
        break;
    case TYPEID_DOMAIN:
        os << "TYPEID_DOMAIN";
        break;
    }

    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, ExprElement& object)
{
    switch(object.exprKind)
    {
    case EXPR_KIND_PATHEXPR:
        os << "exprNodeKind EXPR_KIND_PATHEXPR" << endl;
        os << "pathExpr " << object.pathExpr << endl;
        break;
    case EXPR_KIND_AGGRFUNC:
        os << "exprNodeKind EXPR_KIND_AGGRFUNC" << endl;
        os << "aggrFunc " << object.aggrFunc << endl;
        break;
    case EXPR_KIND_FUNCTION:
        os << "exprNodeKind EXPR_KIND_FUNCTION" << endl;
        os << "func " << object.func << endl;
        break;
    case EXPR_KIND_OPER:
        os << "exprNodeKind EXPR_KIND_OPER" << endl;
        os << "oper " << object.oper;
        break;
    case EXPR_KIND_VALUE:
        os << "exprNodeKind EXPR_KIND_VALUE" << endl;
        os << "value " << object.value << endl;
        break;
    case EXPR_KIND_CONS:
        os << "exprNodeKind EXPR_KIND_CONS" << endl;
        os << "cons " << object.cons << endl;
        break;
    }

    os << "operandType  ";
    switch(object.operandType)
    {
    case EXPR_OPERANDTYPE_CONTAIN_METHOD:
        os << "EXPR_OPERANDTYPE_CONTAIN_METHOD" << endl;
        break;

    case EXPR_OPERANDTYPE_CONTAIN_NOMETHOD:
        os << "EXPR_OPERANDTYPE_CONTAIN_NOMETHOD" << endl;
        break;
    }
    os << "refCount " << object.refCount << endl;

    os << "resultType " << object.resultType << endl;
    switch(object.complexType)
    {
    case COMPLEX_TYPE_SIMPLE:
        os << "complexType COMPLEX_TYPE_SIMPLE" << endl;
        break;
    case COMPLEX_TYPE_SET:
        os << "complexType COMPLEX_TYPE_SET" << endl;
        break;
    case COMPLEX_TYPE_BAG:
        os << "complexType COMPLEX_TYPE_BAG" << endl;
        break;
    case COMPLEX_TYPE_LIST:
        os << "complexType COMPLEX_TYPE_LIST" << endl;
        break;
    case COMPLEX_TYPE_ARRAY:
        os << "complexType COMPLEX_TYPE_ARRAY" << endl;
        break;
    }

    os << "resultLength " << object.resultLength << endl;
    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, CollectionElement& object)
{
    os << "collectionType ";
    switch(object.collectionType)
    {
    case COLTYPE_SET:
        os << "COLTYPE_SET";
        break;
    case COLTYPE_BAG:
        os << "COLTYPE_BAG";
        break;
    case COLTYPE_LIST:
        os << "COLTYPE_LIST";
        break;
    case COLTYPE_ARRAY:
        os << "COLTYPE_ARRAY";
        break;
    }
    os << endl;

    os << "elementType ";
    switch(object.elementType)
    {
    case COLELMTYPE_INTEGER: 
        os << "COLELMTYPE_INTEGER";
        break;
    case COLELMTYPE_REAL: 
        os << "COLELMTYPE_REAL";
        break;
    case COLELMTYPE_STRING: 
        os << "COLELMTYPE_STRING";
        break;
    case COLELMTYPE_BOOL: 
        os << "COLELMTYPE_BOOL";
        break;
    case COLELMTYPE_MBR: 
        os << "COLELMTYPE_MBR";
        break;
    }
    os << endl;

    os << "value " << object.value << endl;

    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, MBRElement& object)
{
    os << "x1 " << object.x1 << endl;
    os << "y1 " << object.y1 << endl;
    os << "x2 " << object.x2 << endl;
    os << "y2 " << object.y2 << endl;

    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, ValueElement& object)
{
    switch(object.valueKind)
    {
    case VALUE_KIND_STRING:
        os << "valueKind VALUE_KIND_STRING" << endl;
        os << "string " << object.string << endl;
        break;
    case VALUE_KIND_INTEGER:
        os << "valueKind VALUE_KIND_INTEGER" << endl;
        os << "integer " << object.integer << endl;
        break;
    case VALUE_KIND_REAL:
        os << "valueKind VALUE_KIND_REAL" << endl;
        os << "real " << object.real << endl;
        break;
    case VALUE_KIND_BOOL:
        os << "valueKind VALUE_KIND_BOOL" << endl;
        os << "bool " << object.boolean << endl;
        break;
    case VALUE_KIND_NIL:
        os << "valueKind VALUE_KIND_NIL" << endl;
        os << "nil " << object.nil << endl;
        break;
    case VALUE_KIND_MBR:
        os << "valueKind VALUE_KIND_MBR" << endl;
        os << "mbr " << object.mbr << endl;
        break;
	case VALUE_KIND_DATE:
		os << "valueKind VALUE_KIND_DATE" << endl;
        os << "date " << object.date << endl;
		break;
	case VALUE_KIND_TIME:
		os << "valueKind VALUE_KIND_TIME" << endl;
        os << "time " << object.time << endl;
		break;
	case VALUE_KIND_TIMESTAMP:
		os << "valueKind VALUE_KIND_TIMESTAMP" << endl;
        os << "timestamp " << object.timestamp << endl;
		break;
	case VALUE_KIND_INTERVAL:
		os << "valueKind VALUE_KIND_INTERVAL" << endl;
        os << "interval " << object.interval << endl;
		break;
	case VALUE_KIND_TEXT:
		os << "valueKind VALUE_KIND_TEXT" << endl;
		os << "textUpdateMode ";
		if(TEXT_UPDATE_MODE_DEFERRED)
			os << "TEXT_UPDATE_MODE_DEFERRED" << endl;
		else
			os << "TEXT_UPDATE_MODE_IMMEDIDATE" << endl;
		break;
    }
	

    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, DateElement& object)
{
	os << "[";
	os << object.year << "-";
	os << object.month << "-";
	os << object.day;
	os << "]";

	return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, TimeElement& object)
{
	os << "[";
	os << object.hour << ":";
	os << object.minute << ":";
	os << object.second;
	os << "]";
	return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, TimestampElement& object)
{
	os << "[";
	os << object.year << "-";
	os << object.month << "-";
	os << object.day << "  ";
	os << object.hour << ":";
	os << object.minute << ":";
	os << object.second;
	os << "]";
	
	return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, IntervalElement& object)
{
	os << "structure not defined";
	return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, SelListElement& object)
{
    switch(object.selElemKind)
    {
    case SELLIST_KIND_PATHEXPR:
        os << "selElemKind SELLIST_KIND_PATHEXPR" << endl;
        os << "pathExpr " << object.pathExpr << endl;
        break;
    case SELLIST_KIND_AGGRFUNC:
        os << "selElemKind SELLIST_KIND_AGGRFUNC" << endl;
        os << "aggrFunc " << object.aggrFunc << endl;
        break;
    case SELLIST_KIND_FUNC:
        os << "selElemKind SELLIST_KIND_FUNC" << endl;
        os << "func     " << object.func << endl;
        break;
    case SELLIST_KIND_OPER:
        os << "selElemKind SELLIST_KIND_OPER" << endl;
        os << "oper     " << object.oper << endl;
        break;
    case SELLIST_KIND_CONS:
        os << "selElemKind SELLIST_KIND_CONS" << endl;
        os << "cons     " << object.cons << endl;
        break;
    }

    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, OrderByListElement& object)
{
    os << "asc_or_desc_Flag " << ((object.asc_or_desc_Flag)?"SM_TRUE":"SM_FALSE") << endl;
    switch(object.ordByKeyKind)
    {
    case ORDERBYLIST_KIND_PATHEXPR:
        os << "ordByKeyKind ORDERBYLIST_KIND_PATHEXPR" << endl;
        os << "pathExpr " << object.pathExpr << endl;
        break;
    case ORDERBYLIST_KIND_AGGRFUNC:
        os << "ordByKeyKind ORDERBYLIST_KIND_AGGRFUNC" << endl;
        os << "aggrFunc " << object.aggrFunc << endl;
        break;
    case ORDERBYLIST_KIND_FUNC:
        os << "ordByKeyKind ORDERBYLIST_KIND_FUNC" << endl;
        os << "func " << object.func << endl;
        break;
    }

    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, OOSQL_StorageManager::OID& object)
{
	os << '{' << object.pageNo << ',' << object.volNo << ',' << object.slotNo;
	os << ',' << object.unique << ',' << object.classID << '}';

	return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, TargetListElement& object)
{
    os << "collectionInfo " << object.collectionInfo << endl;
    os << "starFalg       " << ((object.starFlag)?"SM_TRUE":"SM_FALSE") << endl;
    os << "aliasName      " << object.aliasName << endl;

	if(object.isTargetOid)
		os << "oid            " << object.oid << endl;

    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, GroupByListElement& object)
{
    switch(object.grpByKeyKind)
    {
    case GROUPBY_KIND_PATHEXPR:
        os << "grpByKeyKind GROUPBY_KIND_PATHEXPR" << endl;
        os << "pathExpr " << object.pathExpr << endl;
        break;
    case GROUPBY_KIND_EXPR:
        os << "grpByKeyKind GROUPBY_KIND_EXPR" << endl;
        os << "expr " << object.expr << endl;
        break;
    }
    os << "aliasName " << object.aliasName << endl;

    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, QGNode& object)
{
    os << "classInfo " << object.classInfo << endl;
    os << "aliasName " << object.aliasName << endl;
   
    switch(object.implicitJoinAttrKind)
    {
    case QGNODE_KIND_FROMNONE:
        os << "implicitJoinAttrKind QGNODE_KIND_FROMNONE" << endl;
        break;
    case QGNODE_KIND_FROMATTR:
        os << "implicitJoinAttrKind QGNODE_KIND_FROMATTR" << endl;
        os << "fromAttrInfo " << object.fromAttrInfo << endl;
        break;
    case QGNODE_KIND_FROMMETHOD:
        os << "implicitJoinAttrKind QGNODE_KIND_FROMMETHOD" << endl;
        os << "fromMethodInfo " << object.fromMethodInfo << endl;
        break;
    }

    os << "usedMemberList   " << object.usedMemberList << endl;
    os << "refClassList     " << object.refClassList << endl;
    os << "subClasses       " << object.subClasses << endl;
    os << "nextRefClass     " << object.nextRefClass << endl;
    os << "explicitJoinList " << object.explicitJoinList << endl;
    os << "pathExprList     " << object.pathExprList << endl;
    os << "conditionList    " << object.conditionList << endl;
    os << "planNo           " << object.planNo << endl;

	if(object.isTargetOid)
		os << "oid              " << object.oid << endl;

    return os;
}   

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, MemberElement& object)
{
    switch(object.memberKind)
    {
    case MEMBER_KIND_NONE:
        os << "memberKind MEMBER_KIND_NONE" << endl;
        os << "NONE" << endl;
        break;
    case MEMBER_KIND_ATTR:
        os << "memberKind MEMBER_KIND_ATTR" << endl;
        os << "attrInfo " << object.attr.attrInfo << endl;
        break;
    case MEMBER_KIND_METHOD:
        os << "memberKind MEMBER_KIND_METHOD" << endl;
        os << "methodInfo " << object.method.methodInfo << endl;
        break;
    }
    os << "nextMember " << object.nextMember << endl;

    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, OOSQL_StorageManager::IndexID &object)
{
#ifndef GEOSTORE
    if(object.isLogical == SM_FALSE && object.index.physical_iid.volNo == NIL)
        os << "NIL";
    else
    {
        switch(object.isLogical)
        {
        case SM_TRUE:
            os << "isLogical SM_TRUE" << endl;
            os << "index.physical_iid ";
            os << "(serial " << object.index.physical_iid.serial << ", ";
            os << "volNo  " << object.index.physical_iid.volNo << ")";
            os << endl;
            break;
        case SM_FALSE:
            os << "isLogical SM_FALSE" << endl;
            os << "index.logical_iid ";
            os << "(pageNo " << object.index.logical_iid.pageNo << ", ";
            os << "volNo " << object.index.logical_iid.volNo << ", ";
            os << "slotNo " << object.index.logical_iid.slotNo << ", ";
            os << "unique " << object.index.logical_iid.unique << ")";
            os << endl;
            break;
        default:
            os << "INVALID INDEXID" << endl;
            break;
        }
    }
#else
	os << "GEOSTORE " << endl;
#endif
    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, SubClassElement& object)
{
    os << "subClassInfo " << object.subClassInfo << endl;
    os << "classId      " << object.classId << endl;
    //switch(object.accessMethod)
    //{
    //}
    os << "indexInfo    " << object.indexInfo << endl;
    os << "condNodes    " << object.condNodes << endl;
    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, JoinInfoElement& object)
{
    os << "joinClass " << object.joinClass << endl;
    os << "nextJoinInfo " << object.nextJoinInfo << endl;

    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, PathExprInfoElement& object)
{
    os << "pathExpr         " << object.pathExpr << endl;
    os << "nextPathExprInfo " << object.nextPathExprInfo << endl;

    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, CondListElement& object)
{
    os << "expr          " << object.expr << endl;
    os << "nextCondInfo  " << object.nextCondInfo << endl;

    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, ConstructElement& object)
{
    switch(object.consKind)
    {
    case CONSTRUCT_KIND_OBJECT:
        os << "consKind CONSTRUCT_KIND_OBJECT" << endl;
        os << "object " << object.object << endl;
        break;
    case CONSTRUCT_KIND_STRUCTURE:
        os << "consKind CONSTRUCT_KIND_STRUCTURE" << endl;
        os << "structure " << object.structure << endl;
        break;
    case CONSTRUCT_KIND_COLLECTION:
        os << "consKind CONSTRUCT_KIND_COLLECTION" << endl;
        os << "collection " << object.collection << endl;
        break;
    }

    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, StringPool& object)
{
    Four    i;

    os << "DUMP START: " << object.name() << endl;

    for(i = 0;i < object.top;i++)
		if(object.elements[i] == 0)
			os << ' ';
		else
			os << object.elements[i];
    os << endl;
    os << "DUMP END  : " << object.name() << endl;

    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, AP_ColNoMapElement& object)
{
    os << "offset " << object.offset << endl;
    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, AP_UsedColElement& object)
{
    os << "typeId " << object.typeId << endl;
    os << "length " << object.length << endl;
    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, AP_MethodNoMapElement& object)
{
    os << "offset " << object.offset << endl;
    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, AP_UsedMethodElement& object)
{
    os << "returnType   " << object.returnType << endl;
    os << "returnLength " << object.returnLength << endl;
    os << "pMethod      " << object.pMethod << endl;
    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, AP_ProjectionElement& object)
{
    os << "resultType   " << object.resultType << endl;
    os << "resultLength " << object.resultLength << endl;
    switch(object.projectionKind)
    {
    case PROJECTION_KIND_PATHEXPR:
        os << "projectionKind PROJECTION_KIND_PATHEXPR" << endl;
        os << "pathExpr " << object.pathExpr << endl;
        break;
    case PROJECTION_KIND_AGGRFUNCRESULT:
        os << "projectionKind PROJECTION_KIND_AGGRFUNCRESULT" << endl;
        os << "pathExpr " << object.aggrFuncResult << endl;
        break;
    case PROJECTION_KIND_FUNCRESULT:
        os << "projectionKind PROJECTION_KIND_FUNCRESULT" << endl;
        os << "funcResult " << object.funcResult << endl;
        break;
    case PROJECTION_KIND_FUNCEVAL:
        os << "projectionKind PROJECTION_KIND_FUNCEVAL" << endl;
        os << "funcEval " << object.funcEval << endl;
        break;
    case PROJECTION_KIND_VALUE:
        os << "projectionKind PROJECTION_KIND_VALUE" << endl;
        os << "value " << object.value << endl;
        break;
    case PROJECTION_KIND_OPER:
        os << "projectionKind PROJECTION_KIND_OPER" << endl;
        os << "oper " << object.oper << endl;
        break;
    case PROJECTION_KIND_OID:
        os << "projectionKind PROJECTION_KIND_OID" << endl;
        os << "oid_of_plan " << object.oid_of_plan << endl;
        break;
	case PROJECTION_KIND_EXPR:
		os << "projectionKind PROJECTION_KIND_EXPR" << endl;
		os << " expr " << object.expr << endl;
		break;
    }

    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, AP_ExprElement& object)
{
    switch(object.exprKind)
    {
    case EXPR_KIND_PATHEXPR:
        os << "nodeKind EXPR_KIND_PATHEXPR" << endl;
        os << "pathExpr " << object.pathExpr << endl;
        break;
    case EXPR_KIND_AGGRFUNCRESULT: 
        os << "nodeKind EXPR_KIND_AGGRFUNCRESULT" << endl;
        os << "AggrFuncResult " << object.aggrFuncResult << endl;
        break;
    case EXPR_KIND_FUNCRESULT: 
        os << "nodeKind EXPR_KIND_FUNCRESULT" << endl;
        os << "funcResult " << object.funcResult << endl;
        break;
    case EXPR_KIND_FUNCEVAL:
        os << "nodeKind EXPR_KIND_FUNCEVAL" << endl;
        os << "funcEvel " << object.funcEval << endl;
        break;
    case EXPR_KIND_VALUE:
        os << "nodeKind EXPR_KIND_VALUE" << endl;
        os << "value " << object.value << endl;
        break;
    case EXPR_KIND_OPER:
        os << "nodeKind EXPR_KIND_OPER" << endl;
        os << "oper " << object.oper;
        break;
    case EXPR_KIND_CONS:
        os << "nodeKind EXPR_KIND_CONS" << endl;
        os << "cons " << object.cons << endl;
        break;
    default:
        os << "nodeKind UNKNOWN" << endl;
        break;
    }

    os << "resultType   " << object.resultType << endl;
    os << "resultLength " << object.resultLength << endl;

    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, AP_CondListElement& object)
{
    os << "expr " << object.expr << endl;

    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, AP_FuncEvalInfo& object)
{
    os << "functionID " << object.functionID << endl;
    os << "argument   " << object.argument << endl;

    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, AP_ArgumentElement& object)
{
    switch(object.argumentKind)
    {
    case ARGUMENT_KIND_PATHEXPR:
        os << "argumentKind ARGUMENT_KIND_PATHEXPR" << endl;
        os << "pathExpr" << object.pathExpr << endl;
        break;
    case ARGUMENT_KIND_AGGRFUNCRESULT:
        os << "argumentKind ARGUMENT_KIND_AGGRFUNCRESULT" << endl;
        os << "aggrFuncResult " << object.aggrFuncResult << endl;
        break;
    case ARGUMENT_KIND_FUNCRESULT:
        os << "argumentKind ARGUMENT_KIND_RESULTACCESS" << endl;
        os << "funcResult " << object.funcResult << endl;
        break;
    case ARGUMENT_KIND_TEMPFILECOL:
        os << "argumentKind ARGUMENT_KIND_TEMPFILECOL" << endl;
        os << "tempFileCol " << object.tempFileCol << endl;
        break;
    case ARGUMENT_KIND_FUNCEVAL:
        os << "argumentKind ARGUMENT_KIND_FUNC" << endl;
        os << "funcEval " << object.funcEval << endl;
        break;
    case ARGUMENT_KIND_VALUE:
        os << "argumentKind ARGUMENT_KIND_VALUE" << endl;
        os << "value " << object.value << endl;
        break;
    case ARGUMENT_KIND_DOMAIN:
        os << "argumentKind ARGUMENT_KIND_DOMAIN" << endl;
        os << "domain " << object.domain << endl;
        break;
    case ARGUMENT_KIND_EXPR:
        os << "argumentKind ARGUMENT_KIND_ARGUMENT" << endl;
        os << "expr " << object.expr << endl;
        break;
    case ARGUMENT_KIND_TEXTIR_SUBPLAN:
        os << "argumentKind ARGUMENT_KIND_TEXTIR_SUBPLAN" << endl;
        os << "textIndexSubPlan " << endl << object.textIndexSubPlan << endl;
        break;
    }
    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, AP_ProjectionListElement& object)
{
	Four i;

	os << "projectionInfo  " << object.projectionInfo << endl;
	os << "nSortKeys       " << object.nSortKeys << endl;
	os << "sortKeys        ";
	for(i = 0; i < object.nSortKeys; i++)
		os << object.sortKeys[i] << " ";
	os << endl;
	os << "sortAscDesc     ";
	for(i = 0; i < object.nSortKeys; i++)
	{
		if(object.sortAscDesc[i])
			os << "ASC  ";
		else
			os << "DESC ";
	}
	os << endl;
	os << "tempFileNum     " << object.tempFileNum << endl;

	switch(object.projectionType)
	{
	case PROJECTION_NONE:
		os << "projectionType  " << "PROJECTION_NONE" << endl;
		break;
	case PROJECTION_UPDATE:
		os << "projectionType  " << "PROJECTION_UPDATE" << endl;
		os << "classInfo       " << object.updateInfo.classInfo << endl;
		os << "updateValueList " << object.updateInfo.updateValueList << endl;
		break;
	case PROJECTION_INSERT:
		os << "projectionType  " << "PROJECTION_INSERT" << endl;
		os << "classInfo       " << object.insertInfo.classInfo << endl;
		os << "insertValueList " << object.insertInfo.insertValueList << endl;
		break;
	case PROJECTION_DELETE:
		os << "projectionType   " << "PROJECTION_DELETE" << endl;
		os << "classInfo        " << object.deleteInfo.classInfo << endl;
		os << "isDeferredDelete ";
		if(object.deleteInfo.isDeferredDelete)	os << "SM_TRUE"  << endl;
		else						 		    os << "SM_FALSE" << endl;
		break;
	};

    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, AP_BoolExprElement& object)
{
	os << "colNo  " << object.colNo << endl;
	os << "type   " << object.type << endl;
	os << "length " << object.length << endl;
	os << "op     " << object.op << endl;
	os << "key    " << object.key << endl;
	return os;
}

OQL_OutStream& operator<<(OQL_OutStream& os, OOSQL_StorageManager::CompOp op)
{
    switch(op)
    {
    case OOSQL_StorageManager::SM_EQ:
        os << "SM_EQ";
        break;
    case OOSQL_StorageManager::SM_LT:
        os << "SM_LT";
        break;
    case OOSQL_StorageManager::SM_LE:
        os << "SM_LE";
        break;
    case OOSQL_StorageManager::SM_GT:
        os << "SM_GT";
        break;
    case OOSQL_StorageManager::SM_GE:
        os << "SM_GE";
        break;
    case OOSQL_StorageManager::SM_NE:
        os << "SM_NE";
        break;
    case OOSQL_StorageManager::SM_EOF:
        os << "SM_EOF";
        break;
    case OOSQL_StorageManager::SM_BOF:
        os << "SM_EOF";
        break;
    }

	return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, AP_BoundCondInfo& object)
{
    os << "op  " << object.op << endl;
    os << "key " << object.key << endl;

    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, AP_TempFileInfoElement& object)
{
    os << "typeId " << object.typeId << endl;
    os << "length " << object.length << endl;
    switch(object.kind)
    {
	case TFS_KIND_NONE:
		os << "kind TFS_KIND_NONE" << endl;
        break;

    case TFS_KIND_PATHEXPR:
        os << "kind TFS_KIND_PATHEXPR" << endl;
        os << "pathExpr " << object.pathExpr << endl;
        break;

    case TFS_KIND_AGGRFUNC:
        os << "kind TFS_KIND_AGGRFUNC" << endl;
        os << "aggrFunc " << object.aggrFunc << endl;
        break;

    case TFS_KIND_FUNC:
        os << "kind TFS_KIND_FUNC" << endl;
        os << "func " << object.func << endl;
        break;

    case TFS_KIND_VALUE:
        os << "kind TFS_KIND_VALUE" << endl;
        os << "value " << object.value << endl;
        break;

    case TFS_KIND_OPER:
        os << "kind TFS_KIND_OPER" << endl;
        os << "oper " << object.oper << endl;
        break;

    case TFS_KIND_CONS:
        os << "kind TFS_KIND_CONS" << endl;
        os << "cons " << object.cons << endl;
        break;
    }

    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, AP_TempFileAccessInfo& object)
{
    os << "tempFileNum " << object.tempFileNum << ", ";
    os << "colNo       " << object.colNo << endl;

    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, AP_BtreeIndexCond& object)
{
    os << "startBound " << object.startBound << endl;
    os << "stopBound  " << object.stopBound << endl;

    return os;
}

OQL_OutStream& operator<<(OQL_OutStream& os, AP_MlgfIndexCond& object)
{
	os << endl << "nKeys	" << object.nKeys << endl;
	
	for(Four i = 0; i < object.nKeys; i++)
	{
		os << "lowerBoundExprFlag[" << i << "] = " << object.lowerBoundExprFlag[i] << ", ";
		os << "upperBoundExprFlag[" << i << "] = " << object.upperBoundExprFlag[i] << endl;
		os << "lowerBound[" << i << "] = " << object.lowerBound[i] << ", ";
		os << "upperBound[" << i << "] = " << object.upperBound[i] << endl;
		os << "lowerBoundExpr = " << object.lowerBoundExpr << ", ";
		os << "upperBoundExpr = " << object.upperBoundExpr << endl;
	}
	
	return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, AP_MlgfMbrIndexCond& object)
{
    switch(object.spatialOp)
    {
    case OP_NONE:
        os << "spatialOp " << "OP_NONE" << endl;
        break;
    case OP_GEO_EAST: 
        os << "spatialOp " << "OP_GEO_EAST" << endl;
        break;
    case OP_GEO_WEST: 
        os << "spatialOp " << "OP_GEO_WEST" << endl;
        break;
    case OP_GEO_NORTH:
        os << "spatialOp " << "OP_GEO_NORTH" << endl;
        break;
    case OP_GEO_SOUTH:
        os << "spatialOp " << "OP_GEO_SOUTH" << endl;
        break;
    case OP_GEO_CONTAIN:
        os << "spatialOp " << "OP_GEO_CONTAIN" << endl;
        break;
    case OP_GEO_CONTAINED:
        os << "spatialOp " << "OP_GEO_CONTAINED" << endl;
        break;
    case OP_GEO_COVER:
        os << "spatialOp " << "OP_GEO_COVER" << endl;
        break;
    case OP_GEO_COVERED:
        os << "spatialOp " << "OP_GEO_COVERED" << endl;
        break;
    case OP_GEO_EQUAL:
        os << "spatialOp " << "OP_GEO_EQUAL" << endl;
        break;
    case OP_GEO_MEET:
        os << "spatialOp " << "OP_GEO_MEET" << endl;
        break;
    case OP_GEO_DISJOINT:
        os << "spatialOp " << "OP_GEO_DISJOINT" << endl;
        break;
    case OP_GEO_OVERLAP:
        os << "spatialOp " << "OP_GEO_OVERLAP" << endl;
        break;
    default:
        os << "INVALID SPATIAL OPERATOR" << endl;
        break;
    }

    switch(object.operandType)
    {
    case AP_SPATIAL_OPERAND_TYPE_NONE:
        os << "operandType " << "AP_SPATIAL_OPERAND_TYPE_NONE" << endl;
        break;
    case AP_SPATIAL_OPERAND_TYPE_MBR:
        os << "operandType " << "AP_SPATIAL_OPERAND_TYPE_MBR" << endl;
        os << "mbr " << object.mbr << endl;
        break;
    case AP_SPATIAL_OPERAND_TYPE_PATHEXPR:
        os << "operandType " << "AP_SPATIAL_OPERAND_TYPE_PATHEXPR" << endl;
        os << "pathExpr " << object.pathExpr << endl;
        break;
    case AP_SPATIAL_OPERAND_TYPE_INDEX:
        os << "operandType " << "AP_SPATIAL_OPERAND_TYPE_INDEX" << endl;
        os << "indexId " << object.indexId << endl;
        break;
    case AP_SPATIAL_OPERAND_TYPE_OPER:
        os << "operandType " << "AP_SPATIAL_OPERAND_TYPE_OPER" << endl;
        os << "indexId " << object.indexId << endl;
        break;
	
    }
    os << "window " << object.window << endl;
    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, AP_TextIndexSubPlan& object)
{
    os << "matchFuncNum  " << object.matchFuncNum << endl;
    os << "textIndexCond " << object.textIndexCond << endl;

    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, AP_IndexOperatorInfo& object)
{
    switch(object.operatorID)
    {
    case OP_INDEX_NONE:
        os << "operatorID " << "OP_INDEX_NONE" << endl;
        break;
    case OP_INDEX_AND:
        os << "operatorID " << "OP_INDEX_AND" << endl;
        break;
    case OP_INDEX_OR:
        os << "operatorID " << "OP_INDEX_OR" << endl;
        break;
    }
    os << "op1 " << object.op1 << endl;
    os << "op2 " << object.op2 << endl;

    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, AP_IndexScanInfo& object)
{
	os << "classId " << object.classId << endl;
	os << "colNo   " << object.colNo   << endl;
    os << "indexId " << object.indexId << endl;

    switch(object.indexType)
    {
    case INDEXTYPE_NONE:
        os << "indexType " << "INDEXTYPE_NONE" << endl;
        break;
    case INDEXTYPE_BTREE:
        os << "indexType " << "INDEXTYPE_BTREE" << endl;
        os << "btree     " << object.btree;
        break;
    case INDEXTYPE_TEXT:
        os << "indexType " << "INDEXTYPE_TEXT" << endl;
        os << "text      " << object.text;
        break;
    case INDEXTYPE_MLGF:
        os << "indexType " << "INDEXTYPE_MLGF" << endl;
        os << "mlgf      " << object.mlgf;
        break;
	case INDEXTYPE_MLGF_MBR:
        os << "indexType " << "INDEXTYPE_MLGF_MBR" << endl;
        os << "mlgfmbr	 " << object.mlgfmbr;
        break;
    }

    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, AP_IndexInfoElement& object)
{
    switch(object.nodeKind)
    {
    case INDEXINFO_NONE:
        os << "nodeKind " << "INDEXINFO_NONE" << endl;
        break;
    case INDEXINFO_OPERATOR:
        os << "nodeKind " << "INDEXINFO_OPERATOR" << endl;
        os << "oper     " << object.oper << endl;
        break;
    case INDEXINFO_SCAN:
        os << "nodeKind " << "INDEXINFO_SCAN" << endl;
        os << "scan     " << object.scan << endl;
        break;
    }

    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, AP_TextIndexOperatorInfo& object)
{   
    os << "operatorID " << object.operatorID << endl;
    os << "op1        " << object.op1 << endl;
    os << "op2        " << object.op2 << endl;
	os << "op3        " << object.op3 << endl;

    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, AP_TextIndexKeywordInfo& object)
{
    switch(object.accessMethod)
    {
    case KEYWORD_SEQ_SCAN:
        os << "accessMethod " << "KEYWORD_SEQ_SCAN" << endl;
        break;
    case KEYWORD_IDX_SCAN:
        os << "accessMethod " << "KEYWORD_IDX_SCAN" << endl;
        break;
    case REVERSEKEYWORD_IDX_SCAN:
        os << "accessMethod " << "REVERSEKEYWORD_IDX_SCAN" << endl;
        break;
    }

    os << "startBound " << object.startBound << endl;
    os << "stopBound  " << object.stopBound << endl;

    switch(object.usedPostingKind)
    {
    case TEXTINDEXCOND_NONE:
        os << "usedPostingKind " << "TEXTINDEXCOND_NONE" << endl;
        break;
    case TEXTINDEXCOND_OPERATOR:
        os << "usedPostingKind " << "TEXTINDEXCOND_OPERATOR" << endl;
        break;
    case TEXTINDEXCOND_KEYWORD:
        os << "usedPostingKind " << "TEXTINDEXCOND_KEYWORD" << endl;
        break;
    case TEXTINDEXCOND_CONSTANT:
        os << "usedPostingKind " << "TEXTINDEXCOND_CONSTANT" << endl;
        break;
    }
    os << "keywordWithWildChar " << object.keywordWithWildChar << endl;
    
    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, AP_TextIndexCondElement& object)
{
    switch(object.nodeKind)
    {
    case TEXTINDEXCOND_NONE:
        os << "nodeKind " << "TEXTINDEXCOND_NONE" << endl;
        break;
    case TEXTINDEXCOND_OPERATOR:
        os << "nodeKind " << "TEXTINDEXCOND_OPERATOR" << endl;
        os << "oper     " << object.oper << endl;
        break;
    case TEXTINDEXCOND_KEYWORD:
        os << "nodeKind " << "TEXTINDEXCOND_KEYWORD" << endl;
        os << "keyword  " << object.keyword << endl;
        break;
    case TEXTINDEXCOND_CONSTANT:
        os << "nodeKind " << "TEXTINDEXCOND_CONSTANT" << endl;
        os << "constant " << object.constant << endl;
        break;
    }

    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, SelectQueryElement& object)
{
    os << "selList     " << object.selList << endl;
    
    os << "selListType ";
    if(object.selListType & STAR_BIT)
        os << "STAR_BIT ";
    if(object.selListType & DIST_BIT)
        os << "DIST_BIT ";
    if(object.selListType & PATH_BIT)
        os << "PATH_BIT ";
    if(object.selListType & AGGR_BIT)
        os << "AGGR_BIT ";
    os << endl;
    
    os << "targetList  " << object.targetList << endl;
    os << "whereCond   " << object.whereCond << endl;
    os << "groupByList " << object.groupByList << endl;
    os << "havingCond  " << object.havingCond << endl;
    os << "orderByList " << object.orderByList << endl;
    os << "queryGraph  " << object.queryGraph << endl;

	return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, UpdateQueryElement& object)
{
	os << "targetList      " << object.targetList << endl;
	os << "updateValueList " << object.updateValueList << endl;
	os << "whereCond       " << object.whereCond << endl;
	os << "queryGraph      " << object.queryGraph << endl;

	return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, InsertQueryElement& object)
{
    os << "selList     " << object.selList << endl;
    
    os << "selListType ";
    if(object.selListType & STAR_BIT)
        os << "STAR_BIT ";
    if(object.selListType & DIST_BIT)
        os << "DIST_BIT ";
    if(object.selListType & PATH_BIT)
        os << "PATH_BIT ";
    if(object.selListType & AGGR_BIT)
        os << "AGGR_BIT ";
    os << endl;
    
    os << "targetList  " << object.targetList << endl;
    os << "whereCond   " << object.whereCond << endl;
    os << "groupByList " << object.groupByList << endl;
    os << "havingCond  " << object.havingCond << endl;
    os << "orderByList " << object.orderByList << endl;
    os << "queryGraph  " << object.queryGraph << endl;
	os << "insertTarget    " << object.insertTarget << endl;
	os << "insertValueList " << object.insertValueList << endl;

	return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, DeleteQueryElement& object)
{
	os << "targetList       " << object.targetList << endl;
	os << "whereCond        " << object.whereCond << endl;
	os << "queryGraph       " << object.queryGraph << endl;
	os << "isDeferredDelete " << object.isDeferredDelete << endl;
	return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, UpdateValueElement& object)
{
	os << "attrInfo  " << object.attrInfo << endl;
	os << "isParam   " << object.isParam << endl;
	os << "expr      " << object.expr << endl;
	os << "type      " << object.type << endl;
	os << "length    " << object.length << endl;

	return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, InsertValueElement& object)
{
	os << "attrInfo  " << object.attrInfo << endl;
	os << "isParam   " << object.isParam << endl;
	os << "value     " << object.value << endl;
	os << "type      " << object.type << endl;
	os << "length    " << object.length << endl;

	return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, AP_UpdateValueElement& object)
{
	os << "colNo     " << object.colNo << endl;
	os << "isParam   " << object.isParam << endl;
	os << "expr      " << object.expr << endl;
	os << "type      " << object.type << endl;
	os << "length    " << object.length << endl;

	return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, AP_InsertValueElement& object)
{
	os << "colNo     " << object.colNo << endl;
	os << "isParam   " << object.isParam << endl;
	os << "value     " << object.value << endl;
	os << "type      " << object.type << endl;
	os << "length    " << object.length << endl;

	return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, CommonAP_Element& object)
{
    Four    i;

    os << "classInfo       " << object.classInfo << endl;
    os << "joinMethod      ";
    switch(object.joinMethod)
    {
    case CAP_JOINMETHOD_NONE:
        os << "NONE";
        break;
    case CAP_JOINMETHOD_CARTESIAN_PRODUCT:
        os << "CARTESIAN_PRODUCT";
        break;
    case CAP_JOINMETHOD_IMPLICIT_FORWARD: 
        os << "IMPLICIT_FORWARD";
        break;
    case CAP_JOINMETHOD_IMPLICIT_BACKWARD:
        os << "IMPLICIT_BACKWARD";
        break;
    case CAP_JOINMETHOD_EXPLICIT_NESTEDLOOP:
        os << "EXPLICIT_NESTEDLOOP";
        break;
    case CAP_JOINMETHOD_EXPLICIT_SORTMERGE:
        os << "EXPLICIT_SORTMERGE";
        break;
    case CAP_JOINMETHOD_OUTERMOST_CLASS:
        os << "OUTERMOST_CLASS";
        break;
    }
    os << endl; 

    os << "joinClass       " << object.joinClass << endl;
    os << "isInnermostPlanElem ";
    if(object.isInnermostPlanElem)
        os << "SM_TRUE" << endl;
    else
        os << "FLASE" << endl;
    os << "isMethod        " << ((object.isMethod)? "SM_TRUE":"SM_FALSE") << endl;
    if(object.isMethod)
        os << "implicitJoinMethodNo " << object.implicitJoinMethodNo << endl;
    else
        os << "implicitJoinColNo " << object.implicitJoinColNo << endl;

    switch(object.accessMethod)
    {
    case CAP_ACCESSMETHOD_NONE: 
        os << "accessMethod    " << "CAP_ACCESSMETHOD_NONE" << endl;
        break;
    case CAP_ACCESSMETHOD_OIDFETCH:
        os << "accessMethod    " << "CAP_ACCESSMETHOD_OIDFETCH" << endl;
        break;
    case CAP_ACCESSMETHOD_SEQSCAN:
        os << "accessMethod    " << "CAP_ACCESSMETHOD_SEQSCAN" << endl;
        break;
    case CAP_ACCESSMETHOD_BTREE_INDEXSCAN: 
        os << "accessMethod    " << "CAP_ACCESSMETHOD_BTREE_INDEXSCAN" << endl;
        break;
    case CAP_ACCESSMETHOD_TEXT_INDEXSCAN: 
        os << "accessMethod    " << "CAP_ACCESSMETHOD_TEXT_INDEXSCAN" << endl;
        break;
    case CAP_ACCESSMETHOD_MLGF_INDEXSCAN: 
        os << "accessMethod    " << "CAP_ACCESSMETHOD_MLGF_INDEXSCAN" << endl;
        break;
    }
    
	if(object.isUseOid)
		os << "oid             " << object.oid << endl;

    os << "indexInfo       " << object.indexInfo << endl;
    os << "condNodes       " << object.condNodes << endl;
    os << "subClassInfo    " << object.subClassInfo << endl;
    os << "colNoMap        " << object.colNoMap << endl;
    os << "usedColInfo     " << object.usedColInfo << endl;
    os << "methodNoMap     " << object.methodNoMap << endl;
    os << "usedMethodInfo  " << object.usedMethodInfo << endl;
    os << "nUsedFuncMatch  " << object.nUsedFuncMatch << endl;
    os << "aggrFuncInfo    " << object.aggrFuncInfo << endl;
    os << "nGrpByKeys      " << object.nGrpByKeys << endl;
    os << "grpByKeys       ";
    for(i = 0; i < object.nGrpByKeys; i++)
        os << object.grpByKeys[i] << " ";
    os << endl;
    os << "noHavConds      " << object.noHavConds << endl;
    os << "havCondNodes    " << object.havCondNodes << endl;
    os << "projectionList  " << object.projectionList << endl;

    os << "selDistinctFlag ";
    if(object.selDistinctFlag)  os << "SM_TRUE"  << endl;
    else				        os << "SM_FALSE" << endl;

    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, ClientAP_Element& object)
{
    os << "classInfo       " << object.classInfo;
    os << "accessMethod    " << object.accessMethod << endl;
    os << "indexId         " << object.indexId << endl;
    os << "condNodes       " << object.condNodes << endl;
    os << "subClassInfo    " << object.subClassInfo << endl;
    os << "colNoMap        " << object.colNoMap << endl;
    os << "usedColInfo     " << object.usedColInfo << endl;
    os << "methodNoMap     " << object.methodNoMap << endl;
    os << "usedMethodInfo  " << object.usedMethodInfo << endl;
    os << "nSortKeys       " << object.nSortKeys << endl;
    os << "sortKeys        " << object.sortKeys << endl;
    os << "sortAscDesc     " << object.sortAscDesc << endl;
    os << "aggrFuncInfo    " << object.aggrFuncInfo << endl;
    os << "nGrpByKeys      " << object.nGrpByKeys << endl;
    os << "grpByKeys       " << object.grpByKeys << endl;
    os << "noHavConds      " << object.noHavConds << endl;
    os << "havCondNodes    " << object.havCondNodes << endl;
    os << "projectionList  " << object.projectionList << endl;
    os << "selDistinctFlag " << object.selDistinctFlag << endl;
    os << "tempFileNum     " << object.tempFileNum << endl;

    return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, AttributeInfoElement& object)
{
	os << "attributeName " << object.attributeName << endl;
	os << "domain        " << object.domain << endl;
	os << "length        " << object.length << endl;
	return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, MethodInfoElement& object)
{
	os << "functionName"     << object.functionName << endl;
	os << "argumentTypeList" << object.argumentTypeList << endl;
	os << "returnType"		 << object.returnType << endl;
	os << "specificName"	 << object.specificName << endl;
	os << "externalName"     << object.externalName << endl;
	os << "deterministic"	 << object.deterministic << endl;
	os << "externalAction"   << object.externalAction << endl;
	os << "fenced"           << object.fenced << endl;
	os << "nullCall"         << object.nullCall << endl;
	os << "language"         << object.language << endl;
	os << "parameterStyle"   << object.parameterStyle << endl;
	os << "scratchPad"       << object.scratchPad << endl;
	os << "finalCall"        << object.finalCall << endl;
	os << "allowParallel"    << object.allowParallel << endl;
	os << "dbInfo"           << object.dbInfo << endl;
	return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, KeyInfoElement& object)
{
	os << "keyColNo " << object.keyColNo << endl;
	return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, CreateTableInfo& object)
{
	os << "className     " << object.className << endl;
	os << "superClasses  " << object.superClasses << endl;
	os << "attributeList " << object.attributeList << endl;
	os << "keyList       " << object.keyList << endl;
	os << "isTemporary   " << object.isTemporary << endl;
	return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, CreateIndexInfo& object)
{
	switch(object.indexType)
	{
	case INDEXTYPE_BTREE:
		os << "indexType " << "INDEXTYPE_BTREE" << endl;
		break;

	case INDEXTYPE_MLGF:
		os << "indexType " << "INDEXTYPE_MLGF" << endl;
		break;

	case INDEXTYPE_TEXT:
		os << "indexType " << "INDEXTYPE_TEXT" << endl;
		break;
	}
	os << "isUnique     " << object.isUnique << endl;
	os << "isClustering " << object.isClustering << endl;
	os << "indexName    " << object.indexName << endl;
	os << "className    " << object.className << endl;
	os << "keyList      " << object.keyList << endl;

	return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, DropTableInfo& object)
{
	os << "className " << object.className << endl;
	return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, DropIndexInfo& object)
{
	os << "indexName " << object.indexName << endl;
	return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, DBCommandElement& object)
{
	switch(object.command)
	{
	case DBCOMMAND_CREATE_TABLE:
		os << "command         " << "DBCOMMAND_CREATE_TABLE" << endl;
		os << "createTableInfo " << endl << object.createTableInfo;
		break;

	case DBCOMMAND_CREATE_INDEX:
		os << "command         " << "DBCOMMAND_CREATE_INDEX" << endl;
		os << "createIndexInfo " << endl << object.createIndexInfo;
		break;

	case DBCOMMAND_DROP_TABLE:
		os << "command       " << "DBCOMMAND_DROP_TABLE" << endl;
		os << "dropTableInfo " << endl << object.dropTableInfo;
		break;

	case DBCOMMAND_DROP_INDEX:
		os << "command       " << "DBCOMMAND_DROP_INDEX" << endl;
		os << "dropIndexInfo " << endl << object.dropIndexInfo;
		break;
	}
	return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, SuperClassElement& object)
{
	os << "className " << object.className << endl;

	return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, ParameterMode parameterMode)
{
	switch(parameterMode)
	{
	case PARM_IN:
		os << "IN";
		break;
	case PARM_OUT:
		os << "OUT";
		break;
	case PARM_INOUT:
		os << "INOUT";
		break;
	default:
		os << "undefined value";
		break;
	}

	return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, ArgumentTypeElement& object)
{
	os << "argumentName  " << object.argumentName << endl;
	os << "argumentType  " << object.argumentType << endl;
	os << "parameterMode " << object.parameterMode << endl;
	os << "asLocator     " << object.asLocator << endl;

	return os;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
void QGNode::init()
{
    aliasName.setNull();
    usedMemberList.setNull();
    refClassList.setNull();
    nextRefClass.setNull();
    subClasses.setNull();
    explicitJoinList.setNull();
    pathExprList.setNull();
    conditionList.setNull();
    planNo.setNull();

	isTargetOid = SM_FALSE;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
void AP_ProjectionListElement::init(ProjectionType aprojectionType)
{
	projectionInfo.setNull();
	nSortKeys      = 0;
	tempFileNum    = -1;	
	projectionType = aprojectionType;
	switch(projectionType)
	{
	case PROJECTION_UPDATE:
		updateInfo.classInfo.classKind = CLASSKIND_NONE;
		updateInfo.updateValueList.setNull();
		break;
	case PROJECTION_INSERT:
		insertInfo.classInfo.classKind = CLASSKIND_NONE;
		insertInfo.insertValueList.setNull();
		break;
	case PROJECTION_DELETE:
		deleteInfo.classInfo.classKind = CLASSKIND_NONE;
		deleteInfo.isDeferredDelete = SM_FALSE;
		break;
	}
}

void CommonAP_Element::init()
{
	classInfo.classKind  = CLASSKIND_NONE;

    joinMethod           = CAP_JOINMETHOD_NONE;
    joinClass            = -1;

    isInnermostPlanElem  = SM_TRUE;

    isMethod             = SM_TRUE;
    implicitJoinMethodNo = -1;

    accessMethod         = CAP_ACCESSMETHOD_NONE;
    indexInfo.setNull();
    condNodes.setNull();
	isUseOid             = SM_FALSE;
    
    subClassInfo.setNull();

    colNoMap.setNull();
    usedColInfo.setNull();

    methodNoMap.setNull();
    usedMethodInfo.setNull();

    nUsedFuncMatch       = 0;

    aggrFuncInfo.setNull();

    nGrpByKeys           = 0;
    noHavConds           = 0;
    havCondNodes.setNull();

    projectionList.setNull();

    selDistinctFlag      = SM_FALSE;
}

#ifdef  TEMPLATE_NOT_SUPPORTED
/////////////////////////////////////////////////////////////////////////////////
implement(OQL_Pool,PathExprElement);
implement(OQL_Pool,ArgumentElement);
implement(OQL_Pool,StructureElement);
implement(OQL_Pool,ExprElement);
implement(OQL_Pool,ValueElement);
implement(OQL_Pool,SelListElement);
implement(OQL_Pool,OrderByListElement);
implement(OQL_Pool,TargetListElement);
implement(OQL_Pool,GroupByListElement);
implement(OQL_Pool,float);
implement(OQL_Pool,int);
implement(OQL_Pool,char);
implement(OQL_Pool,QGNode);
implement(OQL_Pool,AggrFuncElement);
implement(OQL_Pool,FunctionElement);
implement(OQL_Pool,DomainElement);
implement(OQL_Pool,CollectionElement);
implement(OQL_Pool,MBRElement); 
implement(OQL_Pool,ObjectElement);
implement(OQL_Pool,MemberElement);
implement(OQL_Pool,SubClassElement);
implement(OQL_Pool,JoinInfoElement);
implement(OQL_Pool,PathExprInfoElement);
implement(OQL_Pool,CondListElement);
implement(OQL_Pool,ConstructElement);
implement(OQL_Pool,AP_ColNoMapElement);
implement(OQL_Pool,AP_UsedColElement);
implement(OQL_Pool,AP_MethodNoMapElement);
implement(OQL_Pool,AP_UsedMethodElement);
implement(OQL_Pool,AP_ProjectionElement);
implement(OQL_Pool,AP_ExprElement);
implement(OQL_Pool,AP_CondListElement);
implement(OQL_Pool,AP_ArgumentElement);
implement(OQL_Pool,AP_AggrFuncElement);
implement(OQL_Pool,CommonAP_Element);
implement(OQL_Pool,ClientAP_Element);
implement(OQL_Pool,AP_ProjectionListElement);
implement(OQL_Pool,AP_TempFileInfoElement);
implement(OQL_Pool,AP_IndexInfoElement);
implement(OQL_Pool,AP_TextIndexCondElement);
implement(OQL_Pool,DateElement);
implement(OQL_Pool,TimeElement);
implement(OQL_Pool,TimestampElement);
implement(OQL_Pool,IntervalElement);
implement(OQL_Pool,SelectQueryElement);
implement(OQL_Pool,UpdateQueryElement);
implement(OQL_Pool,InsertQueryElement);
implement(OQL_Pool,DeleteQueryElement);
implement(OQL_Pool,UpdateValueElement);
implement(OQL_Pool,InsertValueElement);
implement(OQL_Pool,AP_UpdateValueElement);
implement(OQL_Pool,AP_InsertValueElement);
implement(OQL_Pool,AP_BoolExprElement);

implement(OQL_Pool,AttributeInfoElement)
implement(OQL_Pool,MethodInfoElement)
implement(OQL_Pool,KeyInfoElement)
implement(OQL_Pool,DBCommandElement)
implement(OQL_Pool,SuperClassElement)
implement(OQL_Pool,ArgumentTypeElement)

/////////////////////////////////////////////////////////////////////////////////
implement(OQL_PoolElements,PathExprElement);
implement(OQL_PoolElements,ArgumentElement);
implement(OQL_PoolElements,StructureElement);
implement(OQL_PoolElements,ExprElement);
implement(OQL_PoolElements,ValueElement);
implement(OQL_PoolElements,SelListElement);
implement(OQL_PoolElements,OrderByListElement);
implement(OQL_PoolElements,TargetListElement);
implement(OQL_PoolElements,GroupByListElement);
implement(OQL_PoolElements,float);
implement(OQL_PoolElements,int);
implement(OQL_PoolElements,char);
implement(OQL_PoolElements,QGNode);
implement(OQL_PoolElements,AggrFuncElement);
implement(OQL_PoolElements,FunctionElement);
implement(OQL_PoolElements,DomainElement);
implement(OQL_PoolElements,CollectionElement);
implement(OQL_PoolElements,MBRElement); 
implement(OQL_PoolElements,ObjectElement);
implement(OQL_PoolElements,MemberElement);
implement(OQL_PoolElements,SubClassElement);
implement(OQL_PoolElements,JoinInfoElement);
implement(OQL_PoolElements,PathExprInfoElement);
implement(OQL_PoolElements,CondListElement);
implement(OQL_PoolElements,ConstructElement);
implement(OQL_PoolElements,AP_ColNoMapElement);
implement(OQL_PoolElements,AP_UsedColElement);
implement(OQL_PoolElements,AP_MethodNoMapElement);
implement(OQL_PoolElements,AP_UsedMethodElement);
implement(OQL_PoolElements,AP_ProjectionElement);
implement(OQL_PoolElements,AP_ExprElement);
implement(OQL_PoolElements,AP_CondListElement);
implement(OQL_PoolElements,AP_ArgumentElement);
implement(OQL_PoolElements,AP_AggrFuncElement);
implement(OQL_PoolElements,CommonAP_Element);
implement(OQL_PoolElements,ClientAP_Element);
implement(OQL_PoolElements,AP_ProjectionListElement);
implement(OQL_PoolElements,AP_TempFileInfoElement);
implement(OQL_PoolElements,AP_IndexInfoElement);
implement(OQL_PoolElements,AP_TextIndexCondElement);
implement(OQL_PoolElements,DateElement);
implement(OQL_PoolElements,TimeElement);
implement(OQL_PoolElements,TimestampElement);
implement(OQL_PoolElements,IntervalElement);
implement(OQL_PoolElements,SelectQueryElement);
implement(OQL_PoolElements,UpdateQueryElement);
implement(OQL_PoolElements,InsertQueryElement);
implement(OQL_PoolElements,DeleteQueryElement);
implement(OQL_PoolElements,UpdateValueElement);
implement(OQL_PoolElements,InsertValueElement);
implement(OQL_PoolElements,AP_UpdateValueElement);
implement(OQL_PoolElements,AP_InsertValueElement);
implement(OQL_PoolElements,AP_BoolExprElement);

implement(OQL_PoolElements,AttributeInfoElement)
implement(OQL_PoolElements,MethodInfoElement)
implement(OQL_PoolElements,KeyInfoElement)
implement(OQL_PoolElements,DBCommandElement)
implement(OQL_PoolElements,SuperClassElement)
implement(OQL_PoolElements,ArgumentTypeElement)

/////////////////////////////////////////////////////////////////////////////////
implement(PoolIndex,PathExprElement);
implement(PoolIndex,ArgumentElement);
implement(PoolIndex,StructureElement);
implement(PoolIndex,ExprElement);
implement(PoolIndex,ValueElement);
implement(PoolIndex,SelListElement);
implement(PoolIndex,OrderByListElement);
implement(PoolIndex,TargetListElement);
implement(PoolIndex,GroupByListElement);
implement(PoolIndex,float);
implement(PoolIndex,int);
implement(PoolIndex,char);
implement(PoolIndex,QGNode);
implement(PoolIndex,AggrFuncElement);
implement(PoolIndex,FunctionElement);
implement(PoolIndex,DomainElement);
implement(PoolIndex,CollectionElement);
implement(PoolIndex,MBRElement); 
implement(PoolIndex,ObjectElement);
implement(PoolIndex,MemberElement);
implement(PoolIndex,SubClassElement);
implement(PoolIndex,JoinInfoElement);
implement(PoolIndex,PathExprInfoElement);
implement(PoolIndex,CondListElement);
implement(PoolIndex,ConstructElement);
implement(PoolIndex,AP_ColNoMapElement);
implement(PoolIndex,AP_UsedColElement);
implement(PoolIndex,AP_MethodNoMapElement);
implement(PoolIndex,AP_UsedMethodElement);
implement(PoolIndex,AP_ProjectionElement);
implement(PoolIndex,AP_ExprElement);
implement(PoolIndex,AP_CondListElement);
implement(PoolIndex,AP_ArgumentElement);
implement(PoolIndex,AP_AggrFuncElement);
implement(PoolIndex,CommonAP_Element);
implement(PoolIndex,ClientAP_Element);
implement(PoolIndex,AP_ProjectionListElement);
implement(PoolIndex,AP_TempFileInfoElement);
implement(PoolIndex,AP_IndexInfoElement);
implement(PoolIndex,AP_TextIndexCondElement);
implement(PoolIndex,DateElement);
implement(PoolIndex,TimeElement);
implement(PoolIndex,TimestampElement);
implement(PoolIndex,IntervalElement);
implement(PoolIndex,SelectQueryElement);
implement(PoolIndex,UpdateQueryElement);
implement(PoolIndex,InsertQueryElement);
implement(PoolIndex,DeleteQueryElement);
implement(PoolIndex,UpdateValueElement);
implement(PoolIndex,InsertValueElement);
implement(PoolIndex,AP_UpdateValueElement);
implement(PoolIndex,AP_InsertValueElement);
implement(PoolIndex,AP_BoolExprElement);

implement(PoolIndex,AttributeInfoElement)
implement(PoolIndex,MethodInfoElement)
implement(PoolIndex,KeyInfoElement)
implement(PoolIndex,DBCommandElement)
implement(PoolIndex,SuperClassElement)
implement(PoolIndex,ArgumentTypeElement)

#endif  /* TEMPLATE_NOT_SUPPORTED */
