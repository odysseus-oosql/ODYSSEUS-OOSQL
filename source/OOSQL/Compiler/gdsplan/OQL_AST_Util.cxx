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

#include <stdio.h>
#include "OOSQL_Error.h"
#include "OQL_AST_Util.hxx"

extern "C" {
#include "OQL.yacc.h"
#include "y.tab.h"
#include "OQL_AST.h"
#undef AST
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_AST_Util::OQL_AST_Util(OQL_AST& ast, OQL_GDSPOOL& pool)
{
    m_ast     = &ast;
    m_pool    = &pool;

    // assumption
    //  m_pool->intPool, m_pool->realPool, m_pool->stringPool, m_pool->stringIndexPool is already 
    //  initialized by OQL_ASTtoGDS
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_AST_Util::~OQL_AST_Util()
{
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eBADASTNODE_OOSQL   
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::select_GetN_Terms(ASTNodeIdx root)
{
    ASTNodeIdx  select;
    ASTNodeIdx  sTerm;
    Four        length;

    // Assumption : AST must be semantically and syntatically correct
    //              otherwise wrong result can be returned
    // AST structure
    //
    // QuProg
    //   |
    // QuDef* - Query
    //
    // Query - QuSel
    //          |
    //        select - from - where - groupby - having - orderby
    //          
	
    select = AST(AST(AST(root).son).brother).son;

	if(AST(AST(AST(root).son).brother).nodeName != QuSel)
		return 0;
    if(!(AST(select).nodeName == ClSelAll || AST(select).nodeName == ClSelDist))
        OOSQL_ERR(eINVALIDAST_OOSQL);

    sTerm = AST(select).son;

    length = 0;

    while(sTerm != AST_NULL)
    {
        switch(AST(sTerm).nodeName)
        {
        case ProAll:
        case ProSmp:
        case ProAs:
            break;
        default:
            OOSQL_ERR(eBADASTNODE_OOSQL);
        }

        sTerm = AST(sTerm).brother;
        length ++;
    }

    return length;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eNOTIMPLEMENTED_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::constructTermString_Query(ASTNodeIdx node)
{
    Four    e;

    switch (AST(node).nodeName) 
    {
    case null:              // null node
        e = addToStringBuffer("");
        OOSQL_CHECK_ERR(e);
        break;

    case QuSmp:             // Simple Query : arithmetic and string expressions
        e = constructTermString_QuSmp(node);
        OOSQL_CHECK_ERR(e);
        break;

    case QuCmp:             // Comparision Query : =, !=, >, <, >=, <=, like
        e = constructTermString_QuCmp(node);
        OOSQL_CHECK_ERR(e);
        break;

    case QuBln:             // Boolean Query : and, or, not
        e = constructTermString_QuBln(node);
        OOSQL_CHECK_ERR(e);
        break;

    case QuCltnIn:          // Collection Query : membership testing (in operator)
        e = constructTermString_QuCltnIn(node);
        OOSQL_CHECK_ERR(e);
        break;

    case QuCltnCmpSome:     // Collection Query : quantified comparison (existential)
        e = constructTermString_QuCltnCmpSome(node);
        OOSQL_CHECK_ERR(e);
        break;

    case QuCltnCmpAny:      // Collection Query : quantified comparison (existential)
        e = constructTermString_QuCltnCmpAny(node);
        OOSQL_CHECK_ERR(e);
        break;

    case QuCltnCmpAll:      // Collection Query : quantified comparison universal
        e = constructTermString_QuCltnCmpAll(node);
        OOSQL_CHECK_ERR(e);
        break;
    
    case QuSet:             // Set Query : intersect, union, except
        e = constructTermString_QuSet(node);
        OOSQL_CHECK_ERR(e);
        break;

    case QuGeoCmp:          // GeoSQL Query : topological relationship operators
        e = constructTermString_QuGeoCmp(node);
        OOSQL_CHECK_ERR(e);
        break;

    case QuAccPaex:         // Accessor Query : path expr
        e = constructTermString_QuAccPaex(node);
        OOSQL_CHECK_ERR(e);
        break;

    case QuAccDref:         // Accessor Query : dereferencing an object
        // Not Implemented
        e = constructTermString_QuAccDref(node);
        OOSQL_CHECK_ERR(e);
        break;

    case QuAccIcltnElem:    // Accessor Query : getting an element of indexed collection
        // Not Implemented
        e = constructTermString_QuAccIcltnElem(node);
        OOSQL_CHECK_ERR(e);
        break;

    case QuAccIcltnSub:     // Accessor Query : extracting and subcollection of indexed collection
        // Not Implemented
        e = constructTermString_QuAccIcltnSub(node);
        OOSQL_CHECK_ERR(e);
        break;

    case QuAccIcltnFr:      // Accessor Query : getting the first element of indexed collection
        // Not Implemented
        e = constructTermString_QuAccIcltnFr(node);
        OOSQL_CHECK_ERR(e);
        break;

    case QuAccIcltnLs:      // Accessor Query : getting the last element of indexed collection
        // Not Implemented
        e = constructTermString_QuAccIcltnLs(node);
        OOSQL_CHECK_ERR(e);
        break;

    case QuCltnAll:         // Collection Query : universal quantification (for all)
        e = constructTermString_QuCltnAll(node);
        OOSQL_CHECK_ERR(e);
        break;

    case QuCltnEx:          // Collection Query : existential quantification (exists in)
        e = constructTermString_QuCltnEx(node);
        OOSQL_CHECK_ERR(e);
        break;

    case QuCltnExany:       // Collection Query : existential quantification (exists)
        e = constructTermString_QuCltnExany(node);
        OOSQL_CHECK_ERR(e);
        break;

    case QuCltnUni:         // Collection Query : existential quantification (unique)
        e = constructTermString_QuCltnUni(node);
        OOSQL_CHECK_ERR(e);
        break;

    case QuCltnAggDist: 
        e = constructTermString_QuCltnAggDist(node);
        OOSQL_CHECK_ERR(e);
        break;

    case QuCltnAgg:         // Collection Query : aggregate functions
        e = constructTermString_QuCltnAgg(node);
        OOSQL_CHECK_ERR(e);
        break;

    case QuSel:             // Select Query
        OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
        break;

    case QuCnvL2s:          // Conversion Query : converting list to set
        e = constructTermString_QuCnvL2s(node);
        OOSQL_CHECK_ERR(e);
        break;

    case QuCnvElem:         // Conversion Query : extracting the element of a singleton
        e = constructTermString_QuCnvElem(node);
        OOSQL_CHECK_ERR(e);
        break;

    case QuCnvDist:         // Conversion Query : removing duplicates
        e = constructTermString_QuCnvDist(node);
        OOSQL_CHECK_ERR(e);
        break;

    case QuCnvFlat:         // Conversion Query : flattening collection of collections
        e = constructTermString_QuCnvFlat(node);
        OOSQL_CHECK_ERR(e);
        break;

    case QuCnvType:         // Conversion Query : typing an expressions
        e = constructTermString_QuCnvType(node);
        OOSQL_CHECK_ERR(e);
        break;

    case QuGeoFnNum:        // GeoSQL Query : numerical functions
        e = constructTermString_QuGeoFnNum(node);
        OOSQL_CHECK_ERR(e);
        break;

    case QuGeoFnSpa:        // GeoSQL Query : spatial functions
        e = constructTermString_QuGeoFnSpa(node);
        OOSQL_CHECK_ERR(e);
        break;

    case QuIrFnMatch:
        e = constructTermString_QuIrFnMatch(node);
        OOSQL_CHECK_ERR(e);
        break;

    case QuIrFnNmatch:
        e = constructTermString_QuIrFnNmatch(node);
        OOSQL_CHECK_ERR(e);
        break;

    case QuIrFnWeight:
        e = constructTermString_QuIrFnWeight(node);
        OOSQL_CHECK_ERR(e);
        break;

    case VaNil:             // Value : Nil
    case VaTr:              // Value : True
    case VaFls:             // Value : False
    case INTEGER:
    case REAL:
    case STRING:
    case GeoMbr:
        e = constructTermString_Value(node);
        OOSQL_CHECK_ERR(e);
        break;

    case QuCnsObj:          // Construction Query : object
    case QuCnsStruct:       // Construction Query : structure literal
    case QuCnsSet:          // Construction Query : set literal
    case QuCnsBag:          // Construction Query : bag literal
    case QuCnsLst:          // Construction Query : list literal
    case QuCnsLstrn:        // Construction Query : list literal with range
    case QuCnsArr:          // Construction Query : array literal
        e = constructTermString_QuCns(node);
        OOSQL_CHECK_ERR(e);
        break;

    case FnOrCltnobj:       // Function call or collection object construction
        e = constructTermString_FnOrCltnobj(node);
        OOSQL_CHECK_ERR(e);
        break;

    case ID:                // ID : alias name, class name, attribute name, method name
        e = addToStringBufferFromID_Node(node);
        OOSQL_CHECK_ERR(e);
        break;

    case VaLt:              // Value : integer, float, character, string literal
        OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
        break;

	case QuOgisTranslatableOp:
	case QuOgisGeometricOp:
	case QuOgisRelationalOp:
	case QuOgisMiscellaneousOp:

        e = constructTermString_QuOgisOp(node);
        OOSQL_CHECK_ERR(e);
	
		break;

    default:
        OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
        break;
    }

    return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eNOTIMPLEMENTED_OOSQL
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::constructTermString_QuDef(ASTNodeIdx node)
{
    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
}
/****************************************************************************
DESCRIPTION:

RETURN VALUE:
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::constructTermString_QuOgisOp(ASTNodeIdx node)
{
    ASTNodeIdx          s;
    ASTNodeIdx          op1;
    ASTNodeIdx          op2;
	ASTNodeIdx          op3;
    Four                e;

    s = AST(node).son;

    switch(AST(s).nodeName)
    {
        case QuOgisTrGeometryFT:
            e = addToStringBuffer("GeometryFromText");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisTrPointFT:
            e = addToStringBuffer("PointFromText");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisTrLineStringFT:
            e = addToStringBuffer("LineStringFromText");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisTrPolygonFT:
            e = addToStringBuffer("PolygonFromText");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisTrMultiPointFT:
            e = addToStringBuffer("MultiPointFromText ");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisTrMultiLineStringFT:
            e = addToStringBuffer("MultiLineStringFromText");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisTrMultiPolygonFT:
            e = addToStringBuffer("MultiPolygonFromText");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisTrGeometryCollectionFT:
            e = addToStringBuffer("GeometryCollectionFromText");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisTrGeometryFB:
            e = addToStringBuffer("GeometryFromWKB");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisTrPointFB:
            e = addToStringBuffer("PointFromWKB");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisTrLineStringFB:
            e = addToStringBuffer("LineStringFromWKB");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisTrPolygonFB:
            e = addToStringBuffer("PolygonFromWKB");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisTrMultiPointFB:
            e = addToStringBuffer("MultiPointFromWKB");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisTrMultiLineStringFB:
            e = addToStringBuffer("MultiLineFromWKB");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisTrMultiPolygonFB:
            e = addToStringBuffer("MultiPolygonFromWKB");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisTrGeometryCollectionFB:
            e = addToStringBuffer("GeometryCollectionFromWKB");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisTrAsText:
            e = addToStringBuffer("AsText");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisTrAsBinary:
            e = addToStringBuffer("AsBinary");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisGeDimension:
            e = addToStringBuffer("Dimension");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisGeGeometryType:
            e = addToStringBuffer("GeometryType");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisGeSRID:
            e = addToStringBuffer("SRID");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisGeBoundary:
            e = addToStringBuffer("Boundary");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisGeLength:
            e = addToStringBuffer("OGIS_Length");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisGeX:
            e = addToStringBuffer("OGIS_X");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisGeY:
            e = addToStringBuffer("OGIS_Y");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisGeArea:
            e = addToStringBuffer("Area");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisGeNumGeometries:
            e = addToStringBuffer("Numgeometries");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisGeNumPoints:
            e = addToStringBuffer("NumPoints");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisGeNumInteriorRings:
            e = addToStringBuffer("NumInteriorRings");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisReIsEmpty:
            e = addToStringBuffer("IsEmpty");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisReIsSimple:
            e = addToStringBuffer("IsSimple");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisReIsClosed:
            e = addToStringBuffer("IsClosed");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisReIsRing:
            e = addToStringBuffer("IsRing");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisReContains:
            e = addToStringBuffer("Contains");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisReCrosses:
            e = addToStringBuffer("Crosses");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisReDisjoint:
            e = addToStringBuffer("Disjoint");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisReEquals:
            e = addToStringBuffer("Equals");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisReIntersects:
            e = addToStringBuffer("Intersects");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisReOverlaps:
            e = addToStringBuffer("Overlaps");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisReRelated:
            e = addToStringBuffer("Related");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisReTouches:
            e = addToStringBuffer("Touches");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisReWithin:
            e = addToStringBuffer("Within");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisMiDifference:
            e = addToStringBuffer("Difference");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisMiIntersection:
            e = addToStringBuffer("Intersection");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisMiSymDifference:
            e = addToStringBuffer("SymDifference");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisMiUnion:
            e = addToStringBuffer("Union");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisMiDistance:
            e = addToStringBuffer("Distance");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisMiEnvelope:
            e = addToStringBuffer("Envelope");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisMiBuffer:
            e = addToStringBuffer("Buffer");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisMiConvexHull:
            e = addToStringBuffer("ConvexHull");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisMiExteriorRing:
            e = addToStringBuffer("ExteriorRing");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisMiInteriorRingN:
            e = addToStringBuffer("InteriorRingN");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisMiCentriod:
            e = addToStringBuffer("Centroid");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisMiStartPoint:
            e = addToStringBuffer("StartPoint");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisMiEndPoint:
            e = addToStringBuffer("EndPoint");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisMiPointOnSurface:
            e = addToStringBuffer("PointOnSurface");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisMiPointN:
            e = addToStringBuffer("PointN");
            OOSQL_CHECK_ERR(e);
            break;
        case QuOgisMiGeometryN:
            e = addToStringBuffer("GeometryN");
            OOSQL_CHECK_ERR(e);
            break;
		default:
            OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
	}

    e = addToStringBuffer("(");
    OOSQL_CHECK_ERR(e);

	op1 = AST(s).son;

    e = constructTermString_Query(op1);
    OOSQL_CHECK_ERR(e);

	op2 = AST(op1).brother;
	if(op2 != AST_NULL)
	{
		e = addToStringBuffer(",");
        OOSQL_CHECK_ERR(e);

        e = constructTermString_Query(op2);
        OOSQL_CHECK_ERR(e);

		op3 = AST(op2).brother;
		if(op3 != AST_NULL)
		{
			e = addToStringBuffer(",");
			OOSQL_CHECK_ERR(e);

			e = constructTermString_Query(op3);
			OOSQL_CHECK_ERR(e);
		}
	}
    
    e = addToStringBuffer(")");
    OOSQL_CHECK_ERR(e);
    
	return eNOERROR;
            
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eUNHANDLED_CASE_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::constructTermString_QuSmp(ASTNodeIdx node)
{
    ASTNodeIdx          s;
    ASTNodeIdx          op1;
    ASTNodeIdx          op2;
    Four                e;

    s = AST(node).son;

    // divide operators into binary or unary operator
    if(AST(s).nodeName == OpUnMin || AST(s).nodeName == OpUnAbs)
    {
        // unary operator
        switch(AST(s).nodeName)
        {
        case OpUnMin:
            // 1. int, float unary minus
            e = addToStringBuffer("-");
            OOSQL_CHECK_ERR(e);

            op1 = AST(s).brother;
            
            e = constructTermString_Query(op1);
            OOSQL_CHECK_ERR(e);
            break;

        case OpUnAbs:
            // 1. int, float absolute value
            e = addToStringBuffer("ABS(");
            OOSQL_CHECK_ERR(e);

            op1 = AST(s).brother;
            
            e = constructTermString_Query(op1);
            OOSQL_CHECK_ERR(e);

            e = addToStringBuffer(")");
            OOSQL_CHECK_ERR(e);
            break;

        default:
            OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
            break;
        }
    }
    else
    {
        // binary operators
        switch ( AST(s).nodeName )      // kind of operator
        {   
        case OpBiPlu:
            op1 = AST(s).brother;
            op2 = AST(op1).brother;
            
            e = constructTermString_Query(op1);  OOSQL_CHECK_ERR(e);

            e = addToStringBuffer(" + ");
            OOSQL_CHECK_ERR(e);

            e = constructTermString_Query(op2);  OOSQL_CHECK_ERR(e);
            
            break;
                
        case OpBiMin:
            // 1. int, float subtraction
            op1 = AST(s).brother;
            op2 = AST(op1).brother;
            
            e = constructTermString_Query(op1);  OOSQL_CHECK_ERR(e);

            e = addToStringBuffer(" - ");
            OOSQL_CHECK_ERR(e);

            e = constructTermString_Query(op2);  OOSQL_CHECK_ERR(e);
            
            break;

        case OpBiMul:
            // 1. int, float multiplication
            op1 = AST(s).brother;
            op2 = AST(op1).brother;
            
            e = constructTermString_Query(op1);  OOSQL_CHECK_ERR(e);

            e = addToStringBuffer(" * ");
            OOSQL_CHECK_ERR(e);

            e = constructTermString_Query(op2);  OOSQL_CHECK_ERR(e);
            
            break;

        case OpBiDiv:
            // 1. int, float division
             
            op1 = AST(s).brother;
            op2 = AST(op1).brother;
            
            e = constructTermString_Query(op1);  OOSQL_CHECK_ERR(e);

            e = addToStringBuffer(" / ");
            OOSQL_CHECK_ERR(e);

            e = constructTermString_Query(op2);  OOSQL_CHECK_ERR(e);
            
            break;

        case OpBiMod:
            // 1. int modulo
             
            op1 = AST(s).brother;
            op2 = AST(op1).brother;
            
            e = constructTermString_Query(op1);  OOSQL_CHECK_ERR(e);

            e = addToStringBuffer(" % ");
            OOSQL_CHECK_ERR(e);

            e = constructTermString_Query(op2);  OOSQL_CHECK_ERR(e);
            
            break;

        case OpBiStrcat:
            // 1. string concatenation
            op1 = AST(s).brother;
            op2 = AST(op1).brother;
            
            e = constructTermString_Query(op1);  OOSQL_CHECK_ERR(e);

            e = addToStringBuffer(" + ");
            OOSQL_CHECK_ERR(e);

            e = constructTermString_Query(op2);  OOSQL_CHECK_ERR(e);
            
            break;
            
        default:
            OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
        }   
    }

    return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eUNHANDLED_CASE_OOSQL 
< eNOERROR         

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::constructTermString_QuCmp(ASTNodeIdx node)
{
    ASTNodeIdx       s;
    ASTNodeIdx       op1;
    ASTNodeIdx       op2;
    Four             e;

    s = AST(node).son;

    switch ( AST(s).nodeName ) 
    {
    case OpCmpLike:
        // 1. string matching: opr1:(string), opr2:(string literal containing wild char.)
        op1 = AST(s).brother;
        op2 = AST(op1).brother;

        e = constructTermString_Query(op1);  OOSQL_CHECK_ERR(e);

        e = addToStringBuffer(" LIKE ");
        OOSQL_CHECK_ERR(e);

        e = constructTermString_Query(op2);  OOSQL_CHECK_ERR(e);

        break;

    case OpCmpEq:
        op1 = AST(s).brother;
        op2 = AST(op1).brother;
        
        e = constructTermString_Query(op1);  OOSQL_CHECK_ERR(e);

        e = addToStringBuffer(" = ");
        OOSQL_CHECK_ERR(e);

        e = constructTermString_Query(op2);  OOSQL_CHECK_ERR(e);
        
        break;
    case OpCmpNe:
        // 1. literal (int, real, char, string)
        // 2. mutable object
        // 3. dereferenced object (value)
        op1 = AST(s).brother;
        op2 = AST(op1).brother;
        
        e = constructTermString_Query(op1);  OOSQL_CHECK_ERR(e);

        e = addToStringBuffer(" != ");
        OOSQL_CHECK_ERR(e);

        e = constructTermString_Query(op2);  OOSQL_CHECK_ERR(e);
        
        break;
        
    case OpCmpGt:
        op1 = AST(s).brother;
        op2 = AST(op1).brother;
        
        e = constructTermString_Query(op1);  OOSQL_CHECK_ERR(e);

        e = addToStringBuffer(" > ");
        OOSQL_CHECK_ERR(e);

        e = constructTermString_Query(op2);  OOSQL_CHECK_ERR(e);
        
        break;
    case OpCmpLt:
        op1 = AST(s).brother;
        op2 = AST(op1).brother;
        
        e = constructTermString_Query(op1);  OOSQL_CHECK_ERR(e);

        e = addToStringBuffer(" < ");
        OOSQL_CHECK_ERR(e);

        e = constructTermString_Query(op2);  OOSQL_CHECK_ERR(e);
        
        break;
    case OpCmpGe:
        op1 = AST(s).brother;
        op2 = AST(op1).brother;
        
        e = constructTermString_Query(op1);  OOSQL_CHECK_ERR(e);

        e = addToStringBuffer(" >= ");
        OOSQL_CHECK_ERR(e);

        e = constructTermString_Query(op2);  OOSQL_CHECK_ERR(e);
        
        break;
    case OpCmpLe:
        // 1. literal (int, real, char, string)
        // 2. set, bag inclusion

        op1 = AST(s).brother;
        op2 = AST(op1).brother;
        
        e = constructTermString_Query(op1);  OOSQL_CHECK_ERR(e);

        e = addToStringBuffer(" <= ");
        OOSQL_CHECK_ERR(e);

        e = constructTermString_Query(op2);  OOSQL_CHECK_ERR(e);
        
        break;
	case OpCmpIsNull:
		op1 = AST(s).brother;
		
		e = constructTermString_Query(op1);  OOSQL_CHECK_ERR(e);
		
		e = addToStringBuffer(" IS NULL ");
		OOSQL_CHECK_ERR(e);
		
		break;
	case OpCmpIsNotNull:
		op1 = AST(s).brother;
		
		e = constructTermString_Query(op1);  OOSQL_CHECK_ERR(e);
		
		e = addToStringBuffer(" IS NOT NULL ");
		OOSQL_CHECK_ERR(e);
		
		break;
    }

    return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eUNHANDLED_CASE_OOSQL 
< eNOERROR         

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::constructTermString_QuBln(ASTNodeIdx node)
{
    ASTNodeIdx       s, op1, op2;
    Four             e;

    s = AST(node).son;

    if(AST(s).nodeName == OpBlnNot)
    {
        // unary operator
        switch(AST(s).nodeName)
        {
        case OpBlnNot:
            // 1. b1 : boolean
            op1 = AST(s).brother;

            e = addToStringBuffer(" NOT ");
            OOSQL_CHECK_ERR(e);

            constructTermString_Query(op1);

            break;

        default:
            OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
            break;  
        }
    }
    else
    {
        // binary operator
        switch (AST(s).nodeName) { 
        case OpBlnAnd:
            op1 = AST(s).brother;
            op2 = AST(op1).brother;
            
            e = constructTermString_Query(op1);  OOSQL_CHECK_ERR(e);

            e = addToStringBuffer(" AND ");
            OOSQL_CHECK_ERR(e);

            e = constructTermString_Query(op2);  OOSQL_CHECK_ERR(e);

            break;
        case OpBlnOr:
            op1 = AST(s).brother;
            op2 = AST(op1).brother;
            
            e = constructTermString_Query(op1);  OOSQL_CHECK_ERR(e);

            e = addToStringBuffer(" OR ");
            OOSQL_CHECK_ERR(e);

            e = constructTermString_Query(op2);  OOSQL_CHECK_ERR(e);

            break;

        default:
            OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
            break;  
        }
    }

    return eNOERROR;

}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eNOTIMPLEMENTED_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::constructTermString_QuCns(ASTNodeIdx node)
{
    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::constructTermString_QuAccPaex(ASTNodeIdx node)
{
    Four                    e;

    // construct path expression
    e = constructTermString_QuAccPaexRecursive(node);
    OOSQL_CHECK_ERR(e);

    return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eBADASTNODE_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::constructTermString_QuAccPaexRecursive(ASTNodeIdx node)
{
    ASTNodeIdx          b1,b2;
    Four                e;

    switch (AST(node).nodeName) 
    {
    case QuAccPaex:
        b1 = AST(node).son;
        b2 = AST(b1).brother;

        if(AST(b1).nodeName == QuCnvType)
        {
            e = constructTermString_QuAccPaexRecursive(b1);
            OOSQL_CHECK_ERR(e);
            
            e = addToStringBuffer(".");
            OOSQL_CHECK_ERR(e);

            e = constructTermString_QuAccPaexRecursive(b2);
            OOSQL_CHECK_ERR(e);
        }
        else
        {
            e = constructTermString_QuAccPaexRecursive(b1);
            OOSQL_CHECK_ERR(e);

            e = addToStringBuffer(".");
            OOSQL_CHECK_ERR(e);

            e = constructTermString_QuAccPaexRecursive(b2);
            OOSQL_CHECK_ERR(e);
        }

        break;

    case ID:
        e = addToStringBufferFromID_Node(node);
        OOSQL_CHECK_ERR(e);
        break;

    case FnOrCltnobj:
        b1 = AST(node).son;
        b2 = AST(b1).brother;

        e = addToStringBufferFromID_Node(b1);
        OOSQL_CHECK_ERR(e);
        e = constructTermString_Arguments(b2);
        OOSQL_CHECK_ERR(e);

        break;

    case QuCnvType:
        // this node has two children
        // first child is an id, which represents substituted class
        // second child is an pathexpr
        b1 = AST(node).son;
        b2 = AST(b1).brother;

        e = constructTermString_QuAccPaexRecursive(b2);
        OOSQL_CHECK_ERR(e);

        e = addToStringBuffer("[");
        OOSQL_CHECK_ERR(e);
        e = addToStringBufferFromID_Node(b1);
        OOSQL_CHECK_ERR(e);
        e = addToStringBuffer("]");
        OOSQL_CHECK_ERR(e);

        break;

    case QuCnvTypeStar:
        // this node has two children
        // first child is an id, which represents substituted class
        // second child is an pathexpr
        b1 = AST(node).son;
        b2 = AST(b1).brother;

        e = constructTermString_QuAccPaexRecursive(b2);
        OOSQL_CHECK_ERR(e);

        e = addToStringBuffer("[");
        OOSQL_CHECK_ERR(e);
        e = addToStringBufferFromID_Node(b1);
        OOSQL_CHECK_ERR(e);
        e = addToStringBuffer("*]");
        OOSQL_CHECK_ERR(e);

        break;
    case Method:
        b1 = AST(node).son;
        b2 = AST(b1).brother;

        e = addToStringBufferFromID_Node(b1);
        OOSQL_CHECK_ERR(e);
        e = constructTermString_Arguments(b2);
        OOSQL_CHECK_ERR(e);

        break;

    case QuGeoFnSpa:
        b1 = AST(node).son;

        e = constructTermString_QuGeoFnSpa(b1);
        OOSQL_CHECK_ERR(e);

        break;

    case QuCnsObj:
        b1 = AST(node).son;

        e = constructTermString_QuCns(b1);
        OOSQL_CHECK_ERR(e);

        break;

    default:
        OOSQL_ERR(eBADASTNODE_OOSQL);
        break;
    }

    return eNOERROR;

}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eNOTIMPLEMENTED_OOSQL
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::constructTermString_IcltnElem(ASTNodeIdx node)
{
    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eNOTIMPLEMENTED_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::constructTermString_QuAccIcltnSub(ASTNodeIdx node)
{
    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eNOTIMPLEMENTED_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::constructTermString_QuAccIcltnFr(ASTNodeIdx node)
{
    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eNOTIMPLEMENTED_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::constructTermString_QuAccIcltnLs(ASTNodeIdx node)
{
    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eNOTIMPLEMENTED_OOSQL
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::constructTermString_QuCltnAll(ASTNodeIdx node)
{
    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eNOTIMPLEMENTED_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::constructTermString_QuCltnEx(ASTNodeIdx node)
{
    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eNOTIMPLEMENTED_OOSQL 
< eNOERROR         

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::constructTermString_QuCltnExany(ASTNodeIdx node)
{
    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eNOTIMPLEMENTED_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::constructTermString_QuCltnUni(ASTNodeIdx node)
{
    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eNOTIMPLEMENTED_OOSQL 
< eNOERROR         

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::constructTermString_QuCltnIn(ASTNodeIdx node)
{
    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eNOTIMPLEMENTED_OOSQL
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::constructTermString_QuCltnCmpSome(ASTNodeIdx node)
{
    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eNOTIMPLEMENTED_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::constructTermString_QuCltnCmpAny(ASTNodeIdx node)
{
    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eNOTIMPLEMENTED_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::constructTermString_QuCltnCmpAll(ASTNodeIdx node)
{
    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::constructTermString_QuCltnAgg(ASTNodeIdx node)
{
    ASTNodeIdx              s;
    ASTNodeIdx              p1;
    Four                    e;

    s = AST(node).son;

    switch ( AST(s).nodeName ) 
    {
    case FnAggCntall:       // count(*)
        e = addToStringBuffer("COUNT(*)");
        OOSQL_CHECK_ERR(e);
        break;

    case FnAggCnt:          // count(...)
        e = addToStringBuffer("COUNT(");
        OOSQL_CHECK_ERR(e);

        p1 = AST(s).brother;
        e = constructTermString_Query(p1);
        OOSQL_CHECK_ERR(e);

        e = addToStringBuffer(")");
        OOSQL_CHECK_ERR(e);
        break;
    case FnAggSum:          // sum(...)
        e = addToStringBuffer("SUM(");
        OOSQL_CHECK_ERR(e);

        p1 = AST(s).brother;
        e = constructTermString_Query(p1);
        OOSQL_CHECK_ERR(e);

        e = addToStringBuffer(")");
        OOSQL_CHECK_ERR(e);
        break;
    case FnAggMin:          // min(...)
        e = addToStringBuffer("MIN(");
        OOSQL_CHECK_ERR(e);

        p1 = AST(s).brother;
        e = constructTermString_Query(p1);
        OOSQL_CHECK_ERR(e);

        e = addToStringBuffer(")");
        OOSQL_CHECK_ERR(e);
        break;
    case FnAggMax:          // max(...)
        e = addToStringBuffer("MAX(");
        OOSQL_CHECK_ERR(e);

        p1 = AST(s).brother;
        e = constructTermString_Query(p1);
        OOSQL_CHECK_ERR(e);

        e = addToStringBuffer(")");
        OOSQL_CHECK_ERR(e);
        break;
    case FnAggAvg:          // avg(...)
        e = addToStringBuffer("AVG(");
        OOSQL_CHECK_ERR(e);

        p1 = AST(s).brother;
        e = constructTermString_Query(p1);
        OOSQL_CHECK_ERR(e);

        e = addToStringBuffer(")");
        OOSQL_CHECK_ERR(e);
        break;
    }

    return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
< eNOERROR         

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::constructTermString_QuCltnAggDist(ASTNodeIdx node)
{
    ASTNodeIdx              s;
    ASTNodeIdx              p1;
    Four                    e;

    s = AST(node).son;

    switch ( AST(s).nodeName ) 
    {
    case FnAggCntall:       // count(*)
        e = addToStringBuffer("COUNT(DISTINCT *)");
        OOSQL_CHECK_ERR(e);
        break;

    case FnAggCnt:          // count(...)
        e = addToStringBuffer("COUNT(DISTINCT ");
        OOSQL_CHECK_ERR(e);

        p1 = AST(s).brother;
        e = constructTermString_Query(p1);
        OOSQL_CHECK_ERR(e);

        e = addToStringBuffer(")");
        OOSQL_CHECK_ERR(e);
        break;
    case FnAggSum:          // sum(...)
        e = addToStringBuffer("SUM(DISTINCT ");
        OOSQL_CHECK_ERR(e);

        p1 = AST(s).brother;
        e = constructTermString_Query(p1);
        OOSQL_CHECK_ERR(e);

        e = addToStringBuffer(")");
        OOSQL_CHECK_ERR(e);
        break;
    case FnAggMin:          // min(...)
        e = addToStringBuffer("MIN(DISTINCT ");
        OOSQL_CHECK_ERR(e);

        p1 = AST(s).brother;
        e = constructTermString_Query(p1);
        OOSQL_CHECK_ERR(e);

        e = addToStringBuffer(")");
        OOSQL_CHECK_ERR(e);
        break;
    case FnAggMax:          // max(...)
        e = addToStringBuffer("MAX(DISTINCT ");
        OOSQL_CHECK_ERR(e);

        p1 = AST(s).brother;
        e = constructTermString_Query(p1);
        OOSQL_CHECK_ERR(e);

        e = addToStringBuffer(")");
        OOSQL_CHECK_ERR(e);
        break;
    case FnAggAvg:          // avg(...)
        e = addToStringBuffer("AVG(DISTINCT ");
        OOSQL_CHECK_ERR(e);

        p1 = AST(s).brother;
        e = constructTermString_Query(p1);
        OOSQL_CHECK_ERR(e);

        e = addToStringBuffer(")");
        OOSQL_CHECK_ERR(e);
        break;
    }

    return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eNOTIMPLEMENTED_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::constructTermString_QuSet(ASTNodeIdx node)
{
    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eNOTIMPLEMENTED_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::constructTermString_QuCnvL2s(ASTNodeIdx node)
{
    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eNOTIMPLEMENTED_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::constructTermString_QuCnvElem(ASTNodeIdx node)
{
    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eNOTIMPLEMENTED_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::constructTermString_QuCnvDist(ASTNodeIdx node)
{
    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eNOTIMPLEMENTED_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::constructTermString_QuCnvFlat(ASTNodeIdx node)
{
    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eNOTIMPLEMENTED_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::constructTermString_QuCnvType(ASTNodeIdx node)
{
    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::constructTermString_ID(ASTNodeIdx node)
{
    Four e;

    e = addToStringBufferFromID_Node(node); 
    OOSQL_CHECK_ERR(e);
    return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eNOTIMPLEMENTED_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::constructTermString_QuGeoCmp(ASTNodeIdx node)
{
    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::constructTermString_QuGeoFnNum(ASTNodeIdx node)
{
	ASTNodeIdx              s, s1, s2;
    Four                    e;
	
	s = AST(node).son;

	switch(AST(s).nodeName)
	{
	case FnGeoDistance:
		e = addToStringBuffer("DISTANCE");		
		OOSQL_CHECK_ERR(e);
		break;
	case FnGeoArea:
		e = addToStringBuffer("AREA");			
		OOSQL_CHECK_ERR(e);
		break;
	case FnGeoLength:
		e = addToStringBuffer("LENGTH");		
		OOSQL_CHECK_ERR(e);
		break;
	default:
		OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
	}
	
	e = addToStringBuffer("(");		
	OOSQL_CHECK_ERR(e);

    s1 = AST(s).brother;
	if(AST(s1).brother != AST_NULL)
		s2 = AST(s1).brother;
	else
		s2 = null;

    // check each operand type and make global data structure
    e = constructTermString_Query(s1);
    OOSQL_CHECK_ERR(e);

	if(s2 != null)
	{
		e = addToStringBuffer(", ");
		OOSQL_CHECK_ERR(e);

		e = constructTermString_Query(s2);     
		OOSQL_CHECK_ERR(e);
	}
    
    e = addToStringBuffer(")");
    OOSQL_CHECK_ERR(e);

    return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eNOTIMPLEMENTED_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::constructTermString_QuGeoFnSpa(ASTNodeIdx node)
{
    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eTYPE_ERROR_OOSQL   
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::constructTermString_GeoMbr(ASTNodeIdx node)
{
    ASTNodeIdx      s1, s2, s3, s4;
	char            valueString[100];
	Four			e;

    s1 = AST(node).son;
    s2 = AST(s1).brother;
    s3 = AST(s2).brother;
    s4 = AST(s3).brother;

	e = addToStringBuffer("[");
	OOSQL_CHECK_ERR(e);

    if(!(AST(s1).nodeName == INTEGER))
        OOSQL_ERR(eTYPE_ERROR_OOSQL);
    if(!(AST(s2).nodeName == INTEGER))
        OOSQL_ERR(eTYPE_ERROR_OOSQL);
    if(!(AST(s3).nodeName == INTEGER))
        OOSQL_ERR(eTYPE_ERROR_OOSQL);
    if(!(AST(s4).nodeName == INTEGER))
        OOSQL_ERR(eTYPE_ERROR_OOSQL);

	sprintf(valueString, "%ld, ", m_pool->intPool[AST(s1).tokenVal]);
	e = addToStringBuffer(valueString);
	OOSQL_CHECK_ERR(e);

	sprintf(valueString, "%ld, ", m_pool->intPool[AST(s2).tokenVal]);
	e = addToStringBuffer(valueString);
	OOSQL_CHECK_ERR(e);

	sprintf(valueString, "%ld, ", m_pool->intPool[AST(s3).tokenVal]);
	e = addToStringBuffer(valueString);
	OOSQL_CHECK_ERR(e);

	sprintf(valueString, "%ld", m_pool->intPool[AST(s4).tokenVal]);
	e = addToStringBuffer(valueString);
	OOSQL_CHECK_ERR(e);

	e = addToStringBuffer("]");
	OOSQL_CHECK_ERR(e);

    return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::constructTermString_QuIrFnMatch(ASTNodeIdx node)
{
    ASTNodeIdx              s1, s2, s3;
    Four                    e;
    
    e = addToStringBuffer("MATCH(");
    OOSQL_CHECK_ERR(e);

    s1 = AST(node).son;
    s2 = AST(s1).brother;
    if(AST(s2).brother != AST_NULL)
        s3 = AST(s2).brother;
    else
        s3 = null;

    // check each operand type and make global data structure
    e = constructTermString_Query(s1);
    OOSQL_CHECK_ERR(e);

    e = constructTermString_TextIrExpression(s2);     
    OOSQL_CHECK_ERR(e);
    
    if(s3 != null)
    {
        e = constructTermString_Query(s3);
        OOSQL_CHECK_ERR(e);
    }

    e = addToStringBuffer(")");
    OOSQL_CHECK_ERR(e);

    return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eNOTIMPLEMENTED_OOSQL
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::constructTermString_QuIrFnNmatch(ASTNodeIdx node)
{
    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::constructTermString_QuIrFnWeight(ASTNodeIdx node)
{
    ASTNodeIdx              s;
    Four                    e;
    
    e = addToStringBuffer("WEIGHT(");
    OOSQL_CHECK_ERR(e);

    s = AST(node).son;

    // check each operand type and make global data structure
    if(s != AST_NULL)
    {
        e = constructTermString_Query(s);
        OOSQL_CHECK_ERR(e);
    }

    e = addToStringBuffer(")");
    OOSQL_CHECK_ERR(e);

    return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eNOTIMPLEMENTED_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::constructTermString_QuAccDref(ASTNodeIdx node)
{
    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eNOTIMPLEMENTED_OOSQL
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::constructTermString_QuAccIcltnElem(ASTNodeIdx node)
{
    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eNOTIMPLEMENTED_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::constructTermString_FnOrCltnobj(ASTNodeIdx node)
{
	ASTNodeIdx		s;
	ASTNodeIdx		sArgument;
    Four			e;	
	SimpleString	functionName;
	Four			i, j;
	Boolean			flag;

    if(AST(node).nodeName != FnOrCltnobj)
        OOSQL_ERR(eBADASTNODE_OOSQL);
    
    // Possible type of FnOrCltnobj's is ID
    // ID is the name of member function or collection
    s = AST(node).son;

	e = constructTermString_Query(s);
	OOSQL_CHECK_ERR(e);

	e = addToStringBuffer("()");
    OOSQL_CHECK_ERR(e);

    return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::constructTermString_Arguments(ASTNodeIdx node)
{
    // Construct argument list in argumentPool
    Four                    nth;
    Four                    nArguments;
    ASTNodeIdx              b;
    Four                    e;
    
    // count the number of arguments
    b           = node;
    nArguments  = 0;
    while(b != AST_NULL)
    {
        b = AST(b).brother;
        nArguments ++;
    }

    // fill argument m_pool elements
    b     = node;
    nth   = 0;
    while(b != AST_NULL)
    {
        // possible type of b are
        //  1. path expression
        //  2. value (string, integer, real, bool, structure, collection)
        //  3. function
        //  4. domain

        e = constructTermString_Query(b);
        OOSQL_CHECK_ERR(e);
        
        b = AST(b).brother;
        nth   ++;

        if(b != AST_NULL)
        {
            e = addToStringBuffer(", ");
            OOSQL_CHECK_ERR(e);
        }
    }

    return eNOERROR;
}


/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eUNHANDLED_CASE_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::constructTermString_Value(ASTNodeIdx node)
{
    char            valueString[100];
    Four            e;

    switch(AST(node).nodeName)
    {
    case VaNil:             // Value : Nil
        e = addToStringBuffer("NIL");
        OOSQL_CHECK_ERR(e);
        break;          

    case VaTr:              // Value : True
        e = addToStringBuffer("TRUE");
        OOSQL_CHECK_ERR(e);
        break;

    case VaFls:             // Value : False
        e = addToStringBuffer("FALSE");
        OOSQL_CHECK_ERR(e);
        break;

    case INTEGER:
        sprintf(valueString, "%ld", m_pool->intPool[AST(node).tokenVal]);
        e = addToStringBuffer(valueString);
        OOSQL_CHECK_ERR(e);
        break;

    case REAL:
        sprintf(valueString, "%f", m_pool->realPool[AST(node).tokenVal]);
        e = addToStringBuffer(valueString);
        OOSQL_CHECK_ERR(e);
        break;

    case STRING:
        e = addToStringBufferFromSTRING_Node(node);
        OOSQL_CHECK_ERR(e);
        break;
    
    case GeoMbr:
        e = constructTermString_GeoMbr(node);
        OOSQL_CHECK_ERR(e);
        break;
    default:
        OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
    }

    return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eUNHANDLED_CASE_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::constructTermString_TextIrExpression(ASTNodeIdx node)
{
    Four    e;
    
    switch(AST(node).nodeName)
    {
    case OpIrBlnAccum:          // +
    case OpIrBlnOr:             // |
    case OpIrBlnAnd:            // &
    case OpIrBlnMinus:          // -
    case OpIrBlnThreshold:      // >
    case OpIrBlnMultiply:       // *
    case OpIrBlnMax:            // :
    case OpIrBlnNear:           // 
        e = constructTermString_TextIrOperator(node);
        OOSQL_CHECK_ERR(e);
        break;

    case STRING:
        e = constructTermString_Value(node);
        OOSQL_CHECK_ERR(e);
        break;

    case REAL:
    case INTEGER:
        e = constructTermString_Value(node);
        OOSQL_CHECK_ERR(e);
        break;

    default:
        OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
    }

    return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eUNHANDLED_CASE_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::constructTermString_TextIrOperator(ASTNodeIdx node)
{
    Four                e;
    ASTNodeIdx          op1, op2;

    switch(AST(node).nodeName)
    {
    case OpIrBlnAccum:          // +
        op1 = AST(node).son;
        op2 = AST(op1).brother;

        e = constructTermString_TextIrExpression(op1);   OOSQL_CHECK_ERR(e);

        e = addToStringBuffer(" + ");
        OOSQL_CHECK_ERR(e);

        e = constructTermString_TextIrExpression(op2);   OOSQL_CHECK_ERR(e);

        break;
    case OpIrBlnOr:             // |
        op1 = AST(node).son;
        op2 = AST(op1).brother;

        e = constructTermString_TextIrExpression(op1);   OOSQL_CHECK_ERR(e);

        e = addToStringBuffer(" | ");
        OOSQL_CHECK_ERR(e);

        e = constructTermString_TextIrExpression(op2);   OOSQL_CHECK_ERR(e);

        break;
    case OpIrBlnAnd:            // &
        op1 = AST(node).son;
        op2 = AST(op1).brother;

        e = constructTermString_TextIrExpression(op1);   OOSQL_CHECK_ERR(e);

        e = addToStringBuffer(" & ");
        OOSQL_CHECK_ERR(e);

        e = constructTermString_TextIrExpression(op2);   OOSQL_CHECK_ERR(e);

        break;
    case OpIrBlnMinus:          // -
        op1 = AST(node).son;
        op2 = AST(op1).brother;

        e = constructTermString_TextIrExpression(op1);   OOSQL_CHECK_ERR(e);

        e = addToStringBuffer(" - ");
        OOSQL_CHECK_ERR(e);

        e = constructTermString_TextIrExpression(op2);   OOSQL_CHECK_ERR(e);

        break;
    case OpIrBlnThreshold:      // >
        op1 = AST(node).son;
        op2 = AST(op1).brother;

        e = constructTermString_TextIrExpression(op1);   OOSQL_CHECK_ERR(e);

        e = addToStringBuffer(" > ");
        OOSQL_CHECK_ERR(e);

        e = constructTermString_TextIrExpression(op2);   OOSQL_CHECK_ERR(e);

        break;
    case OpIrBlnMultiply:       // *
        op1 = AST(node).son;
        op2 = AST(op1).brother;

        e = constructTermString_TextIrExpression(op1);   OOSQL_CHECK_ERR(e);

        e = addToStringBuffer(" * ");
        OOSQL_CHECK_ERR(e);

        e = constructTermString_TextIrExpression(op2);   OOSQL_CHECK_ERR(e);

        break;
    case OpIrBlnMax:            // :
        op1 = AST(node).son;
        op2 = AST(op1).brother;

        e = constructTermString_TextIrExpression(op1);   OOSQL_CHECK_ERR(e);

        e = addToStringBuffer(" : ");
        OOSQL_CHECK_ERR(e);

        e = constructTermString_TextIrExpression(op2);   OOSQL_CHECK_ERR(e);

        break;
    case OpIrBlnNear:           // ~
        op1 = AST(node).son;
        op2 = AST(op1).brother;

        e = constructTermString_TextIrExpression(op1);   OOSQL_CHECK_ERR(e);

        e = addToStringBuffer(" ~ ");
        OOSQL_CHECK_ERR(e);

        e = constructTermString_TextIrExpression(op2);   OOSQL_CHECK_ERR(e);

        break;

	case OpIrBlnNearWithOrder:	// ^
        op1 = AST(node).son;
        op2 = AST(op1).brother;

        e = constructTermString_TextIrExpression(op1);   OOSQL_CHECK_ERR(e);

        e = addToStringBuffer(" ^ ");
        OOSQL_CHECK_ERR(e);

        e = constructTermString_TextIrExpression(op2);   OOSQL_CHECK_ERR(e);

        break;

    default:
        OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
    }

    return eNOERROR;
}


/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eBADPARAMETER_OOSQL 
eNOTIMPLEMENTED_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::select_GetIthTerm(ASTNodeIdx root, Four ith, Four astringBufferSize, char* astringBuffer)
{
    ASTNodeIdx  select;
    ASTNodeIdx  sTerm;
    ASTNodeIdx  son;
    Four        length;
    Four        e;

    // Assumption : AST must be semantically and syntatically correct
    //              otherwise wrong result can be returned
    // AST structure
    //
    // QuProg
    //   |
    // QuDef* - Query
    //
    // Query  QuSel
    //          |
    //        select - from - where - groupby - having - orderby
    //          

    // init m_stringBuffer
    m_stringBuffer     = astringBuffer;
    m_stringBufferSize = astringBufferSize;
    strcpy(m_stringBuffer, "");

    // point select AST node
    select = AST(AST(AST(root).son).brother).son;

    // check AST node
    if(!(AST(select).nodeName == ClSelAll || AST(select).nodeName == ClSelDist))
	{
		return eNOERROR;
	}

    sTerm = AST(select).son;

    length = 0;

    while(sTerm != AST_NULL)
    {
        if(ith == length)       // make return string
        {
            switch(AST(sTerm).nodeName)
            {
				case ProAllLogicalID:
					// select #
					e = addToStringBuffer("#");
					OOSQL_CHECK_ERR(e);
					return eNOERROR;

                case ProAll:
                    // select *
                    e = addToStringBuffer("*");
                    OOSQL_CHECK_ERR(e);
                    return eNOERROR;

                case ProSmp:
                    // select [pathexpr | aggrfunc | function | value | expr | construct]
                    son = AST(sTerm).son;

                    e = constructTermString_Query(son);
                    OOSQL_CHECK_ERR(e);

                    return eNOERROR;
                case ProAs:
                    // select query AS ID
                    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);

                default:
                    OOSQL_ERR(eBADASTNODE_OOSQL);
            }
        }

        sTerm = AST(sTerm).brother;
        length ++;
    }

    OOSQL_ERR(eBADPARAMETER_OOSQL);
}


/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eSTRINGBUFFER_OVERFLOW_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::addToStringBufferFromStringPool(StringPoolIndex poolIndex)
{
    int   i;
    char* s;

    s = m_stringBuffer;

    // move to end of string
    while(*s != '\0')
    {
        s ++;

        if((s - m_stringBuffer + 1) > m_stringBufferSize)
            return eSTRINGBUFFER_OVERFLOW_OOSQL;
    }

    for(i = 0;i < poolIndex.size; i++)
    {
        *s = m_pool->stringPool[i + poolIndex.startIndex];
        s ++;

        if((s - m_stringBuffer + 1) > m_stringBufferSize)
            return eSTRINGBUFFER_OVERFLOW_OOSQL;
    }
    *s = '\0';

    return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eBADASTNODE_OOSQL   
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::addToStringBufferFromSTRING_Node(ASTNodeIdx node)
{
    StringPoolIndex poolIndex;
    Four            e;

    if(AST(node).nodeName != STRING)
        OOSQL_ERR(eBADASTNODE_OOSQL);

    poolIndex.setPoolIndex(m_pool->stringIndexPool[AST(node).tokenVal],
                           m_pool->stringIndexPool[AST(node).tokenVal + 1] -
                           m_pool->stringIndexPool[AST(node).tokenVal]);

    e = addToStringBufferFromStringPool(poolIndex);
    OOSQL_CHECK_ERR(e);

    return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eBADASTNODE_OOSQL   
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::addToStringBufferFromID_Node(ASTNodeIdx node)
{
    StringPoolIndex poolIndex;
    Four            e;

    if(AST(node).nodeName != ID)
        OOSQL_ERR(eBADASTNODE_OOSQL);

    poolIndex.setPoolIndex(m_pool->stringIndexPool[AST(node).tokenVal],
                           m_pool->stringIndexPool[AST(node).tokenVal + 1] -
                           m_pool->stringIndexPool[AST(node).tokenVal]);
    e = addToStringBufferFromStringPool(poolIndex);
    OOSQL_CHECK_ERR(e);
    
    return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eSTRINGBUFFER_OVERFLOW_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_AST_Util::addToStringBuffer(char* string)
{
    int   i;
    char* s;

    s = m_stringBuffer;

    // move to end of string
    while(*s != '\0')
    {
        s ++;

        if((s - m_stringBuffer + 1) > m_stringBufferSize)
            return eSTRINGBUFFER_OVERFLOW_OOSQL;
    }

    for(i = 0; i < (int)strlen(string); i++)
    {
        *s = string[i];
        s ++;

        if((s - m_stringBuffer + 1) > m_stringBufferSize)
            return eSTRINGBUFFER_OVERFLOW_OOSQL;
    }
    *s = '\0';

    return eNOERROR;
}
