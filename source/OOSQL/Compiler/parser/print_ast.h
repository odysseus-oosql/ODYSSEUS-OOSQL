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

#ifndef	PRINT_AST_H
#define	PRINT_AST_H

/*
    MODULE:
    	print_ast_h

    DESCRIPTION:
	header file for printing ast nodes after parsing in query processor

    LOG:
*/


/***************************************** 
 ** typedef ASTNodeIdx	int;			**
 ** typedef YYSTYPE	ASTNodeIdx			**
 **										**
 ** typedef struct ast_node {			**
 **	 short nodeType;					**
 **	 char *tokenVal;					**
 **	 struct ASTnode *brother;			**
 **	 struct ASTnode *son;				**
 ** } ASTNode;							**
 **										**
 ** ASTNode	*root, *tmp;				**
 **										**
 *****************************************/

#define TABS if (which) tabs(level)

 case (QuProg)	:
		TABS;
		printf("QuProg\n");
		break;

 case (null)	:
		TABS;
		printf("(null)\t");
		break;

 case (QuDef)	:
		TABS;
		printf("QuDef\n");
		break;

 case (QuSmp)	:
		TABS;
		printf("QuSmp\n");
		break;

 case (QuCmp)	:
		TABS;
		printf("QuCmp\n");
		break;
 case (QuBln)	:
		TABS;
		printf("QuBln\n");
		break;
 case (QuCnsObj)	:
		TABS;
		printf("QuCnsObj\n");
		break;

 case (QuCnsStruct)	:
		TABS;
		printf("QuCnsStruct\n");
		break;

 case (QuCnsSet)	:
		TABS;
		printf("QuCnsSet\n");
		break;

 case (QuCnsBag)	:
		TABS;
		printf("QuCnsBag\n");
		break;

 case (QuCnsLst)	:
		TABS;
		printf("QuCnsLst\n");
		break;

 case (QuCnsLstrn)	:
		TABS;
		printf("QuCnsLstrn\n");
		break;

 case (QuCnsArr)	:
		TABS;
		printf("QuCnsArr\n");
		break;

 case (QuAccPaex)	:
		TABS;
		printf("QuAccPaex\n");
		break;

 case (QuAccPaexAll):
        TABS;
        printf("QuAccPaexAll\n");
        break;

 case (QuAccDref)	:
		TABS;
		printf("QuAccDref\n");
		break;

 case (QuAccIcltnElem)	:
		TABS;
		printf("QuAccIcltnElem\n");
		break;

 case (QuAccIcltnSub)	:
		TABS;
		printf("QuAccIcltnSub\n");
		break;

 case (QuAccIcltnFr)	:
		TABS;
		printf("QuAccIcltnFr\n");
		break;

 case (QuAccIcltnLs)	:
		TABS;
		printf("QuAccIcltnLs\n");
		break;

 case (QuCltnAll)	:
		TABS;
		printf("QuCltnAll\n");
		break;

 case (QuCltnEx)	:
		TABS;
		printf("QuCltnEx\n");
		break;

 case (QuCltnExany)	:
		TABS;
		printf("QuCltnExany\n");
		break;

 case (QuCltnUni)	:
		TABS;
		printf("QuCltnUni\n");
		break;

 case (QuCltnIn)	:
		TABS;
		printf("QuCltnIn\n");
		break;

 case (QuCltnCmpSome)	:
		TABS;
		printf("QuCltnCmpSome\n");
		break;

 case (QuCltnCmpAny)	:
		TABS;
		printf("QuCltnCmpAny\n");
		break;

 case (QuCltnCmpAll)	:
		TABS;
		printf("QuCltnCmpAll\n");
		break;

 case (QuCltnAgg)	:
		TABS;
		printf("QuCltnAgg\n");
		break;

 case (QuCltnAggDist)	:
		TABS;
		printf("QuCltnAggDist\n");
		break;

 case (QuSel)	:
		TABS;
		printf("QuSel\n");
		break;

 case (QuDel)	:
		TABS;
		printf("QuDel\n");
		break;

 case (QuDeferredDel)	:
		TABS;
		printf("QuDeferredDel\n");
		break;

 case (QuIns)	:
		TABS;
		printf("QuIns\n");
		break;

 case (QuInsCol)	:
		TABS;
		printf("QuInsCol\n");
		break;

 case (QuInsValue)	:
		TABS;
		printf("QuInsValue\n");
		break;

 case (QuUpd)	:
		TABS;
		printf("QuUpd\n");
		break;

 case (QuUpdSetList)	:
		TABS;
		printf("QuUpdSetList\n");
		break;

 case (QuUpdSet)	:
		TABS;
		printf("QuUpdSet\n");
		break;

 case (QuSet)	:
		TABS;
		printf("QuSet\n");
		break;

 case QuCreateTbl:
		TABS;
		printf("QuCreateTbl\n");
		break;

 case QuCreateTempTbl:
		TABS;
		printf("QuCreateTempTbl\n");
		break;

 case QuCreateIdx:
		TABS;
		printf("QuCreateIdx\n");
		break;

 case QuDropTbl:
		TABS;
		printf("QuDropTbl\n");
		break;

 case QuDropIdx:
		TABS;
		printf("QuDropIdx\n");
		break;

 case (QuCnvL2s)	:
		TABS;
		printf("QuCnvL2s\n");
		break;

 case (QuCnvElem)	:
		TABS;
		printf("QuCnvElem\n");
		break;

 case (QuCnvDist)	:
		TABS;
		printf("QuCnvDist\n");
		break;

 case (QuCnvFlat)	:
		TABS;
		printf("QuCnvFlat\n");
		break;

 case (QuCnvType)	:
		TABS;
		printf("QuCnvType\n");
		break;
 case (QuCnvTypeStar)	:
		TABS;
		printf("QuCnvTypeStar\n");
		break;


 case (QuGeoCmp)	:
		TABS;
		printf("QuGeoCmp\n");
		break;

 case (QuGeoFnNum)	:
		TABS;
		printf("QuGeoFnNum\n");
		break;

 case (QuGeoFnSpa)	:
		TABS;
		printf("QuGeoFnSpa\n");
		break;


 case (ClSelAll)	:
		TABS;
		printf("ClSelAll\n");
		break;

 case (ClSelDist)	:
		TABS;
		printf("ClSelDist\n");
		break;

 case (ClFr)	:
		TABS;
		printf("ClFr\n");
		break;

 case (ClWh)	:
		TABS;
		printf("ClWh\n");
		break;

 case (ClGrp)	:
		TABS;
		printf("ClGrp\n");
		break;

 case (ClHav)	:
		TABS;
		printf("ClHav\n");
		break;

 case (ClOrd)	:
		TABS;
		printf("ClOrd\n");
		break;


 case (OpBiPlu)	:
		TABS;
		printf("OpBiPlu\t");
		break;

 case (OpBiMin)	:
		TABS;
		printf("OpBiMin\t");
		break;

 case (OpBiMul)	:
		TABS;
		printf("OpBiMul\t");
		break;

 case (OpBiDiv)	:
		TABS;
		printf("OpBiDiv\t");
		break;

 case (OpBiMod)	:
		TABS;
		printf("OpBiMod\t");
		break;

 case (OpBiStrcat)	:
		TABS;
		printf("OpBiStrcat\t");
		break;

 case (OpUnMin)	:
		TABS;
		printf("OpUnMin\t");
		break;

 case (OpUnAbs)	:
		TABS;
		printf("OpUnAbs\t");
		break;

 case (OpCmpLike)	:
		TABS;
		printf("OpCmpLike\t");
		break;

 case (OpCmpEq)	:
		TABS;
		printf("OpCmpEq\t");
		break;

 case (OpCmpNe)	:
		TABS;
		printf("OpCmpNe\t");
		break;

 case (OpCmpGt)	:
		TABS;
		printf("OpCmpGt\t");
		break;

 case (OpCmpLt)	:
		TABS;
		printf("OpCmpLt\t");
		break;

 case (OpCmpGe)	:
		TABS;
		printf("OpCmpGe\t");
		break;

 case (OpCmpLe)	:
		TABS;
		printf("OpCmpLe\t");
		break;

 case (OpCmpIsNull) :
	 	TABS;
		printf("OpCmpIsNull\t");
		break;
		
 case (OpCmpIsNotNull)  :
	    TABS;
		printf("OpCmpIsNotNull\t");
		break;

 case (OpBlnNot)	:
		TABS;
		printf("OpBlnNot\t");
		break;

 case (OpBlnAnd)	:
		TABS;
		printf("OpBlnAnd\t");
		break;

 case (OpBlnOr)	:
		TABS;
		printf("OpBlnOr\t");
		break;

 case (OpSetInt)	:
		TABS;
		printf("OpSetInt\t");
		break;

 case (OpSetUni)	:
		TABS;
		printf("OpSetUni\t");
		break;

 case (OpSetExc)	:
		TABS;
		printf("OpSetExc\t");
		break;


 case (ProAll)	:
		TABS;
		printf("ProAll\n");
		break;

 case (ProSmp)	:
		TABS;
		printf("ProSmp\n");
		break;

 case (ProAs)	:
		TABS;
		printf("ProAs\n");
		break;

 case (ProAllLogicalID)	:
		TABS;
		printf("ProAllLogicalID\n");
		break;

 case (FrCltn)	:
 		TABS;
 		printf("FrCltn\n");
 		break;
 case (FrCltnStar)	:
 		TABS;
 		printf("FrCltnStar\n");
 		break;

 case (FnAggCntall)	:
		TABS;
		printf("FnAggCntall\n");
		break;

 case (FnAggCnt)	:
		TABS;
		printf("FnAggCnt\n");
		break;

 case (FnAggSum)	:
		TABS;
		printf("FnAggSum\n");
		break;

 case (FnAggMin)	:
		TABS;
		printf("FnAggMin\n");
		break;

 case (FnAggMax)	:
		TABS;
		printf("FnAggMax\n");
		break;

 case (FnAggAvg)	:
		TABS;
		printf("FnAggAvg\n");
		break;


 case (VaNil)	:
		TABS;
		printf("VaNil");
		break;

 case (VaTr)	:
		TABS;
		printf("VaTr");
		break;

 case (VaFls)	:
		TABS;
		printf("VaFls");
		break;

 case (VaLtDate)	:
		TABS;
		printf("VaLtDate\n");
		break;

 case (VaLtTime)	:
		TABS;
		printf("VaLtTime\n");
		break;

 case (VaLtTimestamp)	:
		TABS;
		printf("VaLtTimestamp\n");
		break;

  case (VaLtComplex)	:
		TABS;
		printf("VaLtComplex\n");
		break;

 case (VaLtInterval)	:
		TABS;
		printf("VaLtInterval\n");
		break;

 case (IntervalYear)	:
		TABS;
		printf("IntervalYear\n");
		break;

 case (VaLt)	:
		TABS;
		printf("VaLt");
		break;


 case (OrdAsc)	:
		TABS;
		printf("OrdAsc\t");
		break;

 case (OrdDesc)	:
		TABS;
		printf("OrdDesc\t");
		break;


 case (Method)	:
		TABS;
		printf("Method\n");
		break;

 case (FnOrCltnobj)	:
		TABS;
		printf("FnOrCltnobj\n");
		break;


 case (OpGeoNorth)	:
		TABS;
		printf("OpGeoNorth\t");
		break;

 case (OpGeoSouth)	:
		TABS;
		printf("OpGeoSouth\t");
		break;

 case (OpGeoEast)	:
		TABS;
		printf("OpGeoEast\t");
		break;

 case (OpGeoWest)	:
		TABS;
		printf("OpGeoWest\t");
		break;

 case (OpGeoCover)	:
		TABS;
		printf("OpGeoCover\t");
		break;

 case (OpGeoCovered)	:
		TABS;
		printf("OpGeoCovered\t");
		break;

 case (OpGeoContain)	:
		TABS;
		printf("OpGeoContain\t");
		break;

 case (OpGeoContained)	:
		TABS;
		printf("OpGeoContained\t");
		break;

 case (OpGeoDisjoint)	:
		TABS;
		printf("OpGeoDisjoint\t");
		break;

 case (OpGeoEqual)	:
		TABS;
		printf("OpGeoEqual\t");
		break;

 case (OpGeoMeet)	:
		TABS;
		printf("OpGeoMeet\t");
		break;

 case (OpGeoOverlap)	:
		TABS;
		printf("OpGeoOverlap\t");
		break;


 case (FnGeoDistance)	:
		TABS;
		printf("FnGeoDistance\n");
		break;

 case (FnGeoArea)	:
		TABS;
		printf("FnGeoArea\n");
		break;

 case (FnGeoLength)	:
		TABS;
		printf("FnGeoLength\n");
		break;

 case (FnGeoNearest)	:
		TABS;
		printf("FnGeoNearest\n");
		break;

 case (FnGeoFurthest)	:
		TABS;
		printf("FnGeoFurthest\n");
		break;

 case (FnGeoBoundary)	:
		TABS;
		printf("FnGeoBoundary\n");
		break;

 case (FnGeoInterior)	:
		TABS;
		printf("FnGeoInterior\n");
		break;

 case (FnGeoBuffer)	:
		TABS;
		printf("FnGeoBuffer\n");
		break;

 case (FnGeoUnion)	:
		TABS;
		printf("FnGeoUnion\n");
		break;

 case (FnGeoIntersect)	:
		TABS;
		printf("FnGeoIntersect\n");
		break;

 case (FnGeoDifference)	:
		TABS;
		printf("FnGeoDifference\n");
		break;


 case (GeoMbr)	:
		TABS;
		printf("GeoMbr\n");
		break;

 case (QuIrFnWeight)	:
		TABS;
		printf("QuIrFnWeight\n");
		break;

 case (QuIrFnNmatch)	:
		TABS;
		printf("QuIrFnNmatch\n");
		break;

 case (QuIrFnMatch)	:
		TABS;
		printf("QuIrFnMatch\n");
		break;

 case (OpIrBlnAccum)	:
		TABS;
		printf("OpIrBlnAccum\n");
		break;

 case (OpIrBlnOr)	:
		TABS;
		printf("OpIrBlnOr\n");
		break;

 case (OpIrBlnAnd)	:
		TABS;
		printf("OpIrBlnAnd\n");
		break;

 case (OpIrBlnMinus)	:
		TABS;
		printf("OpIrBlnMinus\n");
		break;

 case (OpIrBlnThreshold) :
		TABS;
		printf("OpIrBlnThreshold\n");
		break;

 case (OpIrBlnMultiply)	:
		TABS;
		printf("OpIrBlnMultiply\n");
		break;

 case (OpIrBlnMax)	:
		TABS;
		printf("OpIrBlnMax\n");
		break;

 case (OpIrBlnNear)	:
		TABS;
		printf("OpIrBlnNear\n");
		break;

 case (OpIrBlnNearWithOrder)	:
		TABS;
		printf("OpIrBlnNearWithOrder\n");
		break;

 case (OpIrBlnUnIn)	:
		TABS;
		printf("OpIrBlnUnIn\n");
		break;

 case OpIrRange:
		TABS;
		printf("OpIrRange\n");
		break;

 case OpIrBetween:
		TABS;
		printf("OpIrBetween\n");
		break;

 case OpIrScanForward:
		TABS;
		printf("OpIrScanForward\n");
		break;

 case OpIrScanBackward:
		TABS;
		printf("OpIrScanBackward\n");
		break;

 case QuUserFunction:
		TABS;
		printf("QuUserFunction\n");
		break;

 case QuAlterTbl:
		TABS;
		printf("QuAlterTbl\n");
		break;

 case QuCreateSeq:
		TABS;
		printf("QuCreateSeq\n");
		break;

 case QuDropSeq:
		TABS;
		printf("QuDropSeq\n");
		break;

 case QuCreateFunc:
		TABS;
		printf("QuCreateFunc\n");
		break;

 case QuCreateMethod:
		TABS;
		printf("QuCreateMethod\n");
		break;

 case QuCreateProc:
		TABS;
		printf("QuCreateProc\n");
		break;

 case QuDropFunc:
		TABS;
		printf("QuDropFunc\n");
		break;

 case QuDropSpecFunc:
		TABS;
		printf("QuDropSpecFunc\n");
		break;

 case QuDropProc:
		TABS;
		printf("QuDropProc\n");
		break;

 case QuDropSpecProc:
		TABS;
		printf("QuDropSpecProc\n");
		break;

 case QuCallProc:
		TABS;
		printf("QuCallProc\n");
		break;

 case (INTEGER) :
		TABS;	 
		printf("%d", IntPool[AST(t).tokenVal]);
		break;
 
 case (REAL): TABS;	 
		printf("%f", RealPool[AST(t).tokenVal]);
		break;
 
 case (STRING) :
		TABS; 
		printf("\"");
		printname(AST(t).tokenVal);
		printf("\"");
		break;
 
 case (ID ) :
		TABS; 
		printname(AST(t).tokenVal);
		break;
 
 case (YEAR) :	
		TABS;	 
		printf("YEAR");
		break;
 
 case TblAttrList:
		TABS;	 
		printf("TblAttrList\n");
		break;

 case TblAttr:
		TABS;	 
		printf("TblAttr\n");
		break;

 case TblKeyDef:
		TABS;	 
		printf("TblKeyDef\n");
		break;

 case TblAttrName:
		TABS;	 
		printf("TblAttrName\n");
		break;

 case TblAttrDomain:
		TABS;	 
		printf("TblAttrDomain\n");
		break;

 case IdxTypeNormal:
		TABS;	 
		printf("IdxTypeNormal");
		break;

 case IdxTypeUnique:
		TABS;	 
		printf("IdxTypeUnique");
		break;

 case IdxTypeCluster:
		TABS;	 
		printf("IdxTypeCluster");
		break;

 case IdxTypeUniqueCluster:
		TABS;	 
		printf("IdxTypeUniqueCluster");
		break;

 case IdxKeyList:
		TABS;	 
		printf("IdxKeyList\n");
		break;
		
 case SuperClasses:
		TABS;	 
		printf("SuperClasses\n");
		break;

 case VaParam:
		TABS;	 
		printf("VaParam");
		break;

 case VaText:
		TABS;	 
		printf("VaText\n");
		break;

 case TextUpdModeDeferred:
		TABS;	 
		printf("TextUpdModeDeferred");
		break;

 case TextUpdModeImmediate:
		TABS;	 
		printf("TextUpdModeImmediate");
		break;

 case FrObject:
		TABS;	 
		printf("FrObject\n");
		break;
	
 case FuncArgList:
		TABS;	 
		printf("FuncArgList\n");
		break;

 case FuncReturns:
		TABS;	 
		printf("FuncReturns\n");
		break;

 case FuncArg:
		TABS;	 
		printf("FuncArg\n");
		break;

 case FucnArgAsLocator:
		TABS;	 
		printf("FucnArgAsLocator\n");
		break;

 case FuncSpecific:
		TABS;	 
		printf("FuncSpecific\n");
		break;

 case FuncExternalName:
		TABS;	 
		printf("FuncExternalName\n");
		break;

 case FuncDeterministic:
		TABS;	 
		printf("FuncDeterministic\n");
		break;

 case FuncExternalAction:
		TABS;	 
		printf("FuncExternalAction\n");
		break;

 case FuncNullCall:
		TABS;	 
		printf("FuncNullCall\n");
		break;

 case FuncLanguage:
		TABS;	 
		printf("FuncLanguage\n");
		break;

 case FuncParameterStyle:
		TABS;	 
		printf("FuncParameterStyle\n");
		break;

 case FuncScratchpad:
		TABS;	 
		printf("FuncScratchpad\n");
		break;

 case FuncFinalCall:
		TABS;	 
		printf("FuncFinalCall\n");
		break;

 case FuncParallel:
		TABS;	 
		printf("FuncParallel\n");
		break;

 case FuncDbinfo:
		TABS;	 
		printf("FuncDbinfo\n");
		break;

 case ProcArgList:
		TABS;	 
		printf("ProcArgList\n");
		break;

 case ProcArg:
		TABS;	 
		printf("ProcArg\n");
		break;

 case ProcArgModeIn:
 		TABS;	 
		printf("ProcArgModeIn\n");
		break;

 case ProcArgModeOut:
		TABS;	 
		printf("ProcArgModeOut\n");
		break;

 case ProcArgModeInOut:
		TABS;	 
		printf("ProcArgModeInOut\n");
		break;

 case FuncFenced:
		TABS;	 
		printf("FuncFenced\n");
		break;

 case ProcResultSet:
		TABS;	 
		printf("ProcResultSet\n");
		break;

 case AltActList:
		TABS;	 
		printf("AltActList\n");
		break;

 case AltActAddCol:
		TABS;	 
		printf("AltActAddCol\n");
		break;

 case AltActDropCol:
		TABS;	 
		printf("AltActDropCol\n");
		break;
 
 case AltActDropColList:
		TABS;	 
		printf("AltActDropColList\n");
		break;


 case SeqOptList:
		TABS;	 
		printf("SeqOpttList\n");
		break;

 case SeqStartWith:
		TABS;	 
		printf("SeqStartWith\n");
		break;

 case VaSeqCurr:
		TABS;	 
		printf("VaSeqCurr\n");
		break;

 case VaSeqNext:
		TABS;	 
		printf("VaSeqNext\n");
		break;
#endif
