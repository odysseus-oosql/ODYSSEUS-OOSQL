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

#ifndef __OQL_UTILITY_H__
#define __OQL_UTILITY_H__

#include <string.h>

extern "C" {
#include "OQL.yacc.h"
#include "OQL_AST.h"
#undef AST
}
#include "OQL_AST.hxx"
#include "OQL_GDSPOOL.hxx"
#include "OQL_SimpleString.hxx"
#include "OOSQL_Common.h"
#include "OOSQL_MemoryManager.hxx"
#include "OOSQL_MemoryManagedObject.hxx"

class OQL_AST_Util : public OOSQL_MemoryManagedObject {
public:
    OQL_AST_Util(OQL_AST& ast, OQL_GDSPOOL& pool);
    virtual ~OQL_AST_Util();

    Four select_GetN_Terms(ASTNodeIdx root);
    Four select_GetIthTerm(ASTNodeIdx root, Four ith, Four stringBufferSize, char* stringBuffer);

private:
    /* internal functions */
    Four addToStringBuffer(char* string);
    Four addToStringBufferFromStringPool(StringPoolIndex poolIndex);
    Four addToStringBufferFromSTRING_Node(ASTNodeIdx node);
    Four addToStringBufferFromID_Node(ASTNodeIdx node);

    Four constructTermString_Query(ASTNodeIdx node);
    Four constructTermString_QuOgisOp(ASTNodeIdx node);
    Four constructTermString_QuDef(ASTNodeIdx node);
    Four constructTermString_QuSmp(ASTNodeIdx node);
    Four constructTermString_QuCmp(ASTNodeIdx node);
    Four constructTermString_QuBln(ASTNodeIdx node);
    Four constructTermString_QuCns(ASTNodeIdx node);
    Four constructTermString_QuAccPaex(ASTNodeIdx node);
    Four constructTermString_QuAccPaexRecursive(ASTNodeIdx node);
    Four constructTermString_IcltnElem(ASTNodeIdx node);
    Four constructTermString_QuAccIcltnSub(ASTNodeIdx node);
    Four constructTermString_QuAccIcltnFr(ASTNodeIdx node);
    Four constructTermString_QuAccIcltnLs(ASTNodeIdx node);
    Four constructTermString_QuCltnAll(ASTNodeIdx node);
    Four constructTermString_QuCltnEx(ASTNodeIdx node);
    Four constructTermString_QuCltnExany(ASTNodeIdx node);
    Four constructTermString_QuCltnUni(ASTNodeIdx node);
    Four constructTermString_QuCltnIn(ASTNodeIdx node);
    Four constructTermString_QuCltnCmpSome(ASTNodeIdx node);
    Four constructTermString_QuCltnCmpAny(ASTNodeIdx node);
    Four constructTermString_QuCltnCmpAll(ASTNodeIdx node);
    Four constructTermString_QuCltnAgg(ASTNodeIdx node);
    Four constructTermString_QuCltnAggDist(ASTNodeIdx node);
    Four constructTermString_QuSet(ASTNodeIdx node);
    Four constructTermString_QuCnvL2s(ASTNodeIdx node);
    Four constructTermString_QuCnvElem(ASTNodeIdx node);
    Four constructTermString_QuCnvDist(ASTNodeIdx node);
    Four constructTermString_QuCnvFlat(ASTNodeIdx node);
    Four constructTermString_QuCnvType(ASTNodeIdx node);
    Four constructTermString_ID(ASTNodeIdx node);
    Four constructTermString_QuGeoCmp(ASTNodeIdx node);
    Four constructTermString_QuGeoFnNum(ASTNodeIdx node);
    Four constructTermString_QuGeoFnSpa(ASTNodeIdx node);
    Four constructTermString_GeoMbr(ASTNodeIdx node);
    Four constructTermString_QuIrFnMatch(ASTNodeIdx node);
    Four constructTermString_QuIrFnNmatch(ASTNodeIdx node);
    Four constructTermString_QuIrFnWeight(ASTNodeIdx node);
    Four constructTermString_QuAccDref(ASTNodeIdx node);
    Four constructTermString_QuAccIcltnElem(ASTNodeIdx node);
    Four constructTermString_FnOrCltnobj(ASTNodeIdx node);
    Four constructTermString_Arguments(ASTNodeIdx node);
    Four constructTermString_Value(ASTNodeIdx node);
    Four constructTermString_TextIrExpression(ASTNodeIdx node);
    Four constructTermString_TextIrOperator(ASTNodeIdx node);

    ASTNode AST(ASTNodeIdx index) { return m_ast->getASTNode(index); }
private:
    // private data member
    OQL_AST         *m_ast;
    OQL_GDSPOOL     *m_pool;

    Four  m_stringBufferSize;
    char* m_stringBuffer;
};

#endif

