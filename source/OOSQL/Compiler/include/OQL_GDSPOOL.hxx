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

#ifndef __OQL_GDS_POOL_H__
#define __OQL_GDS_POOL_H__

#include "OQL_OutStream.hxx"
#include "OOSQL_Common.h"
#include "OQL_GDS_Struct.hxx"
#include "OQL_Plan_Struct.hxx"
#include "OOSQL_MemoryManager.hxx"
#include "OOSQL_MemoryManagedObject.hxx"

class OQL_GDS;
class OQL_ASTtoGDS;
class OQL_AST_Util;
class OQL_GDStoCommonAP;
class OOSQL_Compiler;
class OOSQL_Evaluator;

class OQL_GDSPOOL : public OOSQL_MemoryManagedObject {
public:
    OQL_GDSPOOL();
    virtual ~OQL_GDSPOOL();
	
    friend OQL_OutStream& operator<<(OQL_OutStream& os, OQL_GDSPOOL& object);
	friend class OQL_GDS;
    friend class OQL_ASTtoGDS;
    friend class OQL_AST_Util;
    friend class OQL_GDStoCommonAP;
    friend class OOSQL_Compiler;
	friend class OOSQL_Evaluator;

private:
	SelectQueryPool			selectQueryPool;
	UpdateQueryPool			updateQueryPool;
	InsertQueryPool			insertQueryPool;
	DeleteQueryPool			deleteQueryPool;

    SelListPool             selListPool;
    TargetListPool          targetListPool;
    ExprPool                exprPool;
    GroupByListPool         groupByListPool;
    OrderByListPool			orderByListPool; 

    QGNodePool              qgNodePool;
    PathExprPool            pathExprPool;
    ArgumentPool            argumentPool;
    StructurePool           structurePool;
    ValuePool               valuePool;
    IntegerPool             intPool;
    RealPool                realPool;
    StringPool              stringPool;
    StringIndexPool         stringIndexPool;
    FunctionPool            funcPool;
    AggrFuncPool            aggrFuncPool;
    CollectionPool          collectionPool;
    DomainPool              domainPool;
    MBRPool                 mbrPool;
    ObjectPool              objectPool;
    MemberPool              memberPool;
    SubClassPool            subClassPool;
    JoinInfoPool            joinInfoPool;
    PathExprInfoPool        pathExprInfoPool;
    CondListPool            condInfoPool;
    ConstructPool           constructPool;
	DatePool				datePool;
	TimePool				timePool;
	TimestampPool			timestampPool;
	IntervalPool			intervalPool;

	InsertValuePool			insertValuePool;
	UpdateValuePool			updateValuePool;

    AP_ColNoMapPool         colNoMapPool;
    AP_UsedColPool          usedColPool;
    AP_MethodNoMapPool      methodNoMapPool;
    AP_UsedMethodPool       usedMethodPool;
    AP_ProjectionPool       projectionPool;
    AP_CondListPool         ap_condListPool;
    AP_ExprPool             ap_exprPool;
    AP_ArgumentPool         ap_argumentPool;
    AP_AggrFuncPool         ap_aggrFuncPool;
    AP_ProjectionListPool   projectionListPool;
    AP_TempFileInfoPool     tempFileInfoPool;
    AP_IndexInfoPool        indexInfoPool;
    AP_TextIndexCondPool    textIndexCondPool;
	AP_InsertValuePool		ap_insertValuePool;
	AP_UpdateValuePool		ap_updateValuePool;

    CommonAP_Pool			commonAP_Pool;
    ClientAP_Pool			clientAP_Pool;

	AttributeInfoPool		attributeInfoPool;
	MethodInfoPool			methodInfoPool;
	KeyInfoPool				keyInfoPool;
	DBCommandPool			dbCommandPool;
	SuperClassPool			superClassPool;
	ArgumentTypePool		argumentTypePool;
	AP_BoolExprPool			ap_boolExprPool;

	LimitClausePool			limitClausePool;
};

#endif /* __OQL_GDS_POOL_H__ */

