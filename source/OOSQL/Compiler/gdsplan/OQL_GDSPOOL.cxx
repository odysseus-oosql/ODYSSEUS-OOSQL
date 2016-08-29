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

#include "OQL_GDSPOOL.hxx"

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_GDSPOOL::OQL_GDSPOOL()
:	selectQueryPool(pMemoryManager), 
	updateQueryPool(pMemoryManager),
	insertQueryPool(pMemoryManager), 
	deleteQueryPool(pMemoryManager),
    selListPool(pMemoryManager), 
	targetListPool(pMemoryManager),
    exprPool(pMemoryManager), 
	groupByListPool(pMemoryManager),
    orderByListPool(pMemoryManager), 
	qgNodePool(pMemoryManager),
    pathExprPool(pMemoryManager), 
	argumentPool(pMemoryManager),
    structurePool(pMemoryManager), 
	valuePool(pMemoryManager),
    intPool(pMemoryManager),			
    realPool(pMemoryManager),			
    stringPool(pMemoryManager),
    stringIndexPool(pMemoryManager),
    funcPool(pMemoryManager),			
    aggrFuncPool(pMemoryManager),		
    collectionPool(pMemoryManager),		
    domainPool(pMemoryManager),			
    mbrPool(pMemoryManager),				
    objectPool(pMemoryManager),			
    memberPool(pMemoryManager),			
    subClassPool(pMemoryManager),		
    joinInfoPool(pMemoryManager),		
    pathExprInfoPool(pMemoryManager),	
    condInfoPool(pMemoryManager),		
    constructPool(pMemoryManager),		
	datePool(pMemoryManager),			
	timePool(pMemoryManager),			
	timestampPool(pMemoryManager),		
	intervalPool(pMemoryManager),		
	insertValuePool(pMemoryManager),		
	updateValuePool(pMemoryManager),		
	colNoMapPool(pMemoryManager),		
    usedColPool(pMemoryManager),		
    methodNoMapPool(pMemoryManager),
    usedMethodPool(pMemoryManager),		
    projectionPool(pMemoryManager),		
    ap_condListPool(pMemoryManager),		
    ap_exprPool(pMemoryManager),			
    ap_argumentPool(pMemoryManager),		
    ap_aggrFuncPool(pMemoryManager),		
    indexInfoPool(pMemoryManager),		
    projectionListPool(pMemoryManager),	
    textIndexCondPool(pMemoryManager),	
    tempFileInfoPool(pMemoryManager),	
	ap_insertValuePool(pMemoryManager),	
	ap_updateValuePool(pMemoryManager),	
    commonAP_Pool(pMemoryManager),		
    clientAP_Pool(pMemoryManager),		
	attributeInfoPool(pMemoryManager),	
	methodInfoPool(pMemoryManager),	
	keyInfoPool(pMemoryManager),			
	dbCommandPool(pMemoryManager),		
	superClassPool(pMemoryManager),
	argumentTypePool(pMemoryManager),
	ap_boolExprPool(pMemoryManager),
	limitClausePool(pMemoryManager)
{
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_GDSPOOL::~OQL_GDSPOOL()
{
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
static void linesplit(OQL_OutStream& os)
{
    os << "-------------------------------------------------------------------------------" << endl;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(OQL_OutStream& os, OQL_GDSPOOL& object)
{
#if !defined(__GNUC__)
	os << object.selectQueryPool;	linesplit(os);
	os << object.updateQueryPool;	linesplit(os);
	os << object.insertQueryPool;	linesplit(os);
	os << object.selectQueryPool;	linesplit(os);
    os << object.selListPool;       linesplit(os);
    os << object.targetListPool;    linesplit(os);
    os << object.exprPool;          linesplit(os);
    os << object.groupByListPool;   linesplit(os);
    os << object.orderByListPool;   linesplit(os);

    os << object.qgNodePool;        linesplit(os);
    os << object.pathExprPool;      linesplit(os);
    os << object.argumentPool;      linesplit(os);
    os << object.structurePool;     linesplit(os);
    os << object.valuePool;         linesplit(os);
    os << object.intPool;           linesplit(os);
    os << object.realPool;          linesplit(os);
    os << object.stringPool;        linesplit(os);
    os << object.stringIndexPool;   linesplit(os);
    os << object.funcPool;          linesplit(os);
    os << object.aggrFuncPool;      linesplit(os);
    os << object.collectionPool;    linesplit(os);
    os << object.domainPool;        linesplit(os);
    os << object.mbrPool;           linesplit(os);
    os << object.objectPool;        linesplit(os);
    os << object.memberPool;        linesplit(os);
    os << object.subClassPool;      linesplit(os);
    os << object.joinInfoPool;      linesplit(os);
    os << object.pathExprInfoPool;  linesplit(os);
    os << object.condInfoPool;      linesplit(os);
    os << object.constructPool;     linesplit(os);
	os << object.datePool;			linesplit(os);
	os << object.timePool;			linesplit(os);
	os << object.timestampPool;		linesplit(os);
	os << object.intervalPool;		linesplit(os);

	os << object.insertValuePool;	linesplit(os);
	os << object.updateValuePool;	linesplit(os);

	os << object.colNoMapPool;      linesplit(os);
    os << object.usedColPool;       linesplit(os);
    os << object.methodNoMapPool;   linesplit(os);
    os << object.usedMethodPool;    linesplit(os);
    os << object.ap_exprPool;       linesplit(os);
    os << object.ap_condListPool;   linesplit(os);
    os << object.ap_argumentPool;   linesplit(os);
    os << object.ap_aggrFuncPool;   linesplit(os);
    os << object.projectionPool;    linesplit(os);
    os << object.projectionListPool;linesplit(os);
    os << object.tempFileInfoPool;  linesplit(os);
    os << object.indexInfoPool;     linesplit(os);
    os << object.textIndexCondPool; linesplit(os);
	os << object.ap_insertValuePool;linesplit(os);
	os << object.ap_updateValuePool;linesplit(os);
    os << object.commonAP_Pool;     linesplit(os);
    os << object.clientAP_Pool;     linesplit(os);

	os << object.attributeInfoPool;	linesplit(os);
	os << object.methodInfoPool;	linesplit(os);
	os << object.keyInfoPool;		linesplit(os);
	os << object.dbCommandPool;		linesplit(os);
	os << object.superClassPool;	linesplit(os);
	os << object.argumentTypePool;	linesplit(os);
	os << object.ap_boolExprPool;   linesplit(os);
#endif

    return os;
}


