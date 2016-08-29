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

#ifndef _OOSQL_EVALUATOR_HXX_
#define _OOSQL_EVALUATOR_HXX_

/*
    MODULE:
        OOSQL_Evaluator.hxx

    DESCRIPTION:
        This header file defines OOSQL_Evaluator class
        which evaluates the access plan given as input.
*/


/*
 * include files
 */
#include "OOSQL_Common.h"					// common header file
#include "OOSQL_Eval_DS.hxx"				// data structures for query evaluation module
#include "OOSQL_AccessPlan.hxx"				// access plan for OOSQL processor
#include "OOSQL_Eval_Util.hxx"
#include "OOSQL_Error.h"					// error code
#include "OOSQL_MemoryManager.hxx"
#include "OOSQL_UserMemoryManager.hxx"
#include "OOSQL_MemoryManagedObject.hxx"
#include "OQL_GDSPOOL.hxx"
#include "OOSQL_StorageManager.hxx"
#include "OOSQL_Catalog.hxx"
#include "OOSQL_Compiler.hxx"
#include "OOSQL_ErrorMessage.hxx"

#define EVALSTATUS_GLOBAL_INIT          0
#define EVALSTATUS_GLOBAL_READY         1
#define EVALSTATUS_GLOBAL_EVALUATING    2
#define EVALSTATUS_GLOBAL_END           3

#define EVALSTATUS_INIT         0
#define EVALSTATUS_PROCESSING   1
#define EVALSTATUS_END          2


#ifdef  EOS
#define ENDOFSCAN       EOS
#else
#define ENDOFSCAN       1
#endif

#define HAVING_FALSE_OOSQL              0               // having condition is false
#define HAVING_TRUE_OOSQL               1               // having condition is true
#define GROUPING_SAMEGROUP_OOSQL        2               // same group in group by result
#define GROUPING_NEWGROUP_OOSQL         3               // different group in group by result

/* 
 * macros for initialize system resources (e.g., scan, temporary file)
 */
#define RESET_OPENSCANNUM(osn)          (osn = -1)
#define RESET_OPENCLASSNUM(ocn)         (ocn = -1)
#define RESET_INDEXNAME(iname)          (iname[0] = NULL)
#define RESET_CLASSNAME(cname)          (cname[0] = NULL)
#define RESET_TEMPFILENUM(tfn)          (tfn = -1)

/* class ID of all temporary classes */
#define TEMPCLASSID             0

/*
 *      macro definitions for accessing evaluation data structures
 */
#define ACCESSPLAN						 (*m_accessPlan)
#define ACCESSPLANELEMENTS				 (m_accessPlanElements)
#define IS_NULL_ACCESSPLAN				 ((m_accessPlan == NULL)? SM_TRUE: SM_FALSE)

#define EVAL_ACCESSLISTTABLE             m_evalAccessListTable
#define EVAL_INDEX_SCANINFOTABLE		 (*m_evalIndexScanInfoTable)
#define EVAL_INDEX_SCANINFOTABLEELEMENTS (m_evalIndexScanInfoTableElements)
#define EVAL_TEMPFILEINFOTABLE           (*m_evalTempFileInfoTable)
#define EVAL_TEMPFILEINFOTABLEELEMENTS	 (m_evalTempFileInfoTableElements)

#define EVAL_EVALBUFFER                  m_evaluationBuffer
#define EVAL_TEXTIR_EVALBUF              (*m_evalTextIR_SubPlanEvalBuffer)
#define EVAL_TEXTIR_EVALBUFELEMENTS		 (m_evalTextIR_SubPlanEvalBufferElements)
#define EVAL_INDEX_EVALBUF				 m_indexEvaluationBuffer

/*
 * macros for accessing evaluation buffer
 */
#define GET_EVALBUF_CLIST(planNo,colNo) \
        (EVAL_EVALBUFFER[planNo].clist[m_accessPlan->getMappedColNo(planNo,colNo)])

/*
 * macro for comparing docId
 */
#define TEXTIR_DOCID_EQ(docId1,docId2,mode)		(((docId1)^(mode)) == ((docId2)^(mode)))
#define TEXTIR_DOCID_NE(docId1,docId2,mode)		(((docId1)^(mode)) != ((docId2)^(mode)))
#define TEXTIR_DOCID_GT(docId1,docId2,mode)		(((docId1)^(mode)) >  ((docId2)^(mode)))
#define TEXTIR_DOCID_LT(docId1,docId2,mode)		(((docId1)^(mode)) <  ((docId2)^(mode)))
#define TEXTIR_DOCID_GE(docId1,docId2,mode)		(((docId1)^(mode)) >= ((docId2)^(mode)))
#define TEXTIR_DOCID_LE(docId1,docId2,mode)		(((docId1)^(mode)) <= ((docId2)^(mode)))
#define TEXTIR_DOCID_DIFF(docId1,docId2,mode)	(((docId1)^(mode)) -  ((docId2)^(mode)))
#define TEXTIR_DOCID_MIN(docId1,docId2,mode)	(((docId1)^(mode)<(docId2)^(mode)) ? (docId1):(docId2))
#define TEXTIR_DOCID_MAX(docId1,docId2,mode)	(((docId1)^(mode)>(docId2)^(mode)) ? (docId1):(docId2))

typedef struct {
	char*						srcPtr;
	Four*						srcLength;
	AP_ProjectionElement*		pProjection;
	Four						projectionKind;
	Four						type;
	Four						projColNo;
	Four						colNo;
	OOSQL_StorageManager::OID*	oid;
	Four						ocn;
	Boolean*					nullFlag;
} OOSQL_GetDataInfo;

class  OOSQL_ExternalFunctionManager;
class  OOSQL_ExternalFunctionDispatcher;
class  OOSQL_MultipleResultBuffer;

struct OOSQL_FastEncodingInfo {
	Four		type;
	Four*		size;
	char*		ptr;
	Boolean*	nullFlag;
};

class OOSQL_Evaluator : public OOSQL_MemoryManagedObject {
		friend class OOSQL_MultipleResultBuffer;

		/*----------------------------------------------------------------------*
		 *      private member variables                                        *
		 *----------------------------------------------------------------------*/

        /*
         * transaction information
         */
		OOSQL_StorageManager*				m_storageManager;
		OOSQL_Catalog*						m_catalog;
		OOSQL_ErrorMessage*					m_errorMessage;
		OOSQL_ExternalFunctionManager*		m_externalFunctionManager;
		OOSQL_ExternalFunctionDispatcher*	m_externalFunctionDispatcher;
        OOSQL_StorageManager::XactID		m_transID;
        Four								m_volID;

        /**********************************
         * evaluation control information *
         **********************************/

        EvalStatus							m_evalStatus;			// evaluation status
        Four								m_currPlanIndex;		// index to the current access plan element
		AP_CondListPoolIndex				m_currWhereCondNodes;	

        /**********************
         * Access Information *
         **********************/
        OOSQL_AccessList				*m_evalAccessListTable;
        OOSQL_ScanInfoTable				*m_evalIndexScanInfoTable;
		OOSQL_ScanInfo					*m_evalIndexScanInfoTableElements;
        OOSQL_TempFileInfoTable			*m_evalTempFileInfoTable;
		OOSQL_TempFileInfo				**m_evalTempFileInfoTableElements;


        /* 
         * access plan
         */
        OOSQL_AccessPlan				*m_accessPlan;
		OQL_GDSPOOL						*m_pool;						
		OOSQL_QueryType					m_queryType;
		CommonAP_Element				*m_accessPlanElements;			

        /**********************************
         * final query result information *
         **********************************/
        OOSQL_ResultInfo				*m_queryResultInfo;

        /* manage query result buffer */
        Four							m_numQueryResultTuples;
		OOSQL_MultipleResultBuffer*		m_multipleResultBuffer;


        /*********************
         * Evaluation Buffer *
         *********************/

        OOSQL_EvalBuffer				*m_evaluationBuffer;			// evaluation buffer for each access plane element
		OOSQL_EvalIndexBuffer			*m_indexEvaluationBuffer;		// evaluation buffer for index operation

        /* extended to support Text IR.
         *      This buffer is used to evaluate Text IR subplan.
         */
        OOSQL_TextIR_SubPlanEvalBuffer		*m_evalTextIR_SubPlanEvalBuffer;
		oosql_TextIR_SubPlanEvalBufferElem	*m_evalTextIR_SubPlanEvalBufferElements;

		OOSQL_SortBufferInfo			m_sortBufferInfo;
		OOSQL_UserMemoryManager			m_sortBufferMemory;				
		Four							m_memorySizeNeededForStarQuery;
		char*							m_sortBufferForStarQuery;		
		Four							m_usedSortBufferForStarQuery;
        Boolean							m_isCheckingHavingCond;			
		Boolean							m_callEvalAndFetchForSelectOnlyFlag;	
																

		Four							m_getoid_nTargetClass;			
		Four*							m_getoid_TargetNoToEvalBufferNo;
	
		Four							m_getdata_nInfo;				
		OOSQL_GetDataInfo*				m_getdata_info;

		Four							m_compareMode;

		/*----------------------------------------------------------------------*
		*      private member functions                                        *
		*----------------------------------------------------------------------*/

		/********************************************************************
		*  functions for executing access methods                          *
		*  NOTE: access method = OOSQL_StorageManager::OID Fetch, Sequential Scan, or Index Scan *
		********************************************************************/

        /*
         * functions to initialize access methods
         */
        Four    initAccessListTable();          
        Four    finalAccessListTable();         

        /* 
         * read the next candidate object to evaluate into evaluation buffer 
         */
        Four    getNextObject();

        /*
         * functions to execute scan
         */
        Four    oidFetch(Four planIndex, Four accessElemIndex);             
        Four    seqScanNext(Four planIndex, Four accessElemIndex);          
        Four    btreeIndexScanNext(Four planIndex, Four accessElemIndex);   
        Four    textIndexScanNext(Four planIndex, Four accessElemIndex);    
        Four    execBtreeIndexInfoNode(AP_IndexInfoPoolIndex, OOSQL_StorageManager::OID*, OOSQL_StorageManager::Cursor*& cursor);
        Four    execTextIndexInfoNode(AP_IndexInfoPoolIndex, OOSQL_TextIR_PostingQueue& postingQueue, Four logicalIdHints);
		Four 	restoreWeightInfo(AP_IndexInfoPoolIndex indexInfo);
		Four	adjustWeightInfo(AP_IndexInfoPoolIndex indexInfo, OOSQL_TextIR_Posting* posting);	

        /* reserved for future extension */
        Four    openOID_SetScan(Four planIndex, Four accessElemIndex);  
        Four    closeOID_SetScan(Four planIndex, Four accessElemIndex); 
        Four    oidSetScanNext(Four planIndex, Four accessElemIndex);       

        /* 
         * functions to execute sequential index scan 
         */
        Four    openTempSeqScan(Four planIndex, Four accessListIndex);
        Four    openSeqScan(Four planIndex, Four accessElemIndex);
        Four    closeSeqScan(Four planIndex, Four accessListIndex);

        /* 
         * functions to execute B-tree index scan 
         */
        Four    openBtreeIndexScan(Four planIndex, Four accessElemIndex);
        Four    closeBtreeIndexScan(Four planIndex, Four accessElemIndex);
        Four    makeBtreeIndexBoundCond(AP_BtreeIndexCond*, OOSQL_StorageManager::BoundCond*, OOSQL_StorageManager::BoundCond*);
        Four    execBtreeIndexScan(AP_IndexInfoPoolIndex, OOSQL_StorageManager::OID*, OOSQL_StorageManager::Cursor*& cursor);

        // construct boolean expressions for the scan of an access plan element
        Four    makeBoolExpression(AP_BoolExprPoolIndex, OOSQL_StorageManager::BoolExp*);
		Four	checkBoolExpression(Four nCols, OOSQL_StorageManager::ColListStruct* clist, Four nBoolExprs, OOSQL_StorageManager::BoolExp* boolExprs, AP_BoolExprElement* boolExprInfos, AP_ColNoMapElement* colNoMap);

        // search the appropriate scan ID for a class
        Four    searchScanIdByClassId(Four, Four);
        Four    searchOcnByClassId(Four, Four);

        /*
         * functions to execute Text index scan 
         */
        Four    openTextIndexScan(Four planIndex, Four accessElemIndex);
        Four    initTextIndexCond(Four ocn, AP_IndexInfoPoolIndex, Four planIndex);
        Four    closeTextIndexScan(Four planIndex, Four accessElemIndex);
        Four    finalTextIndexCond(AP_IndexInfoPoolIndex);

        /*
         * functions to execute MLGF index scan 
         * (reserved for future extension)
         */
        Four    openMLGF_IndexScan(Four planIndex, Four accessElemIndex);
		Four    openMLGF_MBR_IndexScan(Four planIndex, Four accessElemIndex);
		Four    makeMlgfIndexBoundCond(AP_MlgfIndexCond& mlgf);
        Four    closeMLGF_IndexScan(Four planIndex, Four accessElemIndex);
        Four    mlgfIndexScanNext(Four planIndex, Four accessElemIndex);

		/************************************
		 *  functions for evalution control *
		 ************************************/
        Four    mainEvalLoop();
        Four    moveToAndOpenNextPlanElement();
        Four    backtrack();
        Four    moveToLastPlanElemOfNestedLoop();
        Boolean isOutermostClassOfNestedLoop();
        Boolean isLastPlanElement();

        // determine the next access plan element to evaluate
        Four    retraceAccessPlan(Four);

        Four    saveQueryResult(AP_ProjectionListElement*, Boolean);
        Four    saveEvaluatorStatus(Four);
        Four    restoreSavedResultToCharBuf(Four, char*, Four*);

		/****************************************************
		*  functions for processing                        *
		****************************************************/

        Four    process();						
		Four	processForSelectOnly();				
        Four    grouping();
        Four    aggregation();
        Four    projection(One isGroupByResult);
		Four    projectionForUpdate(One isGroupByResult);
		Four    projectionForInsert(One isGroupByResult);
		Four	processDBCommand();					// process DDL

		/********************************************************************
		*  functions for executing various join methods                    *
		********************************************************************/

        Four    initAccessForNestedLoop(Four planIndex);
        Four    execImplicitForwardNestedLoop(Four planIndex);        
        Four    execImplicitBackwardSortDomain(Four planIndex);       
        Four    execNestedLoop(Four planIndex);                       


		/********************************************
		*  functions for expression evaluation     *
		********************************************/

        // check WHERE condition for the current access plan element
        Four            checkWhereCond();

        // evaluate an expression
        Four            evalExpression(AP_ExprPoolIndex*, OOSQL_DB_Value*);
		Four			evalExpression(AP_PathExprAccessInfo*, OOSQL_DB_Value*);
		Four			evalExpression(AP_ValuePoolIndex*, OOSQL_DB_Value*, Four);
		Four			evalExpression(AP_FuncEvalInfo*, OOSQL_DB_Value*);
        Four            evalOperator(AP_OperatorStruct*, OOSQL_DB_Value*);
        Four            evalAggrFuncResult(AP_ExprPoolIndex*, OOSQL_DB_Value*);
        Four            evalFunction(AP_FuncEvalInfo*, OOSQL_DB_Value*);
        Four            evalFunctionResult(AP_FuncResultAccessInfo*, OOSQL_DB_Value*);

        // string manipulation functions
        Four            stringCat(AP_ExprPoolIndex*, AP_ExprPoolIndex*, OOSQL_DB_Value*);
        Four            evalOpLike(AP_ExprPoolIndex*, AP_ExprPoolIndex*, OOSQL_DB_Value*);

        // arithmetic expression evaluation
        Four            arithAdd(AP_ExprPoolIndex*, AP_ExprPoolIndex*, OOSQL_DB_Value*);
        Four            arithSub(AP_ExprPoolIndex*, AP_ExprPoolIndex*, OOSQL_DB_Value*);
        Four            arithMul(AP_ExprPoolIndex*, AP_ExprPoolIndex*, OOSQL_DB_Value*);
        Four            arithDiv(AP_ExprPoolIndex*, AP_ExprPoolIndex*, OOSQL_DB_Value*);
        Four            arithMod(AP_ExprPoolIndex*, AP_ExprPoolIndex*, OOSQL_DB_Value*);
        Four            arithUnMin(AP_ExprPoolIndex*, OOSQL_DB_Value*);
        Four            arithAbs(AP_ExprPoolIndex*, OOSQL_DB_Value*);

        // comparison functions
        Four            arithCmp(AP_ExprPoolIndex*, AP_ExprPoolIndex*);
        Four            stringCmp(AP_ExprPoolIndex*, AP_ExprPoolIndex*);
        Four            collectionCmp(AP_ExprPoolIndex*, AP_ExprPoolIndex*);
		Four            nullCmp(AP_ExprPoolIndex*);
		Four            oidCmp(AP_ExprPoolIndex*, AP_ExprPoolIndex*);
        Four            objectCmp(AP_ExprPoolIndex*, AP_ExprPoolIndex*);
        Four            diffRes2CmpRes(OOSQL_DB_Value*, Four);
        Four            compareColListStruct(Four, OOSQL_StorageManager::ColListStruct*, OOSQL_StorageManager::ColListStruct*);
        Four            compareStringColumn(OOSQL_StorageManager::ColListStruct*, OOSQL_StorageManager::ColListStruct*);
		Four			collectionUnion(AP_ExprPoolIndex*, AP_ExprPoolIndex*);
		Four			collectionExcept(AP_ExprPoolIndex*, AP_ExprPoolIndex*);
		Four			collectionCmpIn(AP_ExprPoolIndex*, AP_ExprPoolIndex*);
		Four            geoSpatialCmpNorth(AP_ExprPoolIndex*, AP_ExprPoolIndex*);
        Four            geoSpatialCmpSouth(AP_ExprPoolIndex*, AP_ExprPoolIndex*);
        Four            geoSpatialCmpEast(AP_ExprPoolIndex*, AP_ExprPoolIndex*);
        Four            geoSpatialCmpWest(AP_ExprPoolIndex*, AP_ExprPoolIndex*);
        Four            geoSpatialCmpContain(AP_ExprPoolIndex*, AP_ExprPoolIndex*);
        Four            geoSpatialCmpCover(AP_ExprPoolIndex*, AP_ExprPoolIndex*);
        Four            geoSpatialCmpDisjoint(AP_ExprPoolIndex*, AP_ExprPoolIndex*);
        Four            geoSpatialCmpEqual(AP_ExprPoolIndex*, AP_ExprPoolIndex*);
        Four            geoSpatialCmpMeet(AP_ExprPoolIndex*, AP_ExprPoolIndex*);
        Four            geoSpatialCmpOverlap(AP_ExprPoolIndex*, AP_ExprPoolIndex*);

		// arithmetic functions
		Four			geoSpatialArithArea(AP_FuncEvalInfo *funcInfo, OOSQL_DB_Value *result);
		Four			geoSpatialArithLength(AP_FuncEvalInfo *funcInfo, OOSQL_DB_Value *result);
		Four			geoSpatialArithDistance(AP_FuncEvalInfo *funcInfo, OOSQL_DB_Value *result);

		// user defined function
		Four			execUserDefinedFunction(AP_FuncEvalInfo *funcInfo, OOSQL_DB_Value *result);

        // check having condition
        Four            checkHavingCond();

#ifndef SLIMDOWN_OPENGIS
        Four            evalOGISOperator(AP_OperatorStruct*, OOSQL_DB_Value&);
#endif

		/****************************************************
		 *  functions for evaluation buffer management      *
		 ****************************************************/

        // calculate the evaluation buffer size of a access plan element
        Four    calcEvalBufSize(Four, Four&, Four&);
        // construct OOSQL_StorageManager::ColListStruct for evaluation buffer
        Four    consColListStructForEvalBuf(Four, OOSQL_EvalBuffer*);

        // allocate evaluation buffer for each access plan element
        Four    prepareEvalBuffer();
        // release all resources of temporary file
        Four    finalizeTempFiles();
        // create temporary file(s) necessary for evaluation according to the access plan.
        Four    prepareTempFile();
		Four    prepareTempFileOpenIt(Four tempFileNum, AP_ProjectionListElement* projList);
        // make column information for temporary file
        Four    makeTempFileInfo(Four, AP_UsedColPoolIndex, Boolean, Boolean);
        // create a temporary file which has the unique name in a database
        Four    createTempFile(OOSQL_TempFileInfo*);
		Four	createSortStream(OOSQL_TempFileInfo*, AP_ProjectionListElement* projInfo);
        Four    consColListStructForTempFile(AP_ProjectionListElement*);

		/********************************************
		 *  functions for processing projection     *
		 ********************************************/

        // make query result information
        Four    prepareQueryResultInfo();

        // process projection
        Four    processProjectionToCharBuf(AP_ProjectionListElement*, Four, char*, Four*, Boolean);
        Four    processProjectionToTempFile(AP_ProjectionListElement*, Boolean);
        Four    projectToColListStruct(AP_ProjectionListElement*, Four, OOSQL_StorageManager::ColListStruct*, Boolean, Boolean, Four&);
        Four    projectPathExprToColListStruct(AP_ProjectionElement*, OOSQL_StorageManager::ColListStruct*, Boolean);
        Four    projectAggrFuncToColListStruct(AP_ProjectionElement*, OOSQL_StorageManager::ColListStruct*);
        Four    projectFunctionToColListStruct(AP_ProjectionElement*, OOSQL_StorageManager::ColListStruct*);
        Four    projectFuncResultToColListStruct(AP_ProjectionElement*, OOSQL_StorageManager::ColListStruct*);
        Four    projectValueToColListStruct(AP_ProjectionElement*, OOSQL_StorageManager::ColListStruct*);
        Four    projectOperatorToColListStruct(AP_ProjectionElement*, OOSQL_StorageManager::ColListStruct*);
		Four    projectOidToColListStruct(AP_ProjectionElement*, OOSQL_StorageManager::ColListStruct*);
		Four    projectionExprToColListStruct(AP_ProjectionElement*, OOSQL_StorageManager::ColListStruct*);


		/************************************************************
		 *  functions for processing grouping and aggregation       *
		 ************************************************************/

        // process grouping
        Four    setGroupByKey();
        // check if the current object belongs to the current group
        Four    isSameGroupByKey();

        Four    prepareGroupingAndAggrFunc(Four);

        // process aggregate function
        Four    initAggrFuncResult(Four);
        Four    processAggrFuncList(Four);
        Four    processAggrFunc(Four, Four);
        Four    calcAggrFuncResult(Four, Four);
        Four    prepareScanForAggrDist(Four);
        Four    readArgumentForAggrDist(AP_AggrFuncElement*);
        Four    getAggrFuncResultType(AP_AggrFuncElement*);

		/********************************************
		 *  functions for processing sorting        *
		 ********************************************/

        // process sorting a temporary file
        Four    sorting();
        Four    sortTempFile(AP_ProjectionListElement*);


		/********************************************************************
		 *  extended to support Text IR index scan                          *
		 *  NOTE: This scan treats MATCH() function as index predicates.    *
		 ********************************************************************/

        Four    allocTextIR_SubPlanEvalBuffer();                                
        Four    initTextIR_IndexScan(Four ocn, AP_IndexInfoPoolIndex indexInfo);            
        Four    initTextIR_SubPlanNode(Four ocn, Two colNo, OOSQL_StorageManager::IndexID*, AP_TextIndexCondPoolIndex, AP_IndexInfoPoolIndex indexInfoPoolIndex);
        Four    openTextIR_IndexScan(Four ocn, Two colNo, OOSQL_StorageManager::IndexID*, AP_TextIndexCondPoolIndex, Four* osn);
        Four    closeTextIR_IndexScan(AP_TextIndexCondPoolIndex);                     

        Four    execTextIR_IndexScan(AP_IndexInfoPoolIndex, OOSQL_TextIR_PostingQueue&, Four);
        Four    execTextIR_SubPlan(AP_TextIndexCondPoolIndex, OOSQL_TextIR_PostingQueue&, Four);
        Four    execTextIR_CmpOp(AP_TextIndexCondPoolIndex, OOSQL_TextIR_PostingQueue&, Four);
        Four    execTextIR_OpAnd(AP_TextIndexCondPoolIndex, OOSQL_TextIR_PostingQueue&, Four);
        Four    execTextIR_OpOr(AP_TextIndexCondPoolIndex, OOSQL_TextIR_PostingQueue&, Four);
        Four    execTextIR_OpMinus(AP_TextIndexCondPoolIndex, OOSQL_TextIR_PostingQueue&, Four);
        Four    execTextIR_OpAccumulate(AP_TextIndexCondPoolIndex, OOSQL_TextIR_PostingQueue&, Four);
        Four    execTextIR_OpMultiply(AP_TextIndexCondPoolIndex, OOSQL_TextIR_PostingQueue&, Four);
        Four    execTextIR_OpThreshold(AP_TextIndexCondPoolIndex, OOSQL_TextIR_PostingQueue&, Four);
        Four    execTextIR_OpMax(AP_TextIndexCondPoolIndex, OOSQL_TextIR_PostingQueue&, Four);
        Four    execTextIR_OpNear(AP_TextIndexCondPoolIndex, OOSQL_TextIR_PostingQueue&, Four);
		Four    execTextIR_OpNearWithOrder(AP_TextIndexCondPoolIndex, OOSQL_TextIR_PostingQueue&, Four);
		Four    execTextIR_OpNear_IsNear(OOSQL_TextIR_Posting* op1Posting, OOSQL_TextIR_Posting* op2Posting,Four distance);
		Four    execTextIR_OpNear_IsNearWithOrder(OOSQL_TextIR_Posting* op1Posting, OOSQL_TextIR_Posting* op2Posting,Four distance);
        Four    execTextIR_KeywordNode(AP_TextIndexCondPoolIndex, OOSQL_TextIR_PostingQueue&, Four);
        Four    execTextIR_IndexScanNextPosting(AP_TextIndexCondPoolIndex, OOSQL_TextIR_PostingQueue&, Four);
        Four    execTextIR_NextPostingFromTempFile(AP_TextIndexCondPoolIndex, OOSQL_TextIR_PostingQueue&, Four);
        Four    execTextIR_FetchAllAndSortPostings(AP_TextIndexCondPoolIndex);
		Four	execTextIR_GetMemorySizeToSortPosting(Four& memorySize);
		Four	execTextIR_GetNPostingsToSort(Four ocn, OOSQL_StorageManager::IndexID* indexId, AP_TextIndexCondPoolIndex indexCondPoolIndex, Four* nPostings);
		Four    execTextIR_GetNPostingsToSort(Four ocn, AP_IndexInfoPoolIndex indexInfoPoolIndex, Four* nPostings); 
		Four	execTextIR_EstimateNumOfResults(Four& estimatedResultNum);
		Four	execTextIR_GetNPostingsToEstimate(Four ocn, OOSQL_StorageManager::IndexID* indexId, AP_TextIndexCondPoolIndex indexCondPoolIndex, Four* nPostings); 
		Four    execTextIR_GetNPostingsToEstimate(Four ocn, AP_IndexInfoPoolIndex indexInfoPoolIndex, Four* nPostings); 

        /* interact with Min-Max Heap */
        Four    execTextIR_ConstructMinMaxHeap(AP_TextIndexCondPoolIndex, Four);
        Four    execTextIR_NextMinMaxHeapEntry(AP_TextIndexCondPoolIndex, OOSQL_TextIR_Posting**);

        /*
         * support Text IR function
         *      NOTE: Here we evaluate MATCH() function for an object.
         */
        Four    execTextIR_FnMatch(AP_FuncEvalInfo*, OOSQL_DB_Value*);                      
        Four    execTextIR_FnWeight(AP_FuncResultAccessInfo*, OOSQL_DB_Value*);             
        Four    initTextIR_SubPlanNodeForFnMatch(Four, OOSQL_StorageManager::OID*, Four, AP_TextIndexCondPoolIndex);
        Four    openTextIR_Scan(Four, OOSQL_StorageManager::OID*, Four, AP_TextIndexCondPoolIndex, Four *osn);
        Four    closeTextIR_Scan(AP_TextIndexCondPoolIndex);                  
        Four    execTextIR_SubPlanForOneDocument(AP_TextIndexCondPoolIndex, OOSQL_TextIR_Posting**);
        Four    execTextIR_OpAndForOneDocument(AP_TextIndexCondPoolIndex, OOSQL_TextIR_Posting**);
        Four    execTextIR_OpOrForOneDocument(AP_TextIndexCondPoolIndex, OOSQL_TextIR_Posting**);
        Four    execTextIR_OpMinusForOneDocument(AP_TextIndexCondPoolIndex, OOSQL_TextIR_Posting**);
        Four    execTextIR_OpAccumulateForOneDocument(AP_TextIndexCondPoolIndex, OOSQL_TextIR_Posting**);
        Four    execTextIR_OpMultiplyForOneDocument(AP_TextIndexCondPoolIndex, OOSQL_TextIR_Posting**);
        Four    execTextIR_OpThresholdForOneDocument(AP_TextIndexCondPoolIndex, OOSQL_TextIR_Posting**);
        Four    execTextIR_OpNearForOneDocument(AP_TextIndexCondPoolIndex, OOSQL_TextIR_Posting**);
		Four    execTextIR_OpNearWithOrderForOneDocument(AP_TextIndexCondPoolIndex, OOSQL_TextIR_Posting**);
        Four    execTextIR_KeywordNodeForOneDocument(AP_TextIndexCondPoolIndex, OOSQL_TextIR_Posting**);
        Four    execTextIR_ScanNextPosting(AP_TextIndexCondPoolIndex, OOSQL_TextIR_Posting**);

        /*
         * miscellaneous functions for Text IR
         */
#if defined (EXTENDED_BOOLEAN_MODEL)
        Four    execTextIR_setPosting(OOSQL_TextIR_Posting*, OOSQL_TextIR_Posting*, OOSQL_TextIR_Weight);
#elif defined (HEURISTIC_MODEL)
        Four    execTextIR_setPosting(OOSQL_TextIR_Posting*, OOSQL_TextIR_Posting*, OOSQL_TextIR_Weight, OOSQL_TextIR_Bonus);
#endif
        Four    execTextIR_makeMinMaxHeapEntryDescForPosting(OOSQL_MinMaxHeap_EntryDesc**);
        Four    execTextIR_convertPostingToMinMaxHeapEntry(OOSQL_TextIR_Posting*, OOSQL_MinMaxHeap_FieldList*);
        Four    execTextIR_convertMinMaxHeapEntryToPosting(OOSQL_MinMaxHeap_FieldList*, OOSQL_TextIR_Posting*);
        Four    execTextIR_convertColListToPosting(Four postingKind, OOSQL_StorageManager::ColListStruct*, OOSQL_TextIR_Posting*);
        Four    execTextIR_convertPostingToColList(Four postingKind, OOSQL_TextIR_Posting*, OOSQL_StorageManager::ColListStruct*);

        Four    execTextIR_FreeTempFile(oosql_TextIR_TempFileInfo*);

        Four    compareTextIR_Keyword(Two, char*, Two, char*);
		
		Four	determineFunctionForGetEmbeddedAttrVals(OOSQL_StorageManager::EmbeddedAttrTranslationInfo* embeddedAttrTranslationInfo, 
			                                            Four nCols, OOSQL_StorageManager::ColListStruct* clist, OOSQL_GetEmbeddedAttrsValFuncType* func);
		/****************************
		 *  miscellaneous functions *
		 ****************************/

        // data copy functions
        Four    copyColListStructToValue(Four, OOSQL_StorageManager::ColListStruct*, OOSQL_DB_Value*);
        Four    copyDB_ValueToColListStruct(Four, OOSQL_DB_Value*, OOSQL_StorageManager::ColListStruct*);
        Four    copyColListStructToCharBuf(Four, Four, OOSQL_StorageManager::ColListStruct*, char*);

        Four    getArgumentInfo(AP_ArgumentPoolIndex*, Four*, Four*);
        Four    getColInfo(AP_PathExprAccessInfo*, Four*, Four*);
        Four    getColInfo(AP_TempFileAccessInfo*, Four*, Four*);
        Four    getColInfo(AP_AggrFuncResultAccessInfo*, Four*, Four*);
        Four    getColInfo(AP_FuncEvalInfo*, Four*, Four*);
        Four    getColInfo(AP_FuncResultAccessInfo*, Four*, Four*);
        Four    getColInfo(ValueElement*, Four*, Four*);
        Four    getColInfo(OperatorStruct*, Four*, Four*);

        Four    accumulateColListStruct(Four, OOSQL_StorageManager::ColListStruct*, Four, OOSQL_StorageManager::ColListStruct*);
        Four    copyColListStruct(Four, OOSQL_StorageManager::ColListStruct*, OOSQL_StorageManager::ColListStruct*);
        Four    copyArgumentToColListStruct(AP_ArgumentPoolIndex*, OOSQL_StorageManager::ColListStruct*);
        Four    accumulateArgumentToColListStruct(AP_ArgumentPoolIndex*, Four, OOSQL_StorageManager::ColListStruct*);
		Four    isNullArgumentValue(AP_ArgumentPoolIndex*, Boolean*);
        Four    compareArgumentWithColListStruct(AP_ArgumentPoolIndex*, OOSQL_StorageManager::ColListStruct*);

        Four    copyStringValueToString(StringPoolIndex*, char*, Four);

        // support multi-key index
        Four    appendKeyValueToBoundCond(OOSQL_StorageManager::BoundCond*, OOSQL_DB_Value*);
		Four	makeStringFromStringPool(char* string, StringPoolIndex stringPoolIndex);
		Four	resetCurrentWhereCondNodes();

		bool	isUpdateQuery() { 
			if(m_queryType == OOSQL_INSERT_QUERY || 
			   m_queryType == OOSQL_INSERT_QUERY_WITH_PARAM || 
	           m_queryType == OOSQL_DELETE_QUERY ||
	           m_queryType == OOSQL_UPDATE_QUERY ||
	           m_queryType == OOSQL_UPDATE_QUERY_WITH_PARAM ||
	           m_queryType == OOSQL_CREATE_TABLE_QUERY ||
	           m_queryType == OOSQL_DROP_TABLE_QUERY ||
	           m_queryType == OOSQL_CREATE_INDEX_QUERY ||
	           m_queryType == OOSQL_DROP_INDEX_QUERY ||
	           m_queryType == OOSQL_CREATE_VIEW_QUERY ||
	           m_queryType == OOSQL_CREATE_FUNCTION_QUERY ||
	           m_queryType == OOSQL_DROP_FUNCTION_QUERY ||
	           m_queryType == OOSQL_CREATE_PROCEDURE_QUERY ||
	           m_queryType == OOSQL_DROP_PROCEDURE_QUERY)
				return true;
			else
				return false;
		}

		Four checkIfProjectionCanbeFastEncoded(AP_ProjectionListElement* projInfo, Four nCols);
		Four constructFastEncodingInfo(AP_ProjectionListElement* projInfo, Four nCols, Boolean grpByFlag, OOSQL_FastEncodingInfo* encodingInfo);

		/*----------------------------------------------------------------------*
		 *      public member functions                                         *
		 *----------------------------------------------------------------------*/

public:

        // constructor: constructed with access plan
        OOSQL_Evaluator(OOSQL_StorageManager* storageManager, OOSQL_Catalog* catalog, OOSQL_ErrorMessage* errorMessage, 
						OOSQL_ExternalFunctionManager* externalFunctionManager, OOSQL_ExternalFunctionDispatcher* externalFunctionDispatcher,
			            OOSQL_StorageManager::XactID, Four);

        // destructor
        ~OOSQL_Evaluator();

        // prepare evaluation with access plan as input
        Four    Prepare(OOSQL_AccessPlan* , OQL_GDSPOOL&, OOSQL_SortBufferInfo* sortBufferInfo, OOSQL_QueryType queryType);

        // re-prepare evaluation with the same access plane
        Four    RePrepare();

        // perform evaluation and return the result tuple(s) through a stream of bytes
        Four    EvalAndFetch(Four nEvaluationsToDo, void* headerBuffer, Four headerBufferSize, void* dataBuffer, Four dataBufferSize, Four* nEvaluationsDone)
		{
			if(m_callEvalAndFetchForSelectOnlyFlag)
				return EvalAndFetchForSelectOnly(nEvaluationsToDo, headerBuffer, headerBufferSize, dataBuffer, dataBufferSize, nEvaluationsDone);
			else
				return EvalAndFetchForGeneralQuery(nEvaluationsToDo, headerBuffer, headerBufferSize, dataBuffer, dataBufferSize, nEvaluationsDone);
		}
		Four    EvalAndFetchForGeneralQuery(Four nEvaluationsToDo, void* headerBuffer, Four headerBufferSize, void* dataBuffer, Four dataBufferSize, Four* nEvaluationsDone);
		Four	EvalAndFetchForSelectOnly(Four nEvaluationsToDo, void* headerBuffer, Four headerBufferSize, void* dataBuffer, Four dataBufferSize, Four* nEvaluationsDone);

        // reserved for extension of abnormal case path expression
        Four    ResumeAndFetch(Four, char*, Four*);

        // finalize evaluation
        Four    End();

		Four	GetData(Four colNo, Four startPos, void* buffer, Four bufferLength, Four* dataLength);
		Four	GetMultiColumnData(Four nColumns, OOSQL_GetDataStruct* getDataStruct);
		Four	GetMultipleResult(Four nResultsToRead, void* headerBuffer, Four headerBufferSize, void* dataBuffer, Four dataBufferSize, Four* nResultsRead);

		Four	PutData(Four paramNo, Four startPos, void* buffer, Four bufferLength);

		Four	GetPutDataParamType(Four paramNo);

		Four	GetType(Four colNo);

		Four	GetLength(Four colNo);

		Four	GetOID(Four targetNo, OOSQL_StorageManager::OID& oid);

		Four	GetComplexTypeInfo(Two columnNo, OOSQL_ComplexTypeInfo* complexTypeInfo);

		Four 	EstimateNumResults(Four* nResults);
};

inline Four OOSQL_Evaluator::resetCurrentWhereCondNodes()
{
	Four currAccessIndex;

	if(ACCESSPLAN.m_nAP_Elem <= m_currPlanIndex)
		m_currWhereCondNodes.setNull();
	else
	{
		currAccessIndex = EVAL_ACCESSLISTTABLE[m_currPlanIndex].currAccessIndex;
		if(currAccessIndex == 0)
			m_currWhereCondNodes = ACCESSPLANELEMENTS[m_currPlanIndex].condNodes;
		else
			m_currWhereCondNodes = ACCESSPLANELEMENTS[m_currPlanIndex].subClassInfo.getElements(m_pool->subClassPool, currAccessIndex - 1).condNodes;
	}

	return eNOERROR;
}

#endif

