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

#ifndef _OOSQL_Eval_DS_hxx_
#define _OOSQL_Eval_DS_hxx_

/*
    MODULE:
        OOSQL_Eval_DS.hxx

    DESCRIPTION:
        This header file defines data structures for query evaluation module.

*/


/*
 * include files
 */
#include "OOSQL_Common.h"             // common header file
#include "OOSQL_AccessPlan.hxx"
#include "oosql_Eval_TextIR.hxx"
#include "OOSQL_StorageManager.hxx"
#include "OOSQL_MemoryManager.hxx"
#include "OOSQL_MemoryManagedObject.hxx"

class OOSQL_SortStream;
typedef OOSQL_StorageManager::ColListStruct   EVAL_EvalBufferSlot;

/****************************************************************************************************************
 *      evaluation buffer:                                                                                      *
 *                                                                                                              *
 *      NOTE: structure of evaluation buffer                                                                    *
 *                                                                                                              *
 *      +--------------+----------------------------------------+---------------+----------------------------+  *
 *      | used columns | used method results (future extension) | group by keys | aggregate function results |  *
 *      +--------------+----------------------------------------+---------------+----------------------------+  *
 ****************************************************************************************************************/

class   OOSQL_EvalBuffer : public OOSQL_MemoryManagedObject {
public:
        OOSQL_StorageManager::OID	oid;            // oid of an object
        Four						logicalID;      

        /* the # of used attributes + the # of group by keys (if any) + 
         * the # of aggregate function results (if any)
         */
        Four						nCols;
        Four						nGrpByCols;
        Four						nAggrFuncResults;
        Four						nFuncMatchResults;      // the # of MATCH() result buffer slots
        Four						strBufSize;             // maximum size of all string and variable string for one ColListStruct

        Boolean						isDualBuf;              // flag indicating dual buffer mode

        EVAL_EvalBufferSlot			*clist;                 // ptr. to primary column list struct
        EVAL_EvalBufferSlot			*prevColList;           // ptr. to secondary column list struct
        EVAL_EvalBufferSlot			*methodResult;          // reserved for method support
        EVAL_EvalBufferSlot			*grpByColList;
        EVAL_EvalBufferSlot			*aggrFuncResults;
        EVAL_EvalBufferSlot			*funcMatchResults;      // MATCH() result buffer slots
		Four                        *nTuplesForSumAndAvg;   // the # of distinct data for SUM and AVG aggregate func. with distinct flag
        Four						*nTuplesForAvg;         // the # of distinct data for AVG aggregate func. with distinct flag
        char						*strBuf;				// pointer to string buffer
															// NOTE: if isDualBuf is TRUE, (strBufSize * 2) bytes are allocated.
															//
public:

        /* constructor */
        OOSQL_EvalBuffer();
		virtual ~OOSQL_EvalBuffer();
		
		Four init(Four, Four, Four, Four, Four, Boolean = SM_FALSE );

        /* provides member functions to access buffer slot in a customized fashion */
		OOSQL_StorageManager::OID*  getOID_Ptr()		{ return &oid; }
		void						clearOID()			{ OOSQL_StorageManager::OIDCLEAR(oid); }
		void						setOID(OOSQL_StorageManager::OID* oid)	{ this->oid = *oid; }
        Four						getLogicalID();     
        EVAL_EvalBufferSlot*		getColSlotPtr( Four = 0 );
        EVAL_EvalBufferSlot*		getGrpBySlotPtr( Four = 0 );
        EVAL_EvalBufferSlot*		getAggrFuncResSlotPtr( Four = 0 );
        EVAL_EvalBufferSlot*		getFnMatchSlotPtr( Four = 0 );

#ifdef  TRACE
        void    dump();
#endif
};

class OOSQL_EvalIndexBuffer : public OOSQL_MemoryManagedObject {
public:
	OOSQL_EvalIndexBuffer();
	virtual ~OOSQL_EvalIndexBuffer();

private:
};

/* 
 *	The value of each field can affect the evaluation control.
 */
struct  EvalStatus {
        Four    globalStatus;                   /* status indicating what the evaluator is doing currently */

        One     endOfEval;                      /* TRUE/SM_FALSE */
        One     queryResultBufferFull;          /* TRUE/SM_FALSE */

        /* projection of the previous evaluation */
        One     wasLastProjectionCompleted;     /* TRUE/SM_FALSE */
        One     wasGroupByResult;               /* TRUE/SM_FALSE */

        /* flag indicating it needs to process the current plan element. */
        One     needToProcessCurrPlanElem;      /* TRUE/SM_FALSE */

        One     groupingStatus;                 /* EVALSTATUS_INIT, EVALSTATUS_PROCESSING, EVALSTATUS_END */
        One     aggregationStatus;              /* EVALSTATUS_INIT, EVALSTATUS_PROCESSING, EVALSTATUS_END */
        One     prepareAndSortStatus;           /* EVALSTATUS_INIT, EVALSTATUS_PROCESSING, EVALSTATUS_END */

        /* added to process end-of-scan for class kind of null */
        One     nullClassEndOfScan;             /* TRUE/SM_FALSE */

        /* added to record the first plan element of the current nested-loop */
        Four    firstPlanElemOfCurrNestedLoop;
        Four    lastPlanElemOfCurrNestedLoop;
};

/************************************************************************
 *      data structures necessary for storing access information        *
 ************************************************************************/
class OOSQL_ScanInfo : public OOSQL_MemoryManagedObject {
public:
	Four									ocn;
	Four									scanId;
	Four									nBoolExprs;
	OOSQL_StorageManager::BoolExp*			boolExprs;
	AP_BoolExprElement*						boolExprInfos;
	OOSQL_StorageManager::OID*				oid;
	OOSQL_StorageManager::ColListStruct*	clist;
	Four									nCols;
	AP_ColNoMapElement*						colNoMap;
	AP_IndexInfoElement*					indexNode;
	EVAL_EvalBufferSlot*					fnMatchResult;
	OOSQL_TextIR_PostingQueue				postingQueue;
	OOSQL_TextIR_PostingQueue				op1PostingQueue;
	OOSQL_TextIR_PostingQueue				op2PostingQueue;

	OOSQL_ScanInfo() : postingQueue(pMemoryManager), op1PostingQueue(pMemoryManager), op2PostingQueue(pMemoryManager) {
		ocn             = -1;
		scanId			= -1;
		nBoolExprs		= 0;
		boolExprs		= NULL;
		boolExprInfos	= NULL;
		oid             = NULL;
		clist           = NULL;
		nCols           = 0;
		colNoMap        = NULL;
		indexNode       = NULL;
		fnMatchResult	= NULL;
	}
};

class   OOSQL_ScanInfoTable : public OOSQL_MemoryManagedObject {
public:

    Four						  tableSize;
	OOSQL_ScanInfo*				  scanInfos;

    /*
     * public memeber functions
     */
    OOSQL_ScanInfoTable(Four size );
	virtual ~OOSQL_ScanInfoTable();

    /* method for accessing this table by PoolIndex */
    OOSQL_ScanInfo&   operator [] (int index) 
	{
		return scanInfos[index];
	}

	Four prepareBoolExpression(int index, int nBools);
	Four resetBoolExpression(int index);
};

class   OOSQL_AccessElement : public OOSQL_MemoryManagedObject {
public:

        Four				classId;        /* class ID: necessary to find open scan number by class ID */
        Four				ocn;            // open class number (copied from System Catalog)
        Four				osn;            // open scan number
        Four				accessMethod;   // access method

        union {
            /* sequential scan information */
            Four        scanDirection;  // scan direction

            /* index scan information */
            AP_IndexInfoPoolIndex indexInfo;
        };

        /* constructor */
        OOSQL_AccessElement();
};


#define ACCESSDIRECTION_FORWARD         0
#define ACCESSDIRECTION_BACKWARD        1

class   OOSQL_AccessList : public OOSQL_MemoryManagedObject {
public:
        Four            numClasses;             // the # of classes to access
        Four            endOfCurrAccess;        // flag indicating if we accessed all classes
        Four            accessDirection;        
        Four            currAccessIndex;        // array index for the current access

		// For Supporting DML operation
		Four									writeOcn;
		OOSQL_StorageManager::ColListStruct*	writeColList;

        /* list of access for an access plan element
         * NOTE: An access plan element accesses the class and its subclass(es),
         *       so an access element contains the information to access a class.
         */
        OOSQL_AccessElement* accessList;

        /*
         * public member functions
         */
        /* constructor */
        OOSQL_AccessList();
		virtual ~OOSQL_AccessList();

		// initialize
		Four    init(Four);

        // check if the current access ended
        Four    isEndOfCurrAccess();
        // check if all classes in the access list are accessed
        Four    isEndOfAllAccess();

        Four    setEndOfAllAccess();

        Four    reverseAccessDirection();

        // move to the next access element
        Four    moveToNextAccessElem();

        Four    getCurrClassID();
        Four    getCurrOcn();

        // get the current scan ID which is asserted to be opened
        Four    getCurrScanID();

#ifdef  TRACE
        void    dump();
#endif
};


/************************************************************************
 *      temporary file information                                      *
 *      NOTE: This contains scan no. of the temp. file and              *
 *            index of the plan element.                                *
 *            Plan index is used to control the evaluation sequence.    *
 ************************************************************************/

class   OOSQL_TempFileInfo : public OOSQL_MemoryManagedObject {
public:
        char							name[MAXCLASSNAME];     // temp. file name
        Four							ocn;                    // open class number
        Four							osn;                    // open scan number
        Four							nCols;                  // the # of columns of temp. file
        OOSQL_StorageManager::AttrInfo	*attrInfo;              // column information
		OOSQL_SortStream*				sortStream;				// sort stream object

        char							indexName[MAXINDEXNAME];
        OOSQL_StorageManager::IndexID   indexId;

        /* buffer for writing tuples into the temp. file
         * NOTE: This buffer is allocated for each temporary file and
         *      used only for writing purpose. It is differenct from 'Evaluation buffer' 
         *      which used for reading object from a class.
         */

        Boolean								isDualBuf;              // flag indicating dual buffer mode

        Four								strBufSize;
        char								*strBuf;

        OOSQL_StorageManager::OID           oid;

        OOSQL_StorageManager::ColListStruct *clist;                 // ptr. to primary column list struct
        OOSQL_StorageManager::ColListStruct *prevColList;           // ptr. to secondary column list struct

		Four								isTextAttrExist;	
		Four                                useFastEncoding;		
		Four								firstFastEncodedNextScan;	

        /*
         * public member functions
         */
        OOSQL_TempFileInfo(Four, Four, Boolean = SM_FALSE );
		virtual ~OOSQL_TempFileInfo();

        // exchange clist and prevColList
        Four    saveCurrTuple();

        // check if the current temp. file is not yet created
        Four    isNotCreated();

        // get open class number for the current temp. file 
        Four    getOcn();
        // get open scan number for the current temp. file 
        Four    getOsn();
};


class   OOSQL_TempFileInfoTable : public OOSQL_MemoryManagedObject {
public:
        Four					nTempFiles;             // the # of temporary files
        OOSQL_TempFileInfo**	tempFileInfo;		    // array of ptr. to temporary file information

        // contructor
        OOSQL_TempFileInfoTable(Four);
		virtual ~OOSQL_TempFileInfoTable();

        /* indexing operator: indexing by temp. file number */
        OOSQL_TempFileInfo*     operator [] (Four tempFileNum)
		{
	        /* check input parameter */
		    if (tempFileNum < 0 || nTempFiles <= tempFileNum)
				return NULL;

			/* return */
			return tempFileInfo[tempFileNum];
		}
};


/****************************************
 *      Evaluation control information  *
 ****************************************/

struct  TempFileSavingInfo {
        Four            savingPlanNo;   // index to A.P. elem. in which tuples are saved

        TempFileSavingInfo();
};


struct  GroupingInfo {
        Four    grpByPlanNo;
        Four    totalNumOfGroup;        // total # of groups
};

struct  AggregateInfo {
        Four    isEndOfScan;            // TRUE if end of scan is occurred in temp. file containing argument of aggr. distinct
        Four    aggregatePlanNo;        // index to access plan where aggr. func. is being processed
};


#endif

