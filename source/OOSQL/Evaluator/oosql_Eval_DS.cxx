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

/*
    MODULE:
        OOSQL_EvalDS.C

    DESCRIPTION:
        This module implements the data structures used for query evaluation module.

    IMPORTS:

    EXPORTS:
*/


#include "OOSQL_Eval_DS.hxx"

OOSQL_EvalBuffer::OOSQL_EvalBuffer()
{
    nCols				= 0;
    nGrpByCols			= 0;
    nAggrFuncResults	= 0;
    nFuncMatchResults	= 0;
    strBufSize			= 0;
    isDualBuf			= SM_FALSE;
    clist				= NULL;
    prevColList			= NULL; 
    methodResult		= NULL;
    grpByColList		= NULL;
    aggrFuncResults		= NULL;
    funcMatchResults	= NULL;
	nTuplesForSumAndAvg = NULL;
    strBuf				= NULL;
}

Four OOSQL_EvalBuffer::init(
    Four numCols,               /* IN: evaluation buffer size (the # of columns) */
    Four nGrpByKeys,            /* IN: */
    Four nAggrFunc,             /* IN: */
    Four nFuncMatch,            /* IN: # of MATCH function result */
    Four stringSize,            /* IN: memory size for string and variable string */
    Boolean isDualBuffer        /* IN: flag indicating */
)
{
    char *pCurrBufSlot;

    if (numCols < 0) {
        OOSQL_PRTERR(eBADPARAMETER_OOSQL);
        numCols = 0;
    }
    if (nGrpByKeys < 0) {
        OOSQL_PRTERR(eBADPARAMETER_OOSQL);
        nGrpByKeys = 0;
    }
    if (nAggrFunc < 0) {
        OOSQL_PRTERR(eBADPARAMETER_OOSQL);
        nAggrFunc = 0;
    }
    if (nFuncMatch < 0) {
        OOSQL_PRTERR(eBADPARAMETER_OOSQL);
        nFuncMatch = 0;
    }
    if (stringSize < 0) {
        OOSQL_PRTERR(eBADPARAMETER_OOSQL);
        stringSize = 0;
    }

	this->isDualBuf = isDualBuffer;

    this->nCols = numCols;
    this->clist = (EVAL_EvalBufferSlot*)pMemoryManager->Alloc(sizeof(EVAL_EvalBufferSlot) * 
																	(numCols + nGrpByKeys + nAggrFunc + nFuncMatch));
	if(this->isDualBuf == SM_TRUE) 
		this->prevColList 
		        = (EVAL_EvalBufferSlot*)pMemoryManager->Alloc(sizeof(EVAL_EvalBufferSlot) * 
																	(numCols + nGrpByKeys + nAggrFunc + nFuncMatch));
	else
        this->prevColList = NULL;

    this->nGrpByCols   = nGrpByKeys;
    this->grpByColList = (EVAL_EvalBufferSlot*) pMemoryManager->Alloc(sizeof(EVAL_EvalBufferSlot) * nGrpByKeys);

    this->nAggrFuncResults = nAggrFunc;
    this->aggrFuncResults  = (EVAL_EvalBufferSlot*) pMemoryManager->Alloc(sizeof(EVAL_EvalBufferSlot) * nAggrFunc);

    this->aggrFuncResults->nullFlag = SM_FALSE;

	this->nTuplesForSumAndAvg    = (Four*) pMemoryManager->Alloc(sizeof(Four) * nAggrFunc);

    this->nFuncMatchResults = nFuncMatch;
    this->funcMatchResults  = (EVAL_EvalBufferSlot*) pMemoryManager->Alloc(sizeof(EVAL_EvalBufferSlot) * nFuncMatch);

    this->strBufSize = stringSize;
	if(isDualBuffer == SM_TRUE)
		this->strBuf = (char*) pMemoryManager->Alloc(sizeof(char) * stringSize * 2);
	else
		this->strBuf = (char*) pMemoryManager->Alloc(sizeof(char) * stringSize);

	return eNOERROR;
}

OOSQL_EvalBuffer::~OOSQL_EvalBuffer()
{

	pMemoryManager->Free(clist);
	if(prevColList)
		pMemoryManager->Free(prevColList);
	pMemoryManager->Free(grpByColList);
	pMemoryManager->Free(aggrFuncResults);
	pMemoryManager->Free(nTuplesForSumAndAvg);
	pMemoryManager->Free(funcMatchResults);
	pMemoryManager->Free(strBuf);
}

Four    OOSQL_EvalBuffer::getLogicalID()
/* 
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{

    return logicalID;
}


EVAL_EvalBufferSlot*    OOSQL_EvalBuffer::getColSlotPtr(
        Four    colSlotIndex                    // IN:
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{

    /* check input paramter */
    if (colSlotIndex < 0 || nCols <= colSlotIndex) {
        return NULL;
    }

    /* return pointer to the buffer slot */
    return &clist[colSlotIndex];
}

EVAL_EvalBufferSlot*    OOSQL_EvalBuffer::getGrpBySlotPtr(
        Four    grpBySlotIndex                  // IN:
)
/* 
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{

    /* check input paramter */
    if (grpBySlotIndex < 0 || nGrpByCols <= grpBySlotIndex) {
        return NULL;
    }

    /* return pointer to the buffer slot */
    return &grpByColList[grpBySlotIndex];
}

EVAL_EvalBufferSlot*    OOSQL_EvalBuffer::getAggrFuncResSlotPtr(
        Four    aggrFuncIndex                   // IN:
)
/* 
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{

    /* check input paramter */
    if (aggrFuncIndex < 0 || nAggrFuncResults <= aggrFuncIndex) {
        return NULL;
    }

    /* return pointer to the buffer slot */
    return &aggrFuncResults[aggrFuncIndex];
}

EVAL_EvalBufferSlot*    OOSQL_EvalBuffer::getFnMatchSlotPtr(
        Four    matchFuncNum                    // IN:
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{

    /* check input parameter */
    if (matchFuncNum < 0 || nFuncMatchResults <= matchFuncNum) {
        return NULL;
    }

    /* return pointer to the buffer slot */
    return &funcMatchResults[matchFuncNum];
}


OOSQL_ScanInfoTable::OOSQL_ScanInfoTable(
        Four            numElem         // IN:
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{

    /* check input parameters */
    if (numElem < 0)
        return;

    /* allocate memory */
    tableSize = (numElem > 0)? numElem: 0;
	if(tableSize > 0)
	{
		OOSQL_ARRAYNEW(scanInfos, pMemoryManager, OOSQL_ScanInfo, tableSize);
	}
	else
	{
		scanInfos = NULL;
	}
}

OOSQL_ScanInfoTable::~OOSQL_ScanInfoTable()
{
	if(scanInfos)
	{
		for(int i = 0; i < tableSize; i++)
		{
			if(scanInfos[i].boolExprs)
				pMemoryManager->Free(scanInfos[i].boolExprs);
		}

		OOSQL_ARRAYDELETE(OOSQL_ScanInfo, scanInfos);
	}
}

Four OOSQL_ScanInfoTable::prepareBoolExpression(int index, int nBools)
{
	if(scanInfos[index].boolExprs)
		pMemoryManager->Free(scanInfos[index].boolExprs);
	
	scanInfos[index].boolExprs  = (OOSQL_StorageManager::BoolExp*)pMemoryManager->Alloc(sizeof(OOSQL_StorageManager::BoolExp) * nBools);
	scanInfos[index].nBoolExprs = nBools;

	return eNOERROR;
}

Four OOSQL_ScanInfoTable::resetBoolExpression(int index)
{
	if(scanInfos[index].boolExprs)
		pMemoryManager->Free(scanInfos[index].boolExprs);
	scanInfos[index].boolExprs  = NULL;
	scanInfos[index].nBoolExprs = 0;

	return eNOERROR;
}

OOSQL_AccessElement::OOSQL_AccessElement()
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{

        /* initialize member variables */
		classId = -1;
        ocn = -1;
        osn = -1;
}


OOSQL_AccessList::OOSQL_AccessList()
/*
    Function:
        Constructor

    Side effect:
        Initialize members.

    Return value:
*/
{
	// do minimal initialization
    numClasses		= 0;
	accessList		= NULL;
    endOfCurrAccess = SM_FALSE;
    accessDirection = ACCESSDIRECTION_FORWARD;
    currAccessIndex = 0;
	writeOcn		= -1;
	writeColList	= NULL;

}

Four OOSQL_AccessList::init(
	Four            num_scans       // IN: the # of scans
)
/*
    Function:
        Constructor

    Side effect:
        Initialize members.

    Return value:
*/
{
    int     i;

    /*
     * initialze member variables
     */
    numClasses = (num_scans > 0)? num_scans: 0;

	if(numClasses > 0)
	{
		OOSQL_ARRAYNEW(accessList, pMemoryManager, OOSQL_AccessElement, numClasses);
	}
	else
		accessList = NULL;
    endOfCurrAccess = SM_FALSE;

    /* initialized accessDirection */
    accessDirection = ACCESSDIRECTION_FORWARD;
    currAccessIndex = 0;

	writeOcn = -1;
	writeColList = NULL;

	return eNOERROR;
}

OOSQL_AccessList::~OOSQL_AccessList()
{
	if(accessList)
		OOSQL_ARRAYDELETE(OOSQL_AccessElement, accessList);
}

Four    OOSQL_AccessList::isEndOfCurrAccess()
/*
    Function:
        Check if the current scan reached to the end.

    Side effect:

    Return value:
        SM_TRUE    if end of scan
        SM_FALSE   otherwise
*/
{

    if (endOfCurrAccess == SM_TRUE) 
        return SM_TRUE;
    else 
        return SM_FALSE;
}


Four    OOSQL_AccessList::isEndOfAllAccess()
/*
    Function:
        Check if all of the scans reached to the end.

    Side effect:

    Return value:
        SM_TRUE    if end of all scans
        SM_FALSE   otherwise
*/
{

    if (isEndOfCurrAccess()) {
        switch (accessDirection) {
            case ACCESSDIRECTION_FORWARD:
                if (currAccessIndex == (numClasses-1)) 
                    return SM_TRUE;
                break;

            case ACCESSDIRECTION_BACKWARD:
                if (currAccessIndex == 0) 
                    return SM_TRUE;
                break;

            default:
                OOSQL_ERR(eINVALID_CASE_OOSQL);
        }
    }

    return SM_FALSE;
}


Four    OOSQL_AccessList::setEndOfAllAccess()
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{

    endOfCurrAccess = SM_TRUE;

    switch (accessDirection) {
        case ACCESSDIRECTION_FORWARD:
            currAccessIndex = numClasses-1;
            break;

        case ACCESSDIRECTION_BACKWARD:
            currAccessIndex = 0;
            break;

        default:
            OOSQL_ERR(eINVALID_CASE_OOSQL);
    }

    return(eNOERROR);
}


Four    OOSQL_AccessList::getCurrClassID()
/*
    Function:
        Return class ID of the current class.

    Side effect:

    Return value:
        class ID
*/
{

        return accessList[currAccessIndex].classId;
}

Four    OOSQL_AccessList::getCurrOcn()
/*
    Function:
        Return open class number of the current class.

    Side effect:

    Return value:
        >= 0    open class number
*/
{

        return accessList[currAccessIndex].ocn;
}

Four    OOSQL_AccessList::getCurrScanID()
/*
    Function:
        Return scan ID of the current scan.

    Side effect:

    Return value:
        >= 0    scan ID
*/
{

        return accessList[currAccessIndex].osn;
}


Four    OOSQL_AccessList::moveToNextAccessElem()
/*
    Function:
        Prepare the next scan.

    Side effect:

    Return value:
*/
{

#ifdef  OOSQL_DEBUG       
    if (currAccessIndex < 0 || (numClasses-1) <= currAccessIndex)
        OOSQL_ERR(eINTERNAL_INCORRECTEXECSEQUENCE_OOSQL);
#endif

    /* distinguished the direction of accessing classes (for class hierarchy search) */
    switch (accessDirection) {
        case ACCESSDIRECTION_FORWARD:
            if (currAccessIndex < (numClasses-1)) {
                currAccessIndex ++;
                endOfCurrAccess = SM_FALSE;
            }
            break;

        case ACCESSDIRECTION_BACKWARD:
            if (currAccessIndex > 0) {
                currAccessIndex --;
                endOfCurrAccess = SM_FALSE;
            }
            break;

        default:
            OOSQL_ERR(eINVALID_CASE_OOSQL);
    }

    return(eNOERROR);
}


Four    OOSQL_AccessList::reverseAccessDirection()
/*
    Function:
        Close all scans and reopen them for the current scan list.

    Side effect:
        All scans previously opened are closed and then reopened.

    Return value:
        eNOERROR        if no error
        < eNOERROR      error code
*/
{

    /* re-initialize scan list control variable */
    endOfCurrAccess = SM_FALSE;

    /* reverses the direction of accessing access elements */
    switch (accessDirection) {
        case ACCESSDIRECTION_FORWARD:
            accessDirection = ACCESSDIRECTION_BACKWARD;
            break;
        case ACCESSDIRECTION_BACKWARD:
            accessDirection = ACCESSDIRECTION_FORWARD;
            break;
        default:
            OOSQL_ERR(eINVALID_CASE_OOSQL);
    }

    /* do not reinitialize currAccessIndex here because
     * the order of accesses go and return.
    currAccessIndex = 0;
    */

    return(eNOERROR);
}

TempFileSavingInfo::TempFileSavingInfo()
/*
    Function:

    Side effect:

    Return value:
*/
{

        savingPlanNo = -1;
}

OOSQL_TempFileInfoTable::OOSQL_TempFileInfoTable(
        Four            numTempFiles    // the # of temporary files
)
/*
    Function:
        Constructor.

    Side effect:
        Initialize member variables.

    Return value:
*/
{

    nTempFiles   = ((numTempFiles > 0)? numTempFiles: 0);
	tempFileInfo = (OOSQL_TempFileInfo**)pMemoryManager->Alloc(sizeof(OOSQL_TempFileInfo*) * nTempFiles);
}

OOSQL_TempFileInfoTable::~OOSQL_TempFileInfoTable()
{
	pMemoryManager->Free(tempFileInfo);
}

Four    OOSQL_TempFileInfo::isNotCreated()
/*
    Function:
        Check if temp. file for 'tempFileNum' is not yet created.

    Side effect:

    Return value:
        SM_TRUE    if temp. file is not yet created
        SM_FALSE   otherwise
*/
{

    if ( name[0] == NULL )
        return SM_TRUE;
    else
        return SM_FALSE;
}


Four    OOSQL_TempFileInfo::getOcn()
/*
    Function:
        Get open class number for 'tempFileNum'.

    Side effect:

    Return value:
        > 0             if temp. file is created and opened
        < eNOERROR      error code
*/
{

	if(sortStream)
	{
		return NIL;
	}
    else if(name[0] == NULL) 
	{
        OOSQL_ERR(eINVALID_TEMPFILENUM_OOSQL);
    }

    return ocn;
}


Four    OOSQL_TempFileInfo::getOsn()
/*
    Function:
        Get open scan number for 'tempFileNum'.

    Side effect:

    Return value:
        > 0             if temp. file is created and opened
        < eNOERROR      error code
*/
{

    if ( name[0] == NULL ) {
        OOSQL_ERR( eINVALID_TEMPFILENUM_OOSQL);
    }

    return osn;
}


OOSQL_TempFileInfo::OOSQL_TempFileInfo(
        Four            numCols,
        Four            stringBufSize,
        Boolean         isDualBuffer
)
/*
    Function:

    Side effect:

    Return value:
*/
{
    Four    strBufSizeToAlloc;


	name[0] = NULL;
	ocn = -1;
	osn = -1;
	sortStream = NULL;

	indexName[0] = NULL;

	nCols = (numCols > 0)? numCols: 0;
	isDualBuf = isDualBuffer;
	strBufSize = (stringBufSize > 0)? stringBufSize: 0;

	clist = (OOSQL_StorageManager::ColListStruct*)pMemoryManager->Alloc(sizeof(OOSQL_StorageManager::ColListStruct) * nCols);
	if (isDualBuf == SM_TRUE) 
	{
		prevColList = (OOSQL_StorageManager::ColListStruct*)pMemoryManager->Alloc(sizeof(OOSQL_StorageManager::ColListStruct) * nCols);
		strBufSizeToAlloc = (stringBufSize > 0)? (stringBufSize * 2): 0;
	}
	else
	{
		prevColList = NULL;
		strBufSizeToAlloc = (stringBufSize > 0)? stringBufSize: 0;
	}

	strBuf  = (char*)pMemoryManager->Alloc(strBufSizeToAlloc);
	attrInfo = (OOSQL_StorageManager::AttrInfo*)pMemoryManager->Alloc(sizeof(OOSQL_StorageManager::AttrInfo) * nCols);

	isTextAttrExist = NIL;
	useFastEncoding = NIL;
	firstFastEncodedNextScan = SM_TRUE;
}

OOSQL_TempFileInfo::~OOSQL_TempFileInfo()
{
	pMemoryManager->Free(clist);
	if(prevColList)
		pMemoryManager->Free(prevColList);
	pMemoryManager->Free(strBuf);
	pMemoryManager->Free(attrInfo);
}

Four    OOSQL_TempFileInfo::saveCurrTuple()
/*
    Function:
        Save the current data stored in OOSQL_StorageManager::ColListStruct by exchanging
        two pointers for OOSQL_StorageManager::ColListStruct.

    Side effect:

    Return value:
*/
{
    OOSQL_StorageManager::ColListStruct   *temp;


//    temp = clist;
//    clist = prevColList;
    *prevColList = *clist;

    return eNOERROR;
}


OOSQL_EvalIndexBuffer::OOSQL_EvalIndexBuffer()
{
}

OOSQL_EvalIndexBuffer::~OOSQL_EvalIndexBuffer()
{
}


