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
        oosql_Eval_Misc.cxx

    DESCRIPTION:

    IMPORTS:

    EXPORTS:
*/

#include "OOSQL_Evaluator.hxx"
#include "oosql_Eval_Expr.hxx"
#include <string.h>

Four OOSQL_Evaluator::copyColListStruct(
        Four                                    colType,
        OOSQL_StorageManager::ColListStruct     *src,
        OOSQL_StorageManager::ColListStruct     *dest
)
/*
    Function:

    Side effect:

    Return value:
*/
{
    // check input parameter
#ifdef  OOSQL_DEBUG
    if (src == NULL || dest == NULL) {
        OOSQL_ERR( eBADPARAMETER_OOSQL);
    }
#endif

	if(!src->nullFlag)
	{
		switch (colType) {
			case OOSQL_TYPE_short:
				dest->data.s = src->data.s;
				dest->dataLength = dest->retLength = OOSQL_TYPE_SHORT_SIZE;
				break;
				
			case OOSQL_TYPE_int:
				dest->data.i = src->data.i;
				dest->dataLength = dest->retLength = OOSQL_TYPE_INT_SIZE;
				break;
				
			case OOSQL_TYPE_long:
				dest->data.l = src->data.l;
				dest->dataLength = dest->retLength = OOSQL_TYPE_LONG_SIZE;
				break;
				
			case OOSQL_TYPE_long_long:
				dest->data.ll = src->data.ll;
				dest->dataLength = dest->retLength = OOSQL_TYPE_LONG_LONG_SIZE;
				break;
				
			case OOSQL_TYPE_float:
				dest->data.f = src->data.f;
				dest->dataLength = dest->retLength = OOSQL_TYPE_FLOAT_SIZE;
				break;
				
			case OOSQL_TYPE_double:
				dest->data.d = src->data.d;
				dest->dataLength = dest->retLength = OOSQL_TYPE_DOUBLE_SIZE;
				break;
				
			case OOSQL_TYPE_OID:
				dest->data.oid = src->data.oid;
				dest->dataLength = dest->retLength = OOSQL_TYPE_OID_SIZE;
				break;
				
			case OOSQL_TYPE_string:
			case OOSQL_TYPE_varstring:
			case OOSQL_TYPE_TEXT:       
				if(dest->data.ptr == NULL)
				{
					dest->data.ptr = src->data.ptr;
					dest->dataLength = dest->retLength = src->retLength;
				}
				else
				{
					memcpy( dest->data.ptr, src->data.ptr, (int)src->retLength );   
					dest->dataLength = dest->retLength = src->retLength;            
				}
				break;
				
			case OOSQL_TYPE_DATE:
				dest->data.date = src->data.date;
				dest->dataLength = dest->retLength = OOSQL_TYPE_DATE_SIZE;
				break;
			case OOSQL_TYPE_TIME:
				dest->data.time = src->data.time;
				dest->dataLength = dest->retLength = OOSQL_TYPE_TIME_SIZE;
				break;
			case OOSQL_TYPE_TIMESTAMP:
				dest->data.timestamp = src->data.timestamp;
				dest->dataLength = dest->retLength = OOSQL_TYPE_TIMESTAMP_SIZE;
				break;
			case OOSQL_TYPE_INTERVAL:
				dest->data.interval = src->data.interval;
				dest->dataLength = dest->retLength = OOSQL_TYPE_INTERVAL_SIZE;
				break;
				
		}
	}
	dest->length = src->length;
	dest->nullFlag = src->nullFlag;

    return eNOERROR;
}

Four    OOSQL_Evaluator::appendKeyValueToBoundCond(
        OOSQL_StorageManager::BoundCond     *boundCond,     // IN/OUT:
        OOSQL_DB_Value						*value          // IN:
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
	Two length;

    /* check input parameters */
#ifdef  OOSQL_DEBUG
    if (boundCond == NULL) 
        OOSQL_ERR(eBADPARAMETER_OOSQL);
    if (value == NULL)
        OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

    if(value->type == OOSQL_TYPE_string)
    {
        memcpy(&(boundCond->key.val[boundCond->key.len]),
				value->data.ptr, value->length);
        boundCond->key.len += (Two)value->length;
	}
	else if(value->type == OOSQL_TYPE_varstring)
	{
		length = value->length;
		memcpy(&(boundCond->key.val[boundCond->key.len]), &length, sizeof(Two));
		memcpy(&(boundCond->key.val[boundCond->key.len + sizeof(Two)]), value->data.ptr, value->length);
        boundCond->key.len += (value->length + sizeof(Two));
	}
    else
    {
        memcpy(&(boundCond->key.val[boundCond->key.len]),
               &(value->data.ptr), value->length);
	    boundCond->key.len += value->length;
    }

    /* return */
    return eNOERROR;
}


Four    OOSQL_Evaluator::copyColListStructToValue(
        Four								type,           // IN: type ID
        OOSQL_StorageManager::ColListStruct *clist,         // IN: ptr. to OOSQL_StorageManager::ColListStruct
        OOSQL_DB_Value						*value          // IN: ptr. to DB_Value
)
	/*
    Function:

    Side effect:

    Return value:
*/
{
    value->type = type;
    value->length = clist->retLength;

	
	value->nullFlag = clist->nullFlag;

	if(!clist->nullFlag)
	{

    switch (type) {
        case OOSQL_TYPE_short:
            value->data.s = clist->data.s;
            break;

        case OOSQL_TYPE_int:
            value->data.i = clist->data.i;
            break;

        case OOSQL_TYPE_long:
            value->data.l = clist->data.l;
            break;
		
        case OOSQL_TYPE_long_long:
            value->data.ll = clist->data.ll;
            break;

        case OOSQL_TYPE_float:
            value->data.f = clist->data.f;
            break;

        case OOSQL_TYPE_double:
            value->data.d = clist->data.d;
            break;

        case OOSQL_TYPE_OID:
            value->data.oid = clist->data.oid;
            break;

        case OOSQL_TYPE_string:
        case OOSQL_TYPE_varstring:
        case OOSQL_TYPE_GEOMETRY:
        case OOSQL_TYPE_POINT:
        case OOSQL_TYPE_LINESTRING:
        case OOSQL_TYPE_POLYGON:
        case OOSQL_TYPE_GEOMETRYCOLLECTION:
        case OOSQL_TYPE_MULTIPOINT:
        case OOSQL_TYPE_MULTILINESTRING:
        case OOSQL_TYPE_MULTIPOLYGON:
			if(clist->nullFlag == SM_TRUE)
			{
				value->nullFlag = SM_TRUE;
				value->length   = 0;
			}
			else
			{
				value->length = clist->retLength;
				value->PrepareData(value->length + 1);
				memcpy(value->data.ptr, clist->data.ptr, value->length);
				((char*)value->data.ptr)[value->length] = 0;	// ensure null terminated string
			}
            break;

        case OOSQL_TYPE_DATE:
			value->data.date = clist->data.date;
			break;

        case OOSQL_TYPE_TIME:
			value->data.time = clist->data.time;
			break;

        case OOSQL_TYPE_TIMESTAMP:
			value->data.timestamp = clist->data.timestamp;
			break;

        case OOSQL_TYPE_INTERVAL:
			value->data.interval = clist->data.interval;
			break;
    }
	} // end of if(!clist->nullFlag)
    return eNOERROR;
}


Four    OOSQL_Evaluator::copyDB_ValueToColListStruct(
        Four								type,           // IN: type ID
        OOSQL_DB_Value						*value,         // IN: ptr. to DB_Value
        OOSQL_StorageManager::ColListStruct *clist          // IN: ptr. to OOSQL_StorageManager::ColListStruct
)
/*
    Function:

    Side effect:

    Return value:
*/
{
	clist->nullFlag = value->nullFlag;
	
	if(!value->nullFlag)
	{ 

    switch (type) {
        case OOSQL_TYPE_short:
            clist->data.s = value->data.s;
            break;
        case OOSQL_TYPE_int:
            clist->data.i = value->data.i;
            break;
        case OOSQL_TYPE_long:
            clist->data.l = value->data.l;
            break;
        case OOSQL_TYPE_long_long:
            clist->data.ll = value->data.ll;
            break;
        case OOSQL_TYPE_float:
            	clist->data.f = value->data.f;
            break;
        case OOSQL_TYPE_double:
            clist->data.d = value->data.d;
            break;
        case OOSQL_TYPE_OID:
            clist->data.oid = value->data.oid;
            break;

        case OOSQL_TYPE_string:
        case OOSQL_TYPE_varstring:
        case OOSQL_TYPE_GEOMETRY:
        case OOSQL_TYPE_POINT:
        case OOSQL_TYPE_LINESTRING:
        case OOSQL_TYPE_POLYGON:
        case OOSQL_TYPE_GEOMETRYCOLLECTION:
        case OOSQL_TYPE_MULTIPOINT:
        case OOSQL_TYPE_MULTILINESTRING:
        case OOSQL_TYPE_MULTIPOLYGON:
            clist->length   = value->length;
            clist->data.ptr = value->data.ptr;
            break;
            
        case OOSQL_TYPE_DATE:
			clist->data.date = value->data.date;
			break;

        case OOSQL_TYPE_TIME:
			clist->data.time = value->data.time;
			break;

        case OOSQL_TYPE_TIMESTAMP:
			clist->data.timestamp = value->data.timestamp;
			break;

        case OOSQL_TYPE_INTERVAL:
			clist->data.interval = value->data.interval;
			break;
    }

	//clist->nullFlag = SM_FALSE;
	} // end of if(value->nullFlag)

    return eNOERROR;
}


Four    OOSQL_Evaluator::copyColListStructToCharBuf( 
        Four            colType,        // IN: column type
        Four            colLength,      // IN: column length
        OOSQL_StorageManager::ColListStruct   *clist,         // IN: source
        char            *charBuf        // IN/OUT: destination
)
/*
    Function:
        Copy OOSQL_StorageManager::ColListStruct to char buffer.

    Side effect:

    Return value:
        max. length defined in the system catalog       for fixed sized column
        length of the actually read-in data             for variable sized column
*/
{
    switch (colType) {
        case OOSQL_TYPE_short:
            memcpy( charBuf, &(clist->data.s), (int)colLength );
            break;

        case OOSQL_TYPE_int:
            memcpy( charBuf, &(clist->data.i), (int)colLength );
            break;

        case OOSQL_TYPE_long:
            memcpy( charBuf, &(clist->data.l), (int)colLength );
            break;

        case OOSQL_TYPE_long_long:
            memcpy( charBuf, &(clist->data.ll), (int)colLength );
            break;

        case OOSQL_TYPE_float:
            memcpy( charBuf, &(clist->data.f), (int)colLength );
            break;

        case OOSQL_TYPE_double:
            memcpy( charBuf, &(clist->data.d), (int)colLength );
            break;

        case OOSQL_TYPE_string:
            memcpy( charBuf, clist->data.ptr, (int)colLength );
            break;

        case OOSQL_TYPE_varstring:
        case OOSQL_TYPE_GEOMETRY:
        case OOSQL_TYPE_POINT:
        case OOSQL_TYPE_LINESTRING:
        case OOSQL_TYPE_POLYGON:
        case OOSQL_TYPE_GEOMETRYCOLLECTION:
        case OOSQL_TYPE_MULTIPOINT:
        case OOSQL_TYPE_MULTILINESTRING:
        case OOSQL_TYPE_MULTIPOLYGON:
            memcpy( charBuf, clist->data.ptr, (int)clist->retLength );
            break;

        case OOSQL_TYPE_OID:
            memcpy( charBuf, &(clist->data.oid), (int)colLength );
            break;
            
        case OOSQL_TYPE_DATE:
          memcpy(charBuf, &(clist->data.date), (int)colLength);
          break;
        case OOSQL_TYPE_TIME:
          memcpy(charBuf, &(clist->data.time), (int)colLength);
          break;
        case OOSQL_TYPE_TIMESTAMP:
          memcpy(charBuf, &(clist->data.timestamp), (int)colLength);
          break;
        case OOSQL_TYPE_INTERVAL:
          memcpy(charBuf, &(clist->data.interval), (int)colLength);
          break;
        default:
            OOSQL_ERR(eINVALID_CASE_OOSQL);
    }

    // return actually copied data length
    switch(colType) {
        case OOSQL_TYPE_GEOMETRY:
        case OOSQL_TYPE_POINT:
        case OOSQL_TYPE_LINESTRING:
        case OOSQL_TYPE_POLYGON:
        case OOSQL_TYPE_GEOMETRYCOLLECTION:
        case OOSQL_TYPE_MULTIPOINT:
        case OOSQL_TYPE_MULTILINESTRING:
        case OOSQL_TYPE_MULTIPOLYGON:
        case OOSQL_TYPE_varstring:
        return clist->retLength;
            break;
	default:
        return colLength;
    }
}


Four    OOSQL_Evaluator::copyStringValueToString(
        StringPoolIndex *strIdx,
        char            *dest,
        Four            length
)
/*
    Function:

    Side effect:

    Return value:
*/
{
    StringPoolElements      src;
    Four					i;

    src = ACCESSPLAN.getStringPool( *strIdx );

    for (i = 0; i < length; i++) {
        dest[i] = src[i];
    }

    return eNOERROR;
}


Four    OOSQL_Evaluator::getArgumentInfo(
        AP_ArgumentPoolIndex    *arg,           // IN:
        Four                    *argType,       // OUT:
        Four                    *argLength      // OUT:
)
/*
    Function:

    Side effect:

    Return value:
*/
{
    AP_ArgumentElement      *argElem;
    Four            e;

    argElem = ACCESSPLAN.getArgumentElem( *arg );
    switch (argElem->argumentKind) {
        case ARGUMENT_KIND_PATHEXPR:
            e = getColInfo( &(argElem->pathExpr), argType, argLength );
            if (e < eNOERROR)
                OOSQL_ERR(e);
            break;

        case ARGUMENT_KIND_TEMPFILECOL:
            e = getColInfo( &(argElem->tempFileCol), argType, argLength );
            if (e < eNOERROR)
                OOSQL_ERR(e);
            break;

        case ARGUMENT_KIND_VALUE:
            OOSQL_ERR(eNOTIMPLEMENTED_ARGUMENT_OOSQL);
        case ARGUMENT_KIND_FUNC:
            OOSQL_ERR(eNOTIMPLEMENTED_ARGUMENT_OOSQL);
        case ARGUMENT_KIND_DOMAIN:
            OOSQL_ERR(eNOTIMPLEMENTED_ARGUMENT_OOSQL);
        case ARGUMENT_KIND_AGGRFUNC:
            OOSQL_ERR(eNOTIMPLEMENTED_ARGUMENT_OOSQL);
        default:
            OOSQL_ERR(eINVALID_CASE_OOSQL);
    }

    return eNOERROR;
}


Four    OOSQL_Evaluator::getColInfo( 
        AP_PathExprAccessInfo   *pathExpr, 
        Four                    *colType,       
        Four                    *colLength
)
/*
    Function:

    Side effect:

    Return value:
*/
{
    AP_UsedColElement       *colInfo;

    if (pathExpr->kind == PATHEXPR_KIND_METHOD) {
            OOSQL_ERR( eNOTIMPLEMENTED_METHOD_OOSQL);
    }

    colInfo = ACCESSPLAN.getUsedColElem( pathExpr->col.planNo, pathExpr->col.colNo );

    *colType = colInfo->typeId;
    *colLength = colInfo->length;

    return eNOERROR;
}


Four    OOSQL_Evaluator::getColInfo( 
        AP_TempFileAccessInfo   *tempFileCol, 
        Four                    *colType,       
        Four                    *colLength
)
/*
    Function:

    Side effect:

    Return value:
*/
{
    OOSQL_TempFileInfo      *tempFileInfo;

    // check input parameter
#ifdef  OOSQL_DEBUG
    if (tempFileCol == NULL || colType == NULL || colLength == NULL) {
            OOSQL_ERR( eBADPARAMETER_OOSQL);
    }

    // check input parameter
    if (tempFileCol->tempFileNum < 0 || EVAL_TEMPFILEINFOTABLE.nTempFiles <= tempFileCol->tempFileNum) {
            OOSQL_ERR( eBADPARAMETER_OOSQL);
    }
#endif

    tempFileInfo = EVAL_TEMPFILEINFOTABLEELEMENTS[tempFileCol->tempFileNum];

    // check input parameter
#ifdef  OOSQL_DEBUG
    if (tempFileCol->colNo < 0 || tempFileInfo->nCols <= tempFileCol->colNo) {
            OOSQL_ERR( eBADPARAMETER_OOSQL);
    }
#endif

    *colType = tempFileInfo->attrInfo[tempFileCol->colNo].type;
    *colLength = tempFileInfo->attrInfo[tempFileCol->colNo].length;

    return eNOERROR;
}


Four    OOSQL_Evaluator::copyArgumentToColListStruct(
    AP_ArgumentPoolIndex *arg,          /* IN: */
    OOSQL_StorageManager::ColListStruct *clist                /* INOUT: */
)
/*
    Function:

    Side effect:

    Return value:
*/
{
    AP_ArgumentElement *argElem;
    Four argType;
    Four argLength;
    Four argPlanNo;
    Four argTempFileNum;
    Four argColNo;
    Four e;

    argElem = ACCESSPLAN.getArgumentElem( *arg );
    switch (argElem->argumentKind) {
        case ARGUMENT_KIND_PATHEXPR:
            e = getColInfo( &(argElem->pathExpr), &argType, &argLength );
            if (e < eNOERROR) 
                OOSQL_ERR(e);

            switch (argElem->pathExpr.kind) {
                case PATHEXPR_KIND_ATTR:
                    argPlanNo = argElem->pathExpr.col.planNo;
                    argColNo = ACCESSPLAN.getMappedColNo( argPlanNo, argElem->pathExpr.col.colNo );
                    e = copyColListStruct( argType, EVAL_EVALBUFFER[argPlanNo].getColSlotPtr(argColNo), clist );
                    if (e < eNOERROR) 
                        OOSQL_ERR(e);
					clist->retLength = argLength; 
                    break;

                case PATHEXPR_KIND_OBJECT:
                    OOSQL_ERR( eNOTIMPLEMENTED_OBJECTPROJ_OOSQL);

                case PATHEXPR_KIND_METHOD:
                    OOSQL_ERR( eNOTIMPLEMENTED_METHOD_OOSQL);
            }
            break;

        case ARGUMENT_KIND_TEMPFILECOL:
            /* get the temporary file number and column number */
            argTempFileNum = argElem->tempFileCol.tempFileNum;
            argColNo = argElem->tempFileCol.colNo;

            /* get the type of the argument value */
            argType = EVAL_TEMPFILEINFOTABLEELEMENTS[argTempFileNum]->attrInfo[argColNo].type;

            /* copy */
            e = copyColListStruct( argType, &(EVAL_TEMPFILEINFOTABLEELEMENTS[argTempFileNum]->clist[argColNo]), clist );
            if (e < eNOERROR)
                OOSQL_ERR(e);
            break;

        case ARGUMENT_KIND_VALUE:
            OOSQL_ERR( eNOTIMPLEMENTED_ARGUMENT_OOSQL);

        case ARGUMENT_KIND_FUNC:
            OOSQL_ERR( eNOTIMPLEMENTED_ARGUMENT_OOSQL);

        case ARGUMENT_KIND_DOMAIN:
            OOSQL_ERR( eNOTIMPLEMENTED_ARGUMENT_OOSQL);

        case ARGUMENT_KIND_AGGRFUNC:
            OOSQL_ERR( eNOTIMPLEMENTED_ARGUMENT_OOSQL);

        case ARGUMENT_KIND_EXPR:
            OOSQL_ERR( eNOTIMPLEMENTED_ARGUMENT_OOSQL);

        case ARGUMENT_KIND_AGGRFUNCRESULT:
            OOSQL_ERR( eNOTIMPLEMENTED_ARGUMENT_OOSQL);

        case ARGUMENT_KIND_FUNCRESULT:
            OOSQL_ERR( eNOTIMPLEMENTED_ARGUMENT_OOSQL);

        case ARGUMENT_KIND_FUNCEVAL:
            OOSQL_ERR( eNOTIMPLEMENTED_ARGUMENT_OOSQL);

        case ARGUMENT_KIND_TEXTIR_SUBPLAN:
            OOSQL_ERR( eNOTIMPLEMENTED_ARGUMENT_OOSQL);

        default:
            OOSQL_ERR(eINVALID_CASE_OOSQL);
    }

    return(eNOERROR);
}


Four    OOSQL_Evaluator::compareArgumentWithColListStruct(
    AP_ArgumentPoolIndex *arg,          /* IN: */
    OOSQL_StorageManager::ColListStruct *clist                /* INOUT: */
)
/*
    Function:

    Side effect:

    Return value:
*/
{
    AP_ArgumentElement *argElem;
    Four argType;
    Four argLength;
    Four argPlanNo;
    Four argTempFileNum;
    Four argColNo;
    Four cmpResult;
    Four e;

    argElem = ACCESSPLAN.getArgumentElem( *arg );
    switch (argElem->argumentKind) {
        case ARGUMENT_KIND_PATHEXPR:
            e = getColInfo( &(argElem->pathExpr), &argType, &argLength );
            if (e < eNOERROR) 
                OOSQL_ERR(e);

            switch (argElem->pathExpr.kind) {
                case PATHEXPR_KIND_ATTR:
                    argPlanNo = argElem->pathExpr.col.planNo;
                    argColNo = ACCESSPLAN.getMappedColNo( argPlanNo, argElem->pathExpr.col.colNo );
                    cmpResult = compareColListStruct( argType, EVAL_EVALBUFFER[argPlanNo].getColSlotPtr(argColNo), clist );
                    if (cmpResult < eNOERROR) 
                        OOSQL_ERR(cmpResult);
                    break;

                case PATHEXPR_KIND_OBJECT:
                    OOSQL_ERR( eNOTIMPLEMENTED_OBJECTPROJ_OOSQL);

                case PATHEXPR_KIND_METHOD:
                    OOSQL_ERR( eNOTIMPLEMENTED_METHOD_OOSQL);
            }
            break;

        case ARGUMENT_KIND_TEMPFILECOL:
            /* get temporary file number and column number */
            argTempFileNum = argElem->tempFileCol.tempFileNum;
            argColNo = argElem->tempFileCol.colNo;

            /* get the type of the argument value */
            argType = EVAL_TEMPFILEINFOTABLEELEMENTS[argTempFileNum]->attrInfo[argColNo].type;

            /* compare */
            cmpResult = compareColListStruct( argType, &(EVAL_TEMPFILEINFOTABLEELEMENTS[argTempFileNum]->clist[argColNo]), clist );
            if (cmpResult < eNOERROR)
                OOSQL_ERR(cmpResult);
            break;

        case ARGUMENT_KIND_VALUE:
            OOSQL_ERR(eNOTIMPLEMENTED_ARGUMENT_OOSQL);

        case ARGUMENT_KIND_FUNC:
            OOSQL_ERR(eNOTIMPLEMENTED_ARGUMENT_OOSQL);

        case ARGUMENT_KIND_DOMAIN:
            OOSQL_ERR(eNOTIMPLEMENTED_ARGUMENT_OOSQL);

        case ARGUMENT_KIND_AGGRFUNC:
            OOSQL_ERR(eNOTIMPLEMENTED_ARGUMENT_OOSQL);

        case ARGUMENT_KIND_EXPR:
            OOSQL_ERR(eNOTIMPLEMENTED_ARGUMENT_OOSQL);

        case ARGUMENT_KIND_AGGRFUNCRESULT:
            OOSQL_ERR(eNOTIMPLEMENTED_ARGUMENT_OOSQL);

        case ARGUMENT_KIND_FUNCRESULT:
            OOSQL_ERR(eNOTIMPLEMENTED_ARGUMENT_OOSQL);

        case ARGUMENT_KIND_FUNCEVAL:
            OOSQL_ERR(eNOTIMPLEMENTED_ARGUMENT_OOSQL);

        case ARGUMENT_KIND_TEXTIR_SUBPLAN:
            OOSQL_ERR(eNOTIMPLEMENTED_ARGUMENT_OOSQL);

        default:
            OOSQL_ERR(eINVALID_CASE_OOSQL);
    }

    /* return comparison result */
    return(cmpResult);
}


Four    OOSQL_Evaluator::accumulateArgumentToColListStruct(
    AP_ArgumentPoolIndex *arg,          /* IN: */
    Four resultType,                    /* IN: */
    OOSQL_StorageManager::ColListStruct *clist                /* INOUT: */
)
/*
    Function:

    Side effect:

    Return value:
*/
{
    AP_ArgumentElement *argElem;
    Four argType;
    Four argLength;
    Four argPlanNo;
    Four argTempFileNum;
    Four argColNo;
    Four e;

    argElem = ACCESSPLAN.getArgumentElem(*arg);
    switch (argElem->argumentKind) {
        case ARGUMENT_KIND_PATHEXPR:
            e = getColInfo( &(argElem->pathExpr), &argType, &argLength );
            if (e < eNOERROR)
                OOSQL_ERR(e);

            switch (argElem->pathExpr.kind) {
                case PATHEXPR_KIND_ATTR:
                    argPlanNo = argElem->pathExpr.col.planNo;
                    argColNo = ACCESSPLAN.getMappedColNo( argPlanNo, argElem->pathExpr.col.colNo );
                    e = accumulateColListStruct( argType, EVAL_EVALBUFFER[argPlanNo].getColSlotPtr(argColNo), 
                            resultType, clist );
                    if (e < eNOERROR) 
                        OOSQL_ERR(e);
                    break;

                case PATHEXPR_KIND_OBJECT:
                    OOSQL_ERR( eNOTIMPLEMENTED_OBJECTPROJ_OOSQL);

                case PATHEXPR_KIND_METHOD:
                    OOSQL_ERR( eNOTIMPLEMENTED_METHOD_OOSQL);
            }
            break;

        case ARGUMENT_KIND_TEMPFILECOL:
            /* get temporary file number and column number */
            argTempFileNum = argElem->tempFileCol.tempFileNum;
            argColNo = argElem->tempFileCol.colNo;

            /* get the type of the argument value */
            argType = EVAL_TEMPFILEINFOTABLEELEMENTS[argTempFileNum]->attrInfo[argColNo].type;

            /* compare */
            e = accumulateColListStruct( argType, &(EVAL_TEMPFILEINFOTABLEELEMENTS[argTempFileNum]->clist[argColNo]),
                        resultType, clist );
            if (e < eNOERROR)
                OOSQL_ERR(e);
            break;

        case ARGUMENT_KIND_VALUE:
            OOSQL_ERR(eNOTIMPLEMENTED_ARGUMENT_OOSQL);

        case ARGUMENT_KIND_FUNC:
            OOSQL_ERR(eNOTIMPLEMENTED_ARGUMENT_OOSQL);

        case ARGUMENT_KIND_DOMAIN:
            OOSQL_ERR(eNOTIMPLEMENTED_ARGUMENT_OOSQL);

        case ARGUMENT_KIND_AGGRFUNC:
            OOSQL_ERR(eNOTIMPLEMENTED_ARGUMENT_OOSQL);

        case ARGUMENT_KIND_EXPR:
            OOSQL_ERR(eNOTIMPLEMENTED_ARGUMENT_OOSQL);

        case ARGUMENT_KIND_AGGRFUNCRESULT:
            OOSQL_ERR(eNOTIMPLEMENTED_ARGUMENT_OOSQL);

        case ARGUMENT_KIND_FUNCRESULT:
            OOSQL_ERR(eNOTIMPLEMENTED_ARGUMENT_OOSQL);

        case ARGUMENT_KIND_FUNCEVAL:
            OOSQL_ERR(eNOTIMPLEMENTED_ARGUMENT_OOSQL);

        case ARGUMENT_KIND_TEXTIR_SUBPLAN:
            OOSQL_ERR(eNOTIMPLEMENTED_ARGUMENT_OOSQL);

        default:
            OOSQL_ERR(eINVALID_CASE_OOSQL);
    }

    return(eNOERROR);
}


Four    OOSQL_Evaluator::isNullArgumentValue(
		AP_ArgumentPoolIndex *arg,          /* IN: */
		Boolean              *flag          /* OUT: */
		)           
/*
   Function:
   
   Side effect:
   
   Return value:
*/
{
	AP_ArgumentElement *argElem;
	Four argType;
	Four argLength;
	Four argPlanNo;
	Four argTempFileNum;
	Four argColNo;
	Four e;
	
	argElem = ACCESSPLAN.getArgumentElem(*arg);
	switch (argElem->argumentKind) {
		case ARGUMENT_KIND_PATHEXPR:
			switch (argElem->pathExpr.kind) {
				case PATHEXPR_KIND_ATTR:
					argPlanNo = argElem->pathExpr.col.planNo;
					argColNo = ACCESSPLAN.getMappedColNo( argPlanNo, argElem->pathExpr.col.colNo );
					
					if ((EVAL_EVALBUFFER[argPlanNo].getColSlotPtr(argColNo))->nullFlag)
						*flag = SM_TRUE;
					else
						*flag = SM_FALSE;
					
					break;
					
				case PATHEXPR_KIND_OBJECT:
					OOSQL_ERR( eNOTIMPLEMENTED_OBJECTPROJ_OOSQL);
					
				case PATHEXPR_KIND_METHOD:
					OOSQL_ERR( eNOTIMPLEMENTED_METHOD_OOSQL);
					
				default:
					OOSQL_ERR( eINVALID_CASE_OOSQL);
			}
			break;
			
		case ARGUMENT_KIND_TEMPFILECOL:
			/* get temporary file number and column number */
			argTempFileNum = argElem->tempFileCol.tempFileNum;
			argColNo = argElem->tempFileCol.colNo;
			
			if ((EVAL_TEMPFILEINFOTABLEELEMENTS[argTempFileNum]->clist[argColNo]).nullFlag)
				*flag = SM_TRUE;
			else
				*flag = SM_FALSE;
			
			break;
		
		case ARGUMENT_KIND_VALUE:
			OOSQL_ERR(eNOTIMPLEMENTED_ARGUMENT_OOSQL);
			
		case ARGUMENT_KIND_FUNC:
			OOSQL_ERR(eNOTIMPLEMENTED_ARGUMENT_OOSQL);
			
		case ARGUMENT_KIND_DOMAIN:
			OOSQL_ERR(eNOTIMPLEMENTED_ARGUMENT_OOSQL);
			
		case ARGUMENT_KIND_AGGRFUNC:
			OOSQL_ERR(eNOTIMPLEMENTED_ARGUMENT_OOSQL);
			
		case ARGUMENT_KIND_EXPR:
			OOSQL_ERR(eNOTIMPLEMENTED_ARGUMENT_OOSQL);
			
		case ARGUMENT_KIND_AGGRFUNCRESULT:
			OOSQL_ERR(eNOTIMPLEMENTED_ARGUMENT_OOSQL);
			
		case ARGUMENT_KIND_FUNCRESULT:
			OOSQL_ERR(eNOTIMPLEMENTED_ARGUMENT_OOSQL);
			
		case ARGUMENT_KIND_FUNCEVAL:
			OOSQL_ERR(eNOTIMPLEMENTED_ARGUMENT_OOSQL);
			
		case ARGUMENT_KIND_TEXTIR_SUBPLAN:
			OOSQL_ERR(eNOTIMPLEMENTED_ARGUMENT_OOSQL);
			
		default:
			OOSQL_ERR(eINVALID_CASE_OOSQL);
	}
	
	return eNOERROR;
}

Four    OOSQL_Evaluator::accumulateColListStruct(
        Four            colType,
        OOSQL_StorageManager::ColListStruct   *op,
        Four            resultType,
        OOSQL_StorageManager::ColListStruct   *result
)
/*
    Function:

    Side effect:

    Return value:
*/
{
    // check input parameter
#ifdef  OOSQL_DEBUG
    if (op == NULL || result == NULL) {
        OOSQL_ERR( eBADPARAMETER_OOSQL);
    }
#endif

    if(op->nullFlag)
		return eNOERROR;

    switch (resultType) {
        case OOSQL_TYPE_long:
            switch (colType) {
                case OOSQL_TYPE_short:
                    result->data.l = (Four_Invariable)op->data.s + result->data.l;
                    break;

                case OOSQL_TYPE_int:
                    result->data.l = (Four_Invariable)op->data.i + result->data.l;
                    break;

                case OOSQL_TYPE_long:
                    result->data.l = op->data.l + result->data.l;
                    break;

                case OOSQL_TYPE_long_long:
                    result->data.ll = op->data.ll + result->data.ll;
                    break;

                default:
                    OOSQL_ERR( eINVALID_OPERAND_OOSQL);
                    break;
            }
            break;

        case OOSQL_TYPE_long_long:
            switch (colType) {
                case OOSQL_TYPE_short:
                    result->data.ll = (Eight_Invariable)op->data.s + result->data.ll;
                    break;

                case OOSQL_TYPE_int:
                    result->data.ll = (Eight_Invariable)op->data.i + result->data.ll;
                    break;

                case OOSQL_TYPE_long:
                    result->data.ll = (Eight_Invariable)op->data.l + result->data.ll;
                    break;

                case OOSQL_TYPE_long_long:
                    result->data.ll = op->data.ll + result->data.ll;
                    break;

                default:
                    OOSQL_ERR( eINVALID_OPERAND_OOSQL);
                    break;
            }
            break;

        case OOSQL_TYPE_double:
            switch (colType) {
                case OOSQL_TYPE_short:
                    result->data.d = (double)op->data.s + result->data.d;
                    break;

                case OOSQL_TYPE_int:
                    result->data.d = (double)op->data.i + result->data.d;
                    break;

                case OOSQL_TYPE_long:
                    result->data.d = (double)op->data.l + result->data.d;
                    break;

                case OOSQL_TYPE_long_long:
                    result->data.d = (double)op->data.ll + result->data.d;
                    break;

                case OOSQL_TYPE_float:
                    result->data.d = (double)op->data.f + result->data.d;
                    break;

                case OOSQL_TYPE_double:
                    result->data.d = op->data.d + result->data.d;
                    break;

                default:
                    OOSQL_ERR( eINVALID_OPERAND_OOSQL);
                    break;
            }
            break;

        default:
            OOSQL_ERR( eINVALID_OPERAND_OOSQL);
    }

    return eNOERROR;
}

Four OOSQL_Evaluator::makeStringFromStringPool(char* string, StringPoolIndex stringPoolIndex)
{
	StringPoolElements	stringElements(m_pool->stringPool);
	Four				i;

	stringElements = stringPoolIndex;
	for(i = 0; i < stringElements.size; i++)
	{
		string[i] = stringElements[i];
	}

	string[i] = '\0';

	return eNOERROR;
}
