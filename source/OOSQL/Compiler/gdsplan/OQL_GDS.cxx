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

#include "OQL_GDS.hxx"

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_GDS::OQL_GDS()
{
	queryType = NO_QUERY;
	m_unusedConditionList.setNull();
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_GDS::~OQL_GDS()
{}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eUNHANDLED_CASE_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_GDS::init(OQL_GDSPOOL *pool, QueryType aqueryType)
{
	SelectQueryPoolElements	selectQueryElements(pool->selectQueryPool);
	UpdateQueryPoolElements	updateQueryElements(pool->updateQueryPool);
	InsertQueryPoolElements	insertQueryElements(pool->insertQueryPool);
	DeleteQueryPoolElements	deleteQueryElements(pool->deleteQueryPool);

	queryType = aqueryType;
	m_unusedConditionList.setNull();

	switch(queryType)
	{
	case OQL_GDS::SELECT_QUERY:
		selectQueryElements = selectQuery  = pool->selectQueryPool.addNewEntry();
		selectQueryElements[0].selList.setNull();
		selectQueryElements[0].selListType = 0;
		selectQueryElements[0].targetList.setNull();
		selectQueryElements[0].whereCond.setNull();
		selectQueryElements[0].groupByList.setNull();
		selectQueryElements[0].havingCond.setNull();
		selectQueryElements[0].orderByList.setNull();
		selectQueryElements[0].queryGraph.setNull();
		break;
	case OQL_GDS::UPDATE_QUERY:
		updateQueryElements = updateQuery      = pool->updateQueryPool.addNewEntry();
		updateQueryElements[0].targetList.setNull();
		updateQueryElements[0].updateValueList.setNull();
		updateQueryElements[0].whereCond.setNull();
		updateQueryElements[0].queryGraph.setNull();
		break;
	case OQL_GDS::INSERT_QUERY:
		insertQueryElements = insertQuery      = pool->insertQueryPool.addNewEntry();
		insertQueryElements[0].selList.setNull();
		insertQueryElements[0].selListType     = 0;
		insertQueryElements[0].targetList.setNull();
		insertQueryElements[0].whereCond.setNull();
		insertQueryElements[0].groupByList.setNull();
		insertQueryElements[0].havingCond.setNull();
		insertQueryElements[0].orderByList.setNull();
		insertQueryElements[0].queryGraph.setNull();
		insertQueryElements[0].insertValueList.setNull();
		break;
	case OQL_GDS::DELETE_QUERY:
		deleteQueryElements = deleteQuery = pool->deleteQueryPool.addNewEntry();
		deleteQueryElements[0].targetList.setNull();
		deleteQueryElements[0].whereCond.setNull();
		deleteQueryElements[0].queryGraph.setNull();
		deleteQueryElements[0].isDeferredDelete = SM_FALSE;
		break;
	case OQL_GDS::NO_QUERY:
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
Four OQL_GDS::setTargetList(OQL_GDSPOOL *pool, TargetListPoolIndex atargetList)
{
	SelectQueryPoolElements	selectQueryElements(pool->selectQueryPool);
	UpdateQueryPoolElements	updateQueryElements(pool->updateQueryPool);
	InsertQueryPoolElements	insertQueryElements(pool->insertQueryPool);
	DeleteQueryPoolElements	deleteQueryElements(pool->deleteQueryPool);

	switch(queryType)
	{
	case OQL_GDS::SELECT_QUERY:
		selectQueryElements = selectQuery;
		selectQueryElements[0].targetList = atargetList; break;
	case OQL_GDS::UPDATE_QUERY:
		updateQueryElements = updateQuery;
		updateQueryElements[0].targetList = atargetList; break;
	case OQL_GDS::INSERT_QUERY:
		insertQueryElements = insertQuery;
		insertQueryElements[0].targetList = atargetList; break;
	case OQL_GDS::DELETE_QUERY:
		deleteQueryElements = deleteQuery;
		deleteQueryElements[0].targetList = atargetList; break;
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
Four OQL_GDS::getTargetList(OQL_GDSPOOL *pool, TargetListPoolIndex& atargetList)
{
	SelectQueryPoolElements	selectQueryElements(pool->selectQueryPool);
	UpdateQueryPoolElements	updateQueryElements(pool->updateQueryPool);
	InsertQueryPoolElements	insertQueryElements(pool->insertQueryPool);
	DeleteQueryPoolElements	deleteQueryElements(pool->deleteQueryPool);

	switch(queryType)
	{
	case OQL_GDS::SELECT_QUERY:
		selectQueryElements = selectQuery;
		atargetList  = selectQueryElements[0].targetList; break;
	case OQL_GDS::UPDATE_QUERY:
		updateQueryElements = updateQuery;
		atargetList  = updateQueryElements[0].targetList; break;
	case OQL_GDS::INSERT_QUERY:
		insertQueryElements = insertQuery;
		atargetList  = insertQueryElements[0].targetList; break;
	case OQL_GDS::DELETE_QUERY:
		deleteQueryElements = deleteQuery;
		atargetList  = deleteQueryElements[0].targetList; break;
	default:
		OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
	}

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_GDS::getTargetList(OQL_GDSPOOL *pool, TargetListPoolElements& atargetList)
{
	TargetListPoolIndex targetListPoolIndex;
	Four                e;

	e = getTargetList(pool, targetListPoolIndex); 
	OOSQL_CHECK_ERR(e);

	atargetList.setPool(pool->targetListPool);
	atargetList = targetListPoolIndex;

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eUNHANDLED_CASE_OOSQL
< eNOERROR         

IMPLEMENTATION:
****************************************************************************/
Four OQL_GDS::setSelList(OQL_GDSPOOL *pool, SelListPoolIndex aselList)
{
	SelectQueryPoolElements	selectQueryElements(pool->selectQueryPool);
	InsertQueryPoolElements	insertQueryElements(pool->insertQueryPool);	

	switch(queryType)
	{
	case OQL_GDS::SELECT_QUERY:
		selectQueryElements = selectQuery;
		selectQueryElements[0].selList = aselList; break;
	case OQL_GDS::INSERT_QUERY:
		insertQueryElements = insertQuery;
		insertQueryElements[0].selList = aselList; break;
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
Four OQL_GDS::getSelList(OQL_GDSPOOL *pool, SelListPoolIndex& aselList)
{
	SelectQueryPoolElements	selectQueryElements(pool->selectQueryPool);
	InsertQueryPoolElements	insertQueryElements(pool->insertQueryPool);	

	switch(queryType)
	{
	case OQL_GDS::SELECT_QUERY:
		selectQueryElements = selectQuery;
		aselList = selectQueryElements[0].selList; break;
	case OQL_GDS::INSERT_QUERY:
		insertQueryElements = insertQuery;
		aselList = insertQueryElements[0].selList; break;
	default:
		OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
	}

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_GDS::getSelList(OQL_GDSPOOL *pool, SelListPoolElements& aselList)
{
	SelListPoolIndex selListPoolIndex;
	Four             e;

	e = getSelList(pool, selListPoolIndex); 
	OOSQL_CHECK_ERR(e);

	aselList.setPool(pool->selListPool);
	aselList = selListPoolIndex;

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eUNHANDLED_CASE_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_GDS::setSelListType(OQL_GDSPOOL *pool, Four aselListType)
{
	SelectQueryPoolElements	selectQueryElements(pool->selectQueryPool);
	InsertQueryPoolElements	insertQueryElements(pool->insertQueryPool);	

	switch(queryType)
	{
	case OQL_GDS::SELECT_QUERY:
		selectQueryElements = selectQuery;
		selectQueryElements[0].selListType = aselListType; break;
	case OQL_GDS::INSERT_QUERY:
		insertQueryElements = insertQuery;
		insertQueryElements[0].selListType = aselListType; break;
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
Four OQL_GDS::getSelListType(OQL_GDSPOOL *pool, Four& aselListType)
{
	SelectQueryPoolElements	selectQueryElements(pool->selectQueryPool);
	InsertQueryPoolElements	insertQueryElements(pool->insertQueryPool);	

	switch(queryType)
	{
	case OQL_GDS::SELECT_QUERY:
		selectQueryElements = selectQuery;
		aselListType = selectQueryElements[0].selListType; break;
	case OQL_GDS::INSERT_QUERY:
		insertQueryElements = insertQuery;
		aselListType = insertQueryElements[0].selListType; break;
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
Four OQL_GDS::setDeferredDeleteFlag(OQL_GDSPOOL *pool, Boolean flag)
{
	DeleteQueryPoolElements	deleteQueryElements(pool->deleteQueryPool);

	switch(queryType)
	{
	case OQL_GDS::DELETE_QUERY:
		deleteQueryElements = deleteQuery;
		deleteQueryElements[0].isDeferredDelete = flag; break;
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
Four OQL_GDS::getDeferredDeleteFlag(OQL_GDSPOOL *pool, Boolean& flag)
{
	DeleteQueryPoolElements	deleteQueryElements(pool->deleteQueryPool);

	switch(queryType)
	{
	case OQL_GDS::DELETE_QUERY:
		deleteQueryElements = deleteQuery;
		flag = deleteQueryElements[0].isDeferredDelete; break;
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
Four OQL_GDS::setWhereCond(OQL_GDSPOOL *pool, ExprPoolIndex awhereCond)
{
	SelectQueryPoolElements	selectQueryElements(pool->selectQueryPool);
	UpdateQueryPoolElements	updateQueryElements(pool->updateQueryPool);
	DeleteQueryPoolElements	deleteQueryElements(pool->deleteQueryPool);
	InsertQueryPoolElements	insertQueryElements(pool->insertQueryPool);	

	switch(queryType)
	{
	case OQL_GDS::SELECT_QUERY:
		selectQueryElements = selectQuery;
		selectQueryElements[0].whereCond = awhereCond; break;
	case OQL_GDS::UPDATE_QUERY:
		updateQueryElements = updateQuery;
		updateQueryElements[0].whereCond = awhereCond; break;
	case OQL_GDS::DELETE_QUERY:
		deleteQueryElements = deleteQuery;
		deleteQueryElements[0].whereCond = awhereCond; break;
	case OQL_GDS::INSERT_QUERY:
		insertQueryElements = insertQuery;
		insertQueryElements[0].whereCond = awhereCond; break;
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
Four OQL_GDS::getWhereCond(OQL_GDSPOOL *pool, ExprPoolIndex& awhereCond)
{
	SelectQueryPoolElements	selectQueryElements(pool->selectQueryPool);
	UpdateQueryPoolElements	updateQueryElements(pool->updateQueryPool);
	DeleteQueryPoolElements	deleteQueryElements(pool->deleteQueryPool);
	InsertQueryPoolElements	insertQueryElements(pool->insertQueryPool);	

	switch(queryType)
	{
	case OQL_GDS::SELECT_QUERY:
		selectQueryElements = selectQuery;
		awhereCond  = selectQueryElements[0].whereCond; break;
	case OQL_GDS::UPDATE_QUERY:
		updateQueryElements = updateQuery;
		awhereCond  = updateQueryElements[0].whereCond; break;
	case OQL_GDS::DELETE_QUERY:
		deleteQueryElements = deleteQuery;
		awhereCond  = deleteQueryElements[0].whereCond; break;
	case OQL_GDS::INSERT_QUERY:
		insertQueryElements = insertQuery;
		awhereCond  = insertQueryElements[0].whereCond; break;
	default:
		OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
	}

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_GDS::getWhereCond(OQL_GDSPOOL *pool, ExprPoolElements& awhereCond)
{
	ExprPoolIndex exprPoolIndex;
	Four          e;

	e = getWhereCond(pool, exprPoolIndex); 
	OOSQL_CHECK_ERR(e);

	awhereCond.setPool(pool->exprPool);
	awhereCond = exprPoolIndex;

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eUNHANDLED_CASE_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_GDS::setGroupByList(OQL_GDSPOOL *pool, GroupByListPoolIndex agroupByList)
{
	SelectQueryPoolElements	selectQueryElements(pool->selectQueryPool);
	InsertQueryPoolElements	insertQueryElements(pool->insertQueryPool);	

	switch(queryType)
	{
	case OQL_GDS::SELECT_QUERY:
		selectQueryElements = selectQuery;
		selectQueryElements[0].groupByList = agroupByList; break;
	case OQL_GDS::INSERT_QUERY:
		insertQueryElements = insertQuery;
		insertQueryElements[0].groupByList = agroupByList; break;
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
Four OQL_GDS::getGroupByList(OQL_GDSPOOL *pool, GroupByListPoolIndex& agroupByList)
{
	SelectQueryPoolElements	selectQueryElements(pool->selectQueryPool);
	InsertQueryPoolElements	insertQueryElements(pool->insertQueryPool);	

	switch(queryType)
	{
	case OQL_GDS::SELECT_QUERY:
		selectQueryElements = selectQuery;
		agroupByList = selectQueryElements[0].groupByList; break;
	case OQL_GDS::INSERT_QUERY:
		insertQueryElements = insertQuery;
		agroupByList = insertQueryElements[0].groupByList; break;
	default:
		OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
	}

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_GDS::getGroupByList(OQL_GDSPOOL *pool, GroupByListPoolElements& agroupByList)
{
	GroupByListPoolIndex	groupByListPoolIndex;
	Four					e;

	e = getGroupByList(pool, groupByListPoolIndex); 
	OOSQL_CHECK_ERR(e);

	agroupByList.setPool(pool->groupByListPool);
	agroupByList = groupByListPoolIndex;

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eUNHANDLED_CASE_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_GDS::setHavingCond(OQL_GDSPOOL *pool, ExprPoolIndex ahavingCond)
{
	SelectQueryPoolElements	selectQueryElements(pool->selectQueryPool);
	InsertQueryPoolElements	insertQueryElements(pool->insertQueryPool);	

	switch(queryType)
	{
	case OQL_GDS::SELECT_QUERY:
		selectQueryElements = selectQuery;
		selectQueryElements[0].havingCond = ahavingCond; break;
	case OQL_GDS::INSERT_QUERY:
		insertQueryElements = insertQuery;
		insertQueryElements[0].havingCond = ahavingCond; break;
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
Four OQL_GDS::getHavingCond(OQL_GDSPOOL *pool, ExprPoolIndex& ahavingCond)
{
	SelectQueryPoolElements	selectQueryElements(pool->selectQueryPool);
	InsertQueryPoolElements	insertQueryElements(pool->insertQueryPool);	

	switch(queryType)
	{
	case OQL_GDS::SELECT_QUERY:
		selectQueryElements = selectQuery;
		ahavingCond = selectQueryElements[0].havingCond; break;
	case OQL_GDS::INSERT_QUERY:
		insertQueryElements = insertQuery;
		ahavingCond = insertQueryElements[0].havingCond; break;
	default:
		OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
	}

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_GDS::getHavingCond(OQL_GDSPOOL *pool, ExprPoolElements& ahavingCond)
{
	ExprPoolIndex	exprPoolIndex;
	Four			e;

	e = getHavingCond(pool, exprPoolIndex); 
	OOSQL_CHECK_ERR(e);

	ahavingCond.setPool(pool->exprPool);
	ahavingCond = exprPoolIndex;

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eUNHANDLED_CASE_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/

Four OQL_GDS::setOrderByList(OQL_GDSPOOL *pool, OrderByListPoolIndex aorderByList)
{
	SelectQueryPoolElements	selectQueryElements(pool->selectQueryPool);
	InsertQueryPoolElements	insertQueryElements(pool->insertQueryPool);	

	switch(queryType)
	{
	case OQL_GDS::SELECT_QUERY:
		selectQueryElements = selectQuery;
		selectQueryElements[0].orderByList = aorderByList; break;
	case OQL_GDS::INSERT_QUERY:
		insertQueryElements = insertQuery;
		insertQueryElements[0].orderByList = aorderByList; break;
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
Four OQL_GDS::getOrderByList(OQL_GDSPOOL *pool, OrderByListPoolIndex& aorderByList)
{
	SelectQueryPoolElements	selectQueryElements(pool->selectQueryPool);
	InsertQueryPoolElements	insertQueryElements(pool->insertQueryPool);	

	switch(queryType)
	{
	case OQL_GDS::SELECT_QUERY:
		selectQueryElements = selectQuery;
		aorderByList = selectQueryElements[0].orderByList; break;
	case OQL_GDS::INSERT_QUERY:
		insertQueryElements = insertQuery;
		aorderByList = insertQueryElements[0].orderByList; break;
	default:
		OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
	}

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
< eNOERROR         

IMPLEMENTATION:
****************************************************************************/
Four OQL_GDS::getOrderByList(OQL_GDSPOOL *pool, OrderByListPoolElements& aorderByList)
{
	OrderByListPoolIndex	orderByListPoolIndex;
	Four					e;

	e = getOrderByList(pool, orderByListPoolIndex); 
	OOSQL_CHECK_ERR(e);

	aorderByList.setPool(pool->orderByListPool);
	aorderByList = orderByListPoolIndex;

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eUNHANDLED_CASE_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_GDS::setQueryGraph(OQL_GDSPOOL *pool, QGNodePoolIndex aqueryGraph)
{
	SelectQueryPoolElements	selectQueryElements(pool->selectQueryPool);
	UpdateQueryPoolElements	updateQueryElements(pool->updateQueryPool);
	DeleteQueryPoolElements	deleteQueryElements(pool->deleteQueryPool);
	InsertQueryPoolElements	insertQueryElements(pool->insertQueryPool);

	switch(queryType)
	{
	case OQL_GDS::SELECT_QUERY:
		selectQueryElements = selectQuery;
		selectQueryElements[0].queryGraph = aqueryGraph; break;
	case OQL_GDS::UPDATE_QUERY:
		updateQueryElements = updateQuery;
		updateQueryElements[0].queryGraph = aqueryGraph; break;
	case OQL_GDS::DELETE_QUERY:
		deleteQueryElements = deleteQuery;
		deleteQueryElements[0].queryGraph = aqueryGraph; break;
	case OQL_GDS::INSERT_QUERY:
		insertQueryElements = insertQuery;
		insertQueryElements[0].queryGraph = aqueryGraph; break;
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
Four OQL_GDS::getQueryGraph(OQL_GDSPOOL *pool, QGNodePoolIndex& aqueryGraph)
{
	SelectQueryPoolElements	selectQueryElements(pool->selectQueryPool);
	UpdateQueryPoolElements	updateQueryElements(pool->updateQueryPool);
	DeleteQueryPoolElements	deleteQueryElements(pool->deleteQueryPool);
	InsertQueryPoolElements	insertQueryElements(pool->insertQueryPool);

	switch(queryType)
	{
	case OQL_GDS::SELECT_QUERY:
		selectQueryElements = selectQuery;
		aqueryGraph = selectQueryElements[0].queryGraph; break;
	case OQL_GDS::UPDATE_QUERY:
		updateQueryElements = updateQuery;
		aqueryGraph = updateQueryElements[0].queryGraph; break;
	case OQL_GDS::DELETE_QUERY:
		deleteQueryElements = deleteQuery;
		aqueryGraph = deleteQueryElements[0].queryGraph; break;
	case OQL_GDS::INSERT_QUERY:
		insertQueryElements = insertQuery;
		aqueryGraph = insertQueryElements[0].queryGraph; break;
	default:
		OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
	}

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_GDS::getQueryGraph(OQL_GDSPOOL *pool, QGNodePoolElements& aqueryGraph)
{
	QGNodePoolIndex	queryGraphPoolIndex;
	Four			e;

	e = getQueryGraph(pool, queryGraphPoolIndex); 
	OOSQL_CHECK_ERR(e);

	aqueryGraph.setPool(pool->qgNodePool);
	aqueryGraph = queryGraphPoolIndex;

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eUNHANDLED_CASE_OOSQL
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_GDS::setInsertValueList(OQL_GDSPOOL *pool, InsertValuePoolIndex ainsertValueList)
{
	InsertQueryPoolElements	insertQueryElements(pool->insertQueryPool);

	switch(queryType)
	{
	case OQL_GDS::INSERT_QUERY:
		insertQueryElements = insertQuery;
		insertQueryElements[0].insertValueList = ainsertValueList; break;
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
Four OQL_GDS::getInsertValueList(OQL_GDSPOOL *pool, InsertValuePoolIndex& ainsertValueList)
{
	InsertQueryPoolElements	insertQueryElements(pool->insertQueryPool);

	switch(queryType)
	{
	case OQL_GDS::INSERT_QUERY:
		insertQueryElements = insertQuery;
		ainsertValueList = insertQueryElements[0].insertValueList; break;
	default:
		OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
	}

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_GDS::getInsertValueList(OQL_GDSPOOL *pool, InsertValuePoolElements& ainsertValueList)
{
	InsertValuePoolIndex	insertValueListPoolIndex;
	Four					e;

	e = getInsertValueList(pool, insertValueListPoolIndex); 
	OOSQL_CHECK_ERR(e);

	ainsertValueList.setPool(pool->insertValuePool);
	ainsertValueList = insertValueListPoolIndex;

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eUNHANDLED_CASE_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_GDS::setUpdateValueList(OQL_GDSPOOL *pool, UpdateValuePoolIndex aupdateValueList)
{
	UpdateQueryPoolElements	updateQueryElements(pool->updateQueryPool);

	switch(queryType)
	{
	case OQL_GDS::UPDATE_QUERY:
		updateQueryElements = updateQuery;
		updateQueryElements[0].updateValueList = aupdateValueList; break;
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
Four OQL_GDS::getUpdateValueList(OQL_GDSPOOL *pool, UpdateValuePoolIndex& aupdateValueList)
{
	UpdateQueryPoolElements	updateQueryElements(pool->updateQueryPool);

	switch(queryType)
	{
	case OQL_GDS::UPDATE_QUERY:
		updateQueryElements = updateQuery;
		aupdateValueList = updateQueryElements[0].updateValueList; break;
	default:
		OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
	}

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_GDS::getUpdateValueList(OQL_GDSPOOL *pool, UpdateValuePoolElements& aupdateValueList)
{
	UpdateValuePoolIndex	updateValueListPoolIndex;
	Four					e;

	e = getUpdateValueList(pool, updateValueListPoolIndex); 
	OOSQL_CHECK_ERR(e);

	aupdateValueList.setPool(pool->updateValuePool);
	aupdateValueList = updateValueListPoolIndex;

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_GDS::addNodeToUnusedConditionList(OQL_GDSPOOL *pool, ExprPoolIndex& addedNode)
{
	CondListPoolElements    cipElements(pool->condInfoPool);
	CondListPoolElements	unusedConditionList(pool->condInfoPool);

	cipElements = pool->condInfoPool.addNewEntry();
	cipElements[0].expr = addedNode.getPoolIndex();
	
	if(m_unusedConditionList == NULL_POOLINDEX)
	{
		m_unusedConditionList = cipElements.getPoolIndex();
		cipElements[0].nextCondInfo.setNull();
	}
	else
	{
		unusedConditionList = m_unusedConditionList;
		cipElements[0].nextCondInfo = unusedConditionList[0].nextCondInfo;
		unusedConditionList[0].nextCondInfo = cipElements.getPoolIndex();
	}
	
	return eNOERROR;
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
OQL_OutStream& operator<<(OQL_OutStream& os, OQL_GDS& object)
{
    linesplit(os);
	switch(object.queryType)
	{
	case OQL_GDS::SELECT_QUERY:
		os << "queryType   " << "SELECT_QUERY" << endl;
		os << "selectQuery " << object.selectQuery << endl;
		break;
	case OQL_GDS::UPDATE_QUERY:
		os << "queryType   " << "UPDATE_QUERY" << endl;
		os << "updateQuery " << object.updateQuery << endl;
		break;
	case OQL_GDS::INSERT_QUERY:
		os << "queryType " << "INSERT_QUERY" << endl;
		os << "insertQuery " << object.insertQuery << endl;
		break;
	case OQL_GDS::DELETE_QUERY:
		os << "queryType " << "DELETE_QUERY" << endl; 
		os << "deleteQuery " << object.deleteQuery << endl;
		break;
	case OQL_GDS::NO_QUERY:
		os << "queryType " << "NO_QUERY" << endl; 
		break;
	}
    linesplit(os);

    return os;
}


Four OQL_GDS::setLimitClause(OQL_GDSPOOL *pool, LimitClausePoolIndex alimitClause)
{
	SelectQueryPoolElements selectQueryElements(pool->selectQueryPool);

	switch(queryType)
	{
	case OQL_GDS::SELECT_QUERY:
		selectQueryElements = selectQuery;
		selectQueryElements[0].limitClause = alimitClause; break;
	case OQL_GDS::INSERT_QUERY:
		break;
	default:
		OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
	}

	return eNOERROR;
}

Four OQL_GDS::getLimitClause(OQL_GDSPOOL *pool, LimitClausePoolIndex& alimitClause)
{
	SelectQueryPoolElements selectQueryElements(pool->selectQueryPool);

	switch(queryType)
	{
	case OQL_GDS::SELECT_QUERY:
	case OQL_GDS::INSERT_QUERY:
		selectQueryElements = selectQuery;
		alimitClause = selectQueryElements[0].limitClause; break;
	default:
		OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
	}

	return eNOERROR;
}

Four OQL_GDS::getLimitClause(OQL_GDSPOOL *pool, LimitClausePoolElements& alimitClause)
{
	LimitClausePoolIndex	limitClausePoolIndex;
	Four					e;

	e = getLimitClause(pool, limitClausePoolIndex); 
	OOSQL_CHECK_ERR(e);

	alimitClause.setPool(pool->limitClausePool);
	alimitClause = limitClausePoolIndex;

	return eNOERROR;
}

