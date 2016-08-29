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

#include "OOSQL_MultipleResultBuffer.hxx"

OOSQL_MultipleResultBuffer::OOSQL_MultipleResultBuffer(OOSQL_Evaluator* evaluator, Four nResultsToRead, void* headerBuffer, Four headerBufferSize, void* dataBuffer, Four dataBufferSize)
{
	InitInternalData(evaluator, nResultsToRead, headerBuffer, headerBufferSize, dataBuffer, dataBufferSize);
}

OOSQL_MultipleResultBuffer::OOSQL_MultipleResultBuffer(OOSQL_MemoryManager* memoryManager, OOSQL_Evaluator* evaluator, Four nResultsToRead, void* headerBuffer, Four headerBufferSize, void* dataBuffer, Four dataBufferSize)
: OOSQL_MemoryManagedObject(memoryManager)
{
	InitInternalData(evaluator, nResultsToRead, headerBuffer, headerBufferSize, dataBuffer, dataBufferSize);
}

OOSQL_MultipleResultBuffer::~OOSQL_MultipleResultBuffer()
{
	if(m_realColumnSizes)
		pMemoryManager->Free(m_realColumnSizes);
}

void OOSQL_MultipleResultBuffer::InitInternalData(OOSQL_Evaluator* evaluator, Four nResultsToRead, void* headerBuffer, Four headerBufferSize, void* dataBuffer, Four dataBufferSize)
{
	m_evaluator				= evaluator;
	m_nResultsToRead		= nResultsToRead;
	m_headerBuffer			= (char*)headerBuffer;
	m_headerBufferSize		= headerBufferSize;
	m_headerBufferOffset	= 0;
	m_dataBuffer			= (char*)dataBuffer;
	m_dataBufferSize		= dataBufferSize;
	m_dataBufferOffset		= 0;
	m_nResultsRead			= 0;

	m_getdata_info			= m_evaluator->m_getdata_info;

	m_nColumns   = m_evaluator->m_getdata_nInfo - m_evaluator->m_getoid_nTargetClass;
	if(m_nColumns < 0) m_nColumns = 0;

	m_headerSize = sizeof(Four) + sizeof(Four) +
	               ((((m_nColumns) / 8 + 1 + (sizeof(Four) - 1)) / sizeof(Four)) * sizeof(Four)) +
		           sizeof(Four) * m_nColumns + sizeof(Four) * m_nColumns +
				   sizeof(OOSQL_StorageManager::OID) * m_nColumns +
				   sizeof(Four) * m_nColumns;

	m_realColumnSizes = (Four*)pMemoryManager->Alloc(sizeof(Four) * m_nColumns);
	for(Four i = 0; i < m_nColumns; i++)
	{
		switch(m_getdata_info[i].type)
		{
		case OOSQL_TYPE_SHORT:
			m_realColumnSizes[i] = OOSQL_TYPE_SHORT_SIZE;
			break;
		case OOSQL_TYPE_INT:
			m_realColumnSizes[i] = OOSQL_TYPE_INT_SIZE;
			break;
		case OOSQL_TYPE_LONG:
			m_realColumnSizes[i] = OOSQL_TYPE_LONG_SIZE;
			break;
		case OOSQL_TYPE_LONG_LONG:
			m_realColumnSizes[i] = OOSQL_TYPE_LONG_LONG_SIZE;
			break;
		case OOSQL_TYPE_FLOAT:
			m_realColumnSizes[i] = OOSQL_TYPE_FLOAT_SIZE;
			break;
		case OOSQL_TYPE_DOUBLE:
			m_realColumnSizes[i] = OOSQL_TYPE_DOUBLE_SIZE;
			break;
		case OOSQL_TYPE_STRING:
			m_realColumnSizes[i] = NIL;
			break;
		case OOSQL_TYPE_VARSTRING:
			m_realColumnSizes[i] = NIL;
			break;
		case OOSQL_TYPE_OID:
			m_realColumnSizes[i] = OOSQL_TYPE_OID_SIZE;
			break;
		case OOSQL_TYPE_TEXT:
			m_realColumnSizes[i] = NIL;
			break;
		case OOSQL_TYPE_GEOMETRY:
		case OOSQL_TYPE_POINT:
		case OOSQL_TYPE_LINESTRING:
		case OOSQL_TYPE_POLYGON:	
		case OOSQL_TYPE_GEOMETRYCOLLECTION:
		case OOSQL_TYPE_MULTIPOINT:
		case OOSQL_TYPE_MULTILINESTRING:
		case OOSQL_TYPE_MULTIPOLYGON:
			m_realColumnSizes[i] = NIL;
			break;
		case OOSQL_TYPE_DATE:
			m_realColumnSizes[i] = OOSQL_TYPE_DATE_SIZE;
			break;
		case OOSQL_TYPE_TIME:
			m_realColumnSizes[i] = OOSQL_TYPE_TIME_SIZE;
			break;
		case OOSQL_TYPE_TIMESTAMP:
			m_realColumnSizes[i] = OOSQL_TYPE_TIMESTAMP_SIZE;
			break;
		case OOSQL_TYPE_PAGEID:
			m_realColumnSizes[i] = OOSQL_TYPE_PAGEID_SIZE;
			break;
		case OOSQL_TYPE_FILEID:
			m_realColumnSizes[i] = OOSQL_TYPE_FILEID_SIZE;
			break;
		case OOSQL_TYPE_INDEXID:
			m_realColumnSizes[i] = OOSQL_TYPE_INDEXID_SIZE;
			break;
        case OOSQL_TYPE_MBR:
            m_realColumnSizes[i] = OOSQL_TYPE_MBR_SIZE;
            break;
		default:
			if(OOSQL_MASK_COMPLEXTYPE(m_getdata_info[i].type) == OOSQL_COMPLEXTYPE_BASIC)
			{
				OOSQL_ERR_EXIT(eNOTIMPLEMENTED_OOSQL); // unhandled case error
			}
			else
				m_realColumnSizes[i] = 0;
			break;
		}
	}

	m_headerNullVectorOffset = sizeof(Four) + sizeof(Four);
	m_headerSizesOffset      = m_headerNullVectorOffset + ((((m_nColumns) / 8 + 1 + (sizeof(Four) - 1)) / sizeof(Four)) * sizeof(Four));
	m_headerRealSizesOffset  = m_headerSizesOffset + m_nColumns * sizeof(Four);
	m_headerOidsOffset       = m_headerRealSizesOffset + m_nColumns * sizeof(Four);
	m_headerColNosOffset     = m_headerOidsOffset + m_nColumns * sizeof(OOSQL_StorageManager::OID);

	if(m_headerBuffer && m_dataBuffer)
		m_appendObjectType = 0;
	else if(m_headerBuffer && !m_dataBuffer)
		m_appendObjectType = 1;
	else if(!m_headerBuffer && m_dataBuffer)
		m_appendObjectType = 2;
	else if(!m_headerBuffer && !m_dataBuffer)
		m_appendObjectType = 3;
}

