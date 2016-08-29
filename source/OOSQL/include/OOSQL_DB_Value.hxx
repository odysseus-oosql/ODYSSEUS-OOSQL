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

#ifndef _OOSQL_DB_VALUE_H_
#define _OOSQL_DB_VALUE_H_

/*
    MODULE:     
        OOSQL_Internal.hxx

    DESCRIPTION:
        This header defines constants and basic types necessary for OOSQL Server.
*/

#include "OOSQL_StorageManager.hxx"
#include "OOSQL_MemoryManagedObject.hxx"
#include "OOSQL_Eval_Util.hxx"
#include "OOSQL_Error.h"

/* structure to store return value and actual parameters of a method */
class  OOSQL_DB_Value : public OOSQL_MemoryManagedObject {
public:
    Two     type;										// type of data
	Boolean nullFlag;
    union {
		Boolean							b;				// OOSQL_TYPE_BOOL

        Two_Invariable					s;              // OOSQL_TYPE_SHORT 
        Four_Invariable					i;              // OOSQL_TYPE_INT 
        Four_Invariable					l;              // OOSQL_TYPE_LONG 
        Eight_Invariable				ll;             // OOSQL_TYPE_LONG_LONG 

        float							f;				// OOSQL_TYPE_FLOAT 
        double							d;              // OOSQL_TYPE_DOUBLE 
        OOSQL_StorageManager::OID		oid;            // OOSQL_TYPE_OID  
        void*							ptr;			// OOSQL_TYPE_STRING, OOSQL_TYPE_VARSTRING 
		OOSQL_StorageManager::Date		date;			// OOSQL_TYPE_DATE
        OOSQL_StorageManager::Time		time;			// OOSQL_TYPE_TIME
        OOSQL_StorageManager::Timestamp	timestamp;		// OOSQL_TYPE_TIMESTAMP
        OOSQL_StorageManager::Interval	interval;		// OOSQL_TYPE_INTERVAL
		OOSQL_StorageManager::MBR		mbr;			// OOSQL_TYPE_MBR
    } data;
    Four    length;										// actual length of an variable-length string

	OOSQL_DB_Value()
	{
		m_data   = NULL;
		nullFlag = SM_FALSE;
	}

	OOSQL_DB_Value(OOSQL_MemoryManager* memoryManager) : OOSQL_MemoryManagedObject(memoryManager)
	{
		m_data = NULL;
		nullFlag = SM_FALSE;
	}

	OOSQL_DB_Value(const OOSQL_DB_Value& a)
	{
		pMemoryManager = a.pMemoryManager;
		
		type     = a.type;
		length   = a.length;
		nullFlag = SM_FALSE;

		memcpy(&data, &a.data, sizeof(data));
		
		if(IsStringData())
		{
			PrepareData(a.m_dataSize);
			memcpy(m_data, a.m_data, m_dataSize);
		}
		else
			DestroyData();
	}

	~OOSQL_DB_Value()
	{
		if(m_data && m_data != m_internalBuffer)
			pMemoryManager->Free(m_data);
	}

	Four PrepareData(Four size)
	{
		if(IsStringData())
		{
			if(m_data)
			{
				if(m_dataSize < size)
				{
					if(m_data == m_internalBuffer)
						m_data = (char*)(char*)pMemoryManager->Alloc(size);
					else
						m_data = (char*)pMemoryManager->Realloc(m_data, size);
					m_dataSize = size;
				}
			}
			else
			{
				if(size < sizeof(m_internalBuffer))
				{
					m_data     = m_internalBuffer;
					m_dataSize = sizeof(m_internalBuffer);
				}
				else
				{
					m_data     = (char*)pMemoryManager->Alloc(size);
					m_dataSize = size;
				}
			}
		}
		else
		{
			type = OOSQL_TYPE_STRING;
			if(size < sizeof(m_internalBuffer))
			{
				m_data     = m_internalBuffer;
				m_dataSize = sizeof(m_internalBuffer);
			}
			else
			{
				m_data     = (char*)pMemoryManager->Alloc(size);
				m_dataSize = size;
			}
		}

		data.ptr = m_data;

		return eNOERROR;
	}

	Four IsStringData()
	{
		if(type == OOSQL_TYPE_STRING || type == OOSQL_TYPE_VARSTRING
                                        || type == OOSQL_TYPE_GEOMETRY
                                        || type == OOSQL_TYPE_POINT
                                        || type == OOSQL_TYPE_LINESTRING
                                        || type == OOSQL_TYPE_POLYGON
                                        || type == OOSQL_TYPE_GEOMETRYCOLLECTION
                                        || type == OOSQL_TYPE_MULTIPOINT
                                        || type == OOSQL_TYPE_MULTILINESTRING
                                        || type == OOSQL_TYPE_MULTIPOLYGON)
			return 1;
		else
			return 0;
	}

	Four DestroyData()
	{
		if(m_data && m_data != m_internalBuffer)
			pMemoryManager->Free(m_data);
		m_data     = NULL;
		m_dataSize = 0;

		return eNOERROR;
	}

	const OOSQL_DB_Value& operator = (const OOSQL_DB_Value& a)
	{
		type   = a.type;
		length = a.length;

		memcpy(&data, &a.data, sizeof(data));

		if(IsStringData())
		{
			PrepareData(a.m_dataSize);
			memcpy(m_data, a.m_data, m_dataSize);
		}
		else
			DestroyData();

		return *this;
	}

	Two_Invariable GetDataAsShort(void)
	{
		switch(type)
		{
		case OOSQL_TYPE_SHORT:
			return (Two_Invariable)data.s;
		case OOSQL_TYPE_INT:
			return (Two_Invariable)data.i;
		case OOSQL_TYPE_LONG:
			return (Two_Invariable)data.l;
		case OOSQL_TYPE_LONG_LONG:
			return (Two_Invariable)data.ll;
		case OOSQL_TYPE_FLOAT:
			return (Two_Invariable)data.f;
		case OOSQL_TYPE_DOUBLE:
			return (Two_Invariable)data.d;
		default:
			return 0;
		}
	}
	Four_Invariable    GetDataAsInteger()
	{
		switch(type)
		{
		case OOSQL_TYPE_SHORT:
			return (Four_Invariable)data.s;
		case OOSQL_TYPE_INT:
			return (Four_Invariable)data.i;
		case OOSQL_TYPE_LONG:
			return (Four_Invariable)data.l;
		case OOSQL_TYPE_LONG_LONG:
			return (Four_Invariable)data.ll;
		case OOSQL_TYPE_FLOAT:
			return (Four_Invariable)data.f;
		case OOSQL_TYPE_DOUBLE:
			return (Four_Invariable)data.d;
		default:
			return 0;
		}
	}
	Four_Invariable GetDataAsLong()
	{
		switch(type)
		{
		case OOSQL_TYPE_SHORT:
			return (Four_Invariable)data.s;
		case OOSQL_TYPE_INT:
			return (Four_Invariable)data.i;
		case OOSQL_TYPE_LONG:
			return (Four_Invariable)data.l;
		case OOSQL_TYPE_LONG_LONG:
			return (Four_Invariable)data.ll;
		case OOSQL_TYPE_FLOAT:
			return (Four_Invariable)data.f;
		case OOSQL_TYPE_DOUBLE:
			return (Four_Invariable)data.d;
		default:
			return 0;
		}
	}

	Eight_Invariable GetDataAsLongLong()
	{
		switch(type)
		{
		case OOSQL_TYPE_SHORT:
			return (Eight_Invariable)data.s;
		case OOSQL_TYPE_INT:
			return (Eight_Invariable)data.i;
		case OOSQL_TYPE_LONG:
			return (Eight_Invariable)data.l;
		case OOSQL_TYPE_LONG_LONG:
			return (Eight_Invariable)data.ll;
		case OOSQL_TYPE_FLOAT:
			return (Eight_Invariable)data.f;
		case OOSQL_TYPE_DOUBLE:
			return (Eight_Invariable)data.d;
		default:
			return 0;
		}
	}
	float  GetDataAsFloat()
	{
		switch(type)
		{
		case OOSQL_TYPE_SHORT:
			return (float)data.s;
		case OOSQL_TYPE_INT:
			return (float)data.i;
		case OOSQL_TYPE_LONG:
			return (float)data.l;
		case OOSQL_TYPE_LONG_LONG:
			return (float)data.ll;
		case OOSQL_TYPE_FLOAT:
			return (float)data.f;
		case OOSQL_TYPE_DOUBLE:
			return (float)data.d;
		default:
			return 0;
		}
	}
	double GetDataAsDouble()
	{
		switch(type)
		{
		case OOSQL_TYPE_SHORT:
			return (double)data.s;
		case OOSQL_TYPE_INT:
			return (double)data.i;
		case OOSQL_TYPE_LONG:
			return (double)data.l;
		case OOSQL_TYPE_LONG_LONG:
			return (double)data.ll;
		case OOSQL_TYPE_FLOAT:
			return (double)data.f;
		case OOSQL_TYPE_DOUBLE:
			return (double)data.d;
		default:
			return 0;
		}
	}
    Four convertType(Four newtype)
	{
		if(newtype == type)
			return eNOERROR;

		if(nullFlag)
		{
			if(newtype == OOSQL_TYPE_BOOL)
			{
				data.b   = SM_FALSE;
				nullFlag = SM_FALSE;
			}
			type     = newtype;
		}
		else
		{
		    switch(newtype)
		    {
		    case OOSQL_TYPE_BOOL:
			    switch(type)
			    {
			    case OOSQL_TYPE_SHORT:
				    if(data.s)
					    data.b = SM_TRUE;
				    else
					    data.b = SM_FALSE;
				    length = OOSQL_TYPE_BOOL_SIZE;
				    type   = newtype;
				    break;
			    case OOSQL_TYPE_INT:
				    if(data.i)
					    data.b = SM_TRUE;
				    else
					    data.b = SM_FALSE;
				    length = OOSQL_TYPE_BOOL_SIZE;
				    type   = newtype;
				    break;
			    case OOSQL_TYPE_LONG:
				    if(data.l)
					    data.b = SM_TRUE;
				    else
					    data.b = SM_FALSE;
				    length = OOSQL_TYPE_BOOL_SIZE;
				    type   = newtype;
				    break;
			    case OOSQL_TYPE_LONG_LONG:
				    if(data.ll)
					    data.b = SM_TRUE;
				    else
					    data.b = SM_FALSE;
				    length = OOSQL_TYPE_BOOL_SIZE;
				    type   = newtype;
				    break;
			    case OOSQL_TYPE_FLOAT:
				    if(data.f)
					    data.b = SM_TRUE;
				    else
					    data.b = SM_FALSE;
				    length = OOSQL_TYPE_BOOL_SIZE;
				    type   = newtype;
				    break;
			    case OOSQL_TYPE_DOUBLE:
				    if(data.d)
					    data.b = SM_TRUE;
				    else
					    data.b = SM_FALSE;
				    length = OOSQL_TYPE_BOOL_SIZE;
				    type   = newtype;
				    break;
				case OOSQL_TYPE_STRING:
				case OOSQL_TYPE_VARSTRING:
					if(length)
						data.b = SM_TRUE;
					else
						data.b = SM_FALSE;
					length = OOSQL_TYPE_BOOL_SIZE;
					type   = newtype;
					break;
			    default:
				    OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
			    }
			    break;

		    case OOSQL_TYPE_SHORT:
			    switch(type)
			    {
			    case OOSQL_TYPE_SHORT:
				    data.s = (Two_Invariable)data.s;
				    length = OOSQL_TYPE_SHORT_SIZE;
				    type   = newtype;
				    break;
			    case OOSQL_TYPE_INT:
				    data.s = (Two_Invariable)data.i;
				    length = OOSQL_TYPE_SHORT_SIZE;
				    type   = newtype;
				    break;
			    case OOSQL_TYPE_LONG:
				    data.s = (Two_Invariable)data.l;
				    length = OOSQL_TYPE_SHORT_SIZE;
				    type   = newtype;
				    break;
			    case OOSQL_TYPE_LONG_LONG:
				    data.s = (Two_Invariable)data.ll;
				    length = OOSQL_TYPE_SHORT_SIZE;
				    type   = newtype;
				    break;
			    case OOSQL_TYPE_FLOAT:
				    data.s = (Two_Invariable)data.f;
				    length = OOSQL_TYPE_SHORT_SIZE;
				    type   = newtype;
				    break;
			    case OOSQL_TYPE_DOUBLE:
				    data.s = (Two_Invariable)data.d;
				    length = OOSQL_TYPE_SHORT_SIZE;
				    type   = newtype;
				    break;
			    default:
				    OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
			    }
			    break;
		    case OOSQL_TYPE_INT:
			    switch(type)
			    {
			    case OOSQL_TYPE_SHORT:
				    data.i = (Four_Invariable)data.s;
				    length = OOSQL_TYPE_INT_SIZE;
				    type   = newtype;
				    break;
			    case OOSQL_TYPE_INT:
				    data.i = (Four_Invariable)data.i;
				    length = OOSQL_TYPE_INT_SIZE;
				    type   = newtype;
				    break;
			    case OOSQL_TYPE_LONG:
				    data.i = (Four_Invariable)data.l;
				    length = OOSQL_TYPE_INT_SIZE;
				    type   = newtype;
				    break;
			    case OOSQL_TYPE_LONG_LONG:
				    data.i = (Four_Invariable)data.ll;
				    length = OOSQL_TYPE_INT_SIZE;
				    type   = newtype;
				    break;
			    case OOSQL_TYPE_FLOAT:
				    data.i = (Four_Invariable)data.f;
				    length = OOSQL_TYPE_INT_SIZE;
				    type   = newtype;
				    break;
			    case OOSQL_TYPE_DOUBLE:
				    data.i = (Four_Invariable)data.d;
				    length = OOSQL_TYPE_INT_SIZE;
				    type   = newtype;
				    break;
			    default:
				    OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
			    }
			    break;
		    case OOSQL_TYPE_LONG:
			    switch(type)
			    {
			    case OOSQL_TYPE_SHORT:
				    data.l = (Four_Invariable)data.s;
				    length = OOSQL_TYPE_LONG_SIZE;
				    type   = newtype;
				    break;
			    case OOSQL_TYPE_INT:
				    data.l = (Four_Invariable)data.i;
				    length = OOSQL_TYPE_LONG_SIZE;
				    type   = newtype;
				    break;
			    case OOSQL_TYPE_LONG:
				    data.l = (Four_Invariable)data.l;
				    length = OOSQL_TYPE_LONG_SIZE;
				    type   = newtype;
				    break;
			    case OOSQL_TYPE_FLOAT:
				    data.l = (Four_Invariable)data.f;
				    length = OOSQL_TYPE_LONG_SIZE;
				    type   = newtype;
				    break;
			    case OOSQL_TYPE_DOUBLE:
				    data.l = (Four_Invariable)data.d;
				    length = OOSQL_TYPE_LONG_SIZE;
				    type   = newtype;
				    break;
			    default:
				    OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
			    }
			    break;
		    case OOSQL_TYPE_LONG_LONG:
			    switch(type)
			    {
			    case OOSQL_TYPE_SHORT:
				    data.ll = (Eight_Invariable)data.s;
				    length = OOSQL_TYPE_LONG_LONG_SIZE;
				    type   = newtype;
				    break;
			    case OOSQL_TYPE_INT:
				    data.ll = (Eight_Invariable)data.i;
				    length = OOSQL_TYPE_LONG_LONG_SIZE;
				    type   = newtype;
				    break;
			    case OOSQL_TYPE_LONG:
				    data.ll = (Eight_Invariable)data.l;
				    length = OOSQL_TYPE_LONG_LONG_SIZE;
				    type   = newtype;
				    break;
			    case OOSQL_TYPE_FLOAT:
				    data.ll = (Eight_Invariable)data.f;
				    length = OOSQL_TYPE_LONG_LONG_SIZE;
				    type   = newtype;
				    break;
			    case OOSQL_TYPE_DOUBLE:
				    data.ll = (Eight_Invariable)data.d;
				    length = OOSQL_TYPE_LONG_LONG_SIZE;
				    type   = newtype;
				    break;
			    default:
				    OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
			    }
				break;
		    case OOSQL_TYPE_FLOAT:
			    switch(type)
			    {
			    case OOSQL_TYPE_SHORT:
				    data.f = (float)data.s;
				    length = OOSQL_TYPE_FLOAT_SIZE;
				    type   = newtype;
				    break;
			    case OOSQL_TYPE_INT:
				    data.f = (float)data.i;
				    length = OOSQL_TYPE_FLOAT_SIZE;
				    type   = newtype;
				    break;
			    case OOSQL_TYPE_LONG:
				    data.f = (float)data.l;
				    length = OOSQL_TYPE_FLOAT_SIZE;
				    type   = newtype;
				    break;
			    case OOSQL_TYPE_LONG_LONG:
				    data.f = (float)data.ll;
				    length = OOSQL_TYPE_FLOAT_SIZE;
				    type   = newtype;
				    break;
			    case OOSQL_TYPE_FLOAT:
				    data.f = (float)data.f;
				    length = OOSQL_TYPE_FLOAT_SIZE;
				    type   = newtype;
				    break;
			    case OOSQL_TYPE_DOUBLE:
				    data.f = (float)data.d;
				    length = OOSQL_TYPE_FLOAT_SIZE;
				    type   = newtype;
				    break;
			    default:
				    OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
			    }
			    break;
		    case OOSQL_TYPE_DOUBLE:
			    switch(type)
			    {
			    case OOSQL_TYPE_SHORT:
				    data.d = (double)data.s;
				    length = OOSQL_TYPE_DOUBLE_SIZE;
				    type   = newtype;
				    break;
			    case OOSQL_TYPE_INT:
				    data.d = (double)data.i;
				    length = OOSQL_TYPE_DOUBLE_SIZE;
				    type   = newtype;
				    break;
			    case OOSQL_TYPE_LONG:
				    data.d = (double)data.l;
				    length = OOSQL_TYPE_DOUBLE_SIZE;
				    type   = newtype;
				    break;
			    case OOSQL_TYPE_LONG_LONG:
				    data.d = (double)data.ll;
				    length = OOSQL_TYPE_DOUBLE_SIZE;
				    type   = newtype;
				    break;
			    case OOSQL_TYPE_FLOAT:
				    data.d = (double)data.f;
				    length = OOSQL_TYPE_DOUBLE_SIZE;
				    type   = newtype;
				    break;
			    case OOSQL_TYPE_DOUBLE:
				    data.d = (double)data.d;
				    length = OOSQL_TYPE_DOUBLE_SIZE;
				    type   = newtype;
				    break;
			    default:
				    OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
			    }
			    break;
		    case OOSQL_TYPE_STRING:
			    switch(type)
			    {
			    case OOSQL_TYPE_STRING:
				    type   = newtype;
				    break;
			    case OOSQL_TYPE_VARSTRING:
				    type   = newtype;
				    break;
			    default:
                    OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
			    }
			    break;
		    case OOSQL_TYPE_VARSTRING:
			    switch(type)
			    {
			    case OOSQL_TYPE_STRING:
				    type   = newtype;
				    break;
			    case OOSQL_TYPE_VARSTRING:
				    type   = newtype;
				    break;
			    default:
                    OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
			    }
			    break;
		    case OOSQL_TYPE_OID:
			    switch(type)
			    {
			    case OOSQL_TYPE_STRING:
				    OOSQL_StorageManager::OID oid;
				    oosql_ConvertFromOIDStringToOIDStructure((char*)data.ptr, &oid);
				    data.oid = oid;
				    length   = OOSQL_TYPE_OID_SIZE;
				    type     = newtype;
				    break;
			    default:
                    OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
			    }
                break;
		    default:
                OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
		    }
        }

		return eNOERROR;
	}
	void SetType(Two type)
	{
		this->type = type;
		switch(type)
		{
        case OOSQL_TYPE_SHORT:
			length = OOSQL_TYPE_SHORT_SIZE;
			break;
        case OOSQL_TYPE_INT:
			length = OOSQL_TYPE_INT_SIZE;
			break;
        case OOSQL_TYPE_LONG:
			length = OOSQL_TYPE_LONG_SIZE;
			break;
        case OOSQL_TYPE_LONG_LONG:
			length = OOSQL_TYPE_LONG_LONG_SIZE;
			break;
        case OOSQL_TYPE_FLOAT: 
			length = OOSQL_TYPE_FLOAT_SIZE;
			break;
        case OOSQL_TYPE_DOUBLE: 
			length = OOSQL_TYPE_DOUBLE_SIZE;
			break;
        case OOSQL_TYPE_OID:  
			length = OOSQL_TYPE_OID_SIZE;
			break;
        case OOSQL_TYPE_STRING:
			break;
		case OOSQL_TYPE_VARSTRING:
			break;
		case OOSQL_TYPE_DATE:
			length = OOSQL_TYPE_DATE_SIZE;
			break;
        case OOSQL_TYPE_TIME:
			length = OOSQL_TYPE_TIME_SIZE;
			break;
        case OOSQL_TYPE_TIMESTAMP:
			length = OOSQL_TYPE_TIMESTAMP_SIZE;
			break;
        case OOSQL_TYPE_INTERVAL:
			length = OOSQL_TYPE_INTERVAL_SIZE;
			break;
		case OOSQL_TYPE_MBR:
			length = OOSQL_TYPE_MBR_SIZE;
			break;
		default:
			length = 0;
		}
	}
	void SetTypeLength(Two type, Four length)
	{
		this->type = type;
		this->length = length;
	}

private:
	char*	m_data;
	Four	m_dataSize;
	char    m_internalBuffer[OOSQL_DB_VALUE_INTERNALBUFFERSIZE];
};

#endif // _OOSQL_DB_VALUE_H_

