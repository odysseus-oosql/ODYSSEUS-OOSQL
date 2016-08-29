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

#include "OOSQL_StorageSystemHeaders.h"
#include "OOSQL_StorageManager.hxx"
#include "OOSQL_APIs_Internal.hxx"
#include "OOSQL_Error.h"
#include <malloc.h>
#include <string.h>


Four OOSQL_UDTObject_AllocBuffer(
    OOSQL_UDTObject*    UDTObject,
    Four                size)
{
    if (size > UDTOBJECT_MAX_SIZE || size < 0)
    	return eOUTOFMEMORY_OOSQL;
        
    UDTObject->encodedUDTObjectData    = (char*)malloc(size);
    UDTObject->encodedUDTObjectSize    = size;

	return eNOERROR;
}

Four OOSQL_UDTObject_FreeBuffer(
    OOSQL_UDTObject*    UDTObject)
{
	if (UDTObject->encodedUDTObjectData != NULL)
	{
        free(UDTObject->encodedUDTObjectData);
        UDTObject->encodedUDTObjectData    = NULL;
        UDTObject->encodedUDTObjectSize    = 0;
    }

	return eNOERROR;
}

char* OOSQL_UDTObject_GetData(
    OOSQL_UDTObject*    UDTObject)
{
    return UDTObject->encodedUDTObjectData;
}

void OOSQL_UDTObject_SetData(
    OOSQL_UDTObject*    UDTObject,
    char*               data)
{
	UDTObject->encodedUDTObjectData = data;
}

Four OOSQL_UDTObject_GetSize(
    OOSQL_UDTObject*    UDTObject)
{
    return UDTObject->encodedUDTObjectSize;
}

void OOSQL_UDTObject_SetSize(
    OOSQL_UDTObject*    UDTObject,
    Four                size)
{
    UDTObject->encodedUDTObjectSize = size;
}

Four OOSQL_UDTObject_GetLength(
    OOSQL_UDTObject*    UDTObject)
{
    return *(Four*)(&UDTObject->encodedUDTObjectData[UDTOBJECT_LENGTH_OFFSET]);
}

void OOSQL_UDTObject_SetLength(
    OOSQL_UDTObject*    UDTObject,
    Four                length)
{
    *(Four*)(&UDTObject->encodedUDTObjectData[UDTOBJECT_LENGTH_OFFSET]) = length;
}

Four OOSQL_UDTObject_GetTypeID(
    OOSQL_UDTObject*    UDTObject)
{
    return *(Four*)(&UDTObject->encodedUDTObjectData[UDTOBJECT_TYPEID_OFFSET]);
}

void OOSQL_UDTObject_SetTypeID(
    OOSQL_UDTObject*    UDTObject,
    Four                UDTId)
{
	*(Four*)(&UDTObject->encodedUDTObjectData[UDTOBJECT_TYPEID_OFFSET]) = UDTId;
}

Four OOSQL_UDTObject_GetNAttrs(
    OOSQL_UDTObject*    UDTObject)
{
    return *(Four*)(&UDTObject->encodedUDTObjectData[UDTOBJECT_NATTRS_OFFSET]);
}

void OOSQL_UDTObject_SetNAttrs(
    OOSQL_UDTObject*    UDTObject,
    Four nAttrs)
{
	*(Four*)(&UDTObject->encodedUDTObjectData[UDTOBJECT_NATTRS_OFFSET]) = nAttrs;
}

Four OOSQL_UDTObject_GetNTHAttrOffset(
    OOSQL_UDTObject*    UDTObject,
    Four                nth)
{
    return *(Four*)(&UDTObject->encodedUDTObjectData[UDTOBJECT_POINTER_ARRAY_OFFSET + UDTOBJECT_POINTER_ELEMENT_SIZE*nth]);
}

void OOSQL_UDTObject_SetNTHAttrOffset(
    OOSQL_UDTObject*    UDTObject,
    Four                nth, 
    Four                offset)
{
	*(Four*)(&UDTObject->encodedUDTObjectData[UDTOBJECT_POINTER_ARRAY_OFFSET + UDTOBJECT_POINTER_ELEMENT_SIZE*nth]) = offset;
}

Four OOSQL_UDTObject_GetNTHAttrType(
    OOSQL_UDTObject*    UDTObject,
    Four                nth)
{
	Four	nAttrs;

	nAttrs = OOSQL_UDTObject_GetNAttrs(UDTObject);

	return *(Four*)(&UDTObject->encodedUDTObjectData[UDTOBJECT_TYPE_ARRAY_OFFSET(nAttrs) + UDTOBJECT_TYPE_ELEMENT_SIZE*nth]);
}

void OOSQL_UDTObject_SetNTHAttrType(
    OOSQL_UDTObject*    UDTObject,
    Four                nth, 
    Four                attrType)
{
	Four	nAttrs;

	nAttrs = OOSQL_UDTObject_GetNAttrs(UDTObject);

	*(Four*)(&UDTObject->encodedUDTObjectData[UDTOBJECT_TYPE_ARRAY_OFFSET(nAttrs) + UDTOBJECT_TYPE_ELEMENT_SIZE*nth]) = attrType;
}

Four OOSQL_UDTObject_GetNTHAttrLength(
    OOSQL_UDTObject*    UDTObject,
    Four                nth)
{
    Four    length;
    Four    attrOffset;
    Four    attrType;

    attrOffset  = OOSQL_UDTObject_GetNTHAttrOffset(UDTObject, nth);
    attrType    = OOSQL_UDTObject_GetNTHAttrType(UDTObject, nth);

    if (attrOffset != NIL)
    {
        switch (attrType)
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
        case OOSQL_TYPE_FLOAT:
            length = OOSQL_TYPE_FLOAT_SIZE;
            break;
        case OOSQL_TYPE_DOUBLE:
            length = OOSQL_TYPE_DOUBLE_SIZE;
            break;
        case OOSQL_TYPE_STRING:
        case OOSQL_TYPE_VARSTRING:
        case OOSQL_TYPE_GEOMETRY:
        case OOSQL_TYPE_POINT:
        case OOSQL_TYPE_LINESTRING:
        case OOSQL_TYPE_POLYGON:
        case OOSQL_TYPE_GEOMETRYCOLLECTION:
        case OOSQL_TYPE_MULTIPOINT:
        case OOSQL_TYPE_MULTILINESTRING:
        case OOSQL_TYPE_MULTIPOLYGON:
            length = *(Four*)(&UDTObject->encodedUDTObjectData[attrOffset]);
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
        case OOSQL_TYPE_OID:
            length = OOSQL_TYPE_OID_SIZE;
            break;
        case OOSQL_TYPE_MBR:
            length = OOSQL_TYPE_MBR_SIZE;
            break;
        default:
            length = 0;
            break;
        }
    }
    else 
    {
        length = 0;
    }

    return length;
}

void OOSQL_UDTObject_SetNTHAttrLength(
    OOSQL_UDTObject*    UDTObject,
    Four                nth, 
    Four                length)
{
    Four    attrOffset;
    Four    attrType;

    attrOffset  = OOSQL_UDTObject_GetNTHAttrOffset(UDTObject, nth);
    attrType    = OOSQL_UDTObject_GetNTHAttrType(UDTObject, nth);

    switch (attrType)
    {
    case OOSQL_TYPE_STRING:
    case OOSQL_TYPE_VARSTRING:
        *(Four*)(&UDTObject->encodedUDTObjectData[attrOffset]) = length;
        break;
    default:
        break;
    }
}

char* OOSQL_UDTObject_GetNTHAttrData(
    OOSQL_UDTObject*    UDTObject,
    Four                nth)
{
    char    *data;
    Four    attrOffset;
    Four    attrType;

    attrOffset  = OOSQL_UDTObject_GetNTHAttrOffset(UDTObject, nth);
    attrType    = OOSQL_UDTObject_GetNTHAttrType(UDTObject, nth);

    switch (attrType)
    {
    case OOSQL_TYPE_SHORT:
    case OOSQL_TYPE_INT:
    case OOSQL_TYPE_LONG:
    case OOSQL_TYPE_FLOAT:
    case OOSQL_TYPE_DOUBLE:
    case OOSQL_TYPE_DATE:
    case OOSQL_TYPE_TIME:
    case OOSQL_TYPE_TIMESTAMP:
    case OOSQL_TYPE_OID:
        case OOSQL_TYPE_GEOMETRY:
        case OOSQL_TYPE_POINT:
        case OOSQL_TYPE_LINESTRING:
        case OOSQL_TYPE_POLYGON:
        case OOSQL_TYPE_GEOMETRYCOLLECTION:
        case OOSQL_TYPE_MULTIPOINT:
        case OOSQL_TYPE_MULTILINESTRING:
        case OOSQL_TYPE_MULTIPOLYGON:
    case OOSQL_TYPE_MBR:
        data = &UDTObject->encodedUDTObjectData[attrOffset];
        break;
    case OOSQL_TYPE_STRING:
    case OOSQL_TYPE_VARSTRING:
        data = &UDTObject->encodedUDTObjectData[attrOffset+4];
        break;
    default:
        data = NULL;
        break;
    }

    return data;
}


Four OOSQL_UDTObject_SetNTHAttrData(
    OOSQL_UDTObject*    UDTObject,
    Four                nth, 
    Four                length, 
    void*               data)
{
    Four    i;
    Four    oldDataLength;
    Four    oldAttrOffset;
    Four    oldAttrLength;
    Four    newAttrLength;
    Four    attrType;

    Four    attrLength;
    Four    attrOffset;

    attrLength  = OOSQL_UDTObject_GetLength(UDTObject);
    attrOffset  = OOSQL_UDTObject_GetNTHAttrOffset(UDTObject, nth);


    // calculate old length
    oldDataLength = OOSQL_UDTObject_GetLength(UDTObject);
    oldAttrOffset = OOSQL_UDTObject_GetNTHAttrOffset(UDTObject, nth);
    oldAttrLength = OOSQL_UDTObject_GetNTHAttrLength(UDTObject, nth);

    // calculate new length
    attrType    = OOSQL_UDTObject_GetNTHAttrType(UDTObject, nth);
    switch(attrType)
    {
    case OOSQL_TYPE_STRING:
    case OOSQL_TYPE_VARSTRING:
        if (length != 0)    
            newAttrLength = length + 4;
        else                
            newAttrLength = length;
        break;
    default:
        newAttrLength = length;
        break;
    }

    // doubling memory if it is not sufficient
    while ( oldDataLength - oldAttrLength + newAttrLength > UDTObject->encodedUDTObjectSize &&
            UDTObject->encodedUDTObjectSize * 2 < UDTOBJECT_MAX_SIZE)
    {
        UDTObject->encodedUDTObjectData    = (char*)realloc(UDTObject->encodedUDTObjectData, UDTObject->encodedUDTObjectSize * 2);
        UDTObject->encodedUDTObjectSize  = UDTObject->encodedUDTObjectSize * 2;
    }
    if (oldDataLength - oldAttrLength + newAttrLength > UDTObject->encodedUDTObjectSize)
    	return eOUTOFMEMORY_OOSQL;

    // move momory
    if (OOSQL_UDTObject_GetNTHAttrOffset(UDTObject, nth) != NIL)
    {
        memmove(&UDTObject->encodedUDTObjectData[oldAttrOffset + newAttrLength], 
                &UDTObject->encodedUDTObjectData[oldAttrOffset + oldAttrLength], 
                oldDataLength - oldAttrOffset - oldAttrLength);
        OOSQL_UDTObject_SetLength(UDTObject, oldDataLength - oldAttrLength + newAttrLength);        

        // recalculate offsets
        if (newAttrLength == 0)    
            OOSQL_UDTObject_SetNTHAttrOffset(UDTObject, nth, NIL);
        for (i = nth+1; i < OOSQL_UDTObject_GetNAttrs(UDTObject); i++)
        {
            OOSQL_UDTObject_SetNTHAttrOffset(UDTObject, i, OOSQL_UDTObject_GetNTHAttrOffset(UDTObject, i) - oldAttrLength + newAttrLength);
        }
    }
    else 
    {
        if (newAttrLength != 0)
        {
            OOSQL_UDTObject_SetLength(UDTObject, oldDataLength + newAttrLength);        
            OOSQL_UDTObject_SetNTHAttrOffset(UDTObject, nth, oldDataLength);
        }
    }

    // copy data
    if (newAttrLength != 0)
    {
        attrType    = OOSQL_UDTObject_GetNTHAttrType(UDTObject, nth);
        switch (attrType)
        {
        case OOSQL_TYPE_SHORT:
            *(short*)(&UDTObject->encodedUDTObjectData[OOSQL_UDTObject_GetNTHAttrOffset(UDTObject, nth)]) = *(short*)data;
            break;
        case OOSQL_TYPE_INT:
            *(int*)(&UDTObject->encodedUDTObjectData[OOSQL_UDTObject_GetNTHAttrOffset(UDTObject, nth)]) = *(int*)data;
            break;
        case OOSQL_TYPE_LONG:
            *(long*)(&UDTObject->encodedUDTObjectData[OOSQL_UDTObject_GetNTHAttrOffset(UDTObject, nth)]) = *(long*)data;
            break;
        case OOSQL_TYPE_FLOAT:
            *(float*)(&UDTObject->encodedUDTObjectData[OOSQL_UDTObject_GetNTHAttrOffset(UDTObject, nth)]) = *(float*)data;
            break;
        case OOSQL_TYPE_DOUBLE:
            *(double*)(&UDTObject->encodedUDTObjectData[OOSQL_UDTObject_GetNTHAttrOffset(UDTObject, nth)]) = *(double*)data;
            break;
        case OOSQL_TYPE_STRING:
        case OOSQL_TYPE_VARSTRING:
            memcpy(&UDTObject->encodedUDTObjectData[OOSQL_UDTObject_GetNTHAttrOffset(UDTObject, nth)+4], data, length);
            OOSQL_UDTObject_SetNTHAttrLength(UDTObject, nth, length);
            break;
        case OOSQL_TYPE_DATE:
            memcpy(&UDTObject->encodedUDTObjectData[OOSQL_UDTObject_GetNTHAttrOffset(UDTObject, nth)], data, OOSQL_TYPE_DATE_SIZE);
            break;
        case OOSQL_TYPE_TIME:
            memcpy(&UDTObject->encodedUDTObjectData[OOSQL_UDTObject_GetNTHAttrOffset(UDTObject, nth)], data, OOSQL_TYPE_TIME_SIZE);
            break;
        case OOSQL_TYPE_TIMESTAMP:
            memcpy(&UDTObject->encodedUDTObjectData[OOSQL_UDTObject_GetNTHAttrOffset(UDTObject, nth)], data, OOSQL_TYPE_TIMESTAMP_SIZE);
            break;
        case OOSQL_TYPE_OID:
            memcpy(&UDTObject->encodedUDTObjectData[OOSQL_UDTObject_GetNTHAttrOffset(UDTObject, nth)], data, OOSQL_TYPE_OID_SIZE);
            break;
        case OOSQL_TYPE_MBR:
            memcpy(&UDTObject->encodedUDTObjectData[OOSQL_UDTObject_GetNTHAttrOffset(UDTObject, nth)], data, OOSQL_TYPE_MBR_SIZE);
            break;
        case OOSQL_TYPE_GEOMETRY:
        case OOSQL_TYPE_POINT:
        case OOSQL_TYPE_LINESTRING:
        case OOSQL_TYPE_POLYGON:
        case OOSQL_TYPE_GEOMETRYCOLLECTION:
        case OOSQL_TYPE_MULTIPOINT:
        case OOSQL_TYPE_MULTILINESTRING:
        case OOSQL_TYPE_MULTIPOLYGON:
            memcpy(&UDTObject->encodedUDTObjectData[OOSQL_UDTObject_GetNTHAttrOffset(UDTObject, nth)], data, length);
            break;
        default:
            break;
        }
    }

    return eNOERROR;
}
