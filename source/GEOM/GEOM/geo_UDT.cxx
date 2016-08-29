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

#ifndef SLIMDOWN_OPENGIS
#include "geo_UDT.hxx"

#define UDTOBJECT_DEFAULT_SIZE                  1024 * 8

#define UDTOBJECT_POINTER_ELEMENT_SIZE			4
#define UDTOBJECT_TYPE_ELEMENT_SIZE				4

#define	UDTOBJECT_LENGTH_OFFSET					0
#define	UDTOBJECT_LOM_OFFSET					4
#define	UDTOBJECT_NATTRS_OFFSET					8
#define	UDTOBJECT_POINTER_ARRAY_OFFSET			12
#define	UDTOBJECT_TYPE_ARRAY_OFFSET(nAttrs)		(UDTOBJECT_POINTER_ARRAY_OFFSET + nAttrs*UDTOBJECT_POINTER_ELEMENT_SIZE)

#define	UDTOBJECT_FIXED_HEADER_SIZE			    UDTOBJECT_POINTER_ARRAY_OFFSET
#define	UDTOBJECT_VARIABLE_HEADER_SIZE(nAttrs)  (nAttrs*UDTOBJECT_POINTER_ELEMENT_SIZE + nAttrs*UDTOBJECT_TYPE_ELEMENT_SIZE)
#define UDTOBJECT_HEADER_SIZE(nAttrs)			(UDTOBJECT_FIXED_HEADER_SIZE + UDTOBJECT_VARIABLE_HEADER_SIZE(nAttrs))

GEO_UDTObject::GEO_UDTObject(
): encodedUDTObjectData(geoMemoryManager)
{
}

GEO_UDTObject::~GEO_UDTObject()
{
}


Four GEO_UDTObject::getLength()
{
	return *(Four*)(encodedUDTObjectData.str()+UDTOBJECT_LENGTH_OFFSET);
}

void GEO_UDTObject::setLength(Four length)
{
    *(Four*)(encodedUDTObjectData.str()+UDTOBJECT_LENGTH_OFFSET) = length;
}

Four GEO_UDTObject::getTypeID()
{
	return *(Four*)(encodedUDTObjectData.str()+UDTOBJECT_LOM_OFFSET);
}

void GEO_UDTObject::setTypeID(Four UDTId)
{
	*(Four*)(encodedUDTObjectData.str()+UDTOBJECT_LOM_OFFSET) = UDTId;
}

Four GEO_UDTObject::getNAttrs()
{
	return *(Four*)(encodedUDTObjectData.str()+UDTOBJECT_NATTRS_OFFSET);
}

void GEO_UDTObject::setNAttrs(Four nAttrs)
{
	*(Four*)(encodedUDTObjectData.str()+UDTOBJECT_NATTRS_OFFSET) = nAttrs;
}

Four GEO_UDTObject::getNTHAttrOffset(Four nth)
{
	return *(Four*)(encodedUDTObjectData.str()+UDTOBJECT_POINTER_ARRAY_OFFSET+UDTOBJECT_POINTER_ELEMENT_SIZE*nth);
}

void GEO_UDTObject::setNTHAttrOffset(Four nth, Four offset)
{
	*(Four*)(encodedUDTObjectData.str()+UDTOBJECT_POINTER_ARRAY_OFFSET+UDTOBJECT_POINTER_ELEMENT_SIZE*nth) = offset;
}

Four GEO_UDTObject::getNTHAttrType(Four nth)
{
	Four	nAttrs;

	nAttrs = getNAttrs();

	return *(Four*)(encodedUDTObjectData.str()+UDTOBJECT_TYPE_ARRAY_OFFSET(nAttrs)+UDTOBJECT_TYPE_ELEMENT_SIZE*nth);
}

void GEO_UDTObject::setNTHAttrType(Four nth, Four attrType)
{
	Four	nAttrs;

	nAttrs = getNAttrs();

	*(Four*)(encodedUDTObjectData.str()+UDTOBJECT_TYPE_ARRAY_OFFSET(nAttrs)+UDTOBJECT_TYPE_ELEMENT_SIZE*nth) = attrType;
}


Four GEO_UDTObject::getNTHAttrLength(Four nth)
{
    Four    length;


    if (getNTHAttrOffset(nth) != NIL)
    {
        switch (getNTHAttrType(nth))
        {
        case LOM_SHORT:
            length = LOM_SHORT_SIZE;
            break;
        case LOM_INT:
            length = LOM_INT_SIZE;
            break;
        case LOM_LONG:
            length = LOM_LONG_SIZE;
            break;
        case LOM_FLOAT:
            length = LOM_FLOAT_SIZE;
            break;
        case LOM_DOUBLE:
            length = LOM_DOUBLE_SIZE;
            break;
        case LOM_STRING:
        case LOM_VARSTRING:
        case LOM_OGIS_GEOMETRY:
        case LOM_OGIS_POINT:
        case LOM_OGIS_LINESTRING:
        case LOM_OGIS_POLYGON:
        case LOM_OGIS_GEOMETRYCOLLECTION:
        case LOM_OGIS_MULTIPOINT:
        case LOM_OGIS_MULTILINESTRING:
        case LOM_OGIS_MULTIPOLYGON:
            length = *(Four*)(encodedUDTObjectData.str()+getNTHAttrOffset(nth));
            break;
        case LOM_DATE:
            length = LOM_DATE_SIZE;
            break;
        case LOM_TIME:
            length = LOM_TIME_SIZE;
            break;
        case LOM_TIMESTAMP:
            length = LOM_TIMESTAMP_SIZE;
            break;
        case LOM_INTERVAL:
            length = LOM_INTERVAL_SIZE;
            break;
        case LOM_OID:
            length = LOM_OID_SIZE;
            break;
        case LOM_MBR:
            length = LOM_MBR_SIZE;
            break;
        case LOM_REF:
            length = LOM_REF_SIZE;
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

void GEO_UDTObject::setNTHAttrLength(Four nth, Four length)
{
    switch (getNTHAttrType(nth))
    {
    case LOM_STRING:
    case LOM_VARSTRING:
        *(Four*)(encodedUDTObjectData.str()+getNTHAttrOffset(nth)) = length;
        break;
    default:
        break;
    }
}

char* GEO_UDTObject::getNTHAttrData(Four nth)
{
    char    *data;

    switch (getNTHAttrType(nth))
    {
    case LOM_SHORT:
    case LOM_INT:
    case LOM_LONG:
    case LOM_FLOAT:
    case LOM_DOUBLE:
    case LOM_DATE:
    case LOM_TIME:
    case LOM_TIMESTAMP:
    case LOM_INTERVAL:
    case LOM_OID:
    case LOM_MBR:
    case LOM_REF:
    case LOM_OGIS_GEOMETRY:
    case LOM_OGIS_POINT:
    case LOM_OGIS_LINESTRING:
    case LOM_OGIS_POLYGON:
    case LOM_OGIS_GEOMETRYCOLLECTION:
    case LOM_OGIS_MULTIPOINT:
    case LOM_OGIS_MULTILINESTRING:
    case LOM_OGIS_MULTIPOLYGON:
        data = (char*)encodedUDTObjectData.str()+getNTHAttrOffset(nth);
        break;
    case LOM_STRING:
    case LOM_VARSTRING:
        data = (char*)encodedUDTObjectData.str()+getNTHAttrOffset(nth)+4;
        break;
    default:
        data = NULL;
        break;
    }

    return data;
}


Four GEO_UDTObject::setNTHAttrData(Four nth, Four length, void* data)
{
    Four    i;
    Four    oldDataLength;
    Four    oldAttrOffset;
    Four    oldAttrLength;
    Four    newAttrLength;


    // calculate old length
    oldDataLength = getLength();
    oldAttrOffset = getNTHAttrOffset(nth);
    oldAttrLength = getNTHAttrLength(nth);

    // calculate new length
    switch(getNTHAttrType(nth))
    {
    case LOM_STRING:
    case LOM_VARSTRING:
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
    if (oldDataLength - oldAttrLength + newAttrLength > encodedUDTObjectData.length())
		encodedUDTObjectData.resize(encodedUDTObjectData.length() * 2);

    // move momory
    if (getNTHAttrOffset(nth) != NIL)
    {
        memmove((char*)encodedUDTObjectData.str()+oldAttrOffset+newAttrLength,
                (char*)encodedUDTObjectData.str()+oldAttrOffset+oldAttrLength,
                oldDataLength - oldAttrOffset - oldAttrLength);
        setLength(oldDataLength - oldAttrLength + newAttrLength);

        // recalculate offsets
        if (newAttrLength == 0)    setNTHAttrOffset(nth, NIL);
        for (i = nth+1; i < getNAttrs(); i++)
        {
            setNTHAttrOffset(i, getNTHAttrOffset(i) - oldAttrLength + newAttrLength);
        }
    }
    else // getNTHAttrOffset(nth) == NIL
    {
        if (newAttrLength != 0)
        {
            setLength(oldDataLength + newAttrLength);
            setNTHAttrOffset(nth, oldDataLength);
        }
    }

    // copy data
    if (newAttrLength != 0)
    {
        switch (getNTHAttrType(nth))
        {
        case LOM_SHORT:
            *(short*)(encodedUDTObjectData.str()+getNTHAttrOffset(nth)) = *(short*)data;
            break;
        case LOM_INT:
            *(int*)(encodedUDTObjectData.str()+getNTHAttrOffset(nth)) = *(int*)data;
            break;
        case LOM_LONG:
            *(long*)(encodedUDTObjectData.str()+getNTHAttrOffset(nth)) = *(long*)data;
            break;
        case LOM_FLOAT:
            *(float*)(encodedUDTObjectData.str()+getNTHAttrOffset(nth)) = *(float*)data;
            break;
        case LOM_DOUBLE:
            *(double*)(encodedUDTObjectData.str()+getNTHAttrOffset(nth)) = *(double*)data;
            break;
        case LOM_STRING:
        case LOM_VARSTRING:
            memcpy((char*)encodedUDTObjectData.str()+getNTHAttrOffset(nth)+4, data, length);
            setNTHAttrLength(nth, length);
            break;
        case LOM_DATE:
            memcpy((char*)encodedUDTObjectData.str()+getNTHAttrOffset(nth), data, LOM_DATE_SIZE);
            break;
        case LOM_TIME:
            memcpy((char*)encodedUDTObjectData.str()+getNTHAttrOffset(nth), data, LOM_TIME_SIZE);
            break;
        case LOM_TIMESTAMP:
            memcpy((char*)encodedUDTObjectData.str()+getNTHAttrOffset(nth), data, LOM_TIMESTAMP_SIZE);
            break;
        case LOM_INTERVAL:
            memcpy((char*)encodedUDTObjectData.str()+getNTHAttrOffset(nth), data, LOM_INTERVAL_SIZE);
            break;
        case LOM_OID:
            memcpy((char*)encodedUDTObjectData.str()+getNTHAttrOffset(nth), data, LOM_OID_SIZE);
            break;
        case LOM_MBR:
            memcpy((char*)encodedUDTObjectData.str()+getNTHAttrOffset(nth), data, LOM_MBR_SIZE);
            break;
        case LOM_REF:
            memcpy((char*)encodedUDTObjectData.str()+getNTHAttrOffset(nth), data, LOM_REF_SIZE);
            break;
		case LOM_OGIS_GEOMETRY:
        case LOM_OGIS_POINT:
        case LOM_OGIS_LINESTRING:
        case LOM_OGIS_POLYGON:
        case LOM_OGIS_GEOMETRYCOLLECTION:
        case LOM_OGIS_MULTIPOINT:
        case LOM_OGIS_MULTILINESTRING:
        case LOM_OGIS_MULTIPOLYGON:
            memcpy((char*)encodedUDTObjectData.str()+getNTHAttrOffset(nth), data, length);
            break;
        default:
            break;
        }
    }

    return eNOERROR;
}

GEO_SpatialUDTObjectReader::GEO_SpatialUDTObjectReader(char* data, int size)
{
	encodedUDTObjectData.resize(size);
	memcpy((char*)encodedUDTObjectData.str(), data, size);
}

void GEO_SpatialUDTObjectReader::GetMBR(float& xmin, float& ymin, float& xmax, float& ymax)
{
	xmin = *(float*)getNTHAttrData(0);
	ymin = *(float*)getNTHAttrData(1);
	xmax = *(float*)getNTHAttrData(2);
	ymax = *(float*)getNTHAttrData(3);
}
void GEO_SpatialUDTObjectReader::GetSRID(Four& srid)
{
	srid = *(int*)getNTHAttrData(4);
}
void GEO_SpatialUDTObjectReader::GetWKBinary(void*& data, int &size)
{
	data = getNTHAttrData(5);
	size = getNTHAttrLength(5);
}
GEO_SpatialUDTObjectWriter::GEO_SpatialUDTObjectWriter(Four udtId)
{
	encodedUDTObjectData.resize(UDTOBJECT_DEFAULT_SIZE);

	setTypeID(udtId);
	setNAttrs(6);
	setLength(UDTOBJECT_HEADER_SIZE(6));

	setNTHAttrType(0, LOM_FLOAT);
	setNTHAttrOffset(0, -1);

	setNTHAttrType(1, LOM_FLOAT);
	setNTHAttrOffset(1, -1);

	setNTHAttrType(2, LOM_FLOAT);
	setNTHAttrOffset(2, -1);

	setNTHAttrType(3, LOM_FLOAT);
	setNTHAttrOffset(3, -1);

	setNTHAttrType(4, LOM_INT);
	setNTHAttrOffset(4, -1);

	setNTHAttrType(5, LOM_VARSTRING);
	setNTHAttrOffset(5, -1);
}
void GEO_SpatialUDTObjectWriter::SetMBR(float xmin, float ymin, float xmax, float ymax)
{
	setNTHAttrData(0, 4, &xmin);
	setNTHAttrData(1, 4, &ymin);
	setNTHAttrData(2, 4, &ymax);
	setNTHAttrData(3, 4, &ymax);
}
void GEO_SpatialUDTObjectWriter::SetSRID(Four srid)
{
	setNTHAttrData(4, 4, &srid);
}
void GEO_SpatialUDTObjectWriter::SetWKBinary(void* data, int size)
{
	setNTHAttrData(5, size, data);
}
#endif
