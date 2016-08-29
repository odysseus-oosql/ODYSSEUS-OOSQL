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
#include "OOSQL_APIs_Internal.hxx"
#include "OOSQL_Error.h"
#include "OOSQL_ServerQuery.hxx"

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eBADPARAMETER_OOSQL 
< eNOERROR         

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_GetComplexTypeInfo(
	OOSQL_SystemHandle*		systemHandle,		// IN  
	OOSQL_Handle			handle,				// IN  
	Two						columnNumber,		// IN  
	OOSQL_ComplexTypeInfo*  complexTypeInfo     // OUT 
)
{
	if(!OOSQL_CHECKGDSINSTTABLE(systemHandle))
		return eBADPARAMETER_OOSQL;

	if(OOSQL_QUERYINSTTABLE(systemHandle)[handle].inUse == SM_FALSE)
		return eBADPARAMETER_OOSQL;

	complexTypeInfo->systemHandle = systemHandle;

	return OOSQL_QUERYINSTTABLE(systemHandle)[handle].query->GetComplexTypeInfo(columnNumber, complexTypeInfo);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eBADPARAMETER_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_ComplexType_GetElementType(
	OOSQL_ComplexTypeInfo*  complexTypeInfo,    // IN  
	Four*					elementType			// OUT Element Type ID
)
{
	*elementType = complexTypeInfo->elementType;

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eBADPARAMETER_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_ComplexType_GetComplexType(
	OOSQL_ComplexTypeInfo*  complexTypeInfo,    // IN  
	Four*					complexType			// OUT Complex Type ID
)
{
	*complexType = complexTypeInfo->complexType;

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eBADPARAMETER_OOSQL
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_ComplexType_GetNumElements(
	OOSQL_ComplexTypeInfo*  complexTypeInfo,    // IN  
	Four*					numElements			// OUT 
)
{
	Four e;
    Boolean isNull;

	switch(complexTypeInfo->complexType)
	{
	case OOSQL_COMPLEXTYPE_SET:
        isNull = (Boolean)LOM_CollectionSet_IsNull(&OOSQL_GET_LOM_SYSTEMHANDLE(complexTypeInfo->systemHandle), complexTypeInfo->orn, SM_FALSE, 
                                            (TupleID*)&complexTypeInfo->oid, complexTypeInfo->colNo);
        if (isNull == SM_TRUE) OOSQL_ERROR(complexTypeInfo->systemHandle, eNULLCOLLECTION_OOSQL);

        e = LOM_CollectionSet_GetN_Elements(&OOSQL_GET_LOM_SYSTEMHANDLE(complexTypeInfo->systemHandle), complexTypeInfo->orn, SM_FALSE,
			                                (TupleID*)&complexTypeInfo->oid, complexTypeInfo->colNo, numElements);
		if(e < eNOERROR) OOSQL_ERROR(complexTypeInfo->systemHandle, e);
		break;
	case OOSQL_COMPLEXTYPE_BAG:
        isNull = (Boolean)LOM_CollectionBag_IsNull(&OOSQL_GET_LOM_SYSTEMHANDLE(complexTypeInfo->systemHandle), complexTypeInfo->orn, SM_FALSE, 
                                            (TupleID*)&complexTypeInfo->oid, complexTypeInfo->colNo);
        if (isNull == SM_TRUE) OOSQL_ERROR(complexTypeInfo->systemHandle, eNULLCOLLECTION_OOSQL);

		e = LOM_CollectionBag_GetN_Elements(&OOSQL_GET_LOM_SYSTEMHANDLE(complexTypeInfo->systemHandle), complexTypeInfo->orn, SM_FALSE,
			                                (TupleID*)&complexTypeInfo->oid, complexTypeInfo->colNo, numElements);
		if(e < eNOERROR) OOSQL_ERROR(complexTypeInfo->systemHandle, e);
		break;
	case OOSQL_COMPLEXTYPE_LIST:
        isNull = (Boolean)LOM_CollectionList_IsNull(&OOSQL_GET_LOM_SYSTEMHANDLE(complexTypeInfo->systemHandle), complexTypeInfo->orn, SM_FALSE, 
                                            (TupleID*)&complexTypeInfo->oid, complexTypeInfo->colNo);
        if (isNull == SM_TRUE) OOSQL_ERROR(complexTypeInfo->systemHandle, eNULLCOLLECTION_OOSQL);

        e = LOM_CollectionList_GetN_Elements(&OOSQL_GET_LOM_SYSTEMHANDLE(complexTypeInfo->systemHandle), complexTypeInfo->orn, SM_FALSE,
			                                 (TupleID*)&complexTypeInfo->oid, complexTypeInfo->colNo, numElements);
		if(e < eNOERROR) OOSQL_ERROR(complexTypeInfo->systemHandle, e);
		break;
	default:
		return eBADPARAMETER_OOSQL;
	}
	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eBADPARAMETER_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_ComplexType_GetElements(
	OOSQL_ComplexTypeInfo*  complexTypeInfo,    // IN  
	Four					start,				// IN  
	Four					nElements,			// IN  
	Four*					elementSizes,		// OUT 
	Four					sizeOfElements,		// IN  
	void*					elements			// OUT 
)
{
	Four e;
    Boolean isNull;

	switch(complexTypeInfo->complexType)
	{
	case OOSQL_COMPLEXTYPE_SET:
        isNull = (Boolean)LOM_CollectionSet_IsNull(&OOSQL_GET_LOM_SYSTEMHANDLE(complexTypeInfo->systemHandle), complexTypeInfo->orn, SM_FALSE, 
                                            (TupleID*)&complexTypeInfo->oid, complexTypeInfo->colNo);
        if (isNull == SM_TRUE) OOSQL_ERROR(complexTypeInfo->systemHandle, eNULLCOLLECTION_OOSQL);

		e = LOM_CollectionSet_RetrieveElements(&OOSQL_GET_LOM_SYSTEMHANDLE(complexTypeInfo->systemHandle), complexTypeInfo->orn, SM_FALSE,
			                                   (TupleID*)&complexTypeInfo->oid, complexTypeInfo->colNo, start, nElements, elementSizes, sizeOfElements, elements);
		if(e < eNOERROR) OOSQL_ERROR(complexTypeInfo->systemHandle, e);
		break;
	case OOSQL_COMPLEXTYPE_BAG:
        isNull = (Boolean)LOM_CollectionBag_IsNull(&OOSQL_GET_LOM_SYSTEMHANDLE(complexTypeInfo->systemHandle), complexTypeInfo->orn, SM_FALSE, 
                                            (TupleID*)&complexTypeInfo->oid, complexTypeInfo->colNo);
        if (isNull == SM_TRUE) OOSQL_ERROR(complexTypeInfo->systemHandle, eNULLCOLLECTION_OOSQL);

		e = LOM_CollectionBag_RetrieveElements(&OOSQL_GET_LOM_SYSTEMHANDLE(complexTypeInfo->systemHandle), complexTypeInfo->orn, SM_FALSE,
			                                   (TupleID*)&complexTypeInfo->oid, complexTypeInfo->colNo, start, nElements, elementSizes, sizeOfElements, elements);
		if(e < eNOERROR) OOSQL_ERROR(complexTypeInfo->systemHandle, e);
		break;
	case OOSQL_COMPLEXTYPE_LIST:
        isNull = (Boolean)LOM_CollectionList_IsNull(&OOSQL_GET_LOM_SYSTEMHANDLE(complexTypeInfo->systemHandle), complexTypeInfo->orn, SM_FALSE, 
                                            (TupleID*)&complexTypeInfo->oid, complexTypeInfo->colNo);
        if (isNull == SM_TRUE) OOSQL_ERROR(complexTypeInfo->systemHandle, eNULLCOLLECTION_OOSQL);

		e = LOM_CollectionList_RetrieveElements(&OOSQL_GET_LOM_SYSTEMHANDLE(complexTypeInfo->systemHandle), complexTypeInfo->orn, SM_FALSE,
			                                    (TupleID*)&complexTypeInfo->oid, complexTypeInfo->colNo, start, nElements, elementSizes, sizeOfElements, elements);
		if(e < eNOERROR) OOSQL_ERROR(complexTypeInfo->systemHandle, e);
		break;
	default:
		return eBADPARAMETER_OOSQL;
	}
	return e;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eBADPARAMETER_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
#define ELEMENTSIZE_BUFFER_SIZE 100
#define ELEMENT_BUFFER_SIZE     ELEMENTSIZE_BUFFER_SIZE * 20
#define STRING_BUFFER_SIZE      1024

static void EncodeControlString(char* dest, char* src, int srcLength)
{
    unsigned char* s = (unsigned char*)src;
    unsigned char* d = (unsigned char*)dest;
    char buffer[3];

    while(srcLength > 0)
    {
        if(*s > 0x80 && (srcLength > 1 && *(s + 1) > 0x80))
        {   // double byte character
            *d = *s; d++; s++; srcLength--;
            *d = *s; d++; s++; srcLength--;
        }
        else if(*s >= 0x20 && *s < 0x80 && *s != '{' && *s != '}')
        {   // normal character
            *d = *s; d++; s++; srcLength--;
        }
        else 
        {   // encode control character
            sprintf(buffer, "%02x", *s); s++; srcLength--;
            *d = '\\'; d++;
            *d = 'x'; d++; 
            *d = buffer[0]; d++; 
            *d = buffer[1]; d++; 
        }
    }
    *d = '\0';
}

Four OOSQL_ComplexType_GetElementsString(
	OOSQL_ComplexTypeInfo*  complexTypeInfo,    // IN  
	Four					start,				// IN  
	Four					nElements,			// IN  
	char*					string,				// INOUT string buffer
	Four					stringLength		// IN  string buffer size
)
{
	Four				e;
	Four				nElementsRead;
	char				stringBuffer[STRING_BUFFER_SIZE*4];
    char				tmpStringBuffer[STRING_BUFFER_SIZE];
	Four				elementSizes[ELEMENTSIZE_BUFFER_SIZE];
	char				elements[ELEMENT_BUFFER_SIZE];
	char*				pElement;
    short               shortAttr;
    int                 intAttr;
    float               floatAttr;
    double              doubleAttr;
    Four                i;
	Four				retLength;
	OID					oid;
	OOSQL_Date          dateAttr;
	OOSQL_Time          timeAttr;
	OOSQL_Timestamp		timestampAttr;
	Four				stringBufferStringLength;
	Four				nElementsStored;
	Boolean				firstFlag = SM_TRUE;
	Four				ith;
    Four                elementSizeFixed;

	strcpy(string, "{"); string ++; stringLength --;

	e = OOSQL_ComplexType_GetNumElements(complexTypeInfo, &nElementsStored);
	if(e < eNOERROR) OOSQL_ERROR(complexTypeInfo->systemHandle, e);

	ith = start;
	while(1)
	{
		if(ith >= (start + nElements) || ith >= nElementsStored)
			break;

		if((start + nElements - ith) >= ELEMENTSIZE_BUFFER_SIZE)
			e = OOSQL_ComplexType_GetElements(complexTypeInfo, ith, ELEMENTSIZE_BUFFER_SIZE, elementSizes, ELEMENT_BUFFER_SIZE, elements);
		else
			e = OOSQL_ComplexType_GetElements(complexTypeInfo, ith, start + nElements - ith, elementSizes, ELEMENT_BUFFER_SIZE, elements);
		if(e < eNOERROR) OOSQL_ERROR(complexTypeInfo->systemHandle, e);

		nElementsRead = e;

		ith += nElementsRead;

		pElement = elements;
		for(i = 0; i < nElementsRead; i++)
		{
			switch(complexTypeInfo->elementType)
			{
			case OOSQL_TYPE_CHAR:
            case OOSQL_TYPE_VARCHAR:
			case OOSQL_TYPE_TEXT:
				if(elementSizes[i] < sizeof(stringBuffer))
				{
					memcpy(stringBuffer, pElement, elementSizes[i]); pElement += elementSizes[i];
					stringBuffer[elementSizes[i]] = 0;
                    elementSizeFixed = elementSizes[i];
				}
				else
				{
					memcpy(stringBuffer, pElement, sizeof(stringBuffer)); pElement += sizeof(stringBuffer);
					stringBuffer[sizeof(stringBuffer) - 4] = '.';
					stringBuffer[sizeof(stringBuffer) - 3] = '.';
					stringBuffer[sizeof(stringBuffer) - 2] = '.';
					stringBuffer[sizeof(stringBuffer) - 1] = 0;
                    elementSizeFixed = sizeof(stringBuffer);
				}
                memcpy(tmpStringBuffer, stringBuffer, elementSizeFixed);
                EncodeControlString(stringBuffer, tmpStringBuffer, elementSizeFixed);    
                break;
            case OOSQL_TYPE_SMALLINT:
				memcpy(&shortAttr, pElement, elementSizes[i]); pElement += elementSizes[i];
				sprintf(stringBuffer, "%ld", (int)shortAttr);
                break;
            case OOSQL_TYPE_INTEGER:
            case OOSQL_TYPE_LONG:
			case OOSQL_TYPE_LONG_LONG:
				memcpy(&intAttr, pElement, elementSizes[i]); pElement += elementSizes[i];
				sprintf(stringBuffer, "%ld", (int)intAttr);
                break;
			case OOSQL_TYPE_TIME:
				memcpy(&timeAttr, pElement, elementSizes[i]); pElement += elementSizes[i];
				sprintf(stringBuffer, "%2d:%2d:%2d", OOSQL_GetHour(complexTypeInfo->systemHandle, &timeAttr),
					                                 OOSQL_GetMinute(complexTypeInfo->systemHandle, &timeAttr),
												     OOSQL_GetSecond(complexTypeInfo->systemHandle, &timeAttr));
                break;
            case OOSQL_TYPE_DATE:
				memcpy(&dateAttr, pElement, elementSizes[i]); pElement += elementSizes[i];
   				sprintf(stringBuffer, "%4d-%2d-%2d", OOSQL_GetYear(complexTypeInfo->systemHandle, &dateAttr),
    				                                 OOSQL_GetMonth(complexTypeInfo->systemHandle, &dateAttr),
    				                                 OOSQL_GetDay(complexTypeInfo->systemHandle, &dateAttr));
                break;
			case OOSQL_TYPE_TIMESTAMP:
				memcpy(&timestampAttr, pElement, elementSizes[i]); pElement += elementSizes[i];
   				sprintf(stringBuffer, "%4d-%2d-%2d-%2d:%2d:%2d", 
													 OOSQL_GetYear(complexTypeInfo->systemHandle, &timestampAttr.d),
    												 OOSQL_GetMonth(complexTypeInfo->systemHandle, &timestampAttr.d),
    												 OOSQL_GetDay(complexTypeInfo->systemHandle, &timestampAttr.d),
													 OOSQL_GetHour(complexTypeInfo->systemHandle, &timestampAttr.t),
    												 OOSQL_GetMinute(complexTypeInfo->systemHandle, &timestampAttr.t),
    												 OOSQL_GetSecond(complexTypeInfo->systemHandle, &timestampAttr.t));
                break;
            case OOSQL_TYPE_FLOAT:
				memcpy(&floatAttr, pElement, elementSizes[i]); pElement += elementSizes[i];
				sprintf(stringBuffer, "%f", floatAttr);
                break;
            case OOSQL_TYPE_DOUBLE:
                memcpy(&doubleAttr, pElement, elementSizes[i]); pElement += elementSizes[i];
				sprintf(stringBuffer, "%f", doubleAttr);
                break;
			case OOSQL_TYPE_OID:
				memcpy(&oid, pElement, elementSizes[i]); pElement += elementSizes[i];
				e = OOSQL_OIDToOIDString(complexTypeInfo->systemHandle, &oid, stringBuffer); 
				if(e < eNOERROR) OOSQL_ERROR(complexTypeInfo->systemHandle, e);
				break;
			}
			stringBufferStringLength = strlen(stringBuffer);
			if(stringLength > stringBufferStringLength + 3)
			{
				if(!firstFlag)  strcat(string, ",");
				else			firstFlag = SM_FALSE;

				strcat(string, stringBuffer);
				stringLength -= stringBufferStringLength;
			}
			else
			{
				if(stringLength > 5)
					strcat(string, ",...}");
				else
				{
					Four lastindex = strlen(string);

					string[lastindex - 4] = '.';
					string[lastindex - 3] = '.';
					string[lastindex - 2] = '.';
					string[lastindex - 1] = '}';
					string[lastindex - 0] = '0';
				}
				return eNOERROR;				
			}
		}
	}

	strcat(string, "}");

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eBADPARAMETER_OOSQL
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_ComplexType_InsertElements(
	OOSQL_ComplexTypeInfo*  complexTypeInfo,    // IN  
	Four					start,				// IN 
	Four					nElements,			// IN  
	Four*					elementSizes,		// OUT 
	void*					elements			// OUT 
)
{
	Four e;
    Four nElementsStored;
    Boolean isNull;

	switch(complexTypeInfo->complexType)
	{
	case OOSQL_COMPLEXTYPE_SET:
        isNull = (Boolean)LOM_CollectionSet_IsNull(&OOSQL_GET_LOM_SYSTEMHANDLE(complexTypeInfo->systemHandle), complexTypeInfo->orn, SM_FALSE, 
                                            (TupleID*)&complexTypeInfo->oid, complexTypeInfo->colNo);
        if (isNull == SM_TRUE) OOSQL_ERROR(complexTypeInfo->systemHandle, eNULLCOLLECTION_OOSQL);

		e = LOM_CollectionSet_InsertElements(&OOSQL_GET_LOM_SYSTEMHANDLE(complexTypeInfo->systemHandle), complexTypeInfo->orn, SM_FALSE,
			                                 (TupleID*)&complexTypeInfo->oid, complexTypeInfo->colNo, nElements, elementSizes, elements);
		if(e < eNOERROR) OOSQL_ERROR(complexTypeInfo->systemHandle, e);
		break;
	case OOSQL_COMPLEXTYPE_BAG:
        isNull = (Boolean)LOM_CollectionBag_IsNull(&OOSQL_GET_LOM_SYSTEMHANDLE(complexTypeInfo->systemHandle), complexTypeInfo->orn, SM_FALSE, 
                                            (TupleID*)&complexTypeInfo->oid, complexTypeInfo->colNo);
        if (isNull == SM_TRUE) OOSQL_ERROR(complexTypeInfo->systemHandle, eNULLCOLLECTION_OOSQL);

        e = LOM_CollectionBag_InsertElements(&OOSQL_GET_LOM_SYSTEMHANDLE(complexTypeInfo->systemHandle), complexTypeInfo->orn, SM_FALSE,
			                                 (TupleID*)&complexTypeInfo->oid, complexTypeInfo->colNo, nElements, elementSizes, elements);
		if(e < eNOERROR) OOSQL_ERROR(complexTypeInfo->systemHandle, e);
		break;
	case OOSQL_COMPLEXTYPE_LIST:
        isNull = (Boolean)LOM_CollectionList_IsNull(&OOSQL_GET_LOM_SYSTEMHANDLE(complexTypeInfo->systemHandle), complexTypeInfo->orn, SM_FALSE, 
                                            (TupleID*)&complexTypeInfo->oid, complexTypeInfo->colNo);
        if (isNull == SM_TRUE) OOSQL_ERROR(complexTypeInfo->systemHandle, eNULLCOLLECTION_OOSQL);

        e = OOSQL_ComplexType_GetNumElements(complexTypeInfo, &nElementsStored);
        if(e < eNOERROR) OOSQL_ERROR(complexTypeInfo->systemHandle, e);

        if(start == -1 || start > nElementsStored || start < 0)
            start = nElementsStored;

		e = LOM_CollectionList_InsertElements(&OOSQL_GET_LOM_SYSTEMHANDLE(complexTypeInfo->systemHandle), complexTypeInfo->orn, SM_FALSE,
			                                  (TupleID*)&complexTypeInfo->oid, complexTypeInfo->colNo, start, nElements, elementSizes, elements);
		if(e < eNOERROR) OOSQL_ERROR(complexTypeInfo->systemHandle, e);
		break;
	default:
		return eBADPARAMETER_OOSQL;
	}
	return e;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
1                   
0                   
eBADPARAMETER_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_ComplexType_IsNULL(
	OOSQL_ComplexTypeInfo*  complexTypeInfo     // IN 
)
{
	Four e;
    Four numElements;
    Boolean isNull;

	switch(complexTypeInfo->complexType)
	{
	case OOSQL_COMPLEXTYPE_SET:
        isNull = (Boolean)LOM_CollectionSet_IsNull(&OOSQL_GET_LOM_SYSTEMHANDLE(complexTypeInfo->systemHandle), complexTypeInfo->orn, SM_FALSE, 
                                            (TupleID*)&complexTypeInfo->oid, complexTypeInfo->colNo);
		break;
	case OOSQL_COMPLEXTYPE_BAG:
        isNull = (Boolean)LOM_CollectionBag_IsNull(&OOSQL_GET_LOM_SYSTEMHANDLE(complexTypeInfo->systemHandle), complexTypeInfo->orn, SM_FALSE, 
                                            (TupleID*)&complexTypeInfo->oid, complexTypeInfo->colNo);
		break;
	case OOSQL_COMPLEXTYPE_LIST:
        isNull = (Boolean)LOM_CollectionList_IsNull(&OOSQL_GET_LOM_SYSTEMHANDLE(complexTypeInfo->systemHandle), complexTypeInfo->orn, SM_FALSE, 
                                            (TupleID*)&complexTypeInfo->oid, complexTypeInfo->colNo);
		break;
	default:
		return eBADPARAMETER_OOSQL;
	}
        return isNull;
}
