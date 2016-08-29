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
#ifndef ENABLE_OPENGIS_OPTIMIZATION
#include "OOSQL_APIs_Internal.hxx"
#include "Geometry.hxx"
#include <string.h>
#include <stdio.h>
#include "OOSQL_String.hxx"

#define OOSQL_EXTFUNC_1_ARGS \
	void*                       arg0,				\
	void*                       returnValue,		\
	int*                        returnValueLength,	\
	short*                      isNullArg0,			\
	short*                      isNullReturnValue,	\
	char*                       functionName,		\
	char*                       specificName,		\
	char*                       errorMessage,		\
	int*						argSizes,			\
	OOSQL_SystemHandle*         systemHandle,		\
	OOSQL_ScratchPad*			scrachPad,			\
	OOSQL_DbInfo*				dbInfo

#define OOSQL_EXTFUNC_2_ARGS \
	void*                       arg0,				\
	void*                       arg1,				\
	void*                       returnValue,		\
	int*                        returnValueLength,	\
	short*                      isNullArg0,			\
	short*                      isNullArg1,			\
	short*                      isNullReturnValue,	\
	char*                       functionName,		\
	char*                       specificName,		\
	char*                       errorMessage,		\
	int*						argSizes,			\
	OOSQL_SystemHandle*         systemHandle,		\
	OOSQL_ScratchPad*			scrachPad,			\
	OOSQL_DbInfo*				dbInfo

#define OOSQL_EXTFUNC_3_ARGS \
	void*                       arg0,				\
	void*                       arg1,				\
	void*                       arg2,				\
	void*                       returnValue,		\
	int*                        returnValueLength,	\
	short*                      isNullArg0,			\
	short*                      isNullArg1,			\
	short*                      isNullArg2,			\
	short*                      isNullReturnValue,	\
	char*                       functionName,		\
	char*                       specificName,		\
	char*                       errorMessage,		\
	int*						argSizes,			\
	OOSQL_SystemHandle*         systemHandle,		\
	OOSQL_ScratchPad*			scrachPad,			\
	OOSQL_DbInfo*				dbInfo

void GetWKBinaryFromUDT(char* data, int size, WKBinary& wkb);
Four CreateGeometryUDTFromText(OOSQL_SystemHandle* systemHandle, OOSQL_DbInfo* dbInfo, char* text, int srid, OOSQL_TCDynStr& result, char* errorMessage);
Four CreateGeometryUDTFromBinary(OOSQL_SystemHandle* systemHandle, OOSQL_DbInfo* dbInfo, char* binary, int size, int srid, OOSQL_TCDynStr& result, char* errorMessage);

extern "C" {

 int OGIS_GeometryFromText(OOSQL_EXTFUNC_2_ARGS)
{
	OOSQL_TCDynStr result(geoMemoryManager);
	Four e = CreateGeometryUDTFromText(systemHandle, dbInfo, (char*)arg0, *(int*)arg1, result, errorMessage);
	if(e < eNOERROR) return e;
	memcpy(returnValue, result.str(), result.length());
	*returnValueLength = result.length();
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

 int OGIS_PointFromText(OOSQL_EXTFUNC_2_ARGS)
{
	return OGIS_GeometryFromText(arg0, arg1, returnValue, returnValueLength,	isNullArg0, isNullArg1, isNullReturnValue,
							functionName, specificName, errorMessage, argSizes,
							systemHandle, scrachPad, dbInfo);
}

 int OGIS_LineStringFromText(OOSQL_EXTFUNC_2_ARGS)
{
	return OGIS_GeometryFromText(arg0, arg1, returnValue, returnValueLength,	isNullArg0, isNullArg1, isNullReturnValue,
							functionName, specificName, errorMessage, argSizes,
							systemHandle, scrachPad, dbInfo);
}

 int OGIS_PolygonFromText(OOSQL_EXTFUNC_2_ARGS)
{
	return OGIS_GeometryFromText(arg0, arg1, returnValue, returnValueLength,	isNullArg0, isNullArg1, isNullReturnValue,
							functionName, specificName, errorMessage, argSizes,
							systemHandle, scrachPad, dbInfo);
}

 int OGIS_MultiPointFromText(OOSQL_EXTFUNC_2_ARGS)
{
	return OGIS_GeometryFromText(arg0, arg1, returnValue, returnValueLength,	isNullArg0, isNullArg1, isNullReturnValue,
							functionName, specificName, errorMessage, argSizes,
							systemHandle, scrachPad, dbInfo);
}

 int OGIS_MultiLineStringFromText(OOSQL_EXTFUNC_2_ARGS)
{
	return OGIS_GeometryFromText(arg0, arg1, returnValue, returnValueLength,	isNullArg0, isNullArg1, isNullReturnValue,
							functionName, specificName, errorMessage, argSizes,
							systemHandle, scrachPad, dbInfo);
}

 int OGIS_MultiPolygonFromText(OOSQL_EXTFUNC_2_ARGS)
{
	return OGIS_GeometryFromText(arg0, arg1, returnValue, returnValueLength,	isNullArg0, isNullArg1, isNullReturnValue,
							functionName, specificName, errorMessage, argSizes,
							systemHandle, scrachPad, dbInfo);
}

 int OGIS_GeometryCollectionFromText(OOSQL_EXTFUNC_2_ARGS)
{
	return OGIS_GeometryFromText(arg0, arg1, returnValue, returnValueLength,	isNullArg0, isNullArg1, isNullReturnValue,
							functionName, specificName, errorMessage, argSizes,
							systemHandle, scrachPad, dbInfo);
}

 int OGIS_GeometryFromWKB(OOSQL_EXTFUNC_2_ARGS)
{
	OOSQL_TCDynStr result(geoMemoryManager);
	Four e = CreateGeometryUDTFromBinary(systemHandle, dbInfo, (char*)arg0, argSizes[0], *(int*)arg1, result, errorMessage);
	if(e < eNOERROR) return e;
	memcpy(returnValue, result.str(), result.length());
	*returnValueLength = result.length();
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

 int OGIS_PointFromWKB(OOSQL_EXTFUNC_2_ARGS)
{
	return OGIS_GeometryFromWKB(arg0, arg1, returnValue, returnValueLength,	isNullArg0, isNullArg1, isNullReturnValue,
						   functionName, specificName, errorMessage, argSizes,
						   systemHandle, scrachPad, dbInfo);
}

 int OGIS_LineStringFromWKB(OOSQL_EXTFUNC_2_ARGS)
{
	return OGIS_GeometryFromWKB(arg0, arg1, returnValue, returnValueLength,	isNullArg0, isNullArg1, isNullReturnValue,
						   functionName, specificName, errorMessage, argSizes,
						   systemHandle, scrachPad, dbInfo);
}

 int OGIS_PolygonFromWKB(OOSQL_EXTFUNC_2_ARGS)
{
	return OGIS_GeometryFromWKB(arg0, arg1, returnValue, returnValueLength,	isNullArg0, isNullArg1, isNullReturnValue,
						   functionName, specificName, errorMessage, argSizes,
						   systemHandle, scrachPad, dbInfo);
}

 int OGIS_MultiPointFromWKB(OOSQL_EXTFUNC_2_ARGS)
{
	return OGIS_GeometryFromWKB(arg0, arg1, returnValue, returnValueLength,	isNullArg0, isNullArg1, isNullReturnValue,
						   functionName, specificName, errorMessage, argSizes,
						   systemHandle, scrachPad, dbInfo);
}

 int OGIS_MultiLineStringFromWKB(OOSQL_EXTFUNC_2_ARGS)
{
	return OGIS_GeometryFromWKB(arg0, arg1, returnValue, returnValueLength,	isNullArg0, isNullArg1, isNullReturnValue,
						   functionName, specificName, errorMessage, argSizes,
						   systemHandle, scrachPad, dbInfo);
}

 int OGIS_MultiPolygonFromWKB(OOSQL_EXTFUNC_2_ARGS)
{
	return OGIS_GeometryFromWKB(arg0, arg1, returnValue, returnValueLength,	isNullArg0, isNullArg1, isNullReturnValue,
						   functionName, specificName, errorMessage, argSizes,
						   systemHandle, scrachPad, dbInfo);
}

 int OGIS_GeometryCollectionFromWKB(OOSQL_EXTFUNC_2_ARGS)
{
	return OGIS_GeometryFromWKB(arg0, arg1, returnValue, returnValueLength,	isNullArg0, isNullArg1, isNullReturnValue,
						   functionName, specificName, errorMessage, argSizes,
						   systemHandle, scrachPad, dbInfo);
}

 int OGIS_AsText(OOSQL_EXTFUNC_1_ARGS)
{
	WKBinary wkb;
	GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
	OOSQL_TCDynStr result(geoMemoryManager);
	result = WKBinaryToWKText(wkb);
	memcpy(returnValue, result.str(), result.length() + 1);
	*returnValueLength = result.length() + 1;
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

 int OGIS_AsBinary(OOSQL_EXTFUNC_1_ARGS)
{
	WKBinary wkb;
	GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
	memcpy(returnValue, wkb.GetBinary(), wkb.GetSize());
	*returnValueLength = wkb.GetSize();
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

 int OGIS_Dimension(OOSQL_EXTFUNC_1_ARGS)
{
	int dimension = 2;
	memcpy(returnValue, &dimension, sizeof(int));
	*returnValueLength = sizeof(int);
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

 int OGIS_Envelope(OOSQL_EXTFUNC_1_ARGS)
{
	WKBinary wkb;
	GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
    WKBinary result = wkb.Envelope();
	memcpy(returnValue, result.GetBinary(), result.GetSize());
	*returnValueLength = result.GetSize();
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

 int OGIS_GeometryType(OOSQL_EXTFUNC_1_ARGS)
{
	WKBinary wkb;
	GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
	char* typeName = wkb.GetGeometryTypeName();
	memcpy(returnValue, typeName, strlen(typeName) + 1);
	*returnValueLength = strlen(typeName) + 1;
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

 int OGIS_SRID(OOSQL_EXTFUNC_1_ARGS)
{
	OOSQL_UDTObject udt;

	OOSQL_UDTObject_SetData(&udt, (char*)arg0);
	OOSQL_UDTObject_SetSize(&udt, argSizes[0]);

	int srid = *(int*)OOSQL_UDTObject_GetNTHAttrData(&udt, 4);
	memcpy(returnValue, &srid, sizeof(int));
	*returnValueLength = sizeof(int);
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

 int OGIS_Boundary(OOSQL_EXTFUNC_1_ARGS)
{
	WKBinary wkb;
	GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
    WKBinary result = wkb.Boundary();
	memcpy(returnValue, result.GetBinary(), result.GetSize());
	*returnValueLength = result.GetSize();
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

 int OGIS_IsEmpty(OOSQL_EXTFUNC_1_ARGS)
{
	WKBinary wkb;
	GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
    int result = wkb.IsEmpty();
	memcpy(returnValue, &result, sizeof(int));
	*returnValueLength = sizeof(int);
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

 int OGIS_IsSimple(OOSQL_EXTFUNC_1_ARGS)
{
	WKBinary wkb;
	GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
    int result = wkb.IsSimple();
	memcpy(returnValue, &result, sizeof(int));
	*returnValueLength = sizeof(int);
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

 int OGIS_X(OOSQL_EXTFUNC_1_ARGS)
{
	WKBinary wkb;
	GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
	float x = wkb.GetPoint().GetX();
	memcpy(returnValue, &x, sizeof(float));
	*returnValueLength = sizeof(float);
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

 int OGIS_Y(OOSQL_EXTFUNC_1_ARGS)
{
	WKBinary wkb;
	GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
	float y = wkb.GetPoint().GetY();
	memcpy(returnValue, &y, sizeof(float));
	*returnValueLength = sizeof(float);
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

 int OGIS_PointN(OOSQL_EXTFUNC_2_ARGS)
{
	OOSQL_UDTObject udt;

	OOSQL_UDTObject_SetData(&udt, (char*)arg0);
	OOSQL_UDTObject_SetSize(&udt, argSizes[0]);

	int srid = *(int*)OOSQL_UDTObject_GetNTHAttrData(&udt, 4);

	WKBinary wkb;
	GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
	int ith = *(int*)arg1;
	if(ith < 0)
		ith = wkb.GetNumPoints() + ith;
	WKBinary wkbpoint = wkb.GetPointAsWKBinary(ith);

	OOSQL_TCDynStr result(geoMemoryManager);
	Four e = CreateGeometryUDTFromBinary(systemHandle, dbInfo, wkbpoint.GetBinary(), wkbpoint.GetSize(), srid, result, errorMessage);
	if(e < eNOERROR) return e;

	memcpy(returnValue, result.str(), result.length());
	*returnValueLength = result.length();
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

 int OGIS_StartPoint(OOSQL_EXTFUNC_1_ARGS)
{
	int ith = 0;
	short isNullArg1 = 0;
	return OGIS_PointN(arg0, &ith, returnValue, returnValueLength, isNullArg0, &isNullArg1, isNullReturnValue,
				       functionName, specificName, errorMessage, argSizes,
				       systemHandle, scrachPad, dbInfo);

}

 int OGIS_EndPoint(OOSQL_EXTFUNC_1_ARGS)
{
	int   ith = -1;
	short isNullArg1 = 0;
	return OGIS_PointN(arg0, &ith, returnValue, returnValueLength, isNullArg0, &isNullArg1, isNullReturnValue,
				       functionName, specificName, errorMessage, argSizes,
				       systemHandle, scrachPad, dbInfo);
}

 int OGIS_Length(OOSQL_EXTFUNC_1_ARGS)
{
	WKBinary wkb;
	GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
    float result = wkb.Length();
	memcpy(returnValue, &result, sizeof(float));
	*returnValueLength = sizeof(float);
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

 int OGIS_IsClosed(OOSQL_EXTFUNC_1_ARGS)
{
	WKBinary wkb;
	GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
    int result = wkb.IsClosed();
	memcpy(returnValue, &result, sizeof(int));
	*returnValueLength = sizeof(int);
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

 int OGIS_NumPoints(OOSQL_EXTFUNC_1_ARGS)
{
	WKBinary wkb;
	GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
	int numPoints = wkb.GetNumPoints();
	memcpy(returnValue, &numPoints, sizeof(int));
	*returnValueLength = sizeof(int);
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}


 int OGIS_IsRing(OOSQL_EXTFUNC_1_ARGS)
{
	WKBinary wkb;
	GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
    int result = wkb.IsRing();
	memcpy(returnValue, &result, sizeof(int));
	*returnValueLength = sizeof(int);
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

 int OGIS_ExteriorRing(OOSQL_EXTFUNC_1_ARGS)
{
	WKBinary wkb;
	GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
    WKBinary result = wkb.ExteriorRing();
	memcpy(returnValue, result.GetBinary(), result.GetSize());
	*returnValueLength = result.GetSize();
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

 int OGIS_InteriorRingN(OOSQL_EXTFUNC_2_ARGS)
{
	WKBinary wkb;
	GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
    int ith = *(int*)arg1;
    WKBinary result = wkb.InteriorRingN(ith);
	memcpy(returnValue, result.GetBinary(), result.GetSize());
	*returnValueLength = result.GetSize();
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

 int OGIS_NumInteriorRings(OOSQL_EXTFUNC_1_ARGS)
{
	WKBinary wkb;
	GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
    int result = wkb.NumInteriorRings();
	memcpy(returnValue, &result, sizeof(int));
	*returnValueLength = sizeof(int);
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

 int OGIS_Area(OOSQL_EXTFUNC_1_ARGS)
{
	WKBinary wkb;
	GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
    float result = wkb.Area();
	memcpy(returnValue, &result, sizeof(float));
	*returnValueLength = sizeof(float);
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

 int OGIS_Centroid(OOSQL_EXTFUNC_1_ARGS)
{
	WKBinary wkb;
	GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
    WKBinary result = wkb.Centroid();
	memcpy(returnValue, result.GetBinary(), result.GetSize());
	*returnValueLength = result.GetSize();
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

 int OGIS_PointOnSurface(OOSQL_EXTFUNC_1_ARGS)
{
	WKBinary wkb;
	GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
    WKBinary result = wkb.PointOnSurface();
	memcpy(returnValue, result.GetBinary(), result.GetSize());
	*returnValueLength = result.GetSize();
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

 int OGIS_GeometryN(OOSQL_EXTFUNC_2_ARGS)
{
	WKBinary wkb;
	GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
    int ith = *(int*)arg1;
    WKBinary result = wkb.GeometryN(ith);
	memcpy(returnValue, result.GetBinary(), result.GetSize());
	*returnValueLength = result.GetSize();
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

 int OGIS_NumGeometries(OOSQL_EXTFUNC_1_ARGS)
{
	WKBinary wkb;
	GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
    int result = wkb.GetNumGeometries();
	memcpy(returnValue, &result, sizeof(int));
	*returnValueLength = sizeof(int);
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

 int OGIS_Buffer(OOSQL_EXTFUNC_2_ARGS)
{
	WKBinary wkb;
	GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
    WKBinary result = wkb.Buffer();
	memcpy(returnValue, result.GetBinary(), result.GetSize());
	*returnValueLength = result.GetSize();
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

 int OGIS_ConvexHull(OOSQL_EXTFUNC_1_ARGS)
{
	WKBinary wkb;
	GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
    WKBinary result = wkb.ConvexHull();
	memcpy(returnValue, result.GetBinary(), result.GetSize());
	*returnValueLength = result.GetSize();
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

 int OGIS_Difference(OOSQL_EXTFUNC_2_ARGS)
{
	WKBinary wkb1, wkb2;
	GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb1);
    GetWKBinaryFromUDT((char*)arg1, argSizes[1], wkb2);
    WKBinary result = wkb1.Difference(wkb2);
	memcpy(returnValue, result.GetBinary(), result.GetSize());
	*returnValueLength = result.GetSize();
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

 int OGIS_Intersection(OOSQL_EXTFUNC_2_ARGS)
{
	WKBinary wkb1, wkb2;
	GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb1);
    GetWKBinaryFromUDT((char*)arg1, argSizes[1], wkb2);
    WKBinary result = wkb1.Intersection(wkb2);
	memcpy(returnValue, result.GetBinary(), result.GetSize());
	*returnValueLength = result.GetSize();
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

 int OGIS_SymDifference(OOSQL_EXTFUNC_2_ARGS)
{
	WKBinary wkb1, wkb2;
	GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb1);
    GetWKBinaryFromUDT((char*)arg1, argSizes[1], wkb2);
    WKBinary result = wkb1.SymDifference(wkb2);
	memcpy(returnValue, result.GetBinary(), result.GetSize());
	*returnValueLength = result.GetSize();
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

 int OGIS_Union(OOSQL_EXTFUNC_2_ARGS)
{
	WKBinary wkb1, wkb2;
	GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb1);
    GetWKBinaryFromUDT((char*)arg1, argSizes[1], wkb2);
    WKBinary result = wkb1.Union(wkb2);
	memcpy(returnValue, result.GetBinary(), result.GetSize());
	*returnValueLength = result.GetSize();
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

 int OGIS_Contains(OOSQL_EXTFUNC_2_ARGS)
{
	WKBinary wkb1, wkb2;
	GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb1);
    GetWKBinaryFromUDT((char*)arg1, argSizes[1], wkb2);
    int result = wkb1.Contains(wkb2);
	memcpy(returnValue, &result, sizeof(int));
	*returnValueLength = sizeof(int);
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

 int OGIS_Crosses(OOSQL_EXTFUNC_2_ARGS)
{
	WKBinary wkb1, wkb2;
	GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb1);
    GetWKBinaryFromUDT((char*)arg1, argSizes[1], wkb2);
    int result = wkb1.Crosses(wkb2);
	memcpy(returnValue, &result, sizeof(int));
	*returnValueLength = sizeof(int);
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

 int OGIS_Disjoint(OOSQL_EXTFUNC_2_ARGS)
{
	WKBinary wkb1, wkb2;
	GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb1);
    GetWKBinaryFromUDT((char*)arg1, argSizes[1], wkb2);
    int result = wkb1.Disjoint(wkb2);
	memcpy(returnValue, &result, sizeof(int));
	*returnValueLength = sizeof(int);
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

 int OGIS_Distance(OOSQL_EXTFUNC_2_ARGS)
{
	WKBinary wkb1, wkb2;
	GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb1);
    GetWKBinaryFromUDT((char*)arg1, argSizes[1], wkb2);
    float result = wkb1.Distance(wkb2);
	memcpy(returnValue, &result, sizeof(float));
	*returnValueLength = sizeof(float);
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

 int OGIS_Equals(OOSQL_EXTFUNC_2_ARGS)
{
	WKBinary wkb1, wkb2;
	GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb1);
    GetWKBinaryFromUDT((char*)arg1, argSizes[1], wkb2);
    int result = wkb1.Equals(wkb2);
	memcpy(returnValue, &result, sizeof(int));
	*returnValueLength = sizeof(int);
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

 int OGIS_Intersects(OOSQL_EXTFUNC_2_ARGS)
{
	WKBinary wkb1, wkb2;
	GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb1);
    GetWKBinaryFromUDT((char*)arg1, argSizes[1], wkb2);
    int result = wkb1.Intersects(wkb2);
	memcpy(returnValue, &result, sizeof(int));
	*returnValueLength = sizeof(int);
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

 int OGIS_Overlaps(OOSQL_EXTFUNC_2_ARGS)
{
	WKBinary wkb1, wkb2;
	GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb1);
    GetWKBinaryFromUDT((char*)arg1, argSizes[1], wkb2);
    int result = wkb1.Overlaps(wkb2);
	memcpy(returnValue, &result, sizeof(int));
	*returnValueLength = sizeof(int);
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

 int OGIS_Related(OOSQL_EXTFUNC_3_ARGS)
{
	WKBinary wkb1, wkb2;
	GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb1);
    GetWKBinaryFromUDT((char*)arg1, argSizes[1], wkb2);
    char* patternMatrix = (char*)arg2;
    int result = wkb1.Related(wkb2, patternMatrix);
	memcpy(returnValue, &result, sizeof(int));
	*returnValueLength = sizeof(int);
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

 int OGIS_Touches(OOSQL_EXTFUNC_2_ARGS)
{
	WKBinary wkb1, wkb2;
	GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb1);
    GetWKBinaryFromUDT((char*)arg1, argSizes[1], wkb2);
    int result = wkb1.Touches(wkb2);
	memcpy(returnValue, &result, sizeof(int));
	*returnValueLength = sizeof(int);
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

 int OGIS_Within(OOSQL_EXTFUNC_2_ARGS)
{
	WKBinary wkb1, wkb2;
	GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb1);
    GetWKBinaryFromUDT((char*)arg1, argSizes[1], wkb2);
    int result = wkb1.Within(wkb2);
	memcpy(returnValue, &result, sizeof(int));
	*returnValueLength = sizeof(int);
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}
} /* extern "C" */

void GetWKBinaryFromUDT(char* data, int size, WKBinary& wkb)
{
	OOSQL_UDTObject udt;

	OOSQL_UDTObject_SetData(&udt, data);
	OOSQL_UDTObject_SetSize(&udt, size);

	char* binary = OOSQL_UDTObject_GetNTHAttrData(&udt, 5);
	int   length = OOSQL_UDTObject_GetNTHAttrLength(&udt, 5);

	wkb.Init(binary, length);
}

Four CreateGeometryUDTFromText(OOSQL_SystemHandle* systemHandle, OOSQL_DbInfo* dbInfo, char* text, int srid, OOSQL_TCDynStr& result, char* errorMessage)
{
	OOSQL_UDTObject udt;
	Four			e;
	float			xmin, ymin, xmax, ymax;
	WKBinary		wkb;

	try {
		wkb = WKTextToWKBinary(text);
	}
	catch(parse_error& e)
	{
		strcpy(errorMessage, e.what());
		return -1;
	}


	wkb.GetMBR(&xmin, &ymin, &xmax, &ymax);

	OOSQL_UDTObject_AllocBuffer(&udt, UDTOBJECT_DEFAULT_SIZE);
	OOSQL_UDTObject_SetNAttrs(&udt, 6);
	OOSQL_UDTObject_SetLength(&udt, UDTOBJECT_HEADER_SIZE(6));

	OOSQL_UDTObject_SetNTHAttrType(&udt, 0, OOSQL_TYPE_FLOAT);
	OOSQL_UDTObject_SetNTHAttrOffset(&udt, 0, -1);

	OOSQL_UDTObject_SetNTHAttrType(&udt, 1, OOSQL_TYPE_FLOAT);
	OOSQL_UDTObject_SetNTHAttrOffset(&udt, 1, -1);

	OOSQL_UDTObject_SetNTHAttrType(&udt, 2, OOSQL_TYPE_FLOAT);
	OOSQL_UDTObject_SetNTHAttrOffset(&udt, 2, -1);

	OOSQL_UDTObject_SetNTHAttrType(&udt, 3, OOSQL_TYPE_FLOAT);
	OOSQL_UDTObject_SetNTHAttrOffset(&udt, 3, -1);

	OOSQL_UDTObject_SetNTHAttrType(&udt, 4, OOSQL_TYPE_INT);
	OOSQL_UDTObject_SetNTHAttrOffset(&udt, 4, -1);

	OOSQL_UDTObject_SetNTHAttrType(&udt, 5, OOSQL_TYPE_VARSTRING);
	OOSQL_UDTObject_SetNTHAttrOffset(&udt, 5, -1);

	OOSQL_UDTObject_SetNTHAttrData(&udt, 0, 4, &xmin);
	OOSQL_UDTObject_SetNTHAttrData(&udt, 1, 4, &ymin);
	OOSQL_UDTObject_SetNTHAttrData(&udt, 2, 4, &xmax);
	OOSQL_UDTObject_SetNTHAttrData(&udt, 3, 4, &ymax);
	OOSQL_UDTObject_SetNTHAttrData(&udt, 4, 4, &srid);
	OOSQL_UDTObject_SetNTHAttrData(&udt, 5, (int)wkb.GetSize(), wkb.GetBinary());

	result.resize(OOSQL_UDTObject_GetLength(&udt));
	memcpy((char*)result.str(), OOSQL_UDTObject_GetData(&udt), OOSQL_UDTObject_GetLength(&udt));

	OOSQL_UDTObject_FreeBuffer(&udt);

    return eNOERROR;
}

Four CreateGeometryUDTFromBinary(OOSQL_SystemHandle* systemHandle, OOSQL_DbInfo* dbInfo, char* binary, int size, int srid, OOSQL_TCDynStr& result, char* errorMessage)
{
	OOSQL_UDTObject udt;
	Four			e;
	float			xmin, ymin, xmax, ymax;
	WKBinary		wkb(binary, size);

	wkb.GetMBR(&xmin, &ymin, &xmax, &ymax);

	OOSQL_UDTObject_AllocBuffer(&udt, UDTOBJECT_DEFAULT_SIZE);
	OOSQL_UDTObject_SetNAttrs(&udt, 6);
	OOSQL_UDTObject_SetLength(&udt, UDTOBJECT_HEADER_SIZE(6));

	OOSQL_UDTObject_SetNTHAttrType(&udt, 0, OOSQL_TYPE_FLOAT);
	OOSQL_UDTObject_SetNTHAttrOffset(&udt, 0, -1);

	OOSQL_UDTObject_SetNTHAttrType(&udt, 1, OOSQL_TYPE_FLOAT);
	OOSQL_UDTObject_SetNTHAttrOffset(&udt, 1, -1);

	OOSQL_UDTObject_SetNTHAttrType(&udt, 2, OOSQL_TYPE_FLOAT);
	OOSQL_UDTObject_SetNTHAttrOffset(&udt, 2, -1);

	OOSQL_UDTObject_SetNTHAttrType(&udt, 3, OOSQL_TYPE_FLOAT);
	OOSQL_UDTObject_SetNTHAttrOffset(&udt, 3, -1);

	OOSQL_UDTObject_SetNTHAttrType(&udt, 4, OOSQL_TYPE_INT);
	OOSQL_UDTObject_SetNTHAttrOffset(&udt, 4, -1);

	OOSQL_UDTObject_SetNTHAttrType(&udt, 5, OOSQL_TYPE_VARSTRING);
	OOSQL_UDTObject_SetNTHAttrOffset(&udt, 5, -1);

	OOSQL_UDTObject_SetNTHAttrData(&udt, 0, 4, &xmin);
	OOSQL_UDTObject_SetNTHAttrData(&udt, 1, 4, &ymin);
	OOSQL_UDTObject_SetNTHAttrData(&udt, 2, 4, &xmax);
	OOSQL_UDTObject_SetNTHAttrData(&udt, 3, 4, &ymax);
	OOSQL_UDTObject_SetNTHAttrData(&udt, 4, 4, &srid);
	OOSQL_UDTObject_SetNTHAttrData(&udt, 5, (int)wkb.GetSize(), wkb.GetBinary());

	result.resize(OOSQL_UDTObject_GetLength(&udt));
	memcpy((char*)result.str(), OOSQL_UDTObject_GetData(&udt), OOSQL_UDTObject_GetLength(&udt));

	OOSQL_UDTObject_FreeBuffer(&udt);

    return eNOERROR;
}

#else

#include "OOSQL_APIs_Internal.hxx"
#include "Geometry.hxx"
#include <string.h>
#include <stdio.h>
#include "OOSQL_String.hxx"

extern WKBinaryHolder* wkbHolder;
WKBinary wkb, wkb1, wkb2;

#define OOSQL_EXTFUNC_1_ARGS \
	void*                       arg0,				\
	void*                       returnValue,		\
	int*                        returnValueLength,	\
	short*                      isNullArg0,			\
	short*                      isNullReturnValue,	\
	char*                       functionName,		\
	char*                       specificName,		\
	char*                       errorMessage,		\
	int*						argSizes,			\
	OOSQL_SystemHandle*         systemHandle,		\
	OOSQL_ScratchPad*			scrachPad,			\
	OOSQL_DbInfo*				dbInfo,				\
	int							UDFNo,				\
	Boolean*						isConstantArg

#define OOSQL_EXTFUNC_2_ARGS \
	void*                       arg0,				\
	void*                       arg1,				\
	void*                       returnValue,		\
	int*                        returnValueLength,	\
	short*                      isNullArg0,			\
	short*                      isNullArg1,			\
	short*                      isNullReturnValue,	\
	char*                       functionName,		\
	char*                       specificName,		\
	char*                       errorMessage,		\
	int*						argSizes,			\
	OOSQL_SystemHandle*         systemHandle,		\
	OOSQL_ScratchPad*			scrachPad,			\
	OOSQL_DbInfo*				dbInfo,				\
	int							UDFNo,				\
	Boolean*						isConstantArg

#define OOSQL_EXTFUNC_3_ARGS \
	void*                       arg0,				\
	void*                       arg1,				\
	void*                       arg2,				\
	void*                       returnValue,		\
	int*                        returnValueLength,	\
	short*                      isNullArg0,			\
	short*                      isNullArg1,			\
	short*                      isNullArg2,			\
	short*                      isNullReturnValue,	\
	char*                       functionName,		\
	char*                       specificName,		\
	char*                       errorMessage,		\
	int*						argSizes,			\
	OOSQL_SystemHandle*         systemHandle,		\
	OOSQL_ScratchPad*			scrachPad,			\
	OOSQL_DbInfo*				dbInfo,				\
	int							UDFNo,				\
	Boolean*						isConstantArg

void GetWKBinaryFromUDT(char* data, int size, WKBinary& wkb);
Four CreateGeometryUDTFromText(OOSQL_SystemHandle* systemHandle, OOSQL_DbInfo* dbInfo, char* text, int srid, OOSQL_TCDynStr& result, char* errorMessage);
Four CreateGeometryUDTFromBinary(OOSQL_SystemHandle* systemHandle, OOSQL_DbInfo* dbInfo, char* binary, int size, int srid, OOSQL_TCDynStr& result, char* errorMessage);

extern "C" {

int OGIS_GeometryFromText(OOSQL_EXTFUNC_2_ARGS)
{
	OOSQL_TCDynStr result(geoMemoryManager);
	Four e = CreateGeometryUDTFromText(systemHandle, dbInfo, (char*)arg0, *(int*)arg1, result, errorMessage);
	if(e < eNOERROR) return e;
	memcpy(returnValue, result.str(), result.length());
	*returnValueLength = result.length();
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

int OGIS_PointFromText(OOSQL_EXTFUNC_2_ARGS)
{
	return OGIS_GeometryFromText(arg0, arg1, returnValue, returnValueLength,	isNullArg0, isNullArg1, isNullReturnValue,
							functionName, specificName, errorMessage, argSizes,
							systemHandle, scrachPad, dbInfo,
							UDFNo, isConstantArg /* not used in this function */
							);
}

int OGIS_LineStringFromText(OOSQL_EXTFUNC_2_ARGS)
{
	return OGIS_GeometryFromText(arg0, arg1, returnValue, returnValueLength,	isNullArg0, isNullArg1, isNullReturnValue,
							functionName, specificName, errorMessage, argSizes,
							systemHandle, scrachPad, dbInfo,
							UDFNo, isConstantArg /* not used in this function */
							);
}

int OGIS_PolygonFromText(OOSQL_EXTFUNC_2_ARGS)
{
	return OGIS_GeometryFromText(arg0, arg1, returnValue, returnValueLength,	isNullArg0, isNullArg1, isNullReturnValue,
							functionName, specificName, errorMessage, argSizes,
							systemHandle, scrachPad, dbInfo,
							UDFNo, isConstantArg /* not used in this function */
							);
}

int OGIS_MultiPointFromText(OOSQL_EXTFUNC_2_ARGS)
{
	return OGIS_GeometryFromText(arg0, arg1, returnValue, returnValueLength,	isNullArg0, isNullArg1, isNullReturnValue,
							functionName, specificName, errorMessage, argSizes,
							systemHandle, scrachPad, dbInfo,
							UDFNo, isConstantArg /* not used in this function */
							);
}

int OGIS_MultiLineStringFromText(OOSQL_EXTFUNC_2_ARGS)
{
	return OGIS_GeometryFromText(arg0, arg1, returnValue, returnValueLength,	isNullArg0, isNullArg1, isNullReturnValue,
							functionName, specificName, errorMessage, argSizes,
							systemHandle, scrachPad, dbInfo,
							UDFNo, isConstantArg /* not used in this function */
							);
}

int OGIS_MultiPolygonFromText(OOSQL_EXTFUNC_2_ARGS)
{
	return OGIS_GeometryFromText(arg0, arg1, returnValue, returnValueLength,	isNullArg0, isNullArg1, isNullReturnValue,
							functionName, specificName, errorMessage, argSizes,
							systemHandle, scrachPad, dbInfo,
							UDFNo, isConstantArg /* not used in this function */
							);
}

int OGIS_GeometryCollectionFromText(OOSQL_EXTFUNC_2_ARGS)
{
	return OGIS_GeometryFromText(arg0, arg1, returnValue, returnValueLength,	isNullArg0, isNullArg1, isNullReturnValue,
							functionName, specificName, errorMessage, argSizes,
							systemHandle, scrachPad, dbInfo,
							UDFNo, isConstantArg /* not used in this function */
							);
}

int OGIS_GeometryFromWKB(OOSQL_EXTFUNC_2_ARGS)
{
	OOSQL_TCDynStr result(geoMemoryManager);
	Four e = CreateGeometryUDTFromBinary(systemHandle, dbInfo, (char*)arg0, argSizes[0], *(int*)arg1, result, errorMessage);
	if(e < eNOERROR) return e;
	memcpy(returnValue, result.str(), result.length());
	*returnValueLength = result.length();
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

int OGIS_PointFromWKB(OOSQL_EXTFUNC_2_ARGS)
{
	return OGIS_GeometryFromWKB(arg0, arg1, returnValue, returnValueLength,	isNullArg0, isNullArg1, isNullReturnValue,
						   functionName, specificName, errorMessage, argSizes,
						   systemHandle, scrachPad, dbInfo,
							UDFNo, isConstantArg /* not used in this function */
							);
}

int OGIS_LineStringFromWKB(OOSQL_EXTFUNC_2_ARGS)
{
	return OGIS_GeometryFromWKB(arg0, arg1, returnValue, returnValueLength,	isNullArg0, isNullArg1, isNullReturnValue,
						   functionName, specificName, errorMessage, argSizes,
						   systemHandle, scrachPad, dbInfo,
							UDFNo, isConstantArg /* not used in this function */
							);
}

int OGIS_PolygonFromWKB(OOSQL_EXTFUNC_2_ARGS)
{
	return OGIS_GeometryFromWKB(arg0, arg1, returnValue, returnValueLength,	isNullArg0, isNullArg1, isNullReturnValue,
						   functionName, specificName, errorMessage, argSizes,
						   systemHandle, scrachPad, dbInfo,
							UDFNo, isConstantArg /* not used in this function */
							);
}

int OGIS_MultiPointFromWKB(OOSQL_EXTFUNC_2_ARGS)
{
	return OGIS_GeometryFromWKB(arg0, arg1, returnValue, returnValueLength,	isNullArg0, isNullArg1, isNullReturnValue,
						   functionName, specificName, errorMessage, argSizes,
						   systemHandle, scrachPad, dbInfo,
							UDFNo, isConstantArg /* not used in this function */
							);
}

int OGIS_MultiLineStringFromWKB(OOSQL_EXTFUNC_2_ARGS)
{
	return OGIS_GeometryFromWKB(arg0, arg1, returnValue, returnValueLength,	isNullArg0, isNullArg1, isNullReturnValue,
						   functionName, specificName, errorMessage, argSizes,
						   systemHandle, scrachPad, dbInfo,
							UDFNo, isConstantArg /* not used in this function */
							);
}

int OGIS_MultiPolygonFromWKB(OOSQL_EXTFUNC_2_ARGS)
{
	return OGIS_GeometryFromWKB(arg0, arg1, returnValue, returnValueLength,	isNullArg0, isNullArg1, isNullReturnValue,
						   functionName, specificName, errorMessage, argSizes,
						   systemHandle, scrachPad, dbInfo,
							UDFNo, isConstantArg /* not used in this function */
							);
}

int OGIS_GeometryCollectionFromWKB(OOSQL_EXTFUNC_2_ARGS)
{
	return OGIS_GeometryFromWKB(arg0, arg1, returnValue, returnValueLength,	isNullArg0, isNullArg1, isNullReturnValue,
						   functionName, specificName, errorMessage, argSizes,
						   systemHandle, scrachPad, dbInfo,
							UDFNo, isConstantArg /* not used in this function */
							);
}

int OGIS_AsText(OOSQL_EXTFUNC_1_ARGS)
{
	WKBinary wkb;
	OOSQL_TCDynStr result(geoMemoryManager);


	// get first argument
	if (isConstantArg[0])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
			wkbHolder->setWKBinary(UDFNo, wkb);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb);
		}
	}
	else // first argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
	}

	result = WKBinaryToWKText(wkb);
	memcpy(returnValue, result.str(), result.length() + 1);
	*returnValueLength = result.length() + 1;
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

int OGIS_AsBinary(OOSQL_EXTFUNC_1_ARGS)
{
	WKBinary wkb;


	// get first argument
	if (isConstantArg[0])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
			wkbHolder->setWKBinary(UDFNo, wkb);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb);
		}
	}
	else // first argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
	}
	memcpy(returnValue, wkb.GetBinary(), wkb.GetSize());
	*returnValueLength = wkb.GetSize();
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

int OGIS_Dimension(OOSQL_EXTFUNC_1_ARGS)
{
	int dimension = 2;
	memcpy(returnValue, &dimension, sizeof(int));
	*returnValueLength = sizeof(int);
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

int OGIS_Envelope(OOSQL_EXTFUNC_1_ARGS)
{
	WKBinary wkb;
	WKBinary   result;


	// get first argument
	if (isConstantArg[0])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
			wkbHolder->setWKBinary(UDFNo, wkb);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb);
		}
	}
	else // first argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
	}

	result = wkb.Envelope();
	memcpy(returnValue, result.GetBinary(), result.GetSize());
	*returnValueLength = result.GetSize();
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

int OGIS_GeometryType(OOSQL_EXTFUNC_1_ARGS)
{
	WKBinary wkb;
	char* typeName;


	// get first argument
	if (isConstantArg[0])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
			wkbHolder->setWKBinary(UDFNo, wkb);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb);
		}
	}
	else // first argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
	}

	typeName = wkb.GetGeometryTypeName();
	memcpy(returnValue, typeName, strlen(typeName) + 1);
	*returnValueLength = strlen(typeName) + 1;
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

int OGIS_SRID(OOSQL_EXTFUNC_1_ARGS)
{
	OOSQL_UDTObject udt;

	OOSQL_UDTObject_SetData(&udt, (char*)arg0);
	OOSQL_UDTObject_SetSize(&udt, argSizes[0]);

	int srid = *(int*)OOSQL_UDTObject_GetNTHAttrData(&udt, 4);
	memcpy(returnValue, &srid, sizeof(int));
	*returnValueLength = sizeof(int);
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

int OGIS_Boundary(OOSQL_EXTFUNC_1_ARGS)
{
	WKBinary wkb;
	WKBinary result;


	// get first argument
	if (isConstantArg[0])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
			wkbHolder->setWKBinary(UDFNo, wkb);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb);
		}
	}
	else // first argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
	}

    result = wkb.Boundary();
	memcpy(returnValue, result.GetBinary(), result.GetSize());
	*returnValueLength = result.GetSize();
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

int OGIS_IsEmpty(OOSQL_EXTFUNC_1_ARGS)
{
	WKBinary wkb;
	int result;


	// get first argument
	if (isConstantArg[0])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
			wkbHolder->setWKBinary(UDFNo, wkb);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb);
		}
	}
	else // first argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
	}

    result = wkb.IsEmpty();
	memcpy(returnValue, &result, sizeof(int));
	*returnValueLength = sizeof(int);
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

int OGIS_IsSimple(OOSQL_EXTFUNC_1_ARGS)
{
	WKBinary wkb;
	int result;


	// get first argument
	if (isConstantArg[0])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
			wkbHolder->setWKBinary(UDFNo, wkb);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb);
		}
	}
	else // first argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
	}
	
	result = wkb.IsSimple();
	memcpy(returnValue, &result, sizeof(int));
	*returnValueLength = sizeof(int);
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

int OGIS_X(OOSQL_EXTFUNC_1_ARGS)
{
	WKBinary wkb;
	float x;


	// get first argument
	if (isConstantArg[0])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
			wkbHolder->setWKBinary(UDFNo, wkb);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb);
		}
	}
	else // first argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
	}

	x = wkb.GetPoint().GetX();
	memcpy(returnValue, &x, sizeof(float));
	*returnValueLength = sizeof(float);
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

int OGIS_Y(OOSQL_EXTFUNC_1_ARGS)
{
	WKBinary wkb;
	float y;


	// get first argument
	if (isConstantArg[0])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
			wkbHolder->setWKBinary(UDFNo, wkb);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb);
		}
	}
	else // first argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
	}

	y = wkb.GetPoint().GetY();
	memcpy(returnValue, &y, sizeof(float));
	*returnValueLength = sizeof(float);
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

int OGIS_PointN(OOSQL_EXTFUNC_2_ARGS)
{
	OOSQL_UDTObject udt;

	OOSQL_UDTObject_SetData(&udt, (char*)arg0);
	OOSQL_UDTObject_SetSize(&udt, argSizes[0]);

	int srid = *(int*)OOSQL_UDTObject_GetNTHAttrData(&udt, 4);

	WKBinary	wkb;
	int			ith;
	WKBinary	wkbpoint;
	OOSQL_TCDynStr result(geoMemoryManager);
	Four		e;

	// get first argument
	if (isConstantArg[0])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
			wkbHolder->setWKBinary(UDFNo, wkb);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb);
		}
	}
	else // first argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
	}

	ith = *(int*)arg1;
	if(ith < 0)
		ith = wkb.GetNumPoints() + ith;
	wkbpoint = wkb.GetPointAsWKBinary(ith);

	e = CreateGeometryUDTFromBinary(systemHandle, dbInfo, wkbpoint.GetBinary(), wkbpoint.GetSize(), srid, result, errorMessage);
	if(e < eNOERROR) return e;

	memcpy(returnValue, result.str(), result.length());
	*returnValueLength = result.length();
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

int OGIS_StartPoint(OOSQL_EXTFUNC_1_ARGS)
{
	int ith = 0;
	short isNullArg1 = 0;
	return OGIS_PointN(arg0, &ith, returnValue, returnValueLength, isNullArg0, &isNullArg1, isNullReturnValue,
				       functionName, specificName, errorMessage, argSizes,
				       systemHandle, scrachPad, dbInfo,
							UDFNo, isConstantArg /* not used in this function */
							);

}

int OGIS_EndPoint(OOSQL_EXTFUNC_1_ARGS)
{
	int   ith = -1;
	short isNullArg1 = 0;
	return OGIS_PointN(arg0, &ith, returnValue, returnValueLength, isNullArg0, &isNullArg1, isNullReturnValue,
				       functionName, specificName, errorMessage, argSizes,
				       systemHandle, scrachPad, dbInfo,
							UDFNo, isConstantArg /* not used in this function */
							);
}

int OGIS_Length(OOSQL_EXTFUNC_1_ARGS)
{
	WKBinary wkb;
	float result;


	// get first argument
	if (isConstantArg[0])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
			wkbHolder->setWKBinary(UDFNo, wkb);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb);
		}
	}
	else // first argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
	}

    result = wkb.Length();
	memcpy(returnValue, &result, sizeof(float));
	*returnValueLength = sizeof(float);
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

int OGIS_IsClosed(OOSQL_EXTFUNC_1_ARGS)
{
	WKBinary wkb;
	int result;


	// get first argument
	if (isConstantArg[0])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
			wkbHolder->setWKBinary(UDFNo, wkb);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb);
		}
	}
	else // first argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
	}

    result = wkb.IsClosed();
	memcpy(returnValue, &result, sizeof(int));
	*returnValueLength = sizeof(int);
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

int OGIS_NumPoints(OOSQL_EXTFUNC_1_ARGS)
{
	WKBinary wkb;
	int numPoints;


	// get first argument
	if (isConstantArg[0])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
			wkbHolder->setWKBinary(UDFNo, wkb);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb);
		}
	}
	else // first argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
	}

	numPoints = wkb.GetNumPoints();
	memcpy(returnValue, &numPoints, sizeof(int));
	*returnValueLength = sizeof(int);
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}


int OGIS_IsRing(OOSQL_EXTFUNC_1_ARGS)
{
	WKBinary wkb;
	int result;


	// get first argument
	if (isConstantArg[0])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
			wkbHolder->setWKBinary(UDFNo, wkb);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb);
		}
	}
	else // first argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
	}

    result = wkb.IsRing();
	memcpy(returnValue, &result, sizeof(int));
	*returnValueLength = sizeof(int);
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

int OGIS_ExteriorRing(OOSQL_EXTFUNC_1_ARGS)
{
	WKBinary wkb;
	WKBinary result;


	// get first argument
	if (isConstantArg[0])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
			wkbHolder->setWKBinary(UDFNo, wkb);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb);
		}
	}
	else // first argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
	}

    result = wkb.ExteriorRing();
	memcpy(returnValue, result.GetBinary(), result.GetSize());
	*returnValueLength = result.GetSize();
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

int OGIS_InteriorRingN(OOSQL_EXTFUNC_2_ARGS)
{
	WKBinary wkb;
	int ith;


	// get first argument
	if (isConstantArg[0])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
			wkbHolder->setWKBinary(UDFNo, wkb);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb);
		}
	}
	else // first argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
	}

    ith = *(int*)arg1;
    WKBinary result = wkb.InteriorRingN(ith);
	memcpy(returnValue, result.GetBinary(), result.GetSize());
	*returnValueLength = result.GetSize();
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

int OGIS_NumInteriorRings(OOSQL_EXTFUNC_1_ARGS)
{
	WKBinary wkb;
	int result;


	// get first argument
	if (isConstantArg[0])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
			wkbHolder->setWKBinary(UDFNo, wkb);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb);
		}
	}
	else // first argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
	}
	
	result = wkb.NumInteriorRings();
	memcpy(returnValue, &result, sizeof(int));
	*returnValueLength = sizeof(int);
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

int OGIS_Area(OOSQL_EXTFUNC_1_ARGS)
{
	WKBinary wkb;
	float result;


	// get first argument
	if (isConstantArg[0])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
			wkbHolder->setWKBinary(UDFNo, wkb);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb);
		}
	}
	else // first argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
	}

    result = wkb.Area();
	memcpy(returnValue, &result, sizeof(float));
	*returnValueLength = sizeof(float);
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

int OGIS_Centroid(OOSQL_EXTFUNC_1_ARGS)
{
	WKBinary wkb;
	WKBinary result;


	// get first argument
	if (isConstantArg[0])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
			wkbHolder->setWKBinary(UDFNo, wkb);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb);
		}
	}
	else // first argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
	}

	result = wkb.Centroid();
	memcpy(returnValue, result.GetBinary(), result.GetSize());
	*returnValueLength = result.GetSize();
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

int OGIS_PointOnSurface(OOSQL_EXTFUNC_1_ARGS)
{
	WKBinary wkb;
	WKBinary result;


	// get first argument
	if (isConstantArg[0])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
			wkbHolder->setWKBinary(UDFNo, wkb);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb);
		}
	}
	else // first argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
	}

	result = wkb.PointOnSurface();
	memcpy(returnValue, result.GetBinary(), result.GetSize());
	*returnValueLength = result.GetSize();
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

int OGIS_GeometryN(OOSQL_EXTFUNC_2_ARGS)
{
	WKBinary wkb;
	int ith;
	WKBinary result;


	// get first argument
	if (isConstantArg[0])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
			wkbHolder->setWKBinary(UDFNo, wkb);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb);
		}
	}
	else // first argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
	}
	
	ith = *(int*)arg1;
    result = wkb.GeometryN(ith);
	memcpy(returnValue, result.GetBinary(), result.GetSize());
	*returnValueLength = result.GetSize();
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

int OGIS_NumGeometries(OOSQL_EXTFUNC_1_ARGS)
{
	WKBinary wkb;
	int result;


	// get first argument
	if (isConstantArg[0])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
			wkbHolder->setWKBinary(UDFNo, wkb);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb);
		}
	}
	else // first argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
	}

    result = wkb.GetNumGeometries();
	memcpy(returnValue, &result, sizeof(int));
	*returnValueLength = sizeof(int);
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

int OGIS_Buffer(OOSQL_EXTFUNC_2_ARGS)
{
	WKBinary wkb;
	WKBinary result;


	// get first argument
	if (isConstantArg[0])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
			wkbHolder->setWKBinary(UDFNo, wkb);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb);
		}
	}
	else // first argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
	}

	result = wkb.Buffer();
	memcpy(returnValue, result.GetBinary(), result.GetSize());
	*returnValueLength = result.GetSize();
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

int OGIS_ConvexHull(OOSQL_EXTFUNC_1_ARGS)
{
	WKBinary wkb;
	WKBinary result;


	// get first argument
	if (isConstantArg[0])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
			wkbHolder->setWKBinary(UDFNo, wkb);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb);
		}
	}
	else // first argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb);
	}

	result = wkb.ConvexHull();
	memcpy(returnValue, result.GetBinary(), result.GetSize());
	*returnValueLength = result.GetSize();
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

int OGIS_Difference(OOSQL_EXTFUNC_2_ARGS)
{
	WKBinary wkb1, wkb2;
	WKBinary result;


	// get first argument
	if (isConstantArg[0])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb1);
			wkbHolder->setWKBinary(UDFNo, wkb1);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb1);
		}
	}
	else // first argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb1);
	}

	// get second argument
	if (isConstantArg[1])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg1, argSizes[1], wkb2);
			wkbHolder->setWKBinary(UDFNo, wkb2);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb2);
		}
	}
	else // second argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg1, argSizes[0], wkb2);
	}

	result = wkb1.Difference(wkb2);
	memcpy(returnValue, result.GetBinary(), result.GetSize());
	*returnValueLength = result.GetSize();
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

int OGIS_Intersection(OOSQL_EXTFUNC_2_ARGS)
{
	WKBinary wkb1, wkb2;
	WKBinary result;


	// get first argument
	if (isConstantArg[0])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb1);
			wkbHolder->setWKBinary(UDFNo, wkb1);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb1);
		}
	}
	else // first argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb1);
	}

	// get second argument
	if (isConstantArg[1])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg1, argSizes[1], wkb2);
			wkbHolder->setWKBinary(UDFNo, wkb2);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb2);
		}
	}
	else // second argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg1, argSizes[0], wkb2);
	}

	result = wkb1.Intersection(wkb2);
	memcpy(returnValue, result.GetBinary(), result.GetSize());
	*returnValueLength = result.GetSize();
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

int OGIS_SymDifference(OOSQL_EXTFUNC_2_ARGS)
{
	WKBinary wkb1, wkb2;
	WKBinary result;


	// get first argument
	if (isConstantArg[0])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb1);
			wkbHolder->setWKBinary(UDFNo, wkb1);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb1);
		}
	}
	else // first argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb1);
	}

	// get second argument
	if (isConstantArg[1])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg1, argSizes[1], wkb2);
			wkbHolder->setWKBinary(UDFNo, wkb2);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb2);
		}
	}
	else // second argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg1, argSizes[0], wkb2);
	}

	result = wkb1.SymDifference(wkb2);
	memcpy(returnValue, result.GetBinary(), result.GetSize());
	*returnValueLength = result.GetSize();
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

int OGIS_Union(OOSQL_EXTFUNC_2_ARGS)
{
	WKBinary wkb1, wkb2;
	WKBinary result;


	// get first argument
	if (isConstantArg[0])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb1);
			wkbHolder->setWKBinary(UDFNo, wkb1);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb1);
		}
	}
	else // first argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb1);
	}

	// get second argument
	if (isConstantArg[1])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg1, argSizes[1], wkb2);
			wkbHolder->setWKBinary(UDFNo, wkb2);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb2);
		}
	}
	else // second argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg1, argSizes[0], wkb2);
	}

    result = wkb1.Union(wkb2);
	memcpy(returnValue, result.GetBinary(), result.GetSize());
	*returnValueLength = result.GetSize();
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

/* modified */
int OGIS_Contains(OOSQL_EXTFUNC_2_ARGS)
{
//	WKBinary wkb1, wkb2;
	int result;


	// get first argument
	if (isConstantArg[0])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb1);
			wkbHolder->setWKBinary(UDFNo, wkb1);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb1);
		}
	}
	else // first argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb1);
	}

	// get second argument
	if (isConstantArg[1])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg1, argSizes[1], wkb2);
			wkbHolder->setWKBinary(UDFNo, wkb2);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb2);
		}
	}
	else // second argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg1, argSizes[0], wkb2);
	}

	result = wkb1.Contains(wkb2);
	memcpy(returnValue, &result, sizeof(int));
	*returnValueLength = sizeof(int);
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}


int OGIS_Crosses(OOSQL_EXTFUNC_2_ARGS)
{
	WKBinary wkb1, wkb2;
	int result;


	// get first argument
	if (isConstantArg[0])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb1);
			wkbHolder->setWKBinary(UDFNo, wkb1);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb1);
		}
	}
	else // first argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb1);
	}

	// get second argument
	if (isConstantArg[1])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg1, argSizes[1], wkb2);
			wkbHolder->setWKBinary(UDFNo, wkb2);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb2);
		}
	}
	else // second argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg1, argSizes[0], wkb2);
	}

	result = wkb1.Crosses(wkb2);
	memcpy(returnValue, &result, sizeof(int));
	*returnValueLength = sizeof(int);
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

int OGIS_Disjoint(OOSQL_EXTFUNC_2_ARGS)
{
	WKBinary wkb1, wkb2;
	int result;


	// get first argument
	if (isConstantArg[0])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb1);
			wkbHolder->setWKBinary(UDFNo, wkb1);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb1);
		}
	}
	else // first argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb1);
	}

	// get second argument
	if (isConstantArg[1])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg1, argSizes[1], wkb2);
			wkbHolder->setWKBinary(UDFNo, wkb2);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb2);
		}
	}
	else // second argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg1, argSizes[0], wkb2);
	}

    result = wkb1.Disjoint(wkb2);
	memcpy(returnValue, &result, sizeof(int));
	*returnValueLength = sizeof(int);
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

int OGIS_Distance(OOSQL_EXTFUNC_2_ARGS)
{
	WKBinary wkb1, wkb2;
	float result;


	// get first argument
	if (isConstantArg[0])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb1);
			wkbHolder->setWKBinary(UDFNo, wkb1);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb1);
		}
	}
	else // first argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb1);
	}

	// get second argument
	if (isConstantArg[1])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg1, argSizes[1], wkb2);
			wkbHolder->setWKBinary(UDFNo, wkb2);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb2);
		}
	}
	else // second argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg1, argSizes[0], wkb2);
	}

    result = wkb1.Distance(wkb2);
	memcpy(returnValue, &result, sizeof(float));
	*returnValueLength = sizeof(float);
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

int OGIS_Equals(OOSQL_EXTFUNC_2_ARGS)
{
	WKBinary wkb1, wkb2;
	int result;


	// get first argument
	if (isConstantArg[0])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb1);
			wkbHolder->setWKBinary(UDFNo, wkb1);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb1);
		}
	}
	else // first argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb1);
	}

	// get second argument
	if (isConstantArg[1])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg1, argSizes[1], wkb2);
			wkbHolder->setWKBinary(UDFNo, wkb2);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb2);
		}
	}
	else // second argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg1, argSizes[0], wkb2);
	}

    result = wkb1.Equals(wkb2);
	memcpy(returnValue, &result, sizeof(int));
	*returnValueLength = sizeof(int);
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

int OGIS_Intersects(OOSQL_EXTFUNC_2_ARGS)
{
	WKBinary wkb1, wkb2;
	int result;


	// get first argument
	if (isConstantArg[0])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb1);
			wkbHolder->setWKBinary(UDFNo, wkb1);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb1);
		}
	}
	else // first argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb1);
	}

	// get second argument
	if (isConstantArg[1])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg1, argSizes[1], wkb2);
			wkbHolder->setWKBinary(UDFNo, wkb2);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb2);
		}
	}
	else // second argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg1, argSizes[0], wkb2);
	}

    result = wkb1.Intersects(wkb2);
	memcpy(returnValue, &result, sizeof(int));
	*returnValueLength = sizeof(int);
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

int OGIS_Overlaps(OOSQL_EXTFUNC_2_ARGS)
{
	WKBinary wkb1, wkb2;
	int result;


	// get first argument
	if (isConstantArg[0])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb1);
			wkbHolder->setWKBinary(UDFNo, wkb1);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb1);
		}
	}
	else // first argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb1);
	}

	// get second argument
	if (isConstantArg[1])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg1, argSizes[1], wkb2);
			wkbHolder->setWKBinary(UDFNo, wkb2);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb2);
		}
	}
	else // second argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg1, argSizes[0], wkb2);
	}

    result = wkb1.Overlaps(wkb2);
	memcpy(returnValue, &result, sizeof(int));
	*returnValueLength = sizeof(int);
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

int OGIS_Related(OOSQL_EXTFUNC_3_ARGS)
{
	WKBinary wkb1, wkb2;
	char* patternMatrix;
	int result;


	// get first argument
	if (isConstantArg[0])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb1);
			wkbHolder->setWKBinary(UDFNo, wkb1);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb1);
		}
	}
	else // first argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb1);
	}

	// get second argument
	if (isConstantArg[1])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg1, argSizes[1], wkb2);
			wkbHolder->setWKBinary(UDFNo, wkb2);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb2);
		}
	}
	else // second argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg1, argSizes[0], wkb2);
	}

    patternMatrix = (char*)arg2;
    result = wkb1.Related(wkb2, patternMatrix);
	memcpy(returnValue, &result, sizeof(int));
	*returnValueLength = sizeof(int);
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

int OGIS_Touches(OOSQL_EXTFUNC_2_ARGS)
{
	WKBinary wkb1, wkb2;
	int result;


	// get first argument
	if (isConstantArg[0])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb1);
			wkbHolder->setWKBinary(UDFNo, wkb1);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb1);
		}
	}
	else // first argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb1);
	}

	// get second argument
	if (isConstantArg[1])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg1, argSizes[1], wkb2);
			wkbHolder->setWKBinary(UDFNo, wkb2);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb2);
		}
	}
	else // second argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg1, argSizes[0], wkb2);
	}

    result = wkb1.Touches(wkb2);
	memcpy(returnValue, &result, sizeof(int));
	*returnValueLength = sizeof(int);
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}

int OGIS_Within(OOSQL_EXTFUNC_2_ARGS)
{
	WKBinary wkb1, wkb2;
	int result;


	// get first argument
	if (isConstantArg[0])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb1);
			wkbHolder->setWKBinary(UDFNo, wkb1);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb1);
		}
	}
	else // first argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg0, argSizes[0], wkb1);
	}

	// get second argument
	if (isConstantArg[1])
	{
		if (wkbHolder->isEvaluated(UDFNo) == SM_FALSE)
		{
			GetWKBinaryFromUDT((char*)arg1, argSizes[1], wkb2);
			wkbHolder->setWKBinary(UDFNo, wkb2);
		}
		else
		{
			wkbHolder->getWKBinary(UDFNo, wkb2);
		}
	}
	else // second argument is not constant
	{
		GetWKBinaryFromUDT((char*)arg1, argSizes[0], wkb2);
	}

    result = wkb1.Within(wkb2);
	memcpy(returnValue, &result, sizeof(int));
	*returnValueLength = sizeof(int);
	*isNullReturnValue = SM_FALSE;
    return eNOERROR;
}
} /* extern "C" */

	OOSQL_UDTObject udt;
void GetWKBinaryFromUDT(char* data, int size, WKBinary& wkb)
{

	OOSQL_UDTObject_SetData(&udt, data);
	OOSQL_UDTObject_SetSize(&udt, size);

	char* binary = OOSQL_UDTObject_GetNTHAttrData(&udt, 5);
	int   length = OOSQL_UDTObject_GetNTHAttrLength(&udt, 5);

	wkb.Init(binary, length);
}


Four CreateGeometryUDTFromText(OOSQL_SystemHandle* systemHandle, OOSQL_DbInfo* dbInfo, char* text, int srid, OOSQL_TCDynStr& result, char* errorMessage)
{
	OOSQL_UDTObject udt;
	Four			e;
	float			xmin, ymin, xmax, ymax;
	WKBinary		wkb;

	try {
		wkb = WKTextToWKBinary(text);
	}
	catch(parse_error& e)
	{
		strcpy(errorMessage, e.what());
		return -1;
	}

	wkb.GetMBR(&xmin, &ymin, &xmax, &ymax);

	OOSQL_UDTObject_AllocBuffer(&udt, UDTOBJECT_DEFAULT_SIZE);
	OOSQL_UDTObject_SetNAttrs(&udt, 6);
	OOSQL_UDTObject_SetLength(&udt, UDTOBJECT_HEADER_SIZE(6));

	OOSQL_UDTObject_SetNTHAttrType(&udt, 0, OOSQL_TYPE_FLOAT);
	OOSQL_UDTObject_SetNTHAttrOffset(&udt, 0, -1);

	OOSQL_UDTObject_SetNTHAttrType(&udt, 1, OOSQL_TYPE_FLOAT);
	OOSQL_UDTObject_SetNTHAttrOffset(&udt, 1, -1);

	OOSQL_UDTObject_SetNTHAttrType(&udt, 2, OOSQL_TYPE_FLOAT);
	OOSQL_UDTObject_SetNTHAttrOffset(&udt, 2, -1);

	OOSQL_UDTObject_SetNTHAttrType(&udt, 3, OOSQL_TYPE_FLOAT);
	OOSQL_UDTObject_SetNTHAttrOffset(&udt, 3, -1);

	OOSQL_UDTObject_SetNTHAttrType(&udt, 4, OOSQL_TYPE_INT);
	OOSQL_UDTObject_SetNTHAttrOffset(&udt, 4, -1);

	OOSQL_UDTObject_SetNTHAttrType(&udt, 5, OOSQL_TYPE_VARSTRING);
	OOSQL_UDTObject_SetNTHAttrOffset(&udt, 5, -1);

	OOSQL_UDTObject_SetNTHAttrData(&udt, 0, 4, &xmin);
	OOSQL_UDTObject_SetNTHAttrData(&udt, 1, 4, &ymin);
	OOSQL_UDTObject_SetNTHAttrData(&udt, 2, 4, &xmax);
	OOSQL_UDTObject_SetNTHAttrData(&udt, 3, 4, &ymax);
	OOSQL_UDTObject_SetNTHAttrData(&udt, 4, 4, &srid);
	OOSQL_UDTObject_SetNTHAttrData(&udt, 5, (int)wkb.GetSize(), wkb.GetBinary());

	result.resize(OOSQL_UDTObject_GetLength(&udt));
	memcpy((char*)result.str(), OOSQL_UDTObject_GetData(&udt), OOSQL_UDTObject_GetLength(&udt));

	OOSQL_UDTObject_FreeBuffer(&udt);

    return eNOERROR;
}

Four CreateGeometryUDTFromBinary(OOSQL_SystemHandle* systemHandle, OOSQL_DbInfo* dbInfo, char* binary, int size, int srid, OOSQL_TCDynStr& result, char* errorMessage)
{
	OOSQL_UDTObject udt;
	Four			e;
	float			xmin, ymin, xmax, ymax;
	WKBinary		wkb(binary, size);

	wkb.GetMBR(&xmin, &ymin, &xmax, &ymax);

	OOSQL_UDTObject_AllocBuffer(&udt, UDTOBJECT_DEFAULT_SIZE);
	OOSQL_UDTObject_SetNAttrs(&udt, 6);
	OOSQL_UDTObject_SetLength(&udt, UDTOBJECT_HEADER_SIZE(6));

	OOSQL_UDTObject_SetNTHAttrType(&udt, 0, OOSQL_TYPE_FLOAT);
	OOSQL_UDTObject_SetNTHAttrOffset(&udt, 0, -1);

	OOSQL_UDTObject_SetNTHAttrType(&udt, 1, OOSQL_TYPE_FLOAT);
	OOSQL_UDTObject_SetNTHAttrOffset(&udt, 1, -1);

	OOSQL_UDTObject_SetNTHAttrType(&udt, 2, OOSQL_TYPE_FLOAT);
	OOSQL_UDTObject_SetNTHAttrOffset(&udt, 2, -1);

	OOSQL_UDTObject_SetNTHAttrType(&udt, 3, OOSQL_TYPE_FLOAT);
	OOSQL_UDTObject_SetNTHAttrOffset(&udt, 3, -1);

	OOSQL_UDTObject_SetNTHAttrType(&udt, 4, OOSQL_TYPE_INT);
	OOSQL_UDTObject_SetNTHAttrOffset(&udt, 4, -1);

	OOSQL_UDTObject_SetNTHAttrType(&udt, 5, OOSQL_TYPE_VARSTRING);
	OOSQL_UDTObject_SetNTHAttrOffset(&udt, 5, -1);

	OOSQL_UDTObject_SetNTHAttrData(&udt, 0, 4, &xmin);
	OOSQL_UDTObject_SetNTHAttrData(&udt, 1, 4, &ymin);
	OOSQL_UDTObject_SetNTHAttrData(&udt, 2, 4, &xmax);
	OOSQL_UDTObject_SetNTHAttrData(&udt, 3, 4, &ymax);
	OOSQL_UDTObject_SetNTHAttrData(&udt, 4, 4, &srid);
	OOSQL_UDTObject_SetNTHAttrData(&udt, 5, (int)wkb.GetSize(), wkb.GetBinary());

	result.resize(OOSQL_UDTObject_GetLength(&udt));
	memcpy((char*)result.str(), OOSQL_UDTObject_GetData(&udt), OOSQL_UDTObject_GetLength(&udt));

	OOSQL_UDTObject_FreeBuffer(&udt);

    return eNOERROR;
}

#endif
#endif
