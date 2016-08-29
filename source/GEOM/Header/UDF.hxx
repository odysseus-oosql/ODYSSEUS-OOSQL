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

#include "OOSQL_APIs_Internal.hxx"
#include "Geometry.hxx"
#include <stdio.h>

#ifndef ENABLE_OPENGIS_OPTIMIZATION

#define OOSQL_EXTFUNC_1_ARGS \
	void*                       arg0,				\
	void*                       returnValue,		\
	int*                        returnValueLength,	\
    Boolean*                      isNullArg0,         \
    Boolean*                      isNullReturnValue,  \
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
    Boolean*                      isNullArg0,         \
    Boolean*                      isNullArg1,         \
    Boolean*                      isNullReturnValue,  \
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
    Boolean*                      isNullArg0,         \
    Boolean*                      isNullArg1,         \
    Boolean*                      isNullArg2,         \
    Boolean*                      isNullReturnValue,  \
	char*                       functionName,		\
	char*                       specificName,		\
	char*                       errorMessage,		\
	int*						argSizes,			\
	OOSQL_SystemHandle*         systemHandle,		\
	OOSQL_ScratchPad*			scrachPad,			\
	OOSQL_DbInfo*				dbInfo

#else

#define OOSQL_EXTFUNC_1_ARGS \
    void*                       arg0,               \
    void*                       returnValue,        \
    int*                        returnValueLength,  \
    Boolean*                      isNullArg0,         \
    Boolean*                      isNullReturnValue,  \
    char*                       functionName,       \
    char*                       specificName,       \
    char*                       errorMessage,       \
    int*                        argSizes,           \
    OOSQL_SystemHandle*         systemHandle,       \
    OOSQL_ScratchPad*           scrachPad,          \
    OOSQL_DbInfo*               dbInfo,             \
    /* IJKIM24MAR2006 UDF OPTIMIZATION ... */       \
    int                         UDFNo,              \
    Boolean*                      isConstantArg
    /* ... IJKIM24MAR2006 UDF OPTIMIZATION */

#define OOSQL_EXTFUNC_2_ARGS \
    void*                       arg0,               \
    void*                       arg1,               \
    void*                       returnValue,        \
    int*                        returnValueLength,  \
    Boolean*                      isNullArg0,         \
    Boolean*                      isNullArg1,         \
    Boolean*                      isNullReturnValue,  \
    char*                       functionName,       \
    char*                       specificName,       \
    char*                       errorMessage,       \
    int*                        argSizes,           \
    OOSQL_SystemHandle*         systemHandle,       \
    OOSQL_ScratchPad*           scrachPad,          \
    OOSQL_DbInfo*               dbInfo,             \
    int                         UDFNo,              \
    Boolean*                      isConstantArg

#define OOSQL_EXTFUNC_3_ARGS \
    void*                       arg0,               \
    void*                       arg1,               \
    void*                       arg2,               \
    void*                       returnValue,        \
    int*                        returnValueLength,  \
    Boolean*                      isNullArg0,         \
    Boolean*                      isNullArg1,         \
    Boolean*                      isNullArg2,         \
    Boolean*                      isNullReturnValue,  \
    char*                       functionName,       \
    char*                       specificName,       \
    char*                       errorMessage,       \
    int*                        argSizes,           \
    OOSQL_SystemHandle*         systemHandle,       \
    OOSQL_ScratchPad*           scrachPad,          \
    OOSQL_DbInfo*               dbInfo,             \
    int                         UDFNo,              \
    Boolean*                      isConstantArg

#endif

void GetWKBinaryFromUDT(char* data, int size, WKBinary& wkb);
Four CreateGeometryUDTFromText(OOSQL_SystemHandle* systemHandle, OOSQL_DbInfo* dbInfo, char* text, int srid, OOSQL_TCDynStr& result, char* errorMessage);
Four CreateGeometryUDTFromBinary(OOSQL_SystemHandle* systemHandle, OOSQL_DbInfo* dbInfo, char* binary, int size, int srid, OOSQL_TCDynStr& result, char* errorMessage);

extern "C" {

 int OGIS_GeometryFromText(OOSQL_EXTFUNC_2_ARGS);
 int OGIS_PointFromText(OOSQL_EXTFUNC_2_ARGS);
 int OGIS_LineStringFromText(OOSQL_EXTFUNC_2_ARGS);
 int OGIS_PolygonFromText(OOSQL_EXTFUNC_2_ARGS);
 int OGIS_MultiPointFromText(OOSQL_EXTFUNC_2_ARGS);
 int OGIS_MultiLineStringFromText(OOSQL_EXTFUNC_2_ARGS);
 int OGIS_MultiPolygonFromText(OOSQL_EXTFUNC_2_ARGS);
 int OGIS_GeometryCollectionFromText(OOSQL_EXTFUNC_2_ARGS);
 int OGIS_GeometryFromWKB(OOSQL_EXTFUNC_2_ARGS);
 int OGIS_PointFromWKB(OOSQL_EXTFUNC_2_ARGS);
 int OGIS_LineStringFromWKB(OOSQL_EXTFUNC_2_ARGS);
 int OGIS_PolygonFromWKB(OOSQL_EXTFUNC_2_ARGS);
 int OGIS_MultiPointFromWKB(OOSQL_EXTFUNC_2_ARGS);
 int OGIS_MultiLineStringFromWKB(OOSQL_EXTFUNC_2_ARGS);
 int OGIS_MultiPolygonFromWKB(OOSQL_EXTFUNC_2_ARGS);
 int OGIS_GeometryCollectionFromWKB(OOSQL_EXTFUNC_2_ARGS);
 int OGIS_AsText(OOSQL_EXTFUNC_1_ARGS);
 int OGIS_AsBinary(OOSQL_EXTFUNC_1_ARGS);
 int OGIS_Dimension(OOSQL_EXTFUNC_1_ARGS);
 int OGIS_Envelope(OOSQL_EXTFUNC_1_ARGS);
 int OGIS_GeometryType(OOSQL_EXTFUNC_1_ARGS);
 int OGIS_SRID(OOSQL_EXTFUNC_1_ARGS);
 int OGIS_Boundary(OOSQL_EXTFUNC_1_ARGS);
 int OGIS_IsEmpty(OOSQL_EXTFUNC_1_ARGS);
 int OGIS_IsSimple(OOSQL_EXTFUNC_1_ARGS);
 int OGIS_X(OOSQL_EXTFUNC_1_ARGS);
 int OGIS_Y(OOSQL_EXTFUNC_1_ARGS);
 int OGIS_PointN(OOSQL_EXTFUNC_2_ARGS);
 int OGIS_StartPoint(OOSQL_EXTFUNC_1_ARGS);
 int OGIS_EndPoint(OOSQL_EXTFUNC_1_ARGS);
 int OGIS_Length(OOSQL_EXTFUNC_1_ARGS);
 int OGIS_IsClosed(OOSQL_EXTFUNC_1_ARGS);
 int OGIS_NumPoints(OOSQL_EXTFUNC_1_ARGS);
 int OGIS_IsRing(OOSQL_EXTFUNC_1_ARGS);
 int OGIS_ExteriorRing(OOSQL_EXTFUNC_1_ARGS);
 int OGIS_InteriorRingN(OOSQL_EXTFUNC_2_ARGS);
 int OGIS_NumInteriorRings(OOSQL_EXTFUNC_1_ARGS);
 int OGIS_Area(OOSQL_EXTFUNC_1_ARGS);
 int OGIS_Centroid(OOSQL_EXTFUNC_1_ARGS);
 int OGIS_PointOnSurface(OOSQL_EXTFUNC_1_ARGS);
 int OGIS_GeometryN(OOSQL_EXTFUNC_2_ARGS);
 int OGIS_NumGeometries(OOSQL_EXTFUNC_1_ARGS);
 int OGIS_Buffer(OOSQL_EXTFUNC_2_ARGS);
 int OGIS_ConvexHull(OOSQL_EXTFUNC_1_ARGS);
 int OGIS_Difference(OOSQL_EXTFUNC_2_ARGS);
 int OGIS_Intersection(OOSQL_EXTFUNC_2_ARGS);
 int OGIS_SymDifference(OOSQL_EXTFUNC_2_ARGS);
 int OGIS_Union(OOSQL_EXTFUNC_2_ARGS);
 int OGIS_Contains(OOSQL_EXTFUNC_2_ARGS);
 int OGIS_Crosses(OOSQL_EXTFUNC_2_ARGS);
 int OGIS_Disjoint(OOSQL_EXTFUNC_2_ARGS);
 int OGIS_Distance(OOSQL_EXTFUNC_2_ARGS);
 int OGIS_Equals(OOSQL_EXTFUNC_2_ARGS);
 int OGIS_Intersects(OOSQL_EXTFUNC_2_ARGS);
 int OGIS_Overlaps(OOSQL_EXTFUNC_2_ARGS);
 int OGIS_Related(OOSQL_EXTFUNC_3_ARGS);
 int OGIS_Touches(OOSQL_EXTFUNC_2_ARGS);
 int OGIS_Within(OOSQL_EXTFUNC_2_ARGS);
} /* extern "C" */
