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
#include "OOSQL_StorageSystemHeaders.h"
#include "OOSQL_StorageManager.hxx"
#include "OOSQL_Error.h"
#include <assert.h>

// every contents in this file is properly substituted from them of rev1.

OOSQL_StorageManagerGEO::OOSQL_StorageManagerGEO(OOSQL_SystemHandle* oosqlSystemHandle, GEO_Handle* systemHandle)
                        :OOSQL_StorageManagerLOM(oosqlSystemHandle, &GEO_GET_LOM_SYSTEMHANDLE(systemHandle))
{
	m_systemHandle = systemHandle;
	
	// Run time system check...
	assert(sizeof(OOSQL_StorageManager::IndexID) == sizeof(::GEO_IndexID));
	assert(sizeof(OOSQL_StorageManager::IndexDesc) == sizeof(::GEO_IndexDesc));
	assert(sizeof(OOSQL_StorageManager::ColListStruct) == sizeof(::GEO_ColListStruct));
}

OOSQL_StorageManagerGEO::~OOSQL_StorageManagerGEO()
{
}

Four OOSQL_StorageManagerGEO::AddIndex(Four volId, char *className, char *indexName, IndexDesc *indexDesc, IndexID *indexID)
{
	Four e;

    ConvertToUserLevelColNoInIndexDesc(indexDesc);
    e = GEO_AddIndex(m_systemHandle, volId, className, indexName, (::GEO_IndexDesc*)indexDesc, (::GEO_IndexID*)indexID);
    ConvertToSystemLevelColNoInIndexDesc(indexDesc);
    if(e == eINDEXEXIST_LRDS)
        return eINDEXDUPLICATED_OOSQL;
	else
		return e;
}

Four OOSQL_StorageManagerGEO::DropIndex(Four volId, char *indexName)
{
    Four e;

    e = GEO_DropIndex(m_systemHandle, volId, indexName);
    if(e == eINDEXNOTFOUND_LOM)
        return eINDEXNOTDEFINED_OOSQL;
    else
        return e;
}

Four OOSQL_StorageManagerGEO::OpenClass(Four volId, char* className)
{
	return GEO_OpenClass(m_systemHandle, volId, className);
}

Four OOSQL_StorageManagerGEO::OpenClass(Four volId, Four classId)
{
	Four classInfo;
	Four e;
	Four mv;

	e = Catalog_GetMountTableInfo(m_systemHandle, volId, &mv);
	OOSQL_CHECK_ERR(e);

    e = Catalog_GetClassInfo(m_systemHandle, volId, classId, &classInfo);
    OOSQL_CHECK_ERR(e);

    return GEO_OpenClass(
        m_systemHandle, volId,
        CATALOG_GET_CLASSNAME(&(CATALOG_GET_CLASSINFOTBL(m_systemHandle, mv)[classInfo])));
}

Four OOSQL_StorageManagerGEO::CloseClass(Four ocn)
{
	return GEO_CloseClass(m_systemHandle, ocn);
}

Four OOSQL_StorageManagerGEO::GetOpenClassNum(Four volId, Four classId)
{
	Four classInfo;
	Four e;
	Four mv;

	e = Catalog_GetMountTableInfo(m_systemHandle, volId, &mv);
	OOSQL_CHECK_ERR(e);

    e = Catalog_GetClassInfo(m_systemHandle, volId, classId, &classInfo);
    OOSQL_CHECK_ERR(e);

    return GEO_GetOpenClassNum(
        m_systemHandle, volId,
        CATALOG_GET_CLASSNAME(&(CATALOG_GET_CLASSINFOTBL(m_systemHandle, mv)[classInfo])));
}

Four OOSQL_StorageManagerGEO::NextObject(Four scanId, OID* oid, Cursor **cursor)
{
	return GEO_NextObject(m_systemHandle, scanId, (::OID*)oid, (::LRDS_Cursor**)cursor);
}

Four OOSQL_StorageManagerGEO::DestroyObject(Four ocnOrScanId, Boolean useScanFlag, OID* oid)
{
	return GEO_DestroyObject(m_systemHandle, ocnOrScanId, useScanFlag, (::OID*)oid);
}

Four OOSQL_StorageManagerGEO::CreateObjectByColList(Four ocnOrScanId, Boolean useScanFlag, Four nCols, ColListStruct* clist, OID* oid)
{
    ConvertToUserLevelColNoInColListStruct(nCols, clist);
	Four e = GEO_CreateObjectByColListStruct(m_systemHandle, ocnOrScanId, useScanFlag, nCols, (::LOM_ColListStruct*)clist, (::OID*)oid);
    ConvertToSystemLevelColNoInColListStruct(nCols, clist);
    return e;
}

Four OOSQL_StorageManagerGEO::UpdateObjectByColList(Four ocnOrScanId, Boolean useScanFlag, OID* oid, Four nCols, ColListStruct* clist)
{
    ConvertToUserLevelColNoInColListStruct(nCols, clist);
	Four e = GEO_UpdateObjectByColListStruct(m_systemHandle, ocnOrScanId, useScanFlag, (::OID*)oid, nCols, (::LOM_ColListStruct*)clist);
    ConvertToSystemLevelColNoInColListStruct(nCols, clist);
    return e;
}

Four OOSQL_StorageManagerGEO::CloseScan(Four scanId)
{
	return GEO_CloseScan(m_systemHandle, scanId);
}

Four OOSQL_StorageManagerGEO::OpenMBRqueryScan(Four ocn, IndexID *iid, Region queryRegion, Four spatialOp, Four nBools, BoolExp* bools, LockParameter *lockup,
IndexID* tidJoinIndexID, BoundCond* tidJoinIndexStartBound, BoundCond* tidJoinIndexStopBound )
{
	GEO_Region geoQueryRegion;

	geoQueryRegion.x1 = queryRegion.x1;
	geoQueryRegion.y1 = queryRegion.y1;
	geoQueryRegion.x2 = queryRegion.x2;
	geoQueryRegion.y2 = queryRegion.y2;

    ConvertToUserLevelColNoInBoolExp(nBools, bools);
    Four e = GEO_OpenMBRqueryScan(m_systemHandle, ocn, (::LOM_IndexID*)iid, geoQueryRegion, spatialOp, nBools, (::BoolExp*)bools, (::LockParameter*)lockup,
	(::LOM_IndexID*) tidJoinIndexID, (::BoundCond*) tidJoinIndexStartBound, (::BoundCond*) tidJoinIndexStopBound );
    ConvertToSystemLevelColNoInBoolExp(nBools, bools);
    return e;
}

Four OOSQL_StorageManagerGEO::Geometry_GetMBR(Four ocnOrScanId, Boolean useScanFlag, OID* oid, Four colNo, float* xmin, float* ymin, float* xmax, float* ymax)
{
    Four e = GEO_GetMBR(m_systemHandle, ocnOrScanId, useScanFlag, (::OID*)oid, colNo - 1, xmin, ymin, xmax, ymax);
    return e;
}

Four OOSQL_StorageManagerGEO::Geometry_GetMBR(char* data, Four length, float* xmin, float* ymin, float* xmax, float* ymax)
{
    Four e = GEO_GetMBRFromUDTObject(m_systemHandle, data, length, xmin, ymin, xmax, ymax);
    return e;
}

#endif
