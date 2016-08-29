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
#include "geo_Internals.hxx"
#include "geo_UDT.hxx"
#include "geo_Catalog.hxx"
#include "geo_Error.hxx"
#include "Geometry.hxx"
#include "GEO_OIDHashTable.hxx"
#include <math.h>
#include <stdlib.h>

InstanceTable* geo_GDSInstanceTable;
OOSQL_MemoryManager* geoMemoryManager;

#ifdef ENABLE_OPENGIS_OPTIMIZATION
WKBinaryHolder* wkbHolder;
#endif

Four GEO_CreateHandle(
    GEO_Handle*     handle,         /* IN : system handle */
    Four*           procIndex)      /* OUT : proc index */
{
    Four    e;      /* error number */


	if(geo_GDSInstanceTable == NULL)
		geo_GDSInstanceTable = new InstanceTable;

    /* Init LOM */
    e = LOM_CreateHandle(&GEO_GET_LOM_SYSTEMHANDLE(handle), procIndex);
	if(e < 0) GEO_ERROR(handle, e);

	if(geo_GDSInstanceTable->find(handle->instanceId) != geo_GDSInstanceTable->end())
		GEO_ERROR(handle, eBADPARAMETER_GEO);
	(*geo_GDSInstanceTable)[handle->instanceId] = geo_GDSInstance();

    #ifdef ENABLE_OPENGIS_OPTIMIZATION
    if(wkbHolder == NULL)
        wkbHolder = new WKBinaryHolder;
    #endif

    return (eNOERROR);
}   /* GEO_Init() */

Four GEO_DestroyHandle(
    GEO_Handle*     handle,         /* IN : system handle */
    Four            procIndex)      /* IN : proc index */
{
    Four    e;


	std::vector<int> scanIdsToDelete;
    ScanTable& scanTable = GEO_SCANTABLE(handle);
    for(ScanTable::iterator it = scanTable.begin(); it != scanTable.end(); it++)
        scanIdsToDelete.push_back(it->first);

    for(int i = 0; i < scanIdsToDelete.size(); i++)
    {
		e = GEO_CloseScan(handle, scanIdsToDelete[i]);
		if(e < 0) GEO_ERROR(handle, e);
	}

	if(!GEO_VALID_HANDLE(handle))
		GEO_ERROR(handle, eBADPARAMETER_GEO);
	geo_GDSInstanceTable->erase(handle->instanceId);

    /* LOM Final */
    e = LOM_DestroyHandle(&GEO_GET_LOM_SYSTEMHANDLE(handle), procIndex);
	if(e < 0) GEO_ERROR(handle, e);

    #ifdef ENABLE_OPENGIS_OPTIMIZATION
    if(wkbHolder != NULL){
        delete wkbHolder;
		wkbHolder = NULL;
		geoMemoryManager = NULL;
	}
    #endif

    return (eNOERROR);
}   /* GEO_Final() */

#ifdef ENABLE_OPENGIS_OPTIMIZATION
Four GEO_PrepareQuery(OOSQL_MemoryManager* pMemoryManager)
{
    wkbHolder->reset();
	geoMemoryManager = pMemoryManager;

    return (eNOERROR);
}
#endif

Four geo_SetOpenTableTableEntry(GEO_Handle* handle, Four volId, char* tableName, Four otn)
{
	GEO_OPENTABLETABLE(handle)[otn] = geo_OpenTableTableEntry();
	
	geo_OpenTableTableEntry& tableEntry = GEO_OPENTABLETABLE(handle)[otn];

	tableEntry.volId     = volId;
	tableEntry.tableName = tableName;

	catalog_SysClassesOverlay* tableInfo	= geo_catalog_GetClassInfo(handle, otn);

	for(int i = 0; i < tableInfo->nIndexes; i++)
	{
		catalog_SysIndexesOverlay* indexInfo = geo_catalog_GetIndexInfo(handle, volId, tableInfo->indexInfoIndex + i);
		if(indexInfo->indexType == SM_INDEXTYPE_MLGF && indexInfo->kdesc.mlgf.nKeys == MBR_NUM_PARTS)
		{
			Four colNo = indexInfo->colNo[0];
			int  j;
			catalog_SysAttributesOverlay* columnInfo;

			for(j = 0; j < tableInfo->nCols; j++)
			{
				columnInfo = geo_catalog_GetColumnInfo(handle, volId, tableInfo->attrInfoIndex + j);
				if(columnInfo->colNo == colNo)
					break;
			}
			if(j == tableInfo->nCols)
				GEO_ERROR(handle, eINTERNAL_ERROR_GEO);

				Four colType = columnInfo->type;
				if(colType == LOM_OGIS_GEOMETRY			||
				   colType == LOM_OGIS_POINT				||
				   colType == LOM_OGIS_LINESTRING		||
				   colType == LOM_OGIS_POLYGON			||
				   colType == LOM_OGIS_GEOMETRYCOLLECTION	||
				   colType == LOM_OGIS_MULTIPOINT		||
				   colType == LOM_OGIS_MULTILINESTRING	||
				   colType == LOM_OGIS_MULTIPOLYGON)
				{
					GeometryColumnInfo geometryColumnInfo;
					geometryColumnInfo.colNo   = GET_USERLEVEL_COLNO(columnInfo->colNo);
					geometryColumnInfo.colName = columnInfo->name;
					geometryColumnInfo.iid     = indexInfo->iid;
					geometryColumnInfo.kdesc   = indexInfo->kdesc.mlgf;

					GEO_OPENTABLETABLE(handle)[otn].geometryColumnInfos.add(geometryColumnInfo);
					break;
				}
		}
	}
	return eNOERROR;
}

Four GEO_OpenClass(
	GEO_Handle*	handle,			/* IN lom system handle	*/
	Four		volId,			/* IN volume ID	*/
	char*		tableName		/* IN table	name to	open */
)
{
	Four e;
	Four otn;

	e = LOM_OpenClass(&GEO_GET_LOM_SYSTEMHANDLE(handle), volId, tableName);
	if(e < 0) GEO_ERROR(handle, e);
	otn = e;

	e = geo_SetOpenTableTableEntry(handle, volId, tableName, otn);
	if(e < 0) GEO_ERROR(handle, e);

	return otn;
}

Four GEO_GetOpenClassNum(
	GEO_Handle*	handle,			/* IN lom system handle	*/
	Four		volId,			/* IN volume ID	*/
	char*		tableName		/* IN table	name to	open */
)
{
	Four e;
	Four otn;

	e = LOM_GetOpenClassNum(&GEO_GET_LOM_SYSTEMHANDLE(handle), volId, tableName);
	if(e < 0) GEO_ERROR(handle, e);
	otn = e;

	e = geo_SetOpenTableTableEntry(handle, volId, tableName, otn);
	if(e < 0) GEO_ERROR(handle, e);

	return otn;
}

Four GEO_CloseClass(
	GEO_Handle*		handle,
	Four			otn
)
{
	Four e;

	e = LOM_CloseClass(&GEO_GET_LOM_SYSTEMHANDLE(handle), otn);
	if(e < 0) GEO_ERROR(handle, e);

	return eNOERROR;
}

Four geo_spatial_GetObject(GEO_Handle* handle, Four otnOrScanId, Boolean useScanFlag, OID* oid, Four colNo, OOSQL_TCDynStr& object)
{
	Four e;
	ColLengthInfoListStruct llist[1];
	LOM_ColListStruct		clist[1];

	llist[0].colNo = colNo;
	e = LOM_FetchColLength(&GEO_GET_LOM_SYSTEMHANDLE(handle), otnOrScanId, useScanFlag, oid, 1, llist);
	if(e < 0) GEO_ERROR(handle, e);

	Four length = llist[0].length;
	if(length >= 0)
	{
		object.resize(length);

		clist[0].colNo      = colNo;
		clist[0].dataLength = length;
		clist[0].length     = length;
		clist[0].start      = 0;
		clist[0].data.ptr   = (char*)object.str();

		e = LOM_FetchObjectByColList(&GEO_GET_LOM_SYSTEMHANDLE(handle), otnOrScanId, useScanFlag, oid, 1, clist);
		if(e < 0) GEO_ERROR(handle, e);
	}

	if(clist[0].nullFlag)
		object.resize(0);

	return eNOERROR;
}

Four geo_MLGF_IndexObject(GEO_Handle* handle, Four otnOrScanId, Boolean useScanFlag, OID* oid)
{
	Four otn;
	Four e;

	if(useScanFlag)
		otn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(&GEO_GET_LOM_SYSTEMHANDLE(handle)))[otnOrScanId].orn;
	else
		otn = otnOrScanId;

	geo_OpenTableTableEntry& openTableTableEntry = GEO_OPENTABLETABLE(handle)[otn];
	for(int i = 0; i < openTableTableEntry.geometryColumnInfos.numberOfItems(); i++)
	{
		OOSQL_TCDynStr object(geoMemoryManager);
		e = geo_spatial_GetObject(handle, otnOrScanId, useScanFlag, oid, openTableTableEntry.geometryColumnInfos[i].colNo, object);
		if(e < 0) GEO_ERROR(handle, e);
		if(object.length() != 0)
		{
			GEO_SpatialUDTObjectReader reader((char*)object.str(), object.length());
			float minx, miny, maxx, maxy;
			reader.GetMBR(minx, miny, maxx, maxy);

			MLGF_HashValue mlgfKval[MLGF_MAXNUM_KEYS];

			mlgfKval[0] = geo_FloatToHashValue(minx);
			mlgfKval[1] = geo_FloatToHashValue(miny);
			mlgfKval[2] = geo_FloatToHashValue(maxx);
			mlgfKval[3] = geo_FloatToHashValue(maxy);

			e = SM_MLGF_InsertIndexEntry(GEO_GET_LRDS_HANDLE(handle), &(openTableTableEntry.geometryColumnInfos[i].iid.index.physical_iid),
				 							&(openTableTableEntry.geometryColumnInfos[i].kdesc),
											mlgfKval, (ObjectID*)oid, NULL, NULL);
			if(e < 0) GEO_ERROR(handle, e);
		}
	}
	return eNOERROR;
}


Four geo_MLGF_UnIndexObject(GEO_Handle* handle, Four otnOrScanId, Boolean useScanFlag, OID* oid)
{
	Four otn;
	Four e;

	if(useScanFlag)
		otn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(&GEO_GET_LOM_SYSTEMHANDLE(handle)))[otnOrScanId].orn;
	else
		otn = otnOrScanId;

	geo_OpenTableTableEntry& openTableTableEntry = GEO_OPENTABLETABLE(handle)[otn];
	for(int i = 0; i < openTableTableEntry.geometryColumnInfos.numberOfItems(); i++)
	{
		OOSQL_TCDynStr object(geoMemoryManager);
		e = geo_spatial_GetObject(handle, otnOrScanId, useScanFlag, oid, openTableTableEntry.geometryColumnInfos[i].colNo, object);
		if(e < 0) GEO_ERROR(handle, e);
		if(object.length() != 0)
		{
			GEO_SpatialUDTObjectReader reader((char*)object.str(), object.length());
			float minx, miny, maxx, maxy;
			reader.GetMBR(minx, miny, maxx, maxy);

			MLGF_HashValue mlgfKval[MLGF_MAXNUM_KEYS];

			mlgfKval[0] = geo_FloatToHashValue(minx);
			mlgfKval[1] = geo_FloatToHashValue(miny);
			mlgfKval[2] = geo_FloatToHashValue(maxx);
			mlgfKval[3] = geo_FloatToHashValue(maxy);

			e = SM_MLGF_DeleteIndexEntry(GEO_GET_LRDS_HANDLE(handle), &(openTableTableEntry.geometryColumnInfos[i].iid.index.physical_iid),
				 							&(openTableTableEntry.geometryColumnInfos[i].kdesc),
											mlgfKval, (ObjectID*)oid, NULL, NULL);
			if(e < 0) GEO_ERROR(handle, e);
		}
	}
	return eNOERROR;
}

Four geo_CheckIfIndexedGeometryColumnExistsInColList(GEO_Handle* handle, Four otnOrScanId, Boolean useScanFlag, Four nCols, GEO_ColListStruct* clist)
{
	Four otn;
	Four e;
	Four exists;
	exists = SM_FALSE;

	if(useScanFlag)
		otn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(&GEO_GET_LOM_SYSTEMHANDLE(handle)))[otnOrScanId].orn;
	else
		otn = otnOrScanId;

	geo_OpenTableTableEntry& openTableTableEntry = GEO_OPENTABLETABLE(handle)[otn];
	exists = SM_FALSE;
	for(int i = 0; i < openTableTableEntry.geometryColumnInfos.numberOfItems(); i++)
	{
		Four colNo = openTableTableEntry.geometryColumnInfos[i].colNo;
		for(int j = 0; j < nCols; j++)
		{
			if(colNo == clist[j].colNo)
			{
				exists = SM_TRUE;
				break;
			}
		}
	}
	return exists;
}

Four GEO_GetWKBFromUDTObject(GEO_Handle* handle, char* data, Four length, WKBinary* wkb)
{
	Four e;

	GEO_SpatialUDTObjectReader reader(data, length);
	void* wkbData;
	int   wkbLength;
	reader.GetWKBinary(wkbData, wkbLength);
	wkb->Init((char*)wkbData, wkbLength);
	return eNOERROR;
}

Four GEO_GetWKB(GEO_Handle* handle, Four otnOrScanId, Boolean useScanFlag, OID *oid, Four colNo, WKBinary* wkb)
{
	Four e;

	OOSQL_TCDynStr object(geoMemoryManager);
	e = geo_spatial_GetObject(handle, otnOrScanId, useScanFlag, oid, colNo, object);
	if(e < 0) GEO_ERROR(handle, e);
	if(object.length() != 0)
	{
		e = GEO_GetWKBFromUDTObject(handle, (char*)object.str(), object.length(), wkb);
		if(e < 0) GEO_ERROR(handle, e);
	}
	return eNOERROR;
}

Four GEO_CreateObjectByColListStruct(
	GEO_Handle*			handle,
	Four				otnOrScanId,    /* IN otn of scan_id */
	Boolean				useScanFlag,	/* otn or scan_id */
	Four				nCols,			/* IN number of columns to fetch */
	GEO_ColListStruct*	clist,			/* INOUT columns to fetch */
	OID*				oid)			/* OUT tuple to fetch */
{
	Four e;
	Four geometryColumnUpdated;

	e = LOM_CreateObjectByColList(&GEO_GET_LOM_SYSTEMHANDLE(handle), otnOrScanId, useScanFlag, nCols, clist, oid);
	if(e < 0) GEO_ERROR(handle, e);

	e = geo_CheckIfIndexedGeometryColumnExistsInColList(handle, otnOrScanId, useScanFlag, nCols, clist);
	if(e < 0) GEO_ERROR(handle, e);
	geometryColumnUpdated = e;

	if(geometryColumnUpdated)
	{
		e = geo_MLGF_IndexObject(handle, otnOrScanId, useScanFlag, oid);
		if(e < 0) GEO_ERROR(handle, e);
	}

	return eNOERROR;
}
Four GEO_DestroyObject(
	LOM_Handle*			handle,
	Four				otnOrScanId,	/* IN otn or scan-id */
	Boolean				useScanFlag,	/* otn or scan_id */
	OID*				oid)			/* OUT next object */
{

	Four e;

	e = geo_MLGF_UnIndexObject(handle, otnOrScanId, useScanFlag, oid);
	if(e < 0) GEO_ERROR(handle, e);

	e = LOM_DestroyObject(&GEO_GET_LOM_SYSTEMHANDLE(handle), otnOrScanId, useScanFlag, oid);
	if(e < 0) GEO_ERROR(handle, e);

	return eNOERROR;
}

Four GEO_UpdateObjectByColListStruct(
	GEO_Handle*			handle,
	Four				otnOrScanId,    /* IN otn or scan_id */
	Boolean				useScanFlag,	/* IN flag */
	OID*				oid,			/* IN tuple to fetch */
	Four				nCols,			/* IN number of columns to fetch */
	GEO_ColListStruct*	clist)			/* INOUT columns to fetch */
{
	Four e;
	Four geometryColumnUpdated = 0;

	e = geo_CheckIfIndexedGeometryColumnExistsInColList(handle, otnOrScanId, useScanFlag, nCols, clist);
	if(e < 0) GEO_ERROR(handle, e);
	geometryColumnUpdated = e;

	if(geometryColumnUpdated)
	{
		e = geo_MLGF_UnIndexObject(handle, otnOrScanId, useScanFlag, oid);
		if(e < 0) GEO_ERROR(handle, e);
	}

	e = LOM_UpdateObjectByColList(&GEO_GET_LOM_SYSTEMHANDLE(handle), otnOrScanId, useScanFlag, oid, nCols, clist);
	if(e < 0) GEO_ERROR(handle, e);

	if(geometryColumnUpdated)
	{
		e = geo_MLGF_IndexObject(handle, otnOrScanId, useScanFlag, oid);
		if(e < 0) GEO_ERROR(handle, e);
	}

	return eNOERROR;
}

#define min(a,b) (((a)<(b)) ? (a) : (b))
#define max(a,b) (((a)>(b)) ? (a) : (b))

Four GEO_OpenMBRqueryScan(
    GEO_Handle*     handle,         /* IN : system handle */
    Four            otn,            /* IN : open class number */
    GEO_IndexID*    mlgfIndexId,    /* IN : MLGF index ID */
    GEO_Region      queryRegion,    /* IN : search window */
    Four            spatialOp,      /* IN : query type */
    Four            nBools,         /* IN : number of boolean expressions */
    BoolExp         boolExp[],      /* IN : array of boolean expressions */
    LockParameter*  lockup,         /* IN : lockup parameter */
    GEO_IndexID*  	tidJoinIndexID, /* IN : tid join index ID */
    BoundCond*      tidJoinIndexStartBound,     /* IN : start bound of tid join index */
    BoundCond*      tidJoinIndexStopBound       /* IN : stop bound of tid join index */
)
{
    Four            e;              /* error number */
    Four            scanId;         /* scan identifier of new scan */
    MLGF_HashValue  lowerBounds[4]; /* lower bounds */
    MLGF_HashValue  upperBounds[4]; /* upper bounds */
    UFour           minx, miny;
    UFour           maxx, maxy;     /* min, max coordinates of query region */


	try {
		/* check parameters */
		if(mlgfIndexId == NULL)		GEO_ERROR(handle, eBADPARAMETER_GEO);
		if(mlgfIndexId->isLogical)	GEO_ERROR(handle, eBADPARAMETER_GEO);

		catalog_SysIndexesOverlay* indexInfo = geo_catalog_GetIndexInfo(handle, otn, mlgfIndexId);
		if(indexInfo->indexType != SM_INDEXTYPE_MLGF)
			GEO_ERROR(handle, eBADPARAMETER_GEO);
		if(indexInfo->kdesc.mlgf.nKeys != 4)
			GEO_ERROR(handle, eBADPARAMETER_GEO);

		/* make empty scan table entry */
		geo_ScanTableEntry scanEntry;


		if(spatialOp & GEO_SPATIAL_KNN)
		{
			MLGF_HashValue  hashValue[4];
			OID             nearOID;
			GEO_Point       queryPoint;
			
			/* make query point using query region */
			if(queryRegion.x1 != queryRegion.x2) GEO_ERROR(handle, eBADPARAMETER_GEO);
			if(queryRegion.y1 != queryRegion.y2) GEO_ERROR(handle, eBADPARAMETER_GEO);

			queryPoint.x = queryRegion.x1;
			queryPoint.y = queryRegion.y1;

			/* find near object */
#ifdef USE_MLGF_SEARCHNEARTUPLE
			hashValue[0] = geo_FloatToHashValue(queryPoint.x);
			hashValue[1] = geo_FloatToHashValue(queryPoint.y);
			hashValue[2] = geo_FloatToHashValue(queryPoint.x);
			hashValue[3] = geo_FloatToHashValue(queryPoint.y);
			LockParameter lockup2;
			lockup2.mode  = L_S;
			lockup2.duration = L_COMMIT;
	    	e = LOM_MLGF_SearchNearTuple(&GEO_GET_LOM_SYSTEMHANDLE(handle), otn, mlgfIndexId, hashValue, &nearOID, &lockup2);
			if(e < 0) GEO_ERROR(handle, e);

			float xmin, ymin, xmax, ymax;
			e = GEO_GetMBR(handle, otn, SM_FALSE, &nearOID, GET_USERLEVEL_COLNO(indexInfo->colNo[0]), &xmin, &ymin, &xmax, &ymax);
			if(e < 0) GEO_ERROR(handle, e);

			float halfWidth  = max(fabs(xmax - queryPoint.x), fabs(xmin - queryPoint.x));
			float halfHeight = max(fabs(ymax - queryPoint.y), fabs(ymin - queryPoint.y));
			float radius = max(halfWidth, halfHeight);
#else
			float radius = 500.0f;
#endif
			/* make empty scan table entry */
			geo_ScanTableEntry scanEntry;
			
			scanEntry.otn			  = otn;
			scanEntry.cursor		  = NULL;
			scanEntry.info.queryPoint = queryPoint;
			scanEntry.columnNo	      = GET_USERLEVEL_COLNO(indexInfo->colNo[0]);
			scanEntry.scanType		  = GEO_SCAN_NEAREST_OBJECT;
			scanEntry.distanceIndex   = 0;
			scanEntry.knninfo.mlgfIndexId     = mlgfIndexId;
			scanEntry.knninfo.radius          = radius;
			scanEntry.distances.setDelta(100);
			scanEntry.distances.resize(0);
			scanEntry.tidJoinHashtable = NULL;

			if(tidJoinIndexID)
			{
				GEO_OIDHashTable* hashtable = new GEO_OIDHashTable();
				LockParameter lockup2;
				lockup2.duration = L_COMMIT;
				lockup2.mode     = L_S;

				IndexID iid = tidJoinIndexID->index.physical_iid;
				
				e = LRDS_OpenIndexScan(GEO_GET_LRDS_HANDLE(handle), otn, &iid,
					tidJoinIndexStartBound, tidJoinIndexStopBound, 0, NULL, &lockup2);
				if(e < 0) GEO_ERROR(handle, e);
				
				Four scanId2 = e;
				TupleID tid;
				while((e = LRDS_NextTuple(GEO_GET_LRDS_HANDLE(handle), scanId2, &tid, NULL)) != EOS)
				{
					if(e < 0) GEO_ERROR(handle, e);
					hashtable->add(new GEO_OIDHashTableNode(tid));
				}
				
				e = LRDS_CloseScan(GEO_GET_LRDS_HANDLE(handle), scanId2);
				if(e < 0) GEO_ERROR(handle, e);

				scanEntry.tidJoinHashtable = hashtable;
			}

			e = LOM_OpenSeqScan(&GEO_GET_LOM_SYSTEMHANDLE(handle), otn, FORWARD, 0, NULL, lockup);
			if(e < 0) GEO_ERROR(handle, e);
			Four scanId = e;

			GEO_SCANTABLE(handle)[scanId] = scanEntry;

			return scanId;
		}

		/* make query region */
		minx = geo_FloatToHashValue(min(queryRegion.x1, queryRegion.x2));
		maxx = geo_FloatToHashValue(max(queryRegion.x1, queryRegion.x2));
		miny = geo_FloatToHashValue(min(queryRegion.y1, queryRegion.y2));
		maxy = geo_FloatToHashValue(max(queryRegion.y1, queryRegion.y2));

		/* Make MLGF Region query condition */
		lowerBounds[0] = minx;                  upperBounds[0] = minx;
		lowerBounds[1] = miny;                  upperBounds[1] = miny;
		lowerBounds[2] = maxx;                  upperBounds[2] = maxx;
		lowerBounds[3] = maxy;                  upperBounds[3] = maxy;

		scanEntry.scanType = 0;

		/* refine region query condition */
		if(spatialOp & GEO_SPATIAL_INTERSECT)
		{
			lowerBounds[0] = min(lowerBounds[0], MLGF_MIN_HASHVALUE);
			upperBounds[0] = max(upperBounds[0], maxx);

			lowerBounds[1] = min(lowerBounds[1], MLGF_MIN_HASHVALUE);
			upperBounds[1] = max(upperBounds[1], maxy);

			lowerBounds[2] = min(lowerBounds[2], minx);
			upperBounds[2] = max(upperBounds[2], MLGF_MAX_HASHVALUE);

			lowerBounds[3] = min(lowerBounds[3], miny);
			upperBounds[3] = max(upperBounds[3], MLGF_MAX_HASHVALUE);

			scanEntry.scanType |= GEO_SCAN_MBR_INTERSECT;
		}
		if(spatialOp & GEO_SPATIAL_CONTAIN)
		{
			if(minx != MLGF_MAX_HASHVALUE)
				lowerBounds[0] = min(lowerBounds[0], minx + 1);
			upperBounds[0] = max(upperBounds[0], maxx);

			if(miny != MLGF_MAX_HASHVALUE)
				lowerBounds[1] = min(lowerBounds[1], miny + 1);
			upperBounds[1] = max(upperBounds[1], maxy);

			lowerBounds[2] = min(lowerBounds[2], minx);
			if(maxx != 0)
				upperBounds[2] = max(upperBounds[2], maxx - 1);

			lowerBounds[3] = min(lowerBounds[3], miny);
			if(maxy != 0)
				upperBounds[3] = max(upperBounds[3], maxy - 1);

			scanEntry.scanType |= GEO_SCAN_MBR_CONTAIN;
		}
		if(spatialOp & GEO_SPATIAL_CONTAINED)
		{
			lowerBounds[0] = min(lowerBounds[0], MLGF_MIN_HASHVALUE);
			if(minx != 0)
				upperBounds[0] = max(upperBounds[0], minx - 1);

			lowerBounds[1] = min(lowerBounds[1], MLGF_MIN_HASHVALUE);
			if(miny != 0)
				upperBounds[1] = max(upperBounds[1], miny - 1);

			if(maxx != MLGF_MAX_HASHVALUE)
				lowerBounds[2] = min(lowerBounds[2], maxx + 1);
			upperBounds[2] = max(upperBounds[2], MLGF_MAX_HASHVALUE);

			if(maxy != MLGF_MAX_HASHVALUE)
				lowerBounds[3] = min(lowerBounds[3], maxy + 1);
			upperBounds[3] = max(upperBounds[3], MLGF_MAX_HASHVALUE);

			scanEntry.scanType |= GEO_SCAN_MBR_CONTAINED;
		}
		if(spatialOp & GEO_SPATIAL_EQUAL)
		{
			lowerBounds[0] = min(lowerBounds[0], minx);
			upperBounds[0] = max(upperBounds[0], minx);

			lowerBounds[1] = min(lowerBounds[1], miny);
			upperBounds[1] = max(upperBounds[1], miny);

			lowerBounds[2] = min(lowerBounds[2], maxx);
			upperBounds[2] = max(upperBounds[2], maxx);

			lowerBounds[3] = min(lowerBounds[3], maxy);
			upperBounds[3] = max(upperBounds[3], maxy);

			scanEntry.scanType |= GEO_SCAN_MBR_EQUAL;
		}
		if(spatialOp & GEO_SPATIAL_EAST)
		{
			upperBounds[2] = max(upperBounds[2], MLGF_MAX_HASHVALUE);

			scanEntry.scanType |= GEO_SCAN_MBR_EAST;
		}
		if(spatialOp & GEO_SPATIAL_WEST)
		{
			lowerBounds[0] = min(lowerBounds[0], MLGF_MIN_HASHVALUE);

			scanEntry.scanType |= GEO_SCAN_MBR_WEST;
		}
		if(spatialOp & GEO_SPATIAL_NORTH)
		{
			upperBounds[3] = max(upperBounds[3], MLGF_MAX_HASHVALUE);

			scanEntry.scanType |= GEO_SCAN_MBR_NORTH;
		}
		if(spatialOp & GEO_SPATIAL_SOUTH)
		{
			lowerBounds[1] = min(lowerBounds[1], MLGF_MIN_HASHVALUE);

			scanEntry.scanType |= GEO_SCAN_MBR_SOUTH;
		}
		if(spatialOp & GEO_SPATIAL_DISJOINT)
		{
			lowerBounds[0] = MLGF_MIN_HASHVALUE;
			upperBounds[0] = MLGF_MAX_HASHVALUE;

			lowerBounds[1] = MLGF_MIN_HASHVALUE;
			upperBounds[1] = MLGF_MAX_HASHVALUE;

			lowerBounds[2] = MLGF_MIN_HASHVALUE;
			upperBounds[2] = MLGF_MAX_HASHVALUE;

			lowerBounds[3] = MLGF_MIN_HASHVALUE;
			upperBounds[3] = MLGF_MAX_HASHVALUE;

			scanEntry.scanType |= GEO_SCAN_MBR_DISJOINT;
		}

		/* open index scan */
		e = LOM_MLGF_OpenIndexScan(&GEO_GET_LOM_SYSTEMHANDLE(handle), otn, mlgfIndexId, lowerBounds, upperBounds, nBools, boolExp, lockup);
		if(e < 0) GEO_ERROR(handle, e);
		scanId = e;

		/* store scan table information */
		scanEntry.otn              = otn;
		scanEntry.cursor           = NULL;
		scanEntry.info.queryRegion = queryRegion;
		scanEntry.columnNo		   = GET_USERLEVEL_COLNO(indexInfo->colNo[0]);

		GEO_SCANTABLE(handle)[scanId] = scanEntry;

		return scanId;
	}
	catch(geom_error& error)
	{
		GEO_ERROR(handle, error.m_errorCode);
	}
}   /* GEO_OpenMBR_QueryScan() */

Four GEO_GetMLGFindexID(
    GEO_Handle*     handle,         /* IN : system handle */
    Four            volId,          /* IN : volume ID */
    char*           tableName,      /* IN : class name */
	char*			columnName,		/* IN : column name */
    GEO_IndexID*    mlgfIndexId     /* OUT : MLGF index ID */
)
{
	Four e = GEO_GetOpenClassNum(handle, volId, tableName);
	if(e < 0) GEO_ERROR(handle, e);
	Four otn = e;

	OOSQL_TCArray<GeometryColumnInfo>& geometryColumnInfos = GEO_OPENTABLETABLE(handle)[otn].geometryColumnInfos;
	for(int i = 0; i < geometryColumnInfos.numberOfItems(); i++)
	{
		if(geometryColumnInfos[i].colName == columnName)
		{
			*mlgfIndexId = geometryColumnInfos[i].iid;
			return eNOERROR;
		}
	}

	return eNOMLGFINDEX_GEO;
}

Four GEO_OpenNearestObjQueryScan(
    GEO_Handle*     handle,         /* IN : system handle */
    Four            otn,            /* IN : open class number */
    GEO_IndexID*    mlgfIndexId,    /* IN : MLGF index ID */
    GEO_Point       queryPoint,     /* IN : point coord */
	Four			numObjects,		/* IN : number of objects to retrieve */
    Four            nBools,         /* IN : number of boolean expressions */
    BoolExp         boolExp[],      /* IN : array of boolean expressions */
    LockParameter*  lockup          /* IN : lockup parameter */
)
{
	Four			e;
	MLGF_HashValue  hashValue[4];
	OID				nearOID;


	try {
		/* check parameters */
		if(mlgfIndexId == NULL)		GEO_ERROR(handle, eBADPARAMETER_GEO);
		if(mlgfIndexId->isLogical)	GEO_ERROR(handle, eBADPARAMETER_GEO);

		// index type 검사
		catalog_SysIndexesOverlay* indexInfo = geo_catalog_GetIndexInfo(handle, otn, mlgfIndexId);
		if(indexInfo->indexType != SM_INDEXTYPE_MLGF)
			GEO_ERROR(handle, eBADPARAMETER_GEO);
		if(indexInfo->kdesc.mlgf.nKeys != 4)
			GEO_ERROR(handle, eBADPARAMETER_GEO);

		/* make empty scan table entry */
		geo_ScanTableEntry scanEntry;

		/* store scan table information */
		scanEntry.otn			  = otn;
		scanEntry.cursor		  = NULL;
		scanEntry.info.queryPoint = queryPoint;
		scanEntry.columnNo	      = GET_USERLEVEL_COLNO(indexInfo->colNo[0]);
		scanEntry.scanType		  = GEO_SCAN_NEAREST_OBJECT;
		scanEntry.distanceIndex   = 0;
		scanEntry.distances.resize(0);

		/* find near object */
		hashValue[0] = geo_FloatToHashValue(queryPoint.x);
		hashValue[1] = geo_FloatToHashValue(queryPoint.y);
		hashValue[2] = geo_FloatToHashValue(queryPoint.x);
		hashValue[3] = geo_FloatToHashValue(queryPoint.y);
	    e = LOM_MLGF_SearchNearTuple(&GEO_GET_LOM_SYSTEMHANDLE(handle), otn, mlgfIndexId, hashValue, &nearOID, lockup);
		if(e < 0) GEO_ERROR(handle, e);

		float xmin, ymin, xmax, ymax;
		e = GEO_GetMBR(handle, otn, SM_FALSE, &nearOID, scanEntry.columnNo, &xmin, &ymin, &xmax, &ymax);
		if(e < 0) GEO_ERROR(handle, e);

		float halfWidth  = max(fabs(xmax - queryPoint.x), fabs(xmin - queryPoint.x));
		float halfHeight = max(fabs(ymax - queryPoint.y), fabs(ymin - queryPoint.y));
		float radius = max(halfWidth, halfHeight);

		while(1)
		{
			GEO_Region queryRegion;
			queryRegion.x1 = queryPoint.x - radius;
			queryRegion.x2 = queryPoint.x + radius;
			queryRegion.y1 = queryPoint.y - radius;
			queryRegion.y2 = queryPoint.y + radius;

			OID oid;
			e = GEO_OpenMBRqueryScan(handle, otn, mlgfIndexId, queryRegion, GEO_SPATIAL_INTERSECT, nBools, boolExp, lockup);
			if(e < 0) GEO_ERROR(handle, e);
			Four geoScanId = e;

			char pointText[256];
			sprintf(pointText, "POINT(%f %f)", queryPoint.x, queryPoint.y);
			WKBinary wkbQueryPoint = WKTextToWKBinary(pointText);
			scanEntry.distances.resize(0);
			while((e = GEO_NextObject(handle, geoScanId, &oid, NULL)) != EOS)
			{
				WKBinary wkb;
				e = GEO_GetWKB(handle, scanEntry.otn, SM_FALSE, &oid, scanEntry.columnNo, &wkb);
				if(e < 0) GEO_ERROR(handle, e);

				float distance = wkb.Distance(wkbQueryPoint);
				scanEntry.distances.add(DistanceOidPair(distance, oid));
			}
			e = GEO_CloseScan(handle, geoScanId);
			if(e < 0) GEO_ERROR(handle, e);

			if(scanEntry.distances.numberOfItems() > numObjects)
				break;
			else
				radius *= 2;
		}
#ifdef NOUSE_QSORT
		scanEntry.distances.sort();
#else
		qsort((DistanceOidPair*)scanEntry.distances.ptr(),scanEntry.distances.numberOfItems(),sizeof(DistanceOidPair), OOSQL_TCSArray<DistanceOidPair>::cmp);
#endif

		for(UFour i = scanEntry.distances.numberOfItems()-1; i >= numObjects; i--)
		{
			scanEntry.distances.remove(i);
		}

		// dummy scan을 연다
		e = LOM_OpenSeqScan(&GEO_GET_LOM_SYSTEMHANDLE(handle), otn, FORWARD, 0, NULL, lockup);
		if(e < 0) GEO_ERROR(handle, e);
		Four scanId = e;

		GEO_SCANTABLE(handle)[scanId] = scanEntry;

		return scanId;
	}
	catch(geom_error& error)
	{
		GEO_ERROR(handle, error.m_errorCode);
	}
}

Four GEO_CloseScan(
    GEO_Handle* handle,     /* IN : system handle */
    Four        scanId      /* IN : scan to close */
)
{
    Four        e;          /* error code */


    /* Close the LRDS scan */
    e = LOM_CloseScan(&GEO_GET_LOM_SYSTEMHANDLE(handle), scanId);
    if(e < 0) GEO_ERROR(handle, e);

    /* Free the scan table entry */
    if (GEO_VALID_SCANID(handle, scanId))
		GEO_SCANTABLE(handle).erase(scanId);

    return (eNOERROR);
}   /* GEO_CloseScan() */



Four GEO_NextObject(
    GEO_Handle*     handle,     /* IN : system handle */
    Four            scanId,     /* IN : scan id */
    OID*            oid,        /* OUT : object id */
    GEO_Cursor**    cursor      /* OUT : cursor */
)
{
    Four                e;          /* error number */
    OID                 objectId;   /* object ID */
    Boolean             found;      /* SM_TRUE if next object is found */
    float               minx, miny;
    float               maxx, maxy; /* min and max values of coordinates */
    float               o_minx, o_miny;
    float               o_maxx, o_maxy; /* min and max values of coordinates */
    Four                x1, y1;
    Four                x2, y2;     /* coordinate */
    One                 slope;      /* slope of line segment */
    Four                scanType;   /* scan type */


    /* check parameters */
    if(!GEO_VALID_SCANID(handle, scanId))
	{
        e = LOM_NextObject(&GEO_GET_LOM_SYSTEMHANDLE(handle), scanId, oid, cursor);
        if ( e == EOS ) return EOS; /* end of scan */
        if ( e < 0) GEO_ERROR(handle, e);

        return eNOERROR;
	}

	/* Replace GEO_SCANTABLE(handle)[scanId] with scanEntry */
	geo_ScanTableEntry& scanEntry = GEO_SCANTABLE(handle)[scanId];

    /* if the scanType is Nearest Object Query return the next object in buffer */
    if(scanEntry.scanType == GEO_SCAN_NEAREST_OBJECT)
    {
		float radius;
		while(scanEntry.distanceIndex >= scanEntry.distances.numberOfItems())
		{
			LockParameter lockup;
			GEO_Point queryPoint;
			GEO_Region queryRegion;
			OID oid2;
			
			lockup.mode  = L_IX;
			lockup.duration = L_COMMIT;
			
			queryPoint = scanEntry.info.queryPoint;

			radius = scanEntry.knninfo.radius;

			queryRegion.x1 = queryPoint.x - radius;
			queryRegion.x2 = queryPoint.x + radius;
			queryRegion.y1 = queryPoint.y - radius;
			queryRegion.y2 = queryPoint.y + radius;
						
			e = GEO_OpenMBRqueryScan(handle, scanEntry.otn, scanEntry.knninfo.mlgfIndexId, queryRegion, GEO_SPATIAL_INTERSECT, 0, NULL, &lockup);
			if(e < 0) GEO_ERROR(handle, e);
			Four geoScanId = e;

			char pointText[256];
			sprintf(pointText, "POINT(%f %f)", queryPoint.x, queryPoint.y);
			WKBinary wkbQueryPoint = WKTextToWKBinary(pointText);
			Four nTuples = scanEntry.distances.numberOfItems();
			scanEntry.distances.resize(0);
			
			if(!scanEntry.tidJoinHashtable)
			{
				while((e = GEO_NextObject(handle, geoScanId, &oid2, NULL)) != EOS)
				{
					WKBinary wkb;
					e = GEO_GetWKB(handle, scanEntry.otn, SM_FALSE, &oid2, scanEntry.columnNo, &wkb);
					if(e < 0) GEO_ERROR(handle, e);

					float distance = wkb.Distance(wkbQueryPoint);
					if(distance <= radius)
					{
						scanEntry.distances.add(DistanceOidPair(distance, oid2));
					}
				}
			}
			else
			{
				GEO_OIDHashTableNode* key = new GEO_OIDHashTableNode();
				GEO_OIDHashTable* hashtable = scanEntry.tidJoinHashtable;
				while((e = GEO_NextObject(handle, geoScanId, &oid2, NULL)) != EOS)
				{
					WKBinary wkb;
					e = GEO_GetWKB(handle, scanEntry.otn, SM_FALSE, &oid2, scanEntry.columnNo, &wkb);
					if(e < 0) GEO_ERROR(handle, e);

					float distance = wkb.Distance(wkbQueryPoint);
					key->setOID(oid2);
					if(distance <= radius && hashtable->find(key))
					{
						scanEntry.distances.add(DistanceOidPair(distance, oid2));
					}
				}
				delete key;
			}
			e = GEO_CloseScan(handle, geoScanId);
			if(e < 0) GEO_ERROR(handle, e);

			scanEntry.knninfo.radius = radius * 2;
			
			if(nTuples == scanEntry.distances.numberOfItems() && radius >= 1000000) return EOS;
			
#ifdef NOUSE_QSORT
			scanEntry.distances.sort();
#else
			qsort((DistanceOidPair*)scanEntry.distances.ptr(),scanEntry.distances.numberOfItems(),sizeof(DistanceOidPair), OOSQL_TCSArray<DistanceOidPair>::cmp);
#endif


		}
		*oid = scanEntry.distances[scanEntry.distanceIndex].m_oid;
		scanEntry.distanceIndex ++;

		return eNOERROR;
    }

    /* get GEO open class number */
        e = LOM_NextObject(&GEO_GET_LOM_SYSTEMHANDLE(handle), scanId, oid, cursor);
        if ( e == EOS ) return EOS; /* end of scan */
        if ( e < 0) GEO_ERROR(handle, e);

		if(cursor) scanEntry.cursor = *cursor;

        return eNOERROR;

}
Four GEO_AddIndex(GEO_Handle* handle, Four volId, char* tableName, char* indexName, GEO_IndexDesc* idesc, GEO_IndexID* iid)
{
	Four e;

	if(idesc->indexType != SM_INDEXTYPE_MLGF)
	{
		e = LOM_AddIndex(&GEO_GET_LOM_SYSTEMHANDLE(handle), volId, tableName, indexName, idesc, iid);
	    if(e < 0) GEO_ERROR(handle, e);
		return e;
	}

	e = LOM_AddIndex(&GEO_GET_LOM_SYSTEMHANDLE(handle), volId, tableName, indexName, idesc, iid);
	if(e < 0) GEO_ERROR(handle, e);

	e = LOM_OpenClass(&GEO_GET_LOM_SYSTEMHANDLE(handle), volId, tableName);
	if(e < 0) GEO_ERROR(handle, e);
	Four otn = e;

	e = geo_SetOpenTableTableEntry(handle, volId, tableName, otn);
	if(e < 0) GEO_ERROR(handle, e);

	LockParameter lockup;
	lockup.duration = L_COMMIT;
	lockup.mode     = L_S;
	e = LOM_OpenSeqScan(&GEO_GET_LOM_SYSTEMHANDLE(handle), otn, FORWARD, 0, NULL, &lockup);
	if(e < 0) GEO_ERROR(handle, e);
	Four scanId = e;

	OID oid;
	while((e = LOM_NextObject(handle, scanId, &oid, NULL)) != EOS)
	{
		if(e < 0) GEO_ERROR(handle, e);

		e = geo_MLGF_IndexObject(handle, scanId, SM_TRUE, &oid);
		if(e < 0) GEO_ERROR(handle, e);
	}
	e = LOM_CloseScan(&GEO_GET_LOM_SYSTEMHANDLE(handle), scanId);
	if(e < 0) GEO_ERROR(handle, e);

	e = LOM_CloseClass(handle, otn);
	if(e < 0) GEO_ERROR(handle, e);

	return eNOERROR;
}

Four GEO_DropIndex(GEO_Handle* handle, Four volId, char* indexName)
{
	Four e;

	e = LOM_DropIndex(&GEO_GET_LOM_SYSTEMHANDLE(handle), volId, indexName);
	if(e < 0) GEO_ERROR(handle, e);

	return eNOERROR;
}

MLGF_HashValue geo_FloatToHashValue(float f)
{
	union {
		float f;
		struct {
			unsigned sign     : 1;
			unsigned exponent : 8;
			unsigned mantissa :23;
		} encoding;
		MLGF_HashValue h;
	} hash;

	hash.f = f;

	if(hash.f >= 0)
	{
		hash.encoding.sign = 1;
	}
	else
	{
		hash.encoding.sign = 0;
		hash.encoding.exponent = 255 - hash.encoding.exponent;
		hash.encoding.mantissa = 0x007FFFFF - hash.encoding.mantissa;
	}

	return hash.h;
}

float geo_HashValueToFloat(MLGF_HashValue h)
{
	union {
		float f;
		struct {
			unsigned sign     : 1;
			unsigned exponent : 8;
			unsigned mantissa :23;
		} encoding;
		MLGF_HashValue h;
	} hash;

	hash.h = h;

	if(hash.encoding.sign == 1)
	{
		hash.encoding.sign = 0;
	}
	else
	{
		hash.encoding.sign = 1;
		hash.encoding.exponent = 255 - hash.encoding.exponent;
		hash.encoding.mantissa = 0x007FFFFF - hash.encoding.mantissa;
	}

	return hash.f;
}

Four GEO_GetMBRFromUDTObject(GEO_Handle* handle, char* data, Four length, float* xmin, float* ymin, float* xmax, float* ymax)
{
	Four e;

	GEO_SpatialUDTObjectReader reader(data, length);
	reader.GetMBR(*xmin, *ymin, *xmax, *ymax);
	return eNOERROR;
}

Four GEO_GetMBR(GEO_Handle* handle, Four otnOrScanId, Boolean useScanFlag, OID *oid, Four colNo, float* xmin, float* ymin, float* xmax, float* ymax)
{
	Four e;

	OOSQL_TCDynStr object(geoMemoryManager);
	e = geo_spatial_GetObject(handle, otnOrScanId, useScanFlag, oid, colNo, object);
	if(e < 0) GEO_ERROR(handle, e);
	if(object.length() != 0)
	{
		e = GEO_GetMBRFromUDTObject(handle, (char*)object.str(), object.length(), xmin, ymin, xmax, ymax);
		if(e < 0) GEO_ERROR(handle, e);
	}
	return eNOERROR;
}
#endif
