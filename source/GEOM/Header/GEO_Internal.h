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

#ifndef	_GEO_INTERNAL_H_
#define	_GEO_INTERNAL_H_

#ifdef __cplusplus
extern "C" {
#endif
#include "cosmos_r.h"
#include "LOM.h"
#include "GEO_Err.h"
#include "trace.h"
#include <stdio.h>
#ifdef __cplusplus
}
#endif
#define LOM_MAXTABLENAME LOM_MAXTYPENAME


#ifndef MLGF_MIN_HASHVALUE
#define MLGF_MIN_HASHVALUE	((unsigned long)0x0)
#endif

#ifndef MLGF_MAX_HASHVALUE
#define MLGF_MAX_HASHVALUE	((unsigned long)0xFFFFFFFF)
#endif
#include "OOSQL_MemoryManager.hxx"

typedef struct {
	float	x, y;
} GEO_Point;

typedef struct {
	float	x1, y1, x2, y2;
} GEO_Region;

/*
** Global Variables
*/

/*
** Macros
*/

/* GEOM region query spatial operators */
#define	GEO_SPATIAL_INTERSECT 	1
#define	GEO_SPATIAL_CONTAIN		2
#define	GEO_SPATIAL_CONTAINED	4
#define GEO_SPATIAL_EQUAL		8
#define GEO_SPATIAL_DISJOINT	16
#define GEO_SPATIAL_NORTH		32
#define GEO_SPATIAL_SOUTH		64
#define GEO_SPATIAL_EAST		128
#define GEO_SPATIAL_WEST		256
#define GEO_SPATIAL_KNN			512

/* Geom spatial query type macro */
#define GEO_COMPOSITE_SCAN				(GEO_SCAN_REGION_INTERSECT | GEO_SCAN_REGION_CONTAIN | GEO_SCAN_REGION_CONTAINED | GEO_SCAN_REGION_DISJOINT | GEO_SCAN_REGION_EAST | GEO_SCAN_REGION_WEST | GEO_SCAN_REGION_NORTH | GEO_SCAN_REGION_SOUTH)

#define GEO_SCAN_REGION_INTERSECT		1
#define GEO_SCAN_REGION_CONTAIN			2
#define GEO_SCAN_REGION_CONTAINED		4
#define GEO_SCAN_REGION_DISJOINT		8
#define GEO_SCAN_REGION_EAST			16
#define GEO_SCAN_REGION_WEST			32
#define GEO_SCAN_REGION_NORTH			64
#define GEO_SCAN_REGION_SOUTH			128

#define GEO_COMPOSITE_MBR_SCAN			(GEO_SCAN_MBR_EQUAL | GEO_SCAN_MBR_INTERSECT | GEO_SCAN_MBR_CONTAIN | GEO_SCAN_MBR_CONTAINED | GEO_SCAN_MBR_DISJOINT | GEO_SCAN_MBR_EAST | GEO_SCAN_MBR_WEST | GEO_SCAN_MBR_NORTH | GEO_SCAN_MBR_SOUTH)

#define GEO_SCAN_MBR_EQUAL				256
#define GEO_SCAN_MBR_INTERSECT			512
#define GEO_SCAN_MBR_CONTAIN			1024
#define GEO_SCAN_MBR_CONTAINED			2048
#define GEO_SCAN_MBR_DISJOINT			4096
#define GEO_SCAN_MBR_EAST				8192
#define GEO_SCAN_MBR_WEST				16384
#define GEO_SCAN_MBR_NORTH				32768
#define GEO_SCAN_MBR_SOUTH				65536

#define	GEO_SCAN_POINT					131072
#define	GEO_SCAN_CONNECTED_LINESEG		131073
#define	GEO_SCAN_NEAREST_OBJECT			131074
#define	GEO_SCAN_SEQ_SCAN		    	131075
#define GEO_SCAN_INDEX_SCAN				131076

/* GEO predefined classes */
#define	GEO_NONSPATIAL_CLASSTYPE	0
#define	GEO_POINT_CLASSTYPE			1
#define	GEO_LINESEG_CLASSTYPE		2
#define	GEO_POLYGON_CLASSTYPE		3
#define	GEO_POLYLINE_CLASSTYPE		4

/* for trace */
#define	TR_GEO	0x1000

/* Is 'x' the valid scan identifier? */
#define GEO_VALID_OCN(handle, x)	(((x) >= 0)&&((x)<GEO_NUM_OF_ENTRIES_OF_USEROPENCLASSTABLE))

/* Error Handling */
#define GEO_ERROR(handle, e) \
BEGIN_MACRO \
Util_ErrorLog_Printf((char*)"Error : %d(%s) in %s:%d\n", (e), "GEOM", __FILE__, __LINE__); \
if (1) return(e);  \
END_MACRO


/* GEOM System handle */
typedef LOM_Handle  GEO_Handle;
typedef LRDS_Cursor GEO_Cursor;
#define GEO_GET_LOM_SYSTEMHANDLE(handle)	(*(handle))
#define GEO_GET_LRDS_HANDLE(handle)         (LOM_GET_LRDS_HANDLE(&GEO_GET_LOM_SYSTEMHANDLE(handle)))
typedef LOM_ColListStruct GEO_ColListStruct;
typedef LOM_IndexID GEO_IndexID;

#ifdef __cplusplus
extern "C" {
#endif

/*
	Noninterface Function Prototypes of GEOM
*/
Four geo_UpdateObjectByColListStruct(GEO_Handle* handle, Four ocnOrScanId, Boolean useScanFlag, OID *oid, Four nCols, GEO_ColListStruct *clist);
Four geo_FetchObjectByColListStruct(GEO_Handle* handle, Four ocnOrScanId, Boolean useScanFlag, OID *oid, Four nCols, GEO_ColListStruct *clist);
Four geo_ScanTableInit(GEO_Handle* handle);
Four geo_ScanTableFinal(GEO_Handle* handle);
Four geo_ScanTableAllocEntry(GEO_Handle* handle);
Four geo_ScanTableFreeEntry(GEO_Handle* handle, Four scanId);
/*
	Interface Function Prototypes of GEOM
*/
Four GEO_CreateClass(GEO_Handle* handle, Four volId, char* className, char* indexName, LOM_IndexDesc* idesc, Four nAttrs, AttrInfo* ainfo, Four nSuperclasses, char (*superClassName)[LOM_MAXTABLENAME], Four nMethods, MethodInfo* methodinfo, One tmpClassFlag, Four classId);
#ifdef ENABLE_OPENGIS_OPTIMIZATION
Four GEO_PrepareQuery(OOSQL_MemoryManager* pMemoryManager);
#endif
Four GEO_DestroyClass(GEO_Handle* handle, Four volId, char *className);
Four GEO_GetClassType(GEO_Handle* handle, Four volId, Four classId, One *classType);
Four GEO_GetClassName(GEO_Handle* handle, Four volId, Four classId, char *className);
Four GEO_OpenClass(GEO_Handle* handle, Four volId, char *className );
Four GEO_CloseClass(GEO_Handle* handle, Four ocn);
Four GEO_GetOpenClassNum(GEO_Handle* handle, Four volId, char *className );
Four GEO_CloseScan(GEO_Handle* handle, Four scanId );
Four GEO_OpenConnectedLineQueryScan(GEO_Handle* handle, Four ocn, GEO_IndexID *mlgfIndexId, GEO_Point queryPoint, Four errorBound, Four nBools, BoolExp* boolExp, LockParameter *lockup);
Four GEO_OpenNearestObjQueryScan(GEO_Handle* handle, Four ocn, GEO_IndexID *mlgfIndexId, GEO_Point queryPoint, Four numObjects, Four nBools, BoolExp* boolExp, LockParameter *lockup);
Four GEO_OpenPointQueryScan(GEO_Handle* handle, Four ocn, GEO_IndexID *mlgfIndexId, GEO_Point queryPoint, Four nBools, BoolExp* boolExp, LockParameter *lockup);
Four GEO_OpenRegionQueryScan(GEO_Handle* handle, Four ocn, GEO_IndexID *mlgfIndexId, GEO_Region queryRegion, Four spatialOp, Four nBools, BoolExp* boolExp, LockParameter *lockup);
Four GEO_OpenMBRqueryScan(GEO_Handle* handle, Four ocn, GEO_IndexID *mlgfIndexId, GEO_Region queryRegion, Four spatialOp, Four nBools, BoolExp* boolExp, LockParameter *lockup,
		GEO_IndexID* tidJoinIndexID = NULL, BoundCond* tidJoinIndexStartBound = NULL, BoundCond* tidJoinIndexStopBound = NULL);
Four GEO_OpenIndexScan(GEO_Handle* handle, Four ocn, GEO_IndexID *indexId, BoundCond *startCond, BoundCond *stopCond, Four nBools, BoolExp *boolExp, LockParameter *lockup);
Four GEO_OpenSeqScan(GEO_Handle* handle, Four ocn, Four scanDirection, Four nBools, BoolExp *boolExp, LockParameter *lockup);
Four GEO_UpdateObjectByColListStruct(GEO_Handle* handle, Four ocnOrScanId, Boolean useScanFlag, OID *oid, Four nCols, GEO_ColListStruct *clist);
Four GEO_FetchObjectByColListStruct(GEO_Handle* handle, Four ocnOrScanId, Boolean useScanFlag, OID *oid, Four nCols, GEO_ColListStruct *clist);
Four GEO_CreateObjectByColListStruct(GEO_Handle* handle, Four ocnOrScanId, Boolean useScanFlag, Four nCols, GEO_ColListStruct *clist, OID *oid);
Four GEO_DestroyObject(GEO_Handle* handle, Four ocnOrScanId, Boolean useScanFlag, OID *oid);
Four GEO_NextObject(GEO_Handle* handle, Four scanId, OID *oid, LRDS_Cursor** cursor);
Four GEO_GetMLGFindexID(GEO_Handle* handle, Four volId, char *tableName, char* columnName, GEO_IndexID *mlgfIndexId);
Four GEO_CheckUserPassword(GEO_Handle* handle, Four volumeId, char* userName, char* passwd, Four* userId);
Four GEO_GetSchemaIdOfUser(GEO_Handle* handle, Four userId, Four* schemaId);
char *GEO_Err(GEO_Handle* handle, Four err);
Four GEO_GetAttrNum(GEO_Handle* handle, Four volId, char* className, char* attrName, Two* attrNum);
Four GEO_GetClassID(GEO_Handle* handle, Four volId, char* className, Four* classId);
Four GEO_Relationship_CreateInstance(GEO_Handle* handle, Four fromocnOrScanId, Boolean fromUseScanFlag, Four toocnOrScanId, Boolean toUseScanFlag, Four relationshipId, OID* fromOID, OID* toOID);
Four GEO_Relationship_DestroyInstance(GEO_Handle* handle, Four fromocnOrScanId, Boolean fromUseScanFlag, Four toocnOrScanId, Boolean toUseScanFlag, Four relationshipId, OID* fromOID, OID* toOID);
Four GEO_Relationship_OpenScan(GEO_Handle* handle, Four ocnOrScanId, Boolean useScanFlag, OID *oid, Four relationshipId);

/* GEO_Relationship scan is being managed by GEO_SCANTABLE */
Four GEO_Relationship_CloseScan(GEO_Handle* handle, Four scanId);
Four GEO_Relationship_NextInstance(GEO_Handle* handle, Four scanId, Four nOIDs, OID* OIDs);

/* Prototypes in GEO_AliasFunces.c */
#define GEO_IndexDesc LOM_IndexDesc
#define GEO_MBR       LRDS_MBR

Four GEO_Dismount(GEO_Handle* handle, Four volId);
Four GEO_CreateHandle(GEO_Handle* handle, Four* pindex);
Four GEO_DestroyHandle(GEO_Handle* handle, Four pindex);
Four GEO_TransBegin(GEO_Handle* handle, XactID* xactID, ConcurrencyLevel ccLevel);
Four GEO_TransCommit(GEO_Handle* handle, XactID* xactID);
Four GEO_TransAbort(GEO_Handle* handle, XactID* xactID);
Four GEO_Relationship_Create(GEO_Handle* handle, Four volId, char* relationshipName,
                             Four fromClassId, Two fromAttrNum,
                             Four toClassId, Two toAttrNum,
                             One cardinality, One directionality,
                             Four* relationshipId);
Four GEO_Relationship_Destroy(GEO_Handle* handle, Four volId, char* relationshipName);
Four GEO_Relationship_GetId(GEO_Handle* handle, Four volId, char* relationshipName,
                            Four* relationshipId);
Four GEO_GetNewClassId(GEO_Handle* handle, Four volId, Boolean tmpClassFlag, Four* newClassId);
Four GEO_SetLineThickness(GEO_Handle* handle, Four lineThickness);
Four GEO_GetLineThickness(GEO_Handle* handle, Four* lineThickness);

/* Text Interface */
Four GEO_Text_CreateContent(GEO_Handle* handle, Four ocnOrScanId, Boolean useScanFlag, OID *oid, Two colNum, TextColStruct *text, LOM_TextDesc *textDesc);
Four GEO_Text_GetDescriptor(GEO_Handle* handle, Four ocnOrScanId, Boolean useScanFlag, OID *oid, Two colNo, LOM_TextDesc *textDesc);
Four GEO_Text_DestroyContent(GEO_Handle* handle, Four ocnOrScanId, Boolean useScanFlag, OID *oid,	Two colNo, LOM_TextDesc *textDesc);
Four GEO_Text_FetchContent(GEO_Handle* handle, Four ocnOrScanId, Boolean useScanFlag, OID *oid, Two colNo, TextColStruct *text, LOM_TextDesc *textDesc);
Four GEO_Text_UpdateContent(GEO_Handle* handle, Four ocnOrScanId, Boolean useScanFlag, OID *oid, Two colNo, TextColStruct *text, LOM_TextDesc *textDesc);
Four GEO_Text_GetNPostingsOfCurrentKeyword(GEO_Handle* handle, Four textScan, Four *nPostings);
Four GEO_Text_OpenIndexScan(GEO_Handle* handle, Four ocn, GEO_IndexID *indexId,	Four keywordKind, BoundCond *keywordStartBound, BoundCond *keywordStopBound, LockParameter *lockup);
Four GEO_Text_OpenIndexScan_GivenInvertedEntryTupleID(GEO_Handle* handle, Four ocn, Two colNo, TupleID* invertedTableEntryTupleID, LockParameter* lockup);
Four GEO_Text_Scan_Open(GEO_Handle* handle, Four ocn, OID *oid, Two colNo, Four keywordKind, BoundCond *keywordStartBound, BoundCond *keywordStopBound,	LockParameter *lockup);
Four GEO_Text_Scan_Close(GEO_Handle* handle, Four osn);
Four GEO_Text_GetNPostings(GEO_Handle* handle, Four ocn, GEO_IndexID *indexId, Four keywordKind, BoundCond *keywordStartBound, BoundCond *keywordStopBound, LockParameter *lockup, Four *nPostings);
Four GEO_Text_Scan_NextPosting(GEO_Handle* handle, Four textScan, Four bufferLength, char *postingBuffer, Four *requiredSize, PostingWeight *weight);
Four GEO_Text_NextPostings(GEO_Handle* handle, Four textScan, Four postingLengthBufferSize, char* postingLengthBuffer, Four postingBufferSize, char* postingBuffer, Four scanDirection, Four logicalIdHints, Four* nReturnedPosting, Four* requiredSize);
Four GEO_Text_GetCursorKeyword(GEO_Handle* handle, Four textScan, char *keyword);
Four GEO_Text_MakeIndex(GEO_Handle* handle, Four volId, Four temporaryVolId, char *className);
Four GEO_Text_BatchInvertedIndexBuild(GEO_Handle* handle, Four volId, Four temporaryVolId, char *className);

Four GEO_FormatDataVolume(GEO_Handle* handle, Four numOfDevices, char **devNameList, char *volName, Four volId, Four extentSize, Four *numPagesInDevice, Four segmentSize);
Four GEO_FormatTempDataVolume(GEO_Handle* handle, Four numOfDevices, char **devNameList, char *volName, Four volId, Four extentSize, Four *numPagesInDevice, Four segmentSize);
Four GEO_Mount(GEO_Handle* handle, Four numDevices, char** deviceNames, Four* volId);

#define EQUAL_OID(x, y) ((x).volNo == (y).volNo && (x).pageNo == (y).pageNo && (x).unique == (y).unique && (x).classID == (y).classID)

// New interface
Four GEO_OpenTable(GEO_Handle* handle, Four volId, char* tableName);
Four GEO_GetOpenTableNum(GEO_Handle* handle, Four volId, char* tableName);
Four GEO_CloseTable(GEO_Handle* handle, Four otn);
Four GEO_AddIndex(GEO_Handle* handle, Four volId,  char* className, char* indexName, GEO_IndexDesc* idesc, GEO_IndexID* iid);
Four GEO_DropIndex(GEO_Handle* handle, Four volId, char* indexName);
Four GEO_GetMBR(GEO_Handle* handle, Four otnOrScanId, Boolean useScanFlag, OID *oid, Four colNo, float* xmin, float* ymin, float* xmax, float* ymax);
Four GEO_GetMBRFromUDTObject(GEO_Handle* handle, char* data, Four length, float* xmin, float* ymin, float* xmax, float* ymax);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif

