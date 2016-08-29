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

#ifndef __GEO_INTERNALS_HXX__
#define __GEO_INTERNALS_HXX__

#define __NEW_DEF__
extern "C" {
#include "cosmos_r.h"
#include "LOM.h"
#include "Catalog.h"
#include "GEO_Internal.h"
}
#include "OOSQL_String.hxx"
#include "OOSQL_Array.hxx" 
#include "OOSQL_MemoryManager.hxx"
#include "GEO_OIDHashTable.hxx"
#include <vector>
#include <map>
#undef __NEW_DEF__

/*
** Global Variables (Not Shared Variables)
*/
struct DistanceOidPair {
	DistanceOidPair() : m_distance(0) {}
	DistanceOidPair(float distance, OID& oid) : m_distance(distance), m_oid(oid) {}
	float m_distance;
	OID   m_oid;
};

inline bool operator<(const DistanceOidPair& data1, const DistanceOidPair& data2)
{
	if(data1.m_distance < data2.m_distance)
		return true;
	else
		return false;
}

inline bool operator>(const DistanceOidPair& data1, const DistanceOidPair& data2)
{
	if(data1.m_distance > data2.m_distance)
		return true;
	else
		return false;
}

struct geo_ScanTableEntry {
	Four			otn;
	Four			scanType;		/* RegionQuery, ConnectedLineQuery ... */
	union
	{								/* coordinates */
		GEO_Point	queryPoint;		/* point coord. */
		GEO_Region	queryRegion;	/* region coord. */
	} info;
	Four			columnNo;	
	LRDS_Cursor*	cursor;			/* current cursor */
	OOSQL_TCArray<DistanceOidPair> distances;
	int				distanceIndex;
	struct
	{
		GEO_IndexID*    mlgfIndexId;	/* MLGF index id. */
		float			radius;			/* kNN doubling radius */
	} knninfo;
	GEO_OIDHashTable* tidJoinHashtable;  /* Hash table for storing oid */
};

struct GeometryColumnInfo {
	Four			colNo;
	OOSQL_TCDynStr		colName;
	GEO_IndexID		iid;
	MLGF_KeyDesc	kdesc;
};

struct geo_OpenTableTableEntry {
	Four						volId;
	OOSQL_TCDynStr						tableName;
	OOSQL_TCArray<GeometryColumnInfo>	geometryColumnInfos;
};

typedef std::map<int, geo_ScanTableEntry>		ScanTable;
typedef std::map<int, geo_OpenTableTableEntry>	OpenTableTable;

struct geo_GDSInstance {
	OpenTableTable	openTableTable;
	ScanTable		scanTable;
};
typedef std::map<int, geo_GDSInstance>			InstanceTable;

extern InstanceTable* geo_GDSInstanceTable;

inline bool GEO_VALID_HANDLE(GEO_Handle* handle)
{
	if(geo_GDSInstanceTable->find(handle->instanceId) != geo_GDSInstanceTable->end())
		return true;
	else
		return false;
}

inline bool GEO_VALID_SCANID(GEO_Handle* handle, Four scanId)
{
	ScanTable& scanTable = (*geo_GDSInstanceTable)[handle->instanceId].scanTable;
	if(scanTable.find(scanId) != scanTable.end())
		return true;
	else
		return false;
}

inline ScanTable& GEO_SCANTABLE(GEO_Handle* handle)
{
	return (*geo_GDSInstanceTable)[handle->instanceId].scanTable;
}

inline OpenTableTable& GEO_OPENTABLETABLE(GEO_Handle* handle)
{
	return (*geo_GDSInstanceTable)[handle->instanceId].openTableTable;
}


MLGF_HashValue geo_FloatToHashValue(float f);
float geo_HashValueToFloat(MLGF_HashValue h);

#endif  __GEO_INTERNALS_HXX__

