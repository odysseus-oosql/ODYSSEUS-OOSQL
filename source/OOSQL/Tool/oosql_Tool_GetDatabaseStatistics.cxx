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

extern "C" {
#include "OOSQL_Param.h"
#include "cosmos_r.h"

int COSMOS_PAGESIZE = PAGESIZE;

#include "LOM_Internal.h"
#include "LOM.h"
#include "Catalog.h"
}
#include <string.h>
#include <stdlib.h>
#include "DBM.h"
#include "OOSQL_APIs_Internal.hxx"
#include "OOSQL_Error.h"
#include "OOSQL_Tool.hxx"

typedef struct {
    Four numTotalPages;
    Four numSlottedPage;
    Four numLOT_I_Node;
    Four numLOT_L_Node;
    Four numBtree_I_Node;
    Four numBtree_L_Node;
    Four numBtree_O_Node;
    Four numMLGF_I_Node;
    Four numMLGF_L_Node;
    Four numMLGF_O_Node;
    Four numExtEntryPage;
    Four numBitMapPage;
    Four numMasterPage;
    Four numVolInfoPage;
    Four numMetaDicPage;
    Four numUniqueNumPage;
} sm_NumPages;

extern "C" {
	Four _SM_GetStatistics_numExtents(Four, Two*, Four*, Four*);
	Four _SM_GetStatistics_numPages(Four, sm_NumPages*, Boolean, Boolean); 
}

Four oosql_Tool_GetDatabaseStatistics(OOSQL_SystemHandle* systemHandle, char* databaseName, Four databaseId)
{
	Four				e;
	Two					extentSize;
    Four				nTotalExtents;
    Four				nUsedExtents;
    sm_NumPages			pageInfo;
    Four				nUsedPages;
    Four				nFreePages;
	Four				i;
	Four				numObjectsInDatabase;
	Four				numObjectsInVolume;
	XactID				xactId;

	numObjectsInDatabase = 0;
	printf("-------------------------------------------------------------------------------\n");
	printf("Database Name = %s\n", databaseName);
	printf("-------------------------------------------------------------------------------\n");
	for(i = 0; i < MAXNUMOFVOLS; i++)
	{
		if(OOSQL_GDSINSTTABLE[systemHandle->instanceId].userMountVolumeTable[i].volID != -1)
		{
			e = OOSQL_TransBegin(systemHandle, &xactId, X_BROWSE_BROWSE);
			if(e < 0) OOSQL_ERR(e);

			e = _SM_GetStatistics_numExtents(OOSQL_GDSINSTTABLE[systemHandle->instanceId].userMountVolumeTable[i].volID, &extentSize, &nTotalExtents, &nUsedExtents);
			if (e < 0)
				OOSQL_ERR(e);

			e = _SM_GetStatistics_numPages(OOSQL_GDSINSTTABLE[systemHandle->instanceId].userMountVolumeTable[i].volID, &pageInfo, SM_FALSE, SM_FALSE);
			if (e < 0)
				OOSQL_ERR(e);
			printf("Volume Name = %s\n", OOSQL_GDSINSTTABLE[systemHandle->instanceId].userMountVolumeTable[i].volumeName);

			nUsedPages = pageInfo.numTotalPages;
			nFreePages = nTotalExtents * extentSize - nUsedPages;
			printf("# of total pages   = %ld pages (= %ld KBytes)\n", (unsigned long)nTotalExtents * extentSize, (unsigned long)nTotalExtents * (extentSize * COSMOS_PAGESIZE / 1024));
			printf("# of used pages    = %ld pages (= %ld KBytes)\n", nUsedPages, (unsigned long)nUsedPages * (COSMOS_PAGESIZE / 1024));
			printf("# of free pages    = %ld pages (= %ld KBytes)\n", nFreePages, (unsigned long)nFreePages * (COSMOS_PAGESIZE / 1024));
			printf("-------------------------------------------------------------------------------\n");
			printf("Size of Extent     = %ld pages\n", extentSize);
			printf("# of total extents = %ld extents (= %ld KBytes)\n", nTotalExtents, (unsigned long)nTotalExtents * (extentSize * COSMOS_PAGESIZE / 1024));
			printf("# of used extents  = %ld extents (= %ld KBytes)\n", nUsedExtents, (unsigned long)nUsedExtents * (extentSize * COSMOS_PAGESIZE / 1024));
			printf("# of free extents  = %ld extents (= %ld KBytes)\n", nTotalExtents - nUsedExtents, (unsigned long)(nTotalExtents - nUsedExtents) * (extentSize * COSMOS_PAGESIZE / 1024));
			printf("-------------------------------------------------------------------------------\n");
			printf("\n");

			e = OOSQL_GetNumTextObjectsInVolume(systemHandle, OOSQL_GDSINSTTABLE[systemHandle->instanceId].userMountVolumeTable[i].volID,
				                                &numObjectsInVolume);
			if(e < 0) OOSQL_ERR(e);
			numObjectsInDatabase += numObjectsInVolume;

			e = OOSQL_TransCommit(systemHandle, &xactId);
			if(e < 0) OOSQL_ERR(e);
		}
	}
	printf("-------------------------------------------------------------------------------\n");
	printf("Total text objects in the database is %ld\n", numObjectsInDatabase);
	printf("-------------------------------------------------------------------------------\n");

	return eNOERROR;
}
