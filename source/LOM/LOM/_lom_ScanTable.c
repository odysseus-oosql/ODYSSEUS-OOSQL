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

#include "LOM_Internal.h"
#include "LOM.h"

Four lom_ScanTableInit(
	LOM_Handle*     handle         /* IN : system handle */
)
{
	Four e;
	Four i;

	/* Allocate some entries to the Scan Table. */
    e = Util_initVarArray(LOM_GET_LRDS_HANDLE(handle), LOM_SCANTABLE_PTR(handle), sizeof(lom_ScanTableEntry), INITSCAN);
    if (e < 0) LOM_ERROR(handle, e);

	/* Initialize the allocated entries in the Scan Table. */
    for (i = 0; i < LOM_SCANTABLE_ENTRIES(handle); i++)
    {
        LOM_SCANTABLE(handle)[i].lrdsScanId = NIL;
		LOM_SCANTABLE(handle)[i].contentTableScanId = NIL;

		/* make linked list between the scan table entries */
		LOM_SCANTABLE(handle)[i].nextFreeEntryIndex = i + 1;
    }
	LOM_SCANTABLE(handle)[i - 1].nextFreeEntryIndex = NIL;
	LOM_FIRSTFREESCANTABLEENTRYINDEX(handle) = 0;

	return eNOERROR;
}

Four lom_ScanTableFinal(
	LOM_Handle*     handle         /* IN : system handle */
)
{
	Four e;

    /* Finalize the scan table.*/
    e = Util_finalVarArray(LOM_GET_LRDS_HANDLE(handle), LOM_SCANTABLE_PTR(handle));
    if (e < 0) LOM_ERROR(handle, e);

	return eNOERROR;
}

Four lom_ScanTableAllocEntry(
	LOM_Handle*     handle         /* IN : system handle */
)
{
	Four	scanId;
	Four	e;
	Four	i;

	scanId = LOM_FIRSTFREESCANTABLEENTRYINDEX(handle);
	if(scanId == NIL)
	{
        /* there is no empty entry, double scantable */
		scanId = LOM_SCANTABLE_ENTRIES(handle);

        e = Util_doublesizeVarArray(LOM_GET_LRDS_HANDLE(handle), LOM_SCANTABLE_PTR(handle), sizeof(lom_ScanTableEntry));
        if(e < 0) LOM_ERROR(handle, e);

        /* initialize the newly allocated entries */
        for(i = scanId; i < LOM_SCANTABLE_ENTRIES(handle); i++)
        {
	        LOM_SCANTABLE(handle)[i].lrdsScanId = NIL;
			LOM_SCANTABLE(handle)[i].contentTableScanId = NIL;

			/* make linked list between the scan table entries */
			LOM_SCANTABLE(handle)[i].nextFreeEntryIndex = i + 1;
        }
		LOM_SCANTABLE(handle)[i - 1].nextFreeEntryIndex = NIL;
		LOM_FIRSTFREESCANTABLEENTRYINDEX(handle) = scanId;
	}

	LOM_FIRSTFREESCANTABLEENTRYINDEX(handle) = LOM_SCANTABLE(handle)[scanId].nextFreeEntryIndex;
	LOM_SCANTABLE(handle)[scanId].nextFreeEntryIndex = -1;

	return scanId;
}

Four lom_ScanTableFreeEntry(
	LOM_Handle*     handle,        /* IN : system handle */
	Four			scanId		   /* IN : scan id to be freed */
)
{
	LOM_SCANTABLE(handle)[scanId].lrdsScanId = NIL;
	LOM_SCANTABLE(handle)[scanId].contentTableScanId = NIL;

	LOM_SCANTABLE(handle)[scanId].nextFreeEntryIndex = LOM_FIRSTFREESCANTABLEENTRYINDEX(handle);
	LOM_FIRSTFREESCANTABLEENTRYINDEX(handle)         = scanId;

	return eNOERROR;
}




Four lom_RelationshipScanTableInit(
	LOM_Handle*     handle         /* IN : system handle */
)
{
	Four e;
	Four i;

	/* Allocate some entries to the Scan Table. */
    e = Util_initVarArray(LOM_GET_LRDS_HANDLE(handle), LOM_RELATIONSHIPSCANTABLE_PTR(handle), sizeof(lom_RelationshipScanTableEntry), INITSCAN);
    if (e < 0) LOM_ERROR(handle, e);

	/* Initialize the allocated entries in the Scan Table. */
    for (i = 0; i < LOM_RELATIONSHIPSCANTABLE_ENTRIES(handle); i++)
    {
        LOM_RELATIONSHIPSCANTABLE(handle)[i].baseScanId = NIL;

		/* make linked list between the scan table entries */
		LOM_RELATIONSHIPSCANTABLE(handle)[i].nextFreeEntryIndex = i + 1;
    }
	LOM_RELATIONSHIPSCANTABLE(handle)[i - 1].nextFreeEntryIndex = NIL;
	LOM_FIRSTFREERELATIONSHIPSCANTABLEENTRYINDEX(handle) = 0;

	return eNOERROR;
}

Four lom_RelationshipScanTableFinal(
	LOM_Handle*     handle         /* IN : system handle */
)
{
	Four e;

    /* Finalize the scan table.*/
    e = Util_finalVarArray(LOM_GET_LRDS_HANDLE(handle), LOM_RELATIONSHIPSCANTABLE_PTR(handle));
    if (e < 0) LOM_ERROR(handle, e);

	return eNOERROR;
}


Four lom_RelationshipScanTableAllocEntry(
	LOM_Handle*     handle         /* IN : system handle */
)
{
	Four	scanId;
	Four	e;
	Four	i;

	scanId = LOM_FIRSTFREERELATIONSHIPSCANTABLEENTRYINDEX(handle);
	if(scanId == NIL)
	{
        /* there is no empty entry, double scantable */
		scanId = LOM_RELATIONSHIPSCANTABLE_ENTRIES(handle);

        e = Util_doublesizeVarArray(LOM_GET_LRDS_HANDLE(handle), LOM_RELATIONSHIPSCANTABLE_PTR(handle), sizeof(lom_RelationshipScanTableEntry));
        if(e < 0) LOM_ERROR(handle, e);

        /* initialize the newly allocated entries */
        for(i = scanId; i < LOM_RELATIONSHIPSCANTABLE_ENTRIES(handle); i++)
        {
	        LOM_RELATIONSHIPSCANTABLE(handle)[i].baseScanId = NIL;

			/* make linked list between the scan table entries */
			LOM_RELATIONSHIPSCANTABLE(handle)[i].nextFreeEntryIndex = i + 1;
        }
		LOM_RELATIONSHIPSCANTABLE(handle)[i - 1].nextFreeEntryIndex = NIL;
		LOM_FIRSTFREERELATIONSHIPSCANTABLEENTRYINDEX(handle) = scanId;
	}

	LOM_FIRSTFREERELATIONSHIPSCANTABLEENTRYINDEX(handle) = LOM_RELATIONSHIPSCANTABLE(handle)[scanId].nextFreeEntryIndex;
	LOM_RELATIONSHIPSCANTABLE(handle)[scanId].nextFreeEntryIndex = -1;

	return scanId;
}

Four lom_RelationshipScanTableFreeEntry(
	LOM_Handle*     handle,        /* IN : system handle */
	Four			scanId		   /* IN : scan id to be freed */
)
{
	LOM_RELATIONSHIPSCANTABLE(handle)[scanId].baseScanId = NIL;

	LOM_RELATIONSHIPSCANTABLE(handle)[scanId].nextFreeEntryIndex = LOM_FIRSTFREERELATIONSHIPSCANTABLEENTRYINDEX(handle);
	LOM_FIRSTFREERELATIONSHIPSCANTABLEENTRYINDEX(handle)         = scanId;

	return eNOERROR;
}





Four lom_ODMGcollectionScanTableInit(
	LOM_Handle*     handle         /* IN : system handle */
)
{
	Four e;
	Four i;

	/* Allocate some entries to the Scan Table. */
    e = Util_initVarArray(LOM_GET_LRDS_HANDLE(handle), LOM_ODMG_COLLECTIONSCANTABLE_PTR(handle), sizeof(lom_ODMG_CollectionScanTableEntry), INITSCAN);
    if (e < 0) LOM_ERROR(handle, e);

	/* Initialize the allocated entries in the Scan Table. */
    for (i = 0; i < LOM_ODMG_COLLECTIONSCANTABLE_ENTRIES(handle); i++)
    {
        LOM_ODMG_COLLECTIONSCANTABLE(handle)[i].lrdsCollectionScanId = NIL;

		/* make linked list between the scan table entries */
		LOM_ODMG_COLLECTIONSCANTABLE(handle)[i].nextFreeEntryIndex = i + 1;
    }
	LOM_ODMG_COLLECTIONSCANTABLE(handle)[i - 1].nextFreeEntryIndex = NIL;
	LOM_FIRSTFREEODMGCOLLECTIONSCANTABLEENTRYINDEX(handle) = 0;

	return eNOERROR;
}

Four lom_ODMGcollectionScanTableFinal(
	LOM_Handle*     handle         /* IN : system handle */
)
{
	Four e;

    /* Finalize the scan table.*/
    e = Util_finalVarArray(LOM_GET_LRDS_HANDLE(handle), LOM_ODMG_COLLECTIONSCANTABLE_PTR(handle));
    if (e < 0) LOM_ERROR(handle, e);

	return eNOERROR;
}

Four lom_ODMGcollectionScanTableAllocEntry(
	LOM_Handle*     handle         /* IN : system handle */
)
{
	Four	scanId;
	Four	e;
	Four	i;

	scanId = LOM_FIRSTFREEODMGCOLLECTIONSCANTABLEENTRYINDEX(handle);
	if(scanId == NIL)
	{
        /* there is no empty entry, double scantable */
		scanId = LOM_ODMG_COLLECTIONSCANTABLE_ENTRIES(handle);

        e = Util_doublesizeVarArray(LOM_GET_LRDS_HANDLE(handle), LOM_ODMG_COLLECTIONSCANTABLE_PTR(handle), sizeof(lom_ODMG_CollectionScanTableEntry));
        if(e < 0) LOM_ERROR(handle, e);

        /* initialize the newly allocated entries */
        for(i = scanId; i < LOM_ODMG_COLLECTIONSCANTABLE_ENTRIES(handle); i++)
        {
	        LOM_ODMG_COLLECTIONSCANTABLE(handle)[i].lrdsCollectionScanId = NIL;

			/* make linked list between the scan table entries */
			LOM_ODMG_COLLECTIONSCANTABLE(handle)[i].nextFreeEntryIndex = i + 1;
        }
		LOM_ODMG_COLLECTIONSCANTABLE(handle)[i - 1].nextFreeEntryIndex = NIL;
		LOM_FIRSTFREEODMGCOLLECTIONSCANTABLEENTRYINDEX(handle) = scanId;
	}

	LOM_FIRSTFREEODMGCOLLECTIONSCANTABLEENTRYINDEX(handle) = LOM_ODMG_COLLECTIONSCANTABLE(handle)[scanId].nextFreeEntryIndex;
	LOM_ODMG_COLLECTIONSCANTABLE(handle)[scanId].nextFreeEntryIndex = -1;

	return scanId;
}

Four lom_ODMGcollectionScanTableFreeEntry(
	LOM_Handle*     handle,        /* IN : system handle */
	Four			scanId		   /* IN : scan id to be freed */
)
{
	LOM_ODMG_COLLECTIONSCANTABLE(handle)[scanId].lrdsCollectionScanId = NIL;

	LOM_ODMG_COLLECTIONSCANTABLE(handle)[scanId].nextFreeEntryIndex = LOM_FIRSTFREEODMGCOLLECTIONSCANTABLEENTRYINDEX(handle);
	LOM_FIRSTFREEODMGCOLLECTIONSCANTABLEENTRYINDEX(handle)          = scanId;

	return eNOERROR;
}
