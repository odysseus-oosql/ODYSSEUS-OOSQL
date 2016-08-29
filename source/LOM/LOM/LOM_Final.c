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

/*
 * Module: LOM_Final.c
 *
 * Description:
 *  Finalize the LOM.
 *
 * Imports:
 *  LRDS_finalVarArray(LOM_GET_LRDS_HANDLE(handle), )
 *  LRDS_finalHeap(LOM_GET_LRDS_HANDLE(handle), )
 *  SM_FinalSharedDS()
 *  SM_FinalLocalDS() 
 *  LRDS_Dismount(LOM_GET_LRDS_HANDLE(handle), )
 *
 * Exports:
 *  Four LOM_FinalSharedDS(handle, )
 *  Four LOM_FinalLocalDS(handle, ) 
 *
 * Returns:
 *  Error code
 *   some erros caused by function calls
 */


#include "LOM_Internal.h"
#include "LOM.h"
#include "Catalog.h"
#include <dlfcn.h>

Four LOM_FinalLocalDS(LOM_Handle *handle);

Four LOM_DestroyHandle(
    LOM_Handle *handle, 
    Four procIndex
)
{
    Four e;

    e = Catalog_FinalLocalDS(handle);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    e = LOM_FinalLocalDS(handle);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    e = LRDS_FreeHandle(LOM_GET_LRDS_HANDLE(handle));
    if (e < eNOERROR) LOM_ERROR(handle, e);

    e = LRDS_Final();
    if(e < eNOERROR) LOM_ERROR(NULL, e);

    return eNOERROR;
}

Four LOM_FinalLocalDS(
    LOM_Handle *handle
)
{
    Four e;         /* error number */
    Four i;         /* temporary variable */

	e = lom_ScanTableFinal(handle);
	if (e < 0) LOM_ERROR(handle, e);

	e = lom_ODMGcollectionScanTableFinal(handle);
    if (e < 0) LOM_ERROR(handle, e);

	e = lom_RelationshipScanTableFinal(handle);
    if (e < 0) LOM_ERROR(handle, e);

	e = LRDS_finalVarArray(LOM_GET_LRDS_HANDLE(handle), LOM_BULKLOADTABLE_PTR(handle));
    if(e < eNOERROR) LOM_ERROR(handle, e);

	/* Finialize all dynamic library handle */
	for (i = 0; i < LOM_DLFCNTABLE_ENTRIES(handle); i++) 
	{
		if(LOM_DLFCNTABLE(handle)[i].handle.dllHandle)
		{
#ifdef USE_RPC
			if(LOM_DLFCNTABLE(handle)[i].handle.dllClient == NULL)
			{
				e = dlclose(LOM_DLFCNTABLE(handle)[i].handle.dllHandle);
				if(e < eNOERROR) LOM_ERROR(handle, e);

				LOM_DLFCNTABLE(handle)[i].handle.dllHandle = NULL;
			}
			else
			{
				puts("close dll client");
			}
#else
			e = dlclose(LOM_DLFCNTABLE(handle)[i].handle.dllHandle);
			if(e < eNOERROR) LOM_ERROR(handle, e);

			LOM_DLFCNTABLE(handle)[i].handle.dllHandle = NULL;
#endif
		}
	}
	e = LRDS_finalVarArray(LOM_GET_LRDS_HANDLE(handle), LOM_DLFCNTABLE_PTR(handle));
	if(e < eNOERROR) LOM_ERROR(handle, e);

	/* Free global instance */
    LOM_GDSTABLE[handle->instanceId].inUse = 0;

    return(eNOERROR);
    
} /* LOM_FinalLocalDS(handle, ) */

Four LOM_FlushAll(
    LOM_Handle *handle
)
{
    Four i;
    Four e;


    /* Finalize the allocated entries in the Scan Table */
	for (i = 0; i < LOM_SCANTABLE_ENTRIES(handle); i++)
	{
		/* if this entry is being used */
		if (LOM_SCANTABLE(handle)[i].lrdsScanId != NIL)
		{
			switch(LOM_SCANTABLE(handle)[i].scanType)
			{
			case LOM_GENERAL_SCAN:
			case LOM_INVERTEDFILE_KEYWORDBASED_SCAN:
				e = LOM_CloseScan(handle, i);
				if (e < eNOERROR) LOM_ERROR(handle, e);
				break;
			case LOM_INVERTEDFILE_OIDBASED_SCAN:
				e = LOM_Text_Scan_Close(handle, i);
				if (e < eNOERROR) LOM_ERROR(handle, e);
				break;
			default:
				LOM_ERROR(handle, eINTERNAL_LOM);
			}
		}
	}

    /* Finalize the allocated entries in the Relationship Scan Table */
	for (i = 0; i < LOM_RELATIONSHIPSCANTABLE_ENTRIES(handle); i++)
	{
		/* if this entry is being used */
		if (LOM_RELATIONSHIPSCANTABLE(handle)[i].baseScanId != NIL)
		{
			e = LOM_Relationship_CloseScan(handle, i);
			if (e < eNOERROR) LOM_ERROR(handle, e);
		}
	}

    /* Finalize the allocated entries in the ODMG Collection Scan Table */
	for (i = 0; i < LOM_ODMG_COLLECTIONSCANTABLE_ENTRIES(handle); i++)
	{
		/* if this entry is being used */
		if (LOM_ODMG_COLLECTIONSCANTABLE(handle)[i].lrdsCollectionScanId != NIL)
		{
			e = LOM_ODMG_Collection_Scan_Close(handle, LOM_ODMG_COLLECTIONSCANTABLE(handle)[i].complexType, i);
			if (e < eNOERROR) LOM_ERROR(handle, e);
		}
	}


    /* Finalize the allocated entries in the User Open Relation Table. */
    for (i = 0; i < LOM_NUM_OF_ENTRIES_OF_USEROPENCLASSTABLE; i++) {
        while(LOM_USEROPENCLASSTABLE(handle)[i].count > 0) {
            e = LOM_CloseClass(handle, i);
            if(e < eNOERROR) LOM_ERROR(handle, e);
        }
    }

    return eNOERROR;
}
