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
 * Module: LOM_Init.c
 *
 * Description:
 *  Initialize the LOM layer 
 *  In LOM, we should initialize Open Relation Table.
 *
 * Imports:
 *
 * Exports:
 *
 * Returns:
 *  Error code
 *    some errors caused by function calls
 */

#include "LOM_Internal.h"
#include "LOM.h"
#include "Catalog.h"


/*
** Global Variables (Not Shared Variables) 
*/

VarArray lomGDSInstanceTable = {0, NULL};

Four LOM_InitLocalDS(LOM_Handle *handle);

Four LOM_CreateHandle(
    LOM_Handle *handle, 
    Four *procIndex
)
{
    Four e;

    e = LRDS_Init();
    if(e < eNOERROR) LOM_ERROR(handle, e);

    e = LRDS_AllocHandle(&(LOM_GET_LRDS_HANDLE(handle)));
	if (e < eNOERROR) LOM_ERROR(handle, e);

    e = LOM_InitLocalDS(handle);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    e = Catalog_InitLocalDS(handle);
    if(e < eNOERROR) LOM_ERROR(handle, e);


    return(eNOERROR);
}

Four LOM_InitLocalDS(
    LOM_Handle *handle
)
{
    Four e;         /* error number */
    Four i;         /* index variable */
    Four j;
    Four idxForGDSTable;

    /* initialize global data structure table */
    if(lomGDSInstanceTable.nEntries == 0) {
        e = LRDS_initVarArray(LOM_GET_LRDS_HANDLE(handle), &lomGDSInstanceTable, sizeof(lom_GDSInstance), LOM_INIT_NUM_OF_THREADS);
        if (e < 0) {
            LOM_ERROR(handle, e);
        }
        for (i = 0; i < lomGDSInstanceTable.nEntries; i++) {
            LOM_GDSTABLE[i].inUse = 0;
        }
    }

    /* find the available entry of global data structure table */
    for (i = 0; i < lomGDSInstanceTable.nEntries; i++) {
        if(LOM_GDSTABLE[i].inUse == 0) break;
    }

    /* if we donot find the available entry, do doubling */
    if(i == lomGDSInstanceTable.nEntries) {
        e = LRDS_doublesizeVarArray(LOM_GET_LRDS_HANDLE(handle), &lomGDSInstanceTable, sizeof(lom_GDSInstance));
        if(e < eNOERROR) LOM_ERROR(handle, e);

        /* Initialize the newly allocated entries */
        for (j = i; j < lomGDSInstanceTable.nEntries; j++) {
            LOM_GDSTABLE[i].inUse = 0;
        }
    }

    idxForGDSTable = i;
    LOM_GDSTABLE[idxForGDSTable].inUse = 1;

    /* set return value */
    handle->instanceId = idxForGDSTable;

    /* Initialize the allocated entries in the User Open Relation Table. */
    for (i = 0; i < LOM_NUM_OF_ENTRIES_OF_USEROPENCLASSTABLE; i++) {

        /* initialize */
        LOM_USEROPENCLASSTABLE(handle)[i].count = 0;
		LOM_USEROPENCLASSTABLE(handle)[i].openedByLOM_GetOpenClassNum = SM_FALSE;
        LOM_USEROPENCLASSTABLE(handle)[i].numOfTextAttrs = 0;
        LOM_USEROPENCLASSTABLE(handle)[i].numOfodmgCollAttrs = 0;
        LOM_USEROPENCLASSTABLE(handle)[i].ornForContentTable = NIL;
        LOM_USEROPENCLASSTABLE(handle)[i].ornForDeletionListTable = NIL;
        LOM_USEROPENCLASSTABLE(handle)[i].lrdsScanIdForTextScan = NIL;
        LOM_USEROPENCLASSTABLE(handle)[i].contentTableScanIdForTextScan = NIL;

        for(j = 0; j < LOM_MAXNUMOFTEXTCOLUMN; j++) {
            LOM_USEROPENCLASSTABLE(handle)[i].textColNo[j] = NIL;
            LOM_USEROPENCLASSTABLE(handle)[i].ornForInvertedIndexTable[j] = NIL;
            LOM_USEROPENCLASSTABLE(handle)[i].ornForDocIdIndexTable[j] = NIL;
			LOM_USEROPENCLASSTABLE(handle)[i].handleForDLOfKeywordExtractor[j] = NULL;
			LOM_USEROPENCLASSTABLE(handle)[i].handleForDLOfFilter[j] = NULL;
			LOM_USEROPENCLASSTABLE(handle)[i].handleForDLOfStemizer[j] = NULL;
			LOM_USEROPENCLASSTABLE(handle)[i].fptrToKeywordExtractor[j] = NULL;
			LOM_USEROPENCLASSTABLE(handle)[i].fptrToFilter[j] = NULL;
			LOM_USEROPENCLASSTABLE(handle)[i].fptrToStemizer[j] = NULL;
        }

		for(j = 0; j < LOM_MAXNUMOFODMGCOLLCOLUMN; j++) {
            LOM_USEROPENCLASSTABLE(handle)[i].odmgCollColNo[j] = NIL;
            LOM_USEROPENCLASSTABLE(handle)[i].odmgCollDataOrn[j] = NIL;
		}
    }

    /* initialize scan Tables. */
	e = lom_ScanTableInit(handle);
	if (e < 0) LOM_ERROR(handle, e);

	e = lom_ODMGcollectionScanTableInit(handle);
    if (e < 0) LOM_ERROR(handle, e);

	e = lom_RelationshipScanTableInit(handle);
    if (e < 0) LOM_ERROR(handle, e);

	/* allocate and initialize bulk load table */
	e = LRDS_initVarArray(LOM_GET_LRDS_HANDLE(handle), LOM_BULKLOADTABLE_PTR(handle), sizeof(lom_BulkLoadTableEntry), LOM_INITBULKLOAD);
    if (e < 0) {
        LOM_ERROR(handle, e);
    }

    /* Initialize the allocated entries in the bulkload Table. */
    for (i = 0; i < LOM_BULKLOADTABLE_ENTRIES(handle); i++) 
        LOM_BULKLOADTABLE(handle)[i].lrdsBulkLoadId = NIL;

	/* allocate and initialize dlfcn table */
	e = LRDS_initVarArray(LOM_GET_LRDS_HANDLE(handle), LOM_DLFCNTABLE_PTR(handle), sizeof(lom_DllfcnTableEntry), LOM_INITDNFCN);
    if (e < 0) {
        LOM_ERROR(handle, e);
    }

    /* Initialize the allocated entries in the dlfcn Table. */
    for (i = 0; i < LOM_DLFCNTABLE_ENTRIES(handle); i++) 
	{
        LOM_DLFCNTABLE(handle)[i].handle.dllHandle = NULL;
#ifdef USE_RPC
		LOM_DLFCNTABLE(handle)[i].handle.dllClient = NULL;
#endif

		for(j = 0; j < LOM_MAXDLLFUNCPTRS; j++)
		{
			LOM_DLFCNTABLE(handle)[i].func[j].dllFunc   = NULL;
#ifdef USE_RPC
			LOM_DLFCNTABLE(handle)[i].func[j].dllClient = NULL;
#endif
		}
	}

	/* set flag which indicates if LOM uses rpc in dll function call or not */
	LOM_USERPCFORDLLFUNCTIONSFLAG(handle) = SM_FALSE;

#if defined(WIN32) && defined(USE_RPC)	
	rpc_nt_init();
#endif

    return(eNOERROR);
    
} /* LOM_InitLocalDS(handle, ) */

