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
 * Module: Catalog_LoadSystemCatalogs.c
 *
 * Description:
 *
 *
 *
 * Imports:
 *		Four LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), );
 *		Four Catalog_InsertEntry(LOM_Handle *handle, );
 *		Four LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), );
 *
 * Exports:
 *		Four Catalog_LoadSystemCatalogs(LOM_Handle *handle, Four volId) 
 *
*/

#include "LOM_Internal.h"
#include "Catalog_Internal.h"
#include "Catalog.h"


/* default system catalogs */
char *catalog_catalogTable[TOTALNUMLOMCATALOGS] = {LRDS_SYSTABLES_RELNAME, LRDS_SYSCOLUMNS_RELNAME, LRDS_SYSINDEXES_RELNAME,LOM_SYSCLASSES_CLASSNAME,LOM_INHERITANCE_CLASSNAME,LOM_SYSCOLUMNS_CLASSNAME,LOM_SYSMETHODS_CLASSNAME,LOM_SYSRELATIONSHIP_CLASSNAME};

Four Catalog_InitLocalDS(
	LOM_Handle *handle
) 
{
	Four v;

	for (v = 0; v < CATALOG_MAXNUMOFVOLS; v++) {
		LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].volId = 0;
		LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].temporaryClassIdToBeAssigned = -1;
	}

	return eNOERROR;
}

Four Catalog_FinalLocalDS(
	LOM_Handle *handle
) 
{
	Four i, v, e;

	for (v = 0; v < CATALOG_MAXNUMOFVOLS; v++) {
		if(LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].volId != 0) {
			for( i = 0 ; i < LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].count ; i++)
				if( ( e = Catalog_Dismount(handle, LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].volId)) < 0) return e;;
		}
	}
	return eNOERROR;
}

Four Catalog_Mount(
	LOM_Handle *handle, 
	Four volId
) /* volumn Id */
{
	Four orn;
	Four i;
	Four e;
	Four v;			/* arrary index */
	lrds_RelTableEntry	*relTableEntry;


	/* Check whether the volume is already mounted. */
	for (v = 0; v < CATALOG_MAXNUMOFVOLS; v++)
		if (LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].volId == volId) {
			LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].count++;
			return(eNOERROR);   /* already mounted */
		}

	/* Find an empty entry in LOM_GDSTABLE[handle->instanceId].catalogMountTable. */
	for (v = 0; v < CATALOG_MAXNUMOFVOLS; v++)
		if (LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].volId == 0) break; /* empty entry */

	if (v == CATALOG_MAXNUMOFVOLS)
		CATALOG_ERROR(handle, eTOOMANYVOLS_CATALOG);

	/* set volumn id as parameter volId */
	LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].volId = volId;

	/* initially count is set as 0 */
	LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].count = 1;

	LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].temporaryClassIdToBeAssigned = -1;

#ifdef OLDVERSION
	/* to count the number of all classes in this database */
	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId,LRDS_SYSTABLES_RELNAME);
	if(orn < 0) CATALOG_ERROR(orn);

	relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);

	CATALOG_GET_CLASSINFOTBL_SIZE(v) = relTableEntry->ri.nTuples * 2; 
	
	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if (e < 0) CATALOG_ERROR(e);
#else
	CATALOG_GET_CLASSINFOTBL_SIZE(handle, v) = MAXCLASSINFOTBLESIZE;
#endif

	/* class information table initialization */
	e = LRDS_initVarArray(LOM_GET_LRDS_HANDLE(handle), &(LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].classInfoTbl),
		sizeof(catalog_SysClassesOverlay), 
		MIN(CATALOG_GET_CLASSINFOTBL_SIZE(handle, v), MAXCLASSINFOTBLESIZE));
	if (e < 0) CATALOG_ERROR(handle, e);
	for(i = 0; i < MIN(CATALOG_GET_CLASSINFOTBL_SIZE(handle, v), MAXCLASSINFOTBLESIZE); i++) 
		CATALOG_GET_CLASSINFOTBL(handle, v)[i].inUse = 0;
	LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].freeClassInfoEntryIndex = 0;

	/* attribute information table initialization */
	e = LRDS_initVarArray(LOM_GET_LRDS_HANDLE(handle), &(LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].attrInfoTbl), sizeof(catalog_SysAttributesOverlay), MAXATTRINFOTBLESIZE);
	if (e < 0) CATALOG_ERROR(handle, e);
	for(i = 0; i < MAXATTRINFOTBLESIZE; i++) 
		CATALOG_GET_ATTRINFOTBL(handle, v)[i].inUse = 0;
	LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].freeAttrInfoEntryIndex = 0;
		
	/* method information table initialization */
	e = LRDS_initVarArray(LOM_GET_LRDS_HANDLE(handle), &(LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].methodInfoTbl), sizeof(catalog_SysMethodsOverlay), MAXMETHODINFOTBLESIZE);
	if (e < 0) CATALOG_ERROR(handle, e);
	for(i = 0; i < MAXMETHODINFOTBLESIZE; i++) 
		CATALOG_GET_METHODINFOTBL(handle, v)[i].inUse = 0;
	LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].freeMethodInfoEntryIndex = 0;
		
	/* index information table initialization */
	e = LRDS_initVarArray(LOM_GET_LRDS_HANDLE(handle), &(LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].indexInfoTbl), sizeof(catalog_SysIndexesOverlay), MAXINDEXINFOTBLESIZE);
	if (e < 0) CATALOG_ERROR(handle, e);
	for(i = 0; i < MAXINDEXINFOTBLESIZE; i++) 
		CATALOG_GET_INDEXINFOTBL(handle, v)[i].inUse = 0;
	LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].freeIndexInfoEntryIndex = 0;
		
	/* superclass information table initialization */
	e = LRDS_initVarArray(LOM_GET_LRDS_HANDLE(handle), &(LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].superClassInfoTbl), sizeof(catalog_SysSuperClassesOverlay), MAXSUPERCLASSINFOTBLESIZE);
	if (e < 0) CATALOG_ERROR(handle, e);
	for(i = 0; i < MAXSUPERCLASSINFOTBLESIZE; i++) 
		CATALOG_GET_SUPERCLASSINFOTBL(handle, v)[i].inUse = 0;
	LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].freeSuperClassInfoEntryIndex = 0;
		
	/* subclass information table initialization */
	e = LRDS_initVarArray(LOM_GET_LRDS_HANDLE(handle), &(LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].subClassInfoTbl), sizeof(catalog_SysSubClassesOverlay), MAXSUBCLASSINFOTBLESIZE);
	if (e < 0) CATALOG_ERROR(handle, e);
	for(i = 0; i < MAXSUBCLASSINFOTBLESIZE; i++) 
		CATALOG_GET_SUBCLASSINFOTBL(handle, v)[i].inUse = 0;
	LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].freeSubClassInfoEntryIndex = 0;

	/* For Relationship ... */
	e = LRDS_initVarArray(LOM_GET_LRDS_HANDLE(handle), &(LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].relationshipInfoTbl), sizeof(catalog_SysRelationshipOverlay), MAXRELATIONSHIPINFOTBLSIZE);
	if(e < 0) CATALOG_ERROR(handle, e);
        for(i = 0; i < MAXRELATIONSHIPINFOTBLSIZE; i++) 
            CATALOG_GET_RELATIONSHIPINFOTBL(handle, v)[i].inUse = 0;
	LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].freeRelationshipInfoEntryIndex = 0;

	return eNOERROR;
}

