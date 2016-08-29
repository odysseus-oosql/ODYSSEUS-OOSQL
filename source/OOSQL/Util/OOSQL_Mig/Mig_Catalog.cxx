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

#include "OOSQL_Mig.hxx"
extern "C" {
#include "Mig_Catalog.h"
}

Four mig_CreateTmpCatalogTableName(
    char            *orgTableName,
    char            *tmpTableName)
{
    sprintf(tmpTableName, "_mig_%s", orgTableName);

    return eNOERROR;
}


Four Mig_CheckCatalog(
	MigConfig&	config)
{
	Four		e;
	Four		v;
	Four		tableId;
	Four		tableInfo;
	Four		nCols;
	LOM_Handle	*handle;
	Four		volId;

	catalog_SysClassesOverlay   	*ptrToSysClasses;
	catalog_SysAttributesOverlay	*ptrToSysAttributes;


	handle 	= &config.handle.lomSystemHandle;
	volId 	= config.volumeId;
	

	e = LOM_GetClassID(handle, volId, LOM_SYSCOLUMNS_CLASSNAME, &tableId);
	LOM_CHECK_ERR(e);

	e = Catalog_GetClassInfo(handle, volId, tableId, &tableInfo);
	CATALOG_CHECK_ERR(e);
	
	v = Catalog_GetVolIndex(handle, volId);
	CATALOG_CHECK_ERR(v);

	ptrToSysClasses = &CATALOG_GET_CLASSINFOTBL((handle), v)[tableInfo];
	ptrToSysAttributes = &CATALOG_GET_ATTRINFOTBL((handle), v)[CATALOG_GET_ATTRINFOTBL_INDEX(ptrToSysClasses)];

	nCols = CATALOG_GET_ATTRNUM(ptrToSysClasses);

	if (nCols == 8)
	{
		config.originalOdysseusVersion = ODYSSEUS_NOALTERTABLE_NOSQL99;
	}
	else if (nCols == 9)
	{
		config.originalOdysseusVersion = ODYSSEUS_ALTERTABLE_NOSQL99;
	}
	else
	{
		sprintf(config.errorMessage, "It is not possible to recognize the version of given database schema.\n");
		TOOL_ERR(eUNHANDLED_CASE_MIG);
	}


	return eNOERROR;
}


Four Mig_ChangeCatalogForAlterTable(
	MigConfig&	config)
{
	Four		e;
	Four		v;
	Four		tableId;
	Four		tableInfo;
	Four		nCols;
	LOM_Handle	*handle;
	Four		volId;
    char        tmpTableName[LOM_MAXCLASSNAME];

	handle 	= &config.handle.lomSystemHandle;
	volId 	= config.volumeId;



	e = mig_CreateTmpCatalogTableName(LOM_SYSCOLUMNS_CLASSNAME, tmpTableName);
	TOOL_CHECK_ERR(e);

	e = mig_CreateColumnCatalogTableForAlterTable(handle, volId, &config.xactId, tmpTableName, CREATE_TMP); 
	TOOL_CHECK_ERR(e);

	e = mig_CopyColumnCatalogTableForAlterTable(handle, volId, LOM_SYSCOLUMNS_CLASSNAME, tmpTableName, OLD_TO_TMP); 
	TOOL_CHECK_ERR(e);

	e = mig_DestroyColumnCatalogTableForAlterTable(handle, volId, LOM_SYSCOLUMNS_CLASSNAME, DESTROY_OLD); 
	TOOL_CHECK_ERR(e);

	e = mig_CreateColumnCatalogTableForAlterTable(handle, volId, &config.xactId, LOM_SYSCOLUMNS_CLASSNAME, CREATE_NEW); 
	TOOL_CHECK_ERR(e);

	e = mig_CopyColumnCatalogTableForAlterTable(handle, volId, tmpTableName, LOM_SYSCOLUMNS_CLASSNAME, TMP_TO_NEW); 
	TOOL_CHECK_ERR(e);

	e = mig_DestroyColumnCatalogTableForAlterTable(handle, volId, tmpTableName, DESTROY_TMP); 
	TOOL_CHECK_ERR(e);

	return eNOERROR;
}


/*

	changed catalog table list
		lomSysClasses	=>	lomSysTables
		lomInheritance	=>	lomSysDirectSuperTables
		lomSysColumns	=>	lomSysColumns
		lomSysMethods	=> 	lomSysMethodSpecifications, lomSysMethodSpecificationParameters

	unchanged catalog table list
		lomSysIndexes
		lomSysTextIndexes
		lomSysNamedObject
		lomSysTextFilterInfo
		lomSysTextKeywordExtractorInfo
		lomSysTextPreferences
		lomSysTextStemizerInfo

	added catalog table list for SQL99
		lomSysUserDefinedTypes
		lomSysAttributes
		lomSysDataTypeDescriptor
		lomSysRoutines
		lomSysParameters
		lomSysDirectSuperTypes
		lomSysTransforms

	added catalog table list for authorization
		lomSysSchemas
		lomSysUsers
		lomSysRoles
		lomSysRoleAuth
		lomSysEnabledRoles
		lomSysDefaultRoles
		lomSysTablePrivileges
		lomSysColumnPrivileges
		lomSysSystemPrivileges
		lomSysUserId
		lomSysRoleId
		lomSysSchemaId

	removed catalog table list
		lomClassId
		lomSysRelationship
		lomRelationshipId
		lomTextFilterId
		lomSysTextStemizerInfo
		lomTextStemizerId

*/
Four Mig_ChangeCatalogForSQL99(
	MigConfig&	config)
{
	
	return eNOERROR;
}

