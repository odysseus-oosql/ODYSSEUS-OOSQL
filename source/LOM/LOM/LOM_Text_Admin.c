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

#ifndef SLIMDOWN_TEXTIR

#include <malloc.h>
#include <string.h>
#include <stdlib.h>
#include <dlfcn.h>
#include "LOM_Internal.h"
#include "LOM.h"
#include "Catalog.h"
#include "Catalog_Internal.h"


Four lom_Text_CreateFilterCatalog(
	LOM_Handle *handle, 
	Four volId
)
{
	ColInfo lomSysTextFilterInfo[LOM_SYSTEXTFILTERINFO_NUM_COLS];
	LRDS_IndexDesc idesc;
	IndexID iid;
	Four e;

	/* filter name */
	lomSysTextFilterInfo[LOM_SYSTEXTFILTERINFO_FILTERNAME_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysTextFilterInfo[LOM_SYSTEXTFILTERINFO_FILTERNAME_COLNO].type = SM_VARSTRING;
	lomSysTextFilterInfo[LOM_SYSTEXTFILTERINFO_FILTERNAME_COLNO].length = LOM_MAXFILTERNAME;

	/* filter version */
	lomSysTextFilterInfo[LOM_SYSTEXTFILTERINFO_VERSION_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysTextFilterInfo[LOM_SYSTEXTFILTERINFO_VERSION_COLNO].type = LOM_LONG_VAR;
	lomSysTextFilterInfo[LOM_SYSTEXTFILTERINFO_VERSION_COLNO].length = LOM_LONG_SIZE_VAR;

	/* fiter file path */
	lomSysTextFilterInfo[LOM_SYSTEXTFILTERINFO_FILTERFILEPATH_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysTextFilterInfo[LOM_SYSTEXTFILTERINFO_FILTERFILEPATH_COLNO].type = SM_VARSTRING;
	lomSysTextFilterInfo[LOM_SYSTEXTFILTERINFO_FILTERFILEPATH_COLNO].length = LOM_MAXFILTERFILEPATHNAME;

	/* filter function name */
	lomSysTextFilterInfo[LOM_SYSTEXTFILTERINFO_FILTERFUNCTIONNAME_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysTextFilterInfo[LOM_SYSTEXTFILTERINFO_FILTERFUNCTIONNAME_COLNO].type = SM_VARSTRING;
	lomSysTextFilterInfo[LOM_SYSTEXTFILTERINFO_FILTERFUNCTIONNAME_COLNO].length = LOM_MAXFILTERFUNCTIONNAME;

	/* filter no */
	lomSysTextFilterInfo[LOM_SYSTEXTFILTERINFO_FILTERNO_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysTextFilterInfo[LOM_SYSTEXTFILTERINFO_FILTERNO_COLNO].type = LOM_LONG_VAR;
	lomSysTextFilterInfo[LOM_SYSTEXTFILTERINFO_FILTERNO_COLNO].length = LOM_LONG_SIZE_VAR;
	
	idesc.indexType = SM_INDEXTYPE_BTREE;
	idesc.kinfo.btree.flag = KEYFLAG_UNIQUE | KEYFLAG_CLUSTERING;
	idesc.kinfo.btree.nColumns = 1;
	idesc.kinfo.btree.columns[0].colNo = LOM_SYSTEXTFILTERINFO_FILTERNO_COLNO;
	idesc.kinfo.btree.columns[0].flag = KEYINFO_COL_ASC;

	/* create sysTextFilterInfo */
	e = LRDS_CreateRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSTEXTFILTERINFO_CLASSNAME, &idesc,
		LOM_SYSTEXTFILTERINFO_NUM_COLS, &lomSysTextFilterInfo[0], SM_FALSE);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	idesc.indexType = SM_INDEXTYPE_BTREE;
	idesc.kinfo.btree.flag = KEYFLAG_UNIQUE;
	idesc.kinfo.btree.nColumns = 2;
	idesc.kinfo.btree.columns[0].colNo = LOM_SYSTEXTFILTERINFO_FILTERNAME_COLNO;
	idesc.kinfo.btree.columns[0].flag = KEYINFO_COL_ASC;
	idesc.kinfo.btree.columns[1].colNo = LOM_SYSTEXTFILTERINFO_VERSION_COLNO;
	idesc.kinfo.btree.columns[1].flag = KEYINFO_COL_ASC;

	/* add index on filtername + version of sysTextFilterInfo */
	e = LRDS_AddIndex(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSTEXTFILTERINFO_CLASSNAME, &idesc, &iid);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}

Four lom_Text_DestroyFilterCatalog(
	LOM_Handle *handle, 
	Four volId
)
{
	Four e;

	e = LRDS_DestroyRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSTEXTFILTERINFO_CLASSNAME);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}

Four lom_Text_CreateKeywordExtractorCatalog(
	LOM_Handle *handle, 
	Four volId
)
{
	ColInfo lomSysTextKeywordExtractorInfo[LOM_SYSTEXTKEYWORDEXTRACTORINFO_NUM_COLS];
	LRDS_IndexDesc idesc;
	IndexID iid;
	Four e;

	/* keyword extractor name */
	lomSysTextKeywordExtractorInfo[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORNAME_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysTextKeywordExtractorInfo[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORNAME_COLNO].type = SM_VARSTRING;
	lomSysTextKeywordExtractorInfo[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORNAME_COLNO].length = LOM_MAXKEYWORDEXTRACTORNAME;

	/* keyword extractor version */
	lomSysTextKeywordExtractorInfo[LOM_SYSTEXTKEYWORDEXTRACTORINFO_VERSION_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysTextKeywordExtractorInfo[LOM_SYSTEXTKEYWORDEXTRACTORINFO_VERSION_COLNO].type = LOM_LONG_VAR;
	lomSysTextKeywordExtractorInfo[LOM_SYSTEXTKEYWORDEXTRACTORINFO_VERSION_COLNO].length = LOM_LONG_SIZE_VAR;

	/* keyword extractor file path */
	lomSysTextKeywordExtractorInfo[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORFILEPATH_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysTextKeywordExtractorInfo[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORFILEPATH_COLNO].type = SM_VARSTRING;
	lomSysTextKeywordExtractorInfo[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORFILEPATH_COLNO].length = LOM_MAXKEYWORDEXTRACTORFILEPATHNAME;

	/* keyword extractor function name */
	lomSysTextKeywordExtractorInfo[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORFUNCTIONNAME_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysTextKeywordExtractorInfo[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORFUNCTIONNAME_COLNO].type = SM_VARSTRING;
	lomSysTextKeywordExtractorInfo[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORFUNCTIONNAME_COLNO].length = LOM_MAXKEYWORDEXTRACTORFUNCTIONNAME;

	/* keyword extractor get next posting info function name */
	lomSysTextKeywordExtractorInfo[LOM_SYSTEXTKEYWORDEXTRACTORINFO_GETNEXTPOSTINGINFOFUNCTIONNAME_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysTextKeywordExtractorInfo[LOM_SYSTEXTKEYWORDEXTRACTORINFO_GETNEXTPOSTINGINFOFUNCTIONNAME_COLNO].type = SM_VARSTRING;
	lomSysTextKeywordExtractorInfo[LOM_SYSTEXTKEYWORDEXTRACTORINFO_GETNEXTPOSTINGINFOFUNCTIONNAME_COLNO].length = LOM_MAXKEYWORDEXTRACTORFUNCTIONNAME;

	/* keyword extractor finalize keyword extractor function name */
	lomSysTextKeywordExtractorInfo[LOM_SYSTEXTKEYWORDEXTRACTORINFO_FINALIZEKEYWORDEXTRACTORFUNCTIONNAME_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysTextKeywordExtractorInfo[LOM_SYSTEXTKEYWORDEXTRACTORINFO_FINALIZEKEYWORDEXTRACTORFUNCTIONNAME_COLNO].type = SM_VARSTRING;
	lomSysTextKeywordExtractorInfo[LOM_SYSTEXTKEYWORDEXTRACTORINFO_FINALIZEKEYWORDEXTRACTORFUNCTIONNAME_COLNO].length = LOM_MAXKEYWORDEXTRACTORFUNCTIONNAME;

	/* keyword extractor no */
	lomSysTextKeywordExtractorInfo[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORNO_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysTextKeywordExtractorInfo[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORNO_COLNO].type = LOM_LONG_VAR;
	lomSysTextKeywordExtractorInfo[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORNO_COLNO].length = LOM_LONG_SIZE_VAR;
	
	idesc.indexType = SM_INDEXTYPE_BTREE;
	idesc.kinfo.btree.flag = KEYFLAG_UNIQUE | KEYFLAG_CLUSTERING;
	idesc.kinfo.btree.nColumns = 1;
	idesc.kinfo.btree.columns[0].colNo = LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORNO_COLNO;
	idesc.kinfo.btree.columns[0].flag = KEYINFO_COL_ASC;

	/* create sysTextFilterInfo */
	e = LRDS_CreateRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSTEXTKEYWORDEXTRACTORINFO_CLASSNAME, &idesc,
		LOM_SYSTEXTKEYWORDEXTRACTORINFO_NUM_COLS, &lomSysTextKeywordExtractorInfo[0], SM_FALSE);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	idesc.indexType = SM_INDEXTYPE_BTREE;
	idesc.kinfo.btree.flag = KEYFLAG_UNIQUE;
	idesc.kinfo.btree.nColumns = 2;
	idesc.kinfo.btree.columns[0].colNo = LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORNAME_COLNO;
	idesc.kinfo.btree.columns[0].flag = KEYINFO_COL_ASC;
	idesc.kinfo.btree.columns[1].colNo = LOM_SYSTEXTKEYWORDEXTRACTORINFO_VERSION_COLNO;
	idesc.kinfo.btree.columns[1].flag = KEYINFO_COL_ASC;

	/* add index on keyword extractorname + version of sysTextFilterInfo */
	e = LRDS_AddIndex(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSTEXTKEYWORDEXTRACTORINFO_CLASSNAME, &idesc, &iid);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}

Four lom_Text_DestroyKeywordExtractorCatalog(
	LOM_Handle *handle, 
	Four volId
)
{
	Four e;

	e = LRDS_DestroyRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSTEXTKEYWORDEXTRACTORINFO_CLASSNAME);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}

Four lom_Text_CreatePreferenceCatalog(
	LOM_Handle *handle, 
	Four volId
)
{
	ColInfo lomSysTextPreferences[LOM_SYSTEXTKEYWORDEXTRACTORINFO_NUM_COLS];
	LRDS_IndexDesc idesc;
	IndexID iid;
	Four e;

	/* class id */
	lomSysTextPreferences[LOM_SYSTEXTPREFERENCES_CLASSID_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysTextPreferences[LOM_SYSTEXTPREFERENCES_CLASSID_COLNO].type = LOM_LONG_VAR;
	lomSysTextPreferences[LOM_SYSTEXTPREFERENCES_CLASSID_COLNO].length = LOM_LONG_SIZE_VAR;

	/* column no */
	lomSysTextPreferences[LOM_SYSTEXTPREFERENCES_COLNO_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysTextPreferences[LOM_SYSTEXTPREFERENCES_COLNO_COLNO].type = LOM_LONG_VAR;
	lomSysTextPreferences[LOM_SYSTEXTPREFERENCES_COLNO_COLNO].length = LOM_LONG_SIZE_VAR;

	/* filter no */
	lomSysTextPreferences[LOM_SYSTEXTPREFERENCES_FILTERNO_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysTextPreferences[LOM_SYSTEXTPREFERENCES_FILTERNO_COLNO].type = LOM_LONG_VAR;
	lomSysTextPreferences[LOM_SYSTEXTPREFERENCES_FILTERNO_COLNO].length = LOM_LONG_SIZE_VAR;

	/* keyword extractor no */
	lomSysTextPreferences[LOM_SYSTEXTPREFERENCES_KEYWORDEXTRACTORNO_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysTextPreferences[LOM_SYSTEXTPREFERENCES_KEYWORDEXTRACTORNO_COLNO].type = LOM_LONG_VAR;
	lomSysTextPreferences[LOM_SYSTEXTPREFERENCES_KEYWORDEXTRACTORNO_COLNO].length = LOM_LONG_SIZE_VAR;

	/* stemizer no */
	lomSysTextPreferences[LOM_SYSTEXTPREFERENCES_STEMIZERNO_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysTextPreferences[LOM_SYSTEXTPREFERENCES_STEMIZERNO_COLNO].type = LOM_LONG_VAR;
	lomSysTextPreferences[LOM_SYSTEXTPREFERENCES_STEMIZERNO_COLNO].length = LOM_LONG_SIZE_VAR;

	idesc.indexType = SM_INDEXTYPE_BTREE;
	idesc.kinfo.btree.flag = KEYFLAG_UNIQUE | KEYFLAG_CLUSTERING;
	idesc.kinfo.btree.nColumns = 2;
	idesc.kinfo.btree.columns[0].colNo = LOM_SYSTEXTPREFERENCES_CLASSID_COLNO;
	idesc.kinfo.btree.columns[0].flag = KEYINFO_COL_ASC;
	idesc.kinfo.btree.columns[1].colNo = LOM_SYSTEXTPREFERENCES_COLNO_COLNO;
	idesc.kinfo.btree.columns[1].flag = KEYINFO_COL_ASC;

	/* create sysTextFilterInfo */
	e = LRDS_CreateRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSTEXTPREFERENCES_CLASSNAME, &idesc,
		LOM_SYSTEXTPREFERENCES_NUM_COLS, &lomSysTextPreferences[0], SM_FALSE);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}

Four lom_Text_DestroyPreferenceCatalog(
	LOM_Handle *handle, 
	Four volId
)
{
	Four e;

	e = LRDS_DestroyRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSTEXTPREFERENCES_CLASSNAME);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}

Four LOM_Text_AddFilter(
	LOM_Handle *handle, 
	Four volId,
	char *filterName,
	Four version,
	char *filterFilePath,
	char *filterFunctionName,
	Four *filterNo
)
{
	LockParameter lockup;
	Four orn;
	ColListStruct clist[LOM_SYSTEXTFILTERINFO_NUM_COLS];
	Four e;

	if(filterName == NULL || version < 0 || filterFilePath == NULL ||
	filterFunctionName == NULL || filterNo == NULL)
		LOM_ERROR(handle, eBADPARAMETER_LOM);

	/* open text-filter-info. table */
	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSTEXTFILTERINFO_CLASSNAME);
	if(orn < eNOERROR) LOM_ERROR(handle, orn);

	/* make column struct list */
	/* filter name */
	clist[LOM_SYSTEXTFILTERINFO_FILTERNAME_COLNO].colNo = LOM_SYSTEXTFILTERINFO_FILTERNAME_COLNO;
	clist[LOM_SYSTEXTFILTERINFO_FILTERNAME_COLNO].start = ALL_VALUE;
	clist[LOM_SYSTEXTFILTERINFO_FILTERNAME_COLNO].dataLength = strlen(filterName);
	clist[LOM_SYSTEXTFILTERINFO_FILTERNAME_COLNO].length = strlen(filterName);
	clist[LOM_SYSTEXTFILTERINFO_FILTERNAME_COLNO].data.ptr = filterName;
	clist[LOM_SYSTEXTFILTERINFO_FILTERNAME_COLNO].nullFlag = SM_FALSE;

	/* version */
	clist[LOM_SYSTEXTFILTERINFO_VERSION_COLNO].colNo = LOM_SYSTEXTFILTERINFO_VERSION_COLNO;
	clist[LOM_SYSTEXTFILTERINFO_VERSION_COLNO].start = ALL_VALUE;
	clist[LOM_SYSTEXTFILTERINFO_VERSION_COLNO].dataLength = LOM_LONG_SIZE_VAR;
	clist[LOM_SYSTEXTFILTERINFO_VERSION_COLNO].length = LOM_LONG_SIZE_VAR;
	ASSIGN_VALUE_TO_COL_LIST(clist[LOM_SYSTEXTFILTERINFO_VERSION_COLNO], version, sizeof(Four));
	clist[LOM_SYSTEXTFILTERINFO_VERSION_COLNO].nullFlag = SM_FALSE;

	/* filter file path */
	clist[LOM_SYSTEXTFILTERINFO_FILTERFILEPATH_COLNO].colNo = LOM_SYSTEXTFILTERINFO_FILTERFILEPATH_COLNO;
	clist[LOM_SYSTEXTFILTERINFO_FILTERFILEPATH_COLNO].start = ALL_VALUE;
	clist[LOM_SYSTEXTFILTERINFO_FILTERFILEPATH_COLNO].dataLength = strlen(filterFilePath);
	clist[LOM_SYSTEXTFILTERINFO_FILTERFILEPATH_COLNO].length = strlen(filterFilePath);
	clist[LOM_SYSTEXTFILTERINFO_FILTERFILEPATH_COLNO].data.ptr = filterFilePath;
	clist[LOM_SYSTEXTFILTERINFO_FILTERFILEPATH_COLNO].nullFlag = SM_FALSE;

	/* filter function name */
	clist[LOM_SYSTEXTFILTERINFO_FILTERFUNCTIONNAME_COLNO].colNo = LOM_SYSTEXTFILTERINFO_FILTERFUNCTIONNAME_COLNO;
	clist[LOM_SYSTEXTFILTERINFO_FILTERFUNCTIONNAME_COLNO].start = ALL_VALUE;
	clist[LOM_SYSTEXTFILTERINFO_FILTERFUNCTIONNAME_COLNO].dataLength = strlen(filterFunctionName);
	clist[LOM_SYSTEXTFILTERINFO_FILTERFUNCTIONNAME_COLNO].length = strlen(filterFunctionName);
	clist[LOM_SYSTEXTFILTERINFO_FILTERFUNCTIONNAME_COLNO].data.ptr = filterFunctionName;
	clist[LOM_SYSTEXTFILTERINFO_FILTERFUNCTIONNAME_COLNO].nullFlag = SM_FALSE;

	/* get filter number */
	*filterNo = lom_Text_GetAndIncrementLastFilterNo(handle, volId);	
	if(*filterNo < 0) LOM_ERROR(handle, *filterNo);

	clist[LOM_SYSTEXTFILTERINFO_FILTERNO_COLNO].colNo = LOM_SYSTEXTFILTERINFO_FILTERNO_COLNO;
	clist[LOM_SYSTEXTFILTERINFO_FILTERNO_COLNO].start = ALL_VALUE;
	clist[LOM_SYSTEXTFILTERINFO_FILTERNO_COLNO].dataLength = LOM_LONG_SIZE_VAR;
	clist[LOM_SYSTEXTFILTERINFO_FILTERNO_COLNO].length = LOM_LONG_SIZE_VAR;
	ASSIGN_VALUE_TO_COL_LIST(clist[LOM_SYSTEXTFILTERINFO_FILTERNO_COLNO], *filterNo, sizeof(Four));
	clist[LOM_SYSTEXTFILTERINFO_FILTERNO_COLNO].nullFlag = SM_FALSE;

	/* create entry for the give filter information */
	e = LRDS_CreateTuple(LOM_GET_LRDS_HANDLE(handle), orn, SM_FALSE, LOM_SYSTEXTFILTERINFO_NUM_COLS, &clist[0], NULL);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}

Four LOM_Text_DropFilter(
	LOM_Handle *handle, 
	Four volId,
	char *filterName,
	Four version
)
{
	Four e;
	BoundCond bound;
	Two keyLen;
	Two filterNameLength;
	Four orn;
	Four osn;
	lrds_RelTableEntry *relTableEntry;
	TupleID tid;
	ColListStruct clist[1];
	LockParameter lockup;

	filterNameLength = strlen(filterName);

	/* make bound condition */
	bound.op = SM_EQ;
	keyLen = 0;
	bcopy(&filterNameLength, &(bound.key.val[0]), sizeof(Two));
	keyLen += sizeof(Two);
	bcopy(filterName, &(bound.key.val[keyLen]), filterNameLength);
	keyLen += filterNameLength;
	bcopy(&version, &(bound.key.val[keyLen]), sizeof(Four));
	keyLen += sizeof(Four);
	bound.key.len = keyLen;

	/* open text-filter-info. table */
	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSTEXTFILTERINFO_CLASSNAME);
	if(orn < eNOERROR) LOM_ERROR(handle, orn);

	/* Get the relation table entry. */
	relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);

	lockup.mode = L_IX;
	lockup.duration = L_COMMIT;

	/* open index scan */
	osn = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), orn, &((LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[1].iid), &bound, &bound,
					0, NULL, &lockup);
	if(osn < eNOERROR) LOM_ERROR(handle, osn);

	e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), osn, &tid, NULL);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	if(e == EOS) LOM_ERROR(handle, eNOSUCHFILTEREXIST_LOM);

	/* destroy entry */
	e = LRDS_DestroyTuple(LOM_GET_LRDS_HANDLE(handle), osn, SM_TRUE, &tid);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), osn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}

Four LOM_Text_GetFilterNo(
	LOM_Handle *handle, 
	Four volId,
	char *filterName,
	Four version,
	Four *filterNo
)
{
	Four e;
	BoundCond bound;
	Two keyLen;
	Two filterNameLength;
	Four orn;
	Four osn;
	lrds_RelTableEntry *relTableEntry;
	TupleID tid;
	ColListStruct clist[1];
	LockParameter lockup;

	filterNameLength = strlen(filterName);

	/* make bound condition */
	bound.op = SM_EQ;
	keyLen = 0;
	bcopy(&filterNameLength, &(bound.key.val[0]), sizeof(Two));
	keyLen += sizeof(Two);
	bcopy(filterName, &(bound.key.val[keyLen]), filterNameLength);
	keyLen += filterNameLength;
	bcopy(&version, &(bound.key.val[keyLen]), sizeof(Four));
	keyLen += sizeof(Four);
	bound.key.len = keyLen;

	/* open text-filter-info. table */
	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSTEXTFILTERINFO_CLASSNAME);
	if(orn < eNOERROR) LOM_ERROR(handle, orn);

	/* Get the relation table entry. */
	relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);

	lockup.mode = L_IS;
	lockup.duration = L_COMMIT;

	/* open index scan */
	osn = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), orn, &((LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[1].iid), &bound, &bound,
					0, NULL, &lockup);
	if(osn < eNOERROR) LOM_ERROR(handle, osn);

	e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), osn, &tid, NULL);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	if(e == EOS) LOM_ERROR(handle, eNOSUCHFILTEREXIST_LOM);

	clist[0].colNo = LOM_SYSTEXTFILTERINFO_FILTERNO_COLNO;
	clist[0].start = ALL_VALUE;

	/* fetch entry */
	e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), osn, SM_TRUE, &tid, 1, &clist[0]);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	/* set filter number */
	*filterNo = GET_VALUE_FROM_COL_LIST(clist[0], sizeof(*filterNo));

	/* close scan */
	e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), osn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	/* close relation */
	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;

}

Four LOM_Text_SetFilter(
	LOM_Handle *handle, 
	Four volId,
	Four classID,
	Four colNo,
	Four filterNo
)
{
	Four e;
	Four osn;
	Four orn;
	BoundCond bound;
	Two keyLen;
	Four systemLevelColNo;
	lrds_RelTableEntry *relTableEntry;
	TupleID tid;
	ColListStruct clist[3];
	LockParameter lockup;

	systemLevelColNo = GET_SYSTEMLEVEL_COLNO(colNo);

	/* open relation */
	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSTEXTPREFERENCES_CLASSNAME);
	if(orn < eNOERROR) LOM_ERROR(handle, orn);

	/* open index scan */
	bound.op = SM_EQ;
	keyLen = 0;
	bcopy(&classID, &(bound.key.val[keyLen]), sizeof(Four));
	keyLen += sizeof(Four);
	bcopy(&systemLevelColNo, &(bound.key.val[keyLen]), sizeof(Four));
	keyLen += sizeof(Four);
	bound.key.len = keyLen;

	/* Get the relation table entry. */
	relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);

	lockup.mode = L_IX;
	lockup.duration = L_COMMIT;

	/* open index scan */
	osn = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), orn, &((LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[0].iid), &bound, &bound,
					0, NULL, &lockup);
	if(osn < eNOERROR) LOM_ERROR(handle, osn);

	/* move cursor */
	e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), osn, &tid, NULL);
	if(e < eNOERROR) {
		e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), osn);
		if(e < eNOERROR) LOM_ERROR(handle, e);

		e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
		if(e < eNOERROR) LOM_ERROR(handle, e);

		LOM_ERROR(handle, e);
	}

	clist[0].colNo = LOM_SYSTEXTPREFERENCES_CLASSID_COLNO;
	clist[0].start = ALL_VALUE;
	clist[0].length = clist[0].dataLength = LOM_LONG_SIZE_VAR;
	ASSIGN_VALUE_TO_COL_LIST(clist[0], classID, sizeof(Four));
	clist[0].nullFlag = SM_FALSE;

	clist[1].colNo = LOM_SYSTEXTPREFERENCES_COLNO_COLNO;
	clist[1].start = ALL_VALUE;
	clist[1].length = clist[1].dataLength = LOM_LONG_SIZE_VAR;
	ASSIGN_VALUE_TO_COL_LIST(clist[1], systemLevelColNo, sizeof(Four));
	clist[1].nullFlag = SM_FALSE;

	clist[2].colNo = LOM_SYSTEXTPREFERENCES_FILTERNO_COLNO;
	clist[2].start = ALL_VALUE;
	clist[2].length = clist[2].dataLength = LOM_LONG_SIZE_VAR;
	ASSIGN_VALUE_TO_COL_LIST(clist[2], filterNo, sizeof(Four));
	clist[2].nullFlag = SM_FALSE;

	if(e == EOS) {
		e = LRDS_CreateTuple(LOM_GET_LRDS_HANDLE(handle), osn, SM_TRUE, 3, &clist[0], &tid);
		if(e < eNOERROR) LOM_ERROR(handle, e);
	}
	else {
		e = LRDS_UpdateTuple(LOM_GET_LRDS_HANDLE(handle), osn, SM_TRUE, &tid, 1, &clist[2]);
		if(e < eNOERROR) LOM_ERROR(handle, e);
	}

	/* close scan */
	e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), osn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	/* close relation */
	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}

Four LOM_Text_ResetFilter(
	LOM_Handle *handle, 
	Four volId,
	Four classID,
	Four colNo
)
{
	Four e;
	Four osn;
	Four orn;
	BoundCond bound;
	Two keyLen;
	Four systemLevelColNo;
	lrds_RelTableEntry *relTableEntry;
	TupleID tid;
	ColListStruct clist[1];
	LockParameter lockup;

	systemLevelColNo = GET_SYSTEMLEVEL_COLNO(colNo);

	/* open relation */
	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSTEXTPREFERENCES_CLASSNAME);
	if(orn < eNOERROR) LOM_ERROR(handle, orn);

	/* open index scan */
	bound.op = SM_EQ;
	keyLen = 0;
	bcopy(&classID, &(bound.key.val[keyLen]), sizeof(Four));
	keyLen += sizeof(Four);
	bcopy(&systemLevelColNo, &(bound.key.val[keyLen]), sizeof(Four));
	keyLen += sizeof(Four);
	bound.key.len = keyLen;

	/* Get the relation table entry. */
	relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);

	lockup.mode = L_IX;
	lockup.duration = L_COMMIT;

	/* open index scan */
	osn = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), orn, &((LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[0].iid), &bound, &bound,
					0, NULL, &lockup);
	if(osn < eNOERROR) LOM_ERROR(handle, osn);

	/* move cursor */
	e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), osn, &tid, NULL);
	if(e < eNOERROR) {
		e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), osn);
		if(e < eNOERROR) LOM_ERROR(handle, e);

		e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
		if(e < eNOERROR) LOM_ERROR(handle, e);

		LOM_ERROR(handle, e);
	}

	if(e == EOS) {
		if(e < eNOERROR) LOM_ERROR(handle, eNOSUCHFILTEREXIST_LOM);
	}
	else {
	    e = LRDS_DestroyTuple(LOM_GET_LRDS_HANDLE(handle), osn, SM_TRUE, &tid);
	    if(e < eNOERROR) LOM_ERROR(handle, e);
	}

	/* close scan */
	e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), osn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	/* close relation */
	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}

Four LOM_Text_GetFilterInfo(
	LOM_Handle *handle, 
	Four volId,
	Four filterNo,
	char *filePath,
	char *functionName
)
{
	Four e;
	BoundCond bound;
	Two keyLen;
	Four orn;
	Four osn;
	lrds_RelTableEntry *relTableEntry;
	TupleID tid;
	ColListStruct clist[2];
	LockParameter lockup;

	/* make bound condition */
	bound.op = SM_EQ;
	keyLen = 0;
	bcopy(&filterNo, &(bound.key.val[0]), sizeof(Four));
	keyLen += sizeof(Four);
	bound.key.len = keyLen;

	/* open text-filter-info. table */
	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSTEXTFILTERINFO_CLASSNAME);
	if(orn < eNOERROR) LOM_ERROR(handle, orn);

	/* Get the relation table entry. */
	relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);

	lockup.mode = L_IS;
	lockup.duration = L_COMMIT;

	/* open index scan */
	osn = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), orn, &((LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[0].iid), &bound, &bound,
					0, NULL, &lockup);
	if(osn < eNOERROR) LOM_ERROR(handle, osn);

	e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), osn, &tid, NULL);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	if(e == EOS) LOM_ERROR(handle, eNOSUCHFILTEREXIST_LOM);

	clist[0].colNo = LOM_SYSTEXTFILTERINFO_FILTERFILEPATH_COLNO;
	clist[0].start = ALL_VALUE;
	clist[0].length = clist[0].dataLength = LOM_MAXFILTERFILEPATHNAME;
	clist[0].data.ptr = filePath;

	clist[1].colNo = LOM_SYSTEXTFILTERINFO_FILTERFUNCTIONNAME_COLNO;
	clist[1].start = ALL_VALUE;
	clist[1].length = clist[1].dataLength = LOM_MAXFILTERFUNCTIONNAME;
	clist[1].data.ptr = functionName;

	e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), osn, SM_TRUE, &tid, 2, &clist[0]);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), osn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	filePath[clist[0].retLength] = '\0';
	functionName[clist[1].retLength] = '\0';

	return eNOERROR;
}


Four LOM_Text_AddKeywordExtractor(
	LOM_Handle *handle, 
	Four volId,
	char *keywordExtractorName,
	Four version,
	char *keywordExtractorFilePath,
	char *keywordExtractorFunctionName,
	char *getNextPostingFunctionName,
	char *finalizeKeywordExtractorFunctionName,
	Four *keywordExtractorNo
)
{
	LockParameter lockup;
	Four e;
	Four orn;
	ColListStruct clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_NUM_COLS];

	if(keywordExtractorName == NULL || version < 0 || keywordExtractorFilePath == NULL ||
	keywordExtractorFunctionName == NULL || keywordExtractorNo == NULL)
		LOM_ERROR(handle, eBADPARAMETER_LOM);

	/* open text-keywordExtractor-info. table */
	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSTEXTKEYWORDEXTRACTORINFO_CLASSNAME);
	if(orn < eNOERROR) LOM_ERROR(handle, orn);

	/* make column struct list */
	/* keywordExtractor name */
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORNAME_COLNO].colNo = LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORNAME_COLNO;
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORNAME_COLNO].start = ALL_VALUE;
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORNAME_COLNO].dataLength = strlen(keywordExtractorName);
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORNAME_COLNO].length = strlen(keywordExtractorName);
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORNAME_COLNO].data.ptr = keywordExtractorName;
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORNAME_COLNO].nullFlag = SM_FALSE;

	/* version */
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_VERSION_COLNO].colNo = LOM_SYSTEXTKEYWORDEXTRACTORINFO_VERSION_COLNO;
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_VERSION_COLNO].start = ALL_VALUE;
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_VERSION_COLNO].dataLength = LOM_LONG_SIZE_VAR;
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_VERSION_COLNO].length = LOM_LONG_SIZE_VAR;
	ASSIGN_VALUE_TO_COL_LIST(clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_VERSION_COLNO], version, sizeof(Four));
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_VERSION_COLNO].nullFlag = SM_FALSE;

	/* keywordExtractor file path */
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORFILEPATH_COLNO].colNo = LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORFILEPATH_COLNO;
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORFILEPATH_COLNO].start = ALL_VALUE;
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORFILEPATH_COLNO].dataLength = strlen(keywordExtractorFilePath);
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORFILEPATH_COLNO].length = strlen(keywordExtractorFilePath);
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORFILEPATH_COLNO].data.ptr = keywordExtractorFilePath;
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORFILEPATH_COLNO].nullFlag = SM_FALSE;

	/* keywordExtractor function name */
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORFUNCTIONNAME_COLNO].colNo = LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORFUNCTIONNAME_COLNO;
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORFUNCTIONNAME_COLNO].start = ALL_VALUE;
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORFUNCTIONNAME_COLNO].dataLength = strlen(keywordExtractorFunctionName);
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORFUNCTIONNAME_COLNO].length = strlen(keywordExtractorFunctionName);
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORFUNCTIONNAME_COLNO].data.ptr = keywordExtractorFunctionName;
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORFUNCTIONNAME_COLNO].nullFlag = SM_FALSE;

	/* getNextPostingFunctionName function name */
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_GETNEXTPOSTINGINFOFUNCTIONNAME_COLNO].colNo = LOM_SYSTEXTKEYWORDEXTRACTORINFO_GETNEXTPOSTINGINFOFUNCTIONNAME_COLNO;
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_GETNEXTPOSTINGINFOFUNCTIONNAME_COLNO].start = ALL_VALUE;
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_GETNEXTPOSTINGINFOFUNCTIONNAME_COLNO].dataLength = strlen(getNextPostingFunctionName);
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_GETNEXTPOSTINGINFOFUNCTIONNAME_COLNO].length = strlen(getNextPostingFunctionName);
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_GETNEXTPOSTINGINFOFUNCTIONNAME_COLNO].data.ptr = getNextPostingFunctionName;
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_GETNEXTPOSTINGINFOFUNCTIONNAME_COLNO].nullFlag = SM_FALSE;

	/* finalize keywordExtractor function name */
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_FINALIZEKEYWORDEXTRACTORFUNCTIONNAME_COLNO].colNo = LOM_SYSTEXTKEYWORDEXTRACTORINFO_FINALIZEKEYWORDEXTRACTORFUNCTIONNAME_COLNO;
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_FINALIZEKEYWORDEXTRACTORFUNCTIONNAME_COLNO].start = ALL_VALUE;
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_FINALIZEKEYWORDEXTRACTORFUNCTIONNAME_COLNO].dataLength = strlen(finalizeKeywordExtractorFunctionName);
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_FINALIZEKEYWORDEXTRACTORFUNCTIONNAME_COLNO].length = strlen(finalizeKeywordExtractorFunctionName);
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_FINALIZEKEYWORDEXTRACTORFUNCTIONNAME_COLNO].data.ptr = finalizeKeywordExtractorFunctionName;
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_FINALIZEKEYWORDEXTRACTORFUNCTIONNAME_COLNO].nullFlag = SM_FALSE;

	/* get keywordExtractor number */
	*keywordExtractorNo = lom_Text_GetAndIncrementLastKeywordExtractorNo(handle, volId);	
	if(*keywordExtractorNo < 0) LOM_ERROR(handle, *keywordExtractorNo);

	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORNO_COLNO].colNo = LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORNO_COLNO;
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORNO_COLNO].start = ALL_VALUE;
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORNO_COLNO].dataLength = LOM_LONG_SIZE_VAR;
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORNO_COLNO].length = LOM_LONG_SIZE_VAR;
	ASSIGN_VALUE_TO_COL_LIST(clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORNO_COLNO], *keywordExtractorNo, sizeof(Four));
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORNO_COLNO].nullFlag = SM_FALSE;

	/* create entry for the give keywordExtractor information */
	e = LRDS_CreateTuple(LOM_GET_LRDS_HANDLE(handle), orn, SM_FALSE, LOM_SYSTEXTKEYWORDEXTRACTORINFO_NUM_COLS, &clist[0], NULL);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}

Four LOM_Text_AddDefaultKeywordExtractor(
	LOM_Handle *handle, 
	Four volId,
	char *keywordExtractorName,
	Four version,
	char *keywordExtractorFilePath,
	char *keywordExtractorFunctionName,
	char *getNextPostingFunctionName,
	char *finalizeKeywordExtractorFunctionName
)
{
	LockParameter	lockup;
	Four			e;
	Four			orn;
	Four			scanId;
	ColListStruct	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_NUM_COLS];
	Four			keywordExtractorNo;
	TupleID			tid;
	BoundCond		bound;
	Two				keyLen;
	lrds_RelTableEntry *relTableEntry;

	if(keywordExtractorName == NULL || version < 0 || keywordExtractorFilePath == NULL ||
	keywordExtractorFunctionName == NULL)
		LOM_ERROR(handle, eBADPARAMETER_LOM);

	/* open text-keywordExtractor-info. table */
	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSTEXTKEYWORDEXTRACTORINFO_CLASSNAME);
	if(orn < eNOERROR) LOM_ERROR(handle, orn);

	/* drop previous default keyword extractor if any */
	relTableEntry      = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);
	bound.op           = SM_EQ;
	keyLen             = 0;
	keywordExtractorNo = 0;
	memcpy(&(bound.key.val[0]), &keywordExtractorNo, sizeof(Four));
	keyLen             += sizeof(Four);
	bound.key.len      = keyLen;

	lockup.mode     = L_IX;
	lockup.duration = L_COMMIT;
	scanId = e = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), orn, &(LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[0].iid, &bound, &bound, 0, NULL, &lockup);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	while((e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), scanId, &tid, NULL)) != EOS)
	{
		if(e < eNOERROR) LOM_ERROR(handle, e);

		e = LRDS_DestroyTuple(LOM_GET_LRDS_HANDLE(handle), scanId, SM_TRUE, &tid);
		if(e < eNOERROR) LOM_ERROR(handle, e);
	}

	e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), scanId);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	/* make column struct list */
	/* keywordExtractor name */
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORNAME_COLNO].colNo = LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORNAME_COLNO;
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORNAME_COLNO].start = ALL_VALUE;
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORNAME_COLNO].dataLength = strlen(keywordExtractorName);
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORNAME_COLNO].length = strlen(keywordExtractorName);
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORNAME_COLNO].data.ptr = keywordExtractorName;
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORNAME_COLNO].nullFlag = SM_FALSE;

	/* version */
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_VERSION_COLNO].colNo = LOM_SYSTEXTKEYWORDEXTRACTORINFO_VERSION_COLNO;
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_VERSION_COLNO].start = ALL_VALUE;
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_VERSION_COLNO].dataLength = LOM_LONG_SIZE_VAR;
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_VERSION_COLNO].length = LOM_LONG_SIZE_VAR;
	ASSIGN_VALUE_TO_COL_LIST(clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_VERSION_COLNO], version, sizeof(Four));
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_VERSION_COLNO].nullFlag = SM_FALSE;

	/* keywordExtractor file path */
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORFILEPATH_COLNO].colNo = LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORFILEPATH_COLNO;
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORFILEPATH_COLNO].start = ALL_VALUE;
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORFILEPATH_COLNO].dataLength = strlen(keywordExtractorFilePath);
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORFILEPATH_COLNO].length = strlen(keywordExtractorFilePath);
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORFILEPATH_COLNO].data.ptr = keywordExtractorFilePath;
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORFILEPATH_COLNO].nullFlag = SM_FALSE;

	/* keywordExtractor function name */
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORFUNCTIONNAME_COLNO].colNo = LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORFUNCTIONNAME_COLNO;
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORFUNCTIONNAME_COLNO].start = ALL_VALUE;
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORFUNCTIONNAME_COLNO].dataLength = strlen(keywordExtractorFunctionName);
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORFUNCTIONNAME_COLNO].length = strlen(keywordExtractorFunctionName);
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORFUNCTIONNAME_COLNO].data.ptr = keywordExtractorFunctionName;
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORFUNCTIONNAME_COLNO].nullFlag = SM_FALSE;

	/* getNextPostingFunctionName function name */
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_GETNEXTPOSTINGINFOFUNCTIONNAME_COLNO].colNo = LOM_SYSTEXTKEYWORDEXTRACTORINFO_GETNEXTPOSTINGINFOFUNCTIONNAME_COLNO;
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_GETNEXTPOSTINGINFOFUNCTIONNAME_COLNO].start = ALL_VALUE;
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_GETNEXTPOSTINGINFOFUNCTIONNAME_COLNO].dataLength = strlen(getNextPostingFunctionName);
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_GETNEXTPOSTINGINFOFUNCTIONNAME_COLNO].length = strlen(getNextPostingFunctionName);
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_GETNEXTPOSTINGINFOFUNCTIONNAME_COLNO].data.ptr = getNextPostingFunctionName;
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_GETNEXTPOSTINGINFOFUNCTIONNAME_COLNO].nullFlag = SM_FALSE;

	/* finalize keywordExtractor function name */
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_FINALIZEKEYWORDEXTRACTORFUNCTIONNAME_COLNO].colNo = LOM_SYSTEXTKEYWORDEXTRACTORINFO_FINALIZEKEYWORDEXTRACTORFUNCTIONNAME_COLNO;
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_FINALIZEKEYWORDEXTRACTORFUNCTIONNAME_COLNO].start = ALL_VALUE;
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_FINALIZEKEYWORDEXTRACTORFUNCTIONNAME_COLNO].dataLength = strlen(finalizeKeywordExtractorFunctionName);
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_FINALIZEKEYWORDEXTRACTORFUNCTIONNAME_COLNO].length = strlen(finalizeKeywordExtractorFunctionName);
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_FINALIZEKEYWORDEXTRACTORFUNCTIONNAME_COLNO].data.ptr = finalizeKeywordExtractorFunctionName;
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_FINALIZEKEYWORDEXTRACTORFUNCTIONNAME_COLNO].nullFlag = SM_FALSE;

	/* get keywordExtractor number */
	keywordExtractorNo = 0;

	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORNO_COLNO].colNo = LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORNO_COLNO;
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORNO_COLNO].start = ALL_VALUE;
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORNO_COLNO].dataLength = LOM_LONG_SIZE_VAR;
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORNO_COLNO].length = LOM_LONG_SIZE_VAR;
	ASSIGN_VALUE_TO_COL_LIST(clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORNO_COLNO], keywordExtractorNo, sizeof(Four));
	clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORNO_COLNO].nullFlag = SM_FALSE;

	/* create entry for the give keywordExtractor information */
	e = LRDS_CreateTuple(LOM_GET_LRDS_HANDLE(handle), orn, SM_FALSE, LOM_SYSTEXTKEYWORDEXTRACTORINFO_NUM_COLS, &clist[0], NULL);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}

Four LOM_Text_DropKeywordExtractor(
	LOM_Handle *handle, 
	Four volId,
	char *keywordExtractorName,
	Four version
)
{
	Four e;
	BoundCond bound;
	Two keyLen;
	Two keywordExtractorNameLength;
	Four orn;
	Four osn;
	lrds_RelTableEntry *relTableEntry;
	TupleID tid;
	ColListStruct clist[1];
	LockParameter lockup;

	keywordExtractorNameLength = strlen(keywordExtractorName);

	/* make bound condition */
	bound.op = SM_EQ;
	keyLen = 0;
	bcopy(&keywordExtractorNameLength, &(bound.key.val[0]), sizeof(Two));
	keyLen += sizeof(Two);
	bcopy(keywordExtractorName, &(bound.key.val[keyLen]), keywordExtractorNameLength);
	keyLen += keywordExtractorNameLength;
	bcopy(&version, &(bound.key.val[keyLen]), sizeof(Four));
	keyLen += sizeof(Four);
	bound.key.len = keyLen;

	/* open text-keywordExtractor-info. table */
	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSTEXTKEYWORDEXTRACTORINFO_CLASSNAME);
	if(orn < eNOERROR) LOM_ERROR(handle, orn);

	/* Get the relation table entry. */
	relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);

	lockup.mode = L_IX;
	lockup.duration = L_COMMIT;

	/* open index scan */
	osn = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), orn, &((LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[1].iid), &bound, &bound,
					0, NULL, &lockup);
	if(osn < eNOERROR) LOM_ERROR(handle, osn);

	e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), osn, &tid, NULL);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	if(e == EOS) LOM_ERROR(handle, eNOSUCHKEYWORDEXTRACTOREXIST_LOM);

	/* destroy entry */
	e = LRDS_DestroyTuple(LOM_GET_LRDS_HANDLE(handle), osn, SM_TRUE, &tid);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), osn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}


Four LOM_Text_GetKeywordExtractorNo(
	LOM_Handle *handle, 
	Four volId,
	char *keywordExtractorName,
	Four version,
	Four *keywordExtractorNo
)
{
	Four e;
	BoundCond bound;
	Two keyLen;
	Two keywordExtractorNameLength;
	Four orn;
	Four osn;
	lrds_RelTableEntry *relTableEntry;
	TupleID tid;
	ColListStruct clist[1];
	LockParameter lockup;

	keywordExtractorNameLength = strlen(keywordExtractorName);

	/* make bound condition */
	bound.op = SM_EQ;
	keyLen = 0;
	bcopy(&keywordExtractorNameLength, &(bound.key.val[0]), sizeof(Two));
	keyLen += sizeof(Two);
	bcopy(keywordExtractorName, &(bound.key.val[keyLen]), keywordExtractorNameLength);
	keyLen += keywordExtractorNameLength;
	bcopy(&version, &(bound.key.val[keyLen]), sizeof(Four));
	keyLen += sizeof(Four);
	bound.key.len = keyLen;

	/* open text-keywordExtractor-info. table */
	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSTEXTKEYWORDEXTRACTORINFO_CLASSNAME);
	if(orn < eNOERROR) LOM_ERROR(handle, orn);

	/* Get the relation table entry. */
	relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);

	lockup.mode = L_IS;
	lockup.duration = L_COMMIT;

	/* open index scan */
	osn = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), orn, &((LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[1].iid), &bound, &bound,
					0, NULL, &lockup);
	if(osn < eNOERROR) LOM_ERROR(handle, osn);

	e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), osn, &tid, NULL);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	if(e == EOS) LOM_ERROR(handle, eNOSUCHKEYWORDEXTRACTOREXIST_LOM);

	clist[0].colNo = LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORNO_COLNO;
	clist[0].start = ALL_VALUE;

	/* fetch entry */
	e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), osn, SM_TRUE, &tid, 1, &clist[0]);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	/* set keywordExtractor number */
	*keywordExtractorNo = GET_VALUE_FROM_COL_LIST(clist[0], sizeof(*keywordExtractorNo));

	/* close scan */
	e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), osn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	/* close relation */
	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;

}

Four LOM_Text_SetKeywordExtractor(
	LOM_Handle *handle, 
	Four volId,
	Four classID,
	Four colNo,
	Four keywordExtractorNo
)
{
	Four e;
	Four osn;
	Four orn;
	BoundCond bound;
	Two keyLen;
	Four systemLevelColNo;
	lrds_RelTableEntry *relTableEntry;
	TupleID tid;
	ColListStruct clist[3];
	LockParameter lockup;

	systemLevelColNo = GET_SYSTEMLEVEL_COLNO(colNo);

	/* open relation */
	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSTEXTPREFERENCES_CLASSNAME);
	if(orn < eNOERROR) LOM_ERROR(handle, orn);

	/* open index scan */
	bound.op = SM_EQ;
	keyLen = 0;
	bcopy(&classID, &(bound.key.val[keyLen]), sizeof(Four));
	keyLen += sizeof(Four);
	bcopy(&systemLevelColNo, &(bound.key.val[keyLen]), sizeof(Four));
	keyLen += sizeof(Four);
	bound.key.len = keyLen;

	/* Get the relation table entry. */
	relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);

	lockup.mode = L_IX;
	lockup.duration = L_COMMIT;

	/* open index scan */
	osn = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), orn, &((LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[0].iid), &bound, &bound,
					0, NULL, &lockup);
	if(osn < eNOERROR) LOM_ERROR(handle, osn);

	/* move cursor */
	e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), osn, &tid, NULL);
	if(e < eNOERROR) {
		e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), osn);
		if(e < eNOERROR) LOM_ERROR(handle, e);

		e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
		if(e < eNOERROR) LOM_ERROR(handle, e);

		LOM_ERROR(handle, e);
	}

	clist[0].colNo = LOM_SYSTEXTPREFERENCES_CLASSID_COLNO;
	clist[0].start = ALL_VALUE;
	clist[0].length = clist[0].dataLength = LOM_LONG_SIZE_VAR;
	ASSIGN_VALUE_TO_COL_LIST(clist[0], classID, sizeof(Four));
	clist[0].nullFlag = SM_FALSE;

	clist[1].colNo = LOM_SYSTEXTPREFERENCES_COLNO_COLNO;
	clist[1].start = ALL_VALUE;
	clist[1].length = clist[1].dataLength = LOM_LONG_SIZE_VAR;
	ASSIGN_VALUE_TO_COL_LIST(clist[1], systemLevelColNo, sizeof(Four));
	clist[1].nullFlag = SM_FALSE;

	clist[2].colNo = LOM_SYSTEXTPREFERENCES_KEYWORDEXTRACTORNO_COLNO;
	clist[2].start = ALL_VALUE;
	clist[2].length = clist[2].dataLength = LOM_LONG_SIZE_VAR;
	ASSIGN_VALUE_TO_COL_LIST(clist[2], keywordExtractorNo, sizeof(Four));
	clist[2].nullFlag = SM_FALSE;

	if(e == EOS) {
		e = LRDS_CreateTuple(LOM_GET_LRDS_HANDLE(handle), osn, SM_TRUE, 3, &clist[0], &tid);
		if(e < eNOERROR) LOM_ERROR(handle, e);
	}	
	else {
		e = LRDS_UpdateTuple(LOM_GET_LRDS_HANDLE(handle), osn, SM_TRUE, &tid, 1, &clist[2]);
		if(e < eNOERROR) LOM_ERROR(handle, e);
	}

	/* close scan */
	e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), osn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	/* close relation */
	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}

Four LOM_Text_ResetKeywordExtractor(
	LOM_Handle *handle, 
	Four volId,
	Four classID,
	Four colNo
)
{
	Four e;
	Four osn;
	Four orn;
	BoundCond bound;
	Two keyLen;
	Four systemLevelColNo;
	lrds_RelTableEntry *relTableEntry;
	TupleID tid;
	ColListStruct clist[1];
	LockParameter lockup;

	systemLevelColNo = GET_SYSTEMLEVEL_COLNO(colNo);

	/* open relation */
	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSTEXTPREFERENCES_CLASSNAME);
	if(orn < eNOERROR) LOM_ERROR(handle, orn);

	/* open index scan */
	bound.op = SM_EQ;
	keyLen = 0;
	bcopy(&classID, &(bound.key.val[keyLen]), sizeof(Four));
	keyLen += sizeof(Four);
	bcopy(&systemLevelColNo, &(bound.key.val[keyLen]), sizeof(Four));
	keyLen += sizeof(Four);
	bound.key.len = keyLen;

	/* Get the relation table entry. */
	relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);

	lockup.mode = L_IX;
	lockup.duration = L_COMMIT;

	/* open index scan */
	osn = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), orn, &((LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[0].iid), &bound, &bound,
					0, NULL, &lockup);
	if(osn < eNOERROR) LOM_ERROR(handle, osn);

	/* move cursor */
	e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), osn, &tid, NULL);
	if(e < eNOERROR) {
		e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), osn);
		if(e < eNOERROR) LOM_ERROR(handle, e);

		e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
		if(e < eNOERROR) LOM_ERROR(handle, e);

		LOM_ERROR(handle, e);
	}

	if(e == EOS) {
		if(e < eNOERROR) LOM_ERROR(handle, eNOSUCHKEYWORDEXTRACTOREXIST_LOM);
	}	
	else {
		e = LRDS_DestroyTuple(LOM_GET_LRDS_HANDLE(handle), osn, SM_TRUE, &tid);
		if(e < eNOERROR) LOM_ERROR(handle, e);
	}

	/* close scan */
	e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), osn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	/* close relation */
	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}

Four LOM_Text_GetKeywordExtractorInfo(
	LOM_Handle *handle, 
	Four volId,
	Four keywordExtractorNo,
	char *filePath,
	char *keywordExtractorFunctionName,
	char *getNextPostingInfoFunctionName,
	char *finalizeKeywordExtractorFunctionName
)
{
	Four e;
	BoundCond bound;
	Two keyLen;
	Four orn;
	Four osn;
	lrds_RelTableEntry *relTableEntry;
	TupleID tid;
	ColListStruct clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_NUM_COLS];
	LockParameter lockup;
	Four count;

	/* make bound condition */
	bound.op = SM_EQ;
	keyLen = 0;
	bcopy(&keywordExtractorNo, &(bound.key.val[0]), sizeof(Four));
	keyLen += sizeof(Four);
	bound.key.len = keyLen;

	/* open text-keywordExtractor-info. table */
	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSTEXTKEYWORDEXTRACTORINFO_CLASSNAME);
	if(orn < eNOERROR) LOM_ERROR(handle, orn);

	/* Get the relation table entry. */
	relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);

	lockup.mode = L_IS;
	lockup.duration = L_COMMIT;

	/* open index scan */
	osn = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), orn, &((LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[0].iid), &bound, &bound,
					0, NULL, &lockup);
	if(osn < eNOERROR) LOM_ERROR(handle, osn);

	e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), osn, &tid, NULL);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	if(e == EOS) LOM_ERROR(handle, eNOSUCHKEYWORDEXTRACTOREXIST_LOM);

	count = 0;
	clist[count].colNo = LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORFILEPATH_COLNO;
	clist[count].start = ALL_VALUE;
	clist[count].length = clist[count].dataLength = LOM_MAXKEYWORDEXTRACTORFILEPATHNAME;
	clist[count].data.ptr = filePath;
	count++;

	clist[count].colNo = LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORFUNCTIONNAME_COLNO;
	clist[count].start = ALL_VALUE;
	clist[count].length = clist[count].dataLength = LOM_MAXKEYWORDEXTRACTORFUNCTIONNAME;
	clist[count].data.ptr = keywordExtractorFunctionName;
	count++;

	clist[count].colNo = LOM_SYSTEXTKEYWORDEXTRACTORINFO_GETNEXTPOSTINGINFOFUNCTIONNAME_COLNO;
	clist[count].start = ALL_VALUE;
	clist[count].length = clist[count].dataLength = LOM_MAXKEYWORDEXTRACTORFUNCTIONNAME;
	clist[count].data.ptr = getNextPostingInfoFunctionName;
	count++;

	clist[count].colNo = LOM_SYSTEXTKEYWORDEXTRACTORINFO_FINALIZEKEYWORDEXTRACTORFUNCTIONNAME_COLNO;
	clist[count].start = ALL_VALUE;
	clist[count].length = clist[count].dataLength = LOM_MAXKEYWORDEXTRACTORFUNCTIONNAME;
	clist[count].data.ptr = finalizeKeywordExtractorFunctionName;
	count++;

	e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), osn, SM_TRUE, &tid, count, &clist[0]);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), osn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	filePath[clist[0].retLength] = '\0';
	keywordExtractorFunctionName[clist[1].retLength] = '\0';
	getNextPostingInfoFunctionName[clist[2].retLength] = '\0';
	finalizeKeywordExtractorFunctionName[clist[3].retLength] = '\0';

	return eNOERROR;
}

Four LOM_Text_GetCurrentFilterNo (
	LOM_Handle *handle, 
	Four volId,
	Four classId,
	Four colNo,
	Four *filterNo
)
{
	Four e;
	Four osn;
	Four orn;
	BoundCond bound;
	Two keyLen;
	Four systemLevelColNo;
	lrds_RelTableEntry *relTableEntry;
	TupleID tid;
	ColListStruct clist[1];
	LockParameter lockup;

	systemLevelColNo = GET_SYSTEMLEVEL_COLNO(colNo);

	/* open relation */
	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSTEXTPREFERENCES_CLASSNAME);
	if(orn < eNOERROR) LOM_ERROR(handle, orn);

	/* open index scan */
	bound.op = SM_EQ;
	keyLen = 0;
	bcopy(&classId, &(bound.key.val[keyLen]), sizeof(Four));
	keyLen += sizeof(Four);
	bcopy(&systemLevelColNo, &(bound.key.val[keyLen]), sizeof(Four));
	keyLen += sizeof(Four);
	bound.key.len = keyLen;

	/* Get the relation table entry. */
	relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);

	lockup.mode = L_IS;
	lockup.duration = L_COMMIT;

	/* open index scan */
	osn = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), orn, &((LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[0].iid), &bound, &bound,
					0, NULL, &lockup);
	if(osn < eNOERROR) LOM_ERROR(handle, osn);

	/* move cursor */
	e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), osn, &tid, NULL);
	if(e < eNOERROR) {
		e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), osn);
		if(e < eNOERROR) LOM_ERROR(handle, e);

		e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
		if(e < eNOERROR) LOM_ERROR(handle, e);

		LOM_ERROR(handle, e);
	}

	if(e == EOS) LOM_ERROR(handle, eBADPARAMETER_LOM);
	
	clist[0].colNo = LOM_SYSTEXTPREFERENCES_FILTERNO_COLNO;
	clist[0].start = ALL_VALUE;
	clist[0].length = clist[0].dataLength = LOM_LONG_SIZE_VAR;

	e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), osn, SM_TRUE, &tid, 1, &clist[0]);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	/* close scan */
	e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), osn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	/* close relation */
	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	*filterNo = GET_VALUE_FROM_COL_LIST(clist[0], sizeof(*filterNo));

	return eNOERROR;
}

Four LOM_Text_GetCurrentKeywordExtractorNo (
	LOM_Handle *handle, 
	Four volId,
	Four classId,
	Four colNo,
	Four *keywordExtractorNo
)
{
	Four e;
	Four osn;
	Four orn;
	BoundCond bound;
	Two keyLen;
	Four systemLevelColNo;
	lrds_RelTableEntry *relTableEntry;
	TupleID tid;
	ColListStruct clist[1];
	LockParameter lockup;

	systemLevelColNo = GET_SYSTEMLEVEL_COLNO(colNo);

	/* open relation */
	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSTEXTPREFERENCES_CLASSNAME);
	if(orn < eNOERROR) LOM_ERROR(handle, orn);

	/* open index scan */
	bound.op = SM_EQ;
	keyLen = 0;
	bcopy(&classId, &(bound.key.val[keyLen]), sizeof(Four));
	keyLen += sizeof(Four);
	bcopy(&systemLevelColNo, &(bound.key.val[keyLen]), sizeof(Four));
	keyLen += sizeof(Four);
	bound.key.len = keyLen;

	/* Get the relation table entry. */
	relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);

	lockup.mode = L_IS;
	lockup.duration = L_COMMIT;

	/* open index scan */
	osn = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), orn, &((LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[0].iid), &bound, &bound,
					0, NULL, &lockup);
	if(osn < eNOERROR) LOM_ERROR(handle, osn);

	/* move cursor */
	e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), osn, &tid, NULL);
	if(e < eNOERROR) {
		e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), osn);
		if(e < eNOERROR) LOM_ERROR(handle, e);

		e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
		if(e < eNOERROR) LOM_ERROR(handle, e);

		LOM_ERROR(handle, e);
	}

	if(e == EOS) LOM_ERROR(handle, eBADPARAMETER_LOM);
	
	clist[0].colNo = LOM_SYSTEXTPREFERENCES_KEYWORDEXTRACTORNO_COLNO;
	clist[0].start = ALL_VALUE;
	clist[0].length = clist[0].dataLength = LOM_LONG_SIZE_VAR;

	e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), osn, SM_TRUE, &tid, 1, &clist[0]);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	/* close scan */
	e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), osn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	/* close relation */
	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	*keywordExtractorNo = GET_VALUE_FROM_COL_LIST(clist[0], sizeof(*keywordExtractorNo));

	return eNOERROR;
}

Four LOM_Text_GetCurrentStemizerNo (
	LOM_Handle *handle, 
	Four volId,
	Four classId,
	Four colNo,
	Four *stemizerNo
)
{
	Four e;
	Four osn;
	Four orn;
	BoundCond bound;
	Two keyLen;
	Four systemLevelColNo;
	lrds_RelTableEntry *relTableEntry;
	TupleID tid;
	ColListStruct clist[1];
	LockParameter lockup;

	systemLevelColNo = GET_SYSTEMLEVEL_COLNO(colNo);

	/* open relation */
	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSTEXTPREFERENCES_CLASSNAME);
	if(orn < eNOERROR) LOM_ERROR(handle, orn);

	/* open index scan */
	bound.op = SM_EQ;
	keyLen = 0;
	bcopy(&classId, &(bound.key.val[keyLen]), sizeof(Four));
	keyLen += sizeof(Four);
	bcopy(&systemLevelColNo, &(bound.key.val[keyLen]), sizeof(Four));
	keyLen += sizeof(Four);
	bound.key.len = keyLen;

	/* Get the relation table entry. */
	relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);

	lockup.mode = L_IS;
	lockup.duration = L_COMMIT;

	/* open index scan */
	osn = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), orn, &((LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[0].iid), &bound, &bound,
					0, NULL, &lockup);
	if(osn < eNOERROR) LOM_ERROR(handle, osn);

	/* move cursor */
	e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), osn, &tid, NULL);
	if(e < eNOERROR) {
		e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), osn);
		if(e < eNOERROR) LOM_ERROR(handle, e);

		e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
		if(e < eNOERROR) LOM_ERROR(handle, e);

		LOM_ERROR(handle, e);
	}

	if(e == EOS) LOM_ERROR(handle, eBADPARAMETER_LOM);
	
	clist[0].colNo = LOM_SYSTEXTPREFERENCES_STEMIZERNO_COLNO;
	clist[0].start = ALL_VALUE;
	clist[0].length = clist[0].dataLength = LOM_LONG_SIZE_VAR;

	e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), osn, SM_TRUE, &tid, 1, &clist[0]);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	/* close scan */
	e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), osn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	/* close relation */
	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	*stemizerNo = GET_VALUE_FROM_COL_LIST(clist[0], sizeof(*stemizerNo));

	return eNOERROR;
}

Four LOM_Text_OpenHandleForFilter(
	LOM_Handle *handle, 
	Four volId,
	Four classId,
	Four colNo,
	void **fhandle,
	lom_FptrToFilter *fptrToFilter
)
{
	Four e;
	Four filterNo;
	char functionName[LOM_MAXFILTERNAME];
	char filePath[LOM_MAXFILTERFUNCTIONNAME];
	LockParameter lockup;

	/* get the filter number */
	e = LOM_Text_GetCurrentFilterNo ( handle, volId, classId, colNo, &filterNo);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	/* you need not apply filter before extracting keywords */
	if(filterNo == LOM_DEFAULT_FILTER_NO) 
	{
		*fhandle      = NULL;
		*fptrToFilter = NULL;

		return eNOERROR;
	}

	/* get the information on keyword extractor */
	e = LOM_Text_GetFilterInfo(handle, volId, filterNo, filePath, functionName);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	/* dynamically open the shared object */
#ifdef USE_RPC
	if(LOM_USERPCFORDLLFUNCTIONSFLAG(handle))
		*fhandle = lom_RPCdlopen(handle, filePath, RTLD_LAZY);
	else
		*fhandle = lom_dlopen(handle, filePath, RTLD_LAZY);
#else
	*fhandle = lom_dlopen(handle, filePath, RTLD_LAZY);
#endif
	if(*fhandle == NULL) {
		fprintf(stderr,"%s\n", lom_dlerror(handle));
		LOM_ERROR(handle, eCANNOTLINKFILTERFUNCTION_LOM);
	}

	/* dynamically get the function pointer to the given fuction */
#ifdef USE_RPC
	if(LOM_USERPCFORDLLFUNCTIONSFLAG(handle))
		*fptrToFilter = (lom_FptrToFilter)lom_RPCdlsym(handle, *fhandle, functionName);
	else
		*fptrToFilter = (lom_FptrToFilter)lom_dlsym(handle, *fhandle, functionName);
#else
	*fptrToFilter = (lom_FptrToFilter)lom_dlsym(handle, *fhandle, functionName);
#endif
	if(*fptrToFilter  == NULL) {
		fprintf(stderr,"%s\n", lom_dlerror(handle));
		LOM_ERROR(handle, eCANNOTLINKFILTERFUNCTION_LOM);
	}

	return eNOERROR;
}

Four LOM_Text_OpenHandleForKeywordExtractor(
	LOM_Handle *handle, 
	Four volId,
	Four classId,
	Four colNo,
	void **fhandle,
	lom_FptrToKeywordExtractor *fptrToKeywordExtractor,
	lom_FptrToGetNextPostingInfo *fptrToGettingNextPostingInfo,
	lom_FptrToFinalizeKeywordExtraction *fptrToFinalizeKeywordExtraction
)
{
	Four e;
	Four keywordExtractorNo;
	char functionName[LOM_MAXKEYWORDEXTRACTORNAME];
	char functionNameForGettingNextPostingInfo[LOM_MAXKEYWORDEXTRACTORNAME];
	char functionNameForFinalizingKeywordExtractor[LOM_MAXKEYWORDEXTRACTORNAME];
	char filePath[LOM_MAXFILTERFUNCTIONNAME];

	/* get the keywordExtractor number */
	e = LOM_Text_GetCurrentKeywordExtractorNo (handle, volId, classId, colNo,
			&keywordExtractorNo);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	/* get the information on keyword extractor */
	e = LOM_Text_GetKeywordExtractorInfo(handle, volId, keywordExtractorNo, filePath, functionName, functionNameForGettingNextPostingInfo, functionNameForFinalizingKeywordExtractor);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	/* dynamically open the shared object */
#ifdef USE_RPC
	if(LOM_USERPCFORDLLFUNCTIONSFLAG(handle))
		*fhandle = lom_RPCdlopen(handle, filePath, RTLD_LAZY);
	else
		*fhandle = lom_dlopen(handle, filePath, RTLD_LAZY);
#else
	*fhandle = lom_dlopen(handle, filePath, RTLD_LAZY);
#endif
	if(*fhandle == NULL) {
		fprintf(stderr,"%s\n", lom_dlerror(handle));
		LOM_ERROR(handle, eCANNOTLINKFILTERFUNCTION_LOM);
	}

	/* dynamically get the function pointer to the given fuction */
#ifdef USE_RRC
	if(LOM_USERPCFORDLLFUNCTIONSFLAG(handle))
		*fptrToKeywordExtractor = (lom_FptrToKeywordExtractor)lom_RPCdlsym(handle, *fhandle, functionName);
	else
		*fptrToKeywordExtractor = (lom_FptrToKeywordExtractor)lom_dlsym(handle, *fhandle, functionName);
#else
	*fptrToKeywordExtractor = (lom_FptrToKeywordExtractor)lom_dlsym(handle, *fhandle, functionName);
#endif
	if(*fptrToKeywordExtractor  == NULL) {
		fprintf(stderr,"%s\n", lom_dlerror(handle));
		LOM_ERROR(handle, eCANNOTLINKFILTERFUNCTION_LOM);
	}
#ifdef USE_RPC
	if(LOM_USERPCFORDLLFUNCTIONSFLAG(handle))
		*fptrToGettingNextPostingInfo = (lom_FptrToGetNextPostingInfo)lom_RPCdlsym(handle, *fhandle, functionNameForGettingNextPostingInfo);
	else
		*fptrToGettingNextPostingInfo = (lom_FptrToGetNextPostingInfo)lom_dlsym(handle, *fhandle, functionNameForGettingNextPostingInfo);
#else
	*fptrToGettingNextPostingInfo = (lom_FptrToGetNextPostingInfo)lom_dlsym(handle, *fhandle, functionNameForGettingNextPostingInfo);
#endif
	if(*fptrToGettingNextPostingInfo  == NULL) {
		fprintf(stderr,"%s\n", lom_dlerror(handle));
		LOM_ERROR(handle, eCANNOTLINKFILTERFUNCTION_LOM);
	}

#ifdef USE_RPC
	if(LOM_USERPCFORDLLFUNCTIONSFLAG(handle))
		*fptrToFinalizeKeywordExtraction = (lom_FptrToFinalizeKeywordExtraction)lom_RPCdlsym(handle, *fhandle, functionNameForFinalizingKeywordExtractor);
	else
		*fptrToFinalizeKeywordExtraction = (lom_FptrToFinalizeKeywordExtraction)lom_dlsym(handle, *fhandle, functionNameForFinalizingKeywordExtractor);
#else
	*fptrToFinalizeKeywordExtraction = (lom_FptrToFinalizeKeywordExtraction)lom_dlsym(handle, *fhandle, functionNameForFinalizingKeywordExtractor);
#endif
	if(*fptrToFinalizeKeywordExtraction  == NULL) {
		fprintf(stderr,"%s\n", lom_dlerror(handle));
		LOM_ERROR(handle, eCANNOTLINKFILTERFUNCTION_LOM);
	}

	return eNOERROR;
}

Four LOM_Text_OpenHandleForStemizer(
	LOM_Handle *handle, 
	Four volId,
	Four classId,
	Four colNo,
	void **fhandle,
	lom_FptrToStemizer *fptrToStemizer
)
{
	Four e;
	Four stemizerNo;
	char functionName[LOM_MAXKEYWORDEXTRACTORNAME];
	char filePath[LOM_MAXFILTERFUNCTIONNAME];

	/* get the stemizer number */
	e = LOM_Text_GetCurrentStemizerNo (handle, volId, classId, colNo,
			&stemizerNo);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	if(stemizerNo == LOM_DEFAULT_STEMIZER_NO)
	{
		*fhandle        = NULL;
		*fptrToStemizer = NULL;

		return eNOERROR;
	}

	/* get the information on keyword extractor */
	e = LOM_Text_GetStemizerInfo(handle, volId, stemizerNo, filePath, functionName);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	/* dynamically open the shared object */
#ifdef USE_RPC
	if(LOM_USERPCFORDLLFUNCTIONSFLAG(handle))
		*fhandle = lom_RPCdlopen(handle, filePath, RTLD_LAZY);
	else
		*fhandle = lom_dlopen(handle, filePath, RTLD_LAZY);
#else
	*fhandle = lom_dlopen(handle, filePath, RTLD_LAZY);
#endif

	if(*fhandle == NULL) {
		fprintf(stderr,"%s\n", lom_dlerror(handle));
		LOM_ERROR(handle, eCANNOTLINKFILTERFUNCTION_LOM);
	}

	/* dynamically get the function pointer to the given fuction */
#ifdef USE_RPC
	if(LOM_USERPCFORDLLFUNCTIONSFLAG(handle))
		*fptrToStemizer = (lom_FptrToStemizer)lom_RPCdlsym(handle, *fhandle, functionName);
	else
		*fptrToStemizer = (lom_FptrToStemizer)lom_dlsym(handle, *fhandle, functionName);
#else
	*fptrToStemizer = (lom_FptrToStemizer)lom_dlsym(handle, *fhandle, functionName);
#endif
	if(*fptrToStemizer  == NULL) {
		fprintf(stderr,"%s\n", lom_dlerror(handle));
		LOM_ERROR(handle, eCANNOTLINKFILTERFUNCTION_LOM);
	}

	return eNOERROR;
}

Four LOM_Text_CloseHandle(
	LOM_Handle *handle, 
	void *fhandle
)
{
	if(fhandle == NULL) {
#if defined(PRELOAD_KEYWORDEXTRACTOR) && defined(PRELOAD_FILTER)
		LOM_ERROR(handle, eCANNOTCLOSETEXTHANDLE_LOM);
#else
		return eNOERROR;
#endif
	}

#ifndef DLCLOSE_BUG
	if(!lom_dlclose(handle, fhandle)) return eNOERROR;
	else {
		fprintf(stderr,"%s\n", lom_dlerror(handle));
		LOM_ERROR(handle, eCANNOTCLOSETEXTHANDLE_LOM);
	}
#else
	return eNOERROR;
#endif

}

#else /* SLIMDOWN_TEXTIR */


#include "LOM_Internal.h"
#include "LOM.h"
#include "Catalog.h"
#include "Catalog_Internal.h"


Four lom_Text_CreateFilterCatalog(
	LOM_Handle *handle, 
	Four volId
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_DestroyFilterCatalog(
	LOM_Handle *handle, 
	Four volId
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_CreateKeywordExtractorCatalog(
	LOM_Handle *handle, 
	Four volId
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_DestroyKeywordExtractorCatalog(
	LOM_Handle *handle, 
	Four volId
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_CreatePreferenceCatalog(
	LOM_Handle *handle, 
	Four volId
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_DestroyPreferenceCatalog(
	LOM_Handle *handle, 
	Four volId
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four LOM_Text_AddFilter(
	LOM_Handle *handle, 
	Four volId,
	char *filterName,
	Four version,
	char *filterFilePath,
	char *filterFunctionName,
	Four *filterNo
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four LOM_Text_DropFilter(
	LOM_Handle *handle, 
	Four volId,
	char *filterName,
	Four version
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four LOM_Text_GetFilterNo(
	LOM_Handle *handle, 
	Four volId,
	char *filterName,
	Four version,
	Four *filterNo
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four LOM_Text_SetFilter(
	LOM_Handle *handle, 
	Four volId,
	Four classID,
	Four colNo,
	Four filterNo
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four LOM_Text_ResetFilter(
	LOM_Handle *handle, 
	Four volId,
	Four classID,
	Four colNo
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four LOM_Text_GetFilterInfo(
	LOM_Handle *handle, 
	Four volId,
	Four filterNo,
	char *filePath,
	char *functionName
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four LOM_Text_AddKeywordExtractor(
	LOM_Handle *handle, 
	Four volId,
	char *keywordExtractorName,
	Four version,
	char *keywordExtractorFilePath,
	char *keywordExtractorFunctionName,
	char *getNextPostingFunctionName,
	char *finalizeKeywordExtractorFunctionName,
	Four *keywordExtractorNo
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four LOM_Text_AddDefaultKeywordExtractor(
	LOM_Handle *handle, 
	Four volId,
	char *keywordExtractorName,
	Four version,
	char *keywordExtractorFilePath,
	char *keywordExtractorFunctionName,
	char *getNextPostingFunctionName,
	char *finalizeKeywordExtractorFunctionName
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four LOM_Text_DropKeywordExtractor(
	LOM_Handle *handle, 
	Four volId,
	char *keywordExtractorName,
	Four version
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four LOM_Text_GetKeywordExtractorNo(
	LOM_Handle *handle, 
	Four volId,
	char *keywordExtractorName,
	Four version,
	Four *keywordExtractorNo
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four LOM_Text_SetKeywordExtractor(
	LOM_Handle *handle, 
	Four volId,
	Four classID,
	Four colNo,
	Four keywordExtractorNo
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four LOM_Text_ResetKeywordExtractor(
	LOM_Handle *handle, 
	Four volId,
	Four classID,
	Four colNo
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four LOM_Text_GetKeywordExtractorInfo(
	LOM_Handle *handle, 
	Four volId,
	Four keywordExtractorNo,
	char *filePath,
	char *keywordExtractorFunctionName,
	char *getNextPostingInfoFunctionName,
	char *finalizeKeywordExtractorFunctionName
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four LOM_Text_GetCurrentFilterNo (
	LOM_Handle *handle, 
	Four volId,
	Four classId,
	Four colNo,
	Four *filterNo
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four LOM_Text_GetCurrentKeywordExtractorNo (
	LOM_Handle *handle, 
	Four volId,
	Four classId,
	Four colNo,
	Four *keywordExtractorNo
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four LOM_Text_GetCurrentStemizerNo (
	LOM_Handle *handle, 
	Four volId,
	Four classId,
	Four colNo,
	Four *stemizerNo
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four LOM_Text_OpenHandleForFilter(
	LOM_Handle *handle, 
	Four volId,
	Four classId,
	Four colNo,
	void **fhandle,
	lom_FptrToFilter *fptrToFilter
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four LOM_Text_OpenHandleForKeywordExtractor(
	LOM_Handle *handle, 
	Four volId,
	Four classId,
	Four colNo,
	void **fhandle,
	lom_FptrToKeywordExtractor *fptrToKeywordExtractor,
	lom_FptrToGetNextPostingInfo *fptrToGettingNextPostingInfo,
	lom_FptrToFinalizeKeywordExtraction *fptrToFinalizeKeywordExtraction
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four LOM_Text_OpenHandleForStemizer(
	LOM_Handle *handle, 
	Four volId,
	Four classId,
	Four colNo,
	void **fhandle,
	lom_FptrToStemizer *fptrToStemizer
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four LOM_Text_CloseHandle(
	LOM_Handle *handle, 
	void *fhandle
)
{
	return eTEXTIR_NOTENABLED_LOM;
}

#endif /* SLIMDOWN_TEXTIR */
