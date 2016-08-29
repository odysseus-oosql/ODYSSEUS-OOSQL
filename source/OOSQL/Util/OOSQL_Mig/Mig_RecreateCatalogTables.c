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

#include "Mig_Catalog.h"

Four mig_DestroyCatalogTables(
    LOM_Handle      *handle,
	Four			volId)
{
	Four			e;


	e = LRDS_DestroyRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_TEXT_STEMIZERID_CLASSNAME);
	if(e < 0) LOM_ERROR(handle, e);

	e = LRDS_DestroyRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSTEXTSTEMIZERINFO_CLASSNAME);
	if(e < 0) LOM_ERROR(handle, e);

	e = LRDS_DestroyRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_TEXT_KEYWORDEXTRACTORID_CLASSNAME);
	if(e < 0) LOM_ERROR(handle, e);

	e = LRDS_DestroyRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_TEXT_FILTERID_CLASSNAME);
	if(e < 0) LOM_ERROR(handle, e);

	e = LRDS_DestroyRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSTEXTPREFERENCES_CLASSNAME);
	if(e < 0) LOM_ERROR(handle, e);

	e = LRDS_DestroyRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSTEXTKEYWORDEXTRACTORINFO_CLASSNAME);
	if(e < 0) LOM_ERROR(handle, e);

	e = LRDS_DestroyRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSTEXTFILTERINFO_CLASSNAME);
	if(e < 0) LOM_ERROR(handle, e);

	e = LRDS_DestroyRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSTEXTINDEXES_CLASSNAME);
	if(e < 0) LOM_ERROR(handle, e);

	e = LRDS_DestroyRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_RELATIONSHIPID_CLASSNAME);
	if(e < 0) LOM_ERROR(handle, e);

	e = LRDS_DestroyRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSRELATIONSHIP_CLASSNAME);
	if(e < 0) LOM_ERROR(handle, e);

	e = LRDS_DestroyRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_CLASSID_CLASSNAME);
	if(e < 0) LOM_ERROR(handle, e);

	e = LRDS_DestroyRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSMETHODS_CLASSNAME);
	if(e < 0) LOM_ERROR(handle, e);

	e = LRDS_DestroyRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_INHERITANCE_CLASSNAME);
	if(e < 0) LOM_ERROR(handle, e);

	e = LRDS_DestroyRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSINDEXES_CLASSNAME);
	if(e < 0) LOM_ERROR(handle, e);
	
	e = LRDS_DestroyRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSCOLUMNS_CLASSNAME);
	if(e < 0) LOM_ERROR(handle, e);

	e = LRDS_DestroyRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSCLASSES_CLASSNAME);
	if(e < 0) LOM_ERROR(handle, e);

	return eNOERROR;
}

Four mig_CreateCatalogTables(
    LOM_Handle      *handle,
	Four			volId)
{
	Four e;		/* error code */
	ColInfo lomSysClasses[LOM_SYSCLASSES_NUM_COLS];		/* lomSysClasses columns information */
	ColInfo lomInheritance[LOM_INHERITANCE_NUM_COLS];	/* lomSysInheritance columns information */
	ColInfo lomSysColumns[LOM_SYSCOLUMNS_NUM_COLS];		/* lomSysColumns columns information */
	ColInfo lomSysMethods[LOM_SYSMETHODS_NUM_COLS];		/* lomSysclasses columns information */
	ColInfo lomClassId[LOM_CLASSID_NUM_COLS];
	ColInfo lomRelationshipId[LOM_RELATIONSHIPID_NUM_COLS];		
	ColInfo lomSysRelationship[LOM_SYSRELATIONSHIP_NUM_COLS]; /* lomSysRelationship columns information */
	ColInfo lomSysIndexes[LOM_SYSINDEXES_NUM_COLS];		/* lomSysIndexes columns information */
	ColInfo lomTextFilterId[LOM_CLASSID_NUM_COLS];
	ColInfo lomTextKeywordExtractorId[LOM_CLASSID_NUM_COLS];
	ColInfo lomTextStemizerId[LOM_CLASSID_NUM_COLS]; 
	ColListStruct clist[1]; /* column list */
	LRDS_IndexDesc idesc;			/* key information for the clustering index */
	XactID  xactId;			/* transaction id */
	Four orn;				/* open relation number */
	Four catScanId;			/* scan Id */
	IndexID iid;
	LockParameter lockup;   /* IN lock mode & duration */
#ifndef LRDS_COUNTER
	ColInfo lomSysCounter[LOM_SYSCOUNTER_NUM_COLS];		/* lomSysCounter columns information */
#endif


	/* class name */
	lomSysClasses[LOM_SYSCLASSES_CLASSNAME_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysClasses[LOM_SYSCLASSES_CLASSNAME_COLNO].type = SM_VARSTRING;
	lomSysClasses[LOM_SYSCLASSES_CLASSNAME_COLNO].length = MAXRELNAME;

	/* class id */
	lomSysClasses[LOM_SYSCLASSES_CLASSID_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysClasses[LOM_SYSCLASSES_CLASSID_COLNO].type = LOM_LONG_VAR;
	lomSysClasses[LOM_SYSCLASSES_CLASSID_COLNO].length = LOM_LONG_SIZE_VAR;

	/* File id */
	lomSysClasses[LOM_SYSCLASSES_DATAFILEID_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysClasses[LOM_SYSCLASSES_DATAFILEID_COLNO].type = SM_FILEID;
	lomSysClasses[LOM_SYSCLASSES_DATAFILEID_COLNO].length = SM_FILEID_SIZE;

	/* kinfo -> idesc */
	idesc.indexType = SM_INDEXTYPE_BTREE;
	idesc.kinfo.btree.flag = KEYFLAG_CLUSTERING;
	idesc.kinfo.btree.nColumns = 1;
	idesc.kinfo.btree.columns[0].colNo = LOM_SYSCLASSES_CLASSNAME_COLNO;		
	idesc.kinfo.btree.columns[0].flag = KEYINFO_COL_ASC;		

	e = LRDS_CreateRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSCLASSES_CLASSNAME, &idesc, LOM_SYSCLASSES_NUM_COLS, &lomSysClasses[0], SM_FALSE);

	if (e < 0) {
		LRDS_AbortTransaction(LOM_GET_LRDS_HANDLE(handle), &xactId);
		LRDS_Dismount(LOM_GET_LRDS_HANDLE(handle), volId);
		LOM_ERROR(handle,e);
	};
	/* kinfo -> idesc */
	idesc.indexType = SM_INDEXTYPE_BTREE;
	idesc.kinfo.btree.flag = KEYFLAG_UNIQUE;
	idesc.kinfo.btree.nColumns = 1;
	idesc.kinfo.btree.columns[0].colNo = LOM_SYSCLASSES_CLASSID_COLNO;
	idesc.kinfo.btree.columns[0].flag = KEYINFO_COL_ASC;		

	e = LRDS_AddIndex(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSCLASSES_CLASSNAME, &idesc, &iid);
	if (e < 0) {
		LRDS_AbortTransaction(LOM_GET_LRDS_HANDLE(handle), &xactId);
		LRDS_Dismount(LOM_GET_LRDS_HANDLE(handle), volId);
		LOM_ERROR(handle,e);
	};


	/* lomInheritance Catalog table information */

	/* class id */
	lomInheritance[LOM_INHERITANCE_CLASSID_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomInheritance[LOM_INHERITANCE_CLASSID_COLNO].type = LOM_LONG_VAR;
	lomInheritance[LOM_INHERITANCE_CLASSID_COLNO].length = LOM_LONG_SIZE_VAR;

	/* superclass id */
	lomInheritance[LOM_INHERITANCE_SUPERCLASSID_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomInheritance[LOM_INHERITANCE_SUPERCLASSID_COLNO].type = LOM_LONG_VAR;
	lomInheritance[LOM_INHERITANCE_SUPERCLASSID_COLNO].length = LOM_LONG_SIZE_VAR;

	/* order */
	lomInheritance[LOM_INHERITANCE_ORDER_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomInheritance[LOM_INHERITANCE_ORDER_COLNO].type = LOM_LONG_VAR;
	lomInheritance[LOM_INHERITANCE_ORDER_COLNO].length = LOM_LONG_SIZE_VAR;

	/* kinfo -> idesc */
	idesc.indexType = SM_INDEXTYPE_BTREE;
	idesc.kinfo.btree.flag = KEYFLAG_CLUSTERING;
	idesc.kinfo.btree.nColumns = 1;
	idesc.kinfo.btree.columns[0].colNo = LOM_INHERITANCE_CLASSID_COLNO;		
	idesc.kinfo.btree.columns[0].flag = KEYINFO_COL_ASC;		

	e = LRDS_CreateRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_INHERITANCE_CLASSNAME, &idesc, LOM_INHERITANCE_NUM_COLS, &lomInheritance[0], SM_FALSE);
	if (e < 0) {
		LRDS_AbortTransaction(LOM_GET_LRDS_HANDLE(handle), &xactId);
		LRDS_Dismount(LOM_GET_LRDS_HANDLE(handle), volId);
		LOM_ERROR(handle,e);
	};

	idesc.indexType = SM_INDEXTYPE_BTREE;
	idesc.kinfo.btree.flag = 0;
	idesc.kinfo.btree.nColumns = 1;
	idesc.kinfo.btree.columns[0].colNo = LOM_INHERITANCE_SUPERCLASSID_COLNO;		
	idesc.kinfo.btree.columns[0].flag = KEYINFO_COL_ASC;		

	e = LRDS_AddIndex(LOM_GET_LRDS_HANDLE(handle), volId, LOM_INHERITANCE_CLASSNAME, &idesc, &iid);
	if (e < 0) {
		LRDS_AbortTransaction(LOM_GET_LRDS_HANDLE(handle), &xactId);
		LRDS_Dismount(LOM_GET_LRDS_HANDLE(handle), volId);
		LOM_ERROR(handle,e);
	};

	/* lomSysColumns Catalog table information */

	/* class id */
	lomSysColumns[LOM_SYSCOLUMNS_CLASSID_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysColumns[LOM_SYSCOLUMNS_CLASSID_COLNO].type = LOM_LONG_VAR;
	lomSysColumns[LOM_SYSCOLUMNS_CLASSID_COLNO].length = LOM_LONG_SIZE_VAR;

	/* complex type */
	lomSysColumns[LOM_SYSCOLUMNS_COMPLEXTYPE_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysColumns[LOM_SYSCOLUMNS_COMPLEXTYPE_COLNO].type = LOM_SHORT_VAR;
	lomSysColumns[LOM_SYSCOLUMNS_COMPLEXTYPE_COLNO].length = LOM_SHORT_SIZE_VAR;

	/* column type	*/
	/* incluing lom level type such as db_Ref, db_Link
	*/
	lomSysColumns[LOM_SYSCOLUMNS_COLTYPE_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysColumns[LOM_SYSCOLUMNS_COLTYPE_COLNO].type = LOM_SHORT_VAR;
	lomSysColumns[LOM_SYSCOLUMNS_COLTYPE_COLNO].length = LOM_SHORT_SIZE_VAR;

	/* column number */
	lomSysColumns[LOM_SYSCOLUMNS_COLUMNNO_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysColumns[LOM_SYSCOLUMNS_COLUMNNO_COLNO].type = LOM_SHORT_VAR;
	lomSysColumns[LOM_SYSCOLUMNS_COLUMNNO_COLNO].length = LOM_SHORT_SIZE_VAR;
	/* column name */
	lomSysColumns[LOM_SYSCOLUMNS_COLNAME_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysColumns[LOM_SYSCOLUMNS_COLNAME_COLNO].type = SM_VARSTRING;
	lomSysColumns[LOM_SYSCOLUMNS_COLNAME_COLNO].length = MAXRELNAME;

	/* column offset */
	lomSysColumns[LOM_SYSCOLUMNS_COLOFFSET_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysColumns[LOM_SYSCOLUMNS_COLOFFSET_COLNO].type = LOM_LONG_VAR;
	lomSysColumns[LOM_SYSCOLUMNS_COLOFFSET_COLNO].length = LOM_LONG_SIZE_VAR;

	/* inherited from */
	lomSysColumns[LOM_SYSCOLUMNS_INHERITEDFROM_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysColumns[LOM_SYSCOLUMNS_INHERITEDFROM_COLNO].type = LOM_LONG_VAR;
	lomSysColumns[LOM_SYSCOLUMNS_INHERITEDFROM_COLNO].length = LOM_LONG_SIZE_VAR;

	/* domain */
	lomSysColumns[LOM_SYSCOLUMNS_DOMAINID_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysColumns[LOM_SYSCOLUMNS_DOMAINID_COLNO].type = LOM_LONG_VAR;
	lomSysColumns[LOM_SYSCOLUMNS_DOMAINID_COLNO].length = LOM_LONG_SIZE_VAR;

	lomSysColumns[LOM_SYSCOLUMNS_ACTIVECOLUMNNO_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysColumns[LOM_SYSCOLUMNS_ACTIVECOLUMNNO_COLNO].type = LOM_SHORT_VAR;
	lomSysColumns[LOM_SYSCOLUMNS_ACTIVECOLUMNNO_COLNO].length = LOM_SHORT_SIZE_VAR;

	/* key setting */
	idesc.indexType = SM_INDEXTYPE_BTREE;
	idesc.kinfo.btree.flag = KEYFLAG_CLUSTERING;
	idesc.kinfo.btree.nColumns = 1;
	idesc.kinfo.btree.columns[0].colNo = LOM_SYSCOLUMNS_CLASSID_COLNO;		
	idesc.kinfo.btree.columns[0].flag = KEYINFO_COL_ASC;		

	e = LRDS_CreateRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSCOLUMNS_CLASSNAME, &idesc, LOM_SYSCOLUMNS_NUM_COLS, &lomSysColumns[0], SM_FALSE);
	if (e < 0) {
		LRDS_AbortTransaction(LOM_GET_LRDS_HANDLE(handle), &xactId);
		LRDS_Dismount(LOM_GET_LRDS_HANDLE(handle), volId);
		LOM_ERROR(handle,e);
	};

	/* lomSysIndexes Catalog table information */

	/* index name */
	lomSysIndexes[LOM_SYSINDEXES_INDEXNAME_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysIndexes[LOM_SYSINDEXES_INDEXNAME_COLNO].type = SM_VARSTRING;
	lomSysIndexes[LOM_SYSINDEXES_INDEXNAME_COLNO].length = LOM_MAXINDEXNAME;

	/* class name */
	lomSysIndexes[LOM_SYSINDEXES_CLASSNAME_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysIndexes[LOM_SYSINDEXES_CLASSNAME_COLNO].type = SM_VARSTRING;
	lomSysIndexes[LOM_SYSINDEXES_CLASSNAME_COLNO].length = LOM_MAXCLASSNAME;

	/* Index id */
	lomSysIndexes[LOM_SYSINDEXES_INDEXID_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysIndexes[LOM_SYSINDEXES_INDEXID_COLNO].type = SM_STRING;
	lomSysIndexes[LOM_SYSINDEXES_INDEXID_COLNO].length = sizeof(LOM_IndexID);

	/* kinfo -> idesc */
	idesc.indexType = SM_INDEXTYPE_BTREE;
	idesc.kinfo.btree.flag = KEYFLAG_CLUSTERING | KEYFLAG_UNIQUE;
	idesc.kinfo.btree.nColumns = 1;
	idesc.kinfo.btree.columns[0].colNo = LOM_SYSINDEXES_INDEXNAME_COLNO;		
	idesc.kinfo.btree.columns[0].flag = KEYINFO_COL_ASC;		

	e = LRDS_CreateRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSINDEXES_CLASSNAME, &idesc, LOM_SYSINDEXES_NUM_COLS, &lomSysIndexes[0], SM_FALSE);
	if (e < 0) {
		LRDS_AbortTransaction(LOM_GET_LRDS_HANDLE(handle), &xactId);
		LRDS_Dismount(LOM_GET_LRDS_HANDLE(handle), volId);
		LOM_ERROR(handle,e);
	};

	idesc.indexType = SM_INDEXTYPE_BTREE;
	idesc.kinfo.btree.flag = 0;
	idesc.kinfo.btree.nColumns = 1;
	idesc.kinfo.btree.columns[0].colNo = LOM_SYSINDEXES_CLASSNAME_COLNO;		
	idesc.kinfo.btree.columns[0].flag = KEYINFO_COL_ASC;		

	e = LRDS_AddIndex(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSINDEXES_CLASSNAME, &idesc, &iid);
	if (e < 0) {
		LRDS_AbortTransaction(LOM_GET_LRDS_HANDLE(handle), &xactId);
		LRDS_Dismount(LOM_GET_LRDS_HANDLE(handle), volId);
		LOM_ERROR(handle,e);
	};

	/* lomSysTextIndexes Catalog Table information */

	e = lom_Text_CreateCatalogTable(handle, volId);
	if (e < 0) {
		LRDS_AbortTransaction(LOM_GET_LRDS_HANDLE(handle), &xactId);
		LRDS_Dismount(LOM_GET_LRDS_HANDLE(handle), volId);
		LOM_ERROR(handle,e);
	};

	/* lomSysMethods Catalog table information */

	/* class id */
	lomSysMethods[LOM_SYSMETHODS_CLASSID_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysMethods[LOM_SYSMETHODS_CLASSID_COLNO].type = LOM_LONG_VAR;
	lomSysMethods[LOM_SYSMETHODS_CLASSID_COLNO].length = LOM_LONG_SIZE_VAR;

	/* dir path */
	lomSysMethods[LOM_SYSMETHODS_DIRPATH_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysMethods[LOM_SYSMETHODS_DIRPATH_COLNO].type = SM_VARSTRING;
	lomSysMethods[LOM_SYSMETHODS_DIRPATH_COLNO].length = LOM_MAXPATHLENGTH;

	/* method name */
	lomSysMethods[LOM_SYSMETHODS_METHODNAME_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysMethods[LOM_SYSMETHODS_METHODNAME_COLNO].type = SM_VARSTRING;
	lomSysMethods[LOM_SYSMETHODS_METHODNAME_COLNO].length = LOM_MAXMETHODNAME;

	/* function name */
	lomSysMethods[LOM_SYSMETHODS_FUNCTIONNAME_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysMethods[LOM_SYSMETHODS_FUNCTIONNAME_COLNO].type = SM_VARSTRING;
	lomSysMethods[LOM_SYSMETHODS_FUNCTIONNAME_COLNO].length = LOM_MAXFUNCTIONNAME;

	/* nArguments */
	lomSysMethods[LOM_SYSMETHODS_NARGUMENTS_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysMethods[LOM_SYSMETHODS_NARGUMENTS_COLNO].type = LOM_SHORT_VAR;
	lomSysMethods[LOM_SYSMETHODS_NARGUMENTS_COLNO].length = LOM_SHORT_SIZE_VAR;

	/* Argument List */
	lomSysMethods[LOM_SYSMETHODS_ARGUMENTLIST_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysMethods[LOM_SYSMETHODS_ARGUMENTLIST_COLNO].type = SM_VARSTRING;
	lomSysMethods[LOM_SYSMETHODS_ARGUMENTLIST_COLNO].length = LOM_MAXNARGUMENT*LOM_LONG_SIZE_VAR;

	/* Return Type */
	lomSysMethods[LOM_SYSMETHODS_RETURNTYPE_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysMethods[LOM_SYSMETHODS_RETURNTYPE_COLNO].type = LOM_LONG_VAR;
	lomSysMethods[LOM_SYSMETHODS_RETURNTYPE_COLNO].length = LOM_LONG_SIZE_VAR;

	/* Inherited_From */
	lomSysMethods[LOM_SYSMETHODS_INHERITEDFROM_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysMethods[LOM_SYSMETHODS_INHERITEDFROM_COLNO].type = LOM_LONG_VAR;
	lomSysMethods[LOM_SYSMETHODS_INHERITEDFROM_COLNO].length = LOM_LONG_SIZE_VAR;

	idesc.indexType = SM_INDEXTYPE_BTREE;
	idesc.kinfo.btree.flag = KEYFLAG_CLUSTERING;
	idesc.kinfo.btree.nColumns = 1;
	idesc.kinfo.btree.columns[0].colNo = LOM_SYSMETHODS_CLASSID_COLNO;		
	idesc.kinfo.btree.columns[0].flag = KEYINFO_COL_ASC;		

	e = LRDS_CreateRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSMETHODS_CLASSNAME, &idesc, LOM_SYSMETHODS_NUM_COLS, &lomSysMethods[0], SM_FALSE);
	if (e < 0) {
		LRDS_AbortTransaction(LOM_GET_LRDS_HANDLE(handle), &xactId);
		LRDS_Dismount(LOM_GET_LRDS_HANDLE(handle), volId);
		LOM_ERROR(handle,e);
	};


	/* lomSysMethods Catalog table information */

	/* fromClassId */
	lomSysRelationship[LOM_SYSRELATIONSHIP_FROMCLASSID_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysRelationship[LOM_SYSRELATIONSHIP_FROMCLASSID_COLNO].type = LOM_LONG_VAR;
	lomSysRelationship[LOM_SYSRELATIONSHIP_FROMCLASSID_COLNO].length = LOM_LONG_SIZE_VAR;

	/* fromAttrNum */
	lomSysRelationship[LOM_SYSRELATIONSHIP_FROMATTRNUM_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysRelationship[LOM_SYSRELATIONSHIP_FROMATTRNUM_COLNO].type = LOM_SHORT_VAR;
	lomSysRelationship[LOM_SYSRELATIONSHIP_FROMATTRNUM_COLNO].length = LOM_SHORT_SIZE_VAR;

	/* toClassId */
	lomSysRelationship[LOM_SYSRELATIONSHIP_TOCLASSID_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysRelationship[LOM_SYSRELATIONSHIP_TOCLASSID_COLNO].type = LOM_LONG_VAR;
	lomSysRelationship[LOM_SYSRELATIONSHIP_TOCLASSID_COLNO].length = LOM_LONG_SIZE_VAR;

	/* toAttrNum */
	lomSysRelationship[LOM_SYSRELATIONSHIP_TOATTRNUM_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysRelationship[LOM_SYSRELATIONSHIP_TOATTRNUM_COLNO].type = LOM_SHORT_VAR;
	lomSysRelationship[LOM_SYSRELATIONSHIP_TOATTRNUM_COLNO].length = LOM_SHORT_SIZE_VAR;

	/* cardinality */
	lomSysRelationship[LOM_SYSRELATIONSHIP_CARDINALITY_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysRelationship[LOM_SYSRELATIONSHIP_CARDINALITY_COLNO].type = SM_STRING;
	lomSysRelationship[LOM_SYSRELATIONSHIP_CARDINALITY_COLNO].length = 1;

	/* relationship name */
	lomSysRelationship[LOM_SYSRELATIONSHIP_RELATIONSHIPNAME_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysRelationship[LOM_SYSRELATIONSHIP_RELATIONSHIPNAME_COLNO].type = SM_VARSTRING;
	lomSysRelationship[LOM_SYSRELATIONSHIP_RELATIONSHIPNAME_COLNO].length = LOM_MAXRELATIONSHIPNAME;

	/* direction */
	lomSysRelationship[LOM_SYSRELATIONSHIP_DIRECTION_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysRelationship[LOM_SYSRELATIONSHIP_DIRECTION_COLNO].type = SM_STRING;
	lomSysRelationship[LOM_SYSRELATIONSHIP_DIRECTION_COLNO].length = 1;

	/* relationship Id */
	lomSysRelationship[LOM_SYSRELATIONSHIP_RELATIONSHIPID_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysRelationship[LOM_SYSRELATIONSHIP_RELATIONSHIPID_COLNO].type = LOM_LONG_VAR;
	lomSysRelationship[LOM_SYSRELATIONSHIP_RELATIONSHIPID_COLNO].length = LOM_LONG_SIZE_VAR;

	idesc.indexType = SM_INDEXTYPE_BTREE;
	idesc.kinfo.btree.flag = KEYFLAG_CLUSTERING;
	idesc.kinfo.btree.nColumns = 1;
	idesc.kinfo.btree.columns[0].colNo = LOM_SYSRELATIONSHIP_FROMCLASSID_COLNO;
	idesc.kinfo.btree.columns[0].flag = KEYINFO_COL_ASC;
	
	e = LRDS_CreateRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSRELATIONSHIP_CLASSNAME,
							&idesc, LOM_SYSRELATIONSHIP_NUM_COLS,
							&lomSysRelationship[0], SM_FALSE);
	if (e < 0) {
		LRDS_AbortTransaction(LOM_GET_LRDS_HANDLE(handle), &xactId);
		LRDS_Dismount(LOM_GET_LRDS_HANDLE(handle), volId);
		LOM_ERROR(handle,e);
	};
	
	/*
	  Create indexes on toClassId, relationshipName, relationshipId 
	  LOM_SYSRELATIONSHIP_CLASSNAME
	*/
	idesc.indexType = SM_INDEXTYPE_BTREE;
	idesc.kinfo.btree.flag = 0;
	idesc.kinfo.btree.nColumns = 1;
	idesc.kinfo.btree.columns[0].colNo = LOM_SYSRELATIONSHIP_TOCLASSID_COLNO;
	idesc.kinfo.btree.columns[0].flag = KEYINFO_COL_ASC;

	e = LRDS_AddIndex(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSRELATIONSHIP_CLASSNAME, &idesc, &iid);
	if (e < 0) {
		LRDS_AbortTransaction(LOM_GET_LRDS_HANDLE(handle), &xactId);
		LRDS_Dismount(LOM_GET_LRDS_HANDLE(handle), volId);
		LOM_ERROR(handle,e);
	};

	idesc.indexType = SM_INDEXTYPE_BTREE;
	idesc.kinfo.btree.flag = 0;
	idesc.kinfo.btree.nColumns = 1;
	idesc.kinfo.btree.columns[0].colNo = LOM_SYSRELATIONSHIP_RELATIONSHIPNAME_COLNO;
	idesc.kinfo.btree.columns[0].flag = KEYINFO_COL_ASC;

	e = LRDS_AddIndex(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSRELATIONSHIP_CLASSNAME, &idesc, &iid);
	if (e < 0) {
		LRDS_AbortTransaction(LOM_GET_LRDS_HANDLE(handle), &xactId);
		LRDS_Dismount(LOM_GET_LRDS_HANDLE(handle), volId);
		LOM_ERROR(handle,e);
	};

	idesc.indexType = SM_INDEXTYPE_BTREE;
	idesc.kinfo.btree.flag = 0;
	idesc.kinfo.btree.nColumns = 1;
	idesc.kinfo.btree.columns[0].colNo = LOM_SYSRELATIONSHIP_RELATIONSHIPID_COLNO;		
	idesc.kinfo.btree.columns[0].flag = KEYINFO_COL_ASC;		
	
	e = LRDS_AddIndex(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSRELATIONSHIP_CLASSNAME, &idesc, &iid);
	if (e < 0) {
		LRDS_AbortTransaction(LOM_GET_LRDS_HANDLE(handle), &xactId);
		LRDS_Dismount(LOM_GET_LRDS_HANDLE(handle), volId);
		LOM_ERROR(handle,e);
	};
	

	/* class id */
	lomClassId[LOM_CLASSID_CLASSID_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomClassId[LOM_CLASSID_CLASSID_COLNO].type = LOM_LONG_VAR;
	lomClassId[LOM_CLASSID_CLASSID_COLNO].length = LOM_LONG_SIZE_VAR;

	idesc.indexType = SM_INDEXTYPE_BTREE;
	idesc.kinfo.btree.flag = KEYFLAG_CLUSTERING;
	idesc.kinfo.btree.nColumns = 1;
	idesc.kinfo.btree.columns[0].colNo = LOM_CLASSID_CLASSID_COLNO;		
	idesc.kinfo.btree.columns[0].flag = KEYINFO_COL_ASC;		

	e = LRDS_CreateRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_CLASSID_CLASSNAME, &idesc, LOM_CLASSID_NUM_COLS, &lomClassId[0], SM_FALSE);
	if (e < 0) {
		LRDS_AbortTransaction(LOM_GET_LRDS_HANDLE(handle), &xactId);
		LRDS_Dismount(LOM_GET_LRDS_HANDLE(handle), volId);
		LOM_ERROR(handle,e);
	};

	/* relationship id */
	lomRelationshipId[LOM_RELATIONSHIPID_RELATIONSHIPID_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomRelationshipId[LOM_RELATIONSHIPID_RELATIONSHIPID_COLNO].type = LOM_LONG_VAR;
	lomRelationshipId[LOM_RELATIONSHIPID_RELATIONSHIPID_COLNO].length = LOM_LONG_SIZE_VAR;

	idesc.indexType = SM_INDEXTYPE_BTREE;
	idesc.kinfo.btree.flag = KEYFLAG_CLUSTERING;
	idesc.kinfo.btree.nColumns = 1;
	idesc.kinfo.btree.columns[0].colNo = LOM_RELATIONSHIPID_RELATIONSHIPID_COLNO;		
	idesc.kinfo.btree.columns[0].flag = KEYINFO_COL_ASC;		

	e = LRDS_CreateRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_RELATIONSHIPID_CLASSNAME, &idesc, LOM_RELATIONSHIPID_NUM_COLS, &lomRelationshipId[0], SM_FALSE);
	if (e < 0) {
		LRDS_AbortTransaction(LOM_GET_LRDS_HANDLE(handle), &xactId);
		LRDS_Dismount(LOM_GET_LRDS_HANDLE(handle), volId);
		LOM_ERROR(handle,e);
	};

	/* filter id */
	lomTextFilterId[0].complexType = SM_COMPLEXTYPE_BASIC;
	lomTextFilterId[0].type = LOM_LONG_VAR;
	lomTextFilterId[0].length = LOM_LONG_SIZE_VAR;

	idesc.indexType = SM_INDEXTYPE_BTREE;
	idesc.kinfo.btree.flag = KEYFLAG_CLUSTERING;
	idesc.kinfo.btree.nColumns = 1;
	idesc.kinfo.btree.columns[0].colNo = 0;		
	idesc.kinfo.btree.columns[0].flag = KEYINFO_COL_ASC;		

	e = LRDS_CreateRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_TEXT_FILTERID_CLASSNAME, &idesc, LOM_CLASSID_NUM_COLS, &lomTextFilterId[0], SM_FALSE);
	if (e < 0) {
		LRDS_AbortTransaction(LOM_GET_LRDS_HANDLE(handle), &xactId);
		LRDS_Dismount(LOM_GET_LRDS_HANDLE(handle), volId);
		LOM_ERROR(handle,e);
	};

	/* keywordExtractor id */
	lomTextKeywordExtractorId[0].complexType = SM_COMPLEXTYPE_BASIC;
	lomTextKeywordExtractorId[0].type = LOM_LONG_VAR;
	lomTextKeywordExtractorId[0].length = LOM_LONG_SIZE_VAR;

	idesc.indexType = SM_INDEXTYPE_BTREE;
	idesc.kinfo.btree.flag = KEYFLAG_CLUSTERING;
	idesc.kinfo.btree.nColumns = 1;
	idesc.kinfo.btree.columns[0].colNo = 0;		
	idesc.kinfo.btree.columns[0].flag = KEYINFO_COL_ASC;		

	e = LRDS_CreateRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_TEXT_KEYWORDEXTRACTORID_CLASSNAME, &idesc, LOM_CLASSID_NUM_COLS, &lomTextKeywordExtractorId[0], SM_FALSE);
	if (e < 0) {
		LRDS_AbortTransaction(LOM_GET_LRDS_HANDLE(handle), &xactId);
		LRDS_Dismount(LOM_GET_LRDS_HANDLE(handle), volId);
		LOM_ERROR(handle,e);
	};

	/* stemizer id */
	lomTextStemizerId[0].complexType = SM_COMPLEXTYPE_BASIC;
	lomTextStemizerId[0].type = LOM_LONG_VAR;
	lomTextStemizerId[0].length = LOM_LONG_SIZE_VAR;

	idesc.indexType = SM_INDEXTYPE_BTREE;
	idesc.kinfo.btree.flag = KEYFLAG_CLUSTERING;
	idesc.kinfo.btree.nColumns = 1;
	idesc.kinfo.btree.columns[0].colNo = 0;		
	idesc.kinfo.btree.columns[0].flag = KEYINFO_COL_ASC;		

	e = LRDS_CreateRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_TEXT_STEMIZERID_CLASSNAME, &idesc, LOM_CLASSID_NUM_COLS, &lomTextStemizerId[0], SM_FALSE);
	if (e < 0) {
		LRDS_AbortTransaction(LOM_GET_LRDS_HANDLE(handle), &xactId);
		LRDS_Dismount(LOM_GET_LRDS_HANDLE(handle), volId);
		LOM_ERROR(handle,e);
	};

	/* set lock up parameters */
	/* just for reading */
	lockup.mode = L_IX;
	lockup.duration = L_COMMIT;

	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId,LOM_CLASSID_CLASSNAME);
	if (orn < 0)
	{
		LRDS_AbortTransaction(LOM_GET_LRDS_HANDLE(handle), &xactId);
		LRDS_Dismount(LOM_GET_LRDS_HANDLE(handle), volId);
		LOM_ERROR(handle,orn);
	}

	clist[0].colNo = LOM_CLASSID_CLASSID_COLNO;
	clist[0].start = ALL_VALUE;
	clist[0].dataLength = sizeof(Four);
	ASSIGN_VALUE_TO_COL_LIST(clist[0], INITIAL_CLASSID, sizeof(Four));
	clist[0].nullFlag = SM_FALSE;

	e = LRDS_CreateTuple(LOM_GET_LRDS_HANDLE(handle), orn, SM_FALSE, LOM_CLASSID_NUM_COLS, &(clist[0]), (TupleID*)NULL);
	if (e < 0) {
		LRDS_AbortTransaction(LOM_GET_LRDS_HANDLE(handle), &xactId);
		LRDS_Dismount(LOM_GET_LRDS_HANDLE(handle), volId);
		LOM_ERROR(handle,e);
	};

	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if (e < 0) {
		LRDS_AbortTransaction(LOM_GET_LRDS_HANDLE(handle), &xactId);
		LRDS_Dismount(LOM_GET_LRDS_HANDLE(handle), volId);
		LOM_ERROR(handle,e);
	};

	/* create relationshipID tuple */
	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId,LOM_RELATIONSHIPID_CLASSNAME);
	if (e < 0) {
		LRDS_AbortTransaction(LOM_GET_LRDS_HANDLE(handle), &xactId);
		LRDS_Dismount(LOM_GET_LRDS_HANDLE(handle), volId);
		LOM_ERROR(handle,e);
	};

	clist[0].colNo = LOM_RELATIONSHIPID_RELATIONSHIPID_COLNO;
	clist[0].start = ALL_VALUE;
	clist[0].dataLength = sizeof(Four);
	ASSIGN_VALUE_TO_COL_LIST(clist[0], INITIAL_RELATIONSHIPID, sizeof(Four));

	e = LRDS_CreateTuple(LOM_GET_LRDS_HANDLE(handle), orn, SM_FALSE, LOM_RELATIONSHIPID_NUM_COLS, &(clist[0]), (TupleID*)NULL);
	if (e < 0) {
		LRDS_AbortTransaction(LOM_GET_LRDS_HANDLE(handle), &xactId);
		LRDS_Dismount(LOM_GET_LRDS_HANDLE(handle), volId);
		LOM_ERROR(handle,e);
	};

	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if (e < 0) {
		LRDS_AbortTransaction(LOM_GET_LRDS_HANDLE(handle), &xactId);
		LRDS_Dismount(LOM_GET_LRDS_HANDLE(handle), volId);
		LOM_ERROR(handle,e);
	};

	/* create keyword extractor id */
	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId,LOM_TEXT_KEYWORDEXTRACTORID_CLASSNAME);
	if (e < 0) {
		LRDS_AbortTransaction(LOM_GET_LRDS_HANDLE(handle), &xactId);
		LRDS_Dismount(LOM_GET_LRDS_HANDLE(handle), volId);
		LOM_ERROR(handle,e);
	};

	clist[0].colNo = LOM_TEXT_KEYWORDEXTRACTORID_COLNO;
	clist[0].start = ALL_VALUE;
	clist[0].dataLength = sizeof(Four);
	ASSIGN_VALUE_TO_COL_LIST(clist[0], INITIAL_KEYWORDEXTRACTORID + 1, sizeof(Four));

	e = LRDS_CreateTuple(LOM_GET_LRDS_HANDLE(handle), orn, SM_FALSE, 1, &(clist[0]), (TupleID*)NULL);
	if (e < 0) {
		LRDS_AbortTransaction(LOM_GET_LRDS_HANDLE(handle), &xactId);
		LRDS_Dismount(LOM_GET_LRDS_HANDLE(handle), volId);
		LOM_ERROR(handle,e);
	};

	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if (e < 0) {
		LRDS_AbortTransaction(LOM_GET_LRDS_HANDLE(handle), &xactId);
		LRDS_Dismount(LOM_GET_LRDS_HANDLE(handle), volId);
		LOM_ERROR(handle,e);
	};


	/* create stemizer id */
	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId,LOM_TEXT_STEMIZERID_CLASSNAME);
	if (e < 0) {
		LRDS_AbortTransaction(LOM_GET_LRDS_HANDLE(handle), &xactId);
		LRDS_Dismount(LOM_GET_LRDS_HANDLE(handle), volId);
		LOM_ERROR(handle,e);
	};

	clist[0].colNo = LOM_TEXT_STEMIZERID_COLNO;
	clist[0].start = ALL_VALUE;
	clist[0].dataLength = sizeof(Four);
	ASSIGN_VALUE_TO_COL_LIST(clist[0], INITIAL_STEMIZERID + 1, sizeof(Four));

	e = LRDS_CreateTuple(LOM_GET_LRDS_HANDLE(handle), orn, SM_FALSE, 1, &(clist[0]), (TupleID*)NULL);
	if (e < 0) {
		LRDS_AbortTransaction(LOM_GET_LRDS_HANDLE(handle), &xactId);
		LRDS_Dismount(LOM_GET_LRDS_HANDLE(handle), volId);
		LOM_ERROR(handle,e);
	};

	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if (e < 0) {
		LRDS_AbortTransaction(LOM_GET_LRDS_HANDLE(handle), &xactId);
		LRDS_Dismount(LOM_GET_LRDS_HANDLE(handle), volId);
		LOM_ERROR(handle,e);
	};


	/* create filter id */
	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId,LOM_TEXT_FILTERID_CLASSNAME);
	if (e < 0) {
		LRDS_AbortTransaction(LOM_GET_LRDS_HANDLE(handle), &xactId);
		LRDS_Dismount(LOM_GET_LRDS_HANDLE(handle), volId);
		LOM_ERROR(handle,e);
	};

	clist[0].colNo = LOM_TEXT_FILTERID_FILTERID_COLNO;
	clist[0].start = ALL_VALUE;
	clist[0].dataLength = sizeof(Four);
	ASSIGN_VALUE_TO_COL_LIST(clist[0], INITIAL_FILTERID, sizeof(Four));

	e = LRDS_CreateTuple(LOM_GET_LRDS_HANDLE(handle), orn, SM_FALSE, 1, &(clist[0]), (TupleID*)NULL);
	if (e < 0) {
		LRDS_AbortTransaction(LOM_GET_LRDS_HANDLE(handle), &xactId);
		LRDS_Dismount(LOM_GET_LRDS_HANDLE(handle), volId);
		LOM_ERROR(handle,e);
	}

	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if (e < 0) {
		LRDS_AbortTransaction(LOM_GET_LRDS_HANDLE(handle), &xactId);
		LRDS_Dismount(LOM_GET_LRDS_HANDLE(handle), volId);
		LOM_ERROR(handle,e);
	}

#ifndef LRDS_COUNTER
	
	lomSysCounter[LOM_SYSCOUNTER_NAME_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysCounter[LOM_SYSCOUNTER_NAME_COLNO].type = SM_VARSTRING;
	lomSysCounter[LOM_SYSCOUNTER_NAME_COLNO].length = MAXRELNAME;

	lomSysCounter[LOM_SYSCOUNTER_ID_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysCounter[LOM_SYSCOUNTER_ID_COLNO].type = LOM_LONG_VAR;
	lomSysCounter[LOM_SYSCOUNTER_ID_COLNO].length = LOM_LONG_SIZE_VAR;

	lomSysCounter[LOM_SYSCOUNTER_VALUE_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
	lomSysCounter[LOM_SYSCOUNTER_VALUE_COLNO].type = LOM_LONG_VAR;
	lomSysCounter[LOM_SYSCOUNTER_VALUE_COLNO].length = LOM_LONG_SIZE_VAR;

	idesc.indexType = SM_INDEXTYPE_BTREE;
	idesc.kinfo.btree.flag = KEYFLAG_CLUSTERING;
	idesc.kinfo.btree.nColumns = 1;
	idesc.kinfo.btree.columns[0].colNo = LOM_SYSCOUNTER_NAME_COLNO;		
	idesc.kinfo.btree.columns[0].flag = KEYINFO_COL_ASC;		

	/* create logical id column */
	e = LRDS_CreateRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_COUNTER_CLASSNAME, &idesc, LOM_SYSCOUNTER_NUM_COLS, &lomSysCounter[0], SM_FALSE);
	if (e < 0) {
		LRDS_AbortTransaction(LOM_GET_LRDS_HANDLE(handle), &xactId);
		LRDS_Dismount(LOM_GET_LRDS_HANDLE(handle), volId);
		LOM_ERROR(handle,e);
	};

	idesc.indexType = SM_INDEXTYPE_BTREE;
	idesc.kinfo.btree.flag = KEYFLAG_UNIQUE;
	idesc.kinfo.btree.nColumns = 1;
	idesc.kinfo.btree.columns[0].colNo = LOM_SYSCOUNTER_ID_COLNO;		
	idesc.kinfo.btree.columns[0].flag = KEYINFO_COL_ASC;		

	e = LRDS_AddIndex(LOM_GET_LRDS_HANDLE(handle), volId, LOM_COUNTER_CLASSNAME, &idesc, &iid);
	if (e < 0) {
		LRDS_AbortTransaction(LOM_GET_LRDS_HANDLE(handle), &xactId);
		LRDS_Dismount(LOM_GET_LRDS_HANDLE(handle), volId);
		LOM_ERROR(handle,e);
	};

#endif

	/* 
	   added named object table 
	 */
	e = lom_CreateNamedObjectTable(handle, volId);
	if (e < 0) {
		LRDS_AbortTransaction(LOM_GET_LRDS_HANDLE(handle), &xactId);
		LRDS_Dismount(LOM_GET_LRDS_HANDLE(handle), volId);
		LOM_ERROR(handle,e);
	};

	e = lom_ODMG_Collection_CreateCollectionTable(handle, volId);
	if (e < 0) {
		LRDS_AbortTransaction(LOM_GET_LRDS_HANDLE(handle), &xactId);
		LRDS_Dismount(LOM_GET_LRDS_HANDLE(handle), volId);
		LOM_ERROR(handle,e);
	};
 
	/* 
	   create catalogs for text preference information
	*/
	e = lom_Text_CreateFilterCatalog(handle, volId);
	if (e < 0) {
		LRDS_AbortTransaction(LOM_GET_LRDS_HANDLE(handle), &xactId);
		LRDS_Dismount(LOM_GET_LRDS_HANDLE(handle), volId);
		LOM_ERROR(handle,e);
	};
	
	e = lom_Text_CreateKeywordExtractorCatalog(handle, volId);
	if (e < 0) {
		LRDS_AbortTransaction(LOM_GET_LRDS_HANDLE(handle), &xactId);
		LRDS_Dismount(LOM_GET_LRDS_HANDLE(handle), volId);
		LOM_ERROR(handle,e);
	};
	
	e = lom_Text_CreatePreferenceCatalog(handle, volId);
	if (e < 0) {
		LRDS_AbortTransaction(LOM_GET_LRDS_HANDLE(handle), &xactId);
		LRDS_Dismount(LOM_GET_LRDS_HANDLE(handle), volId);
		LOM_ERROR(handle,e);
	};
	
	e = lom_Text_CreateStemizerInfoCatalog(handle, volId);
	if (e < 0) {
		LRDS_AbortTransaction(LOM_GET_LRDS_HANDLE(handle), &xactId);
		LRDS_Dismount(LOM_GET_LRDS_HANDLE(handle), volId);
		LOM_ERROR(handle,e);
	};

	e = LOM_CreateClassCatalogsForSystemTables(handle, volId);
	if (e < 0) {
		LRDS_AbortTransaction(LOM_GET_LRDS_HANDLE(handle), &xactId);
		LRDS_Dismount(LOM_GET_LRDS_HANDLE(handle), volId);
		LOM_ERROR(handle,e);
	};

	e = LRDS_CommitTransaction(LOM_GET_LRDS_HANDLE(handle), &xactId);
	if (e < 0) {
		LRDS_AbortTransaction(LOM_GET_LRDS_HANDLE(handle), &xactId);
		LRDS_Dismount(LOM_GET_LRDS_HANDLE(handle), volId);
		LOM_ERROR(handle,e);
	};
	
	e = LRDS_Dismount(LOM_GET_LRDS_HANDLE(handle), volId);
	if (e < 0) LOM_ERROR(handle, e);

	return eNOERROR;
}


