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

#include "OOSQL_Tool.hxx"
#include "OOSQL_Error.h"
#include "OOSQL_Tool_LoadDB.hxx"
#include "DBM.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include <map>
using namespace std;

#ifdef WIN32
#define DIRECTORY_SEPARATOR "\\"
#define R_OK 04
#define F_OK 00
#else
#define DIRECTORY_SEPARATOR "/"
#endif

#undef ERROR_CHECK
#define ERROR_CHECK(handle, e, procIndex, xactId) \
do { \
if (e < 0) { \
	char errorMessage[4096]; \
	OOSQL_GetErrorName(handle, e, errorMessage, sizeof(errorMessage)); \
	printf("OOSQL ERROR(%s) : ", errorMessage); \
	OOSQL_GetErrorMessage(handle, e, errorMessage, sizeof(errorMessage)); \
	puts(errorMessage); \
	printf ("Do you want to commit transaction? [y/else] "); \
	if (getchar() == 'y') { \
		if((xactId) != NULL) (int)OOSQL_TransCommit(handle, xactId); \
		printf("\nCommit Transaction\n"); \
	} \
	else { \
		if((xactId) != NULL) (int)OOSQL_TransAbort(handle, xactId); \
		printf("\nAbort Transaction\n"); \
	} \
	(int) OOSQL_DestroySystemHandle(handle, procIndex); \
	OOSQL_ERR_EXIT(e); \
}\
} while(0);

#define LOM_CHECK_ERROR(handle, e) \
do {\
    if ((e) < 0) LOM_ERROR(handle, e); \
} while(0);

#define LOADER_CHECK_ERROR(e) \
do {\
	if ((e) < 0) { \
		printf("Error Code: %ld (%s)\n", e, loader_Error(e)); \
		printf("File:%s Line:%ld\n", __FILE__, __LINE__); \
		if (1) return(e);  \
	} \
} while(0);

static VarArray classList;
static VarArray readData;
static Boolean isDeferredTextIndexMode;
static Boolean smallUpdateFlag;
static Boolean useBulkloadingFlag;
static Boolean useDescriptorUpdatingFlag;
static Four loader_lineNo = 0;
static Two loader_classID = -1;
static Two loader_dataNum = 0;
static Two loader_dataCount = 0;
static Four loader_instNo = NOTSPECIFIED;
static Boolean loader_mapTblCreated = (Boolean)SM_FALSE;
static LOM_IndexID loader_iid;
static readState loader_remain = CLOSED;
static Four loader_objectCount = 0;
static Boolean loader_setOpened = (Boolean)SM_FALSE;
static Four loader_cardinality = NOTSPECIFIED;
static Boolean loader_isLongLine = (Boolean)SM_FALSE;
static Four isPageRank = SM_FALSE;

Four loadPageRankFile(char* pageRankFileName, map<int, int>& pageRankMap)
{
    FILE *fp;
    struct stat st;
    int e, i = 0;
    char pageRankID[128];


    if (stat(pageRankFileName, &st) < 0) OOSQL_ERR(eUNIXSTATFAIL_UTIL);

	
    fp = Util_fopen(pageRankFileName, "r");
    if (fp == NULL) OOSQL_ERR(eUNIXFILEOPENERROR_UTIL);


    while (1)
    {
        if (Util_fgets(pageRankID, sizeof(pageRankID), fp) == NULL) break;
        pageRankMap[i] = atoi(pageRankID);
        i++;
    }

    Util_fclose(fp);

    return eNOERROR;
}


Four oosql_Tool_LoadDB(OOSQL_SystemHandle* systemHandle, Four volId, Four temporaryVolId,
                       Boolean isDeferredTextIndexModeParm, Boolean smallUpdateFlagParm, Boolean useBulkloadingParm,
                       Boolean useDescriptorUpdatingFlagParm, char* datafileName,
                       char* pageRankFile, Four pageRankMode)

{
	LOM_Handle* handle;
	Four		e;

	handle = &OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle);

	isDeferredTextIndexMode   = isDeferredTextIndexModeParm;
	smallUpdateFlag			  = smallUpdateFlagParm;
	useBulkloadingFlag		  = useBulkloadingParm;
	useDescriptorUpdatingFlag = useDescriptorUpdatingFlagParm;

	map<int, int> pageRankMap;
    char filename[MAXPATHLEN];
    char *tempDir;

    tempDir = getenv("ODYS_TEMP_PATH");
    if (tempDir == NULL) OOSQL_ERR(eTEMPDIRNOTDEFINED_UTIL);

    isPageRank = pageRankMode;
    if (isPageRank)
    {
         fprintf(stderr, "%s:%d - pageRankFile: %s, length: %d\n", __FILE__, __LINE__, pageRankFile, strlen(pageRankFile));
         sprintf(filename, "%s%s%s", tempDir, DIRECTORY_SEPARATOR, pageRankFile);
         e = loadPageRankFile(filename, pageRankMap);
         fprintf(stderr, "%s:%d - pageRankFile: %s, length: %d\n", __FILE__, __LINE__, pageRankFile, strlen(pageRankFile));
         OOSQL_CHECK_ERROR(e);
    }


	e = loader_Init(handle);
	LOADER_CHECK_ERROR(e);

	e = loader_OpenInputFile(handle, volId, temporaryVolId, datafileName, pageRankMap);
	LOADER_CHECK_ERROR(e);

	e = loader_Finalize(handle);
	LOADER_CHECK_ERROR(e);

	return eNOERROR;
}

Four loader_Init(
	LOM_Handle *handle			/* IN: LOM System Handle */
)
{
	Four e;
	Four i;

	loader_lineNo = 0;
	loader_classID = -1;
	loader_dataNum = 0;
	loader_dataCount = 0;
	loader_instNo = NOTSPECIFIED;
	loader_mapTblCreated = (Boolean)SM_FALSE;
	loader_remain = CLOSED;
	loader_objectCount = 0;
	loader_setOpened = (Boolean)SM_FALSE;
	loader_cardinality = NOTSPECIFIED;

	e = LOM_initVarArray(handle, &classList, sizeof(ClassListEntry), MAXCLASSNUM);
	LOADER_CHECK_ERROR(e);

	for (i=0; i<MAXCLASSNUM-1; i++)
	{
		LOADER_CLASSLIST[i].ocn = NOTOPENEDCLASS;
		LOADER_CLASSLIST[i].oidListFile = NULL;
	}

	return eNOERROR;
}

Four loader_Finalize(
	LOM_Handle *handle			/* IN: LOM System Handle */
)
{
	Four e;
	Four i;
	
	for (i=0; i<MAXCLASSNUM; i++) { 
		if (LOADER_CLASSLIST[i].ocn != NOTOPENEDCLASS)  {
			/* close bulk loading */
			if(useBulkloadingFlag)
			{
				e = LOM_CloseClassBulkLoad(handle, LOADER_CLASSLIST[i].bulkLoadID);
				LOM_CHECK_ERROR(handle, e);
			}

			/* close sequential scan */
#ifndef SLIMDOWN_OPENGIS
			e = GEO_CloseScan(handle, LOADER_CLASSLIST[i].scanID);
			LOM_CHECK_ERROR(handle, e);
#else
			e = LOM_CloseScan(handle, LOADER_CLASSLIST[i].scanID);
			LOM_CHECK_ERROR(handle, e);
#endif
			

			/* close class */
#ifndef SLIMDOWN_OPENGIS
			e = GEO_CloseClass(handle, LOADER_CLASSLIST[i].ocn);
			LOM_CHECK_ERROR(handle, e);
#else
			e = LOM_CloseClass(handle, LOADER_CLASSLIST[i].ocn);
			LOM_CHECK_ERROR(handle, e);
#endif
			/* closd oid list file */
			if(LOADER_CLASSLIST[i].oidListFile != NULL)
				Util_fclose(LOADER_CLASSLIST[i].oidListFile);
		}
		else break;
	}

	/* free global structure */
	e = LOM_finalVarArray(handle, &classList);
	LOADER_CHECK_ERROR(e);

	return eNOERROR;
}

Four loader_CreateMapTable(
	LOM_Handle *handle,		/* IN: LOM System Handle */
	Four volID,				/* IN: volume identifier */
	LOM_IndexID *idxID		/* OUT: index identifier */
)
{
	Four e;
	Four ocn;
	Four scanID;
	Four classID;
	AttrInfo attrInfo[3];
	LOM_IndexDesc idesc;
	LOM_IndexID iid;

	/* attribute definition for temporary relation */
	/* for class identifier */
	attrInfo[0].complexType = SM_COMPLEXTYPE_BASIC;
	attrInfo[0].type = SM_INT;
	attrInfo[0].length = SM_INT_SIZE;
	attrInfo[0].inheritedFrom = -1;
	attrInfo[0].domain = SM_INT;

	/* for instance number */
	attrInfo[1].complexType = SM_COMPLEXTYPE_BASIC;
	attrInfo[1].type = SM_INT;
	attrInfo[1].length = SM_INT_SIZE;
	attrInfo[1].inheritedFrom = -1;
	attrInfo[1].domain = SM_INT;

	/* for object identifier */
	attrInfo[2].complexType = SM_COMPLEXTYPE_BASIC;
	attrInfo[2].type = LOM_OID;
	attrInfo[2].length = LOM_OID_SIZE;
	attrInfo[2].inheritedFrom = -1;
	attrInfo[2].domain = LOM_OID;

	/* index description */
	/* index consists of class ID & instance No */
	idesc.indexType = SM_INDEXTYPE_BTREE;
	idesc.kinfo.btree.flag = KEYFLAG_UNIQUE;
	idesc.kinfo.btree.nColumns = 2;
	Four* tmp = (Four*)&idesc.kinfo.btree.columns[0].colNo;
	tmp[0] = 0;
	tmp[1] = KEYINFO_COL_ASC;
	tmp[2] = 1;
	tmp[3] = KEYINFO_COL_ASC;

	e = LOM_GetNewClassId(handle, volID, (Boolean)SM_TRUE, &classID);
	LOM_CHECK_ERROR(handle, e);

	/* create a temporary class for mapping table */
	e = LOM_CreateClass(handle, volID, TMPRELNAME, NULL, NULL, 3, attrInfo, 0, NULL, 0, NULL, (Boolean)SM_TRUE, classID);
	LOM_CHECK_ERROR(handle, e);

	/* add index for class ID & instance No */
	e = LOM_AddIndex(handle, volID, TMPRELNAME, TMPIDXNAME, &idesc, &iid);
	LOM_CHECK_ERROR(handle, e);

	/* set output argument */
	*idxID = iid;

	/* open temporary class */
	ocn = LOM_OpenTemporaryClass(handle, volID, TMPRELNAME);
	LOM_CHECK_ERROR(handle, ocn);
	LOADER_CLASSLIST[TEMPCLASS].ocn = ocn; 

	return eNOERROR;
}

Four loader_UpdateMapTable(
	LOM_Handle *handle,
	int classID, 
	int instNo, 
	OID *objID
)
{
	LOM_ColListStruct clist[3];
	OID oid;
	Four scanID; 
	Four ocn;
	Four e;
	LockParameter lockup;

	ocn = LOADER_CLASSLIST[TEMPCLASS].ocn; 

	lockup.mode = L_IX;
	lockup.duration = L_COMMIT;
	
	/* open sequential scan */
	scanID = LOM_OpenSeqScan(handle, ocn, FORWARD, 0, NULL, &lockup);
	LOM_CHECK_ERROR(handle, scanID);

	/* set collist structure */
	clist[0].colNo = 0;
	clist[0].start = ALL_VALUE;
	clist[0].dataLength = sizeof(Four);
	ASSIGN_VALUE_TO_COL_LIST(clist[0], classID, sizeof(Four));

	clist[1].colNo = 1;
	clist[1].start = ALL_VALUE;
	clist[1].dataLength = SM_INT_SIZE;
	ASSIGN_VALUE_TO_COL_LIST(clist[1], instNo, sizeof(int));

	clist[2].colNo = 2;
	clist[2].start = ALL_VALUE;
	clist[2].dataLength = SM_OID_SIZE;
	clist[2].data.oid = *objID;

	/* insert a tuple into relation */
	e = LOM_CreateObjectByColList(handle, scanID, (Boolean)SM_TRUE, 3, clist, &oid);
	LOM_CHECK_ERROR(handle, e);

	e = LOM_CloseScan(handle, scanID);
	LOM_CHECK_ERROR(handle, e);

	return eNOERROR;
}

Four loader_GetReferencedOID(
	LOM_Handle *handle,			/* IN: LOM System Handle */
	char *data, 				/* IN: string to reference object */
	Four classNo, 				/* IN: current class number */
	LOM_IndexID *iid,			/* IN: index id for temporary relation */
	OID *objId     				/* OUT: object id to be referenced */
)
{
	LOM_ColListStruct clist[1];
	Four i = 0;
	Four e;
	Four scanID;
	Four orn; 
	OID oid;
	BoundCond bound;
	LockParameter lockup;
	int classID;
	int instNo;
	char className[MAXNAMELEN];
	char instance[MAXNAMELEN];

	data++;					/* skip @ character */
	while (*data != '|') className[i++] = *data++;
	className[i] = '\0';

	for (i=0; i<=classNo; i++)
		if (!strcmp(className, LOADER_CLASSLIST[i].className)) {
			classID = i;
			break;
		}

	if (i > classNo) LOADER_CHECK_ERROR(eNOSUCHCLASS); 

	i = 0;
	data++;					/* skip | character */
	while (*data != '\0') instance[i++] = *data++;
	instance[i] = '\0'; 
	instNo = atoi(instance);

	orn = LOADER_CLASSLIST[TEMPCLASS].ocn;

	/* setup bound condition */
	bound.op = SM_EQ;
	bound.key.len = sizeof(int) * 2;
	memcpy(&(bound.key.val[0]), &classID, sizeof(int));
	memcpy(&(bound.key.val[sizeof(int)]), &instNo, sizeof(int));

	/* initialize lock */
	lockup.mode = L_IX;
	lockup.duration = L_COMMIT;

	scanID = LOM_OpenIndexScan(handle, orn, iid, &bound, &bound, 0, NULL, &lockup);
	LOM_CHECK_ERROR(handle, scanID);

	e = LOM_NextObject(handle, scanID, &oid, NULL);
	LOM_CHECK_ERROR(handle, e);

	/* later I'll make my loader support the forward reference */
	/* currently forward reference is not supproted */
	if (e == EOS) LOADER_CHECK_ERROR(eNOSUCHINSTANCE); 

	/* set up column list structure */
	clist[0].colNo = 2;
	clist[0].start = ALL_VALUE;
	clist[0].dataLength = SM_OID_SIZE;

	/* fetch referenced object id */
	e = LOM_FetchObjectByColList(handle, scanID, (Boolean)SM_TRUE, &oid, 1, clist);
	LOM_CHECK_ERROR(handle, e);

	*objId = clist[0].data.oid;
#ifdef PRINTDATA
	printf("<Referenced OID = ");
	OIDPRINT(*objId);
	printf(">\n");
#endif

	e = LOM_CloseScan(handle, scanID);
	LOM_CHECK_ERROR(handle, e);

	return eNOERROR;
}

Four loader_OpenInputFile(
	LOM_Handle *handle,		/* IN: LOM System Handle */	
	Four volID, 			/* IN: volume identifier */
	Four temporaryVolId,		/* IN: temporary volume id */
	char *filename   		/* IN: input file name */		
	, map<int, int>& pageRankMap
)
{
	FILE *fp;
	Four e;

	e = LOM_initVarArray(handle, &readData, sizeof(char), MAXPOOLLEN); 
	LOADER_CHECK_ERROR(e);

	/* open data file */
	if((fp = Util_fopen(filename, "r")) == NULL)  {
		fprintf(stderr, "can't open %s\n", filename);
		exit(1);
	}

	while ((e = loader_ReadNextLine(handle, volID, temporaryVolId, fp, pageRankMap)) != EOS)
	{
		/* increase line number */
		if (!loader_isLongLine) loader_lineNo++;

		if (e < 0)  {
			printf("Line number: %ld\n", loader_lineNo);
			LOADER_CHECK_ERROR(e);
		}
	}

	e = LOM_finalVarArray(handle, &readData);
	LOADER_CHECK_ERROR(e);

	Util_fclose(fp);

	return eNOERROR;
}

Four loader_ReadNextLine(
	LOM_Handle *handle,		/* IN: LOM System Handle */
	Four volID, 			/* IN: volume identifier */
	Four temporaryVolId,	/* IN: temporary volume id */
	FILE *fp 			    /* IN: file pointer */
	, map<int, int>& pageRankMap
)
{
	OID oid;
	char lineBuffer[MAXLINELEN];
	char command[MAXLINELEN];
	char data[MAXLINELEN];
	readState curState;
	readState prevState;
	char ch;
	Four i;
	Four e;
	Four idx = 0;
	Four stringLength;

	if (loader_ReadLine(lineBuffer, MAXLINELEN, fp) == EOS)  {
		/* when '}' is not found in input file */
		if (loader_setOpened) return eUNDEFINEDSYNTAX;
		/* when insufficient data exist at the end of file */
		if (loader_dataCount > 0) return eINSUFFICIENTDATA;

		printf("Total %ld objects are inserted.\n", loader_objectCount);
		return EOS;
	}

#if COMMENT
	/* skip comment line */
	if (lineBuffer[0] == '-' && lineBuffer[1] == '-')
		return eNOERROR;
#endif

	/* skip empty line */
	if (lineBuffer[0] == '\n' || lineBuffer[0] == '\0')
		return eNOERROR;

	if (lineBuffer[0] == '%')  
	{
		/* command line */
		for (i=0; lineBuffer[i] != '\0'; i++)
		{
			if (!isspace(lineBuffer[i])) command[i] = lineBuffer[i];
			else break;
		}

		command[i] = '\0';

		/* skip white space between command and operand */
		while (isspace(lineBuffer[i])) i++;

		/* currently only %class and %index command is supported */
		if (!strcmp(command, "%class"))  {
			loader_classID++;
			loader_dataNum = loader_GenerateClassInfo(handle, volID, temporaryVolId, loader_classID, lineBuffer+i);
			LOADER_CHECK_ERROR(loader_dataNum);
			loader_dataCount = 0;
		}
		else if (!strcmp(command, "%index")) {
			e = loader_SetTextIndexMode(lineBuffer+i);
			LOADER_CHECK_ERROR(e);
		}

		return eNOERROR;
	}
	else  
	{
		/* data line */
		curState = loader_remain;

		if (!strncmp(lineBuffer, "NULL", 4) && curState == CLOSED)
		{
			e = loader_AccumReadData(handle, data, SM_TRUE, loader_classID, loader_dataCount, NOTSPECIFIED);
			LOADER_CHECK_ERROR(e);

			loader_dataCount++;
			if (loader_dataNum == loader_dataCount)  
			{
				e = loader_InsertObject(handle, loader_classID, loader_dataCount, &oid, pageRankMap);
				LOADER_CHECK_ERROR(e);
				/* when instance no is specified */
				if (loader_instNo >= 0)  
				{
					e = loader_UpdateMapTable(handle, loader_classID, loader_instNo, &oid);
					LOADER_CHECK_ERROR(e);
				}
				loader_dataCount = 0;
				loader_objectCount ++;

				/* Print the current object number */
   				if ((loader_objectCount%500) == 0)
				{
					printf("%ld objects are inserted.\n", loader_objectCount);
					fflush(stdout);
				}

				loader_instNo = NOTSPECIFIED;
			}

			return eNOERROR;
		}

		stringLength = strlen(lineBuffer);
		for (i = 0; i < stringLength; i++)  
		{
			ch = lineBuffer[i];

			if (ch == '{' && curState == CLOSED)  {
				if (loader_setOpened) return eUNDEFINEDSYNTAX;
				else loader_setOpened = (Boolean)SM_TRUE;
				LOADER_COLLIST(loader_classID)[loader_dataCount].nElements = 0;
				loader_cardinality = 0;
				e = Util_initVarArray(LOM_GET_LRDS_HANDLE(handle), &LOADER_ELEMSIZE(loader_classID,loader_dataCount), sizeof(Four), MAXCARDINALITY);
				LOADER_CHECK_ERROR(e);
				continue;
			}
			if (ch == '}' && curState == CLOSED)  {
				if (!loader_setOpened) return eUNDEFINEDSYNTAX;
				else
				{
					loader_setOpened = (Boolean)SM_FALSE;
					if (loader_cardinality == 0)
					{
						LOADER_COLLIST(loader_classID)[loader_dataCount].start  = LOADER_COLLIST(loader_classID)[loader_dataCount-1].start+LOADER_COLLIST(loader_classID)[loader_dataCount-1].length;
						LOADER_COLLIST(loader_classID)[loader_dataCount].length = 0;
					}
				}
				loader_cardinality = NOTSPECIFIED;
				loader_dataCount++;
				if (loader_dataNum == loader_dataCount)  {
					e = loader_InsertObject(handle, loader_classID, loader_dataCount, &oid, pageRankMap);
					LOADER_CHECK_ERROR(e);
					/* when instance no is specified */
					if (loader_instNo >= 0)  
					{
						e = loader_UpdateMapTable(handle, loader_classID, loader_instNo, &oid);
						LOADER_CHECK_ERROR(e);
					}
					loader_dataCount = 0;
					loader_objectCount ++;

					/* Print the current object number */
					if ((loader_objectCount%500) == 0)
						printf("%ld objects are inserted.\n", loader_objectCount);

					loader_instNo = NOTSPECIFIED;
				}
				continue;
			}

			/* skip white space */
			if (isspace(ch) && curState == CLOSED) continue;
			/* skip colon */
			if (ch == ':' && curState == CLOSED) continue;
			/* skip backslash */
			if (ch == '\\' && curState != BACKSLASH)  
			{
				if (curState == OPEN || curState == CLOSED)
					return eUNDEFINEDSYNTAX;
				else  
				{
					if (lineBuffer[i+1] == 'n')  
					{
						data[idx++] = '\n';
						data[idx] = '\0';
						e = loader_AccumReadData(handle, data, SM_FALSE, loader_classID, loader_dataCount, loader_cardinality);
						LOADER_CHECK_ERROR(e);
						loader_remain = curState;
						idx = 0;
						i++;				
						continue;		
					}
					else if (lineBuffer[i+1] != '\n')  
					{
						/* to use " or ' character */
						prevState = curState;
						curState = BACKSLASH;
						continue;
					}
					else  
					{
						/* to seperate data to many lines */
						data[idx] = '\0';
						e = loader_AccumReadData(handle, data, SM_FALSE, loader_classID, loader_dataCount, loader_cardinality);
						LOADER_CHECK_ERROR(e);
						loader_remain = curState;
						idx = 0;

						return eNOERROR;
					}
				}
			}

			/* put a charcter followed by backslash */
			/* ignore if there comes " or ' */
			if (curState == BACKSLASH)  
			{
				data[idx++] = ch;
				curState = prevState;
				continue;
			}

			if (ch == '\'' || ch == '\"')  
			{
				if (ch == '\'')
				{
					if (curState == CLOSED) curState = SINGLE_QUOTE;
					else if (curState == SINGLE_QUOTE)  
					{
						data[idx] = '\0';
						idx = 0;
						e = loader_AccumReadData(handle, data, SM_FALSE, loader_classID, loader_dataCount, loader_cardinality);
						LOADER_CHECK_ERROR(e)
						curState = loader_remain = CLOSED;

						if (!loader_setOpened) loader_dataCount++;
						else loader_cardinality++;

						if (loader_dataNum == loader_dataCount)  {
							e = loader_InsertObject(handle, loader_classID, loader_dataCount, &oid, pageRankMap);
							LOADER_CHECK_ERROR(e);
							/* when instance no is specified */
							if (loader_instNo >= 0)  
							{
								e = loader_UpdateMapTable(handle, loader_classID, loader_instNo, &oid);
								LOADER_CHECK_ERROR(e);
							}
							loader_dataCount = 0;
							loader_objectCount ++;

							/* Print the current object number */
        					if ((loader_objectCount%500) == 0)
							{
								printf("%ld objects are inserted.\n", loader_objectCount);
								fflush(stdout);
							}

							loader_instNo = NOTSPECIFIED;
						}
					}
					else return eUNDEFINEDSYNTAX;
				}
				else 
				{
					if (curState == CLOSED) curState = DOUBLE_QUOTE;
					else if (curState == DOUBLE_QUOTE)  
					{
						data[idx] = '\0';
						idx = 0;
						e = loader_AccumReadData(handle, data, SM_FALSE, loader_classID, loader_dataCount, loader_cardinality);
						LOADER_CHECK_ERROR(e);
						curState = loader_remain = CLOSED;

						if (!loader_setOpened) loader_dataCount++;
						else loader_cardinality++;

						if (loader_dataNum == loader_dataCount)  {
							e = loader_InsertObject(handle, loader_classID, loader_dataCount, &oid, pageRankMap);
							LOADER_CHECK_ERROR(e);

							/* when instance no is specified */
							if (loader_instNo >= 0)  
							{
								e = loader_UpdateMapTable(handle, loader_classID, loader_instNo, &oid);
								LOADER_CHECK_ERROR(e);
							}
							loader_dataCount = 0;
							loader_objectCount ++;

							/* Print the current object number */
					        if ((loader_objectCount%500) == 0)
							{
								printf("%ld objects are inserted.\n", loader_objectCount);
								fflush(stdout);
							}

							loader_instNo = NOTSPECIFIED;
						}
					}
					else return eUNDEFINEDSYNTAX;
				}
				/* read next character */
				continue;
			}

			if ((isdigit(ch) || ch == '+' || ch == '-') && curState == CLOSED) curState = OPEN;
			else if (ch == '@' && curState == CLOSED) curState = OPEN; 
			else if (isspace(ch) && curState == OPEN)  
			{
				data[idx] = '\0';
				idx = 0;
				if (data[0] == '@')  
				{
					/* when reference other object */
					e = loader_GetReferencedOID(handle, data, loader_classID, &loader_iid, &oid);
					LOADER_CHECK_ERROR(e);

					e = loader_AccumReadData(handle, (char *)&oid, SM_FALSE, loader_classID, loader_dataCount, loader_cardinality);
					LOADER_CHECK_ERROR(e);
				}
				else 
				{
					/* ordinary numercial data */
					e = loader_AccumReadData(handle, data, SM_FALSE, loader_classID, loader_dataCount, loader_cardinality);
					LOADER_CHECK_ERROR(e);
				}
				curState = CLOSED;

				if (!loader_setOpened) loader_dataCount++;
				else loader_cardinality++;

				if (loader_dataNum == loader_dataCount)  {
					e = loader_InsertObject(handle, loader_classID, loader_dataCount, &oid, pageRankMap);
					LOADER_CHECK_ERROR(e);
					/* when instance no is specified */
					if (loader_instNo >= 0)  {
						e = loader_UpdateMapTable(handle, loader_classID, loader_instNo, &oid);
						LOADER_CHECK_ERROR(e);
					}
					loader_dataCount = 0;
					loader_objectCount ++;

					/* Print the current object number */
        			if ((loader_objectCount%500) == 0)
					{
						printf("%ld objects are inserted.\n", loader_objectCount);
						fflush(stdout);
					}
					loader_instNo = NOTSPECIFIED;
				}
				/* read next data */
				continue;
			}
			/* when instance no is specified */
			else if (ch == ':' && curState == OPEN)  
			{
				data[idx] = '\0';
				idx = 0;
				loader_instNo = atoi(data);
				curState = CLOSED;
				if (!loader_mapTblCreated)  
				{
					/* create temporary class */
					e = loader_CreateMapTable(handle, volID, &loader_iid);
					LOADER_CHECK_ERROR(e);
					loader_mapTblCreated = (Boolean)SM_TRUE;
				}
				continue;
			}
			else if (!(isdigit(ch) || ch == '+' || ch == '-') && curState == CLOSED)
				return eUNDEFINEDSYNTAX;
			else if (!(isdigit(ch) || ch == '.') && curState == OPEN)
				return eUNDEFINEDSYNTAX;

			data[idx++] = ch;
		}
	}

	return eNOERROR;
}

Four loader_ReadLine (
	char*	lineBuffer,				/* IN: line buffer */
	Four	bufferSize,				/* IN: line buffer size */
	FILE*	inputStream				/* IN: input file stream */
)
{
	int ch;
	int i;

	i = 0;
	while ((ch = fgetc(inputStream)) != EOF)
	{
		if (ch == '\n')
		{
			lineBuffer[i++] = '\n';
			lineBuffer[i] = '\0';
			loader_isLongLine = (Boolean)SM_FALSE;
			return eNOERROR;
		}
		else if (ch == 0)
		{
			printf("Warning: Line %ld may contain non-printing characters. ", loader_lineNo);
			printf("( Important: Make sure to remove all non-printing characters. )\n");
			continue;
		}

		lineBuffer[i++] = (char)ch;

        if (i == bufferSize - 3)
        {
        	if (!loader_isLongLine)
        	{
				printf("Warning: Line %ld is more than 100KB. ", loader_lineNo);
				printf("( Important: Make sure to check the usefulness of data. )\n");
			}
			lineBuffer[i++] = '\\';
			lineBuffer[i++] = '\n';
			lineBuffer[i] = '\0';
			loader_isLongLine = (Boolean)SM_TRUE;
			return eNOERROR;
		}
	}

	if (i == 0)
		return EOS;
	else
	{
		lineBuffer[i] = '\0';
		return eNOERROR;
	}
}

Four loader_GenerateClassInfo(
	LOM_Handle *handle,				/* IN: LOM System Handle */
	Four volID, 					/* IN: volume identifier */
	Four temporaryVolId,			/* IN: temporary volume id */
	Two classNo,   					/* IN: class number */
	char *lineBuf    				/* IN: line buffer */
)
{
	char className[MAXNAMELEN];
	Four classID;
	Four idxForClassInfo;
	Four ocn;
	Four scanID;
	Four attrNum;
	Four dataNum;
	Four e;
	Four v;
	Four i = 0, j;
	LockParameter lockup;
	catalog_SysClassesOverlay *ptrToSysClasses;
	catalog_SysAttributesOverlay *ptrToSysAttributes;
	Four bulkLoadID;

	/* extract class name */
	for (;!isspace(lineBuf[i]) && lineBuf[i] != '(';i++)
		className[i] = lineBuf[i];

	className[i] = '\0';

	e = LOM_GetClassID(handle, volID, className, &classID);
	if (e == eNOSUCHCLASS_LOM) LOADER_CHECK_ERROR(eNOSUCHCLASS); 

	/* store class name */
	strcpy(LOADER_CLASSLIST[classNo].className, className);
	LOADER_CLASSLIST[classNo].classID = classID;

	/* store class type */
	ocn = LOM_OpenClass(handle, volID, className);
	LOM_CHECK_ERROR(handle, ocn);
	LOADER_CLASSLIST[classNo].ocn = ocn;

	/* open sequential scan */
	lockup.mode = L_X;
	lockup.duration = L_COMMIT;
	scanID = LOM_OpenSeqScan(handle, ocn, FORWARD, 0, NULL, &lockup);
	LOM_CHECK_ERROR(handle, scanID);
	LOADER_CLASSLIST[classNo].scanID = scanID;

	e = Catalog_GetClassInfo(handle, volID, classID, &idxForClassInfo);
	LOM_CHECK_ERROR(handle, e);

	v = Catalog_GetVolIndex(handle, volID);
	LOM_CHECK_ERROR(handle, v);

	/* set physical pointer to in-memory catalog */
	ptrToSysClasses = &CATALOG_GET_CLASSINFOTBL(handle, v)[idxForClassInfo];
	ptrToSysAttributes = &CATALOG_GET_ATTRINFOTBL(handle, v)[CATALOG_GET_ATTRINFOTBL_INDEX(ptrToSysClasses)];

	attrNum = CATALOG_GET_ATTRNUM(ptrToSysClasses);
	LOADER_CLASSLIST[classNo].attrNum = attrNum;
	LOADER_CLASSLIST[classNo].colNoToCollistMapMaden = SM_FALSE;
	LOADER_CLASSLIST[classNo].colNoToDataCollistMapMaden = SM_FALSE;

	e = LOM_initVarArray(handle, &LOADER_CLASSLIST[classNo].colList, sizeof(ColListEntry), MAXATTRNUM);
	LOADER_CHECK_ERROR(e);

	e = LOM_initVarArray(handle, &LOADER_CLASSLIST[classNo].attrTypeInfo, sizeof(AttrTypeInfoEntry), MAXATTRNUM);
	LOADER_CHECK_ERROR(e);

	e = loader_MakeColInfo(classNo, ptrToSysAttributes, attrNum, lineBuf+i);
	LOADER_CHECK_ERROR(e);
	dataNum = e;

	Boolean textTypeExistFlag    = SM_FALSE;
	Boolean complexTypeExistFlag = SM_FALSE;
	for(i = 0; i < dataNum; i++)
	{
		if(LOADER_COLLIST(classNo)[i].type == LOM_TEXT)
			textTypeExistFlag = SM_TRUE;
		if(LOADER_COLLIST(classNo)[i].complexType != LOM_COMPLEXTYPE_BASIC)
			complexTypeExistFlag = SM_TRUE;
	}
	
	if(useBulkloadingFlag && (isDeferredTextIndexMode || !textTypeExistFlag) && !complexTypeExistFlag)
		useBulkloadingFlag = SM_TRUE;
	else
		useBulkloadingFlag = SM_FALSE;

	/* open bulk loading */
	if(useBulkloadingFlag)
	{
		if(!smallUpdateFlag)
			bulkLoadID = LOM_OpenClassBulkLoad(handle, volID, temporaryVolId, className, SM_FALSE, SM_TRUE, 100, 100, &lockup);
		else
			bulkLoadID = LOM_OpenClassBulkLoad(handle, volID, temporaryVolId, className, SM_FALSE, SM_FALSE, 100, 100, &lockup);
		LOM_CHECK_ERROR(handle, bulkLoadID);
		LOADER_CLASSLIST[classNo].bulkLoadID = bulkLoadID;
	}
	else
		LOADER_CLASSLIST[classNo].bulkLoadID = -1;

	return e;
}

Four loader_SetTextIndexMode(
	char *lineBuf    				/* IN: line buffer */
)
{
	char indexMode[MAXNAMELEN];
	int i = 0;

	/* extract text index mode */
	while (!isspace(lineBuf[i]))
	{
		indexMode[i] = lineBuf[i];
		i++;
	}

	indexMode[i] = '\0';

	if (!strcmp(indexMode, "deferred")) isDeferredTextIndexMode = (Boolean)SM_TRUE;
	else if (!strcmp(indexMode, "immediate")) isDeferredTextIndexMode = (Boolean)SM_FALSE;
	else return eBADTEXTINDEXMODE_LOADER;

	return eNOERROR;
}


/* need more consideration */
Four loader_MakeColInfo(
	Two classNo, 
    catalog_SysAttributesOverlay *ptrToSysAttributes,
    Two attrNum,
    char *lineBuf
)
{
	Four idx;
	Four e;
	Two attrType;
	Two i;
	Two count=0;
	char attrName[MAXNAMELEN];
	Boolean spatialAttributeFlag;

	for(i = 1; i < attrNum; i++)
	{
		LOADER_ATTRTYPEINFO(classNo)[i - 1].complexType = CATALOG_GET_ATTRCOMPLEXTYPE(&ptrToSysAttributes[i]);
		LOADER_ATTRTYPEINFO(classNo)[i - 1].type        = CATALOG_GET_ATTRTYPE(&ptrToSysAttributes[i]);
	}

	while (isspace(*lineBuf) || *lineBuf == '(') lineBuf++;

	while (*lineBuf != '\0')  {
		idx = 0;

		/* extract attribute name */
		while (!isspace(*lineBuf) && *lineBuf != ')')
			attrName[idx++] = *lineBuf++;
		attrName[idx] = '\0';

		for (i=0; i<attrNum; i++)
			if (!strcmp(attrName, CATALOG_GET_ATTRNAME(&ptrToSysAttributes[i]))) 
				break;

		if (i == attrNum)
			LOADER_CHECK_ERROR(eNOSUCHATTRIBUTE);

		LOADER_COLLIST(classNo)[count].colNo       = GET_USERLEVEL_COLNO(i);
		LOADER_COLLIST(classNo)[count].complexType = CATALOG_GET_ATTRCOMPLEXTYPE(&ptrToSysAttributes[i]);
		LOADER_COLLIST(classNo)[count].size        = CATALOG_GET_ATTRLENGTH(&ptrToSysAttributes[i]);
		LOADER_COLLIST(classNo)[count].type        = attrType = CATALOG_GET_ATTRTYPE(&ptrToSysAttributes[i]);
		count ++;

		/* skip white space to next attribute name */
		lineBuf++;
		while (isspace(*lineBuf) || *lineBuf == ')') lineBuf++;
	}

	return count;
}

Two  accumReadData_prevColNo = -1;
Four accumReadData_prevElementNo = -1;
void loader_AccumReadData_SetNew(void)
{
	accumReadData_prevColNo     = -1;
	accumReadData_prevElementNo = -1;
}

Four loader_AccumReadData(
    LOM_Handle* handle,         /* IN: LOM system handle */
	char *data, 				/* IN: read data from file */
	Boolean isNull,				/* IN: null flag */
	Two classNo, 				/* IN: current class number */
	Two colNo,					/* IN: current column number */
	Four elementNo				/* IN: current element number in set */
)
/* side effect: store data to LOADER_POOL global structure */
{
	Four offset;
	short shortVal;
	int intVal;
	long longVal;
	float floatVal;
	double doubleVal;
	Four i;
	Two complexType;				
	Two attrType;
	Four e;

	complexType = LOADER_COLLIST(classNo)[colNo].complexType;
	attrType = LOADER_COLLIST(classNo)[colNo].type;

	/* compute offset for current attribute */
	if (colNo == 0) offset = 0;
	else 
		offset = LOADER_COLLIST(classNo)[colNo-1].start+LOADER_COLLIST(classNo)[colNo-1].length;

	/* if this coluum is different from previous column */
	if (colNo != accumReadData_prevColNo)  {
		LOADER_COLLIST(classNo)[colNo].start = offset;
		LOADER_COLLIST(classNo)[colNo].length = 0;
	}
	else if (colNo == accumReadData_prevColNo) {
		/* if there are string inserted yet, add the length to offset */
		offset += LOADER_COLLIST(classNo)[colNo].length;
	}

	/* if memory is not enougth, double the size of allocated memory */
	if (offset + SM_OID_SIZE + strlen(data) >= readData.nEntries)
	{
		e = LOM_doublesizeVarArray(handle, &readData, sizeof(char));
		LOADER_CHECK_ERROR(e);
	}

	/* set null flag to FALSE */
	LOADER_COLLIST(classNo)[colNo].isNull = isNull;
	if (isNull) data[0] = '\0';

	/* set loader_cardinality of the set using element no. */
	LOADER_COLLIST(classNo)[colNo].nElements = elementNo + 1;

	if (complexType == SM_COMPLEXTYPE_BASIC)
	{
		/*
		** store bit stream of non-complex type into load buffer.
		*/

		switch(attrType)
		{
		case SM_SHORT:
			shortVal = (short)atoi(data);
			memcpy(LOADER_DATAPOOL+offset, (char*)&shortVal, sizeof(short));
			LOADER_COLLIST(classNo)[colNo].length = sizeof(short);
			break;
		case SM_INT:
			intVal = atoi(data);
			memcpy(LOADER_DATAPOOL+offset, (char*)&intVal, sizeof(int));
			LOADER_COLLIST(classNo)[colNo].length = sizeof(int);
			break;
		case SM_LONG:
			longVal = atol(data);
			memcpy(LOADER_DATAPOOL+offset, (char*)&longVal, sizeof(Four_Invariable));
			LOADER_COLLIST(classNo)[colNo].length = sizeof(Four_Invariable);
			break;
		case SM_LONG_LONG:
			longVal = atoll(data);
			memcpy(LOADER_DATAPOOL+offset, (char*)&longVal, sizeof(Eight_Invariable));
			LOADER_COLLIST(classNo)[colNo].length = sizeof(Eight_Invariable);
			break;
		case SM_FLOAT:
			floatVal = (float)atof(data);
			memcpy(LOADER_DATAPOOL+offset, (char*)&floatVal, sizeof(float));
			LOADER_COLLIST(classNo)[colNo].length = sizeof(float);
			break;
		case SM_DOUBLE:
			doubleVal = atof(data);
			memcpy(LOADER_DATAPOOL+offset, (char*)&doubleVal, sizeof(double));
			LOADER_COLLIST(classNo)[colNo].length = sizeof(double);
			break;
		/* string type */
		case SM_VARSTRING:
		case SM_STRING:
			memcpy(LOADER_DATAPOOL+offset, data, strlen(data));
			LOADER_COLLIST(classNo)[colNo].length += strlen(data);
			break;
		/* OID type */
		case SM_OID:
			memcpy(LOADER_DATAPOOL+offset, data, SM_OID_SIZE);
			LOADER_COLLIST(classNo)[colNo].length = SM_OID_SIZE;
			break;
		/* text type */
		case LOM_TEXT:
			memcpy(LOADER_DATAPOOL+offset, data, strlen(data));
			LOADER_COLLIST(classNo)[colNo].length += strlen(data);
			break;
		/* time type */
		case LOM_TIME:
		case LOM_TIMESTAMP:
		case LOM_DATE:
			memcpy(LOADER_DATAPOOL+offset, data, strlen(data));
			LOADER_COLLIST(classNo)[colNo].length = strlen(data);
			break;
		default: break;
		}
	}
	else
	{
		/* 
		** store bit stream of complex type into load buffer.
		*/

		/* if memory is not enougth, double the size of allocated memory */
		if (LOADER_COLLIST(classNo)[colNo].nElements >= LOADER_ELEMSIZE(classNo,colNo).nEntries)
		{
			e = LOM_doublesizeVarArray(handle, &LOADER_ELEMSIZE(classNo,colNo), sizeof(Four));
            LOADER_CHECK_ERROR(e);
		}

		if (accumReadData_prevElementNo != elementNo) 
			LOADER_ELEMSIZEARRAY(classNo,colNo)[elementNo] = 0;

		switch(attrType)
		{
		case SM_SHORT:
			shortVal = (short)atoi(data);
			memcpy(LOADER_DATAPOOL+offset, (char*)&shortVal, sizeof(short));
			LOADER_COLLIST(classNo)[colNo].length += sizeof(short);
			LOADER_ELEMSIZEARRAY(classNo,colNo)[elementNo] = sizeof(short);
			break;
		case SM_INT:
			intVal = atoi(data);
			memcpy(LOADER_DATAPOOL+offset, (char*)&intVal, sizeof(int));
			LOADER_COLLIST(classNo)[colNo].length += sizeof(int);
			LOADER_ELEMSIZEARRAY(classNo,colNo)[elementNo] = sizeof(int);
			break;
		case SM_LONG:
			longVal = atol(data);
			memcpy(LOADER_DATAPOOL+offset, (char*)&longVal, sizeof(Four_Invariable));
			LOADER_COLLIST(classNo)[colNo].length += sizeof(Four_Invariable);
			LOADER_ELEMSIZEARRAY(classNo,colNo)[elementNo] = sizeof(Four_Invariable);
			break;
		case SM_LONG_LONG:
			longVal = atoll(data);
			memcpy(LOADER_DATAPOOL+offset, (char*)&longVal, sizeof(Eight_Invariable));
			LOADER_COLLIST(classNo)[colNo].length += sizeof(Eight_Invariable);
			LOADER_ELEMSIZEARRAY(classNo,colNo)[elementNo] = sizeof(Eight_Invariable);
			break;
		case SM_FLOAT:
			floatVal = (float)atof(data);
			memcpy(LOADER_DATAPOOL+offset, (char*)&floatVal, sizeof(float));
			LOADER_COLLIST(classNo)[colNo].length += sizeof(float);
			LOADER_ELEMSIZEARRAY(classNo,colNo)[elementNo] = sizeof(float);
			break;
		case SM_DOUBLE:
			doubleVal = atof(data);
			memcpy(LOADER_DATAPOOL+offset, (char*)&doubleVal, sizeof(double));
			LOADER_COLLIST(classNo)[colNo].length += sizeof(double);
			LOADER_ELEMSIZEARRAY(classNo,colNo)[elementNo] = sizeof(double);
			break;
		case SM_VARSTRING:
		case SM_STRING:
			memcpy(LOADER_DATAPOOL+offset, data, strlen(data));
			LOADER_COLLIST(classNo)[colNo].length += strlen(data);
			LOADER_ELEMSIZEARRAY(classNo,colNo)[elementNo] += strlen(data);
			break;
		case LOM_TIME:
			memcpy(LOADER_DATAPOOL+offset, data, strlen(data));
			LOADER_COLLIST(classNo)[colNo].length += strlen(data);
			LOADER_ELEMSIZEARRAY(classNo,colNo)[elementNo] = strlen(data);
			break;
		case LOM_DATE:
			memcpy(LOADER_DATAPOOL+offset, data, strlen(data));
			LOADER_COLLIST(classNo)[colNo].length += strlen(data);
			LOADER_ELEMSIZEARRAY(classNo,colNo)[elementNo] = strlen(data);
			break;
		default: break;
		}

		/* keep the previous element no. */
		accumReadData_prevElementNo = elementNo;
	}

	/* keep the previous column no. */
	accumReadData_prevColNo = colNo;

	return offset;
}

Four loader_InsertObject(
	LOM_Handle *handle,			/* IN: LOM System Handle */
	Two classNo,				/* IN: class number */
	Two colNum,					/* IN: column number */
	OID *oidOut					/* OUT: inserted object identifier */
	, map<int, int>& pageRankMap
)
{
	LOM_ColListStruct clist[MAXATTRNUM];
	OID oid;
	char strBuf[MAXLINELEN];
	char *ptrToBuf;
	char **valueBuf;
	char *setValueBuf;
	Four scanID;
	Four i, j;
	Four e;
	Four offset;
	Four length;
	Four size;
	Boolean isNull;
	Two attrType;
	Two complexType;
	Two colNo;
	Two idx=0;
	Two nAttrs;
	Four nElements;
	TextColStruct text;
	LOM_TextDesc textDesc;
	short shortVal;
	int intVal;
	Four longVal;
	float floatVal;
	double doubleVal;
	LOM_Date dateVar;
	LOM_Time timeVar;
	unsigned short year;
	unsigned short month;
	unsigned short day;
	unsigned short hour;
	unsigned short minute;
	unsigned short second;
	LOM_Timestamp timestampVar;
	Four logicalId;

#ifdef PRINTDATA
	puts("--------------------------------------------------------------------------------");
#endif

	scanID = LOADER_CLASSLIST[classNo].scanID;

	valueBuf = (char **)malloc(sizeof(char *) * colNum);

	for (i=0; i<colNum; i++)
	{
		attrType = LOADER_COLLIST(classNo)[i].type;
		complexType = LOADER_COLLIST(classNo)[i].complexType;
		colNo = LOADER_COLLIST(classNo)[i].colNo;
		offset = LOADER_COLLIST(classNo)[i].start;
		length = LOADER_COLLIST(classNo)[i].length;
		size = LOADER_COLLIST(classNo)[i].size;
		isNull = LOADER_COLLIST(classNo)[i].isNull;
		if (attrType == SM_STRING)
		{
			if(size <= 4096)
				valueBuf[i] = (char *)malloc(sizeof(char) * 4096);
			else
				valueBuf[i] = (char *)malloc(sizeof(char) * size);
		}
		else if (attrType == SM_VARSTRING)
		{
			if(length <= 4096)
				valueBuf[i] = (char *)malloc(sizeof(char) * 4096);
			else
				valueBuf[i] = (char *)malloc(sizeof(char) * length);
		}

		if (complexType == SM_COMPLEXTYPE_BASIC)
		{
			switch (attrType)
			{
			case SM_SHORT:
				memcpy((char *)&shortVal, LOADER_DATAPOOL + offset, length);
				clist[idx].nullFlag = isNull;
				clist[idx].start = ALL_VALUE;
				clist[idx].length = ALL_VALUE;
				clist[idx].dataLength = length;
				clist[idx].colNo = colNo;
				ASSIGN_VALUE_TO_COL_LIST(clist[idx], shortVal, length);
#ifdef PRINTDATA
				printf("%ld\n", shortVal);
#endif
				idx++;
				break;
			case SM_INT:
				memcpy((char *)&intVal, LOADER_DATAPOOL + offset, length);
				clist[idx].nullFlag = isNull;
				clist[idx].start = ALL_VALUE;
				clist[idx].length = ALL_VALUE;
				clist[idx].dataLength = length;
				clist[idx].colNo = colNo;
				ASSIGN_VALUE_TO_COL_LIST(clist[idx], intVal, length);
#ifdef PRINTDATA
				printf("%ld\n", intVal);
#endif
				idx++;
				break;
			case SM_LONG:
			case SM_LONG_LONG:
				memcpy((char *)&longVal, LOADER_DATAPOOL + offset, length);
				clist[idx].nullFlag = isNull;
				clist[idx].start = ALL_VALUE;
				clist[idx].length = ALL_VALUE;
				clist[idx].dataLength = length;
				clist[idx].colNo = colNo;
				ASSIGN_VALUE_TO_COL_LIST(clist[idx], longVal, length);
#ifdef PRINTDATA
				printf("%ld\n", longVal);
#endif
				idx++;
				break;
			case SM_FLOAT:
				memcpy((char *)&floatVal, LOADER_DATAPOOL + offset, length);
				clist[idx].nullFlag = isNull;
				clist[idx].start = ALL_VALUE;
				clist[idx].length = ALL_VALUE;
				clist[idx].dataLength = length;
				clist[idx].colNo = colNo;
				clist[idx].data.f = floatVal;
#ifdef PRINTDATA
				printf("%f\n", floatVal);
#endif
				idx++;
				break;
			case SM_DOUBLE:
				memcpy((char *)&doubleVal, LOADER_DATAPOOL + offset, length);
				clist[idx].nullFlag = isNull;
				clist[idx].start = ALL_VALUE;
				clist[idx].length = ALL_VALUE;
				clist[idx].dataLength = length;
				clist[idx].colNo = colNo;
				clist[idx].data.d = doubleVal;
#ifdef PRINTDATA
				printf("%f\n", doubleVal);
#endif
				idx++;
				break;
			case SM_STRING:
				clist[idx].nullFlag = isNull;
				clist[idx].start = ALL_VALUE;
				clist[idx].length = ALL_VALUE;
				clist[idx].dataLength = size;
				clist[idx].colNo = colNo;

				memset(valueBuf[i], 0, size);
				memcpy(valueBuf[i], LOADER_DATAPOOL+offset, length);

				clist[idx].data.ptr = valueBuf[i];
#ifdef PRINTDATA
				memset(strBuf, 0, MAXLINELEN);
				memcpy(strBuf, LOADER_DATAPOOL+offset, length);
				printf("%s\n", strBuf);
#endif
				idx++;
				break;
			case SM_VARSTRING:
				clist[idx].nullFlag = isNull;
				clist[idx].start = ALL_VALUE;
				clist[idx].length = ALL_VALUE;
				clist[idx].dataLength = length;
				clist[idx].colNo = colNo;

				memset(valueBuf[i], 0, length);
				memcpy(valueBuf[i], LOADER_DATAPOOL+offset, length);

				clist[idx].data.ptr = valueBuf[i];
#ifdef PRINTDATA
				memset(strBuf, 0, MAXLINELEN);
				memcpy(strBuf, LOADER_DATAPOOL+offset, length);
				printf("%s\n", strBuf);
#endif
				idx++;
				break;
			case SM_OID:
				memcpy((char *)&oid, LOADER_DATAPOOL+offset, length);
				clist[idx].nullFlag = isNull;
				clist[idx].start = ALL_VALUE;
				clist[idx].length = ALL_VALUE;
				clist[idx].dataLength = length;
				clist[idx].colNo = colNo;
				clist[idx].data.oid = oid;
#ifdef PRINTDATA
				OIDPRINT(oid);
				putchar('\n');
#endif
				idx++;
				break;
			case LOM_DATE:
				memcpy(strBuf, LOADER_DATAPOOL+offset, length);
				clist[idx].nullFlag = isNull;
				strBuf[length] = '\0';
				clist[idx].start = ALL_VALUE;
				clist[idx].length = LOM_DATE_SIZE;
				clist[idx].dataLength = LOM_DATE_SIZE;
				clist[idx].colNo = colNo;
				if(!isNull)
				{
					e = loader_GetDateValue(strBuf, &year, &month, &day);
					LOADER_CHECK_ERROR(e);
					LOM_SetDate(handle, year, month, day, &dateVar);
					clist[idx].data.date = dateVar;
				}
				idx++;
				break;
			case LOM_TIME:
				memcpy(strBuf, LOADER_DATAPOOL+offset, length);
				strBuf[length] = '\0';
				clist[idx].nullFlag = isNull;
				clist[idx].start = ALL_VALUE;
				clist[idx].length = LOM_TIME_SIZE;
				clist[idx].dataLength = LOM_TIME_SIZE;
				clist[idx].colNo = colNo;
				
				if(!isNull)
				{
					e = loader_GetTimeValue(strBuf, &hour, &minute, &second);
					LOADER_CHECK_ERROR(e);

					timeVar._tzHour   = 0;
					timeVar._tzMinute = 0;
					timeVar._100thSec = 0;
					timeVar._Hour     = hour;
					timeVar._Minute   = minute;
					timeVar._Second   = second;

					/* need the function to set time with given parameter */
					clist[idx].data.time = timeVar;
				}
				idx ++;
				break;
			case LOM_TIMESTAMP:
				memcpy(strBuf, LOADER_DATAPOOL+offset, length);
				strBuf[length] = '\0';
				clist[idx].nullFlag = isNull;
				clist[idx].start = ALL_VALUE;
				clist[idx].length = LOM_TIMESTAMP_SIZE;
				clist[idx].dataLength = LOM_TIMESTAMP_SIZE;
				clist[idx].colNo = colNo;

				if(!isNull)
				{
					for (j = 0; j < length; j++)
						if (isspace(strBuf[j])) break;

					strBuf[j] = '\0';
					ptrToBuf = (char *)&(strBuf[j+1]);

					e = loader_GetTimeValue(strBuf, &hour, &minute, &second);
					LOADER_CHECK_ERROR(e);

					/* set value of time part */
					timestampVar.t._tzHour   = 0;
					timestampVar.t._tzMinute = 0;
					timestampVar.t._100thSec = 0;
					timestampVar.t._Hour     = hour;
					timestampVar.t._Minute   = minute;
					timestampVar.t._Second   = second;

					e = loader_GetDateValue(ptrToBuf, &year, &month, &day);
					LOADER_CHECK_ERROR(e);

					/* set value of date part */
					LOM_SetDate(handle, year, month, day, &(timestampVar.d));

					/* set timestamp type value */
					clist[idx].data.timestamp = timestampVar;
				}
				idx++;
				break;
		 	default: 
				break;
			}
		}

	}

	/* number of non-text attribute */
	nAttrs = idx;

	if(useBulkloadingFlag)
	{
		/* bulk loading */
		Four				start, end;
		Boolean				varstringExist;
		LOM_ColListStruct	bulkloadingClist[MAXATTRNUM];
		Four				clistIndex;
		Four				dataClistIndex;

		start			= -1;
		end				= 0;
		varstringExist	= SM_FALSE;

		if(LOADER_CLASSLIST[classNo].colNoToCollistMapMaden == SM_FALSE)
		{
			for(i = 0; i < LOADER_CLASSLIST[classNo].attrNum - 1; i++)
			{
				for(j = 0; j < colNum; j++)
				{
					if(i == LOADER_COLLIST(classNo)[j].colNo)
						break;
				}
				if(j == colNum)
					LOADER_CLASSLIST[classNo].colNoToCollistMap[i] = NIL;
				else
					LOADER_CLASSLIST[classNo].colNoToCollistMap[i] = j;
			}
			LOADER_CLASSLIST[classNo].colNoToCollistMapMaden = SM_TRUE;
		}
		if(LOADER_CLASSLIST[classNo].colNoToDataCollistMapMaden == SM_FALSE)
		{
			for(i = 0; i < LOADER_CLASSLIST[classNo].attrNum - 1; i++)
			{
				for(j = 0; j < colNum; j++)
				{
					if(i == clist[j].colNo)
						break;
				}
				if(j == colNum)
					LOADER_CLASSLIST[classNo].colNoToDataCollistMap[i] = NIL;
				else
					LOADER_CLASSLIST[classNo].colNoToDataCollistMap[i] = j;
			}
			LOADER_CLASSLIST[classNo].colNoToDataCollistMapMaden = SM_TRUE;
		}

        Four pageRankId = pageRankMap[loader_dataCount];

        if (isPageRank)
            LOM_SetUserGivenLogicalID_BulkLoad(handle, LOADER_CLASSLIST[classNo].bulkLoadID, pageRankMap[loader_objectCount]);

		for(i = 0; i < LOADER_CLASSLIST[classNo].attrNum - 1; i++)
		{
			clistIndex     = LOADER_CLASSLIST[classNo].colNoToCollistMap[i];
			dataClistIndex = LOADER_CLASSLIST[classNo].colNoToDataCollistMap[i];
			if(clistIndex == -1)
			{
				bulkloadingClist[i].nullFlag = SM_TRUE;
				bulkloadingClist[i].colNo    = i;

				attrType    = LOADER_ATTRTYPEINFO(classNo)[i].type;
				complexType = LOADER_ATTRTYPEINFO(classNo)[i].complexType;
				colNo       = i;
				offset      = 0;
				length      = 0;
			}
			else
			{
				if(dataClistIndex != -1)
					bulkloadingClist[i] = clist[dataClistIndex];

				attrType    = LOADER_COLLIST(classNo)[clistIndex].type;
				complexType = LOADER_COLLIST(classNo)[clistIndex].complexType;
				colNo       = LOADER_COLLIST(classNo)[clistIndex].colNo;
				offset      = LOADER_COLLIST(classNo)[clistIndex].start;
				length      = LOADER_COLLIST(classNo)[clistIndex].length;
			}

			if(complexType == LOM_COMPLEXTYPE_BASIC && attrType != LOM_TEXT && attrType != LOM_VARSTRING)
			{
				end = i;
				if(start == -1)
					start = i;
			}
			else
			{
				if(start != -1 && end >= start)
				{
					e = LOM_NextClassBulkLoad(handle, LOADER_CLASSLIST[classNo].bulkLoadID, end - start + 1, 
						                      &bulkloadingClist[start], SM_FALSE, NULL, NULL);
					LOM_CHECK_ERROR(handle, e);

					start = -1;
				}

				if(attrType == LOM_TEXT)
				{
#ifdef PRINTDATA
					memcpy(strBuf, LOADER_DATAPOOL+offset, length);
					printf("%s\n", strBuf);
#endif
					MAKE_NULLTEXTDESC(textDesc);
					text.dataLength = length;
					text.data = LOADER_DATAPOOL + offset;

					if(useDescriptorUpdatingFlag)
					{
						SET_ISINDEXED_TEXTDESC(textDesc);
						SET_HASBEENINDEXED_TEXTDESC(textDesc);
					}

					if (isDeferredTextIndexMode) text.indexMode = LOM_DEFERRED_MODE;
					else text.indexMode = LOM_IMMEDIATE_MODE;

					if (length >= 0)
					{
						e = LOM_Text_CreateContentBulkLoad(handle, LOADER_CLASSLIST[classNo].bulkLoadID, colNo,
														   &text, &textDesc, SM_FALSE, NULL, NULL);
						LOM_CHECK_ERROR(handle, e);
					}
				}
				else if((complexType == LOM_COMPLEXTYPE_BASIC && attrType == LOM_VARSTRING) || complexType != LOM_COMPLEXTYPE_BASIC)
				{
					varstringExist = SM_TRUE;
				}
			}
		}

		if(!varstringExist)
		{
			if(start >= LOADER_CLASSLIST[classNo].attrNum - 1 || start == -1)
			{
				e = LOM_NextClassBulkLoad(handle, LOADER_CLASSLIST[classNo].bulkLoadID, 0, NULL, SM_TRUE, &logicalId, &oid);
				LOM_CHECK_ERROR(handle, e);
			}
			else
			{
				if(end >= start && start >= 0)
				{
					e = LOM_NextClassBulkLoad(handle, LOADER_CLASSLIST[classNo].bulkLoadID, end - start + 1, 
						                      &bulkloadingClist[start], SM_TRUE, &logicalId, &oid);
					LOM_CHECK_ERROR(handle, e);
				}
				else
				{
					e = LOM_NextClassBulkLoad(handle, LOADER_CLASSLIST[classNo].bulkLoadID, 0, NULL, SM_TRUE, &logicalId, &oid);
					LOM_CHECK_ERROR(handle, e);
				}
			}
		}
		else 
		{
			if(start < LOADER_CLASSLIST[classNo].attrNum - 1 && start >= 0 && end >= start)
			{
				e = LOM_NextClassBulkLoad(handle, LOADER_CLASSLIST[classNo].bulkLoadID, end - start + 1, 
					                      &bulkloadingClist[start], SM_FALSE, NULL, NULL);
				LOM_CHECK_ERROR(handle, e);
			}

			for(i = 0; i < LOADER_CLASSLIST[classNo].attrNum - 1; i++)
			{
				clistIndex     = LOADER_CLASSLIST[classNo].colNoToCollistMap[i];
				dataClistIndex = LOADER_CLASSLIST[classNo].colNoToDataCollistMap[i];
				if(clistIndex == -1)
				{
					bulkloadingClist[i].nullFlag = SM_TRUE;
					bulkloadingClist[i].colNo    = i;

					attrType    = LOADER_ATTRTYPEINFO(classNo)[i].type;
					complexType = LOADER_ATTRTYPEINFO(classNo)[i].complexType;
					colNo       = i;
					offset      = 0;
					length      = 0;
				}
				else
				{
					if(dataClistIndex != -1)
						bulkloadingClist[i] = clist[dataClistIndex];

					attrType    = LOADER_COLLIST(classNo)[clistIndex].type;
					complexType = LOADER_COLLIST(classNo)[clistIndex].complexType;
					colNo       = LOADER_COLLIST(classNo)[clistIndex].colNo;
					offset      = LOADER_COLLIST(classNo)[clistIndex].start;
					length      = LOADER_COLLIST(classNo)[clistIndex].length;
				}

				if(attrType == LOM_VARSTRING && complexType == LOM_COMPLEXTYPE_BASIC)
				{
					e = LOM_NextClassBulkLoad(handle, LOADER_CLASSLIST[classNo].bulkLoadID, 1, 
						                      &bulkloadingClist[i], SM_FALSE, NULL, NULL);
					LOM_CHECK_ERROR(handle, e);
				}
				else if(complexType != LOM_COMPLEXTYPE_BASIC)
				{
					setValueBuf = (char *)malloc(length);
					memcpy(setValueBuf, LOADER_DATAPOOL + offset, length);

					/* 
					** create complex type column
					*/
					switch (complexType)
					{
					case SM_COMPLEXTYPE_COLLECTIONSET:
						break;
					case SM_COMPLEXTYPE_COLLECTIONBAG:
						break;
					case SM_COMPLEXTYPE_COLLECTIONLIST:
						break;
					default:
						break;
					}

					free(setValueBuf);
				}
			}

			e = LOM_NextClassBulkLoad(handle, LOADER_CLASSLIST[classNo].bulkLoadID, 0, NULL, SM_TRUE, &logicalId, &oid);
			LOM_CHECK_ERROR(handle, e);
		}
	}
	else
	{
		if (nAttrs == 0)  {
			e = LOM_CreateObjectByColList(handle, scanID, (Boolean)SM_TRUE, 0, NULL, &oid);
			LOM_CHECK_ERROR(handle, e);
		}
		else  {
			{
				e = LOM_CreateObjectByColList(handle, scanID, (Boolean)SM_TRUE, nAttrs, clist, &oid);
				LOM_CHECK_ERROR(handle, e);
			}
		}

		/* text type */
		for (i = 0; i < colNum; i++)  
		{
			attrType = LOADER_COLLIST(classNo)[i].type;
			colNo = LOADER_COLLIST(classNo)[i].colNo;
			offset = LOADER_COLLIST(classNo)[i].start;
			length = LOADER_COLLIST(classNo)[i].length;

			if (attrType == LOM_TEXT)  {
#ifdef PRINTDATA
				memset(strBuf, 0, MAXLINELEN);
				memcpy(strBuf, LOADER_DATAPOOL+offset, length);
				printf("%s\n", strBuf);
#endif
				MAKE_NULLTEXTDESC(textDesc);
				text.dataLength = length;
				text.data = LOADER_DATAPOOL + offset;

				if (isDeferredTextIndexMode) text.indexMode = LOM_DEFERRED_MODE;
				else text.indexMode = LOM_IMMEDIATE_MODE;

				if (length > 0)
				{
					e = LOM_Text_CreateContent(handle, scanID, (Boolean)SM_TRUE, &oid, colNo, &text, &textDesc);
					LOM_CHECK_ERROR(handle, e);
				}
				idx++;
			}
		}

		/* 
			complex type loading
		*/
		for (i=0; i<colNum; i++)
		{
			attrType = LOADER_COLLIST(classNo)[i].type;
			complexType = LOADER_COLLIST(classNo)[i].complexType;
			colNo = LOADER_COLLIST(classNo)[i].colNo;
			offset = LOADER_COLLIST(classNo)[i].start;
			length = LOADER_COLLIST(classNo)[i].length;
			nElements = LOADER_COLLIST(classNo)[i].nElements;

			setValueBuf = (char *)malloc(length);
			memcpy(setValueBuf, LOADER_DATAPOOL + offset, length);

			/* 
			** create complex type column
			*/
			switch (complexType)
			{
			case SM_COMPLEXTYPE_COLLECTIONSET:
				e = LOM_CollectionSet_Create(handle, scanID, (Boolean)SM_TRUE, (TupleID*)&oid, colNo, ALL_VALUE);
				LOM_CHECK_ERROR(handle, e);

				e = LOM_CollectionSet_InsertElements(handle, scanID, (Boolean)SM_TRUE, (TupleID*)&oid, colNo, nElements, LOADER_ELEMSIZEARRAY(classNo,colNo), setValueBuf);
				LOM_CHECK_ERROR(handle, e);

				break;
			case SM_COMPLEXTYPE_COLLECTIONBAG:
				e = LOM_CollectionBag_Create(handle, scanID, (Boolean)SM_TRUE, (TupleID*)&oid, colNo, ALL_VALUE);
				LOM_CHECK_ERROR(handle, e);

				e = LOM_CollectionBag_InsertElements(handle, scanID, (Boolean)SM_TRUE, (TupleID*)&oid, colNo, nElements, LOADER_ELEMSIZEARRAY(classNo,colNo), setValueBuf);
				LOM_CHECK_ERROR(handle, e);

				break;
			case SM_COMPLEXTYPE_COLLECTIONLIST:
				e = LOM_CollectionList_Create(handle, scanID, (Boolean)SM_TRUE, (TupleID*)&oid, colNo);
				LOM_CHECK_ERROR(handle, e);

				e = LOM_CollectionList_AssignElements(handle, scanID, (Boolean)SM_TRUE, (TupleID*)&oid, colNo, nElements, LOADER_ELEMSIZEARRAY(classNo,colNo), setValueBuf);
				LOM_CHECK_ERROR(handle, e);

				break;
			default:
				break;
			}

			free(setValueBuf);
		}

		logicalId = lom_Text_GetLogicalId(handle, LOADER_CLASSLIST[classNo].ocn, SM_FALSE, &oid);
	}

	/* set output argument */
	*oidOut = oid;

	if (LOADER_CLASSLIST[classNo].oidListFile == NULL)
	{
		char filename[MAXPATHLEN];
		char *tmpDir;

		tmpDir = getenv("ODYS_TEMP_PATH");
		if (tmpDir == NULL)
			OOSQL_ERR(eTEMPDIRNOTDEFINED_UTIL)

		sprintf(filename, "%s%sTEXT_%s_OID", tmpDir, DIRECTORY_SEPARATOR, LOADER_CLASSLIST[classNo].className);
		LOADER_CLASSLIST[classNo].oidListFile = Util_fopen(filename, "wb");
		if (LOADER_CLASSLIST[classNo].oidListFile == NULL)
			OOSQL_ERR(eUNIXFILEOPENERROR_UTIL)
	}

	e = Util_fwrite(&logicalId, sizeof(Four), 1, LOADER_CLASSLIST[classNo].oidListFile);
	if (e == 0) OOSQL_ERR(eUNIXFILEWRITEERROR_UTIL)

	e = Util_fwrite(&oid, sizeof(OID), 1, LOADER_CLASSLIST[classNo].oidListFile);
	if (e == 0) OOSQL_ERR(eUNIXFILEWRITEERROR_UTIL)

	for (i=0; i<colNum; i++)
	{
		attrType = LOADER_COLLIST(classNo)[i].type;
		if (attrType == SM_STRING || attrType == SM_VARSTRING)
			free(valueBuf[i]);
	}

	free(valueBuf);

	loader_AccumReadData_SetNew();

	return eNOERROR;
}

Four loader_GetDateValue(
	char *strBuf,
	unsigned short *year,
	unsigned short *month,
	unsigned short *day
)
{
	Four i;
	Four j;
	Four k;
	char temp[10];
	Two itemCount;

	j = 0;
	for (i=j, k=0; i<strlen(strBuf); i++, k++)  
		if (strBuf[i] != '/') temp[k] = strBuf[i];
		else break;

	temp[i] = '\0';
	*month = atoi(temp);

	if (*month < 1 || *month > 12) {
		printf("The value for month is invalid : %ld\n", *month);
		return eBADPARAMETER_LOADER;
	}

	j = i;
	for (i=j+1, k=0; i<strlen(strBuf); i++, k++)  
		if (strBuf[i] != '/') temp[k] = strBuf[i];
		else break;

	temp[k] = '\0';
	*day = atoi(temp);

	if (*day < 1 || *day > 31) {
		printf("The value for day is invalid : %ld\n", *day);
		return eBADPARAMETER_LOADER;
	}

	j = i;
	for (i=j+1, k=0; i<strlen(strBuf); i++, k++)  
		temp[k] = strBuf[i];

	temp[k] = '\0';
	*year = atoi(temp);

	if (*year < 0 || *year > 9999) {
		printf("The value for year is invalid : %ld\n", *year);
		return eBADPARAMETER_LOADER;
	}

	return eNOERROR;
}

Four loader_GetTimeValue(
	char *strBuf,
	unsigned short *hour,
	unsigned short *minute,
	unsigned short *second
)
{
	Four i;
	Four j;
	Four k;
	char temp[10];
	Two itemCount;

	j = 0;
	for (i=j, k=0; i<strlen(strBuf); i++, k++)  
		if (strBuf[i] != ':') temp[k] = strBuf[i];
		else break;

	temp[i] = '\0';
	*hour = atoi(temp);

	if (*hour < 0 || *hour > 24)
	{
		printf("The value for hour is invalid : %ld\n", *hour);
		return eBADPARAMETER_LOADER;
	}

	j = i;
	for (i=j+1, k=0; i<strlen(strBuf); i++, k++)  
		if (strBuf[i] != ':') temp[k] = strBuf[i];
		else break;

	temp[k] = '\0';
	*minute = atoi(temp);

	if (*minute < 0 || *minute > 60) {
		printf("The value for minute is invalid : %ld\n", *minute);
		return eBADPARAMETER_LOADER;
	}

	j = i;
	for (i=j+1, k=0; i<strlen(strBuf); i++, k++)  
		temp[k] = strBuf[i];

	temp[k] = '\0';
	*second = atoi(temp);

	if (*second < 0 || *second > 60) {
		printf("The value for second is invalid : %ld\n", *second);
		return eBADPARAMETER_LOADER;
	}

	return eNOERROR;
}

char *loader_Error(int ecode)
{
	switch (ecode)  {
		case eUNDEFINEDSYNTAX:
			return ("Invalid syntax in input data file");
			break;
		case eNOSUCHCLASS:
			return ("There exists no such a class in schema");
			break;
		case eNOSUCHATTRIBUTE:
			return ("There exists no such an attribute in schema");
			break;
		case eNOSUCHINSTANCE:
			return ("Instance number has not been specified before");
			break;
		case eNOTENOUGHMEMORY:
			return ("Not enough memory to store data");
			break;
		case eBADTEXTINDEXMODE_LOADER:
			return ("Bad text index mode is specified");
			break;
		case eCOLLECTIONSET_ELEMENTEXIST_LRDS:
			return ("There is already same element in set");
			break;
		case eNODISKSPACE_RDSM:
			return ("There is no disk space to load data");
			break;
		case eSPATIALATTRMUSTBEFIRST:
			return ("Spatial Attribute must be first attribute");
			break;
		case eTOOLARGEPOINTSAREUSED:
			return ("Too many points are used in polygon or polyline object");
			break;
		case eINSUFFICIENTDATA:
			return ("Insufficient data exist at the end of input file");
			break;
		default:
			return ("Unknown error in loader");
			break;
	}
}

Four loader_SetSpatialObject(LOM_Handle* handle, One classType, Four ocnOrScanId, Boolean useScanFlag, Four lineSegOcnOrScanId, Boolean lineSegUseScanFlag, OID* oid, char* pointString)
{
	return eNOERROR;
}

char* loader_GetPointFromPointString(char* pString, Four& x, Four& y)
{
	Four i;

	if(sscanf(pString, "%ld %ld", &x, &y) != 2)
		return NULL;

	for(i = 0; i < 2; i++)
	{
		// skip white space
		while(*pString && isspace(*pString)) pString ++;

		// skip element
		while(*pString && !isspace(*pString)) pString ++;

		// skip white space
		while(*pString && isspace(*pString)) pString ++;
	}

	return pString;
}
