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

#include "OOSQL_StorageSystemHeaders.h"
#include "OOSQL_APIs_Internal.hxx"
#include "OOSQL_Error.h"
#include "OOSQL_Tool.hxx"
#include "OOSQL_Tool_MergePosting.hxx"
#include "DBM.h"

#include <string>
#include <vector>
#include <map>

using namespace std;

typedef struct {
	Four docLogicalID;
	OID  oid;
} MappingTable;

MappingTable *mappingTable = NULL;
Four numOfObjs = 0;



Four loadOIDs(char* mappingFileName)
{
    FILE *fp;
	struct stat st;
	int e;

	if (stat(mappingFileName, &st) < 0) OOSQL_ERR(eUNIXSTATFAIL_UTIL);

    mappingTable = (MappingTable*)malloc(st.st_size);
    if (mappingTable == NULL) OOSQL_ERR(eOUTOFMEMORY_UTIL);

	numOfObjs = st.st_size/sizeof(MappingTable);

	fp = Util_fopen(mappingFileName, "rb");
    if (fp == NULL) OOSQL_ERR(eUNIXFILEOPENERROR_UTIL);

	e = Util_fread(mappingTable, sizeof(MappingTable), numOfObjs, fp);
	if (e == 0 && ferror(fp)) OOSQL_ERR(eUNIXFILEREADERROR_UTIL);

	Util_fclose(fp);

	return eNOERROR;
}

Four getOID
(
    Four pseudoDocID,
    OID  *oid,
	Four *docLogicalID
)
{
#ifdef  OOSQL_DEBUG
    if (oid == NULL) OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

    if (pseudoDocID >= numOfObjs || pseudoDocID < 0)
	    OOSQL_ERR(eNOMAPPINGEXIST_UTIL);

    memcpy(oid, &mappingTable[pseudoDocID].oid, sizeof(OID));
	*docLogicalID = mappingTable[pseudoDocID].docLogicalID;

    return eNOERROR;
}

Four loadPageRankFile(map<int, int>& pageRankMap, char* pageRankFileName)
{
    FILE *fp;
	struct stat st;
	int e, i;
	char pageRankID[128];


	if (stat(pageRankFileName, &st) < 0) OOSQL_ERR(eUNIXSTATFAIL_UTIL);


	fp = Util_fopen(pageRankFileName, "r");
    if (fp == NULL) OOSQL_ERR(eUNIXFILEOPENERROR_UTIL);


	for (i = 0; i < numOfObjs; i++)
	{
		Util_fgets(pageRankID, sizeof(pageRankID), fp);
		pageRankMap[atoi(pageRankID)] = i;
	}

	Util_fclose(fp);

	return eNOERROR;
}


Four is_whitespace(char ch)
{
	if(ch == ' ' || ch == '\t' || ch == '\n')
		return 1;
	else
		return 0;
}

Four oosql_Tool_MapPosting(
	OOSQL_SystemHandle*	systemHandle,
	Four 				volId,
	char* 				className,
	char* 				attrName,
	Four				nPostingFiles,
	char** 				postingFileNames,
	char* 				newPostingFileName,
	char* 				oidFileName,
	Four				sortMergeMode,
	char*				pageRankFile,		
	Four				pageRankMode		
)
{
	LOM_Handle*	handle;
    Four e;
	char *tempDir;
	char keyword[LOM_MAXKEYWORDSIZE];
	Eight count;
	Four pseudoDocID;
	Four docLogicalID;
	Four pageRankID;			
	OID  oid;
	lom_PostingBuffer postingBuffer;
    FILE *fp, *fp2;
#ifdef USE_SUBINDEX
	Four postingLengthFieldSize = LOM_LONG_SIZE_VAR;
#else
    Four postingLengthFieldSize = 0;
#endif
	Four							ocn, colNo, i, j;
    catalog_SysClassesOverlay		*ptrToSysClasses;
    catalog_SysAttributesOverlay	*ptrToSysAttributes;
	Four							v;
	Four							idxForClassInfo;
	lom_Text_PostingInfoForReading	postingInfo;
	map<int, int> pageRankMap;	


	handle = &OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle);

    /* check parameters */
    if (volId < 0) OOSQL_ERR(eBADPARAMETER_LOM);
    if (className == NULL) OOSQL_ERR(eBADPARAMETER_LOM);
	if (nPostingFiles < 1) OOSQL_ERR(eBADPARAMETER_LOM);
	if (sortMergeMode < 0 || sortMergeMode > 2) OOSQL_ERR(eBADPARAMETER_LOM);
	if (pageRankMode == SM_TRUE && pageRankFile == NULL) OOSQL_ERR(eBADPARAMETER_LOM);	


    /* open class */
    ocn = LOM_OpenClass(handle, volId, className);
    OOSQL_CHECK_ERROR(ocn);

	/* get class information */
    e = Catalog_GetClassInfo(handle, volId, LOM_USEROPENCLASSTABLE(handle)[ocn].classID, &idxForClassInfo);
    OOSQL_CHECK_ERROR(e);

    v = Catalog_GetVolIndex(handle, volId);
    OOSQL_CHECK_ERROR(v);

    /* set memory pointer */
    ptrToSysClasses    = &CATALOG_GET_CLASSINFOTBL(handle, v)[idxForClassInfo];
    ptrToSysAttributes = &CATALOG_GET_ATTRINFOTBL(handle, v)[CATALOG_GET_ATTRINFOTBL_INDEX(ptrToSysClasses)];

    for(i = 0, colNo = -1; i < CATALOG_GET_ATTRNUM(ptrToSysClasses); i++) { 
        if (CATALOG_GET_ATTRTYPE(&ptrToSysAttributes[i]) != LOM_TEXT)
            continue;
        if (strcmp(CATALOG_GET_ATTRNAME(&ptrToSysAttributes[i]), attrName))
            continue;

        colNo = i;

        break;
    }

	if (colNo == -1)
		OOSQL_ERR(eNOSUCHATTRIBUTE_UTIL);

	/* posting infos for reading temporary file */
	e = lom_Text_PreparePostingInfoForReading(handle, volId,
											  LOM_USEROPENCLASSTABLE(handle)[ocn].classID,
											  colNo, &postingInfo);
	if(e < 0) LOM_ERROR(handle, e);

    e = LOM_AllocPostingBuffer(handle, &postingBuffer, LOM_DEFAULTPOSTINGBUFFERSIZE);
    OOSQL_CHECK_ERROR(e);
    LOM_FREEOFFSET_POSTINGBUFFER(postingBuffer) = 0;

    tempDir = getenv("ODYS_TEMP_PATH");
	if (tempDir == NULL) OOSQL_ERR(eTEMPDIRNOTDEFINED_UTIL);

    /* load mapping file to memory */
	char filename[MAXPATHLEN];
	sprintf(filename, "%s%s%s", tempDir, DIRECTORY_SEPARATOR, oidFileName);
    e = loadOIDs(filename);
    OOSQL_CHECK_ERROR(e);


	/* load pageRank file to memory */
	if (pageRankMode)
	{
		char filename[MAXPATHLEN];
		sprintf(filename, "%s%s%s", tempDir, DIRECTORY_SEPARATOR, pageRankFile);
		e = loadPageRankFile(pageRankMap, filename);
		OOSQL_CHECK_ERROR(e);
	}

	/* open output file */
	sprintf(filename, "%s%s%s", tempDir, DIRECTORY_SEPARATOR, newPostingFileName);
	e = lom_Text_OpenTempFile(handle, filename, "w", &fp2);
	OOSQL_CHECK_ERROR(e);

	count = 0;

	if (nPostingFiles == 1 || (sortMergeMode == NOMERGE || sortMergeMode == UNSORTEDMERGE))
	{
		char*	stringBuffer;
		Four	bufferSize = 1024000;
		char   	readBuffer[1024];
		char*  	stringPtr;
		Four   	stringLength;
		
		stringBuffer = (char *)malloc(bufferSize);

		for (i = 0; i < nPostingFiles; i++)
		{
		    /* add inverted index entry from temproary posting file */
			sprintf(filename, "%s%s%s", tempDir, DIRECTORY_SEPARATOR, postingFileNames[i]);
		    if (access(filename, R_OK) == 0)
		    {
		        e = lom_Text_OpenTempFile(handle, filename, "r", &fp);
		        OOSQL_CHECK_ERROR(e);
		
		        while (1)
		        {
					e = 0;
					strcpy(stringBuffer, "");
					while(1)
					{
						if(Util_fgets(readBuffer, sizeof(readBuffer), fp) == NULL)
						{
							strcpy(readBuffer, "");
							if(!strcmp(stringBuffer,""))
							{
								e = EOS;
								break;
							}
						}
						
						if ((strlen(stringBuffer) + sizeof(readBuffer)) > bufferSize)
						{
							bufferSize *= 2;
							stringBuffer = (char *)realloc(stringBuffer, bufferSize);
						}		
						strcat(stringBuffer, readBuffer);						
		
						if(readBuffer[strlen(readBuffer) - 1] == '\n' || strlen(readBuffer) < sizeof(readBuffer) - 1)
							break;
					}
					if(e == EOS) break;
		
					// parsing stringBuffer
					stringPtr      = (char*)stringBuffer;
					stringLength   = strlen(stringPtr);
					if(sscanf(stringPtr, "%s %ld", keyword, &pseudoDocID) != 2)
					{
						fprintf(stderr, "Skip this line due to invalid posting format. \"%s\"\n", stringPtr);	
						continue;
						OOSQL_ERR(eBADPOSTINGFORMAT_UTIL);
					}

					if (pageRankMode)
					{
						pageRankID = pseudoDocID;

						pseudoDocID = pageRankMap[pageRankID];
					}
					
					e = getOID(pseudoDocID, &oid, &docLogicalID);
			        OOSQL_CHECK_ERROR(e);
		
					Util_fprintf(fp2, "%s ", keyword);
#ifndef _LP64
					if (!pageRankMode) Util_fprintf(fp2, "%i ", docLogicalID);		
					else Util_fprintf(fp2, "%i ", pageRankID);
			        Util_fprintf(fp2, "%i %i %i %i ", oid.pageNo, oid.volNo, oid.slotNo, oid.unique);
#else
					if (!pageRankMode) Util_fprintf(fp2, "%ld ", docLogicalID);	
					else Util_fprintf(fp2, "%ld ", pageRankID);
			        Util_fprintf(fp2, "%ld %d %d %ld ", oid.pageNo, oid.volNo, oid.slotNo, oid.unique);
#endif
		
					// write rest part or posting string
					Four itemCount = 0;
					for(j = 0; j < stringLength; j++)
					{
						if(is_whitespace(stringPtr[j]))
						{
							// skip continuing white space
							for(; j < stringLength; j++)
								if(!is_whitespace(stringPtr[j]))
									break;
		
							itemCount ++;
		
							if(postingInfo.isContainingTupleID && itemCount == 6)
							{
								break;
							}
							else if(!postingInfo.isContainingTupleID && itemCount == 4)
							{
								break;
							}
						}
					}
					if(j == stringLength)
						OOSQL_ERR(eBADPOSTINGFORMAT_UTIL);
		
					Util_fprintf(fp2, "%s", &stringPtr[j]);
		
			        count++;
			        if ((count%20000) == 0)
				        printf("processing %ldth postings...\n", count);
		        }

				e = lom_Text_CloseTempFile(handle, fp);
				OOSQL_CHECK_ERROR(e);
			}
			else
				OOSQL_ERR(eUNIXFILEREADERROR_UTIL);
    	    
    	    Util_fflush(fp2);	
		}
		
		free(stringBuffer);
    }
    else
    {
		string stringBuffer;
		string lineBuffer;
		char   readBuffer[1024];
		char*  stringPtr;
		Four   stringLength;

    	MergePostingFiles*	mpf;
    	
    	/* initialize MergePostingFiles instance */
    	mpf = new MergePostingFiles(handle, nPostingFiles, postingFileNames, tempDir);
    	
    	e = mpf->m_openPostingFiles();
    	OOSQL_CHECK_ERR(e);
    	
    	while (1)
    	{
    		e = mpf->m_findMinPosting(stringBuffer);
    		if (e == EOS) break;
    		
			// parsing stringBuffer
			stringPtr      = (char*)stringBuffer.c_str();
			stringLength   = strlen(stringPtr);
			if(sscanf(stringPtr, "%s %ld", keyword, &pseudoDocID) != 2)
			{
				fprintf(stderr, "Skip this line due to invalid posting format. \"%s\"\n", stringPtr);	
				continue;
				OOSQL_ERR(eBADPOSTINGFORMAT_UTIL);
			}

			if (pageRankMode)
			{
				pageRankID = pseudoDocID;

				pseudoDocID = pageRankMap[pageRankID];
			}
					
			e = getOID(pseudoDocID, &oid, &docLogicalID);
	        OOSQL_CHECK_ERROR(e);
		
			Util_fprintf(fp2, "%s ", keyword);
#ifndef _LP64
			if (!pageRankMode) Util_fprintf(fp2, "%i ", docLogicalID);		
			else Util_fprintf(fp2, "%i ", pageRankID);
	        Util_fprintf(fp2, "%i %i %i %i ", oid.pageNo, oid.volNo, oid.slotNo, oid.unique);
#else
			if (!pageRankMode) Util_fprintf(fp2, "%ld ", docLogicalID);	
			else Util_fprintf(fp2, "%ld ", pageRankID);
	        Util_fprintf(fp2, "%ld %d %d %ld ", oid.pageNo, oid.volNo, oid.slotNo, oid.unique);
#endif
			
			// write rest part or posting string
			Four itemCount = 0;
			for(i = 0; i < stringLength; i++)
			{
				if(is_whitespace(stringPtr[i]))
				{
					// skip continuing white space
					for(; i < stringLength ;i++)
						if(!is_whitespace(stringPtr[i]))
							break;

					itemCount ++;

					if(postingInfo.isContainingTupleID && itemCount == 6)
					{
						break;
					}
					else if(!postingInfo.isContainingTupleID && itemCount == 4)
					{
						break;
					}
				}
			}
			if(i == stringLength)
				OOSQL_ERR(eBADPOSTINGFORMAT_UTIL);

			Util_fprintf(fp2, "%s", &stringPtr[i]);

	        count++;
	        if ((count%20000) == 0)
	        {
		        printf("processing %ldth postings...\n", count);
				Util_fflush(fp2);		
			}
		}

		e = mpf->m_closePostingFiles();
		OOSQL_CHECK_ERR(e);

		delete mpf;
    }

	e = lom_Text_CloseTempFile(handle, fp2);
	OOSQL_CHECK_ERROR(e);

    return eNOERROR;
}

#else /* SLIMDOWN_TEXTIR */

#include "OOSQL_StorageSystemHeaders.h"
#include "OOSQL_APIs_Internal.hxx"
#include "OOSQL_Error.h"
#include "OOSQL_Tool.hxx"
#include "OOSQL_Tool_MergePosting.hxx"

Four oosql_Tool_MapPosting(
    OOSQL_SystemHandle* systemHandle,
    Four                volId,
    char*               className,
    char*               attrName,
    Four                nPostingFiles,
    char**              postingFileNames,
    char*               newPostingFileName,
    char*               oidFileName,
    Four                sortMergeMode,
    char*               pageRankFile,     
    Four                pageRankMode        
)
{
    return eTEXTIR_NOTENABLED_OOSQL;
}


#endif /* SLIMDOWN_TEXTIR */

