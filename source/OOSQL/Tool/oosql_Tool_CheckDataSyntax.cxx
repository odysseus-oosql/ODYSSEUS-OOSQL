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
#include "OOSQL_Tool_CheckDataSyntax.hxx"
#include "DBM.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

static VarArray checker_classList;
static VarArray checker_readData;
static Two checker_classID = -1;
static Two checker_dataNum = 0;
static Two checker_dataCount = 0;
static Four checker_lineNo = 0;
static readState checker_remain = CLOSED;
static Four checker_objectCount = 0;
static Boolean checker_setOpened = (Boolean)SM_FALSE;
static Four checker_cardinality = NOTSPECIFIED;
static Two checker_prevColNo = -1;
static Four checker_prevElementNo = -1;
static Four checker_errorCount = 0;
static Boolean checker_isLongLine = (Boolean)SM_FALSE;

Four oosql_Tool_CheckDataSyntax(OOSQL_SystemHandle* systemHandle, Four volId, char* datafileName, Boolean verboseFlag)
{
	LOM_Handle* handle;
	Four		e;

	handle = &OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle);

	e = checker_Init(handle);
	OOSQL_CHECK_ERROR(e);

	e = checker_InspectSourceData(handle, volId, datafileName, verboseFlag);
	OOSQL_CHECK_ERROR(e);

	e = checker_Finalize(handle);
	OOSQL_CHECK_ERROR(e);

	return eNOERROR;
}

Four checker_Init(
	LOM_Handle *handle			/* IN: LOM System Handle */
)
{
	Four e;
	Four i;

	checker_lineNo = 0;
	checker_classID = -1;
	checker_dataNum = 0;
	checker_dataCount = 0;
	checker_remain = CLOSED;
	checker_objectCount = 0;
	checker_setOpened = (Boolean)SM_FALSE;
	checker_cardinality = NOTSPECIFIED;

	e = LOM_initVarArray(handle, &checker_classList, sizeof(ClassListEntry), MAXCLASSNUM);
	if (e < eNOERROR) LOM_ERROR(handle, e);

	for (i = 0; i < MAXCLASSNUM; i++)
		CHECKER_CLASSLIST[i].ocn = NOTOPENEDCLASS;

	return eNOERROR;
}

Four checker_Finalize(
	LOM_Handle *handle			/* IN: LOM System Handle */
)
{
	Four e;
	Four i;
	
	for (i = 0; i < MAXCLASSNUM; i++)
	{ 
		if (CHECKER_CLASSLIST[i].ocn != NOTOPENEDCLASS)
		{
			/* close sequential scan */
#ifndef SLIMDOWN_OPENGIS
			e = GEO_CloseScan(handle, CHECKER_CLASSLIST[i].scanID);
			if (e < 0) LOM_ERROR(handle, e);
#else
			e = LOM_CloseScan(handle, CHECKER_CLASSLIST[i].scanID);
			if (e < 0) LOM_ERROR(handle, e);
#endif

			/* close class */
#ifndef SLIMDOWN_OPENGIS
			e = GEO_CloseClass(handle, CHECKER_CLASSLIST[i].ocn);
			if (e < 0) LOM_ERROR(handle, e);
#else
			e = LOM_CloseClass(handle, CHECKER_CLASSLIST[i].ocn);
			if (e < 0) LOM_ERROR(handle, e);
#endif
			/* free global structure */
			e = LOM_finalVarArray(handle, &CHECKER_CLASSLIST[i].colList);
			if (e < 0) LOM_ERROR(handle, e);
		}
		else break;
	}

	/* free global structure */
	e = LOM_finalVarArray(handle, &checker_classList);
	if (e < 0) LOM_ERROR(handle, e);

	return eNOERROR;
}

Four checker_ValidateRefObject(
	LOM_Handle *handle,			/* IN: LOM System Handle */
	char *data, 				/* IN: string to reference object */
	Four classNo 				/* IN: current class number */
)
{
	Four i = 0;
	Four e;
	int classID;
	int instNo;
	char className[MAXNAMELEN];
	char instance[MAXNAMELEN];

	data++;					/* skip @ character */
	while (*data != '|') className[i++] = *data++;
	className[i] = '\0';

	for (i = 0; i <= classNo; i++)
		if (!strcmp(className, CHECKER_CLASSLIST[i].className)) {
			classID = i;
			break;
		}

	if (i > classNo)
	{
		DISPLAY_ERROR_MESSAGE(eCANNOTFINDCLASSREF_CHECKER, checker_lineNo, className);
		return eNOERROR_WITH_DISPLAY;
	}

	i = 0;
	data++;					/* skip | character */
	while (*data != '\0') instance[i++] = *data++;
	instance[i] = '\0'; 
	instNo = atoi(instance);
	
	if (instNo < 0)
	{
		DISPLAY_ERROR_MESSAGE(eINVALIDINSTANCENO_CHECKER, checker_lineNo, instance);
		return eNOERROR_WITH_DISPLAY;
	}

	return eNOERROR;
}
			
Four checker_InspectSourceData(
	LOM_Handle *handle,		/* IN: LOM System Handle */	
	Four volID, 			/* IN: volume identifier */
	char *filename,   		/* IN: input file name */		
	Boolean verboseFlag		/* IN: verbose mode flag */
)
{
	char outputFileName[MAXNAMELEN];
	FILE *fp;
	FILE *outfp;
	Four e;

	e = LOM_initVarArray(handle, &checker_readData, sizeof(char), MAXPOOLLEN); 
	if (e < 0) LOM_ERROR(handle, e);

	/* open data file */
	if((fp = Util_fopen(filename, "r")) == NULL)  {
		fprintf(stderr, "can't open %s\n", filename);
		exit(1);
	}

	if (verboseFlag)
	{
		strcpy(outputFileName, filename);
		strcat(outputFileName, ".tmp");
		
		if((outfp = Util_fopen(outputFileName, "w")) == NULL)  {
			fprintf(stderr, "can't open %s\n", outputFileName);
			exit(1);
		}
	}

	/* read data file and check syntax */
	e = checker_InspectDataFile(handle, volID, fp, verboseFlag, outfp);
	OOSQL_CHECK_ERR(e);

	if (verboseFlag) Util_fclose(outfp);
	
	/* close data file */
	Util_fclose(fp);

	e = LOM_finalVarArray(handle, &checker_readData);
	if (e < 0) LOM_ERROR(handle, e);

	return eNOERROR;
}

Four checker_InspectDataFile(
	LOM_Handle *handle,		/* IN: LOM System Handle */
	Four volID, 			/* IN: volume identifier */
	FILE *fp, 			    /* IN: file pointer */
	Boolean verboseFlag,	/* IN: verbose mode flag */
	FILE *output			/* IN: output file pointer */
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

	while (checker_ReadLine(lineBuffer, MAXLINELEN, fp) != EOS)
	{
		/* increase line number */
		if (!checker_isLongLine) checker_lineNo ++;
		
#if COMMENT
		/* skip comment line */
		if (lineBuffer[0] == '-' && lineBuffer[1] == '-')
			continue;
#endif

		/* skip empty line */
		if (lineBuffer[0] == '\n' || lineBuffer[0] == '\0')
			continue;

		if (lineBuffer[0] == '%')  
		{
			/* command line */
			for (i = 0; lineBuffer[i] != '\0'; i++)
			{
				if (!isspace(lineBuffer[i])) command[i] = lineBuffer[i];
				else break;
			}

			command[i] = '\0';

			/* skip white space between command and operand */
			while (isspace(lineBuffer[i])) i++;

			/* currently only %class and %index command is supported */
			if (!strcmp(command, "%class"))
			{
				checker_classID++;
				
				e = checker_GenerateClassInfo(handle, volID, checker_classID, lineBuffer+i, &checker_dataNum);
				if (e == eNOERROR_WITH_DISPLAY)
					return e;
				else
					OOSQL_CHECK_ERR(e);
				
				checker_dataCount = 0;
			}
			else if (!strcmp(command, "%index")) {
				/* nothing to do */
			}
			else {
				DISPLAY_ERROR_MESSAGE(eUNDEFINEDCOMMAND_CHECKER, checker_lineNo, lineBuffer);
				return eNOERROR_WITH_DISPLAY;
			}
		}
		else  
		{
			/* data line */
			curState = checker_remain;

			if (!strncmp(lineBuffer, "NULL", 4) && curState == CLOSED)
			{
				e = checker_AccumReadData(handle, data, SM_TRUE, checker_classID, checker_dataCount, NOTSPECIFIED, verboseFlag, output);
				OOSQL_CHECK_ERROR(e);

				checker_dataCount++;
				
				if (checker_dataNum == checker_dataCount)  
				{
					checker_dataCount = 0;
					checker_objectCount ++;

					RESET_DATA_ACCUMULATOR;
				}

				continue;
			}

			stringLength = strlen(lineBuffer);
			
			if (lineBuffer[stringLength-1] != '\n')
			{
				DISPLAY_ERROR_MESSAGE(eLINETRUNCATED_CHECKER, checker_lineNo, lineBuffer); 
				SKIP_TO_NEXT_DATA_RECORD(fp, lineBuffer, checker_dataCount, checker_dataNum);
				
				checker_dataCount = 0;
				checker_objectCount ++;
				curState = checker_remain = CLOSED;
				
				RESET_DATA_ACCUMULATOR;
				
				continue;
			}
			
			if ((curState == SINGLE_QUOTE || curState == DOUBLE_QUOTE) || 
			    (curState == CLOSED && (lineBuffer[0] == '\'' || lineBuffer[0] == '\"')))
			{
				
				if (!(lineBuffer[stringLength-2] == '\'' || lineBuffer[stringLength-2] == '\"') &&
				    !(lineBuffer[stringLength-3] == '\\' && lineBuffer[stringLength-2] == 'n') &&
				    !(lineBuffer[stringLength-2] == '\\'))
				{
					DISPLAY_ERROR_MESSAGE(eINCOMPLETEDATALINE_CHECKER, checker_lineNo, lineBuffer);
					
					/* display summary information */
					printf("�� %ld���� ��ü�� ���� �����͸� �˻��Ͽ����ϴ�.\n", checker_objectCount);
					if (checker_errorCount > 0)
						printf("�˻� ��� ���� ������ %ld�� �߰ߵǾ����ϴ�.\n", checker_errorCount);
					else
						printf("�˻� ��� ���� ������ �߰ߵ��� �ʾҽ��ϴ�.\n");
					fflush(stdout);
					
					return eNOERROR_WITH_DISPLAY;
				}
			}
			
			for (i = 0; i < stringLength; i++)  
			{
				ch = lineBuffer[i];

				if (ch == '{' && curState == CLOSED)
				{
					if (checker_setOpened)
					{
						DISPLAY_ERROR_MESSAGE(eSETALREADYOPENED_CHECKER, checker_lineNo, lineBuffer);
						SKIP_TO_NEXT_DATA_RECORD(fp, lineBuffer, checker_dataCount, checker_dataNum);
					
						checker_dataCount = 0;
						checker_objectCount ++;
						curState = checker_remain = CLOSED;
						
						RESET_DATA_ACCUMULATOR;
					
						break; 		/* go to next line */
					}
					else
						checker_setOpened = (Boolean)SM_TRUE;
				
					CHECKER_COLLIST(checker_classID)[checker_dataCount].nElements = 0;
					checker_cardinality = 0;
				
					e = LOM_initVarArray(handle, &CHECKER_ELEMSIZE(checker_classID,checker_dataCount), sizeof(Four), MAXCARDINALITY);
					if (e < eNOERROR) LOM_ERROR(handle, e);
				
					continue;
				}
				if (ch == '}' && curState == CLOSED)
				{
					if (!checker_setOpened)
					{
						DISPLAY_ERROR_MESSAGE(eSETALREADYCLOSED_CHECKER, checker_lineNo, lineBuffer);
						SKIP_TO_NEXT_DATA_RECORD(fp, lineBuffer, checker_dataCount, checker_dataNum);

						checker_dataCount = 0;
						checker_objectCount ++;
						curState = checker_remain = CLOSED;
						
						RESET_DATA_ACCUMULATOR;
					
						break; 		/* go to next line */
					}
					else
					{
						checker_setOpened = (Boolean)SM_FALSE;
						
						if (checker_cardinality == 0)
						{
							CHECKER_COLLIST(checker_classID)[checker_dataCount].start  = CHECKER_COLLIST(checker_classID)[checker_dataCount-1].start + CHECKER_COLLIST(checker_classID)[checker_dataCount-1].length;
							CHECKER_COLLIST(checker_classID)[checker_dataCount].length = 0;
						}
					}
					
					checker_cardinality = NOTSPECIFIED;
					checker_dataCount++;
					
					if (checker_dataNum == checker_dataCount)
					{
						checker_dataCount = 0;
						checker_objectCount ++;
						
						RESET_DATA_ACCUMULATOR;
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
					{
						DISPLAY_ERROR_MESSAGE(eINVALIDUSEOFBACKSLASH_CHECKER, checker_lineNo, lineBuffer);
						SKIP_TO_NEXT_DATA_RECORD(fp, lineBuffer, checker_dataCount, checker_dataNum);
					
						checker_dataCount = 0;
						checker_objectCount ++;
						curState = checker_remain = CLOSED;
						
						RESET_DATA_ACCUMULATOR;
					
						break; 		/* go to next line */
					}
					else  
					{
						if (lineBuffer[i+1] == 'n')  
						{
							data[idx++] = '\n';
							data[idx] = '\0';
							
							e = checker_AccumReadData(handle, data, SM_FALSE, checker_classID, checker_dataCount, checker_cardinality, verboseFlag, output);
							OOSQL_CHECK_ERROR(e);
							
							checker_remain = curState;
							idx = 0;
							
							break;		/* go to next line */
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
							
							e = checker_AccumReadData(handle, data, SM_FALSE, checker_classID, checker_dataCount, checker_cardinality, verboseFlag, output);
							OOSQL_CHECK_ERROR(e);
							
							checker_remain = curState;
							idx = 0;

							break;		/* go to next line */
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
						else if (curState == SINGLE_QUOTE && lineBuffer[i+1] == '\n')  
						{
							/* char, varchar data */
							if (CHECKER_COLLIST(checker_classID)[checker_dataCount].type != OOSQL_TYPE_CHAR &&
							    CHECKER_COLLIST(checker_classID)[checker_dataCount].type != OOSQL_TYPE_VARCHAR &&
							    CHECKER_COLLIST(checker_classID)[checker_dataCount].type != OOSQL_TYPE_TIME &&
							    CHECKER_COLLIST(checker_classID)[checker_dataCount].type != OOSQL_TYPE_DATE &&
							    CHECKER_COLLIST(checker_classID)[checker_dataCount].type != OOSQL_TYPE_TIMESTAMP)
							{
								DISPLAY_ERROR_MESSAGE(eINCOMPATIBLEATTRTYPE_CHECKER ,checker_lineNo, lineBuffer);
								SKIP_TO_NEXT_DATA_RECORD(fp, lineBuffer, checker_dataCount, checker_dataNum);
				
								checker_dataCount = 0;
								checker_objectCount ++;		
								curState = checker_remain = CLOSED;
				
								RESET_DATA_ACCUMULATOR;
				
								break; 		/* go to next line */							
							}
							
							data[idx] = '\0';
							idx = 0;
							
							e = checker_AccumReadData(handle, data, SM_FALSE, checker_classID, checker_dataCount, checker_cardinality, verboseFlag, output);
							OOSQL_CHECK_ERROR(e)
							
							curState = checker_remain = CLOSED;

							if (!checker_setOpened) checker_dataCount++;
							else checker_cardinality++;

							if (checker_dataNum == checker_dataCount)
							{
								checker_dataCount = 0;
								checker_objectCount ++;

								RESET_DATA_ACCUMULATOR;
							}
						}
						else if (curState == SINGLE_QUOTE && lineBuffer[i+1] != '\n')
						{
							DISPLAY_ERROR_MESSAGE(eSINGLEQUOTEINDATA_CHECKER, checker_lineNo, lineBuffer);
							SKIP_TO_NEXT_DATA_RECORD(fp, lineBuffer, checker_dataCount, checker_dataNum);
							
							checker_dataCount = 0;
							checker_objectCount ++;
							curState = checker_remain = CLOSED;
							
							RESET_DATA_ACCUMULATOR;
							
							break; 		/* go to next line */
						}
						else
						{
							DISPLAY_ERROR_MESSAGE(eINVALIDUSEOFSINGLEQUOTE_CHECKER ,checker_lineNo, lineBuffer);			
							SKIP_TO_NEXT_DATA_RECORD(fp, lineBuffer, checker_dataCount, checker_dataNum);
							
							checker_dataCount = 0;
							checker_objectCount ++;
							curState = checker_remain = CLOSED;
							
							RESET_DATA_ACCUMULATOR;
							
							break; 		/* go to next line */
						}
					}
					else 
					{
						if (curState == CLOSED) curState = DOUBLE_QUOTE;
						else if (curState == DOUBLE_QUOTE && lineBuffer[i+1] == '\n')  
						{
							/* text data */
							if (CHECKER_COLLIST(checker_classID)[checker_dataCount].type != OOSQL_TYPE_TEXT)
							{
								DISPLAY_ERROR_MESSAGE(eINCOMPATIBLEATTRTYPE_CHECKER ,checker_lineNo, lineBuffer);
								SKIP_TO_NEXT_DATA_RECORD(fp, lineBuffer, checker_dataCount, checker_dataNum);
				
								checker_dataCount = 0;
								checker_objectCount ++;		
								curState = checker_remain = CLOSED;
				
								RESET_DATA_ACCUMULATOR;
				
								break; 		/* go to next line */							
							}
							
							data[idx] = '\0';
							idx = 0;
							
							e = checker_AccumReadData(handle, data, SM_FALSE, checker_classID, checker_dataCount, checker_cardinality, verboseFlag, output);
							OOSQL_CHECK_ERROR(e);
							
							curState = checker_remain = CLOSED;

							if (!checker_setOpened) checker_dataCount++;
							else checker_cardinality++;

							if (checker_dataNum == checker_dataCount)
							{
								checker_dataCount = 0;
								checker_objectCount ++;

								RESET_DATA_ACCUMULATOR;
							}
						}
						else if (curState == DOUBLE_QUOTE && lineBuffer[i+1] != '\n')
						{
							DISPLAY_ERROR_MESSAGE(eDOUBLEQUOTEINDATA_CHECKER, checker_lineNo, lineBuffer);
							SKIP_TO_NEXT_DATA_RECORD(fp, lineBuffer, checker_dataCount, checker_dataNum);
							
							checker_dataCount = 0;
							checker_objectCount ++;
							curState = checker_remain = CLOSED;
							
							RESET_DATA_ACCUMULATOR;
							
							break; 		/* go to next line */
						}
						else
						{
							DISPLAY_ERROR_MESSAGE(eINVALIDUSEOFDOUBLEQUOTE_CHECKER ,checker_lineNo, lineBuffer);
							SKIP_TO_NEXT_DATA_RECORD(fp, lineBuffer, checker_dataCount, checker_dataNum);
							
							checker_dataCount = 0;
							checker_objectCount ++;
							curState = checker_remain = CLOSED;
							
							RESET_DATA_ACCUMULATOR;
							
							break; 		/* go to next line */
						}
					}
					/* read next character */
					continue;
				}

				if ((isdigit(ch) || ch == '+' || ch == '-') && curState == CLOSED) curState = OPEN;
				else if (ch == '@' && curState == CLOSED) curState = REF_OBJECT;
				else if (curState == CLOSED)
				{
					/* when leading character is not a digit, +, -, or @ */
					DISPLAY_ERROR_MESSAGE(eINVALIDNUMBERFORMAT_CHECKER ,checker_lineNo, lineBuffer);
					SKIP_TO_NEXT_DATA_RECORD(fp, lineBuffer, checker_dataCount, checker_dataNum);
				
					checker_dataCount = 0;
					checker_objectCount ++;
					curState = checker_remain = CLOSED;
				
					RESET_DATA_ACCUMULATOR;
				
					break; 		/* go to next line */
				}
				else if (isspace(ch) && (curState == OPEN || curState == REF_OBJECT))  
				{
					data[idx] = '\0';
					idx = 0;
					if (data[0] == '@')  
					{
						/* when reference other object */
						e = checker_ValidateRefObject(handle, data, checker_classID);
						OOSQL_CHECK_ERROR(e);

						e = checker_AccumReadData(handle, (char *)&oid, SM_FALSE, checker_classID, checker_dataCount, checker_cardinality, verboseFlag, output);
						OOSQL_CHECK_ERROR(e);
					}
					else 
					{
						/* ordinary numercial data */
					
						if (CHECKER_COLLIST(checker_classID)[checker_dataCount].type != OOSQL_TYPE_SHORT &&
						    CHECKER_COLLIST(checker_classID)[checker_dataCount].type != OOSQL_TYPE_INT &&
						    CHECKER_COLLIST(checker_classID)[checker_dataCount].type != OOSQL_TYPE_LONG &&
						    CHECKER_COLLIST(checker_classID)[checker_dataCount].type != OOSQL_TYPE_LONG_LONG &&
						    CHECKER_COLLIST(checker_classID)[checker_dataCount].type != OOSQL_TYPE_FLOAT &&
							CHECKER_COLLIST(checker_classID)[checker_dataCount].type != OOSQL_TYPE_DOUBLE &&
						    CHECKER_COLLIST(checker_classID)[checker_dataCount].type != OOSQL_TYPE_REAL)
						{ 
							DISPLAY_ERROR_MESSAGE(eINCOMPATIBLEATTRTYPE_CHECKER ,checker_lineNo, lineBuffer);
							SKIP_TO_NEXT_DATA_RECORD(fp, lineBuffer, checker_dataCount, checker_dataNum);
				
							checker_dataCount = 0;
							checker_objectCount ++;		
							curState = checker_remain = CLOSED;
				
							RESET_DATA_ACCUMULATOR;
				
							break; 		/* go to next line */							
						}
						
						e = checker_AccumReadData(handle, data, SM_FALSE, checker_classID, checker_dataCount, checker_cardinality, verboseFlag, output);
						OOSQL_CHECK_ERROR(e);
					}

					curState = CLOSED;

					if (!checker_setOpened) checker_dataCount++;
					else checker_cardinality++;

					if (checker_dataNum == checker_dataCount)
					{
						checker_dataCount = 0;
						checker_objectCount ++;

						RESET_DATA_ACCUMULATOR;
					}
					/* read next data */
					continue;
				}
				/* when instance no is specified */
				else if (ch == ':' && curState == OPEN)  
				{
					data[idx] = '\0';
					idx = 0;
					curState = CLOSED;
					continue;
				}
				else if (!(isdigit(ch) || ch == '.') && curState == OPEN)
				{
					DISPLAY_ERROR_MESSAGE(eINVALIDNUMBERFORMAT_CHECKER ,checker_lineNo, lineBuffer);
					SKIP_TO_NEXT_DATA_RECORD(fp, lineBuffer, checker_dataCount, checker_dataNum);
				
					checker_dataCount = 0;
					checker_objectCount ++;		
					curState = checker_remain = CLOSED;
				
					RESET_DATA_ACCUMULATOR;
				
					break; 		/* go to next line */
				}

				data[idx++] = ch;
			}
		}
	}

	/* when '}' is not found in input file */
	if (checker_setOpened)
		DISPLAY_ERROR_MESSAGE(eCANNOTCLOSESETCOLUMN_CHECKER, checker_lineNo, NULL);
		
	/* when insufficient data exist at the end of file */
	if (checker_dataCount > 0)
		DISPLAY_ERROR_MESSAGE(eINSUFFICIENTDATA_CHECKER, checker_lineNo, NULL);

	/* display summary information */
	printf("�� %ld���� ��ü�� ���� �����͸� �˻��Ͽ����ϴ�.\n", checker_objectCount);
	if (checker_errorCount > 0)
		printf("�˻� ��� ���� ������ %ld�� �߰ߵǾ����ϴ�.\n", checker_errorCount);
	else
		printf("�˻� ��� ���� ������ �߰ߵ��� �ʾҽ��ϴ�.\n");
	fflush(stdout);

	return eNOERROR;
}

Four checker_ReadLine (
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
			checker_isLongLine = (Boolean)SM_FALSE;
			return eNOERROR;
		}
		else if (ch == 0)
		{
			printf("����: %ld��° ���ο� ��Ʈ�� ĳ���Ͱ� ���ԵǾ� �ֽ��ϴ�. ", checker_lineNo);
			printf("( �߿�: ��� ��Ʈ�� ĳ���͸� ������ �ֽʽÿ�. )\n");
			continue;
		}

		lineBuffer[i++] = (char)ch;

        if (i == bufferSize - 3)
        {
        	if (!checker_isLongLine)
        	{
				printf("����: %ld��° ������ 100KB�� �ʰ��մϴ�. ", checker_lineNo);
				printf("( �߿�: �ùٸ� ���������� Ȯ���� �ֽʽÿ�. )\n");
			}
			lineBuffer[i++] = '\\';
			lineBuffer[i++] = '\n';
			lineBuffer[i] = '\0';
			checker_isLongLine = (Boolean)SM_TRUE;
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

Four checker_GenerateClassInfo(
	LOM_Handle *handle,				/* IN:  LOM System Handle */
	Four volID, 					/* IN:  volume identifier */
	Two classNo,   					/* IN:  class number */
	char *lineBuf,    				/* IN:  line buffer */
	Two *dataNum					/* OUT: specified attribute number */
)
{
	char className[MAXNAMELEN];
	Four classID;
	Four idxForClassInfo;
	Four ocn;
	Four scanID;
	Four attrNum;
	Four e;
	Four v;
	Four i = 0, j;
	LockParameter lockup;
	catalog_SysClassesOverlay *ptrToSysClasses;
	catalog_SysAttributesOverlay *ptrToSysAttributes;

	/* extract class name */
	for (; !isspace(lineBuf[i]) && lineBuf[i] != '(' && lineBuf[i] != '\0'; i++)
		className[i] = lineBuf[i];

	className[i] = '\0';

	e = LOM_GetClassID(handle, volID, className, &classID);
	if (e == eNOSUCHCLASS_LOM)
	{
		DISPLAY_ERROR_MESSAGE(eNOSUCHCLASS_CHECKER, checker_lineNo, className);
		return eNOERROR_WITH_DISPLAY;
	}
	else
		if (e < eNOERROR) LOM_ERROR(handle, e);

	/* store class name */
	strcpy(CHECKER_CLASSLIST[classNo].className, className);
	CHECKER_CLASSLIST[classNo].classID = classID;

	/* store class type */
	ocn = LOM_OpenClass(handle, volID, className);
	if (ocn < eNOERROR) LOM_ERROR(handle, ocn);	
	
	CHECKER_CLASSLIST[classNo].ocn = ocn;

	/* open sequential scan */
	lockup.mode = L_S;
	lockup.duration = L_COMMIT;
	scanID = LOM_OpenSeqScan(handle, ocn, FORWARD, 0, NULL, &lockup);
	if (scanID < eNOERROR) LOM_ERROR(handle, scanID);
	
	CHECKER_CLASSLIST[classNo].scanID = scanID;
//#endif

	e = Catalog_GetClassInfo(handle, volID, classID, &idxForClassInfo);
	if (e < eNOERROR) LOM_ERROR(handle, e);

	v = Catalog_GetVolIndex(handle, volID);
	if (v < eNOERROR) LOM_ERROR(handle, v);

	/* set physical pointer to in-memory catalog */
	ptrToSysClasses = &CATALOG_GET_CLASSINFOTBL(handle, v)[idxForClassInfo];
	ptrToSysAttributes = &CATALOG_GET_ATTRINFOTBL(handle, v)[CATALOG_GET_ATTRINFOTBL_INDEX(ptrToSysClasses)];

	CHECKER_CLASSLIST[classNo].attrNum = attrNum = CATALOG_GET_ATTRNUM(ptrToSysClasses);

	e = LOM_initVarArray(handle, &CHECKER_CLASSLIST[classNo].colList, sizeof(ColListEntry), MAXATTRNUM);
	if (e < eNOERROR) LOM_ERROR(handle, e);

	/* get the information of attributes */
	e = checker_GetColumnInfo(classNo, ptrToSysAttributes, attrNum, lineBuf+i, dataNum);
	if (e == eNOERROR_WITH_DISPLAY)
		return e;
	else
		OOSQL_CHECK_ERR(e);

	return eNOERROR;
}

Four checker_GetColumnInfo(
	Two classNo, 					/* IN:  clsas # appeared in data file */
    catalog_SysAttributesOverlay *ptrToSysAttributes,
    								/* IN:  pointer to in-memory catalog */
    Two attrNum,					/* IN:  the number of total attributes */
    char *lineBuf,					/* IN:  line containing attribute list */
    Two *dataNum					/* OUT: the number of specified attributes */
)
{
	Four idx;
	Four e;
	Two i;
	Two count = 0;
	char attrName[MAXNAMELEN];
	Boolean spatialAttributeFlag;

	while ((isspace(*lineBuf) || *lineBuf == '(') && *lineBuf != '\0') lineBuf++;

	while (*lineBuf != '\0')
	{
		idx = 0;

		/* extract attribute name */
		while (!isspace(*lineBuf) && *lineBuf != ')' && *lineBuf != '\0')
			attrName[idx++] = *lineBuf++;

		attrName[idx] = '\0';

		for (i = 0; i < attrNum; i++)
			if (!strcmp(attrName, CATALOG_GET_ATTRNAME(&ptrToSysAttributes[i]))) 
				break;

		if (i == attrNum)
		{
			DISPLAY_ERROR_MESSAGE(eNOSUCHATTRIBUTE_CHECKER, checker_lineNo, attrName);
			return eNOERROR_WITH_DISPLAY;
		}

		CHECKER_COLLIST(classNo)[count].colNo       = GET_USERLEVEL_COLNO(i);
		CHECKER_COLLIST(classNo)[count].complexType = CATALOG_GET_ATTRCOMPLEXTYPE(&ptrToSysAttributes[i]);
		CHECKER_COLLIST(classNo)[count].size        = CATALOG_GET_ATTRLENGTH(&ptrToSysAttributes[i]);
		CHECKER_COLLIST(classNo)[count].type        = CATALOG_GET_ATTRTYPE(&ptrToSysAttributes[i]);
		count ++;

		/* skip white space to next attribute name */
		lineBuf++;
		while ((isspace(*lineBuf) || *lineBuf == ')') && *lineBuf != '\0') lineBuf++;
	}

	/* set specfied attribute number */
	*dataNum = count;

	return eNOERROR;
}

Four checker_AccumReadData(
    LOM_Handle* handle,         /* IN: LOM system handle */
	char *data, 				/* IN: read data from file */
	Boolean isNull,				/* IN: null flag */
	Two classNo, 				/* IN: current class number */
	Two colNo,					/* IN: current column number */
	Four elementNo,				/* IN: current element number in set */
	Boolean verboseFlag,		/* IN: verbose mode flag */
	FILE *output				/* IN: output file pointer */
)
/* side effect: store data to CHECKER_POOL global structure */
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

	complexType = CHECKER_COLLIST(classNo)[colNo].complexType;
	attrType = CHECKER_COLLIST(classNo)[colNo].type;

	/* compute offset for current attribute */
	if (colNo == 0) offset = 0;
	else 
		offset = CHECKER_COLLIST(classNo)[colNo-1].start+CHECKER_COLLIST(classNo)[colNo-1].length;

	/* if this coluum is different from previous column */
	if (colNo != checker_prevColNo)  {
		CHECKER_COLLIST(classNo)[colNo].start = offset;
		CHECKER_COLLIST(classNo)[colNo].length = 0;
	}
	else if (colNo == checker_prevColNo) {
		/* if there are string inserted yet, add the length to offset */
		offset += CHECKER_COLLIST(classNo)[colNo].length;
	}

	/* if memory is not enougth, double the size of allocated memory */
	if (offset + SM_OID_SIZE + strlen(data) >= checker_readData.nEntries)
	{
		e = LOM_doublesizeVarArray(handle, &checker_readData, sizeof(char));
		if (e < eNOERROR) LOM_ERROR(handle, e);
	}

	/* set null flag to FALSE */
	CHECKER_COLLIST(classNo)[colNo].isNull = isNull;
	if (isNull) data[0] = '\0';

	/* set checker_cardinality of the set using element no. */
	CHECKER_COLLIST(classNo)[colNo].nElements = elementNo + 1;

	if (complexType == SM_COMPLEXTYPE_BASIC)
	{
		/*
		** store bit stream of non-complex type into load buffer.
		*/

		switch(attrType)
		{
		case SM_SHORT:
			shortVal = (short)atoi(data);
			memcpy(CHECKER_DATAPOOL+offset, (char*)&shortVal, sizeof(short));
			CHECKER_COLLIST(classNo)[colNo].length = sizeof(short);
			if (verboseFlag) Util_fprintf(output, "%ld\n", shortVal);
			break;
		case SM_INT:
			intVal = atoi(data);
			memcpy(CHECKER_DATAPOOL+offset, (char*)&intVal, sizeof(int));
			CHECKER_COLLIST(classNo)[colNo].length = sizeof(int);
			if (verboseFlag) Util_fprintf(output, "%ld\n", intVal);
			break;
		case SM_LONG:
			longVal = atol(data);
			memcpy(CHECKER_DATAPOOL+offset, (char*)&longVal, sizeof(Four_Invariable));
			CHECKER_COLLIST(classNo)[colNo].length = sizeof(Four_Invariable);
			if (verboseFlag) Util_fprintf(output, "%ld\n", longVal);
			break;
		case SM_LONG_LONG:
			longVal = atoll(data);
			memcpy(CHECKER_DATAPOOL+offset, (char*)&longVal, sizeof(Eight_Invariable));
			CHECKER_COLLIST(classNo)[colNo].length = sizeof(Eight_Invariable);
			if (verboseFlag) Util_fprintf(output, "%ld\n", longVal);
			break;
		case SM_FLOAT:
			floatVal = (float)atof(data);
			memcpy(CHECKER_DATAPOOL+offset, (char*)&floatVal, sizeof(float));
			CHECKER_COLLIST(classNo)[colNo].length = sizeof(float);
			if (verboseFlag) Util_fprintf(output, "%f\n", floatVal);
			break;
		case SM_DOUBLE:
			doubleVal = atof(data);
			memcpy(CHECKER_DATAPOOL+offset, (char*)&doubleVal, sizeof(double));
			CHECKER_COLLIST(classNo)[colNo].length = sizeof(double);
			if (verboseFlag) Util_fprintf(output, "%f\n", doubleVal);
			break;
		/* string type */
		case SM_VARSTRING:
		case SM_STRING:
			memcpy(CHECKER_DATAPOOL+offset, data, strlen(data));
			CHECKER_COLLIST(classNo)[colNo].length += strlen(data);
			if (verboseFlag) Util_fprintf(output, "%s\n", data);
			break;
		/* OID type */
		case SM_OID:
			memcpy(CHECKER_DATAPOOL+offset, data, SM_OID_SIZE);
			CHECKER_COLLIST(classNo)[colNo].length = SM_OID_SIZE;
			if (verboseFlag) Util_fprintf(output, "%s\n", data);
			break;
		/* text type */
		case LOM_TEXT:
			memcpy(CHECKER_DATAPOOL+offset, data, strlen(data));
			CHECKER_COLLIST(classNo)[colNo].length += strlen(data);
			if (verboseFlag) Util_fprintf(output, "%s\n", data);
			break;
		/* time type */
		case LOM_TIME:
		case LOM_TIMESTAMP:
		case LOM_DATE:
			memcpy(CHECKER_DATAPOOL+offset, data, strlen(data));
			CHECKER_COLLIST(classNo)[colNo].length = strlen(data);
			if (verboseFlag) Util_fprintf(output, "%s\n", data);
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
		if (CHECKER_COLLIST(classNo)[colNo].nElements >= CHECKER_ELEMSIZE(classNo,colNo).nEntries)
		{
			e = LOM_doublesizeVarArray(handle, &CHECKER_ELEMSIZE(classNo,colNo), sizeof(Four));
            if (e < eNOERROR) LOM_ERROR(handle, e);
		}

		if (checker_prevElementNo != elementNo) 
			CHECKER_ELEMSIZEARRAY(classNo,colNo)[elementNo] = 0;

		switch(attrType)
		{
		case SM_SHORT:
			shortVal = (short)atoi(data);
			memcpy(CHECKER_DATAPOOL+offset, (char*)&shortVal, sizeof(short));
			CHECKER_COLLIST(classNo)[colNo].length += sizeof(short);
			CHECKER_ELEMSIZEARRAY(classNo,colNo)[elementNo] = sizeof(short);
			break;
		case SM_INT:
			intVal = atoi(data);
			memcpy(CHECKER_DATAPOOL+offset, (char*)&intVal, sizeof(int));
			CHECKER_COLLIST(classNo)[colNo].length += sizeof(int);
			CHECKER_ELEMSIZEARRAY(classNo,colNo)[elementNo] = sizeof(int);
			break;
		case SM_LONG:
			longVal = atol(data);
			memcpy(CHECKER_DATAPOOL+offset, (char*)&longVal, sizeof(Four_Invariable));
			CHECKER_COLLIST(classNo)[colNo].length += sizeof(Four_Invariable);
			CHECKER_ELEMSIZEARRAY(classNo,colNo)[elementNo] = sizeof(Four_Invariable);
			break;
		case SM_LONG_LONG:
			longVal = atoll(data);
			memcpy(CHECKER_DATAPOOL+offset, (char*)&longVal, sizeof(Eight_Invariable));
			CHECKER_COLLIST(classNo)[colNo].length += sizeof(Eight_Invariable);
			CHECKER_ELEMSIZEARRAY(classNo,colNo)[elementNo] = sizeof(Eight_Invariable);
			break;
		case SM_FLOAT:
			floatVal = (float)atof(data);
			memcpy(CHECKER_DATAPOOL+offset, (char*)&floatVal, sizeof(float));
			CHECKER_COLLIST(classNo)[colNo].length += sizeof(float);
			CHECKER_ELEMSIZEARRAY(classNo,colNo)[elementNo] = sizeof(float);
			break;
		case SM_DOUBLE:
			doubleVal = atof(data);
			memcpy(CHECKER_DATAPOOL+offset, (char*)&doubleVal, sizeof(double));
			CHECKER_COLLIST(classNo)[colNo].length += sizeof(double);
			CHECKER_ELEMSIZEARRAY(classNo,colNo)[elementNo] = sizeof(double);
			break;
		case SM_VARSTRING:
		case SM_STRING:
			memcpy(CHECKER_DATAPOOL+offset, data, strlen(data));
			CHECKER_COLLIST(classNo)[colNo].length += strlen(data);
			CHECKER_ELEMSIZEARRAY(classNo,colNo)[elementNo] += strlen(data);
			break;
		case LOM_TIME:
			memcpy(CHECKER_DATAPOOL+offset, data, strlen(data));
			CHECKER_COLLIST(classNo)[colNo].length += strlen(data);
			CHECKER_ELEMSIZEARRAY(classNo,colNo)[elementNo] = strlen(data);
			break;
		case LOM_DATE:
			memcpy(CHECKER_DATAPOOL+offset, data, strlen(data));
			CHECKER_COLLIST(classNo)[colNo].length += strlen(data);
			CHECKER_ELEMSIZEARRAY(classNo,colNo)[elementNo] = strlen(data);
			break;
		default: break;
		}

		/* keep the previous element no. */
		checker_prevElementNo = elementNo;
	}

	/* keep the previous column no. */
	checker_prevColNo = colNo;

	return offset;
}

void checker_PrintError(
	Four errorCode,				/* IN: error code */
	Four lineNo,				/* IN: line no which an error occurred */
	char *errorData				/* IN: data which contains an error */	
)
{
	Four length = strlen(errorData);
	
	if (++ checker_errorCount > MAXALLOWEDERRORNUM)
	{
		printf("�ʹ� ���� ������ �߰ߵǾ� �˻� ������ �ߴ��մϴ�.\n");
		printf("������� �߰ߵ� ������ �����Ͻ� �� �ٽ� ������ �ֽʽÿ�.\n");
		exit(1);
	}
	
	printf("%ld��° ���ο��� ������ �߰ߵǾ����ϴ�.\n", lineNo);
		
	if (errorData[length - 1] == '\n') errorData[length - 1] = '\0';
	
	switch (errorCode)
	{
	case eCANNOTFINDCLASSREF_CHECKER:
		printf("���� ����:\n@class_ref|instance_no���� class_ref�� ����Ű�� class�� �������� �ʽ��ϴ�.\n");
		printf("\"%s\" class�� ���� data�� source file�� �ִ��� Ȯ���Ͻʽÿ�.\n", errorData);     
		printf("�ذ� ���:\nclass_ref���� ���� source file�� data�� �ִ� class �̸��� �����ּ���.\n");
		break;
	case eINVALIDINSTANCENO_CHECKER:
		printf("���� ����:\n@class_ref|instance_no���� instance_no�� �������Դϴ�.\n");
		printf("\"%s\" instance_no�� �ùٸ��� Ȯ���Ͻʽÿ�.\n", errorData);
		printf("�ذ� ���:\ninstance_no���� source file���� ������ instance ��ȣ���� ����ϼ���.\n");
		break;
	case eUNDEFINEDCOMMAND_CHECKER:
		printf("���� ����:\n");
		printf("(1) %%class, %%index �̿��� �������� �ʴ� command�� ���Ǿ����ϴ�.\n");
		printf("(2) data line�� ù ���ڰ� %% �Դϴ�.\n");
		printf("=>\n%s\n", errorData);
		printf("�ذ� ���:\n");
		printf("(1) command�� �̸��� %%class, %%index�� �����Ͻʽÿ�.\n");
		printf("(2) data line�� ù ���ڷ� %%�� ���� �ʵ��� �����Ͻʽÿ�.\n");
		break;
	case eLINETRUNCATED_CHECKER:
		printf("���� ����:\ndata line �߰��� Ư�� ���ڰ� ���ԵǾ� �ֽ��ϴ�.\n");
		printf("%s ������ data�� ���� �� �ֽ��ϴ�.\n");
		printf("�ذ� ���:\ndata line �߰��� �ִ� Ư�� ���ڸ� �����Ͻʽÿ�.\n");
		break;
	case eINCOMPLETEDATALINE_CHECKER:
		printf("���� ����:\nchar, varchar, text Ÿ���� data line�� �ҿ����ϰ� ����Ǿ����ϴ�.\n");
		printf("=>\n%s\n", errorData);
		printf("�ذ� ���:\n");
		printf("(1) data�� �� line�� ��쿡�� \' Ȥ�� \" �� �������� �߰��ϼ���.\n");
		printf("(2) data�� ���� line�� �����ִ� ��쿡�� \\n Ȥ�� \\ �� �������� �߰��ϼ���.\n"); 
		break;
	case eCANNOTCLOSESETCOLUMN_CHECKER:
		printf("���� ����:\n������ �ݴ� } ��ȣ�� ã�� �� �����ϴ�.\n");
		printf("�ذ� ���:\n������ ���� { ��ȣ�� �ݴ� } ��ȣ�� ��ġ�ǵ��� �����Ͻʽÿ�.\n");
		break;
	case eNOSUCHCLASS_CHECKER:
		printf("���� ����:\ndata�� loading�Ϸ��� class�� DB�� �������� �ʽ��ϴ�.\n");
		printf("\"%s\" class �̸��� �ùٸ��� Ȯ���Ͻʽÿ�.\n", errorData);
		printf("�ذ� ���:\ndata�� loading�Ϸ��� class �̸���\n");
		printf("DB�� �����ϴ� class�� �̸��� �����ϰ� �����Ͻʽÿ�.\n");
		break;
	case eNOSUCHATTRIBUTE_CHECKER:
		printf("���� ����:\ndata�� loading�Ϸ��� attribute�� DB�� �������� �ʽ��ϴ�.\n");
		printf("\"%s\" attribute �̸��� �ùٸ��� Ȯ���Ͻʽÿ�.\n", errorData);
		printf("�ذ� ���:\ndata�� loading�Ϸ��� attribute �̸���\n");
		printf("DB�� �����ϴ� attribute�� �̸��� �����ϰ� �����Ͻʽÿ�.\n");
		break;
	case eINSUFFICIENTDATA_CHECKER:
		printf("���� ����:\n");
		printf("(1) source file���� Ư�� attribute�� data�� ���� �ֽ��ϴ�.\n");
		printf("(2) source file�� �� �κ��� �߷��� �ֽ��ϴ�.\n");
		printf("�ذ� ���:\n");
		printf("(1) source file���� ���� �ִ� attribute�� data�� �߰��Ͻʽÿ�.\n");
		printf("(2) source file�� �߷��� �� �κ��� �߰��Ͻʽÿ�.\n");
		break;
	case eSINGLEQUOTEINDATA_CHECKER:
		printf("���� ����:\n���� ����ǥ�� ���� ����ǥ ���̿� ���ԵǾ� �ֽ��ϴ�.\n");
		printf("=>\n%s\n", errorData);
		printf("�ذ� ���:\n���� ����ǥ �տ� \\�� �ٿ� �ֽʽÿ�.\n");
		break;
	case eDOUBLEQUOTEINDATA_CHECKER:
		printf("���� ����:\nū ����ǥ�� ū ����ǥ ���̿� ���ԵǾ� �ֽ��ϴ�.\n");
		printf("=>\n%s\n", errorData);
		printf("�ذ� ���:\nū ����ǥ �տ� \\�� �ٿ� �ֽʽÿ�.\n");
		break;
	case eINVALIDNUMBERFORMAT_CHECKER:
		printf("���� ����:\n������ ������ �ùٸ��� �ʽ��ϴ�\n");
		printf("=>\n%s\n", errorData);
		printf("�ذ� ���:\n����� �� ���ڸ� ǥ���ϵ��� �����Ͽ� �ֽʽÿ�.\n");
		break;
	case eSETALREADYOPENED_CHECKER:
		printf("���� ����:\n������ ������ ���¿��� �ٽ� ������ ������ �õ��Ͽ����ϴ�.\n");
		printf("=>\n%s\n", errorData);
		printf("�ذ� ���:\n������ �ݰ� ���� �ٽ� ������ ������ �����Ͽ� �ֽʽÿ�.\n");
		break;
	case eSETALREADYCLOSED_CHECKER:
		printf("���� ����:\n������ ���� ���¿��� �ٽ� ������ �������� �õ��Ͽ����ϴ�.\n");
		printf("=>\n%s\n", errorData);
		printf("�ذ� ���:\n������ �ٽ� ���� ���� ������ �ݵ��� �����Ͽ� �ֽʽÿ�.\n");
		break;
	case eINVALIDUSEOFBACKSLASH_CHECKER:
		printf("���� ����:\n\\�� ����ǥ ���̿��� ������ �ʾҽ��ϴ�.\n");
		printf("=>\n%s\n", errorData);
		printf("�ذ� ���:\n����ǥ �ܺο��� ���� \\�� �����Ͽ� �ֽʽÿ�.\n");
		break;
	case eINVALIDUSEOFSINGLEQUOTE_CHECKER:
		printf("���� ����:\n");
		printf("(1) ���� ����ǥ�� ����ǥ ���̿��� ������ �ʾҽ��ϴ�.\n");
		printf("(2) ���� ����ǥ�� ū ����ǥ ���̿� ���ԵǾ� �ֽ��ϴ�.\n");
		printf("=>\n%s\n", errorData);
		printf("�ذ� ���:\n");
		printf("(1) ����ǥ �ܺο��� ���� ���� ����ǥ�� �����Ͽ� �ֽʽÿ�.\n");
		printf("(2) ���� ����ǥ �տ� \\�� �ٿ� �ֽʽÿ�.\n");
		break;
	case eINVALIDUSEOFDOUBLEQUOTE_CHECKER:
		printf("���� ����:\n");
		printf("(1) ū ����ǥ�� ����ǥ ���̿��� ������ �ʾҽ��ϴ�.\n");
		printf("(2) ū ����ǥ�� ���� ����ǥ ���̿� ���ԵǾ� �ֽ��ϴ�.\n");
		printf("=>\n%s\n", errorData);
		printf("�ذ� ���:\n");
		printf("(1) ����ǥ �ܺο��� ���� ū ����ǥ�� �����Ͽ� �ֽʽÿ�.\n");
		printf("(2) ū ����ǥ �տ� \\�� �ٿ� �ֽʽÿ�.\n");
		break;
	case eINCOMPATIBLEATTRTYPE_CHECKER:
		printf("���� ����:\n");
		printf("(1) source file�� �ִ� data�� type�� DB�� ���ǵ� data�� type�� ��ġ���� �ʽ��ϴ�.\n");
		printf("(2) source file���� Ư�� attribute�� data�� ���� �ֽ��ϴ�.\n");
		printf("=>\n%s\n", errorData);
		printf("�ذ� ���:\n");
		printf("(1) source file�� �ִ� data�� type�� DB�� ���ǵ� data�� type�� ���� �ֽʽÿ�.\n");
		printf("(2) source file �߰��� ���� �ִ� attribute�� data�� �߰��� �ֽʽÿ�.\n"); 
		break;
    case eSPATIALATTRMUSTBEFIRST_CHECKER:
		printf("���� ����:\n");
		printf("(1) ���� �Ӽ��� �� ó�� �Ӽ����� ������ �ʾ���.\n");
		printf("=>\n%s\n", errorData);
		printf("�ذ� ���:\n");
		printf("(1) ���� �Ӽ��� �� ó�� �Ӽ����� �ٲ۴�.\n");
		break;
    default:
		printf("Unhandled Case\n");
		break;
	}
	
	printf("\n");
	fflush(stdout);
}

void checker_skipToNextDataRecord(
	FILE *fp,					/* IN: file pointer to read */
	char *line,					/* IN: line data which an error occurred */
	Two dataNo,					/* IN: current attribute # */
	Two totalColNo				/* IN: total attribute # */
)
{
	char	lineBuffer[MAXLINELEN];
	Two		numOfAttrsToSkip;
	Four	length;
	
	numOfAttrsToSkip = totalColNo - dataNo - 1;
	
	length = strlen(line);
	
	if (line[length - 3] == '\\' && line[length - 2] == 'n')
		numOfAttrsToSkip ++;
	else if (line[length - 2] == '\\')
		numOfAttrsToSkip ++;
	
	while (Util_fgets(lineBuffer, MAXLINELEN, fp) != NULL)
	{
		checker_lineNo ++;
		length = strlen(lineBuffer);
		
		if (length <= 1) continue;		
			
		if (lineBuffer[length - 3] == '\\' && lineBuffer[length - 2] == 'n')
			continue;
		else if (lineBuffer[length - 2] == '\\')
			continue;
		else
		{
			numOfAttrsToSkip --;
			if (numOfAttrsToSkip == 0) break;
		}
	}
}

