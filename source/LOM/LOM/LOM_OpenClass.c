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
#include "Catalog_Internal.h"
#include "Catalog.h"
#include <string.h>

Four LOM_OpenClass(
	LOM_Handle *handle, 
	Four volId,         /* IN volume ID */
	char *className)      /* IN classname to open */
{
	Four e;
	Four orn;
	Four i;
	Four idxForClassInfo;
	catalog_SysClassesOverlay *ptrToSysClasses;
	catalog_SysAttributesOverlay *ptrToSysAttributes;
	Four numOfTextAttrs;
	Boolean isFirstTextAttr;
	Four j;
	Four v;
	LockParameter lockup;

	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId,className);
	if(orn < eNOERROR) LOM_ERROR(handle, orn);

	/* check if this class is temporary or not */
	if(LRDS_IS_TEMPORARY_RELATION(LOM_GET_LRDS_HANDLE(handle), orn))
	{
		e = Catalog_GetTemporaryClassId(handle, volId, className, &(LOM_USEROPENCLASSTABLE(handle)[orn].classID));
		if(e<0) LOM_ERROR(handle, e);
	}
	else 
	{
		/* check if this class has alread been opened */
		if(LOM_USEROPENCLASSTABLE(handle)[orn].count == 0) 
		{
			e = lom_GetClassId(handle, volId, className, &(LOM_USEROPENCLASSTABLE(handle)[orn].classID));
			if(e<0) LOM_ERROR(handle, e);
		}
	}

	/* Increment of count is executed at the end of the function */
	if(LOM_USEROPENCLASSTABLE(handle)[orn].count > 0) {
		LOM_USEROPENCLASSTABLE(handle)[orn].count++;
		return orn;
	}

	/* if this class is temporary class */
	if(LOM_USEROPENCLASSTABLE(handle)[orn].classID == 0) return eNOERROR;

	/* Initialize UserOpenClassTableEntry structure */
	LOM_USEROPENCLASSTABLE(handle)[orn].volId = (VolID)volId;
	LOM_USEROPENCLASSTABLE(handle)[orn].numOfTextAttrs = 0;
	LOM_USEROPENCLASSTABLE(handle)[orn].numOfodmgCollAttrs = 0;
	LOM_USEROPENCLASSTABLE(handle)[orn].ornForContentTable = NIL;
	LOM_USEROPENCLASSTABLE(handle)[orn].ornForDeletionListTable = NIL;
	LOM_USEROPENCLASSTABLE(handle)[orn].lrdsScanIdForTextScan = NIL;
	LOM_USEROPENCLASSTABLE(handle)[orn].contentTableScanIdForTextScan = NIL;

	for(j = 0; j < LOM_MAXNUMOFTEXTCOLUMN; j++) {
		LOM_USEROPENCLASSTABLE(handle)[orn].textColNo[j] = NIL;
		LOM_USEROPENCLASSTABLE(handle)[orn].ornForInvertedIndexTable[j] = NIL;
		LOM_USEROPENCLASSTABLE(handle)[orn].ornForDocIdIndexTable[j] = NIL;
		LOM_USEROPENCLASSTABLE(handle)[orn].handleForDLOfKeywordExtractor[j] = NULL;
		LOM_USEROPENCLASSTABLE(handle)[orn].handleForDLOfFilter[j] = NULL;
		LOM_USEROPENCLASSTABLE(handle)[orn].handleForDLOfStemizer[j] = NULL;
		LOM_USEROPENCLASSTABLE(handle)[orn].fptrToKeywordExtractor[j] = NULL;
		LOM_USEROPENCLASSTABLE(handle)[orn].fptrToFilter[j] = NULL;
		LOM_USEROPENCLASSTABLE(handle)[orn].fptrToStemizer[j] = NULL;
	}

	for(j = 0; j < LOM_MAXNUMOFODMGCOLLCOLUMN; j++) {
		LOM_USEROPENCLASSTABLE(handle)[orn].odmgCollColNo[j] = NIL;
		LOM_USEROPENCLASSTABLE(handle)[orn].odmgCollDataOrn[j] = NIL;
	}

	/* check if text attributes exist */
	e = Catalog_GetClassInfo(handle, volId, LOM_USEROPENCLASSTABLE(handle)[orn].classID, &idxForClassInfo);
	if( e < eNOERROR) LOM_ERROR(handle, e);

	v = Catalog_GetVolIndex(handle, volId);
    if(v < eNOERROR) LOM_ERROR(handle, v);

	ptrToSysClasses = &CATALOG_GET_CLASSINFOTBL(handle, v)[idxForClassInfo];

	ptrToSysAttributes =  &CATALOG_GET_ATTRINFOTBL(handle, v)[CATALOG_GET_ATTRINFOTBL_INDEX(ptrToSysClasses)];

	numOfTextAttrs = 0;
	isFirstTextAttr = SM_TRUE;
	j = 0;

	for(i = 0; i < CATALOG_GET_ATTRNUM(ptrToSysClasses); i++) 
	{ 
		if(ptrToSysAttributes[i].type == LOM_TEXT &&
			ptrToSysAttributes[i].activeColNo != NIL)
		{
			if(isFirstTextAttr)
			{
				/* open content table */
				LOM_USEROPENCLASSTABLE(handle)[orn].ornForContentTable =
					e = lom_Text_OpenContentClass(handle, volId, className);
				if(e < eNOERROR) LOM_ERROR(handle, e);

				/* get counter id */
				e = lom_Text_GetCounterId(handle, volId, className, &(LOM_USEROPENCLASSTABLE(handle)[orn].counterId));
				if(e < eNOERROR) LOM_ERROR(handle, e);

				/* open lrds level sequential scan and content table */
				lockup.mode     = L_IS;
				lockup.duration = L_COMMIT;
				LOM_USEROPENCLASSTABLE(handle)[orn].lrdsScanIdForTextScan =
					e = LRDS_OpenSeqScan(LOM_GET_LRDS_HANDLE(handle), orn, FORWARD, 0, NULL, &lockup);
				if(e < eNOERROR) LOM_ERROR(handle, e);

				LOM_USEROPENCLASSTABLE(handle)[orn].contentTableScanIdForTextScan =
					e = LRDS_OpenSeqScan(LOM_GET_LRDS_HANDLE(handle), LOM_USEROPENCLASSTABLE(handle)[orn].ornForContentTable, FORWARD, 0, NULL, &lockup);
				if(e < eNOERROR) LOM_ERROR(handle, e);
			}
			if(LOM_USEROPENCLASSTABLE(handle)[orn].ornForContentTable < eNOERROR) LOM_ERROR(handle, LOM_USEROPENCLASSTABLE(handle)[orn].ornForContentTable);
			isFirstTextAttr = SM_FALSE;

			LOM_USEROPENCLASSTABLE(handle)[orn].textColNo[j] = (Two)i;

			/* open inverted index table */
			LOM_USEROPENCLASSTABLE(handle)[orn].ornForInvertedIndexTable[j] = 
				e = lom_Text_OpenInvertedIndexTable(handle, volId, className, ptrToSysAttributes[i].name);
			if(e < eNOERROR) LOM_ERROR(handle, e);

			/* open inverted index table */
			LOM_USEROPENCLASSTABLE(handle)[orn].ornForDocIdIndexTable[j] = 
				e = lom_Text_OpenDocIdIndexTable(handle, volId, className, ptrToSysAttributes[i].name);
			if(e < eNOERROR) LOM_ERROR(handle, e);

			/* we donot link a keyword shared object dynamically until it will be actually used */
#ifdef PRELOAD_KEYWORDEXTRACTOR
			/* get a function pointer to keyword extractor */
			e = LOM_Text_OpenHandleForKeywordExtractor(handle, 
				volId, 
				LOM_USEROPENCLASSTABLE(handle)[orn].classID,
				GET_USERLEVEL_COLNO(i),	/* user-level function call */
				&LOM_USEROPENCLASSTABLE(handle)[orn].handleForDLOfKeywordExtractor[j],
				&LOM_USEROPENCLASSTABLE(handle)[orn].fptrToKeywordExtractor[j],
				&LOM_USEROPENCLASSTABLE(handle)[orn].fptrToGettingNextPostingInfo[j],
				&LOM_USEROPENCLASSTABLE(handle)[orn].fptrToFinalizeKeywordExtraction[j]);
			if(e < eNOERROR) LOM_ERROR(handle, e);
#else
			LOM_USEROPENCLASSTABLE(handle)[orn].handleForDLOfKeywordExtractor[j] = NULL;
			LOM_USEROPENCLASSTABLE(handle)[orn].fptrToKeywordExtractor[j] = NULL;
#endif

			/* we donot link a filter shared object dynamically until it will be actually used */
#ifdef PRELOAD_FILTER
			/* get a function pointer to filter */
			e = LOM_Text_OpenHandleForFilter(handle, 
				volId, 
				LOM_USEROPENCLASSTABLE(handle)[orn].classID,
				GET_USERLEVEL_COLNO(i),	/* user-level function call */
				&LOM_USEROPENCLASSTABLE(handle)[orn].handleForDLOfFilter[j],
				&LOM_USEROPENCLASSTABLE(handle)[orn].fptrToFilter[j]);
			if(e < eNOERROR) LOM_ERROR(handle, e);
#else
			LOM_USEROPENCLASSTABLE(handle)[orn].handleForDLOfFilter[j] = NULL;
			LOM_USEROPENCLASSTABLE(handle)[orn].fptrToFilter[j] = NULL;
#endif

#ifdef PRELOAD_STEMIZER
			/* get a function pointer to filter */
			e = LOM_Text_OpenHandleForStemizer(handle, 
				volId, 
				LOM_USEROPENCLASSTABLE(handle)[orn].classID,
				GET_USERLEVEL_COLNO(i),	/* user-level function call */
				&LOM_USEROPENCLASSTABLE(handle)[orn].handleForDLOfStemizer[j],
				&LOM_USEROPENCLASSTABLE(handle)[orn].fptrToStemizer[j]);
			if(e < eNOERROR) 
			{
				LOM_PTRERROR(handle, e);
				LOM_USEROPENCLASSTABLE(handle)[orn].handleForDLOfStemizer[j] = NULL;
				LOM_USEROPENCLASSTABLE(handle)[orn].fptrToStemizer[j] = NULL;
			}
#else
			LOM_USEROPENCLASSTABLE(handle)[orn].handleForDLOfStemizer[j] = NULL;
			LOM_USEROPENCLASSTABLE(handle)[orn].fptrToStemizer[j] = NULL;
#endif

			LOM_USEROPENCLASSTABLE(handle)[orn].numOfTextAttrs++;
			j++;

		}
        else if(ptrToSysAttributes[i].complexType == LOM_COMPLEXTYPE_ODMG_COLLECTIONSET ||
                ptrToSysAttributes[i].complexType == LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG ||
                ptrToSysAttributes[i].complexType == LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST ||
                ptrToSysAttributes[i].complexType == LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY)
        {
			Four	index;
			char	dataRelationName[LOM_MAXCLASSNAME];
            
			index = LOM_USEROPENCLASSTABLE(handle)[orn].numOfodmgCollAttrs;

			switch(ptrToSysAttributes[i].complexType)
			{
			case LOM_COMPLEXTYPE_ODMG_COLLECTIONSET:
				strcpy(dataRelationName, LOM_ODMG_COLLECTION_SET_PREFIX); 
				break;
			case LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG:
				strcpy(dataRelationName, LOM_ODMG_COLLECTION_BAG_PREFIX); 
				break;
			case LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST:
				strcpy(dataRelationName, LOM_ODMG_COLLECTION_LIST_PREFIX); 
				break;
			case LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY:
				strcpy(dataRelationName, LOM_ODMG_COLLECTION_VARARRAY_PREFIX); 
				break;
			}

			switch(ptrToSysAttributes[i].type)
            {
            case LOM_SHORT:
				strcat(dataRelationName, LOM_ODMG_COLLECTION_SHORT_POSTFIX);
				break;
			case LOM_INT:
				strcat(dataRelationName, LOM_ODMG_COLLECTION_INT_POSTFIX);
				break;
			case LOM_LONG:
				strcat(dataRelationName, LOM_ODMG_COLLECTION_LONG_POSTFIX);
				break;
			case LOM_LONG_LONG:
				strcat(dataRelationName, LOM_ODMG_COLLECTION_LONG_LONG_POSTFIX);
				break;
			case LOM_FLOAT:
				strcat(dataRelationName, LOM_ODMG_COLLECTION_FLOAT_POSTFIX);
				break;
			case LOM_DOUBLE:
				strcat(dataRelationName, LOM_ODMG_COLLECTION_DOUBLE_POSTFIX);
				break;
			case LOM_STRING:
				strcat(dataRelationName, LOM_ODMG_COLLECTION_STRING_POSTFIX);
				break;
			case LOM_VARSTRING:
				strcat(dataRelationName, LOM_ODMG_COLLECTION_VARSTRING_POSTFIX);
				break;
			case LOM_PAGEID:
				strcat(dataRelationName, LOM_ODMG_COLLECTION_PAGEID_POSTFIX);
				break;
			case LOM_FILEID:
				strcat(dataRelationName, LOM_ODMG_COLLECTION_FILEID_POSTFIX);
				break;
			case LOM_INDEXID:
				strcat(dataRelationName, LOM_ODMG_COLLECTION_INDEXID_POSTFIX);
				break;
			case LOM_OID:
				strcat(dataRelationName, LOM_ODMG_COLLECTION_OID_POSTFIX);
				break;
			case LOM_MBR:
				strcat(dataRelationName, LOM_ODMG_COLLECTION_MBR_POSTFIX);
				break;
			case LOM_REF:
				strcat(dataRelationName, LOM_ODMG_COLLECTION_REF_POSTFIX);
				break;
			case LOM_LINK:
				strcat(dataRelationName, LOM_ODMG_COLLECTION_LINK_POSTFIX);
				break;
			case LOM_TEXT:
				strcat(dataRelationName, LOM_ODMG_COLLECTION_TEXT_POSTFIX);
				break;
			case LOM_DATE:
				strcat(dataRelationName, LOM_ODMG_COLLECTION_DATE_POSTFIX);
				break;
			case LOM_TIME:
				strcat(dataRelationName, LOM_ODMG_COLLECTION_TIME_POSTFIX);
				break;
			case LOM_TIMESTAMP:
				strcat(dataRelationName, LOM_ODMG_COLLECTION_TIMESTAMP_POSTFIX);
				break;
			case LOM_INTERVAL:
				strcat(dataRelationName, LOM_ODMG_COLLECTION_INTERVAL_POSTFIX);
				break;
            }            
            LOM_USEROPENCLASSTABLE(handle)[orn].numOfodmgCollAttrs++;
            LOM_USEROPENCLASSTABLE(handle)[orn].odmgCollColNo[index] = i;
            LOM_USEROPENCLASSTABLE(handle)[orn].odmgCollDataOrn[index] = e = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, dataRelationName);
            if(e < 0) LOM_ERROR(handle, e);
        }
	}/* for */

	LOM_USEROPENCLASSTABLE(handle)[orn].count++;

	return orn;
}

