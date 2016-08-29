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
#include "DBM.h"

#ifndef WIN32
#include <sys/param.h>
#include <unistd.h>
#else
#include <io.h>
#endif
#include <malloc.h>
#include <string.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>

#ifdef WIN32
#define DIRECTORY_SEPARATOR "\\"
#define R_OK 04
#define F_OK 00
#else
#define DIRECTORY_SEPARATOR "/"
#endif

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four oosql_Tool_StorePosting(
	OOSQL_SystemHandle*	systemHandle,	// IN  OOSQL system hadnel
	Four				volId,			// IN  
	char*				className,		// IN  
	char*				attrName,		// IN  
	Boolean				clearFlag		// IN  
)
{
	LOM_Handle*						handle;
	char							filename[MAXPATHLEN];			
	char							tablename[LOM_MAXCLASSNAME];	
	char*							tempDir;					
	FILE*							fp;							
	Four							e;								// error code
	lom_PostingBuffer				postingBuffer;
	Four							ocn, colNo, i;
    catalog_SysClassesOverlay		*ptrToSysClasses;
    catalog_SysAttributesOverlay	*ptrToSysAttributes;
	Four							v;
	Four							idxForClassInfo;
	lom_Text_PostingInfoForReading	postingInfo;
	Four							outOcn, outScanId;
	AttrInfo						outAttrInfo[3];
	LockParameter					lockup;
	char							keyword[LOM_MAXKEYWORDSIZE];
	Four							logicalId;
	Four							index;
	LOM_ColListStruct				clist[3];
	Four							outClassId;
	Four							postingLength;

	handle = &OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle);

	// check parameters
    if (volId < 0) OOSQL_ERR(eBADPARAMETER_LOM);
    if (className == NULL) OOSQL_ERR(eBADPARAMETER_LOM);

	// open class
    ocn = LOM_OpenClass(handle, volId, className);
    OOSQL_CHECK_ERROR(ocn);

	// get class information 
    e = Catalog_GetClassInfo(handle, volId, LOM_USEROPENCLASSTABLE(handle)[ocn].classID, &idxForClassInfo);
    OOSQL_CHECK_ERROR(e);

    v = Catalog_GetVolIndex(handle, volId);
    OOSQL_CHECK_ERROR(v);

    // set memory pointer
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

	// posting infos for reading temporary file 
	e = lom_Text_PreparePostingInfoForReading(handle, volId,
											  LOM_USEROPENCLASSTABLE(handle)[ocn].classID,
											  (Two)colNo, &postingInfo);
	if(e < 0) LOM_ERROR(handle, e);

    e = LOM_AllocPostingBuffer(handle, &postingBuffer, LOM_DEFAULTPOSTINGBUFFERSIZE);
    OOSQL_CHECK_ERROR(e);
    LOM_FREEOFFSET_POSTINGBUFFER(postingBuffer) = 0;

	tempDir = getenv("ODYS_TEMP_PATH");
	if (tempDir == NULL) OOSQL_ERR(eTEMPDIRNOTDEFINED_UTIL);

	sprintf(filename, "%s%sTEXT_%s_%s_Posting", tempDir, DIRECTORY_SEPARATOR, className, attrName);
	sprintf(tablename, "_%s_%s_Posting", className, attrName);

	e = lom_Text_OpenTempFile(handle, filename, "r", &fp);
    OOSQL_CHECK_ERROR(e);

	outOcn = LOM_OpenClass(handle, volId, tablename);
	if(outOcn == eNOERROR)
	{
		if(clearFlag)
		{
			e = LOM_DestroyClass(handle, volId, tablename);
			OOSQL_CHECK_ERROR(e);
		}
	}
	else if(e != eRELATIONNOTFOUND_LRDS)
	{
		OOSQL_CHECK_ERROR(outOcn);
	}

	if(clearFlag || e == eRELATIONNOTFOUND_LRDS)
	{
		outAttrInfo[0].complexType   = LOM_COMPLEXTYPE_BASIC;
		outAttrInfo[0].type          = LOM_VARSTRING;
		outAttrInfo[0].length		 = LOM_MAXKEYWORDSIZE;
		outAttrInfo[0].inheritedFrom = LOM_INHERITEDFROM_THIS_CLASS;
		outAttrInfo[0].domain        = LOM_VARSTRING;
		strcpy(outAttrInfo[0].name, "keyword");

		outAttrInfo[1].complexType   = LOM_COMPLEXTYPE_BASIC;
		outAttrInfo[1].type          = LOM_LONG_VAR;
		outAttrInfo[0].length		 = LOM_LONG_SIZE_VAR;
		outAttrInfo[1].inheritedFrom = LOM_INHERITEDFROM_THIS_CLASS;
		outAttrInfo[1].domain        = LOM_LONG_VAR;
		strcpy(outAttrInfo[1].name, "logicalId");

		outAttrInfo[2].complexType   = LOM_COMPLEXTYPE_BASIC;
		outAttrInfo[2].type          = LOM_VARSTRING;
		outAttrInfo[0].length		 = LOM_MAXLARGEOBJECTSIZE;
		outAttrInfo[2].inheritedFrom = LOM_INHERITEDFROM_THIS_CLASS;
		outAttrInfo[2].domain        = LOM_VARSTRING;
		strcpy(outAttrInfo[2].name, "posting");

		e = LOM_GetNewClassId(handle, volId, SM_FALSE, &outClassId);
		e = LOM_CreateClass(handle, volId, tablename, NULL, NULL, 3, outAttrInfo, 0, NULL, 0, NULL, SM_FALSE, outClassId);
		OOSQL_CHECK_ERROR(e);

		outOcn = LOM_OpenClass(handle, volId, tablename);
		OOSQL_CHECK_ERROR(outOcn);
	}

	lockup.mode     = L_X;
	lockup.duration = L_COMMIT;
	outScanId = LOM_OpenSeqScan(handle, outOcn, FORWARD, 0, NULL, &lockup);
	OOSQL_CHECK_ERROR(outScanId);

	while(1)
	{
		e = lom_Text_GetPostingFromTempFile(handle, ocn, SM_FALSE, (Two)colNo, &postingInfo, fp, &postingBuffer);
	    if(e == EOS) break;
        OOSQL_CHECK_ERROR(e);

		index = 0;
		strcpy(keyword, &(LOM_PTR_POSTINGBUFFER(postingBuffer)[index]));
		index += strlen(keyword)+1;			/* keyword */
		memcpy(&postingLength, &(LOM_PTR_POSTINGBUFFER(postingBuffer)[index]), sizeof(Four));
		index += sizeof(Four);				/* posting length */
		memcpy(&logicalId, &(LOM_PTR_POSTINGBUFFER(postingBuffer)[index]), sizeof(Four));

		clist[0].colNo = 0;
		clist[0].nullFlag = SM_FALSE;
		clist[0].data.ptr = keyword;
		clist[0].dataLength = strlen(keyword);
		clist[0].start = ALL_VALUE;
		clist[0].length = ALL_VALUE;

		clist[1].colNo = 1;
		clist[1].nullFlag = SM_FALSE;
		ASSIGN_VALUE_TO_COL_LIST(clist[1], logicalId, sizeof(Four));
		clist[1].dataLength = sizeof(Four);
		clist[1].start = ALL_VALUE;
		clist[1].length = ALL_VALUE;

		clist[2].colNo = 2;
		clist[2].nullFlag = SM_FALSE;
		clist[2].data.ptr = &(LOM_PTR_POSTINGBUFFER(postingBuffer)[0]);
		clist[2].dataLength = strlen(keyword) + 1 + sizeof(Four) + postingLength;
		clist[2].start = ALL_VALUE;
		clist[2].length = ALL_VALUE;

		e = LOM_CreateObjectByColList(handle, outScanId, SM_TRUE, 3, clist, NULL);
		OOSQL_CHECK_ERROR(e);
	}

	e = LOM_CloseScan(handle, outScanId);
	OOSQL_CHECK_ERROR(e);

	e = LOM_CloseClass(handle, outOcn);
	OOSQL_CHECK_ERROR(e);

	e = lom_Text_CloseTempFile(handle, fp);
    OOSQL_CHECK_ERROR(e);

	return eNOERROR;
}

#else /* SLIMDOWN_TEXTIR */

#include "OOSQL_StorageSystemHeaders.h"
#include "OOSQL_APIs_Internal.hxx"
#include "OOSQL_Error.h"
#include "OOSQL_Tool.hxx"

Four oosql_Tool_StorePosting(
    OOSQL_SystemHandle* systemHandle,   // IN  OOSQL system hadnel
    Four                volId,          // IN  
    char*               className,      // IN  
    char*               attrName,       // IN  
    Boolean             clearFlag       // IN
)
{
    return eTEXTIR_NOTENABLED_OOSQL;
}

#endif /* SLIMDOWN_TEXTIR */

