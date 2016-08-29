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

#ifndef WIN32
#include <unistd.h>
#include <sys/param.h>
#else
#include <io.h>
#endif
#include <malloc.h>
#include <string.h>

#ifdef WIN32
#define DIRECTORY_SEPARATOR "\\"
#define R_OK 04
#else
#define DIRECTORY_SEPARATOR "/"
#endif

#define TEXT_TEMP_DIR   "ODYS_TEMP_PATH"

#ifndef min
#define min(a,b) (((a)<(b)) ? (a) : (b))
#endif

Four oosql_Tool_BuildTextIndex
(
    OOSQL_SystemHandle*						systemHandle,
    Four									volId,
	Four									temporaryVolId,
    char*									className,
    char*									attrName,
	lom_Text_ConfigForInvertedIndexBuild*	config
)
{
	LOM_Handle					 *handle;
    Four                         v;
    Four                         ocn;
    Four                         idxForClassInfo;
    Four                         lomScanNum;
    Four                         i;
    Four                         e;
    Four                         classId;
    catalog_SysIndexesOverlay    *ptrToSysIndexes;
    catalog_SysClassesOverlay    *ptrToSysClasses;
    catalog_SysAttributesOverlay *ptrToSysAttributes;
    LOM_IndexID                  iid;
    BoundCond                    startBound;
    BoundCond                    stopBound;
    LockParameter                lockup;
    char                         sortedPostingFileName[1024];
    char                         docIdFileName[1024];
    char                         *tempDir;

	handle = &OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle);

    /* check parameters */
    if (volId < 0) OOSQL_ERR(eBADPARAMETER_LOM);
    if (className == NULL) OOSQL_ERR(eBADPARAMETER_LOM);

    /* open class */
    ocn = LOM_OpenClass(handle, volId, className);
    OOSQL_CHECK_ERROR(ocn);

    e = LOM_GetClassID(handle, volId, className, &classId);
    OOSQL_CHECK_ERROR(ocn);

    /* get class information */
    e = Catalog_GetClassInfo(handle, volId, LOM_USEROPENCLASSTABLE(handle)[ocn].classID, &idxForClassInfo);
    OOSQL_CHECK_ERROR(e);

    v = Catalog_GetVolIndex(handle, volId);
    OOSQL_CHECK_ERROR(v);

    /* set memory pointer */
    ptrToSysClasses    = &CATALOG_GET_CLASSINFOTBL(handle, v)[idxForClassInfo];
    ptrToSysIndexes    = &CATALOG_GET_INDEXINFOTBL(handle, v)[CATALOG_GET_INDEXINFOTBL_INDEX(ptrToSysClasses)];
    ptrToSysAttributes = &CATALOG_GET_ATTRINFOTBL(handle, v)[CATALOG_GET_ATTRINFOTBL_INDEX(ptrToSysClasses)];

    for(i = 0; i < CATALOG_GET_INDEXNUM(ptrToSysClasses); i++) {
        if (CATALOG_GET_INDEXCOLNO(ptrToSysIndexes)[i] == LOM_LOGICALID_COLNO) {
            iid = ptrToSysIndexes->iid;
            break;
        }
    }

    if (i == CATALOG_GET_INDEXNUM(ptrToSysClasses))
        OOSQL_ERR(eNOLOGICALIDINDEX_UTIL);

    startBound.op = SM_BOF;
    stopBound.op = SM_EOF;

    /* lock parameter */
    /* we here donot release file-level lock until commit */
    lockup.mode = L_X;
    lockup.duration = L_COMMIT;

    /* open index scan */
    lomScanNum = LOM_OpenIndexScan(handle, ocn, &iid, &startBound, &stopBound, 0, (BoolExp *)NULL, &lockup);
    if(lomScanNum < eNOERROR) OOSQL_ERR(lomScanNum);

    tempDir = (char*)getenv(TEXT_TEMP_DIR);
	if(tempDir == NULL) LOM_ERROR(handle, eCONFIGURATION_LOM);

    for(i = 0; i < CATALOG_GET_ATTRNUM(ptrToSysClasses); i++)
    { 
        if (CATALOG_GET_ATTRTYPE(&ptrToSysAttributes[i]) != LOM_TEXT)
            continue;

        if (strcmp(CATALOG_GET_ATTRNAME(&ptrToSysAttributes[i]), attrName) != 0)
            continue;

        strcpy(sortedPostingFileName, tempDir);
        strcat(sortedPostingFileName, DIRECTORY_SEPARATOR);
        strcat(sortedPostingFileName, "TEXT_");
        strcat(sortedPostingFileName, className);
        strcat(sortedPostingFileName, "_");
        strcat(sortedPostingFileName, CATALOG_GET_ATTRNAME(&ptrToSysAttributes[i]));
		if(config->isSortingPostingFile)
			strcat(sortedPostingFileName, "_Posting");
		else
			strcat(sortedPostingFileName, "_SortedPosting");

        strcpy(docIdFileName, tempDir);
        strcat(docIdFileName, DIRECTORY_SEPARATOR);
        strcat(docIdFileName, "TEXT_");
        strcat(docIdFileName, className);
        strcat(docIdFileName, "_");
        strcat(docIdFileName, CATALOG_GET_ATTRNAME(&ptrToSysAttributes[i]));
        strcat(docIdFileName, "_DocId");

        if (access(sortedPostingFileName, R_OK) == 0)
        {
            e = lom_Text_AddInvertedIndexEntryFromTempFile(handle, temporaryVolId, lomScanNum, SM_TRUE, (Two)i, sortedPostingFileName, docIdFileName, SM_FALSE, config);
            OOSQL_CHECK_ERROR(e);
        }

        break;
    }

    e = LOM_CloseScan(handle, lomScanNum);
    if(e < eNOERROR) OOSQL_ERR(e);

    e = LOM_CloseClass(handle, ocn);
    if(e < eNOERROR) OOSQL_ERR(e);

    return eNOERROR;
}

#else /* SLIMDOWN_TEXTIR */

#include "OOSQL_StorageSystemHeaders.h"
#include "OOSQL_APIs_Internal.hxx"
#include "OOSQL_Error.h"
#include "OOSQL_Tool.hxx"

Four oosql_Tool_BuildTextIndex
(
    OOSQL_SystemHandle*                     systemHandle,
    Four                                    volId,
    Four                                    temporaryVolId,
    char*                                   className,
    char*                                   attrName,
    lom_Text_ConfigForInvertedIndexBuild*   config
)
{
    return eTEXTIR_NOTENABLED_OOSQL;
}

#endif /* SLIMDOWN_TEXTIR */

