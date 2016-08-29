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
#include "OOSQL_ServerQuery.hxx"

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eBADPARAMETER_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_Text_AddKeywordExtractor(
	OOSQL_SystemHandle* systemHandle,							// IN  
	Four				volID,									// IN  
	char*				keywordExtractor,						// IN  
	Four				version,								// IN  
	char*				keywordExtractorFilePath,				// IN  
	char*				keywordExtractorFunctionName,			// IN  
	char*				getNextPostingFunctionName,				// IN  
	char*				finalizeKeywordExtractorFunctionName,	// IN  
	Four*				keywordExtractorNo						// OUT
)
{
	if(!OOSQL_CHECKGDSINSTTABLE(systemHandle))
		return eBADPARAMETER_OOSQL;

	return LOM_Text_AddKeywordExtractor(&OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle), volID, keywordExtractor, version, keywordExtractorFilePath, keywordExtractorFunctionName, getNextPostingFunctionName, finalizeKeywordExtractorFunctionName, keywordExtractorNo);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eBADPARAMETER_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_Text_AddDefaultKeywordExtractor(
	OOSQL_SystemHandle* systemHandle,							// IN  
	Four				volID,									// IN  
	char*				keywordExtractor,						// IN  
	Four				version,								// IN  
	char*				keywordExtractorFilePath,				// IN 
	char*				keywordExtractorFunctionName,			// IN  
	char*				getNextPostingFunctionName,				// IN  
	char*				finalizeKeywordExtractorFunctionName	// IN 
)
{
	if(!OOSQL_CHECKGDSINSTTABLE(systemHandle))
		return eBADPARAMETER_OOSQL;
	
	return LOM_Text_AddDefaultKeywordExtractor(&OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle), volID, keywordExtractor, version, keywordExtractorFilePath, keywordExtractorFunctionName, getNextPostingFunctionName, finalizeKeywordExtractorFunctionName);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eBADPARAMETER_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_Text_DropKeywordExtractor(
	OOSQL_SystemHandle* systemHandle,			// IN  
	Four				volID,					// IN  
	char*				keywordExtractorName,	// IN  
	Four				version					// IN  keyword extractor version
)
{
	if(!OOSQL_CHECKGDSINSTTABLE(systemHandle))
		return eBADPARAMETER_OOSQL;

	return LOM_Text_DropKeywordExtractor(&OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle), volID, keywordExtractorName, version);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eBADPARAMETER_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_Text_GetKeywordExtractorNo(
	OOSQL_SystemHandle* systemHandle,			// IN  
	Four				volID,					// IN 
	char*				keywordExtractorName,	// IN  
	Four				version,				// IN  keyword extractor version
	Four*				keywordExtractorNo		// OUT 
)
{
	if(!OOSQL_CHECKGDSINSTTABLE(systemHandle))
		return eBADPARAMETER_OOSQL;

	return LOM_Text_GetKeywordExtractorNo(&OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle), volID, keywordExtractorName, version, keywordExtractorNo);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eBADPARAMETER_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_Text_SetKeywordExtractor(
	OOSQL_SystemHandle* systemHandle,		// IN  
	Four				volID,				// IN  
	char*				className,			// IN  
	char*				columnName,			// IN  
	Four				keywordExtractorNo	// IN  
)
{
    Four classID;
    Four colNo;
    Four mv;
    Four attrInfo;
    Four e;

	if(!OOSQL_CHECKGDSINSTTABLE(systemHandle))
		return eBADPARAMETER_OOSQL;

    e = LOM_GetClassID(&OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle), volID, className, &classID);
    if(e < 0) OOSQL_ERROR(systemHandle, e);

    e = Catalog_GetMountTableInfo(&OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle), volID, &mv);
    if(e < 0) OOSQL_ERROR(systemHandle, e);

    e = Catalog_GetAttrInfoByName(&OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle), volID, classID, columnName, &attrInfo);
    if(e < 0) OOSQL_ERROR(systemHandle, e);

    colNo = CATALOG_GET_ATTRCOLNO(&(CATALOG_GET_ATTRINFOTBL((&OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle)), mv)[attrInfo])) - 1;

    return LOM_Text_SetKeywordExtractor(&OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle), volID, classID, colNo, keywordExtractorNo);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eBADPARAMETER_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_Text_AddFilter(
	OOSQL_SystemHandle* systemHandle,		// IN 
	Four				volID,				// IN  
	char*				filterName,			// IN  
	Four				version,			// IN  filter version
	char*				filterFilePath,		// IN  
	char*				filterFunctionName,	// IN  
	Four*				filterNo			// OUT 
)
{
	if(!OOSQL_CHECKGDSINSTTABLE(systemHandle))
		return eBADPARAMETER_OOSQL;

	return LOM_Text_AddFilter(&OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle), volID, filterName, version, filterFilePath, filterFunctionName, filterNo);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eBADPARAMETER_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_Text_DropFilter(
	OOSQL_SystemHandle* systemHandle,		// IN  
	Four				volID,				// IN  
	char*				filterName,			// IN  
	Four				version				// IN  filter version
)
{
	if(!OOSQL_CHECKGDSINSTTABLE(systemHandle))
		return eBADPARAMETER_OOSQL;

	return LOM_Text_DropFilter(&OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle), volID, filterName, version);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eBADPARAMETER_OOSQL
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_Text_GetFilterNo(
	OOSQL_SystemHandle* systemHandle,			// IN 
	Four				volID,					// IN  
	char*				filterName,				// IN  
	Four				version,				// IN  filter version
	Four*				filterNo				// OUT 
)
{
	if(!OOSQL_CHECKGDSINSTTABLE(systemHandle))
		return eBADPARAMETER_OOSQL;

	return LOM_Text_GetFilterNo(&OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle), volID, filterName, version, filterNo);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eBADPARAMETER_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_Text_SetFilter(
	OOSQL_SystemHandle* systemHandle,	// IN  
	Four				volID,			// IN  
	char*				className,		// IN  
	char*				columnName,		// IN 
	Four				filterNo		// IN  
)
{
	Four classID;
    Four colNo;
    Four mv;
    Four attrInfo;
    Four e;

	if(!OOSQL_CHECKGDSINSTTABLE(systemHandle))
		return eBADPARAMETER_OOSQL;

    e = LOM_GetClassID(&OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle), volID, className, &classID);
    if(e < 0) OOSQL_ERROR(systemHandle, e);

    e = Catalog_GetMountTableInfo(&OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle), volID, &mv);
    if(e < 0) OOSQL_ERROR(systemHandle, e);

    e = Catalog_GetAttrInfoByName(&OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle), volID, classID, columnName, &attrInfo);
    if(e < 0) OOSQL_ERROR(systemHandle, e);

    colNo = CATALOG_GET_ATTRCOLNO(&(CATALOG_GET_ATTRINFOTBL((&OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle)), mv)[attrInfo])) - 1;

	return LOM_Text_SetFilter(&OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle), volID, classID, colNo, filterNo);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eBADPARAMETER_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_Text_MakeIndex(
	OOSQL_SystemHandle* systemHandle,	// IN  
	Four				volID,			// IN  
	Four				temporaryVolId,	// IN  
	char*				className		// IN  
)
{
	if(!OOSQL_CHECKGDSINSTTABLE(systemHandle))
		return eBADPARAMETER_OOSQL;

	return LOM_Text_BatchInvertedIndexBuild(&OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle), volID, temporaryVolId, className);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eBADPARAMETER_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_Text_DefinePostingStructure(
	OOSQL_SystemHandle*			systemHandle,	// IN 
	Four						volID,			// IN  
	char*						className,		// IN  
	char*						attrName,		// IN  
	OOSQL_PostingStructureInfo*	postingInfo		// IN  
)
{
	if(!OOSQL_CHECKGDSINSTTABLE(systemHandle))
		return eBADPARAMETER_OOSQL;

	return LOM_Text_DefinePostingStructure(&OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle), volID, className, attrName, (PostingStructureInfo*)postingInfo);
}

#else /* SLIMDOWN_TEXTIR */


#include "OOSQL_StorageSystemHeaders.h"
#include "OOSQL_APIs_Internal.hxx"
#include "OOSQL_Error.h"
#include "OOSQL_ServerQuery.hxx"



Four OOSQL_Text_AddKeywordExtractor(
    OOSQL_SystemHandle* systemHandle,                           // IN  
    Four                volID,                                  // IN  
    char*               keywordExtractor,                       // IN  
    Four                version,                                // IN  
    char*               keywordExtractorFilePath,               // IN  
    char*               keywordExtractorFunctionName,           // IN  
    char*               getNextPostingFunctionName,             // IN  
    char*               finalizeKeywordExtractorFunctionName,   // IN  
    Four*               keywordExtractorNo                      // OUT
)
{
    return eTEXTIR_NOTENABLED_OOSQL;
}


Four OOSQL_Text_AddDefaultKeywordExtractor(
    OOSQL_SystemHandle* systemHandle,                           // IN  
    Four                volID,                                  // IN  
    char*               keywordExtractor,                       // IN  
    Four                version,                                // IN 
    char*               keywordExtractorFilePath,               // IN  
    char*               keywordExtractorFunctionName,           // IN  
    char*               getNextPostingFunctionName,             // IN  
    char*               finalizeKeywordExtractorFunctionName    // IN  
)
{
    return eTEXTIR_NOTENABLED_OOSQL;
}


Four OOSQL_Text_DropKeywordExtractor(
    OOSQL_SystemHandle* systemHandle,           // IN  
    Four                volID,                  // IN  
    char*               keywordExtractorName,   // IN  
    Four                version                 // IN  keyword extractor version
)
{
    return eTEXTIR_NOTENABLED_OOSQL;
}


Four OOSQL_Text_GetKeywordExtractorNo(
    OOSQL_SystemHandle* systemHandle,           // IN  
    Four                volID,                  // IN  
    char*               keywordExtractorName,   // IN  
    Four                version,                // IN  keyword extractor version
    Four*               keywordExtractorNo      // OUT 
)
{
    return eTEXTIR_NOTENABLED_OOSQL;
}


Four OOSQL_Text_SetKeywordExtractor(
    OOSQL_SystemHandle* systemHandle,       // IN  
    Four                volID,              // IN  
    char*               className,          // IN  
    char*               columnName,         // IN  
    Four                keywordExtractorNo  // IN  
)
{
    return eTEXTIR_NOTENABLED_OOSQL;
}


Four OOSQL_Text_AddFilter(
    OOSQL_SystemHandle* systemHandle,       // IN  
    Four                volID,              // IN 
    char*               filterName,         // IN  
    Four                version,            // IN  filter version
    char*               filterFilePath,     // IN  
    char*               filterFunctionName, // IN  
    Four*               filterNo            // OUT 
)
{
    return eTEXTIR_NOTENABLED_OOSQL;
}


Four OOSQL_Text_DropFilter(
    OOSQL_SystemHandle* systemHandle,       // IN  
    Four                volID,              // IN  
    char*               filterName,         // IN  
    Four                version             // IN  filter version
)
{
    return eTEXTIR_NOTENABLED_OOSQL;
}


Four OOSQL_Text_GetFilterNo(
    OOSQL_SystemHandle* systemHandle,           // IN  
    Four                volID,                  // IN  
    char*               filterName,             // IN  
    Four                version,                // IN  filter version
    Four*               filterNo                // OUT 
)
{
    return eTEXTIR_NOTENABLED_OOSQL;
}


Four OOSQL_Text_SetFilter(
    OOSQL_SystemHandle* systemHandle,   // IN  
    Four                volID,          // IN  
    char*               className,      // IN  
    char*               columnName,     // IN  
    Four                filterNo        // IN  
)
{
    return eTEXTIR_NOTENABLED_OOSQL;
}



Four OOSQL_Text_MakeIndex(
    OOSQL_SystemHandle* systemHandle,   // IN 
    Four                volID,          // IN  
    Four                temporaryVolId, // IN  
    char*               className       // IN  
)
{
    return eTEXTIR_NOTENABLED_OOSQL;
}


Four OOSQL_Text_DefinePostingStructure(
    OOSQL_SystemHandle*         systemHandle,   // IN  
    Four                        volID,          // IN  
    char*                       className,      // IN  
    char*                       attrName,       // IN 
    OOSQL_PostingStructureInfo* postingInfo     // IN  
)
{
    return eTEXTIR_NOTENABLED_OOSQL;
}

#endif /* SLIMDOWN_TEXTIR */


