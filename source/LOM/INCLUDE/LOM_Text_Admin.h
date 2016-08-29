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

Four lom_Text_CreateFilterCatalog(
	LOM_Handle *handle, 
	Four volId
);

Four lom_Text_DestroyFilterCatalog(
	LOM_Handle *handle, 
	Four volId
);

Four lom_Text_CreateKeywordExtractorCatalog(
	LOM_Handle *handle, 
	Four volId
);

Four lom_Text_DestroyKeywordExtractorCatalog(
	LOM_Handle *handle, 
	Four volId
);

Four lom_Text_CreatePreferenceCatalog(
	LOM_Handle *handle, 
	Four volId
);

Four lom_Text_DestroyPreferenceCatalog(
	LOM_Handle *handle, 
	Four volId
);

Four lom_Text_CreateStemizerInfoCatalog(
	LOM_Handle *handle, 
	Four volId
);

Four lom_Text_DestroyStemizerInfoCatalog(
	LOM_Handle *handle, 
	Four volId
);

Four LOM_Text_AddFilter(
	LOM_Handle *handle, 
	Four volId,
	char *filterName,
	Four version,
	char *filterFilePath,
	char *filterFunctionName,
	Four *filterNo
);

Four LOM_Text_DropFilter(
	LOM_Handle *handle, 
	Four volId,
	char *filterName,
	Four version
);

Four LOM_Text_GetFilterNo(
	LOM_Handle *handle, 
	Four volId,
	char *filterName,
	Four version,
	Four *filterNo
);

Four LOM_Text_SetFilter(
	LOM_Handle *handle, 
	Four volId,
	Four classID,
	Four colNo,
	Four filterNo
);

Four LOM_Text_GetFilterInfo(
	LOM_Handle *handle, 
	Four volId,
	Four filterNo,
	char *filePath,
	char *functionName
);

Four LOM_Text_AddKeywordExtractor(
	LOM_Handle *handle, 
	Four volId,
	char *keywordExtractor,
	Four version,
	char *keywordExtractorFilePath,
	char *keywordExtractorFunctionName,
	char *getNextPostingFunctionName,
	char *finalizeKeywordExtractorFunctionName,
	Four *keywordExtractorNo
);

Four LOM_Text_AddDefaultKeywordExtractor(
	LOM_Handle *handle, 
	Four volId,
	char *keywordExtractor,
	Four version,
	char *keywordExtractorFilePath,
	char *keywordExtractorFunctionName,
	char *getNextPostingFunctionName,
	char *finalizeKeywordExtractorFunctionName
);

Four LOM_Text_DropKeywordExtractor(
	LOM_Handle *handle, 
	Four volId,
	char *keywordExtractorName,
	Four version
);

Four LOM_Text_GetKeywordExtractorNo(
	LOM_Handle *handle, 
	Four volId,
	char *keywordExtratorName,
	Four version,
	Four *keywordExtractorNo
);

Four LOM_Text_SetKeywordExtractor(
	LOM_Handle *handle, 
	Four volId,
	Four classID,
	Four colNo,
	Four keywordExtractorNo
);

Four LOM_Text_GetKeywordExtractorInfo(
	LOM_Handle *handle, 
	Four volId,
	Four keywordExtractorNo,
	char *filePath,
	char *functionName,
	char *getNextPostingInfoFunctionName,
	char *finalizeKeywordExtractorFunctionName
);

Four LOM_Text_GetCurrentFilterNo (
	LOM_Handle *handle, 
	Four volId,
	Four classId,
	Four colNo,
	Four *filterNo
);

Four LOM_Text_GetCurrentKeywordExtractorNo (
	LOM_Handle *handle, 
	Four volId,
	Four classId,
	Four colNo,
	Four *KeywordExtractorNo
);

Four LOM_Text_OpenHandleForFilter(
	LOM_Handle *handle, 
	Four volId,
	Four classId,
	Four colNo,
	void **fhandle,
	lom_FptrToFilter *fptrToFilter
);

Four LOM_Text_OpenHandleForKeywordExtractor(
	LOM_Handle *handle, 
	Four volId,
	Four classId,
	Four colNo,
	void **fhandle,
	lom_FptrToKeywordExtractor *fptrToKeywordExtractor,
	lom_FptrToGetNextPostingInfo *fptrToGettingNextPostingInfo,
	lom_FptrToFinalizeKeywordExtraction *fptrToFinalizeKeywordExtraction
);

Four LOM_Text_CloseHandle(
	LOM_Handle *handle, 
	void *fhandle
);

Four lom_Text_GetKeywordExtractorFPtr(
	LOM_Handle *handle, 
	Four ocn,
	Four colNo,
	lom_FptrToKeywordExtractor *fptr
);

Four lom_Text_GetGetNextPostingInfoFPtr(
	LOM_Handle *handle, 
	Four ocn,
	Four colNo,
	lom_FptrToGetNextPostingInfo *fptr
);

Four lom_Text_GetFinalizeKeywordExtractionFPtr(
	LOM_Handle *handle,
	Four ocn,
	Four colNo,
	lom_FptrToFinalizeKeywordExtraction *fptr
);

Four lom_Text_GetFilterFPtr(
	LOM_Handle *handle, 
	Four ocn,
	Four colNo,
	lom_FptrToFilter *fptr
);

Four LOM_Text_ResetFilter(
	LOM_Handle *handle, 
	Four volId,
	Four classID,
	Four colNo
);

Four LOM_Text_ResetKeywordExtractor(
	LOM_Handle *handle, 
	Four volId,
	Four classID,
	Four colNo
);

Four LOM_Text_AddStemizer(
	LOM_Handle *handle, 
	Four volId,
	char *stemizerName,
	Four version,
	char *stemizerFilePath,
	char *stemizerFunctionName,
	Four *stemizerNo
);

Four LOM_Text_AddDefaultStemizer(
	LOM_Handle *handle, 
	Four volId,
	char *stemizerName,
	Four version,
	char *stemizerFilePath,
	char *stemizerFunctionName
);

Four LOM_Text_DropStemizer(
	LOM_Handle *handle, 
	Four volId,
	char *stemizerName,
	Four version
);

Four LOM_Text_GetStemizerNo(
	LOM_Handle *handle, 
	Four volId,
	char *stemizerName,
	Four version,
	Four *stemizerNo
);

Four LOM_Text_SetStemizer(
	LOM_Handle *handle, 
	Four volId,
	Four classID,
	Four colNo,
	Four stemizerNo
);

Four LOM_Text_GetStemizerInfo(
	LOM_Handle *handle, 
	Four volId,
	Four stemizerNo,
	char *stemizerFilePath,
	char *stemizerFunctionName
);

