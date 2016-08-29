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

// Include Header file
#include "Export.hxx"


Boolean export_checkSystemDefinedClasses (
    const char* className)
{
	char	delStr[MAXSIZEOFNAMESTR];
	int		delStrSize;


    static char* systemDefinedClassNames[] = {
        "lrdsSysTables", "lrdsSysColumns", "lrdsSysIndexes", 
        "lomSysClasses", "lomInheritance", "lomSysColumns", "lomSysIndexes",
        "lomSysMethods", "lomSysRelationship", "lomClassId", "lomRelationshipId",
        "lomTextFilterId", "lomTextKeywordExtractorId", "lomTextStemizerId", "lomSysTextIndexes",
        "lomSysTextFilterInfo", "lomSysTextKeywordExtractorInfo", "lomSysTextStemizerInfo", "lomSysTextPreferences",
        "LOM_SYS_FUNCTIONS", "LOM_SYS_FUNCPARMS", "LOM_SYS_PROCEDURES", "LOM_SYS_PROCPARMS",
		"_LOM_SYS_FUNCTIONS_DeletionList", "_LOM_SYS_FUNCPARMS_DeletionList", 
		"_LOM_SYS_PROCEDURES_DeletionList", "_LOM_SYS_PROCPARMS_DeletionList",
            
		"web_page", "MultimediaObj", "Image", "externalFunction", 
		"webMovie", "webImage", "webAudio", "webDocument", 
		"parameters", "Document", "Audio", "Video", 

        NULL
    };

    for(int i = 0; systemDefinedClassNames[i] != NULL; i++) 
    {
        if(!strcmp(systemDefinedClassNames[i], className))
            return (Boolean)TRUE;
    }

	strcpy(delStr, "_Inverted");
	delStrSize = strlen(delStr);
    if (className[0] == '_' && !strncmp(&className[strlen(className)-delStrSize], delStr, delStrSize)) 
        return (Boolean)TRUE;

	strcpy(delStr, "_docId");
	delStrSize = strlen(delStr);
    if (className[0] == '_' && !strncmp(&className[strlen(className)-delStrSize], delStr, delStrSize)) 
        return (Boolean)TRUE;

	strcpy(delStr, "_DeletionList");
	delStrSize = strlen(delStr);
    if (className[0] == '_' && !strncmp(&className[strlen(className)-delStrSize], delStr, delStrSize)) 
        return (Boolean)TRUE;


    return (Boolean)FALSE;
}


Four export_GetClassNames(
    ExportConfig&       configuration,  // IN/OUT
    Array<String>&      classNames)     // OUT
{
    Four            i;
    Four            e;
    Four            orn;                        // Open Relation Number
    Four            scanId;                     // scan identifier
    LockParameter   lockup;
    TupleID         tid;                        // a tuple identifier
    ColListStruct   clist[1];                   // column list
    char            relName[LOM_MAXCLASSNAME];  // relation name

    orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(&configuration.handle.lomSystemHandle), configuration.volumeId, LOM_SYSCLASSES_CLASSNAME);
    LRDS_CHECK_ERR(orn);

    lockup.mode     = L_S;
    lockup.duration = L_COMMIT;
    scanId = LRDS_OpenSeqScan(LOM_GET_LRDS_HANDLE(&configuration.handle.lomSystemHandle), orn, FORWARD, 0, NULL, &lockup);
    LRDS_CHECK_ERR(scanId);

    clist[0].colNo = LOM_SYSCLASSES_CLASSNAME_COLNO;
    clist[0].start = ALL_VALUE;
    clist[0].dataLength = LOM_MAXCLASSNAME; 
    clist[0].data.ptr = &relName[0];

    while ((e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(&configuration.handle.lomSystemHandle), scanId, &tid, NULL)) != EOS) {
        LRDS_CHECK_ERR(e);

        e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(&configuration.handle.lomSystemHandle), scanId, (Boolean)TRUE, &tid, 1, &clist[0]);
        LRDS_CHECK_ERR(e);
            
        relName[clist[LOM_SYSCLASSES_CLASSNAME_COLNO].retLength] = '\0';


        if(!export_checkSystemDefinedClasses(relName))
        {
            classNames.add(String(relName));
        }
    }

    e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(&configuration.handle.lomSystemHandle), scanId);
    LRDS_CHECK_ERR(e);

    e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(&configuration.handle.lomSystemHandle), orn);
    LRDS_CHECK_ERR(e);


    return eNOERROR;
}


Four export_GetInstalledKEInfo (
    ExportConfig&                   configuration,      // IN
    Array<ExportInstalledKEInfo>&   installedKEInfos)   // OUT
{
    Four                i;
    Four                e;
    Four                orn;                        // Open Relation Number
    Four                scanId;                     // scan identifier
    LockParameter       lockup;
    TupleID             tid;                        // a tuple identifier
    ColListStruct       clist[LOM_SYSTEXTKEYWORDEXTRACTORINFO_NUM_COLS];    // column list
    char                names[LOM_SYSTEXTKEYWORDEXTRACTORINFO_NUM_COLS][MAXSIZEOFNAMESTR];  // relation name


    orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(&configuration.handle.lomSystemHandle), configuration.volumeId, LOM_SYSTEXTKEYWORDEXTRACTORINFO_CLASSNAME); 
    LRDS_CHECK_ERR(orn);

    lockup.mode     = L_S;
    lockup.duration = L_COMMIT;
    scanId = LRDS_OpenSeqScan(LOM_GET_LRDS_HANDLE(&configuration.handle.lomSystemHandle), orn, FORWARD, 0, NULL, &lockup);
    LRDS_CHECK_ERR(scanId);

    clist[0].colNo = LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORNAME_COLNO;
    clist[0].start = ALL_VALUE;
    clist[0].length = ALL_VALUE;
    clist[0].dataLength = MAXSIZEOFNAMESTR; 
    clist[0].data.ptr = &names[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORNAME_COLNO];

    clist[1].colNo = LOM_SYSTEXTKEYWORDEXTRACTORINFO_VERSION_COLNO;
    clist[1].start = ALL_VALUE;
    clist[1].length = ALL_VALUE;

    clist[2].colNo = LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORFILEPATH_COLNO;
    clist[2].start = ALL_VALUE;
    clist[2].length = ALL_VALUE;
    clist[2].dataLength = MAXSIZEOFNAMESTR; 
    clist[2].data.ptr = &names[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORFILEPATH_COLNO];

    clist[3].colNo = LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORFUNCTIONNAME_COLNO;
    clist[3].start = ALL_VALUE;
    clist[3].length = ALL_VALUE;
    clist[3].dataLength = MAXSIZEOFNAMESTR; 
    clist[3].data.ptr = &names[LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORFUNCTIONNAME_COLNO];

    clist[4].colNo = LOM_SYSTEXTKEYWORDEXTRACTORINFO_GETNEXTPOSTINGINFOFUNCTIONNAME_COLNO;
    clist[4].start = ALL_VALUE;
    clist[4].length = ALL_VALUE;
    clist[4].dataLength = MAXSIZEOFNAMESTR; 
    clist[4].data.ptr = &names[LOM_SYSTEXTKEYWORDEXTRACTORINFO_GETNEXTPOSTINGINFOFUNCTIONNAME_COLNO];

    clist[5].colNo = LOM_SYSTEXTKEYWORDEXTRACTORINFO_FINALIZEKEYWORDEXTRACTORFUNCTIONNAME_COLNO;
    clist[5].start = ALL_VALUE;
    clist[5].length = ALL_VALUE;
    clist[5].dataLength = MAXSIZEOFNAMESTR; 
    clist[5].data.ptr = &names[LOM_SYSTEXTKEYWORDEXTRACTORINFO_FINALIZEKEYWORDEXTRACTORFUNCTIONNAME_COLNO];

    clist[6].colNo = LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORNO_COLNO;
    clist[6].start = ALL_VALUE;
    clist[6].length = ALL_VALUE;


    while ((e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(&configuration.handle.lomSystemHandle), scanId, &tid, NULL)) != EOS) {
        LRDS_CHECK_ERR(e);

        ExportInstalledKEInfo   installedKEInfo;


        e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(&configuration.handle.lomSystemHandle), scanId, (Boolean)TRUE, &tid, LOM_SYSTEXTKEYWORDEXTRACTORINFO_NUM_COLS, &clist[0]);
        LRDS_CHECK_ERR(e);
            
        i = LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORNAME_COLNO; 
        names[i][clist[i].retLength] = '\0';
        i = LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORFILEPATH_COLNO;
        names[i][clist[i].retLength] = '\0';
        i = LOM_SYSTEXTKEYWORDEXTRACTORINFO_KEYWORDEXTRACTORFUNCTIONNAME_COLNO;
        names[i][clist[i].retLength] = '\0';
        i = LOM_SYSTEXTKEYWORDEXTRACTORINFO_GETNEXTPOSTINGINFOFUNCTIONNAME_COLNO;
        names[i][clist[i].retLength] = '\0';
        i = LOM_SYSTEXTKEYWORDEXTRACTORINFO_FINALIZEKEYWORDEXTRACTORFUNCTIONNAME_COLNO;
        names[i][clist[i].retLength] = '\0';


        installedKEInfo.keywordExtractorName = names[0];  
        installedKEInfo.version = GET_VALUE_FROM_COL_LIST(clist[1], sizeof(Four));
        installedKEInfo.keywordExtractorFilePath = names[2];
        installedKEInfo.keywordExtractorFunctionName = names[3];
        installedKEInfo.getNextPostingInfoFunctionName = names[4];
        installedKEInfo.finalizeKeywordExtractorFunctionName = names[5];
        installedKEInfo.keywordExtractorNo = GET_VALUE_FROM_COL_LIST(clist[6], sizeof(Four));


        installedKEInfos.add(installedKEInfo);
    }

    e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(&configuration.handle.lomSystemHandle), scanId);
    LRDS_CHECK_ERR(e);

    e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(&configuration.handle.lomSystemHandle), orn);
    LRDS_CHECK_ERR(e);


    return eNOERROR;
}



Four export_GetSetupedKEInfo (
    ExportConfig&                   configuration,      // IN
    Array<ExportSetupedKEInfo>&     setupedKEInfos)     // OUT
{
    Four                i;
    Four                e;
    Four                orn;                        // Open Relation Number
    Four                scanId;                     // scan identifier
    LockParameter       lockup;
    TupleID             tid;                        // a tuple identifier
    ColListStruct       clist[LOM_SYSTEXTSTEMIZERINFO_NUM_COLS];    // column list
    char                names[LOM_SYSTEXTSTEMIZERINFO_NUM_COLS][MAXSIZEOFNAMESTR];  // relation name


    orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(&configuration.handle.lomSystemHandle), configuration.volumeId, LOM_SYSTEXTPREFERENCES_CLASSNAME); 
    LRDS_CHECK_ERR(orn);

    lockup.mode     = L_S;
    lockup.duration = L_COMMIT;
    scanId = LRDS_OpenSeqScan(LOM_GET_LRDS_HANDLE(&configuration.handle.lomSystemHandle), orn, FORWARD, 0, NULL, &lockup);
    LRDS_CHECK_ERR(scanId);

    clist[0].colNo = LOM_SYSTEXTPREFERENCES_CLASSID_COLNO;
    clist[0].start = ALL_VALUE;
    clist[0].length = ALL_VALUE;

    clist[1].colNo = LOM_SYSTEXTPREFERENCES_COLNO_COLNO;
    clist[1].start = ALL_VALUE;
    clist[1].length = ALL_VALUE;

    clist[2].colNo = LOM_SYSTEXTPREFERENCES_FILTERNO_COLNO;
    clist[2].start = ALL_VALUE;
    clist[2].length = ALL_VALUE;

    clist[3].colNo = LOM_SYSTEXTPREFERENCES_KEYWORDEXTRACTORNO_COLNO;
    clist[3].start = ALL_VALUE;
    clist[3].length = ALL_VALUE;

    clist[4].colNo = LOM_SYSTEXTPREFERENCES_STEMIZERNO_COLNO;
    clist[4].start = ALL_VALUE;
    clist[4].length = ALL_VALUE;


    while ((e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(&configuration.handle.lomSystemHandle), scanId, &tid, NULL)) != EOS) {
        LRDS_CHECK_ERR(e);

        ExportSetupedKEInfo     setupedKEInfo;


        e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(&configuration.handle.lomSystemHandle), scanId, (Boolean)TRUE, &tid, LOM_SYSTEXTPREFERENCES_NUM_COLS, &clist[0]);
        LRDS_CHECK_ERR(e);
            

        setupedKEInfo.classId = GET_VALUE_FROM_COL_LIST(clist[0], sizeof(Four));
        setupedKEInfo.colNo = GET_VALUE_FROM_COL_LIST(clist[1], sizeof(Four));
        setupedKEInfo.filterNo = GET_VALUE_FROM_COL_LIST(clist[2], sizeof(Four));
        setupedKEInfo.keywordExtractorNo = GET_VALUE_FROM_COL_LIST(clist[3], sizeof(Four));
        setupedKEInfo.stemizerNo = GET_VALUE_FROM_COL_LIST(clist[4], sizeof(Four));


        setupedKEInfos.add(setupedKEInfo);
    }

    e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(&configuration.handle.lomSystemHandle), scanId);
    LRDS_CHECK_ERR(e);

    e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(&configuration.handle.lomSystemHandle), orn);
    LRDS_CHECK_ERR(e);


    return eNOERROR;
}
