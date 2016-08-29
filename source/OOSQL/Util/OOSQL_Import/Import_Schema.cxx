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
#include "Import.hxx"



Four Import_ImportSchema (
    ImportConfig&                   configuration,          // IN
    Array<ImportClassInfo>&         classInfos,             // OUT
    Array<ImportClassIdMapping>&    classIdMappingTable)    // OUT
{
    Four                            i;
    Four                            j;
    Four                            e;
    Four                            idx;
    Array<String>                   classNames;
    Array<ImportTextKeywordExtractorInfo>   extractorInfo;
    

    // 
    // In previous importing, schema import is complete
    //
    if (configuration.importLog.mainImportPhase >= ENDOFSCHEMAIMPORT)
    {
        // Get class names from "export.tbl" file
        e = import_ReadClassNames(configuration, classNames);
        TOOL_CHECK_ERR(e);

        // Get class infomation from *_scm.tmp file
        for (i = 0; i < classNames.numberOfItems(); i++)
        {
            ImportClassInfo classInfo;

            e = import_ReadClassInfo(configuration, classNames[i], classInfo, (Boolean)TRUE);
            TOOL_CHECK_ERR(e);

            classInfos.add(classInfo);
        }

        return eNOERROR;
    }



    // I. Install Keyword Extractor
    e = import_InstallKeywordExtractor(configuration, classInfos, extractorInfo);
    TOOL_CHECK_ERR(e);


    // II. Get class names from "export.tbl" file
    e = import_ReadClassNames(configuration, classNames);
    TOOL_CHECK_ERR(e);


    // III. Get class infomation from *.scm file
    for (i = 0; i < classNames.numberOfItems(); i++) 
    {
        ImportClassInfo classInfo;

        e = import_ReadClassInfo(configuration, classNames[i], classInfo, (Boolean)FALSE);
        TOOL_CHECK_ERR(e);

        classInfos.add(classInfo);
    }
    

    //
    // Begin Transaction
    //
    e = import_TransBegin(configuration);
    TOOL_CHECK_ERR(e);


    // IV. Get new classId for each tables
    for (i = 0; i < classInfos.numberOfItems(); i++) 
    {
        e = LOM_GetNewClassId(&configuration.handle.lomSystemHandle, configuration.volumeId, 
                              (Boolean)FALSE, &classInfos[i].newClassId);
        LOM_CHECK_ERR(e);
    }


    // V. Insert className & classId into classIdMappingTable
    for (i = 0; i < classInfos.numberOfItems(); i++) 
    {
        ImportClassIdMapping    classIdMapping;

        classIdMapping.className = classInfos[i].className;
        classIdMapping.oldClassId = classInfos[i].oldClassId;
        classIdMapping.newClassId = classInfos[i].newClassId;
        classIdMapping.classInfoIdx = i;

        classIdMappingTable.add(classIdMapping);
    }


    // VI. Sort classIdMappingTable by oldClassId incremental order
    String      tclassName;
    Four        toldClassId;
    Four        tnewClassId;
    Four        tclassInfoIdx;

    for (i = 1; i < classIdMappingTable.numberOfItems(); i++)
    {
        tclassName = classIdMappingTable[i].className;
        toldClassId = classIdMappingTable[i].oldClassId;
        tnewClassId = classIdMappingTable[i].newClassId;
        tclassInfoIdx = classIdMappingTable[i].classInfoIdx;

        for (j = i; j > 0; j--)
        {
            if (toldClassId < classIdMappingTable[j-1].oldClassId)
            {
                classIdMappingTable[j].className = classIdMappingTable[j-1].className;
                classIdMappingTable[j].oldClassId = classIdMappingTable[j-1].oldClassId;
                classIdMappingTable[j].newClassId = classIdMappingTable[j-1].newClassId;
                classIdMappingTable[j].classInfoIdx = classIdMappingTable[j-1].classInfoIdx;
            }
            else
            {
                break;
            }
        }
        classIdMappingTable[j].className = tclassName;
        classIdMappingTable[j].oldClassId = toldClassId;
        classIdMappingTable[j].newClassId = tnewClassId;
        classIdMappingTable[j].classInfoIdx = tclassInfoIdx;
    }


    // VII. Modify 
    //    LOM_SYSCOLUMNS - inheritedfrom
    //    LOM_SYSCOLUMNS - domainid
    //    LOM_SYSMETHODS - inheritedfrom
    Four    tInheritedFrom;
    Four    tDomainId;

    for (i = 0; i < classInfos.numberOfItems(); i++)
    {
        for (j = 0; j < classInfos[i].attrsInfo.numberOfItems(); j++)
        {
            tInheritedFrom  = import_ConvertClassId(configuration, classIdMappingTable, 
                                                    classInfos[i].attrsInfo[j].inheritedFrom);
            TOOL_CHECK_ERR(tInheritedFrom);

            tDomainId       = import_ConvertClassId(configuration, classIdMappingTable, 
                                                    classInfos[i].attrsInfo[j].domainId);
            TOOL_CHECK_ERR(tDomainId);

            classInfos[i].attrsInfo[j].inheritedFrom    = tInheritedFrom;
            classInfos[i].attrsInfo[j].domainId         = tDomainId;
        }

        for (j = 0; j < classInfos[i].methodsInfo.numberOfItems(); j++)
        {
            tInheritedFrom  = import_ConvertClassId(configuration, classIdMappingTable, 
                                                    classInfos[i].methodsInfo[j].inheritedFrom);
            TOOL_CHECK_ERR(tInheritedFrom);
            classInfos[i].methodsInfo[j].inheritedFrom = tInheritedFrom;
        }
    }


    // VIII. Check className existence in database
    Array<String>       classNamesInDB;

    // 1. Fetch class names from database
    e = import_GetClassNames(configuration, classNamesInDB);
    TOOL_CHECK_ERR(e);
    
    // 2. Check className existence in database
    for (i = 0; i < classIdMappingTable.numberOfItems(); i++) 
    {
        idx = classIdMappingTable[i].classInfoIdx;

        for (j = 0; j < classNamesInDB.numberOfItems(); j++)
        {
            if (strcmp((const char *)classInfos[idx].className, (const char*) classNamesInDB[j]) == 0)
            {
                configuration.errorMessage  = classInfos[idx].className;
                configuration.errorMessage += " table exist already in ";
                configuration.errorMessage += configuration.databaseName;
                configuration.errorMessage += " database.";
                TOOL_ERR(eALREADY_EXIST_TABLE_IMPORT);
            }
        }
    }


    // IX. Create classes by oldClassId order
    for (i = 0; i < classIdMappingTable.numberOfItems(); i++) 
    {
        idx = classIdMappingTable[i].classInfoIdx;

        e = import_CreateClass(configuration, classInfos[idx]);
        TOOL_CHECK_ERR(e);

        e = import_WriteClassInfo(configuration, classInfos[idx]);
        TOOL_CHECK_ERR(e);
    }


    //
    // Commit Transaction
    //
    e = import_TransCommit(configuration);
    TOOL_CHECK_ERR(e);


    // X. Setup Keyword Extractor
    e = import_SetupKeywordExtractor(configuration, classInfos, extractorInfo);
    TOOL_CHECK_ERR(e);


    // Write log
    configuration.importLog.mainImportPhase = ENDOFSCHEMAIMPORT;
    e = import_WriteLog(configuration);
    TOOL_CHECK_ERR(e);


    return eNOERROR;
}



Four import_ConvertClassId (
    ImportConfig&                       configuration,          // IN
    const Array<ImportClassIdMapping>&  classIdMappingTable,    // IN
    Four                                oldClassId)             // IN
{
    Four                                i;


    // if oldClassId value is not classId
    if (oldClassId < INITIAL_CLASSID)
        return oldClassId;

    // Find matched newClassId and return that value
    for (i = 0; i < classIdMappingTable.numberOfItems(); i++)
    {
        if (oldClassId == classIdMappingTable[i].oldClassId)
        {
            return classIdMappingTable[i].newClassId;
        }
    }


    // Unhandled case occur
    if (i == classIdMappingTable.numberOfItems())
    {
        configuration.errorMessage  = "Subclass must be imported with that's superclass.";
        TOOL_ERR(eUNHANDLED_CASE_IMPORT);
    }

    return eUNHANDLED_CASE_IMPORT;
}



Four import_ReadClassNames (
    ImportConfig&       configuration,  // IN
    Array<String>&      classNames)     // OUT
{
    Array<String>       allClassNames;
    FILE                *fp;
    char                *chk;
    char                fileName[MAXFILENAME];
    char                tableName[MAXSIZEOFLINEBUFFER];
    Four                i, j;
    Four                e;


    // I-1. make data file name 
    strcpy(fileName, (const char *)configuration.dirPath);
    strcat(fileName, DIRECTORY_SEPARATOR);
    strcat(fileName, TABLEFILENAME);
    
    // I-2. Open "export.tbl" file to be imported
    fp = Util_fopen(fileName, "rb");
    if (fp == NULL) 
    {
        configuration.errorMessage  = "Can't open '";
        configuration.errorMessage += fileName;
        configuration.errorMessage += "' file";
        TOOL_ERR(eCANNOT_OPEN_FILE_IMPORT);
    }


    // II. Read "export.tbl" file
    chk = fgets(tableName, MAXSIZEOFLINEBUFFER, fp);
    while (chk != NULL)
    {
        tableName[strlen(tableName)-1] = '\0';
        allClassNames.add(String(tableName));

        chk = fgets(tableName, MAXSIZEOFLINEBUFFER, fp);
    }


    // III. Fill classNames
    if (configuration.full == (Boolean)TRUE) 
    {
        for (i = 0; i < allClassNames.numberOfItems(); i++)
        {
            classNames.add(allClassNames[i]);
        }
    }
    else 
    {
        for (i = 0; i < configuration.tables.numberOfItems(); i++)
        {
            for (j = 0; j < allClassNames.numberOfItems(); j++)
            {
                if (strcmp((const char *)allClassNames[j], (const char *)configuration.tables[i]) == 0)
                {
                    classNames.add(configuration.tables[i]);
                    break;
                }
            }
            if (j == allClassNames.numberOfItems())
            {
                configuration.errorMessage  = "There is not ";
                configuration.errorMessage += configuration.tables[i];
                configuration.errorMessage += " table data in exported data.";
                TOOL_ERR(eNO_EXIST_DATA_IMPORT);
            }            
        }
    }


    // IV. Close "export.tbl" file
    e = Util_fclose(fp);
    if (e == EOF) 
    {
        configuration.errorMessage  = "Can't close '";
        configuration.errorMessage += fileName;
        configuration.errorMessage += "' file";
        TOOL_ERR(eCANNOT_CLOSE_FILE_IMPORT);
    }

    return eNOERROR;
}



Four import_ReadClassInfo (
    ImportConfig&       configuration,      // IN/OUT
    const String&       className,          // IN
    ImportClassInfo&    classInfo,          // OUT
    Boolean             tmpRead)            // OUT
{
    FILE                *fp;
    char                *chk;
    char                *token;
    char                *token1;
    char                *token2;
    char                fileName[MAXFILENAME];
    char                lineBuff[MAXSIZEOFLINEBUFFER];
    char                paramBuff[MAXSIZEOFLINEBUFFER];
    Four                i, j;
    Four                e;


    // I-1. make data file name 
    strcpy(fileName, (const char *)configuration.dirPath);
    strcat(fileName, DIRECTORY_SEPARATOR);
    strcat(fileName, (const char *)className);
    if (tmpRead == (Boolean)TRUE)   strcat(fileName, "_scm.tmp");
    else                            strcat(fileName, ".scm");
    
    // I-2. Open schema file to be imported
    fp = Util_fopen(fileName, "rb");
    if (fp == NULL) 
    {
        configuration.errorMessage  = "Can't open '";
        configuration.errorMessage += fileName;
        configuration.errorMessage += "' file";
        TOOL_ERR(eCANNOT_OPEN_FILE_IMPORT);
    }


    // II. Read <className>.scm file
    Four        step = 0;
    Four        p0, p1, p2;
    char        *subStr;
    char        strBuff1[MAXSIZEOFLINEBUFFER];
    char        strBuff2[MAXSIZEOFLINEBUFFER];
    char        strBuff3[MAXSIZEOFLINEBUFFER];
    char        strBuff4[MAXSIZEOFLINEBUFFER];

    // initialize
    classInfo.nAttrs        = 0;
    classInfo.nMethods      = 0;
    classInfo.nSuperclasses = 0;
    classInfo.nIndexes      = 0;

    chk = fgets(lineBuff, MAXSIZEOFLINEBUFFER, fp);
    while (chk != NULL)
    {
        lineBuff[strlen(lineBuff)-1] = '\0';

        if (lineBuff[0] == '#')
        {
            step ++;
        }
        else
        {
            ImportAttrInfo          attrInfo;
            ImportMethodInfo        methodInfo;
            ImportSuperclassInfo    superclassInfo;
            ImportIndexInfo         indexInfo;

            switch(step)
            {
            case 1 :    // className
                sscanf(lineBuff, "%s", strBuff1);
                classInfo.className = strBuff1;
                break;


            case 2 :    // classId
                if (tmpRead == (Boolean)TRUE)   
                    sscanf(lineBuff, "%ld", &classInfo.newClassId);
                else 
                {
                    sscanf(lineBuff, "%ld", &classInfo.oldClassId);
                    classInfo.newClassId = classInfo.oldClassId;
                }
                break;


            case 3 :    // attributes
                token = strtok(lineBuff, " ");
                strcpy(paramBuff, token);
                attrInfo.columnNo       = atol(paramBuff);

                token = strtok(NULL, " ");
                strcpy(paramBuff, token);
                attrInfo.complexType    = atol(paramBuff);
            
                token = strtok(NULL, " ");
                strcpy(paramBuff, token);
                attrInfo.colType        = atol(paramBuff);

                token = strtok(NULL, " ");
                strcpy(paramBuff, token);
                attrInfo.length         = atol(paramBuff);

                token = strtok(NULL, " ");
                strcpy(paramBuff, token);
                attrInfo.colName = paramBuff;

                token = strtok(NULL, " ");
                strcpy(paramBuff, token);
                attrInfo.inheritedFrom  = atol(paramBuff);

                token = strtok(NULL, " ");
                strcpy(paramBuff, token);
                attrInfo.domainId       = atol(paramBuff);


                classInfo.attrsInfo.add(attrInfo);
                classInfo.nAttrs ++;
                break;


            case 4 :    // methods
                for (p1 = 0; p1 < strlen(lineBuff); p1++)
                    if (lineBuff[p1] == '[')    break;
                for (p2 = 0; p2 < strlen(lineBuff); p2++)
                    if (lineBuff[p2] == ']')    break;


                // phase 1
                strcpy(strBuff1, lineBuff);
                strBuff1[p1] = '\0';
                strBuff1[p2] = '\0';

                token = strtok(strBuff1, " ");
                strcpy(paramBuff, token);
                methodInfo.dirPath      = paramBuff;

                token = strtok(NULL, " ");
                strcpy(paramBuff, token);
                methodInfo.methodName   = paramBuff;

                token = strtok(NULL, " ");
                strcpy(paramBuff, token);
                methodInfo.functionName = paramBuff;

                token = strtok(NULL, " ");
                strcpy(paramBuff, token);
                methodInfo.nArguments   = atol(paramBuff);


                // phase 2
                strcpy(strBuff2, lineBuff);
                strBuff2[p1] = '\0';
                strBuff2[p2] = '\0';

                token = strtok(&strBuff2[p1+1], " ");
                while (token != NULL)
                {
                    Four    argument;

                    strcpy(paramBuff, token);
                    argument    = atol(paramBuff);

                    methodInfo.ArgumentsList.add(argument);

                    token = strtok(NULL, " ");
                }


                // phase 3
                strcpy(strBuff3, lineBuff);
                strBuff3[p1] = '\0';
                strBuff3[p2] = '\0';

                token = strtok(&strBuff3[p2+1], " ");
                strcpy(paramBuff, token);
                methodInfo.returnType       = atol(paramBuff);

                token = strtok(NULL, " ");
                strcpy(paramBuff, token);
                methodInfo.inheritedFrom    = atol(paramBuff);


                classInfo.methodsInfo.add(methodInfo);
                classInfo.nMethods ++;
                break;


            case 5 :    // superclasses

                token = strtok(lineBuff, " ");
                strcpy(paramBuff, token);
                superclassInfo.superclassId     = atol(paramBuff);

                token = strtok(NULL, " ");
                strcpy(paramBuff, token);
                superclassInfo.superclassName   = paramBuff;

                classInfo.superclassesInfo.add(superclassInfo);
                classInfo.nSuperclasses ++;

                break;


            case 6 :    // indexes

                // phase 1
                strcpy(strBuff1, lineBuff);

                token = strtok(strBuff1, " ");
                strcpy(paramBuff, token);
                indexInfo.indexName     = paramBuff;

                token = strtok(NULL, " ");
                strcpy(paramBuff, token);
                indexInfo.indexType     = atol(paramBuff);


                switch(indexInfo.indexType)
                {
                case LOM_INDEXTYPE_BTREE:

                    for (p0 = 0; p0 < strlen(lineBuff); p0++)
                        if (strncmp(&lineBuff[p0], "btree(", 6) == 0)   break;
                    for (p1 = 0; p1 < strlen(lineBuff); p1++)
                        if (lineBuff[p1] == '[')    break;
                    for (p2 = 0; p2 < strlen(lineBuff); p2++)
                        if (lineBuff[p2] == ']')    break;


                    // phase 2
                    strcpy(strBuff2, lineBuff);
                    strBuff2[p1] = '\0';
                    strBuff2[p2] = '\0';

                    token = strtok(&strBuff2[p0+6], " ");
                    strcpy(paramBuff, token);
                    indexInfo.btree.flag        = atol(paramBuff);

                    token = strtok(NULL, " ");
                    strcpy(paramBuff, token);
                    indexInfo.btree.nColumns    = atol(paramBuff);


                    // phase 3
                    strcpy(strBuff3, lineBuff);
                    strBuff3[p1] = '\0';
                    strBuff3[p2] = '\0';

                    token1 = strtok(&strBuff3[p1+1], " ()");
                    token2 = strtok(NULL, " ()");
                    while (token1 != NULL && token2 != NULL)
                    {
                        Importcolumn        column;

                        strcpy(paramBuff, token1);
                        column.colNo    = atol(paramBuff);

                        strcpy(paramBuff, token2);
                        column.flag     = atol(paramBuff);

                        indexInfo.btree.columns.add(column);

                        token1 = strtok(NULL, " ()");
                        token2 = strtok(NULL, " ()");
                    }
                    break;

                case LOM_INDEXTYPE_MLGF:
                    for (p0 = 0; p0 < strlen(lineBuff); p0++)
                        if (strncmp(&lineBuff[p0], "mlgf(", 5) == 0)   break;
                    for (p1 = 0; p1 < strlen(lineBuff); p1++)
                        if (lineBuff[p1] == '[')    break;
                    for (p2 = 0; p2 < strlen(lineBuff); p2++)
                        if (lineBuff[p2] == ']')    break;


                    // phase 2
                    strcpy(strBuff2, lineBuff);
                    strBuff2[p1] = '\0';
                    strBuff2[p2] = '\0';

                    token = strtok(&strBuff2[p0+5], " ");
                    strcpy(paramBuff, token);
                    indexInfo.mlgf.flag     = atol(paramBuff);

                    token = strtok(NULL, " ");
                    strcpy(paramBuff, token);
                    indexInfo.mlgf.nColumns = atol(paramBuff);


                    // phase 3
                    strcpy(strBuff3, lineBuff);
                    strBuff3[p1] = '\0';
                    strBuff3[p2] = '\0';

                    token = strtok(&strBuff3[p1+1], " ");
                    while (token != NULL)
                    {
                        Two     colNo;

                        strcpy(paramBuff, token);
                        colNo   = atol(paramBuff);

                        indexInfo.mlgf.colNo.add(colNo);

                        token = strtok(NULL, " ");
                    }

                    // phase 4
                    strcpy(strBuff4, lineBuff);
                    strBuff4[p1] = '\0';
                    strBuff4[p2] = '\0';

                    token = strtok(&strBuff4[p2+1], " ");
                    strcpy(paramBuff, token);
                    indexInfo.mlgf.extraDataLen = atol(paramBuff);
                    break;
    
                case LOM_INDEXTYPE_TEXT:
                    for (p0 = 0; p0 < strlen(lineBuff); p0++)
                        if (strncmp(&lineBuff[p0], "text(", 5) == 0)   break;
                    for (p1 = 0; p1 < strlen(lineBuff); p1++)
                        if (lineBuff[p1] == '[')    break;
                    for (p2 = 0; p2 < strlen(lineBuff); p2++)
                        if (lineBuff[p2] == ']')    break;


                    // phase 2
                    strcpy(strBuff2, lineBuff);
                    strBuff2[p1] = '\0';
                    strBuff2[p2] = '\0';

                    token = strtok(&strBuff2[p0+5], " ");
                    strcpy(paramBuff, token);
                    indexInfo.invertedIndex.colName = paramBuff;

                    token = strtok(NULL, " ");
                    strcpy(paramBuff, token);
                    indexInfo.invertedIndex.postingInfo.isContainingTupleID = (Boolean)atol(paramBuff);

                    token = strtok(NULL, " ");
                    strcpy(paramBuff, token);
                    indexInfo.invertedIndex.postingInfo.isContainingSentenceAndWordNum = (Boolean)atol(paramBuff);

                    token = strtok(NULL, " ");
                    strcpy(paramBuff, token);
                    indexInfo.invertedIndex.postingInfo.isContainingByteOffset = (Boolean)atol(paramBuff);

                    token = strtok(NULL, " ");
                    strcpy(paramBuff, token);
                    indexInfo.invertedIndex.postingInfo.nEmbeddedAttributes = (Two)atol(paramBuff);


                    // phase 3
                    strcpy(strBuff3, lineBuff);
                    strBuff3[p1] = '\0';
                    strBuff3[p2] = '\0';

                    token = strtok(&strBuff3[p1+1], " ");
                    while (token != NULL)
                    {
                        Two     AttrNo;

                        strcpy(paramBuff, token);
                        AttrNo   = atol(paramBuff);

                        indexInfo.invertedIndex.postingInfo.embeddedAttrNo.add(AttrNo);

                        token = strtok(NULL, " ");
                    }
                    break;

                default:
                    break;
                }
                
                classInfo.indexesInfo.add(indexInfo);
                classInfo.nIndexes ++;
                break;

            default :
                break;
            }
        }

        chk = fgets(lineBuff, MAXSIZEOFLINEBUFFER, fp);
    }
    

    // III. Close schema file
    e = Util_fclose(fp);
    if (e == EOF) 
    {
        configuration.errorMessage  = "Can't close '";
        configuration.errorMessage += fileName;
        configuration.errorMessage += "' file";
        TOOL_ERR(eCANNOT_CLOSE_FILE_IMPORT);
    }

    return eNOERROR;
}



Four import_CreateClass (
    ImportConfig&           configuration,  // IN/OUT
    const ImportClassInfo&  classInfo)      // IN
{
    Four                    e;
    Four                    i, j;
    AttrInfo                attrInfo[MAXNUMOFATTRIBUTES];
    MethodInfo              methodInfo[MAXNUMOFMETHODS];
    char                    superclassList[MAXNUMOFSUPERCLASSES][LOM_MAXCLASSNAME];
    Two                     idxOfIndexInfo;
    PostingStructureInfo    postingInfo;


    // I-1. Fill attrInfo 
    for (i = 0; i < classInfo.nAttrs; i++) 
    {
        strcpy(attrInfo[i].name, (const char *)classInfo.attrsInfo[i].colName);
        attrInfo[i].complexType     = classInfo.attrsInfo[i].complexType;
        attrInfo[i].length          = classInfo.attrsInfo[i].length;
        attrInfo[i].inheritedFrom   = classInfo.attrsInfo[i].inheritedFrom;
        attrInfo[i].type            = classInfo.attrsInfo[i].colType;
        attrInfo[i].domain          = classInfo.attrsInfo[i].domainId;
    }

    // I-2. Fill superclassList
    for (i = 0; i < classInfo.nSuperclasses; i++)
    {
        strcpy(superclassList[i], classInfo.superclassesInfo[i].superclassName);
    }

    // I-3. Fill methodInfo
    for (i = 0; i < classInfo.nMethods; i++)
    {
        strcpy(methodInfo[i].dirPath, classInfo.methodsInfo[i].dirPath);
        strcpy(methodInfo[i].name, classInfo.methodsInfo[i].methodName);
        strcpy(methodInfo[i].functionName, classInfo.methodsInfo[i].functionName);

        methodInfo[i].nArguments    = classInfo.methodsInfo[i].nArguments;
        methodInfo[i].inheritedFrom = classInfo.methodsInfo[i].inheritedFrom;
        methodInfo[i].returnType    = classInfo.methodsInfo[i].returnType;

        for (j = 0; j < classInfo.methodsInfo[i].nArguments; j++)
        {
            methodInfo[i].ArgumentsList[j] = classInfo.methodsInfo[i].ArgumentsList[j];
        }
    }


    // II. Create class
    e = LOM_CreateClass(
            &configuration.handle.lomSystemHandle,
            configuration.volumeId,
            (char *)(const char *)classInfo.className,
            NULL,
            NULL,
            classInfo.nAttrs,
            (AttrInfo *)&attrInfo[0],
            classInfo.nSuperclasses,
            superclassList,
            classInfo.nMethods,
            (MethodInfo *)methodInfo,
            SM_FALSE,
            classInfo.newClassId);
    LOM_CHECK_ERR(e);


    for (i = 0; i < classInfo.nIndexes; i++) 
    {
        if (classInfo.indexesInfo[i].indexType == LOM_INDEXTYPE_TEXT &&
            classInfo.indexesInfo[i].invertedIndex.postingInfo.nEmbeddedAttributes > 0)
        {
            postingInfo.isContainingTupleID             = classInfo.indexesInfo[i].invertedIndex.postingInfo.isContainingTupleID;
            postingInfo.isContainingSentenceAndWordNum  = classInfo.indexesInfo[i].invertedIndex.postingInfo.isContainingSentenceAndWordNum;
            postingInfo.isContainingByteOffset          = classInfo.indexesInfo[i].invertedIndex.postingInfo.isContainingByteOffset;
            postingInfo.nEmbeddedAttributes             = classInfo.indexesInfo[i].invertedIndex.postingInfo.nEmbeddedAttributes;
            for (j = 0; j < postingInfo.nEmbeddedAttributes; j++)
            {
                postingInfo.embeddedAttrNo[j] = classInfo.indexesInfo[i].invertedIndex.postingInfo.embeddedAttrNo[j]; 
            }

            e = LOM_Text_DefinePostingStructure(
                    &configuration.handle.lomSystemHandle,
                    configuration.volumeId,
                    (char *)(const char *)classInfo.className,
                    (char *)(const char *)classInfo.indexesInfo[i].invertedIndex.colName,
                    (PostingStructureInfo*)&postingInfo);
            LOM_CHECK_ERR(e);
        }
    }

    return eNOERROR;
}



Four import_WriteClassInfo (
    ImportConfig&           configuration,  // IN/OUT
    const ImportClassInfo&  classInfo)      // IN
{
    Four                    i, j;
    Four                    e;
    FILE                    *fp;
    char                    fileName[MAXFILENAME];


    // I-1. make data file name
    strcpy(fileName, (const char *)configuration.dirPath);
    strcat(fileName, DIRECTORY_SEPARATOR);
    strcat(fileName, (const char *)classInfo.className);
    strcat(fileName, "_scm.tmp");

    // I-2. Open data file to be exported
    fp = Util_fopen(fileName, "wb");
    if (fp == NULL)
    {
        configuration.errorMessage  = "Can't open '";
        configuration.errorMessage += fileName;
        configuration.errorMessage += "' file";
        TOOL_ERR(eCANNOT_OPEN_FILE_IMPORT);
    }


    // II. write classInfo.className
    e = fprintf(fp, "# class name\n");
    if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_IMPORT);

    e = fprintf(fp, "%s\n", (const char *)classInfo.className);
    if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_IMPORT);


    // III. write classInfo.classId
    e = fprintf(fp, "# class ID\n");
    if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_IMPORT);

    e = fprintf(fp, "%ld\n", (const char *)classInfo.newClassId);
    if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_IMPORT);


    // IV. write attribute Info
    e = fprintf(fp, "# class attributes\n");
    if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_IMPORT);

    for (i = 0; i < classInfo.nAttrs; i++)
    {
        e = fprintf(fp, "%ld %ld %ld %ld %s %ld %ld\n",
                    classInfo.attrsInfo[i].columnNo,
                    classInfo.attrsInfo[i].complexType,
                    classInfo.attrsInfo[i].colType,
                    classInfo.attrsInfo[i].length,
                    (const char *)classInfo.attrsInfo[i].colName,
                    classInfo.attrsInfo[i].inheritedFrom,
                    classInfo.attrsInfo[i].domainId);
        if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_IMPORT);
    }


    // V. write method Info
    e = fprintf(fp, "# class methods\n");
    if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_IMPORT);

    for (i = 0; i < classInfo.nMethods; i++)
    {
        e = fprintf(fp, "%s %s %s %ld [ ",
                    (const char *)classInfo.methodsInfo[i].dirPath,
                    (const char *)classInfo.methodsInfo[i].methodName,
                    (const char *)classInfo.methodsInfo[i].functionName,
                    classInfo.methodsInfo[i].nArguments);
        if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_IMPORT);

        for (j = 0; j < classInfo.methodsInfo[i].nArguments; j++)
        {
            e = fprintf(fp, "%ld ", classInfo.methodsInfo[i].ArgumentsList[j]);
            if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_IMPORT);
        }

        e = fprintf(fp, "] %ld %ld\n",
                    classInfo.methodsInfo[i].returnType,
                    classInfo.methodsInfo[i].inheritedFrom);
        if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_IMPORT);
    }


    // VI. write superclass Info
    e = fprintf(fp, "# class's superclasses\n");
    if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_IMPORT);

    for (i = 0; i < classInfo.nSuperclasses; i++)
    {
        e = fprintf(fp, "%ld %s\n",
                    classInfo.superclassesInfo[i].superclassId,
                    (const char *)classInfo.superclassesInfo[i].superclassName);
        if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_IMPORT);
    }


    // VII. write index Info
    e = fprintf(fp, "# class indexes\n");
    if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_IMPORT);


    for (i = 0; i < classInfo.nIndexes; i++)
    {
        e = fprintf(fp, "%s %ld ",
            (const char *)classInfo.indexesInfo[i].indexName,
            classInfo.indexesInfo[i].indexType);
        if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_IMPORT);

        // There is no need to export LOM_IndexID

        switch(classInfo.indexesInfo[i].indexType)
        {
        case LOM_INDEXTYPE_BTREE:
            e = fprintf(fp, "btree(%ld %ld [ ",
                        classInfo.indexesInfo[i].btree.flag,
                        classInfo.indexesInfo[i].btree.nColumns);
            if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_IMPORT);

            for(j = 0; j < classInfo.indexesInfo[i].btree.nColumns; j++)
            {
                e = fprintf(fp, "(%ld %ld) ",
                            classInfo.indexesInfo[i].btree.columns[j].colNo,
                            classInfo.indexesInfo[i].btree.columns[j].flag);
                if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_IMPORT);
            }

            e = fprintf(fp, "])");
            if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_IMPORT);
            break;

        case LOM_INDEXTYPE_MLGF:
            e = fprintf(fp, "mlgf(%ld %ld [ ",
                        classInfo.indexesInfo[i].mlgf.flag,
                        classInfo.indexesInfo[i].mlgf.nColumns);
            if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_IMPORT);

            for(j = 0; j < classInfo.indexesInfo[i].mlgf.nColumns; j++)
            {
                e = fprintf(fp, "%ld ",
                            classInfo.indexesInfo[i].mlgf.colNo[j]);
                if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_IMPORT);
            }

            e = fprintf(fp, "] %ld)",
                        classInfo.indexesInfo[i].mlgf.extraDataLen);
            if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_IMPORT);
            break;

        case LOM_INDEXTYPE_TEXT:
            e = fprintf(fp, "text(%s %ld %ld %ld %ld [ ",
                        (char *)(const char *)classInfo.indexesInfo[i].invertedIndex.colName,
                        classInfo.indexesInfo[i].invertedIndex.postingInfo.isContainingTupleID,
                        classInfo.indexesInfo[i].invertedIndex.postingInfo.isContainingSentenceAndWordNum,
                        classInfo.indexesInfo[i].invertedIndex.postingInfo.isContainingByteOffset,
                        classInfo.indexesInfo[i].invertedIndex.postingInfo.nEmbeddedAttributes);
            if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_IMPORT);

            for (j = 0; j < classInfo.indexesInfo[i].invertedIndex.postingInfo.nEmbeddedAttributes; j++)
            {
                e = fprintf(fp, "%ld ",
                            classInfo.indexesInfo[i].invertedIndex.postingInfo.embeddedAttrNo[j]);
                if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_IMPORT);
            }

            e = fprintf(fp, "])");
            if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_IMPORT);

            break;

        default:
            TOOL_ERR(eUNHANDLED_TYPE_IMPORT);
            break;
        }

        e = fprintf(fp, "\n");
        if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_IMPORT);
    }

    // Close data file to be exported
    e = Util_fclose(fp);
    if (e == EOF)
    {
        configuration.errorMessage  = "Can't close '";
        configuration.errorMessage += fileName;
        configuration.errorMessage += "' file";
        TOOL_ERR(eCANNOT_CLOSE_FILE_IMPORT);
    }

    return eNOERROR;
}


Four import_InstallKeywordExtractor (
    ImportConfig&                   configuration,          // IN
    Array<ImportClassInfo>&         classInfos,             // IN
    Array<ImportTextKeywordExtractorInfo>&  extractorInfo)  // OUT
{
    Four                            e;
    FILE*                           fp;
    char                            *chk;
    char                            fileName[MAXFILENAME];
    char                            buffer[MAXSIZEOFLINEBUFFER];
    char                            commandStr[MAXCOMMANDLENGTH];


    // I-1. Make input file names
    strcpy(fileName, (const char *)configuration.dirPath);
    strcat(fileName, DIRECTORY_SEPARATOR);
    strcat(fileName, TEXTKEYWORDEXT_INFOFILENAME);


    // I-2. Open "TextKeywordExtractorInfo.info" file to be imported
    fp = Util_fopen(fileName, "rb");
    if (fp == NULL) 
    {
        configuration.errorMessage  = "Can't open '";
        configuration.errorMessage += fileName;
        configuration.errorMessage += "' file";
        TOOL_ERR(eCANNOT_OPEN_FILE_IMPORT);
    }

    // II. Read "TextKeywordExtractorInfo.info" file
    chk = fgets(buffer, MAXSIZEOFLINEBUFFER, fp);
    while (chk != NULL)
    {
        ImportTextKeywordExtractorInfo  info;

        buffer[strlen(buffer)-1] = '\0';
        strcpy(info.filterName, buffer);

        chk = fgets(buffer, MAXSIZEOFLINEBUFFER, fp);
        buffer[strlen(buffer)-1] = '\0';
        info.version = atoi(buffer);

        chk = fgets(buffer, MAXSIZEOFLINEBUFFER, fp);
        buffer[strlen(buffer)-1] = '\0';
        strcpy(info.keywordExtractorFilePath, buffer);

        chk = fgets(buffer, MAXSIZEOFLINEBUFFER, fp);
        buffer[strlen(buffer)-1] = '\0';
        strcpy(info.keywordExtractorFunctionName, buffer);

        chk = fgets(buffer, MAXSIZEOFLINEBUFFER, fp);
        buffer[strlen(buffer)-1] = '\0';
        strcpy(info.getNextPostingInfoFunctionName, buffer);

        chk = fgets(buffer, MAXSIZEOFLINEBUFFER, fp);
        buffer[strlen(buffer)-1] = '\0';
        strcpy(info.finalizeKeywordExtractorFunctionName, buffer);

        chk = fgets(buffer, MAXSIZEOFLINEBUFFER, fp);
        buffer[strlen(buffer)-1] = '\0';
        info.keywordExtractorNo = atoi(buffer);

        // black line skip
        chk = fgets(buffer, MAXSIZEOFLINEBUFFER, fp);


        if (info.keywordExtractorNo == 0)
        {
            sprintf(commandStr, "%s %s %s %s %s %s %s",
                    "InstallDefaultKeywordExtractor",
                    (const char *)configuration.databaseName,
                    info.filterName,
                    info.keywordExtractorFunctionName,
                    info.getNextPostingInfoFunctionName,
                    info.finalizeKeywordExtractorFunctionName,
                    info.keywordExtractorFilePath); 
        }
        else
        {
            sprintf(commandStr, "%s %s %s %s %s %s %s",
                    "InstallKeywordExtractor",
                    (const char *)configuration.databaseName,
                    info.filterName,
                    info.keywordExtractorFunctionName,
                    info.getNextPostingInfoFunctionName,
                    info.finalizeKeywordExtractorFunctionName,
                    info.keywordExtractorFilePath); 
        }
        fprintf(stderr, "%s\n", commandStr);
        e = system((const char *)commandStr);
        if (e != NORMALEXIT)  
        {
            printf("warning : Can't install keyword extractor automatically\n");
        }

        // add info 
        extractorInfo.add(info);

        // Next keyword extractor info's first line
        chk = fgets(buffer, MAXSIZEOFLINEBUFFER, fp);
    }



    // III. Close "TextKeywordExtractorInfo.info" files
    e = Util_fclose(fp);
    if (e == EOF) 
    {
        configuration.errorMessage  = "Can't close '";
        configuration.errorMessage += fileName;
        configuration.errorMessage += "' file";
        TOOL_ERR(eCANNOT_CLOSE_FILE_IMPORT);
    }

    return eNOERROR;
}


Four import_SetupKeywordExtractor (
    ImportConfig&                           configuration,          // IN
    Array<ImportClassInfo>&                 classInfos,             // IN
    Array<ImportTextKeywordExtractorInfo>&  extractorInfo)          // IN 
{
    Four                            e;
    Four                            i;
    Four                            j;
    FILE*                           fp;
    char                            *chk;
    char                            fileName[MAXFILENAME];
    char                            keywordExtractorName[MAXFILENAME];
    char                            className[MAXFILENAME];
    char                            colName[MAXFILENAME];
    char                            buffer[MAXSIZEOFLINEBUFFER];
    char                            commandStr[MAXCOMMANDLENGTH];
    ImportTextKeywordExtractorPrefer    info;


    // I-1. Make input file names
    strcpy(fileName, (const char *)configuration.dirPath);
    strcat(fileName, DIRECTORY_SEPARATOR);
    strcat(fileName, TEXTPREFERENCE_INFOFILENAME);


    // I-2. Open "SysTextPreferences.info" file to be imported
    fp = Util_fopen(fileName, "rb");
    if (fp == NULL) 
    {
        configuration.errorMessage  = "Can't open '";
        configuration.errorMessage += fileName;
        configuration.errorMessage += "' file";
        TOOL_ERR(eCANNOT_OPEN_FILE_IMPORT);
    }


    // II. Read "SysTextPreferences.info" file
    chk = fgets(buffer, MAXSIZEOFLINEBUFFER, fp);
    while (chk != NULL)
    {
        buffer[strlen(buffer)-1] = '\0';
        info.classId = atoi(buffer);

        chk = fgets(buffer, MAXSIZEOFLINEBUFFER, fp);
        buffer[strlen(buffer)-1] = '\0';
        info.colNo = atoi(buffer);

        chk = fgets(buffer, MAXSIZEOFLINEBUFFER, fp);
        buffer[strlen(buffer)-1] = '\0';
        info.filterNo = atoi(buffer);

        chk = fgets(buffer, MAXSIZEOFLINEBUFFER, fp);
        buffer[strlen(buffer)-1] = '\0';
        info.keywordExtractorNo = atoi(buffer);

        chk = fgets(buffer, MAXSIZEOFLINEBUFFER, fp);
        buffer[strlen(buffer)-1] = '\0';
        info.stemizerNo = atoi(buffer);

        // blank line skip
        chk = fgets(buffer, MAXSIZEOFLINEBUFFER, fp);

        if (info.keywordExtractorNo != 0)
        {
            for (i = 0; i < extractorInfo.numberOfItems(); i++)
            {
                if (extractorInfo[i].keywordExtractorNo == info.keywordExtractorNo) 
                {
                    strcpy(keywordExtractorName, (const char *)extractorInfo[i].filterName);
                    break;
                }
            }
            for (i = 0; i < classInfos.numberOfItems(); i++)
            {
                if (classInfos[i].oldClassId == info.classId) 
                {
                    strcpy(className, (const char *)classInfos[i].className);

                    for (j = 0; j < classInfos[i].nAttrs; j++)
                    {
                        if (classInfos[i].attrsInfo[j].columnNo == info.colNo-1)
                        {
                            strcpy(colName, (const char *)classInfos[i].attrsInfo[j].colName);
                            break;
                        }
                    }

                    break;
                }
            }
            sprintf(commandStr, "%s %s %s %s %s",
                    "SetupKeywordExtractor",
                    (const char *)configuration.databaseName,
                    keywordExtractorName, className, colName);
                    
            fprintf(stderr, "%s\n", commandStr);
            e = system((const char *)commandStr);
            if (e != NORMALEXIT)  TOOL_ERR(eFILE_EXEC_FAIL_IMPORT);
        }

        // Next keyword extractor info's first line
        chk = fgets(buffer, MAXSIZEOFLINEBUFFER, fp);
    }

    // III. Close "SysTextPreferences.info" files
    e = Util_fclose(fp);
    if (e == EOF) 
    {
        configuration.errorMessage  = "Can't close '";
        configuration.errorMessage += fileName;
        configuration.errorMessage += "' file";
        TOOL_ERR(eCANNOT_CLOSE_FILE_IMPORT);
    }

    return eNOERROR;
}
