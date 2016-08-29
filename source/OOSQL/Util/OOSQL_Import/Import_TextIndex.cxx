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

// FOR TIME CHECK
#ifndef WIN32
extern "C" int ftime(struct timeb *tp);
#endif
static struct timeb timevar3;
static struct timeb timevar4;


Four Import_ImportTextIndex (
    ImportConfig&                   configuration,      // IN/OUT
    Array<ImportClassInfo>&         classInfos,         // IN
    Array<ImportOIDMappingTable>&   oidMappingTables)   // IN/OUT
{
    Four                            e;
    Four                            i;


    // FOR TIME CHECK
    ftime(&timevar3);
    fprintf(stderr, "\nText Index Convert Start: %lds %ldms\n", timevar3.time, timevar3.millitm);

    if (configuration.importLog.mainImportPhase < ENDOFTEXTINDEXCONVERT)
    {
        for (i = configuration.importLog.textIndexConvertPhase; i < classInfos.numberOfItems(); i++)
        {
            // Read oid mapping array
            e = import_ReadOidMappingTable(configuration, classInfos[i], oidMappingTables[i]);
            TOOL_CHECK_ERR(e);

            // sorted postingÀÇ convert
            e = import_ConvertSortedPosting(configuration, classInfos[i], oidMappingTables[i]);
            TOOL_CHECK_ERR(e);

            // Empty oidArray
            oidMappingTables[i].oidArray.resize(0);

            // Write Log
            configuration.importLog.textIndexConvertPhase++;
            e = import_WriteLog(configuration);
            TOOL_CHECK_ERR(e);
        }

        configuration.importLog.mainImportPhase = ENDOFTEXTINDEXCONVERT;
        e = import_WriteLog(configuration);
        TOOL_CHECK_ERR(e);
    }

    // FOR TIME CHECK
    ftime(&timevar4);
    fprintf(stderr, "Text Index Convert End : %lds %ldms\n", timevar4.time, timevar4.millitm);
    fprintf(stderr, "Total Text Index Converting Time : %ld:%ld:%ld\n",
                    (timevar4.time-timevar3.time)/3600,
                    ((timevar4.time-timevar3.time)%3600)/60,
                    (timevar4.time-timevar3.time)%60);

    // FOR TIME CHECK
    ftime(&timevar3);
    fprintf(stderr, "\nText Index Import Start: %lds %ldms\n", timevar3.time, timevar3.millitm);

    if (configuration.importLog.mainImportPhase < ENDOFTEXTINDEXIMPORT)
    {
        for (i = configuration.importLog.textIndexImportPhase; i < classInfos.numberOfItems(); i++)
        {
            e = import_TextIndexBuilding(configuration, classInfos[i]);
            TOOL_CHECK_ERR(e);

            // Write Log
            configuration.importLog.textIndexImportPhase++;
            e = import_WriteLog(configuration);
            TOOL_CHECK_ERR(e);
        }

        configuration.importLog.mainImportPhase = ENDOFTEXTINDEXIMPORT;
        e = import_WriteLog(configuration);
        TOOL_CHECK_ERR(e);
    }

    // FOR TIME CHECK
    ftime(&timevar4);
    fprintf(stderr, "Text Index Import End : %lds %ldms\n", timevar4.time, timevar4.millitm);
    fprintf(stderr, "Total Text Index Importing Time : %ld:%ld:%ld\n",
                    (timevar4.time-timevar3.time)/3600,
                    ((timevar4.time-timevar3.time)%3600)/60,
                    (timevar4.time-timevar3.time)%60);

    return eNOERROR;
}



Four import_ConvertPosting (
    ImportConfig&                   configuration,      // IN/OUT
    const ImportClassInfo&          classInfo,          // IN
    const Four                      colNo,              // IN
    const ImportOIDMappingTable&    oidMappingTable)    // IN
{
    Four                            e;
    Four                            i;
    Four                            j;
    FILE*                           sourFp;
    FILE*                           destFp;
    char                            sourPostingFileName[MAXFILENAME];
    char                            destPostingFileName[MAXFILENAME];
    char                            fileName[MAXFILENAME];
    Four                            fileCount;
    
    static char                     inLineBuffer[MAXPOSTINGBUFFERSIZE];
    static char                     outLineBuffer[MAXPOSTINGBUFFERSIZE];
    char                            *chk;
    Four                            spaceNum;
    Four                            beginPosition;
    char                            *token;
    char                            keyword[LOM_MAXKEYWORDSIZE];
    char                            tokenBuff[MAXSIZEOFLINEBUFFER];
    Four                            logicalDocID;


    // find posting file count of text index for given class & colNo
    sprintf(fileName, "%s_%s_%s_%s",
            "TEXT",
            (char *)(const char *)classInfo.className,
            (char *)(const char *)classInfo.attrsInfo[colNo].colName,
            "SortedPosting");

    for (i = 0; i < classInfo.postingFileInfo.nPostingFiles; i++)
    {
        if (strcmp((const char*)classInfo.postingFileInfo.postingFileName[i], fileName) == 0)
            fileCount = classInfo.postingFileInfo.postingFileCount[i];
    }



    for (i = 0; i < fileCount; i++)
    {
        // Make source file name 
        sprintf(sourPostingFileName, "%s%s%s_%s_%s_%s_%ld",
                (char *)(const char *)configuration.dirPath,
                DIRECTORY_SEPARATOR,
                "TEXT",
                (char *)(const char *)classInfo.className,
                (char *)(const char *)classInfo.attrsInfo[colNo].colName,
                "SortedPosting",
                i);

        // Open source posting file 
        sourFp = Util_fopen((const char *)sourPostingFileName, "rb");
        if (sourFp == NULL)
        {
            configuration.errorMessage  = "Can't open '";
            configuration.errorMessage += sourPostingFileName;
            configuration.errorMessage += "' file";
            TOOL_ERR(eCANNOT_OPEN_FILE_IMPORT);
        }

        // Make dest file name 
        sprintf(destPostingFileName, "%s%s%s_%s_%s_%s_%ld",
                (char *)(const char *)configuration.tmpPath,
                DIRECTORY_SEPARATOR,
                "TEXT",
                (char *)(const char *)classInfo.className,
                (char *)(const char *)classInfo.attrsInfo[colNo].colName,
                "SortedPosting",
                i);

        // Open dest posting file 
        destFp = Util_fopen(destPostingFileName, "wb");
        if (destFp == NULL)
        {
            configuration.errorMessage  = "Can't open '";
            configuration.errorMessage += destPostingFileName;
            configuration.errorMessage += "' file";
            TOOL_ERR(eCANNOT_OPEN_FILE_IMPORT);
        }


        chk = fgets(inLineBuffer, MAXPOSTINGBUFFERSIZE, sourFp);
        while (chk != NULL)
        {
            spaceNum = 0;
            for (j = 0; spaceNum != 6; j++)
            {
                if (inLineBuffer[j] == ' ')     spaceNum++;
            }
            beginPosition = j;

            token = strtok(inLineBuffer, " ");
            strcpy(keyword, token);

            token = strtok(NULL, " ");
            strcpy(tokenBuff, token);
            logicalDocID = atol(tokenBuff);

            sprintf(outLineBuffer, "%s %ld %ld %ld %ld %ld %s",
                            keyword,
                            logicalDocID,
                            oidMappingTable.oidArray[logicalDocID].pageNo,
                            oidMappingTable.oidArray[logicalDocID].volNo,
                            oidMappingTable.oidArray[logicalDocID].slotNo,
                            oidMappingTable.oidArray[logicalDocID].unique,
                            &inLineBuffer[beginPosition]);

            fputs(outLineBuffer, destFp);

            chk = fgets(inLineBuffer, MAXPOSTINGBUFFERSIZE, sourFp);
        }

        // Close source posting file
        e = Util_fclose(sourFp);
        if (e == EOF) 
        {
            configuration.errorMessage  = "Can't close '";
            configuration.errorMessage += sourPostingFileName;
            configuration.errorMessage += "' file";
            TOOL_ERR(eCANNOT_CLOSE_FILE_IMPORT);
        }

        // Close dest posting file
        e = Util_fclose(destFp);
        if (e == EOF) 
        {
            configuration.errorMessage  = "Can't close '";
            configuration.errorMessage += destPostingFileName;
            configuration.errorMessage += "' file";
            TOOL_ERR(eCANNOT_CLOSE_FILE_IMPORT);
        }
    }

      
    return eNOERROR;
}



Four import_ConvertSortedPosting (
    ImportConfig&                   configuration,      // IN
    ImportClassInfo&                classInfo,          // IN
    const ImportOIDMappingTable&    oidMappingTable)    // IN
{
    Four                            e;
    Four                            i;


    e = import_ReadPostingFileInfo(configuration, classInfo);
    TOOL_CHECK_ERR(e);

    for (i = 0; i < classInfo.nAttrs; i++) 
    {
        if (classInfo.attrsInfo[i].colType == LOM_TEXT && classInfo.dataStatistics.maxLogicalOid > 0) 
        {
            e = import_ConvertPosting(configuration, classInfo, i, oidMappingTable);
            TOOL_CHECK_ERR(e);
        }
    }

    return eNOERROR;
}


Four import_ReadOidMappingTable (
    ImportConfig&           configuration,      // IN
    const ImportClassInfo&  classInfo,          // IN
    ImportOIDMappingTable&  oidMappingTable)    // OUT
{
    Four                    i;
    Four                    e;
    Four                    sizeOfOidArray;
    FILE                    *fp;
    char                    fileName[MAXFILENAME];


    // I-1. Make data file name
    strcpy(fileName, (const char *)configuration.dirPath);
    strcat(fileName, DIRECTORY_SEPARATOR);
    strcat(fileName, (const char *)classInfo.className);
    strcat(fileName, ".oid");


    // Open data file to read 
    fp = Util_fopen(fileName, "rb");
    if (fp == NULL)
    {
        configuration.errorMessage  = "Can't open '";
        configuration.errorMessage += fileName;
        configuration.errorMessage += "' file";
        TOOL_ERR(eCANNOT_OPEN_FILE_IMPORT);
    }


    // Read data from oid array file 
    e = fread(&sizeOfOidArray, sizeof(Four), 1, fp);
    if (e < 1)      TOOL_ERR(eFILE_READ_FAIL_IMPORT);
    oidMappingTable.oidArray.resize(sizeOfOidArray);

    for (i = 0; i < sizeOfOidArray; i++)
    {
        e = fread(&oidMappingTable.oidArray[i], sizeof(ImportOID), 1, fp);
        if (e < 1)  TOOL_ERR(eFILE_READ_FAIL_IMPORT);
    }


    // Close data file to read 
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



Four import_TextIndexBuilding (
    ImportConfig&           configuration,      // IN/OUT
    const ImportClassInfo&  classInfo)          // IN
{
    Four                    e;
    Four                    i;
    Four                    j;
    Four                    count;
    Four                    fileCount;  
    char                    countStr[MAXFILENAME];
    char                    commandStr[MAXCOMMANDLENGTH];   
    char                    destFileName[MAXFILENAME];
    char                    sourFileName[MAXFILENAME];



    for (i = 0; i < classInfo.nAttrs; i++) 
    {
        if (classInfo.attrsInfo[i].colType == LOM_TEXT && classInfo.dataStatistics.maxLogicalOid > 0) 
        {
            // find posting file count of text index for given class & colNo
            sprintf(destFileName, "%s_%s_%s_%s",
                "TEXT",
                (char *)(const char *)classInfo.className,
                (char *)(const char *)classInfo.attrsInfo[i].colName,
                "SortedPosting");

            for (j = 0; j < classInfo.postingFileInfo.nPostingFiles; j++)
            {
                if (strcmp((const char*)classInfo.postingFileInfo.postingFileName[j], destFileName) == 0)
                    fileCount = classInfo.postingFileInfo.postingFileCount[j];
            }

            for (j = 0; j < fileCount; j++)
            {
                sprintf(sourFileName, "%s_%ld", destFileName, j);    

                sprintf(commandStr, "\\mv %s%s%s %s%s%s", 
                    (char *)(const char *)configuration.tmpPath,
                    DIRECTORY_SEPARATOR,
                    sourFileName, 
                    (char *)(const char *)configuration.tmpPath,
                    DIRECTORY_SEPARATOR,
                    destFileName); 
                printf("%s\n", (const char *)commandStr);
                e = system((const char *)commandStr);
                if (e != NORMALEXIT)  TOOL_ERR(eFILE_EXEC_FAIL_IMPORT);

                sprintf(commandStr, "%s %s %s %s %s", 
                    "OOSQL_BuildTextIndex", 
                    (const char *)configuration.databaseName,
                    (const char *)configuration.volumeName,
                    (const char *)classInfo.className, 
                    (const char *)classInfo.attrsInfo[i].colName);
                printf("%s\n", (const char *)commandStr);
                e = system((const char *)commandStr);
                if (e != NORMALEXIT)  TOOL_ERR(eFILE_EXEC_FAIL_IMPORT);

                sprintf(commandStr, "\\mv %s%s%s %s%s%s", 
                    (char *)(const char *)configuration.tmpPath,
                    DIRECTORY_SEPARATOR,
                    destFileName, 
                    (char *)(const char *)configuration.tmpPath,
                    DIRECTORY_SEPARATOR,
                    sourFileName); 
                printf("%s\n", (const char *)commandStr);
                e = system((const char *)commandStr);
                if (e != NORMALEXIT)  TOOL_ERR(eFILE_EXEC_FAIL_IMPORT);
            }
        }
    }

    sprintf(commandStr, "%s %s %s %s", 
            "OOSQL_UpdateTextDescriptor", 
            (const char *)configuration.databaseName,
            (const char *)configuration.volumeName,
            (const char *)classInfo.className);

    printf("%s\n", (const char *)commandStr);
    e = system((const char *)commandStr);
    if (e != NORMALEXIT)  TOOL_ERR(eFILE_EXEC_FAIL_IMPORT);


    return eNOERROR;
}


Four import_WriteTextIndexCount (
    ImportConfig&                   configuration,      // IN/OUT
    const ImportClassInfo&          classInfo,          // IN
    const Four                      colNo,              // IN
    const Four                      fileCount)          // IN
{
    Four                            e;
    FILE*                           cntFp;
    String                          cntFileName;
    

    // I-1. Make posting file names
    cntFileName  = configuration.dirPath; 
    cntFileName += DIRECTORY_SEPARATOR;
    cntFileName += "TEXT_";
    cntFileName += classInfo.className;
    cntFileName += "_";
    cntFileName += classInfo.attrsInfo[colNo].colName;
    cntFileName += "_SortedPosting.cnt";

    // I-2. Open posting files
    cntFp = Util_fopen((const char *)cntFileName, "wb");
    if (cntFp == NULL)
    {
        configuration.errorMessage  = "Can't open '";
        configuration.errorMessage += cntFileName;
        configuration.errorMessage += "' file";
        TOOL_ERR(eCANNOT_OPEN_FILE_IMPORT);
    }


    // II. Write number of sorted posting file 
    e = fwrite(&fileCount, sizeof(Four), 1, cntFp);
    if (e < 1)          TOOL_ERR(eFILE_WRITE_FAIL_IMPORT);


    // III. Close posting files
    e = Util_fclose(cntFp);
    if (e == EOF)
    {
        configuration.errorMessage  = "Can't close '";
        configuration.errorMessage += cntFileName;
        configuration.errorMessage += "' file";
        TOOL_ERR(eCANNOT_CLOSE_FILE_IMPORT);
    }


    return eNOERROR;
}


Four import_ReadTextIndexCount (
    ImportConfig&                   configuration,      // IN/OUT
    const ImportClassInfo&          classInfo,          // IN
    const Four                      colNo,              // IN
    Four&                           fileCount)          // OUT 
{
    Four                            e;
    FILE*                           cntFp;
    String                          cntFileName;
    

    // I-1. Make posting file names
    cntFileName  = configuration.dirPath; 
    cntFileName += DIRECTORY_SEPARATOR;
    cntFileName += "TEXT_";
    cntFileName += classInfo.className;
    cntFileName += "_";
    cntFileName += classInfo.attrsInfo[colNo].colName;
    cntFileName += "_SortedPosting.cnt";

    // I-2. Open posting files
    cntFp = Util_fopen((const char *)cntFileName, "rb");
    if (cntFp == NULL)
    {
        configuration.errorMessage  = "Can't open '";
        configuration.errorMessage += cntFileName;
        configuration.errorMessage += "' file";
        TOOL_ERR(eCANNOT_OPEN_FILE_IMPORT);
    }


    // II. Write number of sorted posting file 
    e = fread(&fileCount, sizeof(Four), 1, cntFp);
    if (e < 1)          TOOL_ERR(eFILE_READ_FAIL_IMPORT);


    // III. Close posting files
    e = Util_fclose(cntFp);
    if (e == EOF)
    {
        configuration.errorMessage  = "Can't close '";
        configuration.errorMessage += cntFileName;
        configuration.errorMessage += "' file";
        TOOL_ERR(eCANNOT_CLOSE_FILE_IMPORT);
    }


    return eNOERROR;
}

Four import_ReadPostingFileInfo (
    ImportConfig&           configuration,  // IN/OUT
    ImportClassInfo&        classInfo)      // IN
{
    Four                    e;
    Four                    i;
    FILE*                   fp;
    char                    fileName[MAXFILENAME];
    char                    postingFileName[MAXFILENAME];
    Four                    postingFileNameLen;
    Four                    postingFileCount;


    // I-1. make data file name
    strcpy(fileName, (const char *)configuration.dirPath);
    strcat(fileName, DIRECTORY_SEPARATOR);
    strcat(fileName, (const char *)classInfo.className);
    strcat(fileName, ".pfi");

    // I-2. Open data file to be exported
    fp = Util_fopen(fileName, "rb");
    if (fp == NULL)
    {
        configuration.errorMessage  = "Can't open '";
        configuration.errorMessage += fileName;
        configuration.errorMessage += "' file";
        TOOL_ERR(eCANNOT_OPEN_FILE_IMPORT);
    }

    e = fread(&classInfo.postingFileInfo.nPostingFiles, sizeof(Four), 1, fp);
    if (e < 1)      TOOL_ERR(eFILE_READ_FAIL_IMPORT);

    for (i = 0; i < classInfo.postingFileInfo.nPostingFiles; i++)
    {
        e = fread(&postingFileNameLen, sizeof(Four), 1, fp);
        if (e < 1)      TOOL_ERR(eFILE_READ_FAIL_IMPORT);

        e = fread(postingFileName, postingFileNameLen, 1, fp);
        if (e < 1)      TOOL_ERR(eFILE_READ_FAIL_IMPORT);
        postingFileName[postingFileNameLen] = '\0';

        e = fread(&postingFileCount, sizeof(Four), 1, fp);
        if (e < 1)      TOOL_ERR(eFILE_READ_FAIL_IMPORT);

        classInfo.postingFileInfo.postingFileName.add(String(postingFileName));
        classInfo.postingFileInfo.postingFileCount.add(postingFileCount);
    }

    // III. Close data file to be exported
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

