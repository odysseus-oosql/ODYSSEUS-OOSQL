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



Four Import_ImportData (
    ImportConfig&                   configuration,      // IN
    Array<ImportClassInfo>&         classInfos,         // IN
    Array<ImportOIDMappingTable>&   oidMappingTables)   // OUT
{
    Four                            e;
    Four                            i;


    if (configuration.importLog.mainImportPhase >= ENDOFDATAIMPORT)
    {
        for (i = 0; i < classInfos.numberOfItems(); i++) 
        {
            // Read data file statistics 
            e = import_ReadDataStatistics(configuration, classInfos[i]); 
            TOOL_CHECK_ERR(e);
        }

        return eNOERROR;
    }


    // Read data file statistics
    for (i = 0; i < classInfos.numberOfItems(); i++) 
    {
        // Read data file statistics 
        e = import_ReadDataStatistics(configuration, classInfos[i]); 
        TOOL_CHECK_ERR(e);
    }

    // Resize oidMappingTables size
    oidMappingTables.resize(classInfos.numberOfItems());

    for (i = configuration.importLog.dataImportPhase; i < classInfos.numberOfItems(); i++) 
    {
        // Print out message to show this class's data importing is completed
        printf("%s class's data importing process start\n", (const char *)classInfos[i].className);


        // 1. Begin Transaction
        e = import_TransBegin(configuration);
        TOOL_CHECK_ERR(e);

        // 2. Resize and Initialize oidMappingTable's size
        if (configuration.indexes == TRUE)
        {
            if (classInfos[i].dataStatistics.numberOfTuple > 0)
                oidMappingTables[i].oidArray.resize(classInfos[i].dataStatistics.maxLogicalOid+1);
        }
    
        // 3. Insert data into class
        e = import_InsertClassData(configuration, classInfos[i], oidMappingTables[i]);
        TOOL_CHECK_ERR(e);

        // 4. Write oidMappingTables[i] into disk
        if (configuration.indexes == TRUE)
        {
            e = import_WriteOidMappingTable(configuration, classInfos[i], oidMappingTables[i]);
            TOOL_CHECK_ERR(e);
        }

        // 5. Empty oidArray
        if (configuration.indexes == TRUE)
        {
            oidMappingTables[i].oidArray.resize(0);
        }


        // 6. Commit Transaction
        e = import_TransCommit(configuration);
        TOOL_CHECK_ERR(e);

        // Write log
        configuration.importLog.dataImportPhase++;
        e = import_WriteLog(configuration);
        TOOL_CHECK_ERR(e);


        // Print out message to show this class's data importing is completed
        printf("\n%s class's data importing process complete\n\n", (const char *)classInfos[i].className);
    }

    // Write Log
    configuration.importLog.mainImportPhase = ENDOFDATAIMPORT;
    e = import_WriteLog(configuration);
    TOOL_CHECK_ERR(e);


    return eNOERROR;
}



Four import_ReadDataStatistics (
    ImportConfig&           configuration,      // IN
    ImportClassInfo&        classInfo)          // IN/OUT
{
    Four                    e;
    Four                    i;
    Four                    sizeOfFileH;
    Four                    sizeOfFileL;
    FILE*                   fp;
    char                    fileName[MAXFILENAME];


    // I-1. make data file name
    strcpy(fileName, (const char *)configuration.dirPath);
    strcat(fileName, DIRECTORY_SEPARATOR);
    strcat(fileName, (const char *)classInfo.className);
    strcat(fileName, ".dst");


    // I-2. Open data file to be imported
    fp = Util_fopen(fileName, "rb");
    if (fp == NULL)
    {
        configuration.errorMessage  = "Can't open '";
        configuration.errorMessage += fileName;
        configuration.errorMessage += "' file";
        TOOL_ERR(eCANNOT_OPEN_FILE_IMPORT);
    }


    // II-2. Find size of data
    e = fread(&classInfo.dataStatistics.numberOfFile, sizeof(Four), 1, fp);
    if (e < 1)          TOOL_ERR(eFILE_READ_FAIL_IMPORT);

    for (i = 0; i < classInfo.dataStatistics.numberOfFile; i++)
    {
        e = fread(&sizeOfFileH, sizeof(Four), 1, fp);
        if (e < 1)          TOOL_ERR(eFILE_READ_FAIL_IMPORT);
        e = fread(&sizeOfFileL, sizeof(Four), 1, fp);
        if (e < 1)          TOOL_ERR(eFILE_READ_FAIL_IMPORT);
        
        classInfo.dataStatistics.sizeOfFileH.add(sizeOfFileH);
        classInfo.dataStatistics.sizeOfFileL.add(sizeOfFileL);
    }

    // II-3. Find # of tuple
    e = fread(&classInfo.dataStatistics.numberOfTuple, sizeof(Four), 1, fp);
    if (e < 1)          TOOL_ERR(eFILE_READ_FAIL_IMPORT);

    // II-4. Find maxinum size of tuple
    e = fread(&classInfo.dataStatistics.maxSizeOfTuple, sizeof(Four), 1, fp);
    if (e < 1)          TOOL_ERR(eFILE_READ_FAIL_IMPORT);

    // II-5. Find maxinum logical OID
    e = fread(&classInfo.dataStatistics.maxLogicalOid, sizeof(Four), 1, fp);
    if (e < 1)          TOOL_ERR(eFILE_READ_FAIL_IMPORT);


    // III. Close data file to be imported
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



Four import_InsertClassData (
    ImportConfig&           configuration,      // IN
    ImportClassInfo&        classInfo,          // IN
    ImportOIDMappingTable&  oidMappingTable)    // OUT
{
    Four                    e;
    Four                    i;
    Four                    ocn;
    Four                    bulkLoadID;
    Four                    scanId;
    Four                    offset;
    FILE                    *fp;
    char                    fileName[MAXFILENAME];
    LockParameter           lockup;

    Four                    fileCount = 0;
    filepos_t               fileSize;
    Four                    numberOfTuple;
    Four                    maxSizeOfTuple;
    Four                    maxLogicalOid;


    // I-1. make data file name 
    sprintf(fileName, "%s%s%s_%ld.dat",
            (const char *)configuration.dirPath,
            DIRECTORY_SEPARATOR,
            (const char *)classInfo.className,
            fileCount);
    configuration.incompleteFile = fileName;

    // I-2. Open data file to be imported
    fp = Util_fopen(fileName, "rb");
    if (fp == NULL) 
    {
        configuration.errorMessage  = "Can't open '";
        configuration.errorMessage += fileName;
        configuration.errorMessage += "' file";
        TOOL_ERR(eCANNOT_OPEN_FILE_IMPORT);
    }

    // II. Set data file statistics variable
#ifndef USE_LARGE_FILE      /* if not using large file, check file size parameter */
    fileSize        = (filepos_t)(classInfo.dataStatistics.sizeOfFileL[fileCount]);
#else
    fileSize        = (filepos_t)(classInfo.dataStatistics.sizeOfFileH[fileCount] * LONG_MAX) + 
                      (filepos_t)(classInfo.dataStatistics.sizeOfFileL[fileCount]);;
#endif
    numberOfTuple   = classInfo.dataStatistics.numberOfTuple;
    maxSizeOfTuple  = classInfo.dataStatistics.maxSizeOfTuple;
    maxLogicalOid   = classInfo.dataStatistics.maxLogicalOid;


    // IV. Open class to import
    /* open class */
    ocn = LOM_OpenClass(&configuration.handle.lomSystemHandle, configuration.volumeId, 
                        (char*)(const char*)classInfo.className);
    LOM_CHECK_ERR(ocn);
    lockup.mode     = L_X;
    lockup.duration = L_COMMIT;


    // V. Setup scan condition
    scanId = LOM_OpenSeqScan(&configuration.handle.lomSystemHandle, ocn, FORWARD, 0, NULL, &lockup);
    LOM_CHECK_ERR(scanId);



    /* open bulk loading */
    if (configuration.bulkload)
    {
        bulkLoadID = LOM_OpenClassBulkLoad(&configuration.handle.lomSystemHandle, configuration.volumeId, 
                                            configuration.volumeId, (char*)(const char*)classInfo.className, 
                                            SM_FALSE, SM_TRUE, 100, 100, &lockup);
        LOM_CHECK_ERR(bulkLoadID);
    }



    
    // VI. Construct ColList
    Four                        nCols = classInfo.attrsInfo.numberOfItems();
    Array<LOM_ColListStruct>    clist(nCols);
    Array<TextColStruct>        text(nCols);


    for(i = 0; i < nCols; i++)
    {
        if(classInfo.attrsInfo[i].complexType == LOM_COMPLEXTYPE_BASIC)
        {
            switch (classInfo.attrsInfo[i].colType)
            {
            case LOM_TEXT :
                clist[i].colNo      = classInfo.attrsInfo[i].columnNo;
                clist[i].start      = 0;
                clist[i].length     = 0;
                clist[i].dataLength = 0;
                clist[i].data.ptr   = NULL;

                text[i].start       = ALL_VALUE;
                text[i].length      = ALL_VALUE;
                text[i].indexMode   = LOM_DEFERRED_MODE;
                break;

            default :
                clist[i].colNo      = classInfo.attrsInfo[i].columnNo;
                clist[i].start      = ALL_VALUE;
                clist[i].length     = ALL_VALUE;
                break;
            }            
        }
        else
        {
            // Not considered case
            TOOL_ERR(eUNHANDLED_TYPE_IMPORT);
        }
    }


    // VII. Insert each object
    Four                tupleSize;
    Four                columnSize;
    Four                tupleCount;
    OID                 oid;
    Four                logicalDocId;
    Array<One>          nullVector(nCols);
    Array<char>         stringBuffer(maxSizeOfTuple);
    
    tupleCount = 0;

    while(fileSize > (filepos_t)0)
    {
        // 0. reset offset
        offset = 0;

        // 1. Read tuple size
        e = fread(&tupleSize, sizeof(Four), 1, fp);
        if (e < 1) 
        {
            TOOL_ERR(eFILE_READ_FAIL_IMPORT);
        }

        
        // 2. Read OID and Logical OID
        e = import_ReadOID(fp, oid, logicalDocId);
        TOOL_CHECK_ERR(e);


        // 3-1. Read null vector */
        e = import_ReadNullVector(fp, nCols, nullVector);
        TOOL_CHECK_ERR(e);


        // 3-2. Set null vector 
        for(i = 0; i < nCols; i++)
            clist[i].nullFlag = (Boolean)nullVector[i];


        // 4. Read each column data
        for(i = 0; i < nCols; i++)
        {
            // if column data is NULL
            if(clist[i].nullFlag == TRUE)
            {
                continue;
            }


            // if column data is NOT NULL
            switch(classInfo.attrsInfo[i].complexType)
            {
            case LOM_COMPLEXTYPE_BASIC:
                switch(classInfo.attrsInfo[i].colType)
                {
                case LOM_OCTET:
                case LOM_STRING:
                case LOM_VARSTRING:
                    // read column size
                    e = fread(&columnSize, sizeof(Four), 1, fp);
                    if (e < 1)      TOOL_ERR(eFILE_READ_FAIL_IMPORT);


                    // set clist 
                    clist[i].dataLength = columnSize;
                    clist[i].data.ptr   = &stringBuffer[offset];

                    // read column data
                    e = import_ReadData(configuration, fp, classInfo.attrsInfo[i].colType, 
                                        clist[i].dataLength, clist[i].data.ptr);
                    TOOL_CHECK_ERR(e);

                    // increment index & offset
                    offset += columnSize;
                    break;

                case LOM_TEXT:
                    // read column size
                    e = fread(&columnSize, sizeof(Four), 1, fp);
                    if (e < 1)      TOOL_ERR(eFILE_READ_FAIL_IMPORT);

                    // set text
                    text[i].dataLength  = columnSize;
                    text[i].data        = &stringBuffer[offset];

                    // read column data
                    e = import_ReadData(configuration, fp, classInfo.attrsInfo[i].colType, 
                                        text[i].dataLength, text[i].data);
                    TOOL_CHECK_ERR(e);

                    // increment index & offset
                    offset += columnSize;
                    break;

                default:
                    // read column data
                    e = import_ReadData(configuration, fp, classInfo.attrsInfo[i].colType, 
                                        clist[i].length, &clist[i].data.s);
                    TOOL_CHECK_ERR(e);
                    break;
                }
                break;
                // LOM_COMPLEXTYPE_BASIC case end

            case LOM_COMPLEXTYPE_SET:
            case LOM_COMPLEXTYPE_COLLECTIONSET:
            case LOM_COMPLEXTYPE_COLLECTIONBAG:
            case LOM_COMPLEXTYPE_COLLECTIONLIST:
            case LOM_COMPLEXTYPE_ODMG_COLLECTIONSET:
            case LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG:
            case LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST:
            case LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY:
                // Not yet implemented case
                TOOL_ERR(eUNHANDLED_CASE_IMPORT);
                break;

            default:
                // Not considered case
                TOOL_ERR(eUNHANDLED_TYPE_IMPORT);
                break;
            }
        }


        //
        //  Use bulkloading
        //
        if (configuration.bulkload)
        {
            if (configuration.indexes == TRUE)
            {
                e = LOM_SetUserGivenLogicalID_BulkLoad(&configuration.handle.lomSystemHandle, 
                                                        bulkLoadID, logicalDocId);
                LOM_CHECK_ERR(e);
            }
    
    
            Four                start, end;
            Boolean             varstringExist;
            LOM_TextDesc        textDesc;
    
            start           = -1;
            end             = 0;
            varstringExist  = SM_FALSE;

            for(i = 0; i < nCols; i++)
            {
                switch(classInfo.attrsInfo[i].complexType)
                {
                case LOM_COMPLEXTYPE_BASIC:
                    if(classInfo.attrsInfo[i].colType != LOM_TEXT && 
                       classInfo.attrsInfo[i].colType != LOM_VARSTRING &&
                       classInfo.attrsInfo[i].colType != LOM_OCTET)
                    {
                        end = i;
                        if(start == -1) start = i;
                    }
                    else
                    {
                        if(start != -1)
                        {
                            e = LOM_NextClassBulkLoad(&configuration.handle.lomSystemHandle, bulkLoadID, 
                                                      end - start + 1, &clist[start], SM_FALSE, NULL, NULL);
                            LOM_CHECK_ERR(e);
                            start = -1; 
                        }
    
                        if(classInfo.attrsInfo[i].colType == LOM_TEXT)
                        {
                            MAKE_NULLTEXTDESC(textDesc);
    
                            if (text[i].dataLength > 0)
                            {
                                e = LOM_Text_CreateContentBulkLoad(&configuration.handle.lomSystemHandle, bulkLoadID, 
                                                                   classInfo.attrsInfo[i].columnNo, &text[i], 
                                                                   &textDesc, SM_FALSE, NULL, NULL); 
                                LOM_CHECK_ERR(e);
                            }
                        }
                        else 
                        {
                            varstringExist = SM_TRUE;
                        }
                    }
                    break;
    
                case LOM_COMPLEXTYPE_SET:
                case LOM_COMPLEXTYPE_COLLECTIONSET:
                case LOM_COMPLEXTYPE_COLLECTIONBAG:
                case LOM_COMPLEXTYPE_COLLECTIONLIST:
                case LOM_COMPLEXTYPE_ODMG_COLLECTIONSET:
                case LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG:
                case LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST:
                case LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY:
                    // Not yet implemented case
                    TOOL_ERR(eUNHANDLED_CASE_IMPORT);
                    break;
    
                default:
                    // Not considered case
                    TOOL_ERR(eUNHANDLED_TYPE_IMPORT);
                    break;
                }
            }   
            
            if(!varstringExist)
            {
                if (end >= start && start >= 0)
                {
                    e = LOM_NextClassBulkLoad(&configuration.handle.lomSystemHandle, bulkLoadID, end - start + 1, 
                                              &clist[start], SM_TRUE, &logicalDocId, &oid);
                    LOM_CHECK_ERR(e);
                }
                else
                {
                    e = LOM_NextClassBulkLoad(&configuration.handle.lomSystemHandle, bulkLoadID, 0, NULL, 
                                              SM_TRUE, &logicalDocId, &oid);
                    LOM_CHECK_ERR(e);
                }
            }
            else
            {
                if(start < nCols && start >= 0 && end >= start)
                {
                    e = LOM_NextClassBulkLoad(&configuration.handle.lomSystemHandle, bulkLoadID, end - start + 1, 
                                              &clist[start], SM_FALSE, NULL, NULL);
                    LOM_CHECK_ERR(e);
                }
    
                for(i = 0; i < nCols; i++)
                {
                    switch(classInfo.attrsInfo[i].complexType)
                    {
                    case LOM_COMPLEXTYPE_BASIC:
                        if(classInfo.attrsInfo[i].colType == LOM_VARSTRING ||
                           classInfo.attrsInfo[i].colType == LOM_OCTET)
                        {
                            e = LOM_NextClassBulkLoad(&configuration.handle.lomSystemHandle, bulkLoadID, 1, 
                                                      &clist[i], SM_FALSE, NULL, NULL);
                            LOM_CHECK_ERR(e);
                        }
    
                        break;
    
                    case LOM_COMPLEXTYPE_SET:
                    case LOM_COMPLEXTYPE_COLLECTIONSET:
                    case LOM_COMPLEXTYPE_COLLECTIONBAG:
                    case LOM_COMPLEXTYPE_COLLECTIONLIST:
                    case LOM_COMPLEXTYPE_ODMG_COLLECTIONSET:
                    case LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG:
                    case LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST:
                    case LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY:
                        // Not yet implemented case
                        TOOL_ERR(eUNHANDLED_CASE_IMPORT);
                        break;
    
                    default:
                        // Not considered case
                        TOOL_ERR(eUNHANDLED_TYPE_IMPORT);
                        break;
                    }
                }
    
                e = LOM_NextClassBulkLoad(&configuration.handle.lomSystemHandle, bulkLoadID, 0, 
                                          NULL, SM_TRUE, &logicalDocId, &oid);
                LOM_CHECK_ERR(e);
            }
        }
        //
        // No bulkloading
        //
        else    
        {
            // 5. Insert non-text type column into database
            Array<LOM_ColListStruct>    tlist(nCols);
            Four                        idx = 0;
            Four                        nAttrs;
    
            for(i = 0; i < nCols; i++)
            {
                switch(classInfo.attrsInfo[i].complexType)
                {
                case LOM_COMPLEXTYPE_BASIC:
                    if (classInfo.attrsInfo[i].colType != LOM_TEXT)
                    {
                        tlist[idx]  = clist[i];
                        idx ++;
                    }
                    break;
    
                case LOM_COMPLEXTYPE_SET:
                case LOM_COMPLEXTYPE_COLLECTIONSET:
                case LOM_COMPLEXTYPE_COLLECTIONBAG:
                case LOM_COMPLEXTYPE_COLLECTIONLIST:
                case LOM_COMPLEXTYPE_ODMG_COLLECTIONSET:
                case LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG:
                case LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST:
                case LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY:
                    // Not yet implemented case
                    TOOL_ERR(eUNHANDLED_CASE_IMPORT);
                    break;
    
                default:
                    // Not considered case
                    TOOL_ERR(eUNHANDLED_TYPE_IMPORT);
                    break;
                }
            }
            nAttrs = idx;
    
            if (nAttrs == 0)
            {
                e = LOM_CreateObjectByColList(&configuration.handle.lomSystemHandle, scanId,
                                            (Boolean)SM_TRUE, 0, NULL, &oid);
                LOM_CHECK_ERR(e);
            }
            else
            {
                e = LOM_CreateObjectByColList(&configuration.handle.lomSystemHandle, scanId,
                                            (Boolean)SM_TRUE, nAttrs, &tlist[0], &oid);
                LOM_CHECK_ERR(e);
            }
    
    
            // 6. Insert text type column into database
            LOM_TextDesc        textDesc;
    
            for(i = 0; i < nCols; i++)
            {
                switch(classInfo.attrsInfo[i].complexType)
                {
                case LOM_COMPLEXTYPE_BASIC:
                    if (classInfo.attrsInfo[i].colType == LOM_TEXT && clist[i].nullFlag != TRUE)
                    {
                        MAKE_NULLTEXTDESC(textDesc);
    
                        if (text[i].dataLength > 0)
                        {
                            e = LOM_Text_CreateContent(&configuration.handle.lomSystemHandle, scanId,
                                                    (Boolean)SM_TRUE, &oid, classInfo.attrsInfo[i].columnNo,
                                                    &text[i], &textDesc);
                            LOM_CHECK_ERR(e);
                        }
                    }
                    break;
    
                case LOM_COMPLEXTYPE_SET:
                case LOM_COMPLEXTYPE_COLLECTIONSET:
                case LOM_COMPLEXTYPE_COLLECTIONBAG:
                case LOM_COMPLEXTYPE_COLLECTIONLIST:
                case LOM_COMPLEXTYPE_ODMG_COLLECTIONSET:
                case LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG:
                case LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST:
                case LOM_COMPLEXTYPE_ODMG_COLLECTIONVARARRAY:
                    // Not yet implemented case
                    TOOL_ERR(eUNHANDLED_CASE_IMPORT);
                    break;
    
                default:
                    // Not considered case
                    TOOL_ERR(eUNHANDLED_TYPE_IMPORT);
                    break;
                }
            }

            if (configuration.indexes == TRUE)
            {
                e = lom_Text_SetLogicalId(&configuration.handle.lomSystemHandle, scanId,
                                        (Boolean)SM_TRUE, &oid, logicalDocId);
                LOM_CHECK_ERR(e);
            }
        }


        if (configuration.indexes == TRUE)
        {
            e = import_InsertOIDIntoMappingTable (oid, logicalDocId, oidMappingTable);
            TOOL_CHECK_ERR(e);
        }


        // 10. increase tupleCount 
        tupleCount ++;

        // 11. display progress
        if (tupleCount % 1000 == 0)
        {
            printf("\r %ld / %ld", tupleCount, numberOfTuple);
            fflush(stdout);
        }

        // 9. Decrease fileSize
        fileSize = (filepos_t)fileSize - (filepos_t)(tupleSize + sizeof(Four));

        if (fileSize <= (filepos_t)0 && fileCount+1 < classInfo.dataStatistics.numberOfFile)
        {
            // Close data file to be imported
            e = Util_fclose(fp);
            if (e == EOF) 
            {
                configuration.errorMessage  = "Can't close '";
                configuration.errorMessage += fileName;
                configuration.errorMessage += "' file";
                TOOL_ERR(eCANNOT_CLOSE_FILE_IMPORT);
            }
        
            fileCount++;
#ifndef USE_LARGE_FILE      /* if not using large file, check file size parameter */
            fileSize =  (filepos_t)(classInfo.dataStatistics.sizeOfFileL[fileCount]);
#else
            fileSize =  (filepos_t)(classInfo.dataStatistics.sizeOfFileH[fileCount] * LONG_MAX) + 
                        (filepos_t)(classInfo.dataStatistics.sizeOfFileL[fileCount]);;
#endif

            // make data file name 
            sprintf(fileName, "%s%s%s_%ld.dat",
                    (const char *)configuration.dirPath,
                    DIRECTORY_SEPARATOR,
                    (const char *)classInfo.className,
                    fileCount);
            configuration.incompleteFile = fileName;
        
            // Open data file to be imported
            fp = Util_fopen(fileName, "rb");
            if (fp == NULL) 
            {
                configuration.errorMessage  = "Can't open '";
                configuration.errorMessage += fileName;
                configuration.errorMessage += "' file";
                TOOL_ERR(eCANNOT_OPEN_FILE_IMPORT);
            }
        }
    }
    printf("\r %ld / %ld", tupleCount, numberOfTuple);
    fflush(stdout);


    // VIII. Assert fileSize
    if (fileSize != (filepos_t)0 || tupleCount != numberOfTuple)
    {
        configuration.errorMessage  = fileName;
        configuration.errorMessage += " file is corrupt";
        TOOL_ERR(eCORRUPT_DATA_FILE_IMPORT);
    }

    if (configuration.indexes == TRUE)
    {
        for (i = 0; i < (maxLogicalOid + 1) - numberOfTuple; i++) 
        {
            e = lom_Text_GetAndIncrementLogicalId(&configuration.handle.lomSystemHandle, ocn);
            LOM_CHECK_ERR(e);
        }
    }
    

    // X. Close bulkload 
    if (configuration.bulkload)
    {
        e = LOM_CloseClassBulkLoad(&configuration.handle.lomSystemHandle, bulkLoadID);
        LOM_CHECK_ERR(e);
    }


    // XI. Close scan
    e = LOM_CloseScan(&configuration.handle.lomSystemHandle, scanId);
    LOM_CHECK_ERR(e);


    // XII. Close class
    e = LOM_CloseClass(&configuration.handle.lomSystemHandle, ocn);
    LOM_CHECK_ERR(e);


    // XIII. Close data file to be imported
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



Four import_ReadOID (
    FILE                *fp,                // IN
    OID&                oid,                // OUT
    Four&               logicalDocId)       // OUT
{
    Four                e;


    // read physical OID
    e = fread(&oid.pageNo, sizeof(PageNo), 1, fp);
    if (e < 1)      TOOL_ERR(eFILE_READ_FAIL_IMPORT);

    e = fread(&oid.volNo, sizeof(VolID), 1, fp);
    if (e < 1)      TOOL_ERR(eFILE_READ_FAIL_IMPORT);

    e = fread(&oid.slotNo, sizeof(SlotNo), 1, fp);
    if (e < 1)      TOOL_ERR(eFILE_READ_FAIL_IMPORT);

    e = fread(&oid.unique, sizeof(Unique), 1, fp);
    if (e < 1)      TOOL_ERR(eFILE_READ_FAIL_IMPORT);


    // write Logical OID
    e = fread(&logicalDocId, sizeof(Four), 1, fp);
    if (e < 1)      TOOL_ERR(eFILE_READ_FAIL_IMPORT);


    return eNOERROR;
}


Four import_InsertOIDIntoMappingTable (
    const OID&              oid,                // IN
    const Four              logicalDocId,       // IN
    ImportOIDMappingTable&  oidMappingTable)    // OUT
{
    oidMappingTable.oidArray[logicalDocId].pageNo   = oid.pageNo;
    oidMappingTable.oidArray[logicalDocId].volNo    = oid.volNo; 
    oidMappingTable.oidArray[logicalDocId].slotNo   = oid.slotNo;
    oidMappingTable.oidArray[logicalDocId].unique   = oid.unique;

    return eNOERROR;
}



Four import_ReadNullVector (
    FILE                *fp,                // IN
    const Four          nCols,              // IN
    Array<One>&         nullVector)         // OUT
{
    Four                e;
    Four                i;


    for (i = 0; i < nCols; i++)
    {
        e = fread(&nullVector[i], sizeof(One), 1, fp);
        if (e < 1)      TOOL_ERR(eFILE_READ_FAIL_IMPORT);
    }

    return eNOERROR;
}


Four import_ReadData (
    ImportConfig&   configuration,      // IN/OUT
    FILE            *fp,                // IN
    const Four      type,               // IN 
    const Four      length,             // IN
    void            *ptr)               // OUT
{
    Four            e;
    Four            i;

    switch(type)
    {
    case LOM_SHORT:
    case LOM_USHORT:
        e = fread(ptr, sizeof(Two_Invariable), 1, fp);
        if (e < 1)      TOOL_ERR(eFILE_READ_FAIL_IMPORT);
        break;

    case LOM_INT:
    case LOM_LONG:
    case LOM_ULONG:
        e = fread(ptr, sizeof(Four_Invariable), 1, fp);
        if (e < 1)      TOOL_ERR(eFILE_READ_FAIL_IMPORT);
        break;

    case LOM_LONG_LONG:
        e = fread(ptr, sizeof(Eight_Invariable), 1, fp);
        if (e < 1)      TOOL_ERR(eFILE_READ_FAIL_IMPORT);
        break;

    case LOM_FLOAT:
        e = fread(ptr, sizeof(float), 1, fp);
        if (e < 1)      TOOL_ERR(eFILE_READ_FAIL_IMPORT);
        break;

    case LOM_DOUBLE:
        e = fread(ptr, sizeof(double), 1, fp);
        if (e < 1)      TOOL_ERR(eFILE_READ_FAIL_IMPORT);
        break;

    case LOM_OCTET:
    case LOM_STRING:
    case LOM_VARSTRING:
    case LOM_TEXT:
        e = fread(ptr, length, 1, fp);
        if (e < 1 && length != 0)       TOOL_ERR(eFILE_READ_FAIL_IMPORT);
        break;

    case LOM_BOOLEAN:
        e = fread(ptr, sizeof(LOM_Boolean), 1, fp);
        if (e < 1)      TOOL_ERR(eFILE_READ_FAIL_IMPORT);
        break;

    case LOM_DATE:
        e = fread(ptr, sizeof(LOM_Date), 1, fp);
        if (e < 1)      TOOL_ERR(eFILE_READ_FAIL_IMPORT);
        break;

    case LOM_PAGEID:
    case LOM_FILEID:
    case LOM_INDEXID:
    case LOM_OID:
    case LOM_REF:
    case LOM_LINK:
    case LOM_MBR:
    case LOM_TIME:
    case LOM_TIMESTAMP:
    case LOM_INTERVAL:
        TOOL_ERR(eUNHANDLED_CASE_IMPORT);
        break;

    default:
        TOOL_ERR(eUNHANDLED_TYPE_IMPORT);
        break;
    }

    return eNOERROR;
}



Four import_WriteOidMappingTable (
    ImportConfig&           configuration,      // IN
    ImportClassInfo&        classInfo,          // IN
    ImportOIDMappingTable&  oidMappingTable)    // IN 
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


    // Open data file to write 
    fp = Util_fopen(fileName, "wb");
    if (fp == NULL)
    {
        configuration.errorMessage  = "Can't open '";
        configuration.errorMessage += fileName;
        configuration.errorMessage += "' file";
        TOOL_ERR(eCANNOT_OPEN_FILE_IMPORT);
    }

    // Write oid array to file 
    sizeOfOidArray = oidMappingTable.oidArray.size();
    e = fwrite(&sizeOfOidArray, sizeof(Four), 1, fp);
    if (e < 1)  TOOL_ERR(eFILE_WRITE_FAIL_IMPORT);

    for (i = 0; i < oidMappingTable.oidArray.size(); i++)
    {
        e = fwrite(&oidMappingTable.oidArray[i], sizeof(ImportOID), 1, fp);
        if (e < 1)  TOOL_ERR(eFILE_WRITE_FAIL_IMPORT);
    }

    // Close data file to write 
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
