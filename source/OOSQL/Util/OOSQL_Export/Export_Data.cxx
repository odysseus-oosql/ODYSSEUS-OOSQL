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


#define STRING_BUFFER_SIZE 1024*8


Four Export_ExportData (
    ExportConfig&               configuration,  // IN/OUT
    Array<ExportClassInfo>&     classInfos)     // IN
{
    Four                    i;
    Four                    e;
    ExportBuffer            expBuffer;


    if (configuration.exportLog.mainExportPhase >= ENDOFDATAEXPORT)
    {
        return eNOERROR;
    }

    // export data for each class
    for (i = configuration.exportLog.dataExportPhase; i < classInfos.numberOfItems(); i++) 
    {
        e = export_WriteClassData(configuration, classInfos[i], expBuffer);
        TOOL_CHECK_ERR(e);

        // Write log
        configuration.exportLog.dataExportPhase++;
        e = export_WriteLog(configuration);
        TOOL_CHECK_ERR(e);
    }

    // Write Log
    configuration.exportLog.mainExportPhase = ENDOFDATAEXPORT;
    e = export_WriteLog(configuration);
    TOOL_CHECK_ERR(e);


    return eNOERROR;
}


Four export_WriteClassData (
    ExportConfig&       configuration,  // IN/OUT
    ExportClassInfo&    classInfo,      // IN
    ExportBuffer&       expBuffer)      // INOUT
{
    FILE                *fp;
    Four                e;
    char                fileName[MAXFILENAME];
    Four                i;
    Four                ocn;
    Four                scanId;
    LockParameter       lockup;
    BoundCond           startBound;
    BoundCond           stopBound;
    LOM_IndexID         iid;
    Four                nBtreeIndexes;
    Four                fileCount = 0;
    filepos_t           fileSize = 0;
    Four                maxLogicalOid = -1;
    Four                numberOfTuple = 0;
    Four                maxSizeOfTuple = 0;



    // II. Open class to export
    ocn = LOM_OpenClass(&configuration.handle.lomSystemHandle, configuration.volumeId, 
                        (char*)(const char*)classInfo.className);
    LOM_CHECK_ERR(ocn);
    lockup.mode     = L_S;
    lockup.duration = L_COMMIT;


    //  III. Setup scan condition
    Boolean         findIndex = (Boolean)FALSE;

    for(i = 0, nBtreeIndexes = 0; i < classInfo.indexesInfo.numberOfItems(); i++)
    {
        if(classInfo.indexesInfo[i].indexType == LOM_INDEXTYPE_BTREE)
            nBtreeIndexes ++;
    }
    
    if(nBtreeIndexes == 0 || configuration.seqret == (Boolean)TRUE)
    {
        scanId = LOM_OpenSeqScan(&configuration.handle.lomSystemHandle, ocn, FORWARD, 0, NULL, &lockup);
        LOM_CHECK_ERR(scanId);

        // FOR DEBUG
        fprintf(stderr, "Sequential Scan used in %s class export\n", 
                        (const char *)classInfo.className);
    }
    else
    {
        if(classInfo.byindexName != "")    
        {        
            for(i = 0; i < classInfo.indexesInfo.numberOfItems(); i++)
            {
                if(classInfo.indexesInfo[i].indexName == classInfo.byindexName &&
                   classInfo.indexesInfo[i].indexType == LOM_INDEXTYPE_BTREE)
                {
                    iid = classInfo.indexesInfo[i].iid;

                    // FOR DEBUG
                    fprintf(stderr, "%s Index given by User used in %s class export\n",     
                                    (const char *)classInfo.indexesInfo[i].indexName,
                                    (const char *)classInfo.className);

                    break;
                }
            }

            if(i == classInfo.indexesInfo.numberOfItems())
            {
                TOOL_ERR(eGIVEN_INDEXNAME_INVALID_EXPORT);
            }
        }
        else
        {
            for(i = 0; i < classInfo.indexesInfo.numberOfItems(); i++)
            {
                if(classInfo.indexesInfo[i].indexType == LOM_INDEXTYPE_BTREE &&
                   classInfo.indexesInfo[i].btree.flag & KEYFLAG_CLUSTERING)
                {
                    iid = classInfo.indexesInfo[i].iid;

                    // FOR DEBUG
                    fprintf(stderr, "%s Clustering Btree Index used in %s class export\n",     
                                    (const char *)classInfo.indexesInfo[i].indexName,
                                    (const char *)classInfo.className);

                    findIndex = (Boolean)TRUE;
                    break;
                }
            }

            if (findIndex == (Boolean)FALSE)
            for(i = 0; i < classInfo.indexesInfo.numberOfItems(); i++)
            {
                if(classInfo.indexesInfo[i].indexType == LOM_INDEXTYPE_BTREE &&
                        classInfo.indexesInfo[i].btree.flag & KEYFLAG_UNIQUE)
                {
                    iid = classInfo.indexesInfo[i].iid;

                    // FOR DEBUG
                    fprintf(stderr, "%s Unique Btree Index used in %s class export\n",     
                                    (const char *)classInfo.indexesInfo[i].indexName,
                                    (const char *)classInfo.className);
                    findIndex = (Boolean)TRUE;
                    break;
                }
            }

            if (findIndex == (Boolean)FALSE)
            for(i = 0; i < classInfo.indexesInfo.numberOfItems(); i++)
            {
                if (classInfo.indexesInfo[i].indexType == LOM_INDEXTYPE_BTREE)
                {
                    iid = classInfo.indexesInfo[i].iid;

                    // FOR DEBUG
                    fprintf(stderr, "%s Btree Index used in %s class export\n",     
                                    (const char *)classInfo.indexesInfo[i].indexName,
                                    (const char *)classInfo.className);
                    findIndex = (Boolean)TRUE;
                    break;
                }
            }
            
            if(findIndex == (Boolean)FALSE)
            {
                // invalid index
                TOOL_ERR(eINVALID_INDEX_EXPORT);
            }
        }

        startBound.key.len = 0;
        startBound.op = SM_BOF;

        stopBound.key.len = 0;
        stopBound.op = SM_EOF;

        scanId = LOM_OpenIndexScan(&configuration.handle.lomSystemHandle, ocn, &iid, 
                                   &startBound, &stopBound, 0, NULL, &lockup);
        LOM_CHECK_ERR(scanId);
    }


    // IV. Construct ColList
    Four                        nCols = classInfo.attrsInfo.numberOfItems();
	Four						nNonComplexCols;
    Array<LOM_ColListStruct>    clist(nCols);
    LOM_ColListStruct           stringClist[1];
    char                        stringBuffer[STRING_BUFFER_SIZE];
    OID                         oid;
    Array<int>                  nullVector(nCols);

	nNonComplexCols = 0;
    for(i = 0; i < nCols; i++)
    {
        if(classInfo.attrsInfo[i].complexType == LOM_COMPLEXTYPE_BASIC)
        {
        	clist[i].colNo = classInfo.attrsInfo[i].columnNo;

            if(classInfo.attrsInfo[i].colType == LOM_STRING    || 
               classInfo.attrsInfo[i].colType == LOM_VARSTRING ||
               classInfo.attrsInfo[i].colType == LOM_TEXT      ||
               classInfo.attrsInfo[i].colType == LOM_OCTET)
            {
                clist[i].start      = 0;
                clist[i].length     = 0;
                clist[i].dataLength = 0;
                clist[i].data.ptr   = NULL;
            }
            else
            {
                clist[i].start      = ALL_VALUE;
                clist[i].length     = ALL_VALUE;
            }

			nNonComplexCols++;
        }
        else
        {
            clist[i].start      = 0;
            clist[i].length     = 0;
            clist[i].dataLength = 0;
            clist[i].data.ptr   = NULL;
			clist[i].nullFlag   = (Boolean)TRUE;
        }
    }





    //  V. Fetch each tuple and Write tuple data to the opened file
    Four        tupleSize;
    Four        loop = 0;   
    Four        nullVar = 0;
    Boolean     firstRecord = (Boolean)TRUE;


    // initialize export buffer
    expBuffer.ptr = 0;


    // I-1. Make data file name 
    sprintf(fileName, "%s%s%s_%ld.dat", 
            (const char *)configuration.dirPath,
            DIRECTORY_SEPARATOR,
            (const char *)classInfo.className,
            fileCount);
    configuration.incompleteFile = fileName;


    // I-2. Open data file to be exported
    fp = Util_fopen(fileName, "w");
    if (fp == NULL) 
    {
        configuration.errorMessage  = "Can't open '";
        configuration.errorMessage += fileName;
        configuration.errorMessage += "' file";
        TOOL_ERR(eCANNOT_OPEN_FILE_EXPORT);
    }

    if(configuration.expType == TEXT_EXPORT)
    {
        e = fprintf(fp, "%cclass %s ( ", '%', (const char *)classInfo.className);
        if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);

        for (i = 0; i < classInfo.nAttrs; i++)
        {
            e = fprintf(fp, "%s ", (const char *)classInfo.attrsInfo[i].colName);
            if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
        }

        e = fprintf(fp, ")\n");
        if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
    }

    while((e = LOM_NextObject(&configuration.handle.lomSystemHandle, scanId, &oid, NULL)) != EOS)
    {
        LOM_CHECK_ERR(e);

        // 1. initialize tuple size
        tupleSize = 0;


        // 2. fetch object by collist
		if (nNonComplexCols > 0)
		{
        	e = LOM_FetchObjectByColList(&configuration.handle.lomSystemHandle, scanId, SM_TRUE, 
           	                          	 &oid, nNonComplexCols, &clist[0]);
        	LOM_CHECK_ERR(e);
		}


        // 3. skip writing tuple size
        if(configuration.expType == TEXT_EXPORT)
        {
            if (firstRecord == (Boolean)TRUE)
            {
                firstRecord = (Boolean)FALSE;
            }
            else
            {
                e = fprintf(fp, "\n\n");
                if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
            }
        }
        else
        {
            Util_fseek(fp, (filepos_t)(sizeof(Four)), SEEK_CUR);
        }
        

        // 4-1. write OID
        e = export_WriteOID(configuration, fp, scanId, oid, expBuffer);
        TOOL_CHECK_ERR(e);
        tupleSize += sizeof(PageNo) + sizeof(VolID) + sizeof(SlotNo) + sizeof(Unique);

        // 4-2. write Logical OID
        e = export_WriteLogicalOID(configuration, fp, scanId, oid, maxLogicalOid, expBuffer);
        TOOL_CHECK_ERR(e);
        tupleSize += sizeof(Four);


        // 5. wrtie null vector
        for(i = 0; i < nCols; i++)
            nullVector[i] = clist[i].nullFlag;


        e = export_WriteNullVector(configuration, fp, nCols, &nullVector[0], expBuffer);
        TOOL_CHECK_ERR(e);
        tupleSize += nCols;


        // 6. write each column data
        for(i = 0; i < nCols; i++)
        {
            const ExportAttrInfo*   attrInfo;
            TextColStruct           text;
            LOM_TextDesc            textDesc;
            Four                    columnSize;

            attrInfo = &classInfo.attrsInfo[i];

            // if column data is NULL
            if(nullVector[i] == TRUE)
            {
                if(configuration.expType == TEXT_EXPORT)
                {
                    e = fprintf(fp, "NULL\n");
                    if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
                    continue;
                }
                else
                {
                    continue;
                }
            }

            // if column data is NOT NULL
            switch(attrInfo->complexType)
            {
            case LOM_COMPLEXTYPE_BASIC:
                switch(attrInfo->colType)
                {
                case LOM_OCTET:
                case LOM_STRING:
                case LOM_VARSTRING:
                    stringClist[0].colNo      = clist[i].colNo;
                    stringClist[0].start      = 0;
                    stringClist[0].length     = sizeof(stringBuffer);
                    stringClist[0].dataLength = sizeof(stringBuffer);
                    stringClist[0].data.ptr   = stringBuffer;

                    // skip writing column size
                    if(configuration.expType == TEXT_EXPORT)
                    {
                        e = fprintf(fp, "\'");
                        if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
                    }
                    else
                    {
                        Util_fseek(fp, (filepos_t)(sizeof(Four)), SEEK_CUR);

                        tupleSize += sizeof(Four);
                    }

                    columnSize = 0;
                    do {
                        e = LOM_FetchObjectByColList(&configuration.handle.lomSystemHandle, scanId, 
                                                     SM_TRUE, &oid, 1, stringClist);
                        LOM_CHECK_ERR(e);

                        e = export_WriteData(configuration, fp, attrInfo->colType, stringClist[0].retLength, 
                                             stringClist[0].data.ptr, expBuffer);
                        TOOL_CHECK_ERR(e);

                        stringClist[0].start += stringClist[0].retLength;
                        columnSize += stringClist[0].retLength;

                    } while(stringClist[0].retLength == sizeof(stringBuffer));

                    tupleSize += columnSize;


                    if(configuration.expType == TEXT_EXPORT)
                    {
                        e = fprintf(fp, "\'\n");
                        if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
                    }
                    else
                    {
                        e = export_WriteColumnSize(configuration, fp, columnSize, expBuffer);
                        TOOL_CHECK_ERR(e);
                    }

                    break;

                case LOM_TEXT:
                    e = LOM_Text_GetDescriptor(&configuration.handle.lomSystemHandle, scanId, 
                                               SM_TRUE, &oid, clist[i].colNo, &textDesc);
                    LOM_CHECK_ERR(e);

                    text.start      = 0;
                    text.length     = sizeof(stringBuffer);
                    text.dataLength = sizeof(stringBuffer);
                    text.data       = stringBuffer;

                    // skip writing column size
                    if(configuration.expType == TEXT_EXPORT)
                    {
                        e = fprintf(fp, "\"");
                        if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
                    }
                    else
                    {
                        Util_fseek(fp, (filepos_t)(sizeof(Four)), SEEK_CUR);
                        tupleSize += sizeof(Four);
                    }

                    columnSize = 0;                    
                    do {
                        e = LOM_Text_FetchContent(&configuration.handle.lomSystemHandle, scanId, 
                                                  SM_TRUE, &oid, clist[i].colNo, &text, &textDesc);
                        LOM_CHECK_ERR(e);

                        e = export_WriteData(configuration, fp, attrInfo->colType, text.retLength, 
                                             text.data, expBuffer);
                        TOOL_CHECK_ERR(e);

                        text.start += text.retLength;
                        columnSize += text.retLength;

                    } while(text.retLength == sizeof(stringBuffer));

                    if (columnSize == -1)   columnSize = 0;
                    tupleSize += columnSize;

                    if(configuration.expType == TEXT_EXPORT)
                    {
                        e = fprintf(fp, "\"\n");
                        if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
                    }
                    else
                    {
                        e = export_WriteColumnSize(configuration, fp, columnSize, expBuffer);
                        TOOL_CHECK_ERR(e);
                    }

                    break;

                default:
                    e = export_WriteData(configuration, fp, attrInfo->colType, clist[i].retLength, 
                                         &clist[i].data.s, expBuffer);
                    TOOL_CHECK_ERR(e);
                    columnSize = clist[i].retLength;
                    tupleSize += columnSize;

                    break;
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
                TOOL_ERR(eUNHANDLED_CASE_EXPORT);
                break;
            default:
                TOOL_ERR(eUNHANDLED_TYPE_EXPORT);
                break;
            }
        }

        // 8. write tuplesize 
        e = export_WriteTupleSize(configuration, fp, tupleSize, expBuffer);
        TOOL_CHECK_ERR(e);

        // 9. increment # of tuple
        numberOfTuple ++;

        // 10. increment size of file
        fileSize += (filepos_t)tupleSize + (filepos_t)sizeof(Four);

        // 11. find max size of tuple
        if (tupleSize > maxSizeOfTuple)
        {
            maxSizeOfTuple = tupleSize;
        }

        // 12. handle file to exceed given maximum file size
        if (configuration.dataFileSize != (filepos_t)INFINITEFILESIZE && 
            (filepos_t)fileSize > configuration.dataFileSize)
        {
            // Close data file to be exported
            e = Util_fclose(fp);
            if (e == EOF) 
            {
                configuration.errorMessage  = "Can't close '";
                configuration.errorMessage += fileName;
                configuration.errorMessage += "' file";
                TOOL_ERR(eCANNOT_CLOSE_FILE_EXPORT);
            }

            fileCount++;
#ifndef USE_LARGE_FILE      /* if not using large file, check file size parameter */
            classInfo.dataStatistics.sizeOfFileH.add(0);
            classInfo.dataStatistics.sizeOfFileL.add(fileSize);
#else
            classInfo.dataStatistics.sizeOfFileH.add((Four)(fileSize / LONG_MAX));
            classInfo.dataStatistics.sizeOfFileL.add((Four)(fileSize % LONG_MAX));
#endif
            fileSize = (filepos_t)0;

            // Make data file name 
            sprintf(fileName, "%s%s%s_%ld.dat", 
                    (const char *)configuration.dirPath,
                    DIRECTORY_SEPARATOR,
                    (const char *)classInfo.className,
                    fileCount);
            configuration.incompleteFile = fileName;
        
        
            // Open data file to be exported
            fp = Util_fopen(fileName, "w");
            if (fp == NULL) 
            {
                configuration.errorMessage  = "Can't open '";
                configuration.errorMessage += fileName;
                configuration.errorMessage += "' file";
                TOOL_ERR(eCANNOT_OPEN_FILE_EXPORT);
            }

            if(configuration.expType == TEXT_EXPORT)
            {
                e = fprintf(fp, "%cclass %s ( ", '%', (const char *)classInfo.className);
                if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);

                for (i = 0; i < classInfo.nAttrs; i++)
                {
                    e = fprintf(fp, "%s ", (const char *)classInfo.attrsInfo[i].colName);
                    if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
                }

                e = fprintf(fp, ")\n");
                if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
            }
        }
    }

    // VII. Close scan
    e = LOM_CloseScan(&configuration.handle.lomSystemHandle, scanId);
    LOM_CHECK_ERR(e);

    // VIII. Close class
    e = LOM_CloseClass(&configuration.handle.lomSystemHandle, ocn);
    LOM_CHECK_ERR(e);


    // IX. Close data file to be exported
    e = Util_fclose(fp);
    if (e == EOF) 
    {
        configuration.errorMessage  = "Can't close '";
        configuration.errorMessage += fileName;
        configuration.errorMessage += "' file";
        TOOL_ERR(eCANNOT_CLOSE_FILE_EXPORT);
    }
#ifndef USE_LARGE_FILE      /* if not using large file, check file size parameter */
    classInfo.dataStatistics.sizeOfFileH.add(0);
    classInfo.dataStatistics.sizeOfFileL.add(fileSize);
#else
    classInfo.dataStatistics.sizeOfFileH.add((Four)(fileSize / LONG_MAX));
    classInfo.dataStatistics.sizeOfFileL.add((Four)(fileSize % LONG_MAX));
#endif


    // VI. Write Statistics
    classInfo.dataStatistics.numberOfFile        = fileCount + 1;
    classInfo.dataStatistics.numberOfTuple     = numberOfTuple;
    classInfo.dataStatistics.maxSizeOfTuple    = maxSizeOfTuple;
    classInfo.dataStatistics.maxLogicalOid     = maxLogicalOid;
    e = export_WriteDataStatistics(configuration, classInfo); 
    TOOL_CHECK_ERR(e);


    return eNOERROR;
}




Four export_WriteOID(
    ExportConfig&   configuration,  // IN/OUT
    FILE            *fp,            // IN
    Four            scanId,         // IN
    OID&            oid,            // IN
    ExportBuffer&   expBuffer)      // INOUT
{
    Four    e;

    if(configuration.expType == TEXT_EXPORT)
    {
    }
    else
    {
        // write OID
        e = fwrite(&oid.pageNo, sizeof(PageNo), 1, fp);
        if (e < 1)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);

        e = fwrite(&oid.volNo, sizeof(VolID), 1, fp);
        if (e < 1)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);

        e = fwrite(&oid.slotNo, sizeof(SlotNo), 1, fp);
        if (e < 1)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);

        e = fwrite(&oid.unique, sizeof(Unique), 1, fp);
        if (e < 1)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
    }

    return eNOERROR;
}


Four export_WriteLogicalOID(
    ExportConfig&   configuration,  // IN/OUT
    FILE            *fp,            // IN
    Four            scanId,         // IN
    OID&            oid,            // IN
    Four&           maxLogicalOid,  // IN/OUT
    ExportBuffer&   expBuffer)      // INOUT
{
    Four    e;
    Four    logicalDocId;

    logicalDocId = lom_Text_GetLogicalId(&configuration.handle.lomSystemHandle, scanId, SM_TRUE, &oid);

    if (maxLogicalOid < logicalDocId)
    {
        maxLogicalOid = logicalDocId;
    }

    if(configuration.expType == TEXT_EXPORT)
    {
    }
    else
    {
        // write Logical OID
        e = fwrite(&logicalDocId, sizeof(Four), 1, fp);
        if (e < 1)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);

    }

    return eNOERROR;
}


Four export_WriteNullVector(
    ExportConfig&   configuration,  // IN/OUT
    FILE            *fp,            // IN
    Four            nCols,          // IN
    int             *nullVector,    // IN
    ExportBuffer&   expBuffer)      // IN/OUT
{
    Four            e;
    unsigned char   one = 1;
    unsigned char   zero = 0;

    if(configuration.expType == TEXT_EXPORT)
    {
    }
    else
    {
        for(int i = 0; i < nCols; i++)
        {
            if(nullVector[i]) {
                e = fwrite(&one, 1, 1, fp);
                if (e < 1)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
            }
            else {
                e = fwrite(&zero, 1, 1, fp);
                if (e < 1)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
            }
        }        

    }

    return eNOERROR;
}



Four export_WriteDataStatistics (
    ExportConfig&           configuration,  // IN/OUT
    ExportClassInfo&        classInfo)      // IN
{
    Four                    e;
    Four                    i;
    FILE*                   fp;
    char                    fileName[MAXFILENAME];

    // I-1. make data file name
    strcpy(fileName, (const char *)configuration.dirPath);
    strcat(fileName, DIRECTORY_SEPARATOR);
    strcat(fileName, (const char *)classInfo.className);
    strcat(fileName, ".dst");

    // I-2. Open data file to be exported
    fp = Util_fopen(fileName, "w");
    if (fp == NULL)
    {
        configuration.errorMessage  = "Can't open '";
        configuration.errorMessage += fileName;
        configuration.errorMessage += "' file";
        TOOL_ERR(eCANNOT_OPEN_FILE_EXPORT);
    }

    if(configuration.expType == TEXT_EXPORT)
    {
        e = fprintf(fp, "number of file  : %ld\n", classInfo.dataStatistics.numberOfFile);
        if (e < 1)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);

        e = fprintf(fp, "file sizes      : ", classInfo.dataStatistics.numberOfFile);
        if (e < 1)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
        for (i = 0; i < classInfo.dataStatistics.numberOfFile; i++)
        {
#ifndef USE_LARGE_FILE      /* if not using large file, check file size parameter */
            e = fprintf(fp, "[%ld byte] ", classInfo.dataStatistics.sizeOfFileL[i]); 
#else
            e = fprintf(fp, "[%lld byte] ", 
                (filepos_t)(classInfo.dataStatistics.sizeOfFileH[i] * LONG_MAX) + 
                (filepos_t)(classInfo.dataStatistics.sizeOfFileL[i]));
#endif
            if (e < 1)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
        }           
        e = fprintf(fp, "\n");
        if (e < 1)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);

        e = fprintf(fp, "number of tuple : %ld\n", classInfo.dataStatistics.numberOfTuple);
        if (e < 1)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);

        e = fprintf(fp, "max tuple size  : %ld\n", classInfo.dataStatistics.maxSizeOfTuple);
        if (e < 1)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);

        e = fprintf(fp, "max logical oid : %ld\n", classInfo.dataStatistics.maxLogicalOid);
        if (e < 1)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
    }
    else
    {
        e = fwrite(&classInfo.dataStatistics.numberOfFile, sizeof(Four), 1, fp);
        if (e < 1)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);

        for (i = 0; i < classInfo.dataStatistics.numberOfFile; i++)
        {
            e = fwrite(&classInfo.dataStatistics.sizeOfFileH[i], sizeof(Four), 1, fp);
            if (e < 1)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);

            e = fwrite(&classInfo.dataStatistics.sizeOfFileL[i], sizeof(Four), 1, fp);
            if (e < 1)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
        }

        e = fwrite(&classInfo.dataStatistics.numberOfTuple, sizeof(Four), 1, fp);
        if (e < 1)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);

        e = fwrite(&classInfo.dataStatistics.maxSizeOfTuple, sizeof(Four), 1, fp);
        if (e < 1)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);

        e = fwrite(&classInfo.dataStatistics.maxLogicalOid, sizeof(Four), 1, fp);
        if (e < 1)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
    }

    // III. Close data file to be exported
    e = Util_fclose(fp);
    if (e == EOF)
    {
        configuration.errorMessage  = "Can't close '";
        configuration.errorMessage += fileName;
        configuration.errorMessage += "' file";
        TOOL_ERR(eCANNOT_CLOSE_FILE_EXPORT);
    }


    return eNOERROR;
}



Four export_WriteTupleSize(
    ExportConfig&   configuration, 
    FILE            *fp,
    Four            tupleSize,
    ExportBuffer&   expBuffer)      // IN/OUT
{
    Four    e;

    if(configuration.expType == TEXT_EXPORT)
    {
    }
    else
    {
        Util_fseek(fp, (filepos_t)(-(tupleSize+sizeof(Four))), SEEK_CUR);

        e = fwrite(&tupleSize, sizeof(Four), 1, fp);
        if (e < 1)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);

        Util_fseek(fp, (filepos_t)(tupleSize), SEEK_CUR);

    }

    return eNOERROR;
}


Four export_WriteColumnSize(
    ExportConfig&   configuration, 
    FILE            *fp,
    Four            columnSize,
    ExportBuffer&   expBuffer)      // INOUT
{
    Four            e;

    if(configuration.expType == TEXT_EXPORT)
    {
    }
    else
    {
        Util_fseek(fp, (filepos_t)(-(columnSize+sizeof(Four))), SEEK_CUR);

        e = fwrite(&columnSize, sizeof(Four), 1, fp);
        if (e < 1)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);

        Util_fseek(fp, (filepos_t)(columnSize), SEEK_CUR);

    }

    return eNOERROR;
}


Four export_WriteData (
    ExportConfig&   configuration, 
    FILE            *fp, 
    Four            type, 
    Four            length, 
    void            *ptr,
    ExportBuffer&   expBuffer)      // INOUT
{
    Four    e;
    Four    i;

    char encodedString[STRING_BUFFER_SIZE * 4 + 1];

    if(configuration.expType == TEXT_EXPORT)
    {
        switch(type)
        {
        case LOM_SHORT:
            e = fprintf(fp, "%ld\n", *(short*)ptr);
            if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
            break;

        case LOM_USHORT:
            e = fprintf(fp, "%u\n", *(unsigned short*)ptr);
            if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
            break;

        case LOM_INT:
            e = fprintf(fp, "%ld\n", *(int*)ptr);
            if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
            break;

        case LOM_LONG:
            e = fprintf(fp, "%ld\n", *(long*)ptr);
            if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
            break;

        case LOM_LONG_LONG:
            e = fprintf(fp, "%ld\n", *(long*)ptr);
            if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
            break;

        case LOM_ULONG:
            e = fprintf(fp, "%lu\n", *(unsigned long *)ptr);
            if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
            break;

        case LOM_FLOAT:
            e = fprintf(fp, "%f\n", *(float*)ptr);
            if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
            break;

        case LOM_DOUBLE:
            e = fprintf(fp, "%f\n", *(double*)ptr);
            if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
            break;

        case LOM_STRING:
        case LOM_OCTET:
        case LOM_VARSTRING:
        case LOM_TEXT:
            memset(encodedString, 0, sizeof(encodedString));

            e = export_EncodeString(type, length, (char*)ptr, encodedString);
            TOOL_CHECK_ERR(e);

            e = fprintf(fp, "%s", encodedString);
            if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
            break;

        case LOM_BOOLEAN:
            e = fprintf(fp, "%ld\n", *(long *)ptr);
            if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
            break;

        case LOM_DATE:
            e = fprintf(fp, "\'%u/%u/%u\'\n", 
                LOM_GetMonth(&configuration.handle.lomSystemHandle, (LOM_Date *)ptr),
                LOM_GetDay(&configuration.handle.lomSystemHandle, (LOM_Date *)ptr),
                LOM_GetYear(&configuration.handle.lomSystemHandle, (LOM_Date *)ptr));
            if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
            break;

        case LOM_TIME:
            e = fprintf(fp, "\'%u:%u:%u\'\n", 
                LOM_GetHour(&configuration.handle.lomSystemHandle, (LOM_Time *)ptr),
                LOM_GetMinute(&configuration.handle.lomSystemHandle, (LOM_Time *)ptr),
                LOM_GetSecond(&configuration.handle.lomSystemHandle, (LOM_Time *)ptr));
            if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
            break;

        case LOM_PAGEID:
        case LOM_FILEID:
        case LOM_INDEXID:
        case LOM_OID:
        case LOM_REF:
        case LOM_LINK:
        case LOM_MBR:
        case LOM_TIMESTAMP:
        case LOM_INTERVAL:
            TOOL_ERR(eUNHANDLED_CASE_EXPORT);
            break;
        default:
            TOOL_ERR(eUNHANDLED_TYPE_EXPORT);
            break;
        }
    }
    else
    {
        switch(type)
        {
        case LOM_SHORT:
        case LOM_USHORT:
            e = fwrite(ptr, sizeof(Two), 1, fp);
            if (e < 1)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
            break;

        case LOM_INT:
            e = fwrite(ptr, sizeof(int), 1, fp);
            if (e < 1)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
            break;

        case LOM_LONG:
        case LOM_ULONG:
        case LOM_LONG_LONG:
            e = fwrite(ptr, sizeof(Four), 1, fp);
            if (e < 1)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
            break;

        case LOM_FLOAT:
            e = fwrite(ptr, sizeof(float), 1, fp);
            if (e < 1)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
            break;

        case LOM_DOUBLE:
            e = fwrite(ptr, sizeof(double), 1, fp);
            if (e < 1)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
            break;

        case LOM_OCTET:
        case LOM_STRING:
        case LOM_VARSTRING:
        case LOM_TEXT:
            e = fwrite(ptr, length, 1, fp);
            if (e < 1 && length != 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
            break;

        case LOM_BOOLEAN:
            e = fwrite(ptr, sizeof(LOM_Boolean), 1, fp);
            if (e < 1)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
            break;

        case LOM_DATE:
            e = fwrite(ptr, sizeof(LOM_Date), 1, fp);
            if (e < 1)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
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
            TOOL_ERR(eUNHANDLED_CASE_EXPORT);
            break;

        default:
            TOOL_ERR(eUNHANDLED_TYPE_EXPORT);
            break;
        }
    }

    return eNOERROR;
}

    
Four export_WriteBuffer (
    void            *ptr,    
    Four            length, 
    ExportBuffer&   expBuffer,      // INOUT
    FILE            *fp)
{
    Four            e;

    if (expBuffer.ptr + length < EXPORT_BUFFER_SIZE)
    {
        memcpy(&expBuffer.buffer[expBuffer.ptr], ptr, length);
        expBuffer.ptr += length;
    }
    else 
    {
        // flush buffer
        e = fwrite(&expBuffer.buffer[0], expBuffer.ptr, 1, fp);
        if (e < 1)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
        expBuffer.ptr = 0;

        memcpy(&expBuffer.buffer[expBuffer.ptr], ptr, length);
        expBuffer.ptr += length;
    }

    return eNOERROR;
}


/*
 * export_EncodeString :
 */
Four export_EncodeString (
    Four        type, 
    Four        length,
    char*       src,
    char*       dest)
{
    Four        i, j;
    char        buff[5];
    One         digit1, digit2, digit3;

    for(i = 0, j = 0; i < length; i++)
    {
        // character is alpha-numeric
        if (isprint(src[i]) || src[i] & 0x80)
        {
            switch(src[i])
            {
            case '\'' : 
                dest[j++] = '\\';
                dest[j++] = src[i];
                break;
            case '\"' : 
                dest[j++] = '\\';
                dest[j++] = src[i];
                break;
            case '\\' : 
                dest[j++] = '\\';
                dest[j++] = src[i];
                break;
            default   :
                dest[j++] = src[i];
                break;
            }
        }
        else if (src[i] == '\0')
        {
            if (type == LOM_STRING || type == LOM_TEXT)
                break;
            else 
            {
                if (i+1 == length && (length == 1 || (length > 1 && (isprint(src[i-1]) || src[i-1] & 0x80))))
                    break;
                dest[j++] = '\\';
                dest[j++] = '0';
                dest[j++] = '0';
                dest[j++] = '0';
            }
        }
        else if (src[i] = '\n')
        {
            dest[j++] = '\\';
            dest[j++] = 'n';
            dest[j++] = '\n';
        }
        // character is non alpha-numeric
        else 
        {
            digit1 = (unsigned char)src[i] >> 6;
            digit2 = ((unsigned char)src[i] - (digit1 << 6)) >> 3;
            digit3 = ((unsigned char)src[i] - (digit1 << 6)) & 7;

            sprintf(buff, "\\%ld%ld%ld", digit1, digit2, digit3); 
            dest[j + 0] = buff[0];
            dest[j + 1] = buff[1];
            dest[j + 2] = buff[2];
            dest[j + 3] = buff[3];
            j += 4;
        }
    }
    dest[j] = '\0';


    return eNOERROR;
}

