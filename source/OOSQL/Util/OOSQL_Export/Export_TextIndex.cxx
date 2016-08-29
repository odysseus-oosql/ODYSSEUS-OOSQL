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

Four Export_ExportTextIndex (
    ExportConfig&                   configuration,  // IN/OUT
    Array<ExportClassInfo>&         classInfos)     // IN
{
    Four                            i;
    Four                            e;

    if (configuration.exportLog.mainExportPhase < ENDOFTEXTINDEXEXPORT)
    {
        for (i = configuration.exportLog.textIndexExportPhase; i < classInfos.numberOfItems(); i++) 
        {
            e = export_WriteTextIndex(configuration, classInfos[i]);
            TOOL_CHECK_ERR(e);

            // Write Log
            configuration.exportLog.textIndexExportPhase++;
            e = export_WriteLog(configuration);
            TOOL_CHECK_ERR(e);
        }

        // Write Log
        configuration.exportLog.mainExportPhase = ENDOFTEXTINDEXEXPORT;
        e = export_WriteLog(configuration);
        TOOL_CHECK_ERR(e);
    }

    return eNOERROR;
}


Four export_DumpPosting(
    ExportConfig&           configuration,  // IN/OUT
    ExportClassInfo&        classInfo,      // IN
    const Four              colNo)          // IN
{
    Four                    e;
    BoundCond               keywordStartBound;
    BoundCond               keywordStopBound;
    LockParameter           lockup;
    LOM_IndexID             indexID;
    Four                    ocn;
    Four                    classID;
    Two                     startKeyLen;
    Two                     stopKeyLen;
    char                    startKeyword[LOM_MAXKEYWORDSIZE];
    char                    stopKeyword[LOM_MAXKEYWORDSIZE];
    Four                    scanId;
    Four                    nPostings;
    char                    keyword[LOM_MAXKEYWORDSIZE];
    static char             postingLengthBuffer[MAXPOSTINGBUFFERSIZE/40];	
    static char             postingBuffer[MAXPOSTINGBUFFERSIZE];
    Four                    i, j;
    char*                   position;
    char*                   nextPosition;
    char*                   lengthPosition;		
    FILE*                   fp;
    char                    sortedPostingFileName[MAXFILENAME];
    Four                    nReturnedPosting, requiredBufferSize;

    Two                     idxOfIndexInfo;
    LOM_ColListStruct       embeddedAttrsClist[MAX_NUM_EMBEDDEDATTRIBUTES]; 
    static char             embeddedAttrsBuffer[MAX_NUM_EMBEDDEDATTRIBUTES][MAXEMBDDEDATTRSBUFFERSIZE];
    char                    encodedStr[LOM_MAXVARSTRINGSIZE];

    Four                    postingSize;
    Four                    logicalDocID;
    TupleID                 docID;
    Four                    nPositions;
    Four                    sentence;
    Two                     word;
    Two                     attrNo, attrOffset, attrVarColNo;   

    Four                    fileCount = 0;
    filepos_t               fileSize = 0;
    Four                    lineSize = 0;
    static char             outLineBuffer[MAXPOSTINGBUFFERSIZE];

    char                    commandStr[MAXCOMMANDLENGTH];

#ifdef COMPRESSION
    VolNo                   volNoOfPostingTupleID;
    Four                    lastDocId;
    Four    				logicalDocIdGap;
    Four					inIndex;
    Four    				sentencePosGap;    
#endif

    sprintf(sortedPostingFileName, "%s%s%s_%s_%s_%s_%d",
            (const char *)configuration.dirPath,
            DIRECTORY_SEPARATOR,
            "TEXT",
            (const char *)classInfo.className,
            (const char *)classInfo.attrsInfo[colNo].colName,
            "SortedPosting",
            fileCount);
    configuration.incompleteFile = sortedPostingFileName;
    

    fp = Util_fopen(sortedPostingFileName, "w");
    if(fp == NULL)
    {
        configuration.errorMessage  = "Can't open '";
        configuration.errorMessage += sortedPostingFileName;
        configuration.errorMessage += "' file";
        TOOL_ERR(eCANNOT_OPEN_FILE_EXPORT);
    }

    
    ocn = LOM_OpenClass(&configuration.handle.lomSystemHandle, configuration.volumeId, 
                        (char*)(const char*)classInfo.className);
    LOM_CHECK_ERR(ocn);

    e = LOM_GetClassID(&configuration.handle.lomSystemHandle, configuration.volumeId, 
                        (char*)(const char*)classInfo.className, &classID);
    LOM_CHECK_ERR(e);

    e = LOM_Text_GetIndexID(&configuration.handle.lomSystemHandle, ocn, colNo, &indexID);
    LOM_CHECK_ERR(e);



    // lockup
    lockup.mode     = L_S;
    lockup.duration = L_COMMIT;

    startKeyword[0] = 0;
    stopKeyword[0]  = 0xff;
    stopKeyword[1]  = 0;

    startKeyLen = strlen(startKeyword);
    keywordStartBound.op = SM_GE;
    keywordStartBound.key.len = sizeof(Two) + startKeyLen;
    memcpy(&(keywordStartBound.key.val[0]), &startKeyLen, sizeof(Two));
    memcpy(&(keywordStartBound.key.val[sizeof(Two)]), startKeyword, startKeyLen);

    stopKeyLen = strlen(stopKeyword);
    keywordStopBound.op = SM_LE;
    keywordStopBound.key.len = sizeof(Two) + stopKeyLen;
    memcpy(&(keywordStopBound.key.val[0]), &stopKeyLen, sizeof(Two));
    memcpy(&(keywordStopBound.key.val[sizeof(Two)]), stopKeyword, stopKeyLen);


    scanId = LOM_Text_OpenIndexScan(&configuration.handle.lomSystemHandle, 
                                    ocn, &indexID, KEYWORD, &keywordStartBound, &keywordStopBound, &lockup);
    LOM_CHECK_ERR(scanId);


    // if "user defined posting" is used
    idxOfIndexInfo = classInfo.attrsInfo[colNo].idxOfIndexInfo;
    if (classInfo.indexesInfo[idxOfIndexInfo].invertedIndex.postingInfo.nEmbeddedAttributes > 0)
    {
        while((e = LOM_Text_NextPostings(&configuration.handle.lomSystemHandle, scanId,
                                         sizeof(postingLengthBuffer), postingLengthBuffer,
                                         sizeof(postingBuffer), postingBuffer, LOM_TEXT_SCAN_FORWARD, -1,
#ifndef COMPRESSION
                                         &nReturnedPosting, &requiredBufferSize)) != EOS)
#else
                                         &nReturnedPosting, &requiredBufferSize, &volNoOfPostingTupleID, &lastDocId)) != EOS)
#endif                                         
        {
            LOM_CHECK_ERR(e);
    
#ifdef COMPRESSION		    
			inIndex = 0;
#endif			
			
            e = LOM_Text_GetCursorKeyword(&configuration.handle.lomSystemHandle, scanId, keyword);
            if(e < 0) OOSQL_ERROR(systemHandle, e);
    
            e = LOM_Text_GetNPostingsOfCurrentKeyword(&configuration.handle.lomSystemHandle, scanId, &nPostings);
            if(e < 0) OOSQL_ERROR(systemHandle, e);
    


            // DEBUG
            //fprintf(fp, "nReturnedPosting = %ld, requiredBufferSize =%ld, nPostings=%ld\n", 
            //          nReturnedPosting, requiredBufferSize, nPostings);   

 
            lengthPosition = postingLengthBuffer;
            position = nextPosition = postingBuffer;
            for(i = 0; i < nReturnedPosting; i++)
            {
                lineSize = 0;
                position = nextPosition;
#if defined (ORDEREDSET_BACKWARD_SCAN)
				/* read postingSize from postingLengthBuffer */
                memcpy(&postingSize, lengthPosition, sizeof(Four));
                lengthPosition = lengthPosition + sizeof(Four);
                nextPosition = position + postingSize;
#else
                memcpy(&postingSize, position, sizeof(Four));
                nextPosition = position + postingSize + sizeof(Four);
    
                // skip size field 
                position += sizeof(Four);
#endif
    
#ifndef COMPRESSION
                memcpy(&logicalDocID, position, sizeof(logicalDocID)); position += sizeof(logicalDocID);
                if (classInfo.indexesInfo[idxOfIndexInfo].invertedIndex.postingInfo.isContainingTupleID == SM_TRUE)
                    memcpy(&docID, position, sizeof(TupleID)); position += sizeof(TupleID);
                memcpy(&nPositions, position, sizeof(nPositions)); position += sizeof(nPositions);
#else                
				/* logicalDocID */
    			LOM_VARIABLE_BYTE_DECODING(position, inIndex, logicalDocIdGap);
    			lastDocId += logicalDocIdGap;
    			logicalDocID = lastDocId;
    			
				/*  docID */
    			LOM_VARIABLE_BYTE_DECODING(position, inIndex, docID.pageNo);
    			docID.volNo = volNoOfPostingTupleID;
    			LOM_VARIABLE_BYTE_DECODING(position, inIndex, docID.slotNo);
    			LOM_VARIABLE_BYTE_DECODING(position, inIndex, docID.unique);
    
    			/* nPositions */
    			LOM_VARIABLE_BYTE_DECODING(position, inIndex, nPositions);
#endif                
                // DEBUG
                /*
                e = sprintf(&outLineBuffer[lineSize], "%ld %s %ld %ld %ld %ld %ld %ld ",
                        postingSize,
                        &keyword[2],
                        logicalDocID,
                        docID.pageNo, docID.volNo, docID.slotNo, docID.unique,
                        nPositions);
                */
                e = sprintf(&outLineBuffer[lineSize], "%s %ld %ld %ld %ld %ld %ld ",
                        &keyword[2],
                        logicalDocID,
                        docID.pageNo, docID.volNo, docID.slotNo, docID.unique,
                        nPositions);
                if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
                lineSize += strlen(&outLineBuffer[lineSize]);
    

                ///////////////////////////////////////////////////////////////
                // if "OOSQL_Export" exports posting list information - BEGIN
                ///////////////////////////////////////////////////////////////
                if (configuration.postingList == TRUE) {


                if (classInfo.indexesInfo[idxOfIndexInfo].invertedIndex.postingInfo.isContainingSentenceAndWordNum == SM_TRUE)
                {
#ifdef COMPRESSION
               		sentence = 0;
#endif               		

                    for(j = 0; j < nPositions; j++)
                    {
#ifndef COMPRESSION       	
                        memcpy(&sentence, position, sizeof(sentence)); position += sizeof(sentence);    
                        memcpy(&word, position, sizeof(word)); position += sizeof(word);
#else
	    				/* sentencePos uncompression */
	    				LOM_VARIABLE_BYTE_DECODING(position, inIndex, sentencePosGap);
	                    sentence += sentencePosGap;
                            
	    				/* wordPos uncompression */
	    				LOM_VARIABLE_BYTE_DECODING(position, inIndex, word);
#endif                

                        if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
                        lineSize += strlen(&outLineBuffer[lineSize]);
                    }
                }

#ifdef COMPRESSION
                position += inIndex;
#endif               		

                // Not yet implemented
                if (classInfo.indexesInfo[idxOfIndexInfo].invertedIndex.postingInfo.isContainingByteOffset == SM_TRUE)
                {
                    ;
                }


                // make collist structure
                for (j = 0; j < classInfo.indexesInfo[idxOfIndexInfo].invertedIndex.postingInfo.nEmbeddedAttributes; j++)
                {
                    attrNo = classInfo.indexesInfo[idxOfIndexInfo].invertedIndex.postingInfo.embeddedAttrNo[j];
                    embeddedAttrsClist[j].colNo = attrNo - 1; 
                    embeddedAttrsClist[j].start = ALL_VALUE;
                    embeddedAttrsClist[j].length = ALL_VALUE; 

                    switch(classInfo.attrsInfo[attrNo - 1].colType)
                    {
                    case LOM_STRING:
                    case LOM_VARSTRING:
                    case LOM_OCTET: 
                        embeddedAttrsClist[j].dataLength = classInfo.attrsInfo[attrNo - 1].length; 
                        embeddedAttrsClist[j].data.ptr = embeddedAttrsBuffer[j]; 
                        break;
                    default:
                        break;
                    }
                }

                // get embedded attribute's value by collist structure
                e = LOM_GetEmbeddedAttrsVal(&configuration.handle.lomSystemHandle, scanId, 
					position, nextPosition - position + 1,
                    classInfo.indexesInfo[idxOfIndexInfo].invertedIndex.postingInfo.nEmbeddedAttributes, 
                    embeddedAttrsClist); 
                LOM_CHECK_ERR(e);

                // print embedded attribute
                for (j = 0; j < classInfo.indexesInfo[idxOfIndexInfo].invertedIndex.postingInfo.nEmbeddedAttributes; j++)
                {
                    attrNo = classInfo.indexesInfo[idxOfIndexInfo].invertedIndex.postingInfo.embeddedAttrNo[j];

                    // print if embedded attribute' value is null or not 
                    if (embeddedAttrsClist[j].nullFlag)
                    {
                        e = sprintf(&outLineBuffer[lineSize], "1 ");
                        if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
                        lineSize += strlen(&outLineBuffer[lineSize]);
                    }
                    else
                    {
                        e = sprintf(&outLineBuffer[lineSize], "0 ");
                        if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
                        lineSize += strlen(&outLineBuffer[lineSize]);
                    }
                    
                    // print embedded attribute' value
                    switch(classInfo.attrsInfo[attrNo - 1].colType)
                    {   
                    case LOM_LONG:
                    case LOM_ULONG:
                        e = sprintf(&outLineBuffer[lineSize], "%ld ", embeddedAttrsClist[j].data.l); 
                        if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
                        lineSize += strlen(&outLineBuffer[lineSize]);
                        break;
                    case LOM_LONG_LONG:
                        e = sprintf(&outLineBuffer[lineSize], "%ld ", embeddedAttrsClist[j].data.ll); 
                        if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
                        lineSize += strlen(&outLineBuffer[lineSize]);
                        break;
                    case LOM_INT:
                        e = sprintf(&outLineBuffer[lineSize],"%ld ", embeddedAttrsClist[j].data.i);
                        if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
                        lineSize += strlen(&outLineBuffer[lineSize]);
                        break;
                    case LOM_SHORT:
                    case LOM_USHORT:
                        e = sprintf(&outLineBuffer[lineSize],"%ld ", embeddedAttrsClist[j].data.s);
                        if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
                        lineSize += strlen(&outLineBuffer[lineSize]);
                        break;
                    case LOM_DATE:
                        e = sprintf(&outLineBuffer[lineSize],"%ld ", (long)embeddedAttrsClist[j].data.date);
                        if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
                        lineSize += strlen(&outLineBuffer[lineSize]);
                        break;
                    case LOM_TIME:
                        e = sprintf(&outLineBuffer[lineSize],"%ld:%ld:%ld:%ld:%ld:%ld ", 
                            embeddedAttrsClist[j].data.time._tzHour, embeddedAttrsClist[j].data.time._tzMinute, 
                            embeddedAttrsClist[j].data.time._Hour, embeddedAttrsClist[j].data.time._Minute, 
                            embeddedAttrsClist[j].data.time._Second, embeddedAttrsClist[j].data.time._100thSec);
                        if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
                        lineSize += strlen(&outLineBuffer[lineSize]);
                        break;
                    case LOM_OID:
                        e = sprintf(&outLineBuffer[lineSize],"%ld:%ld:%ld:%ld:%ld ", 
                            embeddedAttrsClist[j].data.oid.pageNo, embeddedAttrsClist[j].data.oid.volNo, 
                            embeddedAttrsClist[j].data.oid.slotNo, embeddedAttrsClist[j].data.oid.unique, 
                            embeddedAttrsClist[j].data.oid.classID);
                        if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
                        lineSize += strlen(&outLineBuffer[lineSize]);
                        break;
                    case LOM_STRING:
                    case LOM_VARSTRING:
                        lom_EncodeStringVal(&(embeddedAttrsClist[j]), encodedStr);
                        e = sprintf(&outLineBuffer[lineSize], "%s ", encodedStr);
                        if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
                        lineSize += strlen(&outLineBuffer[lineSize]);
                        break;
                    }
                }
                }
                ///////////////////////////////////////////////////////////////
                // if "OOSQL_Export" exports posting list information - END
                ///////////////////////////////////////////////////////////////


                e = sprintf(&outLineBuffer[lineSize], "\n");
                if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
                lineSize += strlen(&outLineBuffer[lineSize]);

                e = fprintf(fp, "%s", outLineBuffer);
                if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
                fileSize += (filepos_t)lineSize;

                if (configuration.indexFileSize != (filepos_t)INFINITEFILESIZE && 
                    (filepos_t)fileSize + (filepos_t)lineSize > configuration.indexFileSize)
                {
                    e = Util_fclose(fp);
                    if (e == EOF) 
                    {
                        configuration.errorMessage  = "Can't close '";
                        configuration.errorMessage += sortedPostingFileName;
                        configuration.errorMessage += "' file";
                        TOOL_ERR(eCANNOT_CLOSE_FILE_EXPORT);
                    }

                    fileCount++;
                    fileSize = (filepos_t)0;
                    classInfo.postingFileInfo.postingFileCount[classInfo.postingFileInfo.nPostingFiles]++;

                    sprintf(sortedPostingFileName, "%s%s%s_%s_%s_%s_%ld",
                            (const char *)configuration.dirPath,
                            DIRECTORY_SEPARATOR,
                            "TEXT",
                            (const char *)classInfo.className,
                            (const char *)classInfo.attrsInfo[colNo].colName,
                            "SortedPosting",
                            fileCount);
                    configuration.incompleteFile = sortedPostingFileName;
                    
                    fp = Util_fopen(sortedPostingFileName, "w");
                    if(fp == NULL)
                    {
                        configuration.errorMessage  = "Can't open '";
                        configuration.errorMessage += sortedPostingFileName;
                        configuration.errorMessage += "' file";
                        TOOL_ERR(eCANNOT_OPEN_FILE_EXPORT);
                    }
                }
            }
        }
    }
    else
    {
        while((e = LOM_Text_NextPostings(&configuration.handle.lomSystemHandle, scanId,
                                         sizeof(postingLengthBuffer), postingLengthBuffer,
                                         sizeof(postingBuffer), postingBuffer, LOM_TEXT_SCAN_FORWARD, -1,
#ifndef COMPRESSION
                                         &nReturnedPosting, &requiredBufferSize)) != EOS)
#else
                                         &nReturnedPosting, &requiredBufferSize, &volNoOfPostingTupleID, &lastDocId)) != EOS)
#endif                                         
        {
            LOM_CHECK_ERR(e);
    
#ifdef COMPRESSION		    
			inIndex = 0;
#endif			
    
            e = LOM_Text_GetCursorKeyword(&configuration.handle.lomSystemHandle, scanId, keyword);
            if(e < 0) OOSQL_ERROR(systemHandle, e);
    
            e = LOM_Text_GetNPostingsOfCurrentKeyword(&configuration.handle.lomSystemHandle, scanId, &nPostings);
            if(e < 0) OOSQL_ERROR(systemHandle, e);
    
   
            lengthPosition = postingLengthBuffer;
            position = nextPosition = postingBuffer;
            for(i = 0; i < nReturnedPosting; i++)
            {
                lineSize = 0;
                position = nextPosition;
#if defined (ORDEREDSET_BACKWARD_SCAN)
                /* read postingSize from postingLengthBuffer */
                memcpy(&postingSize, lengthPosition, sizeof(Four));
                lengthPosition = lengthPosition + sizeof(Four);
                nextPosition = position + postingSize;
#else
                memcpy(&postingSize, position, sizeof(Four));
                nextPosition = position + postingSize + sizeof(Four);
    
                // skip size field 
                position += sizeof(Four);
#endif
    
#ifndef COMPRESSION    
                memcpy(&logicalDocID, position, sizeof(logicalDocID)); position += sizeof(logicalDocID);
                memcpy(&docID, position, sizeof(TupleID)); position += sizeof(TupleID);
                memcpy(&nPositions, position, sizeof(nPositions)); position += sizeof(nPositions);
#else   
				/* logicalDocID */
    			LOM_VARIABLE_BYTE_DECODING(position, inIndex, logicalDocIdGap);
    			lastDocId += logicalDocIdGap;
    			logicalDocID = lastDocId;
    			
				/*  docID */
    			LOM_VARIABLE_BYTE_DECODING(position, inIndex, docID.pageNo);
    			docID.volNo = volNoOfPostingTupleID;
    			LOM_VARIABLE_BYTE_DECODING(position, inIndex, docID.slotNo);
    			LOM_VARIABLE_BYTE_DECODING(position, inIndex, docID.unique);
    
    			/* nPositions */
    			LOM_VARIABLE_BYTE_DECODING(position, inIndex, nPositions);
#endif                
                e = sprintf(&outLineBuffer[lineSize], "%s %i %i %i %i %i %i ",
                        &keyword[2],
                        logicalDocID,
                        docID.pageNo, docID.volNo, docID.slotNo, docID.unique,
                        nPositions);
                if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
                lineSize += strlen(&outLineBuffer[lineSize]);

                
                ///////////////////////////////////////////////////////////////
                // if "OOSQL_Export" exports posting list information - BEGIN
                ///////////////////////////////////////////////////////////////
                if (configuration.postingList == TRUE) {

#ifdef COMPRESSION
               		sentence = 0;
#endif               		

                for(j = 0; j < nPositions; j++)
                {
#ifndef COMPRESSION                 	
                    memcpy(&sentence, position, sizeof(sentence)); position += sizeof(sentence);    
                    memcpy(&word, position, sizeof(word)); position += sizeof(word);
#else
    				/* sentencePos uncompression */
    				LOM_VARIABLE_BYTE_DECODING(position, inIndex, sentencePosGap);
                    sentence += sentencePosGap;

    				/* wordPos uncompression */
    				LOM_VARIABLE_BYTE_DECODING(position, inIndex, word);
#endif                
                    e = sprintf(&outLineBuffer[lineSize], "%i %i ",  (int)sentence, (int)word);
                    if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
                    lineSize += strlen(&outLineBuffer[lineSize]);
                }

                }
                ///////////////////////////////////////////////////////////////
                // if "OOSQL_Export" exports posting list information - END
                ///////////////////////////////////////////////////////////////


                e = sprintf(&outLineBuffer[lineSize], "\n");
                if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
                lineSize += strlen(&outLineBuffer[lineSize]);

                e = fprintf(fp, "%s", outLineBuffer);
                if (e < 0)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);
                fileSize += (filepos_t)lineSize;

                if (configuration.indexFileSize != (filepos_t)INFINITEFILESIZE &&
                    (filepos_t)fileSize + (filepos_t)lineSize > configuration.indexFileSize)
                {
                    e = Util_fclose(fp);
                    if (e == EOF) 
                    {
                        configuration.errorMessage  = "Can't close '";
                        configuration.errorMessage += sortedPostingFileName;
                        configuration.errorMessage += "' file";
                        TOOL_ERR(eCANNOT_CLOSE_FILE_EXPORT);
                    }

                    fileCount++;
                    fileSize = (filepos_t)0;
                    classInfo.postingFileInfo.postingFileCount[classInfo.postingFileInfo.nPostingFiles]++;

                    sprintf(sortedPostingFileName, "%s%s%s_%s_%s_%s_%ld",
                            (const char *)configuration.dirPath,
                            DIRECTORY_SEPARATOR,
                            "TEXT",
                            (const char *)classInfo.className,
                            (const char *)classInfo.attrsInfo[colNo].colName,
                            "SortedPosting",
                            fileCount);
                    configuration.incompleteFile = sortedPostingFileName;
                    
                    fp = Util_fopen(sortedPostingFileName, "w");
                    if(fp == NULL)
                    {
                        configuration.errorMessage  = "Can't open '";
                        configuration.errorMessage += sortedPostingFileName;
                        configuration.errorMessage += "' file";
                        TOOL_ERR(eCANNOT_OPEN_FILE_EXPORT);
                    }
                }

            }
        }
    }

    e = LOM_CloseScan(&configuration.handle.lomSystemHandle, scanId);
    LOM_CHECK_ERR(e);


    e = LOM_CloseClass(&configuration.handle.lomSystemHandle, ocn);
    LOM_CHECK_ERR(e);


    if (fileCount == 0 && fileSize == 0)
    {
        e = Util_fclose(fp);
        if (e == EOF) 
        {
            configuration.errorMessage  = "Can't close '";
            configuration.errorMessage += sortedPostingFileName;
            configuration.errorMessage += "' file";
            TOOL_ERR(eCANNOT_CLOSE_FILE_EXPORT);
        }

        // delete empty "sortedPostingFileName" file    
        sprintf(commandStr, "\\rm  %s", sortedPostingFileName);
        e = system((const char *)commandStr);
        if (e != NORMALEXIT)  TOOL_ERR(eFILE_EXEC_FAIL_EXPORT);
    }
    else
    {
        e = Util_fclose(fp);
        if (e == EOF) 
        {
            configuration.errorMessage  = "Can't close '";
            configuration.errorMessage += sortedPostingFileName;
            configuration.errorMessage += "' file";
            TOOL_ERR(eCANNOT_CLOSE_FILE_EXPORT);
        }
        classInfo.postingFileInfo.postingFileCount[classInfo.postingFileInfo.nPostingFiles]++;

        // increase # of posting file
        classInfo.postingFileInfo.nPostingFiles++;
    }


    return eNOERROR;
}



Four export_WriteTextIndex (
    ExportConfig&           configuration,  // IN/OUT
    ExportClassInfo&        classInfo)      // IN
{
    Four                    i;
    Four                    e;
    char                    fileName[MAXFILENAME];


    classInfo.postingFileInfo.nPostingFiles = 0;

    for (i = 0; i < classInfo.nAttrs; i++) 
    {
        if (classInfo.attrsInfo[i].colType == LOM_TEXT)
        {
            // DEBUG
            //if (!strcmp((const char *)classInfo.attrsInfo[i].colName,"description"))
            //{
    
            sprintf(fileName, "%s_%s_%s_%s",
                "TEXT",
                (const char *)classInfo.className,
                (const char *)classInfo.attrsInfo[i].colName,
                "SortedPosting");
            classInfo.postingFileInfo.postingFileName.add(String(fileName));
            classInfo.postingFileInfo.postingFileCount.add(0);

            e = export_DumpPosting(configuration, classInfo, i); 
            TOOL_CHECK_ERR(e);


            //} // END_DEBUG
        }
    }

    e = export_WritePostingFileInfo(configuration, classInfo);
    TOOL_CHECK_ERR(e);

    return eNOERROR;
}


Four export_WritePostingFileInfo (
    ExportConfig&           configuration,  // IN/OUT
    ExportClassInfo&        classInfo)      // IN
{
    Four                    e;
    Four                    i;
    FILE*                   fp;
    char                    fileName[MAXFILENAME];
    char                    postingFileName[MAXFILENAME];
    Four                    postingFileNameLen;

    // I-1. make data file name
    strcpy(fileName, (const char *)configuration.dirPath);
    strcat(fileName, DIRECTORY_SEPARATOR);
    strcat(fileName, (const char *)classInfo.className);
    strcat(fileName, ".pfi");

    // I-2. Open data file to be exported
    fp = Util_fopen(fileName, "w");
    if (fp == NULL)
    {
        configuration.errorMessage  = "Can't open '";
        configuration.errorMessage += fileName;
        configuration.errorMessage += "' file";
        TOOL_ERR(eCANNOT_OPEN_FILE_EXPORT);
    }

    e = fwrite(&classInfo.postingFileInfo.nPostingFiles, sizeof(Four), 1, fp);
    if (e < 1)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);

    for (i = 0; i < classInfo.postingFileInfo.nPostingFiles; i++)
    {
        sprintf(postingFileName, "%s", (char*)(const char*)classInfo.postingFileInfo.postingFileName[i]);
        postingFileNameLen = strlen(postingFileName);

        e = fwrite(&postingFileNameLen, sizeof(Four), 1, fp);
        if (e < 1)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);

        e = fwrite(postingFileName, postingFileNameLen, 1, fp);
        if (e < 1)      TOOL_ERR(eFILE_WRITE_FAIL_EXPORT);

        e = fwrite(&classInfo.postingFileInfo.postingFileCount[i], sizeof(Four), 1, fp);
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

