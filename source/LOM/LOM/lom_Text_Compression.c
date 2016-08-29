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

#ifdef COMPRESSION

#include <stdlib.h>
#include "LOM_Internal.h"
#include "LOM_Param.h"
#include "LOM.h"
#include "LOM_Param.h"
#include <sys/time.h>

/*
 * Function: Four lom_Text_Compression(handle, Four, Four, Four *, char *);
 *
 * Description:
 *
 * Retuns:
 *  error code
 */
 
#define MICRO_IN_SEC 1000000

Four lom_Text_Compression
(
    LOM_Handle *handle,
    char *data,                     /* IN: data */
    Four dataLength,                /* IN: length of data */
    char *compressedData,           /* INOUT: compressed data */
    Four *compressedDataLength      /* INOUT: length of compressed data */
)
{
    Four	        e;
	Two		        compressionLevel;
    
	if(data == NULL || dataLength < 0 || compressedData == NULL || compressedDataLength == NULL)
	{
		LOM_ERROR(handle, eBADPARAMETER_LOM);
	}

    if(*compressedDataLength < compressBound(dataLength))
    {
		LOM_ERROR(handle, eBADPARAMETER_LOM);
	}
    
    if(dataLength < LOM_NO_COMPRESSION_LENGTH)
	{
	    *compressedDataLength = dataLength + sizeof(char);
	    *compressedData = 0;
	    memcpy(compressedData + sizeof(char), data, dataLength);
	    return eNOERROR;
	}

    *compressedData = 1;
    
	if(dataLength < LOM_COMPRESSION_MIN_LENGTH)
	{   
		compressionLevel = LOM_COMPRESSION_MIN_LEVEL;
	}
	else
	{
		compressionLevel = LOM_COMPRESSION_MAX_LEVEL;
	}

   	e = compress2(compressedData + sizeof(char), compressedDataLength, data, dataLength, compressionLevel);
   	if(e == Z_OK)
   	{
   	    *compressedDataLength += sizeof(char);
   	}
    /* there was not enough memory */
    else if(e == Z_MEM_ERROR)
    {
        LOM_ERROR(handle, eOUTOFMEMORY_LOM);
    }
    /* there was not enough room in the output buffer or the level parameter is invalid */
    else if(e == Z_BUF_ERROR || e == Z_STREAM_ERROR)
    {
        LOM_ERROR(handle, eBADPARAMETER_LOM);
    }
    else
	{
		LOM_ERROR(handle, eINTERNAL_LOM);
	}

    return eNOERROR;
}

Four lom_Text_Uncompression
(
    LOM_Handle *handle,
    char *compressedData,	        /* IN: compressed data */
    Four compressedDataLength,      /* IN: length of compressed data */
    char **uncompressedData,         /* OUT: uncompressed data */
    Four *uncompressedDataLength    /* OUT: length of uncompressed data */
)
{
	Four e;
	
	*uncompressedData = (char*)malloc(*uncompressedDataLength);
	if(*uncompressedData == NULL)
	{
		LOM_ERROR(handle, eOUTOFMEMORY_LOM);
	}

    if(*compressedData)
    {
    	while((e = uncompress(*uncompressedData, uncompressedDataLength, compressedData+sizeof(char), compressedDataLength-sizeof(char))) != Z_OK)
    	{
        	/* there was not enough memory */
        	if(e == Z_MEM_ERROR) 
    		{
    			LOM_ERROR(handle, eOUTOFMEMORY_LOM);
    		}	
       		/* there was not enough room in the output buffer */
        	else if(e == Z_BUF_ERROR) 
    		{
    			*uncompressedDataLength *= 2;
    			*uncompressedData = (char*)realloc((char*)*uncompressedData, *uncompressedDataLength);
    			if(*uncompressedData == NULL) LOM_ERROR(handle, eOUTOFMEMORY_LOM);
    			
    		}
        	/* the input data was corrupted or incomplete */
     		else if(e == Z_DATA_ERROR) 
    		{
    			LOM_ERROR(handle, eBADPARAMETER_LOM);
    		}
    		else
    		{
    			LOM_ERROR(handle, eINTERNAL_LOM);
    		}
    	}
	}
	else
	{
	    *uncompressedDataLength = compressedDataLength - sizeof(char);
	    memcpy(*uncompressedData, compressedData + sizeof(char), *uncompressedDataLength);
	}

    return eNOERROR;
}

#define DEALLOCMEM_lom_Text_PostingList_Compression(handle, e)	\
{																\
	if(buffer != NULL) free(buffer);							\
	LOM_ERROR(handle, e);										\
}

Four lom_Text_PostingList_Compression
(
	lom_Text_PostingInfoForReading	*postingInfo,			/* IN */
	Four							nPostings,				/* IN */
	Four							*lengthOfPostingList,	/* INOUT */
	char							*ptrToPostingList,		/* INOUT */
	char							*ptrToCompressedPostingList,		/* INOUT */
	VolNo							*volNoOfPostingTupleID,	/* OUT */
	Boolean                         encodeDocIdFlag,
	Four                            *lastDocId              /* OUT */
)
{
	Four	e;
    Four	postingLength;
    Four    postingLengthIndex;
    Four	docId;
    Four    oldDocId;
    Four    docIdGap;
    PageNo	pageNo;
    VolNo	volNo;
    SlotNo	slotNo;
    Unique	unique;
	Four	count;
	Four    _count;
    Four	sentencePos;
    Two		wordPos;
	Four	byteOffset;
	Four	inIndex;
	Four	outIndex;
	Four    oldOutIndex;
	Four	postingStartIndex;
	Four	i;
	Four    j;
	char 	*buffer;
    char 	tempData[sizeof(Four)*2];
	char	encodedData[(sizeof(docId) + sizeof(PageNo) + sizeof(SlotNo) + sizeof(Unique) + sizeof(count))*2];
    Four	encodedDataLen;
    Four	encodedDataIndex;
    Four    sentencePosGap;
    Four	oldSentencePos; 
    Four    byteOffsetGap;
    Four	oldByteOffset;
    Four    postingListLen;
    Four    embeddedAttrsLen;

    if(ptrToPostingList == NULL || ptrToCompressedPostingList == NULL)
        LOM_ERROR(handle, eBADPARAMETER_LOM);	 

	buffer              = ptrToCompressedPostingList;
    postingListLen      = *lengthOfPostingList;
	postingStartIndex   = 0;
	outIndex            = 0;
	oldOutIndex         = 0;
    oldDocId            = 0;
    
    if(encodeDocIdFlag && postingInfo->isContainingTupleID && 
       postingInfo->isContainingSentenceAndWordNum && !postingInfo->isContainingByteOffset)
    {
        for(i = 0; i < nPostings; i++)
    	{
    		inIndex = postingStartIndex;
    		encodedDataIndex = 0;
    
			/* read data */
    		memcpy(&postingLength, ptrToPostingList + inIndex, sizeof(postingLength));
    		inIndex += sizeof(postingLength);		

    		memcpy(&docId, ptrToPostingList + inIndex, sizeof(docId));
            inIndex += sizeof(docId);

			memcpy(&pageNo, ptrToPostingList + inIndex, sizeof(pageNo));
			inIndex += sizeof(pageNo);

			memcpy(&volNo, ptrToPostingList + inIndex, sizeof(volNo));
			inIndex += sizeof(volNo);

			memcpy(&slotNo, ptrToPostingList + inIndex, sizeof(slotNo));
			inIndex += sizeof(slotNo);

			memcpy(&unique, ptrToPostingList + inIndex, sizeof(unique));
			inIndex += sizeof(unique);

    		memcpy(&count, ptrToPostingList + inIndex, sizeof(count));
    		inIndex += sizeof(count);

    		postingLengthIndex = outIndex;    
    		outIndex += sizeof(postingLength);

            docIdGap = docId - oldDocId; 
            oldDocId = docId;
            
#ifdef PRINT_POSTING
			printf("%d %d %d %d %d| ", docIdGap, pageNo, slotNo, unique, count);
#endif

            LOM_VARIABLE_BYTE_ENCODING(docIdGap, tempData, encodedData, encodedDataLen);
            encodedDataIndex += encodedDataLen;
    
    
        	LOM_VARIABLE_BYTE_ENCODING(pageNo, tempData, encodedData + encodedDataIndex, encodedDataLen);
        	encodedDataIndex += encodedDataLen;
                	

   			LOM_VARIABLE_BYTE_ENCODING(slotNo, tempData, encodedData + encodedDataIndex, encodedDataLen);
   			encodedDataIndex += encodedDataLen;
   			
   	     	LOM_VARIABLE_BYTE_ENCODING(unique, tempData, encodedData + encodedDataIndex, encodedDataLen);
   	     	encodedDataIndex += encodedDataLen;
			    
    		_count = count;
            LOM_VARIABLE_BYTE_ENCODING(_count, tempData, encodedData + encodedDataIndex, encodedDataLen);
            encodedDataIndex += encodedDataLen;
    		
    		if(outIndex + sizeof(postingLength) + encodedDataIndex > postingListLen) 
    			DEALLOCMEM_lom_Text_PostingList_Compression(handle, eINTERNAL_LOM);
    			
            memcpy(buffer + outIndex, encodedData, encodedDataIndex);
            outIndex += encodedDataIndex;
    
           	oldSentencePos = 0;

			for(j = 0; j < count; j++)
			{
			    encodedDataIndex = 0;

				/* read data */
				memcpy(&sentencePos, ptrToPostingList + inIndex, sizeof(sentencePos));
				inIndex += sizeof(sentencePos);
			    
				memcpy(&wordPos, ptrToPostingList + inIndex, sizeof(wordPos));
				inIndex += sizeof(wordPos);

                sentencePosGap = sentencePos - oldSentencePos;
                oldSentencePos = sentencePos;

#ifdef PRINT_POSTING
				printf("%d %d ", sentencePosGap, wordPos);
#endif

   		     	LOM_VARIABLE_BYTE_ENCODING(sentencePosGap, tempData, encodedData, encodedDataLen);
       	     	encodedDataIndex += encodedDataLen;
   		     				
   		     	LOM_VARIABLE_BYTE_ENCODING(wordPos, tempData, encodedData + encodedDataIndex, encodedDataLen);
   		     	encodedDataIndex += encodedDataLen;
				
				if(outIndex + encodedDataIndex > postingListLen) 
					DEALLOCMEM_lom_Text_PostingList_Compression(handle, eINTERNAL_LOM);

   		     	memcpy(buffer + outIndex, encodedData, encodedDataIndex);
   		     	outIndex += encodedDataIndex;
			}
			
#ifdef PRINT_POSTING
			printf("\n");
#endif

    		postingStartIndex += postingLength + sizeof(postingLength);
            embeddedAttrsLen = postingStartIndex - inIndex;
            
    		if(outIndex + embeddedAttrsLen > postingListLen) 
    			DEALLOCMEM_lom_Text_PostingList_Compression(handle, eINTERNAL_LOM);
    			
    		memcpy(buffer + outIndex, ptrToPostingList + inIndex, embeddedAttrsLen);
    		outIndex += embeddedAttrsLen;
    
    		postingLength = outIndex - oldOutIndex - sizeof(postingLength); 
    		memcpy(buffer + postingLengthIndex, &postingLength, sizeof(postingLength));
    		oldOutIndex = outIndex;
    	}
    }
    else
    {
        for(i = 0; i < nPostings; i++)
    	{
    		inIndex = postingStartIndex;
    
    		memcpy(&postingLength, ptrToPostingList + inIndex, sizeof(postingLength));
    		postingLengthIndex = outIndex;    
    		inIndex += sizeof(postingLength);		
    		outIndex += sizeof(postingLength);

            if(encodeDocIdFlag)
            {
                memcpy(&docId, ptrToPostingList + inIndex, sizeof(docId));
       		    inIndex += sizeof(docId);
       		
                docIdGap = docId - oldDocId; 
                oldDocId = docId;

#ifdef PRINT_POSTING
			printf("%d ", docIdGap);
#endif
                
                LOM_VARIABLE_BYTE_ENCODING(docIdGap, tempData, encodedData, encodedDataLen);
                
                if(outIndex + encodedDataLen > postingListLen) 
    			    DEALLOCMEM_lom_Text_PostingList_Compression(handle, eINTERNAL_LOM);
    			    
                memcpy(buffer + outIndex, encodedData, encodedDataLen);
                outIndex += encodedDataLen;
            }
            else
            {
                if(outIndex + sizeof(docId) > postingListLen) 
    			    DEALLOCMEM_lom_Text_PostingList_Compression(handle, eINTERNAL_LOM);
    			    
                memcpy(buffer + outIndex, ptrToPostingList + inIndex, sizeof(docId));
                inIndex += sizeof(docId);
                outIndex += sizeof(docId);
            }
            
            encodedDataIndex = 0;
                		
    		if(postingInfo->isContainingTupleID) 
    		{
    			/* read data */
    			memcpy(&pageNo, ptrToPostingList + inIndex, sizeof(pageNo));
    			inIndex += sizeof(pageNo);

    			memcpy(&volNo, ptrToPostingList + inIndex, sizeof(volNo));
    			inIndex += sizeof(volNo);

				memcpy(&slotNo, ptrToPostingList + inIndex, sizeof(slotNo));
    			inIndex += sizeof(slotNo);

				memcpy(&unique, ptrToPostingList + inIndex, sizeof(unique));
    			inIndex += sizeof(unique);

#ifdef PRINT_POSTING
			printf("%d %d %d ", pageNo, slotNo, unique);
#endif
    			
            	LOM_VARIABLE_BYTE_ENCODING(pageNo, tempData, encodedData, encodedDataLen);
            	encodedDataIndex += encodedDataLen;
    			
       			LOM_VARIABLE_BYTE_ENCODING(slotNo, tempData, encodedData + encodedDataIndex, encodedDataLen);
       			encodedDataIndex += encodedDataLen;
    			
       	     	LOM_VARIABLE_BYTE_ENCODING(unique, tempData, encodedData + encodedDataIndex, encodedDataLen);
       	     	encodedDataIndex += encodedDataLen;
       	    }
			    
    		memcpy(&count, ptrToPostingList + inIndex, sizeof(count));
    		_count = count;
    		inIndex += sizeof(count);
    		
#ifdef PRINT_POSTING
			printf("%d |", count);
#endif
    		
            LOM_VARIABLE_BYTE_ENCODING(_count, tempData, encodedData + encodedDataIndex, encodedDataLen);
            encodedDataIndex += encodedDataLen;
    		
    		if(outIndex + sizeof(postingLength) + encodedDataIndex > postingListLen) 
    			DEALLOCMEM_lom_Text_PostingList_Compression(handle, eINTERNAL_LOM);
    			
            memcpy(buffer + outIndex, encodedData, encodedDataIndex);
            outIndex += encodedDataIndex;
    
            if(postingInfo->isContainingSentenceAndWordNum || postingInfo->isContainingByteOffset) 
    		{
               	oldSentencePos = 0;
               	oldByteOffset = 0;
    
    			for(j = 0; j < count; j++)
    			{
    			    encodedDataIndex = 0;
    			    
    				memcpy(&sentencePos, ptrToPostingList + inIndex, sizeof(sentencePos));
    				inIndex += sizeof(sentencePos);
    				
    				memcpy(&wordPos, ptrToPostingList + inIndex, sizeof(wordPos));
    				inIndex += sizeof(wordPos);
    
                    sentencePosGap = sentencePos - oldSentencePos;
                    oldSentencePos = sentencePos;
    
       		     	LOM_VARIABLE_BYTE_ENCODING(sentencePosGap, tempData, encodedData, encodedDataLen);
           	     	encodedDataIndex += encodedDataLen;

       		     	LOM_VARIABLE_BYTE_ENCODING(wordPos, tempData, encodedData + encodedDataIndex, encodedDataLen);
       		     	encodedDataIndex += encodedDataLen;
    				
       		     	if(postingInfo->isContainingByteOffset) 
    				{
    					memcpy(&byteOffset, ptrToPostingList + inIndex, sizeof(byteOffset));
    					inIndex += sizeof(byteOffset);
    
                        byteOffsetGap = byteOffset - oldByteOffset;
                        oldByteOffset = byteOffset;
    
       		     		LOM_VARIABLE_BYTE_ENCODING(byteOffsetGap, tempData, encodedData + encodedDataIndex, encodedDataLen);
       		     		encodedDataIndex += encodedDataLen;
    				}
    				
                    if(outIndex + encodedDataIndex > postingListLen) 
    					DEALLOCMEM_lom_Text_PostingList_Compression(handle, eINTERNAL_LOM);
    
       		     	memcpy(buffer + outIndex, encodedData, encodedDataIndex);
       		     	outIndex += encodedDataIndex;
        		}
			}
			
    		postingStartIndex += postingLength + sizeof(postingLength);
            embeddedAttrsLen = postingStartIndex - inIndex;
            
    		if(outIndex + embeddedAttrsLen > postingListLen) 
    			DEALLOCMEM_lom_Text_PostingList_Compression(handle, eINTERNAL_LOM);
    			
    		memcpy(buffer + outIndex, ptrToPostingList + inIndex, embeddedAttrsLen);
    		outIndex += embeddedAttrsLen;
    
    		postingLength = outIndex - oldOutIndex - sizeof(postingLength); 
    		memcpy(buffer + postingLengthIndex, &postingLength, sizeof(postingLength));
    		oldOutIndex = outIndex;

#ifdef PRINT_POSTING
			printf(", %d\n", postingLength);
#endif    		
    	}
    }
    
	*lengthOfPostingList    = outIndex;
	*volNoOfPostingTupleID  = volNo;
	*lastDocId              = docId;

    return eNOERROR;
}

Four lom_Text_PostingList_Uncompression
(
    lom_Text_PostingInfoForReading  *postingInfo,           		/* IN */
    Four                            nPostings,              		/* IN */
	Boolean							hasPostingLength,				/* IN */
	Four							postingLengthListBufferSize,	/* IN */
    char                            *postingLengthListBuffer,		/* INOUT */
    Four                            postingListBufferSize,    		/* IN */
    char                            *postingListBuffer,				/* INOUT */
	VolNo							volNoOfPostingTupleID,			/* IN */
	Four							scanDirection,					/* IN */
	Four                            uncompressedPostingListBufferSize,    
	char                            *uncompressedPostingListBuffer,
	Boolean							decodeDocIdFlag,
	Four				            lastDocId
)
{
    Four    docId; 
    Four    docIdGap; 
    PageNo  pageNo;
    VolNo   volNo;
    SlotNo  slotNo;
    Unique  unique;
    Four    count;
    Four    sentencePos;
    Two     wordPos;
    Four    sentencePosGap = 0;
    Four    byteOffsetGap = 0;
    Four    byteOffset;
    Four    i, j;
    Four    postingLengthOfInput;
    Four    postingLengthOfOutput;
    Four    postingStartIndexOfInput;
    Four    postingStartIndexOfOutput;
    Four    embeddedAttrsLength = 0;
    Four    inIndex;
    Four    outIndex;
    
	docId = lastDocId;	

    if(scanDirection != FORWARD && hasPostingLength == SM_TRUE)
    	LOM_ERROR(handle, eBADPARAMETER_LOM);
    		
    if(scanDirection == BACKWARD_NOORDERING)
    {	
		Four	oldDocIdGap = 0;
        char	*positionList = NULL;
    	Four    positionListSize = LOM_MAXPOSITIONLENGTH * (sizeof(Four) + sizeof(Two));
    	Four	positionListOffset;
    		
    	positionList = (char*)malloc(positionListSize);
       	if(positionList == NULL) LOM_ERROR(handle, eOUTOFMEMORY_LOM);
    
    	postingStartIndexOfInput = 0;
    	postingStartIndexOfOutput = uncompressedPostingListBufferSize;
        
        for(i = 0; i < nPostings; i++)
        {
    		inIndex = postingStartIndexOfInput;
    		memcpy(&postingLengthOfInput, &postingLengthListBuffer[-i*sizeof(Four)], sizeof(Four));		
    		postingLengthOfOutput = 0;
            
			if(!decodeDocIdFlag)
			{
            	memcpy(&docId, &postingListBuffer[inIndex], sizeof(docId));
    			inIndex += sizeof(docId);
			}
			else
			{
    			LOM_VARIABLE_BYTE_DECODING(postingListBuffer, inIndex, docIdGap);
				docId -= oldDocIdGap;
				oldDocIdGap = docIdGap;
			}
           	postingLengthOfOutput += sizeof(docId);
            
            if(postingInfo->isContainingTupleID) 
            {   
    			/* pageNo uncompression */
    			LOM_VARIABLE_BYTE_DECODING(postingListBuffer, inIndex, pageNo);
                
    			/* volNo uncompression */
    			volNo = volNoOfPostingTupleID;
    
    			/* slotNo uncompression */
    			LOM_VARIABLE_BYTE_DECODING(postingListBuffer, inIndex, slotNo);
    
    			/* unique uncompression */
    			LOM_VARIABLE_BYTE_DECODING(postingListBuffer, inIndex, unique);
    			
    			postingLengthOfOutput += sizeof(pageNo) + sizeof(volNo) + sizeof(slotNo) + sizeof(unique);
    		}
    
    		/* count uncompression */
    		LOM_VARIABLE_BYTE_DECODING(postingListBuffer, inIndex, count);
            postingLengthOfOutput += sizeof(count);
            
    
            if(postingInfo->isContainingSentenceAndWordNum || postingInfo->isContainingByteOffset)
            {  
               	sentencePos = 0;
               	byteOffset = 0;
               	positionListOffset = 0;
    
                for(j = 0; j < count; j++)
                {
    				/* sentencePos uncompression */
    				LOM_VARIABLE_BYTE_DECODING(postingListBuffer, inIndex, sentencePosGap);
                    sentencePos += sentencePosGap;
    
    				/* wordPos uncompression */
    				LOM_VARIABLE_BYTE_DECODING(postingListBuffer, inIndex, wordPos);
    
    				if(positionListOffset + sizeof(sentencePos) + sizeof(wordPos) > positionListSize) 
					{
						if(positionList != NULL) free(positionList);
						LOM_ERROR(handle, eINTERNAL_LOM);
					}
    
    				memcpy(&positionList[positionListOffset], &sentencePos, sizeof(sentencePos));
    				positionListOffset += sizeof(sentencePos);
    
    				memcpy(&positionList[positionListOffset], &wordPos, sizeof(wordPos));
    				positionListOffset += sizeof(wordPos);
    				
    				postingLengthOfOutput += sizeof(sentencePos) + sizeof(wordPos);
    				{
    					/* byte offset uncompression */
    					LOM_VARIABLE_BYTE_DECODING(postingListBuffer, inIndex, byteOffsetGap);
                    	byteOffset += byteOffsetGap;
    
                        if(positionListOffset + sizeof(byteOffset) > positionListSize) 
						{
							if(positionList != NULL) free(positionList);
    					    LOM_ERROR(handle, eINTERNAL_LOM);
						}
    
        				memcpy(&positionList[positionListOffset], &byteOffset, sizeof(byteOffset));
        				positionListOffset += sizeof(byteOffset);
        				
        				postingLengthOfOutput += sizeof(byteOffset);
    				}
    			}
    		}

            embeddedAttrsLength         = postingLengthOfInput - (inIndex - postingStartIndexOfInput);
   			postingLengthOfOutput       += embeddedAttrsLength;
   			
   			memcpy(&postingLengthListBuffer[-i*sizeof(Four)], &postingLengthOfOutput, sizeof(Four));
   			memcpy(&postingLengthOfInput, &postingLengthListBuffer[-(i+1)*sizeof(Four)], sizeof(Four));

			postingStartIndexOfInput    -= postingLengthOfInput;
			postingStartIndexOfOutput   -= postingLengthOfOutput;	
			outIndex                    = postingStartIndexOfOutput;    		    
						
    		if(postingListBufferSize + postingLengthOfInput < 0)
    		{
    		    if(positionList != NULL) free(positionList);
                LOM_ERROR(handle, eINTERNAL_LOM);
    		}
    		
    		if(postingStartIndexOfOutput < 0) 
			{
				if(positionList != NULL) free(positionList);
                LOM_ERROR(handle, eBIGGERPOSTINGBUFFERNEEDED_LOM);
			}
			    
    		/* copy posting */
    		memcpy(&uncompressedPostingListBuffer[outIndex], &docId, sizeof(docId));
            outIndex += sizeof(docId);
            
            memcpy(&uncompressedPostingListBuffer[outIndex], &pageNo, sizeof(pageNo));
        	outIndex += sizeof(pageNo);
    
        	memcpy(&uncompressedPostingListBuffer[outIndex], &volNo, sizeof(volNo));
        	outIndex += sizeof(volNo);
    
        	memcpy(&uncompressedPostingListBuffer[outIndex], &slotNo, sizeof(slotNo));
        	outIndex += sizeof(slotNo);
    
        	memcpy(&uncompressedPostingListBuffer[outIndex], &unique, sizeof(unique));
        	outIndex += sizeof(unique);
    		
    		memcpy(&uncompressedPostingListBuffer[outIndex], &count, sizeof(count));
        	outIndex += sizeof(count);
        	
    		memcpy(&uncompressedPostingListBuffer[outIndex], positionList, positionListOffset);
    		outIndex += positionListOffset;
    		
    		/* copy embedded attributes */
    		if(embeddedAttrsLength != 0)
    		    memcpy(&uncompressedPostingListBuffer[outIndex], &postingListBuffer[inIndex], embeddedAttrsLength);

    	} 

		if(positionList != NULL)
    		free(positionList);

    } /* if(scanDirection == BACKWARD_NOORDERING) */

    else
    {
    	postingStartIndexOfInput = 0;
    	postingStartIndexOfOutput = 0;
        
        for(i = 0; i < nPostings; i++)
        {
            postingLengthOfOutput = 0;
    		inIndex = postingStartIndexOfInput;
    		outIndex = postingStartIndexOfOutput;
			/*
			printf("outIndex: %d\n", outIndex);
			*/
    
    		if(!hasPostingLength)
    		{
    			memcpy(&postingLengthOfInput, &postingLengthListBuffer[i*sizeof(Four)], sizeof(Four));
    		}
    		else
    		{
            	memcpy(&postingLengthOfInput, &postingListBuffer[inIndex], sizeof(Four));
            	postingLengthOfInput += sizeof(Four);
            	inIndex += sizeof(Four);
            	outIndex += sizeof(Four);
    		}
    
			if(!decodeDocIdFlag)
			{
            	memcpy(&docId, &postingListBuffer[inIndex], sizeof(docId));
    			inIndex += sizeof(docId);
			}
			else
			{
    			LOM_VARIABLE_BYTE_DECODING(postingListBuffer, inIndex, docIdGap);
				docId += docIdGap;
			}

           	if(outIndex + sizeof(docId) > uncompressedPostingListBufferSize) 
            	LOM_ERROR(handle, eBIGGERPOSTINGBUFFERNEEDED_LOM);    		    
            
            memcpy(&uncompressedPostingListBuffer[outIndex], &docId, sizeof(docId));
            outIndex += sizeof(docId);
            
            if(postingInfo->isContainingTupleID) 
            {   
    			/* pageNo uncompression */
    			LOM_VARIABLE_BYTE_DECODING(postingListBuffer, inIndex, pageNo);
                
    			/* volNo uncompression */
    			volNo = volNoOfPostingTupleID;
                
    			/* slotNo uncompression */
    			LOM_VARIABLE_BYTE_DECODING(postingListBuffer, inIndex, slotNo);
    			
    			/* unique uncompression */
    			LOM_VARIABLE_BYTE_DECODING(postingListBuffer, inIndex, unique);
    		    
                if(outIndex + sizeof(pageNo) + sizeof(volNo) + sizeof(slotNo) + sizeof(unique) > uncompressedPostingListBufferSize) 
                    LOM_ERROR(handle, eBIGGERPOSTINGBUFFERNEEDED_LOM);    		    
    		    
    		    memcpy(&uncompressedPostingListBuffer[outIndex], &pageNo, sizeof(pageNo));
        	    outIndex += sizeof(pageNo);
        	    
    		    memcpy(&uncompressedPostingListBuffer[outIndex], &volNo, sizeof(volNo));
        	    outIndex += sizeof(volNo);
        	    
    		    memcpy(&uncompressedPostingListBuffer[outIndex], &slotNo, sizeof(slotNo));
        	    outIndex += sizeof(slotNo);
        	    
    		    memcpy(&uncompressedPostingListBuffer[outIndex], &unique, sizeof(unique));
        	    outIndex += sizeof(unique);	
    		}
    
    		/* count uncompression */
    		LOM_VARIABLE_BYTE_DECODING(postingListBuffer, inIndex, count);
            memcpy(&uncompressedPostingListBuffer[outIndex], &count, sizeof(count));
        	outIndex += sizeof(count);
            
            if(postingInfo->isContainingSentenceAndWordNum || postingInfo->isContainingByteOffset)
            {  
               	sentencePos = 0;
               	byteOffset = 0;
    
                for(j = 0; j < count; j++)
                {
    				/* sentencePos uncompression */
    				LOM_VARIABLE_BYTE_DECODING(postingListBuffer, inIndex, sentencePosGap);
                    sentencePos += sentencePosGap;
    
    				/* wordPos uncompression */
    				LOM_VARIABLE_BYTE_DECODING(postingListBuffer, inIndex, wordPos);
    
                    if(outIndex + sizeof(sentencePos) + sizeof(wordPos) > uncompressedPostingListBufferSize) 
    					LOM_ERROR(handle, eBIGGERPOSTINGBUFFERNEEDED_LOM);
                    
                    memcpy(&uncompressedPostingListBuffer[outIndex], &sentencePos, sizeof(sentencePos));
    		        outIndex += sizeof(sentencePos); 
    
    				memcpy(&uncompressedPostingListBuffer[outIndex], &wordPos, sizeof(wordPos));
    				outIndex += sizeof(wordPos);
    
					/*
    				fprintf(stderr, "|%d %d ", sentencePos, wordPos);
					*/
					
					
    
    				if(postingInfo->isContainingByteOffset) 
    				{
    					/* byte offset uncompression */
    					LOM_VARIABLE_BYTE_DECODING(postingListBuffer, inIndex, byteOffsetGap);
                    	byteOffset += byteOffsetGap;
    
                        if(outIndex + sizeof(byteOffset) > uncompressedPostingListBufferSize) 
    					    LOM_ERROR(handle, eBIGGERPOSTINGBUFFERNEEDED_LOM);
        				
        				memcpy(&uncompressedPostingListBuffer[outIndex], &byteOffset, sizeof(byteOffset));
    				    outIndex += sizeof(byteOffset);
    				}
    			}
    		}
            
            embeddedAttrsLength         = postingLengthOfInput - (inIndex - postingStartIndexOfInput);
		    postingLengthOfOutput       = outIndex - postingStartIndexOfOutput + embeddedAttrsLength;
    		
    		if(!hasPostingLength)
    		{	
    			memcpy(&postingLengthListBuffer[i * sizeof(Four)], &postingLengthOfOutput, sizeof(Four));
    		}
    		else
    		{
    		    memcpy(&uncompressedPostingListBuffer[postingStartIndexOfOutput], &postingLengthOfOutput, sizeof(Four));
    		}			
    	    
    	    if(embeddedAttrsLength != 0)
    	    {
        		if(outIndex + embeddedAttrsLength > uncompressedPostingListBufferSize) 
                    LOM_ERROR(handle, eBIGGERPOSTINGBUFFERNEEDED_LOM);
        					    
        		/* copy embedded attributes */
        		memcpy(&uncompressedPostingListBuffer[outIndex], &postingListBuffer[inIndex], embeddedAttrsLength);
    		}

			postingStartIndexOfInput    += postingLengthOfInput;
			postingStartIndexOfOutput   += postingLengthOfOutput;	
    	}
	}
    
    return eNOERROR;
}

Four lom_Text_GetOffsetOfEmbeddedAttrs
(
    lom_Text_PostingInfoForReading  *postingInfo,           /* IN */
    char                            *posting,				/* IN */
	Four							*offsetForEmbeddedAttrs	/* OUT */
)
{
	Four	postingLength;
    Four    docId; 
    PageNo  pageNo;
    SlotNo  slotNo;
    Unique  unique;
    Four    count;
    Four    sentencePos;
    Two     wordPos;
    Four    byteOffset;
    Four    i;
    Four    offset;

	memcpy(&docId, posting, sizeof(docId));
	offset = sizeof(docId);

    if(postingInfo->isContainingTupleID) 
    {   
		LOM_VARIABLE_BYTE_DECODING(posting, offset, pageNo);
		LOM_VARIABLE_BYTE_DECODING(posting, offset, slotNo);
		LOM_VARIABLE_BYTE_DECODING(posting, offset, unique);
	}

	LOM_VARIABLE_BYTE_DECODING(posting, offset, count);

    if(postingInfo->isContainingSentenceAndWordNum || postingInfo->isContainingByteOffset)
    {  
        for(i = 0; i < count; i++)
        {
			LOM_VARIABLE_BYTE_DECODING(posting, offset, sentencePos);
			LOM_VARIABLE_BYTE_DECODING(posting, offset, wordPos);
    		
			if(postingInfo->isContainingByteOffset) 
			{
				LOM_VARIABLE_BYTE_DECODING(posting, offset, byteOffset);
			}
		}
	}

	*offsetForEmbeddedAttrs = offset + sizeof(postingLength);
    return eNOERROR;
}

#endif
