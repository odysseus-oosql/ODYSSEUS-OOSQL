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
/*    ODYSSEUS/COSMOS General-Purpose Large-Scale Object Storage System --    */
/*    Coarse-Granule Locking (Volume Lock) Version                            */
/*    Version 3.0                                                             */
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
/*
 * Module : OM_Sort.c
 * 
 * Description :
 *  Sort a data file.
 *
 * Exports :
 *  Four OM_SortInto(VolID, ObjectID*, FileID*, sm_CatOverlayForData*, SortKeyDesc*, omGetKeyAttrsFuncPtr_T,
 *                   void*, Boolean, Boolean, Pool*, DeallocListElem*)
 */

#include <assert.h>      /* for assert */
#include <string.h>      /* for memcpy */
#include "common.h"
#include "trace.h"
#include "BfM.h"
#include "OM_Internal.h"
#include "RDsM_Internal.h"	
#include "Util.h"
#include "Util_Sort.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


/*
 *  Global Variable - MUST BE REMOVED !!
 */

/*
 *  Local Function Prototype
 */
Four om_InitOutFilePage(Four, FileID*, VarArray*, Four, Boolean, SlottedPage*);



/* ========================================
 *  OM_SortInto()
 * =======================================*/

/* 
 * Function OM_SortInto(ObjectID*, sm_CatOverlayForData*, SortKeyDesc*, omGetKeyAttrsFuncPtr_T, void*, Boolean, Boolean)
 *
 * Description :
 *  Sort a data file.
 *  If 'newFileFlag' is TRUE, result of sort is inserted into new output file
 *  If 'newFileFlag' is FALSE, result of sort is inserted into input file
 *  Note!! in case of regular file, old pages are internally preserved for recovery until commit or abort
 *
 * Return Values :
 *  Error Code.
 *   eBADPARAMETER
 *
 * Side Effects :
 *  If 'newFileFlag' is FALSE, order of objects in source file will be changed.
 */
Four OM_SortInto(
    Four handle,
    VolID                  tmpVolId,                   /* IN  temporary volume ID in which sort stream is created */
    ObjectID*              catObjForSortFile,          /* IN  ID of catalog object which contains source file's information */
    FileID*                sortIntoFid,                /* IN  ID of sort result file */ 
    sm_CatOverlayForData*  catOverlayForSortFileInto,  /* OUT sorted file's Info */
    SortKeyDesc*           sortKeyDesc,                /* IN  sort key */
    omGetKeyAttrsFuncPtr_T getKeyAttrs,                /* IN  object analysis function */
    void*                  schema,                     /* IN  schema for analysis function */
    Boolean                newFileFlag,                /* IN  flag which indicates source file is preserved */
    Boolean                tmpFileFlag,                /* IN  output file is a temporary file? */
    Pool*                  dlPool,                     /* INOUT pool of dealloc list elements */ 
    DeallocListElem*       dlHead)                     /* INOUT head of dealloc list */ 
{
    Four                   e;                          /* error number */
    Two                    i;                          /* index variable */
    Four                   index;
    Two                    idx;
    SlottedPage*           catPage;                    /* pointer to buffer containing the catalog */
    sm_CatOverlayForData*  catEntry;                   /* pointer to sort file catalog information */
    FileID                 sortFid;                    /* ID of sort file */
    Two                    sortEff;                    /* extent fill factor of the sort file */
    PhysicalFileID         pSortFid;                   /* physical ID of file */
    Boolean                sortFileTmpFlag;            /* flag which indicates input file is temporary or not */

    Four                   firstExtNo;                 /* first extent number of the file */
    PageID                 firstOutPid;                /* first page ID of output file */
    PageID                 pid;                        /* page id of the buffer */
    PageID                 nearPid;                    /* near page id */
    Four                   outPnoIdx;                  /* current index in outPnoArray */
    Four                   numOutPno;                  /* # of page numbers in outPnoArray */
    VarArray               outPnoArray;                /* array which contains pages of output file */
    Four                   numInPno;                   /* # of page numbers in inPnoArray */
    VarArray               inPnoArray;                 /* array which contains pages of input file */
    DeallocListElem*       dlElem;                     /* element of the deallocation list */

    Boolean                done = FALSE;               /* flag which indicates sort stream is empty or not */
    Four                   sortStreamId;               /* ID of sort stream */
    Four                   numSortTuples;              /* # of tuples from sort stream */
    SortStreamTuple        sortTuple;                  /* tuple for sort stream */
    omSortKeyAttrInfo      sortAttrInfo;               /* information about sort key attributes */
    SortTupleDesc          sortTupleDesc;              /* key description of sort stream */

    SlottedPage            buffer;                     /* buffer for slotted page */
    Two                    slotIdx;                    /* index of slot in slotted page */
    Four                   offset;                     /* offset of object in slotted page */
    Four                   neededSpace;                /* needed space for insert object into buffer */
    Four                   alignedLen;                 /* aligned length of inserted object */
    Four                   uniqueNum;                  /* # of allocated unique number */
    Object*                obj;                        /* object pointer which points object in the buffer */
    Object*                origObj;                    /* object pointer which points original object */
    Four                   tempObjLen;                 /* length of temporary object */
    char                   tempObj[LRGOBJ_THRESHOLD];  /* buffer for temporary object */
    char                   bufferForOrigObj[LRGOBJ_THRESHOLD]; /* buffer for original object */
#ifndef NDEBUG
    Four count = 0;
#endif


    /* 
     *  Get the sort file's ID and its extent fill factor. 
     */
    e = BfM_GetTrain(handle, (TrainID*)catObjForSortFile, (char**)&catPage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    GET_PTR_TO_CATENTRY_FOR_DATA(catObjForSortFile, catPage, catEntry);

    sortFid = catEntry->fid;
    sortEff = catEntry->eff;
    MAKE_PHYSICALFILEID(pSortFid, sortFid.volNo, catEntry->firstPage);

    e = BfM_FreeTrain(handle, (TrainID*)catObjForSortFile, PAGE_BUF);
    if (e < 0) ERR(handle, e);


    /* 
     *  Get 'sortFileTmpFlag' and error check
     */

    /* get 'sortFileTmpFlag' */
    e = om_IsTemporary(handle, &sortFid, &sortFileTmpFlag);
    if (e < 0) ERR(handle, e);

    /* if new file isn't created, source file and output file must be same */
    if (!newFileFlag) {
        if (sortFileTmpFlag != tmpFileFlag) ERR(handle, eBADPARAMETER);
        if (!EQUAL_FILEID(sortFid, *sortIntoFid)) ERR(handle, eBADPARAMETER);
    }
    /* if new file is created, source file and output file must have diffrent fid */
    else {
        if (EQUAL_FILEID(sortFid, *sortIntoFid)) ERR(handle, eBADPARAMETER);
    }


    /*
     *  Set 'curVolNo'
     */
    OM_PER_THREAD_DS(handle).curVolNo = sortFid.volNo; 


    /* 
     *  Initialize inPnoArray & outPnoArray
     */ 

    e = Util_initVarArray(handle, &inPnoArray, sizeof(PageNo), SIZE_OF_PNO_ARRAY);
    if (e < 0) ERR(handle, e);

    e = Util_initVarArray(handle, &outPnoArray, sizeof(PageNo), SIZE_OF_PNO_ARRAY);
    if (e < 0) ERR(handle, e);



    /* 
     *  Get 'sortAttrinfo' & 'sortTupleDesc'
     */

    /* Note!! sortAttrInfo doesn't have proper offset value */
    getKeyAttrs(NULL, schema, sortKeyDesc, &sortAttrInfo);

    /* set 'nparts' */
    sortTupleDesc.nparts = sortAttrInfo.nparts;

    /* set 'hdrSize' */
    sortTupleDesc.hdrSize = sizeof(ObjectHdr) + OFFSETARRAYSIZE(sortAttrInfo.nparts);

    /* set each part information */
    for (idx = 0; idx < sortAttrInfo.nparts; idx++ ) {
        sortTupleDesc.parts[idx].type = sortAttrInfo.parts[idx].type;
        sortTupleDesc.parts[idx].length = sortAttrInfo.parts[idx].length;
        sortTupleDesc.parts[idx].flag = sortKeyDesc->parts[idx].flag;
    }


    /*
     *  Open sort stream
     */
    sortStreamId = Util_OpenSortStream(handle, tmpVolId, &sortTupleDesc);
    if (sortStreamId < 0) ERR(handle, sortStreamId);


    /* 
     *  Insert objects of input file into sort stream
     */

    /* initialize numInPno */
    numInPno = 0;

    /* Get the first extent number of input file */
    e = RDsM_PageIdToExtNo(handle, (PageID *)&pSortFid, &firstExtNo);
    if (e < 0) ERR(handle, e);

    /* until all pages of input file go through the buffer */
    for (pid = pSortFid; pid.pageNo != NIL; pid.pageNo = buffer.header.nextPage) {

        /* insert pid into inPnoArray */
        if (numInPno >= inPnoArray.nEntries) {
            /* if Pno array is full, Pno array is doubled */
            e = Util_doublesizeVarArray(handle, &inPnoArray, sizeof(PageNo));
            if (e < 0) ERR(handle, e);
        }
        PNO_ARRAY(inPnoArray)[numInPno++] = pid.pageNo;


        /* read input file's page into 'buffer' */
        /* Note!! you must use 'BfM_readTrain()' not 'BfM_ReadTrain()' because sort doesn't use COSMOS buffer */
        e = BfM_readTrain(handle, &pid, (char *)&buffer, PAGE_BUF);
        if (e < 0) ERR(handle, e);


        /* for each object in 'buffer', add sortkey and insert it into sort stream */
        for (i = 0; i < buffer.header.nSlots; i++ ) {

            /* get offset value */
            offset = buffer.slot[-i].offset;

            /* if slot is empty, skip it */
            if (offset == EMPTYSLOT) continue;

            /* get object pointer */
            obj = (Object *)&(buffer.data[offset]);


            /* if object is moved object, skip it!! */
            if (obj->header.properties & P_MOVED) continue;

            /* if object is forwarded object, convert to normal object */
            if (obj->header.properties & P_FORWARDED) {
                obj->header.properties ^= P_FORWARDED;
            }

            /* if object is large object whose root is in slotted page, convert to normal large object */
            if (obj->header.properties & P_LRGOBJ_ROOTWITHHDR) {
                e = om_ConvertToLargeObject(handle, obj, firstExtNo, sortEff);
                if (e < 0) ERR(handle, e);
            }

            /* get 'tempObj' */
            e = om_GetTempObject(handle, obj, (Object *)tempObj, &tempObjLen, sortKeyDesc, getKeyAttrs, schema);
            if (e < 0) ERR(handle, e);

            /* set sortStreamTuple */
            sortTuple.len = tempObjLen;
            sortTuple.data = tempObj;

            /* insert 'tempObj' into sortStream */
            e = Util_PutTuplesIntoSortStream(handle, sortStreamId, 1, &sortTuple);
            if (e < 0) ERR(handle, e);

        } /* for i : manipulate one object in buffer */

    } /* for : manipulate one page of input file */


    /*
     *  Sort it!!
     */
    e = Util_SortingSortStream(handle, sortStreamId);
    if (e < 0) ERR(handle, e);


    /*
     *  Prepare output file
     */

    /* In case that pages in temporary files is used for output pages */
    /* Note!! pages in temporary file can be overwritten */
    if (sortFileTmpFlag && !newFileFlag) {

        /* set 'firstOutPid' */
        firstOutPid = pSortFid;

        /* initialize 'outPnoArray' for output file */
        numOutPno = numInPno;
        SWAP_VAR_ARRAY(outPnoArray, inPnoArray);

        /* assertion check */
        assert (PNO_ARRAY(outPnoArray)[0] == pSortFid.pageNo);
    }
    /* In case that new pages is used for output pages */
    /* Note!! pages in regular file must always preserved for recovery */
    if (newFileFlag || !sortFileTmpFlag) {

        /* create the segment for the output file if new file is created */
        if (newFileFlag) {
            e = RDsM_CreateSegment(handle, sortFid.volNo, &firstExtNo);
            if (e < 0) ERR(handle, e);
        }

        /* set 'nearPid' */
        e = RDsM_ExtNoToPageId(handle, sortFid.volNo, firstExtNo, &nearPid);
        if (e < 0) ERR(handle, e);

        /* allocate page for output pages & set 'firstOutPid' */
        e = RDsM_AllocTrains(handle, sortFid.volNo, firstExtNo, &nearPid, sortEff, 1, PAGESIZE2, &firstOutPid);
        if (e < 0) ERR(handle, e);

        /* initialize 'outPnoArray' for new output file */
        numOutPno = 1;
        PNO_ARRAY(outPnoArray)[0] = firstOutPid.pageNo;
    }

    /* initialize 'outPnoIdx' */
    outPnoIdx = 0;

    e = om_InitOutFilePage(handle, sortIntoFid, &outPnoArray, outPnoIdx, tmpFileFlag, &buffer);
    if (e < 0) ERR(handle, e);


    /*
     *  Get sorted object from sort stream & insert it into output file
     */

    /* set origObj pointer for fast access */
    origObj = (Object *) bufferForOrigObj;

    while (1) {

        /*
         *  Get object from sort stream
         */

        /* initialize numSortTuples & sortTuples */
        numSortTuples = 1;
        sortTuple.len = LRGOBJ_THRESHOLD;
        sortTuple.data = tempObj;

        /* get object from sort stream */
        e = Util_GetTuplesFromSortStream(handle, sortStreamId, &numSortTuples, &sortTuple, &done);
        if (e < 0) ERR(handle, e);

        /* boundary check */
        if (done) break;

        /* assertion check */
        assert (numSortTuples == 1);

        /* you must remove sort key from each object!! */
        e = om_RestoreTempObject(handle, (Object *)tempObj, origObj, &sortAttrInfo, firstExtNo, sortEff, newFileFlag);
        if (e < 0) ERR(handle, e);
        

        /* 
         *  Caculate needed space for added object 
         */

        /* calculate 'alignedLen' */
        if (origObj->header.properties & P_LRGOBJ) 
            alignedLen = sizeof(ShortPageID);
        else
            alignedLen = MAX(sizeof(ShortPageID), ALIGNED_LENGTH(origObj->header.length));

        /* calculate 'neededSpace' */
        neededSpace = sizeof(ObjectHdr) + alignedLen + sizeof(SlottedPageSlot);


        /* 
         *  Check space in buffer
         *  Note!! Free space of 'buffer' is always contiguous
         */
        if (SP_CFREE(&buffer) < neededSpace) {

            /* move to next page */
            outPnoIdx ++;

            /* if pages of file are exausted */
            if (outPnoIdx >= numOutPno) {

                /* assertion check */
                assert (outPnoIdx > 0);

                /* get 'nearPid' */
                MAKE_PAGEID(nearPid, sortFid.volNo, PNO_ARRAY(outPnoArray)[outPnoIdx-1]);

                /* allocate new page */
                e = RDsM_AllocTrains(handle, sortFid.volNo, firstExtNo, &nearPid, sortEff, 1, PAGESIZE2, &pid);
                if (e < 0) ERR(handle, e);

                /* inset pid value into outPnoArray */
                if (outPnoIdx >= outPnoArray.nEntries) {
                    /* if Pno array is full, Pno array is doubled */
                    e = Util_doublesizeVarArray(handle, &outPnoArray, sizeof(PageNo));
                    if (e < 0) ERR(handle, e);
                }
                PNO_ARRAY(outPnoArray)[outPnoIdx] = pid.pageNo;

                /* update 'numOutPno' value */
                numOutPno++;
            }
            else {
                MAKE_PAGEID(pid, sortFid.volNo, PNO_ARRAY(outPnoArray)[outPnoIdx]);
            }


            /* flush out old buffer */

            /* set link */
            buffer.header.nextPage = pid.pageNo;

            /* write buffer into disk */
            e = RDsM_WriteTrain(handle, (char *)&buffer, &buffer.header.pid, PAGESIZE2);
            if (e < 0) ERR(handle, e);


            /* initialize buffer for new page */
            e = om_InitOutFilePage(handle, sortIntoFid, &outPnoArray, outPnoIdx, tmpFileFlag, &buffer);
            if (e < 0) ERR(handle, e);
        }


        /* 
         *  Insert object 
         */

        /* set 'offset' */
        offset = buffer.header.free;

        /* where to put object */
        obj = (Object *) &(buffer.data[offset]); 

        /*  copy the object's header */
        obj->header = origObj->header;

        /* copy the data into the object. origObj doesn't have sortKeys */
        if (origObj->header.properties & P_LRGOBJ )
            memcpy(obj->data, origObj->data, sizeof(ShortPageID));
        else 
            memcpy(obj->data, origObj->data, origObj->header.length);

        /* update slot info */
        slotIdx = buffer.header.nSlots++;
        buffer.slot[-slotIdx].offset = offset;
        buffer.slot[-slotIdx].unique = buffer.header.unique++;

        /* if allocated 'Unique' values are exausted, allocate them again */
        if (buffer.header.unique >= buffer.header.uniqueLimit) {
            e = RDsM_GetUnique(handle, &buffer.header.pid, &buffer.header.unique, &uniqueNum);
            if (e < 0) ERR(handle, e);
            buffer.header.uniqueLimit = buffer.header.unique + uniqueNum;
        }

        /* update free pointer */
        buffer.header.free += sizeof(ObjectHdr) + alignedLen;

#ifndef NDEBUG
        count++;
#endif
    }

#ifndef NDEBUG
    printf("\n # of sorted object = %ld!!\n", count);
#endif


    /* 
     *  Note!! buffer must be flush out because buffer always 
     *         contains last page of linked list in output pages
     */

    /* set link */
    buffer.header.nextPage = NIL;

    /* write buffer into disk */
    e = RDsM_WriteTrain(handle, (char *)&buffer, &buffer.header.pid, PAGESIZE2);
    if (e < 0) ERR(handle, e);


    /*
     *  Close sort stream
     */
    e = Util_CloseSortStream(handle, sortStreamId);
    if (e < 0) ERR(handle, e);


    /*
     *  Drop unused page
     */
    for (index = outPnoIdx+1; index < numOutPno; index++ ) {

        /* get ID of unused page */
        MAKE_PAGEID(pid, sortFid.volNo, PNO_ARRAY(outPnoArray)[index]);

        /* free unused page */
        e = RDsM_FreeTrain(handle, &pid, PAGESIZE2);
        if (e < 0) ERR(handle, e);
#ifndef NDEBUG
        printf("Drop unused page (%ld, %ld)!!\n", pid.volNo, pid.pageNo);
#endif
    }


    /*
     *  Insert old pages into deallocate list if regular file is sorted itself.
     *  Note!! although regular file is sorted itself, old pages in regular file 
     *         must be preserved until commit or abort for recovery
     */    

    if (!sortFileTmpFlag && !newFileFlag) {

        for (index = 0; index < numInPno; index++) {

            /* get ID of old page */
            MAKE_PAGEID(pid, sortFid.volNo, PNO_ARRAY(inPnoArray)[index]);

            e = Util_getElementFromPool(handle, dlPool, &dlElem);
            if (e < 0) ERR(handle, e);
    
            dlElem->type = DL_PAGE;
            dlElem->elem.pid = pid;          /* save the old page identifier */
            dlElem->next = dlHead->next;     /* insert to the list */
            dlHead->next = dlElem;           /* new first element of the list */
        }
    }


    /* 
     *  Finalize inPnoArray & outPnoArray 
     */ 

    e = Util_finalVarArray(handle, &inPnoArray);
    if (e < 0) ERR(handle, e);

    e = Util_finalVarArray(handle, &outPnoArray);
    if (e < 0) ERR(handle, e);


    /* 
     *  set catOverlayForSortFileInfo
     */
    catOverlayForSortFileInto->fid = *sortIntoFid;
    catOverlayForSortFileInto->eff = sortEff;
    catOverlayForSortFileInto->firstPage = firstOutPid.pageNo;
    catOverlayForSortFileInto->lastPage = buffer.header.pid.pageNo;
    catOverlayForSortFileInto->availSpaceList10 = NIL;
    catOverlayForSortFileInto->availSpaceList20 = NIL;
    catOverlayForSortFileInto->availSpaceList30 = NIL;
    catOverlayForSortFileInto->availSpaceList40 = NIL;
    catOverlayForSortFileInto->availSpaceList50 = NIL;


    return(eNOERROR);
}


/* ========================================
 *  om_InitOutFilePage()
 * =======================================*/

/* 
 * Function om_InitOutFilePage(FileID*, VarArray*, Four, Boolean, SlottedPage*)
 *
 * Description :
 *  Initialize given buffer for slotted page in output file
 * 
 * Return Values :
 *  Error Code.
 *
 * Side Effects :
 *  Given buffer is initialized
 */
Four om_InitOutFilePage(
    Four handle,
    FileID*       fileId,                /* IN  file ID in which given buffer is included */
    VarArray*     outPnoArray,           /* IN  array which contains page numbers of output file */
    Four          outPnoIdx,             /* IN  index of given buffer's page number */
    Boolean       tmpFileFlag,           /* IN  flag which indicates output file is temporary or not */
    SlottedPage*  buffer)                /* OUT buffer for page in output file */
{
    Four          e;                     /* error code */
    Four          uniqueNum;             /* # of allocated unique number */


    /* 
     *  Initialize header of buffer 
     */

    /* set 'pid' */
    MAKE_PAGEID(buffer->header.pid, fileId->volNo, PNO_ARRAY(*outPnoArray)[outPnoIdx]);

    /* set 'flags' */
    SET_PAGE_TYPE(buffer, SLOTTED_PAGE_TYPE);
    if (tmpFileFlag) SET_TEMP_PAGE_FLAG(buffer);
    else             RESET_TEMP_PAGE_FLAG(buffer);

    /* reset 'nSlots', 'free' & 'unused */
    buffer->header.nSlots = 0;
    buffer->header.free = 0;
    buffer->header.unused = 0;

    /* set 'fid' */
    buffer->header.fid = *fileId;

    /* set 'unique' & 'uniqueLimit' */
    e = RDsM_GetUnique(handle, &buffer->header.pid, &buffer->header.unique, &uniqueNum);
    if (e < 0) ERR(handle, e);
    buffer->header.uniqueLimit = buffer->header.unique + uniqueNum;

    /* Note!! 'nextPage' is set when flush out */
    buffer->header.prevPage = (outPnoIdx == 0) ? NIL : PNO_ARRAY(*outPnoArray)[outPnoIdx-1];

    /* set 'spaceListPrev' & 'spaceListNext' */
    buffer->header.spaceListPrev = NIL;
    buffer->header.spaceListNext = NIL;

 
    return eNOERROR;
}
