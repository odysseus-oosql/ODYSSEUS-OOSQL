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
 * Module:	LOT_BulkLoad.c
 *
 * Description:
 *  APIs for direct access of large object such as bulk load.
 *
 * Exports:
 *  Four LOT_BlkLd_CreateLargeObject(ObjectID*, PageID*, PageID*)
 */

#include <stdlib.h> /* for malloc & free */
#include <string.h> /* for memcpy */
#include "common.h"
#include "trace.h"
#include "RDsM_Internal.h"	
#include "BfM.h"
#include "LOT_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


/* internal function prototype */
Four lot_BlkLd_MakeRoot(Four, ObjectID*, PageID*, Four, L_O_T_ItemList*);


/*@================================
 * LOT_BlkLd_CreateLargeObject()
 *================================*/
/*
 * Function: Four LOT_BlkLd_CreateLargeObject(ObjectID*, PageID*, PageID*)
 *
 * Description:
 *
 *
 * Returns:
 *  error code
 *    eBADOBJECTID_LOT
 *    some errors caused by function calls
 */
Four LOT_BlkLd_CreateLargeObject(
    Four handle,
    ObjectID             *catObjForFile,    /* IN  file containing the object */
    PageID               *nearPid,          /* IN  near page ID */
    PageID               *root)             /* OUT pointer to buffer holding the slotted page */
{
    Four                 e;                 /* error number */
    FileID               fid;               /* ID of file containing the LOT */
    Two                  eff;               /* data file's extent fill factor */
    Boolean              isTmp;             /* flag which indicates large object is temporary or not */
    Four                 firstExt;          /* first extent no of file */
    TrainID              leafTrainId;       /* the newly created leaf node */
    L_O_T_INode          *anode;            /* pointer to buffer holding the root node */
    L_O_T_LNode          *leafNode;         /* pointer to buffer holding the leaf node */
    SlottedPage          *catPage;          /* buffer page containing the catalog object */
    sm_CatOverlayForData *catEntry;         /* overay structure for catalog object access */

    
    TR_PRINT(TR_LOT, TR1,
             ("LOT_BlkLd_CreateLargeObject(handle, catObjForFile=%P, nearPid=%P, root=%P)",
	      catObjForFile, nearPid, root));
    

    /* 
     *  Get the file ID & extent fill factor from the catalog object. 
     */
    e = BfM_GetTrain(handle, (TrainID*)catObjForFile, (char**)&catPage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    GET_PTR_TO_CATENTRY_FOR_DATA(catObjForFile, catPage, catEntry);

    fid = catEntry->fid;
    eff = catEntry->eff;
    
    e = BfM_FreeTrain(handle, (TrainID*)catObjForFile, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    

    /* 
     *  check this large object is temporary or not 
     */
    e = lot_IsTemporary(handle, &fid, &isTmp);
    if (e < 0) ERR(handle, e);


    /* 
     *  get the first extent number 
     */
    e = RDsM_PageIdToExtNo(handle, (PageID *)&fid, &firstExt);
    if (e < 0) ERR(handle, e);

   
    /* 
     *  create the leaf node 
     */

    /*@ allocate the new train */
    e = RDsM_AllocTrains(handle, fid.volNo, firstExt, nearPid, eff, 1, TRAINSIZE2, &leafTrainId);
    if (e < 0) ERR(handle, e);

    /*@ Read the new page into the buffer. */
    e = BfM_GetNewTrain(handle, &leafTrainId, (char **)&leafNode, LOT_LEAF_BUF);
    if (e < 0) ERR(handle, e);

    /* initialize the leaf node */
    leafNode->hdr.pid = leafTrainId;
    
    /* set page type */
    SET_PAGE_TYPE(leafNode, LOT_L_NODE_TYPE);

    /* set temporary flag */
    if( isTmp ) SET_TEMP_PAGE_FLAG(leafNode);
    else        RESET_TEMP_PAGE_FLAG(leafNode);

    e = BfM_SetDirty(handle, &leafTrainId, LOT_LEAF_BUF);
    if (e < 0) ERRB1(handle, e, &leafTrainId, LOT_LEAF_BUF);
    
    e = BfM_FreeTrain(handle, &leafTrainId, LOT_LEAF_BUF);
    if (e < 0) ERR(handle, e);


    /* 
     *  create the root page 
     */

    /* allocate page for root */
    e = RDsM_AllocTrains(handle, fid.volNo, firstExt, nearPid, eff, 1, PAGESIZE2, root);
    if (e < 0) ERR(handle, e);

    /* initialize the root node */
    e = BfM_GetNewTrain(handle, root, (char **)&anode, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    /*@ set the node values */
    anode->header.pid = *root;
    anode->header.height = 1;
    anode->header.nEntries = 1;
    anode->entry[0].spid = leafTrainId.pageNo;
    anode->entry[0].count = 0;

    /* set page type */
    SET_PAGE_TYPE(anode, LOT_I_NODE_TYPE);

    /* set temporary flag */
    if( isTmp ) SET_TEMP_PAGE_FLAG(anode);
    else        RESET_TEMP_PAGE_FLAG(anode);

    /* check dirty flag */
    e = BfM_SetDirty(handle, root, PAGE_BUF);
    if (e < 0) ERRB1(handle, e, root, PAGE_BUF);

    /* free allocated page */
    e = BfM_FreeTrain(handle, root, PAGE_BUF);
    if (e < 0) ERR(handle, e);


    return(eNOERROR);
    
} /* LOT_BlkLd_CreateLargeObject() */



/*@================================
 * LOT_BlkLd_AppendToObject()
 *================================*/
/*
 * Function: Four LOT_BlkLd_AppendToObject(ObjectID*, PageID*, Four, char*)
 *
 * Description:
 *  Append data to the large object. 
 *  It is sufficient to handle the large object with LOT_InsertToObject().
 * But this append function is designed specially for efficient storage
 * utilization. User can make very large object with high store utilization
 * by successive call of append function.
 *
 * Returns:
 *  error code
 *    eBADCATALOGOBJECT_LOT
 *    eBADPAGEID_LOT
 *    eBADLENGTH_LOT
 *    eBADPARAMETER_LOT
 *    some errors caused by function calls
 */
Four LOT_BlkLd_AppendToObject(
    Four handle,
    ObjectID*      catObjForFile,  /* IN    file containing the L_O_T */
    PageID*        root,           /* INOUT page ID of root node */
    Four           length,         /* IN    amount of data to append */
    char*          data)           /* IN    user buffer holding the data */
{
    Four           e;              /* error number */
    Four           height;         /* height of root node */
    Boolean        f;              /* indicates the change of root */
    L_O_T_INode*   anode;          /* subtree's root node */
    L_O_T_ItemList childItems;     /* storage to hold slots overflowed from child node */

    
    TR_PRINT(TR_LOT, TR1,
             ("LOT_BlkLd_AppendToObject(handle, catObjForFile=%P, root=%P, length=%ld, data=%P)",
	      catObjForFile, root, length, data));


    /*@ 
     *  checking the parameters 
     */
    if (catObjForFile == NULL)	/* catObjForFile unexpected NULL */
	ERR(handle, eBADCATALOGOBJECT_LOT);

    if (root == NULL)		/* root unexpected NULL */
	ERR(handle, eBADPAGEID_LOT);
    
    if (length < 0)		/* bad length (< 0) of insert */
	ERR(handle, eBADLENGTH_LOT);

    /* just return */
    if (length == 0) return(eNOERROR);
    
    if (data == NULL)		/* data buffer unexpected NULL */
	ERR(handle, eBADPARAMETER_LOT);


    /* 
     *  append the data to the large object tree 
     */
    e = lot_AppendToObject(handle, catObjForFile, root, NIL, length, data, &childItems, &f);
    if(e < 0) ERR(handle, e);


    /* 
     *  overflow handling 
     */
    if (f) {

        /* Read the node from the disk into the buffer for get 'height' */
        e = BfM_GetTrain(handle, root, (char **)&anode, PAGE_BUF);
        if(e < 0) ERR(handle, e);
	
        height = anode->header.height;
	
        e = BfM_FreeTrain(handle, root, PAGE_BUF);
        if(e < 0) ERR(handle, e);

        /* make new root */
	e = lot_BlkLd_MakeRoot(handle, catObjForFile, root, height+1, &childItems);
	if (e < 0) ERR(handle, e);
	
    }
    

    return(eNOERROR);

} /* LOT_BlkLd_AppendToObject() */



/*@================================
 * LOT_BlkLd_WriteObject()
 *================================*/
/*
 * Function: Four LOT_BlkLd_WriteObject(PageID*, Four, Four, char*)
 *
 * Description :
 *  Write the large object data into disk from the user supplied buffer.
 *
 * Returns:
 *  error code
 *    eBADPAGEID
 *    eBADOFFSET
 *    eBADLENGTH
 *    eBADPARAMETER
 *    some errors caused by function calls
 *
 * Note :
 *  The parameters are not checked. The caller should pass the correct
 * parameters. For example, root should not be NIL, start & length must be
 * less than the object size, and buf may not be NULL.
 */
Four LOT_BlkLd_WriteObject(
    Four handle,
    PageID*       root,           /* IN page containing the object */
    Four          start,          /* IN starting offset of read */
    Four          length,         /* IN amount of data to read */
    char*         data)           /* IN user buffer holding the data */
{
    Four          e;              /* error code */

    TR_PRINT(TR_LOT, TR1,
            ("LOT_BlkLd_WriteObject(handle, root=%P, start=%ld, length=%ld, data=%P)\n",
	     root, start, length, data));


    /*@ 
     *  checking the parameters 
     */
    if (root == NULL)		/* pid unexpected NULL */
	ERR(handle, eBADPAGEID_LOT);

    if (start < 0)		/* bad starting offset of insert */
	ERR(handle, eBADOFFSET_LOT);

    if (length < 0)		/* bad length (< 0) of insert */
	ERR(handle, eBADLENGTH_LOT);

    /* just return */
    if (length == 0) return(eNOERROR);
    
    if (data == NULL)		/* data buffer unexpected NULL */
	ERR(handle, eBADPARAMETER_LOT);

    
    /* 
     *  write the data to the large object tree 
     */
    e = lot_WriteObject(handle, root, NIL, start, length, data);
    if (e < 0) ERR(handle, e);
    
    
    return(eNOERROR);

} /* LOT_BlkLd_WriteObject() */



/*@================================
 * lot_BlkLd_MakeRoot()
 *================================*/
/*
 * Function: Four lot_BlkLd_MakeRoot(ObjectID*, PageID*, Four, L_O_T_ItemList*)
 *
 * Description:
 *  make the root from the items
 *
 * Returns:
 *  error code
 *    eMEMALLOCERR_LOT
 *    some errors caused by function calls
 */
Four lot_BlkLd_MakeRoot(
    Four handle,
    ObjectID                    *catObjForFile, /* IN    information for the file */
    PageID                      *root,          /* INOUT page ID of root */
    Four                        height,         /* IN    height of the original tree */
    L_O_T_ItemList              *itemList)      /* IN    entries from which internal tree is constructed */
{
    Four                        e;              /* error number */
    Four                        i;
    Two                         j;           	/* loop variable */
    FileID                      fid;            /* ID of file where the object tree is placed */
    Two                         eff;            /* data file's extent fill factor */
    Boolean                     isTmp;          /* flag which indicates large object is temporary or not */
    Four                        firstExt;       /* first Extent No of the file */
    Four                        nEntries;       /* # of entries fot the current node to hold */
    Four                        nNodes;         /* # of nodes needed to hold the items */
    Four                        entriesPerNode; /* # of entries for one page to hold */
    Four                        remains;        /* small fraction after balancing */
    PageID                      newRoot;        /* the newly created root page's PageID */
    PageID                      *newPids;       /* array of PageIDs to be newly allocated */
    L_O_T_INode                 *nodePtr;       /* pointer to the node */
    L_O_T_INodeEntry            *entryPtr;      /* start of next copy */
    L_O_T_ItemList              localList;      /* list construected at this stage */
    SlottedPage                 *catPage;       /* buffer page containing the catalog object */
    sm_CatOverlayForData        *catEntry; 	/* overay structure for catalog object access */
   
    
    TR_PRINT(TR_LOT, TR1,
             ("lot_BlkLd_MakeRoot(handle, catObjForFile=%P, root=%P, height=%ld, itemList=%P)",
	      catObjForFile, root, height, itemList));


    /*
     *  Get the file ID & extent fill factor from the catalog object. 
     */
    e = BfM_GetTrain(handle, (TrainID*)catObjForFile, (char**)&catPage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    GET_PTR_TO_CATENTRY_FOR_DATA(catObjForFile, catPage, catEntry);

    fid = catEntry->fid;
    eff = catEntry->eff;
    
    e = BfM_FreeTrain(handle, (TrainID*)catObjForFile, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    

    /*
     *  Check large object is temporary or not
     */
    e = lot_IsTemporary(handle, &fid, &isTmp);
    if (e < 0) ERR(handle, e);


    /* 
     *  get the first extent number 
     */
    e = RDsM_PageIdToExtNo(handle, (PageID *)&fid, &firstExt);
    if (e < 0) ERR(handle, e);


    /*
     *  Allocate new root node and insert overflowed entries
     */

    /* I. the slots can be put in one node */
    if (itemList->nEntries <= LOT_MAXENTRIES) {
	
        /* Allocate one page for the root node */	
        e = RDsM_AllocTrains(handle, fid.volNo, firstExt, root, eff, 1, PAGESIZE2, &newRoot);
        if (e < 0) ERR(handle, e);

        /* initialize allocated page */
        e = lot_InitInternal(handle, &newRoot, 1, height, isTmp);
        if (e < 0) ERR(handle, e);

        /* get allocated page into buffer */
        e = BfM_GetNewTrain(handle, &newRoot, (char **)&nodePtr, PAGE_BUF);
        if (e < 0) ERR(handle, e);

        /* insert entries in itemList into new allocated root node */
        nodePtr->header.nEntries = itemList->nEntries;
        memcpy((char*)nodePtr->entry, (char*)itemList->entry, itemList->nEntries*sizeof(L_O_T_INodeEntry));

        /*@ adjust the count fields */
        for (j = 1; j < nodePtr->header.nEntries; j++) {
            nodePtr->entry[j].count += nodePtr->entry[j-1].count;
        }
 
        /* free allocated page */
        e = BfM_SetDirty(handle, &newRoot, PAGE_BUF);
        if (e < 0) ERRB1(handle, e, &newRoot, PAGE_BUF); 
	    
        e = BfM_FreeTrain(handle, &newRoot, PAGE_BUF); 
        if (e < 0) ERR(handle, e);

        /* set return parameter */
        *root = newRoot;
	
    }
    /* II. The slots cannot be put in one node */
    /*     We recursively call the lot_BlkLd_MakeRoot() */
    else {
	
	/* calculate 'nNodes' */
	nNodes = itemList->nEntries/LOT_MAXENTRIES + ((itemList->nEntries % LOT_MAXENTRIES) > 0 ? 1:0);

	/* calculate 'entriesPerNode' */
	entriesPerNode = itemList->nEntries / nNodes;

	/* The remain slots 'remains' are distributed evenly from the first node */
	remains = itemList->nEntries % nNodes;

        /* initialize 'localList' */
	localList.nEntries = nNodes;
	localList.entryArrayPtr = NULL; /* In this case, we do not use VarArray. */ 
	localList.entry = (L_O_T_INodeEntry *)malloc(sizeof(L_O_T_INodeEntry)*localList.nEntries);
	if (localList.entry == NULL) ERR(handle, eMEMALLOCERR_LOT);
	
        /* allocate memory for newPids */
	newPids = (PageID *)malloc(sizeof(PageID)*nNodes);
	if (newPids == NULL) ERR(handle, eMEMALLOCERR_LOT);
	
	/* allocate the needed nodes */
	e = RDsM_AllocTrains(handle, fid.volNo, firstExt, root, eff, nNodes, PAGESIZE2, newPids);
	if (e < 0) ERR(handle, e);

	e = lot_InitInternal(handle, newPids, nNodes, height, isTmp);
	if (e < 0) ERR(handle, e);

	/* Evenly distribute the items in itemList */
	entryPtr = itemList->entry;
	for (i = 0; i < nNodes; i++) {

            /* get allocated page into buffer */
	    e = BfM_GetNewTrain(handle, &newPids[i], (char **)&nodePtr, PAGE_BUF);
	    if (e < 0) ERR(handle, e);
	    
	    /* calculate 'nEntries' */
	    nEntries = entriesPerNode + ((remains > i) ? 1:0);
	    
	    /* distribute entries */
	    nodePtr->header.nEntries = nEntries;
	    memcpy((char*)nodePtr->entry, (char*)entryPtr, nEntries*sizeof(L_O_T_INodeEntry));
	    entryPtr += nEntries;
	    
	    /* adjust the count fields */
            for(j = 1; j < nEntries; j++) {
		nodePtr->entry[j].count += nodePtr->entry[j-1].count;
            }

	    /* Construct the L_O_T_ItemList */
	    localList.entry[i].spid = newPids[i].pageNo;
	    localList.entry[i].count = nodePtr->entry[nEntries-1].count;
	    
            /* check dirty flag */
	    e = BfM_SetDirty(handle, &newPids[i], PAGE_BUF);
	    if (e < 0) ERRB1(handle, e, &newPids[i], PAGE_BUF);
	    
            /* free allocated page */
	    e = BfM_FreeTrain(handle, &newPids[i], PAGE_BUF);
	    if (e < 0) ERR(handle, e);
	}
	
	/* recursive call */
	/* Note!! 'root' is assigned new value in below recursive function */
	e = lot_BlkLd_MakeRoot(handle, catObjForFile, root, height+1, &localList);
	if (e < 0) ERR(handle, e);

        /* free allocated memory */
	free(localList.entry);
	free(newPids);
    }


    return (eNOERROR);

} /* lot_BlkLd_MakeRoot() */
