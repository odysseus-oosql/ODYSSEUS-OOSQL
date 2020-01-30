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
/*    Fine-Granule Locking Version                                            */
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
 * Module : om_LargeObjectForSort.c
 *
 * Description :
 *  manipulate large object in sort
 *
 * Exports :
 */


#include <stdio.h>  /* for printf */
#include <stdlib.h> /* for malloc & free */
#include <string.h> /* for memcpy */
#include "perProcessDS.h"
#include "perThreadDS.h"

#include "common.h"
#include "error.h"
#include "BfM.h"
#include "OM.h"
#include "LOT.h"
#include "RDsM.h"


Four om_ReadLargeObjectInternal(Four, L_O_T_INode*, Four, Four, char*);
Four om_ReadLargeObjectLeaf(Four, L_O_T_LNode*, Four, Four, char*);
Four om_CopyLargeObjectInternal(Four, XactTableEntry_T*, FileID*, SegmentID_T*, SegmentID_T*, L_O_T_INode*, L_O_T_INode*, LogParameter_T*); 
Four om_CopyLargeObjectLeaf(Four, L_O_T_LNode*, L_O_T_LNode*, Four);



/* =================================
 *  OM_ReadLargeObject( )
 * ================================*/

/*
 * Function OM_ReadLargeObject(handle, Object*, Four, Four, char*)
 *
 * Description :
 *  Read data from large object
 *
 * Return Values :
 *  Error Code
 *   eBADPARAMETER
 *
 * Side Effects :
 *  'buf' will contain data
 *
 */
Four OM_ReadLargeObject(
    Four            handle,
    Object          *obj,               /* IN pointer which points large object */
    Four            start,              /* IN start offset of reading */
    Four            length,             /* IN amount of data to read */
    char            *buf)               /* OUT user buffer holding the data */
{
    Four            e;                  /* error code */
    PageID          pid;                /* pid of large object's root page */
    L_O_T_INode     *rootNode;          /* root node of large object */
    L_O_T_INodePage *rootNodePage;      /* root node of large object */
    Buffer_ACC_CB   *rootNode_BCBP;     /* buffer access control block for root node of large object */

    /* pointer for OM Data Structure of perThreadTable */
    OM_PerThreadDS_T *om_perThreadDSptr = OM_PER_THREAD_DS_PTR(handle);

    /* parameter check */
    if( !(obj->header.properties & P_LRGOBJ) ) ERR(handle, eBADPARAMETER);


    /* get root node of large object */
    if( obj->header.properties & P_LRGOBJ_ROOTWITHHDR )
    	rootNode = (L_O_T_INode * ) obj->data;
    else {

	/* volume number를 어떻게 처리할 것인가???? */
	/* parameter로 처리하기 힘든 이유는 이 함수를 getAttrs함수에서 사용하기 때문이다.*/
	/* How manages the volume number ??? */
	/* It's hard to manage using parameter because getAttrs() function uses this function. */ 

    	MAKE_PAGEID(pid, om_perThreadDSptr->curVolNo, *((PageNo *)obj->data));

        e = BfM_getAndFixBuffer(handle, (TrainID*)&pid, M_FREE, &rootNode_BCBP, PAGE_BUF);
        if (e < eNOERROR) ERR(handle, e);

        rootNodePage = (L_O_T_INodePage *) rootNode_BCBP->bufPagePtr;
        rootNode = &rootNodePage->node;
    }

    /* at this point, 'rootNode' points root node of large object */

    /* read data!! */
    e = om_ReadLargeObjectInternal(handle, rootNode, start, length, buf);
    if (e < eNOERROR) ERR(handle, e);

    if( !(obj->header.properties & P_LRGOBJ_ROOTWITHHDR) ) {
    	e = BfM_unfixBuffer(handle, rootNode_BCBP, PAGE_BUF);
    	if(e < eNOERROR) ERR(handle, e);
    }

    return(eNOERROR);
}


/* =================================
 *  om_ReadLargeObjectInternal()
 * ================================*/

/*
 * Function om_ReadLargeObjectInternal(L_O_T_INode*, Four, Four, char*)
 *
 * Description :
 *
 *
 * Return Values :
 *  Error Code
 *
 * Side Effects :
 *
 */
Four om_ReadLargeObjectInternal(
    Four	    handle,
    L_O_T_INode     *node,              /* IN root node of large object */
    Four            start,              /* IN start offset of reading */
    Four            length,             /* IN amount of data to read */
    char            *buf)               /* IN user supplied buffer which will contains read data */
{
    Four            e;                  /* error code */
    Four            i;                  /* index of large object's child node */
    Four            newLength;          /* length which will be used in child node */
    Four            newStart;           /* start which will be used in child node */
    PageID          childPid;           /* pid of large object's child node */
    L_O_T_INodePage *childINodePage;    /* page which contains child node of large object if child is internal node */
    L_O_T_INode     *childINode;        /* child node of large object if child is internal node */
    L_O_T_LNode     *childLNode;        /* child node of large object if child is leaf node */
    Buffer_ACC_CB   *childNode_BCBP;

    /* pointer for OM Data Structure of perThreadTable */
    OM_PerThreadDS_T *om_perThreadDSptr = OM_PER_THREAD_DS_PTR(handle);

    /* get index of child node */
    e = i = lot_SearchInNode(handle, node, start);
    if (e < eNOERROR) ERR(handle, e);

    /* calculate 'newLength' and 'newStart' value */
    newLength = MIN(node->entry[i].count - start, length);
    newStart = start - ((i==0) ? 0 : node->entry[i-1].count);

    while( length>0 ) {

        /* volume number를 어떻게 처리할 것인가???? */
        /* parameter로 처리하기 힘든 이유는 이 함수를 getAttrs함수에서 사용하기 때문이다.*/
        /* How manages the volume number ??? */
        /* It's hard to manage using parameter because getAttrs() function uses this function. */ 

    	MAKE_PAGEID(childPid, om_perThreadDSptr->curVolNo, node->entry[i].spid);

    	/* if child node is leaf node */
    	if( node->header.height==1 ) {

            e = BfM_getAndFixBuffer(handle, (TrainID*)&childPid, M_FREE, &childNode_BCBP, TRAIN_BUF);
            if (e < eNOERROR) ERR(handle, e);

            childLNode = (L_O_T_LNode *) childNode_BCBP->bufPagePtr;

            e = om_ReadLargeObjectLeaf(handle, childLNode, newStart, newLength, buf);
            if (e < eNOERROR) ERR(handle, e);

            e = BfM_unfixBuffer(handle, childNode_BCBP, TRAIN_BUF);
            if(e < eNOERROR) ERR(handle, e);
        }
        /* if child node is internal node */
        else {

            e = BfM_getAndFixBuffer(handle, (TrainID*)&childPid, M_FREE, &childNode_BCBP, PAGE_BUF);
            if (e < eNOERROR) ERR(handle, e);

            childINodePage = (L_O_T_INodePage *) childNode_BCBP->bufPagePtr;
            childINode = &childINodePage->node;

            e = om_ReadLargeObjectInternal(handle, childINode, newStart, newLength, buf);
            if (e < eNOERROR) ERR(handle, e);

            e = BfM_unfixBuffer(handle, childNode_BCBP, PAGE_BUF);
            if(e < eNOERROR) ERR(handle, e);
        }

        /* udate 'i' 'buf' 'length' */
        i++;
        buf += newLength;
        length -= newLength;

        /* update 'newLength' and 'newStart' value */
        if( length>0 ) {
            newLength = MIN(node->entry[i].count - node->entry[i-1].count, length);
            newStart = 0;
        }
    }

    return(eNOERROR);
}


/* =================================
 *  om_ReadLargeObjectLeaf()
 * ================================*/

/*
 * Function om_ReadLargeObjectLeaf(L_O_T_LNode*, Four, Four, char*)
 *
 * Description :
 *  Read data from large object's leaf node
 *
 * Return Values :
 *  Error Code
 *
 * Side Effects :
 *
 */
Four om_ReadLargeObjectLeaf(
    Four	handle,
    L_O_T_LNode *leaf,                    /* IN leaf node of large object */
    Four        start,                    /* IN start offset of reading */
    Four        length,                   /* IN amount of data to read */
    char        *buf)                     /* IN user supplied buffer which will contains read data */
{
    memcpy(buf, (char *)&leaf->data[start], length);

    return(eNOERROR);
}


/* ==================================
 *  om_ConvertToLargeObject( )
 * =================================*/

/*
 * Function om_ConvertToLargeObject(handle, Object*, Four)
 *
 * Description :
 *  convert to normal large object
 *  Note!! space in 'obj' is always enough to contain 'ShortPageID'
 *
 * Return Values :
 *  Error Code
 *
 * Side Effects :
 *
 */
Four om_ConvertToLargeObject(
    Four 	     handle,
    XactTableEntry_T *xactEntry,             /* IN */
    DataFileInfo     *finfo,                 /* IN data file info which large object is contained */
    Object           *obj,                   /* INOUT object which will be converted to normal large object */
    LogParameter_T   *logParam)              /* IN */
{
    Four             e;                      /* error code */
    PageID           rootPid;                /* pid of large object's root page */
    PageID           leafPid;                /* pid of large object's leaf train */
    L_O_T_INode      *aNode;                 /* root node of large object which is in slotted page */
    L_O_T_INodePage  *rootNodePage;          /* root node of large object */
    L_O_T_LNode      *leafNode;              /* leaf node of large object */
    SegmentID_T      pageSegmentID;          /* page segment ID */
    SegmentID_T      trainSegmentID;         /* train segment ID */
    FileID	     *fid;                   /* file ID in which large object is contained */

    /* pointer for OM Data Structure of perThreadTable */
    OM_PerThreadDS_T *om_perThreadDSptr = OM_PER_THREAD_DS_PTR(handle);

    /* if 'obj' is already normal large object */
    if( (obj->header.properties & P_LRGOBJ) && !(obj->header.properties & P_LRGOBJ_ROOTWITHHDR) )
        return(eNOERROR);

    /* set 'fid' from Data File Info */
    fid = &finfo->fid;

	e = om_GetSegmentIDFromDataFileInfo(handle, xactEntry, finfo, &pageSegmentID, PAGESIZE2);
    if (e < eNOERROR) ERR(handle, e);

    e = RDsM_AllocTrains(handle, xactEntry, om_perThreadDSptr->curVolNo, &pageSegmentID, NULL, 1, PAGESIZE2, FALSE, &rootPid, logParam);
    if (e < eNOERROR) ERR(handle, e);
    
    /* allocate buffer which contains new page */
    rootNodePage = (L_O_T_INodePage *) malloc(sizeof(L_O_T_INodePage));

    /* initialize internal node page */
    LOT_INIT_INODE_PAGE_HDR(rootNodePage, *fid, rootPid); 

    /* if 'obj' is small object */
    if( !(obj->header.properties & P_LRGOBJ) ) {

        /* First, create new leaf node and copy data */

		e = om_GetSegmentIDFromDataFileInfo(handle, xactEntry, finfo, &trainSegmentID, TRAINSIZE2);
        if (e < eNOERROR) ERR(handle, e);

        e = RDsM_AllocTrains(handle, xactEntry, om_perThreadDSptr->curVolNo, &trainSegmentID, NULL, 1, TRAINSIZE2, FALSE, &leafPid, logParam);
        if (e < eNOERROR) ERR(handle, e);
        
        /* allocate buffer which contains new page */
        leafNode = (L_O_T_LNode *) malloc(sizeof(L_O_T_LNode));

        /* initialize leaf node page */
        LOT_INIT_LEAF_NODE(leafNode, *fid, leafPid);

        /* copy obj's data into leaf train of large object */
        memcpy(leafNode->data, obj->data, obj->header.length);

        /* write to disk */
        e = RDsM_WriteTrain(handle, (char *)leafNode, &leafPid, TRAINSIZE2);
        if (e < eNOERROR) ERR(handle, e);


        /* Second, set root node */

        /* set the root node of large object */
        rootNodePage->node.header.height = 1;
        rootNodePage->node.header.nEntries = 1;
        rootNodePage->node.entry[0].spid = leafPid.pageNo;
        rootNodePage->node.entry[0].count = obj->header.length;
    }
    /* if 'obj' is large object whose root is in slotted page */
    else  {

        /* get pointer of root node which is in slotted page */
        aNode = (L_O_T_INode *) obj->data;

        /* set the new root node of large object */
        rootNodePage->node.header.height = aNode->header.height;
        rootNodePage->node.header.nEntries = aNode->header.nEntries;
        memcpy(rootNodePage->node.entry, aNode->entry, sizeof(L_O_T_INodeEntry)*aNode->header.nEntries);
    }

    /* insert page number of large object's root */
    /* Note!! space in 'obj' is always enough to contain 'ShortPageID' */
    memcpy(obj->data, &rootPid.pageNo, sizeof(ShortPageID));

    /* set obj's header properly */
    obj->header.properties = P_LRGOBJ;

    /* write to disk */
    e = RDsM_WriteTrain(handle, (char *)rootNodePage, &rootPid, PAGESIZE2);
    if (e < eNOERROR) ERR(handle, e);


    return(eNOERROR);
}


/* ===================================
 *  om_RestoreLargeObject( )
 * ==================================*/

/*
 * Function om_RestoreLargeObject(handle, Object*, Object*, Four, Four)
 *
 * Description :
 *  Extract sort key from large object 'srcObj' and construct large object 'destObj'
 *  Note!! when page is read, it doesn't go through 'buffer'. Because that page is used only once!!
 *                                                                                      ^^^^^^^^^^^
 * Return Values :
 *  Error Code
 *
 * Side Effects :
 *  'destObj' will contains page number of root page or root page itself
 */
Four om_RestoreLargeObject(
    Four 	     handle,
    XactTableEntry_T *xactEntry,           /* IN */
    FileID           *destFid,             /* IN file ID in which destination large object is contained */
    SegmentID_T      *pageSegmentID,       /* IN page segment ID of data file */
    SegmentID_T      *trainSegmentID,      /* IN page segment ID of data file */
    Object           *destObj,             /* OUT object which doesn't contain sort key */
    Object           *srcObj,              /* IN object which contains sort key */
    Four             startOffset,          /* IN start offset of pid in object 'srcObj' */
    Four             flag,                 /* IN flag which indicates whether large object tree must be copied or not */
    LogParameter_T   *logParam)            /* IN */
{
    Four             e;                    /* error code */
    PageID           srcPid;               /* pid of srcObj's root node */
    PageNo           srcPno;               /* page number of srcObj's root node */
    PageID           destPid;              /* pid of destObj's root node */
    L_O_T_INodePage  *srcNodePage;         /* root node of 'srcObj' */
    L_O_T_INodePage  *destNodePage;        /* root node of 'destObj' */
    Buffer_ACC_CB    *srcNode_BCBP;

    /* pointer for OM Data Structure of perThreadTable */
    OM_PerThreadDS_T *om_perThreadDSptr = OM_PER_THREAD_DS_PTR(handle);

    /* if large object tree need not be copied */
    if( !flag ) {

        /* copy pid of root node  */
        memcpy(destObj->data, &srcObj->data[startOffset], sizeof(ShortPageID));
    }
    /* if large object tree must be copied */
    else {

        /* allocate memory for srcNode & destNode */
        destNodePage = (L_O_T_INodePage *) malloc(sizeof(L_O_T_INodePage));

        /* make pid of srcObj's root node */
        memcpy(&srcPno, &srcObj->data[startOffset], sizeof(ShortPageID));
        MAKE_PAGEID(srcPid, om_perThreadDSptr->curVolNo, srcPno);

        /* read root node of 'srcObj' into buffer */
        e = BfM_getAndFixBuffer(handle, (TrainID*)&srcPid, M_FREE, &srcNode_BCBP, PAGE_BUF);
        if (e < eNOERROR) ERR(handle, e);

        srcNodePage = (L_O_T_INodePage *) srcNode_BCBP->bufPagePtr;

        /* allocate new page which will be root page of large object */
        e = RDsM_AllocTrains(handle, xactEntry, om_perThreadDSptr->curVolNo, pageSegmentID, NULL, 1, PAGESIZE2, FALSE, &destPid, logParam);
        if (e < eNOERROR) ERR(handle, e);

        /* initialize internal node page */
        LOT_INIT_INODE_PAGE_HDR(destNodePage, *destFid, destPid); 

        /* copy new root node of large object */
        memcpy(destObj->data, &destPid.pageNo, sizeof(ShortPageID));

        /* copy large object */
        e = om_CopyLargeObjectInternal(handle, xactEntry, destFid, pageSegmentID, trainSegmentID, 
				       &destNodePage->node, &srcNodePage->node, logParam); 
        if (e < eNOERROR) ERR(handle, e);

        /* write to disk */
        e = RDsM_WriteTrain(handle, (char *)destNodePage, &destPid, PAGESIZE2);
        if (e < eNOERROR) ERR(handle, e);

        /* unfix buffer */
        e = BfM_unfixBuffer(handle, srcNode_BCBP, PAGE_BUF);
        if (e < eNOERROR) ERR(handle, e);

        /* free alocated memory */
        free(destNodePage);
    }


    return(eNOERROR);
}


/* ==================================
 *  om_CopyLargeObjectInternal()
 * =================================*/

/*
 * Function om_CopyLargeObjectInternal(L_O_T_INode*, L_O_T_INode*)
 *
 * Description :
 *  Copy large object to another
 *  Note!! when page is read, it doesn't go through 'buffer'. Because that page is used only once!!
 *                                                                                      ^^^^^^^^^^^
 *
 * Return Values :
 *  Error Code
 *
 * Side Effects :
 *  new page will be allocated and 'dest' will points that page
 */
Four om_CopyLargeObjectInternal(
    Four	     handle,
    XactTableEntry_T *xactEntry,      /* IN */
    FileID           *destFid,        /* IN file ID in which destination large object is contained */ 
    SegmentID_T      *pageSegmentID,  /* IN page segment ID of data file */
    SegmentID_T      *trainSegmentID, /* IN train segment ID of data file */
    L_O_T_INode      *dest,           /* OUT */
    L_O_T_INode      *src,            /* IN */
    LogParameter_T   *logParam)       /* IN */
{
    Four             e;               /* error code */
    Four             i;               /* index variable */
    PageID           srcPid;          /* pid of src's child node */
    PageID           destPid;         /* pid of dest's child node */
    L_O_T_INodePage  *srcINodePage;   /* child node of 'src' if node is internal node */
    L_O_T_INodePage  *destINodePage;  /* child node of 'dest' if node is internal node */
    L_O_T_LNode      *srcLNode;       /* child node of 'src' if node is leaf node */
    L_O_T_LNode      *destLNode;      /* child node of 'dest' if node is leaf node */
    Buffer_ACC_CB    *srcNode_BCBP;

    /* pointer for COMMON Data Structure of perThreadTable */
    OM_PerThreadDS_T *om_perThreadDSptr = OM_PER_THREAD_DS_PTR(handle);

    /* copy header */
    memcpy(&dest->header, &src->header, sizeof(L_O_T_INodeHdr));

    /* if child node is leaf node */
    if( src->header.height==1 ) {

        /* allocate memory for destNode */
        destLNode = (L_O_T_LNode *) malloc(sizeof(L_O_T_LNode));

    	for( i=0; i<src->header.nEntries; i++ ) {

    	    /* make pid of src's child node */
            MAKE_PAGEID(srcPid, om_perThreadDSptr->curVolNo, src->entry[i].spid);

            /* read child node of 'src' into 'srcLNode' */
            e = BfM_getAndFixBuffer(handle, (TrainID*)&srcPid, M_FREE, &srcNode_BCBP, TRAIN_BUF);
            if (e < eNOERROR) ERR(handle, e);

            srcLNode = (L_O_T_LNode *) srcNode_BCBP->bufPagePtr;

            /* allocate new page which will be child node of 'dest' */
            e = RDsM_AllocTrains(handle, xactEntry, om_perThreadDSptr->curVolNo, trainSegmentID, NULL, 1, TRAINSIZE2, FALSE, &destPid, logParam);
            if (e < eNOERROR) ERR(handle, e);

            /* initialize leaf node page */
            LOT_INIT_LEAF_NODE(destLNode, *destFid, destPid); 

            /* copy child subtree by recursive call */
            e = om_CopyLargeObjectLeaf(handle, destLNode, srcLNode,
                                       (i == 0) ? src->entry[i].count : src->entry[i].count - src->entry[i-1].count);
            if (e < eNOERROR) ERR(handle, e);

            /* write child node into disk */
            e = RDsM_WriteTrain(handle, (char *)destLNode, &destPid, TRAINSIZE2);
            if (e < eNOERROR) ERR(handle, e);

            /* copy entry */
            dest->entry[i].spid = destPid.pageNo;
            dest->entry[i].count = src->entry[i].count;

            /* unfix buffer */
            e = BfM_unfixBuffer(handle, srcNode_BCBP, TRAIN_BUF);
            if (e < eNOERROR) ERR(handle, e);
        }

        /* free allocated memory */
        free(destLNode);
    }
    /* if child node is internal node */
    else {

        /* allocate memory for destNode */
        destINodePage = (L_O_T_INodePage *) malloc(sizeof(L_O_T_INodePage));

        for( i=0; i<src->header.nEntries; i++ ) {

            /* make pid of src's child node */
            MAKE_PAGEID(srcPid, om_perThreadDSptr->curVolNo, src->entry[i].spid);

            /* read child node of 'src' into 'srcINode' */
            e = BfM_getAndFixBuffer(handle, (TrainID*)&srcPid, M_FREE, &srcNode_BCBP, PAGE_BUF);
            if (e < eNOERROR) ERR(handle, e);

            srcINodePage = (L_O_T_INodePage *) srcNode_BCBP->bufPagePtr;

            /* allocate new page which will be child node of 'dest' */
            e = RDsM_AllocTrains(handle, xactEntry, om_perThreadDSptr->curVolNo, pageSegmentID, NULL, 1, PAGESIZE2, FALSE, &destPid, logParam);
            if (e < eNOERROR) ERR(handle, e);

            /* initialize internal node page */
            LOT_INIT_INODE_PAGE_HDR(destINodePage, *destFid, destPid);

            /* copy child subtree by recursive call */
            e = om_CopyLargeObjectInternal(handle, xactEntry, destFid, pageSegmentID, trainSegmentID,
					   &destINodePage->node, &srcINodePage->node, logParam);
            if (e < eNOERROR) ERR(handle, e);

            /* write child node into disk */
            e = RDsM_WriteTrain(handle, (char *)destINodePage, &destPid, PAGESIZE2);
            if (e < eNOERROR) ERR(handle, e);

            /* copy entry */
            dest->entry[i].spid = destPid.pageNo;
            dest->entry[i].count = src->entry[i].count;

            /* unfix buffer */
            e = BfM_unfixBuffer(handle, srcNode_BCBP, PAGE_BUF);
            if (e < eNOERROR) ERR(handle, e);
        }

        /* free allocated memory */
        free(destINodePage);
    }

    return(eNOERROR);
}


/* ==================================
 *  om_CopyLargeObjectLeaf()
 * =================================*/

/*
 * Function om_CopyLargeObjectLeaf(L_O_T_LNode*, L_O_T_LNode*, Four)
 *
 * Description :
 *  Copy large object to another
 *
 * Return Values :
 *  Error Code
 *
 * Side Effects :
 *
 */
Four om_CopyLargeObjectLeaf(
    Four	   handle,
    L_O_T_LNode    *dest,                /* OUT */
    L_O_T_LNode    *src,                 /* IN */
    Four           size)                 /* IN */
{
    memcpy(dest->data, src->data, size);

    return(eNOERROR);
}
