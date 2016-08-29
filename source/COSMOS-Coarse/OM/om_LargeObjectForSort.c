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

#include "common.h"
#include "BfM.h"
#include "OM_Internal.h"
#include "LOT_Internal.h"
#include "RDsM_Internal.h"	
#include "perThreadDS.h"
#include "perProcessDS.h"


Four om_ReadLargeObjectInternal(Four, L_O_T_INode*, Four, Four, char*);
Four om_ReadLargeObjectLeaf(Four, L_O_T_LNode*, Four, Four, char*);
Four om_CopyLargeObjectInternal(Four, L_O_T_INode*, L_O_T_INode*, Four, Two);
Four om_CopyLargeObjectLeaf(Four, L_O_T_LNode*, L_O_T_LNode*, Two);

/* =================================
 *  OM_ReadLargeObject()
 * ================================*/

/*
 * Function OM_ReadLargeObject(Object*, Four, Four, char*)
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
    Four handle,	    			/* IN handle for multi threading */
    Object*      obj,               /* IN pointer which points large object */
    Four         start,             /* IN start offset of reading */
    Four         length,            /* IN amount of data to read */
    char*        buf)               /* OUT user buffer holding the data */
{
    Four         e;                 /* error code */
    PageID       pid;               /* pid of large object's root page */ 
    L_O_T_INode* rootNode;          /* root node of large object */


    /* parameter check */
    if ( !(obj->header.properties & P_LRGOBJ) ) ERR(handle, eBADPARAMETER_OM);


    /* get root node of large object */
    if (obj->header.properties & P_LRGOBJ_ROOTWITHHDR) 
    	rootNode = (L_O_T_INode * ) obj->data;
    else {

	/* volume number를 어떻게 처리할 것인가???? */
	/* parameter로 처리하기 힘든 이유는 이 함수를 getAttrs함수에서 사용하기 때문이다.*/
	/* How manages the volume number ??? */
	/* It's hard to manage using parameter because getAttrs() function uses this function. */ 

    	MAKE_PAGEID(pid, OM_PER_THREAD_DS(handle).curVolNo, *((PageNo *)obj->data)); 
        e = BfM_GetTrain(handle, &pid, (char**)&rootNode, PAGE_BUF);
	if (e < 0) ERR(handle, e);
    }

    /* at this point, 'rootNode' points root node of large object */

    /* read data!! */
    e = om_ReadLargeObjectInternal(handle, rootNode, start, length, buf);
    if (e < 0) ERR(handle, e);

    if ( !(obj->header.properties & P_LRGOBJ_ROOTWITHHDR) ) {
    	e = BfM_FreeTrain(handle, &pid, PAGE_BUF);
    	if (e < 0) ERR(handle, e);
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
    Four handle,
    L_O_T_INode *node,             /* IN root node of large object */
    Four start,                    /* IN start offset of reading */
    Four length,                   /* IN amount of data to read */
    char *buf)                     /* IN user supplied buffer which will contains read data */
{
    Four e;                        /* error code */
    Four i;                        /* index of large object's child node */
    Four newLength;                /* length which will be used in child node */
    Four newStart;                 /* start which will be used in child node */
    PageID childPid;               /* pid of large object's child node */
    L_O_T_INode *childINode;       /* child node of large object if child is internal node */
    L_O_T_LNode *childLNode;       /* child node of large object if child is leaf node */


    /* get index of child node */
    e = i = lot_SearchInNode(handle, node, start);
    if (e < 0) ERR(handle, e);

    /* calculate 'newLength' and 'newStart' value */
    newLength = MIN(node->entry[i].count - start, length);
    newStart = start - ((i==0) ? 0 : node->entry[i-1].count);

    while( length>0 ) {

	/* volume number를 어떻게 처리할 것인가???? */
	/* parameter로 처리하기 힘든 이유는 이 함수를 getAttrs함수에서 사용하기 때문이다.*/
	/* How manages the volume number ??? */
	/* It's hard to manage using parameter because getAttrs() function uses this function. */ 

    	MAKE_PAGEID(childPid, OM_PER_THREAD_DS(handle).curVolNo, node->entry[i].spid); 

    	/* if child node is leaf node */
    	if (node->header.height == 1) {

            e = BfM_GetTrain(handle, &childPid, (char**)&childLNode, LOT_LEAF_BUF);
            if (e < 0) ERR(handle, e);

            e = om_ReadLargeObjectLeaf(handle, childLNode, newStart, newLength, buf);
            if (e < 0) ERR(handle, e);

            e = BfM_FreeTrain(handle, &childPid, LOT_LEAF_BUF);
            if (e < 0) ERR(handle, e);
        }
        /* if child node is internal node */
        else {

            e = BfM_GetTrain(handle, &childPid, (char**)&childINode, PAGE_BUF);
            if (e < 0) ERR(handle, e);

            e = om_ReadLargeObjectInternal(handle, childINode, newStart, newLength, buf);
            if (e < 0) ERR(handle, e);

            e = BfM_FreeTrain(handle, &childPid, PAGE_BUF);
            if (e < 0) ERR(handle, e);
        }

        /* udate 'i' 'buf' 'length' */
        i++;
        buf += newLength;
        length -= newLength;

        /* update 'newLength' and 'newStart' value */
        if (length > 0) {
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
    Four handle,
    L_O_T_LNode *leaf,             /* IN leaf node of large object */
    Four start,                    /* IN start offset of reading */
    Four length,                   /* IN amount of data to read */
    char *buf)                     /* OUT user supplied buffer which will contains read data */
{
    memcpy(buf, (char *)&leaf->data[start], length);

    return(eNOERROR);
}


/* ==================================
 *  om_ConvertToLargeObject()
 * =================================*/

/* 
 * Function om_ConvertToLargeObject(Object*, Four, Two)
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
    Four handle,
    Object*      obj,                   /* INOUT object which will be converted to normal large object */
    Four         firstExtNo,            /* IN    first extent number in which obj is stored */ 
    Two          sortEff)               /* IN   */
{
    Four         e;                     /* error code */
    PageID       rootPid;               /* pid of large object's root page */
    PageID       leafPid;               /* pid of large object's leaf train */
    L_O_T_INode* aNode;                 /* root node of large object which is in slotted page */
    L_O_T_INode* rootNode;              /* root node of large object */
    L_O_T_LNode* leafNode;              /* leaf node of large object */


    /* if 'obj' is already normal large object */
    if ((obj->header.properties & P_LRGOBJ) && !(obj->header.properties & P_LRGOBJ_ROOTWITHHDR)) return(eNOERROR);

    /* allocate new page which will be root page of large object */
    e = RDsM_AllocTrains(handle, OM_PER_THREAD_DS(handle).curVolNo, firstExtNo, NULL, sortEff, 1, PAGESIZE2, &rootPid); 
    if (e < 0) ERR(handle, e);

    /* get allocated page into buffer */
    e = BfM_GetNewTrain(handle, &rootPid, (char **)&rootNode, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    /* if 'obj' is small object */
    if ( !(obj->header.properties & P_LRGOBJ) ) {

        /* First, create new leaf node and copy data */

        /* allocate new page which will be leaf train of large object */
        e = RDsM_AllocTrains(handle, OM_PER_THREAD_DS(handle).curVolNo, firstExtNo, NULL, sortEff, 1, LOT_LEAF_SIZE2, &leafPid);
        if (e < 0) ERRB1(handle, e, &rootPid, PAGE_BUF);

        /* get allocated leaf train into buffer */
        e = BfM_GetNewTrain(handle, &leafPid, (char **)&leafNode, LOT_LEAF_BUF);
        if (e < 0) ERRB1(handle, e, &rootPid, PAGE_BUF);

        /* copy obj's data into leaf train of large object */
        memcpy(leafNode->data, obj->data, obj->header.length);

        /* set dirty */
        e = BfM_SetDirty(handle, &leafPid, LOT_LEAF_BUF);
        if (e < 0) ERRB2(handle, e, &rootPid, PAGE_BUF, &leafPid, LOT_LEAF_BUF);

        /* free train */
        e = BfM_FreeTrain(handle, &leafPid, LOT_LEAF_BUF);
        if (e < 0) ERRB1(handle, e, &rootPid, PAGE_BUF);


        /* Second, set root node */

        /* set the root node of large object */
        rootNode->header.height = 1;
        rootNode->header.nEntries = 1;
        rootNode->entry[0].spid = leafPid.pageNo;
        rootNode->entry[0].count = obj->header.length;
    }
    /* if 'obj' is large object whose root is in slotted page */
    else  {
		
        /* get pointer of root node which is in slotted page */
        aNode = (L_O_T_INode *) obj->data;
		
        /* set the new root node of large object */
        rootNode->header.height = aNode->header.height;
        rootNode->header.nEntries = aNode->header.nEntries;
        memcpy(rootNode->entry, aNode->entry, sizeof(L_O_T_INodeEntry)*aNode->header.nEntries);
    }

    /* insert page number of large object's root */
    /* Note!! space in 'obj' is always enough to contain 'ShortPageID' */
    memcpy(obj->data, &rootPid.pageNo, sizeof(ShortPageID));

    /* set obj's header properly */
    obj->header.properties = P_LRGOBJ;


    /* set dirty */
    e = BfM_SetDirty(handle, &rootPid, PAGE_BUF);
    if (e < 0) ERRB1(handle, e, &rootPid, PAGE_BUF);

    /* free train */
    e = BfM_FreeTrain(handle, &rootPid, PAGE_BUF);
    if (e < 0) ERR(handle, e);


    return(eNOERROR);
}


/* ===================================
 *  om_RestoreLargeObject()
 * ==================================*/

/*
 * Function om_RestoreLargeObject(Object*, Object*, Four, Four, Two, Boolean)
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
    Four handle,
    Object* destObj,            /* OUT object which doesn't contain sort key */
    Object* srcObj,             /* IN object which contains sort key */
    Four    startOffset,        /* IN start offset of pid in object 'srcObj' */
    Four    firstExtNo,         /* IN first extent number */
    Two     sortEff,            /* IN */
    Boolean flag)               /* IN flag which indicates whether large object tree must be copied or not */
{
    Four e;                     /* error code */
    PageID srcPid;              /* pid of srcObj's root node */
    PageNo srcPno;              /* page number of srcObj's root node */
    PageID destPid;             /* pid of destObj's root node */
    L_O_T_INode *srcNode;       /* root node of 'srcObj' */
    L_O_T_INode *destNode;      /* root node of 'destObj' */


    /* if large object tree need not be copied */
    if ( !flag ) {

        /* copy pid of root node  */
        memcpy(destObj->data, &srcObj->data[startOffset], sizeof(ShortPageID));
    }
    /* if large object tree must be copied */
    else {

        /*
         *  initialize srcNode, destNode
         */

        /* make pid of srcObj's root node */
        memcpy(&srcPno, &srcObj->data[startOffset], sizeof(ShortPageID));
        MAKE_PAGEID(srcPid, OM_PER_THREAD_DS(handle).curVolNo, srcPno); 

        /* read root node of 'srcObj' into buffer */
        e = BfM_GetTrain(handle, &srcPid, (char**)&srcNode, PAGE_BUF);
        if (e < 0) ERR(handle, e);

        /* allocate memory for 'destNode' */
        destNode = (L_O_T_INode *) malloc(sizeof(L_O_T_INode));


        /*
         *  Allocate page for destNode & copy page ID into destObj
         */

        /* allocate new page which will be root page of large object */
        e = RDsM_AllocTrains(handle, OM_PER_THREAD_DS(handle).curVolNo, firstExtNo, NULL, sortEff, 1, PAGESIZE2, &destPid); 
        if (e < 0) ERR(handle, e);

        /* copy new root node of large object */
        memcpy(destObj->data, &destPid.pageNo, sizeof(ShortPageID));


        /* 
         *  copy large object 
         */
        e = om_CopyLargeObjectInternal(handle, destNode, srcNode, firstExtNo, sortEff);
        if (e < 0) ERR(handle, e);


        /*
         *  Finalize srtNode, destNode
         */

        /* unfix buffer */
        e = BfM_FreeTrain(handle, &srcPid, PAGE_BUF);
        if (e < 0) ERR(handle, e);

        /* Write destNode into disk */
        e = RDsM_WriteTrain(handle, (char *)destNode, &destPid, PAGESIZE2);
        if (e < 0) ERR(handle, e);

        /* free alocated memory */
        free(destNode);
    }


    return(eNOERROR);
}


/* ==================================
 *  om_CopyLargeObjectInternal()
 * =================================*/

/* 
 * Function om_CopyLargeObjectInternal(L_O_T_INode*, L_O_T_INode*, Four, Two)
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
    Four handle,
    L_O_T_INode* dest,         /* OUT */
    L_O_T_INode* src,          /* IN */
    Four         firstExtNo,   /* IN first extent number */
    Two          sortEff)
{
    Four e;                    /* error code */
    Two  i;                    /* index variable */
    PageID srcPid;             /* pid of src's child node */
    PageID destPid;            /* pid of dest's child node */
    L_O_T_INode *srcINode;     /* child node of 'src' if node is internal node */
    L_O_T_INode *destINode;    /* child node of 'dest' if node is internal node */
    L_O_T_LNode *srcLNode;     /* child node of 'src' if node is leaf node */
    L_O_T_LNode *destLNode;    /* child node of 'dest' if node is leaf node */
	

    /* copy header */
    memcpy(&dest->header, &src->header, sizeof(L_O_T_INodeHdr));

    /* if child node is leaf node */
    if (src->header.height == 1) {

    	/* allocate memory for destNode */
    	destLNode = (L_O_T_LNode *) malloc(sizeof(L_O_T_LNode));

    	for (i = 0; i < src->header.nEntries; i++ ) {

            /*
             *  Get leaf node of 'src'
             */

            /* make pid of src's child node */
            MAKE_PAGEID(srcPid, OM_PER_THREAD_DS(handle).curVolNo, src->entry[i].spid); 

            /* read child node of 'src' into 'srcLNode' */
            e = BfM_GetTrain(handle, &srcPid, (char**)&srcLNode, LOT_LEAF_BUF);
            if (e < 0) ERR(handle, e);


            /*
             *  Allocate train for leaf node of 'dst'
             */

            /* allocate new page which will be child node of 'dest' */
            e = RDsM_AllocTrains(handle, OM_PER_THREAD_DS(handle).curVolNo, firstExtNo, NULL, sortEff, 1, LOT_LEAF_SIZE2, &destPid); 
            if (e < 0) ERR(handle, e);


            /*
             *  Copy leaf node & entry
             */

            /* copy child subtree by recursive call */
            e = om_CopyLargeObjectLeaf(handle, destLNode, srcLNode, 
                                       (i == 0) ? src->entry[i].count : src->entry[i].count - src->entry[i-1].count);
            if (e < 0) ERR(handle, e);

            /* write child node into disk */
            e = RDsM_WriteTrain(handle, (char *)destLNode, &destPid, LOT_LEAF_SIZE2);
            if (e < 0) ERR(handle, e);

            /* copy entry */
            dest->entry[i].spid = destPid.pageNo;
            dest->entry[i].count = src->entry[i].count;


            /* 
             *  unfix leaf node of 'src' 
             */
            e = BfM_FreeTrain(handle, &srcPid, LOT_LEAF_BUF);
            if (e < 0) ERR(handle, e);
        }

        /* free allocated memory */
        free(destLNode);
    }
    /* if child node is internal node */
    else {

        /* allocate memory for destNode */
        destINode = (L_O_T_INode *) malloc(sizeof(L_O_T_INode));

        for (i = 0; i < src->header.nEntries; i++ ) {

            /*
             *  Get internal node of 'src'
             */

            /* make pid of src's child node */
            MAKE_PAGEID(srcPid, OM_PER_THREAD_DS(handle).curVolNo, src->entry[i].spid); 

            /* read child node of 'src' into 'srcINode' */
            e = BfM_GetTrain(handle, &srcPid, (char**)&srcINode, PAGE_BUF);
            if (e < 0) ERR(handle, e);


            /*
             *  Allocate page for internal node of 'dst'
             */

            /* allocate new page which will be child node of 'dest' */
            e = RDsM_AllocTrains(handle, OM_PER_THREAD_DS(handle).curVolNo, firstExtNo, NULL, sortEff, 1, PAGESIZE2, &destPid); 
            if (e < 0) ERR(handle, e);


            /*
             *  Copy internal node & entry
             */

            /* copy child subtree by recursive call */
            e = om_CopyLargeObjectInternal(handle, destINode, srcINode, firstExtNo, sortEff);
            if (e < 0) ERR(handle, e);

            /* write child node into disk */
            e = RDsM_WriteTrain(handle, (char *)destINode, &destPid, PAGESIZE2);
            if (e < 0) ERR(handle, e);

            /* copy entry */
            dest->entry[i].spid = destPid.pageNo;
            dest->entry[i].count = src->entry[i].count;


            /* 
             *  unfix internal node of 'src' 
             */
            e = BfM_FreeTrain(handle, &srcPid, PAGE_BUF);
            if (e < 0) ERR(handle, e);
        }

        /* free allocated memory */
        free(destINode);
    }

    return(eNOERROR);
}


/* ==================================
 *  om_CopyLargeObjectLeaf()
 * =================================*/

/* 
 * Function om_CopyLargeObjectLeaf(L_O_T_LNode*, L_O_T_LNode*, Two)
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
    Four handle,
    L_O_T_LNode *dest,         /* OUT */
    L_O_T_LNode *src,          /* IN */
    Two         size)          /* IN */
{
    memcpy(dest->data, src->data, size);

    return(eNOERROR);
}
