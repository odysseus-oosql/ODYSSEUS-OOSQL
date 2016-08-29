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
 * Module: RDsM_GetStatistics.c
 *
 * Description:
 *
 *
 * Exports:
 *  Four RDsM_GetStatistics_numExtents(Four, Two*, Four*, Four*)
 */

#include <stdlib.h> /* for malloc & free */
#include "common.h"
#include "error.h"
#include "trace.h"
#include "RDsM_Internal.h"
#include "BtM_Internal.h"
#include "BfM.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


Four rdsm_GetStatistics_numPagesInExtent(Four, VolumeTable*, Four, sm_NumPages*, Boolean, Boolean); 


/*@================================
 * RDsM_GetStatistics_numExtents()
 *================================*/
/*
 * Function: Four RDsM_GetStatistics_numExtents(Four, Two*, Four*, Four*)
 *
 * Description:
 *  Given the ID of a volume, get # of extents
 *
 * Returns:
 *  error code
 *    eVOLNOTMOUNTED_RDSM - volume not mounted
 *
 * Side Effects:
 *  None
 */
Four RDsM_GetStatistics_numExtents(
    Four handle,
    Four         volId,                    /* IN  volume id */
    Two*         extentSize,               /* OUT extent size */
    Four*        nTotalExtents,            /* OUT # of total extents */
    Four*        nUsedExtents)             /* OUT # of used extents */
{
    Two          i;                        /* loop index */
    Four         e;                        /* error code */
    VolInfoPage  volInfoPage;              /* volume information page */

    TR_PRINT(TR_RDSM, TR1, ("RDsM_GetStatistics_ExtentInfo()"));

    /* find the corresponding volume table entry */
    for (i = 0; i < MAXNUMOFVOLS; i++) {
        if (RDSM_PER_THREAD_DS(handle).volTable[i].volNo == volId) break; 
    }
    if (i >= MAXNUMOFVOLS) ERR(handle, eVOLNOTMOUNTED_RDSM);


    /* modify the way of accessing volTable for multi threading */
    if (RDSM_PER_THREAD_DS(handle).volTable[i].lockMode == L_NL || RDSM_PER_THREAD_DS(handle).volTable[i].numOfFreeExts == NOT_ASSIGNED || 
	RDSM_PER_THREAD_DS(handle).volTable[i].firstFreeExt == NOT_ASSIGNED) {

        /* read volume information page */
        e = rdsm_ReadTrain(handle, DEVINFO_ARRAY(RDSM_PER_THREAD_DS(handle).volTable[i].devInfo)[0].devAddr,
                           RDSM_PER_THREAD_DS(handle).volTable[i].volInfoPageId.pageNo, &volInfoPage, PAGESIZE2);
        if (e < eNOERROR) ERR(handle, e);

        /* set numOfFreeExts & firstFreeExt */
        RDSM_PER_THREAD_DS(handle).volTable[i].numOfFreeExts = volInfoPage.numOfFreeExts;
        RDSM_PER_THREAD_DS(handle).volTable[i].firstFreeExt = volInfoPage.firstFreeExt;
    }

    /* get statistics value about extent */
    *extentSize = RDSM_PER_THREAD_DS(handle).volTable[i].sizeOfExt;
    *nTotalExtents = RDSM_PER_THREAD_DS(handle).volTable[i].numOfExts;
    *nUsedExtents = RDSM_PER_THREAD_DS(handle).volTable[i].numOfExts - RDSM_PER_THREAD_DS(handle).volTable[i].numOfFreeExts;

    return(eNOERROR);

} /* RDsM_GetStatistics_numExtents() */



/*@================================
 * RDsM_GetStatistics_numPages()
 *================================*/
/*
 * Function: Four RDsM_GetStatistics_numPages(Four, sm_NumPages*, Boolean, Boolean)
 *
 * Description:
 *  Given the ID of a train, read it from a disk into a main memory buffer.
 *
 * Returns:
 *  error code
 *    eVOLNOTMOUNTED_RDSM - volume not mounted
 *    eMEMORYALLOCERR - no more free memory
 *
 * Side Effects:
 *  None
 */
Four RDsM_GetStatistics_numPages(
    Four handle,
    Four         volId,                 /* IN  volume id */
    sm_NumPages* numPages,              /* OUT # of pages */
    Boolean      getKindFlag,           /* IN  get kind of page if TRUE */ 
    Boolean      bitmapPrintFlag)       /* IN  print bitmap if TRUE */
{
    Four         e;                     /* error code */
    Four         i;                     /* loop index */
    Four         prevext, nextext;      /* previous & next extent number */
    Four         extNum;                /* extent number */
    char*        extentAllocMaps;       /* allocation map in given volume */
    VolumeTable* v;                     /* pointer to an entry of the volTable */
    VolInfoPage  volInfoPage;           /* volume information page */ 
    Two          idx;


    TR_PRINT(TR_RDSM, TR1, ("RDsM_GetStatistics_numPages(handle)"));


    /* get the corresponding volume table entry via searching the volTable */
    for (idx = 0; idx < MAXNUMOFVOLS; idx++) {
        if (RDSM_PER_THREAD_DS(handle).volTable[idx].volNo == volId) break; 
    }
    if (idx >= MAXNUMOFVOLS) ERR(handle, eVOLNOTMOUNTED_RDSM);

    /*@ set v to point to the corresponding entry */
    v = &(RDSM_PER_THREAD_DS(handle).volTable[idx]); 


    if (v->lockMode == L_NL || v->numOfFreeExts == NOT_ASSIGNED || v->firstFreeExt == NOT_ASSIGNED) {

        /* read volume information page */
        e = rdsm_ReadTrain(handle, DEVINFO_ARRAY(v->devInfo)[0].devAddr, v->volInfoPageId.pageNo, &volInfoPage, PAGESIZE2);
        if (e < eNOERROR) ERR(handle, e);

        /* set numOfFreeExts & firstFreeExt */
        v->numOfFreeExts = volInfoPage.numOfFreeExts;
        v->firstFreeExt = volInfoPage.firstFreeExt;
    }

    /* allocate memory */
    extentAllocMaps = (char*) malloc(v->numOfExts);
    if (extentAllocMaps == NULL) ERR(handle, eMEMORYALLOCERR);

    /* initialize 'extentAllocMaps' */
    for (i = 0; i < v->numOfExts; i++) extentAllocMaps[i] = 0;

    /* set value of 'extentAllocMaps' */
    extNum = v->firstFreeExt;
    while (extNum != NIL) {
        extentAllocMaps[extNum] = 1;
        e = RDsM_get_prev_next_ext(handle, v, extNum, &prevext, &nextext);
		if (e < eNOERROR) ERR(handle, e);
        extNum = nextext;
    }

    /* initialize 'numPages' */
    numPages->numTotalPages = 0; 
    numPages->numSlottedPage = 0;
    numPages->numLOT_I_Node = 0;
    numPages->numLOT_L_Node = 0;
    numPages->numBtree_I_Node = 0;
    numPages->numBtree_L_Node = 0;
    numPages->numBtree_O_Node = 0;
    numPages->numMLGF_I_Node = 0;
    numPages->numMLGF_L_Node = 0;
    numPages->numMLGF_O_Node = 0;
    numPages->numExtEntryPage = 0;
    numPages->numBitMapPage = 0;
    numPages->numMasterPage = 0;
    numPages->numVolInfoPage = 0;
    numPages->numMetaDicPage = 0;
    numPages->numUniqueNumPage = 0;

    /* get value of 'numPages' */
    for (i = 0; i < v->numOfExts; i++) {
        if (extentAllocMaps[i] == 0) {
            e = rdsm_GetStatistics_numPagesInExtent(handle, v, i, numPages, getKindFlag, bitmapPrintFlag); 
            if (e < eNOERROR) ERR(handle, e);
        }
    }

    /* free allocated memory */
    free(extentAllocMaps);


    return(eNOERROR);

} /* RDsM_GetStatistics_numPages() */



/*@=====================================
 * rdsm_GetStatistics_numPagesInExtent()
 *=====================================*/
/*
 * Function: Four rdsm_GetStatistics_numPagesInExtent(VolumeTable*, Four, sm_NumPages*, Boolean, Boolean)
 *
 * Description:
 *  get a bit map information (set or reset) for an extent
 *
 * Returns:
 *  error code
 */
Four rdsm_GetStatistics_numPagesInExtent(
    Four 			handle,
    VolumeTable 	*v,					/* IN  pointer to an entry of the shmPtr->volTable */
    Four	 		extNum,            	/* IN  extent number in question */
    sm_NumPages		*numPages,          /* OUT # of pages */
    Boolean      	getKindFlag,       	/* IN  get kind of page if TRUE */ 
    Boolean      	bitmapPrintFlag)   	/* IN  print the bitmap if TRUE */
{
    Four	 		e;                 	/* returned error code */
    Two          	i;                 	/* loop index */
    Four         	devNo;             	/* number of device in which given extent is located */
    Four         	pageOffset;        	/* offset of given extent (unit = # of page) */
    Four         	extOffset;         	/* offset of given extent (unit = # of extent) */
    Two          	unit;              	/* unit of movement */
    Four	 		start;             	/* start position to be checked */
    PageID	 		pageId;            	/* a page identifier */
    PageID	 		bitMapPageId;      	/* a page identifier */
    Page*        	aPage;             	/* pointer to a buffer page */
    BtreePage*   	btreePage;         	/* pointer to a btree page */
    PageType*    	bitMapPage;        	/* pointer to a buffer page */



    /*
     *  Get physical information about given extNum
     */
    e = rdsm_GetPhysicalInfo(handle, v, extNum*v->sizeOfExt, &devNo, &pageOffset);
    if (e < eNOERROR) ERR(handle, e);

    extOffset = pageOffset / v->sizeOfExt;


    /*
     *  find the PageMap page of the extent
     */
    bitMapPageId.volNo = DEVINFO_ARRAY(v->devInfo)[devNo].bitMapPageId.volNo;
    bitMapPageId.pageNo = DEVINFO_ARRAY(v->devInfo)[devNo].bitMapPageId.pageNo + extOffset/((BITMAP_USABLEBYTESPERPAGE*BITSPERBYTE)/v->sizeOfExt);

    /*@ get the bit map page */
    e = BfM_GetTrain(handle, &bitMapPageId, (char**)&bitMapPage, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);


    /*
     *  Get statistics
     */

    /* set the first bit position start and remaining number of bits to be checked */
    start = (extOffset % ((BITMAP_USABLEBYTESPERPAGE*BITSPERBYTE)/v->sizeOfExt)) * v->sizeOfExt;

    /* for each bit map pages in given extent, check corresponding page is allocated */
    for (i = 0; i < v->sizeOfExt; i += unit) {

        /* check bit is set or not */
        e = RDsM_find_bits(handle, bitMapPage, start+i, 1, 1);
        if (e != NIL && e < eNOERROR) ERR(handle, e); 

        /* if bit is set, i.e. corresponding page isn't allocated */
        if (e != NIL) {
            unit = PAGESIZE2;
        }
        /* if bit isn't set, i.e. corresponding page is allocated */
        else {

            /* update numTotalPages */
            numPages->numTotalPages++;

            /* skip page kind check */
            if ( !getKindFlag ) {
                unit = PAGESIZE2;
                continue;
            }

            /* get ID of corresponding page */
            pageId.volNo = v->volNo;
            pageId.pageNo = extNum*v->sizeOfExt + i;

            /* get the corresponding page */
            e = BfM_GetTrain(handle, &pageId, (char**)&aPage, PAGE_BUF);
            if (e < eNOERROR) ERR(handle, e); 

            switch(aPage->header.flags & PAGE_TYPE_VECTOR_MASK) {

                case SLOTTED_PAGE_TYPE : numPages->numSlottedPage++; break;
                case LOT_I_NODE_TYPE : numPages->numLOT_I_Node++; break;
                case LOT_L_NODE_TYPE : numPages->numLOT_L_Node++; break;
                case BTREE_PAGE_TYPE : btreePage = (BtreePage *) aPage;
                                       if(btreePage->any.hdr.type & INTERNAL || btreePage->any.hdr.type & ROOT)
                                           numPages->numBtree_I_Node++;
                                       else if(btreePage->any.hdr.type & LEAF)
                                           numPages->numBtree_L_Node++;
                                       else if(btreePage->any.hdr.type & OVERFLOW)
                                           numPages->numBtree_O_Node++;
                                       else
                                           return(eINVALIDPAGETYPE_RDSM);
                                       break;
                case MLGF_PAGE_TYPE :  /*
                                       numPages->numMLGF_I_Node++;
                                       numPages->numMLGF_L_Node++;
                                       numPages->numMLGF_O_Node++;
                                       break;
                                       */
                                       return(eINVALIDPAGETYPE_RDSM);
                case EXT_ENTRY_PAGE_TYPE : numPages->numExtEntryPage++; break;
                case BITMAP_PAGE_TYPE : numPages->numBitMapPage++; break;
                case MASTER_PAGE_TYPE : numPages->numMasterPage++; break;
                case VOL_INFO_PAGE_TYPE : numPages->numVolInfoPage++; break;
                case META_DIC_PAGE_TYPE : numPages->numMetaDicPage++; break;
                case UNIQUE_NUM_PAGE_TYPE : numPages->numUniqueNumPage++; break;

                default : ERR(handle, eINVALIDPAGETYPE_RDSM);
            }

            /* get 'unit' */
            if((aPage->header.flags & PAGE_TYPE_VECTOR_MASK) == LOT_L_NODE_TYPE)
                unit = LOT_LEAF_SIZE2;
            else
                unit = PAGESIZE2;

            /* free corresponding page */
            e = BfM_FreeTrain(handle, &pageId, PAGE_BUF);
            if (e < eNOERROR) ERR(handle, e); 
        }
    }

    /* if needed, print bit map */
    if (bitmapPrintFlag) {
        /* print bits in this page from start by the size of an extent */
        (void) RDsM_print_bits(handle, bitMapPage, start, v->sizeOfExt);
    }


    /*@
     *  Free this page
     */
    e = BfM_FreeTrain(handle, &bitMapPageId, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e); 


    return(eNOERROR);

} /* rdsm_GetStatistics_numPagesInExtent() */
