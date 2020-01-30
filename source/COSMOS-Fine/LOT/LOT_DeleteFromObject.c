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
 * Module: LOT_DeleteFromObject.c
 *
 * Description:
 *  Delete the given bytes from the large object tree.
 *
 * Exports:
 *  Four LOT_DeleteFromObject(Four, DataFileInfo*, PageID*, Four, Four, Four)
 */


#include <assert.h>
#include <stdlib.h> /* for malloc & free */
#include "common.h"
#include "error.h"
#include "trace.h"
#include "TM.h"
#include "BfM.h"
#include "LOT.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*@================================
 * LOT_DeleteFromObject( )
 *================================*/
/*
 * Function: Four LOT_DeleteFromObject(Four, DataFileInfo*, PageID*, Four, Four, Four)
 *
 * Description:
 *  Delete the given bytes from the large object tree.
 *
 * Returns:
 *  Error codes
 *    eBADCATOBJ
 *    eBADPAGEID
 *    eBADOFFSET_LOT
 *    eBADLENGTH_LOT
 *    eBADSLOTNO_LOT
 */
Four LOT_DeleteFromObject(
    Four handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    DataFileInfo *finfo,	/* IN file information */
    char *anodeOrRootPageNo,    /* INOUT anode or root page no */
    Boolean *rootWithHdr_Flag,  /* INOUT TRUE if root is with header */
    Four maxNodeLen,            /* IN node can grow at most to this value */
    Four start,			/* IN starting offset of delete */
    Four length,		/* IN amount of data to delete */
    LogParameter_T *logParam) /* IN log parameter */
{
    Four e;			/* Error Number */
    L_O_T_ItemList list;	/* infomation for root node */
    Boolean uf;			/* underflow flag */
    Boolean mf;			/* merge flag */
    PageID root;		/* root of large object tree */
    L_O_T_Path path;		/* cut path */
    Boolean f;			/* indicates the change of root PageID */
    Four c_which;
    L_O_T_INodeEntry tmpEntries[2]; /* temporary entries */


    TR_PRINT(handle, TR_LOT, TR1, ("LOT_DeleteFromObject()"));


    /*@ checking the parameters */
    if (finfo == NULL)	/* catObjForFile unexpected NULL */
	ERR(handle, eBADPARAMETER);

    if (start < 0)		/* bad starting offset of insert*/
	ERR(handle, eBADPARAMETER);

    if (length < 0)		/* bad length (< 0) of insert */
	ERR(handle, eBADPARAMETER);

    if (length == 0) return(eNOERROR);


    list.nEntries = 2;
    list.entry = tmpEntries;
    list.entry[0].spid = (*rootWithHdr_Flag == TRUE) ? NIL : (*(ShortPageID*)anodeOrRootPageNo);
    list.entry[1].spid = NIL;

    /*@ Initialize the path */
    lot_InitPath(handle, &path);

    e = lot_DeleteFromObject(handle, xactEntry, finfo, &list, (L_O_T_INode*)anodeOrRootPageNo, start,
			     start+length-1, &c_which, &uf, &mf, &path, logParam);
    if (e < 0) {
	lot_FinalPath(handle, &path);
	ERR(handle, e);
    }

    assert(list.entry[0].count != 0);

    e = lot_RebalanceTree(handle, xactEntry, finfo, &path, anodeOrRootPageNo,
                          rootWithHdr_Flag, maxNodeLen, &uf, logParam);
    if (e < 0) {
        lot_FinalPath(handle, &path);
        ERR(handle, e);
    }

    return(eNOERROR);

} /* LOT_DeleteFromObject( ) */
