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
/******************************************************************************//*
 * Module: OM_Dump.c
 *
 * Description:
 *  (1) What to do?
 *  This module contains the utility functions to dump the page and the record.
 *  OM_DumpSlottedPage( ) displays the contents of the slotted page and
 *  OM_DumpObject( ) displays the contents of the object.
 *
 * Exports:
 *  Four om_DumpSlottedPage(Four, SlottedPage *)
 *  Four OM_DumpSlottedPage(Four, PageID *)
 *  Four om_DumpObject(Four, Object*, ObjectID*)
 *  Four OM_DumpObject(Four, ObjectID *)
 *
 * Return Values:
 *  Error Code:
 *    some errors caused by function calls
 *
 * Side Effects :
 *  None
 */

#include <stdio.h>
#include <ctype.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "TM.h"
#include "BfM.h"
#include "LOT.h"
#include "OM.h"
#include "LM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


/*
 * Function: Four om_DumpSlottedPage(Four, SlottedPage *)
 *
 * Description:
 *  Print the all contents of the given slotted page.
 *  The page is specified with the pointer to the buffer holding the page.
 *
 * Returns:
 *  Error code
 *     eNOERROR
 */
Four om_DumpSlottedPage(
    Four handle,
    SlottedPage *apage)		/* IN a buffer containning the slotted page */
{
    Object *obj;		/* points to the currently displayed object */
    ObjectID movedOID;		/* ObjectID to which the obj was moved */
    Four i;			/* index variable */
    Four j;			/* index variable */

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_OM, TR1, ("OM_DumpSlottedPage(apage=%P)", apage));

    printf("+------------------------------------------------------------+\n");
    printf("|  nSlots = %-3ld         free = %-4ld          unused = %-4ld   |\n",
	   apage->header.nSlots, apage->header.free, apage->header.unused);
    printf("| FREE = %-4ld           CFREE = %-4ld                         |\n",
	   SP_FREE(common_perThreadDSptr->nilXactId, apage), SP_CFREE(apage));
    printf("+------------------------------------------------------------+\n");
    printf("| fid = (%4ld, %4ld)                                      |\n",
	   apage->header.fid.volNo, apage->header.fid.serial); 
    printf("| nextPage = %-4ld                    prevPage = %-4ld         |\n",
	   apage->header.nextPage, apage->header.prevPage);
    printf("+------------------------------------------------------------+\n");

    for (i = 0; i < apage->header.nSlots; i++) {
	if (apage->slot[-i].offset == EMPTYSLOT) continue;

	obj = (Object *)&(apage->data[apage->slot[-i].offset]);

	printf("|%3ld| ", i);
	printf("%c", (obj->header.properties & P_MOVED) ? 'M':' ');
	printf("%c", (obj->header.properties & P_FORWARDED) ? 'F':' ');
	printf("%c", (obj->header.properties & P_LRGOBJ) ? 'L':' ');
	printf("%c", (obj->header.properties & P_LRGOBJ_ROOTWITHHDR) ? 'R':' ');

	printf(" %4ld ", obj->header.length);

	if (obj->header.properties & P_MOVED) {
	    /* moved object: print the moved ObjectID */

	    movedOID = *((ObjectID *)(obj->data));
	    printf("(%4ld, %4ld, %4ld, %4ld)                    ",
		   movedOID.volNo, movedOID.pageNo, movedOID.slotNo, movedOID.unique);

	} else {
	    /* plain data */

	    if (obj->header.properties & P_LRGOBJ) {

		for(j = 0; j < 44; j++) putchar(' ');

	    } else {

		for(j = 0; j < 44 && j < obj->header.length; j++)
		    if (isprint(obj->data[j])) putchar(obj->data[j]);
		    else putchar('~');

		for(; j < 44; j++)
		    putchar(' ');
	    }
	}

	printf(" |\n");
    }
    printf("+------------------------------------------------------------+\n");
    return(eNOERROR);

} /* om_DumpSlottedPage( ) */


/*
 * Function: Four OM_DumpSlottedPage(Four, PageID*, LockParameter*)
 *
 * Description:
 *  Print all the contents of the given slotted page.
 *  The page to be printed is specified with its page identifier.
 *
 * Returns:
 *  Error code
 *    eBADPAGEID_OM
 *    some errors caused by function calls
 */
Four OM_DumpSlottedPage(
    Four handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    FileID *fid,                /* IN file containing this page */
    PageID *pid,		/* IN page to dump */
    LockParameter *lockup)      /* IN request lock or not */

{
    Four e;			/* error number */
    SlottedPage *apage;		/* pointer to buffer holding the page */
    Buffer_ACC_CB *aPage_BCBP;	/* buffer access control block containing data */
    LockReply lockReply;	/* lock reply */
    LockMode oldMode;


    TR_PRINT(handle, TR_OM, TR1, ("OM_DumpSlottedPage(pid=%P)", pid));


    /* parameter checking */
    if (pid == NULL || pid->pageNo == NIL) ERR(handle, eBADPAGEID);

    if(lockup){
	/* lock the page */
	e = LM_getPageLock(handle, &xactEntry->xactId, pid, fid, lockup->mode,
			   lockup->duration, L_UNCONDITIONAL, &lockReply, &oldMode);
	if (e < eNOERROR) ERR(handle, e);

	if(lockReply == LR_DEADLOCK){
	    ERR(handle, eDEADLOCK);		 /* deadlock */
	}
    }

    /* read the page into the buffer */
    e = BfM_getAndFixBuffer(handle, pid, M_FREE, &aPage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    apage = (SlottedPage *)aPage_BCBP->bufPagePtr;

    printf("+------------------------------------------------------------+\n");
    printf("|                 PageID = (%4ld, %4ld)                      |\n",
	   pid->volNo, pid->pageNo);
    printf("+------------------------------------------------------------+\n");

    /* call auxiary function with the pointer to the buffer */
    e = om_DumpSlottedPage(handle, apage);
    if (e < eNOERROR) ERRB1(handle, e, aPage_BCBP, PAGE_BUF);

    /* free the buffer */
    e = BfM_unfixBuffer(handle, aPage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* OM_DumpSlottedPage() */


/*
 * Function: Four om_DumpObject(Four, Object*, ObjectID*)
 *
 * Description:
 *  Print an object togather with its properties.
 *  The object is specified with the pointer to the object.
 *  ObjectID of the object is needed for the large object access.
 *
 * Returns:
 *  Error code
 *    some errors caused by function calls
 */
Four om_DumpObject(
    Four handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    Object *obj,		/* IN object to dump */
    ObjectID *oid)		/* IN page containing the object */
{
    Four e;			/* error number */
    ObjectID movedOID;		/* moved ObjectID */
    Four i;			/* index variable */
    char ch;


    TR_PRINT(handle, TR_OM, TR1, ("om_DumpObject(obj=%P)", obj));

    printf("[PROPERTIES] : ");
    if (!obj->header.properties) printf("PLAIN ");
    if (obj->header.properties & P_MOVED) printf("MOVED ");
    if (obj->header.properties & P_FORWARDED) printf("FORWARDED ");
    if (obj->header.properties & P_LRGOBJ) printf("LRGOBJ ");
    if (obj->header.properties & P_LRGOBJ_ROOTWITHHDR) printf("ROOTWITHHDR ");
    printf("\n");

    printf("[TAG] : %ld\n", obj->header.tag);

    printf("[LENGTH] : %ld\n", obj->header.length);

    printf("[DATA] : ");
    if (obj->header.properties & P_MOVED) {
	/* moved object: print the moved ObjectID */

	movedOID = *((ObjectID *)&(obj->data[0]));
	printf("(%ld, %ld, %ld, %ld)\n",
	       movedOID.volNo, movedOID.pageNo, movedOID.slotNo, movedOID.unique);

    } else {
	/* plain data */

	if (obj->header.properties & P_LRGOBJ) {

	    if (oid  != NULL) {
		for(i = 0; i < 70 && i < obj->header.length; i++) {
		    e = LOT_ReadObject(handle, xactEntry, oid->volNo, obj->data,
                                       IS_LRGOBJ_ROOTWITHHDR(obj->header.properties),
                                       i, 1, &ch);
		    if (e < eNOERROR) ERR(handle, e);

		    if (isprint(ch)) putchar(ch);
		    else putchar('~');
		}
	    }

	} else {

	    for(i = 0; i < 70 && i < obj->header.length; i++)
		if (isprint(obj->data[i])) putchar(obj->data[i]);
		else putchar('~');
	}
    }
    printf("\n");

    return(eNOERROR);

} /* om_DumpObject() */


/*
 * Function: Four OM_DumpObject(Four, ObjectID *, LockParameter*)
 *
 * Description:
 *  Print all the contents of the object togather of its header information.
 *  The object is specified with its object identifier.
 *
 * Returns:
 *  Error codes
 *    eBADOBJECTID_OM
 *    some errors caused by function calls
 */
Four OM_DumpObject(
    Four handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    FileID   *fid,              /* IN FileID containing this object */
    ObjectID *oid,		/* IN ObjectID to dump */
    LockParameter *lockup)      /* IN request lock or not */
{
    Four e;			/* error number */
    PageID pid;			/* PageID of page holding the given object */
    SlottedPage *apage;		/* pointer to buffer of slotted page */
    Buffer_ACC_CB *aPage_BCBP;	/* buffer access control block containing data */
    Four offset;		/* starting offset of object within page */
    Object *obj;		/* points to the given object in page */
    LockReply lockReply;	/* lock reply */
    LockMode oldMode;


    TR_PRINT(handle, TR_OM, TR1, ("OM_DumpObject(oid=%P)", oid));


    if (oid == NULL)	ERR(handle, eBADOBJECTID_OM);

    /* make pid point to page containing the object */
    pid = *((PageID *)oid);

    if(lockup){
	/* lock the page */
	e = LM_getPageLock(handle, &xactEntry->xactId, &pid, fid, lockup->mode,
			   lockup->duration, L_UNCONDITIONAL, &lockReply, &oldMode);
	if (e < eNOERROR) ERR(handle, e);

	if(lockReply == LR_DEADLOCK){
	    ERR(handle, eDEADLOCK);     /* deadlock */
	}
    }

    e = BfM_getAndFixBuffer(handle, &pid, M_FREE, &aPage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    apage = (SlottedPage *)aPage_BCBP->bufPagePtr;

    if (!IS_VALID_OBJECTID(oid, apage))
	ERRB1(handle, eBADOBJECTID_OM, aPage_BCBP, PAGE_BUF);

    printf("[ObjectID] : (%ld, %ld, %ld, %ld)\n",
	   oid->volNo, oid->pageNo, oid->slotNo, oid->unique);

    offset = apage->slot[-(oid->slotNo)].offset;
    obj = (Object *)&(apage->data[offset]);

    e = om_DumpObject(handle, xactEntry, obj, oid);
    if (e < eNOERROR) ERRB1(handle, e, aPage_BCBP, PAGE_BUF);

    if (obj->header.properties & P_MOVED) {
	e = OM_DumpObject(handle, xactEntry, fid, (ObjectID *)(obj->data), lockup);
	if (e < eNOERROR) ERRB1(handle, e, aPage_BCBP, PAGE_BUF);
    }

    e = BfM_unfixBuffer(handle, aPage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* OM_DumpObject( ) */
