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
 * Module: OM_Dump.c
 *
 * Description:
 *  This module contains the utility functions to dump the page and the record.
 *
 * Exports:
 *  Four om_DumpSlottedPage(SlottedPage *)
 *  Four OM_DumpSlottedPage(PageID *)
 *  Four om_DumpObject(Object*, ObjectID*)
 *  Four OM_DumpObject(ObjectID *)
 *  Four om_DumpSpaceList(Four, ShortPageID)
 *  Four OM_DumpSpaceList(ObjectID*)
 *
 * Returns:
 *  error Code:
 *    some errors caused by function calls
 */


#include <stdio.h>
#include <ctype.h>
#include "common.h"
#include "trace.h"
#include "BfM.h"
#include "LOT.h"
#include "OM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * om_DumpSlottedPage()
 *================================*/
/*
 * Function: Four om_DumpSlottedPage(SlottedPage *)
 *
 * Description:
 *  Print the all contents of the given slotted page.
 *  The page is specified with the pointer to the buffer holding the page.
 *
 * Returns:
 *  error code
 *     eNOERROR
 */
Four om_DumpSlottedPage(
    Four handle,
    SlottedPage *apage)		/* IN a buffer containning the slotted page */
{
    Object 		*obj;		/* points to the currently displayed object */
    ObjectID 	movedOID;	/* ObjectID to which the obj was moved */
    Two  		i;			/* index variable */
    Four 		j;			/* index variable */

    
    TR_PRINT(TR_OM, TR1, ("OM_DumpSlottedPage(handle, apage=%P)", apage));
    
    printf("+------------------------------------------------------------+\n");
    printf("|  nSlots = %-3ld         free = %-4ld          unused = %-4ld   |\n",
	   apage->header.nSlots, apage->header.free, apage->header.unused);
    printf("| FREE = %-4ld           CFREE = %-4ld                         |\n",
	   SP_FREE(apage), SP_CFREE(apage));
    printf("+------------------------------------------------------------+\n");
    printf("| fid = (%4ld, %4ld)                                      |\n",
	   apage->header.fid.volNo, apage->header.fid.serial);                
    printf("| nextPage = %-4ld                    prevPage = %-4ld         |\n",
	   apage->header.nextPage, apage->header.prevPage);
    printf("| spaceListPrev = %-4ld               spaceListNext = %-4ld    |\n",
	   apage->header.spaceListPrev, apage->header.spaceListNext);
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

} /* om_DumpSlottedPage() */



/*@================================
 * OM_DumpSlottedPage()
 *================================*/
/*
 * Function: Four OM_DumpSlottedPage(PageID*)
 *
 * Description:
 *  Print all the contents of the given slotted page.
 *  The page to be printed is specified with its page identifier.
 *
 * Returns:
 *  error code
 *    eBADPAGEID_OM
 *    some errors caused by function calls
 */
Four OM_DumpSlottedPage(
    Four handle,
    PageID *pid)		/* IN page to dump */
{
    Four 		e;			/* error number */
    SlottedPage *apage;		/* pointer to buffer holding the page */

    
    TR_PRINT(TR_OM, TR1, ("OM_DumpSlottedPage(handle, pid=%P)", pid));

    /*@ parameter checking */
    if (pid == NULL || pid->pageNo == NIL) ERR(handle, eBADPAGEID_OM);
    
    /*@ read the page into the buffer */
    e = BfM_GetTrain(handle, pid, (char **)&apage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    printf("+------------------------------------------------------------+\n");
    printf("|                 PageID = (%4ld, %4ld)                      |\n",
	   pid->volNo, pid->pageNo);
    printf("+------------------------------------------------------------+\n");

    /* call auxiary function with the pointer to the buffer */    
    e = om_DumpSlottedPage(handle, apage);
    if (e < 0) ERRB1(handle, e, pid, PAGE_BUF);
    
    /*@ free the buffer */
    e = BfM_FreeTrain(handle, pid, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    return(eNOERROR);
    
} /* OM_DumpSlottedPage() */



/*@================================
 * om_DumpObject()
 *================================*/
/*
 * Function: Four om_DumpObject(Object*, ObjectID*)
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
    Object 		*obj,		/* IN object to dump */
    ObjectID 	*oid)		/* IN page containing the object */
{
    Four 		e;			/* error number */
    ObjectID 	movedOID;	/* moved ObjectID */
    Four        i;          /* index variable */
    char 		ch;

    
    TR_PRINT(TR_OM, TR1, ("om_DumpObject(handle, obj=%P)", obj));

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
		    e = LOT_ReadObject(handle, (PageID *)oid, oid->slotNo, i, 1, &ch);
		    if (e < 0) ERR(handle, e);
		
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
     


/*@================================
 * OM_DumpObject()
 *================================*/
/*
 * Function: Four OM_DumpObject(ObjectID *)
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
    ObjectID *oid)		/* IN ObjectID to dump */
{
    Four 			e;			/* error number */
    PageID 			pid;		/* PageID of page holding the given object */
    SlottedPage 	*apage;		/* pointer to buffer of slotted page */
    Four 			offset;		/* starting offset of object within page */
    Object 			*obj;		/* points to the given object in page */

    
    TR_PRINT(TR_OM, TR1, ("OM_DumpObject(handle, oid=%P)", oid));

    if (oid == NULL)	ERR(handle, eBADOBJECTID_OM);
    
    /*@ make pid point to page containing the object */
    pid = *((PageID *)oid);
    
    e = BfM_GetTrain(handle, &pid, (char **)&apage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    if (!IS_VALID_OBJECTID(oid, apage))
	ERRB1(handle, eBADOBJECTID_OM, &pid, PAGE_BUF);

    printf("[ObjectID] : (%ld, %ld, %ld, %ld)\n",
	   oid->volNo, oid->pageNo, oid->slotNo, oid->unique);
    
    offset = apage->slot[-(oid->slotNo)].offset;
    obj = (Object *)&(apage->data[offset]);
    
    e = om_DumpObject(handle, obj, oid);
    if (e < 0) ERRB1(handle, e, &pid, PAGE_BUF);

    if (obj->header.properties & P_MOVED) {
	e = OM_DumpObject(handle, (ObjectID *)(obj->data));
	if (e < 0) ERRB1(handle, e, &pid, PAGE_BUF);
    }
    
    e = BfM_FreeTrain(handle, &pid, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    return(eNOERROR);

} /* OM_DumpObject() */



/*@================================
 * om_DumpSpaceList()
 *================================*/
/*
 * Function: Four om_DumpSpaceList(Four, ShortPageID)
 *
 * Description:
 *  Print an available space list.
 *  For each element in the list print its previous element, current element,
 *  and the next element.
 *  'head' is the head of the list to be printed.
 *
 * Returns:
 *  error code
 *    some errors caused by function calls
 */
Four om_DumpSpaceList(
    Four handle,
    Four 		volID,		/* IN volume ID */
    ShortPageID head)		/* IN list head */
{
    Four 		e;			/* error number */
    PageID 		curPID;		/* current page */
    SlottedPage *apage;		/* pointer to buffer holding the page */

    while(head != NIL) {
	
	MAKE_PAGEID(curPID, volID, head);

	e = BfM_GetTrain(handle, &curPID, (char **)&apage, PAGE_BUF);
	if (e < 0) ERR(handle, e);

        printf("Previus: %4ld   Current: %4ld   Next: %4ld\n",
	       apage->header.spaceListPrev, head, apage->header.spaceListNext);

	head = apage->header.spaceListNext;
	
	e = BfM_FreeTrain(handle, &curPID, PAGE_BUF);
	if (e < 0) ERR(handle, e);
	
    }

    return(eNOERROR);
    
} /* om_DumpSpaceList() */



/*@================================
 * OM_DumpSpaceList()
 *================================*/
/*
 * Function: Four OM_DumpSpaceList(ObjectID*)
 *
 * Description:
 *  Print all the available space lists of the given file.
 *  The file is specified with 'catObjForFile', object identifier of the
 *  catalog object for the file.
 *  This routine calls an auxiliary function om_DumpSpaceList().
 *
 * Returns:
 *  error code
 *    eBADCATALOGOBJECT_OM
 *    some errors caused by function calls
 */
Four OM_DumpSpaceList(
    Four handle,
    ObjectID 		*catObjForFile)	/* IN file whose space list will be printed */
{
    Four        			e;			/* error number */
    SlottedPage 			*catPage;	/* buffer page containing the catalog object */
    sm_CatOverlayForData 	*catEntry; 	/* catalog entry overlay data structure */

    /*@ check parameter */
    if (catObjForFile == NULL) ERR(handle, eBADCATALOGOBJECT_OM);

    /* Get pointer to the catalog object for the file */
    e = BfM_GetTrain(handle, (TrainID*)catObjForFile, (char**)&catPage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    GET_PTR_TO_CATENTRY_FOR_DATA(catObjForFile, catPage, catEntry);
    
    printf("<10% availSpaceList>\n");
    e = om_DumpSpaceList(handle, catEntry->fid.volNo, catEntry->availSpaceList10);
    if (e < 0) ERRB1(handle, e, (TrainID*)catObjForFile, PAGE_BUF);

    printf("<20% availSpaceList>\n");
    e = om_DumpSpaceList(handle, catEntry->fid.volNo, catEntry->availSpaceList20);
    if (e < 0) ERRB1(handle, e, (TrainID*)catObjForFile, PAGE_BUF);

    printf("<30% availSpaceList>\n");
    e = om_DumpSpaceList(handle, catEntry->fid.volNo, catEntry->availSpaceList30);
    if (e < 0) ERRB1(handle, e, (TrainID*)catObjForFile, PAGE_BUF);

    printf("<40% availSpaceList>\n");
    e = om_DumpSpaceList(handle, catEntry->fid.volNo, catEntry->availSpaceList40);
    if (e < 0) ERRB1(handle, e, (TrainID*)catObjForFile, PAGE_BUF);

    printf("<50% availSpaceList>\n");
    e = om_DumpSpaceList(handle, catEntry->fid.volNo, catEntry->availSpaceList50);
    if (e < 0) ERRB1(handle, e, (TrainID*)catObjForFile, PAGE_BUF);

    e = BfM_FreeTrain(handle, (TrainID*)catObjForFile, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    return (eNOERROR);
    
} /* OM_DumpSpaceList() */
