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
 * Module: LRDS_NextTuple.c
 *
 * Description:
 *  Returns the TupleID of the next tuple.
 *
 * Exports:
 *  Four LRDS_NextTuple(Four, TupleID*, LRDS_Cursor**)
 *
 * Returns:
 *  1) EOS - end of scan
 *  2) Error code - if return value is a negative number
 *       eBADPARAMETER
 *       some erros caused by function calls
 */

#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "Util.h"
#include "SM_Internal.h"	
#include "LRDS.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


/* Local function prototype */
Boolean lrds_BoolExpCompare(Four, ColDesc*, ColListStruct*, BoolExp*);


Four LRDS_NextTuple(
    Four handle,
    Four                scanId,                 /* IN used scan */
    TupleID             *tid,                   /* OUT next tuple */
    LRDS_Cursor         **cursor)               /* OUT cursor */
{
    Four                e;                      /* error number */
    Four                i;                      /* index variable */
    ColListStruct       clist[1];               /* column list structure */
    char                data[MAXKEYLEN];        /* a column value */
    Boolean             notFound = TRUE;        /* TRUE if the wanted tuple is not found. */
    LockParameter       lockup;                 /* lockup for SM_Fetch Tuple */
    LockReply           reply;                  /* lock reply */
    lrds_RelTableEntry  *relTableEntry;         /* pointer to an entry of relation table */
    LockParameter       *lockupPtr;             /* pointer to the lockup value */
    ColDesc             *relTableEntry_cdesc;   


    TR_PRINT(TR_LRDS, TR1, ("LRDS_NextTuple(handle, scanId=%ld, tid=%P)", scanId, tid));


    /* check parameters */
    if (!LRDS_VALID_SCANID(handle, scanId)) ERR(handle, eBADPARAMETER);


    /* Get the relation table entry. */
    relTableEntry = LRDS_GET_RELTABLE_ENTRY(handle, LRDS_SCANTABLE(handle)[scanId].orn);
    relTableEntry_cdesc = PHYSICAL_PTR(relTableEntry->cdesc); 


    /* Prepare lock parameter */
    /* if the relation is a catalog relation then lockup is NULL */
    /* Our policy :: no lock on LRDS catalog relation exists under the LRDS layer */
    /* no lock for the temporary relation */
    if (LRDS_USEROPENRELTABLE(handle)[LRDS_SCANTABLE(handle)[scanId].orn].tmpRelationFlag) {
	lockupPtr = NULL;

    } else {
	lockup.mode = L_S;
	lockup.duration = L_COMMIT;

	lockupPtr = &lockup;
    }


    while (notFound) {
	/* call SM_NextObject() */
	e = SM_NextObject(handle, LRDS_SCANTABLE(handle)[scanId].smScanId,
			  &(LRDS_SCANTABLE(handle)[scanId].tid), NULL, NULL, (Cursor**)cursor,
			  (relTableEntry->isCatalog)? NULL :lockupPtr);
	if (e < 0) {
	    SET_NILTUPLEID(LRDS_SCANTABLE(handle)[scanId].tid);
	    ERR(handle, e);
	}

	if (e == EOS) {
	    SET_NILTUPLEID(LRDS_SCANTABLE(handle)[scanId].tid);
	    return(EOS);
	}

	/* Does the tuple satisfy the boolean expressions? */
	for (i = 0; i < LRDS_SCANTABLE(handle)[scanId].nBools; i++) {

	    /* Read a value of the column on which a boolean expression is defined. */
	    clist[0].colNo = LRDS_SCANTABLE(handle)[scanId].bool[i].colNo;
	    clist[0].start = ALL_VALUE;
	    clist[0].dataLength = relTableEntry_cdesc[clist[0].colNo].length; 
	    if (relTableEntry_cdesc[clist[0].colNo].type == SM_VARSTRING ||
		relTableEntry_cdesc[clist[0].colNo].type == SM_STRING) 
		clist[0].data.ptr = &(data[0]);

	    e = LRDS_FetchTuple(handle, scanId, TRUE, (TupleID*)NULL, 1, &(clist[0]));
	    if (e < 0) ERR(handle, e);

	    if (!lrds_BoolExpCompare(handle, &(relTableEntry_cdesc[clist[0].colNo]),
				     &(clist[0]),
				     &(LRDS_SCANTABLE(handle)[scanId].bool[i]))) break; 
	}

	if (i == LRDS_SCANTABLE(handle)[scanId].nBools) notFound = FALSE; /* found!!! */

    }


    /* Return the current tuple id. */
    if (tid != NULL)	*tid = LRDS_SCANTABLE(handle)[scanId].tid;


    return(eNOERROR);

} /* LRDS_NextTuple() */


/*
 * Function: lrds_BoolExpCompare()
 *
 * Description:
 *  Compare the given data with the boolean expression.
 *
 * Returns:
 *  Result of compare
 */
Boolean lrds_BoolExpCompare(
    Four handle,
    ColDesc             *cdesc,	        /* IN description of column on which */
				        /* the boolean expression is defined */
    ColListStruct       *clist,	        /* IN a column list struct */
    BoolExp             *bool)	        /* IN a boolean expression */
{
    Four                i;		/* index variable */
    CompOp              result;		/* result of compare */
    Two_Invariable      s1, s2;         /* short values */
    Four_Invariable     i1, i2;         /* int values */
    Four_Invariable     l1, l2;         /* long values */
    Eight_Invariable    ll1, ll2;       /* long long values */
    float               f1, f2;		/* float values */
    double              d1, d2;		/* double values */
    PageID              pid1, pid2;	/* PageID values */
    OID                 oid1, oid2;	/* OID values */ 
    Two                 len1, len2;	/* length of the varialbe string */
    unsigned char       *p1, *p2;


    TR_PRINT(TR_LRDS, TR1,
             ("lrds_BoolExpCompare(handle, cdesc=%P, clist=%P, bool=%P)",
	      cdesc, clist, bool));


    /* comparison result changed; EQUAL => SM_EQ, LESS => SM_LT, GREAT => SM_GT */

    /* Initially the result of compare is SM_EQ. */
    result = SM_EQ;

    switch (cdesc->type) {
      case SM_SHORT:
	s1 = clist->data.s;
	s2 = bool->data.s;

	if (s1 > s2) result = SM_GT;
	else if(s1 < s2) result = SM_LT;

	break;

      case SM_INT:
	i1 = clist->data.i;
	i2 = bool->data.i;

	if (i1 > i2) result = SM_GT;
	else if (i1 < i2) result = SM_LT;

	break;

      case SM_LONG:
	l1 = clist->data.l;
	l2 = bool->data.l;

	if (l1 > l2) result = SM_GT;
	else if (l1 < l2) result = SM_LT;

	break;

      case SM_LONG_LONG: 
        ll1 = clist->data.l;
        ll2 = bool->data.l;

        if (ll1 > ll2) result = SM_GT;
        else if (ll1 < ll2) result = SM_LT;

        break;

      case SM_FLOAT:
	f1 = clist->data.f;
	f2 = bool->data.f;

	if (f1 > f2) result = SM_GT;
	else if (f1 < f2) result = SM_LT;

	break;

      case SM_DOUBLE:
	d1 = clist->data.d;
	d2 = bool->data.d;

	if (d1 > d2) result = SM_GT;
	else if (d1 < d2) result = SM_LT;

	break;

      case SM_STRING:	/* fixed length string */
	p1 = (unsigned char*)clist->data.ptr;
	p2 = (unsigned char*)&(bool->data.str[0]);

	for (i = 0; i < cdesc->length; i++, p1++, p2++) {
	    if (*p1 > *p2) {
		result = SM_GT;
		break;
	    } else if (*p1 < *p2) {
		result = SM_LT;
		break;
	    }
	}

	break;

      case SM_VARSTRING:	/* variable length string */
	/* get the string length */
	len1 = clist->retLength;
	len2 = bool->length;

	p1 = (unsigned char*)clist->data.ptr;
	p2 = (unsigned char*)&(bool->data.str[0]);

	for (i = MIN(len1, len2); i > 0; i--, p1++, p2++) {
	    if (*p1 > *p2) {
		result = SM_GT;
		break;
	    } else if (*p1 < *p2) {
		result = SM_LT;
		break;
	    }
	}

	/* left and right strings are same in MIN(len1, len2) bytes */
	if (len1 > len2) result = SM_GT;
	else if (len1 < len2) result = SM_LT;

	break;

      case SM_PAGEID:
      case SM_FILEID:
      case SM_INDEXID:
	pid1 = clist->data.pid;
	pid2 = bool->data.pid;

	if (pid1.volNo > pid2.volNo) result = SM_GT;
	else if (pid1.volNo < pid2.volNo) result = SM_LT;
	else if (pid1.pageNo > pid2.pageNo) result = SM_GT;
	else if (pid1.pageNo < pid2.pageNo) result = SM_LT;

	break;

      case SM_OID:
	oid1 = clist->data.oid;
	oid2 = bool->data.oid;

	if (oid1.volNo > oid2.volNo) result = SM_GT;
	else if (oid1.volNo < oid2.volNo) result = SM_LT;
	else if (oid1.pageNo > oid2.pageNo) result = SM_GT;
	else if (oid1.pageNo < oid2.pageNo) result = SM_LT;
	else if (oid1.slotNo > oid2.slotNo) result = SM_GT;
	else if (oid1.slotNo < oid2.slotNo) result = SM_LT;

	break;
    }

    if (result & bool->op) return(TRUE);

    return(FALSE);

} /* lrds_BoolExpCompare() */
