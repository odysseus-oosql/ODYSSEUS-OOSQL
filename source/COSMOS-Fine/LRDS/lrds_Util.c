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
 * Module: lrds_Util.c
 *
 * Description:
 *  some utility functions
 *
 * Exports:
 *  void lrds_KeyInfoToKeyDesc(Four, ColInfo*, ColDesc*, KeyInfo*, KeyDesc*)
 *  void lrds_MLGF_KeyInfoToMLGF_KeyDesc(Four, ColInfo*, ColDesc*, MLGF_KeyInfo*, MLGF_KeyDesc*)
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "SM.h"
#include "LRDS.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*
 * Function: void lrds_KeyInfoToKeyDesc(Four, ColInfo*, ColDesc*, KeyInfo*, KeyDesc*);
 *
 * Description:
 *  Convert KeyInfo data structure to KeyDesc data structure.
 *  Column informations are required to conver into KeyDesc. The column
 *  information can be given either by ColInfo structure or by ColDesc
 *  structure.
 *
 * Returns:
 *  None
 */
void lrds_KeyInfoToKeyDesc(
    Four handle,
    ColInfo *cinfo,		/* IN column information using ColInfo */
    ColDesc *cdesc,		/* IN column information using ColDesc */
    KeyInfo *kinfo,		/* IN Key Information */
    KeyDesc *kdesc)		/* OUT Btree Key Description */
{
    Four i;


    TR_PRINT(handle, TR_LRDS, TR1,
	     ("lrds_IndexDescToKeyDesc(cinfo=%P, cdesc=%P, kinfo=%P, kdesc=%P)",
	      cinfo, cdesc, kinfo, kdesc));


    kdesc->flag = kinfo->flag;
    kdesc->nparts = kinfo->nColumns;

    if (cinfo != NULL) {
	if (cinfo[kinfo->columns[0].colNo].type == SM_TEXT) {
	    /* The index on SM_TEXT column consists of a key part of SM_VARSTRING. */
	    kdesc->kpart[0].type = SM_VARSTRING;
            if (kinfo->columns[0].flag & KEYINFO_COL_DESC) kdesc->kpart[0].type |= SM_DESC;
	    /* 'offset' field of KeyDesc is not used. */
	    kdesc->kpart[0].length = sizeof(Two) + MAXKEYWORDLEN;
	} else {
	    for (i = 0; i < kinfo->nColumns; i++) {
		kdesc->kpart[i].type = cinfo[kinfo->columns[i].colNo].type;

                if (kinfo->columns[i].flag & KEYINFO_COL_DESC) kdesc->kpart[i].type |= SM_DESC;
		/* 'offset' field of KeyDesc is not used. */
		kdesc->kpart[i].length = cinfo[kinfo->columns[i].colNo].length;
	    }
	}
    } else { /* should be cdesc != NULL */
	if (cdesc[kinfo->columns[0].colNo].type == SM_TEXT) {
	    /* The index on SM_TEXT column consists of a key part of SM_VARSTRING. */
	    kdesc->kpart[0].type = SM_VARSTRING;
            if (kinfo->columns[0].flag & KEYINFO_COL_DESC) kdesc->kpart[0].type |= SM_DESC;
	    /* 'offset' field of KeyDesc is not used. */
	    kdesc->kpart[0].length = sizeof(Two) + MAXKEYWORDLEN;
	} else {
	    for (i = 0; i < kinfo->nColumns; i++) {
		kdesc->kpart[i].type = cdesc[kinfo->columns[i].colNo].type;
                if (kinfo->columns[i].flag & KEYINFO_COL_DESC) kdesc->kpart[i].type |= SM_DESC;
		/* 'offset' field of KeyDesc is not used. */
		kdesc->kpart[i].length = cdesc[kinfo->columns[i].colNo].length;
	    }
	}
    }

} /* lrds_KeyInfoToKeyDesc() */



/*
 * Function: void lrds_MLGF_KeyInfoToMLGF_KeyDesc(Four, ColInfo*, ColDesc*,
 *                                                MLGF_KeyInfo*, MLGF_KeyDesc*);
 *
 * Description:
 *  Convert MLGF_KeyInfo data structure to MLGF_KeyDesc data structure.
 *  Column informations are required to conver into MLGF_KeyDesc. The column
 *  information can be given either by ColInfo structure or by ColDesc
 *  structure.
 *
 * Returns:
 *  None
 */
void lrds_MLGF_KeyInfoToMLGF_KeyDesc(
    Four handle,
    ColInfo *cinfo,		/* IN column information using ColInfo */
    ColDesc *cdesc,		/* IN column information using ColDesc */
    MLGF_KeyInfo *kinfo,	/* IN Key Information */
    MLGF_KeyDesc *kdesc)	/* OUT MLGF Key Description */
{
    Four k;			/* index variable */
    Four type;			/* data type of the first key attribute */


    TR_PRINT(handle, TR_LRDS, TR1,
	     ("lrds_MLGF_KeyInfoToMLGF_KeyDesc(handle, cinfo=%P, cdesc=%P, kinfo=%P, kdesc=%P)",
	      cinfo, cdesc, kinfo, kdesc));


    type = (cinfo != NULL) ? cinfo[kinfo->colNo[0]].type : cdesc[kinfo->colNo[0]].type;

    kdesc->flag = kinfo->flag;
    kdesc->extraDataLen = kinfo->extraDataLen;

    /* OpenGIS ... */
    /* Geometry type의 UDT인 경우에도 동작하도록 수정 ... */
    /* SM_VARSTRING type can be UDT of geometry type, thus it should be handled, too */
    if (type == SM_MBR || type == SM_VARSTRING){
    /* ... OpenGIS */
	kdesc->nKeys = MBR_NUM_PARTS;
	for (k = 0; k < kdesc->nKeys/2; k++)
	    MLGF_KEYDESC_SET_MINTYPE(*kdesc, k);
	for ( ; k < kdesc->nKeys; k++)
	    MLGF_KEYDESC_SET_MAXTYPE(*kdesc, k);
    } else {
	kdesc->nKeys = kinfo->nColumns;
	MLGF_KEYDESC_CLEAR_MINMAXTYPEVECTOR(*kdesc);
    }

} /* lrds_IndexDescToKeyDesc() */



