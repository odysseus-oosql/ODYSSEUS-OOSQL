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
 * Module: lrds_Check.c
 *
 * Description:
 *  Check if the given data structures are validate.
 *
 * Exports:
 *  Boolean lrds_CheckColInfo(Two, ColInfo*)
 *  Boolean lrds_CheckKeyInfo(Two, ColInfo*, ColDesc*, KeyInfo*)
 *  Boolean lrds_CheckMLGF_KeyInfo(Two, ColInfo*, ColDesc*, MLGF_KeyInfo*)
 *  Boolean lrds_CheckIndexDesc(Two, ColInfo*, ColDesc*, IndexDesc*)
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "SM_Internal.h"
#include "LRDS.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


/*
 * Function: Boolean lrds_CheckColInfo(Two, ColInfo*)
 *
 * Description:
 *  Check if the given ColInfo is validate.
 *
 * Returns:
 */
Boolean lrds_CheckColInfo(
    Four handle,
    Two         nCols,          /* IN number of columns */
    ColInfo     *cinfo)         /* IN column information */
{
    Two         i;

    TR_PRINT(TR_LRDS, TR1, ("lrds_CheckColInfo(handle, nCols=%ld, cinfo=%P)", nCols, cinfo));

    for (i = 0; i < nCols; i++) {
	if (cinfo[i].complexType != SM_COMPLEXTYPE_BASIC &&
	    cinfo[i].complexType != SM_COMPLEXTYPE_SET &&
	    cinfo[i].complexType != SM_COMPLEXTYPE_ORDEREDSET && 
	    cinfo[i].complexType != SM_COMPLEXTYPE_COLLECTIONSET &&
	    cinfo[i].complexType != SM_COMPLEXTYPE_COLLECTIONBAG &&
	    cinfo[i].complexType != SM_COMPLEXTYPE_COLLECTIONLIST
            ) return(FALSE);

	switch (cinfo[i].type) {
	  case SM_SHORT:
	  case SM_INT:
	  case SM_LONG:
          case SM_LONG_LONG:
	  case SM_FLOAT:
	  case SM_DOUBLE:
	  case SM_STRING:
	  case SM_VARSTRING:
	  case SM_PAGEID:
	  case SM_FILEID:
	  case SM_INDEXID:
	  case SM_OID:
	  case SM_MBR:		
	    /* do nothing */
	    break;
	  case SM_TEXT:
	    if (cinfo[i].complexType != SM_COMPLEXTYPE_BASIC) return(FALSE);
	    break;
	  default:
	    return(FALSE);
	}
    }

    return(TRUE);

} /* lrds_CheckColInfo() */



/*
 * Function: Boolean lrds_CheckKeyInfo(Two, ColInfo*, ColDesc*, KeyInfo*)
 *
 * Description:
 *  Check if the given KeyInfo is validate.
 *  Column informations are required to check KeyInfo. The column information
 *  can be given either by ColInfo structure or by ColDesc structure.
 *
 * Returns:
 *  TRUE if the given KeyInfo is correct
 *  FALSE otherwise
 */
Boolean lrds_CheckKeyInfo(
    Four handle,
    Two         nCols,          /* IN number of columns */
    ColInfo     *cinfo,         /* IN column information using ColInfo */
    ColDesc     *cdesc,         /* IN column information using ColDesc */
    KeyInfo     *kinfo)         /* IN key information */
{
    Two         i;
    Two         keyLength;      /* maximum key length */


    TR_PRINT(TR_LRDS, TR1,
             ("lrds_CheckKeyInfo(handle, nCols=%ld, cinfo=%P, cdesc=%P, kinfo=%P)",
	      nCols, cinfo, cdesc, kinfo));


    if (kinfo->nColumns <= 0 || kinfo->nColumns > MAXNUMKEYPARTS) return(FALSE);

    if (cinfo != NULL) {

	for (keyLength = 0, i = 0; i < kinfo->nColumns; i++) {
	    if (kinfo->columns[i].colNo < 0 || kinfo->columns[i].colNo >= nCols) return(FALSE);

	    /* SM_MBR cannot be an key part of Btree Index. */
	    if (cinfo[kinfo->columns[i].colNo].type == SM_MBR) return(FALSE);

	    /* The following data types should consist in an index by itself. */
	    /* SM_COMPLEXTYPE_SET, SM_TEXT */
	    if ((cinfo[kinfo->columns[i].colNo].complexType == SM_COMPLEXTYPE_SET ||
                 cinfo[kinfo->columns[i].colNo].complexType == SM_COMPLEXTYPE_ORDEREDSET || 
                 cinfo[kinfo->columns[i].colNo].complexType == SM_COMPLEXTYPE_COLLECTIONSET  ||
                 cinfo[kinfo->columns[i].colNo].complexType == SM_COMPLEXTYPE_COLLECTIONBAG  ||
                 cinfo[kinfo->columns[i].colNo].complexType == SM_COMPLEXTYPE_COLLECTIONLIST ||
		 cinfo[kinfo->columns[i].colNo].type == SM_TEXT) && i > 0) 
		return(FALSE);

	    switch (cinfo[kinfo->columns[i].colNo].type) {
	      case SM_VARSTRING:
		keyLength += sizeof(Two) + cinfo[kinfo->columns[i].colNo].length;
		break;

	      case SM_TEXT:
		keyLength += sizeof(Two) + MAXKEYWORDLEN;
		break;

	      default:
		keyLength += cinfo[kinfo->columns[i].colNo].length;
		break;
	    }
	}
    } else { /* should be cdesc != NULL */

	for (keyLength = 0, i = 0; i < kinfo->nColumns; i++) {
	    if (kinfo->columns[i].colNo < 0 || kinfo->columns[i].colNo >= nCols) return(FALSE);

	    /* SM_MBR cannot be an key part of Btree Index. */
	    if (cdesc[kinfo->columns[i].colNo].type == SM_MBR) return(FALSE);

	    /* The following data types should consist in an index by itself. */
	    /* SM_COMPLEXTYPE_SET, SM_TEXT */
	    if ((cdesc[kinfo->columns[i].colNo].complexType == SM_COMPLEXTYPE_SET ||
                 cdesc[kinfo->columns[i].colNo].complexType == SM_COMPLEXTYPE_ORDEREDSET || 
                 cdesc[kinfo->columns[i].colNo].complexType == SM_COMPLEXTYPE_COLLECTIONSET  ||
                 cdesc[kinfo->columns[i].colNo].complexType == SM_COMPLEXTYPE_COLLECTIONBAG  ||
                 cdesc[kinfo->columns[i].colNo].complexType == SM_COMPLEXTYPE_COLLECTIONLIST ||
		 cdesc[kinfo->columns[i].colNo].type == SM_TEXT) && i > 0) 
		return(FALSE);

	    switch (cdesc[kinfo->columns[i].colNo].type) {
	      case SM_VARSTRING:
		keyLength += sizeof(Two) + cdesc[kinfo->columns[i].colNo].length;
		break;

	      case SM_TEXT:
		keyLength += sizeof(Two) + MAXKEYWORDLEN;
		break;

	      default:
		keyLength += cdesc[kinfo->columns[i].colNo].length;
		break;
	    }
	}
    }

    if (keyLength > MAXKEYLEN) return(FALSE);

    return(TRUE);

} /* lrds_CheckKeyInfo() */



/*
 * Function: Boolean lrds_CheckMLGF_KeyInfo(Two, ColInfo*, ColDesc*, MLGF_KeyInfo*)
 *
 * Description:
 *  Check if the given MLGF_KeyInfo is validate.
 *  Column informations are required to check MLGF_KeyInfo. The column
 *  information can be given either by ColInfo structure or by ColDesc
 *  structure.
 *
 * Returns:
 *  TRUE if the given MLGF_KeyInfo is correct
 *  FALSE otherwise
 */
Boolean lrds_CheckMLGF_KeyInfo(
    Four handle,
    Two                 nCols,          /* IN number of columns */
    ColInfo             *cinfo,         /* IN column information using ColInfo */
    ColDesc             *cdesc,         /* IN column information using ColDesc */
    MLGF_KeyInfo        *kinfo)         /* IN key information */
{
    Two                 i;


    TR_PRINT(TR_LRDS, TR1,
             ("lrds_CheckKeyInfo(handle, nCols=%ld, cinfo=%P, cdesc=%P, kinfo=%P)",
	      nCols, cinfo, cdesc, kinfo));


    if (kinfo->nColumns <= 0 || kinfo->nColumns > nCols || kinfo->nColumns > MLGF_MAXNUM_KEYS)
	return(FALSE);

    if (cinfo != NULL) {

	for (i = 0; i < kinfo->nColumns; i++) {
	    if (kinfo->colNo[i] < 0 || kinfo->colNo[i] >= nCols) return(FALSE);

	    /* The following data types should consist in an index by itself. */
	    /* SM_COMPLEXTYPE_SET, SM_MBR */
	    if ((cinfo[kinfo->colNo[i]].complexType == SM_COMPLEXTYPE_SET ||
                 cinfo[kinfo->colNo[i]].complexType == SM_COMPLEXTYPE_ORDEREDSET || 
                 cinfo[kinfo->colNo[i]].complexType == SM_COMPLEXTYPE_COLLECTIONSET  ||
                 cinfo[kinfo->colNo[i]].complexType == SM_COMPLEXTYPE_COLLECTIONBAG  ||
                 cinfo[kinfo->colNo[i]].complexType == SM_COMPLEXTYPE_COLLECTIONLIST ||
		 cinfo[kinfo->colNo[i]].type == SM_MBR) && i > 0)
		return(FALSE);

	    switch (cinfo[kinfo->colNo[i]].type) {
	      case SM_SHORT:
	      case SM_INT:
	      case SM_LONG:
	      case SM_MBR:
          case SM_VARSTRING:

		/* do nothing */
		break;

	      default:
		return(FALSE);
	    }
	}
    } else { /* should be cdesc != NULL */

	for (i = 0; i < kinfo->nColumns; i++) {
	    if (kinfo->colNo[i] < 0 || kinfo->colNo[i] >= nCols) return(FALSE);

	    /* The following data types should consist in an index by itself. */
	    /* SM_COMPLEXTYPE_SET, SM_MBR */
	    if ((cdesc[kinfo->colNo[i]].complexType == SM_COMPLEXTYPE_SET ||
                 cdesc[kinfo->colNo[i]].complexType == SM_COMPLEXTYPE_ORDEREDSET || 
                 cdesc[kinfo->colNo[i]].complexType == SM_COMPLEXTYPE_COLLECTIONSET  ||
                 cdesc[kinfo->colNo[i]].complexType == SM_COMPLEXTYPE_COLLECTIONBAG  ||
                 cdesc[kinfo->colNo[i]].complexType == SM_COMPLEXTYPE_COLLECTIONLIST ||
		 cdesc[kinfo->colNo[i]].type == SM_MBR) && i > 0)
		return(FALSE);

	    switch (cdesc[kinfo->colNo[i]].type) {
	      case SM_SHORT:
	      case SM_INT:
	      case SM_LONG:
	      case SM_MBR:
          case SM_VARSTRING:

		/* do nothing */
		break;

	      default:
		return(FALSE);
	    }
	}
    }
    return(TRUE);

} /* lrds_CheckMLGF_KeyInfo() */



/*
 * Function: Boolean lrds_CheckIndexDesc(Two, ColInfo*, ColDesc*, IndexDesc*)
 *
 * Description:
 *  Check if the given LRDS_IndexDesc is validate.
 *  Column informations are required to check LRDS_IndexDesc. The column
 *  information can be given either by ColInfo structure or by ColDesc
 *  structure.
 *
 * Returns:
 *  TRUE if the given LRDS_IndexDesc is correct
 *  FALSE otherwise
 */
Boolean lrds_CheckIndexDesc(
    Four handle,
    Two       nCols,                    /* IN number of columns */
    ColInfo *cinfo,             /* IN column information using ColInfo */
    ColDesc *cdesc,             /* IN column information using ColDesc */
    LRDS_IndexDesc* idesc)      /* IN index descriptiron */
{
    Boolean result;


    TR_PRINT(TR_LRDS, TR1,
             ("lrds_CheckIndexDesc(handle, nCols=%ld, cinfo=%P, cdesc=%P, idesc=%P)",
	      nCols, cinfo, cdesc, idesc));


    switch (idesc->indexType) {
      case SM_INDEXTYPE_BTREE:
	result = lrds_CheckKeyInfo(handle, nCols, cinfo, cdesc, &idesc->kinfo.btree);
	break;
      case SM_INDEXTYPE_MLGF:
	result = lrds_CheckMLGF_KeyInfo(handle, nCols, cinfo, cdesc, &idesc->kinfo.mlgf);
	break;
      default:
	return(FALSE);
    }

    return(result);

} /* lrds_CheckIndexDesc() */
