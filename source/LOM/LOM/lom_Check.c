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
/*    ODYSSEUS/OOSQL DB-IR-Spatial Tightly-Integrated DBMS                    */
/*    Version 5.0                                                             */
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
 * Module: lom_Check.c
 *
 * Description:
 *  Check if the given data structures are validate.
 *
 * Imports:
 *  None
 *
 * Exports:
 *  Boolean lom_CheckAttrInfo(handle, Four, AttrInfo*)
 *  Boolean lom_CheckKeyInfo(handle, Four, AttrInfo*, ColDesc*, KeyInfo*)
 *  Boolean lom_CheckMLGF_KeyInfo(handle, Four, AttrInfo*, ColDesc*, MLGF_KeyInfo*)
 *  Boolean lom_CheckIndexDesc(handle, Four, AttrInfo*, ColDesc*, IndexDesc*)
 */


#include "LOM_Internal.h"
#include "LOM.h"


/*
 * Function: Boolean lom_CheckAttrInfo(handle, Four, AttrInfo*)
 *
 * Description:
 *  Check if the given AttrInfo is validate.
 *
 * Returns:
 */
Boolean lom_CheckAttrInfo(
    LOM_Handle *handle, 
    Four nAttrs,			/* IN number of columns */
    AttrInfo *attrinfo)		/* IN column information */
{
    int i;
    
    for (i = 0; i < nAttrs; i++) {
		if (attrinfo[i].complexType != SM_COMPLEXTYPE_BASIC &&
			attrinfo[i].complexType != SM_COMPLEXTYPE_SET) return(SM_FALSE);
		
		switch (attrinfo[i].type) {
		  case SM_SHORT:
		  case SM_INT:
		  case SM_LONG:
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
			if (attrinfo[i].complexType != SM_COMPLEXTYPE_BASIC) return(SM_FALSE);
		  default:
			return(SM_FALSE);
		}
	}

	return(SM_TRUE);
		
} /* lom_CheckAttrInfo(handle, ) */




/*
 * Function: Boolean lom_CheckIndexDesc(handle, Four, AttrInfo*, ColDesc*, IndexDesc*)
 *
 * Description:
 *  Check if the given LRDS_IndexDesc is validate.
 *  Column informations are required to check LRDS_IndexDesc. The column
 *  information can be given either by AttrInfo structure or by ColDesc
 *  structure.
 *
 * Returns:
 *  SM_TRUE if the given LRDS_IndexDesc is correct
 *  SM_FALSE otherwise
 */
Boolean lom_CheckIndexDesc(
    LOM_Handle *handle, 
    Four nAttrs,			/* IN number of columns */
    AttrInfo *attrinfo,		/* IN column information using AttrInfo */
    ColDesc *cdesc,		/* IN column information using ColDesc */    
    LRDS_IndexDesc* idesc)	/* IN index descriptiron */
{
    Boolean result;

    switch (idesc->indexType) {
      case SM_INDEXTYPE_BTREE:
		result = lom_CheckKeyInfo(handle, nAttrs, attrinfo, cdesc, &idesc->kinfo.btree);
		break;
      case SM_INDEXTYPE_MLGF:
		result = lom_CheckMLGF_KeyInfo(handle, nAttrs, attrinfo, cdesc, &idesc->kinfo.mlgf);
		break;
      default:
		return(SM_FALSE);
    }

    return(result);
    
} /* lom_CheckIndexDesc(handle, ) */



/*
 * Function: Boolean lom_CheckKeyInfo(handle, Four, AttrInfo*, ColDesc*, KeyInfo*)
 *
 * Description:
 *  Check if the given KeyInfo is validate.
 *  Column informations are required to check KeyInfo. The column information
 *  can be given either by AttrInfo structure or by ColDesc structure.
 *
 * Returns:
 *  SM_TRUE if the given KeyInfo is correct
 *  SM_FALSE otherwise
 */
Boolean lom_CheckKeyInfo(
    LOM_Handle *handle, 
    Four nAttrs,			/* IN number of columns */
    AttrInfo *attrinfo,		/* IN column information using AttrInfo */
    ColDesc *cdesc,		/* IN column information using ColDesc */    
    KeyInfo* kinfo)		/* IN key information */
{
    Four i;
    Four keyLength;             /* maximum key length */

    
    if (kinfo->nColumns <= 0 || kinfo->nColumns > MAXNUMKEYPARTS) return(SM_FALSE);
    
    if (attrinfo != NULL) {
	
	for (keyLength = 0, i = 0; i < kinfo->nColumns; i++) {
	    if (kinfo->columns[i].colNo < 0 || kinfo->columns[i].colNo >= nAttrs) return(SM_FALSE);

	    /* SM_MBR cannot be an key part of Btree Index. */
	    if (attrinfo[kinfo->columns[i].colNo].type == SM_MBR) return(SM_FALSE);
	    
	    /* The following data types should consist in an index by itself. */
	    /* SM_COMPLEXTYPE_SET, SM_TEXT */
	    if ((attrinfo[kinfo->columns[i].colNo].complexType == SM_COMPLEXTYPE_SET ||
			attrinfo[kinfo->columns[i].colNo].type == SM_TEXT) && i > 0) 
			return(SM_FALSE);
	    
	    switch (attrinfo[kinfo->columns[i].colNo].type) {
	      case SM_VARSTRING:
			keyLength += sizeof(Two) + attrinfo[kinfo->columns[i].colNo].length;
			break;

	      case SM_TEXT:
			keyLength += sizeof(Two) + MAXKEYWORDLEN;
			break;
		
	      default:
			keyLength += attrinfo[kinfo->columns[i].colNo].length;
			break;
	    }
	}
    } else { /* should be cdesc != NULL */
	
	for (keyLength = 0, i = 0; i < kinfo->nColumns; i++) {
	    if (kinfo->columns[i].colNo < 0 || kinfo->columns[i].colNo >= nAttrs) return(SM_FALSE);

	    /* SM_MBR cannot be an key part of Btree Index. */
	    if (cdesc[kinfo->columns[i].colNo].type == SM_MBR) return(SM_FALSE);
	    
	    /* The following data types should consist in an index by itself. */
	    /* SM_COMPLEXTYPE_SET, SM_TEXT */
	    if ((cdesc[kinfo->columns[i].colNo].complexType == SM_COMPLEXTYPE_SET ||
			cdesc[kinfo->columns[i].colNo].type == SM_TEXT) && i > 0) 
			return(SM_FALSE);
	    
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
    return(SM_TRUE);
    
} /* lom_CheckKeyInfo(handle, ) */



/*
 * Function: Boolean lom_CheckMLGF_KeyInfo(handle, Four, AttrInfo*, ColDesc*, MLGF_KeyInfo*)
 *
 * Description:
 *  Check if the given MLGF_KeyInfo is validate.
 *  Column informations are required to check MLGF_KeyInfo. The column
 *  information can be given either by AttrInfo structure or by ColDesc
 *  structure.
 *
 * Returns:
 *  SM_TRUE if the given MLGF_KeyInfo is correct
 *  SM_FALSE otherwise
 */
Boolean lom_CheckMLGF_KeyInfo(
    LOM_Handle *handle, 
    Four nAttrs,			/* IN number of columns */
    AttrInfo *attrinfo,		/* IN column information using AttrInfo */
    ColDesc *cdesc,		/* IN column information using ColDesc */    
    MLGF_KeyInfo* kinfo)	/* IN key information */
{
    Four i;

    
    if (kinfo->nColumns <= 0 || kinfo->nColumns > nAttrs || kinfo->nColumns > MLGF_MAXNUM_KEYS)
	return(SM_FALSE);
    
    if (attrinfo != NULL) {
	
	for (i = 0; i < kinfo->nColumns; i++) {
	    if (kinfo->colNo[i] < 0 || kinfo->colNo[i] >= nAttrs) return(SM_FALSE);
	    
	    /* The following data types should consist in an index by itself. */
	    /* SM_COMPLEXTYPE_SET, SM_MBR */
	    if ((attrinfo[kinfo->colNo[i]].complexType == SM_COMPLEXTYPE_SET ||
		 attrinfo[kinfo->colNo[i]].type == SM_MBR) && i > 0)
		return(SM_FALSE);
	    
	    switch (attrinfo[kinfo->colNo[i]].type) {
	      case SM_SHORT:
	      case SM_INT:
	      case SM_LONG:
	      case SM_LONG_LONG:
	      case SM_MBR:
		/* do nothing */
		break;
		
	      default:
		return(SM_FALSE);
	    }
	}
    } else { /* should be cdesc != NULL */
	
	for (i = 0; i < kinfo->nColumns; i++) {
	    if (kinfo->colNo[i] < 0 || kinfo->colNo[i] >= nAttrs) return(SM_FALSE);

	    /* The following data types should consist in an index by itself. */
	    /* SM_COMPLEXTYPE_SET, SM_MBR */
	    if ((cdesc[kinfo->colNo[i]].complexType == SM_COMPLEXTYPE_SET ||
		 cdesc[kinfo->colNo[i]].type == SM_MBR) && i > 0)
		return(SM_FALSE);
	    
	    switch (cdesc[kinfo->colNo[i]].type) {
	      case SM_SHORT:
	      case SM_INT:
	      case SM_LONG:
	      case SM_LONG_LONG:
	      case SM_MBR:
		/* do nothing */
		break;
		
	      default:
		return(SM_FALSE);
	    }
	}
    }
    return(SM_TRUE);
    
} /* lom_CheckMLGF_KeyInfo(handle, ) */

