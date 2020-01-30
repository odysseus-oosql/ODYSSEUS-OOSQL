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
/******************************************************************************/
/*                                                                            */
/*    This module has been implemented based on "The Multilevel Grid File     */
/*    (MLGF) Version 4.0," which can be downloaded at                         */
/*    "http://dblab.kaist.ac.kr/Open-Software/MLGF/main.html".                */
/*                                                                            */
/******************************************************************************/

#include "common.h"
#include "trace.h"
#include "error.h"
#include "TM.h"
#include "MLGF.h"
#include "perProcessDS.h"
#include "perThreadDS.h"

Four MLGF_CreateIndex(
    Four 		handle,
    XactTableEntry_T 	*xactEntry, 	/* IN transaction table entry */
    IndexID		*iid,		/* IN allocated index ID for new MLGF */
    MLGF_KeyDesc 	*kdesc,		/* IN key descriptor of the new MLGF index */
    PageID 		*rootPid,	/* OUT PageID of root of newly created index */
    LogParameter_T 	*logParam)   	/* IN log parameter */
{
    printf("MLGF_CreateIndex() not implemented\n");
    return(eNOERROR);
}


Four MLGF_DeleteObject(
    Four 		handle,
    PageID 		*rootPid,	/* IN PageID of the root page */
    MLGF_KeyDesc 	*kdesc,		/* IN key descriptor for MLGF index */
    MLGF_HashValue 	*keys,		/* IN hash values of keys */
    ObjectID 		*oid,		/* IN object to delete */
    LockParameter 	*lockup,      	/* IN request lock or not */
    LogParameter_T 	*logParam) 	/* IN log parameter */
{
    printf("MLGF_DeleteObject() not implemented\n");
    return(eNOERROR);
}


Four MLGF_DropIndex(
    Four 		handle,
    XactTableEntry_T 	*xactEntry, 	/* IN transaction table entry */
    PageID 		*rootPid,	/* IN root page of the dropped MLGF index */
    Boolean 		immediateFlag,  /* IN TRUE if drop immediately */
    LogParameter_T 	*logParam) 	/* IN log parameter */
{
    printf("MLGF_DropIndex() not implemented\n");
    return(eNOERROR);
}



Four MLGF_Fetch(
    Four 		handle,
    PageID 		*rootPid,	/* IN PageID of the root page */
    MLGF_KeyDesc 	*kdesc,		/* IN key descriptor of this index */
    MLGF_HashValue 	*lowerBound,	/* IN lower bound of region to fetch */
    MLGF_HashValue 	*upperBound,	/* IN upper bound of region to fetch */
    MLGF_Cursor 	*cursor,	/* OUT return the position of fetched object */
    char 		*data,		/* OUT return the extra data */
    LockParameter 	*lockup)      	/* IN request lock or not */
{
    printf("MLGF_Fetch() not implemented\n");
    return(eNOERROR);
}



Four MLGF_FetchNext(
    Four 		handle,
    PageID 		*rootPid,	/* IN PageID of the root page */
    MLGF_KeyDesc 	*kdesc,		/* IN key descriptor of this index */
    MLGF_HashValue 	*lowerBound,	/* IN lower bound of region to fetch */
    MLGF_HashValue 	*upperBound,	/* IN upper bound of region to fetch */
    MLGF_Cursor 	*cursor,	/* INOUT return the position of fetched object */
    char 		*data,		/* OUT return the extra data */
    LockParameter 	*lockup)      	/* IN request lock or not */
{
    printf("MLGF_FetchNext() not implemented\n");
    return(eNOERROR);
}



Four MLGF_InitSharedDS()
{
    printf("MLGF_InitSharedDS() not implemented\n");
    return(eNOERROR);
}


Four MLGF_InitLocalDS()
{
    printf("MLGF_InitLocalDS() not implemented\n");
    return(eNOERROR);
}



Four MLGF_InsertObject(
    Four 		handle,
    XactTableEntry_T 	*xactEntry, 	/* IN transaction table entry */
    PageID 		*rootPid,	/* IN root page of MLGF */
    MLGF_KeyDesc 	*kdesc,		/* IN key descriptor of MLGF */
    MLGF_HashValue 	*keys,		/* IN hash values of keys */
    ObjectID 		*oid,		/* IN Object to insert */
    char 		*data,		/* IN additional data to store */
    LockParameter 	*lockup,      	/* IN request lock or not */
    LogParameter_T 	*logParam) 	/* IN log parameter */
{
    printf("MLGF_InsertObject() not implemented\n");
    return(eNOERROR);
}


Four MLGF_SearchNearObject(
    Four 		handle,
    PageID        	*rootPid,	/* IN root of a MLGF index */
    MLGF_KeyDesc   	*kdesc,		/* IN key descriptor of a MLGF index */
    MLGF_HashValue 	keys[],		/* IN hash values of the new object */
    ObjectID       	*oid,		/* OUT found near object */
    LockParameter 	*lockup)      	/* IN request lock or not */
{
    printf("MLGF_SearchNearObject() not implemented\n");
    return(eNOERROR);
}
