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
 * Function: Redo_OM_InsertIntoSmallObject.c
 *
 * Description:
 *  redo inserting some data into a small object
 *
 * Exports:
 *  Four Redo_OM_InsertIntoSmallObject(Four, SlottedPage*, LOG_LogRecInfo_T*)
 */


#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "OM.h"
#include "LOG.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


Four Redo_OM_InsertIntoSmallObject(
    Four handle,
    void *anyPage,		/* OUT updated page */
    LOG_LogRecInfo_T *logRecInfo) /* IN log record information */
{
    SlottedPage *aPage = anyPage;
    Four e;                     /* error code */
    Object *obj;                /* points to the updated object */
    LOG_Image_OM_ObjDataInPage_T *objDataInfoPtr; /* specify some portion of an object data */
    Four alignedOrigLen;	/* aligned length of original length */
    Four alignedNewLen;         /* aligned length of new length */


    TR_PRINT(handle, TR_REDO, TR1, ("Redo_OM_InsertIntoSmallObject(aPage=%P, logRecInfo=%P)", aPage, logRecInfo));


    /*
     *	check input parameter
     */
    if (aPage == NULL || logRecInfo == NULL) ERR(handle, eBADPARAMETER);


    /* get the images */
    objDataInfoPtr = (LOG_Image_OM_ObjDataInPage_T*)logRecInfo->imageData[0];


    /* Points the object. */
    obj = (Object*)&(aPage->data[aPage->slot[-(objDataInfoPtr->slotNo)].offset]);

    alignedOrigLen = MAX(MIN_OBJECT_DATA_SIZE, ALIGNED_LENGTH(obj->header.length));
    alignedNewLen = MAX(MIN_OBJECT_DATA_SIZE, ALIGNED_LENGTH(obj->header.length+objDataInfoPtr->length));

    /* Change the size of the object data part */
    e = om_ChangeObjectSize(handle, &logRecInfo->xactId, aPage, objDataInfoPtr->slotNo, alignedOrigLen, alignedNewLen, FALSE);
    if (e < eNOERROR) ERR(handle, e);

    /* In om_ChangeObjectSize( ), obj may be moved to someplace. */
    obj = (Object*)&(aPage->data[aPage->slot[-(objDataInfoPtr->slotNo)].offset]);

    /*
     *	redo inserting some data into a small object
     */
    /* make room by moving some data */
    memmove(&(obj->data[objDataInfoPtr->start + objDataInfoPtr->length]),
            &(obj->data[objDataInfoPtr->start]),
            obj->header.length - objDataInfoPtr->start);

    /* insert the data */
    memcpy(&(obj->data[objDataInfoPtr->start]), logRecInfo->imageData[1], objDataInfoPtr->length);

    /* increment the object length */
    obj->header.length += objDataInfoPtr->length;


    return(eNOERROR);

} /* Redo_OM_InsertIntoSmallObject( ) */
