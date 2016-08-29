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
#include <string.h> /* for memcpy */

#include "common.h"
#include "trace.h"
#include "BfM.h"
#include "OM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"

Four om_GetTempSmallObject(Four, Object *, Object *, omSortKeyAttrInfo *, Four);
Four om_RestoreTempSmallObject(Four, Object *, Object *, omSortKeyAttrInfo *, Two);


Four om_GetTempObject(
    Four handle,
    Object*                obj,
    Object*                tmpObj,
    Four*                  tmpObjLen,
    SortKeyDesc*           sortKeyDesc,               /* IN  sort key */
    omGetKeyAttrsFuncPtr_T getKeyAttrs,
    void*                  schema)
{
    Four                   e;
    Two                    i;
    Two                    numVarAttrs;
    Two                    sortKeyLen;
    Four                   accumLen;
    Two                    varAttrLen;
    char*                  attrPtr;
    omSortKeyAttrInfo      attrInfo;


    /* get key attribute info */
    getKeyAttrs(obj, schema, sortKeyDesc, &attrInfo);

    /* calculate sort key length */
    e = om_CalculateSortKeyLengthFromAttrInfo(handle, &attrInfo, &sortKeyLen, &numVarAttrs);
    if (e < 0) ERR(handle, e);

    /* calculate length of tempObject */
    /* Note!! in case of small object, you must consider length field!! */
    if(obj->header.properties & P_LRGOBJ)
        *tmpObjLen = sizeof(ObjectHdr) + OFFSETARRAYSIZE(sortKeyDesc->nparts) + sortKeyLen + sizeof(ShortPageID);
    else
        *tmpObjLen = sizeof(ObjectHdr) + OFFSETARRAYSIZE(sortKeyDesc->nparts) + numVarAttrs*sizeof(Two) + obj->header.length;

    /* we can't handle this case!! */
    if(*tmpObjLen > LRGOBJ_THRESHOLD) ERR(handle, eTOOLARGESORTKEY_OM);


    /* 1. insert object's header to added object */
    /*    Note!! tmpObj's length field have length of original object */
    tmpObj->header.properties = obj->header.properties;
    tmpObj->header.tag = obj->header.tag;
    tmpObj->header.length = obj->header.length;


    /* 2. insert sort key */

    /* if object is large object */
    if( obj->header.properties & P_LRGOBJ ) {

        /* reset accumulate length of object's sort key */
        /* Note!! insert offset array before sort key    */
        accumLen = OFFSETARRAYSIZE(sortKeyDesc->nparts);

        /* insert sortKey to added object */
        for( i=0; i<attrInfo.nparts; i++ ) {

            /* in case of variable length attribute, you must add length field */
            if(IS_VARIABLE_FOR_SORT(attrInfo.parts[i].type)) {
                varAttrLen = attrInfo.parts[i].length;
                /* length field's type is Two!! */
                memcpy(&tmpObj->data[accumLen], &varAttrLen, sizeof(Two));
                accumLen += sizeof(Two);
            }

            /* insert attribute to added object */
            e = OM_ReadLargeObject(handle, obj, attrInfo.parts[i].offset, 
                                   attrInfo.parts[i].length, &tmpObj->data[accumLen]);
            if( e<0 ) ERR(handle, e);

            /* update accumulate length of sort key */
            accumLen += attrInfo.parts[i].length;
        }
    }
    /* if object is small object */
    else {

        /* reset accumulate length of object's sort key */
        /* Note!! insert offset array before sort key    */
        accumLen = OFFSETARRAYSIZE(sortKeyDesc->nparts);

        /* insert sortKey to added object */
        for( i=0; i<attrInfo.nparts; i++ ) {

            /* in case of variable length attribute, you must add length field */
            if(IS_VARIABLE_FOR_SORT(attrInfo.parts[i].type)) {
                varAttrLen = attrInfo.parts[i].length;
                /* length field's type is Two!! */
                memcpy(&tmpObj->data[accumLen], &varAttrLen, sizeof(Two));
                accumLen += sizeof(Two);
            }

            /* get pointer which points attribute */
            attrPtr = &(obj->data[attrInfo.parts[i].offset]);

            /* insert this attribute to added object */
            memcpy(&tmpObj->data[accumLen], attrPtr, attrInfo.parts[i].length);

            /* update accumulate length of sort key */
            accumLen += attrInfo.parts[i].length;
        }
    }


    /* 3. insert real object to added object and insert offset array if small object */

    if( obj->header.properties & P_LRGOBJ ) {
        memcpy(&tmpObj->data[accumLen], obj->data, sizeof(ShortPageID));
    }
    else {
        e = om_GetTempSmallObject(handle, tmpObj, obj, &attrInfo, accumLen);
        if( e<0 ) ERR(handle, e);
    }


    return eNOERROR;
}


Four om_RestoreTempObject(
    Four handle,
    Object*              obj,               /* IN */
    Object*              origObj,           /* OUT */
    omSortKeyAttrInfo*   sortAttrInfo,      /* IN  Note!! this attrInfo have not proper offset & variable length value */
    Four                 firstExtNo,        /* IN */
    Two                  sortEff,           /* IN */
    Boolean              copyLargeObjFlag)  /* IN */
{
    Four                 e;
    Four                 sortKeyLen;


    /* calculate length of sort key */
    e = sortKeyLen = om_CalculateSortKeyLengthFromSortKey(handle, obj, sortAttrInfo);
    if(e < 0) ERR(handle, e);

    /* set add object's header value */
    origObj->header.properties = obj->header.properties;
    origObj->header.tag = obj->header.tag;
    origObj->header.length = obj->header.length;

    /* if object is large object */
    if( obj->header.properties & P_LRGOBJ ) { 

        /* restore large object i.e. remove sort key from its head and copy large object tree if needed */
        e = om_RestoreLargeObject(handle, origObj, obj, OFFSETARRAYSIZE(sortAttrInfo->nparts)+sortKeyLen, firstExtNo, sortEff, copyLargeObjFlag);
        if( e<0 ) ERR(handle, e);
    }
    /* if object is small object */
    else {
        /* restore object i.e. remove sort key from its front part and insert them proper location */
        e = om_RestoreTempSmallObject(handle, origObj, obj, sortAttrInfo, sortKeyLen);
        if( e<0 ) ERR(handle, e);
    }


    return eNOERROR;
}




Four om_GetTempSmallObject(
    Four handle,
    Object*            tempObject,         /* OUT buffer which contains temp object */
    Object*            obj,                /* IN  buffer which contains original object */
    omSortKeyAttrInfo* attrInfo,           /* IN  attribute information of object */
    Four               startOffset)        /* IN  start offset to which object part is inserted */
{
    Two                i, j;               /* index variable */
    Two                attrNum;            /* number of key attribute for fast access */
    Two                tmpAttrNum;         /* number of key attribute for temporary usage */
    Two                keyAttrNumSortByOffset[MAXNUMKEYPARTS]; /* number of key attribute sorted by offset */
    Two                keyAttrOffsetArray[MAXNUMKEYPARTS];     /* offset of key attribute */
    Four               accumOffset;        /* accumOffset in tempObject */
    Four               objPartOffset;      /* offset of object's part in obj */
    Four               objPartLength;      /* length of object's part */


    /* I. get 'keyAttrNumSortByOffset' */

    /* initialize */
    for (i = 0; i < attrInfo->nparts; i++ ) {
        keyAttrNumSortByOffset[i] = i;
    }

    /* sort attribute number by offset order */
    for (i = 0; i < attrInfo->nparts; i++ ) {
        for ( j = i+1; j < attrInfo->nparts; j++ ) {
            if (attrInfo->parts[keyAttrNumSortByOffset[i]].offset > attrInfo->parts[keyAttrNumSortByOffset[j]].offset) {
                tmpAttrNum = keyAttrNumSortByOffset[i];
                keyAttrNumSortByOffset[i] = keyAttrNumSortByOffset[j];
                keyAttrNumSortByOffset[j] = tmpAttrNum;
            }
            else if (attrInfo->parts[keyAttrNumSortByOffset[i]].offset == attrInfo->parts[keyAttrNumSortByOffset[j]].offset) {
                if (attrInfo->parts[keyAttrNumSortByOffset[j]].length == 0) {
                    tmpAttrNum = keyAttrNumSortByOffset[i];
                    keyAttrNumSortByOffset[i] = keyAttrNumSortByOffset[j];
                    keyAttrNumSortByOffset[j] = tmpAttrNum;
                }
            }
        }
    }


    /* II. insert object part except key attributes to temp object and set keyAttrOffsetArray */

    /* initialize offset */
    accumOffset = startOffset;

    /* initialize objPartOffset */
    objPartOffset = 0;

    for (i = 0; i < attrInfo->nparts; i++ ) {

        /* get 'attrNum' */
        attrNum = keyAttrNumSortByOffset[i];

        /* calculate objPartLength */
        objPartLength = attrInfo->parts[attrNum].offset - objPartOffset;

        if(objPartLength > 0) {

            memcpy(&tempObject->data[accumOffset], &obj->data[objPartOffset], objPartLength);

            /* update offset */
            accumOffset += objPartLength;
        }

        /* update objPartOffset */
        objPartOffset += objPartLength + attrInfo->parts[attrNum].length;

        /* set keyAttrOffsetArray */
        keyAttrOffsetArray[attrNum] = attrInfo->parts[attrNum].offset;
    }
    if(objPartOffset < obj->header.length) {
        objPartLength = obj->header.length - objPartOffset;
        memcpy(&tempObject->data[accumOffset], &obj->data[objPartOffset], objPartLength);
    }


    /* III. insert keyAttrOffsetArray front of tempObject */
    memcpy(&tempObject->data[0], keyAttrOffsetArray, OFFSETARRAYSIZE(attrInfo->nparts));


    return(eNOERROR);
}


Four om_RestoreTempSmallObject(
    Four handle,
    Object*            obj,                /* OUT buffer which contains object */
    Object*            tempObject,         /* IN  buffer which contains temp object */
    omSortKeyAttrInfo* sortAttrInfo,       /* IN */
    Two                sortKeyLength)      /* IN  length of sort key */
{
    Two                i, j;               /* index variable */
    Two                varAttrLength;      /* length of variable length attribute */
    Four               accumOffset;        /* accumulated offset */
    Four               objPartOffset;      /* offset of object's part in 'tempObject' */
    Four               objPartLength;      /* length of object's part in tempObject */
    Two               offset[MAXNUMKEYPARTS];                 /* offset of each attribute in front key attributes */
    Two                length[MAXNUMKEYPARTS];                 /* length of each attribute in front key attributes */
    Two                attrNum;                                /* number of key attribute */
    Two                tmpAttrNum;                             /* number of attribute for temporary usage */
    Two                keyAttrNumSortByOffset[MAXNUMKEYPARTS]; /* number of key attribute sorted by offset */
    Two                keyAttrOffsetArray[MAXNUMKEYPARTS];     /* offset of key attribute in temp object */
    Two                oldKeyAttrOffset;                       /* previous offset of key attribute in temp object */
    Two                oldKeyAttrLength;                       /* previous length of key attribute in temp object */


    /* I. get 'offset' array & 'length' array from tempObject */

    /* initialize accumOffset */
    accumOffset = OFFSETARRAYSIZE(sortAttrInfo->nparts);

    for( i=0; i<sortAttrInfo->nparts; i++ ) {

        if(IS_VARIABLE_FOR_SORT(sortAttrInfo->parts[i].type)) {

            /* get variable attribute's length */
            memcpy(&varAttrLength, &(tempObject->data[accumOffset]), sizeof(Two));

            /* set offset & length */
            offset[i] = accumOffset + sizeof(Two);
            length[i] = varAttrLength;

            /* update accumOffset, Note!! you must remember length field */
            accumOffset += sizeof(Two) + varAttrLength;
        }
        else {
            /* set offset & length */
            offset[i] = accumOffset;
            length[i] = sortAttrInfo->parts[i].length;

            /* update accumOffset, Note!! you must remember length field */
            accumOffset += sortAttrInfo->parts[i].length;
        }
    }


    /* II. get keyAttrOffsetArray from tempObject */
    memcpy(keyAttrOffsetArray, &tempObject->data[0], OFFSETARRAYSIZE(sortAttrInfo->nparts));


    /* III. get 'keyAttrNumSortByOffset' */

    /* initialize */
    for( i=0; i<sortAttrInfo->nparts; i++ ) {
        keyAttrNumSortByOffset[i] = i;
    }

    /* sort attribute number by offset order */
    for( i=0; i<sortAttrInfo->nparts; i++ ) {
        for( j=i+1; j<sortAttrInfo->nparts; j++ ) {
            if(keyAttrOffsetArray[keyAttrNumSortByOffset[i]] > keyAttrOffsetArray[keyAttrNumSortByOffset[j]]) {
                tmpAttrNum = keyAttrNumSortByOffset[i];
                keyAttrNumSortByOffset[i] = keyAttrNumSortByOffset[j];
                keyAttrNumSortByOffset[j] = tmpAttrNum;
            }
            else if(keyAttrOffsetArray[keyAttrNumSortByOffset[i]] == keyAttrOffsetArray[keyAttrNumSortByOffset[j]]) {
                if(length[keyAttrNumSortByOffset[j]] == 0) {
                    tmpAttrNum = keyAttrNumSortByOffset[i];
                    keyAttrNumSortByOffset[i] = keyAttrNumSortByOffset[j];
                    keyAttrNumSortByOffset[j] = tmpAttrNum;
                }
            }
        }
    }


    /* IV. construct obj */

    /* initialize accumOffset */
    /* Note!! accumOffset is offset of 'obj' */
    accumOffset = 0;

    /* initialize objPartOffset */
    /* Note!! accumOffset is offset of 'tempObject' */
    objPartOffset = OFFSETARRAYSIZE(sortAttrInfo->nparts) + sortKeyLength;

    /* initialize oldKeyAttrOffset & oldKeyAttrLength */
    oldKeyAttrOffset = oldKeyAttrLength = 0;

    for( i=0; i<sortAttrInfo->nparts; i++ ) {

        /* get attrNum for fast access */
        attrNum = keyAttrNumSortByOffset[i];

        /* calculate objPartLength */
        objPartLength = keyAttrOffsetArray[attrNum] - oldKeyAttrOffset - oldKeyAttrLength;

        /* 1. copy object part from temp object */
        if(objPartLength > 0) {

            memcpy(&obj->data[accumOffset], &tempObject->data[objPartOffset], objPartLength);

            /* update objPartOffset */
            objPartOffset += objPartLength;

            /* update accumOffset */
            accumOffset += objPartLength;
        }

        /* 2. copy attribute from front sort key */
        if(length[attrNum] > 0) {

            memcpy(&obj->data[accumOffset], &tempObject->data[offset[attrNum]], length[attrNum]);

            /* update accumOffset */
            accumOffset += length[attrNum];
        }

        /* update oldKeyAttrOffset & oldKeyLength */
        oldKeyAttrOffset = keyAttrOffsetArray[attrNum];
        oldKeyAttrLength = length[attrNum];
    }
    if(accumOffset < obj->header.length) {
        objPartLength = obj->header.length - accumOffset;
        memcpy(&obj->data[accumOffset], &tempObject->data[objPartOffset], objPartLength);
    }


    return(eNOERROR);
}
