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
 * Module: OM_GetKeyOIDSortStream.c
 *
 * Description:
 *  Get key,oid sort stream for index bulk load.
 *
 * Exports:
 *  Four OM_GetKeyOIDSortStream(VolID, ObjectID*, Two, SortKeyDesc*, omGetKeyAttrsFuncPtr_T,
 *                                   void*, PageID*, Four*)
 *
 */

#include <stdlib.h> /* for malloc & free */
#include <string.h>
#include "common.h"
#include "param.h"
#include "bl_param.h"
#include "RDsM_Internal.h"
#include "BfM.h"           
#include "OM_Internal.h"
#include "BL_OM_Internal.h"
#include "Util_Sort.h"
#include "perThreadDS.h"
#include "perProcessDS.h"

/*@========================================
 *  OM_GetKeyOIDSortStream()
 * =======================================*/

/*
 * Function : Four OM_GetKeyOIDSortStream(VolID, ObjectID*, Two, SortKeyDesc*, omGetKeyAttrsFuncPtr_T,
 *                                             void*, PageID*, Four*)
 *
 * Description :
 *  Get key,oid sort stream for index bulk load.
 *
 * Return Values :
 *  error code.
 *
 * Side Effects :
 *  0)
 *
 */

Four OM_GetKeyOIDSortStream(
    Four handle,
    VolID       				tmpVolId,                  	/* IN  temporary volume ID in which sort stream is created */
    ObjectID    				*catObjForFile,            	/* IN  file in which data bulk load is to be processed */
    Two         				numIndex,                  	/* IN  number of index */
    SortKeyDesc 				*kdesc,                    	/* IN  sort key description */
    omGetKeyAttrsFuncPtr_T   	getKeyAttrs,  	   			/* IN  object analysis function */
    void        				*schema,                   	/* IN  schema for analysis function */
    PageID      				*firstPageID,              	/* IN  first page id */ 
    Four        				*keyOIDSortStreamID)       	/* OUT key-oid sort stream ID */
{

    Four 		 			e;                /* error number */
    Two                 	i;                /* index variable */
    Two                 	j;                /* index variable */
    Two                 	k;                /* index variable */
    SortStreamTuple     	sortTuple;        /* tuple for sort stream */
    SortTupleDesc       	sortTupleDesc;    /* key description of sort stream */
    omSortKeyAttrInfo   	sortAttrInfo;     /* information about sort key attributes */
    sm_CatOverlayForData 	*catEntry;        /* pointer to data file catalog information */
    SlottedPage          	*catPage;         /* pointer to buffer containing the catalog */
    PageID               	nextPage;         /* next page ID to read */
    Four                 	offset;           /* start offset of object in data area */
    Object               	*obj;             /* object for sorting */
    char                 	tmpForm[20480];   /* temporary form for sorting */
    ObjectID             	oid;              /* object id which sorted */
    Two                  	numVarAttrs;      /* number of variable length column in the relation */
    Two                  	sortKeyLen;       /* length of sort key of the object */
    Four                 	accumLen;         /* accumulate length of object's sort key */
    Two                  	varAttrLen;       /* length of variable column of the object */
    char*                	attrPtr;          /* pointer which points attribute */
    SlottedPage          	*dataFileBuffer;  /* buffer which contains objects for bulk load in slotted page format*/
    PhysicalFileID       	pFid;             /* physical ID of file */ 


    /*@ 
	 * parameter checking 
	 */

    if (catObjForFile == NULL) ERR(handle, eBADPARAMETER);

    if (kdesc == NULL || getKeyAttrs == NULL ) ERR(handle, eBADPARAMETER); 


    /*
     * Initailze the main memory data structure used in Data File Bulk Load
     */

    dataFileBuffer     = (SlottedPage *) malloc(PAGESIZE); 

    /*@ Get the file's ID */
    e = BfM_GetTrain(handle, (TrainID*)catObjForFile, (char**)&catPage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    GET_PTR_TO_CATENTRY_FOR_DATA(catObjForFile, catPage, catEntry);
    MAKE_PHYSICALFILEID(pFid, catEntry->fid.volNo, catEntry->firstPage); 

    e = BfM_FreeTrain(handle, (TrainID*)catObjForFile, PAGE_BUF);
    if (e < 0) ERR(handle, e);


    /*
     *   open sort stream 
     */

    for(i = 0; i < numIndex; i++) {

        /*  Get 'sortAttrinfo' & 'sortTupleDesc' */

        /* Note!! sortAttrInfo doesn't have proper offset value */
        getKeyAttrs(NULL, schema, &(kdesc[i]), &sortAttrInfo);

        /* set 'nparts' */
        sortTupleDesc.nparts = sortAttrInfo.nparts;

        /* set 'hdrSize' */
        sortTupleDesc.hdrSize = 0;

        /* set each part information */
        for (k = 0; k < sortAttrInfo.nparts; k++ ) {
            sortTupleDesc.parts[k].type = sortAttrInfo.parts[k].type;
            sortTupleDesc.parts[k].length = sortAttrInfo.parts[k].length;
            sortTupleDesc.parts[k].flag = kdesc->parts[k].flag;
        }
    
        /* append OID */
        sortTupleDesc.nparts++; 

        sortTupleDesc.parts[k].type   = SM_OBJECT_ID;
        sortTupleDesc.parts[k].length = SM_OBJECT_ID_SIZE;
        sortTupleDesc.parts[k].flag   = SORTKEYDESC_ATTR_ASC;

        /*  Open sort stream */
        keyOIDSortStreamID[i] = Util_OpenSortStream(handle, tmpVolId, &sortTupleDesc);
        if (keyOIDSortStreamID[i] < 0) ERR(handle, keyOIDSortStreamID[i]);
    }


    /*
     *   insert (key,oid) pair into sort stream 
     */

    nextPage = pFid; 

    if (firstPageID != NULL) {
        nextPage.pageNo = firstPageID->pageNo;
        nextPage.volNo  = firstPageID->volNo;
    }
    
    /* read each object in the data file */
    while(nextPage.pageNo != NIL) {
        
        e = RDsM_ReadTrain(handle, &nextPage, (char*)dataFileBuffer, PAGESIZE2 );
        if( e < 0 ) ERR(handle,  e );

        oid.volNo  = dataFileBuffer->header.pid.volNo;
        oid.pageNo = dataFileBuffer->header.pid.pageNo;

        for (j = 0; j < dataFileBuffer->header.nSlots; j++) {
            
            if (dataFileBuffer->slot[-j].offset != EMPTYSLOT) {
                    
                /* initialize variable */ 
 
                offset = dataFileBuffer->slot[-j].offset;
                obj = (Object *) &(dataFileBuffer->data[offset]);

                oid.slotNo = j;
                oid.unique = dataFileBuffer->slot[-j].unique;

                for(i = 0; i < numIndex; i++) {

                    /* get key attribute info */
                    getKeyAttrs(obj, schema, &(kdesc[i]), &sortAttrInfo);

                    /* calculate sort key length */
                    e = om_CalculateSortKeyLengthFromAttrInfo(handle, &sortAttrInfo, &sortKeyLen, &numVarAttrs);
                    if (e < 0) ERR(handle, e);

		    		/* we can't handle this case!! (The reason is explained in detail */
                    if( sortKeyLen > LRGOBJ_THRESHOLD) ERR(handle, eTOOLARGESORTKEY_OM);
                   
                    /* insert sort key */

                    /* if object is large object */
                    if( obj->header.properties & P_LRGOBJ ) {
                           
                        /* reset accumulate length of object's sort key */
                        accumLen = 0;

                        /* insert sortKey to added object */ 
                        for( k=0; k<sortAttrInfo.nparts; k++ ) {
                        
                            /* in case of variable length attribute, you must add length field */
                            if(IS_VARIABLE_FOR_SORT(sortAttrInfo.parts[k].type)) {
                                varAttrLen = sortAttrInfo.parts[k].length;
                                /* length field's type is Two!! */
                                memcpy(&(tmpForm[accumLen]), &varAttrLen, sizeof(Two));
                                accumLen += sizeof(Two);
                            }

                            /* insert attribute to added object */
                            e = OM_ReadLargeObject(handle, obj, sortAttrInfo.parts[k].offset,
                                                  sortAttrInfo.parts[k].length, &(tmpForm[accumLen]));
                            if( e<0 ) ERR(handle, e);

                            /* update accumulate length of sort key */
                            accumLen += sortAttrInfo.parts[k].length;
                        
                        }
                    }
                    /* if object is small object */
                    else {
                    
                        /* reset accumulate length of object's sort key */
                        accumLen = 0;

                        /* insert sortKey to added object */
                        for( k=0; k<sortAttrInfo.nparts; k++ ) {
							
                        
                            /* in case of variable length attribute, you must add length field */
                            if(IS_VARIABLE_FOR_SORT(sortAttrInfo.parts[k].type)) {
                                varAttrLen = sortAttrInfo.parts[k].length;
                                /* length field's type is Two!! */
                                memcpy( &(tmpForm[accumLen]), &varAttrLen, sizeof(Two));
                                accumLen += sizeof(Two);
                            }

                            /* get pointer which points attribute */
                            attrPtr = &(obj->data[sortAttrInfo.parts[k].offset]); 

                            /* insert this attribute to added object */
                            memcpy(&(tmpForm[accumLen]), attrPtr, sortAttrInfo.parts[k].length);

                            /* update accumulate length of sort key */
                            accumLen += sortAttrInfo.parts[k].length;
                        }
                    }

                    /* append OID */
                    memcpy(&(tmpForm[accumLen]), ((char*)&oid), sizeof(ObjectID));
                    accumLen += sizeof(ObjectID);

                    /* set sortStreamTuple */
                    sortTuple.len  = accumLen;
                    sortTuple.data = tmpForm;

                    /* insert into sort stream */
                    Util_PutTuplesIntoSortStream(handle, keyOIDSortStreamID[i], 1, &sortTuple);
                    
                } /* end of for i */
            } /* end of if */
        } /* end of for j */

        /* Next page */
        nextPage.pageNo = dataFileBuffer->header.nextPage;

    } /* end of while */


    /*
     * sorting 
     */

    for(i = 0; i < numIndex; i++) {
        e = Util_SortingSortStream(handle, keyOIDSortStreamID[i]);
        if (e < 0) ERR(handle, e);
    }  

    /*
     * Finalize 
     */

    free(dataFileBuffer);

    return(eNOERROR);

} /* OM_GetKeyOIDSortStream() */
