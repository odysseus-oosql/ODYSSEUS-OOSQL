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
 * Module: rdsm_Segment.c
 *
 * Description:
 *  Manipulate segments
 *
 * Exports:
 *  Four rdsm_GetPhysicalInfo(VolumeTable*, PageNo, Four*, Four*)
 */


#include <assert.h>
#include "common.h"
#include "trace.h"
#include "error.h"
#include "RDsM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


/*
 * Function: Four rdsm_GetPhysicalInfo(RDsM_VolumeInfo_T*, PageNo, Four*, Four*)
 *
 * Description:
 *  Given the ID of a train, get physical information about location of the train
 *
 * Returns:
 *  Error code
 */
Four rdsm_GetPhysicalInfo(
    Four 			handle,
    VolumeTable 	*v,                      /* IN  */
    PageNo       	pageNo,                  /* IN  */
    Four         	*devNo,                  /* OUT */
    Four         	*pageOffset)             /* OUT */
{
    Four         	idx;
    Four        	extNo;
    Four         	start, end, middle;


    /*
     *  parameter check
     */
    if (pageNo < 0 || pageNo >= v->numOfExts*v->sizeOfExt) ERR(handle, eBADPARAMETER);


    /*
     *  Calculate 'extNo' which contains given page
     */
    extNo = pageNo / v->sizeOfExt;


    /*
     *  find page segment which contains given page
     */

    /* initialize 'start' & 'end' */
    start = 0;
    end = v->numDevices - 1;

    /* assertion check */
    assert (DEVINFO_ARRAY(v->devInfo)[end].firstExtNo < v->numOfExts);

    /* binary search */
    while (start <= end) {

        /* get 'middle' */
        middle = (start + end) / 2;

        /* if segment is found, break */
        if (DEVINFO_ARRAY(v->devInfo)[middle].firstExtNo == extNo) break;

        /* update 'start' or 'end' */
        if (DEVINFO_ARRAY(v->devInfo)[middle].firstExtNo > extNo)
            end = middle - 1;
        else
            start = middle + 1;
    }


    /*
     *  Get 'idx' in which given page is located
     */
    if (start <= end) idx = middle;
    else              idx = end;


    /*
     *  Set return value
     */
    *devNo = idx;
    *pageOffset = pageNo - DEVINFO_ARRAY(v->devInfo)[idx].firstExtNo*v->sizeOfExt;
    assert(*pageOffset >= 0);


    return eNOERROR;
}


/*
 * Function: Four rdsm_GetPhysicalInfoForLogVolume(RDsM_VolumeInfo_T*, PageNo, Four*, Four*)
 *
 * Description:
 *  Given the ID of a train, get physical information about location of the train for log volume
 *
 * Returns:
 *  Error code
 */
Four rdsm_GetPhysicalInfoForLogVolume(
    Four 			handle,
    VolumeTable  	*v,                      /* IN  */
    PageNo       	pageNo,                  /* IN  */
    Four         	*devNo,                  /* OUT */
    Four         	*pageOffset)             /* OUT */
{
    Four         	idx;
    Four         	extNo;
    Four         	firstExtNoForLogVolume;
    Four         	start, end, middle;

    /*
     *  parameter check
     */
    if (pageNo < 0 ||
        pageNo >= (v->numOfExts-NUM_EXTS_FOR_MASTER_PAGES*v->numDevices)*v->sizeOfExt) ERR(handle, eBADPARAMETER);


    /*
     *  Calculate 'extNo' which contains given page
     */
    extNo = pageNo / v->sizeOfExt;


    /*
     *  find page segment which contains given page
     */

    /* initialize 'start' & 'end' */
    start = 0;
    end = v->numDevices - 1;

    /* assertion check */
    assert (DEVINFO_ARRAY(v->devInfo)[end].firstExtNo < v->numOfExts);

    /* binary search */
    while (start <= end) {

        /* get 'middle' */
        middle = (start + end) / 2;

        /* calculate 'firstExtNoForLogVolume' */
        firstExtNoForLogVolume = DEVINFO_ARRAY(v->devInfo)[middle].firstExtNo - middle*NUM_EXTS_FOR_MASTER_PAGES;

        /* if segment is found, break */
        if (firstExtNoForLogVolume == extNo) break;

        /* update 'start' or 'end' */
        if (firstExtNoForLogVolume > extNo)
            end = middle - 1;
        else
            start = middle + 1;
    }


    /*
     *  Get 'idx' in which given page is located
     */
    if (start <= end) idx = middle;
    else              idx = end;


    /*
     *  Set return value
     */

    /* calculate 'firstExtNoForLogVolume' */
	firstExtNoForLogVolume = DEVINFO_ARRAY(v->devInfo)[idx].firstExtNo - idx*NUM_EXTS_FOR_MASTER_PAGES; 

    /* set devNo */
    *devNo = idx;

    /* set pageOffset */
    *pageOffset = pageNo - (firstExtNoForLogVolume-NUM_EXTS_FOR_MASTER_PAGES)*v->sizeOfExt;
    assert(*pageOffset >= 0);


    return eNOERROR;
}
