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

#ifndef _OID_h_
#define _OID_h_


/*
    MODULE:
        OID.h

    DESCRIPTION:
        This header file defines OID structure.
*/

#if !defined(_BASICTYPES_H_) && !defined(__COSMOS_R_H__) && !defined(__DBLABLIB_H__)

typedef long PageNo;
typedef short VolID;
typedef unsigned long Unique;
typedef short SlotNo;

typedef struct {        /* OID is used accross the volumes */
        PageNo pageNo;      /* specify the page holding the object */
        VolID  volNo;       /* specify the volume in which object is in */
        SlotNo slotNo;      /* specify the slotNo within the page */
        Unique unique;      /* Unique No for checking dangling object */
        long   classID;        /* specify the class including the object */
} OID;

#endif


/************************************************
 * macro definitions manipulating OID structure *
 ************************************************/

/* check if two OIDs are equal */
#define OIDEQ(a,b)      ((a).unique == (b).unique && \
                         (a).classID == (b).classID && \
                         (a).pageNo  == (b).pageNo  && \
                         (a).volNo == (b).volNo && \
                         (a).slotNo  == (b).slotNo)

/* check if OID a is greater than OID b */
#define OIDGT(a,b)      ((a).volNo > (b).volNo || \
                        ((a).volNo == (b).volNo && (a).pageNo > (b).pageNo) || \
                        ((a).volNo == (b).volNo && (a).pageNo == (b).pageNo && \
                        (a).slotNo > (b).slotNo))

/* check if OID a is less than OID b */
#define OIDLT(a,b)      ((a).volNo < (b).volNo || \
                        ((a).volNo == (b).volNo && (a).pageNo < (b).pageNo) || \
                        ((a).volNo == (b).volNo && (a).pageNo == (b).pageNo && \
                        (a).slotNo < (b).slotNo)) 

/* make OID null */
#define OIDCLEAR(a)     ((a).pageNo = 0, \
                        (a).volNo = 0, \
                        (a).slotNo = 0, \
                        (a).classID = 0)

/* check if OID is null */
#define TESTOIDCLEAR(a) ((a).pageNo == 0 && \
                         (a).volNo == 0 && \
                         (a).slotNo == 0 && \
                         (a).classID == 0)

/* linearize OID for hash function */
#define LINEAROID(a)    ((a).volNo + (a).pageNo + (a).slotNo)

#define OIDEQPTR(a,b)   ((a)->unique == (b)->unique && \
                         (a)->classID == (b)->classID && \
                         (a)->pageNo  == (b)->pageNo  && \
                         (a)->volNo == (b)->volNo && \
                         (a)->slotNo  == (b)->slotNo)

/* make OID null by pointer */
#define OIDCLEARPTR(a)  ((a)->pageNo = 0, \
                        (a)->volNo = 0, \
                        (a)->slotNo = 0, \
                        (a)->classID = 0)

/* check if OID is null by pointer */
#define TESTOIDCLEARPTR(a) ((a)->pageNo == 0 && \
                            (a)->volNo == 0 && \
                            (a)->slotNo == 0 && \
                            (a)->classID == 0)

/* print the content of OID */
#define PRINTOID(a) printf("{DBID=%d, PageID=%d, slotNo=%d, classID=%d, unique=%d}",\
                    (a).volNo, (a).pageNo, (a).slotNo, (a).classID,\
                    (a).unique);

/* print the content of OID by pointer */
#define PRINTOIDPTR(a)  if ((a) == NULL) printf("NULL"); else\
                    printf("{DBID=%d, PageID=%d, slotNo=%d, classID=%d, unique=%d}",\
                    (a)->volNo, (a)->pageNo, (a)->slotNo, (a)->classID,\
                    (a)->unique);


#endif  
