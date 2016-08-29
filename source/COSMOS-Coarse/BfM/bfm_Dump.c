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
 * Module: bfm_Dump.c
 *
 * Description:
 *  dump the buffer table and hash table.
 *
 * Exports:
 *  void bfm_dump_buffertable(Four)
 *  void bfm_dump_hashtable(Four)
 */


#include "common.h"
#include "trace.h"
#include "BfM_Internal.h"
#include "perThreadDS_Internal.h"
#include "perProcessDS.h"


/*@ macor definition */
#define BUFT(i)	(BI_BUFTABLE_ENTRY(type,i))



/*@================================
 * bfm_dump_buffertable()
 *================================*/
/*
 * Function: void bfm_dump_buffertable(Four)
 *
 * Description:
 *  dump the buffer table.
 *
 * Returns:
 *  None
 */
void bfm_dump_buffertable(
    Four 	type)			/* IN buffer type */
{				
    Two         i;                      

    
    TR_PRINT(TR_BFM, TR1, ("bfm_dump_buffertable(handle, type=%lD)", type));
    
    
    printf("\n\t|==================================================|\n");
    printf("\t|                 Buffer Table                     |\n");
    printf("\t|-------------+-------------+-------------+--------|\n");
    printf("\t|%10s   |%10s   |%10s   |  bits  |\n", "volNo", "pageNo", "fixed");
    printf("\t|-------------+-------------+-------------+--------|\n");
    for( i = 0; i < BI_NBUFS(type); i++ ) 
        printf("\t|%10ld   |%10ld   |%10ld   |  0x%-2p  |\n", BUFT(i).key.volNo,
               BUFT(i).key.pageNo, BUFT(i).fixed, (CONSTANT_CASTING_TYPE)BUFT(i).bits ); 
    printf("\t|==================================================|\n");

} /* bfm_dump_buffertable() */



/*@================================
 * bfm_dump_hashtable()
 *================================*/
/*
 * Function: void bfm_dump_hashtable(Four)
 *
 * Description:
 *  dump the hash table.
 *
 * Returns:
 *  None
 */
void bfm_dump_hashtable(
    Four 		type)			/* IN buffer type */
{
    Two                  i;             
    Four                 j;             

    
    TR_PRINT(TR_BFM, TR1, ("bfm_dump_buffertable(handle, type=%lD)", type));
    
    
    printf("\n\t|=======================================================================================|\n");
    printf("\t|                                        Hash Table                                     |\n");
    printf("\t|=======|=======|=======|=======|=======|=======|=======|=======|=======|=======|=======|\n");
    printf("\t|       |    0  |    1  |    2  |    3  |    4  |    5  |    6  |    7  |    8  |    9  |\n");
    printf("\t|=======|=======|=======|=======|=======|=======|=======|=======|=======|=======|=======|\n");
    for( i = 0; i <= (CONSTANT_CASTING_TYPE)(HASHTABLESIZE(type)/10);i++ ) { 
        printf("\t|%5ld  |", i);
    	for( j = 0; j < 10; j++) 
	    if((i*10+j) < HASHTABLESIZE(type))
                printf("%5ld  |", BI_HASHTABLEENTRY(type,i*10+j)); 
	    else
		printf("       |");
        if(i < (CONSTANT_CASTING_TYPE)(HASHTABLESIZE(type)/10)) 
	    printf("\n\t|-------|-------|-------|-------|-------|-------|-------|-------|-------|-------|-------|\n");
    }
    printf("\n\t|=======|=======|=======|=======|=======|=======|=======|=======|=======|=======|=======|\n");
    
} /* bfm_dump_hashtable() */
