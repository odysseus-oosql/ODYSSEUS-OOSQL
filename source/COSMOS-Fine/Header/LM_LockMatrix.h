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
#ifndef _LM_LOCKMATRIX_H_
#define _LM_LOCKMATRIX_H_

#define MAXLOCKMODES	6
#define NOTOK		0
#define OK		1
#define NONEED		2

static Two_Invariable LOCK_conversion[MAXLOCKMODES][MAXLOCKMODES]= {
  { L_NL,  L_IS,  L_IX,  L_S,   L_SIX, L_X },
  { L_IS,  L_IS,  L_IX,  L_S,   L_SIX, L_X },
  { L_IX,  L_IX,  L_IX,  L_SIX, L_SIX, L_X },
  { L_S,   L_S,   L_SIX, L_S,   L_SIX, L_X },
  { L_SIX, L_SIX, L_SIX, L_SIX, L_SIX, L_X },
  { L_X,   L_X,   L_X,   L_X,   L_X,   L_X }
};

static Two_Invariable LOCK_compatible[MAXLOCKMODES][MAXLOCKMODES]= {
  { OK,  OK,    OK,    OK,    OK,    OK    },
  { OK,  OK,    OK,    OK,    OK,    NOTOK },
  { OK,  OK,    OK,    NOTOK, NOTOK, NOTOK },
  { OK,  OK,    NOTOK, OK,    NOTOK, NOTOK },
  { OK,  OK,    NOTOK, NOTOK, NOTOK, NOTOK },
  { OK,  NOTOK, NOTOK, NOTOK, NOTOK, NOTOK }
};

static Two_Invariable LOCK_supreme[MAXLOCKMODES][MAXLOCKMODES] = {
  { L_NL,  L_IS,  L_IX,  L_S,   L_SIX, L_X },
  { L_IS,  L_IS,  L_IX,  L_S,   L_SIX, L_X },
  { L_IX,  L_IX,  L_IX,  L_SIX, L_SIX, L_X },
  { L_S,   L_S,   L_SIX, L_S,   L_SIX, L_X },
  { L_SIX, L_SIX, L_SIX, L_SIX, L_SIX, L_X },
  { L_X,   L_X,   L_X,   L_X,   L_X,   L_X }
};

static Two_Invariable LOCK_super[MAXLOCKMODES] = { L_NL,   L_S,   L_X,   L_S,   L_X,   L_X };

/* table for rule of lock hierachy */
/* left side is parent level and right side is child level */
/* NOTOK  :: child lock request violates lock request rule */
/* OK     :: child lock request matches lock request rule */
/* NONEED :: parent lock implicitly sets on its descendants */

static Two_Invariable LOCK_hierarchy[MAXLOCKMODES][MAXLOCKMODES] = {
  { NOTOK, NOTOK,  NOTOK,  NOTOK,  NOTOK,  NOTOK },
  { OK,    OK,     NOTOK,  OK,     NOTOK,  NOTOK },
  { OK,    OK,     OK,     OK,     OK,     OK },
  { OK,    NONEED, NOTOK,  NONEED, NOTOK,  NOTOK },
  { OK,    NONEED, OK,     NONEED, OK,     OK },
  { OK,    NONEED, NONEED, NONEED, NONEED, NONEED }
};

/* ----------------- BEGIN of per Process Section --------------------- */

/* Shared Memory Data Structures */

typedef struct LM_PerProcessDS_T_tag {
     Two_Invariable LOCK_conversion[MAXLOCKMODES][MAXLOCKMODES];
     Two_Invariable LOCK_compatible[MAXLOCKMODES][MAXLOCKMODES];
     Two_Invariable LOCK_supreme[MAXLOCKMODES][MAXLOCKMODES];
     Two_Invariable LOCK_super[MAXLOCKMODES];

/* table for rule of lock hierachy */
/* left side is parent level and right side is child level */
/* NOTOK  :: child lock request violates lock request rule */
/* OK     :: child lock request matches lock request rule */
/* NONEED :: parent lock implicitly sets on its descendants */

     Two_Invariable LOCK_hierarchy[MAXLOCKMODES][MAXLOCKMODES];

} LM_PerProcessDS_T;


/* ----------------- END of per Process Section --------------------- */

#endif /* _LM_LOCKMATRIX_H_ */
