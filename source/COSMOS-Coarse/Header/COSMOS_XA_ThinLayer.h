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
#ifndef _COSMOS_XA_THINLAYER_H_
#define _COSMOS_XA_THINLAYER_H_

#include <string.h>  /* for memcpy */
#include <stdlib.h>  /* for malloc, free */

#include "xa.h"


/*
 * Type Definition
 */


/* Global Transaction Status */
typedef enum { COSMOS_XA_UNINITIALIZED, COSMOS_XA_INITIALIZED } COSMOSXAopenStatus;
typedef enum { COSMOS_XA_NOTASSOCIATED, COSMOS_XA_ASSOCIATED, COSMOS_XA_RETRY } COSMOSXAassociationStatus;
typedef enum { COSMOS_XA_NONTRANSACTION, COSMOS_XA_ACTIVE, COSMOS_XA_IDLE, COSMOS_XA_PREPARED, COSMOS_XA_ROLLBACKONLY, COSMOS_XA_HEURISTICCOMPLETION, COSMOS_XA_RETRY_TRAN} COSMOSXAtranStatus;
typedef enum { COSMOS_XA_SCANSTARTED, COSMOS_XA_SCANENDED } COSMOSXAscanStatus;

typedef XID COSMOS_XA_XID;

/*
 * Transaction branch identification: XID and NULLXID:
 */



/*
 * Constant Variable Definition
 */

#define COSMOS_XA_OPENSTRINGHEADER      "COSMOS_XA"
#define COSMOS_XA_MAXOPENSTRINGLEN      1024

/*
 *Flag definitions for the RM switch
 */

#define COSMOS_XA_TMASYNC      0x80000000L        /* perform routine asynchronously */
#define COSMOS_XA_TMONEPHASE   0x40000000L        /* caller is using on-phase commit
                                                       optimisation */
#define COSMOS_XA_TMFAIL       0x20000000L        /* dissociates caller and marks
                                                       transaction branch rollback-only */
#define COSMOS_XA_TMNOWAIT     0x10000000L        /* return if blocking condition exists */
#define COSMOS_XA_TMRESUME     0x08000000L        /* caller is resuming association
                                                       with suspended transaction branch */
#define COSMOS_XA_TMSUCCESS    0x04000000L        /* dissociate caller from transaction branch*/
#define COSMOS_XA_TMSUSPEND    0x02000000L        /* caller is suspending, not ending, association */
#define COSMOS_XA_TMSTARTRSCAN 0x01000000L        /* start a recovery scan */
#define COSMOS_XA_TMENDRSCAN   0x00800000L        /* end a recovery scan */
#define COSMOS_XA_TMMULTIPLE   0x00400000L        /* wait for any asynchronous operation */
#define COSMOS_XA_TMJOIN       0x00200000L        /* caller is joining existing transaction branch */
#define COSMOS_XA_TMMIGRATE    0x00100000L        /* caller intends to perfrom migration */

/*
 *Flag definitions for the RM switch
 */

#define COSMOS_XA_TMNOFLAGS    0x00000000L        /* no resource manager features selected */
#define COSMOS_XA_TMREGISTER   0x00000001L        /* resource manager dynamically registers */
#define COSMOS_XA_TMNOMIGRATE  0x00000002L        /* resource manager does not support association migration */
#define COSMOS_XA_TMUSEASYNC   0x00000004L        /* resource manager supports asynchronous operations */


/*
 * Macro Definitions
 */

#define COSMOS_XA_RMOPENSTATUS          (cosmos_xa_rmOpenStatus)
#define COSMOS_XA_RMASSOCIATIONSTATUS   (cosmos_xa_rmAssociationStatus)
#define COSMOS_XA_RMTRANSTATUS          (cosmos_xa_rmTranStatus)


#define COSMOS_XA_SCANSTATUS            (cosmos_xa_scanStatus)
#define COSMOS_XA_PREPAREDNUM           (cosmos_xa_preparedNum)
#define COSMOS_XA_CURRENTPOS            (cosmos_xa_currentPos)
#define COSMOS_XA_PREPAREDLIST          (cosmos_xa_preparedList)

#define COSMOS_XA_VOLID                 (cosmos_xa_volId)


/*
 * Type Definition
 */





/*
** Interface Function Prototypes of COSMOS_XA_ThinLayer
*/

int COSMOS_XA_Commit(Four, XID*, int, long);
int COSMOS_XA_Forget(Four, XID *, int, long);
int COSMOS_XA_Prepare(Four, XID *, int, long);
int COSMOS_XA_Start(Four, XID *, int, long);
int COSMOS_XA_Complete(Four, int*, int *, int, long);
int COSMOS_XA_Open(Four, char*, int, long);
int COSMOS_XA_Recover(Four, XID *, long, int, long);
int COSMOS_XA_Close(Four, char *, int, long);
int COSMOS_XA_End(Four, XID *, int, long);
int COSMOS_XA_Rollback(Four, XID *, int, long);
int COSMOS_XA_AxReg(Four, int, XID * , long); 
int COSMOS_XA_AxUnreg(Four, int, long); 


/*
** Noninterface Function Prototypes of COSMOS_XA
*/

Four cosmos_xa_GetInitInfo(Four, char*, Four*, char***, char*);
Four cosmos_xa_ErrorConvert(Four, Four, int* );
            

#endif /* _COSMOS_XA_THINLAYER_H_ */
