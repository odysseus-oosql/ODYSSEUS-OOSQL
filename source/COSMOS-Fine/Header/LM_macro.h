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
#ifndef _LM_MACRO_H_
#define _LM_MACRO_H_

#include <stdlib.h>

#define	XACTTABLE_HASH(k)	((k)->low % MAXXACTBUCKET)

#define LOCKTABLE_FILEID_HASH(k,temp) \
( (temp=(UFour)((k)->volNo + (k)->serial), rand_r(&temp)) % MAXLOCKHASHENTRY)

#define LOCKTABLE_PAGEID_HASH(k,temp) \
( (temp=(UFour)((k)->volNo + (k)->pageNo), rand_r(&temp)) % MAXLOCKHASHENTRY)

#define LOCKTABLE_OBJECTID_HASH(k,temp)\
	((temp=(UFour)((k)->volNo + (k)->pageNo + (k)->slotNo), rand_r(&temp)) % MAXLOCKHASHENTRY)

#define LOCKTABLE_KEYVALUE_HASH(k,temp) \
	LOCKTABLE_PAGEID_HASH(k,temp)

#define LOCKTABLE_LOCKID_HASH(k,temp,l) \
	((l == L_FILE)?LOCKTABLE_FILEID_HASH(&k.fileID,temp):((l == L_PAGE || l == L_FLAT_PAGE)?LOCKTABLE_PAGEID_HASH(&k.pageID,temp):((l == L_KEYVALUE)?LOCKTABLE_KEYVALUE_HASH(&k.keyValue,temp):((l == L_OBJECT || l == L_FLAT_OBJECT)?LOCKTABLE_OBJECTID_HASH(&k.objectID,temp):0))))

/* moved from LM_lock.c */
/* ADD_LOCK_HIERARCHY ::
   add lower lockbucket into the hierarchy tree of higher lockbucket */
#define ADD_LOCK_HIERARCHY(_handle, higher, lower)\
	ERROR_PASS(_handle, SHM_getLatch(_handle, &(higher)->latch, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL));\
	(lower)->prevSetLock = LOGICAL_PTR(NULL);\
	if ( !PHYSICAL_PTR((higher)->lowerLock) )\
	    (lower)->nextSetLock = LOGICAL_PTR(NULL);\
	else {\
	    (lower)->nextSetLock = (higher)->lowerLock;\
	    ((LockBucket_Type*)PHYSICAL_PTR((lower)->nextSetLock))->prevSetLock = LOGICAL_PTR(lower);\
	}\
	(higher)->lowerLock = LOGICAL_PTR(lower);\
	(lower)->higherLock = LOGICAL_PTR(higher);\
 	ERROR_PASS(_handle, SHM_releaseLatch(_handle, &(higher)->latch, procIndex));

#define LM_DISABLE_ACTION_FLAG(_handle)         (perThreadTable[_handle].lmDS.LM_actionFlag = FALSE)
#define LM_ENABLE_ACTION_FLAG(_handle)          (perThreadTable[_handle].lmDS.LM_actionFlag = TRUE)
#define LM_TEST_ACTION_FLAG_ON(_handle)         (perThreadTable[_handle].lmDS.LM_actionFlag)
#define LM_DISABLE_AUTO_ACTION_FLAG(_handle)    (perThreadTable[_handle].lmDS.LM_autoActionFlag = FALSE)
#define LM_ENABLE_AUTO_ACTION_FLAG(_handle)     (perThreadTable[_handle].lmDS.LM_autoActionFlag = TRUE)
#define LM_TEST_AUTO_ACTION_FLAG_ON(_handle)    (perThreadTable[_handle].lmDS.LM_autoActionFlag)

/* APPEND_REQUSETNODE_TO_DLIST
   :: append new node as the last entry of doubly linked list */

#define APPEND_TO_REQUESTNODE_DLIST(first, new)\
	{  RequestNode_Type *_lastRequest; \
	   (_lastRequest) = PHYSICAL_PTR(first); \
	   if(!(_lastRequest)) (first) = LOGICAL_PTR(new); \
	   else { \
	      while ((_lastRequest) && PHYSICAL_PTR((_lastRequest)->next)) \
	   	     (_lastRequest) = PHYSICAL_PTR((_lastRequest)->next); \
              \
	      (_lastRequest)->next = LOGICAL_PTR(new); \
	      (new)->prev = LOGICAL_PTR(_lastRequest); \
	    } \
	   (new)->next = LOGICAL_PTR(NULL); \
       }

#define ADD_INTO_XACTBUCKET_DLIST(HeadPtr, new) \
	if (PHYSICAL_PTR(HeadPtr)) ((XactBucket_Type*)PHYSICAL_PTR(HeadPtr))->prev = LOGICAL_PTR(new); \
	(new)->next = (HeadPtr);\
	(new)->prev = LOGICAL_PTR(NULL);\
	(HeadPtr) = LOGICAL_PTR(new);

#define ADD_INTO_LOCKBUCKET_DLIST(HeadPtr, new) \
	if (PHYSICAL_PTR(HeadPtr)) ((LockBucket_Type*)PHYSICAL_PTR(HeadPtr))->prev = LOGICAL_PTR(new); \
	(new)->next = (HeadPtr);\
	(new)->prev = LOGICAL_PTR(NULL);\
	(HeadPtr) = LOGICAL_PTR(new);

#define DELETE_FROM_REQUESTNODE_DLIST(headPtr, old)\
	if (PHYSICAL_PTR((old)->next)) ((RequestNode_Type*)PHYSICAL_PTR((old)->next))->prev = (old)->prev;\
	if (PHYSICAL_PTR((old)->prev)) ((RequestNode_Type*)PHYSICAL_PTR((old)->prev))->next = (old)->next;\
	if (PHYSICAL_PTR(headPtr) == (old)) (headPtr) = (old)->next;

#define DELETE_FROM_XACTBUCKET_DLIST(headPtr, old)\
	if (PHYSICAL_PTR((old)->next)) ((XactBucket_Type*)PHYSICAL_PTR((old)->next))->prev = (old)->prev;\
	if (PHYSICAL_PTR((old)->prev)) ((XactBucket_Type*)PHYSICAL_PTR((old)->prev))->next = (old)->next;\
	if (PHYSICAL_PTR(headPtr) == (old)) (headPtr) = (old)->next; \
	old->next = old->prev = LOGICAL_PTR(NULL);

#define DELETE_FROM_LOCKBUCKET_DLIST(headPtr, old)\
	if (PHYSICAL_PTR((old)->next)) ((LockBucket_Type*)PHYSICAL_PTR((old)->next))->prev = (old)->prev;\
	if (PHYSICAL_PTR((old)->prev)) ((LockBucket_Type*)PHYSICAL_PTR((old)->prev))->next = (old)->next;\
	if (PHYSICAL_PTR(headPtr) == (old)) (headPtr) = (old)->next;
	
#define ADD_INTO_REQUESTNODE_DLIST2(HeadPtr, new) \
	if (PHYSICAL_PTR(HeadPtr)) ((RequestNode_Type*)PHYSICAL_PTR(HeadPtr))->prevGrantedEntry = LOGICAL_PTR(new); \
	(new)->nextGrantedEntry = (HeadPtr);\
	(new)->prevGrantedEntry = LOGICAL_PTR(NULL);\
 	(HeadPtr) = LOGICAL_PTR(new);

#define DELETE_FROM_REQUESTNODE_DLIST2(headPtr, old)\
	if (PHYSICAL_PTR((old)->nextGrantedEntry)) \
	   ((RequestNode_Type*)PHYSICAL_PTR((old)->nextGrantedEntry))->prevGrantedEntry = (old)->prevGrantedEntry;\
	if (PHYSICAL_PTR((old)->prevGrantedEntry)) \
	   ((RequestNode_Type*)PHYSICAL_PTR((old)->prevGrantedEntry))->nextGrantedEntry = (old)->nextGrantedEntry;\
	if (PHYSICAL_PTR(headPtr) == (old)) \
	   (headPtr) = (old)->nextGrantedEntry;


/* WFG */
#define GET_NEWXACTBUCKET(_handle, x, id, cc)\
	e = Util_getElementFromPool(_handle, &LM_XACTBUCKETPOOL, &(x));\
	if ( e < eNOERROR ) ERR(_handle, e);\
	ASSIGN_XACTID((x)->xactID, (id));\
	(x)->status = X_NORMAL;\
	(x)->ccLevel = (cc);\
	(x)->maxLowLocks = 0;\
	(x)->waitingLock = LOGICAL_PTR(NULL);\
	(x)->waitingfor = LOGICAL_PTR(NULL);\
	for (i =0; i < N_LEVEL; i++) {\
	    (x)->nLock[i] = 0;\
	    (x)->nUnlock[i] = 0;\
	    (x)->grantedList[i] = LOGICAL_PTR(NULL);\
	    (x)->startAction[i] = LOGICAL_PTR(NULL);\
	}

#define FREE_XACTBUCKET(_handle, x)\
        e = Util_freeElementToPool(_handle, &LM_XACTBUCKETPOOL, (x));\
        if ( e < 0 ) ERR(_handle, e);

#define GET_NEWREQUESTNODE(_handle, p, xid, m, dur, st, xbucket, _e) \
	(_e) = Util_getElementFromPool(_handle, &LM_REQUESTNODEPOOL, &(p));\
	if ( (_e) >= 0 ){ \
	   ASSIGN_XACTID((p)->xactID, *(xid));\
	   (p)->counter = 1; \
      	   (p)->mode = (m); \
           (p)->oldMode = L_NL; \
	   (p)->duration = (dur);\
	   (p)->status = (st);\
	   (p)->xcbPtr = LOGICAL_PTR(MY_PROCENTRY(_handle));\
	   (p)->next = LOGICAL_PTR(NULL);\
	   (p)->prev = LOGICAL_PTR(NULL);\
	   (p)->higherRequestNode = LOGICAL_PTR(NULL);\
	   (p)->xactBucketPtr = LOGICAL_PTR(xbucket);\
	}

#define FREE_REQUESTNODE(_handle, p)\
        e = Util_freeElementToPool(_handle, &LM_REQUESTNODEPOOL, (p));\
        if ( e < eNOERROR ) ERRL1(_handle, e, &LM_LATCH);

#define SEARCH_FILELOCKBUCKET(link, fid, locate)\
	(locate) = NULL;\
	while ((link)) {\
	    if ( EQUAL_FILEID((link)->lockHDR->target.fileID, *(fid))) {\
		(locate) = (link)->lockHDR;\
		break;\
	    }\
	    (link) = (link)->next;\
	}

#define SEARCH_PAGELOCKBUCKET(link, oid, locate)\
	(locate) = NULL;\
	while ((link)) {\
	    if (EQUAL_PAGEID((link)->lockHDR->target.pageID, *(oid))){\
		(locate) = (link)->lockHDR;\
		break;\
	    }\
	    (link) = (link)->next;\
	}

#define SEARCH_REQUESTNODEOFFILE(head, targetID, nodeptr)\
	nodeptr = PHYSICAL_PTR(head);\
	while ( nodeptr && !EQUAL_FILEID(((LockBucket_Type*)PHYSICAL_PTR(nodeptr->lockHDR))->target.fileID, *targetID) )\
	nodeptr = PHYSICAL_PTR(nodeptr->nextGrantedEntry);

#define SEARCH_REQUESTNODEOFPAGE(head, targetID, nodeptr)\
	nodeptr = PHYSICAL_PTR(head);\
	while ( nodeptr && !EQUAL_PAGEID(((LockBucket_Type*)PHYSICAL_PTR(nodeptr->lockHDR))->target.pageID, *targetID) )\
	nodeptr = PHYSICAL_PTR(nodeptr->nextGrantedEntry);

#define SEARCH_REQUESTNODEOFKEYVALUE(head, targetID, nodeptr)\
	nodeptr = PHYSICAL_PTR(head);\
	while ( nodeptr && !EQUAL_KEYVALUELOCKID(((LockBucket_Type*)PHYSICAL_PTR(nodeptr->lockHDR))->target.keyValue, *targetID) )\
	nodeptr = PHYSICAL_PTR(nodeptr->nextGrantedEntry);

#define SEARCH_REQUESTNODEOFOBJECT(head, targetID, nodeptr)\
	nodeptr = PHYSICAL_PTR(head);\
	while ( nodeptr && !EQUAL_OBJECTID(((LockBucket_Type*)PHYSICAL_PTR(nodeptr->lockHDR))->target.objectID, *targetID) )\
	nodeptr = PHYSICAL_PTR(nodeptr->nextGrantedEntry);

#define INSTANT_DURATION_HANDLING(_handle, latch, lm_latch, lockCounter, reply) \
	{\
	    e = SHM_releaseLatch(_handle, &latch, procIndex);\
	    if (e < 0) ERRL1(_handle, e, &lm_latch);\
	    (lockCounter)++;\
	    \
	    e = SHM_releaseLatch(_handle, &lm_latch, procIndex);\
	    if (e < 0) ERR(_handle, e);\
	    \
	    (reply) = (LockReply)mode;\
	    return(eNOERROR);\
	}

#define CONDITIONAL_LOCK_HANDLING(_handle, latch, lm_latch, reply) \
	{\
	    e = SHM_releaseLatch(_handle, &latch, procIndex);\
	    if (e < 0) ERRL1(_handle, e, &lm_latch);\
	    \
	    e = SHM_releaseLatch(_handle, &lm_latch, procIndex);\
	    if (e < 0) ERR(_handle, e);\
	    \
	    (reply) = LR_NOTOK;\
	    return(eNOERROR);\
	}

#define EQUAL_KEYVALUELOCKID(x,y) \
	EQUAL_PAGEID(x,y)

#define EQUAL_TARGETID(x, y, _level) \
	((_level == L_FILE)?EQUAL_FILEID(x.fileID, y->fileID):((_level == L_PAGE || _level == L_FLAT_PAGE)?EQUAL_PAGEID(x.pageID, y->pageID):((_level == L_OBJECT || _level == L_FLAT_OBJECT)?EQUAL_OBJECTID(x.objectID, y->objectID):EQUAL_KEYVALUELOCKID(x.keyValue, y->keyValue))))

#define ROOT_KEY_VALUE  100000
#define GET_NUM_CIPHER(radix) log10(ROOT_KEY_VALUE-1)/log10(radix)
#define KEY_DIGIT(k, radix, cipher)  ((Four)(k/pow(radix, cipher)) % radix)
#define GET_KEY_VALUE(k, radix, cipher) (k - (k % (Four)(pow(radix, cipher))))
#define GET_NEXT_KEY_VALUE(k, radix, cipher) (k + pow(radix, cipher))
#define EQUAL_KEY_DIGIT(k1, k2, radix, cipher) (KEY_DIGIT(k1, radix, cipher) == KEY_DIGIT(k2, radix, cipher))

#define DIFFERENCE_BETWEEN_KEY_VALUES(k1, k2)    (k2 - k1)
#define NEED_PARENT_LOCK(l, u, radix, cipher) (DIFFERENCE_BETWEEN_KEY_VALUES(KEY_DIGIT(l, radix, cipher), KEY_DIGIT(u, radix, cipher)) > (radix / 2))


#define LM_INIT_MAX_LOCKS_ON_FILE  100 

#endif /* _LM_MACRO_H_ */

