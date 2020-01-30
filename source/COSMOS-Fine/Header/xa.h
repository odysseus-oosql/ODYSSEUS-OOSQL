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
 * Start of xa.h header
 *
 * Define a symbol to prevent multiple inclusions of this header file
 */

#ifndef XA_H
#define XA_H

/*
 * Transaction branch identification: XID and NULLXID:
 */
#ifndef	XIDDATASIZE		      /* guard against redefinition in tx.h */

#define XIDDATASIZE   128         /* size in bytes */
#define	MAXGTRIDSIZE  64	      /* maximum size in bytes of gtrid */
#define	MAXBQUALSIZE  64	      /* maximum size in bytes of bqual */
struct xid_t {
	long formatID;                      /* format identifier */
	long gtrid_length;                  /* value from 1 through 64 */
	long bqual_length;                  /* value from 1 through 64 */
	char data[XIDDATASIZE];
};
typedef struct xid_t XID;

/*
 * A value of -1 in formatID means that the XID is null.
 */

#endif /* XIDDATASIZE */

/*
 * Declarations of routines by which RMs call TMs:
 */
#if	__STDC__
extern int ax_reg(int, XID *, long);
extern int ax_unreg(int, long);
#else	/* ifndef __STDC__ */
extern int ax_reg();
extern int ax_unreg();
#endif	/* ifndef __STDC__ */

/*
 * XA Switch Data Structure
 */
#define RMNAMESZ 32                   /* length of resource manager name,
										 including the null terminator */
#define	MAXINFOSIZE 256		      	  /* maximum size in bytes of xa_info strings,
										 including the null terminator */

struct xa_switch_t {
  char name[RMNAMESZ];                /* name of resource manager */
  long flags;                         /* resource manager specific options */
  long version;                       /* must be 0 */
  int (*xa_open_entry) (char *, int, long);		/* xa_open function pointer */
  int (*xa_close_entry) (char *, int, long);	/* xa_close function pointer*/
  int (*xa_start_entry) (XID *, int, long);		/* xa_start function pointer */
  int (*xa_end_entry) (XID *, int, long);		/* xa_end function pointer */
  int (*xa_rollback_entry) (XID *, int, long);	/* xa_rollback function pointer */
  int (*xa_prepare_entry) (XID *, int, long);	/* xa_prepare function pointer */
  int (*xa_commit_entry) (XID *, int, long);	/* xa_commit function pointer */
  int (*xa_recover_entry) (XID *, long, int, long);
                                       			/* xa_recover function pointer*/
  int (*xa_forget_entry) (XID *, int, long);	/* xa_forget function pointer */
  int (*xa_complete_entry) (int *, int *, int, long);
                                       			/* xa_complete function pointer */
};

/*
 *Flag definitions for the RM switch
 */
#define TMNOFLAGS    0x00000000L       /* no resource manager features selected */
#define TMREGISTER   0x00000001L       /* resource manager dynamically registers */
#define TMNOMIGRATE  0x00000002L       /* resource manager does not support
                                       association migration */
#define TMUSEASYNC   0x00000004L       /* resource manager supports
                                       asynchronous operations */
/*
 * Flag definitions for xa_and ax_ routines
 */
/* use TMNOFLAGS, defined above, when not specifying other flags */
#define TMASYNC      0x80000000L        /* perform routine asynchronously */
#define TMONEPHASE   0x40000000L        /* caller is using on-phase commit
                                        optimisation */
#define TMFAIL       0x20000000L        /* dissociates caller and marks
                                        transaction branch rollback-only */
#define TMNOWAIT     0x10000000L        /* return if blocking condition exists */
#define TMRESUME     0x08000000L        /* caller is resuming association
                                        with suspended transaction branch */
#define TMSUCCESS    0x04000000L        /* dissociate caller from transaction
                                        branch*/
#define TMSUSPEND    0x02000000L        /* caller is suspending, not ending,
                                        association */
#define TMSTARTRSCAN 0x01000000L        /* start a recovery scan */
#define TMENDRSCAN   0x00800000L        /* end a recovery scan */
#define TMMULTIPLE   0x00400000L        /* wait for any asynchronous operation */
#define TMJOIN       0x00200000L        /* caller is joining existing transaction
                                        branch */
#define TMMIGRATE    0x00100000L        /* caller intends to perfrom migration */
/*
 * ax_() return codes (transaction manager reports to resource manager)
 */
#define TM_JOIN      2                  /* caller is joining existing transaction
                                        branch */
#define TM_RESUME    1                  /* caller is resuming association with 
                                        suspended transaction branch */
#define TM_OK        0                  /* normal execution */
#define TMER_TMERR   -1                 /* an error occured in the 
                                        transaction manager */
#define TMER_INVAL   -2                 /* invalid arguments were given */
#define TMER_PROTO   -3                 /* routine invoked in an improper context */
/*
 * xa_() return codes (resource manager reports to transaction manager)
 */
#define XA_RBBASE      100               /* The inclusive lower bound of the 
                                        rollback codes */
#define XA_RBROLLBACK  XA_RBBASE         /* The rollback was caused by an 
                                        unspecified reason */
#define XA_RBCOMMFAIL  XA_RBBASE+1       /* The rollback was caused by a 
                                        communication failure */
#define XA_RBDEADLOCK  XA_RBBASE+2       /* A deadlock was detected */
#define XA_RBINTEGRITY XA_RBBASE+3      /* A condition that violates the integrity
                                        of the resources was detected */
#define XA_RBOTHER     XA_RBBASE+4       /* The resource manager rolled back the
                                        transaction branch for a reason not on
                                        this list */
#define XA_RBPROTO     XA_RBBASE+5       /* A protocol error occurred in the 
                                        resource manager */
#define XA_RBTIMEOUT   XA_RBBASE+6       /* A transaction branch took too long */
#define XA_RBTRANSIENT XA_RBBASE+7      /* May retry the transaction branch */
#define XA_RBEND       XA_RBTRANSIENT    /* The inclusive upper bound of the 
                                        rollback codes */

#define XA_NOMIGRATE  9                 /* resumption must occur where 
                                        suspension occured */
#define XA_HEURHAZ    8                 /* the transaction branch may have
                                        been heuristically completed */
#define XA_HEURCOM    7                 /* the transaction branch has been 
                                        heuristically committed */
#define XA_HEURRB     6                 /* the transaction branch has been 
                                        heuristically rolled back */
#define XA_HEURMIX    5                 /* the transaction branch has been 
                                        heuristically committed and rolled 
                                        back */
#define XA_RETRY      4                 /* routine returned with no effect and 
                                        may be re-issued */
#define XA_RDONLY     3                 /* the transaction branch was read-
                                         only and has been committed */
#define XA_OK         0                 /* normal execution */
#define XAER_ASYNC    -2                /* asynchronous operation already outstanding */
#define XAER_RMERR    -3                /* a resource manager error occured in 
                                        the transaction branch */
#define XAER_NOTA     -4                /* the XID is not valid */
#define XAER_INVAL    -5                /* invalid arguments were given */
#define XAER_PROTO    -6                /* routine invoked in an improper context */
#define XAER_RMFAIL   -7                /* resource manager unavailable */
#define XAER_DUPID    -8                /* the XID already exits */
#define XAER_OUTSIDE  -9                /* resource manager doing work outside */
                                        /* global transaction */

#endif /* ifndef XA_H */
/*
 * End of xa.h header
 */            
