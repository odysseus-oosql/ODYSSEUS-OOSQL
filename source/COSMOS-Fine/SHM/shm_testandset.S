#if defined(sparc)

#ifdef SOLARIS2 
#define PROC(x) .seg "text" ; .proc 14 ; .global x ; x :
#else /* SOLARIS2 */
#ifdef __STDC__
#define PROC(x) .seg "text" ; .proc 14 ; .global _##x ; _##x :
#else
#define PROC(x) .seg "text" ; .proc 14 ; .global _/**/x ; _/**/x :
#endif
#endif /* SOLARIS2 */

PROC(testandset)
	retl
	ldstub	[%o0], %o0 

PROC(testandset_release)
	retl
	stb	%g0,[%o0]

PROC(testandset_init)
	retl
	stb	%g0,[%o0]

PROC(testandset_examine)
	retl
	ldub	[%o0], %o0

#endif

#ifdef hp9000s800
        .SPACE  $TEXT$,SORT=8
        .SUBSPA $LIT$,QUAD=0,ALIGN=8,ACCESS=44,SORT=16
$THIS_LIT$

        .SUBSPA $LITSTATIC$,QUAD=0,ALIGN=8,ACCESS=44,SORT=16
$THIS_LITSTATIC$

        .SPACE  $PRIVATE$,SORT=16
        .SUBSPA $DATA$,QUAD=1,ALIGN=8,ACCESS=31,SORT=16
$THIS_DATA$

        .SUBSPA $SHORTDATA$,QUAD=1,ALIGN=8,ACCESS=31,SORT=16
$THIS_SHORTDATA$

        .SUBSPA $BSS$,QUAD=1,ALIGN=8,ACCESS=31,ZERO,SORT=82
$THIS_BSS$

        .SUBSPA $SHORTBSS$,QUAD=1,ALIGN=8,ACCESS=31,ZERO,SORT=80
$THIS_SHORTBSS$

        .SUBSPA $STATICDATA$,QUAD=1,ALIGN=8,ACCESS=31,SORT=16
$THIS_STATICDATA$

        .SUBSPA $SHORTSTATICDATA$,QUAD=1,ALIGN=8,ACCESS=31,SORT=24
$THIS_SHORTSTATICDATA$

        .END
#endif


#if defined(i386)
/*
 * bts: bit test and set
 * The bit is copied into the carry flag (CF) and then set bit
 * xchg: exchange memory and register
 * exchange contents of memory location and register
 *
 * currently ignores flag value
 */

#define	PROC(x)	.text ; .align 2 ; .globl x ; x:

#define SYSV	/* TEMPORARY_PATCH */
	
#ifdef SYSV
PROC(testandset)
#else
PROC(_testandset)
#endif
	pushl	%ebp	/* build stack frame for debuggers */
	movl	%esp,%ebp
	movl	8(%ebp),%ecx	/* addr -> cx */
	movl	$1,%eax		/* "1" == test and set grabbed */
	lock
	xchgb  (%ecx),%al	/* return value in %eax */
	leave
	ret
#ifdef SYSV
PROC(testandset_release)
#else
PROC(_testandset_release)
#endif
	pushl	%ebp	/* build stack frame for debuggers */
	movl	%esp,%ebp
	movl	8(%ebp),%ecx	/* addr */
	movb	$0,(%ecx)	/* clear it */
	leave
	ret

#ifdef SYSV
PROC(testandset_init)
#else
PROC(_testandset_init)
#endif
	pushl	%ebp	/* build stack frame for debuggers */
	movl	%esp,%ebp
	movl	8(%ebp),%ecx	/* addr */
	movb	$0,(%ecx)	/* clear it */
	leave
	ret

#ifdef SYSV
PROC(testandset_examine)
#else
PROC(_testandset_examine)
#endif
	pushl	%ebp
	movl	%esp,%ebp
	movl	8(%ebp),%ecx
	movl	$0,%eax
	movb	(%ecx),%al
	leave
	ret
#endif
