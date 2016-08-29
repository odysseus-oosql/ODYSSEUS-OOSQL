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

#ifndef _values_h
#define _values_h

/* These values work with any binary representation of integers
 * where the high-order bit contains the sign. */

/* a number used normally for size of a shift */
#if gcos
#define BITSPERBYTE     9
#else
#define BITSPERBYTE     8
#endif
#define BITS(type)      (BITSPERBYTE * (int)sizeof(type))

/* short, regular and long ints with only the high-order bit turned on */
#define HIBITS  ((short)(1 << BITS(short) - 1))
#define HIBITI  (1 << BITS(int) - 1)
#define HIBITL  (1L << BITS(long) - 1)

/* largest short, regular and long int */
#define MAXSHORT        ((short)~HIBITS)
#define MAXINT  (~HIBITI)
#define MAXLONG (~HIBITL)

/* various values that describe the binary floating-point representation
 * _EXPBASE     - the exponent base
 * DMAXEXP      - the maximum exponent of a double (as returned by frexp())
 * FMAXEXP      - the maximum exponent of a float  (as returned by frexp())
 * DMINEXP      - the minimum exponent of a double (as returned by frexp())
 * FMINEXP      - the minimum exponent of a float  (as returned by frexp())
 * MAXDOUBLE    - the largest double
                        ((_EXPBASE ** DMAXEXP) * (1 - (_EXPBASE ** -DSIGNIF)))
 * MAXFLOAT     - the largest float
                        ((_EXPBASE ** FMAXEXP) * (1 - (_EXPBASE ** -FSIGNIF)))
 * MINDOUBLE    - the smallest double (_EXPBASE ** (DMINEXP - 1))
 * MINFLOAT     - the smallest float (_EXPBASE ** (FMINEXP - 1))
 * DSIGNIF      - the number of significant bits in a double
 * FSIGNIF      - the number of significant bits in a float
 * DMAXPOWTWO   - the largest power of two exactly representable as a double
 * FMAXPOWTWO   - the largest power of two exactly representable as a float
 * _IEEE        - 1 if IEEE standard representation is used
 * _DEXPLEN     - the number of bits for the exponent of a double
 * _FEXPLEN     - the number of bits for the exponent of a float
 * _HIDDENBIT   - 1 if high-significance bit of mantissa is implicit
 * LN_MAXDOUBLE - the natural log of the largest double  -- log(MAXDOUBLE)
 * LN_MINDOUBLE - the natural log of the smallest double -- log(MINDOUBLE)
 * LN_MAXFLOAT  - the natural log of the largest float  
 * LN_MINFLOAT  - the natural log of the smallest float
 */
#if u3b || u3b5 || sun
#define MAXDOUBLE       1.797693134862315708e+308
#define MAXFLOAT        ((float)3.40282346638528860e+38)
#define MINDOUBLE       4.94065645841246544e-324
#define MINFLOAT        ((float)1.40129846432481707e-45)
#define _IEEE           1
#define _DEXPLEN        11
#define _HIDDENBIT      1
#define DMINEXP (-(DMAXEXP + DSIGNIF - _HIDDENBIT - 3))
#define FMINEXP (-(FMAXEXP + FSIGNIF - _HIDDENBIT - 3))
#define LN_MAXFLOAT     8.8722839052068e+01
#define LN_MINFLOAT     -1.03278929903432e+02
#endif
#if pdp11 || vax
#define MAXDOUBLE       1.701411834604692293e+38
#define MAXFLOAT        ((float)1.701411733192644299e+38)
/* The following is kludged because the PDP-11 compilers botch the simple form.
   The kludge causes the constant to be computed at run-time on the PDP-11,
   even though it is still "folded" at compile-time on the VAX. */
#define MINDOUBLE       (0.01 * 2.938735877055718770e-37)
#define MINFLOAT        ((float)MINDOUBLE)
#define _IEEE           0
#define _DEXPLEN        8
#define _HIDDENBIT      1
#define DMINEXP (-DMAXEXP)
#define FMINEXP (-FMAXEXP)
#endif
#if gcos
#define MAXDOUBLE       1.7014118346046923171e+38
#define MAXFLOAT        ((float)1.7014118219281863150e+38)
#define MINDOUBLE       2.9387358770557187699e-39
#define MINFLOAT        ((float)MINDOUBLE)
#define _IEEE           0
#define _DEXPLEN        8
#define _HIDDENBIT      0
#define DMINEXP (-(DMAXEXP + 1))
#define FMINEXP (-(FMAXEXP + 1))
#endif
#if u370
#define _LENBASE        4
#else
#define _LENBASE        1
#endif
#define _EXPBASE        (1 << _LENBASE)
#define _FEXPLEN        8
#define DSIGNIF (BITS(double) - _DEXPLEN + _HIDDENBIT - 1)
#define FSIGNIF (BITS(float)  - _FEXPLEN + _HIDDENBIT - 1)
#define DMAXPOWTWO      ((double)(1L << BITS(long) - 2) * \
                                (1L << DSIGNIF - BITS(long) + 1))
#define FMAXPOWTWO      ((float)(1L << FSIGNIF - 1))
#define DMAXEXP ((1 << _DEXPLEN - 1) - 1 + _IEEE)
#define FMAXEXP ((1 << _FEXPLEN - 1) - 1 + _IEEE)
#define LN_MAXDOUBLE    (M_LN2 * DMAXEXP)
#define LN_MINDOUBLE    (M_LN2 * (DMINEXP - 1))

#define H_PREC  (DSIGNIF % 2 ? (1L << DSIGNIF/2) * M_SQRT2 : 1L << DSIGNIF/2)
#define X_EPS   (1.0/H_PREC)
#define X_PLOSS ((double)(long)(M_PI * H_PREC))
#define X_TLOSS (M_PI * DMAXPOWTWO)
#define M_LN2   0.69314718055994530942
#define M_PI    3.14159265358979323846
#define M_SQRT2 1.41421356237309504880
#define MAXBEXP DMAXEXP /* for backward compatibility */
#define MINBEXP DMINEXP /* for backward compatibility */
#define MAXPOWTWO       DMAXPOWTWO /* for backward compatibility */

#endif /*!_values_h*/
