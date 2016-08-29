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

/*
* Description:  Header file for command line parsing class. The class
*               OOSQL_TCOptions is an abstract class that defines the methods
*               used to parse the command line in standard System V
*               style.
*
*
****************************************************************************/

#ifndef _OOSQL_TCL_OPTIONS_H_
#define _OOSQL_TCL_OPTIONS_H_

#include "OOSQL_Common.h"
#include <iostream.h>

/*---------------------------- Typedef's etc -----------------------------*/

#define OPT_ALLDONE     -1
#define OPT_PARAMETER   -2
#define OPT_INVALID     -3
#define OPT_HELP        -4

#define MAXARG      40

// OOSQL_TCOption type sepecifiers

#define OPT_INTEGER     'd'
#define OPT_HEX         'h'
#define OPT_OCTAL       'o'
#define OPT_UNSIGNED    'u'
#define OPT_LINTEGER    'D'
#define OPT_LHEX        'H'
#define OPT_LOCTAL      'O'
#define OPT_LUNSIGNED   'U'
#define OPT_FLOAT       'f'
#define OPT_DOUBLE      'F'
#define OPT_STRING      's'
#define OPT_SWITCH      '!'

/*--------------------------- Class Definition ----------------------------*/

//---------------------------------------------------------------------------
// The following structure is used to describe each of the options to the
// options parsing routines. An array of these will need to be set up
// in the constructor call for your derived OOSQL_TCOptions class below.
//---------------------------------------------------------------------------

struct OOSQL_TCOption {
    UOne    opt;                // The letter to describe the option
    UOne    type;               // Type descriptor for the option
    void    *arg;               // Place to store the argument
    char    *desc;              // Description for this option
};

//---------------------------------------------------------------------------
// The following class is an abstract base class to provide the generalised
// methods to parse a command line. Descendants of this class will need
// to set up the optarr pointer in the constructor before parse() is
// called.
//---------------------------------------------------------------------------

class OOSQL_TCOptions {
protected:
    int             nextargv;       // Variable required by getopt
    char*           nextchar;
    OOSQL_TCOption  *optarr;        // Pointer to option desciption array
    int             numopt;         // Number of options in table

            // Protected member to parse a single option
            int getopt(int argc,char** argv,char* format,char** argument);

            // Protected member to convert a single option
            int convert(const OOSQL_TCOption& option,char* argument);

            // Protected pure virtual to parse a single parameter
    virtual int doParam(char* param,int num) = 0;

public:
            // Constructor
            OOSQL_TCOptions()   { optarr = NULL; };

            // Method to parse the command line
            int parse(int argc,char *argv[]);

            // Friend function to display usage information
    friend  ostream& operator << (ostream& o,OOSQL_TCOptions& opt);
};

#endif  // _OOSQL_TCL_OPTIONS_H_
