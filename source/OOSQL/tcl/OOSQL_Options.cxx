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

/****************************************************************************
*
* Description:	This module contains code to parse the command line,
*				extracting options and parameters in standard System V
*				style.
*
****************************************************************************/

#include <stdlib.h>
#include <string.h>
#include "scitech.h"
#ifdef	__LINUX__
#include <strstream.h>
#else
#include <strstrea.h>
#endif
#include "OOSQL_Options.hxx"

/*-------------------------- Implementation -------------------------------*/

// The following macros determine if a character is a valid command line
// switch or not. Under UNIX, only the '-' is recognised, while under
// MS-DOS we recognise both '/' and '-'.

#ifdef	__MSDOS__
#define	IS_SWITCH_CHAR(c)		(c == '-') || (c == '/')
#define	IS_NOT_SWITCH_CHAR(c)	(c != '-') && (c != '/')
#else
#define	IS_SWITCH_CHAR(c)		(c == '-')
#define	IS_NOT_SWITCH_CHAR(c)	(c != '-')
#endif

int OOSQL_TCOptions::getopt(int argc,char** argv,char* format,char** argument)
/****************************************************************************
*
* Function:		OOSQL_TCOptions::getopt
* Parameters:	argc		-	Value passed to program through argc
*								variable in the function main.
*				argv		- 	Pointer to the argv array that is passed to
*								the program in function main.
*				format		-	A string representing the expected format
*								of the command line options that need to be
*								parsed.
*				argument	- 	Pointer to optional argument on command
*								line.
*
* Returns:		Character code representing the next option parsed from the
*				command line by getopt. Returns ALLDONE (-1) when there are
*				no more parameters to be parsed on the command line,
*				PARAMETER (-2) when the argument being parsed is a
*				parameter and not an option switch and lastly INVALID (-3)
*				if an error occured while parsing the command line.
*
* Description:	Function to parse the command line option switches in
*				UNIX System V style. When getopt is called, it returns the
*				character code of the next valid option that is parsed from
*				the command line as specified by the Format string. The
*				format string should be in the following form:
*
*						"abcd:e:f:"
*
*				where a,b and c represent single switch style options and
*				the character code returned by getopt is the only value
*				returned. Also d, e and f represent options that expect
*				arguments immediately after them on the command line. The
*				argument that follows the option on the command line is
*				returned via a reference in the pointer argument. Thus
*				a valid command line for this format string might be:
*
*					myprogram -adlines /b /f format infile outfile
*
*				where a and b will be returned as single character options
*				with no argument, while d is returned with the argument
*				lines and f is returned with the argument format. Note that
*				either UNIX style or MS-DOS command switches may be used
*				interchangeably under MSDOS, but under UNIX only the UNIX
*				style switches are supported.
*
*				When getopt returns with PARAMETER (we attempted to parse
*				a paramter, not an option), the global variable NextArgv
*				will hold an index in the argv array to the argument on the
*				command line AFTER the options, ie in the above example the
*				string 'infile'. If the parameter is successfully used,
*				NextArgv should be incremented and getopt can be called
*				again to parse any more options. Thus you can also have
*				options interspersed throught the command line. eg:
*
*					myprogram -adlines infile /b outfile /f format
*
*				can be made to be a valid form of the above command line.
*
****************************************************************************/
{
	char	ch;
	char	*formatchar;

	if (argc > nextargv) {
		if (nextchar == NULL) {
			nextchar = argv[nextargv];		// Index next argument
			if (nextchar == NULL) {
				nextargv++;
				return OPT_ALLDONE;			// No more options
				}
			if (IS_NOT_SWITCH_CHAR(*nextchar)) {
				nextchar = NULL;
				return OPT_PARAMETER;		// We have a parameter
				}
			nextchar++;						// Move past switch operator
			if (IS_SWITCH_CHAR(*nextchar)) {
				nextchar = NULL;
				return OPT_INVALID;			// Ignore rest of line
				}
			}

		if ((ch = *(nextchar++)) == 0) {
			nextchar = NULL;
			return OPT_INVALID;				// No options on line
			}

		if (ch == ':' ||  (formatchar = strchr(format, ch)) == NULL)
			return OPT_INVALID;

		if (*(++formatchar) == ':') {	// Expect an argument after option
			nextargv++;
			if (*nextchar == 0) {
				if (argc <= nextargv)
					return OPT_INVALID;
				nextchar = argv[nextargv++];
				}
			*argument = nextchar;
			nextchar = NULL;
			}
		else {						// We have a switch style option
			if (*nextchar == 0) {
				nextargv++;
				nextchar = NULL;
			}
			*argument = NULL;
			}
		return ch;					// return the option specifier
		}
	nextchar = NULL;
	nextargv++;
	return OPT_ALLDONE;				// no arguments on command line
}

int OOSQL_TCOptions::convert(const OOSQL_TCOption& option,char *argument)
/****************************************************************************
*
* Function:		OOSQL_TCOptions::convert
* Parameters:	option		- Description for the option we are converting
*				argument	- String to convert
* Returns:		OPT_INVALID on error, OPT_ALLDONE on success.
*
* Description:	Converts the argument string depending on the type of
*				argument that is expected, filling in the argument for that
*				option. Note that to parse a string, we simply return a
*				pointer to the argument.
*
****************************************************************************/
{
	istrstream	in(argument);

	switch ((Four_Invariable)option.type) {
		case OPT_INTEGER:
			in >> *((int*)option.arg);
			break;
		case OPT_HEX:
			in >> hex >> *((UFour*)option.arg);
			break;
		case OPT_OCTAL:
			in >> oct >> *((UFour*)option.arg);
			break;
		case OPT_UNSIGNED:
			in >> *((UFour*)option.arg);
			break;
		case OPT_LINTEGER:
			in >> *((long*)option.arg);
			break;
		case OPT_LHEX:
			in >> hex >> *((UFour*)option.arg);
			break;
		case OPT_LOCTAL:
			in >> oct >> *((UFour*)option.arg);
			break;
		case OPT_LUNSIGNED:
			in >> *((UFour*)option.arg);
			break;
		case OPT_FLOAT:
			in >> *((float*)option.arg);
			break;
		case OPT_DOUBLE:
			in >> *((double*)option.arg);
			break;
		case OPT_STRING:
			*((char**)option.arg) = argument;
			return OPT_ALLDONE;
		default:
			return OPT_INVALID;
		}

	// Here we check to ensure that the conversion was valid.

	if (in.fail())
		return OPT_INVALID;
	else
		return OPT_ALLDONE;
}

int OOSQL_TCOptions::parse(int argc,char *argv[])
/****************************************************************************
*
* Function:		getargs
* Parameters:	argc		- Number of arguments on command line
*				argv		- Array of command line arguments
* Returns:		OPT_ALLDONE, OPT_INVALID or OPT_HELP or the value
*				returned by doParam if it wasn't OPT_ALLDONE.
*
* Description:	Method to parse the command line according to the table of
*				options. This routine calls getopt above to parse each
*				individual option and attempts to parse each option into
*				a variable of the specified type. The routine can parse
*				integers and long integers in either decimal, octal,
*				hexadecimal notation, unsigned integers and unsigned longs,
*				strings and option switches. OOSQL_TCOption switches are simply
*				boolean variables that get turned on if the switch was
*				parsed.
*
*				Parameters are extracted from the command line by calling
*				a the virtual member doParam() to handle each parameter
*				as it is encountered. The member doParam() should accept
*				a pointer to the parameter on the command line and an
*				integer representing how many parameters have been
*				encountered (ie: 1 if this is the first parameter, 10 if
*				it is the 10th etc), and return OPT_ALLDONE upon
*				successfully parsing it or INVALID if the parameter was
*				invalid.
*
*				We return either OPT_ALLDONE if all the options were
*				successfully parsed, INVALID if an invalid option was
*				encountered or HELP if any of -h, -H or -? were present
*				on the command line.
*
****************************************************************************/
{
	int		i,opt;
	char	*argument;
	int		param_num = 1;
	char	cmdstr[MAXARG*2 + 4];

	if (optarr == NULL) {
		cerr << "You _MUST_ setup optarr before calling parse()" << endl;
		exit(1);
		}

	nextargv = 1;			// Index into argv array
	nextchar = NULL;		// Pointer to next character

	// Build the command string from the array of options

	strcpy(cmdstr,"hH?");
	for (i = 0,opt = 3; i < numopt; i++,opt++) {
		cmdstr[opt] = optarr[i].opt;
		if (optarr[i].type != OPT_SWITCH) {
			cmdstr[++opt] = ':';
			}
		}
	cmdstr[opt] = '\0';

	while (true) {
		opt = getopt(argc,argv,cmdstr,&argument);
		switch (opt) {
			case 'H':
			case 'h':
			case '?':
				return OPT_HELP;
			case OPT_ALLDONE:
				return OPT_ALLDONE;
			case OPT_INVALID:
				return OPT_INVALID;
			case OPT_PARAMETER:
				i = doParam(argv[nextargv],param_num);
				if (i != OPT_ALLDONE)
					return i;
				nextargv++;
				param_num++;
				break;
			default:

				// Search for the option in the option array. We are
				// guaranteed to find it.

				for (i = 0; i < numopt; i++) {
					if (optarr[i].opt == opt)
						break;
					}
				if (optarr[i].type == OPT_SWITCH)
					*((bool*)optarr[i].arg) = true;
				else {
					if (convert(optarr[i],argument) == OPT_INVALID)
						return OPT_INVALID;
					}
				break;
			}
		}
}

ostream& operator << (ostream& o,OOSQL_TCOptions& opt)
/****************************************************************************
*
* Function:		operator <<
* Parameters:	o	- Stream to send usage info to
*				opt	- OOSQL_TCOption class to display
*
* Description:	Prints the description of each option in a standard format
*				to the specified stream. The description for each option
*				is obtained from the table of options.
*
****************************************************************************/
{
	for (int i = 0; i < opt.numopt; i++) {
		if (opt.optarr[i].type == OPT_SWITCH)
			o << "  " << '-' << opt.optarr[i].opt
			  << "       " << opt.optarr[i].desc << endl;
		else
			o << "  " << '-' << opt.optarr[i].opt
			  << "<arg>  " << opt.optarr[i].desc << endl;
		}
	return o;
}
