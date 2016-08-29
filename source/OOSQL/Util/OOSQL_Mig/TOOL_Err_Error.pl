#!/usr/local/bin/perl

#/******************************************************************************/
#/*                                                                            */
#/*    Copyright (c) 1990-2016, KAIST                                          */
#/*    All rights reserved.                                                    */
#/*                                                                            */
#/*    Redistribution and use in source and binary forms, with or without      */
#/*    modification, are permitted provided that the following conditions      */
#/*    are met:                                                                */
#/*                                                                            */
#/*    1. Redistributions of source code must retain the above copyright       */
#/*       notice, this list of conditions and the following disclaimer.        */
#/*                                                                            */
#/*    2. Redistributions in binary form must reproduce the above copyright    */
#/*       notice, this list of conditions and the following disclaimer in      */
#/*       the documentation and/or other materials provided with the           */
#/*       distribution.                                                        */
#/*                                                                            */
#/*    3. Neither the name of the copyright holder nor the names of its        */
#/*       contributors may be used to endorse or promote products derived      */
#/*       from this software without specific prior written permission.        */
#/*                                                                            */
#/*    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS     */
#/*    "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT       */
#/*    LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS       */
#/*    FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE          */
#/*    COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,    */
#/*    INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,    */
#/*    BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;        */
#/*    LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER        */
#/*    CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT      */
#/*    LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN       */
#/*    ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE         */
#/*    POSSIBILITY OF SUCH DAMAGE.                                             */
#/*                                                                            */
#/******************************************************************************/
#/******************************************************************************/
#/*                                                                            */
#/*    ODYSSEUS/OOSQL DB-IR-Spatial Tightly-Integrated DBMS                    */
#/*    Version 5.0                                                             */
#/*                                                                            */
#/*    Developed by Professor Kyu-Young Whang et al.                           */
#/*                                                                            */
#/*    Advanced Information Technology Research Center (AITrc)                 */
#/*    Korea Advanced Institute of Science and Technology (KAIST)              */
#/*                                                                            */
#/*    e-mail: odysseus.oosql@gmail.com                                        */
#/*                                                                            */
#/*    Bibliography:                                                           */
#/*    [1] Whang, K., Lee, J., Lee, M., Han, W., Kim, M., and Kim, J., "DB-IR  */
#/*        Integration Using Tight-Coupling in the Odysseus DBMS," World Wide  */
#/*        Web, Vol. 18, No. 3, pp. 491-520, May 2015.                         */
#/*    [2] Whang, K., Lee, M., Lee, J., Kim, M., and Han, W., "Odysseus: a     */
#/*        High-Performance ORDBMS Tightly-Coupled with IR Features," In Proc. */
#/*        IEEE 21st Int'l Conf. on Data Engineering (ICDE), pp. 1104-1105     */
#/*        (demo), Tokyo, Japan, April 5-8, 2005. This paper received the Best */
#/*        Demonstration Award.                                                */
#/*    [3] Whang, K., Park, B., Han, W., and Lee, Y., "An Inverted Index       */
#/*        Storage Structure Using Subindexes and Large Objects for Tight      */
#/*        Coupling of Information Retrieval with Database Management          */
#/*        Systems," U.S. Patent No.6,349,308 (2002) (Appl. No. 09/250,487     */
#/*        (1999)).                                                            */
#/*    [4] Whang, K., Lee, J., Kim, M., Lee, M., Lee, K., Han, W., and Kim,    */
#/*        J., "Tightly-Coupled Spatial Database Features in the               */
#/*        Odysseus/OpenGIS DBMS for High-Performance," GeoInformatica,        */
#/*        Vol. 14, No. 4, pp. 425-446, Oct. 2010.                             */
#/*    [5] Whang, K., Lee, J., Kim, M., Lee, M., and Lee, K., "Odysseus: a     */
#/*        High-Performance ORDBMS Tightly-Coupled with Spatial Database       */
#/*        Features," In Proc. 23rd IEEE Int'l Conf. on Data Engineering       */
#/*        (ICDE), pp. 1493-1494 (demo), Istanbul, Turkey, Apr. 16-20, 2007.   */
#/*                                                                            */
#/******************************************************************************/

#
# usage: <this-script> [-d] error_data_file
#

require 'getopts.pl';
&Getopts('d');
  
&usage if ($#ARGV != 0);
  
$error_codes_file = './TOOL_errorcodes.h';  
$error_init_file = './TOOL_Err_InitVariables.i';


foreach $file (@ARGV) {
  &generate_err_files($file);
}

sub generate_err_files {
  local($file)=$_[0];

  open(FILE,$file) || die "cannot open $file\n";

 LINE: while ($line = <FILE>) {
    next LINE if ($line =~ m/^\#/);
    next LINE if ($line =~ m/^\s*$/);

    if ($line =~ m/\s*(\w+)\s+(".*")\s*\{\s*$/) {
      if ($opt_d) {
	print "BEGIN BLOCK: ", "base name = ", $1, " base msg = ", $2, "\n";
      }
      
      $errorbasename = $1;
      $errorbasemsg = $2;
      $errorinfosref = [];
      
    } elsif ($line =~ m/\s*(\w+)\s+(".*")\s*$/) {
      if ($opt_d) {
	print "ERROR INFO: ", "error name = ", $1, " error msg = ", $2, "\n";
      }

      push @$errorinfosref, [$1,$2];
      
    } elsif ($line =~ m/\s*\}\s*$/) {
      if ($opt_d) {
	print "END BLOCK: ", $line, "\n";
      }

      push @errorbaseinfo, [$errorbasename, $errorbasemsg, $errorinfosref];
    } else {
      die "$file: wrong format\n";
    }
  }


  #
  # ## $error_codes_file
  #
  open(ERROR_CODES, ">$error_codes_file") || die "cannot create $error_codes_file: $! \n";

  #
  # write some macros
  #
  $error_encode_macro = <<_EOF;
#define TOOL_ERR_ENCODE_ERROR_CODE(base,no)        ( -1 * (((base) << 16) + no) )
_EOF
  $error_get_base_macro = <<_EOF;
#define TOOL_ERR_GET_BASE_FROM_ERROR_CODE(code)    ( (((code) * -1) >> 16) & 0x0000FFFF )
_EOF
  $error_get_no_macro = <<_EOF;
#define TOOL_ERR_GET_NO_FROM_ERROR_CODE(code)      ( ((code) * -1) & 0x0000FFFF )
_EOF

  print ERROR_CODES "/*\n * Macro Definitions\n */\n";
  print ERROR_CODES "$error_encode_macro";
  print ERROR_CODES "$error_get_base_macro";
  print ERROR_CODES "$error_get_no_macro";
  
  #
  # write error base definitions
  #    UNIX_ERR_BASE has the base 0.
  #
  print ERROR_CODES "\n\n/*\n * Error Base Definitions\n */\n";

  $count = 0;
  printf ERROR_CODES ("#define %-30s %d\n", "UNIX_ERR_BASE", $count);
  $count++;
  foreach $baseref (@errorbaseinfo) {
    printf ERROR_CODES ("#define %-30s %d\n", $baseref->[0], $count);
    $count++;
  }
  print ERROR_CODES "\n";
  printf ERROR_CODES ("#define %-30s %d\n", "TOOL_NUM_OF_ERROR_BASES", $count);


  #
  # write error definitions
  #
  foreach $baseref (@errorbaseinfo) {
    print ERROR_CODES "\n\n/*\n * Error Definitions for $baseref->[0]\n */\n";
    $count = 0;
    foreach $errorref (@{$baseref->[2]}) {
      printf ERROR_CODES
	("#define %-30s TOOL_ERR_ENCODE_ERROR_CODE(%s,%d)\n", $errorref->[0], $baseref->[0], $count);
      $count++;
    }
    printf ERROR_CODES
      ("#define %-30s %d\n", "NUM_ERRORS_$baseref->[0]", $count);    
  }
  
  close(ERROR_CODES);


  #
  ### $error_init_file
  #
  open(ERROR_INIT, ">$error_init_file") || die "cannot create $error_init_file: $! \n";

  #
  # write error base informations
  #    UNIX_ERR_BASE has the base 0.
  #
  print ERROR_INIT "\n\n/*\n * Error Base Informations\n */\n";

  print ERROR_INIT "TOOL_Err_ErrBaseInfo_T tool_err_errBaseInfo[] = {\n";
  print ERROR_INIT "    { \"UNIX_ERROR_BASE\", \"unix errors\", 0 },\n";
  foreach $base (@errorbaseinfo) {
    print ERROR_INIT "    { \"$base->[0]\", $base->[1], NUM_ERRORS_$base->[0] },\n";
  }
  print ERROR_INIT "};\n";

  
  #
  # write error informations
  #
  foreach $baseref (@errorbaseinfo) {
    print ERROR_INIT "\n\n/*\n * Error Informations for $baseref->[0]\n */\n";
    print ERROR_INIT "static TOOL_Err_ErrInfo_T tool_err_infos_of_\L$baseref->[0]\E[] = {\n";    
    foreach $errorref (@{$baseref->[2]}) {
      print ERROR_INIT "    { $errorref->[0], \"$errorref->[0]\", $errorref->[1] },\n";
    }
    print ERROR_INIT "};\n";
  }
    
  #
  # write error informations for all errors
  #    UNIX_ERR_BASE has the base 0.
  #
  print ERROR_INIT "\n\n/*\n * Error Informations for all errors\n */\n";

  print ERROR_INIT "TOOL_Err_ErrInfo_T *tool_err_allErrInfo[] = {\n";    
  print ERROR_INIT "    NULL,\n";
  foreach $base (@errorbaseinfo) {
    print ERROR_INIT "    tool_err_infos_of_\L$base->[0]\E,\n";
  }
  print ERROR_INIT "};\n";



  close(ERROR_INIT);
}

  
sub usage {
  die "usage: $0 [-d] error_data_file\n";
}
		  
