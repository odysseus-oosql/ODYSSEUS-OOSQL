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
#/*    ODYSSEUS/COSMOS General-Purpose Large-Scale Object Storage System --    */
#/*    Fine-Granule Locking Version                                            */
#/*    Version 3.0                                                             */
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
# usage: <this-script> [-d] log_def_file
#

require 'getopts.pl';
&Getopts('d');
  
&usage if ($#ARGV != 0);
  
$logrectbl_hdr_file = './logRecTbl.h';
$logrectbl_file = './log_LogRecTbl.i';
	
$redo_return_type = "Four";
$redo_proto = "(Four, void*, LOG_LogRecInfo_T*)";
$undo_return_type = "Four";
$undo_proto = "(Four, XactTableEntry_T*, Buffer_ACC_CB*, Lsn_T*, LOG_LogRecInfo_T*)";

foreach $file (@ARGV) {
  &generate_logrectbl_files($file);
}

sub generate_logrectbl_files {
  local($file)=$_[0];

  open(FILE,$file) || die "cannot open $file\n";

  $count = 0;
  
 LINE: while ($line = <FILE>) {
    next LINE if ($line =~ m/^\#/);
    next LINE if ($line =~ m/^\s*$/);

    if ($line =~ m/\s*(\w+)\s*\{\s*$/) {
      if ($opt_d) {
	print "BEGIN BLOCK: ", "log record action name = ", $1, "\n";
      }
      
      $logrec{action_name} = $1;

      @fields = ("buffer_type", "redo_function", "undo_function");
      foreach (@fields) {
	$line = <FILE>;
	if ($line =~ m/\s*(\w+)\s*$/) {
	  $logrec{$_} = $1;
	} else {
	  die "$file: wrong format\n";	  
	}	  
      }
      
      $line = <FILE>;
      if ($line =~ m/\s*\}\s*$/) {
	if ($opt_d) {
	  print "END BLOCK: ", $line, "\n";
	}

	$logrectbl[$count] = {%logrec};
	$count = $count + 1;
      } else {
	  die "$file: wrong format\n";	  
      }      
    } else {
      die "$file: wrong format\n";
    }
  }

  #
  # ## $logrectbl_hdr_file
  #
  open(HDR_FILE, ">$logrectbl_hdr_file") || die "cannot create $logrectbl_hdr_file: $! \n";

  #
  # write action definition table
  #
  print HDR_FILE "\n/* Don't update this file; this file is automatically generated. */\n\n";
  print HDR_FILE "/*\n * Actions of log records\n */\n";
  print HDR_FILE "typedef enum LOG_Action_T_tag {\n";
  for ($i=0; $i <= $#logrectbl; ++$i) {
    $logrecref = $logrectbl[$i];
    print HDR_FILE "    $$logrecref{action_name}=$i";
    if ($i < $#logrectbl) {
      print HDR_FILE ",\n";
    } else {
      print HDR_FILE "\n";
    }
  }
  print HDR_FILE "} LOG_Action_T;\n";


  #
  # write function prototypes for REDO/UNDO functions
  #
  print HDR_FILE "\n\n\n/*\n * REDO/UNDO function prototypes\n */\n";
  foreach $logrecref (@logrectbl) {
    if (! ($$logrecref{redo_function} eq "NULL")) {
      print HDR_FILE "$redo_return_type $$logrecref{redo_function}$redo_proto;\n";
    }
    if (! ($$logrecref{undo_function} eq "NULL")) {
      print HDR_FILE "$undo_return_type $$logrecref{undo_function}$undo_proto;\n";
    }
  }


  #
  # write definition of log record table
  #
  $redofn_typedef = "typedef $redo_return_type (*LOG_RedoFnPtr_T)$redo_proto;";  
  $undofn_typedef = "typedef $undo_return_type (*LOG_UndoFnPtr_T)$undo_proto;";
  $logrectblentry_typedef = <<_EOF;
typedef struct LOG_LogRecTableEntry_T_tag {
  Four bufType;		        /* buffer type */
  LOG_RedoFnPtr_T redoFnPtr;	/* pointer to the redo function */
  LOG_UndoFnPtr_T undoFnPtr;    /* pointer to the undo function */
} LOG_LogRecTableEntry_T;
_EOF
  
  print HDR_FILE "\n\n\n/*\n * Log Record Table\n */\n";
  print HDR_FILE $redofn_typedef, "\n\n"; 
  print HDR_FILE $undofn_typedef, "\n\n"; 
  print HDR_FILE $logrectblentry_typedef, "\n";

  close(HDR_FILE);
  
  #
  # ## $logrectbl_file
  #
  open(LOG_REC_TBL, ">$logrectbl_file") || die "cannot create $logrectbl_file: $! \n";
  print LOG_REC_TBL "/* Don't update this file; this file is automatically generated. */\n\n";
  print LOG_REC_TBL "LOG_LogRecTableEntry_T LOG_logRecTbl[] = {\n";
  foreach $logrecref (@logrectbl) {
    if (! ($$logrecref{buffer_type} eq "NULL")) {
      print LOG_REC_TBL "    $$logrecref{buffer_type}, $$logrecref{redo_function}, $$logrecref{undo_function},\n";
    }
  }
  print LOG_REC_TBL "};\n";
  
  close(LOG_REC_TBL);
}

  
sub usage {
  die "usage: $0 [-d] log_record_table_data_file\n";
}
		  
