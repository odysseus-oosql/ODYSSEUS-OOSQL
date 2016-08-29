#!/usr/local/bin/perl
#
# usage: <this-script> [-d] error_data_file
#

require 'getopts.pl';
&Getopts('d');
  
&usage if ($#ARGV != 0);
  
$error_codes_file = './OOSQL_errorcodes.h';  
$error_init_file = './OOSQL_Err_InitVariables.i';


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
#define OOSQL_ERR_ENCODE_ERROR_CODE(base,no)        ( -1 * (((base) << 16) + no) )
_EOF
  $error_get_base_macro = <<_EOF;
#define OOSQL_ERR_GET_BASE_FROM_ERROR_CODE(code)    ( (((code) * -1) >> 16) & 0x0000FFFF )
_EOF
  $error_get_no_macro = <<_EOF;
#define OOSQL_ERR_GET_NO_FROM_ERROR_CODE(code)      ( ((code) * -1) & 0x0000FFFF )
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
  printf ERROR_CODES ("#define %-30s %d\n", "OOSQL_NUM_OF_ERROR_BASES", $count);


  #
  # write error definitions
  #
  foreach $baseref (@errorbaseinfo) {
    print ERROR_CODES "\n\n/*\n * Error Definitions for $baseref->[0]\n */\n";
    $count = 0;
    foreach $errorref (@{$baseref->[2]}) {
      printf ERROR_CODES
	("#define %-30s OOSQL_ERR_ENCODE_ERROR_CODE(%s,%d)\n", $errorref->[0], $baseref->[0], $count);
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

  print ERROR_INIT "OOSQL_Err_ErrBaseInfo_T oosql_err_errBaseInfo[] = {\n";
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
    print ERROR_INIT "static OOSQL_Err_ErrInfo_T oosql_err_infos_of_\L$baseref->[0]\E[] = {\n";    
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

  print ERROR_INIT "OOSQL_Err_ErrInfo_T *oosql_err_allErrInfo[] = {\n";    
  print ERROR_INIT "    NULL,\n";
  foreach $base (@errorbaseinfo) {
    print ERROR_INIT "    oosql_err_infos_of_\L$base->[0]\E,\n";
  }
  print ERROR_INIT "};\n";



  close(ERROR_INIT);
}

  
sub usage {
  die "usage: $0 [-d] error_data_file\n";
}
		  
