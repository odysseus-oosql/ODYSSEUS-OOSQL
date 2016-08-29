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
#
# *************************************************************
#
# usage: <this-script> [-e] [-d] [-v] filename [filename]*
#
# -v verbose
# -d generate #defines 
# 
#
# *************************************************************
#
# INPUT: any number of sets of error codes for software
#	layers called "name", with (unique) masks as follows:
#
#	name = mask {
#	ERRNAME	Error string
#	ERRNAME	Error string
#	 ...
#	ERRNAME	Error string
#	}
#
#  (mask can be in octal (0777777), hex (0xabcdeff) or decimal 
#   notation)
#
#	(Error string can be Unix:ECONSTANT, in which case the string
#	for Unix's ECONSTANT is used)
#
#	If you don't do that, the name of the error_info structure will
#	have the <name> prepended, e.g.:
#		odys_error_info_t <name>_error_info[] = ...
#
# *************************************************************
#
# OUTPUT:
#  for each software layer ("name"), this script creates:
#	INFO: <name>_einfo.i  
#	HDRE: <name>_error.h (if -e option is used)
#
#	name_einfo.i  contains a definition of a odys_error_info_t array
#	name_error.h  contains an enumeration for the
#		error codes , and an enum containing eERRMIN & eERRMAX
#
# *************************************************************
#
#

if(!$d) {
	die "You must specify -d.";
}
$both = 0;

$cpysrc = <<EOF;
/*******************************************************************/
/*                                                                 */
/*            ODYSSEUS-CS(Client/Server ODYSSEUS)v0.7              */
/*                                                                 */
/*******************************************************************/
EOF


open(ERRS, "gcc -dM -E /usr/include/sys/errno.h |") || die "cannot open /usr/include/sys/errno.h: $!\n";


# set up Unix error codes 
while (<ERRS>)  {
    /^\#define\s+([E]\w*)\s+([1234567890]+)/ && do {
        @errs{$1}=$2;
    }
}
@errkeys = keys %errs;
@errvalues = values %errs;



$_einfo = '_einfo';
$_errmsg = '_errmsg';
$_msg_size = '_msg_size';
$_OK	= 'OK';

foreach $FILE (@ARGV) {

	&translate($FILE);
	if($v) { printf(STDERR "done\n");}
}

sub translate {
	local($file)=@_[0];
	local($warning)="";
	local($base)=\e;

	open(FILE,$file) || die "Cannot open $file\n";
	if($v) { printf (STDERR "translating $file ...\n"); }
	$warning = 
			"\n/* DO NOT EDIT --- GENERATED from $file by errors.pl */\n\n";

	LINE: while (<>)  {
		next LINE if (/^#/);
		# {
		s/\s*[}]// && do {
			if($v) { 
				printf(STDERR "END OF PACKAGE: ".$basename.",".$BaseName." = 0x%x\n", $base);
			}

			# {
			print INFO "};\n\n";
			if($class) {
				print(INFO 'void '.$class.'::init_errorcodes(){'."\n");
				print(INFO "\tif (! (w_error_t::insert(\n\t\t");
				print(INFO $groupname.', '.$class.'::error_info,',"\n\t\t");
				print(INFO $basename.'ERRMAX - '.$basename.'ERRMIN + 1)) ) {');
				print(INFO "\n\t\t\t W_FATAL(fcINTERNAL);\n\t}\n}\n");
			}
			if($d) {
#				print BINFO "};\n\n";
			}

			if($e) {
				#{
				printf(HDRE "};\n\n");
				printf(HDRE "enum {\n");
				printf(HDRE "    %s%s = 0x%x,\n", $basename, 'ERRMIN', $base);
				printf(HDRE "    %s%s = 0x%x\n", $basename, 'ERRMAX', $highest);
				printf(HDRE "};\n\n");
			}
			if($d) {
				printf(HDRD "#define e%s%s%-20s  (d_Long)0x%x\n", 'ERRMIN', '_', $BaseName, $base);
				printf(HDRD "#define e%s%s%-20s  (d_Long)0x%x\n", 'ERRMAX', '_', $BaseName, $highest);
				printf(HDRD "\n");
			}

			# {
#			print STR "\t\"dummy error code\"\n};\n";
#			print STR "const ".$basename.$_msg_size." = $cnt;\n\n";

			if($d) {
				printf(HDRD "#endif /*__".$basename."_error_def_h__*/\n");
				close HDRD;
			}
#			printf(STR "#endif /*__".$basename."_error_i__*/\n");
#			close STR;
			printf(INFO "odys_ErrorInfo ".$basename."_errCode(\"$BaseName\", ".$basename."_error, eERRMIN_".$basename.", eERRMAX_".$basename.");\n\n");
			printf(INFO "#endif /*__".$basename."_einfo_i__*/\n");
			close INFO;
			if($d) {
#				printf(BINFO "#endif /*__".$basename."_einfo_bakw_i__*/\n");
#				close BINFO;
			}

			$basename = "";
			$BaseName = "";
			$base = \e;
		};

		s/\s*(\S+)\s*[=]\s*([0xabcdef123456789]+)\s*(["].*["])\s*(.*)[{]// && do {
			$basename = $1;
			$base = $2;
			$groupname = $3;
			$class = $4;

			$BaseName = $basename;
			$BaseName =~ y/a-z/A-Z/;
			$base = oct($base) if $base =~ /^0/;
			if($class){
				if($v) {
					printf(STDERR "CLASS=$class\n");
				}
			}

			$cnt = -1;
			$highest = 0;

			if($v) { 
				printf(STDERR "PACKAGE: $basename,$BaseName = 0x%x\n", $base);
			}


			if($d) {
				if($v) {printf(STDERR "trying to open ".$basename.$_error_def.".h\n");}
				open(HDRD, ">$basename$_error_def.h") || 
					die "cannot open $basename$_error_def.h: $!\n";

				printf(HDRD "#ifndef __$basename%serror_def_h__\n", '_');
				printf(HDRD "#define __$basename%serror_def_h__\n", '_');
				printf(HDRD $cpysrc);
			}
			if($d) {
				if($v) {printf(STDERR "trying to open ".$basename."_einfo_bakw.h\n");}
#				open(BINFO, ">$basename"."_einfo_bakw.i") || 
#					die "cannot open $basename"."_einfo_bakw.i: $!\n";

#				printf(BINFO "#ifndef __$basename"."_einfo_bakw_i__\n");
#				printf(BINFO "#define __$basename"."_einfo_bakw_i__\n");
#				printf(BINFO $cpysrc);
			}

			if($v) {printf(STDERR "trying to open $basename$_error.i\n");}
#			open(STR, ">$basename$_error.i") || 
#				die "cannot open $basename$_error.i: $!\n";

#			printf(STR "#ifndef __".$basename."_error_i__\n");
#			printf(STR "#define __".$basename."_error_i__\n");
#			printf(STR $cpysrc);

			if($v) {printf(STDERR "trying to open $basename$_einfo.i\n");}
			open(INFO, ">$basename$_einfo.i") || 
				die "cannot open $basename$_einfo.i: $!\n";
			printf(INFO "#ifndef __".$basename."_einfo_i__\n");
			printf(INFO "#define __".$basename."_einfo_i__\n");
			printf(INFO $cpysrc);

			if($d) {
				print HDRD $warning;
#				print BINFO $warning;
			}
#			print STR $warning;
			print INFO $warning;

			if($class) {
				print INFO 
					"odys_error_info_t ".$class."::error".$_info."[] = {\n"; #}
			} else {
				printf(INFO "#include \"%s_Err.h\"\n\n", $BaseName);
				print INFO 
					"odys_error_info_t ".$BaseName.$_error.$_info."_error[] = {\n"; #}
			}
			if($d) { printf(HDRD "#define e%s%s%-20s (d_Long)0\n", 'OK', '_', $BaseName);}
#			printf(STR "static char* ".$basename.$_errmsg."[] = {\n"); #}

			if($d) {
#				print BINFO  
#					"odys_error_info_t ".$basename.$_error.$_info."_bakw[] = {\n"; #}
			}
		};  # } to match the one in the pattern
		

		next LINE if $base =~ \e;
		($def, $msg) = split(/\s+/, $_, 2);
		next LINE unless $def;
		chop $msg;
		++$cnt;
		if($msg =~ m/^Unix:(.*)/) {
			# 		$def is unchanged
			$val = $errs{$1};
			$!= $val;
			$msg = qq/$!/; # force it to be a string
			$val = $val + $base;
		} else {
			$msg = qq/$msg/;
			$val = $cnt + $base;
		}
		if($highest < $val) { $highest = $val; }

		if($d) {
			printf(HDRD "#define e%s%s%-20s (d_Long)0x%x\n", $def, '_', $BaseName, $val);
		}

		if($e) {
#			printf(STR "/* $basename%-18s */ \"%s\",\n",  $def, $msg);
		} else {
			printf(STR "/* e%s%s%-18s */ \"%s\",\n",  $def, '_', $BaseName, $msg);
		}

		if($e) {
			printf(INFO " { $basename%-18s, \"%s\" },\n", $def, $msg);
		} else {
			printf(INFO " { e%s%s%-18s, \"%s\" },\n", $def, '_', $BaseName, $msg);
		}

		if($d) {
#			printf(BINFO " { $BaseName%s%s, \"$BaseName%s%s\" },\n",
#				'_', $def, '_', $def);
		}
	} # LINE: while

	if($v) { printf(STDERR "translated $file\n");}

	close FILE;
}

