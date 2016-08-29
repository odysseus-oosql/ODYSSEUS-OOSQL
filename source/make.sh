#!/bin/tcsh

source setup

if ($1 == "clean") then
	cd $O_LOM_SERVER
	make clean

	cd $O_GEOM
	make clean

	cd $O_OOSQL_SERVER
	make clean
	
else
	echo "rm -rf $O_OOSQL_EXPORT"
	rm -rf $O_OOSQL_EXPORT
	
	echo "mkdir $O_OOSQL_EXPORT"
	mkdir $O_OOSQL_EXPORT
	
	echo "cd $O_OOSQL_EXPORT"
	cd $O_OOSQL_EXPORT
	
	echo "mkdir OOSQLDB temp include bin lib"
	mkdir OOSQLDB temp include bin lib
	
	cd $O_LOM_SERVER
	make clean
	make
	
	cd $O_GEOM
	make clean
	make
	
	cd $O_OOSQL_SERVER
	make clean
	make
	
	cd $O_OOSQL_EXPORT
	set setupfile_name = 'setup'
	echo "create $setupfile_name"
	echo "setenv OOSQL_HOME			$O_OOSQL_EXPORT" >> $setupfile_name
	echo 'setenv ODYS_TEMP_PATH		$OOSQL_HOME/temp' >> $setupfile_name
	echo 'setenv ODYS_OODB			$OOSQL_HOME/OOSQLDB' >> $setupfile_name
	echo ''  >> $setupfile_name
	echo 'setenv OOSQL_SERVER_PATH	$OOSQL_HOME' >> $setupfile_name
	echo ''  >> $setupfile_name
	echo 'set path = (. $OOSQL_HOME/bin $path)' >> $setupfile_name
	echo 'setenv IR_SYSTEM_PATH $OOSQL_HOME/bin' >> $setupfile_name
	echo '' >> $setupfile_name
	echo 'if ($?LD_LIBRARY_PATH == 0) then' >> $setupfile_name
	echo '	setenv LD_LIBRARY_PATH $OOSQL_HOME/lib' >> $setupfile_name
	echo 'else' >> $setupfile_name
	echo '	setenv LD_LIBRARY_PATH $OOSQL_HOME/lib:$LD_LIBRARY_PATH' >> $setupfile_name
	echo 'endif' >> $setupfile_name
endif
