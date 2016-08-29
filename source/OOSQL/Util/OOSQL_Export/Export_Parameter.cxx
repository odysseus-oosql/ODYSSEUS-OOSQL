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

// Include Header file
#include "Export.hxx"

// option case number
enum {
    DBNAME_OPTION = 1,
    CNFFILE_OPTION,
    DIR_OPTION,
    FULL_OPTION,
    DATA_OPTION,
    INDEXES_OPTION,
    TABLES_OPTION,
    BYINDEX_OPTION,
    SEQRET_OPTION,
    TEXT_OPTION,
    VOLNAME_OPTION,
    DFILESIZE_OPTION,
    IFILESIZE_OPTION,
    POSITIONLIST_OPTION
};

char WHITE_SEPARATOR[] = " \t\n";
char VALUE_SEPARATOR[] = ",{}";


// option names
char DBNAME[]       = "DBNAME";
char CNFFILE[]      = "CNFFILE";
char DIR[]          = "DIR";
char FULL[]         = "FULL";
char DATA[]         = "DATA";
char INDEXES[]      = "INDEXES";
char TABLES[]       = "TABLES";
char BYINDEX[]      = "BYINDEX";
char SEQRET[]       = "SEQRET";
char TEXT[]         = "TEXT";
char VOLNAME[]      = "VOLNAME";
char DFILESIZE[]    = "DFILESIZE";
char IFILESIZE[]    = "IFILESIZE";
char POSITIONLIST[] = "POSITIONLIST";

char badParamMsg[] = "\n\tYou must concatenate <parameter>, =, <value> without space.\n\ti.e  <parameter>=<value>";


Four export_CheckParamter(
    char            *param,         // IN
    char            *keyword)       // IN
{
    if (strlen(param) < strlen(keyword)+2)
        TOOL_ERR(eBADPARAMETER_EXPORT);

    if (param[strlen(keyword)] != '=')
        TOOL_ERR(eBADPARAMETER_EXPORT);

    if (param[strlen(keyword)+1] == ' ')
        TOOL_ERR(eBADPARAMETER_EXPORT);

    if (param[strlen(keyword)+1] == '\t')
        TOOL_ERR(eBADPARAMETER_EXPORT);

    if (param[strlen(keyword)+1] == '\n')
        TOOL_ERR(eBADPARAMETER_EXPORT);

    if (param[strlen(keyword)+1] == '{' && param[strlen(param)-1] != '}')
        TOOL_ERR(eBADPARAMETER_EXPORT);

    return eNOERROR;
}


Four export_SetParameter(
    char            *param,         // IN
    ExportConfig&   configuration)  // OUT
{
    Four            e;
    Four            i;
    Four            option;
    char            paramBuffer[MAXSIZEOFPARAMETER];
    char            fileSizeString[100];
    char            *token;
    Four            sizeUnit;


    // find option's case number
    if (strncmp(param, DBNAME, strlen(DBNAME)) == 0) 
    {
        e = export_CheckParamter(param, DBNAME);
        if (e < eNOERROR) 
        {
            configuration.errorMessage = "Error : You used bad parameter in ";
            configuration.errorMessage += DBNAME;
            configuration.errorMessage += " option.";
            configuration.errorMessage += badParamMsg;
            TOOL_ERR(eBADPARAMETER_EXPORT);
        }
        option = DBNAME_OPTION;
    }
    else if (strncmp(param, DIR, strlen(DIR)) == 0) 
    {
        e = export_CheckParamter(param, DIR);
        if (e < eNOERROR) 
        {
            configuration.errorMessage = "Error : You used bad parameter in ";
            configuration.errorMessage += DIR;
            configuration.errorMessage += " option.";
            configuration.errorMessage += badParamMsg;
            TOOL_ERR(eBADPARAMETER_EXPORT);
        }
        option = DIR_OPTION;
    }
    else if (strncmp(param, FULL, strlen(FULL)) == 0) 
    {
        e = export_CheckParamter(param, FULL);
        if (e < eNOERROR) 
        {
            configuration.errorMessage = "Error : You used bad parameter in ";
            configuration.errorMessage += FULL;
            configuration.errorMessage += " option.";
            configuration.errorMessage += badParamMsg;
            TOOL_ERR(eBADPARAMETER_EXPORT);
        }
        option = FULL_OPTION;
    }
    else if (strncmp(param, DATA, strlen(DATA)) == 0) 
    {
        e = export_CheckParamter(param, DATA);
        if (e < eNOERROR) 
        {
            configuration.errorMessage = "Error : You used bad parameter in ";
            configuration.errorMessage += DATA;
            configuration.errorMessage += " option.";
            configuration.errorMessage += badParamMsg;
            TOOL_ERR(eBADPARAMETER_EXPORT);
        }
        option = DATA_OPTION;
    }
    else if (strncmp(param, INDEXES, strlen(INDEXES)) == 0) 
    {
        e = export_CheckParamter(param, INDEXES);
        if (e < eNOERROR) 
        {
            configuration.errorMessage = "Error : You used bad parameter in ";
            configuration.errorMessage += INDEXES;
            configuration.errorMessage += " option.";
            configuration.errorMessage += badParamMsg;
            TOOL_ERR(eBADPARAMETER_EXPORT);
        }
        option = INDEXES_OPTION;
    }
    else if (strncmp(param, SEQRET, strlen(SEQRET)) == 0) 
    {
        e = export_CheckParamter(param, SEQRET);
        if (e < eNOERROR) 
        {
            configuration.errorMessage = "Error : You used bad parameter in ";
            configuration.errorMessage += SEQRET;
            configuration.errorMessage += " option.";
            configuration.errorMessage += badParamMsg;
            TOOL_ERR(eBADPARAMETER_EXPORT);
        }
        option = SEQRET_OPTION;
    }
    else if (strncmp(param, TABLES, strlen(TABLES)) == 0) 
    {
        e = export_CheckParamter(param, TABLES);
        if (e < eNOERROR) 
        {
            configuration.errorMessage = "Error : You used bad parameter in ";
            configuration.errorMessage += TABLES;
            configuration.errorMessage += " option.";
            configuration.errorMessage += badParamMsg;
            TOOL_ERR(eBADPARAMETER_EXPORT);
        }
        option = TABLES_OPTION;
    }
    else if (strncmp(param, BYINDEX, strlen(BYINDEX)) == 0) 
    {
        e = export_CheckParamter(param, BYINDEX);
        if (e < eNOERROR) 
        {
            configuration.errorMessage = "Error : You used bad parameter in ";
            configuration.errorMessage += BYINDEX;
            configuration.errorMessage += " option.";
            configuration.errorMessage += badParamMsg;
            TOOL_ERR(eBADPARAMETER_EXPORT);
        }
        option = BYINDEX_OPTION;
    }
    else if (strncmp(param, TEXT, strlen(TEXT)) == 0) 
    {
        e = export_CheckParamter(param, TEXT);
        if (e < eNOERROR) 
        {
            configuration.errorMessage = "Error : You used bad parameter in ";
            configuration.errorMessage += TEXT;
            configuration.errorMessage += " option.";
            configuration.errorMessage += badParamMsg;
            TOOL_ERR(eBADPARAMETER_EXPORT);
        }
        option = TEXT_OPTION;
    }
    else if (strncmp(param, VOLNAME, strlen(VOLNAME)) == 0) 
    {
        e = export_CheckParamter(param, VOLNAME);
        if (e < eNOERROR) 
        {
            configuration.errorMessage = "Error : You used bad parameter in ";
            configuration.errorMessage += VOLNAME;
            configuration.errorMessage += " option.";
            configuration.errorMessage += badParamMsg;
            TOOL_ERR(eBADPARAMETER_EXPORT);
        }
        option = VOLNAME_OPTION;
    }
    else if (strncmp(param, DFILESIZE, strlen(DFILESIZE)) == 0) 
    {
        e = export_CheckParamter(param, DFILESIZE);
        if (e < eNOERROR) 
        {
            configuration.errorMessage = "Error : You used bad parameter in ";
            configuration.errorMessage += DFILESIZE;
            configuration.errorMessage += " option.";
            configuration.errorMessage += badParamMsg;
            TOOL_ERR(eBADPARAMETER_EXPORT);
        }
        option = DFILESIZE_OPTION;
    }
    else if (strncmp(param, IFILESIZE, strlen(IFILESIZE)) == 0) 
    {
        e = export_CheckParamter(param, IFILESIZE);
        if (e < eNOERROR) 
        {
            configuration.errorMessage = "Error : You used bad parameter in ";
            configuration.errorMessage += IFILESIZE;
            configuration.errorMessage += " option.";
            configuration.errorMessage += badParamMsg;
            TOOL_ERR(eBADPARAMETER_EXPORT);
        }
        option = IFILESIZE_OPTION;
    }
    else if (strncmp(param, POSITIONLIST, strlen(POSITIONLIST)) == 0) 
    {
        e = export_CheckParamter(param, POSITIONLIST);
        if (e < eNOERROR) 
        {
            configuration.errorMessage = "Error : You used bad parameter in ";
            configuration.errorMessage += POSITIONLIST;
            configuration.errorMessage += " option.";
            configuration.errorMessage += badParamMsg;
            TOOL_ERR(eBADPARAMETER_EXPORT);
        }
        option = POSITIONLIST_OPTION;
    }
    else 
    {
        configuration.errorMessage = "Error : You used bad parameter format.";
        configuration.errorMessage += badParamMsg;
        TOOL_ERR(eBADPARAMETER_EXPORT);
    }
   

    switch(option) 
    {
    case DBNAME_OPTION :
        configuration.databaseName = &param[strlen(DBNAME)+1];
        break;

    case VOLNAME_OPTION :
        configuration.volumeName = &param[strlen(VOLNAME)+1];
        break;

    case DFILESIZE_OPTION :
        strcpy(fileSizeString, &param[strlen(DFILESIZE)+1]);

        if      (fileSizeString[strlen(fileSizeString)-1] == 'G' ||
                 fileSizeString[strlen(fileSizeString)-1] == 'g')
        {
            sizeUnit = 1024 * 1024 * 1024;
        }
        else if (fileSizeString[strlen(fileSizeString)-1] == 'M' ||
                 fileSizeString[strlen(fileSizeString)-1] == 'm')
        {
            sizeUnit = 1024 * 1024;
        }
        else if (fileSizeString[strlen(fileSizeString)-1] == 'K' ||
                 fileSizeString[strlen(fileSizeString)-1] == 'k')
        {
            sizeUnit = 1024;
        }
        else
        {
            sizeUnit = 1;
        }
        fileSizeString[strlen(fileSizeString)-1] = '\0';
        configuration.dataFileSize = (filepos_t)sizeUnit * (filepos_t)atol(fileSizeString);

        break;

    case IFILESIZE_OPTION :
        strcpy(fileSizeString, &param[strlen(IFILESIZE)+1]);

        if      (fileSizeString[strlen(fileSizeString)-1] == 'G' ||
                 fileSizeString[strlen(fileSizeString)-1] == 'g')
        {
            sizeUnit = 1024 * 1024 * 1024;
        }
        else if (fileSizeString[strlen(fileSizeString)-1] == 'M' ||
                 fileSizeString[strlen(fileSizeString)-1] == 'm')
        {
            sizeUnit = 1024 * 1024;
        }
        else if (fileSizeString[strlen(fileSizeString)-1] == 'K' ||
                 fileSizeString[strlen(fileSizeString)-1] == 'k')
        {
            sizeUnit = 1024;
        }
        else
        {
            sizeUnit = 1;
        }
        fileSizeString[strlen(fileSizeString)-1] = '\0';
        configuration.indexFileSize = (filepos_t)sizeUnit * (filepos_t)atol(fileSizeString);
        break;

    case FULL_OPTION :  
        if (param[strlen(FULL)+1] == 'Y' || param[strlen(FULL)+1] == 'y')
            configuration.full = (Boolean)TRUE;
        else 
            configuration.full = (Boolean)FALSE;
        break;

    case DATA_OPTION : 
        if (param[strlen(DATA)+1] == 'Y' || param[strlen(DATA)+1] == 'y')
            configuration.data = (Boolean)TRUE;
        else 
            configuration.data = (Boolean)FALSE;
        break;

    case INDEXES_OPTION : 
        if (param[strlen(INDEXES)+1] == 'Y' || param[strlen(INDEXES)+1] == 'y')
            configuration.indexes = (Boolean)TRUE;
        else 
            configuration.indexes = (Boolean)FALSE;
        break;


    case TABLES_OPTION : 
        token = strtok(&param[strlen(TABLES)+1], VALUE_SEPARATOR);
        while (token != NULL) 
        {
            strcpy(paramBuffer, token);
            configuration.tables.add(String(paramBuffer));

            token = strtok(NULL, VALUE_SEPARATOR);
        }
        break;

    case BYINDEX_OPTION : 
        token = strtok(&param[strlen(BYINDEX)+1], VALUE_SEPARATOR);
        while (token != NULL) 
        {
            ExportByIndex       byindex;

            strcpy(paramBuffer, token);

            for (i = 0; i < strlen(paramBuffer); i++)
                if (paramBuffer[i] == ':')      break;

            // if no index is indicated, this parameter is invalid.
            if (i == strlen(paramBuffer))
            {
                configuration.errorMessage = "Error : You used bad parameter in ";
                configuration.errorMessage += BYINDEX;
                configuration.errorMessage += " option.";
                configuration.errorMessage += badParamMsg;
                TOOL_ERR(eBADPARAMETER_EXPORT);
            }
            else
            {
                paramBuffer[i] = '\0';
                byindex.table = paramBuffer;
                byindex.index = &paramBuffer[i+1];
                configuration.byIndexes.add(byindex);
            }

            token = strtok(NULL, VALUE_SEPARATOR);
        }
        break;

    case DIR_OPTION : 
        configuration.dirPath = &param[strlen(DIR)+1];
        break;

    case SEQRET_OPTION : 
        if (param[strlen(SEQRET)+1] == 'Y' || param[strlen(SEQRET)+1] == 'y')
            configuration.seqret = (Boolean)TRUE;
        else 
            configuration.seqret = (Boolean)FALSE;
        break;

    case TEXT_OPTION :  
        if (param[strlen(TEXT)+1] == 'Y' || param[strlen(TEXT)+1] == 'y')
            configuration.expType = TEXT_EXPORT;
        else 
            configuration.expType = BINARY_EXPORT;
        break;

    case POSITIONLIST_OPTION :  
        if (param[strlen(POSITIONLIST)+1] == 'Y' || param[strlen(POSITIONLIST)+1] == 'y')
            configuration.postingList = (Boolean)TRUE;
        else 
            configuration.postingList = (Boolean)FALSE;
        break;


    default : 
        TOOL_ERR(eUNHANDLED_CASE_EXPORT);
        break;
    }


    return eNOERROR;
}



Four Export_InitParameters (
    ExportConfig&   configuration)  // OUT
{
    configuration.dirPath   =  ".";
    configuration.dirPath  += DIRECTORY_SEPARATOR;
    configuration.full      = (Boolean)TRUE;
    configuration.data      = (Boolean)TRUE;
    configuration.indexes   = (Boolean)FALSE;
    configuration.seqret    = (Boolean)TRUE;
    configuration.postingList = (Boolean)FALSE;
    configuration.expType   = BINARY_EXPORT;
    configuration.dataFileSize = (filepos_t)MAXDATAFILESIZE;
    configuration.indexFileSize = (filepos_t)MAXINDEXFILESIZE; 


    return eNOERROR;
}



Four Export_LoadParameters(
    int             argc,           // IN
    char            **argv,         // IN
    ExportConfig&   configuration)  // OUT
{
    Four            e;
    FILE            *fp;
    Four            argCount = 1;
    char            *chk;
    char            *token;
    char            paramBuffer[MAXSIZEOFPARAMETER];
    char            lineBuffer[MAXSIZEOFLINEBUFFER];


    // there is NO parameter
    if (argc == 1)
    {
        Export_DisplayHelp();
        exit(eNOERROR);
    }

    // read paramters
    while (argCount < argc) 
    {
        if (strncmp(argv[argCount], CNFFILE, strlen(CNFFILE)) == 0) 
        {
            // parameter check
            e = export_CheckParamter(argv[argCount], CNFFILE);
            if (e < eNOERROR) 
            {
                configuration.errorMessage = "Error : You used bad parameter in ";
                configuration.errorMessage += CNFFILE;
                configuration.errorMessage += " option.";
                configuration.errorMessage += badParamMsg;
                TOOL_ERR(eBADPARAMETER_EXPORT);
            }

            // configuration file open
            fp = Util_fopen(&argv[argCount][strlen(CNFFILE)+1], "r");
            if (fp == NULL) 
            {
                configuration.errorMessage  = "Error : Can't open '";
                configuration.errorMessage += &argv[argCount][strlen(CNFFILE)+1];
                configuration.errorMessage += "' file";
                TOOL_ERR(eCANNOT_OPEN_FILE_EXPORT);
            }

            // read paramters
            chk = fgets(lineBuffer, MAXSIZEOFLINEBUFFER, fp);
            while (chk != NULL)  
            {
                token = strtok(lineBuffer, WHITE_SEPARATOR);
                if (token != NULL)  
                {
                    strcpy(paramBuffer, token);

                    if (strchr(paramBuffer, '=') == NULL)
                    {
                        configuration.errorMessage  = "Error : You used bad parameter format.";
                        configuration.errorMessage += badParamMsg;
                        e = Util_fclose(fp);
                        if (e == EOF) 
                        {
                            configuration.errorMessage  = "Error : Can't close '";
                            configuration.errorMessage += argv[argCount][strlen(CNFFILE)+1];
                            configuration.errorMessage += "' file";
                            TOOL_ERR(eCANNOT_CLOSE_FILE_EXPORT);
                        }
                        TOOL_ERR(eBADPARAMETER_EXPORT);
                    }
                    else
                    {
                        e = export_SetParameter(paramBuffer, configuration);
                        TOOL_CHECK_ERR(e);
                    }
                }

                chk = fgets(lineBuffer, MAXSIZEOFLINEBUFFER, fp);
            }

            // configuration file close
            e = Util_fclose(fp);
            if (e == EOF) 
            {
                configuration.errorMessage  = "Error : Can't close '";
                configuration.errorMessage += argv[argCount][strlen(CNFFILE)+1];
                configuration.errorMessage += "' file";
                TOOL_ERR(eCANNOT_CLOSE_FILE_EXPORT);
            }
        }
        else 
        {
            e = export_SetParameter(argv[argCount], configuration);
        TOOL_CHECK_ERR(e);
        }

        argCount++;
    }


    // Check parameter constraint 
    if (configuration.databaseName == "") 
    {
        configuration.errorMessage  = "Error : Database name is undefined.";
        TOOL_ERR(eUNDEFINED_DATABASE_NAME_EXPORT);
    }
    if (configuration.volumeName == "") 
    {
        configuration.volumeName = configuration.databaseName;
    }
    if (configuration.tables.numberOfItems() == 0 && configuration.full == FALSE) 
    {
        configuration.errorMessage  = "Error : Tables to be exported is undefined.";
        TOOL_ERR(eUNDEFINED_TABLES_EXPORT);
    }
	if (configuration.tables.numberOfItems() > 0 && configuration.full == TRUE)
	{
		configuration.full = (Boolean)FALSE;	
	}

#ifndef USE_LARGE_FILE      /* if not using large file, check file size parameter */
    if (configuration.dataFileSize <= (filepos_t)0)
    {
        configuration.errorMessage  = "Error : Given data file size is too small.";
        TOOL_ERR(eTOO_BIG_DATAFILESIZE);
    }
    if (configuration.indexFileSize <= (filepos_t)0) 
    {
        configuration.errorMessage  = "Error : Given text index file size is too small.";
        TOOL_ERR(eTOO_BIG_INDEXFILESIZE);
    }

    if (configuration.dataFileSize > (filepos_t)MAXDATAFILESIZE) 
    {
        configuration.errorMessage  = "Error : Given data file size is too big.";
        TOOL_ERR(eTOO_BIG_DATAFILESIZE);
    }
    if (configuration.indexFileSize > (filepos_t)MAXINDEXFILESIZE) 
    {
        configuration.errorMessage  = "Error : Given text index file size is too big.";
        TOOL_ERR(eTOO_BIG_INDEXFILESIZE);
    }
#endif

    return eNOERROR;
}



void Export_DisplayHelp ()
{
    fprintf(stderr, "\n");
    fprintf(stderr, "Export: \n");
    fprintf(stderr, "\n");
    fprintf(stderr, " Copyright (C) 1998 by Kyu-Young Whang. All rights reserved.\n");
    fprintf(stderr, "\n");
    fprintf(stderr, " To specify parameters, you use parameters:\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "    Format : OOSQL_Export <parameters list>\n");
    fprintf(stderr, "    --------------------------------------------------------------------\n");
    fprintf(stderr, "    <parameters> : <%s parameter>\n", DBNAME); 
    fprintf(stderr, "                 | <%s parameter>\n", VOLNAME); 
    fprintf(stderr, "                 | <%s parameter>\n", CNFFILE); 
    fprintf(stderr, "                 | <%s parameter>\n", DIR); 
    fprintf(stderr, "                 | <%s parameter>\n", FULL);
    fprintf(stderr, "                 | <%s parameter>\n", TEXT);
    fprintf(stderr, "                 | <%s parameter>\n", INDEXES);
    fprintf(stderr, "                 | <%s parameter>\n", SEQRET);
    fprintf(stderr, "                 | <%s parameter>\n", TABLES);
    fprintf(stderr, "                 | <%s parameter>\n", BYINDEX);
    fprintf(stderr, "                 | <%s parameter>\n", DFILESIZE);
    fprintf(stderr, "                 | <%s parameter>\n", IFILESIZE);
    fprintf(stderr, "                 | <%s parameter>\n", POSITIONLIST);
    fprintf(stderr, "    <%-12s parameter> : %s=<value>\n", DBNAME, DBNAME);
    fprintf(stderr, "    <%-12s parameter> : %s=<value>\n", VOLNAME, VOLNAME);
    fprintf(stderr, "    <%-12s parameter> : %s=<value>\n", CNFFILE, CNFFILE);
    fprintf(stderr, "    <%-12s parameter> : %s=<value>\n", DIR, DIR);
    fprintf(stderr, "    <%-12s parameter> : %s=<value>  (default value : Y)\n", FULL, FULL);
    fprintf(stderr, "    <%-12s parameter> : %s=<value>  (default value : N)\n", TEXT, TEXT);
    fprintf(stderr, "    <%-12s parameter> : %s=<value>  (default value : Y)\n", INDEXES, INDEXES);
    fprintf(stderr, "    <%-12s parameter> : %s=<value>  (default value : Y)\n", SEQRET, SEQRET);
    fprintf(stderr, "    <%-12s parameter> : %s=<value>  (default value : maxinum OS file size)\n", DFILESIZE, DFILESIZE);
    fprintf(stderr, "    <%-12s parameter> : %s=<value>  (default value : maxinum OS file size)\n", IFILESIZE, IFILESIZE);
    fprintf(stderr, "    <%-12s parameter> : %s=<value>  (default value : N)\n", POSITIONLIST, POSITIONLIST);
    fprintf(stderr, "    <%-12s parameter> : %s={<value>,<value>,...,<value>}\n", TABLES, TABLES);
    fprintf(stderr, "    <%-12s parameter> : %s={<table name>:<index name>,...,<table name>:<index name>}\n", BYINDEX, BYINDEX);
    fprintf(stderr, "\n");
    fprintf(stderr, "    Example1 : OOSQL_Export CNFFILE=exp.cnf\n");
    fprintf(stderr, "    Example2 : OOSQL_Export DBNAME=ICMS_DB FULL=Y TEXT=Y INDEXES=N DIR=./export_result\n");
    fprintf(stderr, "    Example3 : OOSQL_Export DBNAME=ICMS_DB DIR=./export_result TABLES={Journal,Proceeding}\n");
    fprintf(stderr, "    Example4 : OOSQL_Export DBNAME=ICMS_DB DIR=./export_result DFILESIZE=1G POSITIONLIST=N\n");
    fprintf(stderr, "\n");
}
