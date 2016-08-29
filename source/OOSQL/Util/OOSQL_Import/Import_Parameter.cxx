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
#include "Import.hxx"


// option case number
enum {
    DBNAME_OPTION = 1,
    VOLNAME_OPTION,
    CNFFILE_OPTION,
    DIR_OPTION,
    FULL_OPTION,
    INDEXES_OPTION,
    TABLES_OPTION,
    BULK_OPTION
};

char WHITE_SEPARATOR[] = " \t\n";
char VALUE_SEPARATOR[] = ",{}";

// option names
char DBNAME[]   = "DBNAME";
char VOLNAME[]  = "VOLNAME";
char CNFFILE[]  = "CNFFILE";
char DIR[]      = "DIR";
char FULL[]     = "FULL";
char INDEXES[]  = "INDEXES";
char TABLES[]   = "TABLES";
char BULK[]     = "BULK";

char badParamMsg[] = "\n\tYou must concatenate <parameter>, =, <value> without space.\n\ti.e  <parameter>=<value>";


Four import_CheckParamter(
    char            *param,         // IN
    char            *keyword)       // IN
{
    if (strlen(param) < strlen(keyword)+2)
        TOOL_ERR(eBADPARAMETER_IMPORT);

    if (param[strlen(keyword)] != '=')
        TOOL_ERR(eBADPARAMETER_IMPORT);

    if (param[strlen(keyword)+1] == ' ')
        TOOL_ERR(eBADPARAMETER_IMPORT);

    if (param[strlen(keyword)+1] == '\t')
        TOOL_ERR(eBADPARAMETER_IMPORT);

    if (param[strlen(keyword)+1] == '\n')
        TOOL_ERR(eBADPARAMETER_IMPORT);

    if (param[strlen(keyword)+1] == '{' && param[strlen(param)-1] != '}')
        TOOL_ERR(eBADPARAMETER_IMPORT);

    return eNOERROR;
}


Four import_SetParameter(
    char            *param,         // IN
    ImportConfig&   configuration)  // OUT
{
    Four    e;
    Four    option;
    char    paramBuffer[MAXSIZEOFPARAMETER];
    char    *token;


    // find option's case number
    if (strncmp(param, DBNAME, strlen(DBNAME)) == 0) 
    {
        e = import_CheckParamter(param, DBNAME);
        if (e < 0) 
        {
            configuration.errorMessage = "Error : You used bad parameter in ";
            configuration.errorMessage += DBNAME;
            configuration.errorMessage += " option.";
            configuration.errorMessage += badParamMsg;
            TOOL_ERR(eBADPARAMETER_IMPORT);
        }
        option = DBNAME_OPTION;
    }
    else if (strncmp(param, VOLNAME, strlen(VOLNAME)) == 0) 
    {
        e = import_CheckParamter(param, VOLNAME);
        if (e < 0) 
        {
            configuration.errorMessage = "Error : You used bad parameter in ";
            configuration.errorMessage += VOLNAME;
            configuration.errorMessage += " option.";
            configuration.errorMessage += badParamMsg;
            TOOL_ERR(eBADPARAMETER_IMPORT);
        }
        option = VOLNAME_OPTION;
    }
    else if (strncmp(param, DIR, strlen(DIR)) == 0) 
    {
        e = import_CheckParamter(param, DIR);
        if (e < 0) 
        {
            configuration.errorMessage = "Error : You used bad parameter in ";
            configuration.errorMessage += DIR;
            configuration.errorMessage += " option.";
            configuration.errorMessage += badParamMsg;
            TOOL_ERR(eBADPARAMETER_IMPORT);
        }
        option = DIR_OPTION;
    }
    else if (strncmp(param, FULL, strlen(FULL)) == 0) 
    {
        e = import_CheckParamter(param, FULL);
        if (e < 0) {
            configuration.errorMessage = "Error : You used bad parameter in ";
            configuration.errorMessage += FULL;
            configuration.errorMessage += " option.";
            configuration.errorMessage += badParamMsg;
            TOOL_ERR(eBADPARAMETER_IMPORT);
        }
        option = FULL_OPTION;
    }
    else if (strncmp(param, INDEXES, strlen(INDEXES)) == 0) 
    {
        e = import_CheckParamter(param, INDEXES);
        if (e < 0) {
            configuration.errorMessage = "Error : You used bad parameter in ";
            configuration.errorMessage += INDEXES;
            configuration.errorMessage += " option.";
            configuration.errorMessage += badParamMsg;
            TOOL_ERR(eBADPARAMETER_IMPORT);
        }
        option = INDEXES_OPTION;
    }
    else if (strncmp(param, TABLES, strlen(TABLES)) == 0) 
    {
        e = import_CheckParamter(param, TABLES);
        if (e < 0) {
            configuration.errorMessage = "Error : You used bad parameter in ";
            configuration.errorMessage += TABLES;
            configuration.errorMessage += " option.";
            configuration.errorMessage += badParamMsg;
            TOOL_ERR(eBADPARAMETER_IMPORT);
        }
        option = TABLES_OPTION;
    }
    else if (strncmp(param, BULK, strlen(BULK)) == 0) 
    {
        e = import_CheckParamter(param, BULK);
        if (e < 0) {
            configuration.errorMessage = "Error : You used bad parameter in ";
            configuration.errorMessage += BULK;
            configuration.errorMessage += " option.";
            configuration.errorMessage += badParamMsg;
            TOOL_ERR(eBADPARAMETER_IMPORT);
        }
        option = BULK_OPTION;
    }
    else 
    {
        configuration.errorMessage = "Error : You used bad parameter format.";
        configuration.errorMessage += badParamMsg;
        TOOL_ERR(eBADPARAMETER_IMPORT);
    }
   
    
    switch(option) 
    {
    case DBNAME_OPTION :
        configuration.databaseName = &param[strlen(DBNAME)+1];
        break;

    case VOLNAME_OPTION :
        configuration.volumeName = &param[strlen(VOLNAME)+1];
        break;

    case FULL_OPTION :  
        if (param[strlen(FULL)+1] == 'Y' || param[strlen(FULL)+1] == 'y')
            configuration.full = (Boolean)TRUE;
        else 
            configuration.full = (Boolean)FALSE;
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

    case BULK_OPTION :  
        if (param[strlen(BULK)+1] == 'Y' || param[strlen(BULK)+1] == 'y')
            configuration.bulkload = (Boolean)TRUE;
        else 
            configuration.bulkload = (Boolean)FALSE;
        break;

    case DIR_OPTION : 
        configuration.dirPath = &param[strlen(DIR)+1];
        break;

    default : 
        TOOL_ERR(eUNHANDLED_CASE_IMPORT);
        break;
    }

    return eNOERROR;
}




Four Import_InitParameters (
    ImportConfig&   configuration)  // OUT
{
    char*           envStr = NULL;

    envStr = getenv("ODYS_TEMP_PATH");

    // Default parameter setting
    configuration.dirPath =  ".";
    configuration.dirPath += DIRECTORY_SEPARATOR;
    if (envStr == NULL)
        configuration.tmpPath = TMP_DIRECTORY; 
    else
        configuration.tmpPath = (const char *)envStr;
    configuration.full = (Boolean)TRUE;
    configuration.indexes = (Boolean)TRUE;
    configuration.bulkload = (Boolean)FALSE;

    return eNOERROR;
}


Four Import_LoadParameters(
    int             argc,           // IN
    char            **argv,         // IN
    ImportConfig&   configuration)  // OUT
{
    FILE    *fp;
    Four    e;
    Four    argCount = 1;
    char    *chk;
    char    *token;
    char    paramBuffer[MAXSIZEOFPARAMETER];
    char    lineBuffer[MAXSIZEOFLINEBUFFER];


    // there is NO parameter
    if (argc == 1)
    {
        Import_DisplayHelp();
        exit(eNOERROR);
    }


    // read paramters
    while (argCount < argc) 
    {
        if (strncmp(argv[argCount], CNFFILE, strlen(CNFFILE)) == 0) 
        {
            // parameter check
            e = import_CheckParamter(argv[argCount], CNFFILE);
            if (e < eNOERROR) 
            {
                configuration.errorMessage = "Error : You used bad parameter in ";
                configuration.errorMessage += CNFFILE;
                configuration.errorMessage += " option.";
                configuration.errorMessage += badParamMsg;
                TOOL_ERR(eBADPARAMETER_IMPORT);
            }

            // configuration file open
            fp = Util_fopen(&argv[argCount][strlen(CNFFILE)+1], "rb");
            if (fp == NULL) 
            {
                configuration.errorMessage  = "Error : Can't open '";
                configuration.errorMessage += argv[argCount][strlen(CNFFILE)+1];
                configuration.errorMessage += "' file";
                TOOL_ERR(eCANNOT_OPEN_FILE_IMPORT);
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
                            TOOL_ERR(eCANNOT_CLOSE_FILE_IMPORT);
                        }
                        TOOL_ERR(eBADPARAMETER_IMPORT);
                    }
                    else
                    {
                        e = import_SetParameter(paramBuffer, configuration);
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
                TOOL_ERR(eCANNOT_CLOSE_FILE_IMPORT);
            }
        }
        else 
        {
            e = import_SetParameter(argv[argCount], configuration);
        TOOL_CHECK_ERR(e);
        }

        argCount++;
    }

    // constraint parameter
    if (configuration.databaseName == "") 
    {
        configuration.errorMessage  = "Error : Database name is undefined.";
        TOOL_ERR(eUNDEFINED_DATABASE_NAME_IMPORT);
    }
    if (configuration.volumeName == "") 
    {
        configuration.volumeName = configuration.databaseName; 
    }
    if (configuration.tables[0] == "" && configuration.full == FALSE) 
    {
        configuration.errorMessage  = "Error : Tables to be imported is undefined.";
        TOOL_ERR(eUNDEFINED_TABLES_IMPORT);
    }

    return eNOERROR;
}




void Import_DisplayHelp()
{
    fprintf(stderr, "\n");
    fprintf(stderr, "Import: \n");
    fprintf(stderr, "\n");
    fprintf(stderr, " Copyright (c) 1998 by Kyu-Young Whang. All rights reserved.\n");
    fprintf(stderr, "\n");
    fprintf(stderr, " To specify parameters, you use parameters:\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "    Format : OOSQL_Import <parameters list>\n");
    fprintf(stderr, "    --------------------------------------------------------------------\n");
    fprintf(stderr, "    <parameters> : <%s parameter>\n", DBNAME);
    fprintf(stderr, "                 | <%s parameter>\n", VOLNAME);
    fprintf(stderr, "                 | <%s parameter>\n", CNFFILE);
    fprintf(stderr, "                 | <%s parameter>\n", DIR);
    fprintf(stderr, "                 | <%s parameter>\n", FULL);
    fprintf(stderr, "                 | <%s parameter>\n", BULK);
    fprintf(stderr, "                 | <%s parameter>\n", INDEXES);
    fprintf(stderr, "                 | <%s parameter>\n", TABLES);
    fprintf(stderr, "    <%-10s parameter> : %s=<value>\n", DBNAME, DBNAME);
    fprintf(stderr, "    <%-10s parameter> : %s=<value>\n", VOLNAME, VOLNAME);
    fprintf(stderr, "    <%-10s parameter> : %s=<value>\n", CNFFILE, CNFFILE);
    fprintf(stderr, "    <%-10s parameter> : %s=<value>\n", DIR, DIR);
    fprintf(stderr, "    <%-10s parameter> : %s=<value>  (default value : Y)\n", FULL, FULL);
    fprintf(stderr, "    <%-10s parameter> : %s=<value>  (default value : N)\n", BULK, BULK);
    fprintf(stderr, "    <%-10s parameter> : %s=<value>  (default value : Y)\n", INDEXES, INDEXES);
    fprintf(stderr, "    <%-10s parameter> : %s={<value>,<value>,...,<value>}\n", TABLES, TABLES);
    fprintf(stderr, "\n");
    fprintf(stderr, "    Example1 : OOSQL_Import CNFFILE=imp.cnf\n");
    fprintf(stderr, "    Example2 : OOSQL_Import DBNAME=ICMS_DB FULL=Y BULK=Y INDEXES=N DIR=./export_result\n");
    fprintf(stderr, "    Example3 : OOSQL_Import DBNAME=ICMS_DB DIR=./export_result TABLES={Journal,Proceeding}\n");
    fprintf(stderr, "\n");
}    
