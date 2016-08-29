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
#include "OOSQL_Mig.hxx"


// option case number
enum {
    DBNAME_OPTION = 1,
    VOLNAME_OPTION,
    CNFFILE_OPTION,
    TEST_OPTION
};

char WHITE_SEPARATOR[] = " \t\n";
char VALUE_SEPARATOR[] = ",{}";


// option names
char DBNAME[]       = "DBNAME";
char VOLNAME[]      = "VOLNAME";
char CNFFILE[]      = "CNFFILE";
char TEST[]      	= "TEST";

char badParamMsg[] = "\n\tYou must concatenate <parameter>, =, <value> without space.\n\ti.e  <parameter>=<value>\n";



Four Mig_InitParameters(
    MigConfig&	config)		// INOUT
{	
	config.databaseName[0] = '\0';
	config.volumeName[0] = '\0';
	config.targetOdysseusVersion = ODYSSEUS_ALTERTABLE_NOSQL99;
	config.testExec = (Boolean)FALSE;

    return eNOERROR;
}


Four Mig_LoadParameters(
    int         argc,       // IN
    char        **argv,     // IN
    MigConfig&	config)		// OUT
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
        Mig_DisplayHelp();
        exit(eNOERROR);
    }

    // read paramters
    while (argCount < argc) 
    {
        if (strncmp(argv[argCount], CNFFILE, strlen(CNFFILE)) == 0) 
        {
            // parameter check
            e = mig_CheckParameter(argv[argCount], CNFFILE);
            if (e < eNOERROR) 
            {
                sprintf(config.errorMessage, "Error : You used bad parameter in %s option. %s",
					CNFFILE, badParamMsg);
                TOOL_ERR(eBADPARAMETER_MIG);
            }

            // config file open
            fp = fopen(&argv[argCount][strlen(CNFFILE)+1], "r");
            if (fp == NULL) 
            {
                sprintf(config.errorMessage, "Error : Can't open '%s' file.\n", 
					&argv[argCount][strlen(CNFFILE)+1]);
                TOOL_ERR(eCANNOT_OPEN_FILE_MIG);
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
                        sprintf(config.errorMessage, "Error : You used bad parameter format. %s", 
							badParamMsg);
                        TOOL_ERR(eBADPARAMETER_MIG);
                    }
                    else
                    {
                        e = mig_SetParameter(paramBuffer, config);
                        TOOL_CHECK_ERR(e);
                    }
                }

                chk = fgets(lineBuffer, MAXSIZEOFLINEBUFFER, fp);
            }

            // config file close
            e = fclose(fp);
            if (e == EOF) 
            {
                sprintf(config.errorMessage, "Error : Can't close '%s' file.\n", 
					argv[argCount][strlen(CNFFILE)+1]);
                TOOL_ERR(eCANNOT_CLOSE_FILE_MIG);
            }
        }
        else 
        {
            e = mig_SetParameter(argv[argCount], config);
        	TOOL_CHECK_ERR(e);
        }

        argCount++;
    }


    // Check parameter constraint 
    if (config.databaseName[0] == '\0') 
    {
        sprintf(config.errorMessage, "Error : Database name is undefined.");
        TOOL_ERR(eUNDEFINED_DATABASE_NAME_MIG);
    }
    if (config.volumeName[0] == '\0') 
    {
        strcpy(config.volumeName, config.databaseName);
    }


	/* debug */
	/*
	printf("DBNAME = %s\n", config.databaseName);
	printf("VOLNAME = %s\n", config.volumeName);
	*/

    return eNOERROR;
}



void Mig_DisplayHelp()
{
    fprintf(stderr, "\n");
    fprintf(stderr, "OOSQL_Mig: \n");
    fprintf(stderr, "\n");
    fprintf(stderr, " Copyright (C) 1998 by Kyu-Young Whang. All rights reserved.\n");
    fprintf(stderr, "\n");
    fprintf(stderr, " To specify parameters, you use parameters:\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "    Format : OOSQL_Mig <parameters list>\n");
    fprintf(stderr, "    --------------------------------------------------------------------\n");
    fprintf(stderr, "    <parameters> : <%s parameter>\n", DBNAME); 
    fprintf(stderr, "                 | <%s parameter>\n", VOLNAME); 
    fprintf(stderr, "                 | <%s parameter>\n", CNFFILE); 
    fprintf(stderr, "                 | <%s parameter>\n", TEST); 
    fprintf(stderr, "    <%-12s parameter> : %s=<value>\n", DBNAME, DBNAME);
    fprintf(stderr, "    <%-12s parameter> : %s=<value>\n", VOLNAME, VOLNAME);
    fprintf(stderr, "    <%-12s parameter> : %s=<value>\n", CNFFILE, CNFFILE);
    fprintf(stderr, "    <%-12s parameter> : %s=<value>\n", TEST, TEST);
    fprintf(stderr, "\n");
    fprintf(stderr, "    Example1 : OOSQL_Mig CNFFILE=mig.cnf\n");
    fprintf(stderr, "    Example2 : OOSQL_Mig DBNAME=XML_DB VOLNAME=XML_DB\n");
    fprintf(stderr, "    Example3 : OOSQL_Mig DBNAME=XML_DB VOLNAME=XML_VOL TEST=Y\n");
    fprintf(stderr, "\n");
}


Four mig_CheckParameter(
    char            *param,         // IN
    char            *keyword)       // IN
{
    if (strlen(param) < strlen(keyword)+2)
        TOOL_ERR(eBADPARAMETER_MIG);

    if (param[strlen(keyword)] != '=')
        TOOL_ERR(eBADPARAMETER_MIG);

    if (param[strlen(keyword)+1] == ' ')
        TOOL_ERR(eBADPARAMETER_MIG);

    if (param[strlen(keyword)+1] == '\t')
        TOOL_ERR(eBADPARAMETER_MIG);

    if (param[strlen(keyword)+1] == '\n')
        TOOL_ERR(eBADPARAMETER_MIG);

    if (param[strlen(keyword)+1] == '{' && param[strlen(param)-1] != '}')
        TOOL_ERR(eBADPARAMETER_MIG);

    return eNOERROR;
}


Four mig_SetParameter(
    char            *param,     // IN
    MigConfig&   	config)		// OUT
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
        e = mig_CheckParameter(param, DBNAME);
        if (e < eNOERROR) 
        {
            sprintf(config.errorMessage, "Error : You used bad parameter in %s option. %s",
				DBNAME, badParamMsg);
            TOOL_ERR(eBADPARAMETER_MIG);
        }
        option = DBNAME_OPTION;
    }
    else if (strncmp(param, VOLNAME, strlen(VOLNAME)) == 0) 
    {
        e = mig_CheckParameter(param, VOLNAME);
        if (e < eNOERROR) 
        {
            sprintf(config.errorMessage, "Error : You used bad parameter in %s option. %s", 
				VOLNAME, badParamMsg);
            TOOL_ERR(eBADPARAMETER_MIG);
        }
        option = VOLNAME_OPTION;
    }
    else if (strncmp(param, TEST, strlen(TEST)) == 0) 
    {
        e = mig_CheckParameter(param, TEST);
        if (e < eNOERROR) 
        {
            sprintf(config.errorMessage, "Error : You used bad parameter in %s option. %s", 
				TEST, badParamMsg);
            TOOL_ERR(eBADPARAMETER_MIG);
        }
        option = TEST_OPTION;
    }
    else 
    {
        sprintf(config.errorMessage, "Error : You used bad parameter format. %s",
			badParamMsg);
        TOOL_ERR(eBADPARAMETER_MIG);
    }
   

    switch(option) 
    {
    case DBNAME_OPTION :
        strcpy(config.databaseName, &param[strlen(DBNAME)+1]);
        break;

    case VOLNAME_OPTION :
        strcpy(config.volumeName, &param[strlen(VOLNAME)+1]);
        break;

    case TEST_OPTION :
		if (param[strlen(TEST)+1] == 'Y' || param[strlen(TEST)+1] == 'y')
        	config.testExec = (Boolean)TRUE; 
		else
        	config.testExec = (Boolean)FALSE; 
        break;

    default : 
        TOOL_ERR(eUNHANDLED_CASE_MIG);
        break;
    }


    return eNOERROR;
}



