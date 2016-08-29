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

Four    procIndex;


Four import_CheckLogVolume (
    ImportConfig&   configuration)  // IN
{
    Four            e;
    char            ans;
    char*           envStr = NULL;

    envStr = getenv("COSMOS_LOG_VOLUME");
    if (envStr == NULL)
    {   
        fprintf(stderr, "*** Warning : log volume environment variable isn't defined ! ***\n");
        fprintf(stderr, "Do you wish to continue? [y/n] ");
        scanf("%c", &ans);

        if (ans == 'n' || ans == 'N')
        {
            TOOL_ERR(eLOGVOLUME_NOT_DEFINED_IMPORT);
        }
    }

    return eNOERROR;
}


Four Import_Initialize(
    ImportConfig&   configuration)  // IN
{
    Four            e;


    e = import_CheckLogVolume(configuration);
    TOOL_CHECK_ERR(e);

    e = import_ReadLog(configuration);
    TOOL_CHECK_ERR(e);

    e = import_WriteLog(configuration);
    TOOL_CHECK_ERR(e);

    
    e = OOSQL_CreateSystemHandle(&configuration.handle, &procIndex);
    if (e < 0) 
    {
        configuration.errorMessage = "Error : Can't create system handel";
        TOOL_ERR(eOOSQL_ERROR_IMPORT);
    }

    e = OOSQL_MountDB(&configuration.handle, (char *)(const char *)configuration.databaseName, 
                      &configuration.databaseId);
    if (e < 0) 
    {
        configuration.errorMessage = "Error : Can't mound database";
        OOSQL_DestroySystemHandle(&configuration.handle, procIndex);
        TOOL_ERR(eOOSQL_ERROR_IMPORT);
    }
                                                                        
    e = OOSQL_GetVolumeID(&configuration.handle, configuration.databaseId, (char*)(const char*)configuration.volumeName, &configuration.volumeId);
    if (e < 0) 
    {
        configuration.errorMessage = "Error : Can't get default volume";
        OOSQL_DismountDB(&configuration.handle, configuration.databaseId);
        OOSQL_DestroySystemHandle(&configuration.handle, procIndex);
        TOOL_ERR(eOOSQL_ERROR_IMPORT);
    }


    return eNOERROR;
}


Four import_TransBegin (
    ImportConfig&   configuration)  // IN
{
    Four            e;

    e = OOSQL_TransBegin(&configuration.handle, &configuration.xactId, X_RR_RR);
    if (e < 0) 
    {
        configuration.errorMessage = "Error : Can't begin transaction";
        OOSQL_DismountDB(&configuration.handle, configuration.databaseId);
        OOSQL_DestroySystemHandle(&configuration.handle, procIndex);
        TOOL_ERR(eOOSQL_ERROR_IMPORT);
    }
    
    return eNOERROR;
}


Four import_TransCommit (
    ImportConfig&   configuration)  // IN
{
    Four            e;

    e = OOSQL_TransCommit(&configuration.handle, &configuration.xactId);
    if (e < 0) 
    {
        configuration.errorMessage = "Error : Can't commit transaction";
        OOSQL_TransAbort(&configuration.handle, &configuration.xactId);
        OOSQL_DismountDB(&configuration.handle, configuration.databaseId);
        OOSQL_DestroySystemHandle(&configuration.handle, procIndex);
        TOOL_ERR(eOOSQL_ERROR_IMPORT);
    }

    return eNOERROR;
}



Four Import_Finalize (
    ImportConfig&   configuration)  // IN
{
    Four            e;


    e = OOSQL_DismountDB(&configuration.handle, configuration.databaseId);
    if (e < 0) 
    {
        configuration.errorMessage = "Error : Can't dismount database";
        OOSQL_DestroySystemHandle(&configuration.handle, procIndex);
        TOOL_ERR(eOOSQL_ERROR_IMPORT);
    }

    e = OOSQL_DestroySystemHandle(&configuration.handle, procIndex);
    if (e < 0) 
    {
        configuration.errorMessage = "Error : Can't destroy system handle";
        TOOL_ERR(eOOSQL_ERROR_IMPORT);
    }


    return eNOERROR;
}



Four Import_DeleteTmpFile (
    ImportConfig&   configuration)  // IN
{
    Four            e;
    char            commandStr[MAXCOMMANDLENGTH];


    // Delete *.tmp files 
    sprintf(commandStr, "/usr/bin/rm %s%s*.tmp",
            (const char *)configuration.dirPath,
            DIRECTORY_SEPARATOR);

    e = system((const char *)commandStr);
    if (e != NORMALEXIT)  TOOL_ERR(eFILE_EXEC_FAIL_IMPORT);


    if (configuration.indexes == TRUE)
    {
        // Delete *.oid files 
        sprintf(commandStr, "/usr/bin/rm %s%s*.oid",
                (const char *)configuration.dirPath,
                DIRECTORY_SEPARATOR);

        e = system((const char *)commandStr);
        if (e != NORMALEXIT)  TOOL_ERR(eFILE_EXEC_FAIL_IMPORT);
    }

    // Delete import.log files 
    sprintf(commandStr, "/usr/bin/rm %s%simport.log",
            (const char *)configuration.dirPath,
            DIRECTORY_SEPARATOR);

    e = system((const char *)commandStr);
    if (e != NORMALEXIT)  TOOL_ERR(eFILE_EXEC_FAIL_IMPORT);


    return eNOERROR;
}

