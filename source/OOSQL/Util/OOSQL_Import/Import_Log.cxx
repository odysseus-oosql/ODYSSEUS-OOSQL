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



Four import_WriteLog (
    ImportConfig&       configuration)      // IN
{
    Four                i;
    Four                e;
    FILE*               logFp;
    char                logFileName[MAXFILENAME];


    // make log file name
    strcpy(logFileName, (const char *)configuration.dirPath);
    strcat(logFileName, DIRECTORY_SEPARATOR);
    strcat(logFileName, LOGFILENAME);

    // Open data file to write
    logFp = Util_fopen(logFileName, "wb");
    if (logFp == NULL)
    {
        configuration.errorMessage  = "Can't open '";
        configuration.errorMessage += logFileName;
        configuration.errorMessage += "' file";
        TOOL_ERR(eCANNOT_OPEN_FILE_IMPORT);
    }

    Util_fseek(logFp, (filepos_t)(0), SEEK_SET);
    fwrite(&configuration.importLog.mainImportPhase, sizeof(Four), 1, logFp);
    fwrite(&configuration.importLog.dataImportPhase, sizeof(Four), 1, logFp);
    fwrite(&configuration.importLog.textIndexConvertPhase, sizeof(Four), 1, logFp);
    fwrite(&configuration.importLog.textIndexImportPhase, sizeof(Four), 1, logFp);

    // Close data file to write
    e = Util_fclose(logFp);
    if (e == EOF)
    {
        configuration.errorMessage  = "Can't close '";
        configuration.errorMessage += logFileName;
        configuration.errorMessage += "' file";
        TOOL_ERR(eCANNOT_CLOSE_FILE_IMPORT);
    }

    return eNOERROR;
}



Four import_ReadLog (
    ImportConfig&       configuration)      // IN
{
    Four                i;
    Four                e;
    FILE*               logFp;
    char                logFileName[MAXFILENAME];


    // II-1. Open Log File and Read phase in last export
    strcpy(logFileName, (const char *)configuration.dirPath);
    strcat(logFileName, DIRECTORY_SEPARATOR);
    strcat(logFileName, LOGFILENAME);

    logFp = Util_fopen(logFileName, "rb");
    if (logFp == NULL)
    {
        configuration.importLog.mainImportPhase         = BEGINOFIMPORT;
        configuration.importLog.dataImportPhase         = BEGINOFIMPORT;
        configuration.importLog.textIndexConvertPhase   = BEGINOFIMPORT;
        configuration.importLog.textIndexImportPhase    = BEGINOFIMPORT;
    }
    else
    {
        Util_fseek(logFp, (filepos_t)(0), SEEK_SET);
        fread(&configuration.importLog.mainImportPhase, sizeof(Four), 1, logFp);
        fread(&configuration.importLog.dataImportPhase, sizeof(Four), 1, logFp);
        fread(&configuration.importLog.textIndexConvertPhase, sizeof(Four), 1, logFp);
        fread(&configuration.importLog.textIndexImportPhase, sizeof(Four), 1, logFp);

        // Close data file to write
        e = Util_fclose(logFp);
        if (e == EOF)
        {
            configuration.errorMessage  = "Can't close '";
            configuration.errorMessage += logFileName;
            configuration.errorMessage += "' file";
            TOOL_ERR(eCANNOT_CLOSE_FILE_IMPORT);
        }
    }

    return eNOERROR;
}
