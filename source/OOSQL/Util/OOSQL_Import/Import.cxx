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


extern Four procIndex;

// FOR TIME CHECK
#ifndef WIN32
extern "C" int ftime(struct timeb *tp);
#endif
static struct timeb gtimevar1;
static struct timeb gtimevar2;
static struct timeb timevar1;
static struct timeb timevar2;




int main(int argc, char **argv)
{
    ImportConfig                        configuration;
    Array<ImportClassInfo>              classInfos;
    Array<ImportClassIdMapping>         classIdMappingTable;
    Array<ImportOIDMappingTable>        oidMappingTables;
    Four                                e;



    // I. Paramters loading 
    e = Import_InitParameters(configuration);
    if (e < eNOERROR) 
    {
        TOOL_PRTERR(e);
        fprintf(stderr, "%s\n", (const char *)configuration.errorMessage);
        exit(1);
    }

    e = Import_LoadParameters(argc, argv, configuration);
    if (e < eNOERROR) 
    {
        TOOL_PRTERR(e);
        fprintf(stderr, "%s\n", (const char *)configuration.errorMessage);
        exit(1);
    }


    // FOR TIME CHECK
    ftime(&gtimevar1);
    fprintf(stderr, "Import Start : %lds %ldms\n", gtimevar1.time, gtimevar1.millitm);


    // III. System initialize
    e = Import_Initialize(configuration);
    if (e < eNOERROR)
    {
        TOOL_PRTERR(e);
        fprintf(stderr, "%s\n", (const char *)configuration.errorMessage);
        exit(1);
    }

    // FOR DEBUG
    /*
    fprintf(stderr, "=========================\n");
    fprintf(stderr, "main Import Phase        = %ld\n", configuration.importLog.mainImportPhase);
    fprintf(stderr, "data Import Phase        = %ld\n", configuration.importLog.dataImportPhase);
    fprintf(stderr, "text Index Convert Phase = %ld\n", configuration.importLog.textIndexConvertPhase);
    fprintf(stderr, "text Index Import Phase  = %ld\n", configuration.importLog.textIndexImportPhase);
    fprintf(stderr, "=========================\n");
    */





    // IV. Database schema import 

    // FOR TIME CHECK
    ftime(&timevar1);
    fprintf(stderr, "Schema Import Start: %lds %ldms\n", timevar1.time, timevar1.millitm);

    e = Import_ImportSchema(configuration, classInfos, classIdMappingTable);
    if (e < eNOERROR)
    {
        TOOL_PRTERR(e);
        fprintf(stderr, "%s\n", (const char *)configuration.errorMessage);

        OOSQL_TransAbort(&configuration.handle, &configuration.xactId);
        OOSQL_DismountDB(&configuration.handle, configuration.databaseId);
        OOSQL_DestroySystemHandle(&configuration.handle, procIndex);

        exit(1);
    }

    // FOR TIME CHECK
    ftime(&timevar2);
    fprintf(stderr, "Schema Import End : %lds %ldms\n", timevar2.time, timevar2.millitm);
    fprintf(stderr, "Total Schema Importing Time : %ld:%ld:%ld\n", 
                    (timevar2.time-timevar1.time)/3600,
                    ((timevar2.time-timevar1.time)%3600)/60,
                    (timevar2.time-timevar1.time)%60);


    // V. Database data import 
    // FOR TIME CHECK
    ftime(&timevar1);
    fprintf(stderr, "\nData Import Start: %lds %ldms\n", timevar1.time, timevar1.millitm);

    e = Import_ImportData(configuration, classInfos, oidMappingTables);
    if (e < eNOERROR)
    {
        TOOL_PRTERR(e);
        fprintf(stderr, "%s\n", (const char *)configuration.errorMessage);

        OOSQL_TransAbort(&configuration.handle, &configuration.xactId);
        OOSQL_DismountDB(&configuration.handle, configuration.databaseId);
        OOSQL_DestroySystemHandle(&configuration.handle, procIndex);
        exit(1);
    }
    // FOR TIME CHECK
    ftime(&timevar2);
    fprintf(stderr, "Data Import End : %lds %ldms\n", timevar2.time, timevar2.millitm);
    fprintf(stderr, "Total Data Importing Time : %ld:%ld:%ld\n", 
                    (timevar2.time-timevar1.time)/3600,
                    ((timevar2.time-timevar1.time)%3600)/60,
                    (timevar2.time-timevar1.time)%60);



    // VI. Database index import
    if (configuration.indexes == TRUE)
    {
        // FOR TIME CHECK
        ftime(&timevar1);
        fprintf(stderr, "\nIndex Import Start: %lds %ldms\n", timevar1.time, timevar1.millitm);

        e = Import_ImportIndex(configuration, classInfos);
        if (e < eNOERROR)
        {
            TOOL_PRTERR(e);
            fprintf(stderr, "%s\n", (const char *)configuration.errorMessage);

            OOSQL_TransAbort(&configuration.handle, &configuration.xactId);
            OOSQL_DismountDB(&configuration.handle, configuration.databaseId);
            OOSQL_DestroySystemHandle(&configuration.handle, procIndex);

            exit(1);
        }

        // FOR TIME CHECK
        ftime(&timevar2);
        fprintf(stderr, "Index Import End : %lds %ldms\n", timevar2.time, timevar2.millitm);
        fprintf(stderr, "Total Index Importing Time : %ld:%ld:%ld\n", 
                        (timevar2.time-timevar1.time)/3600,
                        ((timevar2.time-timevar1.time)%3600)/60,
                        (timevar2.time-timevar1.time)%60);
    }



    // VI. System finalize
    // FOR TIME CHECK
    ftime(&timevar1);
    fprintf(stderr, "\nSystem finalize Start: %lds %ldms\n", timevar1.time, timevar1.millitm);

    e = Import_Finalize(configuration);
    if (e < eNOERROR)
    {
        TOOL_PRTERR(e);
        fprintf(stderr, "%s\n", (const char *)configuration.errorMessage);

        exit(1);
    }

    // FOR TIME CHECK
    ftime(&timevar2);
    fprintf(stderr, "System finalize End : %lds %ldms\n", timevar2.time, timevar2.millitm);
    fprintf(stderr, "Total System finalizing Time : %ld:%ld:%ld\n", 
                    (timevar2.time-timevar1.time)/3600,
                    ((timevar2.time-timevar1.time)%3600)/60,
                    (timevar2.time-timevar1.time)%60);


    // VII. Text Index Build 
    if (configuration.indexes == TRUE)
    {
        e = Import_ImportTextIndex(configuration, classInfos, oidMappingTables);
        if (e < eNOERROR)
        {
            TOOL_PRTERR(e);
            fprintf(stderr, "%s\n", (const char *)configuration.errorMessage);

            OOSQL_TransAbort(&configuration.handle, &configuration.xactId);
            OOSQL_DismountDB(&configuration.handle, configuration.databaseId);
            OOSQL_DestroySystemHandle(&configuration.handle, procIndex);

            exit(1);
        }
    }


    // VIII. Delete temporary files to be used in import processing
    e = Import_DeleteTmpFile(configuration);
    if (e < eNOERROR)
    {
        TOOL_PRTERR(e);
        fprintf(stderr, "%s\n", (const char *)configuration.errorMessage);
        exit(1);
    }


    // FOR TIME CHECK
    ftime(&gtimevar2);
    fprintf(stderr, "\nImport End : %lds %ldms\n", gtimevar2.time, gtimevar2.millitm);
    fprintf(stderr, "----------------------------------------\n");
    fprintf(stderr, "Total Importing Process Time : %ld:%ld:%ld\n", 
                    (gtimevar2.time-gtimevar1.time)/3600,
                    ((gtimevar2.time-gtimevar1.time)%3600)/60,
                    (gtimevar2.time-gtimevar1.time)%60);

    fprintf(stderr, "\n");

    return eNOERROR;
}

