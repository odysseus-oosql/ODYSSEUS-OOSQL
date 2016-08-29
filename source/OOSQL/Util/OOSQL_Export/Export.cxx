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

// FOR TIME CHECK
#include <sys/types.h>
#include <sys/timeb.h>
#include <time.h>

extern Four procIndex;

// FOR TIME CHECK
extern "C" int ftime(struct timeb *tp);
static struct timeb gtimevar1;
static struct timeb gtimevar2;
static struct timeb timevar1;
static struct timeb timevar2;

//extern Four     io_num_of_writes;
//extern Four     io_num_of_reads;


int main(int argc, char **argv)
{
    ExportConfig            configuration;
    Array<ExportClassInfo>  classInfos;
    Four                    e;

	Four 					io_num_of_writes;
	Four					io_num_of_reads;


    // I. Paramters loading 
    e = Export_InitParameters(configuration);
    if (e < eNOERROR) 
    {
        TOOL_PRTERR(e);
        fprintf(stderr, "%s\n", (const char *)configuration.errorMessage);
        exit(1);
    }
    
    e = Export_LoadParameters(argc, argv, configuration);
    if (e < eNOERROR) 
    {
        TOOL_PRTERR(e);
        fprintf(stderr, "%s\n", (const char *)configuration.errorMessage);
        exit(1);
    }


    // FOR TIME CHECK
    ftime(&gtimevar1);
    fprintf(stderr, "Export Start : %lds %ldms\n", gtimevar1.time, gtimevar1.millitm);


    // III. System initialize
    e = Export_Initialize(configuration);
    if (e < eNOERROR)
    {
        TOOL_PRTERR(e);
        fprintf(stderr, "%s\n", (const char *)configuration.errorMessage);
        exit(1);
    }


    // IV. Database schema export 

    // FOR TIME CHECK
    ftime(&timevar1);
    fprintf(stderr, "Schema Export Start: %lds %ldms\n", timevar1.time, timevar1.millitm);

    e = Export_ExportSchema(configuration, classInfos);
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
    fprintf(stderr, "Schema Export End : %lds %ldms\n", timevar2.time, timevar2.millitm);
    fprintf(stderr, "Total Schema Exporting Time : %ld:%ld:%ld\n", 
                    (timevar2.time-timevar1.time)/3600,
                    ((timevar2.time-timevar1.time)%3600)/60,
                    (timevar2.time-timevar1.time)%60);



    // V. Database data export 
    if (configuration.data == TRUE)
    {
        // FOR TIME CHECK
        ftime(&timevar1);
        fprintf(stderr, "\nData Export Start: %lds %ldms\n", timevar1.time, timevar1.millitm);

        e = Export_ExportData(configuration, classInfos);
        if (e < eNOERROR)
        {
            fprintf(stderr, "*** incomplete file is %s ***\n", (const char *)configuration.incompleteFile);
    
            TOOL_PRTERR(e);
            fprintf(stderr, "%s\n", (const char *)configuration.errorMessage);
            OOSQL_TransAbort(&configuration.handle, &configuration.xactId);
            OOSQL_DismountDB(&configuration.handle, configuration.databaseId);
            OOSQL_DestroySystemHandle(&configuration.handle, procIndex);
            exit(1);
        }

        // FOR TIME CHECK
        ftime(&timevar2);
        fprintf(stderr, "Data Export End : %lds %ldms\n", timevar2.time, timevar2.millitm);
        fprintf(stderr, "Total Data Exporting Time : %ld:%ld:%ld\n", 
                        (timevar2.time-timevar1.time)/3600,
                        ((timevar2.time-timevar1.time)%3600)/60,
                        (timevar2.time-timevar1.time)%60);
    }



    // VI. Database index export
    if (configuration.indexes == TRUE)
    {
        // FOR TIME CHECK
        ftime(&timevar1);
        fprintf(stderr, "\nText Index Export Start: %lds %ldms\n", timevar1.time, timevar1.millitm);

        e = Export_ExportTextIndex(configuration, classInfos);
        if (e < eNOERROR)
        {
            fprintf(stderr, "*** incomplete file is %s ***\n", (const char *)configuration.incompleteFile);

            TOOL_PRTERR(e);
            fprintf(stderr, "%s\n", (const char *)configuration.errorMessage);
            OOSQL_TransAbort(&configuration.handle, &configuration.xactId);
            OOSQL_DismountDB(&configuration.handle, configuration.databaseId);
            OOSQL_DestroySystemHandle(&configuration.handle, procIndex);
            exit(1);
        }

        // FOR TIME CHECK
        ftime(&timevar2);
        fprintf(stderr, "Text Index Export End : %lds %ldms\n", timevar2.time, timevar2.millitm);
        fprintf(stderr, "Total Text Index Exporting Time : %ld:%ld:%ld\n", 
                        (timevar2.time-timevar1.time)/3600,
                        ((timevar2.time-timevar1.time)%3600)/60,
                        (timevar2.time-timevar1.time)%60);
    }


    // VII. System finalize 
    e = Export_Finalize(configuration);
    if (e < eNOERROR)
    {
        TOOL_PRTERR(e);
        fprintf(stderr, "%s\n", (const char *)configuration.errorMessage);
        exit(1);
    }

    // FOR TIME CHECK
    ftime(&gtimevar2);
    fprintf(stderr, "\nExport End : %lds %ldms\n", gtimevar2.time, gtimevar2.millitm);
    fprintf(stderr, "----------------------------------------\n");
    fprintf(stderr, "Total Exporting Process Time : %ld:%ld:%ld\n", 
                    (gtimevar2.time-gtimevar1.time)/3600,
                    ((gtimevar2.time-gtimevar1.time)%3600)/60,
                    (gtimevar2.time-gtimevar1.time)%60);

    fprintf(stderr, "\n");
	e = LRDS_GetNumberOfDiskIO(LOM_GET_LRDS_HANDLE(&(OOSQL_GET_LOM_SYSTEMHANDLE(&configuration.handle))), &io_num_of_reads, &io_num_of_writes);
	OOSQL_CHECK_ERR(e);


    fprintf(stderr, "# of I/O (write) : %ld\n", io_num_of_writes);
    fprintf(stderr, "# of I/O (read)  : %ld\n", io_num_of_reads);

    return eNOERROR;
}

