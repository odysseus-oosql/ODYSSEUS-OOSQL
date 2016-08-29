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

Boolean import_checkSystemDefinedClasses (
    const char  *className)
{
    static char* systemDefinedClassNames[] = {
        "void", "d_Short", "d_Long", "d_UShort", "d_ULong", "d_Float", "d_Double", 
        "d_Char", "d_Octet", "d_Boolean", "d_Date", "d_Time", "d_Timestamp",
        "d_Interval", "d_String", 

        "odmg_set_void", "odmg_set_d_Short", "odmg_set_d_Long", "odmg_set_d_UShort", "odmg_set_d_ULong", "odmg_set_d_Float", "odmg_set_d_Double", 
        "odmg_set_d_Char", "odmg_set_d_Octet", "odmg_set_d_Boolean", "odmg_set_d_Date", "odmg_set_d_Time", "odmg_set_d_Timestamp",
        "odmg_set_d_Interval", "odmg_set_d_String", 

        "odmg_bag_void", "odmg_bag_d_Short", "odmg_bag_d_Long", "odmg_bag_d_UShort", "odmg_bag_d_ULong", "odmg_bag_d_Float", "odmg_bag_d_Double", 
        "odmg_bag_d_Char", "odmg_bag_d_Octet", "odmg_bag_d_Boolean", "odmg_bag_d_Date", "odmg_bag_d_Time", "odmg_bag_d_Timestamp",
        "odmg_bag_d_Interval", "odmg_bag_d_String", 

        "odmg_list_void", "odmg_list_d_Short", "odmg_list_d_Long", "odmg_list_d_UShort", "odmg_list_d_ULong", "odmg_list_d_Float", "odmg_list_d_Double", 
        "odmg_list_d_Char", "odmg_list_d_Octet", "odmg_list_d_Boolean", "odmg_list_d_Date", "odmg_list_d_Time", "odmg_list_d_Timestamp",
        "odmg_list_d_Interval", "odmg_list_d_String", 

        "odmg_varray_void", "odmg_varray_d_Short", "odmg_varray_d_Long", "odmg_varray_d_UShort", "odmg_varray_d_ULong", "odmg_varray_d_Float", "odmg_varray_d_Double", 
        "odmg_varray_d_Char", "odmg_varray_d_Octet", "odmg_varray_d_Boolean", "odmg_varray_d_Date", "odmg_varray_d_Time", "odmg_varray_d_Timestamp",
        "odmg_varray_d_Interval", "odmg_varray_d_String", 

        "d_Object", "d_LargeObj", "db_Ref", "db_Array", "db_Stream", "db_ObjectNameTable", 

        "lrdsSysTables", "lrdsSysColumns", "lrdsSysIndexes",
        "lomSysClasses", "lomInheritance", "lomSysColumns", "lomSysIndexes",
        "lomSysMethods", "lomSysRelationship", "lomClassId", "lomRelationshipId",
        "lomTextFilterId", "lomTextKeywordExtractorId", "lomTextStemizerId", "lomSysTextIndexes",
        "lomSysTextFilterInfo", "lomSysTextKeywordExtractorInfo", "lomSysTextStemizerInfo", "lomSysTextPreferences",
        "LOM_SYS_FUNCTIONS", "LOM_SYS_FUNCPARMS", "LOM_SYS_PROCEDURES", "LOM_SYS_PROCPARMS",
        "_LOM_SYS_FUNCTIONS_DeletionList", "_LOM_SYS_FUNCPARMS_DeletionList",
        "_LOM_SYS_PROCEDURES_DeletionList", "_LOM_SYS_PROCPARMS_DeletionList",

        "web_page", "MultimediaObj", "Image", "externalFunction",
        "webMovie", "webImage", "webAudio", "webDocument",
        "parameters", "Document", "Audio", "Video",

        NULL
    };

    for(int i = 0; systemDefinedClassNames[i] != NULL; i++) 
    {
        if(!strcmp(systemDefinedClassNames[i], className))
            return (Boolean)TRUE;
    }

    if (className[0] == '_' && !strncmp(&className[strlen(className)-9], "_Inverted", 9))
        return (Boolean)TRUE;
    if (className[0] == '_' && !strncmp(&className[strlen(className)-6], "_docId", 6))
        return (Boolean)TRUE;


    return (Boolean)FALSE;
}


Four import_GetClassNames(
    ImportConfig&       configuration,  // IN/OUT
    Array<String>&      classNames)     // OUT
{
    Four            i;
    Four            e;
    Four            orn;                        // Open Relation Number
    Four            scanId;                     // scan identifier
    LockParameter   lockup;
    TupleID         tid;                        // a tuple identifier
    ColListStruct   clist[1];                   // column list
    char            relName[LOM_MAXCLASSNAME];  // relation name


    orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(&configuration.handle.lomSystemHandle), configuration.volumeId, LOM_SYSCLASSES_CLASSNAME);
    LRDS_CHECK_ERR(orn);

    lockup.mode     = L_S;
    lockup.duration = L_COMMIT;

    scanId = LRDS_OpenSeqScan(LOM_GET_LRDS_HANDLE(&configuration.handle.lomSystemHandle), orn, FORWARD, 0, NULL, &lockup);
    LRDS_CHECK_ERR(scanId);

    clist[0].colNo = LOM_SYSCLASSES_CLASSNAME_COLNO;
    clist[0].start = ALL_VALUE;
    clist[0].dataLength = LOM_MAXCLASSNAME; 
    clist[0].data.ptr = &relName[0];

    while ((e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(&configuration.handle.lomSystemHandle), scanId, &tid, NULL)) != EOS) 
    {
        LRDS_CHECK_ERR(e);

        e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(&configuration.handle.lomSystemHandle), scanId, (Boolean)TRUE, &tid, 1, &clist[0]);
        LRDS_CHECK_ERR(e);
            
        relName[clist[LOM_SYSCLASSES_CLASSNAME_COLNO].retLength] = '\0';

        if(!import_checkSystemDefinedClasses(relName))
        {
            // FOR DEBUG
            //fprintf(stderr, "|%s|\n", relName);

            classNames.add(String(relName));
        }
    }

    e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(&configuration.handle.lomSystemHandle), scanId);
    LRDS_CHECK_ERR(e);

    e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(&configuration.handle.lomSystemHandle), orn);
    LRDS_CHECK_ERR(e);

    return eNOERROR;
}
