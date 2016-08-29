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

#ifndef __EXPORT_HXX__
#define __EXPORT_HXX__

/* Header file include */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/statvfs.h>

extern "C" {
#include "param.h"
#include "dblablib.h"
#include "cosmos_r.h"
#include "LOM.h"
#include "Catalog.h"
}
#include "OOSQL_APIs.h"
#include "OOSQL_Error.h"
#include "OOSQL_String.hxx"
#include "OOSQL_Array.hxx"
#include "TOOL_Error.h"


/* Define MACRO */
#define String OOSQL_TCDynStr
#define Array  OOSQL_TCArray

#define MAXCOMMANDLENGTH            1024
#define MAXSIZEOFNAMESTR            256
#define MAXFILENAME                 256
#define MAXSIZEOFPARAMETER          256
#define MAXSIZEOFLINEBUFFER         1024
#define MAXPOSTINGBUFFERSIZE        4 * 1024 * 1000
#define MAXEMBDDEDATTRSBUFFERSIZE   4 * 1024 * 100 

#define NORMALEXIT              0
#define INFINITEFILESIZE        -1  


#ifndef USE_LARGE_FILE      /* if not using large file, check file size parameter */
    #define MAXDATAFILESIZE         2000 * 1000 * 1000
    #define MAXINDEXFILESIZE        2000 * 1000 * 1000
#else
    #define MAXDATAFILESIZE         INFINITEFILESIZE    
    #define MAXINDEXFILESIZE        INFINITEFILESIZE    
#endif


// obsolete
#define EXPORT_BUFFER_SIZE          1024*1024


// Export Type
#define TEXT_EXPORT     0
#define BINARY_EXPORT   1


// Directory separator
#ifdef WIN32
#define DIRECTORY_SEPARATOR "\\"
#else
#define DIRECTORY_SEPARATOR "/"
#endif


// File name
#define TABLEFILENAME       "export.tbl"
#define LOGFILENAME         "export.log"
#define TEXTKEYWORDEXT_INFOFILENAME "TextKeywordExtractorInfo.info"
#define TEXTPREFERENCE_INFOFILENAME "SysTextPreferences.info"

/* Define data structure */
typedef struct {
    String                  table;
    String                  index;
} ExportByIndex;


// Export step
#define BEGINOFEXPORT           0
#define ENDOFSCHEMAEXPORT       1
#define ENDOFDATAEXPORT         2
#define ENDOFTEXTINDEXEXPORT    3


/* Define data structure */

typedef struct {
    Four                    mainExportPhase;
    Four                    dataExportPhase;
    Four                    textIndexExportPhase;
} ExportLog;


typedef struct {
    Array<String>           tables;
    String                  dirPath;
    Boolean                 full;
    Boolean                 data;
    Boolean                 indexes;
    Boolean                 seqret;
    Boolean                 postingList;
    Four                    expType;
    Array<ExportByIndex>    byIndexes;
    
    filepos_t               dataFileSize;
    filepos_t               indexFileSize;

    OOSQL_SystemHandle      handle;
    String                  databaseName;
    String                  volumeName;
    Four                    databaseId;
    Four                    volumeId;
    XactID                  xactId;

    String                  errorMessage;

    struct statvfs          vfsInfo;
    String                  incompleteFile;

    ExportLog               exportLog;
} ExportConfig;


typedef struct {
    Two                     columnNo;        
    Two                     complexType;    // data type of column 
    Two                     colType;        // Attribute type 
    Four                    length;         // Attribute length ,for maximum length of SM_STRING 
    String                  colName;        // Attribute name 
    Four                    inheritedFrom;  // super class ID 
    Four                    domainId;

    Two                     idxOfIndexInfo;
} ExportAttrInfo;


typedef struct {
    String                  dirPath;        // path where this method is implemeeted 
    String                  methodName;     // Method name 
    String                  functionName;   // c function name 
    Two                     nArguments;     // # of arguments 
    Array<Four>             ArgumentsList;  // list of arguments 
    Four                    returnType;
    Four                    inheritedFrom;
} ExportMethodInfo;


typedef struct {
    Four                    superclassId;
    String                  superclassName;
} ExportSuperclassInfo;


typedef struct {
    Four                    colNo;
    Four                    flag;           // ascending/descendig 
} Exportcolumn;


typedef struct {
    Two                     flag;           // UNIQUE, ... 
    Two                     nColumns;       // # of key parts 
    Array<Exportcolumn>     columns;
} ExportKeyInfo;

typedef struct {
    Two                     flag;           // CLUSTERING, ... 
    Two                     nColumns;       // # of columns on which the index is defined 
    Array<Two>              colNo;          // column numbers 
    Two                     extraDataLen;   // length of the extra data for an object 
} ExportMLGF_KeyInfo;

typedef struct {
    Boolean                 isContainingTupleID;
    Boolean                 isContainingSentenceAndWordNum;
    Boolean                 isContainingByteOffset;
    Two                     nEmbeddedAttributes;
    Array<Two>              embeddedAttrNo;
    Array<Two>              embeddedAttrOffset;
    Array<Two>              embeddedAttrVarColNo;
} ExportInMemory_PostingStructureInfo;

typedef struct {
    String                  colName;        // Attribute name 
    IndexID                 keywordIndex;   /* btree index on keyword attribute of inverted index table */
    IndexID                 reverseKeywordIndex; /* SM_TEXT index on reverse-keyword attribtue of inverted index table */
    IndexID                 docIdIndex; /* SM_TEXT index on document id of document-id index table */
    String                  invertedIndexName;
    String                  docIdIndexTableName;
    ExportInMemory_PostingStructureInfo postingInfo;
} ExportInvertedIndexInfo;

typedef struct {
    String                  indexName;
    One                     indexType;
    LOM_IndexID             iid;

    ExportKeyInfo           btree;          // Key Information for Btree
    ExportMLGF_KeyInfo      mlgf;           // Key Information for MLGF
    ExportInvertedIndexInfo invertedIndex;
} ExportIndexInfo;

typedef struct {
    Four                        nPostingFiles;
    Array<String>               postingFileName;
    Array<Four>                 postingFileCount;
} ExportPostingFileInfo;

typedef struct {
    Four                        numberOfFile;                       
    Array<Four>            		sizeOfFileH;
    Array<Four>            		sizeOfFileL;
    Four                        numberOfTuple;
    Four                        maxSizeOfTuple;
    Four                        maxLogicalOid;
} ExportDataStatistics;


typedef struct {
    ExportDataStatistics        dataStatistics;
    ExportPostingFileInfo       postingFileInfo;
    
    String                      className;
    String                      byindexName;
    Four                        classId;

    Four                        nAttrs;
    Array<ExportAttrInfo>       attrsInfo;

    Four                        nMethods;
    Array<ExportMethodInfo>     methodsInfo;

    Four                        nSuperclasses;
    Array<ExportSuperclassInfo> superclassesInfo;

    Four                        nIndexes;
    Array<ExportIndexInfo>      indexesInfo;
} ExportClassInfo;


typedef struct {
    char                        buffer[EXPORT_BUFFER_SIZE];
    Four                        ptr;
} ExportBuffer;


typedef struct {
    String                      keywordExtractorName;
    Four                        version;
    String                      keywordExtractorFilePath;
    String                      keywordExtractorFunctionName;
    String                      getNextPostingInfoFunctionName;
    String                      finalizeKeywordExtractorFunctionName;
    Four                        keywordExtractorNo;
} ExportInstalledKEInfo;


typedef struct {
    Four                        classId;
    Four                        colNo;
    Four                        filterNo;
    Four                        keywordExtractorNo;
    Four                        stemizerNo;
} ExportSetupedKEInfo;


// Error handle MACRO
#undef LRDS_CHECK_ERR
#define LRDS_CHECK_ERR(e) \
    if(e < eNOERROR) TOOL_ERR(eLRDS_ERROR_EXPORT);

#undef LOM_CHECK_ERR
#define LOM_CHECK_ERR(e) \
    if(e < eNOERROR) TOOL_ERR(eLOM_ERROR_EXPORT);

#undef CATALOG_CHECK_ERR
#define CATALOG_CHECK_ERR(e) \
    if(e < eNOERROR) TOOL_ERR(eCATALOG_ERROR_EXPORT);

#undef OOSQL_CHECK_ERR
#define OOSQL_CHECK_ERR(e) \
    if(e < eNOERROR) TOOL_ERR(eOOSQL_ERROR_EXPORT);



/* Function declaration */
Four Export_InitParameters (ExportConfig&);
Four Export_LoadParameters(int, char**, ExportConfig&);
void Export_DisplayHelp();
Four Export_Initialize(ExportConfig&);
Four Export_Finalize(ExportConfig&);

Four export_WriteLog(ExportConfig&);
Four export_ReadLog(ExportConfig&);

Four Export_ExportSchema(ExportConfig&, Array<ExportClassInfo>&);
Four Export_ExportData(ExportConfig&, Array<ExportClassInfo>&);
Four Export_ExportTextIndex(ExportConfig&, Array<ExportClassInfo>&);

Four export_FetchClassNames(ExportConfig&, Array<String>&);
Four export_FetchClassInfo(ExportConfig&, const String&, ExportClassInfo&);
Four export_FetchClassId (ExportConfig&, const String&, Four&);
Four export_FetchClassAttributes (ExportConfig&, const String&, Four&, Array<ExportAttrInfo>&);
Four export_FetchClassMethods (ExportConfig&, const String&, Four&, Array<ExportMethodInfo>&);
Four export_FetchClassSuperclasses (ExportConfig&, const String&, Four&, Array<ExportSuperclassInfo>&);
Four export_FetchClassIndexes (ExportConfig&, const String&, Four&, Array<ExportIndexInfo>&, Array<ExportAttrInfo>&);
Four export_FetchKeywordExtractorInfos(ExportConfig&);


Four export_WriteClassNames(ExportConfig&, const Array<String>&);
Four export_WriteClassInfo(ExportConfig&, const ExportClassInfo&);
Four export_WriteClassData(ExportConfig&, ExportClassInfo&, ExportBuffer&);
Four export_WriteTextIndex(ExportConfig&, ExportClassInfo&);

Four export_WriteOID(ExportConfig&, FILE*, Four, OID&, ExportBuffer&);
Four export_WriteLogicalOID(ExportConfig&, FILE*, Four, OID&, Four&, ExportBuffer&);
Four export_WriteTupleSize(ExportConfig&, FILE*, Four, ExportBuffer&);
Four export_WriteColumnSize(ExportConfig&, FILE*, Four, ExportBuffer&);
Four export_WriteNullVector(ExportConfig&, FILE*, Four, int*, ExportBuffer&);
Four export_WriteData(ExportConfig&, FILE*, Four, Four, void*, ExportBuffer&);
Four export_WriteBuffer(void*, Four, ExportBuffer&, FILE*);
Four export_WriteDataStatistics (ExportConfig&, ExportClassInfo&);
Four export_WritePostingFileInfo (ExportConfig&, ExportClassInfo&);


Four export_EncodeString(Four type, Four length, char* stc, char* dest);
Four export_CheckByIndex(ExportConfig&, ExportClassInfo&);
Four export_GetClassNames(ExportConfig&, Array<String>&);
Four export_GetInstalledKEInfo(ExportConfig&, Array<ExportInstalledKEInfo>&);
Four export_GetSetupedKEInfo(ExportConfig&, Array<ExportSetupedKEInfo>&);

#endif
