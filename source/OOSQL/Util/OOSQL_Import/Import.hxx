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

#ifndef __IMPORT_HXX__
#define __IMPORT_HXX__

/* Header file include */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <sys/types.h>
#ifndef WIN32
#include <unistd.h>
#include <sys/statvfs.h>
#endif

// FOR TIME CHECK
#include <sys/types.h>
#include <sys/timeb.h>
#include <time.h>


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


/* rename file handling functions */
#ifdef WIN32
#define     Util_fopen                  fopen
#define     Util_fclose                 fclose
#define     Util_fseek                  fseek
#define     Util_ftell                  ftell
#define	    Util_fprintf				fprintf
#define	    Util_fscanf					fscanf
#define	    Util_fgets		            fgets
#define	    Util_fputs                  fputs
#define	    Util_fread                  fread
#define	    Util_fwrite                 fwrite
#define	    Util_fflush                 fflush
#endif

/* Define MACRO */
#define String OOSQL_TCDynStr
#define Array  OOSQL_TCArray

#define NORMALEXIT              0

#define MAXCOMMANDLENGTH        1024
#define MAXFILENAME             256
#define MAXSIZEOFPARAMETER      256
#define MAXSIZEOFLINEBUFFER     2048
#define MAXNUMOFLINE            500
#define MAXNUMOFLINEINFILE      1000000
#define MAXNUMOFATTRIBUTES      50
#define MAXNUMOFMETHODS         30
#define MAXNUMOFSUPERCLASSES    30
#define MAXPOSTINGBUFFERSIZE    4 * 1024 * 1000


#ifdef WIN32
#define DIRECTORY_SEPARATOR "\\"
#define TMP_DIRECTORY       "C:\\WINDOWS\\TEMP\\"
#else
#define DIRECTORY_SEPARATOR "/"
#define TMP_DIRECTORY       "/tmp"
#endif


#define TABLEFILENAME       "export.tbl"
#define LOGFILENAME         "import.log"
#define TEXTKEYWORDEXT_INFOFILENAME "TextKeywordExtractorInfo.info"
#define TEXTPREFERENCE_INFOFILENAME "SysTextPreferences.info"

// Import step
#define BEGINOFIMPORT           0
#define ENDOFSCHEMAIMPORT       1
#define ENDOFDATAIMPORT         2
#define ENDOFINDEXIMPORT        3
#define ENDOFTEXTINDEXCONVERT   4
#define ENDOFTEXTINDEXIMPORT    5   


/* Define data structure */

typedef struct {
    Four                    mainImportPhase;
    Four                    dataImportPhase;
    Four                    textIndexConvertPhase;
    Four                    textIndexImportPhase;
} ImportLog;

typedef struct {
    Array<String>           tables;
    String                  dirPath;
    String                  tmpPath;
    Boolean                 full;
    Boolean                 indexes;
    Boolean                 bulkload;

    OOSQL_SystemHandle      handle;
    String                  databaseName;
    String                  volumeName; 
    Four                    databaseId;
    Four                    volumeId;
    XactID                  xactId;

    String                  errorMessage;
    String                  incompleteFile;

    ImportLog               importLog;
} ImportConfig;


typedef struct {
    Two                 columnNo;        
    Two                 complexType;    // data type of column 
    Two                 colType;        // Attribute type 
    Four                length;         // Attribute length ,for maximum length of SM_STRING 
    String              colName;        // Attribute name 
    Four                inheritedFrom;  // super class ID 
    Four                domainId;
} ImportAttrInfo;


typedef struct {
    String              dirPath;        // path where this method is implemeeted 
    String              methodName;     // Method name 
    String              functionName;   // c function name 
    Two                 nArguments;     // # of arguments 
    Array<Four>         ArgumentsList;  // list of arguments 
    Four                returnType;
    Four                inheritedFrom;
} ImportMethodInfo;


typedef struct {
    Four                superclassId;
    String              superclassName;
} ImportSuperclassInfo;


typedef struct {
    Four                colNo;
    Four                flag;           // ascending/descendig 
} Importcolumn;


typedef struct {
    Two                 flag;           // UNIQUE, ... 
    Two                 nColumns;       // # of key parts 
    Array<Importcolumn> columns;
} ImportKeyInfo;


typedef struct {
    Two                 flag;           // CLUSTERING, ... 
    Two                 nColumns;       // # of columns on which the index is defined 
    Array<Two>          colNo;          // column numbers 
    Two                 extraDataLen;   // length of the extra data for an object 
} ImportMLGF_KeyInfo;

typedef struct {
    Boolean                 isContainingTupleID;
    Boolean                 isContainingSentenceAndWordNum;
    Boolean                 isContainingByteOffset;
    Two                     nEmbeddedAttributes;
    Array<Two>              embeddedAttrNo;
    Array<Two>              embeddedAttrOffset;
    Array<Two>              embeddedAttrVarColNo;
} ImportInMemory_PostingStructureInfo;

typedef struct {
    String                  colName;        // Attribute name 
    IndexID                 keywordIndex;   /* btree index on keyword attribute of inverted index table */
    IndexID                 reverseKeywordIndex; /* SM_TEXT index on reverse-keyword attribtue of inverted index table */
    IndexID                 docIdIndex; /* SM_TEXT index on document id of document-id index table */
    String                  invertedIndexName;
    String                  docIdIndexTableName;
    ImportInMemory_PostingStructureInfo postingInfo;
} ImportInvertedIndexInfo;

typedef struct {
    String                  indexName;
    One                     indexType;
    LOM_IndexID             iid;

    ImportKeyInfo           btree;          // Key Information for Btree
    ImportMLGF_KeyInfo      mlgf;           // Key Information for MLGF
    ImportInvertedIndexInfo invertedIndex;
} ImportIndexInfo;


typedef struct {
    Four                        nPostingFiles;
    Array<String>               postingFileName;
    Array<Four>                 postingFileCount;
} ImportPostingFileInfo;


typedef struct {
    Four                        numberOfFile;
    Array<Four>                 sizeOfFileH;
    Array<Four>                 sizeOfFileL;
    Four                        numberOfTuple;
    Four                        maxSizeOfTuple;
    Four                        maxLogicalOid;
} ImportDataStatistics;
    

typedef struct {
    ImportDataStatistics        dataStatistics;
    ImportPostingFileInfo       postingFileInfo;

    String                      className;
    Four                        oldClassId;
    Four                        newClassId;

    Four                        nAttrs;
    Array<ImportAttrInfo>       attrsInfo;

    Four                        nMethods;
    Array<ImportMethodInfo>     methodsInfo;

    Four                        nSuperclasses;
    Array<ImportSuperclassInfo> superclassesInfo;

    Four                        nIndexes;
    Array<ImportIndexInfo>      indexesInfo;
} ImportClassInfo;



// className and classID mapping table
typedef struct {
    String      className;
    Four        oldClassId;
    Four        newClassId;
    Four        classInfoIdx;
} ImportClassIdMapping;


// OID mapping array
typedef struct {
    Four    pageNo;
    Two     volNo;
    Two     slotNo;
    UFour   unique;
} ImportOID;

typedef struct {
    Array<ImportOID>    oidArray;
    Four                classId;
} ImportOIDMappingTable;

typedef struct {
    char    filterName[MAXFILENAME];
    Four    version;
    char    keywordExtractorFilePath[MAXFILENAME];
    char    keywordExtractorFunctionName[MAXFILENAME];
    char    getNextPostingInfoFunctionName[MAXFILENAME];
    char    finalizeKeywordExtractorFunctionName[MAXFILENAME];
    Four    keywordExtractorNo;
} ImportTextKeywordExtractorInfo;

typedef struct {
    Four    classId;
    Four    colNo;
    Four    filterNo;
    Four    keywordExtractorNo;
    Four    stemizerNo;
} ImportTextKeywordExtractorPrefer;


#undef LRDS_CHECK_ERR
#define LRDS_CHECK_ERR(e) \
    if(e < eNOERROR) TOOL_ERR(eLRDS_ERROR_IMPORT);

#undef LOM_CHECK_ERR
#define LOM_CHECK_ERR(e) \
    if(e < eNOERROR) TOOL_ERR(eLOM_ERROR_IMPORT);

#undef CATALOG_CHECK_ERR
#define CATALOG_CHECK_ERR(e) \
    if(e < eNOERROR) TOOL_ERR(eCATALOG_ERROR_IMPORT);

#undef OOSQL_CHECK_ERR
#define OOSQL_CHECK_ERR(e) \
    if(e < eNOERROR) TOOL_ERR(eOOSQL_ERROR_IMPORT);


/* Function declaration */
Four Import_InitParameters (ImportConfig&);
Four Import_LoadParameters(int, char **, ImportConfig&);
void Import_DisplayHelp();

Four import_WriteLog(ImportConfig&);
Four import_ReadLog(ImportConfig&);
Four Import_DeleteTmpFile(ImportConfig&);

Four Import_Initialize(ImportConfig&);
Four Import_Finalize(ImportConfig&);
Four import_TransBegin(ImportConfig&);
Four import_TransCommit(ImportConfig&);

Four Import_ImportSchema (ImportConfig&, Array<ImportClassInfo>&, Array<ImportClassIdMapping>&);
Four import_ConvertClassId (ImportConfig&, const Array<ImportClassIdMapping>&, Four);
Four import_ReadClassNames (ImportConfig&, Array<String>&);
Four import_ReadClassInfo(ImportConfig&, const String&, ImportClassInfo&, Boolean);
Four import_GetClassNames(ImportConfig&, Array<String>&);
Four import_CreateClass (ImportConfig&, const ImportClassInfo&);
Four import_WriteClassInfo (ImportConfig&, const ImportClassInfo&);
Four import_InstallKeywordExtractor ( ImportConfig&, Array<ImportClassInfo>&, Array<ImportTextKeywordExtractorInfo>&);
Four import_SetupKeywordExtractor (ImportConfig&, Array<ImportClassInfo>&, Array<ImportTextKeywordExtractorInfo>&);


Four Import_ImportData (ImportConfig&, Array<ImportClassInfo>&, Array<ImportOIDMappingTable>&);
Four import_ReadDataStatistics (ImportConfig&, ImportClassInfo&);
Four import_InsertClassData (ImportConfig&, ImportClassInfo&, ImportOIDMappingTable&);
Four import_InsertOIDIntoMappingTable (const OID&, const Four, ImportOIDMappingTable&);
Four import_ReadOID (FILE*, OID&, Four&);
Four import_ReadNullVector (FILE*, const Four, Array<One>&);
Four import_ReadData (ImportConfig&, FILE*, const Four, const Four, void*);
Four import_WriteOidMappingTable (ImportConfig&, ImportClassInfo&, ImportOIDMappingTable&);
Four import_ReadOidMappingTable (ImportConfig&, const ImportClassInfo&, ImportOIDMappingTable&);
Four import_ReadPostingFileInfo (ImportConfig&, ImportClassInfo&);


Four Import_ImportIndex (ImportConfig&, const Array<ImportClassInfo>&);
Four import_CreateIndex (ImportConfig&, const ImportClassInfo&);

Four Import_ImportTextIndex (ImportConfig&, Array<ImportClassInfo>&, Array<ImportOIDMappingTable>&);
Four import_ConvertSortedPosting (ImportConfig&, ImportClassInfo&, const ImportOIDMappingTable&);
Four import_ConvertPosting (ImportConfig&, const ImportClassInfo&, const Four, const ImportOIDMappingTable&);
Four import_TextIndexBuilding (ImportConfig&, const ImportClassInfo&);
Four import_WriteTextIndexCount (ImportConfig&, const ImportClassInfo&, const Four, const Four);
Four import_ReadTextIndexCount (ImportConfig&, const ImportClassInfo&, const Four, Four&);

#endif
