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

#ifndef SLIMDOWN_TEXTIR

#include "OOSQL_StorageSystemHeaders.h"
#include "OOSQL_APIs_Internal.hxx"
#include "OOSQL_Error.h"
#include "OOSQL_Tool.hxx"
#include "DBM.h"

#include <stdio.h>
#ifndef WIN32
#include <sys/param.h>
#include <unistd.h>
#else
#include <io.h>
#endif
#include <sys/types.h>
#include <sys/stat.h>
#include <malloc.h>
#include <ctype.h>
#include <assert.h>
#include <string.h>

#include <string>
#include <vector>
#include <map>

using namespace std;

#define USE_FILE_RECOVERY
#define DEFAULT_BUFFER_SIZE 4096
#define MAXBUFFERLEN 1024000

#define DEFAULT_LINE_LENGTH 256

#define CH_BACKSLASH '\\'
#define CH_SINGLEQUOTE '\''
#define CH_DOUBLEQUOTE '\"'
#define CH_LEFTBRACE '{'
#define CH_RIGHTBRACE '}'
#define CH_SPACE ' '
#define CH_TAB '\t'

#ifdef WIN32
#define DIRECTORY_SEPARATOR "\\"
#define F_OK   00
#define W_OK   02
#define R_OK   04
#define RW_OK  06
#else
#define DIRECTORY_SEPARATOR "/"
#endif


typedef struct {
    Four   sentence;
    Four   word;
} SentenceWordPositionListStruct;

typedef struct {
    Four    byteOffset;
} BytePositionListStruct;



class SentenceWordPositionListBuffer {
public:
    SentenceWordPositionListBuffer(int nElements)
    {
        m_sentenceWordPositionBuf = (SentenceWordPositionListStruct*)malloc(sizeof(SentenceWordPositionListStruct) * nElements);
        m_nElements               = nElements;
    }
    ~SentenceWordPositionListBuffer()
    {
        if(m_sentenceWordPositionBuf)
            free(m_sentenceWordPositionBuf);
    }

    SentenceWordPositionListStruct& GetElement(int index) 
    {
        if(index < m_nElements)
            return m_sentenceWordPositionBuf[index];
        else
            return m_sentenceWordPositionBuf[index % m_nElements];
    }

    SentenceWordPositionListStruct* GetBufferPtr()
    {
        return m_sentenceWordPositionBuf;
    }

    int GetNElements()
    {
        return m_nElements;
    }

private:
    SentenceWordPositionListStruct* m_sentenceWordPositionBuf;
    int                             m_nElements;
};


class BytePositionListBuffer {
public:
    BytePositionListBuffer(int nElements)
    {
        m_bytePositionBuf = (BytePositionListStruct*)malloc(sizeof(BytePositionListStruct) * nElements);
        m_nElements               = nElements;
    }
    ~BytePositionListBuffer()
    {
        if(m_bytePositionBuf)
            free(m_bytePositionBuf);
    }

    BytePositionListStruct& GetElement(int index) 
    {
        if(index < m_nElements)
            return m_bytePositionBuf[index];
        else
            return m_bytePositionBuf[index % m_nElements];
    }

    BytePositionListStruct* GetBufferPtr()
    {
        return m_bytePositionBuf;
    }

    int GetNElements()
    {
        return m_nElements;
    }

private:
    BytePositionListStruct* m_bytePositionBuf;
    int                     m_nElements;
};


enum AttrType { ATTRTYPE_VOID, ATTRTYPE_NUMERIC,
                ATTRTYPE_VARCHAR, ATTRTYPE_TEXT };

class AttributeInfo
{
public:
    AttributeInfo() { name[0] = '\0'; attrType = ATTRTYPE_VOID; fptrToKeywordExtractor = NULL; fptrToFilter = NULL; colNo = -1; }
    ~AttributeInfo() {}

    void setAttrName(char* str) { strcpy(name, str); }
    const char* getAttrName() { return name; }

    void setAttrType(AttrType type) { attrType = type; }
    AttrType getAttrType() { return attrType; }
    void setAttrRealType(Four realType) { attrRealType = realType; }
    Four getAttrRealType() { return attrRealType; }

    void setColNo(Four cn) { colNo = cn; }
    Four getColNo() { return colNo; }

    void setKeywordExtractor(lom_FptrToKeywordExtractor fe_open, lom_FptrToGetNextPostingInfo fe_next, lom_FptrToFinalizeKeywordExtraction fe_final) 
    { 
        fptrToKeywordExtractor          = fe_open; 
        fptrToGetNextPostingInfo        = fe_next;
        fptrToFinalizeKeywordExtraction = fe_final;
    }
    void setFilter(lom_FptrToFilter fptr) { fptrToFilter = fptr; }

    void getKeywordExtractor(lom_FptrToKeywordExtractor& fe_open, lom_FptrToGetNextPostingInfo& fe_next, lom_FptrToFinalizeKeywordExtraction& fe_final)  
    {
        fe_open     = fptrToKeywordExtractor;
        fe_next     = fptrToGetNextPostingInfo;
        fe_final    = fptrToFinalizeKeywordExtraction;
    }
    lom_FptrToFilter getFilter() { return fptrToFilter; }

    catalog_SysIndexesOverlay* getTextIndexInfo() { return &textIndexInfo; }
    void setTextIndexInfo(catalog_SysIndexesOverlay indexInfo) { textIndexInfo = indexInfo; }
public:
    char     name[MAXNAMELEN];

    lom_FptrToKeywordExtractor          fptrToKeywordExtractor;
    lom_FptrToGetNextPostingInfo        fptrToGetNextPostingInfo;
    lom_FptrToFinalizeKeywordExtraction fptrToFinalizeKeywordExtraction;
    lom_FptrToFilter                    fptrToFilter;

    AttrType attrType;
    Four     attrRealType;
    Four     colNo;
    catalog_SysIndexesOverlay           textIndexInfo;
};

struct ClassInfo
{
public:
    // Constructors and destructor
    ClassInfo();
    virtual ~ClassInfo();

    // Public member functions
    Four setClassInfo(char* str);

    const char* getNthAttrName(int nth);
    AttrType getNthAttrType(int nth);
    Four getNthAttrRealType(int nth);
    Four getNthAttrColNo(int nth);

    void getNthAttrKeywordExtractor(int nth, lom_FptrToKeywordExtractor& fe_open, lom_FptrToGetNextPostingInfo& fe_next, lom_FptrToFinalizeKeywordExtraction& fe_final)
    { 
        attributeInfo[nth].getKeywordExtractor(fe_open, fe_next, fe_final); 
    }
    lom_FptrToFilter getNthAttrFilter(int nth) { return attributeInfo[nth].getFilter(); }
    catalog_SysIndexesOverlay* getNthAttrInfo(int nth) { return attributeInfo[nth].getTextIndexInfo(); }

    const char* getClassName() { return name; }
    Four getNumOfClassAttr() { return numOfClassAttr; }
    Four getNumOfDataFileAttr() { return numOfDataFileAttr; }

    Four init(OOSQL_SystemHandle& h, const Four vid);

    vector<FILE*> outputStreams;

public:
    char name[MAXNAMELEN];
    Four classId;

    Four openClassNum;

    OOSQL_SystemHandle* oosqlSysHandle;
    Four volId;

    Four                    numOfClassAttr;
    Four                    numOfDataFileAttr;
    vector<AttributeInfo>   attributeInfo;
    vector<int>             dataFileAttrNoToClassAttrNo;      
    map<string, int>        classAttrNameToClassAttrNo;      

};

ClassInfo::ClassInfo()
{
    numOfClassAttr = 0;
    numOfDataFileAttr = 0;

    attributeInfo.resize(MAXNUMOFATTR);
    outputStreams.resize(MAXNUMOFATTR);

    for (int i=0; i<MAXNUMOFATTR; i++)
        outputStreams[i] = NULL;

    name[0] = '\0';

    openClassNum = -1;
    oosqlSysHandle = NULL;
    volId = -1;
}

ClassInfo::~ClassInfo()
{
    Four e;

    for (int i=0; i<outputStreams.size(); i++)
    {
        if (outputStreams[i])
            Util_fclose(outputStreams[i]);
    }

    if(openClassNum >= 0)
    {
        e = LOM_CloseClass(&OOSQL_GET_LOM_SYSTEMHANDLE(oosqlSysHandle), openClassNum);
        if(e < eNOERROR)
            OOSQL_ERR_EXIT(e);
    }
}

Four ClassInfo::init(OOSQL_SystemHandle& h, const Four vid)
{
    oosqlSysHandle = &h;
    volId = vid;

    return eNOERROR;
}

Four ClassInfo::setClassInfo(char* str)
{
    // Initialize class information
    int  i, j, attrNo, attrNum;
    Four index, v, e;
    LOM_Handle *h;
    catalog_SysClassesOverlay *ptrToSysClasses;
    catalog_SysAttributesOverlay *ptrToSysAttributes;
    char className[MAXCLASSNAME];

    h = &OOSQL_GET_LOM_SYSTEMHANDLE(oosqlSysHandle);

    // remove the front spaces
    while (isspace(*str) && *str != '\0') str++;
    if (*str == '\0') OOSQL_ERR(eNOCLASSINFO_UTIL);

    // get the class name
    for (i=0; *str != '\0' && !isspace(*str) && *str != '('; i++, str++)
        name[i] = *str;
    name[i] = '\0';
    strcpy(className, name);

    // get class id
    e = LOM_GetClassID(h, volId, className, &classId);
    OOSQL_CHECK_ERR(e);

    // open class
    openClassNum = LOM_OpenClass(h, volId, className);
    OOSQL_CHECK_ERR(openClassNum);

    // get class information from catalog
    e = Catalog_GetClassInfo(h, volId, classId, &index);
    OOSQL_CHECK_ERR(e);

    v = Catalog_GetVolIndex(h, volId);
    OOSQL_CHECK_ERR(e);

    ptrToSysClasses = &CATALOG_GET_CLASSINFOTBL(h, v)[index];
    ptrToSysAttributes = &CATALOG_GET_ATTRINFOTBL(h, v)[CATALOG_GET_ATTRINFOTBL_INDEX(ptrToSysClasses)];

    attrNum = CATALOG_GET_ATTRNUM(ptrToSysClasses);

    // get rid of the spaces between the class name and the left blanket
    while (isspace(*str) && *str != '(') str++;
    if (*str == '\0') OOSQL_ERR(eNOATTRINFO_UTIL);

    // remove the left blanket
    if (*str == '(') str++;
    else name[0] = '\0';

    vector<string> dataFileAttributes;

    while(*str)
    {
        char attrName[MAXNAMELEN];
        int  idx = 0;

        while (isspace(*str) && *str != ')' && *str != '\0') str++;
        if (*str == '\0')
        {
            name[0] = '\0';
            numOfClassAttr = 0;
            numOfDataFileAttr = 0;
            OOSQL_ERR(eNORIGHTPARENTHESIS_UTIL);
        }
        else if (*str == ')') break;

        // read attribute name
        while (!isspace(*str) && *str !=')')
            attrName[idx++] = *str++;
        attrName[idx] = '\0';

        dataFileAttributes.push_back(attrName);
    }

    for(i = 0, attrNo = 1; attrNo < attrNum; i++, attrNo++)
    {
        attributeInfo[i].setAttrRealType(CATALOG_GET_ATTRTYPE(&ptrToSysAttributes[attrNo]));

        if (CATALOG_GET_ATTRTYPE(&ptrToSysAttributes[attrNo]) == LOM_TEXT)
        {
            lom_FptrToKeywordExtractor          fe_open;
            lom_FptrToGetNextPostingInfo        fe_next;
            lom_FptrToFinalizeKeywordExtraction fe_final;
            lom_FptrToFilter                    ff;

            attributeInfo[i].setAttrType(ATTRTYPE_TEXT);

            e = lom_Text_GetKeywordExtractorFPtr(h, openClassNum, attrNo, &fe_open);
            if(e < eNOERROR) LOM_ERROR(handle, e);
            e = lom_Text_GetGetNextPostingInfoFPtr(h, openClassNum, attrNo, &fe_next);
            if(e < eNOERROR) LOM_ERROR(handle, e);
            e = lom_Text_GetFinalizeKeywordExtractionFPtr(h, openClassNum, attrNo, &fe_final);
            if(e < eNOERROR) LOM_ERROR(handle, e);
            e = lom_Text_GetFilterFPtr(h, openClassNum, attrNo, &ff);
            if(e < eNOERROR) LOM_ERROR(handle, e);

            attributeInfo[i].setKeywordExtractor(fe_open, fe_next, fe_final);
            attributeInfo[i].setFilter(ff);

            Four indexInfoTblIndex;
            Four numOfReturnedIndex = 1;
            e = Catalog_GetIndexInfoByAttrNum(h, volId, classId, GET_USERLEVEL_COLNO(attrNo), &numOfReturnedIndex, &indexInfoTblIndex);
            if(e < eNOERROR) LOM_ERROR(handle, e);

            attributeInfo[i].setTextIndexInfo(CATALOG_GET_INDEXINFOTBL(h, v)[indexInfoTblIndex]);
        }
        else if (CATALOG_GET_ATTRTYPE(&ptrToSysAttributes[attrNo]) == LOM_VARSTRING)
            attributeInfo[i].setAttrType(ATTRTYPE_VARCHAR);
        else if (CATALOG_GET_ATTRTYPE(&ptrToSysAttributes[attrNo]) == LOM_STRING)
            attributeInfo[i].setAttrType(ATTRTYPE_VARCHAR);
        else
            attributeInfo[i].setAttrType(ATTRTYPE_NUMERIC);
        attributeInfo[i].setAttrName(CATALOG_GET_ATTRNAME(&ptrToSysAttributes[attrNo]));
        attributeInfo[i].setColNo(GET_USERLEVEL_COLNO(attrNo));
    }

    numOfDataFileAttr = dataFileAttributes.size();
    numOfClassAttr    = attrNum - 1;

    for(i = 0; i < dataFileAttributes.size(); i++)
    {
        if(dataFileAttributes[i] == className)
            continue;
        for(j = 0; j < numOfClassAttr; j++)
        {
            if(dataFileAttributes[i] == attributeInfo[j].name)
                break;
        }
        if(j == numOfClassAttr)
        {
            printf("'%s' is not exist in class '%s'\n", className, dataFileAttributes[i].c_str());
            OOSQL_ERR(eNOSUCHATTRIBUTE_UTIL);
        }
    }

    for(j = 0; j < numOfClassAttr; j++)
    {
        string attrName;

        attrName = attributeInfo[j].name;
        classAttrNameToClassAttrNo[attrName] = j;
    }

    dataFileAttrNoToClassAttrNo.resize(dataFileAttributes.size());
    for(i = 0; i < dataFileAttributes.size(); i++)
        dataFileAttrNoToClassAttrNo[i] = NIL;
    for(i = 0; i < dataFileAttributes.size(); i++)
    {
        int classAttrNo = classAttrNameToClassAttrNo[dataFileAttributes[i]];

        dataFileAttrNoToClassAttrNo[i] = classAttrNo;
    }

    return eNOERROR;
}

AttrType ClassInfo::getNthAttrType(int nth)
{
    assert(nth >=0 && nth < numOfClassAttr);
    return attributeInfo[nth].getAttrType();
}

Four ClassInfo::getNthAttrRealType(int nth)
{
    assert(nth >=0 && nth < numOfClassAttr);
    return attributeInfo[nth].getAttrRealType();
}

const char* ClassInfo::getNthAttrName(int nth)
{
    assert(nth >=0 && nth < numOfClassAttr);
    return attributeInfo[nth].getAttrName();
}

Four ClassInfo::getNthAttrColNo(int nth)
{
    assert(nth >=0 && nth < numOfClassAttr);
    return attributeInfo[nth].getColNo();
}

class ExtractKeyword
{
public:
    ExtractKeyword(const OOSQL_SystemHandle&, const Four&);
    virtual ~ExtractKeyword();

    Four extractKeywordFromDataFile(char* fileName, char* className, char* attrName, char* outputFileName, Four startObjectNo, Four endObjectNo, bool alwaysUsePreviousPostingFile);
    Four readLine();
    
private:
    enum State { State_DoubleQuote, State_SingleQuote, State_Numeric,
                 State_Set_DoubleQuote, State_Set_SingleQuote,
                 State_Set_Numeric, State_Space, State_Set_Space };
    State state;

    int objectCount;                // total number of inserted objects
    int lastLogicalId;              
    bool usePreviousFile;          
    FILE* inputStream;              // file pointer for data file
    FILE* outputStream;             // file pointer for output posting file
    FILE* saveStream;               // file pointer for save file

    int numberOfClass;              // number of classes
    int currentAttribute;           // column number of current attribute
    vector<ClassInfo> classInfo;    // class informations
    vector<string> attrValues;      // attribute value

    char* buffer;
    char* curPtr;
    int bufferSize;

    char* lineBuffer;
    int lineBufferSize;
    int lineCount;

    bool m_skipLastObject;          

    bool m_isAfterStartObject;
    bool m_isBeforeEndObject;

    filepos_t savedDataFileOffset;
    filepos_t savedPostingFileOffset;

    char inFile[MAXPATHLEN];
    char filteredFile[MAXPATHLEN];

    OOSQL_SystemHandle oosqlSysHandle;
    Four volId;

    Four extractKeywordFromBuffer(char* className, char* attrName, char* outputFileName, SentenceWordPositionListBuffer& sentenceWordPositionListBuffer, BytePositionListBuffer& bytePositionListBuffer);
    Four storeAttributeValue(Four, char*);
    Four writeToPostingFile(Four, char *);
    Four flushPostingFile(Four);
    Four makeEmptyOutputFile(char* outputFileName);
    Four getLastLogicalIdFromPreviousOutputFile(char* outputFileName);
    Four skipFilePosToLastLogicalId(FILE *fp);

    Four getFilePosFromSaveFile(char *);
    Four moveFilePosOfDataFile();
    Four moveFilePosOfPostingFile(FILE *);
    Four writeFilePosToSaveFile(Four);
    Four openSaveFileForWriting(char *);
    Four removeSaveFile(char *);
};

ExtractKeyword::ExtractKeyword(const OOSQL_SystemHandle& h, const Four& vid)
    : oosqlSysHandle(h), volId(vid)
{
    objectCount = 0;
    lastLogicalId = 0;
    numberOfClass = 0;
    inputStream = NULL;

    classInfo.resize(MAXNUMOFCLASS);
    for (int i=0; i<MAXNUMOFCLASS; i++)
        classInfo[i].init(oosqlSysHandle, volId);

    buffer = (char*)malloc(DEFAULT_BUFFER_SIZE);
    if (buffer == NULL) OOSQL_ERR_EXIT(eOUTOFMEMORY_UTIL);
    bufferSize = DEFAULT_BUFFER_SIZE;

    curPtr = buffer;

    lineBuffer = (char*)malloc(DEFAULT_LINE_LENGTH);
    if (lineBuffer == NULL) OOSQL_ERR_EXIT(eOUTOFMEMORY_UTIL);
    lineBufferSize = DEFAULT_LINE_LENGTH;

    lineCount = 0;

    m_skipLastObject = true;           

    m_isAfterStartObject = true;
    m_isBeforeEndObject  = true;

    state = State_Space;

    /* create intermediate file names */
    tmpnam(inFile);
    tmpnam(filteredFile);
}

ExtractKeyword::~ExtractKeyword()
{
    if (buffer) free(buffer);
    if (lineBuffer) free(lineBuffer);

    unlink(filteredFile);
    unlink(inFile);
}

Four ExtractKeyword::readLine()
{
    int ch;
    int i;

    i = 0;
    while ((ch = fgetc(inputStream)) != EOF)
    {
        if(ch == '\n')
        {
            lineBuffer[i] = '\0';
            lineCount++;
            return eNOERROR;
        }
        else if(ch == 0)    
            continue;      

        lineBuffer[i] = (char)ch;

        i++;
        if (i == lineBufferSize)
        {
            char* tmp;
            tmp = (char*)realloc(lineBuffer, lineBufferSize*2);
            if (tmp == NULL)
                OOSQL_ERR(eOUTOFMEMORY_UTIL);
            lineBuffer = tmp;
            lineBufferSize *= 2;
        }
    }

    if (ferror(inputStream))
    {
        OOSQL_ERR(eUNIXFILEREADERROR_UTIL);
    }
    else
    {
        if(i == 0)
            return EOS;
        else
        {
            lineBuffer[i] = '\0';
            return eNOERROR;
        }
    }

    return eNOERROR;
}

Four ExtractKeyword::makeEmptyOutputFile(char* outputFileName)
{
    FILE* fp;

    fp = Util_fopen(outputFileName, "w");
    if(fp == NULL)
    {
        printf("ERROR: Can't open file '%s' for writing.\n", outputFileName);
        OOSQL_ERR(eUNIXFILEOPENERROR_UTIL);
    }

    Util_fclose(fp);

    return eNOERROR;
}

Four ExtractKeyword::getLastLogicalIdFromPreviousOutputFile(char* outputFileName)
{
    FILE*   fp;
    char    keyword[LOM_MAXKEYWORDSIZE];
    Four    logicalId;
    char*   buffer;

    fp = Util_fopen(outputFileName, "r");   // open file for reading and writing
    if(fp == NULL)
    {
        printf("ERROR: Can't open file '%s' for reading.\n", outputFileName);
        OOSQL_ERR(eUNIXFILEOPENERROR_UTIL);
    }
    
    lastLogicalId = 0;
    buffer = (char*)malloc(1024 * 1024);
    if(buffer == NULL) OOSQL_ERR(eOUTOFMEMORY_UTIL);
    while(Util_fgets(buffer, 1024 * 1024, fp) != NULL)
    {
        if(sscanf(buffer, "%s %ld", keyword, &logicalId) == 2)
            lastLogicalId = logicalId;
        else
            break;
    }
    free(buffer);

    Util_fclose(fp);

    return eNOERROR;
}

Four ExtractKeyword::skipFilePosToLastLogicalId(FILE *fp)
{
    char    keyword[LOM_MAXKEYWORDSIZE];
    Four    logicalId;
    char*   buffer;
    filepos_t pos;

    Util_fseek(fp, 0, SEEK_SET);
    buffer = (char*)malloc(1024 * 1024);
    if(buffer == NULL) OOSQL_ERR(eOUTOFMEMORY_UTIL);
    pos = Util_ftell(fp);
    while(Util_fgets(buffer, 1024 * 1024, fp) != NULL)
    {
        if(sscanf(buffer, "%s %ld", keyword, &logicalId) != 2)
            break;

        if(logicalId >= lastLogicalId)
        {
            Util_fseek(fp, pos, SEEK_SET);
            break;
        }
        pos = Util_ftell(fp);
    }
    free(buffer);
    return eNOERROR;
}

Four ExtractKeyword::getFilePosFromSaveFile(char* outputFileName)
{
    FILE*   fp;
    char    saveFileName[MAXPATHLEN];
    
    strcpy(saveFileName, outputFileName);
    strcat(saveFileName, ".save");

    // open save file   
    fp = Util_fopen(saveFileName, "r");
    if (fp == NULL) 
    {
        printf("ERROR: Can't open file '%s' for reading.\n", saveFileName);
        OOSQL_ERR(eUNIXFILEOPENERROR_UTIL);
    }

#ifndef SUPPORT_LARGE_DATABASE2
    if(Util_fscanf(fp, "%d %d %d %d", &savedDataFileOffset, &savedPostingFileOffset, &objectCount, &lineCount) != 4)
#else
    if(Util_fscanf(fp, "%ld %ld %d %d", &savedDataFileOffset, &savedPostingFileOffset, &objectCount, &lineCount) != 4)
#endif
    {
        Util_fclose(fp);
        OOSQL_ERR(eINTERNAL_ERROR_UTIL);
    }

	if (m_skipLastObject)
    {
		lastLogicalId = lastLogicalId + 1;
		printf("Warning: %ldth object is skipped due to segmentation fault.\n", lastLogicalId);
    }

    Util_fclose(fp);
    
    return eNOERROR;
}

Four ExtractKeyword::moveFilePosOfDataFile()
{
#ifndef SUPPORT_LARGE_DATABASE2
    printf("Moving data file offset by %d bytes to %dth object ...\n", savedDataFileOffset, objectCount);
#else
    printf("Moving data file offset by %ld bytes to %dth object ...\n", savedDataFileOffset, objectCount);
#endif
    
    Util_fseek(inputStream, savedDataFileOffset, SEEK_SET); 

    return eNOERROR;
}

Four ExtractKeyword::moveFilePosOfPostingFile(FILE *fp)
{

#ifndef SUPPORT_LARGE_DATABASE2
    printf("Moving posting file offset by %d bytes to docId (%d) ...\n", savedPostingFileOffset, objectCount);
#else
    printf("Moving posting file offset by %ld bytes to docId (%d) ...\n", savedPostingFileOffset, objectCount);
#endif

    Util_fseek(fp, savedPostingFileOffset, SEEK_SET);

    return eNOERROR;
} 

Four ExtractKeyword::writeFilePosToSaveFile(Four attrNo)
{
    filepos_t posOfDataFile;
    filepos_t posOfPostingFile;
    
    posOfDataFile    = Util_ftell(inputStream);
    posOfPostingFile = Util_ftell(classInfo[numberOfClass-1].outputStreams[attrNo]);
    
    Util_fseek(saveStream, 0, SEEK_SET);

#ifndef SUPPORT_LARGE_DATABASE2
    Util_fprintf(saveStream, "%d %d %d %d", posOfDataFile, posOfPostingFile, objectCount + 1, lineCount); 
#else
    Util_fprintf(saveStream, "%ld %ld %d %d", posOfDataFile, posOfPostingFile, objectCount + 1, lineCount);
#endif

    Util_fflush(saveStream);
    
    return eNOERROR;
}

Four ExtractKeyword::openSaveFileForWriting(char* outputFileName)
{
    char    saveFileName[MAXPATHLEN];

    strcpy(saveFileName, outputFileName);
    strcat(saveFileName, ".save");

    // open save file   
    saveStream = Util_fopen(saveFileName, "w");
    if (saveStream == NULL) 
    {
        printf("ERROR: Can't open file '%s' for reading.\n", saveFileName);
        OOSQL_ERR(eUNIXFILEOPENERROR_UTIL);
    }

#ifdef USE_LARGE_FILE
	Util_fprintf(saveStream, "%lld %lld %d %d %d", savedDataFileOffset, savedPostingFileOffset, objectCount, lastLogicalId, lineCount);
#else
	Util_fprintf(saveStream, "%d %d %d %d %d", savedDataFileOffset, savedPostingFileOffset, objectCount, lastLogicalId, lineCount);
#endif

	fseek(saveStream, 0, SEEK_SET);
	fflush(saveStream);

    return eNOERROR;
}

Four ExtractKeyword::removeSaveFile(char* outputFileName)
{
    char    saveFileName[MAXPATHLEN];

    // close save file
    Util_fclose(saveStream);
    
    strcpy(saveFileName, outputFileName);
    strcat(saveFileName, ".save");

    // remove save file
    unlink(saveFileName);       
    
    return eNOERROR;
}

Four ExtractKeyword::extractKeywordFromDataFile(
    char*   className,
    char*   attrName,
    char*   fileName,
    char*   outputFileName,
    Four    startObjectNo,
    Four    endObjectNo,
    bool    alwaysUsePreviousPostingFile
)
{
    int         idx;
    Four        e;
    int         numOfClassAttr;
    int         numOfDataFileAttr;
    int         ch;
    SentenceWordPositionListBuffer sentenceWordPositionListBuffer(LOM_MAXPOSITIONLENGTH);
    BytePositionListBuffer         bytePositionListBuffer(LOM_MAXPOSITIONLENGTH);


    // file open
    inputStream = Util_fopen(fileName, "r");
    if (inputStream == NULL) 
    {
        printf("ERROR: Can't open file '%s' for reading.\n", fileName);
        OOSQL_ERR(eUNIXFILEOPENERROR_UTIL);
    }

    usePreviousFile = false;
    if(access(outputFileName, F_OK) == 0)
    {
        printf("Posting file '%s' is already exist.\n", outputFileName);
        if(alwaysUsePreviousPostingFile)
        {
            printf("Continue extraction using this posting file.\n");
            usePreviousFile = true;
        }
        else
        {
            printf("Do you want to continue extraction using this posting file to extract keyword? [y/else] : ");

            while(1)
            {
                ch = getchar();
                if(ch == 'y')
                {
                    usePreviousFile = true;
                    break;
                }
                else if(ch != EOF)
                {
                    usePreviousFile = false;
                    break;
                }
            }
        }
    }

    if(usePreviousFile)
    {
#ifndef USE_FILE_RECOVERY
        e = getLastLogicalIdFromPreviousOutputFile(outputFileName);
        OOSQL_CHECK_ERR(e);
#else
        e = getFilePosFromSaveFile(outputFileName);
        if (e < eNOERROR)
        {
            printf("Saved file position of '%s' is not available. So, restart keyword extraction from the first of data file.\n", outputFileName);
            usePreviousFile = false;
            e = makeEmptyOutputFile(outputFileName);
            OOSQL_CHECK_ERR(e);
        }
#endif
    }
    else
    {
        // make empty output file
        e = makeEmptyOutputFile(outputFileName);
        OOSQL_CHECK_ERR(e);
    }

#ifdef USE_FILE_RECOVERY
    e = openSaveFileForWriting(outputFileName);
    OOSQL_CHECK_ERR(e);
#endif

    do {
        // read a line
        if (readLine() == EOS)
        {
            Util_fclose(inputStream);
            printf("Total %ld objects are processed\n", objectCount - startObjectNo);

#ifdef USE_FILE_RECOVERY
            e = removeSaveFile(outputFileName);
            OOSQL_CHECK_ERR(e);
#endif

            return EOS;
        }

        // skip comment line
        if (state == State_Space && lineBuffer[0] == '-' && lineBuffer[1] == '-')
            continue;

        int len;
        len = strlen(lineBuffer);
    
        // skip the leading spaces
        idx = 0;
        while (isspace(lineBuffer[idx]) && state == State_Space) idx++;
    
        // skip empty line
        if (lineBuffer[idx] == '\0') continue;
    
        if (lineBuffer[idx] == '%' && state == State_Space)
        // if this line is a command line
        {
            if (!strncmp(lineBuffer, "%class", 6))
            {
                e = classInfo[numberOfClass++].setClassInfo(lineBuffer+strlen("%class"));
                OOSQL_CHECK_ERR(e);

                numOfClassAttr    = classInfo[numberOfClass-1].getNumOfClassAttr();
                numOfDataFileAttr = classInfo[numberOfClass-1].getNumOfDataFileAttr();

                currentAttribute = 0;
                
#ifdef USE_FILE_RECOVERY
                if (usePreviousFile)
                {
                    e = moveFilePosOfDataFile();
                    OOSQL_CHECK_ERR(e); 
                }
#endif
            }
            else if (!strncmp(lineBuffer, "%index", 6))
            {
                // nothing to do
            }
            else
            {
                fprintf(stderr, "Error at line %ld, column no %ld > %s\n", lineCount, currentAttribute, lineBuffer);
                OOSQL_ERR(eUNKNOWNCOMMAND_UTIL);
            }
        }
        else // if this line is a data line
        {
            for (int i = idx; i < len; i++)  
            {
                /* skip white space when State_Space or State_Set_Space */
                if (isspace(lineBuffer[i]) &&
                    (state == State_Space || state == State_Set_Space) )
                    continue;

                switch (lineBuffer[i])
                {
                case CH_BACKSLASH: // ' or " will follow a backslash
                    if (state == State_SingleQuote ||
                        state == State_DoubleQuote ||
                        state == State_Set_SingleQuote ||
                        state == State_Set_DoubleQuote )
                    {
                        ++i;
                        // convert \n to newline
                        if (lineBuffer[i] == 'n' || lineBuffer[i] == '\0')
                            *curPtr++ = '\n';
                        else *curPtr++ = lineBuffer[i];
                    }
                    else
                    {
                        fprintf(stderr, "Error at line %ld, column no %ld > %s\n", lineCount, i, lineBuffer);
                        OOSQL_ERR(eILLEGALUSEOFBACKSLASH_UTIL);
                    }
                    break;
                case CH_DOUBLEQUOTE:
                    if (state == State_Space) state = State_DoubleQuote;
                    else if (state == State_Set_Space)
                        state = State_Set_DoubleQuote;
                    else if (state == State_Numeric ||
                             state == State_Set_Numeric)
                    {
                        fprintf(stderr, "Error at line %ld, column no %ld > %s\n", lineCount, i, lineBuffer);
                        OOSQL_ERR(eILLEGALUSEOFDOUBLEQUOTE_UTIL);
                    }
                    else if (state == State_SingleQuote ||
                             state == State_Set_SingleQuote)
                        *curPtr++ = lineBuffer[i];
                    else if (state == State_DoubleQuote)
                    {
                        state = State_Space;
                        *curPtr = '\0';
                        curPtr = buffer;

                        e = storeAttributeValue(currentAttribute, curPtr);
                        OOSQL_CHECK_ERROR(e);

                        currentAttribute++;
                    }
                    else if (state == State_Set_DoubleQuote)
                        state = State_Set_Space;
                    break;
                case CH_SINGLEQUOTE:
                    if (state == State_Space)
                        state = State_SingleQuote;
                    else if (state == State_Set_Space)
                        state = State_Set_SingleQuote;
                    else if (state == State_DoubleQuote ||
                             state == State_Set_DoubleQuote)
                        *curPtr++ = lineBuffer[i];
                    else if (state == State_Numeric ||
                             state == State_Set_Numeric)
                    {
                        fprintf(stderr, "Error at line %ld, column no %ld > %s\n", lineCount, i, lineBuffer);
                        OOSQL_ERR(eILLEGALUSEOFSINGLEQUOTE_UTIL);
                    }
                    else if (state == State_SingleQuote)
                    {
                        state = State_Space;
                        *curPtr = '\0';
                        curPtr = buffer;

                        e = storeAttributeValue(currentAttribute, curPtr);
                        OOSQL_CHECK_ERROR(e);

                        currentAttribute++;
                    }
                    else if (state == State_Set_SingleQuote)
                        state = State_Set_Space;
                    break;
                case CH_SPACE:
                case CH_TAB:
                    if (state == State_Numeric)
                    {
                        state = State_Space;
                        *curPtr = '\0';
                        curPtr = buffer;

                        e = storeAttributeValue(currentAttribute, curPtr);
                        OOSQL_CHECK_ERROR(e);

                        currentAttribute++;
                    }
                    if (state == State_Set_Numeric)
                        state = State_Set_Space;
                    else if (state == State_DoubleQuote ||
                             state == State_SingleQuote ||
                             state == State_Set_DoubleQuote ||
                             state == State_Set_SingleQuote)
                        *curPtr++ = lineBuffer[i];
                    break;
                case CH_LEFTBRACE:
                    if (state == State_Space || state == State_Numeric)
                        state = State_Set_Space;
                    else if (state == State_DoubleQuote ||
                             state == State_SingleQuote ||
                             state == State_Set_DoubleQuote ||
                             state == State_Set_SingleQuote)
                        *curPtr++ = lineBuffer[i];
                    else
                    {
                        fprintf(stderr, "Error at line %ld, column no %ld > %s\n", lineCount, i, lineBuffer);
                        OOSQL_ERR(eILLEGALUSEOFLEFTBRACE_UTIL);
                    }
                    break;
                case CH_RIGHTBRACE:
                    if (state == State_Set_Space || state == State_Set_Numeric)
                    {
                        currentAttribute++;
                        state = State_Space;
                    }
                    else if (state == State_DoubleQuote ||
                             state == State_SingleQuote ||
                             state == State_Set_DoubleQuote ||
                             state == State_Set_SingleQuote)
                        *curPtr++ = lineBuffer[i];
                    else
                    {
                        fprintf(stderr, "Error at line %ld, column no %ld > %s\n", lineCount, i, lineBuffer);
                        OOSQL_ERR(eILLEGALUSEOFRIGHTBRACE_UTIL);
                    }
                    break;
                default:
                    if (state == State_Space)
                        state = State_Numeric;
                    else if (state == State_Set_Space)
                        state = State_Set_Numeric;
                    *curPtr++ = lineBuffer[i];
                    break;
                }

                if (curPtr - buffer >= bufferSize)
                {
                    char* tmp;
                    tmp = (char*)realloc(buffer, bufferSize*2);
                    if (tmp == NULL)
                    {
                        fprintf(stderr, "Error at line %ld, column no %ld > %s\n", lineCount, i, lineBuffer);
                        OOSQL_ERR(eOUTOFMEMORY_UTIL);
                    }
                    buffer = tmp;
                    curPtr = buffer + bufferSize;
                    bufferSize *= 2;
                }
            }

            if (state == State_Numeric)
            {
                state = State_Space;
                curPtr = buffer;

                e = storeAttributeValue(currentAttribute, lineBuffer);
                OOSQL_CHECK_ERROR(e);

                currentAttribute++;
            }
            else if (state == State_Set_Numeric)
            {
                state = State_Set_Space;
                curPtr = buffer;

                e = storeAttributeValue(currentAttribute, lineBuffer);
                OOSQL_CHECK_ERROR(e);

                currentAttribute++;
            }

            if (currentAttribute == numOfDataFileAttr)
            {
                if(objectCount >= startObjectNo) m_isAfterStartObject = true;
                else m_isAfterStartObject = false;

                if((endObjectNo == -1) || (objectCount <= endObjectNo)) m_isBeforeEndObject = true;
                else m_isBeforeEndObject = false;

                if(!m_isBeforeEndObject)
                {
                    Util_fclose(inputStream);
                    printf("Total %ld objects are processed\n", objectCount - startObjectNo);

#ifdef USE_FILE_RECOVERY
                    e = removeSaveFile(outputFileName);
                    OOSQL_CHECK_ERR(e);
#endif

                    return EOS;
                }
                
                if((objectCount >= lastLogicalId) && m_isAfterStartObject)
                {
                    e = extractKeywordFromBuffer(className, attrName, outputFileName, sentenceWordPositionListBuffer, bytePositionListBuffer);
                    OOSQL_CHECK_ERROR(e);
                }

                objectCount++;
                currentAttribute = 0;

                if(!(objectCount % 100))
                {
                    if((objectCount >= lastLogicalId) && m_isAfterStartObject)
                        printf("Total %ld objects are processed\n", objectCount - startObjectNo);
                    else if(m_isAfterStartObject)
                    {
                        if(!(objectCount % 10000))
                            printf("Total %ld objects are skipped\n", objectCount);
                    }
                }
            }
        }
    } while (1);

    return eNOERROR;
}

Four ExtractKeyword::writeToPostingFile(Four attrNo, char* str)
{
    if (str[0] != '\n')
    {
        if(Util_fprintf(classInfo[numberOfClass-1].outputStreams[attrNo], "%s ", str) < 0)
            OOSQL_ERR(eUNIXFILEWRITEERROR_UTIL);
    }
    else
    {
        if(Util_fprintf(classInfo[numberOfClass-1].outputStreams[attrNo], "\n") < 0)
            OOSQL_ERR(eUNIXFILEWRITEERROR_UTIL);
    }
    return eNOERROR;
}

Four ExtractKeyword::flushPostingFile(Four attrNo)
{
    Util_fflush(classInfo[numberOfClass-1].outputStreams[attrNo]);
    return eNOERROR;
}

static Four IsInvalidKeyword(char* str)
{
    char* s = str;

    while(*s)
    {
        if(!(*s == ' ' || *s == '\t' || *s == '\n'))
            break;
        s ++;
    }

    if(*s == '\0')
        return 1;  

    while(*s)
    {
        if(*s == ' ' || *s == '\t' || *s == '\n')
            break;
        s ++;
    }

    if(*s == '\0')
        return 0;   
    
    while(*s)
    {
        if(!(*s == ' ' || *s == '\t' || *s == '\n'))
            break;
        s ++;
    }

    if(*s == '\0')
        return 0;   
    else
        return 1;  
}

Four ExtractKeyword::storeAttributeValue(Four dataFileAttrNo, char* attrValue)
{
    Four attrNo;

    attrNo = classInfo[numberOfClass- 1].dataFileAttrNoToClassAttrNo[dataFileAttrNo];
    if(attrNo == NIL)
        return eNOERROR;

    if((objectCount >= lastLogicalId) && m_isAfterStartObject)
    {
        if(attrValues.size() < attrNo + 1)
            attrValues.resize(attrNo + 1);

        attrValues[attrNo] = attrValue;
    }

    return eNOERROR;
}

Four getTimeValue(
    char *strBuf,
    unsigned short *hour,
    unsigned short *minute,
    unsigned short *second
)
{
    Four i;
    Four j;
    Four k;
    char temp[10];

    j = 0;
    for (i=j, k=0; i<strlen(strBuf); i++, k++)  
        if (strBuf[i] != ':') temp[k] = strBuf[i];
        else break;

    temp[i] = '\0';
    *hour = atoi(temp);

    if (*hour < 0 || *hour > 24)
    {
        printf("The value for hour is invalid : %ld\n", *hour);
        return eBADPARAMETER_UTIL;
    }

    j = i;
    for (i=j+1, k=0; i<strlen(strBuf); i++, k++)  
        if (strBuf[i] != ':') temp[k] = strBuf[i];
        else break;

    temp[k] = '\0';
    *minute = atoi(temp);

    if (*minute < 0 || *minute > 60) {
        printf("The value for minute is invalid : %ld\n", *minute);
        return eBADPARAMETER_UTIL;
    }

    j = i;
    for (i=j+1, k=0; i<strlen(strBuf); i++, k++)  
        temp[k] = strBuf[i];

    temp[k] = '\0';
    *second = atoi(temp);

    if (*second < 0 || *second > 60) {
        printf("The value for second is invalid : %ld\n", *second);
        return eBADPARAMETER_UTIL;
    }

    return eNOERROR;
}

static Four getDateValue(
    char *strBuf,
    unsigned short *year,
    unsigned short *month,
    unsigned short *day
)
{
    Four i;
    Four j;
    Four k;
    char temp[10];

    j = 0;
    for (i=j, k=0; i<strlen(strBuf); i++, k++)  
        if (strBuf[i] != '/') temp[k] = strBuf[i];
        else break;

    temp[i] = '\0';
    *month = atoi(temp);

    if (*month < 1 || *month > 12) {
        printf("The value for month is invalid : %ld\n", *month);
        return eBADPARAMETER_UTIL;
    }

    j = i;
    for (i=j+1, k=0; i<strlen(strBuf); i++, k++)  
        if (strBuf[i] != '/') temp[k] = strBuf[i];
        else break;

    temp[k] = '\0';
    *day = atoi(temp);

    if (*day < 1 || *day > 31) {
        printf("The value for day is invalid : %ld\n", *day);
        return eBADPARAMETER_UTIL;
    }

    j = i;
    for (i=j+1, k=0; i<strlen(strBuf); i++, k++)  
        temp[k] = strBuf[i];

    temp[k] = '\0';
    *year = atoi(temp);

    if (*year < 0 || *year > 9999) {
        printf("The value for year is invalid : %ld\n", *year);
        return eBADPARAMETER_UTIL;
    }

    return eNOERROR;
}

static int MakeLowercase(char *buffer)
{
    int i, j;
    int length;

    length = strlen(buffer);
    for(i = 0; i < length; i++) 
    {
        if((unsigned char)buffer[i] > (unsigned char)0x80)
            i++;
        else
            buffer[i] = tolower(buffer[i]);
    }
    
    for(i = 0; i < length; i++)
    {
        if(!isspace(buffer[i]) || (unsigned)buffer[i] > (unsigned)0x80)
            break;
    }

    if(i != 0)
    {
        for(j = 0; i < length; i++, j++)
            buffer[j] = buffer[i];
        buffer[j] = '\0';
    }

    length = strlen(buffer);
    for(i = length - 1; i >= 0; i--)
    {
        if(i > 0 && (unsigned)buffer[i - 1] > (unsigned)0x80)
            break;
        if(!isspace(buffer[i]))
            break;
    }
    
    if(i != length - 1)
        buffer[i] = '\0';   

    return eNOERROR;
}


Four ExtractKeyword::extractKeywordFromBuffer(char* className, char* attrName, char* outputFileName, SentenceWordPositionListBuffer& sentenceWordPositionListBuffer, BytePositionListBuffer& bytePositionListBuffer)
{
    char buf[1024];
    Four e;
    Four nPos;
    lom_FptrToFilter                    fptrToFilter;
    lom_FptrToKeywordExtractor          fptrToKeywordExtractor;
    lom_FptrToGetNextPostingInfo        fptrToGetNextPostingInfo;
    lom_FptrToFinalizeKeywordExtraction fptrToFinalizeKeywordExtraction;
    FILE *fp;
    Four attrType;
    Four resultHandle;
    char keyword[LOM_MAXKEYWORDSIZE];
    Four i;
    Four attrNo;
    Four numOfClassAttr;
    catalog_SysIndexesOverlay* indexInfo;


    // find attribute and determine attrNo
    if(strcmp(className, classInfo[numberOfClass-1].getClassName()) != 0)
        return eNOERROR;

    attrNo = classInfo[numberOfClass-1].classAttrNameToClassAttrNo[attrName];

    attrType = classInfo[numberOfClass-1].getNthAttrType(attrNo);

    if (attrType == ATTRTYPE_TEXT)
    {
        if (classInfo[numberOfClass-1].outputStreams[attrNo] == NULL)
        {
            if(usePreviousFile)
                classInfo[numberOfClass-1].outputStreams[attrNo] = Util_fopen(outputFileName, "r+");
            else
                classInfo[numberOfClass-1].outputStreams[attrNo] = Util_fopen(outputFileName, "w");
            if (classInfo[numberOfClass-1].outputStreams[attrNo] == NULL)
            {
                printf("ERROR: Can't open file '%s' for writing.\n", outputFileName);
                OOSQL_ERR(eUNIXFILEOPENERROR_UTIL);
            }
            if(usePreviousFile)
            {
#ifndef USE_FILE_RECOVERY
                e = skipFilePosToLastLogicalId(classInfo[numberOfClass-1].outputStreams[attrNo]);
                OOSQL_CHECK_ERR(e);
#else
                e = moveFilePosOfPostingFile(classInfo[numberOfClass-1].outputStreams[attrNo]);
                OOSQL_CHECK_ERR(e);
#endif
            }
        }
    }
    else
        return eNOERROR;

    strcpy(buffer, attrValues[attrNo].c_str());

    if (strlen(buffer) == 0) return eNOERROR;

    /* get keyword extractor and filter function pointers */
    classInfo[numberOfClass-1].getNthAttrKeywordExtractor(attrNo, fptrToKeywordExtractor, fptrToGetNextPostingInfo, fptrToFinalizeKeywordExtraction);
    fptrToFilter = classInfo[numberOfClass-1].getNthAttrFilter(attrNo);

    /* get posting information */
    indexInfo = classInfo[numberOfClass-1].getNthAttrInfo(attrNo);
    Boolean isContainingSentenceAndWordNum = indexInfo->kdesc.invertedIndex.postingInfo.isContainingSentenceAndWordNum;
    Boolean isContainingByteOffset         = indexInfo->kdesc.invertedIndex.postingInfo.isContainingByteOffset;
    Boolean isContainingTupleID            = indexInfo->kdesc.invertedIndex.postingInfo.isContainingTupleID;
    Two     nEmbeddedAttributes            = indexInfo->kdesc.invertedIndex.postingInfo.nEmbeddedAttributes;
    Two*    embeddedAttrNo                 = indexInfo->kdesc.invertedIndex.postingInfo.embeddedAttrNo;

    /* do keyword extraction */
    if (fptrToFilter)
    {
        /* write buffer to intermediate file */
        fp = Util_fopen(inFile, "w");
        if (inFile == NULL) 
        {
            printf("ERROR: Can't open file '%s' for writing.\n", inFile);
            OOSQL_ERR(eUNIXFILEOPENERROR_UTIL);
        }

        e = Util_fwrite(buffer, strlen(buffer), 1, fp);
        if (e <= 0)
            OOSQL_ERR(eUNIXFILEWRITEERROR_UTIL);

        Util_fclose(fp);

        e = lom_CallDll_Filter(fptrToFilter, TEXT_IN_FILE, NULL, 0, "", NULL, -1, inFile, filteredFile);
        OOSQL_CHECK_ERR(e);

        e = lom_CallDll_KeywordExtractorInit(fptrToKeywordExtractor, TEXT_IN_FILE, NULL, 0, "", NULL, -1, filteredFile, &resultHandle);
        OOSQL_CHECK_ERR(e);
    }
    else
    {
#ifdef USE_IMTERMEDIATE_FILE
        /* write buffer to intermediate file */
        fp = Util_fopen(inFile, "w");
        if (inFile == NULL) 
        {
            printf("ERROR: Can't open file '%s' for writing.\n", inFile);
            OOSQL_ERR(eUNIXFILEOPENERROR_UTIL);
        }

        e = Util_fwrite(buffer, strlen(buffer), 1, fp);
        if (e <= 0)
            OOSQL_ERR(eUNIXFILEWRITEERROR_UTIL);

        Util_fclose(fp);

        e = lom_CallDll_KeywordExtractorInit(fptrToKeywordExtractor, TEXT_IN_FILE, NULL, 0, "", NULL, -1, inFile, &resultHandle);
        OOSQL_CHECK_ERR(e);
#else
        e = lom_CallDll_KeywordExtractorInit(fptrToKeywordExtractor, TEXT_IN_MEMORY, NULL, 0, "", NULL, -1, buffer, &resultHandle);
        OOSQL_CHECK_ERR(e);
#endif
    }

    while ((lom_CallDll_KeywordExtractorNext(fptrToGetNextPostingInfo, resultHandle, keyword, &nPos, (char*)sentenceWordPositionListBuffer.GetBufferPtr())) != TEXT_DONE)
    {
        if(nPos >= sentenceWordPositionListBuffer.GetNElements())
            OOSQL_ERR(eTOOLARGENPOSOTION_UTIL);

        if(usePreviousFile)
        {
            e = flushPostingFile(attrNo);
            OOSQL_CHECK_ERR(e);
        }

        // check keyword
        if(IsInvalidKeyword(keyword))
            continue;

        MakeLowercase(keyword);

        // write keyword
        e = writeToPostingFile(attrNo, keyword);
        OOSQL_CHECK_ERROR(e);

        // write pseudo document id 
        sprintf(buf, "%ld", objectCount);
        e = writeToPostingFile(attrNo, buf);
        OOSQL_CHECK_ERROR(e);

        // write null object id
        if(isContainingTupleID)
        {
            sprintf(buf, "0 0 0 0");
            e = writeToPostingFile(attrNo, buf);
            OOSQL_CHECK_ERROR(e);
        }

        // write number of positions
        sprintf(buf, "%ld", nPos);
        e = writeToPostingFile(attrNo, buf);
        OOSQL_CHECK_ERROR(e);

        // write positions
        if(isContainingSentenceAndWordNum || isContainingByteOffset)
        {
            if(isContainingSentenceAndWordNum)
            {
                for(i = 0; i < nPos; i++)
                {
                    // write sentence and word number
                    sprintf(buf, "%ld %ld", sentenceWordPositionListBuffer.GetElement(i).sentence, 
                                          sentenceWordPositionListBuffer.GetElement(i).word);
                    e = writeToPostingFile(attrNo, buf);
                    OOSQL_CHECK_ERROR(e);
                }
            }
            else
            {
                memcpy((char*)bytePositionListBuffer.GetBufferPtr(), (char*)sentenceWordPositionListBuffer.GetBufferPtr(), 
                        sizeof(BytePositionListStruct) * bytePositionListBuffer.GetNElements());
                for(i = 0; i < nPos; i++)
                {
                    // write byte offset
                    sprintf(buf, "%ld", bytePositionListBuffer.GetElement(i).byteOffset);
                    e = writeToPostingFile(attrNo, buf);
                    OOSQL_CHECK_ERROR(e);
                }
            }
        }

        if(nEmbeddedAttributes > 0)
        {
            for(i = 0; i < nEmbeddedAttributes; i++)
            {
                char encodedStr[LOM_MAXVARSTRINGSIZE];
                char sourceStr[LOM_MAXVARSTRINGSIZE];
                LOM_ColListStruct clist[1];
                char* attrValue = (char*)attrValues[GET_USERLEVEL_COLNO(embeddedAttrNo[i])].c_str();
                unsigned short year, month, day, hour, minute, second;
                LOM_Date date;

                if(attrValue == "NULL")
                {
                    e = writeToPostingFile(attrNo, "1");
                    OOSQL_CHECK_ERROR(e);
                    continue;
                }
                else
                {
                    e = writeToPostingFile(attrNo, "0");
                    OOSQL_CHECK_ERROR(e);
                }

                switch(classInfo[numberOfClass-1].getNthAttrType(GET_USERLEVEL_COLNO(embeddedAttrNo[i])))
                {
                case ATTRTYPE_NUMERIC:
                    switch(classInfo[numberOfClass-1].getNthAttrRealType(GET_USERLEVEL_COLNO(embeddedAttrNo[i])))
                    {
                    case LOM_DATE:
                        e = getDateValue(attrValue, &year, &month, &day);
                        OOSQL_CHECK_ERROR(e);

                        LOM_SetDate(NULL, year, month, day, &date);
                        
                        sprintf(encodedStr, "%ld", (Four)date);

                        e = writeToPostingFile(attrNo, encodedStr);
                        OOSQL_CHECK_ERROR(e);
                        break;
                    case LOM_TIME:
                        e = getTimeValue(attrValue, &hour, &minute, &second);
                        OOSQL_CHECK_ERROR(e);

                        sprintf(encodedStr, "%ld:%ld:%ld:%ld:%ld:%ld", 0, 0, 0, hour, minute, second);

                        e = writeToPostingFile(attrNo, encodedStr);
                        OOSQL_CHECK_ERROR(e);
                        break;
                    case LOM_SHORT:
                    case LOM_INT:
                    case LOM_LONG:
                        sprintf(encodedStr, "%ld", atoi(attrValue));
                        e = writeToPostingFile(attrNo, encodedStr);
                        OOSQL_CHECK_ERROR(e);
                        break;
                    case LOM_LONG_LONG:
                        sprintf(encodedStr, "%ld", atoll(attrValue));
                        e = writeToPostingFile(attrNo, encodedStr);
                        OOSQL_CHECK_ERROR(e);
                        break;
                    case LOM_FLOAT:
                    case LOM_DOUBLE:
                        sprintf(encodedStr, "%f", atof(attrValue));
                        e = writeToPostingFile(attrNo, encodedStr);
                        OOSQL_CHECK_ERROR(e);
                        break;
                    default:
                        e = writeToPostingFile(attrNo, attrValue);
                        OOSQL_CHECK_ERROR(e);
                        break;
                    }
                    break;

                case ATTRTYPE_VARCHAR:
                    // string encoding
                    strncpy(sourceStr, attrValue, sizeof(sourceStr)); 
                    clist[0].data.ptr  = sourceStr;
                    clist[0].retLength = sizeof(sourceStr);
                    lom_EncodeStringVal(clist, encodedStr);
                    e = writeToPostingFile(attrNo, encodedStr);
                    OOSQL_CHECK_ERROR(e);
                    break;

                default:
                    OOSQL_ERR(eUNHANDLED_CASE_UTIL);
                    break;
                }
            }
        }

        e = writeToPostingFile(attrNo, "\n");
        OOSQL_CHECK_ERROR(e);
    }

    e = flushPostingFile(attrNo);
    OOSQL_CHECK_ERR(e);

#ifdef USE_FILE_RECOVERY
    if (m_skipLastObject || !((objectCount + 1) % 100))
    {
        e = writeFilePosToSaveFile(attrNo);
        OOSQL_CHECK_ERR(e);
    }
#endif

    e = lom_CallDll_KeywordExtractorFinal(fptrToFinalizeKeywordExtraction, resultHandle);
    OOSQL_CHECK_ERR(e);

    return eNOERROR;
}

Four oosql_Tool_ExtractKeyword(
    OOSQL_SystemHandle* handle,
    Four                volId,
    char*               className,
    char*               attrName,
    char*               dataFileName,
    char*               outputFileName,
    Four                startObjectNo,
    Four                endObjectNo,
    Four                alwaysUsePreviousPostingFile
)
{
    ExtractKeyword* ek;
    Four            e;
    char*           tempDir;
    char            fullOutputFilename[MAXPATHLEN];

    ek = new ExtractKeyword(*handle, volId);

    tempDir = getenv("ODYS_TEMP_PATH");
    if (tempDir == NULL)
        OOSQL_CHECK_ERROR(eTEMPDIRNOTDEFINED_UTIL);
    sprintf(fullOutputFilename, "%s%s%s", tempDir, DIRECTORY_SEPARATOR, outputFileName);

    if(alwaysUsePreviousPostingFile)
        e = ek->extractKeywordFromDataFile(className, attrName, dataFileName, fullOutputFilename, startObjectNo, endObjectNo, true);
    else
        e = ek->extractKeywordFromDataFile(className, attrName, dataFileName, fullOutputFilename, startObjectNo, endObjectNo, false);
    OOSQL_CHECK_ERROR(e);

    delete ek;

    return eNOERROR;
}

#else /* SLIMDOWN_TEXTIR */

#include "OOSQL_StorageSystemHeaders.h"
#include "OOSQL_APIs_Internal.hxx"
#include "OOSQL_Error.h"
#include "OOSQL_Tool.hxx"

Four oosql_Tool_ExtractKeyword(
    OOSQL_SystemHandle* handle,
    Four                volId,
    char*               className,
    char*               attrName,
    char*               dataFileName,
    char*               outputFileName,
    Four                startObjectNo,
    Four                endObjectNo,
    Four                alwaysUsePreviousPostingFile
)
{
    return eTEXTIR_NOTENABLED_OOSQL;
}

#endif /* SLIMDOWN_TEXTIR */

