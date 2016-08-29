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

#ifndef __MERGE_POSTING_H__
#define __MERGE_POSTING_H__


#ifndef WIN32
#include <sys/param.h>
#include <unistd.h>
#else
#include <io.h>
#endif
#include <malloc.h>
#include <string.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <string>
#include <vector>

using namespace std;

#ifdef WIN32
#define DIRECTORY_SEPARATOR "\\"
#define R_OK 04
#define F_OK 00
#else
#define DIRECTORY_SEPARATOR "/"
#endif

#ifndef MAXPATHLEN
#define MAXPATHLEN 1024
#endif

enum SortMergeMode {
	NOMERGE = 0,
	UNSORTEDMERGE = 1,
	SORTEDMERGE = 2
};


class MergePostingFiles
{
public:
    MergePostingFiles(LOM_Handle* handle, Four nPostingFiles, char** postingFileNames, char* tempDirPath);
    virtual ~MergePostingFiles();

    inline Four m_openPostingFiles();
	inline Four m_findMinPosting(string& stringBuffer);
	inline Four m_closePostingFiles();

	inline Four m_getNextPosting(Four index);
	inline Four m_comparePosting(Four index1, Four index2);
	inline Four m_createLoserTree(Four treeSize);
	inline Four m_updateLoserTree(Four treeSize, Four oldWinner);
	
private:
	LOM_Handle*			m_handle;
	Four 				m_nPostingFiles;
	Four				m_nActivePostingFiles;
	char				m_tempDirPath[MAXPATHLEN];
	vector<string>		m_postingFileNames;
	vector<FILE*> 		m_filePointers;
	vector<string> 		m_postingBuffers;
	bool				m_isAllPostingRead;
	vector<Four>		m_loserTree;	
};

inline Four MergePostingFiles::m_openPostingFiles(void)
{
	char			filename[MAXPATHLEN];
	char			readBuffer[1024];
	char*			postingFileNamePtr;
	string			lineBuffer;
	Four			count;
	Four			e;

	count = 0;

	for (int i = 0; i < m_nPostingFiles; i++)
	{
		postingFileNamePtr = (char *)m_postingFileNames[i].c_str();
		sprintf(filename, "%s%s%s", m_tempDirPath, DIRECTORY_SEPARATOR, postingFileNamePtr);
		
		if (access(filename, R_OK) == 0)
		{
			fprintf(stderr, "About to open: %s file. in %s:%ld\n", filename, __FILE__,__LINE__);
			scanf(" %d", &e);
			e = lom_Text_OpenTempFile(m_handle, filename, "r", &(m_filePointers[count]));
			OOSQL_CHECK_ERROR(e);


			if (m_getNextPosting(count) != EOS) count++;
			else
			{
				e = lom_Text_CloseTempFile(m_handle, m_filePointers[count]);
				OOSQL_CHECK_ERROR(e);
			}
		}
	}
	
	m_nPostingFiles = m_nActivePostingFiles = count;
	if (m_nPostingFiles == 0) m_isAllPostingRead = true;

	/* allocate memory for a loser tree */
	m_loserTree.resize(m_nPostingFiles);

	/* create a loser tree */
	e = m_createLoserTree(m_nPostingFiles);
	OOSQL_CHECK_ERROR(e);
	
	return eNOERROR;
}

inline Four MergePostingFiles::m_findMinPosting(string& stringBuffer)
{
	Four	minPostingFileNum;
	Four	e;

	if (m_isAllPostingRead) return EOS;
	
	minPostingFileNum = m_loserTree[0];
	minPostingFileNum = m_updateLoserTree(m_nActivePostingFiles, minPostingFileNum);
	OOSQL_CHECK_ERROR(minPostingFileNum);

	stringBuffer = m_postingBuffers[minPostingFileNum];

	if (m_getNextPosting(minPostingFileNum) == EOS)
	{
		FILE* tempFilePointer = m_filePointers[minPostingFileNum];
		for (int j = minPostingFileNum; j < (m_nPostingFiles - 1); j++)
		{
			m_filePointers[j]   = m_filePointers[j + 1];
			m_postingBuffers[j] = m_postingBuffers[j + 1]; 
		}
		m_filePointers[m_nPostingFiles - 1] = tempFilePointer;
		
		m_nActivePostingFiles --;
		if (m_nActivePostingFiles == 0) m_isAllPostingRead = true;
		else
		{
			/* recreate a loser tree because one leaf node should be removed */
			e = m_createLoserTree(m_nActivePostingFiles);
			OOSQL_CHECK_ERROR(e);
		}
	}

	return eNOERROR;
}

inline Four MergePostingFiles::m_closePostingFiles(void)
{
	Four		e;

	for (int i = 0; i < m_nPostingFiles; i++)
	{
		e = lom_Text_CloseTempFile(m_handle, m_filePointers[i]);
		OOSQL_CHECK_ERROR(e);
	}
	
	return eNOERROR;
}

inline Four MergePostingFiles::m_getNextPosting(Four index)
{
	char	keyword[1024];
	Four	pseudoDocID;
	char*	postingBufferPtr;
	char	readBuffer[1024];
	string	lineBuffer;

	do {

		m_postingBuffers[index] = "";
		while (1)
		{
			if (Util_fgets(readBuffer, sizeof(readBuffer), m_filePointers[index]) != NULL)
			{
				lineBuffer = readBuffer;
				m_postingBuffers[index] = m_postingBuffers[index] + lineBuffer;
	
				if(readBuffer[strlen(readBuffer) - 1] == '\n' || strlen(readBuffer) < sizeof(readBuffer) - 1)
					break;
			}
			else
				return EOS;
		}

		postingBufferPtr = (char *)m_postingBuffers[index].c_str();

#ifdef DEBUG
		if (sscanf(postingBufferPtr, "%s %d", keyword, &pseudoDocID) != 2)
			printf("Invalid posting exists in %dth file. Skipping ...\n%s", index, postingBufferPtr);
#endif

	} while (sscanf(postingBufferPtr, "%s %d", keyword, &pseudoDocID) != 2);

	return eNOERROR;
}

inline Four MergePostingFiles::m_comparePosting(Four index1, Four index2)
{
	register char   keyword1[1024], keyword2[1024];
	register Four   pseudoDocID1, pseudoDocID2;
	register char*  postingBufferPtr;

	postingBufferPtr = (char *)m_postingBuffers[index1].c_str();
	sscanf(postingBufferPtr, "%s %d", keyword1, &pseudoDocID1);

	postingBufferPtr = (char *)m_postingBuffers[index2].c_str();
	sscanf(postingBufferPtr, "%s %d", keyword2, &pseudoDocID2);

	if((strcmp(keyword1, keyword2) < 0) || (!strcmp(keyword1, keyword2) && pseudoDocID1 < pseudoDocID2))
		return -1;		/* index1 is smaller */
	else 
		return 1;		/* index2 is smaller */
}

/* NOTE:
 *  The two functions below are borrowed from util_LoserTree.c in COSMOS.
 */

inline Four MergePostingFiles::m_createLoserTree(
	Four	treeSize 		/* IN number of 'Tree of Loser''s leaf nodes */
)
{
	Four	i, j, k;		/* variables for indexing */
	Four	winner;			/* variable which contains index of winner, i.e. smaller one */
	Four	tmp;			/* temporary variable for swapping */

	/* initialize 'Tree of Loser' */
	for (i = 0; i < treeSize; i++) m_loserTree[i] = -1;

	/* i -> index of 'tuples', j-> index of 'm_loserTree' */

	/* initialize index */
	i = 0;
	j = treeSize / 2;

	/* determine each internal node's value */
	if (treeSize & 1 == 1) {
		m_loserTree[j] = i;
		i += 1;          
		j += 1;
	}
	else {
		if (m_comparePosting(i, i + 1) > 0) {
			m_loserTree[j] = i;
			m_loserTree[j/2] = i + 1;
		}
		else {
			m_loserTree[j] = i + 1;
			m_loserTree[j/2] = i;
		}

		i += 2;
		j += 1;
	}
	while (j < treeSize) {

		/* determine winner */
		if (m_comparePosting(i, i + 1) > 0) {
			m_loserTree[j] = i;
			winner = i + 1;
		}
		else {
			m_loserTree[j] = i + 1;
			winner = i;
		}

		/* propagate the winner upwards */
		k = j / 2;
		while (1) {
			if (m_loserTree[k] == -1) {
				m_loserTree[k] = winner;
				break;
			}
			if (m_comparePosting(m_loserTree[k], winner) < 0) {
				tmp = m_loserTree[k];
				m_loserTree[k] = winner;
				winner = tmp;
			}
			k /= 2;
		}

		i += 2;
		j += 1;
	}

	return (eNOERROR);
}

inline Four MergePostingFiles::m_updateLoserTree(
	Four	treeSize,		/* IN number of 'Tree of Loser''s leaf node */
	Four	oldWinner 		/* IN index of old winner in 'm_loserTree' */
)
{
	Four	i;				/* index variable */
	Four	tmp;			/* temporary variable for swapping */
	Four	winner;			/* variable which contains index of winner */

	/* initialize winner */
	winner = oldWinner;

	/* propagate to root */
	for (i = (treeSize + winner) / 2; i > 0; i /= 2) {
		if (m_comparePosting(m_loserTree[i], winner) < 0) {
			tmp = m_loserTree[i];
			m_loserTree[i] = winner;
			winner = tmp;
		}
	}

	/* set winner of 'Tree of Loser' - m_loserTree[0] must always contain winner */
	m_loserTree[0] = winner;

	return (winner);
}

#endif		// __MERGE_POSTING_H__

#endif 		// SLIMDOWN_TEXTIR
