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

/*
* Description:  Header file for specialised memory management routines.
*               Two systems are provided. The OOSQL_TCMemStack provides fast
*               and efficient mark and release style memory management
*               (note that destructors are _not_ called by this type of
*               memory management). The OOSQL_TCMemBlocks is a specialisation of
*               the OOSQL_TCMemStack class that maintains a free list of all
*               freed blocks for quick reallocation, but requires all
*               objects allocated to be of the same size.
*
*
****************************************************************************/

#ifndef _OOSQL_TCL_MEMMGR_H_
#define _OOSQL_TCL_MEMMGR_H_

#include "OOSQL_Techlib.hxx"
#include <stddef.h>

#ifndef MIN
#define MIN(x, y) (((x)<(y))?(x):(y))
#endif

#ifndef MAX
#define MAX(x, y) (((x)>(y))?(x):(y))
#endif


/*--------------------------- Class Definitions ---------------------------*/

//---------------------------------------------------------------------------
// The following class manages a stack of arbitrary sized blocks, allocating
// memory from a list of large sized blocks for efficiency. Fast mark
// and release memory management can be performed using the OOSQL_TCMemMarker class.
//
// Note that blocks can only be allocated, not freed using the OOSQL_TCMemStack. The
// allocated memory will be freed when a OOSQL_TCMemMarker object goes out of
// scope.
//
// Note that when the memory is freed by the OOSQL_TCMemMarker, no destructors will
// be called, so use with care.
//---------------------------------------------------------------------------

class OOSQL_TCMemStack {
protected:
    struct BlockList {
        BlockList   *next;          // Pointer to next block in list
        }       *curBlock;          // List of memory pool blocks
    size_t      blockSize;          // Size of memory pool blocks
    size_t      blockCount;         // Number of memory pool blocks allocated
    size_t      curLoc;             // Location within current pool block

            // Return a char * to start of current memory pool
            char *poolStart()   { return (char*)(curBlock+1); };

            // Allocate a new memory pool block
            bool allocatePoolBlock();

            // Free all pool blocks from 'block' to current maximum
            void freeFrom(UFour block);

public:
            // Constructor given size of pool to allocate
            OOSQL_TCMemStack(size_t size = 8192)
                : curBlock(NULL), blockSize(size), blockCount(0),
                  curLoc(size)
                { CHECK(size != 0); };

            // Destructor - free's all memory
            ~OOSQL_TCMemStack() { freeFrom(0); };

            // Allocate a block from the pool of specified size
            void *allocate(size_t size);

private:
    friend  class OOSQL_TCMemMarker;
    };

//---------------------------------------------------------------------------
// The following class is used to mark the current position in a OOSQL_TCMemStack.
// When the OOSQL_TCMemMarker goes out of scope or is deleted, all memory allocated
// after the OOSQL_TCMemMarker was constructed will be automatically freed.
//---------------------------------------------------------------------------

class OOSQL_TCMemMarker {
protected:
    UFour       block;          // Number of currently allocated pool block
    size_t      curLoc;         // Location within current pool block
    OOSQL_TCMemStack&   mem;            // OOSQL_TCMemStack begin marked

public:
            // Constructor
            OOSQL_TCMemMarker(OOSQL_TCMemStack& mem)
                : block(mem.blockCount), curLoc(mem.curLoc), mem(mem) {};

            // Destructor
            ~OOSQL_TCMemMarker()
            {   PRECONDITION(block < mem.blockCount ||
                    (block == mem.blockCount && curLoc <= mem.curLoc));
                mem.freeFrom(block);
                mem.curLoc = curLoc;
            };
    };

//---------------------------------------------------------------------------
// The following class manages a number of fixed sized blocks very
// efficiently using a simple free list mechanism. Classes can use this
// class internally to overload the default memory management for speed
// (be careful with derived classes however!).
//---------------------------------------------------------------------------

class OOSQL_TCMemBlocks : public OOSQL_TCMemStack {
protected:
    void    *freeList;              // Head of the free list
    UFour   size;                   // Size of allocated blocks

public:
            // Constructor given the size and number of blocks to allocate
            OOSQL_TCMemBlocks(UFour size,UFour count = 100)
                : OOSQL_TCMemStack(size * count), freeList(NULL),
                  size(MAX(size,sizeof(void*)))
            {   PRECONDITION(size >= sizeof(void*));
                CHECK(size != 0 && count != 0); };

            // Method to allocate a new block
            void *allocate(size_t sz)
            {
                sz = sz;
                PRECONDITION(size == sz);
                if (freeList == NULL)
                    return OOSQL_TCMemStack::allocate(size);
                else {
                    void *temp = freeList;
                    freeList = *(void **)temp;
                    return temp;
                    }
            };


            // Method to free a used block
            void free(void *p)
            {   *(void **)p = freeList;
                freeList = p;
            };

            // Method to free all used blocks at once (including free list)
            void freeAll()
            {   freeFrom(0);
                curLoc = blockSize;
                freeList = NULL;
            };
    };

/*--------------------------- Inline Functions ----------------------------*/

//---------------------------------------------------------------------------
// Overloaded operator new to allocate memory from a memory stack
// (rather than having to call OOSQL_TCMemStack::allocate()) directly
//---------------------------------------------------------------------------

inline void *operator new(size_t size,OOSQL_TCMemStack& m)
{
    return m.allocate(size);
}

#endif  // _OOSQL_TCL_MEMMGR_H_
