#/******************************************************************************/
#/*                                                                            */
#/*    Copyright (c) 1990-2016, KAIST                                          */
#/*    All rights reserved.                                                    */
#/*                                                                            */
#/*    Redistribution and use in source and binary forms, with or without      */
#/*    modification, are permitted provided that the following conditions      */
#/*    are met:                                                                */
#/*                                                                            */
#/*    1. Redistributions of source code must retain the above copyright       */
#/*       notice, this list of conditions and the following disclaimer.        */
#/*                                                                            */
#/*    2. Redistributions in binary form must reproduce the above copyright    */
#/*       notice, this list of conditions and the following disclaimer in      */
#/*       the documentation and/or other materials provided with the           */
#/*       distribution.                                                        */
#/*                                                                            */
#/*    3. Neither the name of the copyright holder nor the names of its        */
#/*       contributors may be used to endorse or promote products derived      */
#/*       from this software without specific prior written permission.        */
#/*                                                                            */
#/*    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS     */
#/*    "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT       */
#/*    LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS       */
#/*    FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE          */
#/*    COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,    */
#/*    INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,    */
#/*    BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;        */
#/*    LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER        */
#/*    CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT      */
#/*    LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN       */
#/*    ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE         */
#/*    POSSIBILITY OF SUCH DAMAGE.                                             */
#/*                                                                            */
#/******************************************************************************/
#/******************************************************************************/
#/*                                                                            */
#/*    ODYSSEUS/OOSQL DB-IR-Spatial Tightly-Integrated DBMS                    */
#/*    Version 5.0                                                             */
#/*                                                                            */
#/*    Developed by Professor Kyu-Young Whang et al.                           */
#/*                                                                            */
#/*    Advanced Information Technology Research Center (AITrc)                 */
#/*    Korea Advanced Institute of Science and Technology (KAIST)              */
#/*                                                                            */
#/*    e-mail: odysseus.oosql@gmail.com                                        */
#/*                                                                            */
#/*    Bibliography:                                                           */
#/*    [1] Whang, K., Lee, J., Lee, M., Han, W., Kim, M., and Kim, J., "DB-IR  */
#/*        Integration Using Tight-Coupling in the Odysseus DBMS," World Wide  */
#/*        Web, Vol. 18, No. 3, pp. 491-520, May 2015.                         */
#/*    [2] Whang, K., Lee, M., Lee, J., Kim, M., and Han, W., "Odysseus: a     */
#/*        High-Performance ORDBMS Tightly-Coupled with IR Features," In Proc. */
#/*        IEEE 21st Int'l Conf. on Data Engineering (ICDE), pp. 1104-1105     */
#/*        (demo), Tokyo, Japan, April 5-8, 2005. This paper received the Best */
#/*        Demonstration Award.                                                */
#/*    [3] Whang, K., Park, B., Han, W., and Lee, Y., "An Inverted Index       */
#/*        Storage Structure Using Subindexes and Large Objects for Tight      */
#/*        Coupling of Information Retrieval with Database Management          */
#/*        Systems," U.S. Patent No.6,349,308 (2002) (Appl. No. 09/250,487     */
#/*        (1999)).                                                            */
#/*    [4] Whang, K., Lee, J., Kim, M., Lee, M., Lee, K., Han, W., and Kim,    */
#/*        J., "Tightly-Coupled Spatial Database Features in the               */
#/*        Odysseus/OpenGIS DBMS for High-Performance," GeoInformatica,        */
#/*        Vol. 14, No. 4, pp. 425-446, Oct. 2010.                             */
#/*    [5] Whang, K., Lee, J., Kim, M., Lee, M., and Lee, K., "Odysseus: a     */
#/*        High-Performance ORDBMS Tightly-Coupled with Spatial Database       */
#/*        Features," In Proc. 23rd IEEE Int'l Conf. on Data Engineering       */
#/*        (ICDE), pp. 1493-1494 (demo), Istanbul, Turkey, Apr. 16-20, 2007.   */
#/*                                                                            */
#/******************************************************************************/

#.KEEP_STATE:

C = gcc -w -fsigned-char
CC = gcc -w -x c++ -fsigned-char

#DEBUG = -g
DEBUG = -O2

COSMOS = $(O_KAOSS)
LOM    = $(O_LOM_SERVER)
OOSQL  = $(O_OOSQL_SERVER)

#INCLUDE = -I$(COSMOS)/Header -I$(LOM)/INCLUDE -I$(OOSQL)/include -I$(OOSQL)/Compiler/include -I$(SWIG)/lib/swig1.3 -I$(SWIG)/lib/swig1.3/python -I/usr/include/python2.4
#INCLUDE = -I$(COSMOS)/Header -I$(LOM)/INCLUDE -I$(OOSQL)/include -I$(OOSQL)/Compiler/include -I$(SWIG)/lib/swig1.3 -I$(SWIG)/lib/swig1.3/python -I/usr/include/python2.6
INCLUDE = -I$(COSMOS)/Header -I$(LOM)/INCLUDE -I$(OOSQL)/include -I$(OOSQL)/Compiler/include -I$(SWIG)/lib/swig1.3 -I$(SWIG)/lib/swig1.3/python -I$(PYTHON_HEADER)
LIB     = -L$(O_OOSQL_EXPORT)/lib -loosql -lstdc++
#LIB     = -L$(O_OOSQL_EXPORT)/lib -loosql -lpthread -lrt
CFLAGS  = $(INCLUDE)  $(DEBUG)
CCFLAGS = $(INCLUDE)  $(DEBUG)
ifeq ($(shell getconf LONG_BIT),64)
CFLAGS  += -fPIC	# for 64bit
CCFLAGS += -fPIC	# for 64bit
endif

LINT = alint
LINT.c = $(LINT) $(LINTFLAGS) $(CPPFLAGS)

EXEC = _PyOOSQLc.so
all: $(EXEC)


ifeq ($(shell getconf LONG_BIT),64)
ifneq (,$(findstring -DSUPPORT_LARGE_DATABASE2,$(OS64FLAG))) 
_PyOOSQLc.so: _PyOOSQL64_large.i PyOOSQL.py
	$(SWIG)/bin/swig -c++ -python -shadow $(INCLUDE) _PyOOSQL64_large.i
	mv _PyOOSQL64_large_wrap.c _PyOOSQL_wrap.cxx
	$(CC) $(CCFLAGS) -c _PyOOSQL_wrap.cxx
	gcc -shared _PyOOSQL_wrap.o $(LIB) -o $@
	cp *.py $(O_OOSQL_EXPORT)/bin
	cp $@ $(O_OOSQL_EXPORT)/bin
else
_PyOOSQLc.so: _PyOOSQL64.i PyOOSQL.py
	$(SWIG)/bin/swig -c++ -python -shadow $(INCLUDE) _PyOOSQL64.i
	mv _PyOOSQL64_wrap.c _PyOOSQL_wrap.cxx
	$(CC) $(CCFLAGS) -c _PyOOSQL_wrap.cxx
	gcc -shared _PyOOSQL_wrap.o $(LIB) -o $@ 
	cp *.py $(O_OOSQL_EXPORT)/bin
	cp $@ $(O_OOSQL_EXPORT)/bin
endif
else
_PyOOSQLc.so: _PyOOSQL32.i PyOOSQL.py
	$(SWIG)/bin/swig -c++ -python -shadow $(INCLUDE) _PyOOSQL32.i
	mv _PyOOSQL32_wrap.c _PyOOSQL_wrap.cxx
	$(CC) $(CCFLAGS) -c _PyOOSQL_wrap.cxx
	gcc -shared _PyOOSQL_wrap.o $(LIB) -o $@
	cp *.py $(O_OOSQL_EXPORT)/bin
	cp $@ $(O_OOSQL_EXPORT)/bin
endif

.SUFFIXES: .c .cxx .o
.cxx.o:
	$(CC) $(CCFLAGS) -c $<

.c.o:
	$(C) $(CCFLAGS) -c $<

install:
	\cp *.py _PyOOSQLc.so $(O_OOSQL_EXPORT)/bin

clean:
	rm -f *.o $(EXEC) _PyOOSQL_wrap.cxx _PyOOSQL.py
