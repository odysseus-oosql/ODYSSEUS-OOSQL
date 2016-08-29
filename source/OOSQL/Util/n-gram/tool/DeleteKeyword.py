#! /usr/bin/env python

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

import string
import sys
import os
import time
import re

class Dictionary:

    def __init__(self, dicno):
        self.error = 0
        self.dicpath = os.environ['ODYSSEUS_ROOT']
        if self.dicpath == '':
            self.error = 1
            print 'FAIL: ODYSSEUS_ROOT environment varialbe is not defined\n'
            return

        self.dicpath = self.dicpath + '/OOSQL/bin/'

        self.dicname = self.dicpath + 'SPECIALNOUN_PART'+str(dicno)+'.TXT'

        self.dic = {}

        self.count = 0
        self.change = 0

        try:
            self.f_in = open(self.dicname, "r")
        except IOError:
            self.error = 2
            print self.dicname + ' open fail\n'
            return

        line = self.f_in.readline()
        while line:
            w = line.strip()
            if w:
                self.dic[w] = self.count
                self.count = self.count + 1
            line = self.f_in.readline()

        self.f_in.close()
        return
    
    def isNotExist(self, word):
        if self.dic.has_key(word):
            return 0
        else:
            return 1

    def isExist(self, word):
        if self.dic.has_key(word):
            return 1
        else:
            return 0

    def done(self):
        if self.change:
            self.dicname_bak = self.dicname + '.' + 'bak'
            self.dicname_org = self.dicname + '.' + 'org'
#            self.dicname_bak = self.dicname + '.' + re.sub(r'\.*','',str(time.time()))
#            self.dicname_org = self.dicname + '.' + re.sub(r'\.*','',str(time.time()))

            try:
                f_out = open(self.dicname_bak, "w+")
            except IOError:
                print 'IOError: ' + '"'+self.dicname_bak+'"'+ + ' open fail\n'
                return

            i = 0
            dic = self.dic.keys()
            dic.sort()

            while i < len(dic):
                #print dic[i]
                f_out.write(dic[i] + '\n')
                i = i + 1

            f_out.flush()
            f_out.close()
            os.rename(self.dicname, self.dicname_org)
            os.rename(self.dicname_bak, self.dicname)
            print 'INFO: Backup filename is ' + self.dicname_org
            print '      "'+self.dicname+'"'+ ' contains ' + str(len(dic)) + ' keyword(s).'
            
    def add(self, word):
        if self.isNotExist(word):
            #print word
            self.dic[word] = self.count
            self.count = self.count + 1
            self.change = 1
            return word
        else:
            return ''

    def delete(self, word):
        if self.isExist(word):
            del self.dic[word]
            self.change = 1

def Usage():
    print 'USAGE > DeleteKeyword.py <keyword>'

def main():
    if len(sys.argv) == 2:
        d1 = Dictionary(1)
        if d1.error:
           return

        d2 = Dictionary(2)
        if d2.error:
           return

        if d1.isExist(sys.argv[1]):
            print 'FAIL: Cannot delete '+'"'+sys.argv[1]+'"'+' from '+'"'+d1.dicname+'"'+'directly'
            print '      Please manually delete '+'"'+sys.argv[1]+'"'+' from '+'"'+d1.dicname+'"'
        else:
            if d2.isExist(sys.argv[1]):
                d2.delete(sys.argv[1])
                print 'OK: '+'"'+sys.argv[1]+'"'+' is deleted from '+'"'+d2.dicname+'"'
                d2.done()
            else:
                print 'FAIL: '+'"'+sys.argv[1]+'"'+' does not exist in '+'"'+d2.dicname+'"'
    else:
        Usage()

if __name__ == '__main__':
    main()
