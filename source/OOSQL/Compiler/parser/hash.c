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

/*******************************************************************
 * function hash()
 *		It returns the hash function value of string yytext
 *******************************************************************/

oql_hash(int length)
{
    int i;
    unsigned int temp;	/* declared as unsigned type to calculate
    			 * the hash result as positive one for Korean code as well
    			 */
    
    i = 0;
    temp = 0;

    while (i < length) {
    	temp = (temp + yytext[i++]) % MAXHASHBUCKET;
    }

    return(temp);
}

/********************************************************************
 * Function Enter()
 *		  It search string. If not exist, insert string to 
 *		symbol table, and return the index. Else, there 
 *		exist, it returns the index
 ********************************************************************/

Enter(length)
 int length;
{
    int adrs;
    EntryPtr e;
    boolean found;
    int s;
    int i;
    int *temp;

    found = FALSE;
    adrs = oql_hash(length);
    e = HashTable[adrs];
    
    while( e != NULL && !found ) {
	if( StrIndex[e->sindex+1] - StrIndex[e->sindex] != length )
	    e = e->link;
	else {
	    s = StrIndex[e->sindex];
	    i = StringTop;
	    found = TRUE;
	    
	    while( i < StringTop+length && found ) {
		if( StrPool[s] == StrPool[i] ) {
		    s++;
		    i++;
		}
		else {
		    found = FALSE;
		    e = e->link;
		}
	    } /* end of while */
	} /* fi */
    } /* end of while */

    if( !found ) {
	e = (EntryPtr)malloc(sizeof(EntryType));
	e->link = HashTable[adrs];
	HashTable[adrs] = e;
	e->sindex = StrIdxTop;
	
	if( StrIdxTop >= MAXIDX ) {
	    temp = (int *)malloc(sizeof(int) * MAXIDX*2);
	    for( i = 0; i < MAXIDX; i++ )
		*(temp+i) = *(StrIndex+i);
	    free(StrIndex);
	    StrIndex = temp;
	}
	StrIdxTop++;
	StringTop = StringTop + length;
	StrIndex[StrIdxTop] = StringTop;
	StrEntryCnt++;
    } /* fi */

    return(e->sindex);
}

/****************************************************************
 * Function InitHash()
 *		  initializes symbol tables
 ****************************************************************/
InitHash()
{
    int i;

    MAXIDX = 200;			/* initialize table size */
    MAXSTR = 1000;
    maxInt = 200;
    MAXREAL = 200;

    StrIndex = (int *) malloc(sizeof(int) * MAXIDX);	/* initialize index table */
    StrIdxTop = 0;

    StrPool = (char *) malloc(sizeof(char) * MAXSTR);	/* initialize ID table */
    StringTop = 0;
    StrEntryCnt = 0;

    IntPool = (long *) malloc(sizeof(long) * maxInt);	/* initialize integer table */
    IntTop = 0;
    IntEntryCnt = 0;

    RealPool = (float *) malloc(sizeof(float) * MAXREAL);/* initialize real table */
    RealTop = 0;
    RealEntryCnt = 0;

    for( i = 0; i < MAXHASHBUCKET; i++ ) /* initialize hash table */
	HashTable[i] = NULL;

    StrIndex[0] = 0;
}

FinalHash()
{
	EntryPtr	e, p;
	int			i;

	for( i = 0; i < MAXHASHBUCKET; i++ )
	{
		e = HashTable[i];

		while(e != NULL)
		{
			p = e;
			e = e->link;
			free(p);
		}
	}

	free(StrIndex);
	free(StrPool);
	free(IntPool);
	free(RealPool);
}
