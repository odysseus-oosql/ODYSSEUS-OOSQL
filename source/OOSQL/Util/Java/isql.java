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

import OOSQL;
import OOSQL_ByteBuffer;


    
public class isql {
    static {
        try {
			System.out.println("Load oosqljni library");
            System.loadLibrary("oosqljni");
        } catch (UnsatisfiedLinkError e) {
            System.err.println("Cannot load the example native code.\nMake sure your LD_LIBRARY_PATH contains \'.\'\n" + e);
            System.exit(1);
        }
    }
    
    
    
    final static int eISQL_INTERNAL_ERROR = Integer.MIN_VALUE;
    final static int MAXPOINTS = 1024;
    
    
    static void PRINT_ERROR_MESSAGE(OOSQL_SystemHandle systemHandle, int handle, int e) 
    {
        if (e < 0) { 
        	if(e != eISQL_INTERNAL_ERROR) 
            { 
        	    byte[]  errorMessage = new byte[4096]; 
        		OOSQL.OOSQL_GetErrorName(systemHandle.getCPtr(), e, errorMessage, errorMessage.length); 
        		System.out.print("OOSQL ERROR(" + new String(errorMessage, 0, strlen(errorMessage)) + ") : "); 
        	    OOSQL.OOSQL_GetErrorMessage(systemHandle.getCPtr(), e, errorMessage, errorMessage.length); 
        	    System.out.println(new String(errorMessage, 0, strlen(errorMessage))); 
        	    OOSQL.OOSQL_GetQueryErrorMessage(systemHandle.getCPtr(), handle, errorMessage, errorMessage.length); 
        	    System.out.println(new String(errorMessage, 0, strlen(errorMessage))); 
        	} 
    	}
    }
       
     
    static int CHECK_QUERY_ERROR(OOSQL_SystemHandle systemHandle, int handle, int e) 
    {
        if (e < 0) { 
        	if(e != eISQL_INTERNAL_ERROR) 
            { 
        	    byte[]  errorMessage = new byte[4096]; 
        		OOSQL.OOSQL_GetErrorName(systemHandle.getCPtr(), e, errorMessage, errorMessage.length); 
        		System.out.print("OOSQL ERROR(" + new String(errorMessage, 0, strlen(errorMessage)) + ") : "); 
        	    OOSQL.OOSQL_GetErrorMessage(systemHandle.getCPtr(), e, errorMessage, errorMessage.length); 
        	    System.out.print(new String(errorMessage, 0, strlen(errorMessage))); 
        	    OOSQL.OOSQL_GetQueryErrorMessage(systemHandle.getCPtr(), handle, errorMessage, errorMessage.length); 
        	    System.out.print(new String(errorMessage, 0, strlen(errorMessage))); 
                return eISQL_INTERNAL_ERROR; 
        	} 
        	else 
        	    return e; 
        }
        return e;
    }
    
    
    static int CHECK_ERROR(OOSQL_SystemHandle systemHandle, int e) 
    {
        if (e < 0) { 
        	if(e != eISQL_INTERNAL_ERROR) 
            { 
        	    byte[]  errorMessage = new byte[4096]; 
        		OOSQL.OOSQL_GetErrorName(systemHandle.getCPtr(), e, errorMessage, errorMessage.length); 
        		System.out.print("OOSQL ERROR(" + new String(errorMessage, 0, strlen(errorMessage)) + ") : "); 
        	    OOSQL.OOSQL_GetErrorMessage(systemHandle.getCPtr(), e, errorMessage, errorMessage.length); 
        	    System.out.print(new String(errorMessage, 0, strlen(errorMessage))); 
                return eISQL_INTERNAL_ERROR; 
        	} 
        	else 
        	    return e; 
        }
        return e;
    }
    
    static void CHECK_ERROR_XCT(OOSQL_SystemHandle systemHandle, int e, XactID xactId) 
    {
        if (e < 0) { 
            byte[]  errorMessage = new byte[4096]; 
        	OOSQL.OOSQL_GetErrorName(systemHandle.getCPtr(), e, errorMessage, errorMessage.length); 
        	System.out.print("OOSQL ERROR(" + new String(errorMessage, 0, strlen(errorMessage)) + ") : "); 
            OOSQL.OOSQL_GetErrorMessage(systemHandle.getCPtr(), e, errorMessage, errorMessage.length); 
            System.out.print(new String(errorMessage, 0, strlen(errorMessage))); 
            if((xactId) != null) OOSQL.OOSQL_TransAbort(systemHandle.getCPtr(), xactId.getCPtr()); 
            if((systemHandle) != null) OOSQL.OOSQL_DestroySystemHandle(systemHandle.getCPtr(), procIndex[0]); 
            if (true) System.exit(1); 
        }
    }
    
    
    final static int SORT_BUFFER_SIZE = 1024 * 1024 * 2;

    static int                         cclevelIndex;
    static int[]                       indexToCclevel = {OOSQL.X_BROWSE_BROWSE, OOSQL.X_CS_BROWSE, OOSQL.X_CS_CS, OOSQL.X_RR_BROWSE, OOSQL.X_RR_CS, OOSQL.X_RR_RR};
    static int[]                       procIndex = new int[1];




    static int execSQL(
    	OOSQL_SystemHandle      systemHandle,
        int                     volId,
        String                  queryStr,
        OOSQL_SortBufferInfo    sortBufferInfo
    )
    {
        int[]                   handle = new int[1];
        int                     e;
        int[]                   nCols = new int[1];
        int[]                   type = new int[1];
        OOSQL_ByteBuffer        stringAttr = new OOSQL_ByteBuffer(16 * 1024);
        int                     i, j;
        int[]                   retLength = new int[1];
    	OID						oid;
        OOSQL_JavaDate          oosqlDate;
    	OOSQL_JavaTime          oosqlTime; 
        OOSQL_JavaTimestamp     oosqlTimestamp;
    	int[]                   nElements = new int[1];
    	OOSQL_ComplexTypeInfo	complexTypeInfo = new OOSQL_ComplexTypeInfo();

        
        /* GetData Test */    
        e = OOSQL.OOSQL_AllocHandle(systemHandle.getCPtr(), volId, handle);
        CHECK_ERROR(systemHandle, e);

    	e = OOSQL.OOSQL_ResetTimeElapsed(systemHandle.getCPtr());
    	CHECK_ERROR(systemHandle, e);
    
    	//e = OOSQL.OOSQL_ResetPageAccessed(systemHandle.getCPtr());
    	//CHECK_ERROR(systemHandle, e);

    	while(true)
    	{
            e = OOSQL.OOSQL_Prepare(systemHandle.getCPtr(), handle[0], queryStr, sortBufferInfo.getCPtr());
    		if(e == OOSQL.eNOERROR)
    			break;
    		else if(e < OOSQL.eNOERROR)
    		{
    			PRINT_ERROR_MESSAGE(systemHandle, handle[0], e);
    			e = OOSQL.OOSQL_FreeHandle(systemHandle.getCPtr(), handle[0]); 
    			CHECK_ERROR(systemHandle, e);
    			return eISQL_INTERNAL_ERROR;
    		}    	    
        }
        
        e = OOSQL.OOSQL_GetNumResultCols(systemHandle.getCPtr(), handle[0], nCols);
        CHECK_QUERY_ERROR(systemHandle, handle[0], e);        

        e = OOSQL.OOSQL_Execute(systemHandle.getCPtr(), handle[0]);
        if(e < OOSQL.eNOERROR)
        {
    		PRINT_ERROR_MESSAGE(systemHandle, handle[0], e);
            e = OOSQL.OOSQL_FreeHandle(systemHandle.getCPtr(), handle[0]); 
            CHECK_ERROR(systemHandle, e);
    		return eISQL_INTERNAL_ERROR;
        }

    	if(nCols[0] == 0)
    	{
    		//OOSQL_ReportTimeAndPageAccess(systemHandle);
    
    		e = OOSQL.OOSQL_FreeHandle(systemHandle.getCPtr(), handle[0]); 
    		CHECK_ERROR(systemHandle, e);
    		return OOSQL.eNOERROR;
    	}

    	for(i = 0; i < nCols[0]; i++)
    		System.out.print("----------+");
    	System.out.println("");
    	for(i = 0; i < nCols[0]; i++)
    	{
    		e = OOSQL.OOSQL_GetResultColName(systemHandle.getCPtr(), handle[0], (short)i, stringAttr.getBuffer(), stringAttr.getSize());
    		CHECK_QUERY_ERROR(systemHandle, handle[0], e);
    
    		System.out.print(stringAttr + " | ");
    	}
    	System.out.println("");
    	for(i = 0; i < nCols[0]; i++)
    		System.out.print("----------+");
    	System.out.println("");
                        
        
        while((e = OOSQL.OOSQL_Next(systemHandle.getCPtr(), handle[0])) != OOSQL.ENDOFEVAL)
        {
            CHECK_QUERY_ERROR(systemHandle, handle[0], e);
            
            for(i = 0; i < nCols[0]; i++)
            {
                e = OOSQL.OOSQL_GetResultColType(systemHandle.getCPtr(), handle[0], (short)i, type); 
                switch(type[0])
                {
                case OOSQL.OOSQL_TYPE_CHAR:
                case OOSQL.OOSQL_TYPE_VARCHAR:
                    e = OOSQL.OOSQL_GetData(systemHandle.getCPtr(), handle[0], (short)i, 0, 
                                            stringAttr.getBuffer(), stringAttr.getSize(), retLength);
                    CHECK_QUERY_ERROR(systemHandle, handle[0], e);
    				if(retLength[0] == -1)
    				    System.out.print("NULL|");
    				else
    				{
    				    stringAttr.setByte(retLength[0], (byte)0);
    				    System.out.print(stringAttr.toString() + "|");
                    }
                    break;
                    
                case OOSQL.OOSQL_TYPE_SMALLINT:
                    e = OOSQL.OOSQL_GetData(systemHandle.getCPtr(), handle[0], (short)i, 0, 
                                            stringAttr.getBuffer(), stringAttr.getShortSize(), retLength);
                    CHECK_QUERY_ERROR(systemHandle, handle[0], e);
    				if(retLength[0] == -1)
    				    System.out.print("NULL|");
    				else
    				{
    				    System.out.print(stringAttr.toShort() + "|");
                    }
                    break;
                    
                case OOSQL.OOSQL_TYPE_INTEGER:
                case OOSQL.OOSQL_TYPE_LONG:               
                    e = OOSQL.OOSQL_GetData(systemHandle.getCPtr(), handle[0], (short)i, 0, 
                                            stringAttr.getBuffer(), stringAttr.getIntSize(), retLength);
                    CHECK_QUERY_ERROR(systemHandle, handle[0], e);
    				if(retLength[0] == -1)
    				    System.out.print("NULL|");
    				else
    				{
    				    System.out.print(stringAttr.toInt() + "|");
                    }                
                    break;
                    
    			case OOSQL.OOSQL_TYPE_TIME:
                    e = OOSQL.OOSQL_GetData(systemHandle.getCPtr(), handle[0], (short)i, 0, 
                                            stringAttr.getBuffer(), stringAttr.getTimeSize(), retLength);
                    CHECK_QUERY_ERROR(systemHandle, handle[0], e);
    				if(retLength[0] == -1)
    				    System.out.print("NULL|");
    				else
    				{
    				    oosqlTime = stringAttr.toTime();
    				    System.out.print(OOSQL.OOSQL_GetHour(systemHandle.getCPtr(), oosqlTime.getCPtr()) + ":" +
    				                    OOSQL.OOSQL_GetMinute(systemHandle.getCPtr(), oosqlTime.getCPtr()) + ":" +
    				                    OOSQL.OOSQL_GetSecond(systemHandle.getCPtr(), oosqlTime.getCPtr()) + "|");
                    }                
                    break;
                    
                case OOSQL.OOSQL_TYPE_DATE:
                    e = OOSQL.OOSQL_GetData(systemHandle.getCPtr(), handle[0], (short)i, 0, 
                                            stringAttr.getBuffer(), stringAttr.getDateSize(), retLength);
                    CHECK_QUERY_ERROR(systemHandle, handle[0], e);
    				if(retLength[0] == -1)
    				    System.out.print("NULL|");
    				else
    				{
    				    oosqlDate = stringAttr.toDate();
    				    System.out.print(OOSQL.OOSQL_GetYear(systemHandle.getCPtr(), oosqlDate.getCPtr()) + "-" +
    				                    OOSQL.OOSQL_GetMonth(systemHandle.getCPtr(), oosqlDate.getCPtr()) + "-" +
    				                    OOSQL.OOSQL_GetDay(systemHandle.getCPtr(), oosqlDate.getCPtr()) + "|");
                    }                
                    break;
                    
    			case OOSQL.OOSQL_TYPE_TIMESTAMP:
                    e = OOSQL.OOSQL_GetData(systemHandle.getCPtr(), handle[0], (short)i, 0, 
                                            stringAttr.getBuffer(), stringAttr.getTimestampSize(), retLength);
                    CHECK_QUERY_ERROR(systemHandle, handle[0], e);
    				if(retLength[0] == -1)
    				    System.out.print("NULL|");
    				else
    				{
    				    oosqlTimestamp = stringAttr.toTimestamp();
    				    System.out.print(OOSQL.OOSQL_GetYear(systemHandle.getCPtr(), oosqlTimestamp.getD().getCPtr()) + "-" +
    				                    OOSQL.OOSQL_GetMonth(systemHandle.getCPtr(), oosqlTimestamp.getD().getCPtr()) + "-" +
    				                    OOSQL.OOSQL_GetDay(systemHandle.getCPtr(), oosqlTimestamp.getD().getCPtr()) + "-" +
    				                    OOSQL.OOSQL_GetHour(systemHandle.getCPtr(), oosqlTimestamp.getT().getCPtr()) + ":" +
    				                    OOSQL.OOSQL_GetMinute(systemHandle.getCPtr(), oosqlTimestamp.getT().getCPtr()) + ":" +
    				                    OOSQL.OOSQL_GetSecond(systemHandle.getCPtr(), oosqlTimestamp.getT().getCPtr()) + "|");
    				                    
                    }                
                    break;
                    
                case OOSQL.OOSQL_TYPE_FLOAT:
                    e = OOSQL.OOSQL_GetData(systemHandle.getCPtr(), handle[0], (short)i, 0, 
                                            stringAttr.getBuffer(), stringAttr.getFloatSize(), retLength);
                    CHECK_QUERY_ERROR(systemHandle, handle[0], e);
    				if(retLength[0] == -1)
    				    System.out.print("NULL|");
    				else
    				{
    				    System.out.print(stringAttr.toFloat() + "|");
                    }
                    break;
                    
                case OOSQL.OOSQL_TYPE_DOUBLE:
                    e = OOSQL.OOSQL_GetData(systemHandle.getCPtr(), handle[0], (short)i, 0, 
                                            stringAttr.getBuffer(), stringAttr.getDoubleSize(), retLength);
                    CHECK_QUERY_ERROR(systemHandle, handle[0], e);
    				if(retLength[0] == -1)
    				    System.out.print("NULL|");
    				else
    				{
    				    System.out.print(stringAttr.toDouble() + "|");
                    }
                    break;
                    
                case OOSQL.OOSQL_TYPE_TEXT:
                    e = OOSQL.OOSQL_GetData(systemHandle.getCPtr(), handle[0], (short)i, 0, 
                                            stringAttr.getBuffer(), stringAttr.getSize(), retLength);
                    CHECK_QUERY_ERROR(systemHandle, handle[0], e);
    				if(retLength[0] == -1)
    				    System.out.print("NULL|");
    				else
    				{
    				    if (retLength[0] == stringAttr.getSize())
    				        stringAttr.setByte(retLength[0]-1, (byte)0);
    				    else
    				        stringAttr.setByte(retLength[0], (byte)0);
    				    System.out.print(stringAttr.toString() + "|");
                    }                
                    break;
                    
    			case OOSQL.OOSQL_TYPE_OID:
                    e = OOSQL.OOSQL_GetData(systemHandle.getCPtr(), handle[0], (short)i, 0, 
                                            stringAttr.getBuffer(), stringAttr.getOIDSize(), retLength);
                    CHECK_QUERY_ERROR(systemHandle, handle[0], e);
    				if(retLength[0] == -1)
    				    System.out.print("NULL|");
    				else
    				{
    				    oid = stringAttr.toOID();
    				    e = OOSQL.OOSQL_OIDToOIDString(systemHandle.getCPtr(), oid.getCPtr(), stringAttr.getBuffer());
    				    CHECK_QUERY_ERROR(systemHandle, handle[0], e);
    				    
    				    System.out.print(stringAttr.toString() + "|");
                    }
    				break;
    				
    			default:
    				if(OOSQL.OOSQL_MASK_COMPLEXTYPE(type[0]) == (long)OOSQL.OOSQL_COMPLEXTYPE_SET ||
    				   OOSQL.OOSQL_MASK_COMPLEXTYPE(type[0]) == (long)OOSQL.OOSQL_COMPLEXTYPE_BAG ||
    				   OOSQL.OOSQL_MASK_COMPLEXTYPE(type[0]) == (long)OOSQL.OOSQL_COMPLEXTYPE_LIST)
    				{
    					e = OOSQL.OOSQL_GetComplexTypeInfo(systemHandle.getCPtr(), handle[0], (short)i, complexTypeInfo.getCPtr());
    					CHECK_QUERY_ERROR(systemHandle, handle[0], e);
    
    					e = OOSQL.OOSQL_ComplexType_GetNumElements(complexTypeInfo.getCPtr(), nElements);
    					CHECK_QUERY_ERROR(systemHandle, handle[0], e);
    
    					e = OOSQL.OOSQL_ComplexType_GetElementsString(complexTypeInfo.getCPtr(), 0, nElements[0], stringAttr.getBuffer(), stringAttr.getSize());
    					CHECK_QUERY_ERROR(systemHandle, handle[0], e);
    
    					System.out.print(stringAttr.toString() + "|");
    				}
    				else
    					System.out.print("..." + "|");
    				break;
    			}
    			
            }
                                                                                            
            System.out.println("");
            
        }
        
    	for(i = 0; i < nCols[0]; i++)
    		System.out.print("----------+");
    	System.out.println("");

        //OOSQL_ReportTimeAndPageAccess(systemHandle);        
    
        e = OOSQL.OOSQL_FreeHandle(systemHandle.getCPtr(), handle[0]);
        CHECK_ERROR(systemHandle, e);
               
        
        return OOSQL.eNOERROR;
    }



    static boolean isspace(byte chr)
    {
        if (chr == 32 || chr == 12 || chr == 10 || chr == 13 || chr == 9 || chr == 11)
            return true;
        else 
            return false;
    }
    
    static int strlen(byte[] str)
    {
        int     i;
        
        for (i = 0; i < str.length; i++)    
            if (str[i] == 0)    break;
            
        return i;
    }    
    
    static void memset(byte[] str, int start, int end)
    {
        for (int i = start; i < end; i++)   str[i] = 0;
    }
    
    static void StringStrip(byte[] str)
    {
    	int right, left;
    	int i, j;
    	int length;

            
    	/* eliminate left and right white space */	
    	length = strlen(str);
    	for(left = 0; left < length; left++)
    	{
    		if(str[left] >= 0x80 || !isspace(str[left]))
    			break;
    	}
    
    	for(right = length - 1; right >= 0; right--)
    	{
    		if(str[right] >= 0x80 || !isspace(str[right]))
    			break;
    	}
    	
    	for(j = 0, i = left; i <= right; i++, j++)
    	{
    		str[j] = str[i];
    	}

    	str[j] = 0;
    }


    static int execISQL( 
    	OOSQL_SystemHandle      systemHandle,		
        int                     volId,
        OOSQL_SortBufferInfo    sortBufferInfo,
        XactID                  xactID
    )
    {
        byte[]          queryStr = new byte[16 * 1024];
        byte[]          catStr = new byte[1024 * 4];
        int             queryStrLen;
        int             e;
    	int				lineno = 1;

        
    
        while (true) 
        { 
            System.out.print("OOSQL> ");
    
            /* get input query string */
            memset(queryStr, 0, queryStr.length);
            while(true) 
            {
                try {
                    memset(catStr, 0, catStr.length);
                    System.in.read(catStr);
                } catch(java.io.IOException ex) {
                    continue;
                }
    			StringStrip(catStr);
    			
    			System.arraycopy(catStr, 0, queryStr, strlen(queryStr), strlen(catStr));
    	            		
                if(strlen(queryStr) > 0 && queryStr[strlen(queryStr) - 1] == (byte)';')
                {   
                    queryStr[strlen(queryStr) - 1] = (byte)'\0';
                    break;
                }                    
                else if((new String(queryStr, 0, strlen(queryStr))).compareTo("quit") == 0) 
                    return OOSQL.eNOERROR;
    			else if((new String(queryStr, 0, strlen(queryStr))).compareTo("commit") == 0)
    			{
    				System.out.println("OOSQL_TransCommit");
    				e = OOSQL.OOSQL_TransCommit(systemHandle.getCPtr(), xactID.getCPtr());
    				CHECK_ERROR_XCT(systemHandle, e, xactID);
    
    				System.out.println("OOSQL_TransBegin");
    				e = OOSQL.OOSQL_TransBegin(systemHandle.getCPtr(), xactID.getCPtr(), indexToCclevel[cclevelIndex]);
    				CHECK_ERROR_XCT(systemHandle, e, xactID);
    
    				memset(queryStr, 0, queryStr.length);
    				System.out.print("OOSQL> ");
    			}
    			else if((new String(queryStr, 0, strlen(queryStr))).compareTo("abort") == 0) 
    			{
    				System.out.println("OOSQL_TransAbort");
    				e = OOSQL.OOSQL_TransAbort(systemHandle.getCPtr(), xactID.getCPtr());
    				CHECK_ERROR_XCT(systemHandle, e, xactID);
    
    				System.out.println("OOSQL_TransBegin");
    				e = OOSQL.OOSQL_TransBegin(systemHandle.getCPtr(), xactID.getCPtr(), indexToCclevel[cclevelIndex]);
    				CHECK_ERROR_XCT(systemHandle, e, xactID);
    
    				memset(queryStr, 0, queryStr.length);
    				System.out.print("OOSQL> ");
    			}
    			else
    			{
    			    queryStr[strlen(queryStr)] = (byte)'\n';
    				lineno ++;
    				System.out.print("       ");
    			}
    			
            }
            
            // DEBUG
            //System.out.println("|" + new String(queryStr, 0, strlen(queryStr)) + "|");            
            e = execSQL(systemHandle, volId, new String(queryStr, 0, strlen(queryStr)), sortBufferInfo);
            if(e < 0) System.out.println("SQL Error");
    		lineno = 1;
    		
        }
    
        //return OOSQL.eNOERROR;
    }

    static int usage()
    {
    	System.out.println("USAGE : isql [-cclevel <cuncurrency level>] <database name> [<volume name>] [-temporary <database name> [<volume name>]]");
    	System.out.println("");
    	System.out.println("        currency level : 0, 1, 2, 3, 4, 5");
    	System.out.println("                         0 means X_BROWSE_BROWSE,");
    	System.out.println("                         1 means X_CS_BROWSE,");
    	System.out.println("                         2 means X_CS_CS,");
    	System.out.println("                         3 means X_RR_BROWSE,");
    	System.out.println("                         4 means X_RR_CS,");
    	System.out.println("                         5 means X_RR_RR. Default value is 5 (X_RR_RR)");
    	System.out.println("");
    	System.out.println("        temporary : indicating that isql uses separate volume to process");
    	System.out.println("                    orderby, group by, distinct");
        System.exit(1);
        
    	return 0;
    }


    public static void main(String argv[]) {
        int[]                       volID = new int[1];
        int[]                       temporaryVolID = new int[1];
        int[]                       databaseID = new int[1];
        int                         e;
        OOSQL_SystemHandle          systemHandle = new OOSQL_SystemHandle();
        OOSQL_SortBufferInfo        sortBufferInfo = new OOSQL_SortBufferInfo();
        OOSQL_MemorySortBufferInfo  sortMemoryInfo = new OOSQL_MemorySortBufferInfo();
        OOSQL_DiskSortBufferInfo    sortDiskInfo = new OOSQL_DiskSortBufferInfo();
        String                      databaseName;
        String                      volumeName;
        String                      temporaryDatabaseName;
        String                      temporaryVolumeName;
        int                         count;
    
        XactID                      xactID = new XactID();
    
    
    
        System.out.println(OOSQL.OOSQL_GetVersionString());
        
    	/* parse argument */
    	count = 0;
    	if (count >= argv.length) usage();
    	if(argv[count].compareTo("-cclevel") == 0)
    	{
    		count++;	/* skip "-cclevel" */
    		cclevelIndex = (Integer.valueOf(argv[count])).intValue();
    		count++;
    	}
    	else
    		cclevelIndex = 5;

    
    	if(count >= argv.length) usage();
    	databaseName = argv[count++];
    
    	/* next argument is -temporary or there are one more argument to parse.  */
        /* these argument consist of volumeName, datafilename */
    	if ((argv.length > (count + 1) && argv[count + 1].compareTo("-temporary") == 0)  || (argv.length - 1) == count)     
    		volumeName = argv[count++];
    	else           
    		volumeName = databaseName;
    
    	if(count < argv.length && argv[count].compareTo("-temporary") == 0)		/* temporary db is given */
    	{
    		count++;	/* skip "-temporary" */
    		temporaryDatabaseName = argv[count++];
    		if(argv.length > count)	/* there are one more arguments, that means temporary volume name is given */
    			temporaryVolumeName = argv[count++];
    		else
    			temporaryVolumeName = temporaryDatabaseName;
    	}
    	else
    	{
    		temporaryDatabaseName = "";
    		temporaryVolumeName   = "";
    	}
        
        System.out.println("OOSQL_Init");
        e = OOSQL.OOSQL_CreateSystemHandle(systemHandle.getCPtr(), procIndex);
        CHECK_ERROR_XCT(null, e, null);
        System.out.println("");


        e = OOSQL.OOSQL_MountDB(systemHandle.getCPtr(), databaseName, databaseID);
        CHECK_ERROR_XCT(systemHandle, e, null);
        System.out.println("Database " + databaseName + " is mounted");
        
        e = OOSQL.OOSQL_GetVolumeID(systemHandle.getCPtr(), databaseID[0], volumeName, volID);
        CHECK_ERROR_XCT(systemHandle, e, null);

    	if(temporaryDatabaseName.length() != 0)
    	{
    		e = OOSQL.OOSQL_MountVolumeByVolumeName(systemHandle.getCPtr(), temporaryDatabaseName, temporaryVolumeName, temporaryVolID);
    		CHECK_ERROR_XCT(systemHandle, e, null);
    	}
    	else
    		temporaryVolID[0] = volID[0];


        // alloc sort buffer
        sortMemoryInfo.setBufferSize(SORT_BUFFER_SIZE);
        sortMemoryInfo.setSortBufferLength(SORT_BUFFER_SIZE);
        sortMemoryInfo.setSortBufferUsedLength(0);
        
        sortDiskInfo.setSortVolID(volID[0]);
        
        sortBufferInfo.setMode(OOSQL.OOSQL_SB_USE_MEMORY);
        sortBufferInfo.setDiskInfo(sortDiskInfo);
        sortBufferInfo.setMemoryInfo(sortMemoryInfo);

        System.out.println("OOSQL_TransBegin");
        e = OOSQL.OOSQL_TransBegin(systemHandle.getCPtr(), xactID.getCPtr(), indexToCclevel[cclevelIndex]);
        CHECK_ERROR_XCT(systemHandle, e, xactID);


    	e = execISQL(systemHandle, volID[0], sortBufferInfo, xactID);
    	CHECK_ERROR_XCT(systemHandle, e, xactID);

        System.out.println("OOSQL_TransCommit");
        e = OOSQL.OOSQL_TransCommit(systemHandle.getCPtr(), xactID.getCPtr());
        CHECK_ERROR_XCT(systemHandle, e, xactID);

        System.out.println("OOSQL_Dismount");            
        e = OOSQL.OOSQL_DismountDB(systemHandle.getCPtr(), databaseID[0]);
        CHECK_ERROR_XCT(systemHandle, e, null);


    	if(temporaryVolID[0] != volID[0])
    	{
    		e = OOSQL.OOSQL_Dismount(systemHandle.getCPtr(), temporaryVolID[0]);
    		CHECK_ERROR_XCT(systemHandle, e, null);
        }
    
        System.out.println("OOSQL_Final");
        e = OOSQL.OOSQL_DestroySystemHandle(systemHandle.getCPtr(), procIndex[0]);
        CHECK_ERROR_XCT(systemHandle, e, null);
      }
}
