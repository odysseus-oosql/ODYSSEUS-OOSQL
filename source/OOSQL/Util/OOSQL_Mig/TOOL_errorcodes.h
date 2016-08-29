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
 * Macro Definitions
 */
#define TOOL_ERR_ENCODE_ERROR_CODE(base,no)        ( -1 * (((base) << 16) + no) )
#define TOOL_ERR_GET_BASE_FROM_ERROR_CODE(code)    ( (((code) * -1) >> 16) & 0x0000FFFF )
#define TOOL_ERR_GET_NO_FROM_ERROR_CODE(code)      ( ((code) * -1) & 0x0000FFFF )


/*
 * Error Base Definitions
 */
#define UNIX_ERR_BASE                  0
#define OOSQL_MIG_ERR_BASE             1

#define TOOL_NUM_OF_ERROR_BASES        2


/*
 * Error Definitions for OOSQL_MIG_ERR_BASE
 */
#define eOOSQL_ERROR_MIG               TOOL_ERR_ENCODE_ERROR_CODE(OOSQL_MIG_ERR_BASE,0)
#define eBADPARAMETER_MIG              TOOL_ERR_ENCODE_ERROR_CODE(OOSQL_MIG_ERR_BASE,1)
#define eCANNOT_OPEN_FILE_MIG          TOOL_ERR_ENCODE_ERROR_CODE(OOSQL_MIG_ERR_BASE,2)
#define eCANNOT_CLOSE_FILE_MIG         TOOL_ERR_ENCODE_ERROR_CODE(OOSQL_MIG_ERR_BASE,3)
#define eUNDEFINED_DATABASE_NAME_MIG   TOOL_ERR_ENCODE_ERROR_CODE(OOSQL_MIG_ERR_BASE,4)
#define eUNHANDLED_CASE_MIG            TOOL_ERR_ENCODE_ERROR_CODE(OOSQL_MIG_ERR_BASE,5)
#define eLRDS_ERROR_MIG                TOOL_ERR_ENCODE_ERROR_CODE(OOSQL_MIG_ERR_BASE,6)
#define eLOM_ERROR_MIG                 TOOL_ERR_ENCODE_ERROR_CODE(OOSQL_MIG_ERR_BASE,7)
#define eCATALOG_ERROR_MIG             TOOL_ERR_ENCODE_ERROR_CODE(OOSQL_MIG_ERR_BASE,8)
#define NUM_ERRORS_OOSQL_MIG_ERR_BASE  9
