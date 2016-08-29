<?php

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

global $NGRAM_HIGH_LOADED__;
if ($NGRAM_HIGH_LOADED__) return;
$NGRAM_HIGH_LOADED__ = true;

/* if our extension has not been loaded, do what we can */
if (!extension_loaded("ngram_high")) {
	if (!dl("ngram_high.so")) return;
}


/**********************************************************************************************************************************
DESCRIPTION:    get_stopwords()

Arguments:     
            1)  $query:      
            

RETURN VALUE:   stopwords list (string, each word is delimited by ' ')

IMPLEMENTATION: 
**********************************************************************************************************************************/
function get_stopwords($query)
{
	if ($query == "*")
	{
		return "";
	}

	if (strlen($query) == 3 && ($query[0] == "*" || $query[2] == "*"))
	{
		return "";
	}

	$result = swig_get_stopwords($query);
		return $result;
}

/**********************************************************************************************************************************
DESCRIPTION:    make_n_gram()
                MATCH(column-identifier, ir-expression [, lable-id] [, scan-direction]) > weight
                 - column-identifier ::= ID
                 - ir-expression ::=
                       keyword
                       | ir-expression ir-binary-operator ir-expression      
                       | ir-expression ir-unary-operator INTEGER
                       | (ir-expression)
                 - keyword ::= ¡®¡°¡¯ ID ¡®¡°¡¯
                 - ir-binary-operator ::= ¡®&¡¯ | ¡®|¡¯ | ¡®-¡®
                 - ir-unary-operator ::= ¡®>¡¯ | ¡®*¡¯ | ¡®:¡¯
                 - lable-id ::= INTEGER
                 - scan-direction ::= FORWARD | BACKWARD
                 - weight ::= FLOAT

Arguments:     
            1)  $query:     
            

RETURN VALUE:   ir-expression (string)

IMPLEMENTATION: 
**********************************************************************************************************************************/
function make_n_gram($query)
{
	if ($query == "*")
	{
		return "'*'";
	}

	if (strlen($query) == 3 && ($query[0] == "*" || $query[2] == "*"))
	{
		return "'$query'";
	}

	$result = swig_make_n_gram($query);
	if ($result == "")
		return "'$query'";
	else
		return $result;
}


/**********************************************************************************************************************************
DESCRIPTION:    web_highlight_ngram()

Arguments:     
            1)  $target_str:   
            2)  $query:
            

RETURN VALUE:   

IMPLEMENTATION: 
**********************************************************************************************************************************/
function web_highlight_ngram($target_str, $query)
{
	if ($query== "*")
		return $target_str;
	else return swig_web_highlight_ngram($target_str, $query);
}


/**********************************************************************************************************************************
DESCRIPTION:    ngram_desc_cut()

Arguments:     
            1)  $desc:      
            2)  $query:  
            3)  $max_len:

RETURN VALUE:  

IMPLEMENTATION: 
**********************************************************************************************************************************/

function ngram_desc_cut($desc, $query, $max_len)
{
	$result = swig_cut_string_by_query($desc, $query, $max_len);

	/* remove '^^^^' from the string ... */
	$result_len = strlen($result);
	$real_start_of_result = 0;

	$last_carrot_point = -1;
	for ($i=0; $i < $result_len; $i++)
	{
		if ($result[$i] == '^')
			$last_carrot_point = $i;
		else
			break;
	}

	if ($last_carrot_point != -1)
	{
		$result = substr($result, $last_carrot_point+1, strlen($result)-($last_carrot_point+1));
	}
	/* ... remove '^^^^' from the string */
	
	return $result;
}

?>
