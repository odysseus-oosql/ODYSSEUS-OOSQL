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

#include <stdio.h>
#include <ctype.h>
#include <string.h>
#include <stdlib.h>


#include "OOSQL_APIs.h"
#include "QuickFitMM.hxx"

#include "NgramKeywordExtractor.h"


#define CHECK_ERROR(handle, e) if (e < eNOERROR) { printf("Error occurs from keyword extractor (internal error code=%d)\n", e); exit(-1); }
#define QUICKFITMM_ERROR(e)	   if (e < eNOERROR) { printf("Error in Quick Fit Memory Manager\n"); exit(-1); }

#define EOS			'\0'
#define IsVowel(c) ('a'==(c) || 'e'==(c) || 'i'==(c) || 'o'==(c) || 'u'==(c))


#define SGKIM_DEBUG

#define IJKIM_DEBUG 0
#define IJKIM_DEBUG2 0



/* Global Variables */
int startIRFlag = 1;

Dictionary Dic;

QuickFitMM_Handle staticMMHandle;
QuickFitMM_Handle dynamicMMHandle;

KorKeywordExtractorHandle* korKeywordExtractorHandle = NULL;

Four nHandle = 0;
Four maxHandleNum;


/*	for English Stemize	*/
static char LAMBDA[1] = "";
static char *end;





/* Table for converting Hanjo to Hangle */

char HanjaToHangleTable[52][94*2+1] = {
	"가가가가가가가가가가가가가가가가가가가가가가가가가가가가가각각각각각각각각각각각간간간간간간간간간간간간간간간간간간간간간간간간갈갈갈갈갈갈갈갈갈갈감감감감감감감감감감감감감감감감감감감감",
	"갑갑갑갑갑갑강강강강강강강강강강강강강강강강강강강강강강강강개개개개개개개개개개개개개개개개개개개개객객갱갱갱갱갹거거거거거거거거거거거거거거거거거건건건건건건건건건건건건걸걸걸걸검검검검",
	"검검검겁겁겁게게게격격격격격격격견견견견견견견견견견견결결결결결결겸겸겸겸겸겸경경경경경경경경경경경경경경경경경경경경경경경경경경경경경경경경경경경경경경경경경경경경경계계계계계계계계계계",
	"계계계계계계계계계계계계계계고고고고고고고고고고고고고고고고고고고고고고고고고고고고고고고고고고고고고고고곡곡곡곡곡곡곡곤곤곤곤곤곤곤곤곤곤골골골공공공공공공공공공공공공공공공공곶과과과과",
	"과과과과과과과과곽곽곽곽관관관관관관관관관관관관관관관관관괄괄괄괄광광광광광광광광광광광광광괘괘괘괴괴괴괴괴괴괴괴괴굉굉굉굉교교교교교교교교교교교교교교교교교교교교교교교교교구구구구구구구",
	"구구구구구구구구구구구구구구구구구구구구구구구구구구구구구구구구구구구구구구구구구구구구구구구국국국국국국군군군군군군굴굴굴굴궁궁궁궁궁궁권권권권권권권권권권궐궐궐궐궐궤궤궤궤궤궤귀귀귀귀",
	"귀귀규규규규규규규규규규규규규규규균균균균균균균귤극극극극극극극근근근근근근근근근근근근근근근글금금금금금금금금금금금금금금급급급급급급급긍긍긍긍기기기기기기기기기기기기기기기기기기기기기",
	"기기기기기기기기기기기기기기기기기기기기기기기기기기기기기기기기기기기기기기기기기기기긴길길길길김끽나나나나나나나나나나나나나나나낙낙낙낙낙낙낙낙난난난난난난난난난날날남남남남남남남남남납",
	"납납납납낭낭낭낭낭낭낭내내내내내내냉녀년년년념념념념녕녕노노노노노노노노노노노노노노노노노노녹녹녹녹녹녹논농농농농농농농뇌뇌뇌뇌뇌뇌뇨누누누누누누누누눈눌뉴뉴늑늑늠능능능능능능니니닉닉다다",
	"단단단단단단단단단단단단단단단단단단단단달달달달달담담담담담담담담담담담담담담담담담답답답답답당당당당당당당당당당당대대대대대대대대대대대대대대대대댁덕덕도도도도도도도도도도도도도도도도도",
	"도도도도도도도도도도도도도도도도도도도도도도도독독독독독독독독독독돈돈돈돈돈돈돈돈돈돈돌돌동동동동동동동동동동동동동동동동동두두두두두두두두두두두둔둔둔둔둔둔득등등등등등등등등등라라라라라",
	"라라라락락락락락락락락락란란란란란란란란란랄랄람람람람람람람람람람랍랍랍랑랑랑랑랑랑랑랑래래래래랭랭략량량량량량량량량량량량량량려려려려려려려려려려려려려려려려려려력력력력력력력련련련련련",
	"련련련련련련련렬렬렬렬렬렬렴렴렴렴렴렵령령령령령령령령령령령령령령령령령령례례례례례로로로로로로로로로로로로로로로로로로록록록록록록록론롱롱롱롱롱롱롱뢰뢰뢰뢰뢰뢰뢰뢰료료료료료료료료료료료",
	"료료룡루루루루루루루루루루루루루류류류류류류류류류류류류류류륙륙륙륜륜륜륜륜륜률률률률륭륵륵름릉릉릉릉릉릉리리리리리리리리리리리리리리리리리리리리리리리리리리린린린린린린린린린림림림림림립",
	"립립립마마마마마마마마막막막막막막만만만만만만만만만만만만만만만만만만만말말말말말말말망망망망망망망망망망망망매매매매매매매매매매매매매매맥맥맥맥맥맹맹맹맹맹맹멱멱면면면면면면면면면면면멸",
	"멸명명명명명명명명명명명명명명명몌모모모모모모모모모모모모모모모모모모모모모모모모목목목목목목목몰몰몽몽몽묘묘묘묘묘묘묘묘묘묘묘묘무무무무무무무무무무무무무무무무무무무무무무묵묵문문문문문",
	"문문문문문문문물물물미미미미미미미미미미미미미미미미미미미민민민민민민민민민민민민민밀밀밀박박박박박박박박박박박박박박박박박박박반반반반반반반반반반반반반반반반반반반반반반반반반발발발발발",
	"발발발발발발방방방방방방방방방방방방방방방방방방방방방방방방방방방방배배배배배배배배배배배배배배배배배배배배백백백백백백백백번번번번번번번번번번벌벌벌벌범범범범범범범범범법법벽벽벽벽벽벽벽",
	"벽벽벽벽변변변변변변변별별별별병병병병병병병병병병병병병병병병병보보보보보보보보보보보보보보보보복복복복복복복복복복복복복복복복복본볼봉봉봉봉봉봉봉봉봉봉봉봉봉봉봉봉부부부부부부부부부부부",
	"부부부부부부부부부부부부부부부부부부부부부부부부부부부부부부부부북분분분분분분분분분분분분분분분분분분분불불불불불붕붕붕붕붕붕비비비비비비비비비비비비비비비비비비비비비비비비비비비비비비비",
	"비비비비비비비비비비비비빈빈빈빈빈빈빈빈빈빈빈빈빈빈빙빙빙빙사사사사사사사사사사사사사사사사사사사사사사사사사사사사사사사사사사사사사사사사사사사사사사사사사사사사사사사사사사사사삭삭삭삭",
	"산산산산산산산산산산산산살살살살살삼삼삼삼삼삼삼삼삽삽삽삽상상상상상상상상상상상상상상상상상상상상상상상상상상상상상상상새새새색색색색색생생생생생서서서서서서서서서서서서서서서서서서서서서",
	"서서서서서서서서서석석석석석석석석석석석석석석석선선선선선선선선선선선선선선선선선선선선선선선선선선선선선선선선설설설설설설설설설설설설설섬섬섬섬섬섬섬섬섭섭섭섭성성성성성성성성성성성성성",
	"성성성성성세세세세세세세세세소소소소소소소소소소소소소소소소소소소소소소소소소소소소소소소소소소소소소속속속속속속속속속손손손손손손솔송송송송송송송송쇄쇄쇄쇄쇄쇠쇠수수수수수수수수수수수수",
	"수수수수수수수수수수수수수수수수수수수수수수수수수수수수수수수수수수수수수수수수수수수수수수수수수숙숙숙숙숙숙숙숙숙숙숙숙순순순순순순순순순순순순순순순순순순순순순순순순순순순술술술술숭숭",
	"숭슬슬슬습습습습습승승승승승승승승승승시시시시시시시시시시시시시시시시시시시시시시시시시시시시식식식식식식식식식식식식식식식신신신신신신신신신신신신신신신신신신신신신신신신실실실실심심심심",
	"심심심심심심십십십쌍씨아아아아아아아아아아아아아아아아아아악악악악악악악악악악악악악악안안안안안안안안안안알알알알암암암암암암암암압압압압앙앙앙앙앙앙앙애애애애애애애애애애애액액액액액액액",
	"앵앵앵앵야야야야야야야야야야야약약약약약약약약약양양양양양양양양양양양양양양양양양양양양양양양양양양양양양양양어어어어어어어어어어억억억억억언언언언언언얼얼엄엄엄엄엄엄업업엔여여여여여여여",
	"여여여여여여여여여여여여여여여여여역역역역역역역역역역역역역연연연연연연연연연연연연연연연연연연연연연연연연연연연연연연연연연연연연연연연연연연연열열열열열열열열열열염염염염염염염염염염염",
	"염염염염엽엽엽엽영영영영영영영영영영영영영영영영영영영영영영영영영영영영영영영영영영영영영영영영예예예예예예예예예예예예예예예예예예예예예예예예오오오오오오오오오오오오오오오오오오오오오오",
	"오오오오오오오오옥옥옥옥옥온온온온온온올옹옹옹옹옹옹옹옹옹와와와와와와와와완완완완완완완완완완완완완완완완완완왈왕왕왕왕왕왜왜왜왜외외외외외요요요요요요요요요요요요요요요요요요요요요요요요",
	"요요요요요요요요요요요요요요욕욕욕욕욕욕용용용용용용용용용용용용용용용용용용용용용용용용우우우우우우우우우우우우우우우우우우우우우우우우우우우우우우우우욱욱욱욱욱욱욱욱욱운운운운운운운운운",
	"운운운운울울울웅웅원원원원원원원원원원원원원원원원원원원원원원원원원원원월월월위위위위위위위위위위위위위위위위위위위위위위위위위유유유유유유유유유유유유유유유유유유유유유유유유유유유유유유",
	"유유유유유유유유유유유유유유유유유유유유유유유유유유육육육육육육육윤윤윤윤윤윤윤윤윤윤윤윤윤율율율율율융융융융융은은은은은은은을음음음음음음읍읍읍응응응응의의의의의의의의의의의의의의의의의",
	"의의이이이이이이이이이이이이이이이이이이이이이이이이이이이이이이이이이이이이이이익익익익익익익익인인인인인인인인인인인인인인인인인인인인인인인인일일일일일일일일일임임임임임임임임임임임입입",
	"입입입잉잉잉잉자자자자자자자자자자자자자자자자자자자자자자자자자자작작작작작작작작작작작작작잔잔잔잔잔잠잠잠잠잠잠잡장장장장장장장장장장장장장장장장장장장장장장장장장장장장장장장장장장장장",
	"장재재재재재재재재재재재재재재재재재쟁쟁쟁쟁저저저저저저저저저저저저저저저저저저저저저저저저저저저저적적적적적적적적적적적적적적적적적적적적적적적적적전전전전전전전전전전전전전전전전전전전",
	"전전전전전전전전전전전전전전전전전전전전전전절절절절절절절절점점점점점점점점점접접접정정정정정정정정정정정정정정정정정정정정정정정정정정정정정정정정정정정정정정정정정정정정정정정정정정정정",
	"정정정제제제제제제제제제제제제제제제제제제제제제제제조조조조조조조조조조조조조조조조조조조조조조조조조조조조조조조조조조조조조조조조조조조조조조족족족족존존졸졸졸종종종종종종종종종종종종종",
	"종종종종좌좌좌좌좌죄주주주주주주주주주주주주주주주주주주주주주주주주주주주주주주주주주주주주주주주주죽죽준준준준준준준준준준준준준준준준준준준줄중중중중즉즐즙즙즙증증증증증증증증증증증지지",
	"지지지지지지지지지지지지지지지지지지지지지지지지지지지지지지지지직직직직직진진진진진진진진진진진진진진진진진진진진진진진진진진진진진진진진진진진질질질질질질질질질질질질질질질짐짐집집집집집",
	"집집징징징차차차차차차차차차차차차차차차착착착착착착착찬찬찬찬찬찬찬찬찬찬찬찬찬찬찬찰찰찰찰찰참참참참참참참참참참창창창창창창창창창창창창창창창창창창창창창창채채채채채채채채채채채채책책책",
	"책처처처처척척척척척척척척척척척척척척척천천천천천천천천천천천천천천천천천천천철철철철철철철철철철첨첨첨첨첨첨첨첨첨첨첩첩첩첩첩첩첩첩첩첩청청청청청청청청체체체체체체체체체체초초초초초초초",
	"초초초초초초초초초초초초초초초초초초초초촉촉촉촉촉촉촌촌촌총총총총총총총총총총총촬최최최추추추추추추추추추추추추추추추추추추추추추추추축축축축축축축축축축축축춘춘춘출출출충충충충충충췌췌췌",
	"취취취취취취취취취취취취취취취측측측측측층치치치치치치치치치치치치치치치치치치치치치치치치칙칙칙친칠칠칠침침침침침침침침침칩칭칭쾌타타타타타타타타타타타타타타탁탁탁탁탁탁탁탁탁탁탁탁탁탁탁",
	"탁탄탄탄탄탄탄탄탄탄탄탈탈탐탐탐탐탑탑탑탕탕탕탕탕태태태태태태태태태태태태태태택택택탱터토토토토통통통통통통통퇴퇴퇴퇴퇴퇴투투투투투투특특틈파파파파파파파파파파파파파파파파판판판판판판판판",
	"판팔팔팔패패패패패패패패패패패팽팽팽팽퍅편편편편편편편편편편폄평평평평평폐폐폐폐폐폐폐폐폐폐포포포포포포포포포포포포포포포포포포포포포포포포포포포포폭폭폭폭폭폭표표표표표표표표표표표표표표",
	"품품풍풍풍풍풍피피피피피피피필필필필필필필필필필핍핍하하하하하하하하하하하하하하학학학학학한한한한한한한한한한한한한한할할함함함함함함함함함함함함합합합합합합합항항항항항항항항항항항항항항",
	"항항항해해해해해해해해해해해해해해해해해해핵핵행행행행행향향향향향향향향향허허허허헌헌헌헌헐험험혁혁혁혁현현현현현현현현현현현현현현현현현현현현현혈혈혈혈혐협협협협협협협협협협협협형형형형",
	"형형형형형형형형형형형형형형형형혜혜혜혜혜혜혜혜혜호호호호호호호호호호호호호호호호호호호호호호호호호호호호호호호호호호호호호호호호호혹혹혹혼혼혼혼혼혼홀홀홀홍홍홍홍홍홍홍홍홍홍화화화화화화",
	"화화화화화화화화확확확확확확환환환환환환환환환환환환환환환환환활활활활활황황황황황황황황황황황황황황황황황황황황황황황황회회회회회회회회회회회회회회회회회회회회획획횡횡횡효효효효효효효효효",
	"효효효효후후후후후후후후후후후후후훈훈훈훈훈훈훈훈훈훈훙훤훤훤훤훼훼훼휘휘휘휘휘휘휘휘휴휴휴휴휴휼휼휼흉흉흉흉흉흑흔흔흔흔흘흘흘흘흠흠흠흡흡흡흡흥희희희희희희희희희희희희희희희희희희희희힐" };




/* Rule tables for stem */

static RuleList step1a_rules[] =
	{
		101,	"sses",		"ss",	3,	1,	-1,	NULL,
		102,	"ies",		"i",	2,	0,	-1,	NULL,
		103,	"ss",		"ss",	1,	1,	-1,	NULL,
		104,	"s",		LAMBDA,	0,	-1,	-1,	NULL,
		000,	NULL,		NULL,	0,	0,	0,	NULL
	};

static RuleList step1b_rules[] =
	{
		105,	"eed",		"ee",	2,	1,	0,	NULL,
		106,	"ed",		LAMBDA,	1,	-1,	-1,	ContainsVowel,
		107,	"ing",		LAMBDA,	2,	-1,	-1,	ContainsVowel,
		000,	NULL,		NULL,	0,	0,	0,	NULL
	};


static RuleList step1b1_rules[] =
	{
		108, 	"at",	"ate",	1,	2,	-1,	NULL,
		109,	"bl",	"ble",	1,	2,	-1,	NULL,
		110,	"iz",	"ize",	1,	2,	-1,	NULL,
		111,	"bb",	"b",	1,	0,	-1,	NULL,
		112,	"dd",	"d",	1,	0,	-1,	NULL,
		113,	"ff",	"f",	1,	0,	-1,	NULL,
		114,	"gg",	"g",	1,	0,	-1,	NULL,
		115,	"mm",	"m",	1,	0,	-1,	NULL,
		116,	"nn",	"n",	1,	0,	-1,	NULL,
		117,	"pp",	"p",	1,	0,	-1,	NULL,
		118,	"rr",	"r",	1,	0,	-1,	NULL,
		119,	"tt",	"t",	1,	0,	-1,	NULL,
		120,	"ww",	"w",	1,	0,	-1,	NULL,
		121,	"xx",	"x",	1,	0,	-1,	NULL,
		122,	LAMBDA, "e",   -1,	  0,	  -1,	AddAnE,
		000,	NULL,   NULL,   0,	  0,	   0,	 NULL,
		  };

static RuleList step1c_rules[] =
			  {
				123,	"y",	"i",	0,	  0,	  -1, ContainsVowel,
		000,	NULL,	NULL,	0,	0,	0,  NULL,
		  };

static RuleList step2_rules[] =
		  {
		203,	"ational", "ate",  6,   2,	0,	NULL,
		204,	"tional",	"tion", 5,	3,	0,	NULL,
		205,	"enci",	"ence", 3,	3,	0, 	NULL, 
		206, 	"anci",	"ance", 3,	3,	0,	NULL,
		207,	"izer",	"ize",  3,	2,	0,	NULL,
		208, 	"abli",	"able", 3,	3,	0, 	NULL,
		209,	"alli",	"al",	  3,	1, 	0,	NULL,
		210,	"entli",	"ent",	  4, 	2, 	0,	NULL,
		211,	"eli",		"e",	  2,	0,	0,	NULL,
		213, 	"ousli",	"ous",	  4, 	2,	0,	NULL,
		214,	"ization",	"ize",	  6,	2,	0,	NULL,
		215,	"ation",	"ate",	  4,	2,	0,	NULL,
		216,	"ator",	"ate",	  3,	2,	0,	NULL,
		217,	"alism",	"al",	  4, 	1,	0,	NULL,
		218,	"iveness",	"ive",	  6,	2,	0,	NULL,
		219,	"fulnes",	"ful",	  5, 	2,	0,	NULL,
		220,	"ousness",	"ous",	  6, 	2,	0,	NULL,
		221,	"aliti",	"al", 	  4,	1, 	0,	NULL,
		222,	"iviti",	"ive",	  4,	2,	0,	NULL,
		223,	"biliti",	"ble",	  5,	2,	0,	NULL,
		000,	NULL,		NULL,	  0, 	0,	0,	NULL,
		   };

static RuleList step3_rules[] = 
		 {
		301,	"icate",	"ic",	  4,	1,	0,	NULL,
		302,	"ative",	LAMBDA,  4,	-1,	0,	NULL,
		303,	"alize",	"al",	  4,	1,	0,	NULL,
		304,	"iciti",	"ic",	  4,	1,	0,	NULL,
		305,	"ical",	"ic",	  3,	1,	0,	NULL,
		308,	"ful",		LAMBDA, 2,	-1,	0,	NULL,
		309,	"ness",	LAMBDA, 3,	-1,	0,	NULL,
		000,	NULL,		NULL,	  0,	0,	0,	NULL,
		 };

static RuleList step4_rules[] =
		 {	
		401,	"al",		LAMBDA,  1,	-1,	1,	NULL,
		402,	"ance",	LAMBDA,  3,	-1,	1,	NULL,
		403,	"ence",	LAMBDA,  3,	-1,	1,	NULL,
		405,	"er",		LAMBDA,  1,	-1,	1,	NULL,
		406,	"ic",		LAMBDA,  1,	-1,	1,	NULL,
		407,	"able",	LAMBDA,  3,	-1,	1,	NULL,
		408,	"ible",	LAMBDA,  3,	-1,	1,	NULL,
		409,	"ant",		LAMBDA,  2,	-1,	1,	NULL,
		410,	"ement",	LAMBDA,  4,	-1,	1,	NULL,
		411,	"ment",	LAMBDA,  3,	-1,	1,	NULL,
		412,	"ent",		LAMBDA,  2,	-1,	1,	NULL,
		423,	"sion",	"s",  	3,	0,	1,	NULL,
		424,	"tion",	"t",  3,	0,	1,	NULL,
		415,	"ou",		LAMBDA,  1,	-1,	1,	NULL,
		416,	"ism",		LAMBDA,  2,	-1,	1,	NULL,
		417,	"ate",		LAMBDA,  2,	-1,	1,	NULL,
		418,	"iti",		LAMBDA,  2,	-1,	1,	NULL,
		419,	"ous",		LAMBDA,  2,	-1,	1,	NULL,
		420,	"ive",		LAMBDA,  2,	-1,	1,	NULL,
		421,	"ize",		LAMBDA,  2,	-1,	1,	NULL,
		000,	NULL,		NULL,  0,	0,	0,	NULL,
		 };								

static RuleList step5a_rules[] =
		 {	
		501,	"e",		LAMBDA,  0, 	-1,	1,	NULL,
		502,	"e",		LAMBDA,  0, 	-1,   -1,	RemoveAnE,
		000,	NULL,		NULL,  0,	0,	0,	NULL,	
		 };

static RuleList step5b_rules[] = 
		 {
		503,   "ll",		"l",	1, 	0,	1,	NULL,
		000,	NULL,		NULL,	0,	0,	0,	NULL,
		 };






/**************************************/
/*	 	 Functions about stem 	  	  */
/**************************************/	

static int WordSize(register char* word)
{
	
	register int result;
	register int state;

	result = 0;
	state = 0;

	while (EOS != *word)
	{
	switch(state)
		{
		case 0: state = (IsVowel(*word)) ? 1 : 2;
			  break;
		case 1: state = (IsVowel(*word)) ? 1 : 2;
			  if (2 == state) result++;
			  break;				   
		case 2: state = (IsVowel(*word) || ('y' == *word)) ? 1 : 2;
			  break;
		}
	word++;
	}
	
	return(result);
	
}


static int ContainsVowel(register char* word)
{
	
	if (EOS == *word)
		return(FALSE);
	else
		return(IsVowel(*word) || (NULL != strpbrk(word+1, "aeiouy")));
}


static int EndsWithCVC(register char* word)
{
	int length;

	if ((length = strlen(word)) < 3)
		return(FALSE);
	else
	{
		end = word + length - 1;
		return(  (NULL == strchr("aeiouwxy", *end--))
			&& (NULL != strchr("aeiouy", *end--))
			&& (NULL == strchr("aeiou", *end)));
	}
	
}


static int AddAnE(register char *word)
{
	
	return((1 == WordSize(word)) && EndsWithCVC(word));
	
}


static int RemoveAnE(register char* word)
{
	
	return((1 == WordSize(word)) && !EndsWithCVC(word));
	
}


static int ReplaceEnd(register char* word, RuleList* rule)
{
	
	register char *ending;
	char tmp_ch;

	while (0 != rule->id)
		{
		if((rule->old_offset > 0) && ((strlen(word) - 1) < rule->old_offset))
			{
			rule++;
			continue;
			}

		ending = end - rule->old_offset;
		if (word != ending)
			if (0 == strcmp(ending, rule->old_end))
				{
				tmp_ch = *ending;
				*ending = EOS;
				if (rule->min_root_size < WordSize(word))
					if (!rule->condition || (*rule->condition)(word))
						{
						(void)strcat(word, rule->new_end);
						end = ending + rule->new_offset;
						break;
						}
					*ending = tmp_ch;
				}
			rule++;
		}
	return(rule->id);
	
}

	
int Stem(register char* word)
{
	
	int rule;

	for (end = word; *end != EOS; end++)
		if (!isalpha(*end)) return(FALSE);
	end--;

	ReplaceEnd(word, step1a_rules);
	rule = ReplaceEnd(word, step1b_rules);
	if ((106 == rule) || (107 == rule))
		ReplaceEnd(word, step1b1_rules);
	ReplaceEnd(word, step1c_rules);		  

	ReplaceEnd(word, step2_rules);		  

	ReplaceEnd(word, step3_rules);		  

	ReplaceEnd(word, step4_rules);		  

	ReplaceEnd(word, step5a_rules);		  

	ReplaceEnd(word, step5b_rules);		  

	return(TRUE);
	
}




/******************************/
/* Functions about dictoinary */
/******************************/	

/*
 * Function : char* InitJosaEomiTable(void)
 * 
 * Discription : Initialize the Josa and Eomi table 
 * 
 * Returns : Initialized the JosaEomi table pointer 
 */	

char* InitJosaEomiTable(void)
{

	Four i;									/* index of HashSize	*/
	Four allocatedSize = DEFAULTBUFFERSIZE; /* the defult allocated memory size	*/
	Four usedLength = sizeof(Four)*2;		/* the variable having the defult memory size	*/		  

	char* tmpTablePtr;						/* the temporary variable whether the allocated table is null or not	*/



	for(i=0; i<HASHSIZE; i++)
	{
		Dic.JosaEomiTable[i] = (char*)QuickFitMM_Alloc(&staticMMHandle, sizeof(char) * DEFAULTBUFFERSIZE);
		
		if(Dic.JosaEomiTable[i]==NULL) 
		{
			fprintf(stderr, "QuickFitMM_Alloc Error in InitJosaEomiTable\n");
			exit(-1);
		} 
				
		tmpTablePtr = Dic.JosaEomiTable[i];
		
		if(tmpTablePtr == NULL) return NULL;

		memcpy(tmpTablePtr, &allocatedSize, sizeof(Four));

		tmpTablePtr += sizeof(Four);
		memcpy(tmpTablePtr, &usedLength, sizeof(Four));


	}

	return tmpTablePtr;

}

/******************************/
/* Functions about dictoinary */
/******************************/	

/*
 * Function : char* InitJosaTable(void)
 * 
 * Discription : Initialize the Josa table 
 * 
 * Returns : Initialized the Josa table pointer 
 */	

char* InitJosaTable(void)
{

	Four i;									/* index of HashSize	*/
	Four allocatedSize = DEFAULTBUFFERSIZE; /* the defult allocated memory size	*/
	Four usedLength = sizeof(Four)*2;		/* the variable having the defult memory size	*/		  

	char* tmpTablePtr;						/* the temporary variable whether the allocated table is null or not	*/



	for(i=0; i<HASHSIZE; i++)
	{
		Dic.JosaTable[i] = (char*)QuickFitMM_Alloc(&staticMMHandle, sizeof(char) * DEFAULTBUFFERSIZE);
		
		if(Dic.JosaTable[i]==NULL) 
		{
			fprintf(stderr, "QuickFitMM_Alloc Error in InitEomiTable\n");
			exit(-1);
		} 
				
		tmpTablePtr = Dic.JosaTable[i];
		
		if(tmpTablePtr == NULL) return NULL;

		memcpy(tmpTablePtr, &allocatedSize, sizeof(Four));

		tmpTablePtr += sizeof(Four);
		memcpy(tmpTablePtr, &usedLength, sizeof(Four));


	}

	return tmpTablePtr;

}

/*
 * Function : char* InitStopwordTable(void) 
 * 
 * Discription : Initialize the stopword table 
 * 
 * Returns : Initialized the stopword table pointer 
 */	

char* InitStopwordTable(void)
{

	Four i;									/* index of HashSize	*/
	Four allocatedSize = DEFAULTBUFFERSIZE; /* the defult allocated memory size	*/
	Four usedLength = sizeof(Four)*2;		/* the variable having the defult memory size	*/		  

	char* tmpTablePtr;						/* the temporary variable whether the allocated table is null or not	*/


	for(i=0; i<HASHSIZE; i++)
	{
		Dic.StopwordTable[i] = (char*)QuickFitMM_Alloc(&staticMMHandle, sizeof(char) * DEFAULTBUFFERSIZE);
		
		if(Dic.StopwordTable[i]==NULL) 
		{
			fprintf(stderr, "QuickFitMM_Alloc Error in  InitStopwordTable\n");
			exit(-1);
		} 
		
		tmpTablePtr = Dic.StopwordTable[i];
		
		if(tmpTablePtr == NULL) return NULL;

		memcpy(tmpTablePtr, &allocatedSize, sizeof(Four));

		tmpTablePtr += sizeof(Four);
		memcpy(tmpTablePtr, &usedLength, sizeof(Four));
			
		
	}


	return tmpTablePtr;

}


/*
 * Function : char* InitOnenounTable(void) 
 * 
 * Discription : Initialize the useful one-gulja noun table 
 * 
 * Returns : Initialized the onenoun table pointer 
 */	

char* InitOnenounTable(void)
{

	Four i;									/* index of HashSize	*/
	Four allocatedSize = DEFAULTBUFFERSIZE; /* the defult allocated memory size	*/
	Four usedLength = sizeof(Four)*2;		/* the variable having the defult memory size	*/		  

	char* tmpTablePtr;						/* the temporary variable whether the allocated table is null or not	*/


	for(i=0; i<HASHSIZE; i++)
	{
		Dic.OnenounTable[i] = (char*)QuickFitMM_Alloc(&staticMMHandle, sizeof(char) * DEFAULTBUFFERSIZE);
		
		if(Dic.OnenounTable[i]==NULL) 
		{
			fprintf(stderr, "QuickFitMM_Alloc Error in  InitOnenounTable\n");
			exit(-1);
		} 
		
		tmpTablePtr = Dic.OnenounTable[i];
		
		if(tmpTablePtr == NULL) return NULL;

		memcpy(tmpTablePtr, &allocatedSize, sizeof(Four));

		tmpTablePtr += sizeof(Four);
		memcpy(tmpTablePtr, &usedLength, sizeof(Four));
			
		
	}


	return tmpTablePtr;

}


/*
 * Function : char* InitOnepronounTable(void) 
 * 
 * Discription : Initialize the one-gulja pronoun table 
 * 
 * Returns : Initialized the one-gulja pronoun table pointer 
 */	

char* InitOnepronounTable(void)
{

	Four i;									/* index of HashSize	*/
	Four allocatedSize = DEFAULTBUFFERSIZE; /* the defult allocated memory size	*/
	Four usedLength = sizeof(Four)*2;		/* the variable having the defult memory size	*/		  

	char* tmpTablePtr;						/* the temporary variable whether the allocated table is null or not	*/


	for(i=0; i<HASHSIZE; i++)
	{
		Dic.OnepronounTable[i] = (char*)QuickFitMM_Alloc(&staticMMHandle, sizeof(char) * DEFAULTBUFFERSIZE);
		
		if(Dic.OnepronounTable[i]==NULL) 
		{
			fprintf(stderr, "QuickFitMM_Alloc Error in  InitOnepronounTable\n");
			exit(-1);
		} 
		
		tmpTablePtr = Dic.OnepronounTable[i];
		
		if(tmpTablePtr == NULL) return NULL;

		memcpy(tmpTablePtr, &allocatedSize, sizeof(Four));

		tmpTablePtr += sizeof(Four);
		memcpy(tmpTablePtr, &usedLength, sizeof(Four));
			
		
	}


	return tmpTablePtr;

}

/*
 * Function : char* InitSpecialnounTable(void) 
 * 
 * Discription : Initialize the specialnoun table 
 * 
 * Returns : Initialized the specialnoun table pointer 
 */	

char* InitSpecialnounTable(void)	
{

	Four i;									/* index of HashSize	*/
	Four allocatedSize = DEFAULTBUFFERSIZE; /* the defult allocated memory size	*/
	Four usedLength = sizeof(Four)*2;		/* the variable having the defult memory size	*/		  

	char* tmpTablePtr;						/* the temporary variable whether the allocated table is null or not	*/


	for(i=0; i<HASHSIZE; i++)
	{
		Dic.SpecialnounTable[i] = (char*)QuickFitMM_Alloc(&staticMMHandle, sizeof(char) * DEFAULTBUFFERSIZE);
		
		if(Dic.SpecialnounTable[i]==NULL) 
		{
			fprintf(stderr, "QuickFitMM_Alloc Error in  InitSpecialnounTable\n");
			exit(-1);
		} 
		
		tmpTablePtr = Dic.SpecialnounTable[i];
		
		if(tmpTablePtr == NULL) return NULL;

		memcpy(tmpTablePtr, &allocatedSize, sizeof(Four));

		tmpTablePtr += sizeof(Four);
		memcpy(tmpTablePtr, &usedLength, sizeof(Four));
			
		
	}


	return tmpTablePtr;

}

/*
 * Function : Four AddStringToHashTable(char** table, char* word) 
 * 
 * Discription : Insert the word into the table by using hash function 
 * 
 * Returns : the current used length of hash table after inserting the word
 */	

Four AddStringToHashTable(
	char** table,	/* IN	pointer of the table to input the word	*/
	char* word )	/* IN	pointer of the word to input	*/				
{

	Four allocatedSize;			/* the variable for storing the allocated hash table size having the special hash value	*/
	Four usedLength;			/* the variable for storing the used hast table size having the special has value	*/ 
	Four e;						/* error code	*/

	Four wordLen = strlen(word);						/* variable for storing the word length	*/
	Four tmpBufferSize = sizeof(Four) + wordLen + 1;	/* variable for storing the word and word length	*/
	Four hashValue = GetHashValue(word);				/* variable for storing the hash value of the word	*/
	Four tmpUsedLength;									/* variable for storing the temporary variable to update the hash table */ 

	char* tmpTablePtr = table[hashValue];				/* pointer variable to point the given hash value table	*/


	memcpy(&allocatedSize, tmpTablePtr, sizeof(Four));
	tmpTablePtr += sizeof(Four);
	memcpy(&usedLength, tmpTablePtr, sizeof(Four));


	/* if the allocated memory size is smaller, Double the allocated memory	*/

	if( tmpBufferSize + usedLength >= allocatedSize )
	{

		allocatedSize *= 2;

		e = QuickFitMM_ReAlloc(&staticMMHandle, (void**) &(table[hashValue]), allocatedSize);
		if (e < 0) QUICKFITMM_ERROR(e);
		

		memcpy(table[hashValue], &allocatedSize, sizeof(Four));


	}

	/* Store the word in the hash table and update the hash table variable	*/

	tmpUsedLength = usedLength + sizeof(Four) + wordLen + 1;
	
	tmpTablePtr = table[hashValue];
	tmpTablePtr += usedLength;
	memcpy(tmpTablePtr, &wordLen, sizeof(Four));

	tmpTablePtr += sizeof(Four);
	memcpy(tmpTablePtr, word, wordLen+1);

	tmpTablePtr = table[hashValue];
	tmpTablePtr += sizeof(Four);
	memcpy(tmpTablePtr, &tmpUsedLength, sizeof(Four));


	return tmpUsedLength;

}


/*
 * Function : Four FindStringInHashTable(char** table, char* word) 
 * 
 * Discription : Find whether the word is in the table or not  
 * 
 * Returns : TURE if the string is in that table
 *			 FALSE if the string in not in that table 
 */	

Four FindStringInHashTable(
	char** table,		/* IN	pointer of the table to input the word	*/ 
	char* word )		/* IN 	pointer of the word to input	*/
{
	
	Four allocatedSize;						/* the variable for storing the allocated hash table size having the special hash value */
	Four usedLength;						/* the variable for storing the used hast table size having the special has value   */	

	Four wordLen = strlen(word);			/* variable for storing the word length */
	Four hashValue = GetHashValue(word);	/* variable for storing the hash value of the word  */
	Four tmpTableOffset;					/* variable for storing the next position to read	*/
	Four tmpWordLength;						/* variable for storing the temporary variable to update the hash table */	

	char* tmpTablePtr = table[hashValue];	/* pointer variable to point the given hash value table */


	/* Read the allocated hash table size and currently used hash table	*/

	memcpy(&allocatedSize, tmpTablePtr, sizeof(Four));
	tmpTablePtr += sizeof(Four);
	memcpy(&usedLength, tmpTablePtr, sizeof(Four));


	tmpTablePtr += sizeof(Four);
	tmpTableOffset = 2*sizeof(Four);


	/* 
		Compare the given word and the words in the hash table	
	   	return TURE if the word is in the hash table
	   	or not return FALSE 
	*/

	while(tmpTableOffset < usedLength)
	{
		memcpy(&tmpWordLength, tmpTablePtr, sizeof(Four));

		tmpTablePtr += sizeof(Four);

		if(tmpWordLength == wordLen)
		{
				
			if(!memcmp(word, tmpTablePtr, wordLen))	 return TRUE;
			
		}	
		
			tmpTablePtr += tmpWordLength + 1;
			tmpTableOffset += sizeof(Four) + tmpWordLength + 1;	
	
	}
	
	return FALSE;

}
	

/*
 * Function : Four PrintHashTable(char** table, int hashValue) 
 * 
 * Discription : For debug, print the table in size of hash size  
 * 
 * Returns : Nothing
 */	

void PrintHashTable(
	char** table,		/* IN	pointer of the table to print	*/ 
	int hashValue )		/* IN	hash value to print	*/
{

	Four allocatedSize;
	Four usedLength;
	Four tmpTableOffset;
	Four tmpWordLength;


	char* tmpTablePtr;


	fprintf(stderr,"\n=====table[%d]=====\n", hashValue);

	tmpTablePtr = table[hashValue];
	memcpy(&allocatedSize, tmpTablePtr, sizeof(Four));

	tmpTablePtr += sizeof(Four);
	memcpy(&usedLength, tmpTablePtr, sizeof(Four));

	tmpTablePtr += sizeof(Four);
	tmpTableOffset = 2*sizeof(Four);		



	while(tmpTableOffset < usedLength)
	{
		memcpy(&tmpWordLength, tmpTablePtr, sizeof(Four));

		tmpTablePtr += sizeof(Four);

		fprintf(stderr, "word = %s, length = %d\n", tmpTablePtr, tmpWordLength);

		tmpTablePtr += tmpWordLength + 1;
		tmpTableOffset += sizeof(Four) + tmpWordLength + 1;	
		

	}


}




/******************************/
/* Functions about Hash table */
/******************************/

/*
 * Function : Four GetHashValue (char* word) 
 * 
 * Discription : Give the hash value according with the word  
 * 
 * Returns : hash value 
 */	

Four GetHashValue (char* word)	/*	IN 	pointer of the word	*/
{
	Four hashValue = 0;
	Four tmp;

	for( tmp = 0; *word != '\0'; word++)
	{
		tmp = (Four)(0xff & (*word));
		hashValue += tmp;
	}

	return (srand(hashValue), rand()) % HASHSIZE;

}





/**********************************/
/* Functions about the Dictionary */
/**********************************/


/*
 * Function : int InitJosaEomiDic(FILE* fp_josafile, FILE* fp_eomifile) 
 * 
 * Discription : Read the josas and eomis line by line from the josa-file and eoki-file pointers 
 * 				and then insert them into the josa-eomi hash table   
 * 
 * Returns : eNOERROR if there is no error
 *			 Serveral Error code if there is some errors			  
 */	

int InitJosaEomiDic(
	FILE* fp_josafile, 	/* IN	file pointer of the josa file	*/ 
	FILE* fp_eomifile )	/* IN	file pointer of the eomi file	*/
{

	char josa_eomi[MAXJOSAEOMISIZE] ;		/* temporary variable to store the josa and eomi from the file	*/  
	Four	tmp, len ;						/* temporary variables	*/	


	/* assumption : the size of josa is not longer than 10 gulja */
	/* if you want the josa or eomi longer than 10 gulja in the dictionary */
	/* change the JOSAEOMISIZE in the header file */
	
	if(InitJosaEomiTable()==NULL) return eJOSAEOMITABLEINIT;
	

	/* Get the josa in the file line by line */ 
	if( fgets( josa_eomi, MAXJOSAEOMISIZE, fp_josafile ) == NULL )
		return eJOSAINIT ;

	/* Insert the josa in the table */
	while( josa_eomi[0] != '\0' ) 
	{
	 	
	 	/* Remove the special character(ex. carriage return) at the end of the josa or eomi */
		/* Because the mark of ending line is different in accordance with OS */
		len = strlen(josa_eomi) +1;
		for( tmp = 0 ; tmp < len ; tmp ++ )
			if( !(josa_eomi[tmp] & 0x80)) josa_eomi[tmp] = '\0';

		
		/* Inser the josa and eomi in the table by their size */
		AddStringToHashTable(Dic.JosaEomiTable, josa_eomi);


		if( fgets( josa_eomi, MAXJOSAEOMISIZE, fp_josafile ) == NULL )
			break ;
			
	}


	if( fgets( josa_eomi, MAXJOSAEOMISIZE, fp_eomifile ) == NULL )
		return eEOMIINIT ;

	/* Insert the eomi in the table in the same way with josa */
	while( josa_eomi[0] != '\0' ) 
	{
	 			
		len = strlen(josa_eomi)+1;
		for( tmp = 0 ; tmp < len ; tmp ++ )
			if( !(josa_eomi[tmp] & 0x80)) josa_eomi[tmp] = '\0';

				
		/* Inser the josa and eomi in the table by their size */
		AddStringToHashTable(Dic.JosaEomiTable, josa_eomi);

		if( fgets( josa_eomi, MAXJOSAEOMISIZE, fp_eomifile ) == NULL )
			break ;
			
	}

	/* For test, display components in the josa_eomi_table  

	for (tmp = 0 ; tmp < HASHSIZE; tmp++)
		PrintHashTable(Dic.JosaEomiTable, tmp);
	
	 */
	
	return eNOERROR ;
	
}

/*
 * Function : int InitJosaDic(FILE* fp_josafile)
 * 
 * Discription : Read the josas line by line from the josa-file pointers 
 * 				and then insert them into the josa hash table   
 * 
 * Returns : eNOERROR if there is no error
 *			 Serveral Error code if there is some errors			  
 */	

int InitJosaDic(FILE* fp_josafile) 	/* IN	file pointer of the josa file	*/ 
{

	char josa[MAXJOSAEOMISIZE] ;		/* temporary variable to store the josa from the file	*/  
	Four	tmp, len ;						/* temporary variables	*/	


	/* assumption : the size of josa is not longer than 10 gulja */
	/* if you want the josa or eomi longer than 10 gulja in the dictionary */
	/* change the JOSAEOMISIZE in the header file */
	
	if(InitJosaTable()==NULL) return eJOSATABLEINIT;
	
	if (fp_josafile != NULL) rewind(fp_josafile);	

	/* Get the josa in the file line by line */ 
	if( fgets( josa, MAXJOSAEOMISIZE, fp_josafile ) == NULL )
		return eJOSAINIT ;

	/* Insert the josa in the table */
	while( josa[0] != '\0' ) 
	{
	 	
	 	/* Remove the special character(ex. carriage return) at the end of the josa or eomi */
		/* Because the mark of ending line is different in accordance with OS */
		len = strlen(josa) +1;
		for( tmp = 0 ; tmp < len ; tmp ++ )
			if( !(josa[tmp] & 0x80)) josa[tmp] = '\0';

		
		/* Inser the josa in the table by their size */
		AddStringToHashTable(Dic.JosaTable, josa);


		if( fgets( josa, MAXJOSAEOMISIZE, fp_josafile ) == NULL )
			break ;
			
	}

	/* For test, display components in the josa_eomi_table  

	for (tmp = 0 ; tmp < HASHSIZE; tmp++)
		PrintHashTable(Dic.JosaTable, tmp);
	
	 */
	
	return eNOERROR ;
	
}

/*
 * Function : int InitStopwordDic(FILE* fp_korstopfile, FILE* fp_engstopfile) 
 * 
 * Discription : Read the Korean stopword and English Stopword line by line from the stopword-file pointer 
 * 				and then insert them into the stopword hash table   
 * 
 * Returns : eNOERROR if there is no error
 *			 Serveral Error code if there is some errors			  
 */

int InitStopwordDic(
	FILE* fp_korstopfile,	/* IN	file pointer of the Koeran stopword file	*/	 
	FILE* fp_engstopfile )	/* IN	file pointer of the English soptword file	*/
{

	char stopword[MAXSTOPWORDSIZE] ;	/* temporary variable to store the stopword from the stopword file	*/
	Four	tmp, len ;					/* temporary variables	*/
		

	/* assumption : the size of stopword is not longer than 10 gulja */
	/* if you want the stopword longer than 10 gulja in the dictionary */
	/* change the STOPWORDSIZE in the header file */

	if(InitStopwordTable()==NULL) return eSTOPWORDTABLEINIT;



	/* Get the Korean stopword in the file line by line */ 	
	if( fgets( stopword, MAXSTOPWORDSIZE, fp_korstopfile ) == NULL )
		return eKORSTOPWORDINIT ;

	while( stopword[0] != '\0' ) 
	{
	 	
	 	/* Remove the special character(ex. carriage return) at the end of the stopword */
		/* Because the mark of ending line is different in accordance with OS */
		len = strlen(stopword) +1;
		for( tmp = 0 ; tmp < len ; tmp ++ )
			if( !(stopword[tmp] & 0x80)) stopword[tmp] = '\0';

		
		/* Inser the stopword in the table */
		AddStringToHashTable(Dic.StopwordTable, stopword);

		if( fgets( stopword, MAXSTOPWORDSIZE, fp_korstopfile ) == NULL )
			break ;
			
	}

	/* Get the English stopword in the file line by line */ 
	if( fgets( stopword, MAXSTOPWORDSIZE, fp_engstopfile ) == NULL )
	return eENGSTOPWORDINIT ;

	while( stopword[0] != '\0' ) 
	{
	 	
	 	/* assumption : all English stopwords are in lower-case format */
		len = strlen(stopword)+1;
		for( tmp = 0 ; tmp < len ; tmp ++ )
			if((stopword[tmp]<'a') || (stopword[tmp]>'z')) stopword[tmp] = '\0';

		/* Inser the stopword in the table */
		AddStringToHashTable(Dic.StopwordTable, stopword);

		if( fgets( stopword, MAXSTOPWORDSIZE, fp_engstopfile ) == NULL )
			break ;
			
	}

	
	/* For test, display components in the josa_eomi_table  

	for(tmp = 0; tmp<HASHSIZE; tmp++)
		PrintHashTable(Dic.StopwordTable, tmp);

	*/
	
	return eNOERROR ;
	
}


/*
 * Function : int InitOnenounDic(FILE* fp_onenounfile) 
 * 
 * Discription : Read the useful one-gulja nouns line by line from the onenoun-file pointer 
 * 				and then insert them into the onenoun hash table   
 * 
 * Returns : eNOERROR if there is no error
 *			 Serveral Error code if there is some errors			  
 */

int InitOnenounDic(FILE* fp_onenounfile)	/* IN file pointer of useful one-gulja noun file	*/
{

	char onenoun[MAXONENOUNWORDSIZE] ;	/* temporary variable to store the one noun from the onenoun file	*/
	Four	tmp, len ;					/* temporary variables	*/
		
	if(InitOnenounTable()==NULL) return eONENOUNTABLEINIT;


	/* Get the Korean stopword in the file line by line */ 	
	if( fgets( onenoun, MAXONENOUNWORDSIZE, fp_onenounfile ) == NULL )
		return eONENOUNINIT ;

	while( onenoun[0] != '\0' ) 
	{
	 	
	 	/* Remove the special character(ex. carriage return) at the end of the stopword */
		/* Because the mark of ending line is different in accordance with OS */
		len = strlen(onenoun) +1;
		for( tmp = 0 ; tmp < len ; tmp ++ )
			if( !(onenoun[tmp] & 0x80)) onenoun[tmp] = '\0';

		
		/* Inser the stopword in the table */
		AddStringToHashTable(Dic.OnenounTable, onenoun);

		if( fgets( onenoun, MAXONENOUNWORDSIZE, fp_onenounfile ) == NULL )
			break ;
			
	}

	
	/* For test, display components in the josa_eomi_table 

	for(tmp = 0; tmp<HASHSIZE; tmp++)
		PrintHashTable(Dic.OnenounTable, tmp);
	*/

	
	return eNOERROR ;
	
}	


/*
 * Function : int InitOnepronounDic(FILE* fp_onepronounfile) 
 * 
 * Discription : Read the useful one-gulja pronouns line by line from the onepronoun-file pointer 
 * 				and then insert them into the onepronoun hash table   
 * 
 * Returns : eNOERROR if there is no error
 *			 Serveral Error code if there is some errors			  
 */

int InitOnepronounDic(FILE* fp_onepronounfile)	/* IN	file pointer of onepronoun file	*/ 
{

	char onenoun[MAXONENOUNWORDSIZE] ;	/* temporary variable to store the onepronoun from the onepronoun file	*/
	Four	tmp, len ;					/* temporary variables	*/
		
	if(InitOnepronounTable()==NULL) return eONENOUNTABLEINIT;


	/* Get the Korean stopword in the file line by line */ 	
	if( fgets( onenoun, MAXONENOUNWORDSIZE, fp_onepronounfile ) == NULL )
		return eONENOUNINIT ;

	while( onenoun[0] != '\0' ) 
	{
	 	
	 	/* Remove the special character(ex. carriage return) at the end of the stopword */
		/* Because the mark of ending line is different in accordance with OS */
		len = strlen(onenoun) +1;
		for( tmp = 0 ; tmp < len ; tmp ++ )
			if( !(onenoun[tmp] & 0x80)) onenoun[tmp] = '\0';

		
		/* Inser the stopword in the table */
		AddStringToHashTable(Dic.OnepronounTable, onenoun);

		if( fgets( onenoun, MAXONENOUNWORDSIZE, fp_onepronounfile ) == NULL )
			break ;
			
	}

	
	/* For test, display components in the josa_eomi_table 

	for(tmp = 0; tmp<HASHSIZE; tmp++)
		PrintHashTable(Dic.OnerpronounTable, tmp);
	*/
	
	return eNOERROR ;
	
}	


/*
 * Function : int InitSpecialnounDic(FILE* fp_specialnounfile1, FILE* fp_specialnounfile2) 
 * 
 * Discription : Read the specialnoun line by line from the specialnoun-file1 and specialnoun-file2 pointers 
 * 				and then insert them into the specialnoun hash table   
 * 
 * Returns : eNOERROR if there is no error
 *			 Serveral Error code if there is some errors			  
 */	

int InitSpecialnounDic(
	FILE* fp_specialnounfile1, 	/* IN	file pointer of the specialnoun file1	*/ 
	FILE* fp_specialnounfile2)		/* IN	file pointer of the specialnoun file2	*/
{

	char specialnoun[MAXSPECIALNOUNSIZE] ;		/* temporary variable to store the specialnoun from the file	*/  
	Four	tmp, len ;						/* temporary variables	*/	


	/* assumption : the size of specialnoun is not longer than 20 gulja */
	/* if you want the specialnoun longer than 10 gulja in the dictionary */
	/* change the SPECIALNOUNSIZE in the header file */
	
	if(InitSpecialnounTable()==NULL) return eSPECIALNOUNTABLEINIT;
	

	/* Get the specialnoun in the file line by line */ 
	if( fgets( specialnoun, MAXSPECIALNOUNSIZE, fp_specialnounfile1 ) == NULL )
		return eSPECIALNOUNINIT ;

	/* Insert the specialnoun in the table */
	while( specialnoun[0] != '\0' ) 
	{
	 	
	 	/* Remove the special character(ex. carriage return) at the end of the josa or eomi */
		/* Because the mark of ending line is different in accordance with OS */
		len = strlen(specialnoun) +1;
		for( tmp = 0 ; tmp < len ; tmp ++ )
			if( !(specialnoun[tmp] & 0x80)) specialnoun[tmp] = '\0';

		
		/* Inser the specialnoun in the table by their size */
		AddStringToHashTable(Dic.SpecialnounTable, specialnoun);


		if( fgets( specialnoun, MAXSPECIALNOUNSIZE, fp_specialnounfile1 ) == NULL )
			break ;
			
	}


	if( fgets( specialnoun, MAXJOSAEOMISIZE, fp_specialnounfile2 ) != NULL )
	{
		/* Insert the specialnoun in the table in the same way with josa */
		while( specialnoun[0] != '\0' ) 
		{
	 			
			len = strlen(specialnoun)+1;
			for( tmp = 0 ; tmp < len ; tmp ++ )
				if( !(specialnoun[tmp] & 0x80)) specialnoun[tmp] = '\0';

				
			/* Inser the josa and eomi in the table by their size */
			AddStringToHashTable(Dic.SpecialnounTable, specialnoun);

			if( fgets( specialnoun, MAXSPECIALNOUNSIZE, fp_specialnounfile2 ) == NULL )
				break ;
			
		}
	}
	/* For test, display components in the specialnoun_table  

	for (tmp = 0 ; tmp < HASHSIZE; tmp++)
		PrintHashTable(Dic.SpecialnounTable, tmp);
	
	 */
	
	return eNOERROR ;
	
}


/****************************************/
/*  Functions abount processing hangle  */
/****************************************/

/*
 * Function : int ConvertHanjaToHangle( char* ptr_buffer) 
 * 
 * Discription : Conver the word into Hangle if the word is Hanja 
 * 
 * Returns : eNOERROR if there is no error
 */

int ConvertHanjaToHangle( char* ptr_buffer)	/* IN	the word to convert hanja to hangle 	*/
{
	unsigned char tmp_onechar1 = (unsigned char) *ptr_buffer;		/* pointer of the first character	*/
	unsigned char tmp_onechar2 = (unsigned char) *(ptr_buffer+1);	/* pointer of the second character	*/
	
	tmp_onechar1 -= 0xca;
	tmp_onechar2 -= 0xa1;
	
	memcpy(ptr_buffer, &HanjaToHangleTable[(unsigned int)tmp_onechar1][(unsigned int)tmp_onechar2 * 2], 2);
	
	return(eNOERROR);	
}


/*
 * Function : int Remove_SpecialChar(char* buffer) 
 * 
 * Discription : Remove the special characters from the word 
 *				After removing the special characters, the word includes only Hangle, periods(.) 
 * 
 * Returns : eNOERROR if successfully special characters removed 
 */

int Remove_SpecialChar(char* buffer)	/* IN	pointer to remove special characters 	*/	
{
		
	unsigned char * ptr_buffer = (unsigned char*)buffer;	/* pointer to point buffer	*/ 
		
	while(*ptr_buffer)
	{
		/* In case of Hangle */
		if(*ptr_buffer >= 0xb0 && *ptr_buffer <= 0xc8 && *(ptr_buffer+1) >= 0xa1 && *(ptr_buffer+1) <= 0xfe) 
			ptr_buffer += 2;
		/* In case of Hanja */
		else if(*ptr_buffer >= 0xca && *ptr_buffer <=0xfd && *(ptr_buffer+1) >= 0xa1 && *(ptr_buffer+1) <= 0xfe ) 
		{
			ConvertHanjaToHangle( (char*) ptr_buffer);
			ptr_buffer += 2;
		}
		/* Others, a 2-byte special is replaced with space(' ') */
		else if(*ptr_buffer & 0x80)  
		{
			*ptr_buffer++ = ' ';
			*ptr_buffer++ = ' ';
		}	
		/* In case of lowercase letter in English */		
		else if(*ptr_buffer >= 'a' && *ptr_buffer <= 'z') 
			ptr_buffer++;
		/* In case of a capital letter in English */
		else if(*ptr_buffer >= 'A' && *ptr_buffer <= 'Z') 
		{
			*ptr_buffer = (*ptr_buffer)+('a'-'A');
			 ptr_buffer++;
		}
		/* In case of number */
		else if(*ptr_buffer >= '0' && *ptr_buffer <= '9') 
			 ptr_buffer++;
		/* A period is not replaced Because it is used to judge the sentence */
		else if(*ptr_buffer == '.')  
			 ptr_buffer++;
		/* Others, a 1-byte special character is replaced */
		else	*ptr_buffer++ = ' '; 		
		
	}
	
	return eNOERROR;	
	
}


/*
 * Function : int Remove_SpecialCharForQuery(char* buffer) 
 * 
 * Discription : Remove the special characters from the word 
 *				After removing the special characters, the word includes only Hangle, periods(.), parentheses(())
 *				This fuction is needed Because the Query by Web site is including the parentheses	
 * 
 * Returns : eNOERROR if successfully special characters removed 
 */

int Remove_SpecialCharForQuery( char* buffer)	/* IN	pointer to remove special chacters	*/
{
		
	unsigned char * ptr_buffer = (unsigned char*)buffer;	/* pointer to point the buffer	*/ 
		
	while(*ptr_buffer)
	{
		/* In case of Hangle */
		if(*ptr_buffer >= 0xb0 && *ptr_buffer <= 0xc8 && *(ptr_buffer+1) >= 0xa1 && *(ptr_buffer+1) <= 0xfe) 
			ptr_buffer += 2;
		/* In case of Hanja */
		else if(*ptr_buffer >= 0xca && *ptr_buffer <=0xfd && *(ptr_buffer+1) >= 0xa1 && *(ptr_buffer+1) <= 0xfe ) 
		{
			ConvertHanjaToHangle( (char*) ptr_buffer);
			ptr_buffer += 2;
		}
		/* Others, a 2-byte special is replaced with space(' ') */
		else if(*ptr_buffer & 0x80)  
		{
			*ptr_buffer++ = ' ';
			*ptr_buffer++ = ' ';
		}	
		/* In case of lowercase letter in English */		
		else if(*ptr_buffer >= 'a' && *ptr_buffer <= 'z') 
			ptr_buffer++;
		/* In case of a capital letter in English */
		else if(*ptr_buffer >= 'A' && *ptr_buffer <= 'Z') 
		{
			*ptr_buffer = (*ptr_buffer)+('a'-'A');
			 ptr_buffer++;
		}
		/* In case of number */
		else if(*ptr_buffer >= '0' && *ptr_buffer <= '9') 
			 ptr_buffer++;
		else if(*ptr_buffer == '*')
			*ptr_buffer++ ='.' ;
		else if(*ptr_buffer == '(' || *ptr_buffer == ')')  
			 ptr_buffer++;
		/* Others, a 1-byte special character is replaced */
		else	*ptr_buffer++ = ' '; 		
		
	}
	
	return eNOERROR;	
	
}


/*
 * Function : int Remove_SpecialCharForHighlight(char* buffer) 
 * 
 * Discription : Remove the special characters from the word 
 *				After removing the special characters, the word includes only Hangle, periods(.), asterisk(*)
 *				This fuction is needed because the highligt keywords should contain asterisk
 * 
 * Returns : eNOERROR if successfully special characters removed 
 */

int Remove_SpecialCharForHighlight(char* buffer)	/* IN	pointer to remove special characters 	*/	
{
		
	unsigned char * ptr_buffer = (unsigned char*)buffer;	/* pointer to point buffer	*/ 
		
	while(*ptr_buffer)
	{
		/* In case of Hangle */
		if(*ptr_buffer >= 0xb0 && *ptr_buffer <= 0xc8 && *(ptr_buffer+1) >= 0xa1 && *(ptr_buffer+1) <= 0xfe) 
			ptr_buffer += 2;
		/* In case of Hanja */
		else if(*ptr_buffer >= 0xca && *ptr_buffer <=0xfd && *(ptr_buffer+1) >= 0xa1 && *(ptr_buffer+1) <= 0xfe ) 
		{
			ConvertHanjaToHangle( (char*) ptr_buffer);
			ptr_buffer += 2;
		}
		/* Others, a 2-byte special is replaced with space(' ') */
		else if(*ptr_buffer & 0x80)  
		{
			*ptr_buffer++ = ' ';
			*ptr_buffer++ = ' ';
		}	
		/* In case of lowercase letter in English */		
		else if(*ptr_buffer >= 'a' && *ptr_buffer <= 'z') 
			ptr_buffer++;
		/* In case of a capital letter in English */
		else if(*ptr_buffer >= 'A' && *ptr_buffer <= 'Z') 
		{
			*ptr_buffer = (*ptr_buffer)+('a'-'A');
			 ptr_buffer++;
		}
		/* In case of number */
		else if(*ptr_buffer >= '0' && *ptr_buffer <= '9') 
			 ptr_buffer++;
		else if(*ptr_buffer == '*')  
			 ptr_buffer++;
		/* Others, a 1-byte special character is replaced */
		else	*ptr_buffer++ = ' '; 		
		
	}
	
	return eNOERROR;	
	
}



/*
 * Function : int IsInvisible(char ch) 
 * 
 * Discription : Remove the special characters from the word 
 * 
 * Return : TRUE if the character is visible character
 *			False if the character is invisible character
 */

int IsInvisible(char ch) /* IN	input character	*/
{
	if(ch >= 1 && ch <= 27)
		return TRUE;
	else
		return FALSE;
}


/*
 * Function : void EliminateInvisibleChar(char* str) 
 * 
 * Discription : Remove the invisible characters from the word 
 * 
 * Return : Nothing 
 */

void EliminateInvisibleChar(char* str)	/* IN	pointer to eliminate the invisible characters	*/
{
	while(*str)
	{
		if((unsigned char)*str > (unsigned char)0x80)
			str += 2;
		else if(IsInvisible(*str))
			*str = ' ';
		else
			str ++;
	}
}


/*
 * Function : void ConvertSymbolCharToSpace(char* str) 
 * 
 * Discription : Replace the invisible characters with spaces 
 * 
 * Return : Nothing 
 */

void ConvertSymbolCharToSpace(char* str)	/*	IN	pointer to replace symbol characters into spaces	*/
{
	while(*str)
	{
		if((unsigned char)*str == (unsigned char)0xa1 && (unsigned char)*(str + 1) == (unsigned char)0xa1)
		{
			*str = ' ';
			*(str + 1) = ' ';
			str += 2;
		}
		else if((unsigned char)*str == (unsigned char)0xa1 && (unsigned char)*(str + 1) == (unsigned char)0xa4)
		{
			*str = ' ';
			*(str + 1) = ' ';
			str += 2;
		}
		else if((unsigned char)*str == (unsigned char)0xa1 && (unsigned char)*(str + 1) == (unsigned char)0xde)
		{
			*str = ' ';
			*(str + 1) = ' ';
			str += 2;
		}
		else if((unsigned char)*str == (unsigned char)0xa1 && (unsigned char)*(str + 1) == (unsigned char)0xe2)
		{
			*str = ' ';
			*(str + 1) = ' ';
			str += 2;
		}
		else if((unsigned char)*str == (unsigned char)0xa1 && (unsigned char)*(str + 1) == (unsigned char)0xe3)
		{
			*str = ' ';
			*(str + 1) = ' ';
			str += 2;
		}
		else if((unsigned char)*str == (unsigned char)0xa1 && (unsigned char)*(str + 1) == (unsigned char)0xe4)
		{
			*str = ' ';
			*(str + 1) = ' ';
			str += 2;
		}
		else if((unsigned char)*str > (unsigned char)0x80)
		{
			str += 2;
		}
		else if(isalpha(*str) || isdigit(*str) || *str == '.')
			str ++;
		else
		{
			*str = ' ';
			str ++;
		}
	}
}


/*
 * Function : void ConvertSymbolCharExceptAsteriskToSpace(char* str) 
 * 
 * Discription : Replace the symbol characters except asterisk with spaces 
 * 
 * Return : Nothing 
 */

void ConvertSymbolCharExceptAsteriskToSpace(char* str)	/*	IN	pointer to replace symbol characters into spaces	*/
{
	while(*str)
	{
		if((unsigned char)*str == (unsigned char)0xa1 && (unsigned char)*(str + 1) == (unsigned char)0xa1)
		{
			*str = ' ';
			*(str + 1) = ' ';
			str += 2;
		}
		else if((unsigned char)*str == (unsigned char)0xa1 && (unsigned char)*(str + 1) == (unsigned char)0xa4)
		{
			*str = ' ';
			*(str + 1) = ' ';
			str += 2;
		}
		else if((unsigned char)*str == (unsigned char)0xa1 && (unsigned char)*(str + 1) == (unsigned char)0xde)
		{
			*str = ' ';
			*(str + 1) = ' ';
			str += 2;
		}
		else if((unsigned char)*str == (unsigned char)0xa1 && (unsigned char)*(str + 1) == (unsigned char)0xe2)
		{
			*str = ' ';
			*(str + 1) = ' ';
			str += 2;
		}
		else if((unsigned char)*str == (unsigned char)0xa1 && (unsigned char)*(str + 1) == (unsigned char)0xe3)
		{
			*str = ' ';
			*(str + 1) = ' ';
			str += 2;
		}
		else if((unsigned char)*str == (unsigned char)0xa1 && (unsigned char)*(str + 1) == (unsigned char)0xe4)
		{
			*str = ' ';
			*(str + 1) = ' ';
			str += 2;
		}
		else if((unsigned char)*str > (unsigned char)0x80)
		{
			str += 2;
		}
		else if(isalpha(*str) || isdigit(*str) || *str == '.' || *str == '*')
			str ++;
		else
		{
			*str = ' ';
			str ++;
		}
	}
}

void ConvertAsteriskSymbolCharToSpace(char* str)	/*	INOUT	pointer to replace symbol characters into spaces	*/
{
	while(*str)
	{
		if(*str == '*')
		{
			*str = ' ';
			str ++;
		}
		else
		{
			str ++;
		}
	}
}

/*
 * Function : void EliminateTooLongKeyword(char* str) 
 * 
 * Discription : Replace the too long keyword into the space 
 * 
 * Return : Nothing 
 */

void EliminateTooLongKeyword(char* str)	/* IN 	pointer to eliminate the too long keywords	*/	
{
	char* prevSpace = str - 1;
	char* word;
	
	while(*str)
	{
		if(*str==' ')
		{
			if((str-prevSpace) > TOOLONGKEYWORDLEN)
			{
				for(word = prevSpace + 1; word<str; word++)
					*word = ' ';
			}
			
			prevSpace = str;				
		}
		str++;
	}
	
	if((str-prevSpace) > TOOLONGKEYWORDLEN)
	{
		for(word = prevSpace+1; word<str; word++)
			*word = ' ';
	}
	
}



/*
 * Function : int Read_Sentence( char* buffer, char* sentence, Four str_idx_buffer) 
 * 
 * Discription : Read the one sentence from the buffer and Memorize the buffer position of last read sentence position
 *				 The standard of the one sentence is a period(.)  
 * 
 * Return : The index of next reading potision in the buffer  
 */

int Read_Sentence( 
	char* 	buffer,  		  /* IN		pointer to be read one sentence   */
	char* 	sentence,		 /* OUT	pointer to be stored from the buffer   */
	Four 	str_idx_buffer )  /* IN		index of start point to read buffer  */
{
	
	unsigned char *ptr_buffer = (unsigned char*)&buffer[str_idx_buffer];  /* pointer variable to record the start of the buffer */
	unsigned char *ptr_sentence = (unsigned char*) sentence; /* pointer variable to the sentence */
	unsigned char *ptr_str_sentence = (unsigned char*)sentence; /* pointer variable to check the segment fault */
	unsigned char *ptr_tmp_sentence; 
	unsigned char read_onechar; 
	
	Four sentence_len = 0; 		/* length of sentence */
	Four notPerfactFlag = 0;	/* flag variable to check whether the sentence is recored correctly or not*/
	Four numOfFirstSpace = 0;   /* number of space(' ') in the start of the buffer */
		
	memset(sentence, 0, MAXSENTENCESIZE);
	read_onechar = *ptr_buffer++;

	if(read_onechar=='\0') return ENDOFBUFFER;

	/* count the space to count the total read byte */
	while(isspace(read_onechar) )   
	{
		read_onechar = *ptr_buffer++;
		numOfFirstSpace++;
	}

	while( read_onechar != '.' )
	{
		/* Assumption : Hanja is replaced with proper Hangle in the RemoveSpecialChar()*/
		/* In case of Hangle */		
		if(read_onechar & 0x80)  
		{
			/* check the improper recording the sentence */
			if(sentence_len >= MAXSENTENCESIZE-2) 
			{
				notPerfactFlag = 1;
				break;
			}
			else 
			{	
				*ptr_sentence++ = read_onechar;
				read_onechar = *ptr_buffer++;
				*ptr_sentence++ = read_onechar;
				sentence_len += 2;	
			}
		}
		/* In case of 1-byte character */
		else	
		{
			/* check the improper recording the sentence */
			if(sentence_len >= MAXSENTENCESIZE-1)
			{	
				notPerfactFlag = 1;
				break;
			}
			else
			{
				*ptr_sentence++ = read_onechar;
				sentence_len++;	
			}
		}
		
		read_onechar = *ptr_buffer++;
		
	} /* record the sentence until a period(.) */
	
	

	/* when a sentence not fit in the buffer is too long */
	if(notPerfactFlag) 
	{
		/* check if a word not fit in the buffer is too long or not */
		ptr_tmp_sentence = ptr_sentence - 1 ;
		
		while( *ptr_tmp_sentence != ' ') 
		{
			ptr_tmp_sentence--;
			
			if(ptr_tmp_sentence == ptr_str_sentence)
				break;
		}	
		
		/* if a word is longer than 512 gulja(1024-byte), cut the word fit in 512 gulja */
		/* Because the maximum keyword size in OOSQL is 1024 byte */
		if( ptr_tmp_sentence == ptr_str_sentence)  
		{			
			*ptr_sentence = '\0';
			return strlen((char*)sentence)+ numOfFirstSpace;
		}
		else
		{			
			*ptr_tmp_sentence = '\0';	
			return strlen((char*)sentence)+ numOfFirstSpace;
		}
			
	}  
	/* when a sentence is properly read from the buffer */
	else
	{
		/* insert a period('.'), it is used to check the sentence */
		*ptr_sentence++ = read_onechar;
		sentence_len++;	
	}
	
	*ptr_sentence = '\0';		

	
	/* return the read Byte not sentence length */		
	return strlen((char*)sentence)+ numOfFirstSpace;

}



/*
 * Function : int Read_Word( char* sentence, char* word, Four str_idx_sentence) 
 * 
 * Discription : Read the one word from the sentence and Memorize the sentence position of last read sentence position
 *				 The standard of the one sentence is a space(' ')  
 * 
 * Return : The index of next reading potision in the sentence  
 */
int Read_Word( 
	char*	sentence, 					/*  IN	pointer to read one word	 */
	char*	word, 						/*  OUT	pointer to store one word from the sentence  */
	Four 	str_idx_sentence )			/*  IN 	index of start point to read sentence  */
{
	unsigned char *ptr_sentence = (unsigned char*)&sentence[str_idx_sentence]; /* pointer variable to record the start of the sentence */
	unsigned char *ptr_word = (unsigned char*) word;  							/* pointer variable to the word */
	unsigned char *ptr_str_word = (unsigned char*)word;	  						/* pointer variable to check the segment fault */
	unsigned char  read_onechar;												/* temporary variable to store the one character from the sentence	*/	
	
	Four numOfFirstSpace = 0;			/* temporary variables	*/
	Four notPerfactFlag = 0;
	Four word_len = 0;
	

	
	memset(word, 0, MAXWORDSIZE);
	read_onechar = *ptr_sentence++;

	if(read_onechar=='\0') return ENDOFSENTENCE;

	/* count the space to count the total read byte */
	while(isspace(read_onechar))   
	{
		read_onechar = *ptr_sentence++;
		numOfFirstSpace++;
	}
	
	while( read_onechar != ' ' )
	{
		/* read the word from the sentence, its standard is space (' ')	*/
		/* In case of Hangle */
		if(read_onechar & 0x80)  
		{
			if(word_len >= MAXWORDSIZE-2) 
			{
				notPerfactFlag = 1;
				break;
			}
			else 
			{	
				*ptr_word++ = read_onechar;
				read_onechar = *ptr_sentence++;
				*ptr_word++ = read_onechar;
				word_len += 2;	
			}
		}
		/* In case of 1-byte character */
		else	
		{
			/* check the improper recording the word */
			if(word_len >= MAXWORDSIZE-1)
			{	
				notPerfactFlag = 1;
				break;
			}
			else
			{
				*ptr_word++ = read_onechar;
				word_len++;	
			}
		}
		
		read_onechar = *ptr_sentence++;
		
	}  /* record the sentence until a space(' ') */
	
	
	/* when a word not fit in the sentence is too long */
	if(notPerfactFlag) 
	{
			*ptr_word = '\0';
			return strlen((char*)word)+ numOfFirstSpace;			
	}  
	/* when a word is properly read from the sentence */
	else
	{
		/* insert a period(' '), it is used to check the word */
		*ptr_word++ = read_onechar;   
		word_len++;	
	}
	
	*ptr_word = '\0';		
	
	/* return the read Byte not word length */						
	return strlen((char*)word)+ numOfFirstSpace;		
	
}


/*
 * Function : int IsInvisibleChar(char* ptr_buffer)
 *
 * Description : Read the character and decide whether the character is invisible or not	 
 *
 * Return :	TRUE if the character is invisible 
 *			FALSE if the caracter is not invisible
 */

int IsInvisibleChar(char* ptr_buffer)	/* IN	pointer to decide whether the character is invisible or not	*/ 
{
	if(*ptr_buffer>='0' && *ptr_buffer<='9') return FALSE;
	else if(*ptr_buffer>='A' && *ptr_buffer <='Z') return FALSE;
	else if(*ptr_buffer>='a' && *ptr_buffer <='z') return FALSE;
	else if(*ptr_buffer == EOF ) return FALSE;
	else return TRUE;
}


/*
 * Function : int IsWordKorean(char* word)
 *
 * Description : Read the word and decide whether the word is Korean or not 
 *				( English, English + Number, Hangle + Number ) 	 
 *
 * Return :	TRUE if the character is Hangle 
 *			FALSE if the caracter is not Hangle 
 */

int IsWordKorean( char* word )	/* IN	pointer to decide whether the word is pure Hangle or not	*/ 
{
	unsigned char* ptr_word = (unsigned char*) word;
	
	while( *ptr_word != '\0')	
	{
		if(!(*ptr_word & 0x80)) return FALSE;
		ptr_word++;
	}
			
	return TRUE;			
	
}


/*
 * Function : int IsWordEnglish(char* word)
 *
 * Description : Read the word and decide whether the word is English or not 
 *				( Hangle, English + Number, Hangle + Number ) 	 
 *
 * Return :	TRUE if the character is English 
 *			FALSE if the caracter is not English 
 */

int IsWordEnglish( char* word)	/* IN	pointer to decide whether the word is a pure English or not	*/
{
	
	unsigned char* ptr_word = (unsigned char*) word;
	
	while( *ptr_word != '\0')
	{
		if(!isalpha(*ptr_word))	return FALSE;
		ptr_word++;
	}
	
	return TRUE;
}




/***********************************/
/* Functions about saving keywords */
/***********************************/


/* Function : Four strcmp2( unsigned char* str1, unsigned char* str2)
 *
 * Description : Compare two strings and return the result 
 *
 * Return : TRUE if the two strings are same
 *			FALSE if the tow strings are not same
 */

Four strcmp2( 
				unsigned char* str1,		/* IN	the first string  to be compared	*/
				unsigned char* str2 )		/* IN	the second string to be compared	*/
{
	Four len;							/* temporary variable to store the string length	*/
	
	len = (strlen((const char*) str1) < strlen((const char*)str2)) ? strlen((const char*)str1)+1: strlen((const char*)str2)+1;

	/* return the comparison result	*/
	return memcmp(str1, str2, len);
	
}


/* Function	: Four InsertKeyword(Four handle, unsigned char* word, Fou sentence, Four noun)
 *
 * Description : Store the word with sentence and noun information into the keyword buffer given by handle
 *
 * Return : eNOERROR if there is no error
 *			exit fi there is some errors
 */

Four InsertKeyword(
	Four handle,			/* IN	handle number to store the keywrod	*/ 
	unsigned char* word,	/* IN	keyword to store	*/  
	Four sentence,			/* IN   sentence position of the word	*/ 
	Four noun )				/* IN	noun position of the word	*/
{
	Four idx;						/* temporary variable of keyword buffer index	*/
	KeywordBuffer* keywordBuffer;	/* pointer to store the keyword into the keyword buffer	*/ 
	unsigned char* tmpBuffer;		/* pointer to store the keyword	*/
	unsigned char* keyword;			/* pointer to store the keyword	*/
	Four nPositions;				/* temporary variable to store the number of the noun positions	*/
	Four maxPositionNum;			/* temporary variable to store the max. number of the allocated noun positions */
	Four memSize;
	
	Position* positionList;			/* pointer to index of the positions	*/
	

	/* allocate the buffer to store the keywword	*/
	tmpBuffer = (unsigned char *)QuickFitMM_Alloc(&dynamicMMHandle, strlen((char*)word)*sizeof(unsigned char)+1);

	if(tmpBuffer==NULL)
	{
		fprintf(stderr,"QuickFitMM_Alloc Error in InsertKeyword\n");
		exit(-1);
	}


	/* insert the keyword	*/
	memcpy((char*)tmpBuffer, (char*)word, strlen((char*)word)+1);

	idx = korKeywordExtractorHandle[handle].nKeywords;
	keywordBuffer = korKeywordExtractorHandle[handle].keywordBuffer;
	
	keywordBuffer[idx].keyword = tmpBuffer;
	
	
	memSize = sizeof(Position) * MAXNUMOFPOSITIONS;

	/* allocate the memory of position and update the position information	*/  
	
	keywordBuffer[idx].positionList = (Position*)QuickFitMM_Alloc(&dynamicMMHandle, memSize);

	if(keywordBuffer[idx].positionList==NULL)
	{
		fprintf(stderr,"QuickFitMM_Alloc Error in InsertKeyword\n");
		exit(-1);
	}

	memset(keywordBuffer[idx].positionList, 0 , memSize);  
	keywordBuffer[idx].maxPositionNum = MAXNUMOFPOSITIONS;

	
	keywordBuffer[idx].positionList[0].sentence = sentence;
	keywordBuffer[idx].positionList[0].noun = noun;
	keywordBuffer[idx].nPositions = 1;
	
	korKeywordExtractorHandle[handle].nKeywords++;


	/* When we make n-gram keywords for query, we do not sort the keywords.*/
	if (korKeywordExtractorHandle[handle].ngramQueryMode == FALSE)
	{
		/* sort the keyword in the handle, because the keywords are kept in sorted state	*/
		keyword = keywordBuffer[idx].keyword;
		nPositions = keywordBuffer[idx].nPositions;
		positionList = keywordBuffer[idx].positionList;
		maxPositionNum = keywordBuffer[idx].maxPositionNum;
		
		while(idx>0)
		{
			if(strcmp2(keyword, keywordBuffer[idx-1].keyword) > 0)
				break;
			else
			{
				idx--;
				keywordBuffer[idx+1].keyword = keywordBuffer[idx].keyword;
				keywordBuffer[idx+1].nPositions = keywordBuffer[idx].nPositions;
				keywordBuffer[idx+1].positionList = keywordBuffer[idx].positionList;
				keywordBuffer[idx+1].maxPositionNum = keywordBuffer[idx].maxPositionNum;
				
			}
					
		}
		
		keywordBuffer[idx].keyword = keyword;
		keywordBuffer[idx].nPositions = nPositions;
		keywordBuffer[idx].positionList = positionList;
		keywordBuffer[idx].maxPositionNum = maxPositionNum;
	}

#ifndef SGKIM_DEBUG	
	fprintf(stderr,"k: \n%s", keyword);
#endif
	
	return eNOERROR;
	
}


/* Function : Four InsertPosition(Four handle, Four idx, unsigned char* word, Four sentence, Four noun)
 *
 * Description : find the index accoring with keyword and insert the position information
 *
 * Return : eNOERROR if there is no error
 *			exit if there is some error
 */

Four InsertPosition(
	Four handle,			/* IN	handle number to input the potision information	*/ 
	Four idx, 				/* IN	the index of keyword buffer to insert position information	*/
	unsigned char* word,	/* IN	the word of the keyword	*/ 
	Four sentence,			/* IN	the sentence information of the position	*/ 
	Four noun )				/* IN	the noun information of the position	*/
{
	Four e;					/* error code	*/
	Four memSize;			/* temporary variable to store the memory size	*/
	Four maxPositionNum;	/* temporary variable to store the max number of positions	*/
	Four nPositions;		/* temporary variable to store the number of positions	*/
	
	KeywordBuffer* keywordBuffer;	/* pointer of KeywordBuffer	*/
	Position* positionList;			/* pointer of Position	*/
	

	/* get the max number of positions and the number of positions according with the handle	*/	
	maxPositionNum = korKeywordExtractorHandle[handle].keywordBuffer[idx].maxPositionNum;
	nPositions = korKeywordExtractorHandle[handle].keywordBuffer[idx].nPositions;


	/* if the memory is smaller than allocated one, Double the memory	*/	
	if (nPositions >= maxPositionNum) 
	{
		/* Doubling keyword buffer */
		memSize = sizeof(Position) * maxPositionNum * 2;
				
		e = QuickFitMM_ReAlloc(&dynamicMMHandle, (void**)&korKeywordExtractorHandle[handle].keywordBuffer[idx].positionList, memSize);
		if (e < 0) QUICKFITMM_ERROR(e);

		memset(&korKeywordExtractorHandle[handle].keywordBuffer[idx].positionList[maxPositionNum], 0, maxPositionNum);

		korKeywordExtractorHandle[handle].keywordBuffer[idx].maxPositionNum = maxPositionNum*2;
	
	}

	/* insert the potision information	*/	
	keywordBuffer = korKeywordExtractorHandle[handle].keywordBuffer;	
	positionList = keywordBuffer[idx].positionList;
	
	positionList[nPositions].sentence = sentence;
	positionList[nPositions].noun = noun;
		
	keywordBuffer[idx].nPositions++;			
	
	
	return eNOERROR;
	
}


/* Function Four SearchKeyword(Four handle, unsigned char* word)
 *
 * Description : find whether the keyword is in the keyword buffer or not
 *				 if the keyword is in the keyword buffer, return the index number
 *				 or not, return NOTFOUND  
 *
 * Return :	the index number if the keyword is in the keyword buffer
 *			the NOTFOUND if the keyword is not in the keyword buffer
 */

Four SearchKeyword(
	Four handle, 			/* IN	handle number to search keyword	*/
	unsigned char* word )	/* IN	pointer of the word	*/
{
	Four m;														/* temporary variable to store the middle position	*/ 
	Four l=0;													/* temporary variable to store the lower position 	*/ 
	Four u = korKeywordExtractorHandle[handle].nKeywords-1;		/* temporary variable to store the uppoer position	*/
		
	Four result;												/* temporary variable to store whether the word is in the keyword buffer or not	*/


	/* if the word is not in keyword buffer return NOTFOUND ,or not return the index	*/
	if(u==0) return NOTFOUND;
	
	while(l<=u)
	{
		m=(l+u)/2;
		
		result = strcmp2(korKeywordExtractorHandle[handle].keywordBuffer[m].keyword, word);
		
		if(result == 0 ) return m;
		else if(result <0) l = m + 1;
		else u = m - 1;
	}
	
	return NOTFOUND;
	
}


/* Function : Four StoreKeyword(Four handle, unsigned char* word, Four sentence, Four noun)
 *
 * Description : store the keyword according with the handle properly 
 *
 * Return : eNOERROR if there is no error
 */

Four StoreKeyword(
	Four handle,			/* IN	handle number to store the keyword	*/
	unsigned char* word, 	/* IN	pointer to store the word	*/	
	Four sentence,			/* IN	the sentence information of the word	*/  
	Four noun )				/* IN	the noun information of the word	*/
{
	Four e;						/* error code	*/
	Four i;						/* temporary variable	*/
	Four idx;					/* temporary variable to store the index	*/
	Four memSize;				/* temporary variable to store the memory size	*/
	Four maxKeywordNum;			/* temporary variable to store the max number of the keywords	*/
	
	KeywordBuffer* keywordBuffer;	/* pointer of KeywordBuffer	*/


	maxKeywordNum = korKeywordExtractorHandle[handle].maxKeywordNum; 

	/* if the memory is smaller than allocated one, Double the memory	*/ 
	if (korKeywordExtractorHandle[handle].nKeywords >= maxKeywordNum) 
	{
		/* Doubling keyword buffer */
		memSize = sizeof(KeywordBuffer) * maxKeywordNum * 2;
		e = QuickFitMM_ReAlloc(&dynamicMMHandle, (void**)&korKeywordExtractorHandle[handle].keywordBuffer, memSize);
		if (e < 0)  QUICKFITMM_ERROR(e);

		/* Initialize keyword buffer */ 
		memset(&korKeywordExtractorHandle[handle].keywordBuffer[maxKeywordNum], 0, maxKeywordNum);

		korKeywordExtractorHandle[handle].maxKeywordNum = maxKeywordNum*2; 
		
		
	}

#ifndef SGKIM_DEBUG	
	fprintf(stderr,"k: \n%s", word);
#endif	

	if (korKeywordExtractorHandle[handle].ngramQueryMode == FALSE)
	{

		/* if the word already in the keyword buffer, store the position inforation of the word
		   or not, store the keyword and the postion information	*/
		
		idx = SearchKeyword(handle,word);
			
		if(idx == NOTFOUND)
			InsertKeyword(handle, word, sentence, noun);	
		else
			InsertPosition(handle, idx, word, sentence, noun);
	}
	else
	{
		/* When we make n-gram keywords for query, 
		 * we store keyword even if the word is already in the keyword buffer.
		 * Duplication is needed.
		 */
		InsertKeyword(handle, word, sentence, noun);	
	}
		
		
	return eNOERROR;
		
}





/*************************************/
/*	Functions about the handle	 */
/*************************************/


/* Function : int FindFirstFreeHandle(void)
 * 
 * Description : find the first free handle 
 *				if the rest handle doesn't exist, double the handle
 *
 * Return : handle number   
 */

int FindFirstFreeHandle(void)
{
	Four			i;
	Four			e;
	Four			memSize;


	/* if the allocated handle is smaller than allocated one, Double the handle	*/

	if (nHandle >= maxHandleNum)
	{
		memSize = sizeof(KorKeywordExtractorHandle) * maxHandleNum * 2;
		e = QuickFitMM_ReAlloc(&staticMMHandle, (void**)&korKeywordExtractorHandle, memSize); 
		if (e < 0)	QUICKFITMM_ERROR(e);

		/* initialize isUsed field of korKeywordExtractorHandle array */
		for (i = maxHandleNum; i < maxHandleNum*2; i++)
		{
			korKeywordExtractorHandle[i].isUsed  		= FALSE;
			/* default value of ngramQueryMode is FALSE */
			korKeywordExtractorHandle[i].ngramQueryMode= FALSE;
			korKeywordExtractorHandle[i].readBuffer  	= NULL;
			korKeywordExtractorHandle[i].keywordBuffer  = NULL;
			korKeywordExtractorHandle[i].maxKeywordNum	= 0;
			korKeywordExtractorHandle[i].nKeywords 		= 0;
			korKeywordExtractorHandle[i].keywordOffset  = 0;
		}

		maxHandleNum = maxHandleNum * 2;
	}

	/* find the first free handle and then return handle number	*/
	for (i = 0; i < maxHandleNum; i++)
	{	
		if (korKeywordExtractorHandle[i].isUsed == FALSE)
			break;
	}


	return i;
}


/* Function : int InitKorKeywordExtractor(void)
 *
 * Description : initialize the memory handle and then intialize the handle buffer  
 *
 * Return : eNOERROR if there is no error
 *			exit if there is some errors
 */

int InitKorKeywordExtractor(void)
{
	
	Four i;
	Four e;
	Four memSize;
	
	
	/* initialize the memory handle */
	e = QuickFitMM_Init(&staticMMHandle, MAXBUFFERSIZE);
	if (e < 0)	QUICKFITMM_ERROR(e);

	e = QuickFitMM_Init(&dynamicMMHandle, MAXBUFFERSIZE);
	if (e < 0)	QUICKFITMM_ERROR(e);

	/* alloc memory for korKeywordExtractorHandle */
	memSize = sizeof(KorKeywordExtractorHandle) * MAXNUMOFHANDLES;
	korKeywordExtractorHandle = (KorKeywordExtractorHandle*)QuickFitMM_Alloc(&staticMMHandle, memSize); 


	/* if there is some error, exit	*/
	if(korKeywordExtractorHandle==NULL)
	{
		fprintf(stderr,"QuickFitMM_Alloc Error in InitKorKeywordExtractor\n");
		exit(-1);
	}


	/* initialize isUsed field of korKeywordExtractorHandle array */
	for (i = 0; i < MAXNUMOFHANDLES; i++)
	{
		korKeywordExtractorHandle[i].isUsed  		= FALSE;
		/* default value of ngramQueryMode is FALSE */
		korKeywordExtractorHandle[i].ngramQueryMode= FALSE;
		korKeywordExtractorHandle[i].readBuffer  	= NULL;
		korKeywordExtractorHandle[i].readBufferSize	= 0;
		korKeywordExtractorHandle[i].nKeywords 		= 0;
		korKeywordExtractorHandle[i].keywordOffset  = 0;
	}
	
	maxHandleNum = MAXNUMOFHANDLES;			
				
	return eNOERROR;	
	
}


/* Function : InitDictionaryHashTable(void)
 * 
 * Description : Initialize the Josa, Eomi, Stopword, one-gulja useful noun, one-fulja pronoun dictionary 
 *
 * Return : eNOERROR if there is no error
 * 			error code if there is file open error and dictionary allcation error
 *			exit if there is the file close errors
 */

int InitDictionaryHashTable(void)
{

	FILE *fp_josafile;				/*  pointer variable to the josa dictionary file	*/
	FILE *fp_josafile2;				/*  pointer variable to the josa dictionary file	*/
	FILE *fp_eomifile;				/*  pointer variable to the eomi dictionary file	*/
	FILE *fp_korstopwordfile;		/*  pointer variable to the Korean stopword dictionary file	*/
	FILE *fp_engstopwordfile;	 	/*  pointer variable to the English stopword dictionary file	*/
	FILE *fp_onenounfile;		 	/*  pointer variable to the one noun word  dictionary file	*/
	FILE *fp_onepronounfile;	 	/*  pointer variable to the one pronoun word  dictionary file	*/
	FILE *fp_specialnounfile1;	 	/*  pointer variable to the specialnoun dictionary file1	*/
	FILE *fp_specialnounfile2;	 	/*  pointer variable to the specialnoun dictionary file2	*/


	char tmpIRPath[200];											/* temporary variable to store the IR path	*/
	char tmpJosaFilePath[200], tmpEomiFilePath[200];				/* temporary variable to store the JOSA and EOMI path	*/
	char tmpJosaFilePath2[200];				/* temporary variable to store the JOSA2 path	*/
	char tmpKorStopwordFilePath[200], tmpEngStopwordFilePath[200];	/* temporary variable to store the STOPWORD path	*/
	char tmpOnenounFilePath[200];									/* temporary variable to store the ONENOUN path	*/
	char tmpOnepronounFilePath[200];								/* temporary variable to store the ONEPRONOUN path	*/
	char tmpSpecialnounFile1Path[200];								/* temporary variable to store the SPECIALNOUN_FILE1 path	*/
	char tmpSpecialnounFile2Path[200];								/* temporary variable to store the SPECIALNOUN_FILE2 path	*/



	/* get the IR_SYSTEM_PARH and the make the proper file path	to open dictionary files	*/

	if(getenv("IR_SYSTEM_PATH") == NULL)
	{
		printf("IR_SYSTEM_PATH is not defined\nTerminate system\n");
		exit(1);
	}
  else
	{
			strcpy(tmpIRPath,getenv("IR_SYSTEM_PATH"));

#ifdef WIN32
	strcat(tmpIRPath,"\\");
#else
	strcat(tmpIRPath,"/");
#endif

			strcpy(tmpJosaFilePath, tmpIRPath);
			strcpy(tmpEomiFilePath, tmpIRPath);
			strcpy(tmpKorStopwordFilePath, tmpIRPath);
			strcpy(tmpEngStopwordFilePath, tmpIRPath);
			strcpy(tmpOnenounFilePath, tmpIRPath);
			strcpy(tmpOnepronounFilePath, tmpIRPath);
			strcpy(tmpSpecialnounFile1Path, tmpIRPath);
			strcpy(tmpSpecialnounFile2Path, tmpIRPath);

			strcat(tmpJosaFilePath,JOSAFILE);
			strcat(tmpEomiFilePath,EOMIFILE);
			strcat(tmpKorStopwordFilePath,KORSTOPWORDFILE);
			strcat(tmpEngStopwordFilePath,ENGSTOPWORDFILE);
			strcat(tmpOnenounFilePath,ONENOUNFILE);
			strcat(tmpOnepronounFilePath,ONEPRONOUNFILE);
			strcat(tmpSpecialnounFile1Path,SPECIALNOUNFILE1);
			strcat(tmpSpecialnounFile2Path,SPECIALNOUNFILE2);

	}
	
	/* open the dictionary files */		
	
	if( (fp_josafile=fopen(tmpJosaFilePath, "rb")) == NULL)
	{
		fprintf( stderr,"\nJosa File open error...\n");
		return eFILEOPEN;
	}

	if( (fp_eomifile=fopen(tmpEomiFilePath, "rb")) == NULL)
	{
		fprintf( stderr,"\nEomi File open error...\n");
		return eFILEOPEN;
	}
	
	if( (fp_korstopwordfile=fopen(tmpKorStopwordFilePath, "rb")) == NULL)
	{
		fprintf( stderr,"\nStopword File open error...\n");
		return eFILEOPEN;
	}
		
	if( (fp_engstopwordfile=fopen(tmpEngStopwordFilePath, "rb")) == NULL)
	{
		fprintf( stderr,"\nEnglish Stopword File open error...\n");
		return eFILEOPEN;
	}
		
	if( (fp_onenounfile=fopen(tmpOnenounFilePath, "rb")) == NULL)
	{
		fprintf( stderr,"\nOne noun File open error...\n");
		return eFILEOPEN;
	}
	
	if( (fp_onepronounfile=fopen(tmpOnepronounFilePath, "rb")) == NULL)
	{
		fprintf( stderr,"\nOne pronoun File open error...\n");
		return eFILEOPEN;
	}

	if( (fp_specialnounfile1=fopen(tmpSpecialnounFile1Path, "rb")) == NULL)
	{
		fprintf( stderr,"\nSpecialnoun File1 open error...\n");
		return eFILEOPEN;
	}

	if( (fp_specialnounfile2=fopen(tmpSpecialnounFile2Path, "rb")) == NULL)
	{
		fprintf( stderr,"\nSpecialnoun File1 open error...\n");
		return eFILEOPEN;
	}

	/* initialize the dictionary files to memory */
	
	if( eNOERROR==InitJosaEomiDic(fp_josafile, fp_eomifile)) ;
	else return eJOSAEOMIINIT;		

	if( eNOERROR==InitJosaDic(fp_josafile));
	else return eJOSAINIT;		
		
	if( eNOERROR==InitStopwordDic(fp_korstopwordfile, fp_engstopwordfile)) ;
	else return eSTOPWORDINIT;
		
	if( eNOERROR==InitOnenounDic(fp_onenounfile)) ;
	else return eONENOUNINIT;

	if( eNOERROR==InitOnepronounDic(fp_onepronounfile)) ;
	else return eONEPRONOUNINIT;
	
	if( eNOERROR==InitSpecialnounDic(fp_specialnounfile1, fp_specialnounfile2)) ;
	else return eSPECIALNOUNINIT;


	/* close open files */
	if ((fclose(fp_josafile)) == EOF)
	{
		fprintf(stderr, "Can't close josa file in KorKeywordExtractor_Init\n");
		exit(-1);
	}

	if ((fclose(fp_eomifile)) == EOF)
	{
		fprintf(stderr, "Can't close eomi file in KorKeywordExtractor_Init\n");
		exit(-1);
	}
		
	if ((fclose(fp_korstopwordfile)) == EOF)
	{
			fprintf(stderr, "Can't close stopword file in KorKeywordExtractor_Init\n");
			exit(-1);
	}
		
	if( fclose(fp_engstopwordfile) == EOF)
	{
		fprintf( stderr,"Can't close English stopword file in KorKeywordExtractor_Init\n");
		exit(-1);
	}	

	if( fclose(fp_onenounfile) == EOF)
	{
		fprintf( stderr,"Can't close one noun word file in KorKeywordExtractor_Init\n");
		exit(-1);
	}	

	if( fclose(fp_onepronounfile) == EOF)
	{
		fprintf( stderr,"Can't close one pronoun word file in KorKeywordExtractor_Init\n");
		exit(-1);
	}	

	if( fclose(fp_specialnounfile1) == EOF)
	{
		fprintf( stderr,"Can't close special noun file1 in KorKeywordExtractor_Init\n");
		exit(-1);
	}	

	if( fclose(fp_specialnounfile2) == EOF)
	{
		fprintf( stderr,"Can't close special noun file2 in KorKeywordExtractor_Init\n");
		exit(-1);
	}	

	return eNOERROR;	

	
}


/* Function : int InitKorKeyrwordExtractBuffer(Four handle)
 *
 * Description : Initialize the keyword buffer and position buffer according with the handle
 *
 * Return : eNOERROR if there is no error
 *			exit if there is some errors
 */

int InitKorKeywordExtractBuffer(Four handle)	/* IN	handle number	*/
{
	Four i;							/* temporary variable	*/
	Four memSize;					/* temporary variable to store the memory size	*/

	KeywordBuffer* keywordBuffer;	/* pointer of KeywordBuffer	*/ 
	

	/* update the korKeywordExtractorHandle information 
		and allocate the memory of read buffer and keyword buffer	*/

	korKeywordExtractorHandle[handle].isUsed = TRUE;
	/* default value of ngramQueryMode is FALSE*/
	korKeywordExtractorHandle[handle].ngramQueryMode= FALSE;
	korKeywordExtractorHandle[handle].readBuffer = (char *)QuickFitMM_Alloc(&dynamicMMHandle, MAXBUFFERSIZE);

	if( korKeywordExtractorHandle[handle].readBuffer==NULL)
	{
		fprintf(stderr, "QuickFitMM_Alloc Error in InitKorKeywordExtractBuffer\n");
		exit(-1);
	}

	korKeywordExtractorHandle[handle].readBufferSize = MAXBUFFERSIZE;


	memSize = sizeof(KeywordBuffer) * MAXNUMOFKEYWORDS;
	korKeywordExtractorHandle[handle].keywordBuffer = (KeywordBuffer*)QuickFitMM_Alloc(&dynamicMMHandle, memSize); 

	if( korKeywordExtractorHandle[handle].keywordBuffer==NULL)
	{
		fprintf(stderr, "QuickFitMM_Alloc Error in InitKorKeywordExtractBuffer\n");
		exit(-1);
	}
	
	memset(korKeywordExtractorHandle[handle].keywordBuffer, 0, memSize);
	
	korKeywordExtractorHandle[handle].maxKeywordNum = MAXNUMOFKEYWORDS;
	korKeywordExtractorHandle[handle].nKeywords 	= 0;
	korKeywordExtractorHandle[handle].keywordOffset = 0;
	
	return eNOERROR;
	
}


/* Function : void RemoveStopword(char* word)
 *
 * Description : set n-gram query mode. 
 *
 * Return : eCHANGENGRAMQUERYMODE, eNOERROR
 */


int setNgramQueryMode(Four handle, Boolean ngramQueryMode)
{
	if (korKeywordExtractorHandle[handle].nKeywords > 0)
	{
		fprintf(stderr, "Can not change n-gram query mode since there are keyword stored.\n");
		return eCHANGENGRAMQUERYMODE;
	}
	else
	{
		korKeywordExtractorHandle[handle].ngramQueryMode = ngramQueryMode;
		return eNOERROR;
	}
}


/********************************************/
/*	 Extract the keywords form memory		*/
/********************************************/

/* Function : void CheckAndAppendStopword(char* word, char* ptrToStopwords)
 *
 * Description : if the word is a stopword, the word would be the NULL
 *
 * Return : Nothing
 */
void CheckAndAppendStopword(
				char* word, 			/* IN   	pointer of the word */
				char* ptrToStopwords	/* IN/OUT	pointer of the stopwords	*/
				)
{

	if( FindStringInHashTable(Dic.StopwordTable,(char*)word) )
	{
		strcat(ptrToStopwords, " ");
		strcat(ptrToStopwords, word);
	}
	else ;
}


/* Function : void RemoveStopword(char* word)
 *
 * Description : if the word is a stopword, the word would be the NULL
 *
 * Return : Nothing
 */

void RemoveStopword(char* word)	/* IN	pointer of the word	*/
{

	Four result;	/* temporary variable to store whether the word is a stopword or not	*/

	result = FindStringInHashTable(Dic.StopwordTable,(char*)word);

	if(result==TRUE) word[0] = '\0';

}


/* Function : void RemoveJosaEomi(char* word)
 *
 * Description : if the word including the josa or eomi, remove the josa or eomi from the word 
 *
 * Return : Nothing
 */

void RemoveJosaEomi(char* word)	/* IN	pointer of the word	*/
{

	Four result;					/* temporary variable to store whether the word is including the josa or eomi or not	*/		
	Four i;							/* temporary variable 	*/
	Four wordLen = strlen(word);	/* temporary variable to store the word length	*/		

	if(wordLen >= 2)
	{
		for(i=0; i < wordLen; i+=2)
		{
			 result = FindStringInHashTable(Dic.JosaEomiTable,(char*)&word[i]);

			if(result==TRUE) break;

		}
	}

	/* if the word includes the josa or eomi, remove that	*/
	if(result==TRUE) word[i] = '\0'; 

}

/* Function : void RemoveJosa(char* word)
 *
 * Description : if the word including the josa, remove the josa from the word 
 *
 * Return : Nothing
 */

void RemoveJosa(char* word)	/* IN	pointer of the word	*/
{

	Four result;					/* temporary variable to store whether the word is including the josa or not	*/		
	Four i;							/* temporary variable 	*/
	Four wordLen = strlen(word);	/* temporary variable to store the word length	*/		

	i = 0;
	while (i < wordLen)
	{
		if (!isascii(word[i])) {
			result = FindStringInHashTable(Dic.JosaTable,(char*)&word[i]);
			if(result==TRUE) break;
			i += 2;
		} else {
			i++;
		}
	}

	/* if the word includes the josa, remove that	*/
	if(result==TRUE) word[i] = '\0'; 
}

/* Function : void IsOneNoun(char* word)
 *
 * Description : make the word NULL if the word is not a one-gulja useful noun
 *
 * Return : Nothing
 */

void IsOneNoun(char* word)	/* IN	pointer of the word	*/
{
	Four result;

	result = FindStringInHashTable(Dic.OnenounTable,(char*)word);

	if(result==FALSE) word[0] = '\0';

}


/* Function : void IsOneProNoun(char* word)	
 *
 * Description : make the word NULL if the word is a one-gulja pronoun 
 *
 * Return : Nothing
 */

void IsOneProNoun(char* word)	/* IN	pointer of the word	*/
{
	Four result;

	result = FindStringInHashTable(Dic.OnepronounTable,(char*)word);

	if(result==TRUE) word[0] = '\0';

}


/* Function : void IsSpecialNoun(char* word)	
 *
 * Description : make the word NULL if the word is a special noun
 *
 * Return : TURE if the word is special noun
 * 			FALE if the word is not special noun
 */

int IsSpecialNoun(char* word)	/* IN	pointer of the word	*/
{
	Four result;

	result = FindStringInHashTable(Dic.SpecialnounTable,(char*)word);

	return result;

}

/* Function : int IsWordLenghthOne(char* word)
 *
 * Description : decide whether the word is two gulja or not 
 *
 * Return : TURE if the word is two gulja
 * 			FALE if the word is not tow gulja		
 */

int IsWordLengthOne(char* word)	/* IN	pointer of the word		*/
{
	if(strlen(word)==ONENOUNLENGTH) return TRUE;
	else return FALSE;

}


/* Function : int StopwordExtract(Four handle, char* ptrToData, char* ptrToStopwords)
 *
 * Description : read the data and find stopwords and store them
 *
 * Return : eNOERROR if there is no error
 *				
 */

/*
 * Add ngramMode parameter to extract keywords that is not processed by n-gram.
 * Value of ngramMode parameter means following:
 *	 if ngramMode = 1, extract keywords that is processed by n-gram
 *	 else, extract keywords that is not processed by n-gram
 */

int StopwordExtract(
					Four	handle,			/* IN	handle number	*/ 
					char* ptrToData, 	/* IN	pointer of data to extract the keywords	*/
					char* ptrToStopwords, 	/* IN	pointer of stopwords */
					Boolean ngramMode
)
{
		
	unsigned char sentence[MAXSENTENCESIZE];	/*  array to keep the sentence	*/	
	unsigned char word[MAXWORDSIZE];			/*  array to keep the word	without josa */
	unsigned char word_withjosa[MAXWORDSIZE];	/*  array to keep the word with josa	*/
	unsigned char word_pronoun[MAXWORDSIZE];	/*  array to keep the one-gulja having possibility of pronoun	*/
	unsigned char keyword[MAXKEYWORDSIZE];		/*  array to keep the keyword	*/

	unsigned char word_english[MAXWORDSIZE];	/* array to keep the keyword for English word */
	
	Four result;								/*  result variable wether the word is josa, eomi or stopword	*/


	/* add a stemWord_size variable */
	Four buffer_size, sentence_size, word_size, stemWord_size; 	/* the size of buffer, sentence, word and stemized word	*/
	Four notReadyReadByte;							/* the size of buffer not yet processed	*/
	Four readByte;  								/* the number of byte from file to buffer */
	Four readByteBuffer, readByteSentence;  		/* the number of byte from buffer to sentence, the number of byte from sentence to word */

	Four str_idxSentence; 							/* index of sentence to start reading */
	Four str_idxBuffer;								/* index of word to start reading	*/
	Four xth_sentence, xth_word;  					/* variable of keeping sentence position and noun position	*/

	
	Four isNotPerfactFlag = 0; 						/* flag variable to check if a word is loaded or not correctly */
	Four isSpecialNounFlag = 0; 					/* flag variable to check if a word is special nown */

	Four idx_ngram;  								/* index variable used to process by n-gram	*/
	
	Four i,e;										/* temporary variable and error code	*/
	Four rts;


	/* if the Extract keyword is run at first, Initialize the dictionary
		else keyword buffer is reinitialized	*/

	if(startIRFlag == 1)
	{

		if((rts = InitDictionaryHashTable()) == eNOERROR );
		else
		{	
			fprintf(stderr, "\nInitDictionaryHashTable() is failed(%d)\n", rts);
			exit(-1);
		}

		startIRFlag = 0;

	}
	else
	{
		e = QuickFitMM_ReInit(&dynamicMMHandle);
		if(e<0) QUICKFITMM_ERROR(e);
	}
	
	
	buffer_size = strlen(ptrToData);
	
	str_idxBuffer = 0;

	/* read a sentence from the buffer repeatedly	*/
	while( str_idxBuffer < buffer_size)
	{
	
		/* read a sentence from the buffer	*/
		readByteBuffer = Read_Sentence( ptrToData, (char*) sentence, str_idxBuffer);

			
		/* in case of a period('.'), it is ignored */
		if( (sentence_size = strlen((char*)sentence)) <= 1) ;
		else  
		{

			str_idxSentence = 0;
					
			/* read a word from the sentence repeatedly	*/
			while( str_idxSentence < sentence_size)
			{

				/* read a word from the sentence */
				readByteSentence = Read_Word( (char*) sentence, (char*) word, str_idxSentence);
					
				/* in case of a space(' '), it is ignored	*/
				if( (word_size = strlen((char*)word)) <= 1 );
				else
				{	
					/* remove two characters( a space(' ') and a period('.') */
					/* and make the word string	*/
					/* those are used to distinct the sentence and the word	*/	
					if(word[word_size-1]==' '|| word[word_size-1]=='.')					
					{
						word[word_size-1] = '\0';			
					}								
					else ;
						
					/****************************************************/
					/*  process the word properly with the alorithm		*/				
					/****************************************************/
					
					/* when the word is not hangle, check it is the English stopword or not */					
					if( !IsWordKorean((char*)word) )				
					{	
						/*  when it is English*/
						if( IsWordEnglish((char*)word) ) 
						{
							CheckAndAppendStopword(word, ptrToStopwords);
						}
					}
					/* when the word is hangle */
					else
					{			

						/* if word length is more than one	*/ 
						if(!IsWordLengthOne((char*)word))
						{
							if(!IsSpecialNoun((char*)word)) {		
								/* remove the josa or eomi	*/		
								strcpy((char*)word_withjosa, (char*)word);
								RemoveJosaEomi((char*)word);

								isSpecialNounFlag = 0;
							} else {
								isSpecialNounFlag = 1;
							}
					
							/* if the word length is one, check if the word is a pronoun or not	*/
							if(IsWordLengthOne((char*)word))
							{
								strcpy((char*)word_pronoun,(char*)word);	
								IsOneNoun((char*)word);
				
								/* if the word is not a pronoun, store the word	*/
								if(strlen((char*)word) != 0) ; /*StoreKeyword(handle, word, xth_sentence, xth_word);*/
								else
								{
									IsOneProNoun((char*)word_pronoun);

									/* If a word is not a pronoun, store the word with the josa or eomi */	
									if(strlen((char*)word_pronoun) != 0) 
									{
										/* remove the stopword from the word with the josa or eomi	*/
										CheckAndAppendStopword(word_withjosa, ptrToStopwords);
									} /* A word is not a pronoun	*/									
	
								} /* If the word is not a useful word after removing josa and eomi	*/ 
							} /* If the length of a word is one after removing josa and eomi	*/
							else /* If the length of a word is more than one after removing josa and eomi	*/
							{

								if (!isSpecialNounFlag) 
								{
									/* remove the stopword from the word	*/
									CheckAndAppendStopword(word, ptrToStopwords);
								}

							} /* word length is more than two after removing the josa or eomi	*/ 


						} /* word is longer than 2-gulja */	


					} /* process of hangle (else)	*/												
								
			
				} /* it is not a space(' ') (else) */				
			
				str_idxSentence += readByteSentence;
			
			} /* read a word from the sentence (while)	*/
			
		}  /* it is not a period('.') (else)	*/		
			
		str_idxBuffer += readByteBuffer;
				
	}  /* read a sentence from the buffer (while)	*/

		
	return eNOERROR;

}

/* Function : int KeywrodExtract(Four handle, char* ptrToData)
 *
 * Description : read the data and extract keywords and store them
 *
 * Return : eNOERROR if there is no error
 *				
 */

/*
 * Add ngramMode parameter to extract keywords that is not processed by n-gram.
 * Value of ngramMode parameter means following:
 *	 if ngramMode = 1, extract keywords that is processed by n-gram
 *	 else, extract keywords that is not processed by n-gram
 */

int KeywordExtract(
					Four	handle,			/* IN	handle number	*/ 
					char* ptrToData, 	/* IN	pointer of data to extract the keywords	*/
					Boolean ngramMode
)
{
		
	unsigned char sentence[MAXSENTENCESIZE];	/*  array to keep the sentence	*/	
	unsigned char word[MAXWORDSIZE];			/*  array to keep the word	without josa */
	unsigned char word_withjosa[MAXWORDSIZE];	/*  array to keep the word with josa	*/
	unsigned char word_pronoun[MAXWORDSIZE];	/*  array to keep the one-gulja having possibility of pronoun	*/
	unsigned char keyword[MAXKEYWORDSIZE];		/*  array to keep the keyword	*/

	unsigned char word_english[MAXWORDSIZE];	/* array to keep the keyword for English word */
	
	Four result;								/*  result variable wether the word is josa, eomi or stopword	*/


	/* add a stemWord_size variable */
	Four buffer_size, sentence_size, word_size, stemWord_size; 	/* the size of buffer, sentence, word and stemized word	*/
	Four notReadyReadByte;							/* the size of buffer not yet processed	*/
	Four readByte;  								/* the number of byte from file to buffer */
	Four readByteBuffer, readByteSentence;  		/* the number of byte from buffer to sentence, the number of byte from sentence to word */

	Four str_idxSentence; 							/* index of sentence to start reading */
	Four str_idxBuffer;								/* index of word to start reading	*/
	Four xth_sentence, xth_word;  					/* variable of keeping sentence position and noun position	*/
	
	Four tmp_xth_word; 								/* tmporary variable to keep the noun position	*/

	
	Four isNotPerfactFlag = 0; 						/* flag variable to check if a word is loaded or not correctly */
	Four isSpecialNounFlag = 0; 					/* flag variable to check if a word is special nown */

	Four idx_ngram;  								/* index variable used to process by n-gram	*/
	
	Four i,e;										/* temporary variable and error code	*/
	Four rts;
	

	/* if the Extract keyword is run at first, Initialize the dictionary
		else keyword buffer is reinitialized	*/

	if(startIRFlag == 1)
	{

		if((rts = InitDictionaryHashTable()) == eNOERROR );
		else
		{	
			fprintf(stderr, "\nInitDictionaryHashTable() is failed(%d)\n", rts);
			exit(-1);
		}

		startIRFlag = 0;

	}
	else
	{
		e = QuickFitMM_ReInit(&dynamicMMHandle);
		if(e<0) QUICKFITMM_ERROR(e);
	}

	
	/* initialize the variables to store the location of words */
	xth_sentence = 1;
	xth_word = 1;
	tmp_xth_word = 1;
	
	
	buffer_size = strlen(ptrToData);
	
	str_idxBuffer = 0;

	/* read a sentence from the buffer repeatedly	*/
	while( str_idxBuffer < buffer_size)
	{
	
		/* read a sentence from the buffer	*/
		readByteBuffer = Read_Sentence( ptrToData, (char*) sentence, str_idxBuffer);


			
		/* in case of a period('.'), it is ignored */
		if( (sentence_size = strlen((char*)sentence)) <= 1) ;
		else  
		{

			str_idxSentence = 0;
					
			/* read a word from the sentence repeatedly	*/
			while( str_idxSentence < sentence_size)
			{

				/* read a word from the sentence */
				readByteSentence = Read_Word( (char*) sentence, (char*) word, str_idxSentence);


					
				/* in case of a space(' '), it is ignored	*/
				if( (word_size = strlen((char*)word)) <= 1 );
				else
				{	
						
					/* remove two characters( a space(' ') and a period('.') */
					/* and make the word string	*/
					/* those are used to distinct the sentence and the word	*/	
					if(word[word_size-1]==' '|| word[word_size-1]=='.')					
					{
						xth_word = tmp_xth_word;
						tmp_xth_word++;					
						word[word_size-1] = '\0';			
					}								
					else ;
							
						
					/****************************************************/
					/*  process the word properly with the alorithm		*/				
					/****************************************************/
					
					/* when the word is not hangle, check it is the English stopword or not */					
					if( !IsWordKorean((char*)word) )				
					{	
						/*  when it is English*/
						if( IsWordEnglish((char*)word) ) 
						{
								
							RemoveStopword((char*)word);
						
							/* 
							 * When the word is english, it is processed by 3-gram  
							 */
							
							word_size = strlen((char*)word);
							if(word_size == 0);
							else
							{
								strcpy((char*)word_english, (char*)word);
			
								Stem((char*)word);
								
								stemWord_size = strlen((char*)word);
								if(stemWord_size == 0);
								else if(stemWord_size <= ENGNGRAMSIZE)
								{
									StoreKeyword(handle, word, xth_sentence, xth_word);
								}
								else
								{
									idx_ngram = 0;
									

									if (ngramMode == 1)
									{
										while(idx_ngram + ENGNGRAMSIZE <= stemWord_size)
										{
											memcpy(keyword, &word_english[idx_ngram], ENGNGRAMSIZE);
											keyword[ENGNGRAMSIZE] = '\0';
											StoreKeyword(handle, keyword, xth_sentence, xth_word);
											
											idx_ngram += 1;
										}
									}
									else
									{
										StoreKeyword(handle, word, xth_sentence, xth_word);
									}
								}																							
							}
						}
						/*  when it is not Enlish, store it intactly	*/
						else StoreKeyword(handle, word, xth_sentence, xth_word);
#ifndef SGKIM_DEBUG	
	fprintf(stderr,"\n%s", word);
#endif					
																
					}
					/* when the word is hangle */
					else
					{			

								
#ifndef SGKIM_DEBUG	
	fprintf(stderr,"\n%s ", word);
	if(result==TRUE) fprintf(stderr,"   TRUE");
#endif																						

						/* if word is a one-gulja hangle	*/
						if(IsWordLengthOne((char*)word))
						{
							/* if word is a useful word, store the word*/
							IsOneNoun((char*)word);
							if(strlen((char*)word) != 0) StoreKeyword(handle, word, xth_sentence, xth_word);

						} 
						/* if word length is more than one	*/ 
						else
						{
							
							if(!IsSpecialNoun((char*)word)) {		
								/* remove the josa or eomi	*/		
								strcpy((char*)word_withjosa, (char*)word);
								RemoveJosaEomi((char*)word);

								isSpecialNounFlag = 0;
							} else {
								isSpecialNounFlag = 1;
							}
					
							/* if the word length is one, check if the word is a pronoun or not	*/
							if(IsWordLengthOne((char*)word))
							{
								strcpy((char*)word_pronoun,(char*)word);	
								IsOneNoun((char*)word);
				
								/* if the word is not a pronoun, store the word	*/
								if(strlen((char*)word) != 0) StoreKeyword(handle, word, xth_sentence, xth_word);
								else
								{
									IsOneProNoun((char*)word_pronoun);

									/* If a word is not a pronoun, store the word with the josa or eomi */	
									if(strlen((char*)word_pronoun) != 0) 
									{

										/* remove the stopword from the word with the josa or eomi	*/
										RemoveStopword((char*)word_withjosa);
						
										/**********************************/ 
										/*   process the word by n-gram   */ 
										/**********************************/ 
						
										word_size = strlen((char*)word_withjosa);
										
										/*  in case of stopword or josa , ignore it */
										if(word_size==0);
										/* the word size is less than n-gram size, store the word intactly */
										else if(word_size <= NGRAMSIZE*2) StoreKeyword(handle, word_withjosa, xth_sentence, xth_word);
										else
										{
											if (ngramMode == 1)
											{

												idx_ngram = 0;
				
												/*  cut the word in size of n-gram	*/
												while(idx_ngram + NGRAMSIZE*2 <= word_size)
												{
													memcpy(keyword,&word_withjosa[idx_ngram],NGRAMSIZE*2);
													keyword[NGRAMSIZE*2] = '\0';
						
													/* store the keyword */
													StoreKeyword(handle, keyword, xth_sentence, xth_word);
#ifndef SGKIM_DEBUG	
		fprintf(stderr,"\n%s ", keyword);
#endif																			
													idx_ngram += 2 ;
												}						
											}
											else
											{
												StoreKeyword(handle, word_withjosa, xth_sentence, xth_word); 
											}
							
										} /* process of n-gram (else)	*/ 

									} /* A word is not a pronoun	*/									
	
								} /* If the word is not a useful word after removing josa and eomi	*/ 
							} /* If the length of a word is one after removing josa and eomi	*/
							else /* If the length of a word is more than one after removing josa and eomi	*/
							{

								if (!isSpecialNounFlag) 
								{
									/* remove the stopword from the word	*/
									RemoveStopword((char*)word);
								}

								/**********************************/ 
								/*   process the word by n-gram   */ 
								/**********************************/ 
						
								word_size = strlen((char*)word);

								/*  in case of stopword or josa , ignore it */
								if(word_size==0);
								/* the word size is less than n-gram size, store the word intactly */
								else if(word_size <= NGRAMSIZE*2) StoreKeyword(handle, word, xth_sentence, xth_word); 
								else
								{
									if (ngramMode == 1)
									{

										idx_ngram = 0;
					
										/*  cut the word in size of n-gram	*/
										while(idx_ngram + NGRAMSIZE*2 <= word_size)
										{
											memcpy(keyword,&word[idx_ngram],NGRAMSIZE*2);
											keyword[NGRAMSIZE*2] = '\0';
						
											/* store the keyword */
											StoreKeyword(handle, keyword, xth_sentence, xth_word);					

#ifndef SGKIM_DEBUG	
		fprintf(stderr,"\n%s ", keyword);
#endif																			
											idx_ngram += 2 ;
										} /* If the length of a word is more than one after removing josa and eomi	*/	
									} /* ngramMode (if) */
									else
									{
										StoreKeyword(handle, word, xth_sentence, xth_word); 
									} /* ngramMode (else) */
							
								} /* process of n-gram (else)	*/ 

							} /* word length is more than two after removing the josa or eomi	*/ 


						} /* word is longer than 2-gulja */	


					} /* process of hangle (else)	*/												
								
			
				} /* it is not a space(' ') (else) */				
			
				str_idxSentence += readByteSentence;
			
			} /* read a word from the sentence (while)	*/
			

			/*  when the sentence is ended with a period('.'), set the variables properly*/
			if(sentence[sentence_size-1] == '.') 
			{						
				xth_sentence++;				
				tmp_xth_word = 1;
			}
				
		}  /* it is not a period('.') (else)	*/		
			
		str_idxBuffer += readByteBuffer;
				
	}  /* read a sentence from the buffer (while)	*/

		
	return eNOERROR;

}



/******************************************/
/* Main functions to extract the keywords */
/******************************************/


/* Function : int KorExtractor_Init( Four locationOfContent, OOSQL_SystemHandle* handle, char* className,
 *									char* className, OID* oid, Two colNo, char* inFileOrContent, Four* resultHandle )
 * Description : initialize the dictionary and extract the keyword and store
 *
 * Return : eNOERROR if there is no error
 * 			error code if there is soem errors
 *			exit if there is memory init error
 */

int KorExtractor_Init(
	Four 				locationOfContent,		/*  IN flag whether input is file, db, or memory	*/
	OOSQL_SystemHandle* handle,					/*  IN OOSQL handle	*/
	Four 				volId,					/*  IN volume id	*/
	char* 				className,				/*  IN class name	*/
	OID* 				oid,					/*  IN object id	*/
	Two 				colNo,					/*  IN column from which keywords are extracted	*/
	char* 				inFileOrContent,		/*  IN input file name if input is the file, or content string if input is memory	*/
	Four* 				resultHandle )			/*  OUT handle to deal with current keyword extractor	*/
{
	
	FILE *fp_datafile;				/* pointer variable to open the data file	*/
		
	char* ptrToData;				/* temporary variable to point the data	*/	
	char* readBuffer;				/* temporary variable to point the data buffer according with the handle	*/   
	
	Four memSize;					/* temporary variable to store the memory size	*/				
	Four firstFreeHandle;			/* temporary variable to store the first free handle	*/
	Four readBufferSize;			/* temporary variables to store the read data size	*/ 
	Four readByte;					
	Four totalreadByte;

	Boolean ngramMode = TRUE;		/* variable to select whether KeywordExtract function process keyword by n-gram or not */ 
	
	Four e;							/* error code	*/
	


	/* if the KorExtract runs at first, initialize the memory handle	*/	
	if ( korKeywordExtractorHandle == NULL )
	{
		
		if(InitKorKeywordExtractor() == eNOERROR) ;
		else
		{	
			fprintf(stderr, "\nInitKorKeywordExtracHandle() is failed\n");
			exit(-1);
		}			
		
	}
	
	
	/*  find the first free handle and initialize that	*/
	nHandle++;
	firstFreeHandle = FindFirstFreeHandle();
	CHECK_ERROR(handle, firstFreeHandle);
	
	*resultHandle = firstFreeHandle;


	/* Initialize the first free handle */	
	if(InitKorKeywordExtractBuffer(firstFreeHandle)!=eNOERROR) 
	{
			fprintf(stderr, "\nInitKorKeywordExtractBuffer() is failed\n");
			exit(-1);
	}
	
	/*  if input is from file	*/
	if (locationOfContent == OOSQL_TEXT_IN_FILE)
	{
	
		/* open the data file */
		if ((fp_datafile = fopen(inFileOrContent, "r")) == NULL)
		{
			fprintf(stderr, "Can't open %s input-file in KorKeywordExtractor_Init\n", inFileOrContent);
			exit(-1);
		}
			
		
		readBuffer = korKeywordExtractorHandle[firstFreeHandle].readBuffer;
		readBufferSize = korKeywordExtractorHandle[firstFreeHandle].readBufferSize;
		
		totalreadByte = 0;
		
		readByte = fread(readBuffer,1, readBufferSize, fp_datafile);

		
		/* read the data from the file	*/	
		while(readByte)
		{
			/* make the data string including NULL	*/
			if(readByte != readBufferSize)
			{
				readBuffer[readByte] = '\0';				
				break;
			}
			else  /* when there is some more data to read, double the buffer	*/
			{
				korKeywordExtractorHandle[firstFreeHandle].readBufferSize *= 2;
				
				e = QuickFitMM_ReAlloc(&dynamicMMHandle,(void**)&korKeywordExtractorHandle[firstFreeHandle].readBuffer, korKeywordExtractorHandle[firstFreeHandle].readBufferSize);
				
				if( e < 0 ) QUICKFITMM_ERROR(e);												

			}
					
			totalreadByte += readByte;
			readBuffer = &korKeywordExtractorHandle[firstFreeHandle].readBuffer[totalreadByte];
			readBufferSize = korKeywordExtractorHandle[firstFreeHandle].readBufferSize - totalreadByte;
			
			readByte = fread(readBuffer,1, readBufferSize, fp_datafile);			
			
		}
	

		ptrToData = korKeywordExtractorHandle[firstFreeHandle].readBuffer;
		
	
		/* remove the special character in the buffer and get ready to extract keywords from the data	*/
		if( eNOERROR==Remove_SpecialChar(ptrToData)); 
		else 
		{	
			fprintf(stderr,"\nRemove_SpecialChar Error\n");
			exit(-1);
		}

		EliminateInvisibleChar(ptrToData);
		ConvertSymbolCharToSpace(ptrToData);
		EliminateTooLongKeyword(ptrToData);  

		/* if the data is not NULL after removing the special characters, extract keywords	*/	
		if(ptrToData[0]!= '\0')			
			e = KeywordExtract(firstFreeHandle, ptrToData, ngramMode); 
		
		/* if some errors happens, exit the program	*/
		if( e < eNOERROR ) 
		{
			fprintf(stderr, "\nKeywordExtract() failed\n");
			exit(-1);
		}		
	
		/*  close the file	*/
		if ((fclose(fp_datafile)) == EOF)
		{
			fprintf(stderr, "Can't close %s input-file in KorKeywordExtractor_Init\n", inFileOrContent);
			exit(-1);
		}
		

	} /* TEXT_IN_FILE */
	/*  if input is from the memory	*/
	/*  assumption : a input from memory must be string including '\0'	*/
	else if(locationOfContent == OOSQL_TEXT_IN_MEMORY)
	{

		ptrToData = inFileOrContent;
		
		/* remove the special character in the buffer and get ready to extract keywords from the data	*/
		if( eNOERROR==Remove_SpecialChar(ptrToData)); 
		else printf("\nRemove_SpecialChar Error\n");

		EliminateInvisibleChar(ptrToData);
		ConvertSymbolCharToSpace(ptrToData);
		EliminateTooLongKeyword(ptrToData);
		
		/* if the data is not NULL after removing the special characters, extract keywords	*/	
		if(ptrToData[0]!= 0)
			e = KeywordExtract(firstFreeHandle, ptrToData, ngramMode); 
			
		/* if some errors happens, exit the program	*/
		if( e < eNOERROR ) 
		{
			fprintf(stderr, "\nKeywordExtract() failed\n");
			exit(-1);
		}		
		
	
		
	} /* TEXT_IN_MEMORY */
	/*  if input is from DB	*/
	else if( locationOfContent == OOSQL_TEXT_IN_DB)/* TEXT_IN_DB */
	{
		
		readBuffer = korKeywordExtractorHandle[firstFreeHandle].readBuffer;
		readBufferSize = korKeywordExtractorHandle[firstFreeHandle].readBufferSize;
		
		totalreadByte = 0;
		
		e = OOSQL_Text_FetchContent(handle, volId, className, colNo, oid, readBufferSize , readBuffer, &readByte);
		CHECK_ERROR(handle, e);

		/* read the data from the database	*/	
		while(readByte)
		{

			/* make the string including the NULL	*/
			if(readByte != readBufferSize)
			{
				readBuffer[readByte] = '\0';				
				break;
			}
			else  /* when there is some more data to read, double the buffer	*/
			{
				korKeywordExtractorHandle[firstFreeHandle].readBufferSize *= 2;
				
				e = QuickFitMM_ReAlloc(&dynamicMMHandle,(void**)&korKeywordExtractorHandle[firstFreeHandle].readBuffer, korKeywordExtractorHandle[firstFreeHandle].readBufferSize);
				
				if( e < 0 ) QUICKFITMM_ERROR(e);												

			}
			

			
			totalreadByte += readByte;
			readBuffer = &korKeywordExtractorHandle[firstFreeHandle].readBuffer[totalreadByte];
			readBufferSize = korKeywordExtractorHandle[firstFreeHandle].readBufferSize - totalreadByte;
			
			e = OOSQL_Text_FetchContent(handle, volId, className, colNo, oid, readBufferSize , readBuffer, &readByte);
			CHECK_ERROR(handle, e);
						
			
		}
		
		ptrToData = korKeywordExtractorHandle[firstFreeHandle].readBuffer;
		
			
		/* remove the special character in the buffer and get ready to extract keywords from the data	*/
		if( eNOERROR==Remove_SpecialChar(ptrToData)); 
		else 
		{	
			fprintf(stderr,"\nRemove_SpecialChar Error\n");
			exit(-1);
		}

		EliminateInvisibleChar(ptrToData);
		ConvertSymbolCharToSpace(ptrToData);	
		EliminateTooLongKeyword(ptrToData);  

		
		/* if the data is not NULL after removing the special characters, extract keywords	*/	
		if(ptrToData[0]!= '\0')			
			e = KeywordExtract(firstFreeHandle, ptrToData, ngramMode); 
		

		/* if some errors happens, exit the program	*/
		if( e < eNOERROR ) 
		{
			fprintf(stderr, "\nKeywordExtract() failed\n");
			exit(-1);
		}		
		
			
	} /* TEXT_IN_DB */
	else
	{
		fprintf(stderr, "Unknown input method is used. Since input string is now given, abandon extracting keywords\n");
		exit(-1);
	}
		

	return eNOERROR;
	
}


/* Function : int KorExtractor_Next( Four handle, char* keyword, Four *nPositions, char* positionList )
 *
 * Description : store the keyword and position information according with handle	
 *
 * Return : eNOERROR if the keyword is properly stored and the rest keyword to store exits	 
 *			OOSQL_TEXT_DONE if there is no rest keyword to store
 */

int KorExtractor_Next(
	Four  handle,					/*	IN 	handler to deal with current keyword extractor	*/
	char* keyword, 					/*  OUT	current keyword	*/
	Four *nPositions, 				/*  OUT	# of position	*/
	char* positionList )			/*  OUT position list of the current keyword	*/
{	
	
	Four i;								/* temporary variable	*/
	
	Four bufferOffset = 0;				
	Four keywordOffset;					/* temporary variable to store the next read keyword	*/
	Four tmp_nPositions;				/* temporary variable to store the number of postions	*/
	Position* keyword_positionList;		/* pointer of the Postiion	*/ 


	while(1)
	{

		keywordOffset = korKeywordExtractorHandle[handle].keywordOffset;
	
		/* stroe the keyword and number of position	*/
		if(korKeywordExtractorHandle[handle].keywordBuffer[keywordOffset].keyword)
			strcpy(keyword, (char*)korKeywordExtractorHandle[handle].keywordBuffer[keywordOffset].keyword);
		else strcpy(keyword,"");

		*nPositions = tmp_nPositions = korKeywordExtractorHandle[handle].keywordBuffer[keywordOffset].nPositions;
	
	
		/* store the position list	*/
		keyword_positionList = korKeywordExtractorHandle[handle].keywordBuffer[keywordOffset].positionList;

		if(keyword_positionList)
			memcpy(&positionList[bufferOffset], keyword_positionList, sizeof(Position)*tmp_nPositions);

		
		/* the number of stored keyword is increased	*/
		korKeywordExtractorHandle[handle].keywordOffset++; 

		if(strlen(keyword) > 0) break;
		if(keywordOffset >= korKeywordExtractorHandle[handle].nKeywords) break;

	}
	
	/* if the keyword information is stored properly, then return eNOERROR	*/
	if(keywordOffset < korKeywordExtractorHandle[handle].nKeywords) return eNOERROR;
	/* if the rest keyword to store doesn't exists, then return OOSQL_TEXT_DONE	*/ 
	else return OOSQL_TEXT_DONE;
	
}


/* Function : int KorExtractor_Final(Four handle)
 *
 * Description : free the keyword buffer and potition buffer of a given handle 
 *
 * Return : eNOERROR if there is no error
 *			exit if there is some error	 
 */

int KorExtractor_Final(Four handle)	/* IN	handle number	*/
{
	
	Four e;		/* error code	*/
	Four i;		/* temporary variable	*/

	
	/*  Free the keyword buffer of the used handle	*/
	for( i=0; i<korKeywordExtractorHandle[handle].nKeywords; i++)
	{
		e = QuickFitMM_Free(&dynamicMMHandle, (char*)korKeywordExtractorHandle[handle].keywordBuffer[i].keyword);
		if (e < 0)	QUICKFITMM_ERROR(e);	
		
		e = QuickFitMM_Free(&dynamicMMHandle, (char*)korKeywordExtractorHandle[handle].keywordBuffer[i].positionList);
		if (e < 0)	QUICKFITMM_ERROR(e);		
		
	}

	/* Free the data buffer of the sued handle	*/ 
	e = QuickFitMM_Free(&dynamicMMHandle, (char*)korKeywordExtractorHandle[handle].readBuffer);
	if (e < 0)	QUICKFITMM_ERROR(e);	

	e = QuickFitMM_Free(&dynamicMMHandle, (char*)korKeywordExtractorHandle[handle].keywordBuffer);
	if (e < 0)	QUICKFITMM_ERROR(e);	


	/* update the handle information to be used at next	*/
	korKeywordExtractorHandle[handle].isUsed  		= FALSE;
	/* default value of ngramQueryMode is FALSE */
	korKeywordExtractorHandle[handle].ngramQueryMode= FALSE;
	korKeywordExtractorHandle[handle].nKeywords 		= 0;
	korKeywordExtractorHandle[handle].keywordOffset  = 0;
	
	nHandle--;	
	
	return eNOERROR;
		
}


/* Function : void RemoveEmptyParenthesis(char* word)
 *
 * Description : For a query, empty parenthesis( like () or ()& ()| ) is useless so remove them 
 * 
 * Return : Nothing
 */

void RemoveEmptyParenthesis(char* word)	/* IN	pointer of the word	*/
{
	char* ptr_word = word;			/* pointer variable of the word	*/	
	char* tmp_word;					/* temporary pointer variable to rememver the word position	*/	
	char ch;						/* temporary variable to copy a character	*/ 

	char afterEmptyParenthesis[1024*4]; 	/* temporary variable to store the word after removing the empty parentheses	*/ 
	int idx = 0;					/* temporary index of the afterEmptyParenthesis buffer	*/	

	while(*ptr_word)
	{
		ch = *ptr_word;
		
		/* if a read character is '(' tnen check if the useless parentheses exist or not	*/  
		if(ch == '(')
		{
			tmp_word = ptr_word;
			tmp_word++;

			/* if useless parentheses exist, remove them	*/	
			if(*tmp_word == ')')
			{
				ptr_word++;
				ptr_word++;
				
				if(*ptr_word == '&' || *ptr_word == '|') ptr_word++;

			}
			else 
			{
				afterEmptyParenthesis[idx++] = ch;
				ptr_word++;
			}
		}
		else
		{
			afterEmptyParenthesis[idx++] = ch;
			ptr_word++;
		}	

		/* for segment fault error, if the idx is more than 1024*4 then break	*/
		if(idx > 1024*4) break;

	}

	/* make the string including NULL	*/
	afterEmptyParenthesis[idx] = '\0';

	/* copy the word after removing the empty parentheses	*/ 
	strcpy(word,afterEmptyParenthesis);

}


/* Function : int IsCorrectNumOfParenthesis(char* word)
 *
 * Description : check if the correct parentheses exist or not	
 *
 * Return : TURE if there is the correct number of parentheses 
 *			FALSE if there is not the correct number of parentheses
 */

int IsCorrectNumOfParenthesis(char* word)	/* IN	pointer of the word	*/
{
	char* ptr_word = word;			/* temporary pointer of the word	*/

	int nLeftParenthesis = 0;		/* temporary variable to store the number of the left parentheses	*/
	int nRightParenthesis = 0;		/* temporary variable to store the number if the right parentheses	*/
	
	while(*ptr_word)
	{
		if(*ptr_word == '(') nLeftParenthesis++;	
		else if(*ptr_word == ')') nRightParenthesis++;
		else;
		
		ptr_word++;	
	}

	/* if the number of the left parentheses and one of the right ones are same, return TURE
		or not, return FALSE	*/

	if(nLeftParenthesis == nRightParenthesis) return TRUE;
	else return FALSE;

}


/* Function : int GetStopwrods(char* in, char* out)
 * 
 * Description : read the in string and make the stopwords list for a query and store that into the string out	
 *
 * Return : eNOERROR if there is no error
 *			-1 if the parentheses is not matched correctly
 */

int GetStopwords(
	char* in,			/* IN	pointer of input string	*/ 
	char* out )			/* OUT 	pointer of output string after extract keyword from the in string	*/
{
	
	char* ptr_in = in;				/* temporary pointer of the in string	*/
	char* ptr_tmp_in;				/* temporary pointer to store the temporary in string from the in string	*/
	
	char tmp_in[1024];				/* temporary variable to store the temporary in string from the in string	*/
	char tmp_parenthesis[10];		/* temporary variable to store the parentheses ('(' or ')')	*/
	
	char word[1024];				/* temporary variable to store the word from the in	*/ 
	char* ptr_word;					/* temporary pointers to remember the position of the word	*/
	char* ptr_word_tmp_in;
	char readOneChar;				/* temporary variable to store one character	*/
	
	Four firstFreeHandle;			/* temporary variable to store the first free handle	*/

	Four i,e,k;						/* temporary variable and error code	*/		

	Boolean ngramMode = TRUE;		/* variable to select whether KeywordExtract function process keyword by n-gram or not */ 

	Boolean ngramQueryMode = TRUE; /* variable to decide to extract keywords for query */

	Boolean finalFlag = FALSE;


	if(in == NULL || out == NULL)
	{
		fprintf(stderr, "MakeNgramKeywords function gets bad parameter\n");
		return eBADPARAMETER;
	}
	

	/* if the keywrod Extract runs properly, initialize the keywrodExtractor	*/ 
	if ( korKeywordExtractorHandle == NULL )
	{
		
		if(InitKorKeywordExtractor() == eNOERROR) ;
		else
		{	
			fprintf(stderr, "\nInitKorKeywordExtracHandle() is failed\n");
			exit(-1);
		}					
	}

	
	/* get ready before the keyword extract runs	*/ 

	out[0] = '\0';
	tmp_in[0] = '\0';
	ptr_tmp_in = tmp_in;


	while(*ptr_in==' ') ptr_in++;	
	
	while(1)
	{
		/* if the special character and end of the string exits, make the word 	*/
		if(*ptr_in == '|' || *ptr_in =='&' || *ptr_in == '^' || *ptr_in == '~' || *ptr_in == '\0')
		{
			
			*ptr_tmp_in = '\0';	
			
			while(*ptr_in == ' ') ptr_in++;

			if(*ptr_in == '|' || *ptr_in == '&')
 			{	
				ptr_in++;
			}
			else if(*ptr_in == '^' || *ptr_in == '~')
			{
				ptr_in++;

				while(*ptr_in == ' ') ptr_in++;

				ptr_in++;
						
				
				if(*ptr_in >= '0' && *ptr_in <= '9')
				{
					ptr_in++;
					while(*ptr_in >= '0' && *ptr_in <= '9')
					{
						ptr_in++;
					}
					if (*ptr_in == '\0')
					{
						out[0] = '\0';
						return -1;
					}
					else ptr_in--;
					
				}
				else
				{
					out[0] = '\0';
					return -1;
				}
				
				
			}
			else finalFlag = TRUE;
		
			/* remove the special character except the parentheses from the word 	*/						
			if( eNOERROR==Remove_SpecialCharForQuery(tmp_in)); 
			else printf("\nRemove_SpecialChar Error\n");
			
			

			if(tmp_in[0] !='\0')
			{

				ptr_word_tmp_in = tmp_in;
				ptr_word = word;

				while(1)
				{
					readOneChar = *ptr_word_tmp_in++;

					/* if a space , parentheses and end of the string exits, make the temporary word 	*/
					if(readOneChar == ' ' || readOneChar == '\0' || readOneChar == '(' || readOneChar == ')')
					{
						*ptr_word = '\0';	

						while(*ptr_word_tmp_in == ' ') ptr_word_tmp_in++;

						if(readOneChar == '(')
						{
							tmp_parenthesis[0] = readOneChar;
							tmp_parenthesis[1] = '\0';

							strcat(out, tmp_parenthesis);
						}

						if(word[0] != '\0')
						{

							/*  find the first free handle and initialize that	*/
							nHandle++;
							firstFreeHandle = FindFirstFreeHandle();
							CHECK_ERROR(handle, firstFreeHandle);
	
	
							/* Initialize the first free handle */	
							if(InitKorKeywordExtractBuffer(firstFreeHandle)!=eNOERROR) 
							{
								fprintf(stderr, "\nInitKorKeywordExtractBuffer() is failed\n");
								exit(-1);
							}

							/* set keyword sort mode */
							if(setNgramQueryMode(firstFreeHandle, ngramQueryMode)!=eNOERROR)
							{
								fprintf(stderr, "\nsetNgramQueryMode() is failed\n");
								exit(-1);
							}




							/* Extract the stopwords form the words, and then check error	*/

							e = StopwordExtract(firstFreeHandle, word, out, ngramMode); 

   				 			if( e < eNOERROR )
	   			 			{
		   			 			fprintf(stderr, "\nKeywordExtract() in MakeNgramKeywords failed\n");
			   		 			exit(-1);
							}


							/* finialize the handle	*/
							KorExtractor_Final(firstFreeHandle);
						
							ptr_word = word;
							word[0] = '\0';
				
						}
				
						/* if invisible special characters exist, remove them	*/
						if(readOneChar == ')')
						{
							if(out[strlen(out)-1] == '&') out[strlen(out)-1] = '\0';

							tmp_parenthesis[0] = readOneChar;
							tmp_parenthesis[1] = '\0';

							strcat(out, tmp_parenthesis);
						}

					}
					else *ptr_word++ = readOneChar;
			
					/* if the end of the string, break	*/
					if(readOneChar == '\0') break;
				
				}
				
				/* if invisible special characters exist, remove them	*/
				if(out[strlen(out)-1] == '&' ) out[strlen(out)-1] = '\0';

			}
			
			
		
			tmp_in[0] = '\0';
			ptr_tmp_in = tmp_in;
			
			
			/* if the end of the string exits, break	*/ 
			if(finalFlag == TRUE) break;

		}	
		else *ptr_tmp_in++ = *ptr_in++;	

	} /* while	*/	

	if( out[strlen(out)-1] == '|' ||  out[strlen(out)-1] == '&') out[strlen(out)-1] = '\0';


	/* check if the correct parentheses exist, remove empty parentheses and return tne number of keywords	*/
	if(IsCorrectNumOfParenthesis(out)==TRUE) 
	{	
		RemoveEmptyParenthesis(out);
		return eNOERROR;
	}
	/* if the correct number of parentheses exist, make the out string NULL and return -1	*/ 
	else
	{
		out[0] = '\0';	
		return -1;
	}

}



/* Function : int MakeNgramKeywrods(char* in, char* out)
 * 
 * Description : read the in string and make the keyword for a query and store that into the string out	
 *
 * Return : keyword number if the keyword properly extracts
 *			-1 if the parentheses is not matched correctly
 */

int MakeNgramKeywords(
	char* in,			/* IN	pointer of input string	*/ 
	char* out )			/* OUT 	pointer of output string after extract keyword from the in string	*/
{
	
	char* ptr_in = in;				/* temporary pointer of the in string	*/
	char* ptr_tmp_in;				/* temporary pointer to store the temporary in string from the in string	*/
	char tmp_in[1024];				/* temporary variable to store the temporary in string from the in string	*/
	char tmp_out[2*1024];			/* temporary variable to store the  temporary out string after extract keywords frrom tmp_in string	*/ 
	char tmp_char[1024];			/* temporary variable to store the special characters like '&' and '|'	*/
	char tmp_num;					/* temporary variable to store the number after the special characters '~' and '^'	*/
	char ampersand[10];				/* temporary variable to store the ampersand character(&)	*/
	char tmp_parenthesis[10];		/* temporary variable to store the parentheses ('(' or ')')	*/
	
	char word[1024];				/* temporary variable to store the word from the in	*/ 
	char* ptr_word;					/* temporary pointers to remember the position of the word	*/
	char* ptr_word_tmp_in;

	char readOneChar;				/* temporary variable to store one character	*/
	
	Four firstFreeHandle;			/* temporary variable to store the first free handle	*/
	Four tmpnKeywords;				/* temporary variable to store the number of keywords of tmp_in	*/

	Four nKeywords;					/* temporary variable to store the minimum number of n-gram keywords of the keyword in in	*/
	Four totalKeywords;				/* temporary variable to store the total number of keywords of in	*/
	
	Four i,e,k;						/* temporary variable and error code	*/		
	Four countKeyword = 0;			
	Four tmpnKeyword1, tmpnKeyword2;
	char tmpSeparator[10];		

	char nDigits[10];

	
	Boolean ngramMode = TRUE;		/* variable to select whether KeywordExtract function process keyword by n-gram or not */ 

	Boolean ngramQueryMode = TRUE; /* variable to decide to extract keywords for query */


	if(in == NULL || out == NULL)
	{
		fprintf(stderr, "MakeNgramKeywords function gets bad parameter\n");
		return eBADPARAMETER;
	}
	
	/* if the keywrod Extract runs properly, initialize the keywrodExtractor	*/ 
	if ( korKeywordExtractorHandle == NULL )
	{
		
		if(InitKorKeywordExtractor() == eNOERROR) ;
		else
		{	
			fprintf(stderr, "\nInitKorKeywordExtracHandle() is failed\n");
			exit(-1);
		}					
	}

	
	/* get ready before the keyword extract runs	*/ 

	out[0] = '\0';
	tmp_in[0] = '\0';
	tmp_out[0] = '\0';
	ptr_tmp_in = tmp_in;
	nKeywords = 0;
	totalKeywords = 0;
	tmpnKeyword1 = 0;
	tmpnKeyword2 = 0;
	tmpSeparator[0] = '\0';
	nDigits[0] = '\0';

	memset(tmp_char, 0, 1024);

	strcpy(ampersand,"&");

	while(*ptr_in==' ') ptr_in++;	
	
	while(1)
	{
		/* if the special character and end of the string exits, make the word 	*/
		if(*ptr_in == '|' || *ptr_in =='&' || *ptr_in == '^' || *ptr_in == '~' || *ptr_in == '\0')
		{
			
			*ptr_tmp_in = '\0';	
			
			while(*ptr_in == ' ') ptr_in++;

			if(*ptr_in == '|' || *ptr_in == '&')
 			{	
				tmp_char[0] = *ptr_in++;
				tmp_char[1] = '\0';		
			}
			else if(*ptr_in == '^' || *ptr_in == '~')
			{
				memset(tmp_char, 0, 1024);	
				tmp_char[0] = *ptr_in++;

				while(*ptr_in == ' ') ptr_in++;

				tmp_num = *ptr_in++;
						
				
				if(tmp_num >= '0' && tmp_num <= '9') 
				{
					k = 0;
					nDigits[k++] = tmp_num;
					tmp_num = *ptr_in++;
					while(tmp_num >= '0' && tmp_num <= '9')
					{
						nDigits[k++] = tmp_num;
						tmp_num = *ptr_in++;
					}
					if (tmp_num == '\0')
					{
						out[0] = '\0';
						return -1;
					}
					else ptr_in--;
					
					nDigits[k] = '\0';
					strcat(tmp_char, nDigits);
				}
				else
				{
					out[0] = '\0';
					return -1;
				}
				
				
			}
			else strcpy(tmp_char,"");
		
			/* remove the special character except the parentheses from the word 	*/						
			if( eNOERROR==Remove_SpecialCharForQuery(tmp_in)); 
			else printf("\nRemove_SpecialChar Error\n");
			
			tmpnKeywords = 0;	
			

			if(tmp_in[0] !='\0')
			{

				ptr_word_tmp_in = tmp_in;
				ptr_word = word;

				while(1)
				{
					readOneChar = *ptr_word_tmp_in++;

					/* if a space , parentheses and end of the string exits, make the temporary word 	*/
					if(readOneChar == ' ' || readOneChar == '\0' || readOneChar == '(' || readOneChar == ')')
					{
						*ptr_word = '\0';	

						while(*ptr_word_tmp_in == ' ') ptr_word_tmp_in++;

						if(readOneChar == '(')
						{
							tmp_parenthesis[0] = readOneChar;
							tmp_parenthesis[1] = '\0';

							strcat(out, tmp_parenthesis);
						}

						if(word[0] != '\0')
						{

							/*  find the first free handle and initialize that	*/
							nHandle++;
							firstFreeHandle = FindFirstFreeHandle();
							CHECK_ERROR(handle, firstFreeHandle);
	
	
							/* Initialize the first free handle */	
							if(InitKorKeywordExtractBuffer(firstFreeHandle)!=eNOERROR) 
							{
								fprintf(stderr, "\nInitKorKeywordExtractBuffer() is failed\n");
								exit(-1);
							}

							/* set keyword sort mode */
							if(setNgramQueryMode(firstFreeHandle, ngramQueryMode)!=eNOERROR)
							{
								fprintf(stderr, "\nsetNgramQueryMode() is failed\n");
								exit(-1);
							}

							/* Extract the keywords form the words, and then check error	*/

							e = KeywordExtract(firstFreeHandle, word, ngramMode); 

   				 			if( e < eNOERROR )
	   			 			{
		   			 			fprintf(stderr, "\nKeywordExtract() in MakeNgramKeywords failed\n");
			   		 			exit(-1);
							}

					
							tmp_out[0] = '\0';

							/* update the information properly	*/
							tmpnKeywords += korKeywordExtractorHandle[firstFreeHandle].nKeywords;

							/* if the left parenthesis exists, strcat that	*/
							if(korKeywordExtractorHandle[firstFreeHandle].nKeywords != 0) strcat(tmp_out,"(");
		
							/* strcat the extracted keywords into the temporary out string	*/
							for( i=0; i< korKeywordExtractorHandle[firstFreeHandle].nKeywords; i++)
							{
	
								strcat(tmp_out,"\"");			
								strcat(tmp_out,(char*)korKeywordExtractorHandle[firstFreeHandle].keywordBuffer[i].keyword);
								strcat(tmp_out,"\"");

#ifdef NGRAMQUERYEXACTMATCH 
								if(i!= korKeywordExtractorHandle[firstFreeHandle].nKeywords-1) strcat(tmp_out,"^0");
#else
								if(i!= korKeywordExtractorHandle[firstFreeHandle].nKeywords-1) strcat(tmp_out,"|");

#endif /* NGRAMQUERYEXACTMATCH */

							}		
			
							/* if the right parenthesis exists, strcat that	*/
							if(korKeywordExtractorHandle[firstFreeHandle].nKeywords != 0) strcat(tmp_out,")");
		
							/* if the extracted keywords exits, strcat a special character	*/ 							
							if(korKeywordExtractorHandle[firstFreeHandle].nKeywords != 0)
							{
								strcat(out,tmp_out);
						
								if(readOneChar != '\0') strcat(out,ampersand);
								
							}							

							/* finialize the handle	*/
							KorExtractor_Final(firstFreeHandle);
						
							ptr_word = word;
							word[0] = '\0';
				
						}
				
						/* if invisible special characters exist, remove them	*/
						if(readOneChar == ')')
						{
							if(out[strlen(out)-1] == '&') out[strlen(out)-1] = '\0';

							tmp_parenthesis[0] = readOneChar;
							tmp_parenthesis[1] = '\0';

							strcat(out, tmp_parenthesis);
						}

					}
					else *ptr_word++ = readOneChar;
			
					/* if the end of the string, break	*/
					if(readOneChar == '\0') break;
				
				}
				
				/* if invisible special characters exist, remove them	*/
				if(out[strlen(out)-1] == '&' ) out[strlen(out)-1] = '\0';

			}
			

			/* strcat the extracted keywords into out string	*/
			if (tmpnKeywords != 0 ) strcat(out, tmp_char);
			
			
			if (!strcmp(tmp_char,"|"))
				strcpy(tmpSeparator, tmp_char);
				
		
			nKeywords += tmpnKeywords;
			if (tmpnKeyword1 == 0)
				if (tmpnKeyword2 == 0)
					tmpnKeyword1 = tmpnKeywords;
			
			if (tmpnKeyword1 != 0)
				if (tmpnKeyword2 == 0)
					tmpnKeyword2 = tmpnKeywords;					
						
			tmp_in[0] = '\0';
			ptr_tmp_in = tmp_in;
			
			countKeyword++;
			
			/* if the end of the string exits, break	*/ 
			if(!strcmp(tmp_char,"")) break;

		}	
		else *ptr_tmp_in++ = *ptr_in++;	

	} /* while	*/	

	if( out[strlen(out)-1] == '|' ||  out[strlen(out)-1] == '&') out[strlen(out)-1] = '\0';


	/* check if the correct parentheses exist, remove empty parentheses and return tne number of keywords	*/
	if(IsCorrectNumOfParenthesis(out)==TRUE) 
	{	
		RemoveEmptyParenthesis(out);
		
		/*
			return nKeywords;
		*/
		if (countKeyword != 2)
			return nKeywords;
		else 
		{
			if(!strcmp(tmpSeparator,"|") && tmpnKeyword1 == 1 && tmpnKeyword2 == 1)
				return 1;
			else
				return nKeywords;
		}
	}
	/* if the correct number of parentheses exist, make the out string NULL and return -1	*/ 
	else
	{
		out[0] = '\0';	
		return -1;
	}

}

/* Function : int StemizeKeyword(char* in, char* out)	
 *
 * Description : for stemizing the keyword, read the word from the in string and
 *				 remove the josa or eomi from the word. 
 *
 * Return :	eNOERROR if there is no error
 *			exit if there is some error
 */

int StemizeKeyword(		
	char* in, 			/* IN	pointer of the input string	*/
	char* out )			/* OUT	pointer of the output string after stemizing the keyword */
{
	/* if this function runs at first, initialize the memory handle	*/ 
	if ( korKeywordExtractorHandle == NULL )
	{
		
		if(InitKorKeywordExtractor() == eNOERROR) ;
		else
		{	
			fprintf(stderr, "\nInitKorKeywordExtracHandle() is failed\n");
			exit(-1);
		}					
	}


	/* if the dictionary isn't initialized, initialize the dictionary	*/ 
	if(startIRFlag == 1)
	{

		if(InitDictionaryHashTable() == eNOERROR );
		else
		{	
			fprintf(stderr, "\nInitDictionaryHashTable() is failed\n");
			exit(-1);
		}

		startIRFlag = 0;

	}

	fprintf(stderr, "stem_in = %s\n", in);

	strcpy(out, in);

	if(!IsSpecialNoun((char*)out)) {	
		/* remove the josa or eomi	*/		
		RemoveJosa((char*)out);
	}

	fprintf(stderr, "stem_out = %s\n", out);

	return eNOERROR;
}



/* add out2 argument to store original keywords */
/* out argument just store core keywords */

/* Function : int MakeHighLighgtKeywords(char* in, char* out, char* out2)
 *
 * Description : for hightlightening the keywords, read the word from the in string and
 *				 remove the josa or eomi from the word, 
 *				 and write the words with josa or eomi and without josa or eomi 
 *
 * Return :	eNOERROR if there is no error
 *			exit if there is some error
 */

int MakeHighLightKeywords(
	char* in, 			/* IN	pointer of the input string	*/
	char* out,			/* OUT	pointer of the output string(core keywords) after making the keywords to highlighten */
	char* out2)		/* OUT	pointer of the output string(orig keywords) after making the keywords to highlighten */
{

	char* ptr_in = in;				/* temporary pointer of the in string	*/
	char* ptr_tmp_in;				/* temporary pointer to store the temporary in string from the in string	*/
	char tmp_in[1024];				/* temporary variable to store the temporary in string from the in string	*/
	char tmp_char[10];				/* temporary variable to store the special characters like '&' and '|'	*/
	char tmp_num;					/* temporary variable to store the number after the special characters like '^' and '~'	*/
	
	char word[1024];				/* temporary variable to store the word from the in	*/ 
	char word_withjosa_aster[1024];	/* temporary variable to store the word with josa and asterisk from the in	*/
	char* ptr_word;					/* temporary pointers to remember the position of the word	*/
	char* ptr_word_tmp_in;

	char readOneChar;				/* temporary variable to store one character	*/
	
	Four tmpnOrigKeywords;			/* temporary variable to store the number of original keywords of tmp_in	*/

	Four firstFreeHandle;			/* temporary variable to store the first free handle	*/
	Boolean ngramMode = FALSE;		/* variable to select whether KeywordExtract function process keyword by n-gram or not */ 

	Boolean ngramQueryMode = TRUE; /* variable to decide to extract keywords for query */


	Four i,e;						/* temporary variable and error code	*/		


	if(in == NULL || out == NULL || out2 == NULL)
	{
		fprintf(stderr, "MakeHighLightKeywords function gets bad parameter\n");
		return eBADPARAMETER;
	}
	
	/* if this function runs at first, initialize the memory handle	*/ 
	if ( korKeywordExtractorHandle == NULL )
	{
		
		if(InitKorKeywordExtractor() == eNOERROR) ;
		else
		{	
			fprintf(stderr, "\nInitKorKeywordExtracHandle() is failed\n");
			exit(-1);
		}					
	}

	/* if the dictionary isn't initialized, initialize the dictionary	*/ 
	if(startIRFlag == 1)
	{

		if(InitDictionaryHashTable() == eNOERROR );
		else
		{	
			fprintf(stderr, "\nInitDictionaryHashTable() is failed\n");
			exit(-1);
		}

		startIRFlag = 0;

	}


	/* get ready before the keyword extract runs	*/ 
	
	tmp_in[0] = '\0';
	out[0] = '\0';
	out2[0] = '\0';
	ptr_tmp_in = tmp_in;


	while(*ptr_in==' ') ptr_in++;	
	
	
	while(1)
	{
		if (IJKIM_DEBUG) fprintf(stderr, "*ptr_in = [%s]\n", ptr_in);
		
		/* if the special character and end of the string exits, make the word 	*/
		if(*ptr_in == '|' || *ptr_in =='&' || *ptr_in == '^' || *ptr_in == '~' || *ptr_in == '\0')
		{

			*ptr_tmp_in = '\0';	
			
			while(*ptr_in == ' ') ptr_in++;

			if(*ptr_in == '|' || *ptr_in == '&' || *ptr_in == '+' || *ptr_in == '-')
 			{	
				tmp_char[0] = *ptr_in++;
				tmp_char[1] = '\0';		
			}
			else if(*ptr_in == '^' || *ptr_in == '~')
			{
				tmp_char[0] = *ptr_in++;
				
				while(*ptr_in == ' ') ptr_in++;

				i=0;
				while(*ptr_in >= '0' && *ptr_in <= '9')
				{
					tmp_char[i++] = *ptr_in;
					ptr_in++;
				}
				tmp_char[i] = '\0';
			}
			else strcpy(tmp_char,"");


			/* remove the special character except the parentheses from the word 	*/						
			if( eNOERROR==Remove_SpecialCharForHighlight(tmp_in)); 
			else printf("\nRemove_SpecialChar Error\n");


			EliminateInvisibleChar(tmp_in);
 	   		ConvertSymbolCharExceptAsteriskToSpace(tmp_in);

			if (IJKIM_DEBUG) fprintf(stderr, "after ConvertSymbolCharExceptAsteriskToSpace *tmp_in = [%s]\n", tmp_in);

			if(tmp_in[0] !='\0')
			{
				ptr_word_tmp_in = tmp_in;
				ptr_word = word;

				while(1)
				{
					readOneChar = *ptr_word_tmp_in++;

					if (IJKIM_DEBUG) fprintf(stderr, "readOneChar= [%c]\n", readOneChar);
	
					/* if a space and end of the string exits, make the temporary word 	*/
					if(readOneChar == ' ' || readOneChar == '\0')
					{
						*ptr_word = '\0';	

						while(*ptr_word_tmp_in == ' ') 
						{

							ptr_word_tmp_in++;
						}

						if(word[0] != '\0')
						{
							strcpy(word_withjosa_aster, word);
							if (IJKIM_DEBUG) fprintf(stderr, "after word_withjosa_aster = [%s]\n", word_withjosa_aster); 
							tmpnOrigKeywords++;

							if (IJKIM_DEBUG) fprintf(stderr, "before ConvertAsteriskSymbolCharToSpace *word = [%s]\n", tmp_in);
							ConvertAsteriskSymbolCharToSpace(word);
							if (IJKIM_DEBUG) fprintf(stderr, "after ConvertAsteriskSymbolCharToSpace *word = [%s]\n", tmp_in);


							/*  find the first free handle and initialize that	*/
							nHandle++;
							firstFreeHandle = FindFirstFreeHandle();
							CHECK_ERROR(handle, firstFreeHandle);
	
	
							/* Initialize the first free handle */	
							if(InitKorKeywordExtractBuffer(firstFreeHandle)!=eNOERROR) 
							{
								fprintf(stderr, "\nInitKorKeywordExtractBuffer() is failed\n");
								exit(-1);
							}

							/* set keyword sort mode */
							if(setNgramQueryMode(firstFreeHandle, ngramQueryMode)!=eNOERROR)
							{
								fprintf(stderr, "\nsetNgramQueryMode() is failed\n");
								exit(-1);
							}

							/* Extract the keywords form the words, and then check error	*/
							e = KeywordExtract(firstFreeHandle, word, ngramMode); 

   				 			if( e < eNOERROR )
	   			 			{
		   			 			fprintf(stderr, "\nKeywordExtract() in MakeNgramKeywords failed\n");
			   		 			exit(-1);
							}

							/* strcat the extracted keywords into the out string	*/
							if (korKeywordExtractorHandle[firstFreeHandle].nKeywords)
							{
								for( i=0; i< korKeywordExtractorHandle[firstFreeHandle].nKeywords; i++)
								{
									strcat(out,(char*)korKeywordExtractorHandle[firstFreeHandle].keywordBuffer[i].keyword);
									strcat(out," ");
								}		
								if(out[strlen(out)-1] == ' ') out[strlen(out)-1] = '\0'; 
								strcat(out,"|");

								strcat(out2,word_withjosa_aster);
								strcat(out2,"|");
							}

							/* finialize the handle	*/
							KorExtractor_Final(firstFreeHandle);
						
							ptr_word = word;
							word[0] = '\0';
						} 
					} 
					else *ptr_word++ = readOneChar;
			
					/* if the end of the string, break	*/
					if(readOneChar == '\0') break;
				
				}
				
			}

			tmp_in[0] = '\0';
			ptr_tmp_in = tmp_in;
		

			/* if the end of the string exits, break	*/		
			if(!strcmp(tmp_char,"")) break;

		}
		else *ptr_tmp_in++ = *ptr_in++;	


	} /* while	*/	


	if( out[strlen(out)-1] == '|') out[strlen(out)-1] = '\0';

	if( out2[strlen(out2)-1] == '|') out2[strlen(out2)-1] = '\0';


	return eNOERROR;

}

