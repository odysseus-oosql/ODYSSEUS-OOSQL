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

#include <sys/types.h>
#include <time.h>
#ifndef WIN32
#include <sys/time.h>
#endif
#include "LOM_Internal.h"
#include "LOM.h"

static const Four_Invariable _first_day_of_month[12] = {1,32,60,91,121,152,182,213,244,274, 305,335 };

static const unsigned char _days_in_month[12] = {31,28,31,30,31,30,31,31,30,31,30, 31 };

/* Time-related fuctions */
LOM_TimeZone LOM_GetLocalTimeZone(
	LOM_Handle *handle
)
{
#ifdef WIN32
	_tzset();
	return (LOM_TimeZone)(_timezone/(60*60));
#else
	struct timeval tval;
	struct timezone tz;
	gettimeofday(&tval, &tz);

	return (LOM_TimeZone)(((tz.tz_minuteswest)/60)*(-1));
#endif
}


void LOM_SetCurTime(
	LOM_Handle *handle, 
	LOM_Time *_time,
	LOM_TimeZone tz
)
{
	time_t clk = time(0);
	const struct tm* now;

	now = localtime(&clk);
	_time->_tzHour = tz;
	_time->_tzMinute = 0;
	_time->_Hour = now->tm_hour;
	_time->_Minute = now->tm_min;
	_time->_Second = now->tm_sec;
	_time->_100thSec = 0;
}

unsigned short LOM_GetHour(
	LOM_Handle *handle, 
	LOM_Time *time
)
{
	return time->_Hour - time->_tzHour;
}

unsigned short LOM_GetMinute(
	LOM_Handle *handle, 
	LOM_Time *time
)
{
	return time->_Minute - time->_tzMinute;
}

unsigned short LOM_GetSecond(
	LOM_Handle *handle, 
	LOM_Time *time
)
{
	return time->_Second;
}

Four_Invariable LOM_GetJulDay(
	LOM_Handle *handle, 
	unsigned short m,
	unsigned short d, 
	unsigned short y
)
/*
Convert Gregorian calendar date to the corresponding Julian day number
j.  Algorithm 199 from Communications of the ACM, Volume 6, No. 8,
(Aug. 1963), p. 444.  Gregorian calendar started on Sep. 14, 1752.
This function not valid before that.
*/
{
	UFour_Invariable c, ya;
	if (m > 2)
		m -= 3;
	else {
		m += 9;
		y--;
	} /* else */
	c = y / 100;
	ya = y - 100*c;

	return ((146097*c)>>2) + ((1461*ya)>>2) + (153*m + 2)/5 + d + 1721119;
}

void LOM_GetGregorianDate(
	LOM_Handle *handle, 
	LOM_Date *date,
	unsigned short *mm,
	unsigned short *dd,
	unsigned short *yy
)
/*
Convert a Julian day number to its corresponding Gregorian calendar
date.  Algorithm 199 from Communications of the ACM, Volume 6, No. 8,
(Aug. 1963), p. 444.  Gregorian calendar started on Sep. 14, 1752.
This function not valid before that.
*/
{
	UFour_Invariable j = *date - 1721119;
	UFour_Invariable y = ((j<<2) - 1) / 146097;
	UFour_Invariable d;
	UFour_Invariable m;
	j = (j<<2) - 1 - 146097*y;
	d = j>>2;
	j = ((d<<2) + 3) / 1461;
	d = (d<<2) + 3 - 1461*j;
	d = (d + 4)>>2;
	m = (5*d - 3)/153;
	d = 5*d - 3 - 153*m;
	d = (d + 5)/5;
	y = 100*y + j;
	if (m < 10)
		m += 3;
	else {
		m -= 9;
		y++;
	} /* else */
	*mm = (unsigned short)m;
	*dd = (unsigned short)d;
	*yy = (unsigned short)y;
}


void LOM_SetCurDate(
	LOM_Handle *handle, 
	LOM_Date *date
)
{
	time_t clk = time(0);
	const struct tm* now = localtime(&clk);
	*date = (LOM_Date)LOM_GetJulDay(
			handle, 
			(unsigned short)(now->tm_mon + 1), 
			(unsigned short)(now->tm_mday), 
			(unsigned short)(now->tm_year + 1900));
}

void LOM_SetDate(
	LOM_Handle *handle, 
	unsigned short year,
	unsigned short month,
	unsigned short day,
	LOM_Date *date
)
{
	*date = LOM_GetJulDay(handle, month, day, year);
}


unsigned short LOM_GetYear(
	LOM_Handle *handle, 
	LOM_Date *date
)
{
	unsigned short m, d, y;

	LOM_GetGregorianDate(handle, date, &m, &d, &y);
	return y;

}

unsigned short LOM_GetMonth(
	LOM_Handle *handle, 
	LOM_Date *date
)
{
	unsigned short m, d, y;

	LOM_GetGregorianDate(handle, date, &m, &d, &y);
	return m;
}

unsigned short LOM_GetDay(
	LOM_Handle *handle, 
	LOM_Date *date
)
{
	unsigned short m, d, y;

	LOM_GetGregorianDate(handle, date, &m, &d, &y);
	return d;
}

int LOM_CompareDate(
	LOM_Handle *handle, 
	LOM_Date *date1,
	LOM_Date *date2
)
{
	return *(UFour_Invariable *)date1 - *(UFour_Invariable *)date2;
}

