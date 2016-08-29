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

#ifndef SLIMDOWN_OPENGIS
#include "Geometry.hxx"
#include <math.h>
#include <stdio.h>
#include "OOSQL_Array.hxx"
#define ISPOLYGONCONTAINPOINTOPTIMIZE // optimize contains() by jskim
#define EXCEPTION_IGNORE // ignore exception handling for benchmarking
#define CHECKENDIAN_IGNORE // ignore checking endians between machine and data for benchmarking

template <class ItemType>
ItemType min(ItemType a, ItemType b)
{
	if(a < b) return a;
	else      return b;
}

template <class ItemType>
ItemType max(ItemType a, ItemType b)
{
	if(a > b) return a;
	else      return b;
}

	OOSQL_TCDynStr Scanner::GetNextToken()
	{
		unsigned int pos = m_pos;
		OOSQL_TCDynStr	token(geoMemoryManager);
		char	ch;

		while(pos < m_string.length())
		{
			ch = m_string[pos];
			if(ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n')
				pos ++;
			else
				break;
		}

		// [a-zA-Z]+, (, ), [+-0-9.]+
		ch = m_string[pos];
		if(ch == '(')
		{
			m_lastTokenType = LBRASE;
			token += (const char)ch;
			pos ++;
		}
		else if(ch == ')')
		{
			m_lastTokenType = RBRASE;
			token += (const char)ch;
			pos ++;
		}
		else if(ch == ',')
		{
			m_lastTokenType = COMMA;
			token += (const char)ch;
			pos ++;
		}
		else if(('a' <= ch && ch <= 'z') || ('A' <= ch && ch <= 'Z'))
		{
			m_lastTokenType = WORD;
			while(pos < m_string.length())
			{
				ch = m_string[pos];
				if(('a' <= ch && ch <= 'z') || ('A' <= ch && ch <= 'Z'))
				{
					token += (const char)ch;
					pos ++;
				}
				else
					break;
			}
		}
		else if(('0' <= ch && ch <= '9') || ch == '+' || ch == '-' || ch == '.')
		{
			m_lastTokenType = NUMBER;
			while(pos < m_string.length())
			{
				ch = m_string[pos];
				if(('0' <= ch && ch <= '9') || ch == '+' || ch == '-' || ch == '.')
				{
					token += (const char)ch;
					pos ++;
				}
				else
					break;
			}
		}
		else if(pos < m_string.length())
		{
			m_lastTokenType = NONE;
			throw parse_error("Can't parse given string '" + m_string + "'");
		}

		m_pos = pos;

		return token;
	}

	unsigned int Scanner::CountNumberOfCommasInCurrentBracedBlock()
	{
		unsigned int count = 0;
		unsigned int brace = 0;
		for(unsigned int pos = m_pos; pos < m_string.length(); pos++)
		{
			if(m_string[pos] == ',' && brace == 0)
				count ++;
			else if(m_string[pos] == ')')
			{
				if(brace == 0)
					break;
				brace --;
			}
			else if(m_string[pos] == '(')
				brace ++;
		}
		return count;
	}

	double Scanner::GetNextTokenAsDouble()
	{
		OOSQL_TCDynStr token(geoMemoryManager);
		token = GetNextToken();
		if(GetLastTokenType() != NUMBER)
			throw parse_error("Number is expected");
		return atof(token.str());
	}

	static void _MakeUpper(OOSQL_TCDynStr s)
	{
		for(unsigned int i = 0; i < s.length(); i++)
		{
			if('a' <= s[i] && s[i] <= 'z')
				//s[i] = s[i] - 'a' + 'A';
				s.replace(i, s[i] - 'a' + 'A');
		}
	}

	static int _IsMachineLittleEndian()
	{
		static int endian = -1;
		if(endian == -1)
		{
			int value = 0x00010000;
			if(((char*)&value)[0] == 1)
				// big endian
				endian = 0;
			else
				// little endian
				endian = 1;
		}
		return endian;
	}

	WKBinary::WKBinary():m_pos(0), m_mbrGenerated(false), m_binary(geoMemoryManager)
	{
		if(_IsMachineLittleEndian())
			m_isBinaryLittleEndian = m_isMachineLittleEndian = true;
		else
			m_isBinaryLittleEndian = m_isMachineLittleEndian = false;
	}
	WKBinary::WKBinary(char* s, int size): m_binary(geoMemoryManager)
	{
		Init(s, size);
	}

	void WKBinary::Init(char* s, int size)
	{
		m_pos = 0;
		m_mbrGenerated = false;
        m_binary.resize(size);
        memcpy((char*)m_binary.str(), s, size);
		if(_IsMachineLittleEndian())
			m_isBinaryLittleEndian = m_isMachineLittleEndian = true;
		else
			m_isBinaryLittleEndian = m_isMachineLittleEndian = false;
	}

	inline void WKBinary::Append(char value)
	{
		m_binary += ((char*)&value)[0];
	}
	inline void WKBinary::Append(unsigned int value)
	{
		m_binary += ((char*)&value)[0];
		m_binary += ((char*)&value)[1];
		m_binary += ((char*)&value)[2];
		m_binary += ((char*)&value)[3];
	}
	inline void WKBinary::Append(double value)
	{
		m_binary += ((char*)&value)[0];
		m_binary += ((char*)&value)[1];
		m_binary += ((char*)&value)[2];
		m_binary += ((char*)&value)[3];
		m_binary += ((char*)&value)[4];
		m_binary += ((char*)&value)[5];
		m_binary += ((char*)&value)[6];
		m_binary += ((char*)&value)[7];
	}
	inline char WKBinary::ReadNextChar()
	{
		char value = *(char*)(m_binary.str() + m_pos);
		m_pos += 1;
		return value;
	}
	inline static void _SwapBytePosition(char* p, int size)
	{
#ifndef EXCEPTION_IGNORE
		if(size % 2)
			throw runtime_error("Can't swap byte positions, the size should be even number.");
#endif
		for(int i = 0; i < size / 2; i++)
		{
			char t = p[i];
			p[i] = p[size - i - 1];
			p[size - i - 1] = t;
		}
	}

	inline unsigned int WKBinary::ReadNextUnsignedInt()
	{
		unsigned int value;
		memcpy(&value, m_binary.str() + m_pos, 4);
		m_pos += 4;
#ifndef CHECKENDIAN_IGNORE
		if(m_isMachineLittleEndian != m_isBinaryLittleEndian)
		{
			// swap byte position
			_SwapBytePosition((char*)&value, 4);
		}
#endif
		return value;
	}
	inline double WKBinary::ReadNextDouble()
	{
		double value;
		memcpy(&value, m_binary.str() + m_pos, 8);
		m_pos += 8;
#ifndef CHECKENDIAN_IGNORE
		if(m_isMachineLittleEndian != m_isBinaryLittleEndian)
		{
			// swap byte position
			_SwapBytePosition((char*)&value, 8);
		}
#endif
		return value;
	}
	inline unsigned int WKBinary::GetGeometryType()
	{
		ResetReadPos(1);
		return ReadNextUnsignedInt();
	}
	char* WKBinary::GetGeometryTypeName()
	{
		switch(GetGeometryType())
		{
		case WKBPOINT:
			return (char*)"Point";
		case WKBLINESTRING:
			return (char*)"LineString";
		case WKBPOLYGON:
			return (char*)"Polygon";
		case WKBMULTIPOINT:
			return (char*)"MultiPoint";
		case WKBMULTILINESTRING:
			return (char*)"MultiLineString";
		case WKBMULTIPOLYGON:
			return (char*)"MultiPolygon";
		case WKBGEOMETRYCOLLECTION:
			return (char*)"GeometryCollection";
		default:
			return (char*)"";
		}
	}

	Point WKBinary::GetPoint()
	{
#ifndef EXCEPTION_IGNORE
		if(GetGeometryType() == WKBPOINT)
		{
#endif
			ResetReadPos(5);
			float x = ReadNextDouble();
			float y = ReadNextDouble();
			return Point(x, y);
#ifndef EXCEPTION_IGNORE
		}
		else
			throw runtime_error("WKBPOINT is expected");
#endif
	}

	inline Point WKBinary::GetPoint(int ithPoint)
	{
#ifndef EXCEPTION_IGNORE
		if(GetGeometryType() == WKBLINESTRING)
		{
			if(ithPoint >= GetNumPoints())
				throw range_error("out of index");
#endif
			ResetReadPos(9 + ithPoint * 16);
			float x = ReadNextDouble();
			float y = ReadNextDouble();
			return Point(x, y);
#ifndef EXCEPTION_IGNORE
		}
		else
			throw runtime_error("WKBLINESTRING is expected");
#endif
	}

	WKBinary WKBinary::GetPointAsWKBinary(int ith)
	{
		Point point = GetPoint(ith);
		WKBinary wkb;
		wkb.Append((char)_IsMachineLittleEndian());
		wkb.Append((unsigned int)WKBPOINT);
		double x, y;
		x = point.GetX();
		y = point.GetY();
		wkb.Append(x);
		wkb.Append(y);
		return wkb;
	}

	int WKBinary::GetNumPoints()
	{
#ifndef EXCEPTION_IGNORE
		switch(GetGeometryType())
		{
		case WKBPOINT:
			return 1;
		case WKBLINESTRING:
#endif
			ResetReadPos(5);
			return ReadNextUnsignedInt();
#ifndef EXCEPTION_IGNORE
		default:
			throw runtime_error("Can't enumerate the number of points");
		}
#endif
	}

	int	WKBinary::GetNumPoints(int jthRing)
	{
		int			 numPoints;
		unsigned int offset;
#ifndef EXCEPTION_IGNORE
		switch(GetGeometryType())
		{
		case WKBPOLYGON:
			if(jthRing >= GetNumRings())
				throw range_error("out of index");
#endif
			offset = 9;
			for(int j = 0; j < jthRing; j++)
			{
				ResetReadPos(offset);
				numPoints = ReadNextUnsignedInt();
				offset += 4 + numPoints * 16;
			}
			ResetReadPos(offset);
			numPoints = ReadNextUnsignedInt();
			return numPoints;
#ifndef EXCEPTION_IGNORE
		default:
			throw runtime_error("Can't enumerate the number of points");
		}
#endif
	}

	inline int WKBinary::GetNumRings()
	{
#ifndef EXCEPTION_IGNORE
		switch(GetGeometryType())
		{
		case WKBPOLYGON:
#endif
			ResetReadPos(5);
			return ReadNextUnsignedInt();
#ifndef EXCEPTION_IGNORE
		default:
			throw runtime_error("Can't enumerate the number of rings");
		}
#endif
	}

	Point WKBinary::GetPoint(int jthRing, int ithPoint)
	{
		int			 numPoints;
		unsigned int offset;
#ifndef EXCEPTION_IGNORE
		if(GetGeometryType() == WKBPOLYGON)
		{
			if(jthRing >= GetNumRings())
				throw range_error("out of index");
#endif
			offset = 9;
			for(int j = 0; j < jthRing; j++)
			{
				ResetReadPos(offset);
				numPoints = ReadNextUnsignedInt();
				offset += 4 + numPoints * 16;
			}
			ResetReadPos(offset);
			numPoints = ReadNextUnsignedInt();
#ifndef EXCEPTION_IGNORE
			if(ithPoint >= numPoints)
				throw range_error("out of index");
#endif
			ResetReadPos(offset + 4 + ithPoint * 16);
			float x = ReadNextDouble();
			float y = ReadNextDouble();
			return Point(x, y);
#ifndef EXCEPTION_IGNORE
		}
		else
			throw runtime_error("WKBPOLYGON is expected");
#endif
	}

	void WKBinary::GetMBR(float* xmin, float* ymin, float* xmax, float* ymax)
	{
		if(!m_mbrGenerated)
		{
			switch(GetGeometryType())
			{
			case WKBPOINT:
				{
					Point point = GetPoint();
					m_xmin = m_xmax = point.GetX();
					m_ymin = m_ymax = point.GetY();
					break;
				}
			case WKBLINESTRING:
				{
					int		numPoints = GetNumPoints();
					Point	point = GetPoint(0);

					m_xmin = m_xmax = point.GetX();
					m_ymin = m_ymax = point.GetY();
					for(int i = 0; i < numPoints; i++)
					{
						point = GetPoint(i);
						float x = point.GetX();
						float y = point.GetY();
						m_xmin = min(m_xmin, x);
						m_ymin = min(m_ymin, y);
						m_xmax = max(m_xmax, x);
						m_ymax = max(m_ymax, y);
					}
					break;
				}
			case WKBPOLYGON:
				{
					int		numRings = GetNumRings();
					Point	point    = GetPoint(0, 0);

					m_xmin = m_xmax = point.GetX();
					m_ymin = m_ymax = point.GetY();
					for(int i = 0; i < numRings; i++)
					{
						int numPoints = GetNumPoints(i);
						for(int j = 0; j < numPoints; j++)
						{
							point = GetPoint(i, j);
							float x = point.GetX();
							float y = point.GetY();
							m_xmin = min(m_xmin, x);
							m_ymin = min(m_ymin, y);
							m_xmax = max(m_xmax, x);
							m_ymax = max(m_ymax, y);
						}
					}
					break;
				}
			case WKBMULTIPOINT:
			case WKBMULTILINESTRING:
			case WKBMULTIPOLYGON:
			case WKBGEOMETRYCOLLECTION:
				{
					int		 numGeometries = GetNumGeometries();
					float	 x1, y1, x2, y2;
					WKBinary wkb = GetSubWKBinary(0);
					wkb.GetMBR(&m_xmin, &m_ymin, &m_xmax, &m_ymax);
					for(int i = 1; i < numGeometries; i++)
					{
						wkb = GetSubWKBinary(i);
						wkb.GetMBR(&x1, &y1, &x2, &y2);
						m_xmin = min(m_xmin, x1);
						m_ymin = min(m_ymin, y1);
						m_xmax = max(m_xmax, x2);
						m_ymax = max(m_ymax, y2);
					}
					break;
				}
#ifndef EXCEPTION_IGNORE
			default:
				throw runtime_error("unhandled error");
#endif
			}
			m_mbrGenerated = true;
		}

		*xmin = m_xmin; *ymin = m_ymin; *xmax = m_xmax; *ymax = m_ymax;
	}

	int WKBinary::GetNumGeometries()
	{
#ifndef EXCEPTION_IGNORE
		switch(GetGeometryType())
		{
		case WKBMULTIPOINT:
		case WKBMULTILINESTRING:
		case WKBMULTIPOLYGON:
		case WKBGEOMETRYCOLLECTION:
#endif
			ResetReadPos(5);
			return ReadNextUnsignedInt();
#ifndef EXCEPTION_IGNORE
		default:
			throw runtime_error("Can't enumerate the number of geometries");
		}
#endif
	}
	
	int WKBinary::GetSubBinaryLength(unsigned int offset)
	{
		ResetReadPos(offset + 1);
		int type = ReadNextUnsignedInt();
		int numPoints, numRings, numGeometries;
		int length = 0;
		switch(type)
		{
		case WKBPOINT:
			length = 1 + 4 + 16;
			break;
		case WKBLINESTRING:
			ResetReadPos(offset + 1 + 4);
			numPoints = ReadNextUnsignedInt();
			length = 1 + 4 + 4 + numPoints * 16;
			break;
		case WKBPOLYGON:
			ResetReadPos(offset + 1 + 4);
			numRings = ReadNextUnsignedInt();
			length = 1 + 4 + 4;
			for(int i = 0; i < numRings; i++)
			{
				ResetReadPos(offset + length);
				numPoints = ReadNextUnsignedInt();
				length += 4 + 16 * numPoints;
			}
			break;
		case WKBMULTIPOINT:
		case WKBMULTILINESTRING:
		case WKBMULTIPOLYGON:
		case WKBGEOMETRYCOLLECTION:
			ResetReadPos(offset + 1 + 4);
			numGeometries = ReadNextUnsignedInt();
			length += 1 + 4 + 4;
			for(int i = 0; i < numGeometries; i++)
				length += GetSubBinaryLength(offset + length);
			break;
#ifndef EXCEPTION_IGNORE
		default:
			throw runtime_error("unhandled case");
#endif
		}
		return length;
	}
	WKBinary WKBinary::GetSubWKBinary(int ith)
	{
		int type = GetGeometryType();
#ifndef EXCEPTION_IGNORE
		switch(type)
		{
		case WKBMULTIPOINT:
		case WKBMULTILINESTRING:
		case WKBMULTIPOLYGON:
		case WKBGEOMETRYCOLLECTION:
			{
#endif
				int numGeometries = GetNumGeometries();
#ifndef EXCEPTION_IGNORE
				if(ith >= numGeometries)
					throw range_error("out of index");
#endif
				int offset = 1 + 4 + 4;
				for(int i = 0; i < ith; i++)
					offset += GetSubBinaryLength(offset);
				return WKBinary((char*)m_binary.str() + offset, GetSubBinaryLength(offset));
#ifndef EXCEPTION_IGNORE
			}
		default:
			throw runtime_error("WKBMULTIPOINT, WKBMULTILINESTRING, WKBMULTIPOLYGON, or WKBGEOMETRYCOLLECTION is expected");
		}
#endif
	}

	void _WKTextPointToWKBinary(Scanner& scanner, WKBinary& wkb, bool makeHeader=false)
	{
		if(makeHeader)
		{
			wkb.Append((char)_IsMachineLittleEndian());
			wkb.Append((unsigned int)WKBPOINT);
		}
		double x, y;
		x = scanner.GetNextTokenAsDouble();
		y = scanner.GetNextTokenAsDouble();
		wkb.Append(x);
		wkb.Append(y);
	}
	void _WKTextPointStringToWKBinary(Scanner& scanner, WKBinary& wkb)
	{
		unsigned int numPoints = scanner.CountNumberOfCommasInCurrentBracedBlock() + 1;
		wkb.Append(numPoints);

		for(unsigned int i = 0; i < numPoints; i++)
		{
			_WKTextPointToWKBinary(scanner, wkb);
			if(i < numPoints - 1 && scanner.GetNextToken() != ",")
				throw parse_error("Comma is expected");
		}
	}
	void _WKTextLineStringToWKBinary(Scanner& scanner, WKBinary& wkb)
	{
		wkb.Append((char)_IsMachineLittleEndian());
		wkb.Append((unsigned int)WKBLINESTRING);

		if(scanner.GetNextToken() != "(")
			throw parse_error("Left brace is expected");

		_WKTextPointStringToWKBinary(scanner, wkb);

		if(scanner.GetNextToken() != ")")
			throw parse_error("Right brace is expected");
	}

	void _WKTextPolygonToWKBinary(Scanner& scanner, WKBinary& wkb)
	{
		wkb.Append((char)_IsMachineLittleEndian());
		wkb.Append((unsigned int)WKBPOLYGON);

		if(scanner.GetNextToken() != "(")
			throw parse_error("Left brace is expected");

		unsigned int numRings = scanner.CountNumberOfCommasInCurrentBracedBlock() + 1;
		wkb.Append(numRings);

		for(unsigned int i = 0; i < numRings; i++)
		{
			if(scanner.GetNextToken() != "(")
				throw parse_error("Left brace is expected");

			_WKTextPointStringToWKBinary(scanner, wkb);

			if(scanner.GetNextToken() != ")")
				throw parse_error("Right brace is expected");

			if(i < numRings - 1 && scanner.GetNextToken() != ",")
				throw parse_error("Comma is expected");
		}

		if(scanner.GetNextToken() != ")")
			throw parse_error("Right brace is expected");
	}

	void _WKTextToWKBinary(Scanner& scanner, WKBinary& wkb)
	{
		OOSQL_TCDynStr token(geoMemoryManager);
		token = scanner.GetNextToken();
		_MakeUpper(token);
		if(token == "POINT")
		{
			if(scanner.GetNextToken() != "(")
				throw parse_error("Left brace is expected");

			_WKTextPointToWKBinary(scanner, wkb, true);

			if(scanner.GetNextToken() != ")")
				throw parse_error("Right brace is expected");
		}
		else if(token == "LINESTRING")
		{
			_WKTextLineStringToWKBinary(scanner, wkb);
		}
		else if(token == "POLYGON")
		{
			_WKTextPolygonToWKBinary(scanner, wkb);
		}
		else if(token == "MULTIPOINT")
		{
			wkb.Append((char)_IsMachineLittleEndian());
			wkb.Append((unsigned int)WKBMULTIPOINT);

			if(scanner.GetNextToken() != "(")
				throw parse_error("Left brace is expected");

			unsigned int numPoints = scanner.CountNumberOfCommasInCurrentBracedBlock() + 1;
			wkb.Append(numPoints);

			for(unsigned int i = 0; i < numPoints; i++)
			{
				_WKTextPointToWKBinary(scanner, wkb, true);
				if(i < numPoints - 1 && scanner.GetNextToken() != ",")
					throw parse_error("Comma is expected");
			}

			if(scanner.GetNextToken() != ")")
				throw parse_error("Right brace is expected");
		}
		else if(token == "MULTILINESTRING")
		{
			wkb.Append((char)_IsMachineLittleEndian());
			wkb.Append((unsigned int)WKBMULTILINESTRING);

			if(scanner.GetNextToken() != "(")
				throw parse_error("Left brace is expected");

			unsigned int numLines = scanner.CountNumberOfCommasInCurrentBracedBlock() + 1;
			wkb.Append(numLines);

			for(unsigned int i = 0; i < numLines; i++)
			{
				_WKTextLineStringToWKBinary(scanner, wkb);

				if(i < numLines - 1 && scanner.GetNextToken() != ",")
					throw parse_error("Comma is expected");
			}

			if(scanner.GetNextToken() != ")")
				throw parse_error("Right brace is expected");
		}
		else if(token == "MULTIPOLYGON")
		{
			wkb.Append((char)_IsMachineLittleEndian());
			wkb.Append((unsigned int)WKBMULTIPOLYGON);

			if(scanner.GetNextToken() != "(")
				throw parse_error("Left brace is expected");

			unsigned int numPolys = scanner.CountNumberOfCommasInCurrentBracedBlock() + 1;
			wkb.Append(numPolys);

			for(unsigned int i = 0; i < numPolys; i++)
			{
				_WKTextPolygonToWKBinary(scanner, wkb);

				if(i < numPolys - 1 && scanner.GetNextToken() != ",")
					throw parse_error("Comma is expected");
			}

			if(scanner.GetNextToken() != ")")
				throw parse_error("Right brace is expected");
		}
		else if(token == "GEOMETRYCOLLECTION")
		{
			wkb.Append((char)_IsMachineLittleEndian());
			wkb.Append((unsigned int)WKBGEOMETRYCOLLECTION);

			if(scanner.GetNextToken() != "(")
				throw parse_error("Left brace is expected");

			unsigned int numGeoms = scanner.CountNumberOfCommasInCurrentBracedBlock() + 1;
			wkb.Append(numGeoms);

			for(unsigned int i = 0; i < numGeoms; i++)
			{
				_WKTextToWKBinary(scanner, wkb);

				if(i < numGeoms - 1 && scanner.GetNextToken() != ",")
					throw parse_error("Comma is expected");
			}

			if(scanner.GetNextToken() != ")")
				throw parse_error("Right brace is expected");
		}
		else
			throw parse_error("Unknown geometry type");
	}

	WKBinary WKTextToWKBinary(char* s)
	{
		Scanner		scanner(s);
		WKBinary	wkb;

		_WKTextToWKBinary(scanner, wkb);

		return wkb;
	}

	static OOSQL_TCDynStr _DoubleToString(double value)
	{
		char buffer[256];
		sprintf(buffer, "%.2f", value);
		return buffer;
	}

	OOSQL_TCDynStr _WKBinaryPointToWKText(WKBinary& wkb)
	{
		OOSQL_TCDynStr text(geoMemoryManager);
		double x = wkb.ReadNextDouble();
		double y = wkb.ReadNextDouble();
		text += _DoubleToString(x);
		text += " ";
		text += _DoubleToString(y);
		return text;
	}
	OOSQL_TCDynStr _WKBinaryPointStringToWKText(WKBinary& wkb)
	{
		OOSQL_TCDynStr text(geoMemoryManager);
		unsigned int numPoints = wkb.ReadNextUnsignedInt();
		for(unsigned int i = 0; i < numPoints; i++)
		{
			text += _WKBinaryPointToWKText(wkb);
			if(i < numPoints - 1)
				text += ", ";
		}
		return text;
	}

	OOSQL_TCDynStr _WKBinaryToWKText(WKBinary& wkb, bool outputHeader)
	{
		OOSQL_TCDynStr   text(geoMemoryManager);

		wkb.SetBinaryBeLittleEndian(wkb.ReadNextCharAsBool());
		int type   = wkb.ReadNextUnsignedInt();

		switch(type)
		{
		case WKBPOINT:
			{
				if(outputHeader) text += "POINT(";
				text += _WKBinaryPointToWKText(wkb);
				if(outputHeader) text += ")";
				break;
			}
		case WKBLINESTRING:
			{
				if(outputHeader) text += "LINESTRING(";
				text += _WKBinaryPointStringToWKText(wkb);
				if(outputHeader) text += ")";
				break;
			}
		case WKBPOLYGON:
			{
				if(outputHeader) text += "POLYGON(";
				unsigned int numRings = wkb.ReadNextUnsignedInt();
				for(unsigned int i = 0; i < numRings; i++)
				{
					text += "(";
					text += _WKBinaryPointStringToWKText(wkb);
					text += ")";
					if(i < numRings - 1)
						text += ", ";
				}
				if(outputHeader) text += ")";
				break;
			}
		case WKBMULTIPOINT:
			{
				if(outputHeader) text += "MULTIPOINT(";
				unsigned int numPoints = wkb.ReadNextUnsignedInt();
				for(unsigned int i = 0; i < numPoints; i++)
				{
					wkb.SetBinaryBeLittleEndian(wkb.ReadNextCharAsBool());
					int type = wkb.ReadNextUnsignedInt();
					text += _WKBinaryPointToWKText(wkb);
					if(i < numPoints - 1)
						text += ", ";
				}
				if(outputHeader) text += ")";
				break;
			}
		case WKBMULTILINESTRING:
			{
				if(outputHeader) text += "MULTILINESTRING(";
				unsigned int numLines = wkb.ReadNextUnsignedInt();
				for(unsigned int i = 0; i < numLines; i++)
				{
					text += _WKBinaryToWKText(wkb, false);
					if(i < numLines - 1)
						text += ", ";
				}
				if(outputHeader) text += ")";
				break;
			}
		case WKBMULTIPOLYGON:
			{
				if(outputHeader) text += "MULTIPOLYGON(";
				unsigned int numPolys = wkb.ReadNextUnsignedInt();
				for(unsigned int i = 0; i < numPolys; i++)
				{
					text += _WKBinaryToWKText(wkb, false);
					if(i < numPolys - 1)
						text += ", ";
				}
				if(outputHeader) text += ")";
				break;
			}
		case WKBGEOMETRYCOLLECTION:
			{
				text += "GEOMETRYCOLLECTION(";
				unsigned int numGeoms = wkb.ReadNextUnsignedInt();
				for(unsigned int i = 0; i < numGeoms; i++)
				{
					text += _WKBinaryToWKText(wkb, true);
					if(i < numGeoms - 1)
						text += ", ";
				}
				text += ")";
				break;
			}
#ifndef EXCEPTION_IGNORE
		default:
			throw runtime_error("Unknown geometry type");
#endif
		}
		return text;
	}

	OOSQL_TCDynStr WKBinaryToWKText(WKBinary& wkb)
	{
		wkb.ResetReadPos(0);
		return _WKBinaryToWKText(wkb, true);
	}

	// TODO : 구현해야 할 부분들
	WKBinary WKBinary::Envelope()
	{
		return WKBinary();
	}
    WKBinary WKBinary::Boundary()
	{
		return WKBinary();
	}
    int WKBinary::IsEmpty()
	{
		return 0;
	}
    int WKBinary::IsSimple()
	{
		return 0;
	}
    int WKBinary::IsClosed()
	{
		return 0;
	}
    int WKBinary::IsRing()
	{
		return 0;
	}
    double WKBinary::Length()
	{
		return 0;
	}
    WKBinary WKBinary::ExteriorRing()
	{
		return WKBinary();
	}
    WKBinary WKBinary::InteriorRingN(int ith)
	{
		return WKBinary();
	}
    int WKBinary::NumInteriorRings()
	{
		return 0;
	}
    double WKBinary::Area()
	{
		if(GetGeometryType() == WKBPOLYGON)
		{
			double sum = 0;
			int nPoints = GetNumPoints(0);
			for(int i = 0; i < (nPoints - 1); i++)
			{
				Point point1 = GetPoint(0, i);
				Point point2 = GetPoint(0, i+1);
				sum = sum + (point1.m_x + point2.m_x) * (point2.m_y - point1.m_y);
			}
			sum = fabs(sum);
			double area = sum / 2;
			return area;
		}
		return 0;
	}
    WKBinary WKBinary::Centroid()
	{
		return WKBinary();
	}
    WKBinary WKBinary::PointOnSurface()
	{
		return WKBinary();
	}
    WKBinary WKBinary::GeometryN(int ith)
	{
		return WKBinary();
	}
    WKBinary WKBinary::Buffer()
	{
		return WKBinary();
	}

// helper for finding pivot point of graham's scan
#define FIND_LOWEST(p1, p2) (p1.m_y!=p2.m_y?p1.m_y-p2.m_y:p1.m_x-p2.m_x)

// 점 p가 선분 s=(sp1, sp2)의 어느쪽(시계/반시계)에 있는지 판별
// positive if CCW
#define CCW(p, sp1, sp2) ((p.m_x - sp1.m_x) * (sp2.m_y - sp1.m_y) - (p.m_y - sp1.m_y) * (sp2.m_x - sp1.m_x))

int angle_comparor(double px1, double py1, double px2, double py2)
{
	double angle1, angle2;
	angle1 = atan2(py1, px1);
	angle2 = atan2(py2, px2);
	return angle1-angle2;
}

// Graham's scan function
void graham_scan(int n, int *m, Point *p)
{
	int i, j, min;
	Point temp, lp1, lp2;

	min = 0;
	for(i = 1; i < n; i++)
	{
		if(FIND_LOWEST(p[i], p[min]) < 0) min = i;
	}
	temp = p[0];
	p[0] = p[min];
	p[min] = temp;

	// angle sort (use bubble, since we think that obj<20) 
	for(i = 1; i < n; i++)
	{
		for(j = 1; j < n-1; j++)
		{
			if (angle_comparor(p[j].m_x - p[0].m_x,	  p[j].m_y - p[0].m_y, 
							   p[j+1].m_x - p[0].m_x, p[j+1].m_y - p[0].m_y	) > 0)
			{
				temp = p[j];
				p[j] = p[j+1];
				p[j+1] = temp;
			}
		}
	}

	*m = 1;
	for(i = 2; i < n; i++)	{
		lp2 = p[i];	
		while (*m > 0) {
			lp1 = p[*m - 1];
			if (CCW(p[*m], lp1, lp2) <= 0) (*m)--;
			else break;
		}
		(*m)++;
		temp = p[*m];
		p[*m] = p[i];
		p[i] = temp;
	}
}

    WKBinary WKBinary::ConvexHull()
	{
		WKBinary wkb;
		Point* points;
		int nConvexPoints;
		if(GetGeometryType() == WKBPOINT)
		{
	        Point point = GetPoint();
        	wkb.Append((char)_IsMachineLittleEndian());
	        wkb.Append((unsigned int)WKBPOINT);
    	    double x, y;
        	x = point.GetX();
	        y = point.GetY();
    	    wkb.Append(x);
        	wkb.Append(y);
		}
		else if(GetGeometryType() == WKBLINESTRING)
		{
            int nPoints = GetNumPoints();
			points = (Point*)malloc(nPoints*sizeof(Point));
            for(int i = 0; i < nPoints; i++)
            {
                points[i] = GetPoint(i);
            }
			graham_scan(nPoints, &nConvexPoints, points);
        	wkb.Append((char)_IsMachineLittleEndian());
	        wkb.Append((unsigned int)WKBPOLYGON);
			wkb.Append((unsigned int)1); //numRings
			wkb.Append((unsigned int)(nConvexPoints+1)); //numPoints
			for (int i = 0; i < nConvexPoints; i++)
			{
				wkb.Append((double)points[i].m_x);
				wkb.Append((double)points[i].m_y);
			}
			wkb.Append((double)points[0].m_x);
			wkb.Append((double)points[0].m_y);
			free(points);
		}
		else if(GetGeometryType() == WKBPOLYGON)
		{
			int nRings = GetNumRings();
			int nTotalPoints = 0;
			for(int j = 0; j < nRings; j++)
			{
				nTotalPoints += (GetNumPoints(j)-1);
			}
			points = (Point*)malloc(nTotalPoints*sizeof(Point));
			int iPoints = 0;
			for(int j = 0; j < nRings; j++)
			{
				int nPoints = GetNumPoints(j);
				for(int i = 0; i < nPoints - 1; i++) 
				{
					points[iPoints++] = GetPoint(j, i);
				}
			}
			graham_scan(nTotalPoints, &nConvexPoints, points);
        	wkb.Append((char)_IsMachineLittleEndian());
	        wkb.Append((unsigned int)WKBPOLYGON);
			wkb.Append((unsigned int)1); //numRings
			wkb.Append((unsigned int)(nConvexPoints+1)); //numPoints
			for (int i = 0; i < nConvexPoints; i++)
			{
				wkb.Append((double)points[i].m_x);
				wkb.Append((double)points[i].m_y);
			}
			wkb.Append((double)points[0].m_x);
			wkb.Append((double)points[0].m_y);
			free(points);
		}
		return wkb;
	}
    WKBinary WKBinary::Difference(WKBinary& wkb)
	{
		return WKBinary();
	}
    WKBinary WKBinary::Intersection(WKBinary& wkb)
	{
		return WKBinary();
	}
    WKBinary WKBinary::SymDifference(WKBinary& wkb)
	{
		return WKBinary();
	}
    WKBinary WKBinary::Union(WKBinary& wkb)
	{
		return WKBinary();
	}
	bool _IsPolygonContainPoint(WKBinary& polygon, Point pointB);
    int WKBinary::Contains(WKBinary& wkb)
	{	
		float xmin1, ymin1, xmax1, ymax1;
		float xmin2, ymin2, xmax2, ymax2;

		GetMBR(&xmin1, &ymin1, &xmax1, &ymax1);
		wkb.GetMBR(&xmin2, &ymin2, &xmax2, &ymax2);

		if(xmin1 <= xmin2 && xmax2 <= xmax1 && ymin1 <= ymin2 && ymax2 <= ymax1)
		{
			if(GetGeometryType() == WKBPOLYGON)
		    {
		        if(wkb.GetGeometryType() == WKBPOINT)
		        {
		            Point p = wkb.GetPoint();
		            if(_IsPolygonContainPoint(*this, p))
		                return 1;
		            else
		                return 0;
		        }
		        else if(wkb.GetGeometryType() == WKBLINESTRING)
		        {
		            int nPoints = wkb.GetNumPoints();
		            for(int i = 0; i < nPoints; i++)
		            {
		                Point p = wkb.GetPoint(i);
		                if(!_IsPolygonContainPoint(*this, p))
		                    return 0;
		            }
		            return 1;
		        }
		        else if(wkb.GetGeometryType() == WKBPOLYGON)
		        {
		            int nRings = wkb.GetNumRings();
		            for(int j = 0; j < nRings; j++)
		            {
		                int nPoints = wkb.GetNumPoints(j);
		                for(int i = 0; i < nPoints; i++)
		                {
		                    Point p = wkb.GetPoint(j, i);
		                    if(!_IsPolygonContainPoint(*this, p))
		                        return 0;
		                }
		            }
		            return 1;
		        }
		        else
		            return 0;
		    }
		    else
		        return 0;
        }
		else
			return 0;
	}
	bool _LineCrossLine(Point* lineA, Point* lineB);
	int _IsLineCrossGeometry(Point* l1, WKBinary& wkb)
	{
	    if(wkb.GetGeometryType() == WKBLINESTRING)
	    {
            int nPoints = wkb.GetNumPoints();
            if(nPoints == 0)
                return 0;
            Point l2[2];
            l2[0] = wkb.GetPoint(0);
            for(int i = 1; i < nPoints; i++)
            {
                l2[1] = wkb.GetPoint(i);
                if(_LineCrossLine(l1, l2))
                    return 1;
                l2[0] = l2[1];
            }
            return 0;
	    }
	    else if(wkb.GetGeometryType() == WKBPOLYGON)
	    {
            int nRings = wkb.GetNumRings();
            for(int j = 0; j < nRings; j++)
            {
                int nPoints = wkb.GetNumPoints(j);
                if(nPoints > 0)
                {
                    Point l2[2];
                    l2[0] = wkb.GetPoint(j, 0);
                    for(int i = 1; i < nPoints; i++)
                    {
                        l2[1] = wkb.GetPoint(j, i);
    	                if(_LineCrossLine(l1, l2))
    	                    return 1;
    	                l2[0] = l2[1];
                    }
                    l2[1] = wkb.GetPoint(j, 0);
	                if(_LineCrossLine(l1, l2))
	                    return 1;
                }
            }
            return 0;
	    }
	    else
	        return 0;
	}
	
    int WKBinary::Crosses(WKBinary& wkb)
	{
		float xmin1, ymin1, xmax1, ymax1;
		float xmin2, ymin2, xmax2, ymax2;

		GetMBR(&xmin1, &ymin1, &xmax1, &ymax1);
		wkb.GetMBR(&xmin2, &ymin2, &xmax2, &ymax2);

		if(xmax1 < xmin2 || xmax2 < xmin1 || ymax1 < ymin2 || ymax2 < ymin1)
			return 0;

	    if(GetGeometryType() == WKBLINESTRING)
	    {
            int nPoints = GetNumPoints();
            if(nPoints == 0)
                return 0;
            Point l1[2];
            l1[0] = GetPoint(0);
            for(int i = 1; i < nPoints; i++)
            {
                l1[1] = GetPoint(i);
                if(_IsLineCrossGeometry(l1, wkb))
                    return 1;
                l1[0] = l1[1];
            }
            return 0;
	    }
		else if(GetGeometryType() == WKBPOLYGON)
	    {
            int nRings = GetNumRings();
            for(int j = 0; j < nRings; j++)
            {
                int nPoints = GetNumPoints(j);
                if(nPoints > 0)
                {
                    Point l1[2];
                    l1[0] = GetPoint(j, 0);
                    for(int i = 1; i < nPoints; i++)
                    {
                        l1[1] = GetPoint(j, i);
    	                if(_IsLineCrossGeometry(l1, wkb))
    	                    return 1;
    	                l1[0] = l1[1];
                    }
                    l1[1] = GetPoint(j, 0);
	                if(_IsLineCrossGeometry(l1, wkb))
	                    return 1;
                }
            }
            return 0;
	    }
	    else
	        return 0;
	}
    int WKBinary::Disjoint(WKBinary& wkb)
	{
		if(!Intersects(wkb) && !Contains(wkb) && !wkb.Contains(*this))
		    return 1;
		else
		    return 0;		
	}
	bool _LineCrossLine(Point* lineA, Point* lineB)
	{
		double ax1, ay1, ax2, ay2;
		double bx1, by1, bx2, by2;
		double a1, b1;          /* c1 * y = a1 * x + b1 */
		int   c1;
		double a2, b2;          /* c2 * y = a2 * x + b2 */
		int   c2;
		double x, y;

		ax1 = lineA[0].m_x; ay1 = lineA[0].m_y;
		ax2 = lineA[1].m_x; ay2 = lineA[1].m_y;

		bx1 = lineB[0].m_x; by1 = lineB[0].m_y;
		bx2 = lineB[1].m_x; by2 = lineB[1].m_y;

		if(ax1 == ax2)
		{
			c1 = 0;
			a1 = 1;
			b1 = -ax1;
		}
		else
		{
			c1 = 1;
			a1 = (ay1 - ay2) / (ax1 - ax2);
			b1 = ay1 - a1 * ax1;
		}

		if(bx1 == bx2)
		{
			c2 = 0;
			a2 = 1;
			b2 = -bx1;
		}
		else
		{
			c2 = 1;
			a2 = (by1 - by2) / (bx1 - bx2);
			b2 = by1 - a2 * bx1;
		}

		if(c1 && c2)           
		{
			if(a1 == a2)
			{
				if(b1 != b2)    
					return false;
				else
				{
					if(ax2 < bx1 || bx2 < ax1)
						return false;
					else
						return true;
				}
			}

			x = (b2 - b1) / (a1 - a2);
			y = a1 * x + b1;

			if(min(ay1, ay2) <= y && y <= max(ay1, ay2) &&
			min(by1, by2) <= y && y <= max(by1, by2) &&
			min(ax1, ax2) <= x && x <= max(ax1, ax2) &&
			min(bx1, bx2) <= x && x <= max(bx1, bx2))
				return true;     
			else
				return false;
		}
		else if(!c1 && c2)
		{
			x = -b1;          
			y = a2 * x + b2;

			if(min(ay1, ay2) <= y && y <= max(ay1, ay2) &&
			min(by1, by2) <= y && y <= max(by1, by2) &&
			min(ax1, ax2) <= x && x <= max(ax1, ax2) &&
			min(bx1, bx2) <= x && x <= max(bx1, bx2))
				return true;     
			else
				return false;
		}
		else if(c1 && !c2)
		{
			x = -b2;           
			y = a1 * x + b1;

			if(min(ay1, ay2) <= y && y <= max(ay1, ay2) &&
			min(by1, by2) <= y && y <= max(by1, by2) &&
			min(ax1, ax2) <= x && x <= max(ax1, ax2) &&
			min(bx1, bx2) <= x && x <= max(bx1, bx2))
				return true;    
			else
				return false;
		}
		else /* !c1 && !c2 */
			return false;   
	}
	bool _LineCrossLeftHorizontalLine(double x1, double y1, double x2, double y2, double x, double y)
	{
		double  px1, px2, py1, py2;
		Point   lineA[2];
		Point   horizontalLineB[2];

		if (x1 > x2)
		{
			px1 = x2;
			px2 = x1;
		}
		else
		{
			px1 = x1;
			px2 = x2;
		}

		if (y1 > y2 )
		{
			py1 = y2;
			py2 = y1;
		}
		else {
			py1 = y1;
			py2 = y2;
		}

		if(py1 < y && y < py2)
		{
			if(px1 <= x && x <= px2)
			{
				lineA[0].m_x = x1; lineA[0].m_y = y1;
				lineA[1].m_x = x2; lineA[1].m_y = y2;

				horizontalLineB[0].m_x = x;       horizontalLineB[0].m_y = y;
				horizontalLineB[1].m_x = px1 - 1; horizontalLineB[1].m_y = y;

				return _LineCrossLine(lineA, horizontalLineB);
			}
			/*@ give queryPoint is right to line segment */
			else if(x > px2) return true;
		}

		return false;
	}

	bool _LineCrossRightHorizontalLine(double x1, double y1, double x2, double y2, double x, double y)
	{
		double  px1, px2, py1, py2;
		Point   lineA[2];
		Point   horizontalLineB[2];

		if (x1 > x2)
		{
			px1 = x2;
			px2 = x1;
		}
		else
		{
			px1 = x1;
			px2 = x2;
		}

		if (y1 > y2 )
		{
			py1 = y2;
			py2 = y1;
		}
		else {
			py1 = y1;
			py2 = y2;
		}

		if(py1 < y && y < py2)
		{
			if(px1 <= x && x <= px2)
			{
				lineA[0].m_x = x1; lineA[0].m_y = y1;
				lineA[1].m_x = x2; lineA[1].m_y = y2;

				horizontalLineB[0].m_x = x;       horizontalLineB[0].m_y = y;
				horizontalLineB[1].m_x = px2 + 1; horizontalLineB[1].m_y = y;

				return _LineCrossLine(lineA, horizontalLineB);
			}
			/*@ give queryPoint is left to line segment */
			else if(x < px1) return true;
		}

		return false;
	}
	bool _IsPolygonContainPoint(WKBinary& polygon, Point pointB)
	{
#ifdef ISPOLYGONCONTAINPOINTOPTIMIZE
        double xInt;  // x intersection of segment with ray
        int crossings = 0;  // number of segment/ray crossings
        double x1;    // translated coordinates
        double y1;
        double x2;
        double y2;
        Point p1;
		Point p2;
        p1 = polygon.GetPoint(0, 0);
        /*
         * For each segment l = (i-1, i), see if it crosses ray from
         * test point in positive x direction.
         */
        size_t nPts=polygon.GetNumPoints(0);
        for(size_t i=1; i<nPts; i++)
        {
				p2=p1;
                p1=polygon.GetPoint(0, i);
                x1 = p1.m_x - pointB.m_x;
                y1 = p1.m_y - pointB.m_y;
                x2 = p2.m_x - pointB.m_x;
                y2 = p2.m_y - pointB.m_y;

                if (((y1 > 0) && (y2 <= 0)) ||
                        ((y2 > 0) && (y1 <= 0)))
                {
                        /*
                         *  segment straddles x axis, so compute intersection.
                         */
                        //xInt = RobustDeterminant::signOfDet2x2(x1, y1, x2, y2)
                        xInt = (x1*y2>x2*y1?1:-1)
                                / (y2 - y1);

                        if (x1*y2 == x2*y1) xInt = 0;

                        /*
                         *  crosses ray if strictly positive intersection.
                         */
                        if (0.0 < xInt) crossings++;
                }
        }

        /*
         *  p is inside if number of crossings is odd.
         */
        if ((crossings % 2) == 1) return true;
        return false;
#else
		int    i, n;             /* for indexing */
		int    lineCount;        /* # of matched lines */
		int    pointCount;       /* # of matched points */
		int    leftLineResult;
		int    rightLineResult;

		enum { UP, DOWN } prevDirection, direction; /*@ direction of line */

		OOSQL_TCArray<Point> pointsA(geoMemoryManager);
		pointsA.resize(polygon.GetNumPoints(0));
		for(i = 0; i < polygon.GetNumPoints(0); i++)
			pointsA.add(polygon.GetPoint(0, i));

		int nPointsA = pointsA.numberOfItems();

		if (nPointsA < 3)       /*@ less than 3 lines. if below, it has no area */
			return false;

		lineCount = pointCount = 0;
		for(i = 0; i < nPointsA - 1; i++)
		{
			if(pointB.m_y != pointsA[i].m_y && pointsA[i].m_y != pointsA[i + 1].m_y)
				break;
		}

		if(i == (nPointsA - 1))
			return false;

		/*@ Determine previous direction value */
		/* There is no case such that points[i].m_y == points[i+1].m_y. */
		if(pointsA[i].m_y < pointsA[(i + 1) % nPointsA].m_y)
			prevDirection = UP;
		else
			prevDirection = DOWN;

		for(n = 0; n < nPointsA; n++, i++)
		{   /*@ for all points */
			i = i % nPointsA;
			if(pointsA[i].m_y == pointsA[(i + 1) % nPointsA].m_y)
				continue;   /*@ skip horizontal line */
			else if(pointsA[i].m_y < pointsA[(i + 1) % nPointsA].m_y)
				direction = UP;
			else
				direction = DOWN;

			if(pointB.m_y == pointsA[i].m_y && pointB.m_x < pointsA[i].m_x && direction == prevDirection )
				pointCount ++;
			else if(_LineCrossRightHorizontalLine(pointsA[i].m_x, pointsA[i].m_y,
												  pointsA[(i + 1) % nPointsA].m_x,
												  pointsA[(i + 1) % nPointsA].m_y,
												  pointB.m_x, pointB.m_y))
				lineCount ++;

			prevDirection = direction;
		}

		if (((lineCount + pointCount) % 2) == 1)
			rightLineResult = true;
		else
			rightLineResult = false;

		/*
			if the y-coordinate of the first point of polygon is same
			as that of the given point (x,y)
			skip it.
		*/
		lineCount = pointCount = 0;
		for(i = 0; i < nPointsA - 1; i++)
		{
			if(pointB.m_y != pointsA[i].m_y && pointsA[i].m_y != pointsA[i + 1].m_y)
				break;
		}

		if(i == (nPointsA - 1)) 
			return false;

		/*@ Determine previous direction value */
		/* There is no case such that points[i].m_y == points[i+1].m_y. */
		if(pointsA[i].m_y < pointsA[(i + 1) % nPointsA].m_y)
			prevDirection = UP;
		else
			prevDirection = DOWN;

		for(n = 0; n < nPointsA; n++, i++)
		{   /*@ for all points */
			i = i % nPointsA;
			if(pointsA[i].m_y == pointsA[(i + 1) % nPointsA].m_y)
				continue;   /*@ skip horizontal line */
			else if(pointsA[i].m_y < pointsA[(i + 1) % nPointsA].m_y)
				direction = UP;
			else
				direction = DOWN;

			if(pointB.m_y == pointsA[i].m_y && pointB.m_x < pointsA[i].m_x && direction == prevDirection )
				pointCount ++;
			else if(_LineCrossLeftHorizontalLine(pointsA[i].m_x, pointsA[i].m_y,
												 pointsA[(i + 1) % nPointsA].m_x,
												 pointsA[(i + 1) % nPointsA].m_y,
												 pointB.m_x, pointB.m_y))
				lineCount ++;

			prevDirection = direction;
		}

		if (((lineCount + pointCount) % 2) == 1)
			leftLineResult = true;
		else
			leftLineResult = false;

		if(rightLineResult && leftLineResult)
			return true;
		else
			return false;
#endif
	}
	double _GetDistancePointToLine(Point& point, Point& point1, Point& point2)
	{
		double x0, y0, x1, y1, x2, y2;
		double dx, dy, d1, d2, d3, t0;
		double distance;

		x1 = point1.GetX();
		y1 = point1.GetY();
		x2 = point2.GetX();
		y2 = point2.GetY();

		dx = x2 - x1;
		dy = y2 - y1;

		if(dx != 0 || dy != 0)
		{
			t0 = (point.GetX() - x1) * dx + (point.GetY() - y1) * dy;
			t0 = t0 / (dx * dx + dy * dy);
		}
		else
			t0 = 2;

		if(t0 >= 0 && t0 <= 1)
		{
			x0 = x1 + t0 * (x2 - x1);
			y0 = y1 + t0 * (y2 - y1);

			d1 = pow(point.GetX() - x0, 2) + pow(point.GetY() - y0, 2);

			distance = sqrt(d1);
		}
		else
		{
			/*@ distance from the point to the (x1,y1) */
			d2 = pow(point.GetX() - x1, 2) + pow(point.GetY() - y1, 2);

			/*@ distance from the point to the (x2,y2) */
			d3 = pow(point.GetX() - x2, 2) + pow(point.GetY() - y2, 2);

			if(d2 < d3)
				distance = sqrt(d2);
			else
				distance = sqrt(d3);
		}
		return distance;
	}

    double WKBinary::Distance(WKBinary& wkb)
	{
		if(GetGeometryType() == WKBPOINT && wkb.GetGeometryType() == WKBPOINT)
		{
			Point p1 = GetPoint();
			Point p2 = wkb.GetPoint();

			float dx = p1.GetX() - p2.GetX();
			float dy = p1.GetY() - p2.GetY();

			return sqrt(dx * dx + dy * dy);
		}
		else if(GetGeometryType() == WKBPOINT && wkb.GetGeometryType() == WKBPOLYGON)
		{
			Point point = GetPoint();
			if(_IsPolygonContainPoint(wkb, point))
				return 0;
			int nPoints = wkb.GetNumPoints(0);
			double minDistance = -1;
			double distance;
			for(int i = 0; i < (nPoints - 1); i++)
			{
				Point point1 = wkb.GetPoint(0, i);
				Point point2 = wkb.GetPoint(0, i+1);

				distance = _GetDistancePointToLine(point, point1, point2);
				if(distance < minDistance || minDistance == -1)
					minDistance = distance;
			}
			return minDistance;
		}
		else if(GetGeometryType() == WKBPOLYGON && wkb.GetGeometryType() == WKBPOINT)
			return wkb.Distance(*this);
		return 0;
	}
    int WKBinary::Equals(WKBinary& wkb)
	{
	    if(GetGeometryType() != wkb.GetGeometryType())
	        return 0;
	        
	    if(GetGeometryType() == WKBPOINT)
        {
            Point p1, p2;
            p1 = GetPoint();
            p2 = wkb.GetPoint();
            if(p1.GetX() != p2.GetX() || p1.GetY() != p2.GetY())
                return 0;
            else
                return 1;
        }
        else if(GetGeometryType() == WKBLINESTRING)
        {
            if(GetNumPoints() == wkb.GetNumPoints())
            {
                int nPoints = GetNumPoints();
                for(int i = 0; i < nPoints; i++)
                {
                    Point p1, p2;
                    p1 = GetPoint(i);
                    p2 = wkb.GetPoint(i);
                    if(p1.GetX() != p2.GetX() || p1.GetY() != p2.GetY())
                        return 0;
                }
                return 1;
            }
            else
                return 0;
        }
        else if(GetGeometryType() == WKBPOLYGON)
        {
            if(GetNumRings() == wkb.GetNumRings())
            {
                int nRings = GetNumRings();
                for(int j = 0; j < nRings; j++)
                {
                    if(GetNumPoints(j) == wkb.GetNumPoints(j))
                    {
                        int nPoints = GetNumPoints(j);
                        for(int i = 0; i < nPoints; i++)
                        {
                            Point p1, p2;
                            p1 = GetPoint(j, i);
                            p2 = wkb.GetPoint(j, i);
                            if(p1.GetX() != p2.GetX() || p1.GetY() != p2.GetY())
                                return 0;
                        }
                    }
                    else
                        return 0;
                }
                return 1;
            }
        }
	}
    int WKBinary::Intersects(WKBinary& wkb)
	{
		float xmin1, ymin1, xmax1, ymax1;
		float xmin2, ymin2, xmax2, ymax2;

		GetMBR(&xmin1, &ymin1, &xmax1, &ymax1);
		wkb.GetMBR(&xmin2, &ymin2, &xmax2, &ymax2);

		if(xmax1 < xmin2 || xmax2 < xmin1 || ymax1 < ymin2 || ymax2 < ymin1)
			return 0;

        if(GetGeometryType() == WKBPOLYGON && wkb.GetGeometryType() == WKBPOLYGON)
        {
            int nRings = GetNumRings();
            for(int j = 0; j < nRings; j++)
            {
			    int nPoints = GetNumPoints(j);
    			for(int i = 0; i < nPoints; i++)
    			{
    				Point point = GetPoint(j, i);
    				if(_IsPolygonContainPoint(wkb, point))
    					return 1;
    			}
    		}
			return 0;
        }
		else if(GetGeometryType() == WKBLINESTRING && wkb.GetGeometryType() == WKBPOLYGON)
		{
			int nPoints = GetNumPoints();
			for(int i = 0; i < nPoints; i++)
			{
				Point point = GetPoint(i);
				if(_IsPolygonContainPoint(wkb, point))
					return 1;
			}
			return 0;
		}
		else if(GetGeometryType() == WKBPOLYGON && wkb.GetGeometryType() == WKBLINESTRING)
			return wkb.Intersects(*this);
		else if(GetGeometryType() == WKBPOINT && wkb.GetGeometryType() == WKBPOINT)
	    {
            Point p1, p2;
            p1 = GetPoint();
            p2 = wkb.GetPoint();
	        if(p1.GetX() != p2.GetX() || p1.GetY() != p2.GetY())
	            return 0;
	        else
	            return 1;
	    }
	    else if(GetGeometryType() == WKBPOLYGON && wkb.GetGeometryType() == WKBPOINT)
	        return Contains(wkb);
	    else if(GetGeometryType() == WKBPOINT && wkb.GetGeometryType() == WKBPOLYGON)
	        return wkb.Contains(*this);
	    return 0;
	}
    int WKBinary::Overlaps(WKBinary& wkb)
	{
	    return Intersects(wkb);
	}
    int WKBinary::Related(WKBinary& wkb, char* patternMatrix)
	{
		return 0;
	}
    int WKBinary::Touches(WKBinary& wkb)
	{
		return Intersects(wkb);
	}
    int WKBinary::Within(WKBinary& wkb)
	{
		return wkb.Contains(*this);;
	}

    #ifdef ENABLE_OPENGIS_OPTIMIZATION
    WKBinaryHolder::WKBinaryHolder()
    {
        int i;

        data = new WKBinary[INITIALNUMBEROFUDF];
        evaluated = new bool[INITIALNUMBEROFUDF];
        size = INITIALNUMBEROFUDF;

        for (i=0; i < size; i++)
            evaluated[i] = false;
    }

    WKBinaryHolder::~WKBinaryHolder()
    {
        delete[] data;
        delete[] evaluated;
    }

    void WKBinaryHolder::setWKBinary(int UDFNo, WKBinary& wkb)
    {
        if (UDFNo >= 0)
        {
            // if data space to store wkb is not enough, enlarge data and evluated array
            if (UDFNo >= size)
            {
                WKBinary*   dataTmp;
                bool*       evaluatedTmp;
                int         sizeTmp;
                int         i;

                sizeTmp = size*2;
                while (1==1)
                {
                    if(UDFNo < sizeTmp)
                        break;
                    else sizeTmp = sizeTmp * 2;
                }

                dataTmp      = data;
                evaluatedTmp = evaluated;

                data = new WKBinary[sizeTmp];
                evaluated = new bool[sizeTmp];

                // copy previous data, evaluated array
                for (i=0; i<size; i++)
                {
                    data[i]      = dataTmp[i];
                    evaluated[i] = evaluatedTmp[i];
                }

                // delete previous data, evaluated array
                delete[] data;
                delete[] evaluated;

                // reset enlarged evaluated array
                for (i=size; i<sizeTmp; i++)
                    evaluated[i] = false;

                size = sizeTmp;
            }

            // store WKBinary
            //data[UDFNo] = wkb;
            data[UDFNo].Init(wkb.GetBinary(), wkb.GetSize());
            evaluated[UDFNo] = true;
        }
    }

    void WKBinaryHolder::getWKBinary(int UDFNo, WKBinary& wkb)
    {
        if (UDFNo >= 0 && UDFNo < size)
        {
            //wkb = data[UDFNo];
            wkb.Init(data[UDFNo].GetBinary(), data[UDFNo].GetSize());
        }
    }

    bool WKBinaryHolder::isEvaluated(int UDFNo)
    {
        if(UDFNo >= 0 && UDFNo < size)
            return evaluated[UDFNo];
        else return false;
    }

    void WKBinaryHolder::reset()
    {
        int i;

        for (i=0; i < size; i++)
            evaluated[i] = false;
    }
    #endif
#endif
