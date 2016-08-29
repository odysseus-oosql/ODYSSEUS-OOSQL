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

#ifndef __GEOMETRY_HXX__
#define __GEOMETRY_HXX__

#include "OOSQL_String.hxx"
#include "OOSQL_Hashtable.hxx"
#include <stdexcept>

extern OOSQL_MemoryManager* geoMemoryManager;

	enum wkbType
	{
		WKBPOINT = 1,
		WKBLINESTRING = 2,
		WKBPOLYGON = 3,
		WKBMULTIPOINT = 4,
		WKBMULTILINESTRING = 5,
		WKBMULTIPOLYGON = 6,
		WKBGEOMETRYCOLLECTION = 7
	};

	class parse_error : public std::runtime_error {
	public:
		parse_error(const OOSQL_TCDynStr& message) : std::runtime_error((const char*)message) {}
	};

	class Scanner {
	public:
		enum TokenType {NONE, COMMA, LBRASE, RBRASE, WORD, NUMBER};

		Scanner(char* s) :m_string(s, geoMemoryManager), m_pos(0), m_lastTokenType(NONE) {}
		Scanner(OOSQL_TCDynStr s):m_string(s, geoMemoryManager), m_pos(0), m_lastTokenType(NONE) {}

		OOSQL_TCDynStr GetNextToken();
		TokenType GetLastTokenType() { return m_lastTokenType; }
		double GetNextTokenAsDouble();
		unsigned int CountNumberOfCommasInCurrentBracedBlock();
	private:
		OOSQL_TCDynStr		m_string;
		int			m_pos;
		TokenType	m_lastTokenType;
	};

	class Point {
	public:
		Point(): m_x(0), m_y(0) {}
		Point(float x, float y) : m_x(x), m_y(y) {}
		OOSQL_TCDynStr str() { return ""; }
		float GetX() { return m_x; }
		float GetY() { return m_y; }
		float m_x, m_y;
	};

	class WKBinary {
	public:
		WKBinary();
		WKBinary(char* s, int size);

		void			Init(char* s, int size);
	inline	void			Append(char value);
		inline void			Append(unsigned int value);
		inline void			Append(double value);
		void			ResetReadPos(int pos=0) { m_pos = pos; }
	inline	char			ReadNextChar();
	inline	bool			ReadNextCharAsBool() { if(ReadNextChar()) return true; else return false; }
	inline	unsigned int	ReadNextUnsignedInt();
	inline	double			ReadNextDouble();
	inline	void			SetBinaryBeLittleEndian(bool flag) { m_isBinaryLittleEndian = flag; }

		inline unsigned int	GetGeometryType();
		char*			GetGeometryTypeName();
		char*			GetBinary() { return (char*)m_binary.str(); }
		size_t			GetSize() { return m_binary.length(); }
		void			GetMBR(float* xmin, float* ymin, float* xmax, float* ymax);
		Point			GetPoint();
		inline Point			GetPoint(int ith);
		inline Point			GetPoint(int jthRing, int ith);
        WKBinary		GetPointAsWKBinary(int ith);
		int				GetNumPoints();
		inline int				GetNumPoints(int jthRing);
		inline int				GetNumRings();
		int				GetNumGeometries();
		WKBinary		GetSubWKBinary(int ith);


        WKBinary        Envelope();
        WKBinary        Boundary();
        int             IsEmpty();
        int             IsSimple();
        int             IsClosed();
        int             IsRing();
        double          Length();
        WKBinary        ExteriorRing();
        WKBinary        InteriorRingN(int ith);
        int             NumInteriorRings();
        double          Area();
        WKBinary        Centroid();
        WKBinary        PointOnSurface();
        WKBinary        GeometryN(int ith);
        WKBinary        Buffer();
        WKBinary        ConvexHull();
        WKBinary        Difference(WKBinary& wkb);
        WKBinary        Intersection(WKBinary& wkb);
        WKBinary        SymDifference(WKBinary& wkb);
        WKBinary        Union(WKBinary& wkb);
        int             Contains(WKBinary& wkb);
        int             Crosses(WKBinary& wkb);
        int             Disjoint(WKBinary& wkb);
        double          Distance(WKBinary& wkb);
        int             Equals(WKBinary& wkb);
        int             Intersects(WKBinary& wkb);
        int             Overlaps(WKBinary& wkb);
        int             Related(WKBinary& wkb, char* patternMatrix);
        int             Touches(WKBinary& wkb);
        int             Within(WKBinary& wkb);

	private:
		int				GetSubBinaryLength(unsigned int offset);

		OOSQL_TCDynStr			m_binary;
		unsigned int	m_pos;
		bool			m_isMachineLittleEndian;
		bool			m_isBinaryLittleEndian;
		bool			m_mbrGenerated;
		float			m_xmin, m_ymin, m_xmax, m_ymax;


	};

	WKBinary WKTextToWKBinary(char* s);
	OOSQL_TCDynStr WKBinaryToWKText(WKBinary& wkb);

	class Geometry {
	public:
		Geometry() {}

	private:
	};

    #ifdef ENABLE_OPENGIS_OPTIMIZATION
    const int INITIALNUMBEROFUDF = 10;  // initial number of user-defined function

    class WKBinaryHolder {
    public:
        WKBinaryHolder();
        ~WKBinaryHolder();

        void setWKBinary(int UDFNo, WKBinary& wkb);
        void getWKBinary(int UDFNo, WKBinary& wkb);
        bool isEvaluated(int UDFNo);
        void reset();

    private:
        WKBinary* data;
        bool* evaluated;

        int size;
    };
    #endif

#endif  __GEOMETRY_HXX__

