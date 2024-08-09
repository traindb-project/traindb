/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package traindb.sql.fun;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

public class TrainDBSpatialOperatorTable extends ReflectiveSqlOperatorTable {

  //ST_Area()
  public static final SqlFunction ST_AREA =
      new SqlFunction("st_area",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.GEO),
          SqlFunctionCategory.SYSTEM);

  //ST_AsBinary()
  public static final SqlFunction ST_ASBINARY =
      new SqlFunction("st_asbinary",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.VARBINARY),
          null,
          OperandTypes.family(SqlTypeFamily.GEO),
          SqlFunctionCategory.SYSTEM);

  //ST_AsText()
  public static final SqlFunction ST_ASTEXT =
      new SqlFunction("st_astext",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.VARCHAR_2000_NULLABLE,
          null,
          OperandTypes.family(SqlTypeFamily.GEO),
          SqlFunctionCategory.SYSTEM);

  //ST_Buffer()
  public static final SqlFunction ST_BUFFER =
      new SqlFunction(
          "st_buffer",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.GEO, SqlTypeFamily.ANY),
          SqlFunctionCategory.SYSTEM);

  //ST_Centroid()
  public static final SqlFunction ST_CENTROID =
      new SqlFunction(
          "st_centroid",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.GEO),
          SqlFunctionCategory.SYSTEM);

  //ST_Contains()
  public static final SqlFunction ST_CONTAINS =
      new SqlFunction(
          "st_contains",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.ANY),
          SqlFunctionCategory.SYSTEM);

  //ST_ConvexHull()
  public static final SqlFunction ST_CONVEXHULL =
      new SqlFunction(
          "st_convexhull",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.GEO),
          SqlFunctionCategory.SYSTEM);

  //ST_Crosses()
  public static final SqlFunction ST_CROSSES =
      new SqlFunction(
          "st_crosses",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.GEO, SqlTypeFamily.GEO),
          SqlFunctionCategory.SYSTEM);

  //ST_Difference()
  public static final SqlFunction ST_DIFFERRENCE =
      new SqlFunction(
          "st_difference",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.ANY),
          SqlFunctionCategory.SYSTEM);

  //ST_Dimension()
  public static final SqlFunction ST_DIMENSION =
      new SqlFunction(
          "st_dimension",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.INTEGER,
          null,
          OperandTypes.family(SqlTypeFamily.GEO),
          SqlFunctionCategory.SYSTEM);

  //ST_Disjoint()
  public static final SqlFunction ST_DISJOINT =
      new SqlFunction(
          "st_disjoint",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.GEO, SqlTypeFamily.GEO),
          SqlFunctionCategory.SYSTEM);

  //ST_Distance()
  public static final SqlFunction ST_DISTANCE =
      new SqlFunction(
          "st_distance",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.GEO, SqlTypeFamily.GEO),
          SqlFunctionCategory.SYSTEM);

  //ST_EndPoint()
  public static final SqlFunction ST_ENDPOINT =
      new SqlFunction(
          "st_endpoint",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.GEO),
          SqlFunctionCategory.SYSTEM);

  //ST_Envelope()
  public static final SqlFunction ST_ENVELOPE =
      new SqlFunction(
          "st_envelope",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.GEO),
          SqlFunctionCategory.SYSTEM);

  //ST_Equals()
  public static final SqlFunction ST_EQUALS =
      new SqlFunction(
          "st_equals",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.ANY),
          SqlFunctionCategory.SYSTEM);

  //ST_ExteriorRing()
  public static final SqlFunction ST_EXTERIORRING =
      new SqlFunction(
          "st_exteriorring",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.GEO),
          SqlFunctionCategory.SYSTEM);

  //ST_GeometryN()
  public static final SqlFunction ST_GEOMETRYN =
      new SqlFunction(
          "st_geometryn",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.GEO, SqlTypeFamily.INTEGER),
          SqlFunctionCategory.SYSTEM);

  //ST_GeometryType()
  public static final SqlFunction ST_GEOMETRYTYPE =
      new SqlFunction(
          "st_geometrytype",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.INTEGER,
          null,
          OperandTypes.family(SqlTypeFamily.GEO),
          SqlFunctionCategory.SYSTEM);

  //ST_GeomFromText()
  public static final SqlFunction ST_GEOMFROMTEXT =
      new SqlFunction("st_geomfromtext",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.CHARACTER),
          SqlFunctionCategory.SYSTEM);

  //ST_GeomFromWKB()
  public static final SqlFunction ST_GEOMFROMWKB =
      new SqlFunction("st_geomfromwkb",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.BINARY, SqlTypeFamily.INTEGER),
          SqlFunctionCategory.SYSTEM);

  //ST_InteriorRingN()
  public static final SqlFunction ST_INTERIORRINGN =
      new SqlFunction(
          "st_interiorringn",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.GEO, SqlTypeFamily.INTEGER),
          SqlFunctionCategory.SYSTEM);

  //ST_Intersection()
  public static final SqlFunction ST_INTERSECTION =
      new SqlFunction(
          "st_intersection",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.ANY),
          SqlFunctionCategory.SYSTEM);

  //ST_Intersects()
  public static final SqlFunction ST_INTERSECTS =
      new SqlFunction(
          "st_intersects",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.INTEGER,
          null,
          OperandTypes.family(SqlTypeFamily.GEO, SqlTypeFamily.GEO),
          SqlFunctionCategory.SYSTEM);

  //ST_IsClosed()
  public static final SqlFunction ST_ISCLOSED =
      new SqlFunction(
          "st_isclosed",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.INTEGER,
          null,
          OperandTypes.family(SqlTypeFamily.GEO),
          SqlFunctionCategory.SYSTEM);

  //ST_IsEmpty()
  public static final SqlFunction ST_ISEMPTY =
      new SqlFunction(
          "st_isempty",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.INTEGER,
          null,
          OperandTypes.family(SqlTypeFamily.GEO),
          SqlFunctionCategory.SYSTEM);

  //ST_IsSimple()
  public static final SqlFunction ST_ISSIMPLE =
      new SqlFunction(
          "st_issimple",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.INTEGER,
          null,
          OperandTypes.family(SqlTypeFamily.GEO),
          SqlFunctionCategory.SYSTEM);

  //ST_Length()
  public static final SqlFunction ST_LENGTH =
      new SqlFunction(
          "st_length",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.DOUBLE,
          null,
          OperandTypes.family(SqlTypeFamily.GEO),
          SqlFunctionCategory.SYSTEM);

  //ST_LineFromText()
  public static final SqlFunction ST_LINEFROMTEXT =
      new SqlFunction(
          "st_linefromtext",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER),
          SqlFunctionCategory.SYSTEM);

  //ST_LineFromWKB()
  public static final SqlFunction ST_LINEFROMWKB =
      new SqlFunction("st_linefromwkb",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.BINARY, SqlTypeFamily.INTEGER),
          SqlFunctionCategory.SYSTEM);

  //ST_MLineFromText()
  public static final SqlFunction ST_MLINEFROMTEXT =
      new SqlFunction(
          "st_mlinefromtext",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER),
          SqlFunctionCategory.SYSTEM);

  //ST_MPointFromText()
  public static final SqlFunction ST_MPOINTFROMTEXT =
      new SqlFunction(
          "st_mpointfromtext",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER),
          SqlFunctionCategory.SYSTEM);

  //ST_MPointFromWKB()
  public static final SqlFunction ST_MPOINTFROMWKB =
      new SqlFunction("st_mpointfromwkb",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.BINARY, SqlTypeFamily.INTEGER),
          SqlFunctionCategory.SYSTEM);

  //ST_MPolyFromText()
  public static final SqlFunction ST_MPOLYFROMTEXT =
      new SqlFunction(
          "st_mpolyfromtext",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER),
          SqlFunctionCategory.SYSTEM);

  //ST_MPolyFromWKB()
  public static final SqlFunction ST_MPOLYFROMWKB =
      new SqlFunction("st_mpolyfromwkb",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.BINARY, SqlTypeFamily.INTEGER),
          SqlFunctionCategory.SYSTEM);

  //ST_NumGeometries() : for MySQL
  public static final SqlFunction ST_NUMGEOMETRIES =
      new SqlFunction(
          "st_numgeometries",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.INTEGER,
          null,
          OperandTypes.family(SqlTypeFamily.BINARY),
          SqlFunctionCategory.SYSTEM);

  //ST_NumGeometry() : for kairos
  public static final SqlFunction ST_NUMGEOMETRY =
      new SqlFunction(
          "st_numgeometry",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.INTEGER,
          null,
          OperandTypes.family(SqlTypeFamily.GEO),
          SqlFunctionCategory.SYSTEM);

  //ST_NumInteriorRing()
  public static final SqlFunction ST_NUMINTERIORRING =
      new SqlFunction(
          "st_numinteriorring",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.INTEGER,
          null,
          OperandTypes.family(SqlTypeFamily.GEO),
          SqlFunctionCategory.SYSTEM);

  //ST_NumPoints()
  public static final SqlFunction ST_NUMPOINTS =
      new SqlFunction(
          "st_numpoints",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.INTEGER,
          null,
          OperandTypes.family(SqlTypeFamily.GEO),
          SqlFunctionCategory.SYSTEM);

  //ST_Overlaps()
  public static final SqlFunction ST_OVERLAPS =
      new SqlFunction(
          "st_overlaps",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.ANY),
          SqlFunctionCategory.SYSTEM);

  //ST_PointFromText()
  public static final SqlFunction ST_POINTFROMTEXT =
      new SqlFunction("st_pointfromtext",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.CHARACTER),
          SqlFunctionCategory.SYSTEM);

  //ST_PointFromWKB()
  public static final SqlFunction ST_POINTFROMWKB =
      new SqlFunction("st_pointfromwkb",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.BINARY, SqlTypeFamily.INTEGER),
          SqlFunctionCategory.SYSTEM);

  //ST_PointN()
  public static final SqlFunction ST_POINTN =
      new SqlFunction(
          "st_pointn",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.GEO, SqlTypeFamily.INTEGER),
          SqlFunctionCategory.SYSTEM);

  //ST_PolyFromText()
  public static final SqlFunction ST_POLYFROMTEXT =
      new SqlFunction(
          "st_polyfromtext",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER),
          SqlFunctionCategory.SYSTEM);

  //ST_PolyFromWKB()
  public static final SqlFunction ST_POLYFROMWKB =
      new SqlFunction("st_polyfromwkb",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.BINARY, SqlTypeFamily.INTEGER),
          SqlFunctionCategory.SYSTEM);

  //ST_SRID()
  public static final SqlFunction ST_SRID =
      new SqlFunction(
          "st_srid",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.INTEGER),
          null,
          OperandTypes.family(SqlTypeFamily.GEO),
          SqlFunctionCategory.SYSTEM);

  //ST_StartPoint()
  public static final SqlFunction ST_STARTPOINT =
      new SqlFunction(
          "st_startpoint",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.GEO),
          SqlFunctionCategory.SYSTEM);

  //ST_SymDifference()
  public static final SqlFunction ST_SYMDIFFERENCE =
      new SqlFunction(
          "st_symdifference",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.GEO, SqlTypeFamily.GEO),
          SqlFunctionCategory.SYSTEM);

  //ST_Touches()
  public static final SqlFunction ST_TOUCHES =
      new SqlFunction(
          "st_touches",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.GEO, SqlTypeFamily.GEO),
          SqlFunctionCategory.SYSTEM);

  //ST_Union()
  public static final SqlFunction ST_UNION =
      new SqlFunction(
          "st_union",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.ANY),
          SqlFunctionCategory.SYSTEM);

  //ST_Within()
  public static final SqlFunction ST_WITHIN =
      new SqlFunction(
          "st_within",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.GEO, SqlTypeFamily.GEO),
          SqlFunctionCategory.SYSTEM);

  //ST_X()
  public static final SqlFunction ST_X =
      new SqlFunction(
          "st_x",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.DOUBLE_NULLABLE,
          null,
          OperandTypes.family(SqlTypeFamily.GEO),
          SqlFunctionCategory.SYSTEM);

  //ST_Y()
  public static final SqlFunction ST_Y =
      new SqlFunction(
          "st_y",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.DOUBLE_NULLABLE,
          null,
          OperandTypes.family(SqlTypeFamily.GEO),
          SqlFunctionCategory.SYSTEM);

  //ST_ENTERS()
  public static final SqlFunction ST_ENTERS =
      new SqlFunction(
          "st_enters",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.GEO, SqlTypeFamily.GEO),
          SqlFunctionCategory.SYSTEM);

  //ST_FIRSTSUBSEQUENCE()
  public static final SqlFunction ST_FIRSTSUBSEQUENCE =
      new SqlFunction(
          "st_firstsubsequence",
          SqlKind.OTHER_FUNCTION,
          //ReturnTypes.BOOLEAN,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.GEO, SqlTypeFamily.INTEGER),
          SqlFunctionCategory.SYSTEM);

  //ST_IMPORTFROMWKB()
  public static final SqlFunction ST_IMPORTFROMWKB =
      new SqlFunction("st_importfromwkb",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.BINARY),
          SqlFunctionCategory.SYSTEM);

  //ST_IMPORTFROMWKT()
  public static final SqlFunction ST_IMPORTFROMWKT =
      new SqlFunction(
          "st_importfromwkt",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.CHARACTER),
          SqlFunctionCategory.SYSTEM);

  //ST_INSIDES()
  public static final SqlFunction ST_INSIDES =
      new SqlFunction(
          "st_insides",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.INTEGER,
          null,
          OperandTypes.family(SqlTypeFamily.GEO, SqlTypeFamily.GEO),
          SqlFunctionCategory.SYSTEM);

  //ST_LASTSUBSEQUENCE()
  public static final SqlFunction ST_LASTSUBSEQUENCE =
      new SqlFunction("st_lastsubsequence",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.GEO, SqlTypeFamily.INTEGER),
          SqlFunctionCategory.SYSTEM);

  //ST_LEAVES()
  public static final SqlFunction ST_LEAVES =
      new SqlFunction("st_leaves",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.GEO, SqlTypeFamily.GEO),
          SqlFunctionCategory.SYSTEM);

  //ST_LIFETIME()
  public static final SqlFunction ST_LIFETIME =
      new SqlFunction(
          "st_lifetime",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.GEO),
          SqlFunctionCategory.SYSTEM);

  //ST_MEETS()
  public static final SqlFunction ST_MEETS =
      new SqlFunction("st_meets",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.ANY),
          SqlFunctionCategory.SYSTEM);

  //ST_PASSES()
  public static final SqlFunction ST_PASSES =
      new SqlFunction(
          "st_passes",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.GEO, SqlTypeFamily.GEO),
          SqlFunctionCategory.SYSTEM);

  //ST_POINTONSURFACE()
  public static final SqlFunction ST_POINTONSURFACE =
      new SqlFunction(
          "st_pointonsurface",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.GEO),
          SqlFunctionCategory.SYSTEM);

  //ST_PRECEDES()
  public static final SqlFunction ST_PRECEDES =
      new SqlFunction(
          "st_precedes",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.ANY),
          SqlFunctionCategory.SYSTEM);

  //ST_PROJECT()
  public static final SqlFunction ST_PROJECT =
      new SqlFunction(
          "st_project",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.GEO),
          SqlFunctionCategory.SYSTEM);

  //ST_SLICE()
  public static final SqlFunction ST_SLICE =
      new SqlFunction(
          "st_slice",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.GEO, SqlTypeFamily.ANY),
          SqlFunctionCategory.SYSTEM);

  //ST_SLICEBYVALUE()
  public static final SqlFunction ST_SLICEBYVALUE =
      new SqlFunction(
          "st_slicebyvalue",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.GEO, SqlTypeFamily.GEO, SqlTypeFamily.GEO),
          SqlFunctionCategory.SYSTEM);

  //ST_SNAPSHOT()
  public static final SqlFunction ST_SNAPSHOT =
      new SqlFunction(
          "st_snapshot",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.GEO, SqlTypeFamily.CHARACTER),
          SqlFunctionCategory.SYSTEM);

  //ST_SNAPSHOTBYVALUE()
  public static final SqlFunction ST_SNAPSHOTBYVALUE =
      new SqlFunction(
          "st_snapshotbyvalue",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.GEO, SqlTypeFamily.GEO),
          SqlFunctionCategory.SYSTEM);

  //ST_SUBSEQUENCE()
  public static final SqlFunction ST_SUBSEQUENCE =
      new SqlFunction(
          "st_subsequence",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.GEOMETRY),
          null,
          OperandTypes.family(SqlTypeFamily.GEO, SqlTypeFamily.CHARACTER),
          SqlFunctionCategory.SYSTEM);

  private static @MonotonicNonNull TrainDBSpatialOperatorTable instance;

  public static synchronized TrainDBSpatialOperatorTable instance() {
    if (instance == null) {
      instance = new TrainDBSpatialOperatorTable();
      instance.init();
    }
    return instance;
  }

}
