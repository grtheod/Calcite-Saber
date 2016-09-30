package calcite.planner;

import java.io.Serializable;
import java.util.EnumSet;
import java.util.Set;

import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableSet;

//Query Planner for sql validation

public class SaberPlanner implements Planner{
	
	private static final Set<SqlKind> SUPPORTED_OPS = ImmutableSet.of(
        SqlKind.CAST, SqlKind.EQUALS, SqlKind.LESS_THAN,
	    SqlKind.LESS_THAN_OR_EQUAL, SqlKind.GREATER_THAN,
	    SqlKind.GREATER_THAN_OR_EQUAL, SqlKind.NOT_EQUALS, SqlKind.LIKE,
	    SqlKind.AND, SqlKind.OR, SqlKind.NOT);
	
	private static final Set<Serializable> SUPPORTED_OPERATORS = ImmutableSet.of(
			SqlKind.AGGREGATE, SqlKind.COUNT, SqlKind.AS, SqlKind.AVG,
			SqlKind.SUM, SqlKind.JOIN, SqlKind.FILTER, SqlKind.MAX,
			SqlKind.MIN, SqlKind.SELECT);

	public void close() {
		// TODO Auto-generated method stub
		
	}

	public RelNode convert(SqlNode arg0) throws RelConversionException {
		// TODO Auto-generated method stub
		return null;
	}

	public RelTraitSet getEmptyTraitSet() {
		// TODO Auto-generated method stub
		return null;
	}

	public RelDataTypeFactory getTypeFactory() {
		// TODO Auto-generated method stub
		return null;
	}

	public SqlNode parse(String arg0) throws SqlParseException {
		// TODO Auto-generated method stub
		return null;
	}

	public RelRoot rel(SqlNode arg0) throws RelConversionException {
		// TODO Auto-generated method stub
		return null;
	}

	public void reset() {
		// TODO Auto-generated method stub
		
	}

	public RelNode transform(int arg0, RelTraitSet arg1, RelNode arg2) throws RelConversionException {
		// TODO Auto-generated method stub
		return null;
	}

	public SqlNode validate(SqlNode arg0) throws ValidationException {
		// TODO Auto-generated method stub
		return null;
	}

	public Pair<SqlNode, RelDataType> validateAndGetType(SqlNode arg0) throws ValidationException {
		// TODO Auto-generated method stub
		return null;
	}

}
