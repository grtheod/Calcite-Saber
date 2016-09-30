package calcite.planner;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;

public class SaberSqlValidator extends SqlValidatorImpl {
	  /**
	   * Creates a validator.
	   *
	   * @param opTab         Operator table
	   * @param catalogReader Catalog reader
	   * @param typeFactory   Type factory
	   */
	  protected SaberSqlValidator(SqlOperatorTable opTab, SqlValidatorCatalogReader catalogReader, RelDataTypeFactory typeFactory) {
	    /* Note: We may need to define Saber specific SqlConformance instance in future. */
	    super(opTab, catalogReader, typeFactory, SqlConformance.DEFAULT);
	  }

	  @Override
	  protected RelDataType getLogicalSourceRowType(
	      RelDataType sourceRowType, SqlInsert insert) {
	    return ((JavaTypeFactory) typeFactory).toSql(sourceRowType);
	  }

	  @Override
	  protected RelDataType getLogicalTargetRowType(
	      RelDataType targetRowType, SqlInsert insert) {
	    return ((JavaTypeFactory) typeFactory).toSql(targetRowType);
	  }
	}