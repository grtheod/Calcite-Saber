package calcite.utils;

import java.util.List;
import java.util.Map;

import org.apache.calcite.DataContext;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;

public class CustomersTableFactory implements TableFactory<Table>  {
/*
 * @param schema Schema this table belongs to
 * @param name Name of this table
 * @param operand The "operand" JSON property
 * @param rowType Row type. Specified if the "columns" JSON property. 
 */
	public Table create(SchemaPlus schema, String name, Map<String, Object> operand, RelDataType rowType) {
		final Object[][] rows = {
		    {1, 1012},
		    {2, 5334},
		    {3, 2222},
		    {4, 3232},
		    {5, 3121},
		    {6, 13232},
		    {7, 15123},
		};
		return new CustomersTable(ImmutableList.copyOf(rows));
	}

	public static class CustomersTable implements ScannableTable {
		protected final RelProtoDataType protoRowType = new RelProtoDataType() {
			public RelDataType apply(RelDataTypeFactory a0) {
		        return a0.builder()
		            .add("customerid", SqlTypeName.INTEGER)
		            .add("phone", SqlTypeName.BIGINT)
		            .build();
			}
		};

		private final ImmutableList<Object[]> rows;

		public CustomersTable(ImmutableList<Object[]> rows) {
			this.rows = rows;
		}

		public Enumerable<Object[]> scan(DataContext root) {
		    return Linq4j.asEnumerable(rows);
		}

		public RelDataType getRowType(RelDataTypeFactory typeFactory) {
		    return protoRowType.apply(typeFactory);
		}

		public Statistic getStatistic() {
			int rowCount = rows.size();
			return Statistics.of(rowCount, ImmutableList.<ImmutableBitSet>of()); //add List<ImmutableBitSet>
		}

		public Schema.TableType getJdbcTableType() {
		    return Schema.TableType.TABLE;
		}	
		
	}
}

