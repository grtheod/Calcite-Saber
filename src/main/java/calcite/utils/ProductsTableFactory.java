package calcite.utils;

import java.util.List;
import java.util.Map;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
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
import org.apache.calcite.util.ImmutableIntList;

import com.google.common.collect.ImmutableList;

import calcite.utils.OrdersTableFactory.OrdersTable;

public class ProductsTableFactory implements TableFactory<Table>  {
	
	public Table create(SchemaPlus schema, String name, Map<String, Object> operand, RelDataType rowType) {
		final Object[][] rows = {
			{3, "paint"},
		    {7, "paper"},
		    {1, "brush"},
		    {9, "paint"},
		    {10, "paint"},
		    {8, "crate"}
		};
		return new ProductsTable(ImmutableList.copyOf(rows));
	}

	public static class ProductsTable implements ScannableTable {
		protected final RelProtoDataType protoRowType = new RelProtoDataType() {
			public RelDataType apply(RelDataTypeFactory a0) {
		        return a0.builder()
		            .add("productid", SqlTypeName.INTEGER)
		            .add("description", SqlTypeName.VARCHAR,10)
		            .build();
		}
	};

		private final ImmutableList<Object[]> rows;

		public ProductsTable(ImmutableList<Object[]> rows) {
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
			return Statistics.of(rowCount, null); //add List<ImmutableBitSet>
		}

		public Schema.TableType getJdbcTableType() {
		    return Schema.TableType.TABLE;
		}
		
	}

}