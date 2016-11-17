package calcite.utils;

import java.util.Map;

import org.apache.calcite.DataContext;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.RelCollations;
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

import com.google.common.collect.ImmutableList;

public class ProductsTableFactory implements TableFactory<Table>  {
	
	public Table create(SchemaPlus schema, String name, Map<String, Object> operand, RelDataType rowType) {
		final Object[][] rows = {
		    {ts(10,15,0), 3, "paint"},
		    {ts(10,15,0), 7, "paper"},
		    {ts(10,15,0), 1, "brush"},
		    {ts(10,15,0), 9, "paint"},
		    {ts(10,15,0), 10, "paint"},
		    {ts(10,15,0), 8, "crate"}
		};
		return new ProductsTable(ImmutableList.copyOf(rows));
	}

	public static class ProductsTable implements ScannableTable {
		protected final RelProtoDataType protoRowType = new RelProtoDataType() {
			public RelDataType apply(RelDataTypeFactory a0) {
		        return a0.builder()
		        	.add("rowtime", SqlTypeName.TIMESTAMP)
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
			return Statistics.of(rowCount, ImmutableList.<ImmutableBitSet>of(),
					RelCollations.createSingleton(0)); //add List<ImmutableBitSet>
		}

		public Schema.TableType getJdbcTableType() {
		    return Schema.TableType.TABLE;
		}		
	}

    private static Object ts(int h, int m, int s) {
        return DateTimeUtils.unixTimestamp(2016, 10, 8, h, m, s);
    }
}
