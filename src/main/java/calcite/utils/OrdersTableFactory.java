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

public class OrdersTableFactory implements TableFactory<Table>  {
/*
 * @param schema Schema this table belongs to
 * @param name Name of this table
 * @param operand The "operand" JSON property
 * @param rowType Row type. Specified if the "columns" JSON property. 
 */
	public Table create(SchemaPlus schema, String name, Map<String, Object> operand, RelDataType rowType) {
		final Object[][] rows = {
		    {ts(10,15,0), 1, 3, 10, 1},
		    {ts(10,15,0), 2, 7, 5, 1},
		    {ts(10,15,0), 3, 1, 12, 2},
		    {ts(10,15,0), 4, 9, 3, 3},
		    {ts(10,15,0), 5, 10, 3, 1},
		    {ts(10,15,0), 6, 3, 13, 7},
		    {ts(10,15,0), 7, 7, 15, 5},
		    {ts(10,15,0), 8, 1, 2, 3},
		    {ts(10,15,0), 9, 9, 3, 2},
		    {ts(10,15,0), 10, 10, 2, 4},		    
		    {ts(10,15,0), 11, 8, 1, 6},
		    {ts(10,15,0), 12, 8, 12, 6}
		};
		return new OrdersTable(ImmutableList.copyOf(rows));
	}

	public static class OrdersTable implements ScannableTable {
		protected final RelProtoDataType protoRowType = new RelProtoDataType() {
			public RelDataType apply(RelDataTypeFactory a0) {
		        return a0.builder()
		        	.add("rowtime", SqlTypeName.TIMESTAMP)
		            .add("orderid", SqlTypeName.INTEGER)
		            .add("productid", SqlTypeName.INTEGER)
		            .add("units", SqlTypeName.INTEGER)
		            .add("customerid", SqlTypeName.INTEGER)
		            .build();
			}
		};

		private final ImmutableList<Object[]> rows;

		public OrdersTable(ImmutableList<Object[]> rows) {
			this.rows = rows;
		}

		public Enumerable<Object[]> scan(DataContext root) {
		    return Linq4j.asEnumerable(rows);
		}

		public RelDataType getRowType(RelDataTypeFactory typeFactory) {
		    return protoRowType.apply(typeFactory);
		}

		public Statistic getStatistic() {
			//int rowCount = rows.size();
			int rowCount = 32768;
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
