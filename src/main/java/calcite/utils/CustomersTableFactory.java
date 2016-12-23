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

public class CustomersTableFactory implements TableFactory<Table>  {
/*
 * @param schema Schema this table belongs to
 * @param name Name of this table
 * @param operand The "operand" JSON property
 * @param rowType Row type. Specified if the "columns" JSON property. 
 */
	public boolean useRatesCostModel;
	
	@Override
	public Table create(SchemaPlus schema, String name, Map<String, Object> operand, RelDataType rowType) {
		final Object[][] rows = {
			    {ts(10,15,0), 1, 1012},
			    {ts(10,15,0), 2, 5334},
			    {ts(10,15,0), 3, 2222},
			    {ts(10,15,0), 4, 3232},
			    {ts(10,15,0), 5, 3121},
			    {ts(10,15,0), 6, 13232},
			    {ts(10,15,0), 7, 15123},
			};
			return new CustomersTable(ImmutableList.copyOf(rows), useRatesCostModel);
	}
	
	public Table create(SchemaPlus schema, String name, Map<String, Object> operand, RelDataType rowType, boolean useRatesCostModel) {
		this.useRatesCostModel = useRatesCostModel;
		return create(schema, name, operand, rowType);
	}

	public static class CustomersTable implements ScannableTable {
		protected final RelProtoDataType protoRowType = new RelProtoDataType() {
			public RelDataType apply(RelDataTypeFactory a0) {
		        return a0.builder()
		        	.add("rowtime", SqlTypeName.TIMESTAMP)
		            .add("customerid", SqlTypeName.INTEGER)
		            .add("phone", SqlTypeName.BIGINT)
		            .build();
			}
		};

		private final ImmutableList<Object[]> rows;
		public boolean useRatesCostModel;

		public CustomersTable(ImmutableList<Object[]> rows, boolean useRatesCostModel) {
			this.rows = rows;
			this.useRatesCostModel = useRatesCostModel;
		}

		public Enumerable<Object[]> scan(DataContext root) {
		    return Linq4j.asEnumerable(rows);
		}

		public RelDataType getRowType(RelDataTypeFactory typeFactory) {
		    return protoRowType.apply(typeFactory);
		}

		public Statistic getStatistic() {
			//int rowCount = rows.size();
			int rowCount = (this.useRatesCostModel) ? 8192 : 1;
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

