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

public class PaymentsTableFactory implements TableFactory<Table>  {
/*
 * @param schema Schema this table belongs to
 * @param name Name of this table
 * @param operand The "operand" JSON property
 * @param rowType Row type. Specified if the "columns" JSON property. 
 */
	public boolean useRatesCostModel;
	public int inputRate;
	
	public PaymentsTableFactory(int inputRate) {
		this.inputRate = inputRate;
	}

	@Override
	public Table create(SchemaPlus schema, String name, Map<String, Object> operand, RelDataType rowType) {
		final Object[][] rows = {
			    {ts(10,15,0), 1, 1012, 10},
			    {ts(10,15,0), 2, 5334, 12},
			    {ts(10,15,0), 3, 2222, 15},
			    {ts(10,15,0), 4, 3232, 35}
			};
			return new PaymentsTable(ImmutableList.copyOf(rows), useRatesCostModel, inputRate);
	}
	
	public Table create(SchemaPlus schema, String name, Map<String, Object> operand, RelDataType rowType, boolean useRatesCostModel) {
		this.useRatesCostModel = useRatesCostModel;
		return create(schema, name, operand, rowType);
	}

	public static class PaymentsTable implements ScannableTable {
		protected final RelProtoDataType protoRowType = new RelProtoDataType() {
			public RelDataType apply(RelDataTypeFactory a0) {
		        return a0.builder()
		        	.add("rowtime", SqlTypeName.TIMESTAMP)
		            .add("customerid", SqlTypeName.INTEGER)
		            .add("payment_date", SqlTypeName.INTEGER)
		            .add("amount", SqlTypeName.FLOAT)
		            .build();
			}
		};

		private final ImmutableList<Object[]> rows;
		private boolean useRatesCostModel;
		private int inputRate;

		public PaymentsTable(ImmutableList<Object[]> rows, boolean useRatesCostModel, int inputRate) {
			this.rows = rows;
			this.useRatesCostModel = useRatesCostModel;
			this.inputRate = inputRate;
		}

		public Enumerable<Object[]> scan(DataContext root) {
		    return Linq4j.asEnumerable(rows);
		}

		public RelDataType getRowType(RelDataTypeFactory typeFactory) {
		    return protoRowType.apply(typeFactory);
		}

		public Statistic getStatistic() {
			//int rowCount = rows.size();
			int rowCount = (this.useRatesCostModel) ? this.inputRate : 1;
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

