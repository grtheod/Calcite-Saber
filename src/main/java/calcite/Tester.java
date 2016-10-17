package calcite;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.Pair;

import calcite.planner.QueryPlanner;
import calcite.planner.physical.PhysicalRuleConverter;
import calcite.planner.physical.SystemConfig;
import calcite.utils.CustomersTableFactory;
import calcite.utils.OrdersTableFactory;
import calcite.utils.ProductsTableFactory;
import calcite.utils.SchemaConverter;
import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.SystemConf;

public class Tester {

	public static void main(String[] args) throws Exception {

		Class.forName("org.apache.calcite.jdbc.Driver");

		Connection connection = DriverManager.getConnection("jdbc:calcite:");
		CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
		
		SchemaPlus rootSchema = calciteConnection.getRootSchema();
		
		SchemaPlus schema = rootSchema.add("s", new AbstractSchema());
		
		schema.add("orders",   new     OrdersTableFactory().create(schema,    "orders", null, null));
		schema.add("products", new   ProductsTableFactory().create(schema,  "products", null, null));
		schema.add("customers", new CustomersTableFactory().create(schema, "customers", null, null));
		
		/* Create a schema in Saber from a given SchemaPlus and add some mock data for testing.*/
		SchemaConverter schemaConverter = new SchemaConverter(schema);
		List<Pair<String,ITupleSchema>> tablesList = schemaConverter.convert();
		Map<String, Pair<ITupleSchema,Pair<byte [],ByteBuffer>>> tablesMap = schemaConverter.setMockInput(tablesList);
		
		Statement statement = connection.createStatement();
		
		QueryPlanner queryPlanner = new QueryPlanner(rootSchema);
		
		/* Until it is fixed, when joining two tables, the attributes of join predicate should have 
		 * the same order as the tables are defined after FROM. For example:
		 * ...
		 * FROM table1,table2,...
		 * WHERE table1.attrx = table2.attry AND ...
		 * ... 
		 * */
		RelNode logicalPlan = queryPlanner.getLogicalPlan (				
				  "select s.products.productid , sum(units) "
				+ "from s.orders, s.products "
				+ "where s.orders.productid = s.products.productid "
				+ "group by  s.products.productid"
				);
				
		System.out.println (RelOptUtil.toString (logicalPlan, SqlExplainLevel.EXPPLAN_ATTRIBUTES));
		
		/*Set System Configuration.*/
		SystemConf sconf = new SystemConfig()
				.setCircularBufferSize(32 * 1048576)
				.setLatencyOn(false)
				.setSchedulingPolicy(SystemConf.SchedulingPolicy.HLS)
				.setSwitchThreshold(10)
				.setThroughputMonitorInterval(1000L)
				.setPartialWindows(64)
				.setHashTableSize(32768)
				.setUnboundedBufferSize(2 * 1048576)
				.setThreads(1)
				.build();			
	
		long timestampReference = System.nanoTime();
		PhysicalRuleConverter physicalPlan = new PhysicalRuleConverter (logicalPlan, tablesMap, sconf,timestampReference);
		
		physicalPlan.convert (logicalPlan);
		
		// Commented out for now
		physicalPlan.execute();
		
		/*
		 * Notes:
		 * 
		 * main () {
		 * 		
		 * 		schema = ...
		 * 		query = "select..."
		 * 		
		 * 		QueryPlanner planner = new QueryPlanner (schema)
		 * 		RelNode logicalPlanRoot = planner.getLogicalPlan (query);
		 * 		
		 * 		PhysicalRuleConverter converter = new PhysicalRuleConverter (logicalPlanRoot);
		 * 		QueryApplication saberApp = converter.convert ();
		 * 		
		 * 		RamdomDataGenerator generator = new RamdomDataGenerator ()
		 * 			.setSchema (schema)
		 * 			.setBundleSize(1024)
		 * 			...
		 * 		
		 * 		// Set any system configuration parameters
		 * 		saberApp.init ();
		 * 		
		 * 		while (true) {
		 * 			saberApp.processData (generator.nextBundle())
		 * 		}
		 * }
		 */
	}
}
