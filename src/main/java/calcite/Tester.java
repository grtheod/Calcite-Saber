package calcite;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlExplainLevel;

import calcite.planner.QueryPlanner;
import calcite.planner.SaberPlanner;
import calcite.planner.physical.PhysicalRuleConverter;
import calcite.planner.physical.SystemConfig;
import calcite.utils.CustomersTableFactory;
import calcite.utils.DataGenerator;
import calcite.utils.OrdersTableFactory;
import calcite.utils.ProductsTableFactory;
import uk.ac.imperial.lsds.saber.SystemConf;

public class Tester {

	public static void main(String[] args) throws Exception {

		Class.forName("org.apache.calcite.jdbc.Driver");

		Connection connection = DriverManager.getConnection("jdbc:calcite:");
		CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
		
		SchemaPlus rootSchema = calciteConnection.getRootSchema();
		
		SchemaPlus schema = rootSchema.add("s", new AbstractSchema());
		
		// useRatesCostModel is a boolean that defines if we want to use the RatesCostModel or not
		boolean useRatesCostModel = true;
		
		schema.add("customers", new CustomersTableFactory().create(schema, "customers", null, null, useRatesCostModel));
		schema.add("orders",   new     OrdersTableFactory().create(schema,    "orders", null, null, useRatesCostModel));
		schema.add("products", new   ProductsTableFactory().create(schema,  "products", null, null, useRatesCostModel));
		
		/* Create a schema in Saber from a given SchemaPlus and add some mock data for testing.*/
		DataGenerator dataGenerator = new DataGenerator()
						.setSchema(schema, true, new ArrayList<Integer>(Arrays.asList(8192,32768, 16384)))
						.build();
		
		Statement statement = connection.createStatement();
		/*QueryPlanner is a combination of both Volcano and heuristic planner.*/
		/*QueryPlanner's constructor needs (Schema, greedy, useRatesCostModel).
		 * @greedy is a boolean that defines if we want a greedy Join Reorder or not
		 * @useRatesCostModel is a boolean that defines if we want to use the RatesCostModel or not
		 * */
		SaberPlanner queryPlanner = new SaberPlanner(rootSchema, true, useRatesCostModel);
		
		/* Until it is fixed, when joining two tables and then using group by, the attributes of group by predicate should be 
		 * from the first table. For example:
		 * ...
		 * FROM table1,table2,...
		 * WHERE table1.attrx = table2.attry AND ...
		 * ...
		 * GROUP BY table1.attrx, ... 
		 * ... 
		 * */
		/* Recall that a default window is a now-window, i. e., a time-based window of size 1.*/	
		/*We recommend that you always include the rowtime column in the SELECT clause. Having a sorted 
		 * timestamp in each stream and streaming query makes it possible to do advanced calculations later, 
		 * such as GROUP BY and JOIN */
		RelNode logicalPlan = queryPlanner.getLogicalPlan (
		        "select s.orders.productid,s.orders.customerid  " 
		                + "from  s.products, s.orders, s.customers " 
		                + "where s.orders.productid = s.products.productid and s.customers.customerid=s.orders.customerid " 
		                + " and units>5 " );
		
		System.out.println (RelOptUtil.toString (logicalPlan, SqlExplainLevel.ALL_ATTRIBUTES));
			
		/*Set System Configuration.*/
		SystemConf sconf = new SystemConfig()
				.setCircularBufferSize(32 * 1048576)
				.setLatencyOn(false)
				.setSchedulingPolicy(SystemConf.SchedulingPolicy.HLS)
				.setSwitchThreshold(10)
				.setThroughputMonitorInterval(1000L)
				.setPartialWindows(64)
				.setHashTableSize(16*32768)
				.setUnboundedBufferSize(2 * 1048576)
				.setThreads(1)
				.build();			
	
		long timestampReference = System.nanoTime();
		PhysicalRuleConverter physicalPlan = new PhysicalRuleConverter (logicalPlan, dataGenerator.getTablesMap(), sconf,timestampReference);
		
		physicalPlan.convert (logicalPlan);
		
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
