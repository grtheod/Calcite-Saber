package calcite;

import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

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
import uk.ac.imperial.lsds.saber.SystemConf.SchedulingPolicy;

public class Tester {
	
	public static final String usage = "usage: Tester for the system";

	public static void main(String[] args) throws Exception {
		
		int circularBufferSize = 32 * 1048576;
		boolean latencyOn = false;
		SchedulingPolicy schedulingPolicy = SystemConf.SchedulingPolicy.HLS;
		int switchThreshold = 10;
		long throughputMonitorInterval = 1000L;
		int partialWindows = 64;
		int hashTableSize = 16*32768;
		int unboundedBufferSize = 2 * 1048576;
		int threads = 1;
		
		/* Parse command line arguments */
		int i, j;
		for (i = 0; i < args.length; ) {
			if ((j = i + 1) == args.length) {
				System.err.println(usage);
				System.exit(1);
			}
			if (args[i].equals("--threads")) {
				threads = Integer.parseInt(args[j]);
			} else
			if (args[i].equals("--circular-buffer-size")) { 
				circularBufferSize = Integer.parseInt(args[j]);
			} else
			if (args[i].equals("--latency-on")) { 
				latencyOn = Boolean.parseBoolean(args[j]);
			} else
			if (args[i].equals("--scheduling-policy")) { 
				schedulingPolicy = SystemConf.SCHEDULING_POLICY.valueOf(args[j]);
			} else
			if (args[i].equals("--switch-threshold")) { 
				switchThreshold = Integer.parseInt(args[j]);
			} else
			if (args[i].equals("--throughput-monitor-interval")) { 
				throughputMonitorInterval = Long.parseLong(args[j]);
			} else
			if (args[i].equals("--partial-windows")) { 
				partialWindows = Integer.parseInt(args[j]);
			} else
			if (args[i].equals("--hash-table-size")) { 
				hashTableSize = Integer.parseInt(args[j]);
			} else
			if (args[i].equals("--unbounded-buffer-size")) { 
				unboundedBufferSize = Integer.parseInt(args[j]);
			} else {
				System.err.println(String.format("error: unknown flag %s %s", args[i], args[j]));
				System.exit(1);
			}
			i = j + 1;
		}		
		
		/*Set System Configuration.*/
		SystemConf sconf = new SystemConfig()
				.setCircularBufferSize(circularBufferSize)
				.setLatencyOn(latencyOn)
				.setSchedulingPolicy(schedulingPolicy)
				.setSwitchThreshold(switchThreshold)
				.setThroughputMonitorInterval(throughputMonitorInterval)
				.setPartialWindows(partialWindows)
				.setHashTableSize(hashTableSize)
				.setUnboundedBufferSize(unboundedBufferSize)
				.setThreads(threads)
				.build();	

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
		/* We recommend that you always include the rowtime column in the SELECT clause. Having a sorted 
		 * timestamp in each stream and streaming query makes it possible to do advanced calculations later, 
		 * such as GROUP BY and JOIN */
		
		Scanner in = new Scanner(System.in);		
		System.out.print("Enter a query: ");
		String query="";		
	    in.useDelimiter("");
	    while (in.hasNext()) {
	    	String temp = in.next();
	    	if (!temp.equals(";"))
	    		query += temp;
	    	else
	    		break;
	    }
		RelNode logicalPlan = queryPlanner.getLogicalPlan (query);
		
//		RelNode logicalPlan = queryPlanner.getLogicalPlan (
//             "select rowtime, sum(units), count(orderid) "
//             + "from  s.orders "         
//             + "group by rowtime,units,orderid, floor(rowtime to second)" 
//             );
		
		//System.out.println (RelOptUtil.toString (logicalPlan, SqlExplainLevel.ALL_ATTRIBUTES));					
	
		long timestampReference = System.nanoTime();
		PhysicalRuleConverter physicalPlan = new PhysicalRuleConverter (logicalPlan, dataGenerator.getTablesMap(), sconf,timestampReference);
		
		physicalPlan.convert (logicalPlan);
		
		//physicalPlan.execute();
		
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
