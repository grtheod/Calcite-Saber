package calcite.LRB;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;

import calcite.planner.SaberPlanner;
import calcite.planner.physical.PhysicalRuleConverter;
import calcite.planner.physical.SystemConfig;
import calcite.utils.DataGenerator;
import calcite.utils.PosSpeedStrTableFactory;
import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.SystemConf.SchedulingPolicy;

public class LRBTester {
	
	public static final String usage = "usage: Tester for LRB";

	public static void main(String[] args) throws Exception {
		
		int circularBufferSize =   256 * 1048576;
		boolean latencyOn = false;
		SchedulingPolicy schedulingPolicy = SystemConf.SchedulingPolicy.HLS;
		int switchThreshold = 10;
		long throughputMonitorInterval = 1000L;
		int partialWindows = 65536;
		int hashTableSize = 1048576;
		int unboundedBufferSize = 360 * 1048576;
		int threads = 2;
		int batchSize = 1048576;
		
		// useRatesCostModel is a boolean that defines if we want to use the RatesCostModel or not
		boolean useRatesCostModel = false;
		
		// execute determines whether the plan is executed or not
		boolean execute = true;
		
		// greedyJoinOrder determines which rules will be chosen for the join ordering
		boolean greedyJoinOrder = true;
		
		// noOptimization determines whether optimization rules will be applied or not
		boolean noOptimization = false;
		
		// compute all plans
		boolean allPlans =  false;
		
		String paramQuery = "";
		boolean waitForQuery =  true;
		int queryPlanType = -1;
		
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
			} else
			if (args[i].equals("--rates-cost-model")) { 
				useRatesCostModel = Boolean.parseBoolean(args[j]);
			} else
			if (args[i].equals("--execute")) { 
				execute = Boolean.parseBoolean(args[j]);
			} else
			if (args[i].equals("--greedy-join-order")) { 
				greedyJoinOrder = Boolean.parseBoolean(args[j]);
			} else
			if (args[i].equals("--no-optimization")) { 
				noOptimization = Boolean.parseBoolean(args[j]);
			} else
			if (args[i].equals("--batch-size")) { 
				batchSize = Integer.parseInt(args[j]);
			} else
			if (args[i].equals("--all-plans")) { 
				allPlans = Boolean.parseBoolean(args[j]);
			} else
			if (args[i].equals("--query")) { 
				paramQuery = String.valueOf(args[j]);
				waitForQuery = false;
			} else
			if (args[i].equals("--query-plan-type")) { 
				queryPlanType =  Integer.parseInt(args[j]);
				if (queryPlanType == 1) {
					noOptimization = true;
				} else if (queryPlanType == 2){
					noOptimization = false;
					useRatesCostModel = false;
				} else if (queryPlanType == 3){
					noOptimization = false;
					useRatesCostModel = true;
				} else {
					System.err.println(String.format("error: unknown query plan type %s %s", args[i], args[j]));
					System.exit(1);
				}
			}	
			else {
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
		
		schema.add("PosSpeedStr", new PosSpeedStrTableFactory().create(schema, "PosSpeedStr", null, null, useRatesCostModel));
		DataGenerator dataGenerator = new DataGenerator()
						.setSchema(schema, true, new ArrayList<Integer>(Arrays.asList(819, 3276, 3276, 409, 3276)))
						.build();
		
		Statement statement = connection.createStatement();

		
		String query;
		if (waitForQuery == false) {
			query = paramQuery;
			System.out.println(query);
		} else {
			Scanner in = new Scanner(System.in);		
			System.out.print("Enter a query: ");
			 query="";
			in.useDelimiter("");
			while (in.hasNext()) {
				String temp = in.next();
				if (!temp.equals(";"))
					query += temp;
				else
					break;
			}
		}

		
		if (allPlans == true ){
			// Not optimized plan
			SaberPlanner queryPlanner1 = new SaberPlanner(rootSchema, greedyJoinOrder, useRatesCostModel, true);
			RelNode logicalPlan1 = queryPlanner1.getLogicalPlan (query);
			
			// Optimized Plan with built-in cost model
			SaberPlanner queryPlanner2 = new SaberPlanner(rootSchema, greedyJoinOrder, false, false);
			RelNode logicalPlan2 = queryPlanner2.getLogicalPlan (query);
			
			// Optimized Plan with rate-based cost model			
			rootSchema = calciteConnection.getRootSchema();
			schema = rootSchema.add("s", new AbstractSchema());
			schema.add("PosSpeedStr", new PosSpeedStrTableFactory().create(schema, "PosSpeedStr", null, null, useRatesCostModel));
			dataGenerator = new DataGenerator()
							.setSchema(schema, true, new ArrayList<Integer>(Arrays.asList(819, 3276, 3276, 409, 3276)))
							.build();
			SaberPlanner queryPlanner3 = new SaberPlanner(rootSchema, greedyJoinOrder, true, false);
			RelNode logicalPlan3 = queryPlanner3.getLogicalPlan (query);			
		}
		else{
			SaberPlanner queryPlanner = new SaberPlanner(rootSchema, greedyJoinOrder, useRatesCostModel, noOptimization);
			RelNode logicalPlan = queryPlanner.getLogicalPlan (query);
							
		
			long timestampReference = System.nanoTime();
			PhysicalRuleConverter physicalPlan = new PhysicalRuleConverter (logicalPlan, dataGenerator.getTablesMap(), sconf,timestampReference, batchSize);
			
			physicalPlan.convert (logicalPlan);
			
			boolean getThroughput = true;
			if (execute)
				physicalPlan.execute(getThroughput);
		}
	}
}
