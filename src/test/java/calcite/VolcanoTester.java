package calcite;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.CachingRelMetadataProvider;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule;
import org.apache.calcite.rel.rules.AggregateJoinTransposeRule;
import org.apache.calcite.rel.rules.AggregateProjectMergeRule;
import org.apache.calcite.rel.rules.AggregateProjectPullUpConstantsRule;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.rel.rules.AggregateRemoveRule;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterTableScanRule;
import org.apache.calcite.rel.rules.JoinAssociateRule;
import org.apache.calcite.rel.rules.JoinPushExpressionsRule;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.rel.rules.ProjectJoinTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.ProjectTableScanRule;
import org.apache.calcite.rel.rules.ProjectToWindowRule;
import org.apache.calcite.rel.rules.ProjectWindowTransposeRule;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Lists;

import calcite.cost.SaberCostBase;
import calcite.cost.SaberRelOptCostFactory;
import calcite.planner.SaberRelDataTypeSystem;
import calcite.planner.SaberRuleSets;
import calcite.planner.QueryPlanner.MetaDataProviderModifier;
import calcite.planner.physical.PhysicalRuleConverter;
import calcite.planner.physical.SystemConfig;
import calcite.utils.CustomersTableFactory;
import calcite.utils.DataGenerator;
import calcite.utils.OrdersTableFactory;
import calcite.utils.ProductsTableFactory;
import uk.ac.imperial.lsds.saber.SystemConf;

public class VolcanoTester {
	static Planner planner;
	private final static Logger log = LogManager.getLogger (VolcanoTester.class);
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException, ValidationException, RelConversionException {
	    Class.forName("org.apache.calcite.jdbc.Driver");
	    Connection connection =
	        DriverManager.getConnection("jdbc:calcite:");
	    CalciteConnection calciteConnection =
	        connection.unwrap(CalciteConnection.class);
	    SchemaPlus rootSchema = calciteConnection.getRootSchema();
	    SchemaPlus schema = rootSchema.add("s", new AbstractSchema());	    	   	    
	    schema.add("orders", new OrdersTableFactory(32764).create(schema, "orders", null, null));
	    schema.add("products", new ProductsTableFactory(32764).create(schema, "products", null, null));
	    schema.add("customers", new CustomersTableFactory(16480).create(schema, "customers", null, null));	    	  
	    
	    Statement statement = connection.createStatement();	  
	    
	    new VolcanoTester(schema);
	    RelNode rel = getLogicalPlan(
		        "select s.products.productid "
		        + "from s.customers,s.orders,s.products "
		        + "where s.orders.productid = s.products.productid and s.customers.customerid=s.orders.customerid "
		        + "and units>5 "
		        );
	    
	    RelTraitSet traitSet = planner.getEmptyTraitSet().replace(EnumerableConvention.INSTANCE);	
	    RelNode logicalPlan = planner.transform(0, traitSet, rel);
	    
	    HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
	    HepPlanner hepPlanner = new HepPlanner(hepProgramBuilder.build());	   

	    
	    final RelMetadataProvider provider = logicalPlan.getCluster().getMetadataProvider();

	    // Register RelMetadataProvider with HepPlanner.
	    final List<RelMetadataProvider> list = Lists.newArrayList(provider);
	    hepPlanner.registerMetadataProviders(list);
	    final RelMetadataProvider cachingMetaDataProvider = new CachingRelMetadataProvider(ChainedRelMetadataProvider.of(list), hepPlanner);
	    logicalPlan.accept(new MetaDataProviderModifier(cachingMetaDataProvider));
	    
	    
	    System.out.println("Applying rules...");
	    hepPlanner.setRoot(logicalPlan);
	    logicalPlan = hepPlanner.findBestExp();   
	    // I think this line reset the metadata provider instances changed for hep planner execution.
	    rel.accept(new MetaDataProviderModifier(provider));
	    
	    hepPlanner.setRoot(logicalPlan);
	    logicalPlan = hepPlanner.findBestExp();  
	    
	    System.out.println(RelOptUtil.toString(logicalPlan,SqlExplainLevel.EXPPLAN_ATTRIBUTES));
	    
		/* Create a schema in Saber from a given SchemaPlus and add some mock data for testing.*/
		DataGenerator dataGenerator = new DataGenerator()
						.setSchema(schema, true)
						.build();
	    
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
		PhysicalRuleConverter physicalPlan = new PhysicalRuleConverter (logicalPlan, dataGenerator.getTablesMap(), sconf,timestampReference,1048576);
		physicalPlan.convert (logicalPlan);
		
	}
		  
		public VolcanoTester(SchemaPlus schema) {
		    final List<RelTraitDef> traitDefs = new ArrayList<RelTraitDef>();

		    traitDefs.add(ConventionTraitDef.INSTANCE);
		    traitDefs.add(RelCollationTraitDef.INSTANCE);
		    
		    Program program =Programs.ofRules(
		    		//ReduceExpressionsRule.CALC_INSTANCE,
		    	    ProjectToWindowRule.PROJECT,
		    	    //TableScanRule.INSTANCE
		    	    
		    	    // push and merge filter rules
		    	    FilterAggregateTransposeRule.INSTANCE,
		    	    FilterProjectTransposeRule.INSTANCE,
		    	    FilterMergeRule.INSTANCE,
		    	    FilterJoinRule.FILTER_ON_JOIN,
		    	    FilterJoinRule.JOIN, /*push filter into the children of a join*/
		    	    FilterTableScanRule.INSTANCE,
		    	    // push and merge projection rules
		    	    /*check the effectiveness of pushing down projections*/
		    	    ProjectRemoveRule.INSTANCE,
		    	    ProjectJoinTransposeRule.INSTANCE,
		    	    //JoinProjectTransposeRule.BOTH_PROJECT,
		    	    //ProjectFilterTransposeRule.INSTANCE, /*it is better to use filter first an then project*/
		    	    ProjectTableScanRule.INSTANCE,
		    	    ProjectWindowTransposeRule.INSTANCE,
		    	    ProjectMergeRule.INSTANCE,
		    	    //aggregate rules
		    	    AggregateRemoveRule.INSTANCE,
		    	    AggregateJoinTransposeRule.EXTENDED,
		    	    AggregateProjectMergeRule.INSTANCE,
		    	    AggregateProjectPullUpConstantsRule.INSTANCE,
		            AggregateExpandDistinctAggregatesRule.INSTANCE,
		            AggregateReduceFunctionsRule.INSTANCE,
		    	    //join rules    
		    	    /*A simple trick is to consider a window size equal to stream cardinality.
		    	     * For tuple-based windows, the window size is equal to the number of tuples.
		    	     * For time-based windows, the window size is equal to the (input_rate*time_of_window).*/
		    	    //JoinToMultiJoinRule.INSTANCE ,
		    	    //LoptOptimizeJoinRule.INSTANCE ,
		    	    //MultiJoinOptimizeBushyRule.INSTANCE,
		    	    JoinPushThroughJoinRule.RIGHT,
		    	    JoinPushThroughJoinRule.LEFT, /*choose between right and left*/
		    	    JoinPushExpressionsRule.INSTANCE,
		    	    JoinAssociateRule.INSTANCE,
		    	    //JoinCommuteRule.INSTANCE,
		    	    // simplify expressions rules
		    	    //ReduceExpressionsRule.CALC_INSTANCE,
		    	    ReduceExpressionsRule.FILTER_INSTANCE,
		    	    ReduceExpressionsRule.PROJECT_INSTANCE,
		    	    // prune empty results rules   
		    	    PruneEmptyRules.FILTER_INSTANCE,
		    	    PruneEmptyRules.PROJECT_INSTANCE,
		    	    PruneEmptyRules.AGGREGATE_INSTANCE,
		    	    PruneEmptyRules.JOIN_LEFT_INSTANCE,    
		    	    PruneEmptyRules.JOIN_RIGHT_INSTANCE,
		    	    
		    	    /*Enumerable Rules*/
	        		EnumerableRules.ENUMERABLE_FILTER_RULE,
	        		EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
	        		EnumerableRules.ENUMERABLE_PROJECT_RULE,
	        		EnumerableRules.ENUMERABLE_AGGREGATE_RULE,
	        		EnumerableRules.ENUMERABLE_JOIN_RULE,
	        		EnumerableRules.ENUMERABLE_WINDOW_RULE
	        		);

		    SaberRelOptCostFactory saberCostFactory = new SaberCostBase.SaberCostFactory(); //custom factory with rates
		    FrameworkConfig config = Frameworks.newConfigBuilder()
		        .parserConfig(SqlParser.configBuilder()
		            .setLex(Lex.MYSQL)
		            .build())
		        .defaultSchema(schema)
		        .operatorTable(SqlStdOperatorTable.instance()) // TODO: Implement Saber specific operator table
		        .traitDefs(traitDefs)
		        .context(Contexts.EMPTY_CONTEXT)
		        .ruleSets(SaberRuleSets.getRuleSets())
		        .costFactory(saberCostFactory) //If null, use the default cost factory for that planner.
		        .typeSystem(SaberRelDataTypeSystem.SABER_REL_DATATYPE_SYSTEM)
		        .programs(program)		        		        
		        .build();
		    		   
		    this.planner = Frameworks.getPlanner(config);
		}

		public static RelNode getLogicalPlan(String query) throws ValidationException, RelConversionException {
		    SqlNode sqlNode;

		    try {
		      sqlNode = planner.parse(query);
		    } catch (SqlParseException e) {
		      throw new RuntimeException("Query parsing error.", e);
		    }	
		    SqlNode validatedSqlNode = planner.validate(sqlNode);

		    return planner.rel(validatedSqlNode).project();
		}
		
}
