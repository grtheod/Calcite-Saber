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
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.AggregateJoinTransposeRule;
import org.apache.calcite.rel.rules.AggregateProjectMergeRule;
import org.apache.calcite.rel.rules.AggregateProjectPullUpConstantsRule;
import org.apache.calcite.rel.rules.AggregateRemoveRule;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterTableScanRule;
import org.apache.calcite.rel.rules.JoinProjectTransposeRule;
import org.apache.calcite.rel.rules.JoinPushExpressionsRule;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.rel.rules.LoptOptimizeJoinRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.ProjectJoinTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.ProjectTableScanRule;
import org.apache.calcite.rel.rules.ProjectToWindowRule;
import org.apache.calcite.rel.rules.ProjectWindowTransposeRule;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.RuleSets;
import org.apache.calcite.tools.ValidationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import calcite.planner.QueryPlanner;
import calcite.planner.SaberRelDataTypeSystem;
import calcite.planner.SaberRuleSets;
import calcite.planner.logical.JoinToSaberJoinRule;
import calcite.planner.physical.PhysicalRuleConverter;
import calcite.utils.CustomersTableFactory;
import calcite.utils.OrdersTableFactory;
import calcite.utils.ProductsTableFactory;

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
	    schema.add("orders", new OrdersTableFactory().create(schema, "orders", null, null));
	    schema.add("products", new ProductsTableFactory().create(schema, "products", null, null));
	    schema.add("customers", new CustomersTableFactory().create(schema, "customers", null, null));	    	  
	    
	    Statement statement = connection.createStatement();	  
	    
	    new VolcanoTester(schema);
	    RelNode rel = getLogicalPlan(
		        "select s.products.productid "
		        + "from s.products,s.customers,s.orders "
		        + "where s.orders.productid = s.products.productid and s.customers.customerid=s.orders.customerid "
		        + "and units>5 "
		        );
	    
	    RelTraitSet traitSet = planner.getEmptyTraitSet().replace(EnumerableConvention.INSTANCE);	
	    RelNode logicalPlan = planner.transform(0, traitSet, rel);
	    System.out.println(RelOptUtil.toString(logicalPlan,SqlExplainLevel.ALL_ATTRIBUTES));
	    
		log.info(String.format("Root is %s", logicalPlan.toString()));			
		List<RelNode> inputs = logicalPlan.getInputs();
		log.info(String.format("Root has %d inputs", inputs.size()));
		while (true) {
			if (inputs.size() == 1){
				RelNode chainTail = inputs.get(0);		
				log.info(String.format("Node %2d is %s", chainTail.getId(), chainTail.toString()));
				logicalPlan = chainTail;
			} else
			if (inputs.size() == 2) {
				RelNode chainTail = inputs.get(0);		
				log.info(String.format("Node %2d is %s", chainTail.getId(), chainTail.toString()));	
				chainTail = inputs.get(1);		
				log.info(String.format("Node %2d is %s", chainTail.getId(), chainTail.toString()));	
				logicalPlan = chainTail; //follow the second child 
			} else {
				break;
			}			
			inputs = logicalPlan.getInputs();		
		}
		
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

		    FrameworkConfig config = Frameworks.newConfigBuilder()
		        .parserConfig(SqlParser.configBuilder()
		            .setLex(Lex.MYSQL)
		            .build())
		        .defaultSchema(schema)
		        .operatorTable(SqlStdOperatorTable.instance()) // TODO: Implement Saber specific operator table
		        .traitDefs(traitDefs)
		        .context(Contexts.EMPTY_CONTEXT)
		        .ruleSets(SaberRuleSets.getRuleSets())
		        .costFactory(null) //If null, use the default cost factory for that planner.
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
