package calcite;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
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
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterTableScanRule;
import org.apache.calcite.rel.rules.JoinProjectTransposeRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.ProjectJoinTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectTableScanRule;
import org.apache.calcite.rel.rules.ProjectToWindowRule;
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

import calcite.planner.SaberRelDataTypeSystem;
import calcite.planner.SaberRuleSets;
import calcite.utils.CustomersTableFactory;
import calcite.utils.OrdersTableFactory;
import calcite.utils.ProductsTableFactory;

public class VolcanoTester {
	static Planner planner;
	
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
	    RelNode rel = getLogicalPlan("select s.orders.orderid from s.orders where units > 5");
	    RelTraitSet traitSet = planner.getEmptyTraitSet().replace(Convention.NONE);
	    RelNode logicalPlan = planner.transform(0, traitSet, rel);
	    System.out.println(RelOptUtil.toString(logicalPlan,SqlExplainLevel.EXPPLAN_ATTRIBUTES));
	    
		
	}
		  
		public VolcanoTester(SchemaPlus schema) {
		    final List<RelTraitDef> traitDefs = new ArrayList<RelTraitDef>();

		    traitDefs.add(ConventionTraitDef.INSTANCE);
		    traitDefs.add(RelCollationTraitDef.INSTANCE);
		    
		    Program program =Programs.ofRules(ReduceExpressionsRule.CALC_INSTANCE,
	        		FilterJoinRule.FILTER_ON_JOIN,
	        		FilterProjectTransposeRule.INSTANCE,
	        		FilterAggregateTransposeRule.INSTANCE,
	        		FilterTableScanRule.INSTANCE,
	        		ProjectToWindowRule.PROJECT,
	        		ProjectJoinTransposeRule.INSTANCE,
	        		JoinProjectTransposeRule.BOTH_PROJECT,
	        		ProjectTableScanRule.INSTANCE,
	        		ProjectFilterTransposeRule.INSTANCE
	        		//,ProjectMergeRule.INSTANCE,
	        		//FilterMergeRule.INSTANCE
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
