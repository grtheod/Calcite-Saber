package calcite.planner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.metadata.CachingRelMetadataProvider;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
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
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.rel.rules.JoinProjectTransposeRule;
import org.apache.calcite.rel.rules.JoinPushExpressionsRule;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.rel.rules.JoinPushTransitivePredicatesRule;
import org.apache.calcite.rel.rules.JoinToMultiJoinRule;
import org.apache.calcite.rel.rules.LoptJoinTree;
import org.apache.calcite.rel.rules.LoptOptimizeJoinRule;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.rules.MultiJoinOptimizeBushyRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.ProjectJoinTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.ProjectTableScanRule;
import org.apache.calcite.rel.rules.ProjectToWindowRule;
import org.apache.calcite.rel.rules.ProjectWindowTransposeRule;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.rules.TableScanRule;
import org.apache.calcite.schema.SchemaPlus;
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
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.ValidationException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import calcite.cost.SaberCostBase;
import calcite.cost.SaberRelOptCostFactory;
import calcite.planner.logical.EnumerableAggregateToLogicalAggregateRule;
import calcite.planner.logical.EnumerableFilterToLogicalFilterRule;
import calcite.planner.logical.EnumerableJoinToLogicalJoinRule;
import calcite.planner.logical.EnumerableProjectToLogicalProjectRule;
import calcite.planner.logical.EnumerableTableScanToLogicalTableScanRule;
import calcite.planner.logical.EnumerableWindowToLogicalWindowRule;
import calcite.planner.logical.FilterPushThroughFilter;
import calcite.planner.physical.SaberLogicalConvention;

/**
 * A wrapper around Calcite query planner that is used to parse, validate and generate a physical plan from a streaming
 * SQL query.
 */
public class SaberPlanner {

  public static final int STREAM_RULES = 0;
  public static final int SABER_REL_CONVERSION_RULES = 1;

  private final Planner planner;
  private final HepPlanner hepPlanner;

  public SaberPlanner(SchemaPlus schema, boolean greedyJoinOrder, boolean bushyJoin) {
    final List<RelTraitDef> traitDefs = new ArrayList<RelTraitDef>();

    traitDefs.add(ConventionTraitDef.INSTANCE);
    traitDefs.add(RelCollationTraitDef.INSTANCE);
    
    List<RelOptRule> VOLCANO_RULES = new ArrayList<RelOptRule>(Arrays.asList(    		
    	    ProjectToWindowRule.PROJECT,
    	    //TableScanRule.INSTANCE
    	    
    	    /*Unlike select operators, window operators should be pushed up*/
    	    
    	    // push and merge filter rules
    	    FilterAggregateTransposeRule.INSTANCE,
    	    FilterProjectTransposeRule.INSTANCE,
    	    //FilterMergeRule.INSTANCE,
    	    FilterJoinRule.FILTER_ON_JOIN,
    	    FilterJoinRule.JOIN, /*push filter into the children of a join*/
    	    FilterTableScanRule.INSTANCE,
    	    FilterPushThroughFilter.INSTANCE,
    	    
    	    // push and merge projection rules
    	    /*check the effectiveness of pushing down projections*/
    	    ProjectRemoveRule.INSTANCE,
    	    //ProjectJoinTransposeRule.INSTANCE, /*it is implemented in hepPlanner*/
    	    //JoinProjectTransposeRule.BOTH_PROJECT,
    	    //ProjectFilterTransposeRule.INSTANCE, /*it is better to use filter first an then project*/
    	    ProjectTableScanRule.INSTANCE,
    	    ProjectWindowTransposeRule.INSTANCE, /*maybe it has to be implemented in hepPlanner*/
    	    ProjectMergeRule.INSTANCE,
    	    //maybe implement ProjectFilterPullUpConstantsRule.INSTANCE
    	    
    	    //aggregate rules
    	    AggregateRemoveRule.INSTANCE,
    	    AggregateJoinTransposeRule.EXTENDED,
    	    AggregateProjectMergeRule.INSTANCE,
    	    AggregateProjectPullUpConstantsRule.INSTANCE,
            AggregateExpandDistinctAggregatesRule.INSTANCE,
            AggregateReduceFunctionsRule.INSTANCE, /*Has to be implemented in a better way.*/
            //AggregateExpandDistinctAggregatesRule.JOIN,
            
            //join rules    
    	    /*A simple trick is to consider a window size (or better input rate) equal to stream cardinality.
    	     * For tuple-based windows, the window size is equal to the number of tuples.
    	     * For time-based windows, the window size is equal to the (input_rate*time_of_window).*/
    	    //JoinToMultiJoinRule.INSTANCE ,
    	    //LoptOptimizeJoinRule.INSTANCE ,
    	    //MultiJoinOptimizeBushyRule.INSTANCE,
            JoinPushThroughJoinRule.LEFT, 
            JoinPushThroughJoinRule.RIGHT,
            JoinAssociateRule.INSTANCE,
    	    JoinCommuteRule.INSTANCE,
    	    JoinPushExpressionsRule.INSTANCE,
    	    //maybe implement JoinAddNotNullRule.INSTANCE
    	    JoinPushTransitivePredicatesRule.INSTANCE, //Planner rule that infers predicates from on a Join and creates Filter if those predicates can be pushed to its inputs.
    	    
    	    // simplify expressions rules    	    
    	    ReduceExpressionsRule.FILTER_INSTANCE,
    	    ReduceExpressionsRule.PROJECT_INSTANCE,
    	    ReduceExpressionsRule.JOIN_INSTANCE,    	    
    	    //ReduceDecimalsRule.INSTANCE,
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
    	    //EnumerableRules.ENUMERABLE_VALUES_RULE //The VALUES clause creates an inline table with a given set of rows.
    												 //Streaming is disallowed. The set of rows never changes, and therefore 
    												 //a stream would never return any rows.*/
            ));
    
    if (greedyJoinOrder) {
    	VOLCANO_RULES.removeAll(ImmutableList.of(
    			JoinPushThroughJoinRule.LEFT, 
                JoinPushThroughJoinRule.RIGHT,
                JoinAssociateRule.INSTANCE,
        	    JoinCommuteRule.INSTANCE));
    }
    
    Program program =Programs.ofRules(VOLCANO_RULES);
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

    // Create hep planner for optimizations.
    HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
    hepProgramBuilder.addRuleInstance(EnumerableJoinToLogicalJoinRule.INSTANCE);
    hepProgramBuilder.addRuleInstance(EnumerableProjectToLogicalProjectRule.INSTANCE);
    hepProgramBuilder.addRuleInstance(EnumerableFilterToLogicalFilterRule.INSTANCE);
    hepProgramBuilder.addRuleInstance(EnumerableAggregateToLogicalAggregateRule.INSTANCE);
    hepProgramBuilder.addRuleInstance(EnumerableTableScanToLogicalTableScanRule.INSTANCE);
    hepProgramBuilder.addRuleInstance(EnumerableWindowToLogicalWindowRule.INSTANCE);
    hepProgramBuilder.addRuleInstance(ProjectToWindowRule.INSTANCE); 
    hepProgramBuilder.addRuleInstance(ProjectJoinTransposeRule.INSTANCE);
    hepProgramBuilder.addRuleInstance(ProjectRemoveRule.INSTANCE);
    hepProgramBuilder.addRuleInstance(ProjectTableScanRule.INSTANCE);
    hepProgramBuilder.addRuleInstance(ProjectWindowTransposeRule.INSTANCE); 
    hepProgramBuilder.addRuleInstance(ProjectMergeRule.INSTANCE);
    hepProgramBuilder.addRuleInstance(AggregateProjectMergeRule.INSTANCE);
    hepProgramBuilder.addRuleInstance(AggregateProjectPullUpConstantsRule.INSTANCE);
    //hepProgramBuilder.addRuleInstance(JoinToMultiJoinRule.INSTANCE);
    //hepProgramBuilder.addRuleInstance(LoptOptimizeJoinRule.INSTANCE);
       
    hepProgramBuilder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
    /*maybe add addMatchOrder(HepMatchOrder.BOTTOM_UP)to HepPlanner and change
     final HepPlanner hepPlanner = new HepPlanner(hepProgram,null, noDag, null, RelOptCostImpl.FACTORY);
      	with the option to keep the graph a tree(noDAG=true) or allow DAG(noDAG=false).
     */
    this.hepPlanner = new HepPlanner(hepProgramBuilder.build());
    
    /*The order in which the rules within a collection will be attempted is
     arbitrary, so if more control is needed, use addRuleInstance.*/
  }

  public RelNode hepOptimization(RelNode convertedNode) throws RelConversionException {	 	   
    
    final RelMetadataProvider provider = convertedNode.getCluster().getMetadataProvider();

    // Register RelMetadataProvider with HepPlanner.
    final List<RelMetadataProvider> list = Lists.newArrayList(provider);
    hepPlanner.registerMetadataProviders(list);
    final RelMetadataProvider cachingMetaDataProvider = new CachingRelMetadataProvider(ChainedRelMetadataProvider.of(list), hepPlanner);
    convertedNode.accept(new MetaDataProviderModifier(cachingMetaDataProvider));
    
    /*
     * Maybe change the above two lines of code with :
     * RelMetadataProvider plannerChain = ChainedRelMetadataProvider.of(list);
     * convertedNode.getCluster().setMetadataProvider(plannerChain);
     */
    /*
    List<RelMetadataProvider> list = Lists.newArrayList();
    list.add(DefaultRelMetadataProvider.INSTANCE);
    hepPlanner.registerMetadataProviders(list);
    RelMetadataProvider plannerChain =
        ChainedRelMetadataProvider.of(list);
    convertedNode.getCluster().setMetadataProvider(plannerChain);
    */
    
    System.out.println("Applying rules...");
    hepPlanner.setRoot(convertedNode);
    RelNode rel = hepPlanner.findBestExp();
    
    // I think this line reset the metadata provider instances changed for hep planner execution.
    rel.accept(new MetaDataProviderModifier(provider));
   
    return rel;
  }

  private SqlNode validateNode(SqlNode sqlNode) throws ValidationException {
    SqlNode validatedSqlNode = planner.validate(sqlNode);

    validatedSqlNode.accept(new UnsupportedOperatorsVisitor());

    return validatedSqlNode;
  }


  public RelNode getLogicalPlan(String query) throws ValidationException, RelConversionException {
    SqlNode sqlNode;

    try {
      sqlNode = planner.parse(query);
    } catch (SqlParseException e) {
      throw new RuntimeException("Query parsing error.", e);
    }

    SqlNode validatedSqlNode = validateNode(sqlNode);
    RelNode convertedNode = planner.convert(validatedSqlNode); 
          
    //use Volcano Planner to convert nodes
    //RelTraitSet traitSet = beforeplan.getTraitSet();
    //traitSet = traitSet.simplify(); // TODO: Is this the correct thing to do? Why relnode has a composite trait?
    RelTraitSet traitSet = planner.getEmptyTraitSet().replace(EnumerableConvention.INSTANCE);	
    RelNode volcanoPlan = planner.transform(0, traitSet, convertedNode);       
    
    //compute cost
    //final RelMetadataQuery mq = RelMetadataQuery.instance();
    //RelMetadataQuery.THREAD_PROVIDERS.set( JaninoRelMetadataProvider.of(new MyRelMetadataProvider()));
    /*RelOptCost relCost= mq.getCumulativeCost(beforeplan); //beforeplan.computeSelfCost( hepPlanner,mq);	 	   
    System.out.println("Plan cost is : " + relCost.toString());

    System.out.println("Row count : " + mq.getRowCount(beforeplan));*/
    System.out.println("Returning plan...");
    
    RelNode finalPlan= hepOptimization(volcanoPlan);
    return finalPlan;    
    //return planner.rel(validatedSqlNode).project();
  }  
  
  
  public SqlNode parseQuery(String sql) throws Exception {
      SqlParser parser = SqlParser.create(sql);
      return parser.parseQuery();
  }
  
  
  // TODO: This is from Drill. Not sure what it does.
  public static class MetaDataProviderModifier extends RelShuttleImpl {
    private final RelMetadataProvider metadataProvider;

    public MetaDataProviderModifier(RelMetadataProvider metadataProvider) {
      this.metadataProvider = metadataProvider;
    }

    @Override
    public RelNode visit(TableScan scan) {
      scan.getCluster().setMetadataProvider(metadataProvider);
      return super.visit(scan);
    }

    @Override
    public RelNode visit(TableFunctionScan scan) {
      scan.getCluster().setMetadataProvider(metadataProvider);
      return super.visit(scan);
    }

    @Override
    public RelNode visit(LogicalValues values) {
      values.getCluster().setMetadataProvider(metadataProvider);
      return super.visit(values);
    }

    @Override
    protected RelNode visitChild(RelNode parent, int i, RelNode child) {
      child.accept(this);
      parent.getCluster().setMetadataProvider(metadataProvider);
      return parent;
    }
  }

}