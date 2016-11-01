package calcite.planner;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptPlanner;
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
import org.apache.calcite.rel.rules.AggregateJoinTransposeRule;
import org.apache.calcite.rel.rules.AggregateProjectMergeRule;
import org.apache.calcite.rel.rules.AggregateProjectPullUpConstantsRule;
import org.apache.calcite.rel.rules.AggregateRemoveRule;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterTableScanRule;
import org.apache.calcite.rel.rules.JoinAssociateRule;
import org.apache.calcite.rel.rules.JoinProjectTransposeRule;
import org.apache.calcite.rel.rules.JoinPushExpressionsRule;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
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
import org.apache.calcite.tools.ValidationException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import calcite.cost.SaberCostBase;
import calcite.cost.SaberRelOptCostFactory;
import calcite.planner.logical.JoinToSaberJoinRule;
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

  public SaberPlanner(SchemaPlus schema) {
    final List<RelTraitDef> traitDefs = new ArrayList<RelTraitDef>();

    traitDefs.add(ConventionTraitDef.INSTANCE);
    traitDefs.add(RelCollationTraitDef.INSTANCE);
    
    Program program =Programs.ofRules(
    		//ProjectMergeRule.INSTANCE,
    		//ProjectRemoveRule.INSTANCE,
            //ProjectJoinTransposeRule.INSTANCE,
            //JoinProjectTransposeRule.BOTH_PROJECT,
            JoinPushThroughJoinRule.RIGHT,
            JoinPushThroughJoinRule.LEFT, 
            JoinPushExpressionsRule.INSTANCE,
            JoinAssociateRule.INSTANCE,
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
        .costFactory(null) //If null, use the default cost factory for that planner.
        .typeSystem(SaberRelDataTypeSystem.SABER_REL_DATATYPE_SYSTEM)
        .programs(program)
        .build();
    this.planner = Frameworks.getPlanner(config);
    //AggregateExpandDistinctAggregatesRule.INSTANCE,
    //AggregateReduceFunctionsRule.INSTANCE,

    // Create hep planner for optimizations.
    HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
    hepProgramBuilder.addRuleClass(ReduceExpressionsRule.class);
    //hepProgramBuilder.addRuleClass(PruneEmptyRules.class);
    hepProgramBuilder.addRuleClass(FilterTableScanRule.class);
    hepProgramBuilder.addRuleClass(FilterMergeRule.class);
    hepProgramBuilder.addRuleClass(FilterProjectTransposeRule.class);
    hepProgramBuilder.addRuleClass(FilterAggregateTransposeRule.class);
    hepProgramBuilder.addRuleClass(FilterJoinRule.class);
    hepProgramBuilder.addRuleClass(JoinProjectTransposeRule.class);
    hepProgramBuilder.addRuleClass(ProjectToWindowRule.class);
    hepProgramBuilder.addRuleClass(ProjectJoinTransposeRule.class);
    hepProgramBuilder.addRuleClass(ProjectTableScanRule.class);
    hepProgramBuilder.addRuleClass(ProjectFilterTransposeRule.class);
    hepProgramBuilder.addRuleClass(ProjectRemoveRule.class);
    hepProgramBuilder.addRuleClass(AggregateRemoveRule.class);
    hepProgramBuilder.addRuleClass(AggregateJoinTransposeRule.class);
    hepProgramBuilder.addRuleClass(AggregateProjectMergeRule.class);
    hepProgramBuilder.addRuleClass(AggregateProjectPullUpConstantsRule.class);
    hepProgramBuilder.addRuleClass(TableScanRule.class);
    hepProgramBuilder.addRuleClass(ProjectWindowTransposeRule.class);
    hepProgramBuilder.addRuleClass(LoptOptimizeJoinRule.class);
    hepProgramBuilder.addRuleClass(MultiJoinOptimizeBushyRule.class);
    hepProgramBuilder.addRuleClass(JoinPushThroughJoinRule.class);
    hepProgramBuilder.addRuleClass(JoinToMultiJoinRule.class);
    hepProgramBuilder.addRuleClass(ProjectMergeRule.class);
    hepProgramBuilder.addRuleClass(JoinPushExpressionsRule.class);
    
    hepProgramBuilder.addRuleClass(JoinToSaberJoinRule.class);

    //hepProgramBuilder.addMatchOrder(HepMatchOrder.ARBITRARY);
    /*maybe add addMatchOrder(HepMatchOrder.BOTTOM_UP)to HepPlanner and change
     final HepPlanner hepPlanner = new HepPlanner(hepProgram,null, noDag, null, RelOptCostImpl.FACTORY);
      	with the option to keep the graph a tree(noDAG=true) or allow DAG(noDAG=false).
     */

    this.hepPlanner = new HepPlanner(hepProgramBuilder.build());
    
    /*The order in which the rules within a collection will be attempted is
     arbitrary, so if more control is needed, use addRuleInstance instead.*/

    //starting rules      
    /*Unlike select operators, window operators should be pushed up*/
    this.hepPlanner.addRule(ProjectToWindowRule.PROJECT);
    //this.hepPlanner.addRule(TableScanRule.INSTANCE);
    
    // push and merge filter rules
    this.hepPlanner.addRule(FilterAggregateTransposeRule.INSTANCE);
    this.hepPlanner.addRule(FilterProjectTransposeRule.INSTANCE);
    this.hepPlanner.addRule(FilterMergeRule.INSTANCE);
    this.hepPlanner.addRule(FilterJoinRule.FILTER_ON_JOIN);
    this.hepPlanner.addRule(FilterJoinRule.JOIN); /*push filter into the children of a join*/
    this.hepPlanner.addRule(FilterTableScanRule.INSTANCE);
    // push and merge projection rules
    /*check the effectiveness of pushing down projections*/
    this.hepPlanner.addRule(ProjectRemoveRule.INSTANCE);
    this.hepPlanner.addRule(ProjectJoinTransposeRule.INSTANCE);
    this.hepPlanner.addRule(JoinProjectTransposeRule.BOTH_PROJECT);
    //this.hepPlanner.addRule(ProjectFilterTransposeRule.INSTANCE); /*it is better to use filter first an then project*/
    this.hepPlanner.addRule(ProjectTableScanRule.INSTANCE);
    this.hepPlanner.addRule(ProjectWindowTransposeRule.INSTANCE);
    this.hepPlanner.addRule(ProjectMergeRule.INSTANCE);
    //aggregate rules
    this.hepPlanner.addRule(AggregateRemoveRule.INSTANCE);
    this.hepPlanner.addRule(AggregateJoinTransposeRule.EXTENDED);
    this.hepPlanner.addRule(AggregateProjectMergeRule.INSTANCE);
    this.hepPlanner.addRule(AggregateProjectPullUpConstantsRule.INSTANCE);
    //join rules    
    /*A simple trick is to consider a window size equal to stream cardinality.
     * For tuple-based windows, the window size is equal to the number of tuples.
     * For time-based windows, the window size is equal to the (input_rate*time_of_window).*/
    //this.hepPlanner.addRule(JoinToMultiJoinRule.INSTANCE); 
    //this.hepPlanner.addRule(LoptOptimizeJoinRule.INSTANCE); 
    //this.hepPlanner.addRule(MultiJoinOptimizeBushyRule.INSTANCE);
    // simplify expressions rules
    //this.hepPlanner.addRule(ReduceExpressionsRule.CALC_INSTANCE);
    this.hepPlanner.addRule(ReduceExpressionsRule.FILTER_INSTANCE);
    this.hepPlanner.addRule(ReduceExpressionsRule.PROJECT_INSTANCE);
    // prune empty results rules   
    this.hepPlanner.addRule(PruneEmptyRules.FILTER_INSTANCE);
    this.hepPlanner.addRule(PruneEmptyRules.PROJECT_INSTANCE);
    this.hepPlanner.addRule(PruneEmptyRules.AGGREGATE_INSTANCE);
    this.hepPlanner.addRule(PruneEmptyRules.JOIN_LEFT_INSTANCE);    
    this.hepPlanner.addRule(PruneEmptyRules.JOIN_RIGHT_INSTANCE);
            
  }

  private RelNode validateAndConvert(SqlNode sqlNode) throws ValidationException, RelConversionException {
    SqlNode validated = validateNode(sqlNode);
    RelNode relNode = convertToRelNode(validated);

    // Drill does pre-processing too. We can do pre processing here if needed.
    // Drill also preserve the validated type of the sql query for later use. But current Calcite
    // API doesn't provide a way to get validated type.

    return convertToSaberRel(relNode);
  }

  private RelNode convertToSaberRel(RelNode relNode) throws RelConversionException {
    RelTraitSet traitSet = relNode.getTraitSet();
    traitSet = traitSet.simplify(); // TODO: Is this the correct thing to do? Why relnode has a composite trait?
    return planner.transform(SABER_REL_CONVERSION_RULES, traitSet.plus(SaberLogicalConvention.INSTANCE), relNode);
  }

  public RelNode convertToRelNode(SqlNode sqlNode) throws RelConversionException {
	  
	final RelNode convertedNode = planner.convert(sqlNode);    
    
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

  private RelNode applyStreamRules(RelNode relNode) throws RelConversionException {
    return planner.transform(STREAM_RULES, relNode.getTraitSet(), relNode);
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

    SqlNode validatedSqlNode = planner.validate(sqlNode);
      
    RelNode beforeplan= convertToRelNode(validatedSqlNode);   
   
    //use Volcano Planner to conver nodes
    //RelTraitSet traitSet = beforeplan.getTraitSet();
    //traitSet = traitSet.simplify(); // TODO: Is this the correct thing to do? Why relnode has a composite trait?
    RelTraitSet traitSet = planner.getEmptyTraitSet().replace(EnumerableConvention.INSTANCE);	
    //RelNode logicalPlan = planner.transform(0, traitSet, rel);
    beforeplan = planner.transform(0, traitSet, beforeplan);       
    
    //compute cost
    final RelMetadataQuery mq = RelMetadataQuery.instance();
    //RelMetadataQuery.THREAD_PROVIDERS.set( JaninoRelMetadataProvider.of(new MyRelMetadataProvider()));
    /*RelOptCost relCost= mq.getCumulativeCost(beforeplan); //beforeplan.computeSelfCost( hepPlanner,mq);	 	   
    System.out.println("Plan cost is : " + relCost.toString());

    System.out.println("Row count : " + mq.getRowCount(beforeplan));*/
    System.out.println("Returning plan...");
    return beforeplan;    
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