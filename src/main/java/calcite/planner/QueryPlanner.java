package calcite.planner;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptPlanner.Executor;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.metadata.CachingRelMetadataProvider;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
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
import org.apache.calcite.rel.rules.ProjectMultiJoinMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.ProjectTableScanRule;
import org.apache.calcite.rel.rules.ProjectToWindowRule;
import org.apache.calcite.rel.rules.ProjectWindowTransposeRule;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.rules.TableScanRule;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
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
import calcite.planner.logical.rules.EnumerableAggregateToLogicalAggregateRule;
import calcite.planner.logical.rules.EnumerableFilterToLogicalFilterRule;
import calcite.planner.logical.rules.EnumerableJoinToLogicalJoinRule;
import calcite.planner.logical.rules.EnumerableProjectToLogicalProjectRule;
import calcite.planner.logical.rules.EnumerableTableScanToLogicalTableScanRule;
import calcite.planner.logical.rules.EnumerableWindowToLogicalWindowRule;
import calcite.planner.logical.rules.FilterPushThroughFilter;
import calcite.planner.physical.SaberLogicalConvention;

/**
 * A wrapper around Calcite query planner that is used to parse, validate and generate a physical plan from a streaming
 * SQL query.
 */
public class QueryPlanner {

  public static final int STREAM_RULES = 0;
  public static final int SABER_REL_CONVERSION_RULES = 1;

  private final Planner planner;
  private final Statement statement;
  private final SaberRelOptCostFactory saberCostFactory;
  private final boolean bushy;
  private final FrameworkConfig config;

  public QueryPlanner(SchemaPlus schema, Statement statement, boolean bushyJoin) {
	this.statement = statement;
	this.bushy = bushyJoin;
	
    final List<RelTraitDef> traitDefs = new ArrayList<RelTraitDef>();

    traitDefs.add(ConventionTraitDef.INSTANCE);
    traitDefs.add(RelCollationTraitDef.INSTANCE);
     
    saberCostFactory = new SaberCostBase.SaberCostFactory(); //custom factory with rates
    ImmutableList<RelOptRule> preJoinRules = SaberRuleSets.PRE_JOIN_ORDERING_RULES;
    Program program = Programs.ofRules(preJoinRules);
    config = Frameworks.newConfigBuilder()
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
     
  private SqlNode validateNode(SqlNode sqlNode) throws ValidationException {
    SqlNode validatedSqlNode = planner.validate(sqlNode);

    validatedSqlNode.accept(new UnsupportedOperatorsVisitor());

    return validatedSqlNode;
  }


  public RelNode getLogicalPlan(String query) throws ValidationException, RelConversionException, SQLException {
    // 1. Gen Calcite Plan
	SqlNode sqlNode;
    try {
      sqlNode = planner.parse(query);
    } catch (SqlParseException e) {
      throw new RuntimeException("Query parsing error.", e);
    }

    SqlNode validatedSqlNode = validateNode(sqlNode);
    RelNode calciteGenPlan = planner.convert(validatedSqlNode); 
          
    //RelFieldTrimmer fieldTrimmer = new RelFieldTrimmer(SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(), Prepare.CatalogReader , SaberRelDataTypeSystem.SABER_REL_DATATYPE_SYSTEM));
    //fieldTrimmer.trim(calciteGenPlan);
    
    // Create and set MD provider    
    final RelMetadataProvider provider = calciteGenPlan.getCluster().getMetadataProvider();
    RelMetadataQuery.THREAD_PROVIDERS.set(JaninoRelMetadataProvider.of(provider));
    
    // Create executor
    DataContext dataContext =
            Schemas.createDataContext(statement.getConnection());
    final Executor executor = config.getExecutor();//new RexExecutorImpl(dataContext);
    
    // 2. Apply pre-join order optimizations
    //ImmutableList<RelOptRule> preJoinRules = SaberRuleSets.PRE_JOIN_ORDERING_RULES;
    //RelNode calcitePreCboPlan = volcanoPlan(calciteGenPlan, preJoinRules.toArray(new RelOptRule[preJoinRules.size()]));
    RelTraitSet traitSet = planner.getEmptyTraitSet().replace(EnumerableConvention.INSTANCE);	
    RelNode calcitePreCboPlan = planner.transform(0, traitSet, calciteGenPlan);   
    ImmutableList<RelOptRule> convertToLogicalRules = SaberRuleSets.CONVERT_TO_LOGICAL_RULES;
    calcitePreCboPlan = hepPlan(calcitePreCboPlan, true, provider, executor, HepMatchOrder.BOTTOM_UP,
    		convertToLogicalRules.toArray(new RelOptRule[convertToLogicalRules.size()]));     
    calcitePreCboPlan.accept(new MetaDataProviderModifier(provider)); // I think this line reset the metadata provider instances changed for hep planner execution.

    // 3. Apply join order optimizations: reordering MST algorithm       
    RelNode calciteOptimizedPlan = null;
    try {
        List<RelMetadataProvider> list = Lists.newArrayList();
        list.add(provider);

        HepProgramBuilder hepPgmBldr = new HepProgramBuilder().addMatchOrder(HepMatchOrder.BOTTOM_UP);        
        hepPgmBldr.addRuleInstance(JoinToMultiJoinRule.INSTANCE);
        if (bushy)
        	hepPgmBldr.addRuleInstance(MultiJoinOptimizeBushyRule.INSTANCE);
        else
        	hepPgmBldr.addRuleInstance(LoptOptimizeJoinRule.INSTANCE);
        
        HepProgram hepPgm = hepPgmBldr.build();
        HepPlanner hepPlanner = new HepPlanner(hepPgm);

        hepPlanner.registerMetadataProviders(list);
        RelMetadataProvider chainedProvider = ChainedRelMetadataProvider.of(list);
        calcitePreCboPlan.getCluster().setMetadataProvider(new CachingRelMetadataProvider(chainedProvider, hepPlanner));

        RelNode rootRel = calcitePreCboPlan;
        hepPlanner.setRoot(rootRel);

        calciteOptimizedPlan = hepPlanner.findBestExp();
        calciteOptimizedPlan.accept(new MetaDataProviderModifier(provider));
    }
    catch (Exception e) {
    	System.err.println("Missing column stats (see previous messages), skipping join reordering in CBO");
		//System.err.println("Exception:" + e.getMessage());
		//System.exit(1);
    }

    // 4. Run other optimizations that do not need stats
    ImmutableList<RelOptRule> afterJoinRules = SaberRuleSets.AFTER_JOIN_RULES;
    try {
        List<RelMetadataProvider> list = Lists.newArrayList();
        list.add(provider);

        HepProgramBuilder hepPgmBldr = new HepProgramBuilder().addMatchOrder(HepMatchOrder.BOTTOM_UP);        
        for (RelOptRule r : afterJoinRules)
        	hepPgmBldr.addRuleInstance(r);
        
        HepProgram hepPgm = hepPgmBldr.build();
        HepPlanner hepPlanner = new HepPlanner(hepPgm);

        hepPlanner.registerMetadataProviders(list);
        RelMetadataProvider chainedProvider = ChainedRelMetadataProvider.of(list);
        calciteOptimizedPlan.getCluster().setMetadataProvider(new CachingRelMetadataProvider(chainedProvider, hepPlanner));

        RelNode rootRel = calciteOptimizedPlan;
        hepPlanner.setRoot(rootRel);

        calciteOptimizedPlan = hepPlanner.findBestExp();
        calciteOptimizedPlan.accept(new MetaDataProviderModifier(provider));
    }
    catch (Exception e) {
		System.err.println("Exception:" + e.getMessage());
		//System.exit(1);
    }    
    
    // 5. Run rules to aid in translation from Calcite tree to Saber tree => to be implemented
    
    RelNode finalPlan = calciteOptimizedPlan;
    return finalPlan;    
  }  
  
  public SqlNode parseQuery(String sql) throws Exception {
      SqlParser parser = SqlParser.create(sql);
      return parser.parseQuery();
  }
  

  /**
   * Run the HEP Planner with the given rule set.
   *
   * @param basePlan
   * @param followPlanChanges
   * @param mdProvider
   * @param executorProvider
   * @param rules
   * @return optimized RelNode
   */
  private RelNode hepPlan(RelNode basePlan, boolean followPlanChanges,
      RelMetadataProvider mdProvider, Executor executorProvider, RelOptRule... rules) {
	return hepPlan(basePlan, followPlanChanges, mdProvider, executorProvider,
            HepMatchOrder.TOP_DOWN, rules);
  }

  /**
   * Run the HEP Planner with the given rule set.
   *
   * @param basePlan
   * @param followPlanChanges
   * @param mdProvider
   * @param executorProvider
   * @param order
   * @param rules
   * @return optimized RelNode
   */
  private RelNode hepPlan(RelNode basePlan, boolean followPlanChanges,
	        RelMetadataProvider mdProvider, Executor executorProvider, HepMatchOrder order,
	        RelOptRule... rules) {

	  RelNode optimizedRelNode = basePlan;
	  HepProgramBuilder programBuilder = new HepProgramBuilder();
	  if (followPlanChanges) {
	    programBuilder.addMatchOrder(order);
	    programBuilder = programBuilder.addRuleCollection(ImmutableList.copyOf(rules));
	  } else {
	    // TODO: Should this be also TOP_DOWN?
	    for (RelOptRule r : rules)
	      programBuilder.addRuleInstance(r);
	  }
	
	  // Create planner and copy context
	  HepPlanner planner = new HepPlanner(programBuilder.build(),
	          basePlan.getCluster().getPlanner().getContext());
	
	  List<RelMetadataProvider> list = Lists.newArrayList();
	  list.add(mdProvider);
	  planner.registerMetadataProviders(list);
	  RelMetadataProvider chainedProvider = ChainedRelMetadataProvider.of(list);
	  basePlan.getCluster().setMetadataProvider(
	      new CachingRelMetadataProvider(chainedProvider, planner));
	
	  if (executorProvider != null) {
	    basePlan.getCluster().getPlanner().setExecutor(executorProvider);
	  }
	
	  planner.setRoot(basePlan);
	  optimizedRelNode = planner.findBestExp();
	
	  return optimizedRelNode;
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