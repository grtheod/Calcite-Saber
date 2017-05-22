package calcite.planner;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.metadata.CachingRelMetadataProvider;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.schema.SchemaPlus;
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import calcite.cost.SaberCostBase;
import calcite.cost.SaberRelOptCostFactory;
import calcite.planner.logical.SaberRel;
import calcite.planner.logical.rules.SaberCalcAggregateCalcToSaberAggrCalcRule;
import calcite.planner.logical.rules.converter.SaberLogicalAggregateRule;
import calcite.planner.logical.rules.converter.SaberLogicalFilterRule;
import calcite.planner.logical.rules.converter.SaberLogicalJoinRule;
import calcite.planner.logical.rules.converter.SaberLogicalProjectRule;
import calcite.planner.logical.rules.converter.SaberLogicalTableScanRule;
import calcite.planner.logical.rules.converter.SaberLogicalWindowRule;

/**
 * A wrapper around Calcite query planner that is used to parse, validate and generate a physical plan from a streaming
 * SQL query.
 */
public class SaberPlanner {

  public static final int STREAM_RULES = 0;
  public static final int SABER_REL_CONVERSION_RULES = 1;

  private final Planner planner;
  private final boolean greedyJoinOrder;
  private final boolean useRatesCostModel;
  private final boolean noOptimization;
  
  public SaberPlanner(SchemaPlus schema, boolean greedyJoinOrder, boolean useRatesCostModel, boolean noOptimization) {
	this.greedyJoinOrder = greedyJoinOrder;
	this.useRatesCostModel = useRatesCostModel;	
	this.noOptimization = noOptimization;
    final List<RelTraitDef> traitDefs = new ArrayList<RelTraitDef>();

    traitDefs.add(ConventionTraitDef.INSTANCE);
    traitDefs.add(RelCollationTraitDef.INSTANCE);
    
    List<RelOptRule> VOLCANO_RULES = new ArrayList<RelOptRule>();
    		//Arrays.asList(    		    	    
    	    
    		//FilterProjectTransposeRule.INSTANCE,
    		//ProjectFilterTransposeRule.INSTANCE,
    		//AggregateJoinTransposeRule.INSTANCE		    	    

            //));
       
    if (useRatesCostModel) {
    	VOLCANO_RULES.addAll(ImmutableList.of(
        	    SaberLogicalProjectRule.INSTANCE,
        	    SaberLogicalTableScanRule.INSTANCE,
        	    SaberLogicalFilterRule.INSTANCE,
        	    SaberLogicalJoinRule.INSTANCE,
        	    SaberLogicalAggregateRule.INSTANCE,
        	    SaberLogicalWindowRule.INSTANCE,
        	    AbstractConverter.ExpandConversionRule.INSTANCE
    			));
    	VOLCANO_RULES.addAll(SaberRuleSets.VOLCANO_RULES);
    	if (!greedyJoinOrder) {   		
    		VOLCANO_RULES.addAll(SaberRuleSets.EXHAUSTIVE_JOIN_ORDERING_RULES_FOR_RATE);
    		VOLCANO_RULES.remove(SaberRuleSets.SABER_PROJECT_MERGE_RULE); 
    	}
    } else {
    	VOLCANO_RULES.addAll(SaberRuleSets.PRE_JOIN_ORDERING_RULES);
    	if (!greedyJoinOrder)     		
    		VOLCANO_RULES.addAll(SaberRuleSets.EXHAUSTIVE_JOIN_ORDERING_RULES);
    }
      
    Program program =Programs.ofRules(VOLCANO_RULES);
    SaberRelOptCostFactory saberCostFactory = (useRatesCostModel) ? new SaberCostBase.SaberCostFactory() : null; //SaberCostFactory is custom factory with rates

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

  public RelNode hepOptimization(RelNode basePlan, HepMatchOrder order, RelOptRule... rules) throws RelConversionException {	 	   
    
    HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
    hepProgramBuilder.addMatchOrder(order);
    for (RelOptRule r : rules)
	      hepProgramBuilder.addRuleInstance(r);
    /*  
    	HepPlanner hepPlanner = new HepPlanner(hepProgram,null, noDag, null, RelOptCostImpl.FACTORY);
  		with the option to keep the graph a tree(noDAG=true) or allow DAG(noDAG=false).
    */
    HepPlanner hepPlanner = new HepPlanner(hepProgramBuilder.build(),
	          basePlan.getCluster().getPlanner().getContext());
    
    final RelMetadataProvider provider = basePlan.getCluster().getMetadataProvider();
    RelMetadataQuery.THREAD_PROVIDERS.set(JaninoRelMetadataProvider.of(provider));
    
    // Register RelMetadataProvider with HepPlanner.
    final List<RelMetadataProvider> list = Lists.newArrayList(provider);
    hepPlanner.registerMetadataProviders(list);
    final RelMetadataProvider cachingMetaDataProvider = new CachingRelMetadataProvider(ChainedRelMetadataProvider.of(list), hepPlanner);
    basePlan.accept(new MetaDataProviderModifier(cachingMetaDataProvider));
    
    hepPlanner.setRoot(basePlan);
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
	    
	    RelNode finalPlan=null;
	    if (noOptimization) { //skip optimization rules
	        // Optimization Phase 0
	        System.out.println("Optimization Phase 0 : Applying rules required for converting the logical operators to physical...");        
	        ImmutableList<RelOptRule> noOptimizationRules = SaberRuleSets.NO_OPTIMIZATION_RULES;
	        finalPlan = hepOptimization(convertedNode, HepMatchOrder.BOTTOM_UP,
	            noOptimizationRules.toArray(new RelOptRule[noOptimizationRules.size()]) );
	        System.out.println("Skipping other optimization phases...");
	        System.out.println ();
	        System.out.println ("The optimal logical plan is:");
	        System.out.println (RelOptUtil.toString (finalPlan, SqlExplainLevel.ALL_ATTRIBUTES));
	        
	    } else { // apply optimization rules
	    	// Optimization Phase 1
	    	System.out.println("Optimization Phase 1 : Applying Window Rewrite rules with HepPlanner...");
	    	ImmutableList<RelOptRule> windowRewriteRules = SaberRuleSets.WINDOW_REWRITE_RULES;
	    	RelNode preOptimizationNode = hepOptimization(convertedNode, HepMatchOrder.BOTTOM_UP,
	    			windowRewriteRules.toArray(new RelOptRule[windowRewriteRules.size()]) );
	      
	    	// Optimization Phase 2
	    	System.out.println("Optimization Phase 2 : Applying heuristic rules that don't use the cost model with HepPlanner...");
	    	ImmutableList<RelOptRule> preVolcanoStaticRules = SaberRuleSets.PRE_VOLCANO_STATIC_RULES;
	    	RelNode preVolcanoNode = hepOptimization(preOptimizationNode, HepMatchOrder.BOTTOM_UP,
	    			preVolcanoStaticRules.toArray(new RelOptRule[preVolcanoStaticRules.size()]) );
	      
	    	//System.out.println (RelOptUtil.toString (preVolcanoNode, SqlExplainLevel.ALL_ATTRIBUTES));
	    	// Optimization Phase 3
	    	//RelTraitSet traitSet = beforeplan.getTraitSet();
	    	//traitSet = traitSet.simplify(); // TODO: Is this the correct thing to do? Why relnode has a composite trait?
	    	System.out.println("Optimization Phase 3 : Applying cost-based rules that use the cost model with VolcanoPlanner...");
	    	RelTraitSet traitSet = (useRatesCostModel) ? planner.getEmptyTraitSet().replace(SaberRel.SABER_LOGICAL)
	    			: planner.getEmptyTraitSet().replace(EnumerableConvention.INSTANCE);  
	    	RelNode volcanoPlan = planner.transform(0, traitSet, preVolcanoNode);       
	      
	    	//System.out.println (RelOptUtil.toString (volcanoPlan, SqlExplainLevel.ALL_ATTRIBUTES));
	    	RelNode afterJoinNode;
	    	if (greedyJoinOrder) {
	    		// Optimization Phase 4
	    		System.out.println("Optimization Phase 4 : Applying heuristic rules for Join ordering with HepPlanner...");
	    		ImmutableList<RelOptRule> heuristicJoinOrderingRules = (useRatesCostModel) ? SaberRuleSets.HEURISTIC_JOIN_ORDERING_RULES_2
	    				: SaberRuleSets.HEURISTIC_JOIN_ORDERING_RULES;
	    		afterJoinNode = hepOptimization(volcanoPlan, HepMatchOrder.BOTTOM_UP,
	    				heuristicJoinOrderingRules.toArray(new RelOptRule[heuristicJoinOrderingRules.size()]) );        
	    	} else {
	    		System.out.println("Skipping Optimization Phase 4. The Join ordering is chosen dynamically from the previous phase.");
	    		afterJoinNode = volcanoPlan;
	    	}
	      
	    	// Print here the final Cost
	    	//System.out.println (RelOptUtil.toString (afterJoinNode, SqlExplainLevel.ALL_ATTRIBUTES));
	    	if (useRatesCostModel) {
	    		// Optimization Phase 5.a
	    		System.out.println("Optimization Phase 5.a : Applying after Join heuristic rules with HepPlanner...");
	    		ImmutableList<RelOptRule> afterJoinRules = SaberRuleSets.AFTER_JOIN_RULES_2;
	    		RelNode afterJoinPlan = hepOptimization(afterJoinNode, HepMatchOrder.BOTTOM_UP,
	    				afterJoinRules.toArray(new RelOptRule[afterJoinRules.size()]) );
	        
	    		// Optimization Phase 5.b
	    		System.out.println("Optimization Phase 5.b : Applying after Join heuristic rules with HepPlanner...");
	    		ImmutableList<RelOptRule> afterJoinRules2 = SaberRuleSets.PROJECT_PUSH_DOWN;
	    		afterJoinPlan = hepOptimization(afterJoinPlan, HepMatchOrder.BOTTOM_UP,
	    				afterJoinRules2.toArray(new RelOptRule[afterJoinRules.size()]) );
	    		
	    		// Optimization Phase 5.c
	    		System.out.println("Optimization Phase 5.c : Applying Calc rules with HepPlanner...");
	    		ImmutableList<RelOptRule> saberCalcRules = SaberRuleSets.SABER_CALC_RULES;
	    		afterJoinPlan = hepOptimization(afterJoinPlan, HepMatchOrder.BOTTOM_UP,
	    				saberCalcRules.toArray(new RelOptRule[saberCalcRules.size()]) );
	    		
	    		// Optimization Phase 5.d
/*	    		System.out.println("Optimization Phase 5.d : Applying AggrCalc rules with HepPlanner...");
	    		RelOptRule saberAggrCalcRules = SaberCalcAggregateCalcToSaberAggrCalcRule.INSTANCE; //SaberRuleSets.SABER_AGGR_CALC_RULES;
	    		afterJoinPlan = hepOptimization(afterJoinPlan, HepMatchOrder.BOTTOM_UP, saberAggrCalcRules);*/
	    		
	    		// Print here the final Cost
	    		System.out.println ();
	    		System.out.println ("The optimal logical plan is:");
	    		System.out.println (RelOptUtil.toString (afterJoinPlan, SqlExplainLevel.ALL_ATTRIBUTES));
	        
	    		// Optimization Phase 6
	    		System.out.println("Optimization Phase 6 : Applying heuristic rules that convert either Saber or Enumerable Convention operators to Logical with HepPlanner...");
	    		ImmutableList<RelOptRule> convertToLogicalRules = SaberRuleSets.CONVERT_TO_LOGICAL_RULES_2;
	    		finalPlan = hepOptimization(afterJoinPlan, HepMatchOrder.BOTTOM_UP,
	    				convertToLogicalRules.toArray(new RelOptRule[convertToLogicalRules.size()]) );              
	    	} else {
	    		System.out.println("Optimization Phase 5 : Applying heuristic rules that convert either Saber or Enumerable Convention operators to Logical with HepPlanner...");
	    		ImmutableList<RelOptRule> convertToLogicalRules = SaberRuleSets.CONVERT_TO_LOGICAL_RULES;
	    		RelNode convertedLogicalNode = hepOptimization(afterJoinNode, HepMatchOrder.BOTTOM_UP,
	    				convertToLogicalRules.toArray(new RelOptRule[convertToLogicalRules.size()]) );
	                  
	    		// Optimization Phase 6
	    		System.out.println("Optimization Phase 6 : Applying after Join heuristic rules with HepPlanner...");
	    		ImmutableList<RelOptRule> afterJoinRules = SaberRuleSets.AFTER_JOIN_RULES;
	    		finalPlan = hepOptimization(convertedLogicalNode, HepMatchOrder.BOTTOM_UP,
	    				afterJoinRules.toArray(new RelOptRule[afterJoinRules.size()]) );
	        
	    		// Print here the final Cost
	    		System.out.println ();
	    		System.out.println ("The optimal logical plan is:");
	    		System.out.println (RelOptUtil.toString (finalPlan, SqlExplainLevel.ALL_ATTRIBUTES));
	    	}
	    }

	    System.out.println("Returning plan...");    
	    return finalPlan;    
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