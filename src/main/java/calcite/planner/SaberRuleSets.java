package calcite.planner;

import java.util.Iterator;

import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.plan.RelOptRule;
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
import org.apache.calcite.rel.rules.FilterRemoveIsNotDistinctFromRule;
import org.apache.calcite.rel.rules.FilterTableScanRule;
import org.apache.calcite.rel.rules.JoinAssociateRule;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.rel.rules.JoinPushExpressionsRule;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.rel.rules.JoinPushTransitivePredicatesRule;
import org.apache.calcite.rel.rules.JoinToMultiJoinRule;
import org.apache.calcite.rel.rules.LoptOptimizeJoinRule;
import org.apache.calcite.rel.rules.MultiJoinOptimizeBushyRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.ProjectJoinTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.ProjectTableScanRule;
import org.apache.calcite.rel.rules.ProjectToWindowRule;
import org.apache.calcite.rel.rules.ProjectWindowTransposeRule;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.rules.PushProjector;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.stream.StreamRules;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RuleSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import calcite.planner.logical.SaberAggregateRel;
import calcite.planner.logical.SaberFilterRel;
import calcite.planner.logical.SaberJoinRel;
import calcite.planner.logical.SaberProjectRel;
import calcite.planner.logical.SaberRelFactories;
import calcite.planner.logical.rules.EnumerableAggregateToLogicalAggregateRule;
import calcite.planner.logical.rules.EnumerableFilterToLogicalFilterRule;
import calcite.planner.logical.rules.EnumerableJoinToLogicalJoinRule;
import calcite.planner.logical.rules.EnumerableProjectToLogicalProjectRule;
import calcite.planner.logical.rules.EnumerableTableScanToLogicalTableScanRule;
import calcite.planner.logical.rules.EnumerableWindowToLogicalWindowRule;
import calcite.planner.logical.rules.FilterPushThroughFilter;
import calcite.planner.logical.rules.ProjectJoinRemoveRule;
import calcite.planner.logical.rules.SaberAggregateRelToLogicalAggregateRule;
import calcite.planner.logical.rules.SaberFilterRelToLogicalFilterRule;
import calcite.planner.logical.rules.SaberJoinRelToLogicalJoinRule;
import calcite.planner.logical.rules.SaberProjectRelToLogicalProjectRule;
import calcite.planner.logical.rules.SaberTableScanRelToLogicalTableScanRule;
import calcite.planner.logical.rules.SaberWindowRelToLogicalWindowRule;

public class SaberRuleSets {
	public static final ImmutableList<RelOptRule> PRE_JOIN_ORDERING_RULES =
			ImmutableList.of(				
				ProjectToWindowRule.PROJECT,
			   
		        //1. Distinct aggregate rewrite
			    // Run this optimization early, since it is expanding the operator pipeline.
				AggregateExpandDistinctAggregatesRule.INSTANCE,
				
				// 2. Run exhaustive PPD, add not null filters, transitive inference,
		        // constant propagation, constant folding
				FilterAggregateTransposeRule.INSTANCE,
				FilterProjectTransposeRule.INSTANCE,
				//FilterMergeRule.INSTANCE,
				FilterJoinRule.FILTER_ON_JOIN,
				FilterJoinRule.JOIN, /*push filter into the children of a join*/
				FilterPushThroughFilter.INSTANCE,
				ReduceExpressionsRule.FILTER_INSTANCE,
				ReduceExpressionsRule.PROJECT_INSTANCE,
				ReduceExpressionsRule.JOIN_INSTANCE,    	    
				//ReduceDecimalsRule.INSTANCE,
				//maybe implement JoinAddNotNullRule.INSTANCE
				JoinPushTransitivePredicatesRule.INSTANCE, //Planner rule that infers predicates from on a Join and creates Filter if those predicates can be pushed to its inputs.
				AggregateProjectPullUpConstantsRule.INSTANCE,
				AggregateReduceFunctionsRule.INSTANCE, 
				AggregateRemoveRule.INSTANCE,				
				
		        // 3. Merge, remove and reduce Project if possible
				ProjectRemoveRule.INSTANCE,
				ProjectWindowTransposeRule.INSTANCE, 
				ProjectMergeRule.INSTANCE,
				//maybe implement ProjectFilterPullUpConstantsRule.INSTANCE
		        ProjectTableScanRule.INSTANCE,
				
			   	// 4. Prune empty result rules				
				PruneEmptyRules.FILTER_INSTANCE,
				PruneEmptyRules.PROJECT_INSTANCE,
				PruneEmptyRules.AGGREGATE_INSTANCE,
				PruneEmptyRules.JOIN_LEFT_INSTANCE,    
				PruneEmptyRules.JOIN_RIGHT_INSTANCE,
				
				// 5. Enumerable Rules
				EnumerableRules.ENUMERABLE_FILTER_RULE,
				EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
				EnumerableRules.ENUMERABLE_PROJECT_RULE,
				EnumerableRules.ENUMERABLE_AGGREGATE_RULE,
				EnumerableRules.ENUMERABLE_JOIN_RULE,
				EnumerableRules.ENUMERABLE_WINDOW_RULE
				//EnumerableRules.ENUMERABLE_VALUES_RULE //The VALUES clause creates an inline table with a given set of rows.
														 //Streaming is disallowed. The set of rows never changes, and therefore 
														 //a stream would never return any rows.*/					  
				);
	
	//These rules can only be used in Volcano Planner
	public static final ImmutableList<RelOptRule> EXHAUSTIVE_JOIN_ORDERING_RULES =
		ImmutableList.of(
				JoinPushThroughJoinRule.LEFT,
				JoinPushThroughJoinRule.RIGHT,
				JoinAssociateRule.INSTANCE,
				JoinCommuteRule.INSTANCE,
				EnumerableRules.ENUMERABLE_FILTER_RULE,
				EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
				EnumerableRules.ENUMERABLE_PROJECT_RULE,
				EnumerableRules.ENUMERABLE_AGGREGATE_RULE,
				EnumerableRules.ENUMERABLE_JOIN_RULE,
				EnumerableRules.ENUMERABLE_WINDOW_RULE
				);
	
	static final RelOptRule SABER_AGGREGATE_JOIN_TRANSPOSE_RULE = new AggregateJoinTransposeRule(SaberAggregateRel.class, 
														SaberJoinRel.class, SaberRelFactories.SABER_LOGICAL_BUILDER, false);
	static final RelOptRule SABER_FILTER_AGGREGATE_TRANSPOSE_RULE = new FilterAggregateTransposeRule(SaberFilterRel.class, 
														SaberRelFactories.SABER_LOGICAL_BUILDER, SaberAggregateRel.class);
	static final RelOptRule SABER_FILTER_PROJECT_TRANSPOSE_RULE = new FilterProjectTransposeRule(SaberFilterRel.class, SaberProjectRel.class, true, true,
	          											SaberRelFactories.SABER_LOGICAL_BUILDER);
	static final RelOptRule SABER_PROJECT_FILTER_TRANSPOSE_RULE = new ProjectFilterTransposeRule(SaberProjectRel.class, SaberFilterRel.class,
														SaberRelFactories.SABER_LOGICAL_BUILDER, PushProjector.ExprCondition.FALSE);
	static final RelOptRule SABER_PROJECT_JOIN_TRANSPOSE_RULE = new ProjectJoinTransposeRule(PushProjector.ExprCondition.FALSE, SaberRelFactories.SABER_LOGICAL_BUILDER);
	
	static final RelOptRule SABER_PROJECT_MERGE_RULE = new ProjectMergeRule(true, SaberRelFactories.SABER_LOGICAL_PROJECT_FACTORY);

	static final RelOptRule SABER_JOIN_COMMUTE_RULE = new JoinCommuteRule(SaberJoinRel.class, SaberRelFactories.SABER_LOGICAL_BUILDER, false);
	
	public static final ImmutableList<RelOptRule> VOLCANO_RULES =
		ImmutableList.of(
				SABER_AGGREGATE_JOIN_TRANSPOSE_RULE,
				SABER_FILTER_AGGREGATE_TRANSPOSE_RULE,
				SABER_FILTER_PROJECT_TRANSPOSE_RULE,
				SABER_PROJECT_FILTER_TRANSPOSE_RULE,
				//SABER_PROJECT_JOIN_TRANSPOSE_RULE,
				SABER_PROJECT_MERGE_RULE
				);
	
	//These rules are only capable of producing left-deep joins
	public static final ImmutableList<RelOptRule> HEURISTIC_JOIN_ORDERING_RULES =
		ImmutableList.of(
					JoinToMultiJoinRule.INSTANCE ,
					LoptOptimizeJoinRule.INSTANCE
				);
	
	static final RelOptRule SABER_JOIN_TO_MULTIJOIN_RULE = new JoinToMultiJoinRule(SaberJoinRel.class);
	static final RelOptRule SABER_LOPT_OPTIMIZE_JOIN_RULE = new LoptOptimizeJoinRule(
											      SaberRelFactories.SABER_LOGICAL_JOIN_FACTORY,
											      SaberRelFactories.SABER_LOGICAL_PROJECT_FACTORY,
											      SaberRelFactories.SABER_LOGICAL_FILTER_FACTORY);
	
	public static final ImmutableList<RelOptRule> HEURISTIC_JOIN_ORDERING_RULES_2 =
			ImmutableList.of(
						SABER_JOIN_TO_MULTIJOIN_RULE ,
						SABER_LOPT_OPTIMIZE_JOIN_RULE,
						ProjectRemoveRule.INSTANCE,
						SABER_PROJECT_MERGE_RULE,
						SABER_PROJECT_JOIN_TRANSPOSE_RULE
					);
	
	//These rules produce bushy joins
	public static final ImmutableList<RelOptRule> HEURISTIC_BUSHY_JOIN_ORDERING_RULES =
		ImmutableList.of(
		   	        JoinToMultiJoinRule.INSTANCE ,
		   	        MultiJoinOptimizeBushyRule.INSTANCE
				);		

	public static final ImmutableList<RelOptRule> AFTER_JOIN_RULES =
		ImmutableList.of(
				// 1. Run other optimizations that do not need stats
				JoinPushExpressionsRule.INSTANCE,
				ProjectRemoveRule.INSTANCE,
				ProjectMergeRule.INSTANCE,
				AggregateProjectMergeRule.INSTANCE,
				//ProjectJoinTransposeRule.INSTANCE, !!
				//ProjectJoinRemoveRule.INSTANCE,
				//JoinCommuteRule.INSTANCE,
			        
		        // 2. Run aggregate-join transpose (cost based)
				//AggregateJoinTransposeRule.INSTANCE,

		        // 3. Run rule to fix windowing issue when it is done over aggregation columns
				//ProjectToWindowRule.PROJECT,
				ProjectWindowTransposeRule.INSTANCE
				);		
	
	public static final ImmutableList<RelOptRule> CONVERT_TO_LOGICAL_RULES =
		ImmutableList.of(
				EnumerableJoinToLogicalJoinRule.INSTANCE,
				EnumerableProjectToLogicalProjectRule.INSTANCE,
				EnumerableFilterToLogicalFilterRule.INSTANCE,
				EnumerableAggregateToLogicalAggregateRule.INSTANCE,
				EnumerableTableScanToLogicalTableScanRule.INSTANCE,
				EnumerableWindowToLogicalWindowRule.INSTANCE
				);
	
	public static final ImmutableList<RelOptRule> CONVERT_TO_LOGICAL_RULES_2 =
		ImmutableList.of(
				SaberJoinRelToLogicalJoinRule.INSTANCE,
				SaberProjectRelToLogicalProjectRule.INSTANCE,
				SaberFilterRelToLogicalFilterRule.INSTANCE,
				SaberAggregateRelToLogicalAggregateRule.INSTANCE,
				SaberTableScanRelToLogicalTableScanRule.INSTANCE,
				SaberWindowRelToLogicalWindowRule.INSTANCE
				);
	
	public static final ImmutableList<RelOptRule> WINDOW_REWRITE_RULES =
		ImmutableList.of(
				ProjectToWindowRule.PROJECT,
				ReduceExpressionsRule.PROJECT_INSTANCE
				);
	
	public static final ImmutableList<RelOptRule> PRE_VOLCANO_STATIC_RULES =
		ImmutableList.of(
		        //1. Distinct aggregate rewrite
			    // Run this optimization early, since it is expanding the operator pipeline.
				AggregateExpandDistinctAggregatesRule.INSTANCE,
				
				// 2. Run exhaustive PPD, add not null filters, transitive inference,
		        // constant propagation, constant folding
				FilterMergeRule.INSTANCE,
				//FilterPushThroughFilter.INSTANCE,				
				FilterJoinRule.FILTER_ON_JOIN, // add support for WHERE style Joins with the following three rules
				FilterJoinRule.JOIN,
				// FilterRemoveIsNotDistinctFromRule.INSTANCE,
				JoinPushExpressionsRule.INSTANCE,				
				ReduceExpressionsRule.FILTER_INSTANCE,
				ReduceExpressionsRule.PROJECT_INSTANCE,
				ReduceExpressionsRule.JOIN_INSTANCE,    	    
				//ReduceDecimalsRule.INSTANCE,
				//maybe implement JoinAddNotNullRule.INSTANCE
				JoinPushTransitivePredicatesRule.INSTANCE, //Planner rule that infers predicates from on a Join and creates Filter if those predicates can be pushed to its inputs.
				AggregateProjectPullUpConstantsRule.INSTANCE,
				AggregateReduceFunctionsRule.INSTANCE, 
				AggregateRemoveRule.INSTANCE,				
				
		        // 3. Merge, remove and reduce Project if possible
				ProjectRemoveRule.INSTANCE,
				ProjectWindowTransposeRule.INSTANCE, 
				ProjectMergeRule.INSTANCE,
				//maybe implement ProjectFilterPullUpConstantsRule.INSTANCE
		        ProjectTableScanRule.INSTANCE,
				
			   	// 4. Prune empty result rules				
				PruneEmptyRules.FILTER_INSTANCE,
				PruneEmptyRules.PROJECT_INSTANCE,
				PruneEmptyRules.AGGREGATE_INSTANCE,
				PruneEmptyRules.JOIN_LEFT_INSTANCE,    
				PruneEmptyRules.JOIN_RIGHT_INSTANCE	
															
				);
	
	/**
	 * Converter rule set that converts from Calcite logical convention to Saber physical convention.
	 */
	private static final ImmutableSet<RelOptRule> calciteToSaberConversionRules =
	   ImmutableSet.<RelOptRule>builder().add(

	    ).build();

	public static RuleSet[] getRuleSets() {
	  /*
	   * Calcite planner takes an array of RuleSet and we can refer to them by index to activate
	   * each rule set for transforming the query plan based on different criteria.
	   */
	  Programs.ofRules(AFTER_JOIN_RULES);
	  return new RuleSet[]{new SaberRuleSet(StreamRules.RULES), new SaberRuleSet(ImmutableSet.<RelOptRule>builder().addAll(StreamRules.RULES).addAll(calciteToSaberConversionRules).build())};
	}

	private static class SaberRuleSet implements RuleSet {
	  final ImmutableSet<RelOptRule> rules;

	  public SaberRuleSet(ImmutableSet<RelOptRule> rules) {
	    this.rules = rules;
	  }

	  public SaberRuleSet(ImmutableList<RelOptRule> rules) {
	    this.rules = ImmutableSet.<RelOptRule>builder()
	        .addAll(rules)
	        .build();
	  }

	  public Iterator<RelOptRule> iterator() {
	    return rules.iterator();
	  }
	}	
	
}
