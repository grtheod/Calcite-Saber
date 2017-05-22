package calcite.planner.logical.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import calcite.planner.logical.SaberAggrCalcRel;


public class SaberCalcAggregateCalcToSaberAggrCalcRule extends RelOptRule {

	public static final SaberCalcAggregateCalcToSaberAggrCalcRule INSTANCE =
		new SaberCalcAggregateCalcToSaberAggrCalcRule(Calc.class, Calc.class,Aggregate.class,
			RelFactories.LOGICAL_BUILDER);


	//~ Constructors -----------------------------------------------------------

	public SaberCalcAggregateCalcToSaberAggrCalcRule(
		Class<? extends Calc> nextCalcClass,
		Class<? extends Calc> previousCalcClass,
		Class<? extends Aggregate> aggregateClass,
		RelBuilderFactory relBuilderFactory) {
		super(
			operand(nextCalcClass,
		    operand(aggregateClass, 
		    operand(previousCalcClass, any()))),
		    relBuilderFactory, null);
	}

	@Deprecated // to be removed before 2.0
	public SaberCalcAggregateCalcToSaberAggrCalcRule(
		Class<? extends Calc> nextCalcClass,		
		Class<? extends Calc> previousCalcClass,
		Class<? extends Aggregate> aggregateClass,
		RelFactories.AggregateFactory aggregateFactory) {
		this(nextCalcClass, previousCalcClass, aggregateClass, RelBuilder.proto(aggregateFactory));
	}

	//~ Methods ----------------------------------------------------------------
	@Override
	public void onMatch(RelOptRuleCall call) {
	    final Calc nextCalcClass = call.rel(0);
	    final Aggregate aggr = call.rel(1);
	    final Calc previousCalcClass = call.rel(2);
	    
	    final SaberAggrCalcRel newAggrCalc = SaberAggrCalcRel.create(previousCalcClass.getInput(), previousCalcClass.getProgram(), nextCalcClass.getProgram(),
	    		aggr.indicator, aggr.getGroupSet(), aggr.groupSets, aggr.getAggCallList());

	    call.transformTo(newAggrCalc);
		
	}
	
}
