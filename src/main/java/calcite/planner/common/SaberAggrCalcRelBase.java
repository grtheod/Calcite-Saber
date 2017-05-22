package calcite.planner.common;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.util.ImmutableBitSet;

import calcite.cost.SaberCostBase;
import calcite.cost.SaberCostBase.SaberCostFactory;
import calcite.planner.core.AggrCalc;

public abstract class SaberAggrCalcRelBase extends AggrCalc implements SaberRelNode {

	protected SaberAggrCalcRelBase(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexProgram previousProgram,
			RexProgram nextProgram, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets,
			List<AggregateCall> aggCalls) {
		super(cluster, traits, child, previousProgram, nextProgram, indicator, groupSet, groupSets, aggCalls);
	}
	
	// fix it
	@Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
		
	    RelOptCost previousCost = planner.getCost(this.input, mq);
	    double rowCount = mq.getRowCount(this);
		
		RexProgram program = (nextProgram != null) ? nextProgram : previousProgram;
	    double selectivity = mq.getSelectivity(this.getInput(), program.getCondition()); // program.getExprCount()
	    double rate = selectivity * ((SaberCostBase) previousCost).getRate(); // ((SaberCostBase) mq.getCumulativeCost(this.getInput())).getRate();

	    double cpuCost = SaberCostBase.Cs * rate;
	    //System.out.println("selectivity:" + selectivity );	    
	    double window =  selectivity * ((SaberCostBase) previousCost).getWindow(); // ((SaberCostBase) mq.getCumulativeCost(this.getInput())).getWindow();
	    window = (window < 1) ? 1 : window; // fix window size in order to be >= 1

	    double R = (((SaberCostBase) previousCost).getCpu() + cpuCost)/rate; // (((SaberCostBase) mq.getCumulativeCost(this.getInput())).getCpu() + cpuCost) / rate;
		
	    if (Double.isNaN(R))
	      	R = Double.MAX_VALUE;
	    if (Double.isInfinite(rate))
		rate = Double.MAX_VALUE;
	    if (Double.isInfinite(cpuCost))
		cpuCost = Double.MAX_VALUE;
		
	    SaberCostFactory costFactory = (SaberCostFactory)planner.getCostFactory();
	    return costFactory.makeCost(rowCount, cpuCost, 0, rate, 0, window, R);
	}

}
