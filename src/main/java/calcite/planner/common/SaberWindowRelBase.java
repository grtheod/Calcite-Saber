package calcite.planner.common;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;

import calcite.cost.SaberCostBase;
import calcite.cost.SaberCostBase.SaberCostFactory;

public class SaberWindowRelBase extends Window implements SaberRelNode {

	public SaberWindowRelBase(RelOptCluster cluster, RelTraitSet traits,
	                          RelNode child, List<RexLiteral> constants, RelDataType rowType,
	                          List<Group> groups) {
		  super(cluster, traits, child, constants, rowType, groups);
	}
	  
	@Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
	    
	    // To be fixed.
	    // RelOptCost previousCost = planner.getCost(this.input, mq);
	    double rowCount = mq.getRowCount(this); 
	    List<AggregateCall> aggCalls = this.groups.get(0).getAggregateCalls(this);
	    float multiplier = 1f + (float) aggCalls.size() * 0.125f;
	    for (AggregateCall aggCall : aggCalls) {
	      if (aggCall.getAggregation().getName().equals("SUM")) {
	        // Pretend that SUM costs a little bit more than $SUM0,
	        // to make things deterministic.
	        multiplier += 0.0125f;
	      }
	    }	  
	    //System.out.println("multiplier:" + multiplier );
	    double inputRate = ((SaberCostBase) mq.getCumulativeCost(this.getInput())).getRate(); // ((SaberCostBase) mq.getCumulativeCost(this.getInput())).getRate();
	    double outputRate = multiplier * inputRate ;
	    double cpuCost = multiplier * SaberCostBase.Cs * inputRate;	    
	    double window = createWindowFrame(this.getConstants());
	    window = (this.groups.get(0).isRows) ? window :  window * inputRate; // if the window is time-based W=T*λi
	    double memory = window;
	    double R = (((SaberCostBase) mq.getCumulativeCost(this.getInput())).getCpu() + cpuCost) / outputRate;
		
	    SaberCostFactory costFactory = (SaberCostFactory)planner.getCostFactory();
	    return costFactory.makeCost(multiplier * rowCount, cpuCost, 0, outputRate, memory, window, R);
	}

	private int createWindowFrame(List<RexLiteral> constants) {
		int windowFrame = 0;
		for ( RexLiteral con : constants) 
			windowFrame += Integer.parseInt(con.toString());
		return windowFrame;
	}
}
