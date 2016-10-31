package calcite.cost;

import org.apache.calcite.plan.RelOptCost;

public interface SaberRelOptCost extends RelOptCost {
	
	double getRate();
}
