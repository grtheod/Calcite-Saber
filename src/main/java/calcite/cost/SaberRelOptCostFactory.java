package calcite.cost;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;

public interface SaberRelOptCostFactory extends RelOptCostFactory {

   /**
	* Creates a cost object.
	*/
	RelOptCost makeCost(double rate, double cpu, double memory, double window);
}
