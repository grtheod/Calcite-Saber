package calcite.planner.logical;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;

import calcite.planner.common.SaberTableScanRelBase;

public class SaberTableScanRel extends SaberTableScanRelBase implements SaberRel {

	  public SaberTableScanRel(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
	    super(cluster, traitSet, table);
	  }

	  @Override
	  public <T> T accept(SaberRelVisitor<T> visitor) {
	    return null;
	  }
}
