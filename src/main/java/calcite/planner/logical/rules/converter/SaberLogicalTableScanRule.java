package calcite.planner.logical.rules.converter;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;

import com.google.common.collect.Table;

import calcite.planner.logical.SaberRel;
import calcite.planner.logical.SaberTableScanRel;

public class SaberLogicalTableScanRule extends ConverterRule {
	  public static final SaberLogicalTableScanRule INSTANCE = new SaberLogicalTableScanRule();

	  private SaberLogicalTableScanRule() {
	    super(LogicalTableScan.class, Convention.NONE, SaberRel.SABER_LOGICAL, "SaberScanRule");
	  }

	  @Override
	  public RelNode convert(RelNode rel) {
	    final TableScan scan = (TableScan) rel;
	    final Table table = scan.getTable().unwrap(Table.class);

	    return new SaberTableScanRel(scan.getCluster(),
	    		scan.getTraitSet().replace(SaberRel.SABER_LOGICAL),
	            scan.getTable());	    
	  }
}
