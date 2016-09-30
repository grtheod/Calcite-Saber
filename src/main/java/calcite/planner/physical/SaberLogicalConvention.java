package calcite.planner.physical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;

public enum  SaberLogicalConvention implements Convention {
	  INSTANCE;

	  public Class getInterface() {
	    return SaberRel.class;
	  }

	  public String getName() {
	    return "SABER_LOGICAL";
	  }

	  public RelTraitDef getTraitDef() {
	    return ConventionTraitDef.INSTANCE;
	  }


	  public boolean satisfies(RelTrait trait) {
	    return this == trait;
	  }

	  public void register(RelOptPlanner planner) {}


	  public String toString() {
	    return getName();
	  }

	  public boolean canConvertConvention(Convention arg0) {
		  // TODO Auto-generated method stub
		  return false;
	  }

	  public boolean useAbstractConvertersForConversion(RelTraitSet arg0, RelTraitSet arg1) {
		  // TODO Auto-generated method stub
		  return false;
	  }
}