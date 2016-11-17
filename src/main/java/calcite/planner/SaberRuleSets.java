package calcite.planner;

import java.util.Iterator;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.stream.StreamRules;
import org.apache.calcite.tools.RuleSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class SaberRuleSets {

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
