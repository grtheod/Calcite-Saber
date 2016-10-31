package calcite.cost;

import java.util.List;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.util.ImmutableBitSet;

public interface SaberStatistic extends Statistic {

	/** Returns the approximate rate of tuples/sec. */
	Double getRate();

}
