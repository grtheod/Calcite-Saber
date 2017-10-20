package calcite.cost;

import org.apache.calcite.schema.Statistic;

public interface SaberStatistic extends Statistic {

	/** Returns the approximate rate of tuples/sec. */
	Double getRate();

}
