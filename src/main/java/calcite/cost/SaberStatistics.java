package calcite.cost;

import java.util.List;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;

public class SaberStatistics {
	private SaberStatistics() {
	}

	/** Returns a {@link Statistic} that knows nothing about a table. */
	public static final Statistic UNKNOWN =
	  new Statistic() {
	    public Double getRowCount() {
	      return null;
	    }

	    public boolean isKey(ImmutableBitSet columns) {
	      return false;
	    }

	    public List<RelCollation> getCollations() {
	      return ImmutableList.of();
	    }

	    public RelDistribution getDistribution() {
	      return RelDistributionTraitDef.INSTANCE.getDefault();
	    }
  	};

	/** Returns a statistic with a given row count and set of unique keys. */
	public static Statistic of(final double rowCount,
	  final List<ImmutableBitSet> keys) {
		return of(rowCount, keys, ImmutableList.<RelCollation>of());
	}

	/** Returns a statistic with a given row count and set of unique keys. */
	public static Statistic of(final double rowCount,
	  final List<ImmutableBitSet> keys, final List<RelCollation> collations) {
			return new Statistic() {
		  		public Double getRowCount() {
		    		return rowCount;
		  		}

		  public boolean isKey(ImmutableBitSet columns) {
		    for (ImmutableBitSet key : keys) {
		      if (columns.contains(key)) {
		        return true;
		      }
		    }
		    return false;
		  }

		  public List<RelCollation> getCollations() {
		    return collations;
		  }

		  public RelDistribution getDistribution() {
		    return RelDistributionTraitDef.INSTANCE.getDefault();
		  }
		};
	}
}
