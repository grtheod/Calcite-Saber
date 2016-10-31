package calcite.cost;

import java.util.Objects;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.RelOptUtil;

public class SaberCostBase implements SaberRelOptCost {

	static final SaberCostBase INFINITY =
		new SaberCostBase(
			Double.POSITIVE_INFINITY,
		    Double.POSITIVE_INFINITY,
		    Double.POSITIVE_INFINITY,
		    Double.POSITIVE_INFINITY) {
		@Override
		    public String toString() {
		    	return "{inf}";
		    }
		};

	static final SaberCostBase HUGE =
		new SaberCostBase(Double.MAX_VALUE,
			Double.MAX_VALUE,
		    Double.MAX_VALUE,
		    Double.MAX_VALUE) {
		@Override
		    public String toString() {
		    	return "{huge}";
		    }
		};

	static final SaberCostBase ZERO =
		new SaberCostBase(0.0, 0.0, 0.0, 0.0) {
			@Override
			public String toString() {
				return "{0}";
			}
		};
		      
	static final SaberCostBase TINY =
		new SaberCostBase(1.0, 1.0, 0.0, 0.0) {
			@Override
			public String toString() {
				return "{tiny}";
			}
		};

    final double rowCount;
    final double cpu;
    final double io;
    final double network;
    final double rate;

    public SaberCostBase(double rowCount, double cpu, double io, double rate) {
	   this(rowCount, cpu, io, rate, 0);
    }

    public SaberCostBase(double rowCount, double cpu, double io, double rate, double network) {
	   this.rowCount = rowCount;
	   this.cpu = cpu;
	   this.io = io;
	   this.rate = rate;
	   this.network = network;	   
	}
	
    @Override public int hashCode() {
        return Objects.hash(rowCount, cpu, io,rate,network);
    }    
    
	@Override
	public boolean equals(RelOptCost other) {
	    // here we compare the individual components similar to VolcanoCost, however
	    // an alternative would be to add up the components and compare the total.
	    // Note that VolcanoPlanner mainly uses isLe() and isLt() for cost comparisons,
	    // not equals().
	    return this == other
	      || (other instanceof SaberCostBase
	      && (this.cpu == ((SaberCostBase) other).cpu)
	      && (this.io == ((SaberCostBase) other).io)
	      && (this.network == ((SaberCostBase) other).network)
	      && (this.rowCount == ((SaberCostBase) other).rowCount)
	      && (this.rate == ((SaberCostBase) other).rate));
	}
	

	@Override
	public double getCpu() {		
		return cpu;
	}

	@Override
	public double getIo() {
		return io;
	}

	@Override
	public double getRows() {
		return rowCount;
	}

	public double getNetwork() {
	    return network;
	}

	public double getRate() {
	    return rate;
	}
	
	@Override
	public boolean isInfinite() {
	    return (this == INFINITY)
	    		|| (this.cpu == Double.POSITIVE_INFINITY)
	    	    || (this.io == Double.POSITIVE_INFINITY)
	    	    || (this.network == Double.POSITIVE_INFINITY)
	    	    || (this.rowCount == Double.POSITIVE_INFINITY)
	    	    || (this.rate == Double.POSITIVE_INFINITY) ;
	}

	@Override
	public boolean isEqWithEpsilon(RelOptCost other) {
	    if (!(other instanceof SaberCostBase)) {
	        return false;
	      }
	      SaberCostBase that = (SaberCostBase) other;
	      return (this == that)
	    		|| ((Math.abs(this.cpu - that.cpu) < RelOptUtil.EPSILON)
	            && (Math.abs(this.io - that.io) < RelOptUtil.EPSILON)
	            && (Math.abs(this.network - that.network) < RelOptUtil.EPSILON)
	            && (Math.abs(this.rowCount - that.rowCount) < RelOptUtil.EPSILON)
	            && (Math.abs(this.rate - that.rate) < RelOptUtil.EPSILON));
	}

	@Override
	public boolean isLe(RelOptCost other) {
	    SaberCostBase that = (SaberCostBase) other;
	    if (this.rate == that.rate)
	    	return this == that
	    	|| ( (this.cpu + this.io + this.network)
	    		<= (that.cpu + that.io + that.network));
	    else 
	    if (this.rate < that.rate)
	    	return true;
	    else
	    	return false;	    
	}

	@Override
	public boolean isLt(RelOptCost other) {
	    SaberCostBase that = (SaberCostBase) other;
	    if (this.rate == that.rate)
	    	return  ( (this.cpu + this.io + this.network)
	    		< (that.cpu + that.io + that.network) );
	    else 
	    if (this.rate < that.rate)
	    	return true;
	    else
	    	return false;
	}

	@Override
	public RelOptCost plus(RelOptCost other) {
		SaberCostBase that = (SaberCostBase) other;
	    if ((this == INFINITY) || (that == INFINITY)) {
	      return INFINITY;
	    }
	    return new SaberCostBase(
	        this.rowCount + that.rowCount,
	        this.cpu + that.cpu,
	        this.io + that.io,
	        this.rate,
	        this.network + that.network); //change it
	}

	@Override
	public RelOptCost minus(RelOptCost other) {
	    if (this == INFINITY) {
	      return this;
	    }
	    SaberCostBase that = (SaberCostBase) other;
	    return new SaberCostBase(
	        this.rowCount - that.rowCount,
	        this.cpu - that.cpu,
	        this.io - that.io,
	        this.rate - that.rate,
	        this.network - that.network); //change the rate computation
	}

	@Override
	public RelOptCost multiplyBy(double factor) {
	    if (this == INFINITY) {
	      return this;
	    }
	    return new SaberCostBase(rowCount * factor, cpu * factor, io * factor, rate * factor, network * factor);
	}

	@Override
	public double divideBy(RelOptCost cost) {
	    // Compute the geometric average of the ratios of all of the factors
	    // which are non-zero and finite.
	    SaberCostBase that = (SaberCostBase) cost;
	    double d = 1;
	    double n = 0;
	    if ((this.rowCount != 0)
	        && !Double.isInfinite(this.rowCount)
	        && (that.rowCount != 0)
	        && !Double.isInfinite(that.rowCount)) {
	      d *= this.rowCount / that.rowCount;
	      ++n;
	    }
	    if ((this.cpu != 0)
	        && !Double.isInfinite(this.cpu)
	        && (that.cpu != 0)
	        && !Double.isInfinite(that.cpu)) {
	      d *= this.cpu / that.cpu;
	      ++n;
	    }
	    if ((this.io != 0)
	        && !Double.isInfinite(this.io)
	        && (that.io != 0)
	        && !Double.isInfinite(that.io)) {
	      d *= this.io / that.io;
	      ++n;
	    }
	    if ((this.network != 0)
	        && !Double.isInfinite(this.network)
	        && (that.network != 0)
	        && !Double.isInfinite(that.network)) {
	      d *= this.network / that.network;
	      ++n;
	    }
	    if ((this.rate != 0)
		        && !Double.isInfinite(this.rate)
		        && (that.rate != 0)
		        && !Double.isInfinite(that.rate)) {
		      d *= this.rate / that.rate;
		      ++n;
		}
	    
	    if (n == 0) {
	      return 1.0;
	    }
	    return Math.pow(d, 1 / n);
	}

	@Override
	public String toString() {
	    return "{" + rowCount + " rows, " + cpu + " cpu, " + io + " io, " + rate + " rate, " + network + " network}";
	}	

	/** Implementation of {@link RelOptCostFactory} that creates
	 * {@link RelOptCostImpl}s. */
	public static class SaberCostFactory implements SaberRelOptCostFactory {

	    public RelOptCost makeCost(double dRows, double dCpu, double dIo, double dRate, double dNetwork) {
	      return new SaberCostBase(dRows, dCpu, dIo, dRate, dNetwork);
	    }

	    public RelOptCost makeCost(double dRows, double dCpu, double dIo, double dRate) {
	      return new SaberCostBase(dRows, dCpu, dIo, dRate, 0);
	    }

	    public RelOptCost makeCost(double dRows, double dCpu, double dIo) {
	      return new SaberCostBase(dRows, dCpu, dIo, dRows, 0);
	    }

	    public RelOptCost makeHugeCost() {
	      return SaberCostBase.HUGE;
	    }

	    public RelOptCost makeInfiniteCost() {
	      return SaberCostBase.INFINITY;
	    }

	    public RelOptCost makeTinyCost() {
	      return SaberCostBase.TINY;
	    }

	    public RelOptCost makeZeroCost() {
	      return SaberCostBase.ZERO;
	    }
	}	  	  
}
