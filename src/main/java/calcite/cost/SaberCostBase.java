package calcite.cost;

import java.util.Objects;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.RelOptUtil;

public class SaberCostBase implements SaberRelOptCost {

    public static final int BASE_CPU_COST = 1;                        // base cpu cost per 'operation'
    public static final int BYTE_DISK_READ_COST = 32 * BASE_CPU_COST;    // disk read cost per byte
    public static final int BYTE_NETWORK_COST = 16 * BYTE_DISK_READ_COST; // network transfer cost per byte


    public static final int SVR_CPU_COST = 8 * BASE_CPU_COST;          // cpu cost for SV remover
    public static final int FUNC_CPU_COST = 12 * BASE_CPU_COST;         // cpu cost for a function evaluation

    // cpu cost for projecting an expression; note that projecting an expression
    // that is not a simple column or constant may include evaluation, but we
    // currently don't model it at that level of detail.
    public static final int PROJECT_CPU_COST = 4 * BASE_CPU_COST;

    // hash cpu cost per field (for now we don't distinguish between fields of different types) involves
    // the cost of the following operations:
    // compute hash value, probe hash table, walk hash chain and compare with each element,
    // add to the end of hash chain if no match found
    public static final int HASH_CPU_COST = 8 * BASE_CPU_COST;

    // The ratio to convert memory cost into CPU cost.
    public static final double MEMORY_TO_CPU_RATIO = 1.0;

    public static final int RANGE_PARTITION_CPU_COST = 12 * BASE_CPU_COST;

    // cost of comparing one field with another (ignoring data types for now)
    public static final int COMPARE_CPU_COST = 4 * BASE_CPU_COST;
    public static final int AVG_FIELD_WIDTH = 8;
  
    // main cost factor of our model
    public static final int Cs = PROJECT_CPU_COST; // Cost for performing selection
    public static final int Cj = HASH_CPU_COST; // Cost for performing a join,
    // including the cost of probing , insertion and deletion needed : Cj=(Cp+Ci+Cv)
    public static final int Ca = PROJECT_CPU_COST; // Cost of applying an aggregate function
	  		
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
    final double rate;
    final double memory;
    final double window;
    final double R;

    public SaberCostBase(double rowCount, double cpu, double io, double rate) {
	   this(rowCount, cpu, io, rate, 0, 0, 0);
    }

    public SaberCostBase(double rowCount, double cpu, double io, double rate, double memory, double window, double R) {
	   this.rowCount = rowCount;
	   this.cpu = cpu;
	   this.io = io;
	   this.rate = rate;
	   this.memory = memory;
	   this.window = window;
	   this.R = R; // optimization goal min(R == (sum(cpu)/final_rate)) 
	}
	
    @Override public int hashCode() {
        return Objects.hash(rowCount, cpu, io, rate, memory, window, R);
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
	      && (this.R == ((SaberCostBase) other).R)
	      && (this.rowCount == ((SaberCostBase) other).rowCount)
	      && (this.memory == ((SaberCostBase) other).memory)
	      && (this.window == ((SaberCostBase) other).window)
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

    public double getMemory() {
	    return memory;
	}
	  
    public double getRate() {
        return rate;
    }

    public double getR() {
        return R;
    }

    public double getWindow() {
        return window;
    }
	
	@Override
	public boolean isInfinite() {
	    return (this == INFINITY)
	    		|| (this.cpu == Double.POSITIVE_INFINITY)
	    	    || (this.io == Double.POSITIVE_INFINITY)
	    	    || (this.R == Double.POSITIVE_INFINITY)
	    	    || (this.rowCount == Double.POSITIVE_INFINITY)
	    	    || (this.window == Double.POSITIVE_INFINITY)
	    	    || (this.memory == Double.POSITIVE_INFINITY)
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
	            && (Math.abs(this.R - that.R) < RelOptUtil.EPSILON)
	            && (Math.abs(this.rowCount - that.rowCount) < RelOptUtil.EPSILON)
	            && (Math.abs(this.memory - that.memory) < RelOptUtil.EPSILON)
	            && (Math.abs(this.window - that.window) < RelOptUtil.EPSILON)
	            && (Math.abs(this.rate - that.rate) < RelOptUtil.EPSILON));
	}

	@Override
	public boolean isLe(RelOptCost other) {
	    return isLt(other) || equals(other);
	}

	@Override
	public boolean isLt(RelOptCost other) {
	    SaberCostBase that = (SaberCostBase) other;
	    if (true)
	    	//return this.rowCount < that.rowCount;
	        return ( (this.R <= that.R) // R is the main optimization goal
                && (this.memory <= that.memory)
                );
            
            return ((this.cpu + this.memory + this.R)
              < (that.cpu + that.memory + that.R)); 
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
	        this.memory + that.memory,
	        this.window,
	        this.R); 
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
	        this.memory - that.memory,
	        this.window - that.window,
	        this.R - that.R);
	}

	@Override
	public RelOptCost multiplyBy(double factor) {
	    if (this == INFINITY) {
	      return this;
	    }
	    return new SaberCostBase(rowCount * factor, cpu * factor, io * factor, rate * factor, memory * factor, window * factor, R * factor);
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
	    if ((this.R != 0)
	        && !Double.isInfinite(this.R)
	        && (that.R != 0)
	        && !Double.isInfinite(that.R)) {
	      d *= this.R / that.R;
	      ++n;
	    }
	    if ((this.rate != 0)
		        && !Double.isInfinite(this.rate)
		        && (that.rate != 0)
		        && !Double.isInfinite(that.rate)) {
		      d *= this.rate / that.rate;
		      ++n;
		}
        if ((this.memory != 0)
                && !Double.isInfinite(this.memory)
                && (that.memory != 0)
                && !Double.isInfinite(that.memory)) {
              d *= this.memory / that.memory;
              ++n;
        }
        if ((this.window != 0)
                && !Double.isInfinite(this.window )
                && (that.window  != 0)
                && !Double.isInfinite(that.window )) {
              d *= this.window  / that.window ;
              ++n;
        }        
	    if (n == 0) {
	      return 1.0;
	    }
	    return Math.pow(d, 1 / n);
	}

	@Override
	public String toString() {
	    return "{" + rowCount + " rows, " + cpu + " cpu, " + io + " io, " + rate + " rate, " + memory + " memory, " + window + " window, " + R + " R}";
	}	

	/** Implementation of {@link RelOptCostFactory} that creates
	 * {@link RelOptCostImpl}s. */
	public static class SaberCostFactory implements SaberRelOptCostFactory {

	    public RelOptCost makeCost(double dRows, double dCpu, double dIo, double dRate, double dMemory, double dWindow, double dR) {
		      return new SaberCostBase(dRows, dCpu, dIo, dRate, dMemory, dWindow, dR);
		}
		
	    public RelOptCost makeCost(double dRows, double dCpu, double dIo, double dRate, double dR) {
	      return new SaberCostBase(dRows, dCpu, dIo, dRate, 0, 0, dR);
	    }

	    public RelOptCost makeCost(double dRows, double dCpu, double dIo, double dRate) {
	      return new SaberCostBase(dRows, dCpu, dIo, dRate, 0, 0, 0);
	    }

	    public RelOptCost makeCost(double dRows, double dCpu, double dIo) {
	      return new SaberCostBase(dRows, dCpu, dIo, dRows, 0, 0, 0);
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
