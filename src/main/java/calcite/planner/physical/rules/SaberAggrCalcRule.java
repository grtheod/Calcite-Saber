package calcite.planner.physical.rules;

import java.util.HashSet;
import java.util.Set;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Pair;

import calcite.planner.logical.SaberAggrCalcRel;
import calcite.planner.physical.SaberRule;
import calcite.planner.physical.rules.util.SaberAggregateUtil;
import calcite.planner.physical.rules.util.SaberCalcUtil;
import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.Query;
import uk.ac.imperial.lsds.saber.QueryConf;
import uk.ac.imperial.lsds.saber.QueryOperator;
import uk.ac.imperial.lsds.saber.WindowDefinition;
import uk.ac.imperial.lsds.saber.WindowDefinition.WindowType;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;

public class SaberAggrCalcRule implements SaberRule {

	public static final String usage = "usage: AggrCalc";
	
	RelNode rel;
	WindowDefinition window;
	int [] offsets;
	ITupleSchema schema;
	ITupleSchema outputSchema;
	IOperatorCode aggrCpuCode, calc1CpuCode, calc2CpuCode;
	IOperatorCode aggrGpuCode, calc1GpuCode, calc2GpuCode;
	Query query;
	int queryId = 0;
	long timestampReference = 0;
	int windowOffset;
	int windowBarrier;
	int batchSize;
	boolean validCalc;
	
	public SaberAggrCalcRule(ITupleSchema schema, RelNode rel, int queryId , long timestampReference, WindowDefinition window, int windowOffset, int windowBarrier, int batchSize) {
		this.schema = schema;
		this.rel = rel;
		this.queryId = queryId;
		this.timestampReference = timestampReference;
		this.windowOffset = windowOffset;
		this.windowBarrier = windowBarrier;
		this.window = window;
		this.batchSize = batchSize;
	}	
	
	public void prepareRule() {
		
		WindowType windowType = (window!=null) ? window.getWindowType() : WindowType.ROW_BASED;
		long windowRange = (window!=null) ? window.getSize() : 1;
		long windowSlide = (window!=null) ? window.getSlide() : 1;
		
		QueryConf queryConf = new QueryConf (batchSize);		
		window = new WindowDefinition (windowType, windowRange, windowSlide);	
		Set<QueryOperator> operators = new HashSet<QueryOperator>();
		
		SaberAggrCalcRel aggrCalc = (SaberAggrCalcRel) rel;
		
		// 1st CALC Operator
		SaberCalcUtil calc1 = new SaberCalcUtil(aggrCalc.getPreviousProgram(), batchSize, schema, window, windowOffset, windowBarrier);
		calc1.build();
		
		for ( QueryOperator op : calc1.getCalcOperators()) {
			operators.add(op);
		}
		
		window = calc1.getWindow();
		calc1CpuCode = calc1.getCpuCode();
		calc1GpuCode = calc1.getGpuCode();
		outputSchema = calc1.getOutputSchema();  
		
		
		// Aggregate Operator
		// TODO: Handle the case when no window definition is given from SQL!!
		SaberAggregateUtil aggr = new SaberAggregateUtil(aggrCalc.getAggCallList(), aggrCalc.getGroupSet(), batchSize, outputSchema, window);
		aggr.build();
		
		operators.add(aggr.getOperator());
		aggrCpuCode = aggr.getCpuCode();
		aggrGpuCode = aggr.getGpuCode();
		windowOffset = aggr.getWindowOffset();
		windowBarrier = -1;
		outputSchema = aggr.getOutputSchema();
		
		
		// 2nd CALC Operator - it may be skipped if it is redundant
		SaberCalcUtil calc2 = new SaberCalcUtil(aggrCalc.getNextProgram(), batchSize, outputSchema, window, windowOffset, windowBarrier);
		calc2.build();
				
		outputSchema = calc2.getOutputSchema();
		validCalc = calc2.isValid();
		if (validCalc) {			
			for ( QueryOperator op : calc2.getCalcOperators()) {
				operators.add(op);
			}
			window = calc2.getWindow();
			calc2CpuCode = calc2.getCpuCode();
			calc2GpuCode = calc2.getGpuCode();
			//windowType = calc2.getWindow().getWindowType();
			//windowRange = calc2.getWindow().getSize();
			//windowSlide = calc2.getWindow().getSlide();
		}
		else
			System.out.println("The CALC is skipped");
		
		System.out.println("The window is: " + window.toString());
		
		// build the final query
		query = new Query (queryId, operators, schema, window, null, null, queryConf, timestampReference);
				
	}
	
	@Override
	public ITupleSchema getOutputSchema() {
		return this.outputSchema;
	}

	@Override
	public Query getQuery() {
		return this.query;
	}

	@Override
	public IOperatorCode getCpuCode() {
		return this.aggrCpuCode;
	}

	@Override
	public IOperatorCode getGpuCode() {
		return this.aggrGpuCode;
	}

	@Override
	public WindowDefinition getWindow() {
		return this.window;
	}

	@Override
	public WindowDefinition getWindow2() {
		return null;
	}

	@Override
	public Pair<Integer, Integer> getWindowOffset() {
		return new Pair<Integer, Integer>(0,0);
	}

	public boolean isValid() {
		return validCalc;
	}
}
