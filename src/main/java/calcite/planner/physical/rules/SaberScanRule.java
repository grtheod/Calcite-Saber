package calcite.planner.physical.rules;

import java.util.HashSet;
import java.util.Set;

import org.apache.calcite.util.Pair;

import calcite.planner.physical.SaberRule;
import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.Query;
import uk.ac.imperial.lsds.saber.QueryConf;
import uk.ac.imperial.lsds.saber.QueryOperator;
import uk.ac.imperial.lsds.saber.WindowDefinition;
import uk.ac.imperial.lsds.saber.TupleSchema.PrimitiveType;
import uk.ac.imperial.lsds.saber.WindowDefinition.WindowType;
import uk.ac.imperial.lsds.saber.cql.expressions.Expression;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.longs.LongColumnReference;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.Projection;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.ProjectionKernel;

public class SaberScanRule implements SaberRule {
	public static final String usage = "usage: Dummy Scan";
	int [] offsets;
	ITupleSchema schema;
	ITupleSchema outputSchema;
	IOperatorCode cpuCode;
	IOperatorCode gpuCode;
	WindowDefinition window;
	long timestampReference;
	int queryId;
	Query query;
	boolean flag;
	
	public SaberScanRule(ITupleSchema schema, int queryId, long timestampReference, boolean flag) {
		this.schema = schema;
		this.queryId = queryId;
		this.timestampReference = timestampReference;
		this.flag = flag;
	}

	public void prepareRule() {
		//degenerate case : simple TableScan RelNode (select * from table).
		if (flag) {
			System.out.println("Degenerate case.");
			int batchSize = 1048576;
			WindowType windowType = WindowType.ROW_BASED;
			int windowRange = 1;
			int windowSlide = 1;
			int projectedAttributes = 0;
			int expressionDepth = 1;
					
			QueryConf queryConf = new QueryConf (batchSize);
			
			window = new WindowDefinition (windowType, windowRange, windowSlide);
			projectedAttributes = schema.numberOfAttributes();
			
			Expression [] expressions = new Expression [projectedAttributes];
				
			int i;
		
			for (i=0; i < projectedAttributes ; i++) {				
				if (schema.getAttributeType(i).equals(PrimitiveType.INT)) {
					expressions[i] = new IntColumnReference (i);
				}
				else if (schema.getAttributeType(i).equals(PrimitiveType.FLOAT)) 
					expressions[i] = new FloatColumnReference (i);
				else
					expressions[i] = new LongColumnReference (i);
			}
					
			cpuCode = new Projection (expressions);
			gpuCode = new ProjectionKernel (schema, expressions, batchSize, expressionDepth);
			
			QueryOperator operator;
			operator = new QueryOperator (cpuCode, gpuCode);			
			Set<QueryOperator> operators = new HashSet<QueryOperator>();
			operators.add(operator);				
			query = new Query (queryId, operators, schema, window, null, null, queryConf, timestampReference);
		} else{
			System.out.println("General case.");
		}
		
		this.outputSchema = this.schema;
	}

	public ITupleSchema getOutputSchema() {
		return this.outputSchema;
	}

	public Query getQuery() {
		return this.query;
	}

	public IOperatorCode getCpuCode() {
		return this.cpuCode;
	}

	public IOperatorCode getGpuCode() {
		return this.gpuCode;
	}

	public WindowDefinition getWindow() {
		return this.window;
	}

	public WindowDefinition getWindow2() {
		return null;
	}

	public Pair<Integer, Integer> getWindowOffset() {
		return new Pair<Integer, Integer>(0,0);
	}
	
}
