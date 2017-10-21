package calcite.planner.physical.rules;

import java.util.HashSet;
import java.util.Set;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.util.Pair;

import calcite.planner.physical.SaberRule;
import calcite.planner.physical.rules.util.SaberProjectUtil;
import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.Query;
import uk.ac.imperial.lsds.saber.QueryConf;
import uk.ac.imperial.lsds.saber.QueryOperator;
import uk.ac.imperial.lsds.saber.WindowDefinition;
import uk.ac.imperial.lsds.saber.WindowDefinition.WindowType;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;


/*Wrong result after join*/
public class SaberProjectRule implements SaberRule {
	public static final String usage = "usage: Projection";
	
	RelNode rel;
	WindowDefinition window;
	int [] offsets;
	ITupleSchema schema;
	ITupleSchema outputSchema;
	IOperatorCode cpuCode;
	IOperatorCode gpuCode;
	Query query;
	int queryId = 0;
	long timestampReference = 0;
	int windowOffset;
	int windowBarrier;
	int batchSize;
	boolean validProject;
	
	public SaberProjectRule(ITupleSchema schema, RelNode rel, int queryId , long timestampReference, WindowDefinition window, int windowOffset, int windowBarrier, int batchSize) {
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
		
		LogicalProject logProj = (LogicalProject) rel;
		
		SaberProjectUtil proj = new SaberProjectUtil(logProj.getChildExps(), batchSize, schema, window, windowOffset, windowBarrier);
		proj.build();
		
		outputSchema = proj.getOutputSchema();
		validProject = proj.isValid();
		if (validProject) {
			operators.add(proj.getOperator());
			
			windowType = proj.getWindow().getWindowType();
			windowRange = proj.getWindow().getSize();
			windowSlide = proj.getWindow().getSlide();
		    
			WindowDefinition executionWindow = new WindowDefinition (WindowType.ROW_BASED, 1, 1);
			System.out.println("Window is : " + executionWindow.getWindowType().toString() + " with " + executionWindow.toString());
			
			query = new Query (queryId, operators, outputSchema, executionWindow, null, null, queryConf, timestampReference);
		}
		else
			System.out.println("The projection is skipped");
		
		//resize the window according to possible changes from input
		window = new WindowDefinition (windowType, windowRange, windowSlide);
		System.out.println("The window passed in the next operator is:" + window.toString());
	}
		
	public ITupleSchema getOutputSchema(){
		return this.outputSchema;
	}
	
	public Query getQuery(){
		return this.query;
	}
	
	public IOperatorCode getCpuCode(){
		return this.cpuCode;
	}
	
	public IOperatorCode getGpuCode(){
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
	
	public boolean isValid() {
		return validProject;
	}
	
}
