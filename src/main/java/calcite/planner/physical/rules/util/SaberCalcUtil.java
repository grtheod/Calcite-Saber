package calcite.planner.physical.rules.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.QueryOperator;
import uk.ac.imperial.lsds.saber.WindowDefinition;
import uk.ac.imperial.lsds.saber.WindowDefinition.WindowType;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;

public class SaberCalcUtil implements SaberRuleUtil{
	
	RexProgram program;
	WindowDefinition window;
	int [] offsets;
	ITupleSchema schema;
	ITupleSchema outputSchema;
	IOperatorCode cpuCode, filterCpuCode, projectCpuCode;
	IOperatorCode gpuCode, filterGpuCode, projectGpuCode;
	int windowOffset;
	int windowBarrier;
	int batchSize;
	QueryOperator calcOperator;
	Set<QueryOperator> calcOperators;
	
	public SaberCalcUtil(RexProgram program, int batchSize, ITupleSchema schema, WindowDefinition window, int windowOffset, int windowBarrier) {
		this.program = program;
		this.batchSize = batchSize;
		this.schema = schema;
		this.batchSize = batchSize;
		this.windowOffset = windowOffset;
		this.windowBarrier = windowBarrier;
		this.window = window;		
	}
	
	@Override
	public void build() {
		WindowType windowType = (window!=null) ? window.getWindowType() : WindowType.ROW_BASED;
		long windowRange = (window!=null) ? window.getSize() : 1;
		long windowSlide = (window!=null) ? window.getSlide() : 1;
				
		window = new WindowDefinition (windowType, windowRange, windowSlide);	
		calcOperators = new HashSet<QueryOperator>();
		
		// Filter Operator first. Check if we have a condition. If not, there is no filter to create.
		RexLocalRef programCondition = program.getCondition();
		RexNode condition;
		if (programCondition == null) {
			condition = null;
		} else {
		    condition = program.expandLocalRef(programCondition);
		    SaberFilterUtil filter = new SaberFilterUtil(condition, batchSize, schema);
		    filter.build();
			cpuCode = filter.getCpuCode();
			gpuCode = filter.getGpuCode();
			calcOperators.add(filter.getOperator());
			outputSchema = filter.getOutputSchema();
		}
		
		
		// Project Operator second. Check if we have certain attributes to project. If not, there is no project to create.
		List<RexLocalRef> projectList = program.getProjectList();
	    if (projectList != null && !projectList.isEmpty()) {
	        List<RexNode> expandedNodes = new ArrayList<>();
	        for (RexLocalRef project : projectList) {
	            expandedNodes.add(program.expandLocalRef(project));
	        }
	        
			List<RexNode> projectedAttrs = expandedNodes; 
			SaberProjectUtil project = new SaberProjectUtil(projectedAttrs, batchSize, schema, window, windowOffset, windowBarrier);
			project.build();
			
			if (project.isValid()) {
				cpuCode = project.getCpuCode();
				gpuCode = project.getGpuCode();
				
				calcOperators.add(project.getOperator());
				outputSchema = project.getOutputSchema();
				
				// make possible changes in window definition
				windowType = project.getWindow().getWindowType();
				windowRange = project.getWindow().getSize();
				windowSlide = project.getWindow().getSlide();
				window = new WindowDefinition (windowType, windowRange, windowSlide);
			}
	    }
			
		if (programCondition == null && (projectList == null || projectList.isEmpty())) {
			// it shouldn't be happen
	        throw new IllegalStateException("Either projection or condition, or both should be provided.");
		}
		
	}

	@Override
	public ITupleSchema getOutputSchema() {
		return this.outputSchema;
	}

	@Override
	public QueryOperator getOperator() {
		return this.calcOperator;
	}

	@Override
	public IOperatorCode getCpuCode() {
		return this.cpuCode;
	}

	@Override
	public IOperatorCode getGpuCode() {
		return this.gpuCode;
	}

	@Override
	public WindowDefinition getWindow() {
		return this.window;
	}

	@Override
	public WindowDefinition getWindow2() {
		return null;
	}
	
	public Set<QueryOperator> getCalcOperators() {
		return this.calcOperators;
	}

}
