package calcite.planner.physical.rules;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.util.Pair;

import calcite.planner.physical.ExpressionBuilder;
import calcite.planner.physical.PredicateUtil;
import calcite.planner.physical.SaberRule;
import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.Query;
import uk.ac.imperial.lsds.saber.QueryConf;
import uk.ac.imperial.lsds.saber.QueryOperator;
import uk.ac.imperial.lsds.saber.WindowDefinition;
import uk.ac.imperial.lsds.saber.TupleSchema.PrimitiveType;
import uk.ac.imperial.lsds.saber.WindowDefinition.WindowType;
import uk.ac.imperial.lsds.saber.cql.expressions.Expression;
import uk.ac.imperial.lsds.saber.cql.expressions.ExpressionsUtil;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.longs.LongColumnReference;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.Projection;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.Selection;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.ProjectionKernel;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.SelectionKernel;
import uk.ac.imperial.lsds.saber.cql.predicates.IPredicate;

public class SaberCalcRule implements SaberRule {
	
	public static final String usage = "usage: Calc";
	
	RelNode rel;
	WindowDefinition window;
	int [] offsets;
	ITupleSchema schema;
	ITupleSchema outputSchema;
	IOperatorCode cpuCode, filterCpuCode, projectCpuCode;
	IOperatorCode gpuCode, filterGpuCode, projectGpuCode;
	Query query;
	int queryId = 0;
	long timestampReference = 0;
	int windowOffset;
	int windowBarrier;
	int batchSize;
	
	public SaberCalcRule(ITupleSchema schema, RelNode rel, int queryId , long timestampReference, WindowDefinition window, int windowOffset, int windowBarrier, int batchSize) {
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
		int numberOfInputAttrs = schema.numberOfAttributes();
		
		QueryConf queryConf = new QueryConf (batchSize);		
		window = new WindowDefinition (windowType, windowRange, windowSlide);	
		Set<QueryOperator> operators = new HashSet<QueryOperator>();
		
		// Filter Operator first. Check if we have a condition. If not, there is no filter to create.
		LogicalCalc calc = (LogicalCalc) rel;
		RexProgram program = calc.getProgram();
		RexLocalRef programCondition = program.getCondition();
		RexNode condition;
		if (programCondition == null) {
			condition = null;
		} else {
		    condition = program.expandLocalRef(programCondition);
			PredicateUtil predicateHelper = new PredicateUtil();
			Pair<RexNode, IPredicate> pair = predicateHelper.getCondition(condition, 0);
			IPredicate predicate = pair.right;
			System.out.println("Filter Expr is : "+ predicate.toString());
			filterCpuCode = new Selection (predicate);
			filterGpuCode = new SelectionKernel (schema, predicate, null, batchSize);
			cpuCode = filterCpuCode;
			gpuCode = filterGpuCode;
			QueryOperator filterOperator;
			filterOperator = new QueryOperator (filterCpuCode, filterGpuCode);
			operators.add(filterOperator);
			outputSchema = schema;
		}
		
		
		// Project Operator second. Check if we have certain attributes to project. If not, there is no project to create.
		List<RexLocalRef> projectList = program.getProjectList();
	    if (projectList != null && !projectList.isEmpty()) {
	        List<RexNode> expandedNodes = new ArrayList<>();
	        for (RexLocalRef project : projectList) {
	            expandedNodes.add(program.expandLocalRef(project));
	        }
	        
			List<RexNode> projectedAttrs = expandedNodes; 
			
			int projectedAttributes = projectedAttrs.size();
			int expressionDepth = 1;
			
			Expression [] expressions = new Expression [projectedAttributes];
			/* Always project the timestamp */
			//expressions[0] = new LongColumnReference(0);

			int column;
			int i = 0;
			for (RexNode attr : projectedAttrs){
				//System.out.println(attr.toString());
				if (attr.getKind().toString().equals("INPUT_REF")) {				
					column = Integer.parseInt(attr.toString().replace("$", ""));
					if ((windowBarrier >= 0) && (column >= windowOffset) && (column > 0)){ //fix the offset when the previous operator was LogicalWindow
						//if (column==windowOffset)column+=1; 
						column -= windowBarrier;	
					}				
					if ((windowBarrier < 0) && (column >= windowOffset) && (windowOffset > 0)) //fix the offset when the previous operator was LogicalAggregate
						column -= 1;
					
					if (column < 0 && windowBarrier > 0) column+= windowBarrier;
					if (column >= numberOfInputAttrs) column -= 1;
					
					if (schema.getAttributeType(column).equals(PrimitiveType.INT))
						expressions[i] = new IntColumnReference (column);
					else if (schema.getAttributeType(column).equals(PrimitiveType.FLOAT)) 
						expressions[i] = new FloatColumnReference (column);
					else
						expressions[i] = new LongColumnReference (column);
				} else { 
					//pass the windowOffset to more complex expressions
					Pair<Expression, Pair<Integer, Integer>> pair = new ExpressionBuilder(attr, windowOffset).build();			
					expressions[i] = pair.left;
					if (pair.right.left> 0) {
						windowRange = pair.right.left;
						windowSlide = pair.right.right;
						windowType = WindowType.RANGE_BASED;
					}
				}			
				i++;
			}
			
			/*Creating output Schema*/
			ITupleSchema newOutputSchema = ExpressionsUtil.getTupleSchemaFromExpressions(expressions);
			i = 0;
			for (RexNode attr : projectedAttrs){
				if (attr.getKind().toString().equals("INPUT_REF")) {
					column = Integer.parseInt(attr.toString().replace("$", ""));
					if ((windowBarrier >= 0) && (column >= windowOffset) && (column > 0)){ //fix the offset when the previous operator was LogicalWindow
						//if (column==windowOffset)column+=1; 
						column -= windowBarrier;	
					}				
					if ((windowBarrier < 0) && (column >= windowOffset) && (windowOffset > 0)) //fix the offset when the previous operator was LogicalAggregate
						column -= 1;
					
					if (column < 0 && windowBarrier > 0) column+= windowBarrier;
					if (column >= numberOfInputAttrs) column -= 1;
					
					newOutputSchema.setAttributeName(i, schema.getAttributeName(column));
				} else {
					newOutputSchema.setAttributeName(i, attr.toString());
				}			
				i++;
			}
			//System.out.println("Output Schema is : " + newOutputSchema.getSchema());

			if (outputSchema==null || !newOutputSchema.getSchema().equals(outputSchema.getSchema())) {

				IOperatorCode projectCpuCode = new Projection (expressions);
				IOperatorCode projectGpuCode = new ProjectionKernel (schema, expressions, batchSize, expressionDepth);
				cpuCode = projectCpuCode;
				gpuCode = projectGpuCode;
				
				QueryOperator projectOperator;
				projectOperator = new QueryOperator (cpuCode, gpuCode);
				operators.add(projectOperator);
				outputSchema = newOutputSchema;
			}
	    }
			
		if (programCondition == null && (projectList == null || projectList.isEmpty())) {
			// it shouldn't be happen
	        throw new IllegalStateException("Either projection or condition, or both should be provided.");
		}
	    
		WindowDefinition executionWindow = new WindowDefinition (WindowType.ROW_BASED, 1, 1);
		System.out.println("Window is : " + executionWindow.getWindowType().toString() + " with " + executionWindow.toString());
		
		query = new Query (queryId, operators, schema, executionWindow, null, null, queryConf, timestampReference);
		//resize the window according to possible changes from input
		window = new WindowDefinition (windowType, windowRange, windowSlide);			
	}
	
	public ITupleSchema getOutputSchema() {
		return this.outputSchema;
	}
	
	public Query getQuery() {
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

}
