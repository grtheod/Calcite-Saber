package calcite.planner.physical;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;

import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntConstant;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntExpression;
import uk.ac.imperial.lsds.saber.cql.predicates.ANDPredicate;
import uk.ac.imperial.lsds.saber.cql.predicates.IPredicate;
import uk.ac.imperial.lsds.saber.cql.predicates.IntComparisonPredicate;
import uk.ac.imperial.lsds.saber.cql.predicates.ORPredicate;

public class PredicateUtil {
	
	public static final int      EQUAL_OP = 0;
	public static final int   NONEQUAL_OP = 1;
	public static final int       LESS_OP = 2;
	public static final int    NONLESS_OP = 3;
	public static final int    GREATER_OP = 4;
	public static final int NONGREATER_OP = 5;
	
	/* Match a given comparison operator to Saber's operator codes*/
	private int getComparisonOperator(String compOp) {
		int operatorCode; 
		switch (compOp){			
			case "<>" :
				operatorCode = NONEQUAL_OP; break;
			case "<" :
				operatorCode = LESS_OP; break;
			case ">=" :
				operatorCode = NONLESS_OP; break;
			case ">" :
				operatorCode = GREATER_OP; break;
			case "<=" :
				operatorCode = NONGREATER_OP; break;
			default:
				operatorCode = EQUAL_OP; //EQUAL_OP is considered default
		}
		return operatorCode;
	}
	
	/*
	 * Create condition from a given RexNode (only for integer Expressions).
	 * */
	public Pair<RexNode,IPredicate> getCondition(RexNode condition, int joinOffset){		
	    if (condition instanceof RexCall) {
			List <Pair<RexNode,IPredicate>> operands = new ArrayList <Pair<RexNode,IPredicate>>();
	        for (RexNode operand : ((RexCall) condition).getOperands()) {
	        	operands.add(getCondition(operand,joinOffset));	        	
	        }
	        if (((RexCall) condition).getOperator().toString().equals("OR")){
	        	int i=0;
	        	IPredicate [] predicates = new IPredicate [operands.size()];
	        	for (Pair<RexNode,IPredicate> p : operands){
	    			predicates[i] = p.right;
	    			i++;
	        	}
	        	return new Pair<RexNode,IPredicate>(condition,new ORPredicate(predicates));	        	
	        } else
	        if (((RexCall) condition).getOperator().toString().equals("AND")){
	        	int i=0;
	        	IPredicate [] predicates = new IPredicate [operands.size()];
	        	for (Pair<RexNode,IPredicate> p : operands){
	    			predicates[i] = p.right;
	    			i++;
	        	}
	        	return new Pair<RexNode,IPredicate>(condition,new ANDPredicate(predicates));	        	
	        } else{
	        	RexCall rex = (RexCall) condition;
	        	int comparisonOperator = getComparisonOperator(rex.getOperator().toString());
	        	/*supports only integer expressions at the moment*/
	    		IntExpression firstOp,secondOp;
	    		
	    		if(rex.getOperands().get(0).toString().contains("$")){
	    			firstOp = new IntColumnReference(Integer.parseInt(rex.getOperands().get(0).toString().replace("$", "").trim()));
	    		}else {
	    			firstOp = new IntConstant(Integer.parseInt(rex.getOperands().get(0).toString().trim()));
	    		}
	    		if(rex.getOperands().get(1).toString().contains("$")){ //using joinOffset to fix the second operand in the case of join
	    			secondOp = new IntColumnReference(Integer.parseInt(rex.getOperands().get(1).toString().replace("$", "").trim()) - joinOffset);
	    		}else {
	    			secondOp = new IntConstant(Integer.parseInt(rex.getOperands().get(1).toString().trim()));
	    		}	    		
	        	return new Pair<RexNode,IPredicate>(condition, new IntComparisonPredicate(comparisonOperator, firstOp, secondOp));
	        }
	    } else {    	
	    	return new Pair<RexNode,IPredicate>(condition,null);
	    }	
	}		

}
