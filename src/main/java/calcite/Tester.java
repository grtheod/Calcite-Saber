package calcite;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

import calcite.examples.SimpleQueryPlanner;
import calcite.examples.StreamQueryPlanner.Order;
import calcite.examples.StreamQueryPlanner.Os;
import calcite.examples.StreamQueryPlanner.Product;
import calcite.examples.StreamQueryPlanner.Ps;
import calcite.planner.QueryPlanner;
import calcite.planner.physical.PhysicalRuleConverter;
import calcite.utils.OrdersTableFactory;
import calcite.utils.ProductsTableFactory;
import calcite.utils.SaberSchema;
import uk.ac.imperial.lsds.saber.ITupleSchema;

public class Tester {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
	    Class.forName("org.apache.calcite.jdbc.Driver");
	    Connection connection =
	        DriverManager.getConnection("jdbc:calcite:");
	    CalciteConnection calciteConnection =
	        connection.unwrap(CalciteConnection.class);
	    SchemaPlus rootSchema = calciteConnection.getRootSchema();
	    SchemaPlus schema = rootSchema.add("s", new AbstractSchema());	    	   	    
	    schema.add("orders", new OrdersTableFactory().create(schema, "orders", null, null));
	    schema.add("products", new ProductsTableFactory().create(schema, "products", null, null));
	    	  
	    
	    Statement statement = connection.createStatement();
	    QueryPlanner queryPlanner = new QueryPlanner(rootSchema);
	    	    	   	  	    
	    RelNode logicalPlan = queryPlanner.getLogicalPlan("select productid,count(*),sum(units) from s.orders"
	    		+ " where units > 5 group by productid  ");	 //and s.orders.units = 5   
	    //(18 > 5) and((s.orders.units > 5 and (1 > 4) and (3 = 4)) or (1>0))
	    System.out.println(RelOptUtil.toString(logicalPlan,SqlExplainLevel.EXPPLAN_ATTRIBUTES));
	    	    
	    
	    PhysicalRuleConverter physicalPlan = new  PhysicalRuleConverter(logicalPlan);
	    physicalPlan.execute();
	    
	}

}
