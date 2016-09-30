package calcite.examples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.RuleSets;
import org.apache.calcite.tools.ValidationException;

import calcite.planner.QueryPlanner;

public class StreamQueryPlanner {

	private final Planner planner;

	public StreamQueryPlanner(SchemaPlus schema) {
	    final List<RelTraitDef> traitDefs = new ArrayList<RelTraitDef>();

	    traitDefs.add(ConventionTraitDef.INSTANCE);
	    traitDefs.add(RelCollationTraitDef.INSTANCE);

	    FrameworkConfig calciteFrameworkConfig = Frameworks.newConfigBuilder()
	        .parserConfig(SqlParser.configBuilder()
	            // Lexical configuration defines how identifiers are quoted, whether they are converted to upper or lower
	            // case when they are read, and whether identifiers are matched case-sensitively.
	            .setLex(Lex.JAVA)
	            .build())
	        // Sets the schema to use by the planner
	        .defaultSchema(schema)
	        .traitDefs(traitDefs)
	        // Context provides a way to store data within the planner session that can be accessed in planner rules.
	        .context(Contexts.EMPTY_CONTEXT)
	        // Rule sets to use in transformation phases. Each transformation phase can use a different set of rules.
	        .ruleSets(RuleSets.ofList())
	        // Custom cost factory to use during optimization
	        .costFactory(null)
	        .typeSystem(RelDataTypeSystem.DEFAULT)
	        .build();

	    this.planner = Frameworks.getPlanner(calciteFrameworkConfig);
	}

	public RelNode getLogicalPlan(String query) throws ValidationException, RelConversionException {
	    SqlNode sqlNode;

	    try {
	      sqlNode = planner.parse(query);
	    } catch (SqlParseException e) {
	      throw new RuntimeException("Query parsing error.", e);
	    }	
	    SqlNode validatedSqlNode = planner.validate(sqlNode);

	    return planner.rel(validatedSqlNode).project();
	}
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException, ValidationException, RelConversionException {
		// TODO Auto-generated method stub
	    Class.forName("org.apache.calcite.jdbc.Driver");
	    Connection connection =
	        DriverManager.getConnection("jdbc:calcite:");
	    CalciteConnection calciteConnection =
	        connection.unwrap(CalciteConnection.class);
	    SchemaPlus rootSchema = calciteConnection.getRootSchema();
	    rootSchema.add("os", new ReflectiveSchema(new Os()));
	    rootSchema.add("ps", new ReflectiveSchema(new Ps()));
	    Statement statement = connection.createStatement();
	    //ResultSet resultSet = statement.executeQuery("select *\n" + "from \"os\".\"orders\"");
	    QueryPlanner queryPlanner = new QueryPlanner(rootSchema);
	    RelNode loginalPlan = queryPlanner.getLogicalPlan("select os.orders.productid, description, sum(units)*0.3 from os.orders, ps.products where os.orders.productid=ps.products.productid group by os.orders.productid,description having sum(units) > 5  order by sum(units) ");
	    System.out.println(RelOptUtil.toString(loginalPlan));	    
	}

	public static class Os {
	    public final Order[] orders = {
	      new Order(3,1,12),
	      new Order(5,2,7),
	      new Order(15, 3,4),
	    };
	}

	public static class Order {
		public final int productid;
	    public final int orderid;
	    public final int units;
	    
	    public Order(int productid,int orderid, int units ) {
	      this.productid=productid;
	      this.orderid=orderid;
	      this.units=units;      
	    }
	}
	
	public static class Ps {
	    public final Product[] products = {
	      new Product(3,"brush"),
	      new Product(5,"paint"),
	      new Product(15,"box"),
	    };
	}

	public static class Product {
		public final int productid;
	    public final String description;	    

	    public Product(int productid,String description) {
	      this.productid=productid;
	      this.description=description;	            
	    }
	}
	
}
