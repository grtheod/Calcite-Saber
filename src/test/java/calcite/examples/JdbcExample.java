package calcite.examples;

import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Example of using Calcite via JDBC.
 *
 * <p>Schema is specified programmatically.</p>
 */
public class JdbcExample {
  public static void main(String[] args) throws Exception {
    new JdbcExample().run();
  }

  public void run() throws ClassNotFoundException, SQLException {
    Class.forName("org.apache.calcite.jdbc.Driver");
    Connection connection =
        DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    rootSchema.add("os", new ReflectiveSchema(new Os()));
    Statement statement = connection.createStatement();
    ResultSet resultSet =
        statement.executeQuery("select *\n"
            + "from \"os\".\"orders\"");
    final StringBuilder buf = new StringBuilder();
    while (resultSet.next()) {
      int n = resultSet.getMetaData().getColumnCount();
      for (int i = 1; i <= n; i++) {
        buf.append(i > 1 ? "; " : "")
            .append(resultSet.getMetaData().getColumnLabel(i))
            .append("=")
            .append(resultSet.getObject(i));
      }
      System.out.println(buf.toString());
      buf.setLength(0);
    }
    resultSet.close();
    statement.close();
    connection.close();
  }

  /** Object that will be used via reflection to create the "hr" schema. */
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

}