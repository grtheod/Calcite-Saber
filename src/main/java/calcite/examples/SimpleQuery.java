package calcite.examples;

import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Example of using Calcite via JDBC.
 *
 * <p>Schema is specified programmatically.</p>
 */
public class SimpleQuery {
  public static void main(String[] args) throws Exception {
    new SimpleQuery().run();
  }

  public void run() throws ClassNotFoundException, SQLException {
    Class.forName("org.apache.calcite.jdbc.Driver");
	Properties info = new Properties();
	info.setProperty("lex", "JAVA");
    Connection connection =
        DriverManager.getConnection("jdbc:calcite:model="
        		+ "/home/hduser/Downloads/calcite-master/example/csv/target/test-classes/example.json",info);
    CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);
    Statement statement = connection.createStatement();
    ResultSet resultSet =
            statement.executeQuery("select stream * from SS.ORDERS where SS.ORDERS.UNITS > 5");
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

}