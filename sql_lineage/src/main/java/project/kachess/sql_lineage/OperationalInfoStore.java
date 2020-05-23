package project.kachess.sql_lineage;

import java.sql.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertNotNull;

public class OperationalInfoStore {

  private Connection connect = null;
  private static final Pattern SPARK_CLASS_PATTERN = Pattern.compile(".*--class ([^ ]+).*");

  OperationalInfoStore(String url) {
    try {
      Class.forName("com.mysql.jdbc.Driver");
      // Setup the connection with the DB
      connect = DriverManager.getConnection(url);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      assertNotNull(connect);
    }
  }

  public void cleanup() {
    try {
      //      connect.rollback();
      connect.close();
    } catch (SQLException anyExp) {
      anyExp.printStackTrace();
    }
  }

  public String findAirflowHiveSqlSource(String dagTaskCat) {
    String retval = null;
    try {
      Statement stmt = connect.createStatement();
      String sqlCmd =
          String.format(
              "SELECT value FROM task_property WHERE concat(dag_id, '.', task_id)='%s' "
                  + "AND name='bingql_sql_fn' ORDER BY updated DESC LIMIT 1",
              dagTaskCat);
      ResultSet currRs = stmt.executeQuery(sqlCmd);
      if (currRs.next()) {
        retval = currRs.getString(1);
      }
      currRs.close();
      stmt.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return retval;
  }

  public String findAirflowSparkSqlSource(String dagTaskCat) {
    String retval = null;
    try {
      Statement stmt = connect.createStatement();
      String sqlCmd =
          String.format(
              "SELECT value FROM task_property WHERE concat(dag_id, '.', task_id)='%s' "
                  + "AND name='command_prefix' ORDER BY updated DESC LIMIT 1",
              dagTaskCat);
      ResultSet currRs = stmt.executeQuery(sqlCmd);
      if (currRs.next()) {
        String tmpVal = currRs.getString(1);
        Matcher pattMatch = SPARK_CLASS_PATTERN.matcher(tmpVal);
        if (pattMatch.find()) {
          retval = pattMatch.group(1); // whole matched expression
        }
      }
      currRs.close();
      stmt.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return retval;
  }
}
