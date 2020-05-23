package project.kachess.sql_lineage;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

import static org.junit.Assert.assertNotNull;

public abstract class MetaDataService {
  protected Connection connect = null;

  protected void init(String url) {
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
      connect.close();
    } catch (SQLException anyExp) {
      anyExp.printStackTrace();
    }
  }

  public abstract List<String> generateSelectItemInserts(Long tabObjId, String idbName, String itabName);

  public abstract boolean tableExists(String idbName, String itabName);
}
