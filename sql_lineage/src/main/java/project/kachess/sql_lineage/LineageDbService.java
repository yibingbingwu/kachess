package project.kachess.sql_lineage;

import project.kachess.sql_lineage.persistable.Dataset;
import project.kachess.sql_lineage.persistable.DatasetType;
import project.kachess.sql_lineage.persistable.SelectItem;
import project.kachess.sql_lineage.persistable.SelectSectionType;
import project.kachess.sql_lineage.util.AutoIncrement;
import project.kachess.sql_lineage.util.TableNotFound;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertNotNull;

public class LineageDbService {
  private Connection connect = null;
  private MetaDataService metaDataService;

  public Connection getDbConn() {
    return connect;
  }

  public LineageDbService(String url, MetaDataService metaDataService) {
    this.metaDataService = metaDataService;
    try {
      Class.forName("com.mysql.jdbc.Driver");
      connect = DriverManager.getConnection(url + "&useSSL=false");
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

  public MetaDataService getMetaDataService() {
    return metaDataService;
  }

  public void addSelectColumnPair(
      SelectItem aCol, SelectItem itsParent, SelectSectionType itemType) {
    try {
      PreparedStatement pstmt =
          connect.prepareStatement(
               "INSERT IGNORE INTO select_item_rel "
                  + "(parent_select_item_id, child_select_item_id, usage_context, created_dt) "
                  + "VALUES (?, ?, ?, now())");
      pstmt.setLong(1, itsParent.getId());
      pstmt.setLong(2, aCol.getId());
      pstmt.setString(3, itemType.name());
      pstmt.executeUpdate();
      pstmt.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public void addDatasetPair(Dataset aDs, Dataset itsParent) {
    try {
      PreparedStatement pstmt =
          connect.prepareStatement(
              "INSERT IGNORE INTO dataset_rel (parent_dataset_id, child_dataset_id, created_dt) "
                  + "VALUES (?, ?, now())");
      pstmt.setLong(1, itsParent.getId());
      pstmt.setLong(2, aDs.getId());
      pstmt.executeUpdate();
      pstmt.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  /**
   * This method adds external table entry into dataset table If the table already exists in the
   * metadata table, it will be set to READONLY
   */
  public Dataset locateExternalTableInLocalDb(String dbName, String tabName) {
    Dataset retObj =
        new Dataset(this, null, DatasetType.TABLE, dbName, tabName, null, null, null, null, false);
    try {
      Statement stmt = connect.createStatement();
      // Does table already exist?
      String sqlCmd =
          String.format(
              "SELECT id FROM dataset WHERE type='%s' AND map_to_schema='%s' and map_to_table='%s'",
              DatasetType.TABLE, dbName, tabName);
      ResultSet currRs = stmt.executeQuery(sqlCmd);
      if (currRs.next()) {
        retObj.setId(currRs.getLong(1));
        retObj.setReadOnly(true);
        currRs.close();
      } else {
        Long tableObjId = AutoIncrement.nextId();
        retObj.setId(tableObjId);

        // If table does not already exist, most likely nor does columns
        List<String> insertStmts =
            metaDataService.generateSelectItemInserts(tableObjId, dbName, tabName);
        if (insertStmts == null || insertStmts.size() == 0) {
          throw new TableNotFound(
              String.format(
                  "Did not find table %s.%s from metadata DB or external source.",
                  dbName, tabName));
        }

        retObj.saveToDb();
        for (String currInsert : insertStmts) {
          stmt.executeUpdate(currInsert);
        }
      }

      // Now try load Column info
      sqlCmd =
          "SELECT id, alias, definition, map_to_column, data_type FROM select_item WHERE dataset_id="
              + retObj.getId();
      currRs = stmt.executeQuery(sqlCmd);
      List<SelectItem> retColArr = new ArrayList<>();
      while (currRs.next()) {
        Long currColId = currRs.getLong(1);
        String currColAlias = currRs.getString(2);
        String currColDef = currRs.getString(3);
        String currColMap2 = currRs.getString(4);
        String currColType = currRs.getString(5);

        // Let's standardize the type naming
        if (currColType != null) {
          currColType = currColType.toUpperCase();
        }

        SelectItem newCol =
            new SelectItem(
                this,
                currColId,
                currColDef,
                SelectSectionType.SELECT,
                currColAlias,
                dbName,
                tabName,
                currColMap2,
                Boolean.TRUE,
                currColType,
                null,
                retObj.getId(),
                null,
                null,
                null,
                null);
        retColArr.add(newCol);
      }
      retObj.setColumnList(retColArr);

      stmt.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return retObj;
  }

  private void addExtTableInfoToDataset(Long tableObjId, String dbName, String tabName) {
    try {
      Statement stmt = connect.createStatement();
      // First insert into dataset using table info:
      String sqlCmd =
          String.format(
              "INSERT IGNORE INTO dataset (id, type, map_to_schema, map_to_table, is_aggregated, created_dt) "
                  + "VALUES (%d, '%s', '%s', '%s', 0, now())",
              tableObjId, DatasetType.TABLE, dbName, tabName);
      stmt.executeUpdate(sqlCmd);
      stmt.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public void recordInsert(Long dsId, Long jobId, String tabDb, String tabName, String insertType) {
    try {
      Statement stmt = connect.createStatement();
      String sqlCmd =
          String.format(
              "INSERT IGNORE INTO table_insert (dataset_id, sql_source_id, db_schema, db_table, type, created_dt) "
                  + "VALUES (%d, %d, '%s', '%s', '%s', now())",
              dsId, jobId, tabDb, tabName, insertType);
      stmt.executeUpdate(sqlCmd);
      stmt.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public void recordDashboardDatasetLink(Long dsId, Long jobId) {
    try {
      Statement stmt = connect.createStatement();
      String sqlCmd =
          String.format(
              "INSERT IGNORE INTO dashboard_dataset (dataset_id, sql_source_id, created_dt) "
                  + "VALUES (%d, %d, now())",
              dsId, jobId);
      stmt.executeUpdate(sqlCmd);
      stmt.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public PreparedStatement getPreparedStatement(String prepSql) throws SQLException {
    return connect.prepareStatement(prepSql);
  }

  public void runPreparedUpdate(PreparedStatement pstmt) throws SQLException {
    pstmt.executeUpdate();
    pstmt.close();
  }

  public void truncateDb() {
    try {
      Statement stmt = connect.createStatement(), truncateStmt = connect.createStatement();
      String sqlCmd =
          "select concat('truncate table ', TABLE_SCHEMA, '.', TABLE_NAME) from information_schema.tables where "
              + "TABLE_SCHEMA='bingql'";
      ResultSet rs = stmt.executeQuery(sqlCmd);
      while (rs.next()) {
        truncateStmt.executeUpdate(rs.getString(1));
      }
      rs.close();
      stmt.close();
      truncateStmt.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public void recordAlter(Long jobId, String tabDb, String tabName, String storageLoc) {
    try {
      PreparedStatement pstmt =
          connect.prepareStatement(
              "INSERT IGNORE INTO table_add_partition "
                  + "(sql_source_id, db_schema, db_table, location, created_dt) "
                  + "VALUES (?, ?, ?, ?, now())");
      pstmt.setLong(1, jobId);
      pstmt.setString(2, tabDb);
      pstmt.setString(3, tabName);
      pstmt.setString(4, storageLoc);
      pstmt.executeUpdate();
      pstmt.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public void changeDatasetMapping(String dbName, String tabName, String newTabName) {
    try {
      Statement stmt = connect.createStatement();
      String sqlCmd =
          String.format(
              "UPDATE dataset SET map_to_table = '%s' WHERE map_to_schema = '%s' AND map_to_table = '%s'",
              newTabName, dbName, tabName);
      stmt.executeUpdate(sqlCmd);
      stmt.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public void removeTable(Long jobId, String tabDb, String tabName) {
    try {
      Statement stmt = connect.createStatement(), truncateStmt = connect.createStatement();
      String sqlCmd =
          String.format(
              "DELETE FROM table_insert WHERE sql_source_id=%d AND db_schema='%s' AND db_table='%s'",
              jobId, tabDb, tabName);
      stmt.executeUpdate(sqlCmd);
      stmt.close();
      truncateStmt.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}
