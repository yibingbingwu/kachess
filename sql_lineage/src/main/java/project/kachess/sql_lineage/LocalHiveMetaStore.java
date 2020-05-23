package project.kachess.sql_lineage;

import project.kachess.sql_lineage.persistable.DatasetType;
import project.kachess.sql_lineage.persistable.SelectSectionType;
import project.kachess.sql_lineage.util.AutoIncrement;
import project.kachess.sql_lineage.util.MetaDataNotFound;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class LocalHiveMetaStore extends MetaDataService {
  private static final int BATCH_SIZE = 100;

  public LocalHiveMetaStore(String url) {
    super.init(url);
  }

  /**
   * Given a table name, find all the columns in the external source and generate insert statement
   */
  public List<String> generateSelectItemInserts(
      Long tabObjId, String idbName, String itabName) {
    List<String> retVal = new ArrayList<>();
    String tabName = itabName.toLowerCase(), dbName = idbName.toLowerCase();
    try {
      Statement stmt = connect.createStatement();

      String sqlCmd =
          String.format(
              "SELECT si.map_to_column, si.data_type FROM select_item si JOIN dataset ds ON si.dataset_id = ds.id AND" +
                  " si.map_to_schema='%s' AND si.map_to_table='%s' AND usage_context='%s' AND ds.type='%s' AND " +
                  "si.data_type is NOT NULL ORDER BY si.start_line ASC", dbName, tabName,
              SelectSectionType.SELECT.name(), DatasetType.TABLE.name());

      ResultSet tabColRs = stmt.executeQuery(sqlCmd);
      String insertHdr =
          "INSERT IGNORE INTO select_item (id, dataset_id, name, definition, usage_context, "
              + "alias, map_to_schema, map_to_table, map_to_column, is_simple_column, "
              + "data_type, start_line, created_dt) VALUES ";
      StringBuilder insertQry = new StringBuilder(insertHdr);
      int rowCnt = 0;
      while (tabColRs.next()) {
        String currColName = tabColRs.getString(1);
        String currColType = tabColRs.getString(2).toUpperCase();
        Long newColId = AutoIncrement.nextId();
        String tmpVal =
            String.format(
                "(%d, %d, '%s', '%s', '%s', NULL, '%s', '%s', '%s', 1, '%s', %d, now())",
                newColId,
                tabObjId,
                currColName,
                currColName,
                SelectSectionType.SELECT.name(),
                dbName,
                tabName,
                currColName,
                currColType,
                rowCnt + 1);
        if (rowCnt % BATCH_SIZE > 0) {
          insertQry.append(',');
        }
        insertQry.append(tmpVal);

        // Limit insert statement size:
        if (++rowCnt % BATCH_SIZE == 0) {
          retVal.add(insertQry.toString());
          insertQry = new StringBuilder(insertHdr);
        }
      }

      if (rowCnt == 0) {
        throw new MetaDataNotFound(
            String.format("Fail to locate %s.%s from external metadata", dbName, tabName));
      }

      if (rowCnt % BATCH_SIZE > 0) {
        retVal.add(insertQry.toString());
      }

      tabColRs.close();
      stmt.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return retVal;
  }

  public boolean tableExists(String idbName, String itabName) {
    try {
      String tabName = itabName.toLowerCase(), dbName = idbName.toLowerCase();
      Statement stmt = connect.createStatement();
      String sqlCmd =
          String.format(
              "SELECT 1 FROM dataset t WHERE map_to_schema = '%s' AND map_to_table='%s'", dbName, tabName);

      ResultSet tabFndRs = stmt.executeQuery(sqlCmd);
      boolean retval = tabFndRs.next();
      tabFndRs.close();
      return retval;
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return false;
  }
}
