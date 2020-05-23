package project.kachess.sql_lineage;

import project.kachess.sql_lineage.persistable.SelectSectionType;
import project.kachess.sql_lineage.util.AutoIncrement;
import project.kachess.sql_lineage.util.MetaDataNotFound;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class ExternalHiveMetaStore extends MetaDataService {
  private static final int BATCH_SIZE = 100;

  public ExternalHiveMetaStore(String url) {
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
              "SELECT t.TBL_ID, c.COLUMN_NAME, c.TYPE_NAME "
                  + "FROM TBLS t "
                  + "JOIN DBS d ON t.DB_ID = d.DB_ID "
                  + "JOIN SDS s ON t.SD_ID = s.SD_ID "
                  + "JOIN COLUMNS_V2 c ON s.CD_ID = c.CD_ID "
                  + "WHERE TBL_NAME = '%s' AND d.NAME='%s' ORDER by INTEGER_IDX",
              tabName, dbName);

      ResultSet tabColRs = stmt.executeQuery(sqlCmd);
      String insertHdr =
          "INSERT IGNORE INTO select_item (id, dataset_id, name, definition, usage_context, "
              + "alias, map_to_schema, map_to_table, map_to_column, is_simple_column, "
              + "data_type, start_line, created_dt) VALUES ";
      StringBuilder insertQry = new StringBuilder(insertHdr);
      Long extTableId = null;
      int rowCnt = 0;
      while (tabColRs.next()) {
        extTableId = tabColRs.getLong(1);
        String currColName = tabColRs.getString(2);
        String currColType = tabColRs.getString(3).toUpperCase();
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
        if (rowCnt %BATCH_SIZE > 0) {
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
      tabColRs.close();

      // Now add partition keys:
      sqlCmd =
          String.format(
              "SELECT PKEY_NAME, PKEY_TYPE FROM PARTITION_KEYS WHERE TBL_ID=%d ORDER BY INTEGER_IDX",
              extTableId);
      tabColRs = stmt.executeQuery(sqlCmd);
      boolean isFirst = (rowCnt % BATCH_SIZE == 0);
      while (tabColRs.next()) {
        Long newColId = AutoIncrement.nextId();
        String currColName = tabColRs.getString(1);
        String currColType = tabColRs.getString(2).toUpperCase();
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
                ++rowCnt);
        if (!isFirst) {
          insertQry.append(',');
        } else {
          isFirst = false;
        }
        insertQry.append(tmpVal);
      }

      if (!isFirst) {
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
              "SELECT TBL_ID FROM TBLS t "
                  + "JOIN DBS d ON t.DB_ID = d.DB_ID "
                  + "WHERE TBL_NAME = '%s' AND d.NAME='%s'",
              tabName, dbName);

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
