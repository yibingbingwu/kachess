package project.kachess.sql_lineage;

import project.kachess.sql_lineage.persistable.Dataset;
import project.kachess.sql_lineage.persistable.SelectItem;
import project.kachess.sql_lineage.persistable.SelectSectionType;
import project.kachess.sql_lineage.util.AutoIncrement;
import project.kachess.sql_lineage.util.ColumnNotFound;
import project.kachess.sql_lineage.util.MiscChores;
import project.kachess.sql_lineage.util.SelectItemNameWrangler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.TreeSet;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class DatasetPool {
  // Note2self: tried Stack initially however, there may be other steps involved before
  // removing/popping a DS out of the stack. To make traversing easier, decided to change to List
  private final List<DatasetWrapper> datasets = new ArrayList<>();

  // Match that in the log4j2.xml
  private static final String LOGGER_DASHBOARD = "sql_lineage.ts_dashboard";
  private static Logger logger;

  private LineageDbService dbService;

  public DatasetPool(LineageDbService dbService, boolean isDashboard) {
    this.dbService = dbService;
    this.logger = LogManager.getLogger((isDashboard) ? LOGGER_DASHBOARD : this.getClass().getName());
  }

  public void add(DatasetWrapper dataset) {
    assertNotNull(dataset);
    datasets.add(dataset);
  }

  /**
   * Here are the use cases we are trying to resolve:
   *
   * <ul>
   *   <li>t.*
   *   <li>*
   *   <li>t.col1
   *   <li>col1
   *   <li>col1.attr1...
   *   <li>t.col1.attr1...
   * </ul>
   *
   * @param tmpSelItem
   * @param dsId
   * @param skipCols
   * @return
   */
  public List<SelectItem> resolveCurrentItem(
      SelectItem tmpSelItem, Long dsId, TreeSet<String> skipCols) {
    List<SelectItem> retval = new ArrayList<>();

    String rawTxt = tmpSelItem.getDefinition();
    // This is a '?.*' type
    if (rawTxt.endsWith("*")) {
      String[] allParts = rawTxt.split("\\.");
      assertTrue(allParts.length <= 2);
      try {
        List<SelectItem> foundMatches =
            lookupByNames((allParts.length > 1) ? allParts[0] : null, null);
        // Then everything in the return list is parent!
        for (SelectItem parentItem : foundMatches) {
          String refKey = parentItem.knownAs();
          Long newItemId = AutoIncrement.nextId();
          SelectItem newItem =
              new SelectItem(dbService,
                  newItemId,
                  refKey,
                  SelectSectionType.SELECT,
                  null,
                  null,
                  null,
                  null,
                  Boolean.TRUE,
                  parentItem.getDataType(),
                  null,
                  dsId,
                  null,
                  null,
                  null,
                  null);
          newItem.getParentColumns().add(parentItem);
          retval.add(newItem);
        }
      } catch (ColumnNotFound cnf) {
        logger.debug(
            String.format(
                "COL_NOT_FOUND: Cannot resolve column in * \"%s\" at position %d:%d",
                rawTxt, tmpSelItem.getStartLine(), tmpSelItem.getStartPos()));
      }
    }

    // If not, they are individual items. Now need to see if there are dependencies to resolve:
    // From t1 to t1.c1.a1...
    else {
      tmpSelItem.setDatasetId(dsId);
      List<SelectItem> newParentsList = new ArrayList<>();
      HashSet<SelectItem> parentColList = tmpSelItem.getParentColumns();
      boolean usedComplexType = false;
      for (SelectItem aCol : parentColList) {
        String rawParentNameTxt = aCol.getDefinition();

        SelectItemNameWrangler itemNamer = new SelectItemNameWrangler(rawParentNameTxt);
        List<SelectItem> foundParents = null;
        /**
         * Here is what I am trying to do: if a1.b1 comes in, try a1 as alias first. If that doesn't
         * work out, try null.a1.b1 (where b1 is an attr in a struct) next However, if a1 comes in,
         * we should try it only once
         */
        try {
          foundParents = lookupByNames(itemNamer.guessDbName(), itemNamer.guessTableName());
        } catch (ColumnNotFound cnf) {
          try {
            if (itemNamer.canTryAgain()) {
              foundParents = lookupByNames(itemNamer.guessDbName(), itemNamer.guessTableName());
              usedComplexType = true;
            } else {
              throw new ColumnNotFound();
            }
          } catch (ColumnNotFound cnf2) {
            if (!skipCols.contains(rawParentNameTxt)) {
              logger.debug(
                  String.format(
                      "COL_NOT_FOUND: Cannot resolve column \"%s\" in \"%s\" at position %d:%d",
                      rawParentNameTxt,
                      rawTxt,
                      tmpSelItem.getStartLine(),
                      tmpSelItem.getStartPos()));
            }
            continue;
          }
        }
        newParentsList.addAll(foundParents);
      }

      // If this a case where a simple column refers to a parent column/item, and
      // the column is resolved, then we can set the data type as
      // we knew it before
      if (tmpSelItem.getUsageContext() == SelectSectionType.SELECT
          && tmpSelItem.getSimpleColumn()
          && parentColList.size() == 1
          && newParentsList.size() > 0
          && !usedComplexType) {
        tmpSelItem.setDataType(newParentsList.get(0).getDataType());
      }
      // TODO - else if it is a known function, get function return type

      tmpSelItem.getParentColumns().clear();
      tmpSelItem.getParentColumns().addAll(newParentsList);
      retval.add(tmpSelItem);
    }

    return retval;
  }

  private List<SelectItem> lookupByNames(String prefix, String colName) throws ColumnNotFound {
    List<SelectItem> retval = new ArrayList<>();
    boolean foundExactMatch = false;

    for (DatasetWrapper currDataset : datasets) {
      if (SqlMetaDataExtractor.S_COL_ASTERISK.equals(colName)) {
        // If this comes from count(*) or the likes, where * really means little but still
        // need to be addressed to link up tables
        SelectItem newProxy =
            new SelectItem(
                dbService,
                AutoIncrement.nextId(),
                colName,
                SelectSectionType.SELECT,
                colName,
                currDataset.dsObj.getMapToSchema(),
                currDataset.dsObj.getMapToTable(),
                colName,
                Boolean.TRUE,
                null,
                null,
                currDataset.dsObj.getId(),
                null,
                null,
                null,
                null);
        retval.add(newProxy);
        newProxy.saveToDb();
        continue;
      }

      TreeSet<String> currDsRefNames =
          MiscChores.genUniqueStrVals(
              currDataset.alias,
              currDataset.dsObj.getMapToTable(),
              currDataset.dsObj.getDefinedName());

      // When looking for a specific dataset and not found
      if (prefix != null && currDsRefNames != null && !currDsRefNames.contains(prefix)) {
        continue;
      }

      // Here the cases are: "*", "z.*" (and currDs is z), "z.col1", or "col1"
      List<SelectItem> dsCols = currDataset.dsObj.columnList();
      if (dsCols != null) {
        for (SelectItem currCol : dsCols) {
          if (currCol.getUsageContext() != SelectSectionType.SELECT) {
            continue;
          }

          if (colName != null) {
            // This is the case "z.col1" or "col1"
            String refKey = currCol.knownAs();
            if (colName.equalsIgnoreCase(refKey)) {
              // Found exact match by name:
              retval.add(currCol);
              foundExactMatch = true;
              break;
            }
          } else {
            // This is the case "z.*" or "*"
            retval.add(currCol);
          }
        }
      }

      if (foundExactMatch) {
        break;
      }
    }

    if (retval.size() <= 0) {
      throw new ColumnNotFound();
    }
    return retval;
  }

  // Write the current datasets to DB:
  public void flush(Dataset newDs) {
    while (datasets.size() > 0) {
      DatasetWrapper rmMe = datasets.remove(datasets.size() - 1);
      rmMe.dsObj.saveToDb();
      dbService.addDatasetPair(newDs, rmMe.dsObj);
    }
  }

  public int size() {
    return datasets.size();
  }
}
