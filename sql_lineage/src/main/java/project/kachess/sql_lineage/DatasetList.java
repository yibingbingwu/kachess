package project.kachess.sql_lineage;

import project.kachess.sql_lineage.util.MiscChores;

import java.util.ArrayList;

public class DatasetList extends ArrayList<DatasetWrapper> {
  @Override
  public boolean add(DatasetWrapper newDs) {
    DatasetWrapper lastItem = lastItem();
    if (lastItem != null && lastItem.dsObj.canBeUnioned()) {
      // The logic is to merge the two datasets into one (the first one)
      // And set it as the combined result
      MiscChores.union(lastItem.dsObj, newDs.dsObj);
      // Update the changed base record since it must have been saved before.
      // However, the second one may never have:
      lastItem.dsObj.saveToDb();
    } else {
      super.add(newDs);
    }
    return true;
  }

  public DatasetWrapper lastItem() {
    return super.size() > 0 ? super.get(super.size() - 1) : null;
  }
}
