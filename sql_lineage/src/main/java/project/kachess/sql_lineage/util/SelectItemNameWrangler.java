package project.kachess.sql_lineage.util;

import project.kachess.shared.Util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This class tries to standardize item names after SELECT, e.g. col, tab.col, alias.col,
 * col.struct..., alias.col.struct..., etc.
 */
public class SelectItemNameWrangler {
  private List<String> allElements;
  private String origTxt = null;

  public SelectItemNameWrangler(String rawTxt) {
    origTxt = rawTxt;
    reset();
  }

  public int size() {
    return allElements.size();
  }

  public String guessDbName() {
    return allElements.get(0);
  }

  public String guessTableName() {
    return allElements.get(1);
  }

  public boolean canTryAgain() {
    allElements.add(0, null);
    // If the first element in a1.b1.c1 is already pushed to the 3rd position,
    // and the first two are nulls, then it won't work
    return (size() > 2 && allElements.get(1) != null);
  }

  public void reset() {
    String[] compos = Util.separateNamespaces(origTxt, 0);
    allElements = new ArrayList<>();
    allElements.addAll(Arrays.asList(compos));
    // Make sure there is at least two elements: alias then colname:
    if (size() == 1) {
      allElements.add(0, null);
    }
  }
}
