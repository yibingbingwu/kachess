package project.kachess.shared;

public class Util {
  /**
   * A way to parse a.b.c into String[3] = [a, b, c] or b.c into [null, b, c] Always assume the left
   * namespaces can be omitted. So the return array is right aligned with the split one
   *
   * @param rawTxt
   * @param expectedSz, 0 as return all
   * @return
   */
  public static String[] separateNamespaces(String rawTxt, int expectedSz) {
    String[] colNameParts = rawTxt.split("\\.");
    if (colNameParts.length <= expectedSz) {
      String[] retval = new String[expectedSz];
      for (int idx = 0; idx < expectedSz; idx++) {
        int splitIdx = colNameParts.length - idx;
        if (splitIdx > 0) {
          retval[expectedSz - idx - 1] = colNameParts[splitIdx - 1];
        } else {
          retval[expectedSz - idx - 1] = null;
        }
      }
      return retval;
    }
    return colNameParts;
  }

  public static String substrMaxLen(String origTxt, int maxLen) {
    return origTxt.substring(0, Math.min(origTxt.length(), maxLen));
  }

  /**
   * This method is supposed to strip special chars, remove brackets from complex or dirty column
   * names
   *
   * @param rawDef
   * @return
   */
  public static String extractCleanColName(String rawDef) {
    String retval = rawDef;
    int pos = -1;

    pos = retval.indexOf("`");
    if (pos >= 0) {
      retval = retval.replace("`", "");
    }
    pos = retval.indexOf("[");
    if (pos >= 0) {
      retval = retval.substring(0, pos);
    }
    return retval;
  }

  /**
   * Remove only the enclosing single or double quotes
   *
   * @param origStr
   * @return
   */
  public static String removeQuotes(String origStr) {
    if (origStr == null) {
      return null;
    }

    if ((origStr.startsWith("'") && origStr.endsWith("'"))
        || (origStr.startsWith("\"") && origStr.endsWith("\""))
        || (origStr.startsWith("`") && origStr.endsWith("`"))) {
      return origStr.substring(1, origStr.length() - 1);
    }
    return origStr;
  }

  public static <T> T coalesce(T... items) {
    for (T i : items) if (i != null) return i;
    return null;
  }
}
