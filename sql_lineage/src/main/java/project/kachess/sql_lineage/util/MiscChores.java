package project.kachess.sql_lineage.util;

import project.kachess.shared.Util;
import project.kachess.sql_lineage.SqlMetaDataExtractor;
import project.kachess.sql_lineage.persistable.Dataset;
import project.kachess.sql_lineage.persistable.SelectItem;
import project.kachess.sql_lineage.persistable.SelectSectionType;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.misc.Interval;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import static org.junit.Assert.assertTrue;

public class MiscChores {
  /**
   * As of Antlr4 (MS)SQL parser's aliasContext.getText() returns a concatenated "as" and
   * "something". This function is to skip the "as" part if present
   *
   * @param aliasCtx
   * @return
   */
  public static final String extractRealAlias(ParserRuleContext aliasCtx) {
    if (aliasCtx == null) {
      return null;
    }
    if (aliasCtx.getChildCount() > 1) {
      return aliasCtx.getChild(1).getText();
    }
    return aliasCtx.getText();
  }

  /**
   * Best effort to figure out return type The thinking here is to identify the top-most action
   * type: cast/case-when/function/map-struct instantiation and give a best effort estimate on
   * return type There is no need to dive into nested levels unless we need that kind of details.
   *
   * <p>Otherwise, should return a UNKNOWN type so this pattern be fixed/improved later
   */
  public static String guessReturnType(boolean isBoolExp, String rawTxt) {
    if (isBoolExp) {
      return "BOOLEAN";
    }
    return null;
  }

  /**
   * Antl Context object's .getText() returns a non-whitespace string. This helper gets the original
   * text instead altStopIdx: you can choose an alternative stop point (e.g. skip brackets?) maxLen:
   * so the return value won't exceed max value in db def
   */
  public static String getOriginalText(ParserRuleContext ctx, int altStopIdx, int maxLen) {
    int startPt = ctx.start.getStartIndex();
    int stopPt = (altStopIdx > 0) ? altStopIdx : ctx.stop.getStopIndex();
    Interval txtIntv = new Interval(startPt, stopPt);
    String origTxt = ctx.start.getInputStream().getText(txtIntv);
    return (maxLen > 0) ? origTxt : Util.substrMaxLen(origTxt, maxLen);
  }

  public static void union(Dataset unionBase, Dataset unionSecond) {
    // Add column dependencies from the 2nd one to the first one and that is it
    List<SelectItem> baseList = filterSelectTypeOnly(unionBase.columnList()),
        secondList = filterSelectTypeOnly(unionSecond.columnList());
    assertTrue(baseList.size() <= secondList.size());
    for (int idx = 0; idx < baseList.size(); idx++) {
      SelectItem colIn1st = baseList.get(idx), colIn2nd = secondList.get(idx);
      colIn1st.setExtraInfo(
          Util.substrMaxLen(
              SqlMetaDataExtractor.KW_UNION_FLAG + colIn2nd.getDefinition(),
              SqlMetaDataExtractor.CONS_COL_DEF_MAXLEN));
      colIn1st.getParentColumns().addAll(colIn2nd.getParentColumns());
    }
    unionBase.setEndLine(unionSecond.getEndLine());
    unionBase.setEndPos(unionSecond.getEndPos());
    unionBase.setIsUnionBase(false);
  }

  public static List<SelectItem> filterSelectTypeOnly(List<SelectItem> origList) {
    List<SelectItem> retval = new ArrayList<>();
    for (SelectItem currItem : origList) {
      if (currItem.getUsageContext() == SelectSectionType.SELECT) {
        retval.add(currItem);
      }
    }
    return retval;
  }

  public static boolean hasClassInAncestry(ParserRuleContext baseCtx, Class... cls2Look) {
    RuleContext parentCtx = baseCtx;
    while ((parentCtx = parentCtx.parent) != null) {
      for (Class acls : cls2Look) {
        if (acls.isInstance(parentCtx)) {
          return true;
        }
      }
    }
    return false;
  }

  public static TreeSet<String> genUniqueStrVals(String... items) {
    TreeSet<String> retval = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    for (String i : items) {
      if (i != null) {
        retval.add(i);
      }
    }
    return retval;
  }
}
