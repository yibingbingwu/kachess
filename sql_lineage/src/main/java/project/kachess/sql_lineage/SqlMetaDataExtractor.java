package project.kachess.sql_lineage;

import project.kachess.shared.TableNameWrangler;
import project.kachess.shared.Util;
import project.kachess.sql_lineage.persistable.*;
import project.kachess.sql_lineage.util.AutoIncrement;
import project.kachess.sql_lineage.util.FunctionAssesser;
import project.kachess.sql_lineage.util.MetaDataNotFound;
import project.kachess.sql_lineage.util.MiscChores;
import project.kachess.sqlparser.g4generated.BingqlBaseVisitor;
import project.kachess.sqlparser.g4generated.BingqlParser;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Data Structure questions: Why Stack<List> for Datasets - when using JOIN, bunch of tables will
 * appear as parallels/peers in SELECT. They are what the LIST is for
 *
 * <p>This class may get messy as we learn more about SQL metadata. Need to keep an eye out on
 * delegation
 */
public class SqlMetaDataExtractor extends BingqlBaseVisitor<Void> {
  static final String SYS_AIRFLOW = "airflow";
  static final String SYS_DASHBOARD = "dashboard";

  private JobContext jobContext = null;

  // Global objects during a parsing session
  private final Stack<List<SelectItem>> colListMagzine = new Stack<>();
  private final Stack<DatasetList> dsListMagzine = new Stack<>();
  private final Map<String, Dataset> dsCache = new HashMap<>();
  private final Map<String, Dataset> cteCache = new HashMap<>();
  private final Map<String, Dataset> tmptblCache = new HashMap<>();
  private final HashSet<Integer> currGroupByCols = new HashSet<>();
  // Need to keep track of dropped table in the current Session. Otherwise, a date-versioned
  // may already exists in metadata. But b/c we use the earliest day to keep output from changing
  // the date-versioned tables may exist in metadata but have a very different schema
  private final HashSet<String> droppedTables = new HashSet<>();

  private Dataset newlyCreatedTable = null;
  private Dataset newlyAlteredTable = null;

  private boolean isMultiTableInsert = false;

  public static final String KW_UNION_FLAG = "_UNION_APPLIED_";

  // Special column names to help bridge table level dependency without the columns being used or
  // exposed directly
  public static final String S_COL_ASTERISK = "$COL_ASTERISK$";

  public static final int CONS_COL_DEF_MAXLEN = 1024;
  public static final int CONS_TAB_NAME_MAXLEN = 127;

  static final Logger logger = LogManager.getLogger(SqlMetaDataExtractor.class.getName());

  private LineageDbService dbService;

  public void initSession(JobContext jobContext, LineageDbService dbService) {
    this.jobContext = jobContext;
    this.dbService = dbService;

    // Clean slate:
    colListMagzine.clear();
    dsListMagzine.clear();
    dsCache.clear();
    cteCache.clear();
    tmptblCache.clear();
    currGroupByCols.clear();
  }

  public void closeSession() {
    // Remove temporary table from the "scope" by renaming it in DB
    // From design pov, it is cleaner to remove all the artifacts associated with the temporary
    // table. However, when it comes to implementation, it introduces code-data-model dependency.
    // In the end, I compromised
    for (Iterator<Map.Entry<String, Dataset>> eit = tmptblCache.entrySet().iterator();
        eit.hasNext(); ) {
      Map.Entry<String, Dataset> dsEnt = eit.next();

      String nameSuffix =
          Optional.ofNullable(this.jobContext.getSqlSource().getSourceLocator())
              .orElse(UUID.randomUUID().toString().replaceAll("\\.|-| ", ""));
      TableNameWrangler tabNamer =
          new TableNameWrangler(dsEnt.getKey(), jobContext.getCurrentSchema());
      String currName = tabNamer.getTabName();
      String newName = String.format("tmp_%s_%s", currName, nameSuffix);
      dbService.changeDatasetMapping(
          tabNamer.getDbName(true),
          currName,
          newName.substring(0, Math.min(CONS_TAB_NAME_MAXLEN, newName.length())));
      eit.remove();
    }

    // If this is a Dashboard job, the top level SELECTs are inserted into database
    // Similar to the "INSERT OVERWRITE TABLE" on the generation side, a Dashboard is a end node on
    // the consumption side
    if (jobContext.getSystemSource().equalsIgnoreCase(SYS_DASHBOARD)) {
      while (!dsListMagzine.empty()) {
        DatasetList topOfStack = dsListMagzine.pop();
        dbService.recordDashboardDatasetLink(
            topOfStack.lastItem().dsObj.getId(), jobContext.getSqlSource().getId());
      }
    }
  }

  @Override
  public Void visitProgram(BingqlParser.ProgramContext ctx) {
    assertNotNull(jobContext);
    super.visitProgram(ctx);
    return null;
  }

  @Override
  public Void visitUse_stmt(BingqlParser.Use_stmtContext ctx) {
    jobContext.setCurrentSchema(ctx.getChild(1).getText());
    return null;
  }

  @Override
  public Void visitStmt(BingqlParser.StmtContext ctx) {
    // Actually not quite sure if this is the correct, keep for legacy -- DataSet Magazine should be
    // within SELECT but not every Statement:
    dsListMagzine.push(new DatasetList());

    super.visitStmt(ctx);

    // If there is leftover, top-level dataset, save now:
    // A stronger statement should be: assert(dsListMagzine.size() == 1)
    // Except I don't have time to troubleshoot all the cases now ...
    while (!dsListMagzine.empty()) {
      DatasetList dsObjs = dsListMagzine.pop();
      for (DatasetWrapper adsw : dsObjs) {
        adsw.dsObj.saveToDb();
      }
    }

    // CTE scope
    cteCache.clear();

    return null;
  }

  /**
   * Where select_item meets from_datasets. At this point, we should know the dataset's alias as
   * well
   */
  @Override
  public Void visitBasic_select_stmt(BingqlParser.Basic_select_stmtContext ctx) {
    dsListMagzine.push(new DatasetList());

    super.visitBasic_select_stmt(ctx);

    List<SelectItem> selectItems = colListMagzine.pop();
    List<DatasetWrapper> currDsList = dsListMagzine.pop();
    if (isMultiTableInsert) {
      // If this is MultiTableInsert, individual SELECT within an sub-component
      // INSERT will have a NULL currDsList. The DS that is responsible for
      // resolving the SELECT items is actually the one node attached to
      // the beginning/top of the MultipleInsert.
      currDsList = dsListMagzine.peek();
    }

    DatasetWrapper resolvedDs =
        resolveSelectListFromDatasets(selectItems, currDsList, ctx.start, ctx.stop);
    resolveGroupBy(selectItems, currGroupByCols);
    dsListMagzine.peek().add(resolvedDs);

    return null;
  }

  /**
   * Prepare for the coming of bunch of new select_items by creating a busket and add it to the
   * Stack
   */
  @Override
  public Void visitSelect_list(BingqlParser.Select_listContext ctx) {
    colListMagzine.push(new ArrayList<>());
    super.visitSelect_list(ctx);
    return null;
  }

  @Override
  public Void visitSelect_list_asterisk(BingqlParser.Select_list_asteriskContext ctx) {
    Long nextId = AutoIncrement.nextId();
    colListMagzine
        .peek()
        .add(
            new SelectItem(
                dbService,
                nextId,
                ctx.getText(),
                SelectSectionType.SELECT,
                null,
                null,
                null,
                null,
                Boolean.TRUE,
                null, // TODO - how/when?
                null,
                null, // Will be joined at SELECT level
                ctx.start.getLine(),
                ctx.start.getCharPositionInLine(),
                ctx.stop.getLine(),
                ctx.stop.getCharPositionInLine()));
    return super.visitSelect_list_asterisk(ctx);
  }

  @Override
  public Void visitSelect_list_no_asterisk(BingqlParser.Select_list_no_asteriskContext ctx) {
    // This is a new list_item
    String alias = Util.removeQuotes(MiscChores.extractRealAlias(ctx.select_list_alias()));
    int stopPt = (alias == null) ? 0 : ctx.select_list_alias().start.getStartIndex() - 1;
    String cleanedTxt = MiscChores.getOriginalText(ctx, stopPt, CONS_COL_DEF_MAXLEN);
    FunctionType funcType = null;
    Boolean isSimpleCol = Boolean.FALSE;
    String dataType = null;

    // Check known patterns - the depth to traverse is *very* grammar dependent!
    ParseTree firstLevelChildCtx = ctx.getChild(0);
    boolean patternFound = false;

    // func(a, b):
    if (!patternFound) {
      try {
        if (firstLevelChildCtx.getChildCount() == 1
            && isAFunction(firstLevelChildCtx.getChild(0))) {
          // Try to get the Function Type here
          FunctionAssesser funcEval = new FunctionAssesser(cleanedTxt);
          funcType = funcEval.getType();
          dataType = funcEval.getReturnType();
          patternFound = true;
        }
      } catch (Exception anyKind) {
        // Ignore
      }
    }

    // func(a, b).c
    if (!patternFound) {
      try {
        // This is a rather Hive problem - function returns a complex type
        // the func definition will be one level further down compared to just a func itself
        if (firstLevelChildCtx.getChildCount() == 3
            && isAFunction(firstLevelChildCtx.getChild(0).getChild(0))) {
          String structField = firstLevelChildCtx.getChild(2).getText();
          FunctionAssesser funcEval = new FunctionAssesser(cleanedTxt);
          dataType = funcEval.getComplexFieldType(structField);
          patternFound = true;
        }
      } catch (Exception anyKind) {
        // Ignore
      }
    }

    // a.b
    if (!patternFound) {
      try {
        // Is this a simple column reference? Then there should be one first grandchild and has to
        // be of ColNameInExprContext type
        if (firstLevelChildCtx.getChildCount() == 1
            && firstLevelChildCtx.getChild(0) instanceof BingqlParser.ColNameInExprContext) {
          isSimpleCol = Boolean.TRUE;
          patternFound = true;
        }
      } catch (Exception anyKind) {
        // Ignore
      }
    }

    Long nextId = AutoIncrement.nextId();
    SelectItem latestAdd =
        new SelectItem(
            dbService,
            nextId,
            cleanedTxt,
            SelectSectionType.SELECT,
            alias,
            null,
            null,
            null,
            isSimpleCol,
            dataType,
            funcType,
            null, // Will be joined at SELECT level
            ctx.start.getLine(),
            ctx.start.getCharPositionInLine(),
            ctx.stop.getLine(),
            ctx.stop.getCharPositionInLine());
    colListMagzine.peek().add(latestAdd);

    return super.visitSelect_list_no_asterisk(ctx);
  }

  @Override
  public Void visitWhere_clause(BingqlParser.Where_clauseContext ctx) {
    String cleanedTxt = MiscChores.getOriginalText(ctx.bool_expr(), 0, CONS_COL_DEF_MAXLEN);
    Long nextId = AutoIncrement.nextId();
    colListMagzine
        .peek()
        .add(
            new SelectItem(
                dbService,
                nextId,
                cleanedTxt,
                SelectSectionType.WHERE,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null, // Will be joined at SELECT level
                ctx.start.getLine(),
                ctx.start.getCharPositionInLine(),
                ctx.stop.getLine(),
                ctx.stop.getCharPositionInLine()));
    return super.visitWhere_clause(ctx);
  }

  @Override
  public Void visitFrom_join_clause(BingqlParser.From_join_clauseContext ctx) {
    if (ctx.bool_expr() == null) {
      // The JOIN conditions are represented in the WHERE clause in this case (i.e. a comma
      // separated table list)
      return super.visitFrom_join_clause(ctx);
    }

    String cleanedTxt = MiscChores.getOriginalText(ctx.bool_expr(), 0, CONS_COL_DEF_MAXLEN);
    Long nextId = AutoIncrement.nextId();
    colListMagzine
        .peek()
        .add(
            new SelectItem(
                dbService,
                nextId,
                cleanedTxt,
                SelectSectionType.JOIN,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null, // Will be joined at SELECT level
                ctx.start.getLine(),
                ctx.start.getCharPositionInLine(),
                ctx.stop.getLine(),
                ctx.stop.getCharPositionInLine()));
    return super.visitFrom_join_clause(ctx);
  }

  /**
   * Handles both tab.col1 and max(tab.col1, t1.col2) case - there may be more than one column in a
   * list_item
   */
  @Override
  public Void visitColNameInExpr(BingqlParser.ColNameInExprContext ctx) {
    // Unknown origin, ignore
    if (colListMagzine.empty()) {
      return super.visitColNameInExpr(ctx);
    }

    // Ignore ORDER BY, GROUP BY columns
    if (MiscChores.hasClassInAncestry(
        ctx,
        BingqlParser.Order_by_clauseContext.class,
        BingqlParser.Group_by_clauseContext.class)) {
      return super.visitColNameInExpr(ctx);
    }

    List<SelectItem> currSelList = colListMagzine.peek();
    String rawDef = ctx.ident().getText();

    // Find the latest list_item and attach the column there
    SelectItem lastItem = currSelList.get(currSelList.size() - 1);
    lastItem
        .getParentColumns()
        .add(
            new SelectItem(
                dbService,
                null,
                Util.extractCleanColName(rawDef),
                SelectSectionType.SELECT,
                null,
                null,
                null,
                null,
                Boolean.TRUE,
                null,
                null,
                null,
                ctx.start.getLine(),
                ctx.start.getCharPositionInLine(),
                ctx.stop.getLine(),
                ctx.stop.getCharPositionInLine()));
    return null;
  }

  @Override
  public Void visitGroup_by_columns(BingqlParser.Group_by_columnsContext ctx) {
    List<SelectItem> currSelList = colListMagzine.peek();
    for (ParseTree cnode : ctx.children) {
      if (!(cnode instanceof BingqlParser.ExprContext)) {
        continue;
      }
      String rawDef = cnode.getText();
      int fndIdx = -1;
      try {
        fndIdx = findSelectItemByIndex(currSelList, Integer.valueOf(rawDef));
      } catch (RuntimeException any) {
        fndIdx = findSelectItemByName(currSelList, Util.extractCleanColName(rawDef));
      }
      if (fndIdx > -1) {
        currGroupByCols.add(fndIdx);
      }
    }
    return super.visitGroup_by_columns(ctx);
  }

  @Override
  public Void visitExpr_func_param_any(BingqlParser.Expr_func_param_anyContext ctx) {
    // Unknown origin, ignore
    if (colListMagzine.empty()) {
      return super.visitExpr_func_param_any(ctx);
    }

    // Find the latest list_item and attach the column there
    List<SelectItem> currSelList = colListMagzine.peek();
    SelectItem lastItem = currSelList.get(currSelList.size() - 1);
    lastItem
        .getParentColumns()
        .add(
            new SelectItem(
                dbService,
                null,
                S_COL_ASTERISK,
                SelectSectionType.SELECT,
                null,
                null,
                null,
                null,
                Boolean.FALSE,
                null,
                null,
                null,
                ctx.start.getLine(),
                ctx.start.getCharPositionInLine(),
                ctx.stop.getLine(),
                ctx.stop.getCharPositionInLine()));

    return null;
  }

  @Override
  public Void visitExpr(BingqlParser.ExprContext ctx) {
    Void retval = super.visitExpr(ctx);
    // Specifically to deal with the function(a, b).col1 situation
    // This is delaying schema resolution at runtime. fk stupid
    if (ctx.children.size() == 3 && ctx.getChild(1).getText().equals(".")) {
      if (startsWithAFunction(ctx) || matchesArrayOrMap(ctx)) {
        String compoStr = Util.extractCleanColName(ctx.getChild(2).getText());
        if (compoStr != null) {
          List<SelectItem> currSelList = colListMagzine.peek();
          SelectItem lastItem = currSelList.get(currSelList.size() - 1);
          Iterator<SelectItem> parentCols = lastItem.getParentColumns().iterator();
          while (parentCols.hasNext()) {
            SelectItem nextCol = parentCols.next();
            if (compoStr.equalsIgnoreCase(nextCol.getDefinition())
                || compoStr.equalsIgnoreCase(nextCol.getName())) {
              parentCols.remove();
            }
          }
        }
      }
    }
    return retval;
  }

  /**
   * Select * from tab (or cte) as tab0 => pick out schema name, table nanme and alias. And build
   * the Dataset
   */
  @Override
  public Void visitFrom_table_name_clause(BingqlParser.From_table_name_clauseContext ctx) {
    super.visitFrom_table_name_clause(ctx);
    TableNameWrangler tabNamer =
        new TableNameWrangler(ctx.getChild(0).getText(), jobContext.getCurrentSchema());

    String alias = MiscChores.extractRealAlias(ctx.from_alias_clause());
    try {
      DatasetWrapper extTable = findTableInCacheOrPhysicalSrc(tabNamer, alias);
      dsListMagzine.peek().add(extTable);
      if (alias != null) {
        List<DatasetWrapper> topList = dsListMagzine.peek();
        ((DatasetList) topList).lastItem().alias = alias;
      }
    } catch (MetaDataNotFound noSrc) {
      // If this is a Dashboard query, it is very likely the source is a temporary table
      // not in Hive metadata, or a source table outside of known lineage domain, so ignore:
      if (!jobContext.getSystemSource().equalsIgnoreCase(SYS_DASHBOARD)) {
        throw noSrc;
      } else {
        logger.warn(String.format("Found error in a Dashboard query: %s", noSrc.getMessage()));
      }
    }
    return null;
  }

  /** Select * from (subquery) as tab0 => assign tab0 to the dataset for the subquery */
  @Override
  public Void visitFrom_subselect_clause(BingqlParser.From_subselect_clauseContext ctx) {
    super.visitFrom_subselect_clause(ctx);
    String alias = MiscChores.extractRealAlias(ctx.from_alias_clause());
    if (alias != null) {
      List<DatasetWrapper> topList = dsListMagzine.peek();
      ((DatasetList) topList).lastItem().alias = alias;
    }
    return null;
  }

  @Override
  public Void visitFrom_cross_join_clause(BingqlParser.From_cross_join_clauseContext ctx) {
    super.visitFrom_cross_join_clause(ctx);
    try {
      List<DatasetWrapper> topList = dsListMagzine.peek();
      ((DatasetList) topList).lastItem().alias = ctx.ident().getText();
    } catch (NullPointerException npe) {
      // Ignore
    }
    return null;
  }

  @Override
  public Void visitCte_select_stmt_item(BingqlParser.Cte_select_stmt_itemContext ctx) {
    dsListMagzine.push(new DatasetList());
    super.visitCte_select_stmt_item(ctx);
    String cteName = ctx.ident().getText();

    assertTrue(dsListMagzine.peek().size() == 1);
    Dataset cteDs = dsListMagzine.pop().lastItem().dsObj;
    cteDs.setType(DatasetType.CTE);
    cteDs.setDefinedName(cteName);
    cteDs.saveToDb();
    cteCache.put(cteName, cteDs);

    return null;
  }

  @Override
  public Void visitFullselect_set_clause(BingqlParser.Fullselect_set_clauseContext ctx) {
    super.visitFullselect_set_clause(ctx);
    String rawTxt = ctx.getChild(0).getText();
    if (rawTxt != null && rawTxt.equalsIgnoreCase("UNION")) {
      dsListMagzine.peek().lastItem().dsObj.setIsUnionBase(true);
    }
    return null;
  }

  @Override
  public Void visitMultitable_insert_stmt(BingqlParser.Multitable_insert_stmtContext ctx) {
    // Setup stack context since in Hive multitable insert is a combo of SELECT and INSERT rules
    // The top level dslistMagzine entry captures a chain of sub-component INSERTs
    isMultiTableInsert = true;
    dsListMagzine.push(new DatasetList());

    super.visitMultitable_insert_stmt(ctx);
    isMultiTableInsert = false;

    // Because there can be multiple INSERT SELECT blocks, no one is responsible for removing
    // the last dataset chain:
    dsListMagzine.pop();

    return null;
  }

  @Override
  public Void visitInsert_stmt(BingqlParser.Insert_stmtContext ctx) {
    super.visitInsert_stmt(ctx);

    // If this is INSERT ... VALUES ..., skip:
    if (ctx.insert_stmt_rows() != null) {
      return null;
    }

    BingqlParser.Table_nameContext tabNameCtx = ctx.table_name();
    assertNotNull(tabNameCtx);

    // Find out table name and breakdown into components:
    TableNameWrangler tabNamer =
        new TableNameWrangler(ctx.table_name().getText(), jobContext.getCurrentSchema());

    // Find out INSERT type here:
    int startPt = ctx.start.getStartIndex();
    int endPt = tabNameCtx.start.getStartIndex() - 1;
    Interval txtIntv = new Interval(startPt, endPt);
    String insertType = ctx.start.getInputStream().getText(txtIntv);

    processInsertBySelect(tabNamer, insertType);

    return null;
  }

  @Override
  public Void visitCreate_table_stmt(BingqlParser.Create_table_stmtContext ctx) {
    /**
     * CREATE TABLE should have already been executed, meaning the table should have existed either
     * in Cache or in Metadata. Therefore, will return if it is already found in either source. In
     * the rare case the table does not exist -- create the structure in Cache only, assuming the
     * job will run sooner or later and the Metadata store will be updated.
     *
     * <p>The only complicating factor is TEMPORARY TABLE In this case, will create the tmp table in
     * appropriate cache
     *
     * <p>In both cases, CREATE AS SELECT will be treated as CREATE then INSERT
     */
    TableNameWrangler tabNamer =
        new TableNameWrangler(ctx.table_name().getText(), jobContext.getCurrentSchema());
    String tabDb = tabNamer.getDbName(true);
    String tabName = tabNamer.getTabName();

    // Is this a temporary table? If so needs to handle differently
    boolean isTmpTab = (ctx.getChild(1).getText().equalsIgnoreCase("temporary"));

    boolean hasInsert = (ctx.create_table_as() != null);

    boolean objExists =
        !droppedTables.contains(tabNamer.getFullName())
            && dbService.getMetaDataService().tableExists(tabDb, tabName);

    Map<String, Dataset> targetCache = dsCache;
    DatasetType tabType = DatasetType.TABLE;

    if (isTmpTab) {
      targetCache = tmptblCache;
      tabType = DatasetType.TEMPORARY_TABLE;
    }

    boolean run_rest = false;
    if (!objExists || isTmpTab) {
      // Step 1: add a Table
      Long nextId = AutoIncrement.nextId();
      DatasetWrapper newVal = new DatasetWrapper();
      newlyCreatedTable =
          new Dataset(
              dbService,
              nextId,
              tabType,
              tabDb,
              tabName,
              ctx.start.getLine(),
              ctx.start.getCharPositionInLine(),
              ctx.stop.getLine(),
              ctx.stop.getCharPositionInLine(),
              Boolean.FALSE);
      newVal.dsObj = newlyCreatedTable;
      newVal.alias = null;

      // Allow the table creation statements to add columns to the target table/ds:
      // Only create_table_definition will use it at this point:
      super.visitCreate_table_stmt(ctx);
      run_rest = true;

      // Step 2: add columns
      // If it is simple create - take this route
      if (ctx.create_table_definition() != null) {
        assertTrue(newlyCreatedTable.columnList().size() > 0);
      }

      // Else if CREATE new LIKE existing -- take this route
      else if (ctx.create_table_like() != null) {
        TableNameWrangler cpFromNamer =
            new TableNameWrangler(
                ctx.create_table_like().table_name().getText(), jobContext.getCurrentSchema());
        String cpFromDb = cpFromNamer.getDbName(true);
        String cpFromTbl = cpFromNamer.getTabName();
        Dataset srcDs = dbService.locateExternalTableInLocalDb(cpFromDb, cpFromTbl);
        assertNotNull(srcDs);

        // Assuming we have identified the target/external table/ds:
        // Clone the columns from the external table than assign them to the newly created
        // DS/table:
        for (SelectItem currItem : srcDs.columnList()) {
          SelectItem newItem =
              new SelectItem(
                  dbService, AutoIncrement.nextId(), newlyCreatedTable.getId(), currItem);
          newItem.setMapToSchema(tabDb);
          newItem.setMapToTable(tabName);
          newlyCreatedTable.columnList().add(newItem);
        }
      }

      // Else if CREATE new AS SELECT -- take this route
      else if (hasInsert) {
        // Similar to insert_stmt:
        DatasetList topOfStack = dsListMagzine.peek();
        assert (topOfStack.size() == 1);

        Dataset insertSrc = topOfStack.lastItem().dsObj;
        insertSrc.setDefinedName(tabName);
        List<SelectItem> srcItems = insertSrc.columnList();
        List<SelectItem> newTabCols = new ArrayList<>();
        for (SelectItem currItem : srcItems) {
          if (currItem.getUsageContext() == SelectSectionType.SELECT) {
            newTabCols.add(
                new SelectItem(
                    dbService,
                    AutoIncrement.nextId(),
                    currItem.knownAs(),
                    SelectSectionType.SELECT,
                    null,
                    tabDb,
                    tabName,
                    currItem.knownAs(),
                    Boolean.TRUE,
                    currItem.getDataType(),
                    null,
                    newlyCreatedTable.getId(),
                    currItem.getStartLine(),
                    currItem.getStartPos(),
                    currItem.getEndLine(),
                    currItem.getEndPos()));
          }
        }
        newlyCreatedTable.setColumnList(newTabCols);
      }

      newlyCreatedTable.saveToDb();
      targetCache.put(tabNamer.getFullName(), newlyCreatedTable);
      newlyCreatedTable = null;
    }

    if (hasInsert) {
      if (!run_rest) {
        // Need to proceed to parse the SELECT part in INSERT as SELECT. Otherwise
        // the next step will fail as there is no top-level Dataset
        super.visitCreate_table_stmt(ctx);
      }
      processInsertBySelect(tabNamer, tabType.name());
    }

    return null;
  }

  @Override
  public Void visitCreate_table_columns(BingqlParser.Create_table_columnsContext ctx) {
    if (newlyCreatedTable != null) {
      for (BingqlParser.Create_table_columns_itemContext currSubCtx :
          ctx.create_table_columns_item()) {
        String colName = Util.extractCleanColName(currSubCtx.column_name().ident().getText());
        String colType = null;

        if (currSubCtx.dtype() != null) {
          colType = currSubCtx.dtype().getText();
        } else if (currSubCtx.complex_dtypes() != null) {
          colType = MiscChores.getOriginalText(currSubCtx.complex_dtypes(), 0, 64);
        } else {
          throw new RuntimeException("UNEXPECTED DATA TYPE found in Create_table_columns");
        }
        newlyCreatedTable
            .columnList()
            .add(
                new SelectItem(
                    dbService,
                    AutoIncrement.nextId(),
                    colName,
                    SelectSectionType.SELECT,
                    null,
                    newlyCreatedTable.getMapToSchema(),
                    newlyCreatedTable.getMapToTable(),
                    colName,
                    Boolean.TRUE,
                    colType,
                    null,
                    newlyCreatedTable.getId(),
                    currSubCtx.start.getLine(),
                    currSubCtx.start.getCharPositionInLine(),
                    currSubCtx.stop.getLine(),
                    currSubCtx.stop.getCharPositionInLine()));
      }
    }
    return null;
  }

  @Override
  /**
   * lateral view something_func(params, *) alias1 as col1, col2, etc. It is like a new dataset with
   * alias1 as alias and select_items = (col1, col2) In this case, col1, col2 should have params, *
   * as their (shared) parents
   */
  public Void visitLateral_view_clause(BingqlParser.Lateral_view_clauseContext ctx) {
    List<BingqlParser.IdentContext> colNames = ctx.lv_columns().ident();
    List<SelectItem> newDsItems = new ArrayList<>();
    for (BingqlParser.IdentContext currIdCtx : colNames) {
      newDsItems.add(
          new SelectItem(
              dbService,
              AutoIncrement.nextId(),
              Util.removeQuotes(currIdCtx.getText()),
              SelectSectionType.SELECT,
              null,
              null,
              null,
              null,
              Boolean.TRUE,
              null,
              null,
              null,
              currIdCtx.start.getLine(),
              currIdCtx.start.getCharPositionInLine(),
              currIdCtx.stop.getLine(),
              currIdCtx.stop.getCharPositionInLine()));
    }

    // Lateral View is always attached to a previous JOIN. This put the last JOIN
    // in a container so the columns in the LV can be resolved accordingly
    DatasetWrapper lastDs = dsListMagzine.peek().lastItem();
    DatasetList lastDsList = new DatasetList();
    lastDsList.add(lastDs);

    // Need to collect parent column names (from the func_expr, e.g. DateRange(col_a))
    // in fact, all the LV columns share the same parents (as in col_a)
    colListMagzine.push(newDsItems);
    super.visitLateral_view_clause(ctx);
    colListMagzine.pop();

    // Now make the alias columns children of col_a:
    DatasetWrapper newLvDs =
        resolveSelectListFromDatasets(newDsItems, lastDsList, ctx.start, ctx.stop);
    newLvDs.dsObj.setType(DatasetType.LATERAL_VIEW);
    if (ctx.lv_alias() != null) {
      newLvDs.alias = ctx.lv_alias().getText();
    }
    dsListMagzine.peek().add(newLvDs);

    return null;
  }

  @Override
  public Void visitAlter_table_stmt(BingqlParser.Alter_table_stmtContext ctx) {
    // For now we only care that some alter table add partition will link two tables together
    // For other alter table statements ... we don't care
    if (ctx.alter_table_partition() == null || ctx.alter_table_partition().T_ADD2() == null) {
      return null;
    }

    TableNameWrangler tabNamer =
        new TableNameWrangler(ctx.table_name().getText(), jobContext.getCurrentSchema());
    newlyAlteredTable =
        dbService.locateExternalTableInLocalDb(tabNamer.getDbName(true), tabNamer.getTabName());
    assertNotNull(newlyAlteredTable);
    super.visitAlter_table_stmt(ctx);
    dbService.recordAlter(
        jobContext.getSqlSource().getId(),
        tabNamer.getDbName(true),
        tabNamer.getTabName(),
        newlyAlteredTable.getStorageLoc());
    newlyAlteredTable = null;
    return null;
  }

  @Override
  public Void visitTable_data_loc_clause(BingqlParser.Table_data_loc_clauseContext ctx) {
    super.visitTable_data_loc_clause(ctx);
    String locStr = Util.removeQuotes(ctx.L_S_STRING().toString());
    if (newlyCreatedTable != null) {
      newlyCreatedTable.setStorageLoc(locStr);
    } else if (newlyAlteredTable != null) {
      newlyAlteredTable.setStorageLoc(locStr);
    } else {
      logger.warn(
          "MetadataExtraction Logic Warning: did not find either a newlyCreatedTable "
              + "or a newlyAlteredTable for location: "
              + locStr);
    }
    return null;
  }

  @Override
  public Void visitDrop_table(BingqlParser.Drop_tableContext ctx) {
    TableNameWrangler tabNamer =
        new TableNameWrangler(ctx.table_name().getText(), jobContext.getCurrentSchema());
    dbService.removeTable(
        jobContext.getSqlSource().getId(), tabNamer.getDbName(true), tabNamer.getTabName());
    droppedTables.add(tabNamer.getFullName());
    return super.visitDrop_table(ctx);
  }

  private DatasetWrapper findTableInCacheOrPhysicalSrc(TableNameWrangler tabNamer, String alias) {
    // Look up current Cache if there is any match already:
    String shortName = tabNamer.getTabName(), fullName = tabNamer.getFullName();
    Dataset foundDs = null;

    // Is this a CTE candidate?
    if (tabNamer.getDbName(false) == null) {
      foundDs = cteCache.get(shortName);
    }

    // If not CTE, look in Table cache:
    if (foundDs == null
        && ((foundDs = tmptblCache.get(fullName)) != null
            || (foundDs = tmptblCache.get(shortName)) != null
            || (foundDs = dsCache.get(fullName)) != null
            || (foundDs = dsCache.get(shortName)) != null)) {}

    // If not in any Cache:
    if (foundDs == null) {
      // If not found in Cache, look for external data sources (e.g. Hive metadata store)
      foundDs =
          dbService.locateExternalTableInLocalDb(tabNamer.getDbName(true), tabNamer.getTabName());
      dsCache.put(fullName, foundDs);
    }
    assertNotNull(foundDs);

    DatasetWrapper newWrapper = new DatasetWrapper();
    newWrapper.dsObj = foundDs;
    newWrapper.alias = alias;

    return newWrapper;
  }

  private DatasetWrapper resolveSelectListFromDatasets(
      List<SelectItem> selectItems, List<DatasetWrapper> subDatasets, Token start, Token stop) {
    DatasetPool dataSetPool =
        new DatasetPool(dbService, jobContext.getSystemSource().equalsIgnoreCase(SYS_DASHBOARD));

    TreeSet<String> skipCols = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    for (DatasetWrapper currDs : subDatasets) {
      dataSetPool.add(currDs);
      Optional.ofNullable(currDs.skipColNameList).ifPresent(skipCols::addAll);
    }

    // TODO - P2: need to analyze whether there was a GROUP BY in the dataset

    // Now the selectItems becomes the Columns in the new Dataset
    Long nextId = AutoIncrement.nextId();
    Dataset newDs =
        new Dataset(
            dbService,
            nextId,
            DatasetType.SUBQUERY,
            null,
            null,
            start.getLine(),
            start.getCharPositionInLine(),
            stop.getLine(),
            stop.getCharPositionInLine(),
            null);
    List<SelectItem> selectCols = new ArrayList<>();
    List<SelectItem> filterCols = new ArrayList<>();
    List<SelectItem> joinCols = new ArrayList<>();
    for (SelectItem currTmpCol : selectItems) {
      // Remove known non-column names from the parent list, e.g. col1 in max_row(a, b).col1
      //      currTmpCol.getParentColumns().removeIf(ent -> skipCols.contains(ent.getDefinition()));
      if (SelectSectionType.WHERE.equals(currTmpCol.getUsageContext())) {
        // This is the list in the WHERE section
        List<SelectItem> whereColList =
            dataSetPool.resolveCurrentItem(currTmpCol, nextId, skipCols);
        // The way we handle WHERE is to put everything behind it in one token SelectItem
        if (whereColList != null && whereColList.size() > 0) {
          filterCols.addAll(whereColList.get(0).getParentColumns());
        }
      } else if (SelectSectionType.JOIN.equals(currTmpCol.getUsageContext())) {
        // This is the list in the JOIN section
        List<SelectItem> joinColList = dataSetPool.resolveCurrentItem(currTmpCol, nextId, skipCols);
        // The way we handle join is to put all base columns mentioned in the JOIN condition
        // behind one token SelectItem (the raw_text), then apply them all to every SelectItem after
        // SELECT
        if (joinColList != null && joinColList.size() > 0) {
          joinCols.addAll(joinColList.get(0).getParentColumns());
        }
      } else {
        // This is the list following SELECT keyword
        selectCols.addAll(dataSetPool.resolveCurrentItem(currTmpCol, nextId, skipCols));
      }
    }
    newDs.setColumnList(selectCols);
    newDs.setFilterColList(filterCols);
    newDs.setJoinColList(joinCols);

    dataSetPool.flush(newDs);

    DatasetWrapper retval = new DatasetWrapper();
    retval.dsObj = newDs;
    return retval;
  }

  private boolean isAFunction(ParseTree tNode) {
    return tNode instanceof BingqlParser.Expr_agg_window_funcContext
        || tNode instanceof BingqlParser.Expr_funcContext;
  }

  private boolean startsWithAFunction(BingqlParser.ExprContext ctx) {
    ParseTree leftNode = ctx;
    try {
      while ((leftNode = leftNode.getChild(0)) != null) {
        if (isAFunction(leftNode)) {
          return true;
        }
      }
    } catch (Exception ignore) {
    }
    return false;
  }

  private boolean matchesArrayOrMap(BingqlParser.ExprContext ctx) {
    ParseTree leftNode = ctx;
    try {
      while ((leftNode = leftNode.getChild(0)) != null && leftNode.getChildCount() == 2) {
        if (leftNode.getChild(1) instanceof BingqlParser.Expr_map_arrayContext) {
          return true;
        }
      }
    } catch (Exception ignore) {
    }
    return false;
  }

  private void matchInsertsWithActual(Dataset newDs, Dataset existingDs, String fullTabName) {
    List<SelectItem> newCols = newDs.columnList();
    List<SelectItem> existCols = existingDs.columnList();
    // As of now, the existCols may have Partition cols, "*" columns that is not in the New:
    if (newCols.size() > existCols.size()) {
      // Log error then move on
      logger.error(
          String.format(
              "ERROR - TABLE-MISMATCH: Table being inserted: %s, should not have more columns than what's found in "
                  + "metadata",
              fullTabName));
      return;
    }

    for (int idx = 0; idx < newCols.size(); idx++) {
      SelectItem currCol = newCols.get(idx);
      currCol.setMapToSchema(existingDs.getMapToSchema());
      currCol.setMapToTable(existingDs.getMapToTable());
      currCol.setMapToColumn(existCols.get(idx).getName());
    }
  }

  private void processInsertBySelect(TableNameWrangler tabNamer, String insertType) {
    String tabDb = tabNamer.getDbName(true);
    String tabName = tabNamer.getTabName();

    DatasetList topSelDs = dsListMagzine.peek();
    assert (topSelDs.size() == 1);

    DatasetWrapper targetTblWpr = findTableInCacheOrPhysicalSrc(tabNamer, null);
    assert (targetTblWpr.dsObj != null);

    if (targetTblWpr.dsObj.getType() == DatasetType.TEMPORARY_TABLE) {
      tmptblCache.put(tabNamer.getFullName(), topSelDs.lastItem().dsObj);
      topSelDs.lastItem().dsObj.saveToDb();

    } else if (targetTblWpr.dsObj.getType() == DatasetType.TABLE) {
      matchInsertsWithActual(topSelDs.lastItem().dsObj, targetTblWpr.dsObj, tabNamer.getFullName());
      topSelDs.lastItem().dsObj.saveToDb();

      dbService.recordInsert(
          topSelDs.lastItem().dsObj.getId(),
          jobContext.getSqlSource().getId(),
          tabDb,
          tabName,
          insertType.toUpperCase());
    }
  }

  // Find the column by name. Return its position in the SELECT list:
  private Integer findSelectItemByName(List<SelectItem> itemList, String cleanColName) {
    for (int itemListPos = 0; itemListPos < itemList.size(); itemListPos++) {
      SelectItem anItem = itemList.get(itemListPos);
      if (anItem.getUsageContext().equals(SelectSectionType.SELECT)) {
        if (nameMatchSelectItem(anItem, cleanColName)) {
          return itemListPos;
        }

        // This is fucked up man
        if (!anItem.getSimpleColumn()) {
          for (SelectItem pCol : anItem.getParentColumns()) {
            if (nameMatchSelectItem(pCol, cleanColName)) {
              return itemListPos;
            }
          }
        }
      }
    }
    // This is by no means an error. Just tracking for future consideration
    // Here are the scenarios I caught so far
    // The GB columns are in the PARTITION columns already
    // The GB column may be omitted in the SELECT list (Hive is OK with that)
    // The GB column is really complicated, nested function type. In this case, the column in the
    // SELECT only tracks
    // the full definition but GB has the intermediate verison:
    // e.g. select coalesce(func1(col1 ...)) ... group by func1(col1 ...)
    // In other words, not the raw/original column def in SELECT nor its parents list contains the
    // GB reference
    logger.debug(
        String.format(
            "GROUPBY-COL-NO-MATCH - GROUP BY item '%s' not in the current SELECT list",
            cleanColName));

    return -1;
  }

  // Find the column by index. Return its position in the SELECT list:
  private Integer findSelectItemByIndex(List<SelectItem> itemList, int refIndex) {
    if (refIndex == 0) {
      // Some old SQL allows to have 0 as a constant name in GROUP BY
      return -1;
    }

    int selTypeIdx = 0;
    for (int itemListPos = 0; itemListPos < itemList.size(); itemListPos++) {
      SelectItem anItem = itemList.get(itemListPos);
      if (anItem.getUsageContext().equals(SelectSectionType.SELECT)) {
        if (++selTypeIdx == refIndex) return itemListPos;
      }
    }
    return -1;
  }

  private void resolveGroupBy(List<SelectItem> itemList, HashSet<Integer> listIdxTracker) {
    for (int selIdx = 0; selIdx < itemList.size(); selIdx++) {
      SelectItem anItem = itemList.get(selIdx);
      if (anItem.getUsageContext().equals(SelectSectionType.SELECT)) {
        if (!listIdxTracker.contains(selIdx)) {
          // This is a SELECT item but not a group by, then add group by parents to this:
          for (Integer idx : listIdxTracker) {
            try {
              anItem.getParentColumns().addAll(itemList.get(idx).getParentColumns());
            } catch (ArrayIndexOutOfBoundsException aridx) {
              if (idx != -1) {
                // -1 case has been caught when matching returned -1
                logger.debug(
                    String.format(
                        "GROUP-BY-INDEX-ERR: Somehow GROUP BY matching generated an index %d out of bound",
                        idx));
              }
            }
          }
        }
      }
    }
    listIdxTracker.clear();
  }

  private boolean nameMatchSelectItem(SelectItem anItem, String name2Match) {
    if (name2Match.equalsIgnoreCase(anItem.getDefinition().trim())) {
      return true;
    }

    if (name2Match.equalsIgnoreCase(anItem.getName())
        || name2Match.equalsIgnoreCase(anItem.knownAs())) {
      return true;
    }

    // Now desperate:
    String pureV1 = anItem.getDefinition().replaceAll("\\s", "");
    String pureV2 = name2Match.replaceAll("\\s", "");

    return pureV1.equalsIgnoreCase(pureV2);
  }
}
