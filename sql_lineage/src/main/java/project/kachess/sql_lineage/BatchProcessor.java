package project.kachess.sql_lineage;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.FileConverter;
import project.kachess.shared.DbConnectionBuilder;
import project.kachess.shared.SyntaxErrorListener;
import project.kachess.sql_lineage.persistable.SqlDialectType;
import project.kachess.sql_lineage.util.AutoIncrement;
import project.kachess.sqlparser.g4generated.BingqlLexer;
import project.kachess.sqlparser.g4generated.BingqlParser;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.Arrays;

import static project.kachess.sql_lineage.SqlMetaDataExtractor.SYS_AIRFLOW;
import static org.junit.Assert.assertNotNull;

public class BatchProcessor {

  @Parameter(
      names = {"--input-dir", "-i"},
      converter = FileConverter.class,
      required = true,
      description = "The directory that has all the source, raw SQL files")
  protected File inputDir;

  @Parameter(
      names = {"--parse-to", "-o"},
      converter = FileConverter.class,
      required = true,
      description = "Move successfully parsed file to this directory")
  protected File outputDir;

  @Parameter(
      names = {"--skip-to", "-s"},
      converter = FileConverter.class,
      required = true,
      description = "Move failing-to-parse file to this directory")
  protected File skipDir;

  @Parameter(
      names = {"--file-ext", "-e"},
      description = "File extension to look for, default is .sql",
      required = false)
  protected String fileExt = ".sql";

  @Parameter(
      names = {"--auto-incr-seed", "-a"},
      description = "If not set, all IDs will start with the current timestamp epoch",
      required = false)
  protected long autoIncrmentSeed = 0;

  @Parameter(
      names = {"--reset", "-r"},
      description = "If set, will truncate the DB",
      required = false)
  protected boolean needToReset = false;

  @Parameter(
      names = {"--abort-on-err", "-x"},
      description = "If set, program stops on any error caught",
      required = false)
  protected boolean abortOnErr = false;

  @Parameter(
      names = {"--system-source"},
      description = "For example: airflow, explorer. Default is airflow for legacy reasons",
      required = false)
  protected String systemSource = "airflow";

  @Parameter(
      names = {"--sql-lang"},
      description = "For example: HIVE, SPARK or PRESTO. Default is HIVE for legacy reasons",
      required = false)
  protected String sqlLang = SqlDialectType.HIVE.name();

  @Parameter(
      names = {"--num-parallel"},
      description = "Default is 1",
      required = false)
  protected int numParallel = 1;


  private static final String SYS_AIRFLOW = SqlMetaDataExtractor.SYS_AIRFLOW;
  private static final String SYS_DASHBOARD = SqlMetaDataExtractor.SYS_DASHBOARD;
  static final String LANG_SPARK = "spark";
  private static final String[] SYS_SRC_VALUES = {SYS_AIRFLOW.toUpperCase(), SYS_DASHBOARD.toUpperCase()};

  static final Logger logger = LogManager.getLogger(BatchProcessor.class.getName());

  public static void main(String... argv) {
    BatchProcessor main = new BatchProcessor();
    JCommander.newBuilder().addObject(main).build().parse(argv);
    main.run();
  }

  public void run() {
    // Validating input params:
    try {
      sqlLang = sqlLang.toUpperCase();
      SqlDialectType.valueOf(sqlLang);
    } catch (IllegalArgumentException badName) {
      logger.error("Accepted --sql-lang values are " + Arrays.asList(SqlDialectType.values()));
      throw badName;
    }

    systemSource = systemSource.toUpperCase();
    if (!Arrays.asList(SYS_SRC_VALUES).contains(systemSource)) {
      throw new RuntimeException("Invalid --system-source parameter value");
    }

    // Set the starting point of auto-increment. Default (NULL) is current epoch
    AutoIncrement.setCounter((autoIncrmentSeed != 0) ? new Long(autoIncrmentSeed) : null);

    // Remove the Truncate Tables feature now that we are supporting multi-threading
    // External service has to take care of this before calling this module
    /*
    if (needToReset) {
      dbService.truncateDb();
    }
    */

    // Filter sub-directories using anonymous class
    File[] srcFiles =
        inputDir.listFiles(
            new FileFilter() {
              @Override
              public boolean accept(File fn) {
                return fn.getName().endsWith(fileExt);
              }
            });
    assertNotNull("Did not find any " + fileExt + " files from input directory " + inputDir.toString(), srcFiles);

    // Create routing destinations:
    if (!outputDir.exists()) {
      outputDir.mkdirs();
    }
    if (!skipDir.exists()) {
      skipDir.mkdirs();
    }

    int remainSize = srcFiles.length, remainCnt = numParallel, startIdx = 0;
    while (remainCnt > 0) {
      // Even flow -- to make the queue sizes as even as possible
      int chunk = (int) Math.round((double) remainSize / remainCnt);
      File[] fileQ = Arrays.copyOfRange(srcFiles, startIdx, Math.min(startIdx += chunk, srcFiles.length));
      logger.debug("Thread processing size: " + fileQ.length);

      Thread newTask = new Thread(new ParsingTask(this, fileQ));
      newTask.start();

      remainSize -= chunk;
      remainCnt--;
    }
  }
}

class ParsingTask implements Runnable {
  private MetaDataService hiveMetaStore;
  private OperationalInfoStore opsStore;
  private LineageDbService dbService;

  // Application settings that may be shared across all threads:
  private String systemSource;
  private String sqlLang;
  private File[] srcFiles;
  private File outputDir;
  private File skipDir;
  private String fileExt;
  private boolean abortOnErr;

  public ParsingTask(BatchProcessor appContext, File[] srcFiles) {
    this.systemSource = appContext.systemSource;
    this.sqlLang = appContext.sqlLang;
    this.srcFiles = srcFiles;
    this.outputDir = appContext.outputDir;
    this.skipDir = appContext.skipDir;
    this.fileExt = appContext.fileExt;
    this.abortOnErr = appContext.abortOnErr;

    DbConnectionBuilder urlBuilder;
    String connUrl;

    if (systemSource.equalsIgnoreCase(SYS_AIRFLOW)) {
      // Only apply to this case: added to assist local testing:
      urlBuilder = new DbConnectionBuilder("HMET_");
      if (urlBuilder.getSchema() == null) {
        urlBuilder.setSchema("hive");
      }
      // Pure legacy for run-in-prod setup
      connUrl = urlBuilder.getConnUrl(String.format(
          "jdbc:mysql://127.0.0.1:9876/hive?user=%s&password=%s",
          System.getenv("_host_usr"), System.getenv("ABDULLAH")));
      hiveMetaStore = new ExternalHiveMetaStore(connUrl);
    } else {
      // If this is to parse Dashboard SQL, use the local Hive Metadata:
      urlBuilder = new DbConnectionBuilder("LMET_");
      if (urlBuilder.getSchema() == null) {
        urlBuilder.setSchema("hive");
      }
      // Assuming this is Dashboard now:
      String mainMetlDbUrl = urlBuilder.getConnUrl("jdbc:mysql://localhost/bingql?user=dw_app");
      hiveMetaStore = new LocalHiveMetaStore(mainMetlDbUrl);
      //hiveMetaStore = new ExternalHiveMetaStore(mainMetlDbUrl);
    }

    urlBuilder = new DbConnectionBuilder("ARFL_");
    if (urlBuilder.getSchema() == null) {
      urlBuilder.setSchema("afdb");
    }
    connUrl = urlBuilder.getConnUrl("jdbc:mysql://localhost/afdb?user=dw_app");
    opsStore = new OperationalInfoStore(connUrl);

    urlBuilder = new DbConnectionBuilder("METL_");
    if (urlBuilder.getSchema() == null) {
      urlBuilder.setSchema("bingql");
    }
    connUrl = urlBuilder.getConnUrl("jdbc:mysql://localhost/bingql?user=dw_app");
    dbService = new LineageDbService(connUrl, hiveMetaStore);
    BatchProcessor.logger.debug(String.format("In %s Connection URL used is %s", this.getClass().getName(), connUrl));
  }

  public void run() {
    SqlMetaDataExtractor lineageBuilder = new SqlMetaDataExtractor();
    for (File currFile : srcFiles) {
      BatchProcessor.logger.info("Begin parsing " + currFile);

      // invokeParser
      try {
        long startTs = System.currentTimeMillis();

        JobContext newContext = inferContext(systemSource, sqlLang, currFile.getName());
        lineageBuilder.initSession(newContext, dbService);

        CharStream rawInput = CharStreams.fromFileName(currFile.getCanonicalPath());
        BingqlLexer lexer = new BingqlLexer(rawInput);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        BingqlParser parser = new BingqlParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(SyntaxErrorListener.INSTANCE);
        ParseTree exprTree = parser.program();

        // Start extracting metadata here:
        lineageBuilder.visit(exprTree);
        lineageBuilder.closeSession();

        // If successful, save to Parsed dir:
        currFile.renameTo(new File(outputDir.getCanonicalPath() + '/' + currFile.getName()));
        BatchProcessor.logger.info(
            String.format(
                "Successfully parsed %s. It took %d milliseconds. Moving to %s",
                currFile.getName(),
                (System.currentTimeMillis() - startTs),
                outputDir.getCanonicalPath()));
      } catch (IOException ioExp) {
        BatchProcessor.logger.error("Failed to process file " + currFile.getName(), ioExp);
        System.exit(1);
      } catch (Throwable parserGenErr) {
        BatchProcessor.logger.error(
            "FAILED to parse "
                + currFile.getName()
                + " due to "
                + parserGenErr.getClass().getSimpleName(), parserGenErr);
        currFile.renameTo(new File(skipDir.getAbsolutePath() + '/' + currFile.getName()));
        if (abortOnErr) {
          System.exit(1);
        }
      }
    }

    // Now cleaning up ...
    hiveMetaStore.cleanup();
    dbService.cleanup();
    opsStore.cleanup();

    BatchProcessor.logger.info("MAX ID value is " + AutoIncrement.nextId());
  }

  /**
   * Currently this is done via IF/ELSE, may be cleaner to separate into classes in the future If
   * systemSource is "airflow", the filename will be matched into DAG_ID.TASK_ID. If systemSource is
   * "dashboard", the filename will be the ID to construct dashboard URL.
   *
   *
   * @param systemSource
   * @param sqlLang
   * @param fn
   * @return
   */
  private JobContext inferContext(String systemSource, String sqlLang, String fn) {
    String srcLockKey, srcFn;
    if (systemSource.equalsIgnoreCase(SYS_AIRFLOW)) {
      srcLockKey = fn.substring(0, fn.lastIndexOf(fileExt));
      if (sqlLang.equalsIgnoreCase(BatchProcessor.LANG_SPARK)) {
        srcFn = opsStore.findAirflowSparkSqlSource(srcLockKey);
      } else {
        srcFn = opsStore.findAirflowHiveSqlSource(srcLockKey);
      }
    } else { // (systemSource.equalsIgnoreCase("dashboard"))
      srcLockKey = fn.substring(0, fn.length() - fileExt.length());
      srcFn = "https://analytics.tinyspeck.com/v2/dashboard/" + srcLockKey;
    }
    return new JobContext(dbService, systemSource, "adhoc", srcLockKey, srcFn, sqlLang, null);
  }
}