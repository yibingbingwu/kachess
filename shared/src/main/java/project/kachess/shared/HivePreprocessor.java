package project.kachess.shared;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.FileConverter;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertNotNull;

/**
 * This class pre-process HQL script file by - Commenting out Hive configuration (i.e. set something
 * = someval;) - Commenting out other Admin commands (e.g. msck repair ...) - Replace Hive variables
 * (i.e. ${hiveconf:var})
 */
public class HivePreprocessor {
  @Parameter(
      names = {"--input-dir", "-i"},
      converter = FileConverter.class,
      required = true,
      description = "The directory that has all the source, raw SQL files")
  private File inputDir;

  @Parameter(
      names = {"--output-dir", "-o"},
      converter = FileConverter.class,
      required = true,
      description = "Where to write to after pre-process")
  private File outputDir;

  @Parameter(
      names = {"--file-ext", "-e"},
      description = "File extension to look for, default is .sql",
      required = false)
  private String fileExt = ".sql";

  public static void main(String... argv) {
    HivePreprocessor main = new HivePreprocessor();
    JCommander.newBuilder().addObject(main).build().parse(argv);
    main.run();
  }

  public void run() {
    try {
      //     Filter sub-directories using anonymous class
      File[] srcFiles =
          inputDir.listFiles(
              new FileFilter() {
                @Override
                public boolean accept(File fn) {
                  return fn.getName().endsWith(fileExt);
                }
              });
      assertNotNull(
          "Did not find any " + fileExt + " files from input directory " + inputDir.toString(),
          srcFiles);

      // Create output directory
      if (!outputDir.exists()) {
        outputDir.mkdirs();
      }

      for (File currFile : srcFiles) {
        //System.err.println("File is " + currFile.toString());
        byte[] preprocTxt = preprocessHiveFile(currFile);
        FileOutputStream fos =
            new FileOutputStream(outputDir.getAbsolutePath() + '/' + currFile.getName());
        fos.write(preprocTxt);
        fos.close();
      }
    } catch (Throwable anyErr) {
      anyErr.printStackTrace();
      System.exit(2);
    }
  }

  private static final Pattern assignStmtPtn =
      Pattern.compile("^\\s*set\\s+([a-zA-Z0-9\\.:_-]+)\\s*=\\s*(.+)(;|-- |).*$", Pattern.CASE_INSENSITIVE);
  private static final Pattern[] generalSkips = new Pattern[]{
      Pattern.compile("^add (jar|file) .*;", Pattern.CASE_INSENSITIVE)
      //, Pattern.compile("^create temporary macro .*", Pattern.CASE_INSENSITIVE)
      , Pattern.compile("^msck repair .*", Pattern.CASE_INSENSITIVE)};
  private static final String VAR_KW = "${hiveconf:";
  private static final int VAR_KW_LEN = VAR_KW.length();

  private final Map<String, String> confVals = new HashMap<>();

  private byte[] preprocessHiveFile(File currFile) throws IOException {
    BufferedReader reader = new BufferedReader(new FileReader(currFile));
    byte[] retval = processInput(reader);
    reader.close();
    return retval;
  }

  /**
   * This block of code used to be part of the main batch parser in the hope that the same byte[]
   * maybe used for Parsing as well. Now I decided to separate pre-process from the batch parsing It
   * is easier to do iterative debugging. For now keep the interface
   */
  public byte[] processInput(BufferedReader reader) throws IOException {
    ByteArrayOutputStream retval = new ByteArrayOutputStream(10 * 1024);
    String currLine = null;
    while ((currLine = reader.readLine()) != null) {
      String newLine = process(currLine) + "\n";
      retval.write(newLine.getBytes());
    }
    // Add an extra ';' to make parser work with those scripts that don't end with one
    retval.write(";\n".getBytes());
    return retval.toByteArray();
  }

  private String replaceHiveVars(String inStr, Map<String, String> varDict) {
    int sPos = inStr.toLowerCase().indexOf(VAR_KW);
    if (sPos >= 0) {
      StringBuffer retval = new StringBuffer();
      int ePos = inStr.substring(sPos).indexOf('}');
      String newKey = inStr.substring(sPos, sPos + ePos).substring(VAR_KW_LEN);
      String origVal = varDict.get(newKey);
      if (origVal == null) {
        // Allow certain variables to be skipped, e.g very complicated regex may break the parser
        return inStr;
      }

      retval
          .append(inStr.substring(0, sPos) + origVal)
          .append(replaceHiveVars(inStr.substring(sPos + ePos + 1), varDict));
      return retval.toString();
    } else {
      return inStr;
    }
  }

  private void parseHiveVar(String inStr, Map<String, String> varDict) {
    Matcher fndGrp = assignStmtPtn.matcher(inStr);
    while (fndGrp.find()) {
      String key = fndGrp.group(1).trim(), val = fndGrp.group(2).trim();
      varDict.put(key, cleanup(val));
    }
  }

  public String process(String currLine) {
    String cleanLine = currLine.trim(), newLine;

    for (Pattern askip : generalSkips) {
      if (askip.matcher(cleanLine).find()) {
        return "-- " + currLine;
      }
    }

    if (assignStmtPtn.matcher(cleanLine).find()) {
      String tmpLine = replaceHiveVars(currLine, confVals);
      parseHiveVar(tmpLine, confVals);
      newLine = "-- " + currLine;
    } else {
      // Next let's see if we need to replace ${hiveconf:var} to the stored value
      newLine = replaceHiveVars(currLine, confVals);
    }
    return newLine;
  }

  private static final String cleanup(String val) {
    // Remove the last ';' if any:
    StringBuilder retval = new StringBuilder(val);
    if (val.endsWith(";")) {
      retval.deleteCharAt(val.length() - 1);
    }

    // Remove the first instance of "-- " or ";"
    // e.g. set key = abc -- ; this is fkd or key=abc ; -- comment
    List<String> listOfEnds = Arrays.asList(new String[]{";", "-- "});
    listOfEnds.forEach(e -> {
      int pos = retval.indexOf(e);
      if (pos >= 0) {
        retval.delete(pos, retval.length());
      }
    });

    // If the block starts and ends with the same quote char, just remove and keep:
    String[] listOfQuotes = {"'", "\"", "`"};
    for (int idx = 0; idx < listOfQuotes.length; idx++) {
      String currVal = retval.toString().trim();
      String currQ = listOfQuotes[idx];
      if (currVal.startsWith(currQ) && currVal.endsWith(currQ)) {
        retval.delete(0, retval.length());
        retval.append(currVal.substring(1, currVal.length() - 1));
        break;
      }
    }

    return retval.toString().trim();
  }
}
