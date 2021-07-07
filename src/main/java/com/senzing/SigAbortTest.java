package com.senzing;

import com.senzing.api.model.SzRecordId;
import com.senzing.repomgr.RepositoryManager;
import static com.senzing.repomgr.RepositoryManager.*;

import java.io.*;
import java.util.*;

public class SigAbortTest {
  private static final String PASSENGERS = "PASSENGERS";
  private static final String EMPLOYEES  = "EMPLOYEES";
  private static final String VIPS       = "VIPS";
  private static final String MARRIAGES  = "MARRIAGES";

  private static final SzRecordId ABC123 = new SzRecordId(PASSENGERS,
                                                          "ABC123");
  private static final SzRecordId DEF456 = new SzRecordId(PASSENGERS,
                                                          "DEF456");
  private static final SzRecordId GHI789 = new SzRecordId(PASSENGERS,
                                                          "GHI789");
  private static final SzRecordId JKL012 = new SzRecordId(PASSENGERS,
                                                          "JKL012");
  /**
   *
   */
  public static void main(String[] args) {
    if (args.length != 1) {
      System.err.println("Must specify the directory to use.");
      System.exit(1);
    }
    String  directoryName = args[0];
    File    directory     = new File(directoryName);
    if (directory.exists()) {
      System.err.println("Must specify a directory that does not exist: "
                             + directoryName);
      System.exit(1);
    }
    Configuration config = RepositoryManager.createRepo(directory, true);
    Set<String> dataSources = new LinkedHashSet<>();
    dataSources.add(PASSENGERS);
    dataSources.add(EMPLOYEES);
    dataSources.add(VIPS);
    dataSources.add(MARRIAGES);

    RepositoryManager.configSources(directory, dataSources, true);
    File passengerFile = preparePassengerFile();
    passengerFile.deleteOnExit();
    RepositoryManager.loadFile(directory,
                               passengerFile,
                               PASSENGERS,
                               null,
                               true);
  }

  private static File preparePassengerFile() {
    String[] headers = {
        "RECORD_ID", "NAME_FIRST", "NAME_LAST", "PHONE_NUMBER", "ADDR_FULL",
        "DATE_OF_BIRTH"};

    String[][] passengers = {
        {ABC123.getRecordId(), "Joe", "Schmoe", "702-555-1212",
            "101 Main Street, Las Vegas, NV 89101", "1981-01-12"},
        {DEF456.getRecordId(), "Joanne", "Smith", "212-555-1212",
            "101 Fifth Ave, Las Vegas, NV 10018", "1983-05-15"},
        {GHI789.getRecordId(), "John", "Doe", "818-555-1313",
            "100 Main Street, Los Angeles, CA 90012", "1978-10-17"},
        {JKL012.getRecordId(), "Jane", "Doe", "818-555-1212",
            "100 Main Street, Los Angeles, CA 90012", "1979-02-05"}
    };
    return prepareCSVFile("test-passengers-", headers, passengers);
  }

  /**
   * Creates a CSV temp file with the specified headers and records.
   *
   * @param filePrefix The prefix for the temp file name.
   * @param headers    The CSV headers.
   * @param records    The one or more records.
   * @return The {@link File} that was created.
   */
  protected static File prepareCSVFile(String       filePrefix,
                                       String[]     headers,
                                       String[]...  records)
  {
    // check the arguments
    int count = headers.length;
    for (int index = 0; index < records.length; index++) {
      String[] record = records[index];
      if (record.length != count) {
        throw new IllegalArgumentException(
            "The header and records do not all have the same number of "
                + "elements.  expected=[ " + count + " ], received=[ "
                + record.length + " ], index=[ " + index + " ]");
      }
    }

    try {
      File csvFile = File.createTempFile(filePrefix, ".csv");

      // populate the file as a CSV
      try (FileOutputStream fos = new FileOutputStream(csvFile);
           OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
           PrintWriter pw = new PrintWriter(osw)) {
        String prefix = "";
        for (String header : headers) {
          pw.print(prefix);
          pw.print(csvQuote(header));
          prefix = ",";
        }
        pw.println();
        pw.flush();

        for (String[] record : records) {
          prefix = "";
          for (String value : record) {
            pw.print(prefix);
            pw.print(csvQuote(value));
            prefix = ",";
          }
          pw.println();
          pw.flush();
        }
        pw.flush();

      }

      return csvFile;

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Quotes the specified text as a quoted string for a CSV value or header.
   *
   * @param text The text to be quoted.
   * @return The quoted text.
   */
  protected static String csvQuote(String text) {
    if (text.indexOf("\"") < 0 && text.indexOf("\\") < 0) {
      return "\"" + text + "\"";
    }
    char[] textChars = text.toCharArray();
    StringBuilder sb = new StringBuilder(text.length() * 2);
    for (char c : textChars) {
      if (c == '"' || c == '\\') {
        sb.append('\\');
      }
      sb.append(c);
    }
    return sb.toString();
  }

}
