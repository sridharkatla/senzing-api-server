package com.senzing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.senzing.api.model.*;
import com.senzing.api.services.EntityDataServices;
import com.senzing.api.services.ServicesUtil;
import com.senzing.api.services.SzApiProvider;
import com.senzing.g2.engine.G2Engine;
import com.senzing.g2.engine.G2JNI;
import com.senzing.io.IOUtilities;
import com.senzing.repomgr.RepositoryManager;
import com.senzing.util.JsonUtils;
import com.senzing.util.Timers;

import javax.json.*;
import javax.websocket.Session;
import javax.ws.rs.ServerErrorException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;

import static com.senzing.api.model.SzAttributeSearchResultType.*;
import static com.senzing.api.model.SzAttributeSearchResultType.POSSIBLE_MATCH;
import static com.senzing.api.model.SzFeatureMode.REPRESENTATIVE;
import static com.senzing.api.model.SzFeatureMode.WITH_DUPLICATES;
import static com.senzing.api.model.SzHttpMethod.GET;
import static com.senzing.api.services.ServicesUtil.*;
import static com.senzing.g2.engine.G2Engine.*;
import static com.senzing.g2.engine.G2Engine.G2_EXPORT_INCLUDE_NAME_ONLY;
import static com.senzing.repomgr.RepositoryManager.*;

import java.io.*;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.URLEncoder;
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
  private static final SzRecordId MNO345 = new SzRecordId(EMPLOYEES,
                                                          "MNO345");
  private static final SzRecordId PQR678 = new SzRecordId(EMPLOYEES,
                                                          "PQR678");
  private static final SzRecordId STU901 = new SzRecordId(VIPS,
                                                          "STU901");
  private static final SzRecordId XYZ234 = new SzRecordId(VIPS,
                                                          "XYZ234");
  private static final SzRecordId ZYX321 = new SzRecordId(EMPLOYEES,
                                                          "ZYX321");
  private static final SzRecordId CBA654 = new SzRecordId(EMPLOYEES,
                                                          "CBA654");

  private static final SzRecordId BCD123 = new SzRecordId(MARRIAGES,
                                                          "BCD123");
  private static final SzRecordId CDE456 = new SzRecordId(MARRIAGES,
                                                          "CDE456");
  private static final SzRecordId EFG789 = new SzRecordId(MARRIAGES,
                                                          "EFG789");
  private static final SzRecordId FGH012 = new SzRecordId(MARRIAGES,
                                                          "FGH012");

  /**
   * The {@link Map} of {@link SzAttributeSearchResultType} keys to {@link
   * Integer} values representing the flags to apply.
   */
  private static final Map<SzAttributeSearchResultType, Integer>
      RESULT_TYPE_FLAG_MAP;

  static {
    Map<SzAttributeSearchResultType, Integer> map = new LinkedHashMap<>();
    map.put(MATCH, G2_EXPORT_INCLUDE_RESOLVED);
    map.put(POSSIBLE_MATCH, G2_EXPORT_INCLUDE_POSSIBLY_SAME);
    map.put(POSSIBLE_RELATION, G2_EXPORT_INCLUDE_POSSIBLY_RELATED);
    map.put(NAME_ONLY_MATCH, G2_EXPORT_INCLUDE_NAME_ONLY);
    RESULT_TYPE_FLAG_MAP = Collections.unmodifiableMap(map);
  }

  /**
   *
   */
  private static class SearchParameters {
    Map<String, Set<String>>          criteria;
    Set<SzAttributeSearchResultType>  includeOnlySet;
    Integer                           expectedCount;
    Boolean                           forceMinimal;
    SzFeatureMode                     featureMode;
    Boolean                           withFeatureStats;
    Boolean                           withInternalFeatures;
    Boolean                           withRelationships;
    Boolean                           withRaw;

    public SearchParameters(
        Map<String, Set<String>>          criteria,
        Set<SzAttributeSearchResultType>  includeOnlySet,
        Integer                           expectedCount,
        Boolean                           forceMinimal,
        SzFeatureMode                     featureMode,
        Boolean                           withFeatureStats,
        Boolean                           withInternalFeatures,
        Boolean                           withRelationships,
        Boolean                           withRaw)
    {
      this.criteria             = criteria;
      this.includeOnlySet       = includeOnlySet;
      this.expectedCount        = expectedCount;
      this.forceMinimal         = forceMinimal;
      this.featureMode          = featureMode;
      this.withFeatureStats     = withFeatureStats;
      this.withInternalFeatures = withInternalFeatures;
      this.withRelationships    = withRelationships;
      this.withRaw              = withRaw;
    }
  }

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
    File employeeFile = prepareEmployeeFile();
    File vipFile = prepareVipFile();
    File marriagesFile = prepareMariagesFile();

    employeeFile.deleteOnExit();
    passengerFile.deleteOnExit();
    vipFile.deleteOnExit();
    marriagesFile.deleteOnExit();
    passengerFile.deleteOnExit();

    RepositoryManager.loadFile(directory,
                               passengerFile,
                               PASSENGERS,
                               null,
                               true);

    RepositoryManager.loadFile(directory,
                               employeeFile,
                               EMPLOYEES,
                               null,
                               true);

    RepositoryManager.loadFile(directory,
                               vipFile,
                               VIPS,
                               null,
                               true);

    RepositoryManager.loadFile(directory,
                               marriagesFile,
                               MARRIAGES,
                               null,
                               true);
    RepositoryManager.conclude();

    G2Engine engineApi = new G2JNI();

    String initJson = null;
    File initFile = new File(directory, "g2-init.json");
    try (FileInputStream    fis = new FileInputStream(initFile);
         InputStreamReader  isr = new InputStreamReader(fis);
         BufferedReader     br  = new BufferedReader(isr))
    {
      initJson = IOUtilities.readFully(br);

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    engineApi.initV2("Sig Abort Test", initJson, false);
    engineApi.primeEngine();
    EntityDataServices entityDataServices = new EntityDataServices();
    Runnable runnable = () -> {
      List<SearchParameters> paramsList = searchParameters();
      for (SearchParameters params : paramsList) {
        searchByGetJsonAttrsTest(engineApi,
                                 params.criteria,
                                 params.includeOnlySet,
                                 params.expectedCount,
                                 params.forceMinimal,
                                 params.featureMode,
                                 params.withFeatureStats,
                                 params.withInternalFeatures,
                                 params.withRelationships,
                                 params.withRaw);
      }

    };
    Thread engineThread = new Thread(runnable);
    engineThread.start();
    try {
      engineThread.join();
    } catch (InterruptedException ignore) {
      ignore.printStackTrace();
    }
    engineApi.destroy();
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

  private static File prepareEmployeeFile() {
    String[] headers = {
        "RECORD_ID", "NAME_FIRST", "NAME_LAST", "PHONE_NUMBER", "ADDR_FULL",
        "DATE_OF_BIRTH","MOTHERS_MAIDEN_NAME", "SSN_NUMBER"};

    String[][] employees = {
        {MNO345.getRecordId(), "Joseph", "Schmoe", "702-555-1212",
            "101 Main Street, Las Vegas, NV 89101", "1981-01-12", "WILSON",
            "145-45-9866"},
        {PQR678.getRecordId(), "Jo Anne", "Smith", "212-555-1212",
            "101 Fifth Ave, Las Vegas, NV 10018", "1983-05-15", "JACOBS",
            "213-98-9374"},
        {ZYX321.getRecordId(), "Mark", "Hightower", "563-927-2833",
            "1882 Meadows Lane, Las Vegas, NV 89125", "1981-06-22", "JENKINS",
            "873-22-4213"},
        {CBA654.getRecordId(), "Mark", "Hightower", "781-332-2824",
            "2121 Roscoe Blvd, Los Angeles, CA 90232", "1980-09-09", "BROOKS",
            "827-27-4829"}
    };

    return prepareCSVFile("test-employees-", headers, employees);
  }

  private static File prepareVipFile() {
    String[] headers = {
        "RECORD_ID", "NAME_FIRST", "NAME_LAST", "PHONE_NUMBER", "ADDR_FULL",
        "DATE_OF_BIRTH","MOTHERS_MAIDEN_NAME"};

    String[][] vips = {
        {STU901.getRecordId(), "John", "Doe", "818-555-1313",
            "100 Main Street, Los Angeles, CA 90012", "1978-10-17", "GREEN"},
        {XYZ234.getRecordId(), "Jane", "Doe", "818-555-1212",
            "100 Main Street, Los Angeles, CA 90012", "1979-02-05", "GRAHAM"}
    };

    return prepareCSVFile("test-vips-", headers, vips);
  }

  private static File prepareMariagesFile() {
    String[] headers = {
        "RECORD_ID", "NAME_FULL", "AKA_NAME_FULL", "PHONE_NUMBER", "ADDR_FULL",
        "MARRIAGE_DATE", "DATE_OF_BIRTH", "GENDER", "RELATIONSHIP_TYPE",
        "RELATIONSHIP_ROLE", "RELATIONSHIP_KEY" };

    String[][] spouses = {
        {BCD123.getRecordId(), "Bruce Wayne", "Batman", "201-765-3451",
            "101 Wayne Manor Rd; Gotham City, NJ 07017", "2008-06-05",
            "1971-09-08", "M", "SPOUSE", "HUSBAND",
            relationshipKey(BCD123, CDE456)},
        {CDE456.getRecordId(), "Selina Kyle", "Catwoman", "201-875-2314",
            "101 Wayne Manor Rd; Gotham City, NJ 07017", "2008-06-05",
            "1981-12-05", "F", "SPOUSE", "WIFE",
            relationshipKey(BCD123, CDE456)},
        {EFG789.getRecordId(), "Barry Allen", "The Flash", "330-982-2133",
            "1201 Main Street; Star City, OH 44308", "2014-11-07",
            "1986-03-04", "M", "SPOUSE", "HUSBAND",
            relationshipKey(EFG789, FGH012)},
        {FGH012.getRecordId(), "Iris West-Allen", "", "330-675-1231",
            "1201 Main Street; Star City, OH 44308", "2014-11-07",
            "1986-05-14", "F", "SPOUSE", "WIFE",
            relationshipKey(EFG789, FGH012)}
    };

    return prepareCSVFile("test-marriages-", headers, spouses);
  }

  private static String relationshipKey(SzRecordId recordId1,
                                        SzRecordId recordId2) {
    String rec1 = recordId1.getRecordId();
    String rec2 = recordId2.getRecordId();
    if (rec1.compareTo(rec2) <= 0) {
      return rec1 + "|" + rec2;
    } else {
      return rec2 + "|" + rec1;
    }
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

  private static class Criterion {
    private String key;
    private Set<String> values;

    private Criterion(String key, String... values) {
      this.key = key;
      this.values = new LinkedHashSet<>();
      for (String value : values) {
        this.values.add(value);
      }
    }
  }

  private static Criterion criterion(String key, String... values) {
    return new Criterion(key, values);
  }

  private static Map<String, Set<String>> criteria(String key, String... values) {
    Criterion criterion = criterion(key, values);
    return criteria(criterion);
  }

  private static Map<String, Set<String>> criteria(Criterion... criteria) {
    Map<String, Set<String>> result = new LinkedHashMap<>();
    for (Criterion criterion : criteria) {
      Set<String> values = result.get(criterion.key);
      if (values == null) {
        result.put(criterion.key, criterion.values);
      } else {
        values.addAll(criterion.values);
      }
    }
    return result;
  }

  private static List<SearchParameters> searchParameters() {
    boolean supportFiltering = true;

    Map<Map<String, Set<String>>, Map<SzAttributeSearchResultType, Integer>>
        searchCountMap = new LinkedHashMap<>();

    searchCountMap.put(criteria("PHONE_NUMBER", "702-555-1212"),
                       sortedMap(Map.of(POSSIBLE_RELATION, 1)));

    searchCountMap.put(criteria("PHONE_NUMBER", "212-555-1212"),
                       sortedMap(Map.of(POSSIBLE_RELATION, 1)));

    searchCountMap.put(criteria("PHONE_NUMBER", "818-555-1313"),
                       sortedMap(Map.of(POSSIBLE_RELATION, 1)));

    searchCountMap.put(criteria("PHONE_NUMBER", "818-555-1212"),
                       sortedMap(Map.of(POSSIBLE_RELATION, 1)));

    searchCountMap.put(
        criteria("PHONE_NUMBER", "818-555-1212", "818-555-1313"),
        sortedMap(Map.of(POSSIBLE_RELATION, 2)));

    searchCountMap.put(
        criteria(criterion("ADDR_LINE1", "100 MAIN STREET"),
                 criterion("ADDR_CITY", "LOS ANGELES"),
                 criterion("ADDR_STATE", "CALIFORNIA"),
                 criterion("ADDR_POSTAL_CODE", "90012")),
        sortedMap(Map.of(POSSIBLE_RELATION, 2)));

    searchCountMap.put(
        criteria(criterion("NAME_FULL", "JOHN DOE", "JANE DOE"),
                 criterion("ADDR_LINE1", "100 MAIN STREET"),
                 criterion("ADDR_CITY", "LOS ANGELES"),
                 criterion("ADDR_STATE", "CALIFORNIA"),
                 criterion("ADDR_POSTAL_CODE", "90012")),
        sortedMap(Map.of(MATCH, 2)));

    searchCountMap.put(
        criteria(criterion("NAME_FULL", "JOHN DOE"),
                 criterion("ADDR_LINE1", "100 MAIN STREET"),
                 criterion("ADDR_CITY", "LOS ANGELES"),
                 criterion("ADDR_STATE", "CALIFORNIA"),
                 criterion("ADDR_POSTAL_CODE", "90012")),
        sortedMap(Map.of(MATCH, 1, POSSIBLE_RELATION, 1)));

    searchCountMap.put(
        criteria(criterion("NAME_FULL", "Mark Hightower"),
                 criterion("PHONE_NUMBER", "563-927-2833")),
        sortedMap(Map.of(MATCH, 1, NAME_ONLY_MATCH, 1)));

    searchCountMap.put(
        criteria(criterion("NAME_FULL", "Mark Hightower"),
                 criterion("DATE_OF_BIRTH", "1981-03-22")),
        sortedMap(Map.of(POSSIBLE_MATCH, 1)));

    searchCountMap.put(
        criteria(criterion("NAME_FULL", "Mark Hightower"),
                 criterion("PHONE_NUMBER", "563-927-2833"),
                 criterion("PHONE_NUMBER", "781-332-2824"),
                 criterion("DATE_OF_BIRTH", "1981-06-22")),
        sortedMap(Map.of(MATCH, 1, POSSIBLE_MATCH, 1)));

    List<SearchParameters> list = new LinkedList<>();

    List<SzFeatureMode> featureModes = new LinkedList<>();
    featureModes.add(null);
    for (SzFeatureMode featureMode : SzFeatureMode.values()) {
      featureModes.add(featureMode);
    }

    List<List<Boolean>> booleanVariants = getBooleanVariants(5);
    Iterator<List<Boolean>> booleanIter
        = circularIterator(booleanVariants);

    int numerator   = Math.max(booleanVariants.size(),
                               searchCountMap.size() * featureModes.size());
    int denominator = Math.min(booleanVariants.size(),
                               searchCountMap.size() * featureModes.size());
    int loopCount = ((numerator * 2) / denominator) + 1;

    searchCountMap.entrySet().forEach(entry -> {
      Map<String, Set<String>> criteria = entry.getKey();
      Map<SzAttributeSearchResultType, Integer> resultCounts = entry.getValue();

      List<Set<SzAttributeSearchResultType>> typeSetList = new ArrayList<>(20);
      List<Integer> countList = new ArrayList<>(20);

      int sum = resultCounts.values().stream()
          .reduce(0, Integer::sum);

      typeSetList.add(Collections.emptySet());
      countList.add(sum);

      // iterate over the result types and try singleton sets
      resultCounts.forEach((resultType, count) -> {
        // handle the singleton set
        EnumSet<SzAttributeSearchResultType> singletonSet
            = EnumSet.of(resultType);

        typeSetList.add(singletonSet);
        countList.add(supportFiltering ? count : sum);
      });

      // try complemente sets
      EnumSet<SzAttributeSearchResultType> allEnumSet
          = EnumSet.copyOf(resultCounts.keySet());

      Set<SzAttributeSearchResultType> allSet
          = sortedSet(allEnumSet);

      // create the complement set
      EnumSet<SzAttributeSearchResultType> complementEnumSet
          = EnumSet.complementOf(allEnumSet);

      Set<SzAttributeSearchResultType> complementSet
          = sortedSet(complementEnumSet);

      if (complementSet.size() > 0) {
        typeSetList.add(complementSet);
        countList.add(supportFiltering ? 0 : sum);
      }

      // try sets of 2 if we have heterogeneous result types
      if (resultCounts.size() > 1) {
        // try sets of two
        resultCounts.forEach((type1, count1) -> {
          resultCounts.forEach((type2, count2) -> {
            if (type1 != type2) {
              typeSetList.add(sortedSet(Set.of(type1, type2)));
              countList.add(supportFiltering ? (count1 + count2) : sum);
            }
          });
        });
      }

      // check for more than 2 result types
      if (allSet.size() > 2) {
        // try all result types specified individually
        typeSetList.add(allSet);
        countList.add(sum);
      }

      int typeSetIndex = 0;

      for (int index = 0; index < loopCount; index++) {
        for (SzFeatureMode featureMode : featureModes) {
          List<Boolean> booleanList = booleanIter.next();
          Boolean withRaw = booleanList.get(0);
          Boolean forceMinimal = booleanList.get(1);
          Boolean withRelationships = booleanList.get(2);
          Boolean withFeatureStats = booleanList.get(3);
          Boolean withInternalFeatures = booleanList.get(4);

          // try with empty set of result types
          list.add(new SearchParameters(criteria,
                                        typeSetList.get(typeSetIndex),
                                        countList.get(typeSetIndex),
                                        forceMinimal,
                                        featureMode,
                                        withFeatureStats,
                                        withInternalFeatures,
                                        withRelationships,
                                        withRaw));

          // increment the type set index
          typeSetIndex = (typeSetIndex + 1) % typeSetList.size();
        }
      }
    });

    return list;
  }

  private static <T extends Comparable<T>> Set<T> sortedSet(Set<T> set) {
    List<T> list = new ArrayList<>(set.size());
    list.addAll(set);
    Collections.sort(list);
    Set<T> sortedSet = new LinkedHashSet<>();
    sortedSet.addAll(list);
    return sortedSet;
  }

  private static <K extends Comparable<K>, V> Map<K, V> sortedMap(Map<K, V> map)
  {
    List<K> list = new ArrayList<>(map.size());
    list.addAll(map.keySet());
    Collections.sort(list);
    Map<K, V> sortedMap = new LinkedHashMap<>();
    for (K key: list) {
      sortedMap.put(key, map.get(key));
    }
    return sortedMap;
  }

  /**
   * Utility class for circular iteration.
   */
  private static class CircularIterator<T> implements Iterator<T> {
    private Collection<T> collection = null;
    private Iterator<T> iterator = null;
    private CircularIterator(Collection<T> collection) {
      this.collection = collection;
      this.iterator = this.collection.iterator();
    }
    public boolean hasNext() {
      return (this.collection.size() > 0);
    }
    public T next() {
      if (!this.iterator.hasNext()) {
        this.iterator = this.collection.iterator();
      }
      return this.iterator.next();
    }
    public void remove() {
      throw new UnsupportedOperationException(
          "Cannot remove from a circular iterator.");
    }
  }

  /**
   * Returns an iterator that iterates over the specified {@link Collection}
   * in a circular fashion.
   *
   * @param collection The {@link Collection} to iterate over.
   * @return The circular {@link Iterator}.
   */
  protected static <T> Iterator<T> circularIterator(Collection<T> collection) {
    return new CircularIterator<>(collection);
  }

  /**
   * Gets the variant possible values of booleans for the specified
   * parameter count.  This includes <tt>null</tt> values.
   *
   * @param paramCount The number of boolean parameters.
   * @return The {@link List} of parameter value lists.
   */
  protected static List<List<Boolean>> getBooleanVariants(int paramCount) {
    Boolean[] booleanValues = { null, true, false };
    int variantCount = (int) Math.ceil(Math.pow(3, paramCount));
    List<List<Boolean>> variants = new ArrayList<>(variantCount);

    // iterate over the variants
    for (int index1 = 0; index1 < variantCount; index1++) {
      // create the parameter list
      List<Boolean> params = new ArrayList<>(paramCount);

      // iterate over the parameter slots
      for (int index2 = 0; index2 < paramCount; index2++) {
        int repeat = (int) Math.ceil(Math.pow(3, index2));
        int valueIndex = ( index1 / repeat ) % booleanValues.length;
        params.add(booleanValues[valueIndex]);
      }

      // add the combinatorial variant
      variants.add(params);
    }
    return variants;
  }

  public static void searchByGetJsonAttrsTest(
      G2Engine                          engineApi,
      Map<String, Set<String>>          criteria,
      Set<SzAttributeSearchResultType>  includeOnlySet,
      Integer                           expectedCount,
      Boolean                           forceMinimal,
      SzFeatureMode                     featureMode,
      Boolean                           withFeatureStats,
      Boolean                           withInternalFeatures,
      Boolean                           withRelationships,
      Boolean                           withRaw)
  {
    JsonObjectBuilder job = Json.createObjectBuilder();
    criteria.entrySet().forEach(entry -> {
      String key = entry.getKey();
      Set<String> values = entry.getValue();
      if (values.size() == 0) return;
      if (values.size() == 1) {
        job.add(key, values.iterator().next());
      } else {
        JsonArrayBuilder jab = Json.createArrayBuilder();
        for (String value : values) {
          JsonObjectBuilder job2 = Json.createObjectBuilder();
          job2.add(key, value);
          jab.add(job2);
        }
        job.add(key, jab);
      }
    });
    String attrs = JsonUtils.toJsonText(job);

    StringBuilder sb = new StringBuilder();
    sb.append(formatServerUri(
        "entities?attrs=" + urlEncode(attrs)));
    if (includeOnlySet != null && includeOnlySet.size() > 0) {
      for (SzAttributeSearchResultType resultType: includeOnlySet) {
        sb.append("&includeOnly=").append(resultType);
      }
    }
    if (featureMode != null) {
      sb.append("&featureMode=").append(featureMode);
    }
    if (withFeatureStats != null) {
      sb.append("&withFeatureStats=").append(withFeatureStats);
    }
    if (withInternalFeatures != null) {
      sb.append("&withInternalFeatures=").append(withInternalFeatures);
    }
    if (forceMinimal != null) {
      sb.append("&forceMinimal=").append(forceMinimal);
    }
    if (withRelationships != null) {
      sb.append("&withRelationships=").append(withRelationships);
    }
    if (withRaw != null) {
      sb.append("&withRaw=").append(withRaw);
    }
    Set<String> includeOnlyParams = new LinkedHashSet<>();
    includeOnlySet.forEach(
        resultType -> includeOnlyParams.add(resultType.toString()));

    String uriText = sb.toString();
    UriInfo uriInfo = newProxyUriInfo(uriText);

    String result = searchEntitiesByGet(
        engineApi,
        attrs,
        null,
        includeOnlyParams,
        (forceMinimal != null ? forceMinimal : false),
        (featureMode != null ? featureMode : WITH_DUPLICATES),
        (withFeatureStats != null ? withFeatureStats : false),
        (withInternalFeatures != null ? withInternalFeatures : false),
        (withRelationships != null ? withRelationships : false),
        (withRaw != null ? withRaw : false),
        uriInfo);

  }

  /**
   * URL encodes the specified text using UTF-8 character encoding and traps
   * the {@link UnsupportedEncodingException} that is not actually possible with
   * UTF-8 encoding specified.
   *
   * @param text The text to encode.
   *
   * @return The URL-encoded text.
   */
  protected static String urlEncode(String text) {
    try {
      return URLEncoder.encode(text, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  private static String formatServerUri(String basePath) {
    StringBuilder sb = new StringBuilder();
    sb.append("http://localhost:2080");
    if (basePath.startsWith(sb.toString())) {
      sb.delete(0, sb.length());
      sb.append(basePath);
      return basePath;
    } else {
      sb.append("/" + basePath);
    }
    return sb.toString();
  }

  public static String searchEntitiesByGet(
      G2Engine            engineApi,
      String              attrs,
      List<String>        attrList,
      Set<String>         includeOnlySet,
      boolean             forceMinimal,
      SzFeatureMode       featureMode,
      boolean             withFeatureStats,
      boolean             withInternalFeatures,
      boolean             withRelationships,
      boolean             withRaw,
      UriInfo             uriInfo)
  {
    try {
      JsonObject searchCriteria = null;
      if (attrs != null && attrs.trim().length() > 0) {
        try {
          searchCriteria = JsonUtils.parseJsonObject(attrs);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      } else if (attrList != null && attrList.size() > 0) {
        Map<String, List<String>> attrMap = new LinkedHashMap<>();
        JsonObjectBuilder objBuilder = Json.createObjectBuilder();
        for (String attrParam : attrList) {
          // check for the colon
          int index = attrParam.indexOf(":");

          // if not found that is a problem
          if (index < 0) {
            throw new IllegalArgumentException(
                "The attr param value must be a colon-delimited string, "
                    + "but no colon character was found: " + attrParam);
          }
          if (index == 0) {
            throw new IllegalArgumentException(
                "The attr param value must contain a property name followed by "
                    + "a colon, but no property was provided before the colon: "
                    + attrParam);
          }

          // get the property name
          String propName = attrParam.substring(0, index);
          String propValue = "";
          if (index < attrParam.length() - 1) {
            propValue = attrParam.substring(index + 1);
          }

          // store in the map
          List<String> values = attrMap.get(propName);
          if (values == null) {
            values = new LinkedList<>();
            attrMap.put(propName, values);
          }
          values.add(propValue);
        }
        attrMap.entrySet().forEach(entry -> {
          String propName = entry.getKey();
          List<String> propValues = entry.getValue();
          if (propValues.size() == 1) {
            // add the attribute to the object builder
            objBuilder.add(propName, propValues.get(0));
          } else {
            JsonArrayBuilder jab = Json.createArrayBuilder();
            for (String propValue : propValues) {
              JsonObjectBuilder job = Json.createObjectBuilder();
              job.add(propName, propValue);
              jab.add(job);
            }
            objBuilder.add(propName + "_LIST", jab);
          }
        });
        searchCriteria = objBuilder.build();
      }

      // check if we have no attributes at all
      if (searchCriteria == null || searchCriteria.size() == 0) {
        throw new IllegalArgumentException(
            "At least one search criteria attribute must be provided via the "
                + "\"attrs\" or \"attr\" parameter.  attrs=[ " + attrs
                + " ], attrList=[ " + attrList + " ]");
      }

      // defer to the internal method
      return searchByAttributes(engineApi,
                                searchCriteria,
                                includeOnlySet,
                                forceMinimal,
                                featureMode,
                                withFeatureStats,
                                withInternalFeatures,
                                withRelationships,
                                withRaw,
                                uriInfo,
                                GET);

    } catch (ServerErrorException e) {
      e.printStackTrace();
      throw e;

    } catch (WebApplicationException e) {
      throw e;

    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  protected static String searchByAttributes(
      G2Engine            engineApi,
      JsonObject          searchCriteria,
      Set<String>         includeOnlySet,
      boolean             forceMinimal,
      SzFeatureMode       featureMode,
      boolean             withFeatureStats,
      boolean             withInternalFeatures,
      boolean             withRelationships,
      boolean             withRaw,
      UriInfo             uriInfo,
      SzHttpMethod        httpMethod)
  {
    try {
      // check for the include-only parameters, convert to result types
      if (includeOnlySet == null) includeOnlySet = Collections.emptySet();
      List<SzAttributeSearchResultType> resultTypes
          = new ArrayList<>(includeOnlySet.size());
      for (String includeOnly : includeOnlySet) {
        try {
          resultTypes.add(SzAttributeSearchResultType.valueOf(includeOnly));

        } catch (Exception e) {
          throw new IllegalArgumentException(
              "At least one of the includeOnly parameter values was not "
                  + "recognized: " + includeOnly);
        }
      }

      // augment the flags based on includeOnly parameter result types
      int includeFlags = 0;

      boolean supportFiltering = true;

      // only support the include flags on versions where it works
      if (supportFiltering) {
        for (SzAttributeSearchResultType resultType : resultTypes) {
          Integer flag = RESULT_TYPE_FLAG_MAP.get(resultType);
          if (flag == null) continue;
          includeFlags |= flag.intValue();
        }
      }

      // create the response buffer
      StringBuffer sb = new StringBuffer();

      // get the flags
      int flags = getFlags(includeFlags,
                           forceMinimal,
                           featureMode,
                           withFeatureStats,
                           withInternalFeatures,
                           withRelationships);

      // format the search JSON
      final String searchJson = JsonUtils.toJsonText(searchCriteria);

      System.err.println();
      System.err.println("CALLING SEARCH BY ATTRIBUTES....");
      System.err.println("    json  : " + searchJson);
      System.err.println("    flags : " + Integer.toBinaryString(flags));
      int result = engineApi.searchByAttributesV2(searchJson, flags, sb);
      System.err.println("CALLED SEARCH BY ATTRIBUTES.");
      if (result != 0) {
        throw new IllegalStateException(engineApi.getLastException());
      }

      // construct the response
      return sb.toString();

    } catch (ServerErrorException e) {
      e.printStackTrace();
      throw e;

    } catch (WebApplicationException e) {
      throw e;

    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  /**
   * Post-processes the search results according to the specified parameters.
   *
   * @param searchResults The {@link List} of {@link SzAttributeSearchResult}
   *                      to modify.
   *
   * @param forceMinimal Whether or not minimal format is forced.
   *
   * @param featureMode The {@link SzFeatureMode} describing how features
   *                    are retrieved.
   *
   * @param withRelationships Whether or not to include relationships.
   */
  private static void postProcessSearchResults(
      List<SzAttributeSearchResult>   searchResults,
      boolean                         forceMinimal,
      SzFeatureMode                   featureMode,
      boolean                         withRelationships)
  {
    // check if we need to strip out duplicate features
    if (featureMode == REPRESENTATIVE) {
      stripDuplicateFeatureValues(searchResults);
    }

    // check if fields are going to be null if they would otherwise be set
    if (featureMode == SzFeatureMode.NONE || forceMinimal) {
      setEntitiesPartial(searchResults);
    }
  }

  /**
   * Sets the partial flags for the resolved entity and related
   * entities in the {@link SzEntityData}.
   */
  private static void setEntitiesPartial(
      List<SzAttributeSearchResult> searchResults)
  {
    searchResults.forEach(e -> {
      e.setPartial(true);

      e.getRelatedEntities().forEach(e2 -> {
        e2.setPartial(true);
      });
    });
  }

  /**
   * Strips out duplicate feature values for each feature in the search
   * result entities of the specified {@link List} of {@link
   * SzAttributeSearchResult} instances.
   */
  private static void stripDuplicateFeatureValues(
      List<SzAttributeSearchResult> searchResults)
  {
    searchResults.forEach(e -> {
      stripDuplicateFeatureValues(e);

      e.getRelatedEntities().forEach(e2 -> {
        stripDuplicateFeatureValues(e2);
      });
    });
  }

  /**
   * Strips out duplicate feature values for each feature in the resolved
   * and related entities of the specified {@link SzEntityData}.
   */
  static void stripDuplicateFeatureValues(SzEntityData entityData) {
    stripDuplicateFeatureValues(entityData.getResolvedEntity());
    List<SzRelatedEntity> relatedEntities = entityData.getRelatedEntities();
    if (relatedEntities != null) {
      relatedEntities.forEach(e -> stripDuplicateFeatureValues(e));
    }
  }

  /**
   * Strips out duplicate feature values in the specified {@link
   * SzResolvedEntity}.
   */
  static void stripDuplicateFeatureValues(SzResolvedEntity entity) {
    Map<String, List<SzEntityFeature>> featureMap = entity.getFeatures();
    if (featureMap != null) {
      featureMap.values().forEach(list -> {
        list.forEach(f -> f.setDuplicateValues(null));
      });
    }
  }

  /**
   * Creates a proxy {@link UriInfo} to simulate a {@link UriInfo} when directly
   * calling the API services functions.
   *
   * @param selfLink The simulated request URI.
   *
   * @return The proxied {@link UriInfo} object using the specified URI.
   */
  protected static UriInfo newProxyUriInfo(String selfLink) {
    try {
      final URI uri = new URI(selfLink);

      InvocationHandler handler = (p, m, a) -> {
        if (m.getName().equals("getRequestUri")) {
          return uri;
        }
        throw new UnsupportedOperationException(
            "Operation not implemented on proxy UriInfo");
      };

      ClassLoader loader = SigAbortTest.class.getClassLoader();
      Class[] classes = {UriInfo.class};

      return (UriInfo) Proxy.newProxyInstance(loader, classes, handler);

    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
