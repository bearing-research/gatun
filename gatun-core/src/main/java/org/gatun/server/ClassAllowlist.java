package org.gatun.server;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * The classes a client may instantiate or call static members on.
 *
 * <p>A built-in list covers the JDK types the client itself relies on and the Spark stack. A
 * deployment adds its own libraries through two system properties, read once at startup:
 *
 * <ul>
 *   <li>{@code gatun.allowlist.classes}: comma-separated fully qualified class names
 *   <li>{@code gatun.allowlist.prefixes}: comma-separated package prefixes; every class in the
 *       package and its subpackages is allowed
 * </ul>
 *
 * <p>The Python launcher sets them from {@code connect(allowed_classes=..., allowed_prefixes=...)}.
 */
final class ClassAllowlist {
  static final String CLASSES_PROPERTY = "gatun.allowlist.classes";
  static final String PREFIXES_PROPERTY = "gatun.allowlist.prefixes";

  private static final Set<String> DEFAULT_CLASSES =
      Set.of(
          "java.util.ArrayList",
          "java.util.LinkedList",
          "java.util.HashMap",
          "java.util.LinkedHashMap",
          "java.util.HashSet",
          "java.util.LinkedHashSet",
          "java.util.TreeMap",
          "java.util.TreeSet",
          "java.util.Collections",
          "java.util.Arrays",
          "java.lang.String",
          "java.lang.Integer",
          "java.lang.Long",
          "java.lang.Double",
          "java.lang.Float",
          "java.lang.Boolean",
          "java.lang.Byte",
          "java.lang.Short",
          "java.lang.Character",
          "java.lang.Math",
          "java.lang.StringBuilder",
          "java.lang.StringBuffer",
          "java.lang.System", // For setting system properties (e.g., spark.master)
          "java.lang.Class", // For reflection-based array creation
          "java.lang.reflect.Array", // For array operations
          "java.sql.Timestamp", // For datetime conversion
          "java.sql.Date", // For date conversion
          "java.sql.Time"); // For time conversion

  private static final List<String> DEFAULT_PREFIXES =
      List.of(
          "org.apache.spark.", // Apache Spark
          "org.apache.log4j.", // Log4J (used by Spark)
          "scala." // Scala standard library
          );

  // A dotted Java name: identifiers separated by single dots ('$' covers nested classes)
  private static final Pattern QUALIFIED_NAME =
      Pattern.compile("[A-Za-z_$][A-Za-z0-9_$]*(\\.[A-Za-z_$][A-Za-z0-9_$]*)*");

  private final Set<String> classes;
  private final List<String> prefixes;

  ClassAllowlist(List<String> extraClasses, List<String> extraPrefixes) {
    Set<String> allClasses = new HashSet<>(DEFAULT_CLASSES);
    for (String className : extraClasses) {
      requireQualifiedName(className, CLASSES_PROPERTY);
      allClasses.add(className);
    }
    List<String> allPrefixes = new ArrayList<>(DEFAULT_PREFIXES);
    for (String prefix : extraPrefixes) {
      String normalized = normalizePrefix(prefix);
      if (!allPrefixes.contains(normalized)) {
        allPrefixes.add(normalized);
      }
    }
    this.classes = Set.copyOf(allClasses);
    this.prefixes = List.copyOf(allPrefixes);
  }

  /** The built-in list plus whatever the two system properties add. */
  static ClassAllowlist fromSystemProperties() {
    return new ClassAllowlist(
        parseList(System.getProperty(CLASSES_PROPERTY)),
        parseList(System.getProperty(PREFIXES_PROPERTY)));
  }

  boolean isAllowed(String className) {
    if (className == null) {
      return false;
    }
    if (classes.contains(className)) {
      return true;
    }
    for (String prefix : prefixes) {
      if (className.startsWith(prefix)) {
        return true;
      }
    }
    return false;
  }

  /** What was added to the built-in list, for the startup log. */
  String describeAdditions() {
    Set<String> extraClasses = new HashSet<>(classes);
    extraClasses.removeAll(DEFAULT_CLASSES);
    List<String> extraPrefixes = new ArrayList<>(prefixes);
    extraPrefixes.removeAll(DEFAULT_PREFIXES);
    return "classes=" + new java.util.TreeSet<>(extraClasses) + ", prefixes=" + extraPrefixes;
  }

  boolean hasAdditions() {
    return classes.size() > DEFAULT_CLASSES.size() || prefixes.size() > DEFAULT_PREFIXES.size();
  }

  private static List<String> parseList(String value) {
    List<String> items = new ArrayList<>();
    if (value == null) {
      return items;
    }
    for (String item : value.split(",")) {
      String trimmed = item.trim();
      if (!trimmed.isEmpty()) {
        items.add(trimmed);
      }
    }
    return items;
  }

  /**
   * Turn "com.example", "com.example." or "com.example.*" into "com.example.". The trailing dot is
   * what keeps the prefix "com.example" from also matching "com.examples.Thing".
   */
  static String normalizePrefix(String prefix) {
    String name = prefix;
    if (name.endsWith(".*")) {
      name = name.substring(0, name.length() - 2);
    } else if (name.endsWith(".")) {
      name = name.substring(0, name.length() - 1);
    }
    requireQualifiedName(name, PREFIXES_PROPERTY);
    return name + ".";
  }

  private static void requireQualifiedName(String name, String property) {
    if (!QUALIFIED_NAME.matcher(name).matches()) {
      throw new IllegalArgumentException(
          "Invalid entry in "
              + property
              + ": '"
              + name
              + "'. Expected a dotted Java name such as com.example.Thing (a class) or"
              + " com.example (a package prefix).");
    }
  }
}
