package org.gatun.server;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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

  // Instances of these types can load any class or run any code, whatever the list above says:
  // obj.getClass().getMethod(...).invoke(...) reaches java.lang.Runtime in three calls. Their
  // methods and fields are usable only when the type itself has been allowed.
  private static final List<Class<?>> RESTRICTED_TYPES =
      List.of(
          java.lang.reflect.AccessibleObject.class, // Method, Constructor, Field
          java.lang.invoke.MethodHandle.class,
          java.lang.invoke.VarHandle.class,
          java.lang.invoke.MethodHandles.Lookup.class,
          ClassLoader.class,
          Module.class,
          ModuleLayer.class,
          Runtime.class,
          ProcessBuilder.class,
          Process.class,
          ProcessHandle.class,
          Thread.class,
          ThreadGroup.class);

  // Members of allowed classes that stay refused: they end the server for every session, load
  // native code, or create an object of a class that was never checked.
  private static final Map<String, Set<String>> REFUSED_MEMBERS =
      Map.of(
          "java.lang.System", Set.of("exit", "load", "loadLibrary"),
          "java.lang.Class", Set.of("newInstance"));

  // A dotted Java name: identifiers separated by single dots ('$' covers nested classes)
  private static final Pattern QUALIFIED_NAME =
      Pattern.compile("[A-Za-z_$][A-Za-z0-9_$]*(\\.[A-Za-z_$][A-Za-z0-9_$]*)*");

  private final Set<String> classes;
  private final Set<String> dottedClasses; // the same names with '$' written as '.'
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
    Set<String> dotted = new HashSet<>();
    for (String className : allClasses) {
      dotted.add(className.replace('$', '.'));
    }
    this.dottedClasses = Set.copyOf(dotted);
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
    // A nested class may be named with a dot or with the '$' of its binary name
    if (classes.contains(className) || dottedClasses.contains(className.replace('$', '.'))) {
      return true;
    }
    for (String prefix : prefixes) {
      if (className.startsWith(prefix)) {
        return true;
      }
    }
    return false;
  }

  /** Refuse a static method or field that stays off limits even though its class is allowed. */
  void checkStaticMember(String className, String member) {
    Set<String> refused = REFUSED_MEMBERS.get(className);
    if (refused != null && refused.contains(member)) {
      throw new SecurityException(className + "." + member + " is not allowed");
    }
  }

  /**
   * Refuse a method call or field access on an object the allowlist has to cover even though no
   * class name was given: instance members are otherwise open, because the objects a permitted call
   * returns can be of any class.
   */
  void checkInstanceMember(Object target, String member) {
    Class<?> type = target.getClass();
    String restricted = restrictedTypes.get(type);
    if (!restricted.isEmpty()) {
      throw new SecurityException(
          "Objects of type "
              + type.getName()
              + " cannot be used from Python unless "
              + restricted
              + " is allowed. To allow it, pass allowed_classes to connect() or set the "
              + CLASSES_PROPERTY
              + " system property.");
    }
    checkStaticMember(type.getName(), member);
  }

  // The name to allow for a class whose instances are restricted, or "" if they are not. Cached
  // per class: this runs on every method call.
  private final ClassValue<String> restrictedTypes =
      new ClassValue<>() {
        @Override
        protected String computeValue(Class<?> type) {
          for (Class<?> restricted : RESTRICTED_TYPES) {
            if (restricted.isAssignableFrom(type)) {
              if (isAllowed(restricted.getName()) || isAllowed(type.getName())) {
                return "";
              }
              // A Method is allowed by its own name; a class loader or thread by the public
              // type, since the concrete class is often internal
              return restricted == java.lang.reflect.AccessibleObject.class
                  ? type.getName()
                  : restricted.getName();
            }
          }
          return "";
        }
      };

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
