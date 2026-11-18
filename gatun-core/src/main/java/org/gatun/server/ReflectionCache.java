package org.gatun.server;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * Centralized cache for reflection lookups to avoid repeated reflection overhead.
 *
 * <p>This class caches:
 *
 * <ul>
 *   <li>Method lookups (with MethodHandle for fast invocation)
 *   <li>Constructor lookups
 *   <li>Field lookups (static and instance)
 *   <li>Class lookups by name (keyed by classloader for Spark compatibility)
 *   <li>No-arg methods as MethodHandles for the common case
 * </ul>
 *
 * <p><b>Security boundary:</b> Only PUBLIC methods and constructors are cached and callable. This
 * uses {@code getMethods()} and {@code getConstructors()} which return only public members. The
 * {@code setAccessible(true)} calls are solely for creating MethodHandles on public methods of
 * private inner classes (e.g., {@code ArrayList$Itr}), not for accessing private methods.
 *
 * <p><b>Cache sizing:</b> Signature-based caches (method, constructor, field) use bounded LRU with
 * configurable maximum size to prevent memory growth in long-running sessions (e.g., Spark).
 * Class-keyed caches use WeakHashMap to allow GC when classes are unloaded.
 *
 * <p>All caches are thread-safe.
 */
public final class ReflectionCache {
  private static final Logger LOG = Logger.getLogger(ReflectionCache.class.getName());

  // --- CACHE SIZE CONFIGURATION ---
  // Default max entries for signature-based caches (method, constructor)
  // Configurable via system property for tuning in large Spark jobs
  private static final int DEFAULT_MAX_SIGNATURE_CACHE_SIZE = 10_000;
  private static final int MAX_SIGNATURE_CACHE_SIZE =
      Integer.getInteger("gatun.cache.maxSignatureEntries", DEFAULT_MAX_SIGNATURE_CACHE_SIZE);

  // Default max entries for field caches (typically smaller)
  private static final int DEFAULT_MAX_FIELD_CACHE_SIZE = 2_000;
  private static final int MAX_FIELD_CACHE_SIZE =
      Integer.getInteger("gatun.cache.maxFieldEntries", DEFAULT_MAX_FIELD_CACHE_SIZE);

  // --- METHOD CACHE ---
  // Key: (class, methodName, argTypes) -> CachedMethod (method + MethodHandle + varargs info)
  // Bounded LRU to prevent memory growth with dynamic signatures
  private static final BoundedCache<MethodCacheKey, CachedMethod> methodCache =
      new BoundedCache<>(MAX_SIGNATURE_CACHE_SIZE);

  // --- CONSTRUCTOR CACHE ---
  // Key: (class, argTypes) -> CachedConstructor (constructor + varargs info)
  private static final BoundedCache<ConstructorCacheKey, CachedConstructor> constructorCache =
      new BoundedCache<>(MAX_SIGNATURE_CACHE_SIZE);

  // --- NO-ARG CONSTRUCTOR CACHE ---
  // Key: class -> no-arg Constructor
  // Uses WeakHashMap to allow GC when classes are unloaded
  private static final Map<Class<?>, java.lang.reflect.Constructor<?>> noArgConstructorCache =
      java.util.Collections.synchronizedMap(new java.util.WeakHashMap<>());

  // --- NO-ARG METHOD HANDLE CACHE ---
  // Uses a simple two-level map: Class -> methodName -> MethodHandle
  // Outer map uses WeakHashMap to allow GC when classes are unloaded
  private static final Map<Class<?>, Map<String, MethodHandle>> noArgMethodHandleCache =
      java.util.Collections.synchronizedMap(new java.util.WeakHashMap<>());

  // --- STATIC FIELD CACHE ---
  // Key: "className.fieldName" -> Field object
  private static final BoundedCache<String, java.lang.reflect.Field> staticFieldCache =
      new BoundedCache<>(MAX_FIELD_CACHE_SIZE);

  // --- INSTANCE FIELD CACHE ---
  // Key: "className.fieldName" -> Field object
  private static final BoundedCache<String, java.lang.reflect.Field> instanceFieldCache =
      new BoundedCache<>(MAX_FIELD_CACHE_SIZE);

  // --- VAR HANDLE CACHE ---
  // Key: "className.fieldName" -> VarHandle for faster field access
  private static final BoundedCache<String, VarHandle> varHandleCache =
      new BoundedCache<>(MAX_FIELD_CACHE_SIZE);

  // --- STATIC VAR HANDLE CACHE ---
  // Key: "className.fieldName" -> CachedStaticVarHandle with VarHandle + declaring class
  private static final BoundedCache<String, CachedStaticVarHandle> staticVarHandleCache =
      new BoundedCache<>(MAX_FIELD_CACHE_SIZE);

  // --- METHODS CACHE ---
  // Caches getMethods() results per class
  // Uses WeakHashMap to allow GC when classes are unloaded
  private static final Map<Class<?>, java.lang.reflect.Method[]> methodsCache =
      java.util.Collections.synchronizedMap(new java.util.WeakHashMap<>());

  // --- CONSTRUCTORS CACHE ---
  // Caches getConstructors() results per class
  // Uses WeakHashMap to allow GC when classes are unloaded
  private static final Map<Class<?>, java.lang.reflect.Constructor<?>[]> constructorsCache =
      java.util.Collections.synchronizedMap(new java.util.WeakHashMap<>());

  // --- CLASS CACHE ---
  // Caches Class.forName() results, keyed by (ClassLoader, className) to be classloader-safe.
  // Uses WeakHashMap for classloaders to avoid memory leaks when loaders are GC'd (e.g., Spark).
  // Inner map is ConcurrentHashMap for thread-safety within a loader.
  private static final Map<ClassLoader, Map<String, Class<?>>> classCache =
      java.util.Collections.synchronizedMap(new java.util.WeakHashMap<>());

  /** Lookup for converting Methods to MethodHandles. */
  public static final MethodHandles.Lookup METHOD_LOOKUP = MethodHandles.publicLookup();

  /** Empty Class array constant to avoid allocation. */
  public static final Class<?>[] EMPTY_CLASS_ARRAY = new Class<?>[0];

  /** Empty Object array constant to avoid allocation. */
  public static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];

  // Common JDK classes with no-arg constructors to pre-warm the cache
  private static final String[] PREWARM_CLASSES = {
    "java.util.ArrayList",
    "java.util.LinkedList",
    "java.util.HashMap",
    "java.util.LinkedHashMap",
    "java.util.HashSet",
    "java.util.LinkedHashSet",
    "java.util.TreeMap",
    "java.util.TreeSet",
    "java.lang.StringBuilder",
    "java.lang.StringBuffer"
  };

  // Static initializer to pre-warm constructor and method caches
  static {
    for (String className : PREWARM_CLASSES) {
      try {
        Class<?> clazz = Class.forName(className);
        // Pre-cache the no-arg constructor
        java.lang.reflect.Constructor<?> ctor = clazz.getDeclaredConstructor();
        noArgConstructorCache.put(clazz, ctor);
        // Pre-cache common no-arg methods as MethodHandles
        for (String methodName :
            new String[] {"size", "isEmpty", "toString", "hashCode", "clear"}) {
          try {
            java.lang.reflect.Method method = clazz.getMethod(methodName);
            MethodHandle handle = METHOD_LOOKUP.unreflect(method);
            noArgMethodHandleCache
                .computeIfAbsent(clazz, k -> new ConcurrentHashMap<>())
                .put(methodName, handle);
          } catch (NoSuchMethodException | IllegalAccessException e) {
            // Method doesn't exist on this class - skip
          }
        }
      } catch (ClassNotFoundException | NoSuchMethodException e) {
        LOG.fine("Failed to pre-warm cache for " + className + ": " + e.getMessage());
      }
    }
    LOG.fine("Pre-warmed constructor cache with " + noArgConstructorCache.size() + " entries");
  }

  private ReflectionCache() {} // Prevent instantiation

  // ========== NO-ARG CONSTRUCTOR ==========

  /** Get no-arg constructor for a class, using cache. */
  public static java.lang.reflect.Constructor<?> getNoArgConstructor(Class<?> clazz)
      throws NoSuchMethodException {
    java.lang.reflect.Constructor<?> ctor = noArgConstructorCache.get(clazz);
    if (ctor != null) {
      return ctor;
    }
    ctor = clazz.getDeclaredConstructor();
    noArgConstructorCache.put(clazz, ctor);
    return ctor;
  }

  // ========== NO-ARG METHOD HANDLE ==========

  /**
   * Get no-arg method handle for a class, using fast cache lookup.
   *
   * @return the MethodHandle, or null if not found (caller should fall back to normal resolution)
   */
  public static MethodHandle getNoArgMethodHandle(Class<?> clazz, String methodName) {
    Map<String, MethodHandle> classMethods = noArgMethodHandleCache.get(clazz);
    if (classMethods != null) {
      MethodHandle cached = classMethods.get(methodName);
      if (cached != null) {
        return cached;
      }
    }

    // Try to find the no-arg method and convert to MethodHandle
    try {
      java.lang.reflect.Method method = clazz.getMethod(methodName);
      if (java.lang.reflect.Modifier.isStatic(method.getModifiers())) {
        return null; // called without a receiver; normal resolution handles it
      }
      MethodHandle handle = METHOD_LOOKUP.unreflect(accessibleEquivalent(method));
      // Cache it
      noArgMethodHandleCache
          .computeIfAbsent(clazz, k -> new ConcurrentHashMap<>())
          .put(methodName, handle);
      return handle;
    } catch (NoSuchMethodException | IllegalAccessException e) {
      // Not a simple no-arg method or not accessible - return null so caller uses normal resolution
      return null;
    }
  }

  // ========== STATIC FIELD ==========

  /** Get a static field by class and name, using cache. */
  public static java.lang.reflect.Field getStaticField(Class<?> clazz, String fieldName)
      throws NoSuchFieldException {
    String key = clazz.getName() + "." + fieldName;
    java.lang.reflect.Field field = staticFieldCache.get(key);
    if (field != null) {
      return field;
    }
    // Not in cache - look up and cache
    field = clazz.getField(fieldName);
    final java.lang.reflect.Field foundField = field;
    staticFieldCache.put(key, foundField);
    return foundField;
  }

  // ========== INSTANCE FIELD ==========

  /** Get an instance field by class and name, using cache. Searches up the class hierarchy. */
  public static java.lang.reflect.Field getInstanceField(Class<?> clazz, String fieldName)
      throws NoSuchFieldException {
    String key = clazz.getName() + "." + fieldName;
    java.lang.reflect.Field field = instanceFieldCache.get(key);
    if (field != null) {
      return field;
    }
    // Search up the class hierarchy
    Class<?> current = clazz;
    while (current != null) {
      try {
        field = current.getDeclaredField(fieldName);
        final java.lang.reflect.Field foundField = field;
        instanceFieldCache.put(key, foundField);
        return foundField;
      } catch (NoSuchFieldException e) {
        current = current.getSuperclass();
      }
    }
    throw new NoSuchFieldException("No field named '" + fieldName + "' in " + clazz.getName());
  }

  // ========== METHODS CACHE ==========

  /** Get all public methods for a class, using cache. */
  public static java.lang.reflect.Method[] getCachedMethods(Class<?> clazz) {
    java.lang.reflect.Method[] methods = methodsCache.get(clazz);
    if (methods != null) {
      return methods;
    }
    methods = clazz.getMethods();
    methodsCache.put(clazz, methods);
    return methods;
  }

  // ========== CONSTRUCTORS CACHE ==========

  /** Get all public constructors for a class, using cache. */
  public static java.lang.reflect.Constructor<?>[] getCachedConstructors(Class<?> clazz) {
    java.lang.reflect.Constructor<?>[] ctors = constructorsCache.get(clazz);
    if (ctors != null) {
      return ctors;
    }
    ctors = clazz.getConstructors();
    constructorsCache.put(clazz, ctors);
    return ctors;
  }

  // ========== CLASS CACHE ==========

  /**
   * Get the effective classloader for class lookups. Prefers thread context classloader (used by
   * Spark), falls back to system loader.
   */
  private static ClassLoader getEffectiveClassLoader() {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    return cl != null ? cl : ClassLoader.getSystemClassLoader();
  }

  /** Get or create the class name map for a classloader. */
  private static Map<String, Class<?>> getClassMapForLoader(ClassLoader loader) {
    return classCache.computeIfAbsent(loader, k -> new ConcurrentHashMap<>());
  }

  /**
   * {@code Class.forName} as Python calls it. The real one initializes the class it finds, which
   * would let a caller run the static initializer of any class on the classpath, allowed or not;
   * these find the class and leave it uninitialized until something allowed uses it.
   */
  public static Class<?> classForName(String name) throws ClassNotFoundException {
    return getClass(name);
  }

  /** The three-argument form; {@code initialize} is not honoured. */
  public static Class<?> classForName(String name, boolean initialize, ClassLoader loader)
      throws ClassNotFoundException {
    return Class.forName(name, false, loader);
  }

  private static java.lang.reflect.Method withoutClassInitialization(
      java.lang.reflect.Method method) {
    if (method.getDeclaringClass() == Class.class && method.getName().equals("forName")) {
      try {
        return ReflectionCache.class.getMethod("classForName", method.getParameterTypes());
      } catch (NoSuchMethodException e) {
        // forName(Module, String) never initializes
      }
    }
    return method;
  }

  /** Get a class by name, using cache. Classloader-safe for Spark/dynamic loaders. */
  public static Class<?> getClass(String className) throws ClassNotFoundException {
    ClassLoader loader = getEffectiveClassLoader();
    Map<String, Class<?>> loaderCache = getClassMapForLoader(loader);

    Class<?> clazz = loaderCache.get(className);
    if (clazz != null) {
      return clazz;
    }
    clazz = loadClass(className, loader);
    loaderCache.put(className, clazz);
    return clazz;
  }

  /**
   * Load a class without initializing it: naming a class must not run its static initializer, which
   * happens anyway when the class is first used.
   *
   * <p>A nested class may be written the way Java source writes it, {@code
   * java.util.AbstractMap.SimpleEntry}; the binary name the loader wants has a {@code $} there.
   */
  private static Class<?> loadClass(String className, ClassLoader loader)
      throws ClassNotFoundException {
    try {
      return Class.forName(className, false, loader);
    } catch (ClassNotFoundException notFound) {
      Class<?> nested = loadNestedClass(className, loader);
      if (nested == null) {
        throw notFound;
      }
      return nested;
    }
  }

  /**
   * {@code a.b.Outer.Inner} as {@code a.b.Outer$Inner}, or null.
   *
   * <p>A failed load searches the whole classpath, and most names that are not classes are methods
   * or fields being asked about ({@code java.lang.Integer.parseInt}), so this only tries names
   * spelled like a class inside a class: both parts capitalized. A nested class named otherwise is
   * still reachable by its binary name.
   */
  private static Class<?> loadNestedClass(String className, ClassLoader loader) {
    int lastDot = className.lastIndexOf('.');
    if (lastDot <= 0 || lastDot == className.length() - 1) {
      return null;
    }
    String outerName = className.substring(0, lastDot);
    char outerInitial = outerName.charAt(outerName.lastIndexOf('.') + 1);
    char innerInitial = className.charAt(lastDot + 1);
    if (!Character.isUpperCase(outerInitial) || !Character.isUpperCase(innerInitial)) {
      return null;
    }
    // Cached when it is a class; itself looked up as a nested class when it is not
    Class<?> outer = tryGetClass(outerName);
    if (outer == null) {
      return null;
    }
    try {
      return Class.forName(outer.getName() + '$' + className.substring(lastDot + 1), false, loader);
    } catch (ClassNotFoundException | LinkageError e) {
      return null;
    }
  }

  /**
   * Try to get a class by name without throwing. Returns null if not found. Classloader-safe for
   * Spark/dynamic loaders.
   */
  public static Class<?> tryGetClass(String className) {
    ClassLoader loader = getEffectiveClassLoader();
    Map<String, Class<?>> loaderCache = getClassMapForLoader(loader);

    Class<?> clazz = loaderCache.get(className);
    if (clazz != null) {
      return clazz;
    }
    try {
      clazz = loadClass(className, loader);
      loaderCache.put(className, clazz);
      return clazz;
    } catch (ClassNotFoundException | LinkageError e) {
      return null;
    }
  }

  // ========== METHOD RESOLUTION ==========

  /** Get a cached method, or null if not cached. */
  public static CachedMethod getCachedMethod(MethodCacheKey key) {
    return methodCache.get(key);
  }

  /** Cache a resolved method. */
  public static void cacheMethod(MethodCacheKey key, CachedMethod cached) {
    methodCache.put(key, cached);
  }

  // ========== CONSTRUCTOR RESOLUTION ==========

  /** Get a cached constructor, or null if not cached. */
  public static CachedConstructor getCachedConstructor(ConstructorCacheKey key) {
    return constructorCache.get(key);
  }

  /** Cache a resolved constructor. */
  public static void cacheConstructor(ConstructorCacheKey key, CachedConstructor cached) {
    constructorCache.put(key, cached);
  }

  // ========== CACHE KEY CLASSES ==========

  /** Key for method cache lookup. */
  public static final class MethodCacheKey {
    private final Class<?> clazz;
    private final String methodName;
    private final Class<?>[] argTypes;
    private final int hashCode;

    public MethodCacheKey(Class<?> clazz, String methodName, Class<?>[] argTypes) {
      this.clazz = clazz;
      this.methodName = methodName;
      // Defensive copy to protect against caller mutating the array
      this.argTypes = argTypes.clone();
      // Pre-compute hash for faster lookups
      int h = clazz.hashCode() * 31 + methodName.hashCode();
      for (Class<?> t : this.argTypes) {
        h = h * 31 + (t != null ? t.hashCode() : 0);
      }
      this.hashCode = h;
    }

    @Override
    public int hashCode() {
      return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof MethodCacheKey other)) return false;
      if (clazz != other.clazz) return false;
      if (!methodName.equals(other.methodName)) return false;
      if (argTypes.length != other.argTypes.length) return false;
      for (int i = 0; i < argTypes.length; i++) {
        if (argTypes[i] != other.argTypes[i]) return false;
      }
      return true;
    }
  }

  /** Cached method resolution result with MethodHandle for fast invocation. */
  public static final class CachedMethod {
    public final java.lang.reflect.Method method;
    private final java.lang.reflect.Method invocable;
    private final Class<?>[] paramTypes;
    public final MethodHandle handle;
    public final boolean isVarArgs;
    public final int fixedArgCount;
    public final Class<?> varargComponentType;
    public final boolean isStatic;

    public CachedMethod(java.lang.reflect.Method method) {
      this.method = method;
      this.isVarArgs = method.isVarArgs();
      if (isVarArgs) {
        Class<?>[] paramTypes = method.getParameterTypes();
        this.fixedArgCount = paramTypes.length - 1;
        this.varargComponentType = paramTypes[fixedArgCount].getComponentType();
      } else {
        this.fixedArgCount = 0;
        this.varargComponentType = null;
      }
      this.paramTypes = method.getParameterTypes();
      // A public method of a non-public class (a stream, an iterator) is called through the
      // public type that declares it; the class itself may be in a package nobody can open.
      this.invocable = withoutClassInitialization(accessibleEquivalent(method));
      MethodHandle h = null;
      try {
        h = METHOD_LOOKUP.unreflect(invocable);
      } catch (IllegalAccessException e) {
        // Try privateLookupIn for better access (JDK 9+)
        // This works for private inner classes (e.g. ArrayList$Itr) and unnamed modules
        // A substituted method is never traded back for the one it replaces
        if (invocable.getDeclaringClass() != ReflectionCache.class) {
          try {
            method.setAccessible(true);
            MethodHandles.Lookup privateLookup =
                MethodHandles.privateLookupIn(method.getDeclaringClass(), MethodHandles.lookup());
            h = privateLookup.unreflect(method);
          } catch (IllegalAccessException | RuntimeException ex) {
            // Final fallback: use Method.invoke, which reports the access failure if there is one
          }
        }
      }
      // prepareArgs always builds the varargs array itself, so the handle takes it as is
      this.handle = h == null ? null : h.asFixedArity();
      this.isStatic = java.lang.reflect.Modifier.isStatic(method.getModifiers());
    }

    /** The arguments as the method declares them: varargs collected, numbers converted. */
    public Object[] prepareArgs(Object[] args) {
      return fixedArityArgs(paramTypes, isVarArgs, args);
    }

    /** Invoke the method using MethodHandle if available, otherwise fall back to reflection. */
    public Object invoke(Object target, Object[] args) throws Throwable {
      if (handle != null) {
        // Use invokeWithArguments for flexibility with different arg counts
        if (isStatic) {
          return handle.invokeWithArguments(args);
        } else {
          // For instance methods, prepend target to args
          Object[] fullArgs = new Object[args.length + 1];
          fullArgs[0] = target;
          System.arraycopy(args, 0, fullArgs, 1, args.length);
          return handle.invokeWithArguments(fullArgs);
        }
      }
      return invocable.invoke(target, args);
    }
  }

  /** Key for constructor cache lookup. */
  public static final class ConstructorCacheKey {
    private final Class<?> clazz;
    private final Class<?>[] argTypes;
    private final int hashCode;

    public ConstructorCacheKey(Class<?> clazz, Class<?>[] argTypes) {
      this.clazz = clazz;
      // Defensive copy to protect against caller mutating the array
      this.argTypes = argTypes.clone();
      int h = clazz.hashCode();
      for (Class<?> t : this.argTypes) {
        h = h * 31 + (t != null ? t.hashCode() : 0);
      }
      this.hashCode = h;
    }

    @Override
    public int hashCode() {
      return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof ConstructorCacheKey other)) return false;
      if (clazz != other.clazz) return false;
      if (argTypes.length != other.argTypes.length) return false;
      for (int i = 0; i < argTypes.length; i++) {
        if (argTypes[i] != other.argTypes[i]) return false;
      }
      return true;
    }
  }

  /** Cached constructor resolution result with MethodHandle for fast invocation. */
  public static final class CachedConstructor {
    public final java.lang.reflect.Constructor<?> constructor;
    private final Class<?>[] paramTypes;
    public final MethodHandle handle;
    public final boolean isVarArgs;
    public final int fixedArgCount;
    public final Class<?> varargComponentType;

    public CachedConstructor(java.lang.reflect.Constructor<?> constructor) {
      this.constructor = constructor;
      this.isVarArgs = constructor.isVarArgs();
      if (isVarArgs) {
        Class<?>[] paramTypes = constructor.getParameterTypes();
        this.fixedArgCount = paramTypes.length - 1;
        this.varargComponentType = paramTypes[fixedArgCount].getComponentType();
      } else {
        this.fixedArgCount = 0;
        this.varargComponentType = null;
      }
      // Convert to MethodHandle for faster invocation
      MethodHandle h = null;
      try {
        h = METHOD_LOOKUP.unreflectConstructor(constructor);
      } catch (IllegalAccessException e) {
        // Try privateLookupIn for better access (JDK 9+)
        try {
          constructor.setAccessible(true);
          MethodHandles.Lookup privateLookup =
              MethodHandles.privateLookupIn(
                  constructor.getDeclaringClass(), MethodHandles.lookup());
          h = privateLookup.unreflectConstructor(constructor);
        } catch (IllegalAccessException | RuntimeException ex) {
          // Final fallback: use Constructor.newInstance (slower but always works)
        }
      }
      this.paramTypes = constructor.getParameterTypes();
      this.handle = h == null ? null : h.asFixedArity();
    }

    /** The arguments as the constructor declares them: varargs collected, numbers converted. */
    public Object[] prepareArgs(Object[] args) {
      return fixedArityArgs(paramTypes, isVarArgs, args);
    }

    /** Invoke the constructor using MethodHandle if available. */
    public Object newInstance(Object[] args) throws Throwable {
      if (handle != null) {
        return handle.invokeWithArguments(args);
      }
      return constructor.newInstance(args);
    }
  }

  // ========== ARGUMENT PREPARATION ==========

  /**
   * Turn the arguments a caller passed into the ones a method or constructor declares.
   *
   * <p>Trailing arguments of a varargs call are collected into an array, unless the caller passed
   * that array itself (as Java does for {@code String.format(fmt, objectArray)}). Numbers are
   * converted to the declared parameter type where the resolver accepted a different one, such as
   * an {@code Integer} for a {@code Long} parameter.
   */
  static Object[] fixedArityArgs(Class<?>[] paramTypes, boolean varArgs, Object[] args) {
    if (!varArgs) {
      Object[] result = args;
      for (int i = 0; i < args.length && i < paramTypes.length; i++) {
        Object converted = MethodResolver.coerce(args[i], paramTypes[i]);
        if (converted != args[i]) {
          if (result == args) {
            result = args.clone();
          }
          result[i] = converted;
        }
      }
      return result;
    }

    int fixedCount = paramTypes.length - 1;
    Class<?> arrayType = paramTypes[fixedCount];
    Object[] result = new Object[paramTypes.length];
    for (int i = 0; i < fixedCount; i++) {
      result[i] = MethodResolver.coerce(args[i], paramTypes[i]);
    }
    if (args.length == paramTypes.length && arrayType.isInstance(args[fixedCount])) {
      result[fixedCount] = args[fixedCount]; // the caller passed the array itself
      return result;
    }
    Class<?> componentType = arrayType.getComponentType();
    int varargCount = args.length - fixedCount;
    Object varargArray = java.lang.reflect.Array.newInstance(componentType, varargCount);
    for (int i = 0; i < varargCount; i++) {
      java.lang.reflect.Array.set(
          varargArray, i, MethodResolver.coerce(args[fixedCount + i], componentType));
    }
    result[fixedCount] = varargArray;
    return result;
  }

  /**
   * The same method as declared by a public class or interface in an exported package, if there is
   * one. {@code getMethods()} on a runtime class such as a stream reports methods declared by a
   * package-private implementation class, which cannot be called reflectively; the interface's
   * declaration can, and dispatches to the same code.
   */
  static java.lang.reflect.Method accessibleEquivalent(java.lang.reflect.Method method) {
    Class<?> declaring = method.getDeclaringClass();
    if (java.lang.reflect.Modifier.isStatic(method.getModifiers()) || isAccessibleType(declaring)) {
      return method;
    }
    java.util.ArrayDeque<Class<?>> pending = new java.util.ArrayDeque<>();
    java.util.Set<Class<?>> seen = new java.util.HashSet<>();
    pending.add(declaring);
    while (!pending.isEmpty()) {
      Class<?> type = pending.poll();
      if (!seen.add(type)) {
        continue;
      }
      if (isAccessibleType(type)) {
        try {
          java.lang.reflect.Method candidate =
              type.getDeclaredMethod(method.getName(), method.getParameterTypes());
          if (java.lang.reflect.Modifier.isPublic(candidate.getModifiers())) {
            return candidate;
          }
        } catch (NoSuchMethodException e) {
          // not declared here; keep looking
        }
      }
      if (type.getSuperclass() != null) {
        pending.add(type.getSuperclass());
      }
      pending.addAll(java.util.List.of(type.getInterfaces()));
    }
    return method;
  }

  private static boolean isAccessibleType(Class<?> type) {
    return java.lang.reflect.Modifier.isPublic(type.getModifiers())
        && type.getModule().isExported(type.getPackageName());
  }

  // ========== VAR HANDLE SUPPORT ==========

  /** Cached static VarHandle with declaring class for static field access. */
  public static final class CachedStaticVarHandle {
    public final VarHandle handle;
    public final Class<?> declaringClass;

    public CachedStaticVarHandle(VarHandle handle, Class<?> declaringClass) {
      this.handle = handle;
      this.declaringClass = declaringClass;
    }
  }

  /**
   * Get VarHandle for instance field access. Returns null if not available. VarHandle is faster
   * than Field.get/set after JIT warmup.
   */
  public static VarHandle getInstanceVarHandle(Class<?> clazz, String fieldName) {
    String key = clazz.getName() + "." + fieldName;
    VarHandle cached = varHandleCache.get(key);
    if (cached != null) {
      return cached;
    }

    // Try to create VarHandle
    try {
      java.lang.reflect.Field field = getInstanceField(clazz, fieldName);
      // Use privateLookupIn for private fields
      MethodHandles.Lookup lookup =
          MethodHandles.privateLookupIn(field.getDeclaringClass(), METHOD_LOOKUP);
      VarHandle vh = lookup.unreflectVarHandle(field);
      varHandleCache.put(key, vh);
      return vh;
    } catch (NoSuchFieldException | IllegalAccessException e) {
      // Fall back to Field-based access
      return null;
    }
  }

  /** Get VarHandle for static field access. Returns null if not available. */
  public static CachedStaticVarHandle getStaticVarHandle(Class<?> clazz, String fieldName) {
    String key = clazz.getName() + "." + fieldName;
    CachedStaticVarHandle cached = staticVarHandleCache.get(key);
    if (cached != null) {
      return cached;
    }

    try {
      java.lang.reflect.Field field = getStaticField(clazz, fieldName);
      MethodHandles.Lookup lookup =
          MethodHandles.privateLookupIn(field.getDeclaringClass(), METHOD_LOOKUP);
      VarHandle vh = lookup.unreflectVarHandle(field);
      CachedStaticVarHandle result = new CachedStaticVarHandle(vh, field.getDeclaringClass());
      staticVarHandleCache.put(key, result);
      return result;
    } catch (NoSuchFieldException | IllegalAccessException e) {
      return null;
    }
  }

  // ========== CACHE STATISTICS ==========

  /**
   * Get cache statistics for monitoring and debugging.
   *
   * @return formatted string with stats for all caches
   */
  public static String getCacheStats() {
    StringBuilder sb = new StringBuilder();
    sb.append("ReflectionCache Statistics:\n");
    sb.append("  methodCache: ").append(methodCache.getStats()).append("\n");
    sb.append("  constructorCache: ").append(constructorCache.getStats()).append("\n");
    sb.append("  staticFieldCache: ").append(staticFieldCache.getStats()).append("\n");
    sb.append("  instanceFieldCache: ").append(instanceFieldCache.getStats()).append("\n");
    sb.append("  varHandleCache: ").append(varHandleCache.getStats()).append("\n");
    sb.append("  staticVarHandleCache: ").append(staticVarHandleCache.getStats()).append("\n");
    sb.append("  methodsCache: size=").append(methodsCache.size()).append(" (WeakHashMap)\n");
    sb.append("  constructorsCache: size=")
        .append(constructorsCache.size())
        .append(" (WeakHashMap)\n");
    sb.append("  noArgConstructorCache: size=")
        .append(noArgConstructorCache.size())
        .append(" (WeakHashMap)\n");
    sb.append("  noArgMethodHandleCache: size=")
        .append(noArgMethodHandleCache.size())
        .append(" (WeakHashMap)\n");
    // Count class cache entries across all loaders
    int classCount = 0;
    synchronized (classCache) {
      for (Map<String, Class<?>> loaderCache : classCache.values()) {
        classCount += loaderCache.size();
      }
    }
    sb.append("  classCache: size=")
        .append(classCount)
        .append(" across ")
        .append(classCache.size())
        .append(" loaders (WeakHashMap)\n");
    return sb.toString();
  }

  /** Get method cache statistics. */
  public static BoundedCache<MethodCacheKey, CachedMethod> getMethodCache() {
    return methodCache;
  }

  /** Get constructor cache statistics. */
  public static BoundedCache<ConstructorCacheKey, CachedConstructor> getConstructorCache() {
    return constructorCache;
  }

  /** Clear all caches. Useful for testing or when classloaders change. */
  public static void clearAllCaches() {
    methodCache.clear();
    constructorCache.clear();
    staticFieldCache.clear();
    instanceFieldCache.clear();
    varHandleCache.clear();
    staticVarHandleCache.clear();
    methodsCache.clear();
    constructorsCache.clear();
    noArgConstructorCache.clear();
    noArgMethodHandleCache.clear();
    synchronized (classCache) {
      classCache.clear();
    }
    LOG.info("All reflection caches cleared");
  }
}
