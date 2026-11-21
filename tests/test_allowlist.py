"""Tests for extending the class allowlist without rebuilding the server."""

import os
import subprocess
import time

import pytest

from gatun import GatunClient, JavaSecurityException, aconnect, connect
from gatun.bridge_adapters import GatunAdapter
from gatun.config import load_config, reset_config
from gatun.launcher import DEFAULT_JVM_FLAGS, JAR_PATH, _checked_names, launch_gateway

ATOMIC_INT = "java.util.concurrent.atomic.AtomicInteger"
ATOMIC_LONG = "java.util.concurrent.atomic.AtomicLong"


class TestBuiltInList:
    def test_unlisted_class_is_refused_with_a_hint(self, client):
        with pytest.raises(JavaSecurityException) as exc_info:
            client.create_object(ATOMIC_INT)

        message = str(exc_info.value)
        assert f"Class not allowed: {ATOMIC_INT}" in message
        assert "allowed_classes" in message
        assert "allowed_prefixes" in message

    def test_unlisted_static_call_is_refused(self, client):
        with pytest.raises(JavaSecurityException, match="Class not allowed"):
            client.invoke_static_method("java.lang.Runtime", "getRuntime")


class TestAllowedClasses:
    def test_listed_class_can_be_created_and_used(self):
        client = connect(allowed_classes=[ATOMIC_INT])
        try:
            counter = client.create_object(ATOMIC_INT, 41)
            assert counter.incrementAndGet() == 42
            assert client.jvm.java.util.concurrent.atomic.AtomicInteger(7).get() == 7
        finally:
            client.close()

    def test_only_the_listed_class_is_added(self):
        client = connect(allowed_classes=[ATOMIC_INT])
        try:
            with pytest.raises(JavaSecurityException, match="Class not allowed"):
                client.create_object(ATOMIC_LONG)
        finally:
            client.close()

    def test_built_in_classes_stay_allowed(self):
        client = connect(allowed_classes=[ATOMIC_INT])
        try:
            assert client.create_object("java.util.ArrayList").size() == 0
            assert client.jvm.java.lang.Integer.parseInt("42") == 42
        finally:
            client.close()

    def test_static_methods_and_batches(self):
        client = connect(allowed_classes=["java.util.Objects", ATOMIC_INT])
        try:
            assert client.invoke_static_method("java.util.Objects", "isNull", None)
            with client.batch() as b:
                created = b.create(ATOMIC_INT, 5)
                refused = b.create(ATOMIC_LONG, 5)
            assert created.get().get() == 5
            assert refused.is_error
        finally:
            client.close()


class TestAllowedPrefixes:
    @pytest.mark.parametrize(
        "prefix",
        ["java.util.concurrent", "java.util.concurrent.", "java.util.concurrent.*"],
    )
    def test_prefix_covers_the_package_and_its_subpackages(self, prefix):
        client = connect(allowed_prefixes=[prefix])
        try:
            assert client.create_object(ATOMIC_LONG, 3).get() == 3
            assert (
                client.create_object("java.util.concurrent.ConcurrentHashMap").size()
                == 0
            )
        finally:
            client.close()

    def test_prefix_does_not_match_a_longer_package_name(self):
        """'java.util.concurrent' must not allow 'java.util.concurrentx.Thing'.
        The refusal comes before the class is looked up, so the error says
        'not allowed' rather than 'not found'."""
        client = connect(allowed_prefixes=["java.util.concurrent"])
        try:
            with pytest.raises(JavaSecurityException, match="Class not allowed"):
                client.create_object("java.util.concurrentx.Thing")
            with pytest.raises(JavaSecurityException, match="Class not allowed"):
                client.create_object("java.util.Random")
        finally:
            client.close()

    async def test_async_client(self):
        client = await aconnect(allowed_prefixes=["java.util.concurrent.atomic"])
        try:
            counter = await client.create_object(ATOMIC_INT, 1)
            assert await counter.incrementAndGet() == 2
        finally:
            await client.close()

    def test_bridge_adapter(self):
        bridge = GatunAdapter(memory="16MB", allowed_classes=[ATOMIC_INT])
        try:
            counter = bridge.new(ATOMIC_INT, 9)
            assert bridge.call(counter, "get") == 9
        finally:
            bridge.close()


class TestConfigurationSources:
    @pytest.fixture(autouse=True)
    def _fresh_config(self):
        reset_config()
        yield
        reset_config()

    def test_pyproject(self, tmp_path, monkeypatch):
        (tmp_path / "pyproject.toml").write_text(
            "[tool.gatun]\n"
            f'allowed_classes = ["{ATOMIC_INT}"]\n'
            'allowed_prefixes = ["java.util.concurrent.locks"]\n'
        )
        monkeypatch.chdir(tmp_path)

        client = connect()
        try:
            assert client.create_object(ATOMIC_INT, 1).get() == 1
            lock = client.create_object("java.util.concurrent.locks.ReentrantLock")
            assert lock.isLocked() is False
        finally:
            client.close()

    def test_environment(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)  # no pyproject.toml here
        monkeypatch.setenv("GATUN_ALLOWED_CLASSES", f"{ATOMIC_INT}, {ATOMIC_LONG}")

        client = connect()
        try:
            assert client.create_object(ATOMIC_INT, 1).get() == 1
            assert client.create_object(ATOMIC_LONG, 2).get() == 2
        finally:
            client.close()

    def test_jvm_flag_is_combined_with_the_other_sources(self, tmp_path, monkeypatch):
        """Regression: the launcher's own -D flag came last and replaced a list
        the caller had set directly as a system property."""
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("GATUN_ALLOWED_CLASSES", ATOMIC_INT)

        client = connect(
            extra_jvm_flags=[f"-Dgatun.allowlist.classes={ATOMIC_LONG}"],
            allowed_prefixes=["java.util.concurrent.locks"],
        )
        try:
            assert client.create_object(ATOMIC_INT, 1).get() == 1
            assert client.create_object(ATOMIC_LONG, 2).get() == 2
            lock = client.create_object("java.util.concurrent.locks.ReentrantLock")
            assert lock.isLocked() is False
        finally:
            client.close()

    def test_sources_are_combined(self, tmp_path, monkeypatch):
        (tmp_path / "pyproject.toml").write_text(
            f'[tool.gatun]\nallowed_classes = ["{ATOMIC_INT}"]\n'
        )
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("GATUN_ALLOWED_CLASSES", ATOMIC_LONG)

        config = load_config()
        assert config.allowed_classes == [ATOMIC_INT, ATOMIC_LONG]

        client = connect(allowed_classes=["java.util.concurrent.atomic.AtomicBoolean"])
        try:
            assert client.create_object(ATOMIC_INT, 1).get() == 1
            assert client.create_object(ATOMIC_LONG, 2).get() == 2
            flag = client.create_object("java.util.concurrent.atomic.AtomicBoolean")
            assert flag.get() is False
        finally:
            client.close()


class TestValidation:
    @pytest.mark.parametrize(
        "name", ["", "*", "com.example.*.Thing", "com..example", "has space", "9lives"]
    )
    def test_bad_entries_are_rejected_before_launch(self, name):
        with pytest.raises(ValueError, match="Invalid entry"):
            connect(allowed_classes=[name])
        with pytest.raises(ValueError, match="Invalid entry"):
            connect(allowed_prefixes=[name])

    def test_class_names_do_not_take_wildcards(self):
        with pytest.raises(ValueError, match="allowed_classes"):
            connect(allowed_classes=["com.example.*"])

    def test_non_string_entry(self):
        with pytest.raises(TypeError, match="must be strings"):
            connect(allowed_classes=[object])

    def test_prefix_takes_one_suffix_only(self):
        """Regression: Python accepted 'com.example..*' and the server then
        refused to start on it."""
        with pytest.raises(ValueError, match="Invalid entry"):
            connect(allowed_prefixes=["com.example..*"])

    @pytest.mark.parametrize("option", ["allowed_classes", "allowed_prefixes"])
    def test_bare_string_is_not_a_list(self, option):
        """Regression: a string was taken as a list of its characters, which
        allowed every one-letter package named in it."""
        with pytest.raises(TypeError, match=f"{option} must be a list of strings"):
            connect(**{option: "acme"})

    def test_bare_string_in_pyproject(self, tmp_path, monkeypatch):
        (tmp_path / "pyproject.toml").write_text(
            '[tool.gatun]\nallowed_prefixes = "acme"\n'
        )
        monkeypatch.chdir(tmp_path)
        reset_config()
        try:
            with pytest.raises(TypeError, match="allowed_prefixes .* must be a list"):
                connect()
        finally:
            reset_config()

    def test_duplicates_are_dropped(self):
        assert _checked_names(["a.B", "a.B", "a.C"], "allowed_classes") == [
            "a.B",
            "a.C",
        ]

    def test_bad_jvm_flag_is_rejected_before_launch(self):
        with pytest.raises(ValueError, match="Invalid entry in allowed_prefixes"):
            connect(extra_jvm_flags=["-Dgatun.allowlist.prefixes=*"])

    def test_server_rejects_a_bad_property(self, tmp_path):
        """The server checks too, for launches that bypass the Python launcher."""
        java_home = os.environ.get("JAVA_HOME")
        java = os.path.join(java_home, "bin", "java") if java_home else "java"

        result = subprocess.run(
            [
                java,
                *DEFAULT_JVM_FLAGS,
                "-Dgatun.allowlist.prefixes=*",
                "-jar",
                str(JAR_PATH),
                "1048576",
                str(tmp_path / "never.sock"),
            ],
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )

        assert result.returncode != 0
        assert "Invalid entry in gatun.allowlist.prefixes" in result.stderr


class TestReflectionIsNotAWayAround:
    """`java.lang.Class` is allowed, and a Class hands out Methods and loaders."""

    def test_method_object_cannot_be_invoked(self, client):
        runtime = client.jvm.java.lang.Class.forName("java.lang.Runtime")
        get_runtime = runtime.getMethod("getRuntime")

        with pytest.raises(JavaSecurityException) as exc_info:
            get_runtime.invoke(None)

        message = str(exc_info.value)
        assert "java.lang.reflect.Method" in message
        assert "allowed_classes" in message

    def test_constructor_object_cannot_be_used(self, client):
        builder = client.jvm.java.lang.Class.forName("java.lang.ProcessBuilder")
        constructors = builder.getConstructors()

        with pytest.raises(JavaSecurityException, match="Constructor"):
            client.jvm.java.lang.reflect.Array.get(constructors, 0).newInstance()

    def test_class_loader_cannot_be_used(self, client):
        own_class = client.jvm.java.lang.Class.forName("org.gatun.server.GatunServer")
        loader = own_class.getClassLoader()

        with pytest.raises(JavaSecurityException, match="ClassLoader"):
            loader.loadClass("java.lang.Runtime")

    def test_class_cannot_be_instantiated_by_reflection(self, client):
        builder = client.jvm.java.lang.Class.forName("java.lang.ProcessBuilder")
        with pytest.raises(JavaSecurityException, match="newInstance is not allowed"):
            builder.newInstance()

    @pytest.mark.parametrize(
        ("method", "args"),
        [("exit", (0,)), ("load", ("/tmp/x.dylib",)), ("loadLibrary", ("x",))],
    )
    def test_system_methods_that_reach_outside_are_refused(self, client, method, args):
        with pytest.raises(JavaSecurityException, match=f"System.{method}"):
            client.invoke_static_method("java.lang.System", method, *args)
        assert client.ping() is True

    def test_other_system_methods_still_work(self, client):
        assert client.jvm.java.lang.System.currentTimeMillis() > 0
        client.jvm.java.lang.System.setProperty("gatun.test.property", "1")
        assert client.jvm.java.lang.System.getProperty("gatun.test.property") == "1"

    def test_reflection_objects_can_still_be_passed_around(self, client):
        """Only using one from Python is refused; Java code may be handed one."""
        size = client.jvm.java.lang.Class.forName("java.util.ArrayList").getMethod(
            "size"
        )
        described = client.jvm.java.lang.String.valueOf(size)
        assert described == "public int java.util.ArrayList.size()"

    def test_looking_a_class_up_does_not_initialize_it(self, compile_java):
        """Naming a class must not run its static initializer.

        It used to: any class on the classpath could be made to run code, allowed
        or not, by asking what it was or by calling `Class.forName` on it.
        """
        classes = compile_java(
            "Loud",
            "public class Loud {\n"
            '  static { System.setProperty("gatun.test.loud", "ran"); }\n'
            '  public static String hello() { return "hello"; }\n'
            "}\n",
        )

        client = connect(memory="16MB", classpath=[classes])
        try:
            System = client.jvm.java.lang.System
            assert client.reflect("Loud") == "class"
            assert client.reflect("Loud.hello") == "method"
            found = client.jvm.java.lang.Class.forName("Loud")
            assert found.getName() == "Loud"
            # Through an instance, and asking for initialization outright
            loader = found.getClassLoader()
            assert found.forName("Loud", True, loader).getName() == "Loud"
            with pytest.raises(JavaSecurityException, match="Class not allowed"):
                client.invoke_static_method("Loud", "hello")
            assert System.getProperty("gatun.test.loud") is None
        finally:
            client.close()

        # Allowed and used, it is initialized as usual
        client = connect(memory="16MB", classpath=[classes], allowed_classes=["Loud"])
        try:
            assert client.invoke_static_method("Loud", "hello") == "hello"
            assert client.jvm.java.lang.System.getProperty("gatun.test.loud") == "ran"
        finally:
            client.close()

    def test_enum_constants_and_annotations_do_not_initialize(self, compile_java):
        """A class looked up by name is not initialized, and neither are the
        enums its constants or annotations would make."""
        classes = compile_java(
            "Quiet",
            "import java.lang.annotation.Retention;\n"
            "import java.lang.annotation.RetentionPolicy;\n"
            "enum Noisy { ONE;\n"
            '  static { System.setProperty("gatun.test.noisy", "ran"); } }\n'
            "enum Level { LOW;\n"
            '  static { System.setProperty("gatun.test.level", "ran"); } }\n'
            "@Retention(RetentionPolicy.RUNTIME) @interface Tag { Level value(); }\n"
            "@Tag(Level.LOW) public class Quiet {}\n",
        )

        client = connect(memory="16MB", classpath=[classes])
        try:
            Class = client.jvm.java.lang.Class
            with pytest.raises(JavaSecurityException, match="getEnumConstants"):
                Class.forName("Noisy").getEnumConstants()
            quiet = Class.forName("Quiet")
            for read in ("getAnnotations", "getDeclaredAnnotations"):
                with pytest.raises(JavaSecurityException, match=read):
                    getattr(quiet, read)()
            with pytest.raises(JavaSecurityException, match="getAnnotations"):
                quiet.getPackage().getAnnotations()
            System = client.jvm.java.lang.System
            assert System.getProperty("gatun.test.noisy") is None
            assert System.getProperty("gatun.test.level") is None
            # On an allowed class they work as before
            allowed = Class.forName("java.util.ArrayList")
            assert allowed.getEnumConstants() is None
            assert len(allowed.getAnnotations()) == 0
        finally:
            client.close()

        # Allowed, they are what they are
        client = connect(
            memory="16MB", classpath=[classes], allowed_classes=["Noisy", "Quiet"]
        )
        try:
            Class = client.jvm.java.lang.Class
            assert len(Class.forName("Noisy").getEnumConstants()) == 1
            assert len(Class.forName("Quiet").getAnnotations()) == 1
            System = client.jvm.java.lang.System
            assert System.getProperty("gatun.test.noisy") == "ran"
            assert System.getProperty("gatun.test.level") == "ran"
        finally:
            client.close()

    def test_restricted_type_can_be_allowed(self):
        client = connect(memory="16MB", allowed_classes=["java.lang.reflect.Method"])
        try:
            size = client.jvm.java.lang.Class.forName("java.util.ArrayList").getMethod(
                "size"
            )
            assert size.getName() == "size"
            items = client.jvm.java.util.ArrayList()
            items.add("a")
            assert size.invoke(items) == 1
        finally:
            client.close()

    def test_thread_objects_need_allowing(self):
        client = connect(memory="16MB", allowed_classes=["java.lang.Thread"])
        try:
            thread = client.jvm.java.lang.Thread.currentThread()
            assert thread.isAlive() is True
        finally:
            client.close()


class TestNestedClasses:
    def test_nested_class_by_its_source_name(self):
        client = connect(
            memory="16MB", allowed_classes=["java.util.AbstractMap.SimpleEntry"]
        )
        try:
            entry = client.jvm.java.util.AbstractMap.SimpleEntry("k", 1)
            assert entry.getKey() == "k"
            assert client.reflect("java.util.AbstractMap.SimpleEntry") == "class"
        finally:
            client.close()

    def test_nested_class_by_its_binary_name(self):
        client = connect(
            memory="16MB", allowed_classes=["java.util.AbstractMap$SimpleEntry"]
        )
        try:
            entry = client.create_object("java.util.AbstractMap$SimpleEntry", "k", 1)
            assert entry.getValue() == 1
            # Allowed under one spelling means allowed under the other
            entry = client.jvm.java.util.AbstractMap.SimpleEntry("k", 2)
            assert entry.getValue() == 2
        finally:
            client.close()

    def test_nested_class_is_refused_unless_allowed(self, client):
        with pytest.raises(JavaSecurityException, match="Class not allowed"):
            client.jvm.java.util.AbstractMap.SimpleEntry("k", 1)


class TestAllowAll:
    """`allow_all=True` lifts the class list and the restriction on reflection."""

    @pytest.fixture(autouse=True)
    def _fresh_config(self):
        reset_config()
        yield
        reset_config()

    def test_any_class_can_be_created_and_called(self):
        client = connect(allow_all=True)
        try:
            assert client.create_object(ATOMIC_LONG, 3).get() == 3
            assert client.jvm.java.util.concurrent.atomic.AtomicInteger(7).get() == 7
            assert client.invoke_static_method("java.util.Objects", "isNull", None)
            assert client.get_static_field("java.lang.Long", "MAX_VALUE") == 2**63 - 1
            with client.batch() as b:
                created = b.create(ATOMIC_INT, 5)
                called = b.call_static("java.util.Objects", "hash", 1)
            assert created.get().get() == 5
            assert not called.is_error
        finally:
            client.close()

    def test_reflection_objects_can_be_used(self):
        client = connect(allow_all=True)
        try:
            size = client.jvm.java.lang.Class.forName("java.util.ArrayList").getMethod(
                "size"
            )
            items = client.jvm.java.util.ArrayList()
            items.add("a")
            assert size.invoke(items) == 1

            runtime = client.jvm.java.lang.Runtime.getRuntime()
            assert runtime.availableProcessors() >= 1
            assert client.jvm.java.lang.Thread.currentThread().isAlive() is True
        finally:
            client.close()

    @pytest.mark.parametrize(
        ("method", "args"),
        [("exit", (0,)), ("load", ("/tmp/x.dylib",)), ("loadLibrary", ("x",))],
    )
    def test_refused_members_stay_refused(self, method, args):
        client = connect(allow_all=True)
        try:
            with pytest.raises(JavaSecurityException, match=f"System.{method}"):
                client.invoke_static_method("java.lang.System", method, *args)
            builder = client.jvm.java.lang.Class.forName("java.lang.StringBuilder")
            with pytest.raises(
                JavaSecurityException, match="newInstance is not allowed"
            ):
                builder.newInstance()
            assert client.ping() is True
        finally:
            client.close()

    def test_off_unless_asked_for(self):
        client = connect()
        try:
            with pytest.raises(JavaSecurityException, match="Class not allowed"):
                client.create_object(ATOMIC_LONG)
        finally:
            client.close()

    def test_environment(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)  # no pyproject.toml here
        monkeypatch.setenv("GATUN_ALLOW_ALL", "true")

        client = connect()
        try:
            assert client.create_object(ATOMIC_LONG, 2).get() == 2
        finally:
            client.close()

    def test_argument_wins_over_environment(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("GATUN_ALLOW_ALL", "true")

        client = connect(allow_all=False)
        try:
            with pytest.raises(JavaSecurityException, match="Class not allowed"):
                client.create_object(ATOMIC_LONG)
        finally:
            client.close()

    def test_pyproject(self, tmp_path, monkeypatch):
        (tmp_path / "pyproject.toml").write_text("[tool.gatun]\nallow_all = true\n")
        monkeypatch.chdir(tmp_path)
        assert load_config().allow_all is True

        client = connect()
        try:
            assert client.create_object(ATOMIC_LONG, 2).get() == 2
        finally:
            client.close()

    def test_environment_wins_over_pyproject(self, tmp_path, monkeypatch):
        (tmp_path / "pyproject.toml").write_text("[tool.gatun]\nallow_all = true\n")
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("GATUN_ALLOW_ALL", "false")
        assert load_config().allow_all is False

    def test_jvm_flag(self):
        client = connect(extra_jvm_flags=["-Dgatun.allowlist.all=true"])
        try:
            assert client.create_object(ATOMIC_LONG, 2).get() == 2
        finally:
            client.close()

    def test_argument_wins_over_jvm_flag(self):
        client = connect(
            allow_all=False, extra_jvm_flags=["-Dgatun.allowlist.all=true"]
        )
        try:
            with pytest.raises(JavaSecurityException, match="Class not allowed"):
                client.create_object(ATOMIC_LONG)
        finally:
            client.close()

    async def test_async_client(self):
        client = await aconnect(allow_all=True)
        try:
            counter = await client.create_object(ATOMIC_INT, 1)
            assert await counter.incrementAndGet() == 2
        finally:
            await client.close()

    def test_bridge_adapter(self):
        bridge = GatunAdapter(memory="16MB", allow_all=True)
        try:
            counter = bridge.new(ATOMIC_INT, 9)
            assert bridge.call(counter, "get") == 9
        finally:
            bridge.close()

    def test_server_says_so_once_at_start(self):
        session = launch_gateway(allow_all=True)
        try:
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                lines = [
                    line for line in session._stderr_lines if "allowlist is off" in line
                ]
                if lines:
                    break
                time.sleep(0.05)
            assert len(lines) == 1, session._stderr_lines
            assert "gatun.allowlist.all" in lines[0]
        finally:
            session.stop()

    def test_server_says_nothing_when_it_is_off(self):
        session = launch_gateway()
        try:
            client = GatunClient(session.socket_path)
            assert client.connect()
            client.close()
            assert not [
                line for line in session._stderr_lines if "allowlist is off" in line
            ]
        finally:
            session.stop()

    @pytest.mark.parametrize("value", ["yes", "1", "all"])
    def test_environment_value_must_be_true_or_false(
        self, value, tmp_path, monkeypatch
    ):
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("GATUN_ALLOW_ALL", value)
        with pytest.raises(ValueError, match="GATUN_ALLOW_ALL"):
            connect()

    def test_pyproject_value_must_be_a_boolean(self, tmp_path, monkeypatch):
        (tmp_path / "pyproject.toml").write_text('[tool.gatun]\nallow_all = "true"\n')
        monkeypatch.chdir(tmp_path)
        with pytest.raises(TypeError, match="allow_all .* must be true or false"):
            connect()

    def test_argument_must_be_a_boolean(self):
        """A string would count as true whatever it says."""
        with pytest.raises(TypeError, match="allow_all must be True or False"):
            connect(allow_all="false")

    def test_jvm_flag_must_be_true_or_false(self):
        with pytest.raises(ValueError, match=r"gatun\.allowlist\.all"):
            connect(extra_jvm_flags=["-Dgatun.allowlist.all=yes"])

    def test_server_rejects_a_bad_property(self, tmp_path):
        """The server checks too, for launches that bypass the Python launcher."""
        java_home = os.environ.get("JAVA_HOME")
        java = os.path.join(java_home, "bin", "java") if java_home else "java"

        result = subprocess.run(
            [
                java,
                *DEFAULT_JVM_FLAGS,
                "-Dgatun.allowlist.all=yes",
                "-jar",
                str(JAR_PATH),
                "1048576",
                str(tmp_path / "never.sock"),
            ],
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )

        assert result.returncode != 0
        assert "Invalid value for gatun.allowlist.all" in result.stderr

    def test_an_empty_prefix_is_not_the_way(self):
        """An empty prefix would match every class; the error names the option
        that is meant for that."""
        with pytest.raises(ValueError, match="allow_all=True"):
            connect(allowed_prefixes=[""])
