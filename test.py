"""Test utility classes for blue-backup."""
# Copyright 2026 Udi Fuchs

from __future__ import annotations

import pathlib

import pytest

import asyncrunner as a


class Foo:
    def __init__(self, num: int, str_value: str) -> None:
        self.num = num
        self.str = str_value
        if num > 0:
            self.foo = Foo(num - 1, str_value)

    def get_num(self) -> int:
        return self.num

    def get_str(self, postfix: str = "") -> str:
        return self.str + postfix

    def replace_foo(self, foo: Foo) -> None:
        self.foo = foo

    def open_file(self, filepath: pathlib.Path) -> None:
        self.file = filepath.open()

    @property
    def str_with_num(self) -> str:
        return f"{self.str}:{self.num}"


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", [
    "thread",
    "process",
])
async def test_runner(*, mode: str) -> None:
    if mode == "thread":
        foo_exe = a.create_thread(Foo, 3, str_value="hello")
    else:  # mode == "process":
        foo_exe = a.create_process(Foo, 3, str_value="hello")
    exe_name = foo_exe.__class__.__name__

    # Test method call:
    num = await a.run(foo_exe.get_num)
    assert num == 3

    # Test method call with argument:
    str_value = await a.run(foo_exe.get_str, "!")
    assert str_value == "hello!"

    # Test method call with keyword argument:
    str_value = await a.run(foo_exe.get_str, postfix="!")
    assert str_value == "hello!"

    # Instance attribute are not visible until they are attached:
    with pytest.raises(AttributeError) as ex:
        await a.set_value(foo_exe.foo, Foo(0, ""))
    assert str(ex.value) == f"'{exe_name}' object has no attribute 'foo'"

    # Test attaching subclass instance attribute:
    await a.attach(foo_exe, "foo")
    num = await a.run(foo_exe.foo.get_num)
    assert num == 2
    await a.attach(foo_exe.foo, "foo")
    num = await a.run(foo_exe.foo.foo.get_num)
    assert num == 1

    # Test attaching value instance attribute:
    await a.attach(foo_exe, "num")
    await a.set_value(foo_exe.num, 7)
    num = await a.run(foo_exe.get_num)
    assert num == 7
    num = await a.get_value(foo_exe.num)
    assert num == 7

    # Test get value for properties:
    await a.attach(foo_exe, "str_with_num")
    str_with_num = await a.get_value(foo_exe.str_with_num)
    assert str_with_num == "hello:7"
    with pytest.raises(AttributeError) as ex:
        await a.set_value(foo_exe.str_with_num, "123")
    assert str(ex.value) == "property 'str_with_num' of 'Foo' object has no setter"

    # Test exception when trying to access non existing method:
    with pytest.raises(AttributeError) as ex:
        str_value = await a.run(foo_exe.no_such_method)  # type: ignore[attr-defined]
    assert str(ex.value) == f"'{exe_name}' object has no attribute 'no_such_method'"

    a.shutdown(foo_exe)
    with pytest.raises(RuntimeError) as rt_ex:
        num = await a.run(foo_exe.get_num)
    assert str(rt_ex.value) == "cannot schedule new futures after shutdown"


@pytest.mark.asyncio
async def test_type_errors() -> None:
    foo = Foo(3, "goodbye")

    with pytest.raises(TypeError) as ex:
        await a.run(foo.get_num)
    assert str(ex.value).startswith(
        "Can only run an executor method. Got: <test.Foo object"
    )

    with pytest.raises(TypeError) as ex:
        await a.attach(foo, "str_with_num")
    assert str(ex.value).startswith(
        "Can only attach to existing executor. Got: <test.Foo object"
    )

    with pytest.raises(TypeError) as ex:
        await a.set_value(foo.num, 3)
    assert str(ex.value) == "Can only set an executor attribute. Got: 3"

    with pytest.raises(TypeError) as ex:
        await a.get_value(foo.num)
    assert str(ex.value) == "Can only get an executor attribute. Got: 3"

    with pytest.raises(TypeError) as ex:
        a.shutdown(foo)
    assert str(ex.value).startswith(
        "Can only shutdown executor object. Got: <test.Foo object"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", [
    "thread",
    "process",
])
async def test_value_errors(*, mode: str) -> None:
    if mode == "thread":
        foo_exe = a.create_thread(Foo, 3, str_value="hello")
        exe_name = "asyncrunner._ThreadExecutor"
    else:  # mode == "process":
        foo_exe = a.create_process(Foo, 3, str_value="hello")
        exe_name = "test.Foo"

    with pytest.raises(
        ValueError,
        match=fr"Cannot set value to executor: <{exe_name} object"
    ):
        await a.set_value(foo_exe, 3)

    foo = await a.get_value(foo_exe)
    assert foo.num == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", [
    "thread",
    "process",
])
async def test_pickles(*, mode: str) -> None:
    if mode == "thread":
        foo_exe = a.create_thread(Foo, 3, str_value="hello")
    else:  # mode == "process":
        foo_exe = a.create_process(Foo, 3, str_value="hello")

    await a.attach(foo_exe, "foo")
    # Create a file handle that cannot be pickled:
    await a.run(foo_exe.foo.open_file, pathlib.Path("pyproject.toml"))
    if mode == "thread":
        foo = await a.get_value(foo_exe.foo)
        assert foo.num == 2
    else:  # mode == "process":
        with pytest.raises(TypeError, match="cannot pickle 'TextIOWrapper' instances"):
            await a.get_value(foo_exe.foo)

    # Replace the non-picklable foo with a picklable one:
    await a.run(foo_exe.replace_foo, Foo(-77, "world"))
    # This still does not prevent the exception:
    if mode == "thread":
        foo = await a.get_value(foo_exe.foo)
        assert foo.num == -77
    else:  # mode == "process":
        with pytest.raises(TypeError, match="cannot pickle 'TextIOWrapper' instances"):
            await a.get_value(foo_exe.foo)
    # For replace_foo to take effect, we need to re-attach foo:
    await a.attach(foo_exe, "foo")
    foo = await a.get_value(foo_exe.foo)
    assert foo.num == -77
