"""Test utility classes for blue-backup."""
# Copyright 2026 Udi Fuchs

from __future__ import annotations

import array
import pathlib
import sys
from collections.abc import Sequence
from typing import TextIO

import pytest

import asyncrunner as a


class Foo:
    def __init__(self, num: int, str_value: str) -> None:
        self.num = num
        self.str = str_value
        self.list = [1, 2, 3]
        self.arr = array.array("i", [11, 22, 33])
        if num > 0:
            self.foo = Foo(num - 1, str_value)
        self.file: TextIO | None = None

    def get_num(self) -> int:
        self.num += 1
        return self.num

    def get_str(self, postfix: str = "") -> str:
        return self.str + postfix

    def get_list(self) -> Sequence[int]:
        return self.list

    def replace_foo(self, foo: Foo) -> None:
        self.foo = foo

    def open_file(self, filepath: pathlib.Path) -> None:
        self.file = filepath.open()

    @property
    def str_with_num(self) -> str:
        return f"{self.str}:{self.num}"


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", list(a.Mode))
async def test_runner(mode: a.Mode) -> None:
    if mode == a.Mode.INTERPRETER and sys.version_info < (3, 14):
        pytest.skip()

    foo_exe = a.create(mode, Foo, 3, str_value="hello")
    exe_name = foo_exe.__class__.__name__

    # Test method call:
    num = await a.run(foo_exe.get_num)
    assert num == 4

    # Test method call with argument:
    str_value = await a.run(foo_exe.get_str, "!")
    assert str_value == "hello!"

    # Test method call with keyword argument:
    str_value = await a.run(foo_exe.get_str, postfix="!")
    assert str_value == "hello!"

    # Test returning a container:
    foo_list = await a.run(foo_exe.get_list)
    assert foo_list == [1, 2, 3]
    # The list container is shared when using threads:
    foo_list[1] = 7
    foo_list = await a.run(foo_exe.get_list)
    if mode == a.Mode.THREAD:
        assert foo_list == [1, 7, 3]
    else:
        assert foo_list == [1, 2, 3]

    # Instance attribute are not visible until they are attached:
    with pytest.raises(AttributeError) as ex:
        await a.set_value(foo_exe.foo, Foo(0, ""))
    assert str(ex.value) == f"'{exe_name}' object has no attribute 'foo'"

    # Test attaching sub-object instance attribute:
    await a.attach_object(foo_exe, "foo")
    num = await a.run(foo_exe.foo.get_num)
    assert num == 3
    await a.attach_object(foo_exe.foo, "foo")
    num = await a.run(foo_exe.foo.foo.get_num)
    assert num == 2

    # Test attaching value instance attribute:
    await a.attach_value(foo_exe, "num")
    await a.set_value(foo_exe.num, 7)
    num = await a.run(foo_exe.get_num)
    assert num == 8
    num = await a.get_value(foo_exe.num)
    assert num == 8

    await a.attach_value(foo_exe, "list")
    await a.set_value(foo_exe.list, [7, 7, 7])
    foo_list = await a.get_value(foo_exe.list)
    assert foo_list == [7, 7, 7]

    # Test get value for properties:
    await a.attach_value(foo_exe, "str_with_num")
    str_with_num = await a.get_value(foo_exe.str_with_num)
    assert str_with_num == "hello:8"
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
        "Can only run an executor method. Got: <test_runner.Foo object"
    )

    with pytest.raises(TypeError) as ex:
        await a.attach_object(foo, "str_with_num")
    assert str(ex.value).startswith(
        "Can only attach to executor object. Got: <test_runner.Foo object"
    )

    with pytest.raises(TypeError) as ex:
        await a.attach_value(foo, "str_with_num")
    assert str(ex.value).startswith(
        "Can only attach to executor object. Got: <test_runner.Foo object"
    )

    with pytest.raises(TypeError) as ex:
        await a.set_value(foo.num, 3)
    assert str(ex.value) == "Can only set an executor value. Got: 3"

    with pytest.raises(TypeError) as ex:
        await a.get_value(foo.num)
    assert str(ex.value) == "Can only get an executor value. Got: 3"

    with pytest.raises(TypeError) as ex:
        a.shutdown(foo)
    assert str(ex.value).startswith(
        "Can only shutdown executor object. Got: <test_runner.Foo object"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", list(a.Mode))
async def test_value_errors(mode: a.Mode) -> None:
    if mode == a.Mode.INTERPRETER and sys.version_info < (3, 14):
        pytest.skip()

    foo_exe = a.create(mode, Foo, 3, str_value="hello")
    exe_name = foo_exe.__class__.__name__

    with pytest.raises(
        TypeError,
        match=rf"Can only set an executor value. Got: "
        rf"<asyncrunner.{exe_name} object at",
    ):
        # set_value type annotations cannot state that this is a typing error
        # (see comment in code.)
        await a.set_value(foo_exe, 3)

    with pytest.raises(
        TypeError,
        match=rf"Can only get an executor value. Got: "
        rf"<asyncrunner.{exe_name} object at",
    ):
        await a.get_value(foo_exe)

    with pytest.raises(
        AttributeError,
        match=r"Cannot set executor attribute directly, key: num: "
        rf"<asyncrunner.{exe_name} object at",
    ):
        foo_exe.num = 7

    # Python builtins and stdlib classes cannot be attached:
    with pytest.raises(
        TypeError,
        match=r"descriptor 'append' for 'array.array' objects doesn't apply "
        rf"to a '{exe_name}' object",
    ):
        await a.attach_object(foo_exe, "arr")


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", list(a.Mode))
async def test_pickles(mode: a.Mode) -> None:
    if mode == a.Mode.INTERPRETER and sys.version_info < (3, 14):
        pytest.skip()

    foo_exe = a.create(mode, Foo, 3, str_value="hello")

    await a.attach_object(foo_exe, "foo")
    # Create a file handle that cannot be pickled:
    await a.run(foo_exe.foo.open_file, pathlib.Path("pyproject.toml"))
    await a.attach_value(foo_exe.foo, "file")
    match mode:
        case a.Mode.THREAD:
            file = await a.get_value(foo_exe.foo.file)
            assert file is not None
            assert file.name == "pyproject.toml"
        case a.Mode.PROCESS | a.Mode.INTERPRETER:
            pickle_error_message = (
                "cannot pickle 'TextIOWrapper' instances"
                if mode == a.Mode.PROCESS
                else "object does not support cross-interpreter data"
            )
            with pytest.raises(TypeError, match=pickle_error_message):
                await a.get_value(foo_exe.foo.file)
        case _:
            raise AssertionError

    # Replace the non-picklable foo with a picklable one:
    await a.run(foo_exe.replace_foo, Foo(-77, "world"))
    # This still does not prevent the exception:
    match mode:
        case a.Mode.THREAD:
            file = await a.get_value(foo_exe.foo.file)
            assert file is not None
            assert file.name == "pyproject.toml"
        case a.Mode.PROCESS | a.Mode.INTERPRETER:
            with pytest.raises(TypeError, match=pickle_error_message):
                await a.get_value(foo_exe.foo.file)
        case _:
            raise AssertionError
    # For replace_foo to take effect, we need to re-attach foo:
    await a.attach_object(foo_exe, "foo")
    await a.attach_value(foo_exe.foo, "file")
    file = await a.get_value(foo_exe.foo.file)
    assert file is None

    # Attaching a value to a previously attached object, makes the object inaccessible:
    await a.attach_value(foo_exe, "foo")
    with pytest.raises(TypeError) as ex:
        await a.attach_value(foo_exe.foo, "foo")
    exe_name = foo_exe.foo.__class__.__name__
    assert str(ex.value).startswith(
        f"Can only attach to executor object. Got: <asyncrunner.{exe_name}"
    )
    # Attaching an objecy to a previously attached value, makes the value inaccessible:
    await a.attach_object(foo_exe, "foo")
    with pytest.raises(TypeError) as ex:
        await a.set_value(foo_exe.foo, Foo(6, "Now"))
    exe_name = foo_exe.foo.__class__.__name__
    assert str(ex.value).startswith(
        f"Can only set an executor value. Got: <asyncrunner.{exe_name} object at"
    )
