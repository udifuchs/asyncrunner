"""Test utility classes for blue-backup."""
# Copyright 2026 Udi Fuchs

import pytest

import asyncrunner as a


class Foo:
    def __init__(self, num: int, str_value: str) -> None:
        self.num = num
        self.str = str_value

    def get_num(self) -> int:
        return self.num

    def get_str(self, postfix: str = "") -> str:
        return self.str + postfix


@pytest.mark.asyncio
async def test_thread() -> None:
    foo_exe = a.create_thread(Foo, 3, str_value="hello")

    num = await a.run(foo_exe.get_num)
    assert num == 3

    str_value = await a.run(foo_exe.get_str, "!")
    assert str_value == "hello!"

    str_value = await a.run(foo_exe.get_str, postfix="!")
    assert str_value == "hello!"

    with pytest.raises(AttributeError) as ex:
        await a.set_value(foo_exe.num, 7)
    assert str(ex.value) == "'_ThreadExecutor' object has no attribute 'num'"

    await a.attach(foo_exe, "num")
    await a.set_value(foo_exe.num, 7)
    num = await a.run(foo_exe.get_num)
    assert num == 7

    with pytest.raises(AttributeError) as ex:
        str_value = await a.run(foo_exe.no_such_method)  # type: ignore[attr-defined]
    assert str(ex.value) == "'_ThreadExecutor' object has no attribute 'no_such_method'"


@pytest.mark.asyncio
async def test_proc() -> None:
    foo_exe = a.create_process(Foo, 3, str_value="hello")

    num = await a.run(foo_exe.get_num)
    assert num == 3

    str_value = await a.run(foo_exe.get_str, "!")
    assert str_value == "hello!"

    str_value = await a.run(foo_exe.get_str, postfix="!")
    assert str_value == "hello!"

    with pytest.raises(AttributeError) as ex:
        await a.set_value(foo_exe.num, 7)
    assert str(ex.value) == "'_ProcessExecutor' object has no attribute 'num'"

    await a.attach(foo_exe, "num")
    await a.set_value(foo_exe.num, 7)
    num = await a.run(foo_exe.get_num)
    assert num == 7

    with pytest.raises(AttributeError) as ex:
        str_value = await a.run(foo_exe.no_such_method)  # type: ignore[attr-defined]
    assert (
        str(ex.value) == "'_ProcessExecutor' object has no attribute 'no_such_method'"
    )
