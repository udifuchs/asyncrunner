"""Interface for running synchronous routines in python async context."""
# Copyright 2026 Udi Fuchs

import asyncio
import concurrent.futures
import enum
import functools
import sys
import types
from collections.abc import Callable
from typing import ClassVar, cast

PICKLABLE = int, float, complex, str, bytes, bytearray


class _Worker:
    """Worker handles the context for subprocess and interpreter Executors.

    Worker should be initialised with a class. An instance of this class is stored
    as a class variable. This instance is global to the process/interperter, but since
    since each Worker runs in its own subprocess/interperter,
    the instance is local to the Worker.
    """

    _obj_dict: ClassVar[dict[str, object]] = {}

    @staticmethod
    def _register_instance(key: str, instance: object) -> None:
        _Worker._obj_dict[key] = instance

    @staticmethod
    def register_class[T](key: str, klass: Callable[[], T]) -> None:
        _Worker._register_instance(key, klass())

    @staticmethod
    def attach(obj_name: str, attr_name: str) -> type:
        instance = _Worker._obj_dict[obj_name]
        new_obj_name = f"{obj_name}.{attr_name}"
        new_instance = getattr(instance, attr_name)
        if isinstance(new_instance, PICKLABLE):
            _Worker._register_instance(new_obj_name, (instance, attr_name))
        else:
            _Worker._register_instance(new_obj_name, new_instance)
        return cast(type, new_instance.__class__)

    @staticmethod
    def run[T, **P](obj_name: str, partial: functools.partial[T]) -> T:
        func = partial.func
        args = partial.args
        kwargs = partial.keywords
        instance = _Worker._obj_dict[obj_name]
        if isinstance(instance, tuple):
            # This should never happen.
            raise TypeError(f"Cannot run {obj_name}")  # pragma: no cover
        method = cast(Callable[P, T], types.MethodType(func, instance))
        return method(*args, **kwargs)

    @staticmethod
    def set_value[T](obj_name: str, value: T) -> None:
        attribute = _Worker._obj_dict[obj_name]
        if isinstance(attribute, tuple):
            parent, obj_name = attribute
            setattr(parent, obj_name, value)
        else:
            # TRY004 Prefer `TypeError` exception for invalid type
            # ruff: disable[TRY004]
            raise ValueError(f"Cannot set value to executor: {attribute!r}")
            # ruff: enable[TRY004]

    @staticmethod
    def get_value(obj_name: str) -> object:
        attribute = _Worker._obj_dict[obj_name]
        if isinstance(attribute, tuple):
            parent, obj_name = attribute
            attribute = getattr(parent, obj_name)
        return attribute


class _Executor[T, **P]:
    """Executor for running async tasks in an isolated context."""

    def __init__(
        self, cls: Callable[P, T], name: str, executor: concurrent.futures.Executor
    ) -> None:
        self._cls = cls
        self._name = name
        if cls not in PICKLABLE:
            for funcname in dir(cls):
                if funcname.startswith("_"):
                    continue
                func = getattr(cls, funcname)
                if not callable(func):
                    continue
                object.__setattr__(self, funcname, func.__get__(self, cls))
        self._loop = asyncio.get_running_loop()
        self._executor = executor

    async def _attach(self, attr_name: str) -> None:
        raise NotImplementedError

    async def _run[TT, **PP](
        self, func: Callable[PP, TT], *args: PP.args, **kwargs: PP.kwargs
    ) -> TT:
        raise NotImplementedError

    async def _set_value(self, value: T) -> None:
        raise NotImplementedError

    async def _get_value(self) -> T:
        raise NotImplementedError

    def __setattr__(self, key: str, value: object) -> None:
        if key[0] != "_":
            raise AttributeError(
                f"Cannot set executor attribute directly, key: {key}: {self!r}"
            )
        super().__setattr__(key, value)


class _ProcessExecutor[T, **P](_Executor[T, P]):
    """Subprocess executor for running async tasks in an isolated context."""

    async def _attach(self, attr_name: str) -> None:
        obj_name = self._name
        klass = await self._loop.run_in_executor(
            self._executor, _Worker.attach, obj_name, attr_name
        )
        sub_instance = _ProcessExecutor[T, P](
            klass, f"{obj_name}.{attr_name}", self._executor
        )
        object.__setattr__(self, attr_name, sub_instance)

    async def _run[TT, **PP](
        self, func: Callable[PP, TT], *args: PP.args, **kwargs: PP.kwargs
    ) -> TT:
        method = cast(types.MethodType, func)
        obj_name = self._name
        func_with_args = functools.partial(method.__func__, *args, **kwargs)
        return await self._loop.run_in_executor(
            self._executor, _Worker.run, obj_name, func_with_args
        )

    async def _set_value(self, value: T) -> None:
        obj_name = self._name
        await self._loop.run_in_executor(
            self._executor, _Worker.set_value, obj_name, value
        )

    async def _get_value(self) -> T:
        obj_name = self._name
        ret_val = await self._loop.run_in_executor(
            self._executor, _Worker.get_value, obj_name
        )
        return cast(T, ret_val)


class _InterpreterExecutor[T, **P](_ProcessExecutor[T, P]):
    """Subinterpreter executor for running async tasks in an isolated context."""

    async def _attach(self, attr_name: str) -> None:
        obj_name = self._name
        klass = await self._loop.run_in_executor(
            self._executor, _Worker.attach, obj_name, attr_name
        )
        sub_instance = _InterpreterExecutor[T, P](
            klass, f"{obj_name}.{attr_name}", self._executor
        )
        object.__setattr__(self, attr_name, sub_instance)


class _ThreadExecutor[T, **P](_Executor[T, P]):
    """Thread executor for running async tasks in an isolated context."""

    def __init__(
        self,
        cls: Callable[P, T],
        name: str,
        executor: concurrent.futures.Executor,
        parent: object | None,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> None:
        cls2 = cast(type[T], cls)
        super().__init__(cls2, name, executor)
        self._parent = parent
        self._args = args
        self._kwargs = kwargs
        self._instance: T | None = None

    async def _get_instance(self) -> T:
        if self._instance is None:
            func_with_args = functools.partial(self._cls, *self._args, **self._kwargs)
            self._instance = await self._loop.run_in_executor(
                self._executor, func_with_args
            )
        return self._instance

    async def _attach(self, attr_name: str) -> None:
        instance = await self._get_instance()
        new_instance = await self._loop.run_in_executor(
            self._executor, getattr, instance, attr_name
        )
        klass = cast(type, new_instance.__class__)
        sub_instance = _ThreadExecutor(klass, attr_name, self._executor, instance)
        sub_instance._instance = new_instance
        object.__setattr__(self, attr_name, sub_instance)

    async def _run[TT, **PP](
        self, func: Callable[PP, TT], *args: PP.args, **kwargs: PP.kwargs
    ) -> TT:
        method = cast(types.MethodType, func)
        instance = await self._get_instance()
        method = types.MethodType(method.__func__, instance)
        func_with_args = functools.partial(method, *args, **kwargs)
        return await self._loop.run_in_executor(self._executor, func_with_args)

    async def _set_value(self, value: T) -> None:
        if self._parent is None:
            raise ValueError(f"Cannot set value to executor: {self!r}")
        await self._loop.run_in_executor(
            self._executor, setattr, self._parent, self._name, value
        )

    async def _get_value(self) -> T:
        if self._parent is None:
            return await self._get_instance()
        ret_val = await self._loop.run_in_executor(
            self._executor, getattr, self._parent, self._name
        )
        return cast(T, ret_val)


def create_thread[T, **P](
    klass: Callable[P, T], *args: P.args, **kwargs: P.kwargs
) -> T:
    """Create a thread executor with an instance of the specified class.

    The created executor would include all the method of that class.

    The class will only be initiated when you tell it to run something.
    """
    executor = concurrent.futures.ThreadPoolExecutor(
        max_workers=1,
    )
    exe_instance = _ThreadExecutor(klass, "root", executor, None, *args, **kwargs)
    return cast(T, exe_instance)


def create_process[T, **P](
    klass: Callable[P, T], *args: P.args, **kwargs: P.kwargs
) -> T:
    """Create a subprocess executor with an instance of the specified class.

    The created executor would include all the method of that class.

    The worker process will only be created when you tell it to run something.
    """
    func_with_args = functools.partial(klass, *args, **kwargs)
    executor = concurrent.futures.ProcessPoolExecutor(
        max_workers=1,
        initializer=_Worker.register_class,
        initargs=("root", func_with_args),
    )
    instance = _ProcessExecutor(klass, "root", executor)
    return cast(T, instance)


if sys.version_info >= (3, 14):

    def create_interpreter[T, **P](
        klass: Callable[P, T], *args: P.args, **kwargs: P.kwargs
    ) -> T:
        """Create an interpreter executor with an instance of the specified class.

        The created executor would include all the method of that class.

        The worker interpreter will only be created when you tell it to run something.
        """
        func_with_args = functools.partial(klass, *args, **kwargs)
        executor = concurrent.futures.InterpreterPoolExecutor(
            max_workers=1,
            initializer=_Worker.register_class,
            initargs=("root", func_with_args),
        )
        instance = _InterpreterExecutor(klass, "root", executor)
        return cast(T, instance)


class Mode(enum.Enum):
    """Concurrency operation modes for the executor."""

    THREAD = "thread"
    PROCESS = "process"
    INTERPRETER = "interpreter"


def create[T, **P](
    mode: Mode, klass: Callable[P, T], *args: P.args, **kwargs: P.kwargs
) -> T:
    """Create an executor with an instance of the specified class."""
    match mode:
        case Mode.THREAD:
            return create_thread(klass, *args, **kwargs)
        case Mode.PROCESS:
            return create_process(klass, *args, **kwargs)
        case Mode.INTERPRETER:
            return create_interpreter(klass, *args, **kwargs)
        case _:
            raise AssertionError


# ruff: disable[SLF001]  # private-member-access


async def attach(instance: object, attr_name: str) -> None:
    """Attach a class attribute to an executor.

    The new attribute is another executor with the same context as the
    parent executor.
    """
    if not isinstance(instance, _Executor):
        raise TypeError(f"Can only attach to existing executor. Got: {instance!r}")
    await instance._attach(attr_name)


async def run[T, **P](func: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> T:
    """Run a method in an executor context."""
    method = cast(types.MethodType, func)
    instance = method.__self__
    if not isinstance(instance, _Executor):
        raise TypeError(f"Can only run an executor method. Got: {instance!r}")
    return await instance._run(func, *args, **kwargs)


async def set_value[T](attribute: T, value: T) -> None:
    """Set attribute value in an executor context.

    This only works for picklable values.
    """
    if not isinstance(attribute, _Executor):
        raise TypeError(f"Can only set an executor attribute. Got: {attribute!r}")
    await attribute._set_value(value)


async def get_value[T](attribute: T) -> T:
    """Get attribute value from an executor context.

    This only works for picklable values.
    """
    if not isinstance(attribute, _Executor):
        raise TypeError(f"Can only get an executor attribute. Got: {attribute!r}")
    ret_val = await attribute._get_value()
    return cast(T, ret_val)


def shutdown(instance: object) -> None:
    """Shutdown an executor."""
    if not isinstance(instance, _Executor):
        raise TypeError(f"Can only shutdown executor object. Got: {instance!r}")
    instance._executor.shutdown()


# ruff: enable[SLF001]
