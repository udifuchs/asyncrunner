"""Subprocess executor for running async tasks with context."""

import asyncio
import concurrent.futures
import types
from collections.abc import Callable
from typing import ClassVar, cast

PICKLABLE = int, float, complex, str, bytes, bytearray


class _Worker:
    """Worker handles the context for subprocess Executor.

    Worker should be initialised with a class. An instance of this class is stored
    as a class variable. This instance is global to the process, but since
    since each Worker runs in its own subprocess, the instance is local to the Worker.
    """

    _obj_dict: ClassVar[dict[str, object]] = {}

    @staticmethod
    def _register_instance(key: str, instance: object) -> None:
        if key in _Worker._obj_dict:
            raise Exception(f"Worker already contains {key}")
        _Worker._obj_dict[key] = instance

    @staticmethod
    def register_class(
        key: str, klass: type, args: tuple[object, ...], kwargs: dict[str, object]
    ) -> None:
        _Worker._register_instance(key, klass(*args, **kwargs))

    @staticmethod
    def register_attr(obj_name: str, attr_name: str) -> type:
        instance = _Worker._obj_dict[obj_name]
        new_obj_name = f"{obj_name}.{attr_name}"
        new_instance = getattr(instance, attr_name)
        if isinstance(new_instance, PICKLABLE):
            _Worker._register_instance(new_obj_name, (instance, attr_name))
        else:
            _Worker._register_instance(new_obj_name, new_instance)
        return cast(type, new_instance.__class__)

    @staticmethod
    def run[T, **P](
        func: Callable[P, T], obj_name: str, *args: P.args, **kwargs: P.kwargs
    ) -> T:
        try:
            instance = _Worker._obj_dict[obj_name]
            if isinstance(instance, tuple):
                raise TypeError(f"Cannot run {obj_name}")  # noqa: TRY301
            meth = cast(Callable[P, T], types.MethodType(func, instance))
            ret_val = meth(*args, **kwargs)
        except BaseException as ex:
            print(f"Worker.run {func.__qualname__} exception: {ex!r}")
            raise
        else:
            return ret_val

    @staticmethod
    def set[T](obj_name: str, value: T) -> None:
        try:
            attribute = _Worker._obj_dict[obj_name]
            if isinstance(attribute, tuple):
                parent, obj_name = attribute
                setattr(parent, obj_name, value)
            else:
                raise TypeError(f"Cannot set {obj_name}")  # noqa: TRY301
        except BaseException as ex:
            print(f"Worker.set {obj_name} exception: {ex!r}")
            raise

    @staticmethod
    def get(obj_name: str) -> object:
        try:
            attribute = _Worker._obj_dict[obj_name]
            if isinstance(attribute, tuple):
                parent, obj_name = attribute
                attribute = getattr(parent, obj_name)
            else:
                raise TypeError(f"Cannot get {obj_name}")  # noqa: TRY301
        except BaseException as ex:
            print(f"Worker.get {obj_name} exception: {ex!r}")
            raise
        else:
            return attribute


class _Executor[T]:
    """Subprocess executor for running async tasks with context."""

    def __init__(
        self, cls: type[T], name: str, executor: concurrent.futures.Executor,
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
                setattr(self, funcname, func.__get__(self, cls))
        self._loop = asyncio.get_running_loop()
        self._executor = executor

    async def _register_attr(self, attr_name: str) -> None:
        obj_name = self._name
        klass = await self._loop.run_in_executor(
            self._executor, _Worker.register_attr, obj_name, attr_name
        )
        sub_instance = _Executor[T](klass, f"{obj_name}.{attr_name}", self._executor)
        setattr(self, attr_name, sub_instance)

    async def _run[TT, **PP](
        self, func: Callable[PP, TT], *args: PP.args, **kwargs: PP.kwargs
    ) -> TT:
        method = cast(types.MethodType, func)
        obj_name = self._name
        func = method.__func__
        return await self._loop.run_in_executor(
            self._executor, _Worker.run, func, obj_name, *args, **kwargs
        )


class _ThreadExecutor[T, **P](_Executor[T]):
    """Subprocess executor for running async tasks with context."""

    #def run[T, **P](
    def __init__(
        self, cls: Callable[P, T], name: str, executor: concurrent.futures.Executor,
        *args: P.args, **kwargs: P.kwargs
    ) -> None:
        cls2 = cast(type[T], cls)
        super().__init__(cls2, name, executor)
        self.args = args
        self.kwargs = kwargs
        self._instance: T | None = None

    async def _register_attr(self, attr_name: str) -> None:
        if self._instance is None:
            self._instance = await self._loop.run_in_executor(
                self._executor, self._cls, *self.args, **self.kwargs
            )
        new_instance = await self._loop.run_in_executor(
            self._executor, getattr, self._instance, attr_name
        )
        klass = cast(type, new_instance.__class__)
        sub_instance = _ThreadExecutor(klass, f".{attr_name}", self._executor)
        setattr(self, attr_name, sub_instance)

    async def _run[TT, **PP](
        self, func: Callable[PP, TT], *args: PP.args, **kwargs: PP.kwargs
    ) -> TT:
        method = types.MethodType(func, self._instance)
        return await self._loop.run_in_executor(
            self._executor, method, *args, **kwargs
        )

# ruff: disable[SLF001]  # private-member-access


def create_thread[T](klass: type[T], *args: object, **kwargs: object) -> T:
    executor = concurrent.futures.ThreadPoolExecutor(
        max_workers=1,
    )
    instance = klass(*args, **kwargs)
    exe_instance = _ThreadExecutor(klass, "root", executor, instance=instance)
    return cast(T, exe_instance)


def create[T](klass: type[T], *args: object, **kwargs: object) -> T:
    """Create an executor with an instance of the specified class.

    The created executor would include all the method of that class.

    Notice that the worker process will only be created when you tell it
    to run something.
    """
    executor = concurrent.futures.ProcessPoolExecutor(
        max_workers=1,
        initializer=_Worker.register_class,
        initargs=("root", klass, args, kwargs),
    )
    instance = _Executor(klass, "root", executor)
    return cast(T, instance)


async def attach(instance: object, attr_name: str) -> None:
    """Attach a class attribute to an executor.

    The new attribute is another executor with the same context as the
    parent executor.
    """
    if not isinstance(instance, _Executor):
        raise TypeError(f"Can only attach to existing executor. Got: {instance!r}")
    await instance._register_attr(attr_name)


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
    if attribute._instance is not None:
        await attribute._loop.run_in_executor(
            attribute._executor, setattr, attribute._instance, value
        )
    obj_name = attribute._name
    await attribute._loop.run_in_executor(
        attribute._executor, _Worker.set, obj_name, value
    )


async def get_value[T](attribute: T) -> T:
    """Get attribute value from an executor context.

    This only works for picklable values.
    """
    if not isinstance(attribute, _Executor):
        raise TypeError(f"Can only get an executor attribute. Got: {attribute!r}")
    obj_name = attribute._name
    ret_val = await attribute._loop.run_in_executor(
        attribute._executor, _Worker.get, obj_name
    )
    return cast(T, ret_val)


def shutdown(instance: object) -> None:
    """Shutdown an executor."""
    if not isinstance(instance, _Executor):
        raise TypeError("Can only shutdown executor object")
    instance._executor.shutdown()


# ruff: enable[SLF001]
