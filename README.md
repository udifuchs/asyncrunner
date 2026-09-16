asyncrunner
===========

**Interface for running synchronous routines in python's async context**

Generally, synchronous routines should not run in async context because the synchronous routines would block the async event loop.
`asyncrunner` creates an isolated executor context in which these routines can run.

For example:
```python
import time
import asyncrunner as a


class Bar:
    """A synchronous class"""
    def __init__(self, name: str) -> None:
        self.name = name

    def get_name(self) -> str:
        return self.name


class Foo:
    """Another synchronous class"""
    def __init__(self, num: int, name: str) -> None:
        time.sleep(1)
        self.num = num
        self.bar = Bar(name)

    def get_num(self) -> int:
        """A blocking method"""
        self.num += 1
        time.sleep(1)
        return self.num


async def spam() -> None:
    # Instead of creating an object using:
    # foo = Foo(3, "Joe)
    # we create an encapsulated object in a thread executor:
    foo_exe = a.create(a.Mode.THREAD, Foo, 3, "Joe")

    # Now we can call Foo's methods without blocking:
    assert await a.run(foo_exe.get_num) == 4
    assert await a.run(foo_exe.get_num) == 5

    # Accessing Foo's attributes as values requires attaching them first:
    await a.attach_value(foo_exe, "num")
    assert await a.get_value(foo_exe.num) == 5
    await a.set_value(foo_exe.num, 0)

    # Foo's attributes can also be attached as objects with their own methods:
    await a.attach_object(foo_exe, "bar")
    assert await a.run(foo_exe.bar.get_name) == "Joe"
```

The `a.create` routine returns an executor that encapsulates an instance of the `Foo` class.
This executor has the same methods as the original instance (only `get_num` in this example).
But these methods can only be accessed using the asynchronous `a.run` coroutine.
This guarantees that these methods would not block the event loop.

Initially, the executor has access to the class methods, but not to the instance attributes
(`self.num` and `self.bar` in this case).
Instance attributes can be attached to the executor using the `a.attach_value` coroutine.
Then `a.set_value` and `a.get_value` can be used to access these attributes.

Instance attributes can also be attached using `a.attach_object`.
This enables running the attributes methods using `a.run`.

The class instance is hidden inside the executor.
Actually, `a.create` avoids creating this instance since the `__init__` method could be blocking.
The instance is created on the first `a.run`, `a.attach_object` or `a.attach_value` call on the executor.

The instance methods are always executed in the same dedicated thread.
This reduces the chances of concurrency issues due to accessing the same data from different threads.
This risk is not eliminated, since you can still pass containers as arguments or return values.
These containers will be shared between the main thread and the executor thread.

Instead of using threads to manage concurrency, it is possible to use a sub-process or a sub-interpreter, using `a.Mode.PROCESS` and `a.Mode.INTERPRETER` respectively.
`asyncrunner` API is exactly the same for all these concurrency models.
There are some subtle behavior differences between threads and sub-processes/interpreters due to the fact that the latter are completely issolated.

All data passed to sub-processes/interpreters is pickled.
Non-picklable data, such as file handlers, cannot be passed.
Containers, such as lists, can be passed, if all their content is picklable.
But the other end receives a copy of the container.
Therefore, the containers are completely issolated as opposed to the case of threads where the containers were shared.

`asyncrunner` has a test suite with 100% coverage.
Hopefully, this test suite covers all the corner cases that this package could encounter.

Internally, concurrency is managed with [concurrent.futures.Executor](https://docs.python.org/3/library/concurrent.futures.html) using an executor pool with a single worker.

Installation
------------

`asyncrunner` can be installed from [pypi](https://pypi.org):
```bash
pip3 install asyncrunner
```
`asyncrunner` requires python 3.12 or newer.
Using a sub-interpreter executor requires python 3.14 or newer.
There are no external dependencies.

History
-------
#### 0.1.0 (2026-09-15)

* Initial release.
