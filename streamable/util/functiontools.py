from typing import Any, Callable, Coroutine, Generic, Tuple, Type, TypeVar

T = TypeVar("T")
R = TypeVar("R")


class reraise_as(Generic[T, R]):
    def __init__(self, func: Callable[[T], R], source: Type[Exception], target: Type[Exception]) -> None:
        self.func = func
        self.source = source
        self.target = target
    
    def __call__(self, arg: T) -> R:
        try:
            return self.func(arg)
        except self.source as e:
            raise self.target() from e



class sidify(Generic[T]):
    def __init__(self, func: Callable[[T], Any]) -> None:
        self.func = func
    
    def __call__(self, arg: T) -> T:
        self.func(arg)
        return arg


def async_sidify(
    func: Callable[[T], Coroutine]
) -> Callable[[T], Coroutine[Any, Any, T]]:
    async def wrap(arg: T) -> T:
        coroutine = func(arg)
        if not isinstance(coroutine, Coroutine):
            raise TypeError(
                f"The function is expected to be an async function, i.e. it must be a function returning a Coroutine object, but returned a {type(coroutine)}."
            )
        await coroutine
        return arg

    return wrap


class star(Generic[R]):
    def __init__(self, func: Callable[..., R]) -> None:
        self.func = func
    
    def __call__(self, args: Tuple) -> R:
        return self.func(*args)
