import asyncio
import functools
import inspect


def anysync_property(func):
    """ "
    A decorator to make a sync-defined class property be usable in any context (sync or async).

    Example usage e.g. in Django:
    class MyModel(Model):
        @anysync_property
        def my_property():
            return await self.related_objects.first()

    def my_function():
        m = Model.first()
        thing = m.my_property

    async def my_function():
        m = await Models.afirst()
        thing = await m.my_property
    """

    @property
    @functools.wraps(func)
    def wrapper(self):
        if inspect.iscoroutinefunction(func):
            raise TypeError(f"{func.__name__} must be defined as a sync function")

        try:
            asyncio.get_running_loop()  # This will raise RuntimeError if no loop is running
            return asyncio.to_thread(func, self)
        except RuntimeError:
            # We are in a sync context
            return func(self)

    return wrapper
