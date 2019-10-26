class Step:
    """ Abstract step that is callable, runnable, and iterable """
    def __call__(self, *args, **kwargs):
        return self.run(*args, **kwargs)

    def __iter__(self):
        return iter(self.run())

    def run(self):
        raise NotImplementedError('Sub-class must implement')


class Workflow(Step):
    """ Manages calling of reusable code """
    def __init__(self, *nameless_callables, **named_callables):
        """
        :param list nameless_callables: List of callable code with name defaulting to the name of the callable itself.
        :param dict named_callables: List of callable code with a given name.
        """
        self.nameless_callables = nameless_callables
        self.named_callables = named_callables

    @staticmethod
    def _guess_name(call):
        """ Guess the name of the callable """
        if isinstance(call, (tuple, list)):
            call = call[0]

        name = str(call)

        # Name given by the developer
        if 'at 0x' not in name and 'Mock name' not in name and 'built-in ' not in name and '<class ' not in name:
            return name

        # Name attributes
        for name_attr in ('_mock_name', 'name', '__name__'):
            if hasattr(call, name_attr):
                name = getattr(call, name_attr)
                if name:  # It could be blank...
                    return name

        # Class name
        if hasattr(call, '__class__'):
            return call.__class__.__name__

        # Last resort
        return name

    def run(self):
        """ Loop thru the nameless and named callables and run each callable/series """
        result = None

        for call in self.nameless_callables:
            result = self._do(call)

        for name, call in self.named_callables.items():
            result = self._do(call, name=name)

        return result

    def _do(self, call_or_calls, name=None):
        """
        Perform a call or a series of calls.

        :param callable|list call_or_calls: A single callable to call or a list of callables to call in serial. If a
                                            list is provided, the return value from the 1st call will be passed to the
                                            2nd, and same for subsequent calls.
        :param str name: Descriptive name for the call. If not provided, it will be guessed based on the callable name.
        :return: Result from the last call
        """
        result = None

        if callable(call_or_calls):
            if not name:
                name = self._guess_name(call_or_calls)
        else:
            print()

        if name:
            print(name)
            indent = '  '
        else:
            indent = ''

        if callable(call_or_calls):
            result = call_or_calls()

        elif isinstance(call_or_calls, (tuple, list)):
            for i, call in enumerate(call_or_calls):
                if i == 0:
                    print(indent, self._guess_name(call), sep='')
                    result = call()

                else:
                    print(indent + '>>', self._guess_name(call))
                    result = call(result)

        else:
            raise ValueError(f'Expected a callable or list of callables, but got: {call_or_calls}')

        return result
