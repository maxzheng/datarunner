class ARC:
    """ Abstract Runnable/Callable (ARC) that provides a simple call interface """
    def __call__(self, *args, **kwargs):
        return self.run(*args, **kwargs)

    def run(self):
        raise NotImplementedError('Sub-class must implement')


class Cocaller(ARC):
    """ Manages calling of reusable code """
    def __init__(self, *nameless_callables, **named_callables):
        """
        :param list nameless_callables: List of callable code with name defaulting to the name of the callable itself.
        :param dict named_callables: List of callable code with a given name.
        """
        self.nameless_callables = nameless_callables
        self.named_callables = named_callables

    def _guess_name(self, call):
        """ Guess the name of the callable """
        if isinstance(call, (tuple, list)):
            call = call[0]

        for name_attr in ('_mock_name', 'name', '__name__'):
            if hasattr(call, name_attr):
                return getattr(call, name_attr)

        name = str(call)

        if 'object at 0x' in name:
            if hasattr(call, '__class__'):
                return call.__class__.__name__

        return name

    def run(self):
        """ Loop thru the nameless and named callables and run each callable/series """
        result = None

        for call in self.nameless_callables:
            result = self._do(call)

        for name, call in self.named_callables.items():
            result = self._do(call, name=name)

        return result

    def _do(self, call, name=None):
        """
        Perform a call or a series of calls.

        :param callable|list call: A single callable to call or a list of callables to call in serial. If a list is
                                   provided, the return value from the 1st call will be passed to the 2nd, and
                                   same for subsequent calls.
        :param str name: Descriptive name for the call. If not provided, it will be guessed based on the callable name.
        :return: Result from the last call
        """
        result = None

        if not name:
            name = self._guess_name(call)

        if name:
            print(name)
            indent = '  '
        else:
            indent = ''

        if callable(call):
            result = call()

        elif isinstance(call, (tuple, list)):
            for i, c in enumerate(call):
                if i == 0:
                    print(indent, self._guess_name(c), sep='')
                    result = c()

                else:
                    print(indent + '>>', self._guess_name(c))
                    result = c(result)

        else:
            raise ValueError(f'Expected a callable or list of callables, but got: {call}')

        return result
