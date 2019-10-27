class Step:
    """ Abstract step that is callable, runnable, and iterable """
    def __init__(self, callable_or_obj=None):
        """
        :param callable|object callable_or_obj: A callable to be called or an object to be returned when step is run.
        """
        if isinstance(callable_or_obj, Step):
            raise ValueError(f'Must be a callable or an object, but got another Step instance: {callable_or_obj}')

        self.callable_or_obj = callable_or_obj

    def __call__(self, *args, **kwargs):
        self._print_name()
        return self.run(*args, **kwargs)

    def __iter__(self):
        return iter(self())

    @classmethod
    def instance(cls, obj):
        return obj if isinstance(obj, cls) else Step(obj)

    @property
    def name(self):
        """ Guess the name of the callable """
        if not hasattr(self, 'callable_or_obj'):
            raise RuntimeError('Step is not properly initialized. Please call `super().__init__()` in '
                               f'`{self.__class__.__name__}.__init__()`')
        co = self.callable_or_obj or self
        name = str(co)

        # Name given by the developer
        if 'at 0x' not in name and 'Mock name' not in name and 'built-in ' not in name and '<class ' not in name:
            return name

        # Name attributes
        if self.callable_or_obj:
            for name_attr in ('_mock_name', 'name', '__name__'):
                if hasattr(co, name_attr):
                    name = getattr(co, name_attr)
                    if name:  # Ensure it actually has a value
                        return name

        # Class name
        if hasattr(co, '__class__'):
            return co.__class__.__name__

        # Last resort
        return name

    def _print_name(self):
        print(self.name)

    def run(self, *args, **kwargs):
        if self.callable_or_obj:
            return self.callable_or_obj(*args, **kwargs) if callable(self.callable_or_obj) else self.callable_or_obj

        raise NotImplementedError('Please provide a callable or object when instantiating, or override this method.')


class Flow(list, Step):
    """ List of steps that will be run in serial with data being passed from one to another """
    def __init__(self, *steps, name=None):
        """
        :param list steps: List of steps for the flow
        :param str name: Name for the flow
        """
        super().__init__()
        if steps:
            self.extend([Step.instance(s) for s in steps])
        self._name = name

    def __rshift__(self, call):
        self.append(Step.instance(call))
        return self

    def _print_name(self):
        if self._name:
            print(self._name)
            print('-' * 80)

    @property
    def name(self):
        return self._name or len(self) and self[0].name or None

    def run(self):
        result = None

        for i, step in enumerate(self):
            if i == 0:
                result = step()

            else:
                print('>> ', end='')
                result = step(result)

        return result


class Workflow(Flow):
    """ Pythonic workflow engine that helps you write better ETL scripts  """
    def __init__(self, *steps, **flows):
        """
        :param list steps: List of steps (any callable) to run
        :param dict flows: Map of name to list of steps
        """
        super().__init__()
        self.steps = []

        for step in steps:
            if isinstance(step, (Step, Flow)):
                self.steps.append(step)
            elif callable(step):
                self.steps.append(Step(step))
            else:
                self.steps.append(Flow(*step))

        for name, flow in flows.items():
            self.steps.append(Flow(*flow, name=name))

    def run(self):
        """ Run all the steps """
        if self.steps:
            result = None

            for i, step in enumerate(self.steps):
                if i and isinstance(step, Flow):
                    print()
                result = step()

            return result

        self._print_name()
        return super().run()
