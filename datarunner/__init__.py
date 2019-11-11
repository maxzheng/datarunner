class Step:
    """ Abstract step that is callable, runnable, and iterable """
    #: List of attributes that is using templates. The template will be replaced with an actual value before running.
    TEMPLATE_ATTRS = []

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

    def _replace_templates(self, replacements):
        """
        Replace templated attributs with their values

        :param dic replacements: Map of template attribute to value
        """
        for attr in self.TEMPLATE_ATTRS:
            attribute = getattr(self, attr)
            if isinstance(attribute, str):
                setattr(self, attr, attribute.format(**replacements))

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

    def __str__(self):
        result = []

        if self._name:
            result.append(self._name)
            result.append('-' * 80)

        for i, step in enumerate(self):
            prefix = '>> ' if i else ''
            result.append(prefix + step.name)

        return '\n'.join(result)

    @property
    def name(self):
        return self._name or len(self) and self[0].name or None

    def run(self, **replacements):
        """
        Run all the steps

        :param dict replacements: Replacement values for template variables.
        """
        result = None

        for i, step in enumerate(self):
            step._replace_templates(replacements)

            if i == 0:
                result = step()

            else:
                print('>> ', end='')
                result = step(result)

        return result


class Workflow(Flow):
    """ Simple data workflow engine that helps you write better ETL scripts  """
    def __init__(self, *steps, **flows):
        """
        :param list steps: List of steps (any callable) to run
        :param dict flows: Map of name to list of steps
        """
        super().__init__()
        self.flows = []

        for step in steps:
            if isinstance(step, Step):
                self.flows.append(Flow(step))
            elif isinstance(step, Flow):
                self.flows.append(step)
            elif callable(step):
                self.flows.append(Flow(Step(step)))
            else:
                self.flows.append(Flow(*step))

        for name, flow in flows.items():
            self.flows.append(Flow(*flow, name=name))

    def __str__(self):
        if len(self):
            self.flows.append(Flow(*self))

        result = []

        for i, flow in enumerate(self.flows):
            if i:
                result.append('')
            result.append(str(flow))

        return '\n'.join(result) + '\n'

    def run(self, **replacements):
        """
        Run all the steps

        :param dict replacements: Replacement values for template variables.
        """
        if len(self):
            self.flows.append(Flow(*self))

        result = None

        for i, flow in enumerate(self.flows):
            if i:
                print()
            result = flow(**replacements)

        return result
