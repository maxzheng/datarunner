import inspect


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


class Flow(list):
    """ List of steps that will be run in serial with data being passed from one to another """
    def __init__(self, *steps, name=None):
        """
        :param list steps: List of steps for the flow
        :param str name: Name for the flow
        """
        if steps:
            self.extend([Step.instance(s) for s in steps])
        self._name = name

    def __rshift__(self, call):
        self.append(Step.instance(call))
        return self

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
        if self._name:
            print(self._name)
            print('-' * 80)

        result = None

        for i, step in enumerate(self):
            step._replace_templates(replacements)

            step_params = {}
            try:
                if replacements and callable(step.callable_or_obj):
                    candidate_names = [p.name for p in inspect.signature(step.callable_or_obj).parameters.values()
                                       if p.default != p.empty]
                    common_names = set(replacements) & set(candidate_names)
                    step_params = dict((k, v) for k, v in replacements.items() if k in common_names)

            except Exception:
                pass

            if i == 0:
                result = step(**step_params)

            else:
                print('>> ', end='')
                result = step(result, **step_params)

        return result


class Workflow(list):
    """ Simple data workflow engine that helps you write better ETL scripts  """
    def __init__(self, *steps, name=None, **flows):
        """
        :param list steps: List of steps (any callable) to run
        :param str name: Name for the flow when using ">>" operator.
        :param dict flows: Map of name to list of steps
        """
        if name and not isinstance(name, str):
            flows['name'] = name
            name = None

        self._flow = Flow(name=name)

        self.extend(*steps)

        for name, flow in flows.items():
            self.append(Flow(*flow, name=name))

    def __lshift__(self, name):
        self._merge_flow()
        self._flow._name = name
        return self

    def __rshift__(self, call):
        self._flow.append(Step.instance(call))
        return self

    def __str__(self):
        self._merge_flow()
        result = []

        for i, flow in enumerate(self):
            if i:
                result.append('')
            result.append(str(flow))

        return '\n'.join(result)

    def _merge_flow(self):
        if self._flow:
            self.append(self._flow)
            self._flow = Flow()

    def extend(self, *steps):
        self._merge_flow()
        for step in steps:
            if isinstance(step, Step):
                self.append(Flow(step))
            elif isinstance(step, Flow):
                self.append(step)
            elif callable(step):
                self.append(Flow(Step(step)))
            else:
                self.append(Flow(*step))
        return self

    def run(self, **replacements):
        """
        Run all the steps

        :param dict replacements: Replacement values for template variables.
        """
        self._merge_flow()
        result = None

        for key, value in list(replacements.items()):
            value = str(value)
            if '-' in value and (key + '_') not in replacements:
                replacements[key + '_'] = value.replace('-', '')

        for i, flow in enumerate(self):
            if i:
                print()
            result = flow.run(**replacements)

        return result
