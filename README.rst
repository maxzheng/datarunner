cocaller
========

Easily build workflows to call plain Python code that encourages reuse and testing.

Quick Start Tutorial
====================

Install using pip::

    pip install cocaller

Then write a few steps (functions, classes, etc) that can be called, pass to `cocaller.Workflow`, and call run():

.. code-block:: python

    from cocaller import Workflow, Step


    def setup():
        print('Ready to go!')

    def extract():
        return 'data'

    def transform(data):
        return data + ' using reusable lego pieces.'

    class Load(Step):
        """ Sub-class Step to customize the callable """
        def __init__(self, to_dataset):
            self.to_dataset = to_dataset

        def __str__(self):
            return f'Load("{self.to_dataset}")'

        def run(self, data):
            print(f'Loading {data}')


    Workflow(setup,
             etl=[extract, transform, Load('example')]).run()

It should produce the following output::

    setup
    Ready to go!

    etl
      extract
      >> transform
      >> Load("example")
    Loading data using reusable lego pieces.


Links & Contact Info
====================

| PyPI Package: https://pypi.python.org/pypi/cocaller
| GitHub Source: https://github.com/maxzheng/cocaller
| Report Issues/Bugs: https://github.com/maxzheng/cocaller/issues
|
| Engineer: https://www.linkedin.com/in/maxzheng
