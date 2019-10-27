datarunner
==========

A simple data workflow runner that helps you write better ETL scripts using reusable code pieces.

Quick Start Tutorial
====================

Install using pip::

    pip install datarunner

Then write a few steps (functions, classes, etc) that can be called, pass to `datarunner.Workflow`, and call run():

.. code-block:: python

    from datarunner import Workflow, Step


    def setup():
        print('Ready to go!')

    def extract():
        return 'data'

    def transform(data):
        return data + ' using reusable code pieces, like Lego.'

    class Load(Step):
        """ Sub-class Step to customize the callable """
        def __init__(self, to_dataset):
            super().__init__()
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
    --------------------------------------------------------------------------------
    extract
    >> transform
    >> Load("example")
    Loading data using reusable code pieces, like Lego.

If we skip `setup`, then we can also use `>>` operator to convey the same flow:

.. code-block:: python

    flow = Workflow() >> extract >> transform >> Load('example')
    flow.run()

Links & Contact Info
====================

| PyPI Package: https://pypi.python.org/pypi/datarunner
| GitHub Source: https://github.com/maxzheng/datarunner
| Report Issues/Bugs: https://github.com/maxzheng/datarunner/issues
|
| Creator: https://www.linkedin.com/in/maxzheng
