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
        def __init__(self, destination):
            super().__init__()
            self.destination = destination

        def __str__(self):
            return f'Load("{self.destination}")'

        def run(self, data):
            print(f'Loading {data}')

    flow = Workflow(setup,
                    table_name1=[extract, transform, Load('example')])
    flow.run()

It should produce the following output::

    setup
    Ready to go!

    table_name1
    --------------------------------------------------------------------------------
    extract
    >> transform
    >> Load("example")
    Loading data using reusable code pieces, like Lego.

If we skip `setup`, then we can also use `>>` operator to convey the same flow:

.. code-block:: python

    flow = Workflow() >> extract >> transform >> Load('example')
    flow.run()

We can take a step further by using templates to provide some information at run time:

.. code-block:: python

    class Load(Step):
        TEMPLATE_ATTRS = ['destination']

        """ Sub-class Step to customize the callable """
        def __init__(self, destination):
            super().__init__()
            self.destination = destination

        def __str__(self):
            return f'Load("{self.destination}")'

        def run(self, data):
            print(f'Loading {data}')

    flow = Workflow() >> extract >> transform >> Load('{dataset}.table_name1')
    flow.run(dataset='staging')

It produces the following output::

   extract
   >> transform
   >> Load("staging.table_name1")
   Loading data using reusable code pieces, like Lego.

Finally, to test the workflow::

   def test_flow():
      assert """
      extract
      >> transform
      >> Load("{dataset}.table_name1")
      """ == str(flow)

Links & Contact Info
====================

| PyPI Package: https://pypi.python.org/pypi/datarunner
| GitHub Source: https://github.com/maxzheng/datarunner
| Report Issues/Bugs: https://github.com/maxzheng/datarunner/issues
|
| Creator: https://www.linkedin.com/in/maxzheng
