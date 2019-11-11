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

It produces the following output::

    setup
    Ready to go!

    table_name1
    --------------------------------------------------------------------------------
    extract
    >> transform
    >> Load("example")
    Loading data using reusable code pieces, like Lego.

We can also use `>>` operator to convey the same flow:

.. code-block:: python

    flow = (Workflow()
            >> setup

            << 'table_name1'
            >> extract >> transform >> Load('example'))
    flow.run()

To make the workflow more flexible (e.g. write to different dataset), use templates to provide some values at run time:

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

And finally, to test the workflow::

   def test_flow():
      assert """\
   extract
   >> transform
   >> Load("{dataset}.table_name1")""" == str(flow)

Workflow Layout
===============

A recommended file layout for your ETL package::

   my_package/steps/__init__.py            # Generic / common steps
   my_package/steps/bigquery.py            # Group of steps for a specific service, like BigQuery.
   my_package/datasource1.py               # ETL workflow for a single data source with steps specifc for the source
   my_package/datasource2.py               # ETL workflow for another data source

Inside of `datasource*.py`, it should define `flow = Workflow(...)`, but not run. From your ETL script, it should call
`flow.run()` to run the workflow. This ensures the workflow is properly constructed when imported and can be used for
testing without running it.

Links & Contact Info
====================

| PyPI Package: https://pypi.org/project/datarunner/
| GitHub Source: https://github.com/maxzheng/datarunner
| Report Issues/Bugs: https://github.com/maxzheng/datarunner/issues
|
| Creator: https://www.linkedin.com/in/maxzheng
