cocaller
========

Manages the running of reusable code for building data pipelines.

Quick Start Tutorial
====================

Install using pip::

    pip install cocaller

And then write a few callables (functions, classes, etc) that can be called, pass to `Cocaller`, and call run():

.. code-block:: python

    from cocaller import Cocaller, ARC


    def setup():
        print('Ready to go!')

    def extract():
        return 'data'

    def transform(data):
        return data + ' using reusable lego pieces.'

    class Load(ARC):
        """ Sub-class ARC (Abstract Runnable/Callable) to customize the callable """
        def __init__(self, to_dataset):
            self.to_dataset = to_dataset

        def __str__(self):
            return f'Load("{self.to_dataset}")'

        def run(self, data):
            print(f'Loading {data}')


    cc = Cocaller(setup,
                  etl=[extract, transform, Load('example')])
    cc.run()

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
| Contact: https://www.linkedin.com/in/maxzheng
