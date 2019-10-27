import setuptools


setuptools.setup(
    name='datarunner',
    version='1.1.1',

    author='Max Zheng',
    author_email='maxzheng.os@gmail.com',

    description='A simple data workflow runner that helps you write better ETL scripts using reusable code pieces.',
    long_description=open('README.rst').read(),

    url='https://github.com/maxzheng/datarunner',

    install_requires=open('requirements.txt').read(),

    license='MIT',

    packages=setuptools.find_packages(),
    include_package_data=True,

    python_requires='>=3.6',
    setup_requires=['setuptools-git', 'wheel'],

    # entry_points={
    #    'console_scripts': [
    #        'script_name = package.module:entry_callable',
    #    ],
    # },

    # Standard classifiers at https://pypi.org/classifiers/
    classifiers=[
      'Development Status :: 5 - Production/Stable',

      'Intended Audience :: Developers',
      'Topic :: Software Development :: Libraries :: Python Modules',

      'License :: OSI Approved :: MIT License',

      'Programming Language :: Python :: 3',
      'Programming Language :: Python :: 3.6',
      'Programming Language :: Python :: 3.7',
    ],

    keywords='run call code workflow etl extract transform load',
)
