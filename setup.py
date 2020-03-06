from setuptools import setup, find_packages

# Manually kept in sync with loadmgr.__init__.py master_version
version = "2.2.1"

setup(
    name='pyloadmgr',
    version=version,
    packages=find_packages(),
    url='',
    author='wjdecorte',
    author_email='jdecorte@decorteindustries.com',
    description='Load Manager Workflow Engine',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU General Public License (GPL)',
        'Operating System :: Unix',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 2 :: Only'
    ],
    install_requires=[
        'config>=0.3.9',
        'docopt>=0.6.2',
        'functools32>=3.2.3.post2',
        'hdfs>=2.0.16',
        'impyla>=0.14.0',
        'Jinja2>=2.9.6',
        'jsonschema>=2.6.0',
        'MarkupSafe==1.0',
        'peewee>=2.8.2',
        'psycopg2>=2.7.1',
        'pycrypto>=2.6.1',
        'python-dateutil>=2.6.0',
        'requests>=2.13.0',
        'requests-kerberos>=0.11.0',
        'sasl>=0.2.1',
        'six>=1.10.0',
        'thrift>=0.9.3',
        'thrift-sasl>=0.2.1',
        'pswdfile==3.0.0'
    ],
    include_package_data=True,
    entry_points = {
        'console_scripts': [
            'loadmgr = loadmgr.cli:main'
        ]
    },
    scripts=[
      'loadmgr/lmgrworker.py'
    ]
)
