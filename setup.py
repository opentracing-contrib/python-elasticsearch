from setuptools import setup

version = open('VERSION').read()
setup(
    name='elasticsearch_opentracing',
    version=version,
    url='https://github.com/opentracing-contrib/python-elasticsearch/',
    download_url='https://github.com/opentracing-contrib/python-elasticsearch/tarball/'+version,
    license='Apache License 2.0',
    author='Carlos Alberto Cortez',
    author_email='calberto.cortez@gmail.com',
    description='OpenTracing support for Elasticsearch',
    long_description=open('README.rst').read(),
    packages=['elasticsearch_opentracing'],
    platforms='any',
    install_requires=[
        'elasticsearch',
        'opentracing>=2.0,<2.1'
    ],
    tests_require=[
        'elasticsearch-dsl>=5.1',
        'mock'
    ],
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ]
)
