from setuptools import setup, find_packages

setup(
    name="cassandra-connector",
    version="0.3.0",
    packages=find_packages(),
    install_requires=[
        "cassandra-driver>=3.29.1",
        "requests>=2.25.1",
    ],
    python_requires='>=3.8',
    author="Phil Miesle",
    author_email="phil.miesle@datastax.com",
    description="Simplifies connecting to Cassandra and AstraDB when using the DataStax driver",
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    keywords="cassandra astra db connector",
    url="https://github.com/mieslep/cassandra-connector",
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],

)
