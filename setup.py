from setuptools import setup, find_packages

setup(
    name="cassandra_connector",
    version="0.1.1",
    packages=find_packages(),
    install_requires=[
        "cassandra-driver>=3.29.1",
        "requests>=2.25.1",
    ],
    author="Phil Miesle",
    author_email="phil.miesle@datastax.com",
    description="A Python package to manage Cassandra and AstraDB connections.",
    keywords="cassandra astra db connector",
    url="https://github.com/mieslep/cassandra_connector",
)
