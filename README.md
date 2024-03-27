# Cassandra Connector

This package provides a helpful wrapper around the `cassandra-driver` package with the following features:

1. References environment variables for seamless connections to Cassandra and DataStax Astra DB
2. ConnectionManager maintains a dictionary of cluster connections, facilitating connections to multiple 
   clusters in a single place.
3. Automatically downloads Astra secure connect bundles, based on provided endpoint.

## Installing

Until such time as this is published in PyPI, install from the GitHub repo directly:

```bash
pip install git+https://github.com/mieslep/cassandra_connector.git
```

## Using

The simplest approach is to set appropriate environment variables such as

```bash
ASTRA_DB_APPLICATION_TOKEN=AstraCS:<your token here>
ASTRA_DB_API_ENDPOINT=https://<your endpoint here>
```

and then:

```python
from cassandra_connector import CassandraConnectionsManager

cm = CassandraConnectionsManager()
session = cm.get_connector('env_astra').session
```

which will get a driver Session object based on the `ASTRA` environment variables. To get a session object based 
on `CASSANDRA` environment variables, pass in `env_cassandra` instead of `env_astra`. 

You can also pass in your own key and connection string, which is a JSON-formatted string:

```python
session = cm.get_connector('mydb', '{ "authProviderClass": "cassandra.auth.PlainTextAuthProvider", "authProviderArgs": {"username": "cassandra", "password": "cassandra"}, "contact_points": ["localhost"], "port": 9042 }').session
```

Docstrings are the best reference for more detailed usage for the connection arguments.

## Contributing

Contributions welcome, just fire off a pull request :) 

