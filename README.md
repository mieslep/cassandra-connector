# Cassandra Connector

This package provides a helpful wrapper around the `cassandra-driver` package with the following features:

1. References environment variables for seamless connections to Apache Cassandra® and DataStax® Astra DB
2. ConnectionManager maintains a dictionary of cluster connections, facilitating connections to multiple 
   clusters in a single place.
3. Automatically downloads Astra secure connect bundles, based on provided endpoint.

## Installing

```bash
pip install cassandra-connector
```

## Using

### Connecting to Astra

#### Connecting to Astra with Environment Variables

The simplest approach is to set appropriate environment variables:

```bash
ASTRA_DB_APPLICATION_TOKEN=AstraCS:<your token here>
ASTRA_DB_API_ENDPOINT=https://<your endpoint here>
```

and then:

```python
from cassandra_connector import CassandraConnectorManager

cm = CassandraConnectorManager()
astra = cm.get_connector('env_astra')
session = astra.session
```

which will get a driver Session object based on the `ASTRA` environment variables. 

#### Connecting to Astra Directly

You can bypass the `CassandraConnectorManager` and use the `CassandraConnector` directly by passing a keyword `dict`
to the `astra` parameter, including both `token` and `endpoint` parameters:

```python
from cassandra_connector import CassandraConnector

astra = CassandraConnector(astra={"token": "AstraCS:<your token here>", "endpoint": "https://<your endpoint here>"})
session = astra.session
```

### Connecting to Cassandra

Connecting to a Cassandra, DataStax Enterprise (DSE), or any other Cassandra-compatible cluster is similar to 
the driver connection, but as a single step invocation.

There are two parameters introduced with the `CassandraConnector`; these are used to construct the authentication 
provider object that is passed into the Cluster constructor:

* `authProviderClass` is a string representing the Python package of the provider (it must be `import`-able)
* `authProviderArgs` is a `dict` of keyword arguments that are passed to the provider class

All other arguments will be passed directly to the `Cluster` constructor.

**Note**: If you do not need an auth provider to connect, you may omit these parameters. This is generally not 
advised for any production enviroment as it means that anybody with access to your cluster can access the data 
within the cluster.

#### Connecting to Cassandra with Environment Variables

If you want to use `env_cassandra`, represent them as a single JSON document in the `CASSANDRA_CONNECTION` variable, for example:

```bash
CASSANDRA_CONNECTION={"authProviderClass": "cassandra.auth.PlainTextAuthProvider", "authProviderArgs": {"username": "cassandra", "password": "cassandra"}, "contact_points": ["localhost"], "port": 9042}
```

Once set, you can get a Session:

```python
from cassandra_connector import CassandraConnectorManager

cm = CassandraConnectorManager()
cassandra = cm.get_connector('env_cassandra')
session = cassandra.session
```

#### Connecting to Cassandra Directly

You can provide these same arguments directly to the `CassandraConnector`: 

```python
from cassandra_connector import CassandraConnector

cassandra = CassandraConnector(
    authProviderClass="cassandra.auth.PlainTextAuthProvider",
    authProviderArgs={"username": "cassandra", "password": "cassandra"},
    contact_points=["localhost"],
    port=9042)
session = cassandra.session
```

### The `CassandraConnectorManager` Object

In addition to being able to create `CassandraConnector` objects from environment variables, you can use the `get_connector` function
along with a `dict` key of your choosing (other than the above `env_*` keys). The first time a key is used, you must 
pass connection parameters as you would connecting "directly" as documented above, but subsequently you can simply pass the key 
and the `CassandraConnector` will be returned.

### The `CassandraConnector` Object

As seen above, this object has a `.session` property which is the native driver Session object. The object also has a `.cluster` 
property which gives access to the native driver Cluster object. 

The `session()` function has two boolean parameters:
* `replace` will close the existing session and replace it with a new session; you may wish to do this if you have changed something
  in the underlying `Cluster` object for example.
* `new` will create a new (and detached) Session object.

## For More Details

Docstrings are the best reference for more detailed usage for the connection arguments.

## Contributing

Contributions welcome, just fire off a pull request :) 

