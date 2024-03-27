from cassandra.cluster import Cluster
import json
import os
import requests
from tempfile import gettempdir
import time
from urllib.parse import urlparse

class CassandraConnectionsManager:
    """
    Manages connections to Cassandra databases, including Astra DB, by storing
    and utilizing connection parameters defined in environment variables or passed
    dynamically. This manager supports both Cassandra and Astra DB connections,
    initializing them as needed and providing a way to retrieve active connections.
    """
    def __init__(self):
        """
        Initializes the CassandraConnectionsManager instance by loading connection
        parameters from optional environment variables for both Cassandra and Astra DB.
        It sets up the structure for managing these connections.
        """
        self._connection_params = {}
        self._connections = {}

        # Store CASSANDRA connection params if CASSANDRA_CONNECTION env var is set
        cassandra_conn_str = os.getenv("CASSANDRA_CONNECTION")
        if cassandra_conn_str:
            self._connection_params['env_cassandra'] = _parse_connection_args_json(cassandra_conn_str) 

        # Store Astra connection params if Astra env vars are set
        astra_token = os.getenv("ASTRA_DB_APPLICATION_TOKEN")
        astra_endpoint = os.getenv("ASTRA_DB_API_ENDPOINT")
        astra_db_id = os.getenv("ASTRA_DB_DATABASE_ID")
        astra_db_region = os.getenv("ASTRA_DB_REGION")
        astra_scb = os.getenv("ASTRA_DB_SECURE_BUNDLE_PATH")
        if astra_token:
            self._connection_params['env_astra'] = {'astra': {'token': astra_token, 'endpoint': astra_endpoint, 'datacenterID': astra_db_id, 'regionName': astra_db_region, 'scb': astra_scb}}

    def get_connector(self, db_key='env_astra', **connection_args):
        """
        Retrieves an existing connection object based on the provided `db_key`, or
        initializes a new connection if one does not already exist for the key. This
        method accepts additional connection parameters dynamically, which must follow
        a specific format depending on the type of database (Cassandra or Astra).

        Connections specified in the environment may be accessed with db_key values of 
        `env_astra` or `env_cassandra`. Otherwise, `connection_args` must be specified 
        the first time a `db_key` is used.

        See CassandraConnector class for format of `connection_args`.

        Args:
            db_key (str, optional): The key identifying the database connection to retrieve
                or initialize. Defaults to 'env_astra', which corresponds to an Astra DB
                connection specified in environment variables.
            **connection_args: Arbitrary keyword arguments providing additional connection
                parameters for initializing a new connection if necessary, following the
                specific format requirements for Cassandra or Astra.

        Returns:
            object: An instance of the CassandraConnector corresponding to the specified
                `db_key`, configured according to the provided `connection_args`.

        Raises:
            ValueError: If the `db_key` is not recognized, the connection parameters
                were not provided or did not follow the required format, or if the
                connection could not be initialized due to an error.
        """
        # Return the existing connector if it's already initialized
        if db_key in self._connections:
            return self._connections[db_key]
        
        if db_key not in self._connection_params and connection_args:
            self._connection_params[db_key] = connection_args

        if db_key in self._connection_params:
            try:
                params = self._connection_params[db_key]
                self._connections[db_key] = CassandraConnector(**params)
                print(f"Connection for '{db_key}' initialized successfully.")
            except Exception as e:
                print(f"Failed to setup connection for '{db_key}'")
                print(f"Error: {str(e)}")
                raise ValueError(f"Connection for '{db_key}' could not be initialized.")
            return self._connections[db_key]

        # If the connection key is not recognized or parameters were not provided
        raise ValueError(f"Connection parameters for '{db_key}' not configured.")

class CassandraConnector:
    """
    A connector for establishing connections with Cassandra or Astra databases. This class
    handles the initialization of connections by dynamically loading configuration and
    authentication details, catering to both traditional Cassandra deployments and
    DataStax Astra's cloud database service.

    The connector decides the type of connection (Cassandra or Astra) based on the presence
    of 'astra' key in the connection arguments. For Astra connections, it handles the
    download or retrieval of the secure connection bundle necessary for the connection.

    Attributes:
        cluster (Cluster): The Cassandra cluster object, initialized upon a successful connection.
        session (Session): The session object for executing queries on the Cassandra cluster.
    """
    def __init__(self, **connection_args):
        """
        Initializes a new instance of the CassandraConnector class.

        The method determines the connection type based on the provided arguments
        and sets up the connection accordingly.

        For Cassandra connections, `connection_args` should include:
          - "authProviderClass": The authentication provider class from the DataStax Python driver
            (e.g., "cassandra.auth.PlainTextAuthProvider").
          - "authProviderArgs": A dictionary of arguments for the authentication provider class
            (e.g., {"username": "cassandra", "password": "cassandra"}).
          - Additional connection options such as "contact_points" and "port" that you would use
            on the Cluster object in the DataStax Python driver.

        For Astra connections, `connection_args` should be a nested dictionary with the key 'astra',
        containing:
          - 'token': The application token for accessing your Astra database.
          - 'endpoint': The API endpoint for your Astra database.

        For Astra backwards compatbility with connection mechanics such as CassIO, the 'endpoint' may be 
        omitted from `connection_args`, and the following provided:
          - 'datacenterID': DB ID of the database
          - 'scb': Filesystem path to secure connect bundle
          - 'regionName': Astra region entry point (optional, will default to primary region)

        Example for Cassandra:
            CassandraConnector(authProviderClass='cassandra.auth.PlainTextAuthProvider',
                          authProviderArgs={'username': 'cassandra', 'password': 'cassandra'},
                          contact_points=['127.0.0.1'], port=9042)

        Example for Astra:
            CassandraConnector(astra={'endpoint': 'https://your-astra-db-endpoint',
                                      'token': 'your-astra-db-token'})
        Args:
            **connection_args: Arbitrary keyword arguments for connection parameters.
                See above explanation.
        """
        self._connection_args = connection_args
        self._cluster = None
        self._session = None
        if self._connection_args.get('astra'):
            self._connectionType = 'astra'
            self._setup_astra_connection()
        else:
            self._connectionType = 'cassandra'
            self._setup_cassandra_connection()

    @property
    def session(self, new=False):
        """
        Provides access to the session object for executing queries.

        Args:
            new (bool): If True, creates a new session instance instead of returning
                the one created at class instantiation. Defaults to False.

        Returns:
            Session: session object for the Cassandra cluster.
        """
        if new:
            return self._cluster.connect()
        return self._session

    @property
    def cluster(self):
        """
        Provides access to the cluster object associated with this connector.

        Returns:
            Cluster: The Cassandra cluster object.
        """
        return self._cluster

    def _setup_cassandra_connection(self):
        """
        Sets up a connection to a Cassandra cluster using the provided connection arguments.

        This method dynamically loads the authentication provider class if specified,
        and initializes the Cluster and Session objects for Cassandra operations.
        """
        auth_provider_class = self._connection_args.pop('authProviderClass', None)
        auth_provider_args = self._connection_args.pop('authProviderArgs', {})

        if auth_provider_class:
            # Dynamically import the auth provider class
            module_path, class_name = auth_provider_class.rsplit('.', 1)
            module = __import__(module_path, fromlist=[class_name])
            auth_provider_class = getattr(module, class_name)
            auth_provider = auth_provider_class(**auth_provider_args)
        else:
            auth_provider = None

        # Pass the remaining _connection_args and the auth_provider to the Cluster constructor
        self._cluster = Cluster(auth_provider=auth_provider, **self._connection_args)
        self._session = self._cluster.connect()

    def _setup_astra_connection(self):
        """
        Sets up a connection to a DataStax Astra database.

        This method retrieves or downloads the secure connect bundle necessary for the
        connection and then calls `_setup_cassandra_connection` to initialize the Cluster
        and Session objects with Astra-specific parameters.
        """
        astra_args = self._connection_args['astra']
        scb_path = self._get_or_download_secure_connect_bundle(astra_args)        
        self._connection_args = {
            "cloud": {'secure_connect_bundle': scb_path}, 
            "authProviderClass": "cassandra.auth.PlainTextAuthProvider",
            "authProviderArgs": {"username": "token", "password": astra_args['token']},
        }
        self._setup_cassandra_connection() 
    
    def _get_or_download_secure_connect_bundle(self, astra_args):
        """
        Retrieves or downloads the secure connect bundle for Astra connections.

        Args:
            astra_args (dict): A dictionary containing 'endpoint' and 'token' for the
                Astra database, and optionally 'datacenterID' and 'regionName'. If the
                dictionary contains 'scb', that value is returned.

        Returns:
            str: The file path to the secure connect bundle.
        """
        if 'scb' in astra_args and astra_args['scb']:
            return astra_args['scb']

        # Ensure datacenterID and regionName are extracted from endpoint if provided
        if 'endpoint' in astra_args and astra_args['endpoint']:
            # Parse the endpoint URL
            endpoint_parsed = urlparse(astra_args['endpoint'])
            # Extract the hostname without the domain suffix
            hostname_without_suffix = endpoint_parsed.netloc.split('.apps.astra.datastax.com')[0]
            # Split the hostname to get parts
            parts = hostname_without_suffix.split('-')
            # Datacenter is first 5 parts, everything after is region
            datacenterID = '-'.join(parts[:5])
            regionName = '-'.join(parts[5:])

            # Update astra_args with extracted values if not explicitly provided
            astra_args['datacenterID'] = astra_args.get('datacenterID') or datacenterID
            astra_args['regionName'] = astra_args.get('regionName') or regionName
        elif 'datacenterID' not in astra_args or not astra_args['datacenterID']:
            raise ValueError("Astra endpoint or datacenterID must be provided in args.")

        scb_dir = os.path.join(gettempdir(), "cassandra-astra")
        os.makedirs(scb_dir, exist_ok=True)

        # Generate the secure connect bundle filename
        scb_filename = f"astra-secure-connect-{astra_args['datacenterID']}"
        if 'regionName' in astra_args and astra_args['regionName']:
            scb_filename += f"-{astra_args['regionName']}"
        scb_filename += ".zip"
        scb_path = os.path.join(scb_dir, scb_filename)

        if not os.path.exists(scb_path) or time.time() - os.path.getmtime(scb_path) > 360 * 24 * 60 * 60:
            download_url = self._get_secure_connect_bundle_url(astra_args)
            response = requests.get(download_url)
            response.raise_for_status()

            with open(scb_path, 'wb') as f:
                f.write(response.content)
        
        return scb_path

    def _get_secure_connect_bundle_url(self, astra_args):
        """
        Generates the URL for downloading the secure connect bundle for Astra connections.

        Args:
            astra_args (dict): A dictionary containing 'endpoint', 'token', and
                'datacenterID' for the Astra database. Optionally, 'regionName' can be
                provided to download a region-specific secure connect bundle.

        Returns:
            str: The URL to download the secure connect bundle from.
        """
        url_template = astra_args.get(
            'bundleUrlTemplate',
            "https://api.astra.datastax.com/v2/databases/{database_id}/secureBundleURL?all=true"
        )
        url = url_template.replace("{database_id}", astra_args['datacenterID'])

        headers = {
            'Authorization': f"Bearer {astra_args['token']}",
            'Content-Type': 'application/json',
        }
        response = requests.post(url, headers=headers)
        response.raise_for_status()

        data = response.json()
        if not data or len(data) == 0:
            raise ValueError("Failed to get secure bundle URLs.")

        # Default to the first URL if no regionName is specified or if a specific region's bundle cannot be found.
        download_url = data[0]['downloadURL']

        # If 'regionName' is provided, try to find a region-specific bundle.
        if 'regionName' in astra_args and astra_args['regionName']:
            regional_bundle = next((bundle for bundle in data if bundle['region'] == astra_args['regionName']), None)
            if regional_bundle:
                download_url = regional_bundle['downloadURL']
            else:
                raise ValueError(f"Specific bundle for region '{astra_args['regionName']}' not found.")

        return download_url

def _parse_connection_args_json(conn_args_str):
    """
    Parses a connection arguments string in JSON format into a Python dictionary.

    Args:
        conn_args_str (str): Connection arguments as a JSON-formatted string.

    Returns:
        dict: Connection arguments as a dictionary.
    """
    try:
        conn_args_dict = json.loads(conn_args_str)
    except json.JSONDecodeError as e:
        raise ValueError(f"Error parsing connection arguments: {str(e)}")

    return conn_args_dict

