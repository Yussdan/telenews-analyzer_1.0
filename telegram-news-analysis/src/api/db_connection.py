import time
import logging
import socket
from elasticsearch import Elasticsearch
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import pymongo
import psycopg2
from psycopg2 import pool

logger = logging.getLogger(__name__)

def wait_for_service(host, port, timeout=300):
    """Wait for service to be available"""
    start_time = time.time()
    while True:
        try:
            socket.create_connection((host, int(port)), timeout=5)
            logger.info(f"Service at {host}:{port} is available (took {int(time.time() - start_time)}s)")
            return True
        except (socket.timeout, socket.error) as ex:
            if time.time() - start_time >= timeout:
                logger.error(f"Service {host}:{port} not available after {timeout}s: {ex}")
                return False
            logger.info(f"Waiting for {host}:{port} to be available... ({int(time.time() - start_time)}s)")
            time.sleep(5)

def connect_to_elasticsearch(host, port, max_retries=20, retry_delay=30, timeout=90):
    """Connect to Elasticsearch with robust retry logic"""
    # First verify network connectivity
    if not wait_for_service(host, port):
        raise ConnectionError(f"Cannot establish network connection to {host}:{port}")
    
    # Now attempt connecting to Elasticsearch with a progressive backoff
    for attempt in range(max_retries):
        try:
            logger.info(f"Connecting to Elasticsearch {host}:{port}, attempt {attempt+1}/{max_retries}")
            # Increase timeouts for initial connection attempts
            current_timeout = timeout + (attempt * 10)  # Progressive timeout
            
            es = Elasticsearch(
                [f"http://{host}:{port}"],
                timeout=current_timeout,
                max_retries=3,
                retry_on_timeout=True
            )
            
            # First test with basic ping
            if es.ping(request_timeout=current_timeout):
                # Then check cluster health
                health = es.cluster.health(wait_for_status="yellow", timeout=f"{current_timeout}s")
                if health.get("status") in ["yellow", "green"]:
                    logger.info(f"Successfully connected to Elasticsearch at {host}:{port}")
                    return es
            
            logger.warning(f"Elasticsearch not ready on attempt {attempt+1}")
        except Exception as e:
            logger.warning(f"Elasticsearch connection attempt {attempt+1} failed: {str(e)}")
        
        # Calculate a progressive backoff
        current_delay = retry_delay * (1 + (attempt * 0.5))
        logger.info(f"Waiting {current_delay:.1f}s before next retry...")
        time.sleep(current_delay)
    
    logger.error(f"Failed to connect to Elasticsearch after {max_retries} attempts")
    raise ConnectionError(f"Could not connect to Elasticsearch at {host}:{port}")


def connect_to_mongodb(uri, timeout=30000, max_attempts=5):
    """Connect to MongoDB with retry logic"""
    logger = logging.getLogger(__name__)
    host = uri.split("//")[1].split(":")[0]
    port = 27017  # Default MongoDB port
    
    if not wait_for_service(host, port):
        raise ConnectionError(f"Cannot establish network connection to MongoDB at {host}:{port}")
    
    for attempt in range(max_attempts):
        try:
            logger.info(f"Connecting to MongoDB {uri}, attempt {attempt+1}/{max_attempts}")
            client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=timeout)
            client.server_info()  # This will raise an exception if cannot connect
            logger.info(f"Successfully connected to MongoDB at {uri}")
            return client  # Return just the client, not a tuple
        except Exception as e:
            logger.warning(f"MongoDB connection attempt {attempt+1} failed: {str(e)}")
            if attempt < max_attempts - 1:
                time.sleep(10)
    
    logger.error(f"Failed to connect to MongoDB after {max_attempts} attempts")
    raise ConnectionError(f"Could not connect to MongoDB at {uri}")
    
def create_postgres_connection_pool(host, port, dbname, user, password, min_conn=1, max_conn=10, timeout=30):
    """Create a connection pool for PostgreSQL with retry logic"""
    if not wait_for_service(host, port):
        raise ConnectionError(f"Cannot establish network connection to PostgreSQL at {host}:{port}")
    
    max_attempts = 5
    for attempt in range(max_attempts):
        try:
            logger.info(f"Creating PostgreSQL connection pool for {user}@{host}:{port}/{dbname}, attempt {attempt+1}/{max_attempts}")
            
            # Create a threaded connection pool
            connection_pool = pool.ThreadedConnectionPool(
                minconn=min_conn,
                maxconn=max_conn,
                host=host,
                port=port,
                dbname=dbname,
                user=user,
                password=password,
                connect_timeout=timeout
            )
            
            # Test the connection pool by getting a connection
            conn = connection_pool.getconn()
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                assert result[0] == 1, "Test query failed"
            
            # Return the connection to the pool
            connection_pool.putconn(conn)
            
            logger.info(f"Successfully created PostgreSQL connection pool for {host}:{port}/{dbname}")
            return connection_pool
            
        except Exception as e:
            logger.warning(f"PostgreSQL connection pool creation attempt {attempt+1} failed: {str(e)}")
            if attempt < max_attempts - 1:
                time.sleep(10)
    
    logger.error(f"Failed to create PostgreSQL connection pool after {max_attempts} attempts")
    raise ConnectionError(f"Could not connect to PostgreSQL at {host}:{port}/{dbname}")

class PostgresConnectionManager:
    """
    A manager for PostgreSQL connections using connection pooling
    """
    def __init__(self, connection_pool):
        self.connection_pool = connection_pool
        self.connections = {}
    
    def get_connection(self):
        """Get a connection from the pool"""
        import threading
        thread_id = threading.get_ident()
        
        if thread_id not in self.connections:
            self.connections[thread_id] = self.connection_pool.getconn()
        
        return self.connections[thread_id]
    
    def release_connection(self):
        """Release the connection back to the pool"""
        import threading
        thread_id = threading.get_ident()
        
        if thread_id in self.connections:
            self.connection_pool.putconn(self.connections[thread_id])
            del self.connections[thread_id]
    
    def __enter__(self):
        """Context manager support"""
        return self.get_connection()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Release connection when exiting context"""
        self.release_connection()


def init_postgres_with_retry(host, port, dbname, user, password, init_script, max_retries=5, retry_delay=10):
    """Initialize PostgreSQL database with retry logic"""
    if not wait_for_service(host, port):
        raise ConnectionError(f"Cannot establish network connection to PostgreSQL at {host}:{port}")
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Initializing PostgreSQL database {dbname}, attempt {attempt+1}/{max_retries}")
            
            # Connect to PostgreSQL
            conn = psycopg2.connect(
                host=host,
                port=port,
                dbname=dbname,
                user=user,
                password=password,
                connect_timeout=30
            )
            
            # Execute the initialization script
            with conn.cursor() as cur:
                cur.execute(init_script)
            
            # Commit the changes
            conn.commit()
            logger.info(f"Successfully initialized PostgreSQL database {dbname}")
            
            # Close the connection
            conn.close()
            return True
            
        except Exception as e:
            logger.warning(f"PostgreSQL initialization attempt {attempt+1} failed: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                
    logger.error(f"Failed to initialize PostgreSQL database after {max_retries} attempts")
    raise ConnectionError(f"Could not initialize PostgreSQL database {dbname}")
