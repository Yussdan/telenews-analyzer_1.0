import time
import logging
import socket
from elasticsearch import Elasticsearch
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import pymongo

logger = logging.getLogger(__name__)

def wait_for_service(host, port, timeout=300):
    """Wait for service to be available"""
    start_time = time.time()
    while True:
        try:
            socket.create_connection((host, int(port)), timeout=5)
            return True
        except (socket.timeout, socket.error) as ex:
            if time.time() - start_time >= timeout:
                logger.error(f"Service {host}:{port} not available after {timeout}s: {ex}")
                return False
            time.sleep(5)

def connect_to_elasticsearch(host, port, max_retries=20, retry_delay=30, timeout=60):
    """Connect to Elasticsearch with robust retry logic"""
    # First verify network connectivity
    if not wait_for_service(host, port):
        raise ConnectionError(f"Cannot establish network connection to {host}:{port}")
    
    # Initialize HTTP Session with proper retry configuration
    session = requests.Session()
    retries = Retry(
        total=max_retries,
        backoff_factor=0.5,
        status_forcelist=[502, 503, 504],
        allowed_methods=("GET", "HEAD", "OPTIONS", "POST")
    )
    session.mount('http://', HTTPAdapter(max_retries=retries))
    
    # Now attempt connecting to Elasticsearch
    for attempt in range(max_retries):
        try:
            logger.info(f"Connecting to Elasticsearch {host}:{port}, attempt {attempt+1}/{max_retries}")
            es = Elasticsearch(
                [f"http://{host}:{port}"],
                timeout=timeout,
                max_retries=3,
                retry_on_timeout=True
            )
            
            # Test connection with explicit timeout 
            if es.ping(request_timeout=timeout):
                logger.info(f"Successfully connected to Elasticsearch at {host}:{port}")
                return es
            
            logger.warning(f"Elasticsearch ping failed on attempt {attempt+1}")
        except Exception as e:
            logger.warning(f"Elasticsearch connection attempt {attempt+1} failed: {str(e)}")
        
        if attempt < max_retries - 1:
            logger.info(f"Waiting {retry_delay}s before next retry...")
            time.sleep(retry_delay)
    
    logger.error(f"Failed to connect to Elasticsearch after {max_retries} attempts")
    raise ConnectionError(f"Could not connect to Elasticsearch at {host}:{port}")

def connect_to_mongodb(uri, db_name, timeout=30000):
    """Connect to MongoDB with retry logic"""
    host = uri.split("//")[1].split(":")[0]
    port = 27017  # Default MongoDB port
    
    if not wait_for_service(host, port):
        raise ConnectionError(f"Cannot establish network connection to MongoDB at {host}:{port}")
    
    max_attempts = 5
    for attempt in range(max_attempts):
        try:
            logger.info(f"Connecting to MongoDB {uri}, attempt {attempt+1}/{max_attempts}")
            client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=timeout)
            client.server_info()  # This will raise an exception if cannot connect
            db = client[db_name]
            logger.info(f"Successfully connected to MongoDB at {uri}")
            return client, db
        except Exception as e:
            logger.warning(f"MongoDB connection attempt {attempt+1} failed: {str(e)}")
            if attempt < max_attempts - 1:
                time.sleep(10)
    
    logger.error(f"Failed to connect to MongoDB after {max_attempts} attempts")
    raise ConnectionError(f"Could not connect to MongoDB at {uri}")
