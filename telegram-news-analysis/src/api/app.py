from flask import Flask, jsonify, request
from flask_cors import CORS
import logging
import jwt
from datetime import datetime, timedelta
from functools import wraps
import os
import hashlib
from db_connection import PostgresConnectionManager, connect_to_elasticsearch, connect_to_mongodb, create_postgres_connection_pool

# Настройки API
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "5000"))
SECRET_KEY = os.getenv("SECRET_KEY", "your_secret_key")

# MongoDB settings
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://mongodb:27017")
MONGODB_DB = os.getenv("MONGODB_DB", "telegram_news")

# Elasticsearch settings
ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST", "elasticsearch")
ELASTICSEARCH_PORT = os.getenv("ELASTICSEARCH_PORT", "9200")
ELASTICSEARCH_INDEX = os.getenv("ELASTICSEARCH_INDEX", "telegram_news")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "20"))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", "30"))
CONNECTION_TIMEOUT = int(os.getenv("CONNECTION_TIMEOUT", "60"))

# PostgreSQL settings
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_USER = os.getenv("POSTGRES_USER", "telegram_news")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "telegram_password")
POSTGRES_DB = os.getenv("POSTGRES_DB", "telegram_news")

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Создание приложения Flask
app = Flask(__name__)
CORS(app)  # Включение CORS для всех маршрутов

# Добавьте конфигурацию приложения
app.config['ELASTICSEARCH_INDEX'] = ELASTICSEARCH_INDEX

# Подключение к MongoDB
mongo_client = connect_to_mongodb(MONGODB_URI)
db = mongo_client[MONGODB_DB]  # This should now work since mongo_client is just the client
# Подключение к PostgreSQL
def get_pg_connection():
    return create_postgres_connection_pool(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        min_conn=1,
        max_conn=10
    )

# Инициализация базы данных PostgreSQL
def init_postgres_db():
    try:
        manager = PostgresConnectionManager(get_pg_connection())
        with manager as conn:  # This gets the actual connection from the pool
            with conn.cursor() as cur:
            
                # Создание таблицы пользователей
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS users (
                        id SERIAL PRIMARY KEY,
                        username VARCHAR(50) UNIQUE NOT NULL,
                        password_hash VARCHAR(256) NOT NULL,
                        email VARCHAR(100) UNIQUE,
                        is_admin BOOLEAN DEFAULT FALSE,
                        is_active BOOLEAN DEFAULT TRUE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Создание таблицы каналов пользователей
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS user_channels (
                        id SERIAL PRIMARY KEY,
                        user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                        channel_name VARCHAR(100) NOT NULL,
                        channel_title VARCHAR(200),
                        added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(user_id, channel_name)
                    )
                ''')
                
                # Создание администратора по умолчанию, если его нет
                password_hash = hashlib.sha256("password".encode()).hexdigest()  # Используем пароль "password"
                cur.execute('''
                    INSERT INTO users (username, password_hash, is_admin)
                    VALUES (%s, %s, TRUE)
                    ON CONFLICT (username) DO NOTHING
                ''', ('admin', password_hash))
                
                conn.commit()
                logger.info("PostgreSQL database initialized successfully")
    except Exception as e:
        logger.error(f"Error initializing PostgreSQL database: {e}")
        raise
# Инициализация базы данных
init_postgres_db()


def get_user_by_username(username):
    """Получение информации о пользователе по имени пользователя"""
    try:
        from psycopg2.extras import RealDictCursor
        manager = PostgresConnectionManager(get_pg_connection())
        with manager as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM users WHERE username = %s", (username,))
                user = cur.fetchone()
                return user  # Already a dictionary with RealDictCursor
    except Exception as e:
        logger.error(f"Error getting user: {e}")
        return None

def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get('Authorization')
        
        if not token:
            logger.error("No token provided")
            return jsonify({'message': 'Token is missing'}), 401
        
        if token.startswith('Bearer '):
            token = token[7:]  # Remove 'Bearer ' prefix
        
        try:
            # Decode token
            data = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
            logger.info(f"Token data: {data}")
            
            # Validate token structure
            if not isinstance(data, dict):
                raise ValueError("Token payload is not a dictionary")
                
            username = data.get('username')
            if not username:
                raise ValueError("Username not found in token")
                
            # Get user data - ensure this returns a dictionary
            user = get_user_by_username(username)
            if not user:
                raise ValueError(f"User {username} not found")
                
            # Ensure user is a dictionary and has required fields
            if not isinstance(user, dict):
                raise TypeError("User data must be a dictionary")
                
            if 'is_active' not in user:
                raise ValueError("User data missing 'is_active' field")
                
            if not user['is_active']:
                raise ValueError(f"User {username} is not active")
            
            # Add all token claims to user data
            request.user = {
                'id': user.get('id'),  # Make sure this matches your DB structure
                'username': username,
                'is_admin': data.get('is_admin', False),
                'is_active': user['is_active'],
                # Add any other required fields
                **data  # Include all token claims
            }
            
        except jwt.ExpiredSignatureError:
            logger.error("Token has expired")
            return jsonify({'message': 'Token has expired'}), 401
        except jwt.InvalidTokenError:
            logger.error("Invalid token")
            return jsonify({'message': 'Invalid token'}), 401
        except Exception as e:
            logger.error(f"Token validation error: {str(e)}", exc_info=True)
            return jsonify({'message': 'Authentication failed'}), 401
        
        return f(*args, **kwargs)
    
    return decorated



# Декоратор для проверки администратора
def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get('Authorization')
        
        if not token:
            return jsonify({'message': 'Token is missing'}), 401
        
        if token.startswith('Bearer '):
            token = token[7:]
        
        try:
            data = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
            # Проверяем, существует ли пользователь, активен ли он и является ли администратором
            user = get_user_by_username(data['username'])
            if not user or not user['is_active'] or not user['is_admin']:
                raise Exception("User is not an admin")
            
            request.user = user
        except Exception as e:
            logger.error(f"Admin validation error: {e}")
            return jsonify({'message': 'You are not authorized to perform this action'}), 403
        
        return f(*args, **kwargs)
    
    return decorated

# Маршрут для регистрации
@app.route('/auth/register', methods=['POST'])
def register():
    data = request.json
    
    if not data or not data.get('username') or not data.get('password'):
        return jsonify({'message': 'Username and password are required'}), 400
    
    username = data.get('username')
    password = data.get('password')
    email = data.get('email')
    
    # Проверка длины имени пользователя и пароля
    if len(username) < 3 or len(password) < 6:
        return jsonify({'message': 'Username must be at least 3 characters and password at least 6 characters'}), 400
    
    # Хеширование пароля
    password_hash = hashlib.sha256(password.encode()).hexdigest()
    
    try:
       manager = PostgresConnectionManager(get_pg_connection())
       with manager as conn:  # This gets the actual connection from the pool
            with conn.cursor() as cur:
            # Проверка, существует ли уже такой пользователь
                cur.execute("SELECT id FROM users WHERE username = %s", (username,))
                if cur.fetchone():
                    return jsonify({'message': 'Username already exists'}), 409
                
                # Вставка нового пользователя
                cur.execute(
                    "INSERT INTO users (username, password_hash, email) VALUES (%s, %s, %s) RETURNING id",
                    (username, password_hash, email)
                )
                new_user_id = cur.fetchone()[0]
                conn.commit()
            
            return jsonify({'message': 'User registered successfully', 'user_id': new_user_id}), 201
    
    except Exception as e:
        logger.error(f"Registration error: {e}")
        return jsonify({'message': 'Error registering user'}), 500

@app.route('/auth/login', methods=['POST'])
def login():
    auth = request.json
    
    if not auth or not auth.get('username') or not auth.get('password'):
        return jsonify({'message': 'Could not verify'}), 401
    
    username = auth.get('username')
    password = auth.get('password')
    
    # Хеширование введенного пароля
    password_hash = hashlib.sha256(password.encode()).hexdigest()
    
    try:
        user = get_user_by_username(username)
        
        if not user:
            return jsonify({'message': 'Invalid credentials'}), 401
        
        if not user['is_active']:
            return jsonify({'message': 'Account is deactivated'}), 403
        
        if user['password_hash'] != password_hash:
            return jsonify({'message': 'Invalid credentials'}), 401
        
        token = jwt.encode({
            'username': username,  # Используем username вместо user
            'is_admin': user['is_admin'],
            'exp': datetime.utcnow() + timedelta(hours=24)
        }, SECRET_KEY, algorithm='HS256')
        
        return jsonify({
            'token': token,
            'username': username,
            'is_admin': user['is_admin']
        })
    
    except Exception as e:
        logger.error(f"Login error: {e}")
        return jsonify({'message': 'Error during login'}), 500

# Импорт и регистрация маршрутов
from routes import register_routes
register_routes(
    app, 
    db, 
    connect_to_elasticsearch(
        ELASTICSEARCH_HOST, 
        ELASTICSEARCH_PORT,
        max_retries=MAX_RETRIES,
        retry_delay=RETRY_DELAY,
        timeout=CONNECTION_TIMEOUT
    ), 
    token_required, 
    admin_required, 
    PostgresConnectionManager(get_pg_connection()))

if __name__ == '__main__':
    logger.info(f"Starting API server on {API_HOST}:{API_PORT}")
    app.run(host=API_HOST, port=API_PORT, debug=False)
