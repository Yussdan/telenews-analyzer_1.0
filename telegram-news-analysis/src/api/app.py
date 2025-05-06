from flask import Flask, jsonify, request, g
from flask_cors import CORS
import logging
import jwt
from datetime import datetime, timedelta
from functools import wraps
import os
import hashlib
import time
import traceback
from db_connection import PostgresConnectionManager, connect_to_elasticsearch, connect_to_mongodb, create_postgres_connection_pool

# Настройки API
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "5000"))
SECRET_KEY = os.getenv("SECRET_KEY", "your_secret_key")
JWT_EXPIRATION_HOURS = int(os.getenv("JWT_EXPIRATION_HOURS", "24"))
DEBUG_MODE = os.getenv("DEBUG_MODE", "False").lower() in ('true', '1', 't')

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
POSTGRES_MIN_CONN = int(os.getenv("POSTGRES_MIN_CONN", "1"))
POSTGRES_MAX_CONN = int(os.getenv("POSTGRES_MAX_CONN", "10"))

# Настройка логирования
log_level = logging.DEBUG if DEBUG_MODE else logging.INFO
logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


app = Flask(__name__)
CORS(app, supports_credentials=True)

# Добавляем конфигурацию приложения
app.config.update(
    ELASTICSEARCH_INDEX=ELASTICSEARCH_INDEX,
    SECRET_KEY=SECRET_KEY,
    DEBUG=DEBUG_MODE
)

# Инициализация соединений с базами данных
def init_database_connections():
    logger.info("Initializing database connections")

    try:
        app.mongo_client = connect_to_mongodb(MONGODB_URI)
        app.mongo_db = app.mongo_client[MONGODB_DB]
        logger.info("MongoDB connection established")
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        app.mongo_client = None
        app.mongo_db = None
    
    try:
        app.pg_pool = create_postgres_connection_pool(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            min_conn=POSTGRES_MIN_CONN,
            max_conn=POSTGRES_MAX_CONN
        )
        logger.info("PostgreSQL connection pool created")
    except Exception as e:
        logger.error(f"Failed to create PostgreSQL connection pool: {e}")
        app.pg_pool = None
    
    # Подключение к Elasticsearch
    try:
        app.elasticsearch = connect_to_elasticsearch(
            ELASTICSEARCH_HOST, 
            ELASTICSEARCH_PORT,
            max_retries=MAX_RETRIES,
            retry_delay=RETRY_DELAY,
            timeout=CONNECTION_TIMEOUT
        )
        logger.info("Elasticsearch connection established")
    except Exception as e:
        logger.error(f"Failed to connect to Elasticsearch: {e}")
        app.elasticsearch = None

# Инициализация базы данных PostgreSQL
def init_postgres_db():
    if not app.pg_pool:
        logger.error("Cannot initialize PostgreSQL database: connection pool not available")
        return False
        
    try:
        manager = PostgresConnectionManager(app.pg_pool)
        with manager as conn:
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
                        last_login TIMESTAMP,
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
                
                # Создание таблицы для хранения сессий пользователей
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS user_sessions (
                        id SERIAL PRIMARY KEY,
                        user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                        token VARCHAR(512) NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        expires_at TIMESTAMP NOT NULL,
                        is_active BOOLEAN DEFAULT TRUE
                    )
                ''')
                
                # Создание таблицы для хранения пользовательских настроек
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS user_settings (
                        user_id INTEGER REFERENCES users(id) ON DELETE CASCADE PRIMARY KEY,
                        theme VARCHAR(20) DEFAULT 'light',
                        language VARCHAR(10) DEFAULT 'ru',
                        notifications BOOLEAN DEFAULT TRUE,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Создание администратора по умолчанию, если его нет
                admin_password = os.getenv("ADMIN_PASSWORD", "password")
                password_hash = hashlib.sha256(admin_password.encode()).hexdigest()
                
                cur.execute('''
                    INSERT INTO users (username, password_hash, is_admin)
                    VALUES (%s, %s, TRUE)
                    ON CONFLICT (username) DO NOTHING
                ''', ('admin', password_hash))
                
                conn.commit()
                logger.info("PostgreSQL database initialized successfully")
                return True
    except Exception as e:
        logger.error(f"Error initializing PostgreSQL database: {e}")
        logger.debug(traceback.format_exc())
        return False

# Функция для получения информации о пользователе по имени
def get_user_by_username(username):
    """Получение информации о пользователе по имени пользователя"""
    if not hasattr(app, 'pg_pool') or not app.pg_pool:
        logger.error("PostgreSQL connection pool not available")
        return None
        
    try:
        from psycopg2.extras import RealDictCursor
        manager = PostgresConnectionManager(app.pg_pool)
        with manager as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM users WHERE username = %s", (username,))
                user = cur.fetchone()
                return user
    except Exception as e:
        logger.error(f"Error getting user by username: {e}")
        logger.debug(traceback.format_exc())
        return None

# Функция для обновления времени последнего входа пользователя
def update_user_last_login(user_id):
    if not hasattr(app, 'pg_pool') or not app.pg_pool:
        logger.error("PostgreSQL connection pool not available")
        return False
        
    try:
        manager = PostgresConnectionManager(app.pg_pool)
        with manager as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE users SET last_login = CURRENT_TIMESTAMP WHERE id = %s",
                    (user_id,)
                )
                conn.commit()
                return True
    except Exception as e:
        logger.error(f"Error updating user last login: {e}")
        return False

# Декоратор для проверки JWT токена
def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = None
        
        # Получаем токен из заголовка Authorization
        auth_header = request.headers.get('Authorization')
        if auth_header:
            if auth_header.startswith('Bearer '):
                token = auth_header[7:]
            else:
                token = auth_header
        
        # Если токен не найден, пробуем получить его из query параметров или из cookies
        if not token:
            token = request.args.get('token')
        
        if not token:
            token = request.cookies.get('token')
            
        if not token:
            logger.warning("No token provided in request")
            return jsonify({'message': 'Authentication token is missing'}), 401
        
        try:
            # Декодирование и проверка токена
            data = jwt.decode(token, app.config['SECRET_KEY'], algorithms=['HS256'])
            
            # Получение данных пользователя
            username = data.get('username')
            if not username:
                raise ValueError("Username not found in token")
                
            # Получаем данные пользователя из базы
            user = get_user_by_username(username)
            if not user:
                raise ValueError(f"User {username} not found")
                
            if not user['is_active']:
                raise ValueError(f"User {username} is inactive")
            
            # Сохраняем данные пользователя для использования в обработчике
            g.user = {
                'id': user['id'],
                'username': username,
                'is_admin': user['is_admin'],
                'is_active': user['is_active'],
                'email': user['email'],
            }
            
            # Добавляем пользователя в объект request для совместимости
            request.user = g.user
            
        except jwt.ExpiredSignatureError:
            logger.warning(f"Expired token for user")
            return jsonify({'message': 'Token has expired. Please log in again.'}), 401
        except jwt.InvalidTokenError:
            logger.warning(f"Invalid token")
            return jsonify({'message': 'Invalid authentication token'}), 401
        except Exception as e:
            logger.error(f"Token validation error: {str(e)}")
            if DEBUG_MODE:
                logger.debug(traceback.format_exc())
            return jsonify({'message': 'Authentication failed'}), 401
        
        return f(*args, **kwargs)
    
    return decorated

# Декоратор для проверки прав администратора
def admin_required(f):
    @wraps(f)
    @token_required  # Сначала проверяем токен
    def decorated(*args, **kwargs):
        # Проверяем, является ли пользователь администратором
        if not g.user.get('is_admin'):
            logger.warning(f"Non-admin user {g.user.get('username')} attempted to access admin endpoint")
            return jsonify({'message': 'Admin privileges required for this operation'}), 403
        
        return f(*args, **kwargs)
    
    return decorated

# Маршрут для проверки здоровья API
@app.route('/health', methods=['GET'])
def health_check():
    health = {
        'status': 'ok',
        'timestamp': datetime.now().isoformat(),
        'components': {
            'api': {'status': 'ok'},
            'postgres': {'status': 'unknown'},
            'mongodb': {'status': 'unknown'},
            'elasticsearch': {'status': 'unknown'}
        }
    }

    try:
        if hasattr(app, 'pg_pool') and app.pg_pool:
            manager = PostgresConnectionManager(app.pg_pool)
            with manager as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    health['components']['postgres'] = {'status': 'ok'}
        else:
            health['components']['postgres'] = {'status': 'unavailable', 'message': 'No connection pool'}
    except Exception as e:
        health['components']['postgres'] = {'status': 'error', 'message': str(e)}
        health['status'] = 'degraded'

    try:
        if hasattr(app, 'mongo_client') and app.mongo_client:
            app.mongo_client.admin.command('ping')
            health['components']['mongodb'] = {'status': 'ok'}
        else:
            health['components']['mongodb'] = {'status': 'unavailable', 'message': 'No client'}
    except Exception as e:
        health['components']['mongodb'] = {'status': 'error', 'message': str(e)}
        health['status'] = 'degraded'

    try:
        if hasattr(app, 'elasticsearch') and app.elasticsearch:
            es_health = app.elasticsearch.cluster.health()
            if es_health['status'] in ('green', 'yellow'):
                health['components']['elasticsearch'] = {'status': 'ok', 'cluster_status': es_health['status']}
            else:
                health['components']['elasticsearch'] = {'status': 'warning', 'cluster_status': es_health['status']}
                health['status'] = 'degraded'
        else:
            health['components']['elasticsearch'] = {'status': 'unavailable', 'message': 'No client'}
            health['status'] = 'degraded'
    except Exception as e:
        health['components']['elasticsearch'] = {'status': 'error', 'message': str(e)}
        health['status'] = 'degraded'
    
    status_code = 200 if health['status'] == 'ok' else 500
    return jsonify(health), status_code

# Маршрут для регистрации пользователя
@app.route('/auth/register', methods=['POST'])
def register():
    data = request.json
    
    if not data:
        return jsonify({'message': 'No data provided'}), 400
        
    username = data.get('username')
    password = data.get('password')
    email = data.get('email')
    
    if not username or not password:
        return jsonify({'message': 'Username and password are required'}), 400

    if len(username) < 3:
        return jsonify({'message': 'Username must be at least 3 characters'}), 400
        
    if len(password) < 6:
        return jsonify({'message': 'Password must be at least 6 characters'}), 400
    
    if email and '@' not in email:
        return jsonify({'message': 'Invalid email format'}), 400

    password_hash = hashlib.sha256(password.encode()).hexdigest()
    
    try:
        if not hasattr(app, 'pg_pool') or not app.pg_pool:
            return jsonify({'message': 'Database connection not available'}), 503
            
        manager = PostgresConnectionManager(app.pg_pool)
        with manager as conn:
            with conn.cursor() as cur:
                # Проверка существования пользователя
                cur.execute("SELECT id FROM users WHERE username = %s", (username,))
                if cur.fetchone():
                    return jsonify({'message': 'Username already exists'}), 409
                
                # Проверка email, если он указан
                if email:
                    cur.execute("SELECT id FROM users WHERE email = %s", (email,))
                    if cur.fetchone():
                        return jsonify({'message': 'Email already registered'}), 409
                
                # Вставка нового пользователя
                cur.execute(
                    "INSERT INTO users (username, password_hash, email) VALUES (%s, %s, %s) RETURNING id",
                    (username, password_hash, email)
                )
                new_user_id = cur.fetchone()[0]

                cur.execute(
                    "INSERT INTO user_settings (user_id) VALUES (%s)",
                    (new_user_id,)
                )
                
                conn.commit()
                
                logger.info(f"User {username} registered successfully")
                return jsonify({
                    'message': 'User registered successfully',
                    'user_id': new_user_id
                }), 201
    
    except Exception as e:
        logger.error(f"Error during user registration: {e}")
        if DEBUG_MODE:
            logger.debug(traceback.format_exc())
        return jsonify({'message': 'Error registering user'}), 500

# Маршрут для входа в систему
@app.route('/auth/login', methods=['POST'])
def login():
    data = request.json
    
    if not data:
        return jsonify({'message': 'No data provided'}), 400
        
    username = data.get('username')
    password = data.get('password')
    
    if not username or not password:
        return jsonify({'message': 'Username and password are required'}), 400
    
    # Хеширование введенного пароля
    password_hash = hashlib.sha256(password.encode()).hexdigest()
    
    try:
        if not hasattr(app, 'pg_pool') or not app.pg_pool:
            return jsonify({'message': 'Database connection not available'}), 503
            
        # Получение пользователя
        user = get_user_by_username(username)
        
        if not user:
            # Используем одинаковое сообщение для безопасности
            return jsonify({'message': 'Invalid credentials'}), 401
        
        if not user['is_active']:
            return jsonify({'message': 'Account is inactive'}), 403
        
        if user['password_hash'] != password_hash:
            return jsonify({'message': 'Invalid credentials'}), 401
        
        # Создание JWT токена
        expiration = datetime.utcnow() + timedelta(hours=JWT_EXPIRATION_HOURS)
        token = jwt.encode({
            'username': username,
            'is_admin': user['is_admin'],
            'exp': expiration
        }, app.config['SECRET_KEY'], algorithm='HS256')
        
        # Обновление времени последнего входа
        update_user_last_login(user['id'])
        
        # Сохранение сессии в базе данных
        manager = PostgresConnectionManager(app.pg_pool)
        with manager as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO user_sessions (user_id, token, expires_at) VALUES (%s, %s, %s)",
                    (user['id'], token, expiration)
                )
                conn.commit()
        
        logger.info(f"User {username} logged in successfully")
        
        # Формирование ответа
        response = jsonify({
            'token': token,
            'username': username,
            'is_admin': user['is_admin'],
            'expires_at': expiration.isoformat()
        })
        
        # Добавление токена в cookies, если требуется
        if data.get('remember_me', False):
            response.set_cookie(
                'token',
                token,
                httponly=True,
                secure=not DEBUG_MODE,
                samesite='Strict',
                expires=expiration
            )
        
        return response
    
    except Exception as e:
        logger.error(f"Error during login: {e}")
        if DEBUG_MODE:
            logger.debug(traceback.format_exc())
        return jsonify({'message': 'Error during login'}), 500

# Маршрут для выхода из системы
@app.route('/auth/logout', methods=['POST'])
@token_required
def logout():
    try:
        if not hasattr(app, 'pg_pool') or not app.pg_pool:
            return jsonify({'message': 'Database connection not available'}), 503
            
        # Получаем токен
        token = None
        auth_header = request.headers.get('Authorization')
        if auth_header:
            if auth_header.startswith('Bearer '):
                token = auth_header[7:]
            else:
                token = auth_header
        
        if not token:
            token = request.cookies.get('token')
            
        # Инвалидируем сессию в базе данных
        manager = PostgresConnectionManager(app.pg_pool)
        with manager as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE user_sessions SET is_active = FALSE WHERE user_id = %s AND token = %s",
                    (g.user['id'], token)
                )
                conn.commit()
        
        logger.info(f"User {g.user['username']} logged out")

        response = jsonify({'message': 'Logout successful'})

        response.set_cookie('token', '', expires=0)
        
        return response
    
    except Exception as e:
        logger.error(f"Error during logout: {e}")
        if DEBUG_MODE:
            logger.debug(traceback.format_exc())
        return jsonify({'message': 'Error during logout'}), 500

# Маршрут для изменения пароля
@app.route('/auth/change-password', methods=['POST'])
@token_required
def change_password():
    data = request.json
    
    if not data:
        return jsonify({'message': 'No data provided'}), 400
        
    current_password = data.get('current_password')
    new_password = data.get('new_password')
    
    if not current_password or not new_password:
        return jsonify({'message': 'Current and new password are required'}), 400
    
    if len(new_password) < 6:
        return jsonify({'message': 'New password must be at least 6 characters'}), 400
    
    try:
        if not hasattr(app, 'pg_pool') or not app.pg_pool:
            return jsonify({'message': 'Database connection not available'}), 503
            
        # Проверка текущего пароля
        current_hash = hashlib.sha256(current_password.encode()).hexdigest()
        new_hash = hashlib.sha256(new_password.encode()).hexdigest()
        
        manager = PostgresConnectionManager(app.pg_pool)
        with manager as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT password_hash FROM users WHERE id = %s",
                    (g.user['id'],)
                )
                result = cur.fetchone()
                
                if not result or result[0] != current_hash:
                    return jsonify({'message': 'Current password is incorrect'}), 401
                
                # Обновление пароля
                cur.execute(
                    "UPDATE users SET password_hash = %s WHERE id = %s",
                    (new_hash, g.user['id'])
                )
                
                # Инвалидация всех сессий пользователя, кроме текущей
                token = None
                auth_header = request.headers.get('Authorization')
                if auth_header and auth_header.startswith('Bearer '):
                    token = auth_header[7:]
                
                if token:
                    cur.execute(
                        "UPDATE user_sessions SET is_active = FALSE WHERE user_id = %s AND token != %s",
                        (g.user['id'], token)
                    )
                
                conn.commit()
                
                logger.info(f"Password changed for user {g.user['username']}")
                return jsonify({'message': 'Password changed successfully'})
    
    except Exception as e:
        logger.error(f"Error changing password: {e}")
        if DEBUG_MODE:
            logger.debug(traceback.format_exc())
        return jsonify({'message': 'Error changing password'}), 500

# Обработчик ошибок
@app.errorhandler(404)
def not_found(error):
    return jsonify({'message': 'Resource not found'}), 404

@app.errorhandler(405)
def method_not_allowed(error):
    return jsonify({'message': 'Method not allowed'}), 405

@app.errorhandler(500)
def internal_server_error(error):
    logger.error(f"Internal server error: {error}")
    return jsonify({'message': 'Internal server error'}), 500

# Инициализация приложения
def init_app():
    # Инициализация подключений к базам данных
    init_database_connections()
    
    # Инициализация структуры PostgreSQL
    if not init_postgres_db():
        logger.warning("PostgreSQL database initialization failed, retrying...")
        time.sleep(5)  # Задержка перед повторной попыткой
        init_postgres_db()
    
    # Импорт и регистрация маршрутов
    try:
        from routes import register_routes
        register_routes(
            app, 
            app.mongo_db if hasattr(app, 'mongo_db') else None, 
            app.elasticsearch if hasattr(app, 'elasticsearch') else None,
            token_required, 
            admin_required, 
            PostgresConnectionManager(app.pg_pool) if hasattr(app, 'pg_pool') else None
        )
        logger.info("Routes registered successfully")
    except Exception as e:
        logger.error(f"Error registering routes: {e}")
        if DEBUG_MODE:
            logger.debug(traceback.format_exc())

if __name__ == '__main__':
    # Инициализация приложения
    init_app()
    
    # Запуск сервера
    logger.info(f"Starting API server on {API_HOST}:{API_PORT} (Debug: {DEBUG_MODE})")
    app.run(host=API_HOST, port=API_PORT, debug=DEBUG_MODE)
else:
    init_app()
