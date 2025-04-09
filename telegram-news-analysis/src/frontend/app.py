from flask import Flask, render_template, request, redirect, url_for, session, jsonify, flash

import requests
import os
import logging
from datetime import datetime

# Настройки
FRONTEND_HOST = os.getenv("FRONTEND_HOST", "0.0.0.0")
FRONTEND_PORT = int(os.getenv("FRONTEND_PORT", "8080"))
API_URL = os.getenv("API_URL", "http://api:5000")

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__, static_folder='static')
app.secret_key = os.getenv("SECRET_KEY", "frontend_secret_key")

@app.route('/')
def index():
    """Главная страница"""
    if 'token' not in session:
        return redirect(url_for('login'))
    
    return render_template('index.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    """Страница авторизации"""
    error = None
    
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        
        try:
            logger.info(f"Trying to log in user: {username}")
            response = requests.post(
                f"{API_URL}/auth/login",
                json={"username": username, "password": password}
            )
            
            logger.info(f"Login response status: {response.status_code}")
            logger.info(f"Login response body: {response.text}")
            
            if response.status_code == 200:
                data = response.json()
                session['token'] = data['token']
                session['username'] = data['username']
                session['is_admin'] = data['is_admin']
                logger.info(f"User {username} logged in successfully")
                return redirect(url_for('index'))
            else:
                error = "Неверное имя пользователя или пароль"
        
        except Exception as e:
            logger.error(f"Login error: {e}")
            error = "Ошибка подключения к API"
    
    return render_template('login.html', error=error)


@app.route('/register', methods=['GET', 'POST'])
def register():
    """Страница регистрации"""
    error = None
    success = None
    
    if 'token' in session:
        # Если пользователь уже авторизован, перенаправляем на главную
        flash('Вы уже авторизованы. Сначала выйдите из системы.')
        return redirect(url_for('index'))
    
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        password_confirm = request.form.get('password_confirm')
        email = request.form.get('email')
        
        # Проверка введенных данных
        if len(username) < 3:
            error = "Имя пользователя должно содержать не менее 3 символов"
        elif len(password) < 6:
            error = "Пароль должен содержать не менее 6 символов"
        elif password != password_confirm:
            error = "Пароли не совпадают"
        else:
            try:
                response = requests.post(
                    f"{API_URL}/auth/register",
                    json={
                        "username": username,
                        "password": password,
                        "email": email
                    }
                )
                
                # Проверяем ответ API
                if response.status_code == 201:
                    # Успешная регистрация
                    success = "Регистрация успешна! Теперь вы можете войти в систему."
                    # Добавляем задержку, чтобы пользователь мог прочитать сообщение
                    flash('Регистрация успешна! Теперь вы можете войти в систему.')
                    # Перенаправляем на страницу входа после успешной регистрации
                    return redirect(url_for('login'))
                else:
                    # Получаем сообщение об ошибке из ответа API
                    data = response.json()
                    error = data.get('message', 'Ошибка при регистрации')
            
            except Exception as e:
                logger.error(f"Registration error: {e}")
                error = "Ошибка подключения к API"
    
    return render_template('register.html', error=error, success=success)


@app.route('/logout')
def logout():
    """Выход из системы"""
    # Удаляем все данные пользователя из сессии
    session.clear()
    # Или удаляем только данные аутентификации
    # session.pop('token', None)
    # session.pop('username', None)
    # session.pop('is_admin', None)
    
    # Добавляем сообщение об успешном выходе
    flash('Вы успешно вышли из системы')
    
    # Перенаправляем на страницу входа
    return redirect(url_for('login'))

@app.route('/news')
def news():
    """Страница новостей"""
    if 'token' not in session:
        return redirect(url_for('login'))
    
    return render_template('news.html')

@app.route('/trends')
def trends():
    """Страница трендов"""
    if 'token' not in session:
        return redirect(url_for('login'))
    
    return render_template('trends.html')

@app.route('/channels')
def channels():
    """Страница каналов"""
    if 'token' not in session:
        return redirect(url_for('login'))
    
    return render_template('channels.html')

@app.route('/admin')
def admin():
    """Административная панель"""
    if 'token' not in session:
        return redirect(url_for('login'))
    
    if not session.get('is_admin', False):
        return redirect(url_for('index'))
    
    return render_template('admin.html')

@app.route('/admin/user/<int:user_id>')
def admin_user_details(user_id):
    """Детальная информация о пользователе для админа"""
    if 'token' not in session:
        return redirect(url_for('login'))
    
    if not session.get('is_admin', False):
        return redirect(url_for('index'))
    
    return render_template('admin_user_details.html', user_id=user_id)

# API proxy routes для избежания проблем с CORS
@app.route('/api/proxy/<path:endpoint>', methods=['GET', 'POST', 'PATCH', 'DELETE'])
def api_proxy(endpoint):
    """Прокси для API запросов"""
    if 'token' not in session and endpoint not in ['auth/register', 'auth/login']:
        return jsonify({"error": "Unauthorized"}), 401
    
    url = f"{API_URL}/api/{endpoint}"
    
    headers = {}
    if 'token' in session:
        headers['Authorization'] = f"Bearer {session['token']}"
    
    try:
        if request.method == 'GET':
            response = requests.get(url, headers=headers, params=request.args)
        elif request.method == 'POST':
            response = requests.post(url, headers=headers, json=request.json or request.form.to_dict())
        elif request.method == 'PATCH':
            response = requests.patch(url, headers=headers, json=request.json)
        elif request.method == 'DELETE':
            response = requests.delete(url, headers=headers)
        
        return response.json(), response.status_code
    
    except Exception as e:
        logger.error(f"API proxy error: {e}")
        return jsonify({"error": "Error connecting to API"}), 500

# Форматирование дат для шаблонов
@app.template_filter('formatdatetime')
def format_datetime(value):
    """Форматирование даты и времени"""
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
        except:
            return value
    else:
        dt = value
    
    return dt.strftime('%d.%m.%Y %H:%M')

if __name__ == '__main__':
    logger.info(f"Starting frontend server on {FRONTEND_HOST}:{FRONTEND_PORT}")
    app.run(host=FRONTEND_HOST, port=FRONTEND_PORT, debug=True)

