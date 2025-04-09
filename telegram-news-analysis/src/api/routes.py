from flask import jsonify, request
import logging
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)


def get_filtered_query(user_id, pg_manager):
    """Возвращает список каналов пользователя для фильтрации"""
    try:
        with pg_manager as conn:  # Use connection manager as context manager
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT channel_name FROM user_channels 
                    WHERE user_id = %s
                """, (user_id,))
                channels = [row[0] for row in cur.fetchall()]
        return channels
    
    except Exception as e:
        logger.error(f"Error getting user channels: {e}")
        return []


def register_routes(app, db, es, token_required, admin_required, pg_manager):
    """Регистрация маршрутов API"""

    @app.route('/api/news', methods=['GET'])
    @token_required
    def get_news():
        try:
            user_id = request.user['id']
            user_filter = get_filtered_query(user_id, pg_manager)
            page = int(request.args.get('page', 1))
            per_page = int(request.args.get('per_page', 10))

            
            # Запрос к MongoDB с пагинацией
            total = db.messages.count_documents(user_filter)
            news = list(db.messages.find(
                user_filter,
                {'_id': 0}
            ).sort('date', -1).skip((page-1)*per_page).limit(per_page))
            
            return jsonify({
                'data': news,
                'total': total,
                'page': page,
                'per_page': per_page
            })
        except Exception as e:
            logger.error(f"Error getting news: {e}")
            return jsonify({'message': 'Error getting news'}), 500
    
    @app.route('/api/news/latest', methods=['GET'])
    @token_required
    def get_latest_news():
        """Получение последних новостей"""
        try:
            user_id = request.user.get('id')
            user_channels = get_filtered_query(user_id, pg_manager)
            limit = int(request.args.get('limit', 10))
            skip = int(request.args.get('skip', 0))
            channel = request.args.get('channel')
            
            # Построение запроса к Elasticsearch
            query = {
                "sort": [{"date": {"order": "desc"}}],
                "size": limit,
                "from": skip
            }
            
            # Добавляем фильтр по каналам, если указан конкретный канал или есть каналы пользователя
            if channel:
                if channel not in user_channels:
                    return jsonify({"error": "Channel not available for user"}), 403
                query["query"] = {
                    "term": {"channel_name.keyword": channel}
                }
            elif user_channels:  # Фильтр по всем каналам пользователя
                query["query"] = {
                    "terms": {"channel_name.keyword": user_channels}
                }
            
            result = es.search(index=app.config['ELASTICSEARCH_INDEX'], body=query)
            
            news = []
            for hit in result['hits']['hits']:
                item = hit['_source']
                news.append(item)
            
            return jsonify({
                "news": news,
                "total": result['hits']['total']['value'],
                "page": skip // limit + 1 if limit > 0 else 1,
                "limit": limit
            })
        
        except Exception as e:
            logger.error(f"Error retrieving latest news: {e}")
            return jsonify({"error": str(e)}), 500
    
    @app.route('/api/news/search', methods=['GET'])
    @token_required
    def search_news():
        """Поиск новостей по запросу"""
        try:
            query_text = request.args.get('q', '')
            limit = int(request.args.get('limit', 10))
            skip = int(request.args.get('skip', 0))
            
            if not query_text:
                return jsonify({"error": "Query parameter 'q' is required"}), 400
            
            # Построение запроса поиска
            query = {
                "query": {
                    "multi_match": {
                        "query": query_text,
                        "fields": ["text", "entities.entity^2", "topics^1.5"],
                        "type": "best_fields",
                        "fuzziness": "AUTO"
                    }
                },
                "sort": [{"_score": {"order": "desc"}}, {"date": {"order": "desc"}}],
                "size": limit,
                "from": skip
            }
            
            result = es.search(index=app.config['ELASTICSEARCH_INDEX'], body=query)
            
            news = []
            for hit in result['hits']['hits']:
                item = hit['_source']
                item['score'] = hit['_score']
                news.append(item)
            
            return jsonify({
                "news": news,
                "total": result['hits']['total']['value'],
                "page": skip // limit + 1 if limit > 0 else 1,
                "limit": limit,
                "query": query_text
            })
        
        except Exception as e:
            logger.error(f"Error searching news: {e}")
            return jsonify({"error": str(e)}), 500
    
    @app.route('/api/trends', methods=['GET'])
    @token_required
    def get_trends():
        """Получение текущих трендов"""
        try:
            user_id = request.user.get('id')
            user_channels = get_filtered_query(user_id, pg_manager)
            time_period = request.args.get('period', '24h')
            
            # Определение временного интервала
            if time_period == '24h':
                time_range = "now-1d"
            elif time_period == '7d':
                time_range = "now-7d"
            elif time_period == '1h':
                time_range = "now-1h"
            else:
                time_range = "now-1d"
            
            # Запрос для получения популярных тем
            query = {
                "query": {
                    "bool": {
                        "must": [
                            {"terms": {"channel_name.keyword": user_channels}},
                            {
                                "range": {
                                    "date": {
                                        "gte": time_range,
                                        "lte": "now"
                                    }
                                }
                            }
                        ]
                    }
                },
                "aggs": {
                    "topics": {
                        "terms": {
                            "field": "topics.keyword",
                            "size": 20
                        }
                    },
                    "entities": {
                        "nested": {
                            "path": "entities"
                        },
                        "aggs": {
                            "entity_types": {
                                "terms": {
                                    "field": "entities.type",
                                    "size": 5
                                },
                                "aggs": {
                                    "top_entities": {
                                        "terms": {
                                            "field": "entities.entity",
                                            "size": 10
                                        }
                                    }
                                }
                            }
                        }
                    },
                    "sentiments": {
                        "terms": {
                            "field": "sentiment.label",
                            "size": 3
                        }
                    }
                },
                "size": 0
            }
            
            result = es.search(index=app.config['ELASTICSEARCH_INDEX'], body=query)
            
            # Обработка результатов
            topics = [{"topic": bucket["key"], "count": bucket["doc_count"]} 
                      for bucket in result["aggregations"]["topics"]["buckets"]]
            
            entities = {}
            for type_bucket in result["aggregations"]["entities"]["entity_types"]["buckets"]:
                entity_type = type_bucket["key"]
                entities[entity_type] = [
                    {"entity": entity["key"], "count": entity["doc_count"]}
                    for entity in type_bucket["top_entities"]["buckets"]
                ]
            
            sentiments = [{"sentiment": bucket["key"], "count": bucket["doc_count"]} 
                          for bucket in result["aggregations"]["sentiments"]["buckets"]]
            
            return jsonify({
                "trends": {
                    "topics": topics,
                    "entities": entities,
                    "sentiments": sentiments
                },
                "period": time_period
            })
        
        except Exception as e:
            logger.error(f"Error retrieving trends: {e}")
            return jsonify({"error": str(e)}), 500
    
    @app.route('/api/channels', methods=['GET'])
    @token_required
    def get_channels():
        """Получение списка каналов пользователя"""
        try:
            user_id = request.user.get('id')
            if not user_id:
                return jsonify({"error": "ID пользователя не найден"}), 400
            channels = []
            
            # Use the connection manager properly
            with pg_manager as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT id, channel_name, channel_title, added_at 
                        FROM user_channels 
                        WHERE user_id = %s
                        ORDER BY added_at DESC
                    """, (user_id,))
                    
                    channels = cur.fetchall()
            
            # Получение статистики из Elasticsearch
            channel_names = [channel['channel_name'] for channel in channels]
            
            if channel_names:
                # Запрос статистики для каналов пользователя
                query = {
                    "size": 0,
                    "aggs": {
                        "channels": {
                            "terms": {
                                "field": "channel_name.keyword",
                                "size": 100,
                                "include": channel_names
                            },
                            "aggs": {
                                "last_24h": {
                                    "filter": {
                                        "range": {
                                            "date": {
                                                "gte": "now-1d",
                                                "lte": "now"
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                
                result = es.search(index=app.config['ELASTICSEARCH_INDEX'], body=query)
                
                # Обработка результатов
                channels_stats = {}
                for bucket in result["aggregations"]["channels"]["buckets"]:
                    channel = bucket["key"]
                    channels_stats[channel] = {
                        "total_messages": bucket["doc_count"],
                        "last_24h_messages": bucket["last_24h"]["doc_count"]
                    }
                
                # Обогащение данных каналов статистикой
                for channel in channels:
                    channel_name = channel.get("channel_name", "")
                    if channel_name in channels_stats:
                        channel["stats"] = channels_stats[channel_name]
                    else:
                        channel["stats"] = {"total_messages": 0, "last_24h_messages": 0}
            
            return jsonify({"channels": channels})
        
        except Exception as e:
            logger.error(f"Error retrieving user channels: {e}")
            return jsonify({"error": str(e)}), 500
    
    @app.route('/api/channels', methods=['POST'])
    @token_required
    def add_channel():
        """Добавление нового канала для пользователя"""
        try:
            user_id = request.user.get('id')
            data = request.json
            
            if not data or not data.get('channel_name'):
                return jsonify({"error": "Channel name is required"}), 400
            
            channel_name = data.get('channel_name')
            channel_title = data.get('channel_title', channel_name)
            
            # Убираем @ из имени канала, если есть
            if channel_name.startswith('@'):
                channel_name = channel_name[1:]
            
            # Use the connection manager properly
            with pg_manager as conn:
                with conn.cursor() as cur:
                    # Проверяем, не добавлен ли уже этот канал
                    cur.execute("""
                        SELECT id FROM user_channels 
                        WHERE user_id = %s AND channel_name = %s
                    """, (user_id, channel_name))
                    
                    if cur.fetchone():
                        return jsonify({"error": "Channel already added"}), 409
                    
                    # Добавляем канал
                    cur.execute("""
                        INSERT INTO user_channels (user_id, channel_name, channel_title)
                        VALUES (%s, %s, %s)
                        RETURNING id
                    """, (user_id, channel_name, channel_title))
                    
                    channel_id = cur.fetchone()[0]
                    conn.commit()
            
            return jsonify({
                "message": "Channel added successfully",
                "channel_id": channel_id,
                "channel_name": channel_name,
                "channel_title": channel_title
            }), 201
            
        except Exception as e:
            logger.error(f"Error in channel addition: {e}")
            return jsonify({"error": str(e)}), 500
    
    @app.route('/api/channels/<int:channel_id>', methods=['DELETE'])
    @token_required
    def delete_channel(channel_id):
        """Удаление канала пользователя"""
        try:
            user_id = request.user.get('id')
            
            # Use the connection manager properly
            with pg_manager as conn:
                with conn.cursor() as cur:
                    # Проверяем, принадлежит ли канал пользователю
                    cur.execute("""
                        SELECT id FROM user_channels 
                        WHERE id = %s AND user_id = %s
                    """, (channel_id, user_id))
                    
                    if not cur.fetchone():
                        return jsonify({"error": "Channel not found or not owned by user"}), 404
                    
                    # Удаляем канал
                    cur.execute("DELETE FROM user_channels WHERE id = %s", (channel_id,))
                    conn.commit()
            
            return jsonify({"message": "Channel deleted successfully"}), 200
        
        except Exception as e:
            logger.error(f"Error deleting channel: {e}")
            return jsonify({"error": str(e)}), 500
    
    # Админ-маршруты
    @app.route('/api/admin/users', methods=['GET'])
    @admin_required
    def get_users():
        """Получение списка всех пользователей (только для админа)"""
        try:
            users = []
            
            # Use the connection manager properly
            with pg_manager as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT id, username, email, is_admin, is_active, created_at 
                        FROM users
                        ORDER BY created_at DESC
                    """)
                    
                    users = cur.fetchall()
                    
                    # Подсчет каналов для каждого пользователя
                    for user in users:
                        cur.execute("""
                            SELECT COUNT(*) FROM user_channels
                            WHERE user_id = %s
                        """, (user['id'],))
                        
                        channels_count = cur.fetchone()['count']
                        user['channels_count'] = channels_count
            
            return jsonify({"users": users})
        
        except Exception as e:
            logger.error(f"Error retrieving users: {e}")
            return jsonify({"error": str(e)}), 500
    
    @app.route('/api/admin/users/<int:user_id>', methods=['GET'])
    @admin_required
    def get_user_details(user_id):
        """Получение подробной информации о пользователе (только для админа)"""
        try:
            # Use the connection manager properly
            with pg_manager as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    # Получаем информацию о пользователе
                    cur.execute("""
                        SELECT id, username, email, is_admin, is_active, created_at 
                        FROM users
                        WHERE id = %s
                    """, (user_id,))
                    
                    user = cur.fetchone()
                    
                    if not user:
                        return jsonify({"error": "User not found"}), 404
                    
                    # Преобразуем datetime объекты в строки
                    if user and 'created_at' in user and user['created_at']:
                        user['created_at'] = user['created_at'].isoformat()
                    
                    # Получаем каналы пользователя
                    cur.execute("""
                        SELECT id, channel_name, channel_title, added_at 
                        FROM user_channels
                        WHERE user_id = %s
                        ORDER BY added_at DESC
                    """, (user_id,))
                    
                    channels = cur.fetchall()
                    
                    # Преобразуем datetime объекты в строки для каждого канала
                    for channel in channels:
                        if 'added_at' in channel and channel['added_at']:
                            channel['added_at'] = channel['added_at'].isoformat()
                    
                    user['channels'] = channels
            
            return jsonify({"user": user})
        
        except Exception as e:
            logger.error(f"Error retrieving user details: {e}")
            return jsonify({"error": str(e)}), 500

    
    @app.route('/api/admin/users/<int:user_id>', methods=['PATCH'])
    @admin_required
    def update_user(user_id):
        """Обновление информации о пользователе (только для админа)"""
        try:
            data = request.json
            
            # Проверка, не пытается ли админ отключить свой аккаунт
            if user_id == request.user['id'] and 'is_active' in data and not data['is_active']:
                return jsonify({"error": "Cannot deactivate your own account"}), 400
            
            # Use the connection manager properly
            with pg_manager as conn:
                with conn.cursor() as cur:
                    # Проверяем, существует ли пользователь
                    cur.execute("SELECT id FROM users WHERE id = %s", (user_id,))
                    
                    if not cur.fetchone():
                        return jsonify({"error": "User not found"}), 404
                    
                    # Строим запрос на обновление
                    update_fields = []
                    update_values = []
                    
                    if 'is_active' in data:
                        update_fields.append("is_active = %s")
                        update_values.append(data['is_active'])
                    
                    if 'is_admin' in data:
                        update_fields.append("is_admin = %s")
                        update_values.append(data['is_admin'])
                    
                    if 'email' in data:
                        update_fields.append("email = %s")
                        update_values.append(data['email'])
                    
                    if update_fields:
                        update_query = f"UPDATE users SET {', '.join(update_fields)} WHERE id = %s"
                        update_values.append(user_id)
                        
                        cur.execute(update_query, update_values)
                        conn.commit()
                
                return jsonify({"message": "User updated successfully"}), 200
        
        except Exception as e:
            logger.error(f"Error updating user: {e}")
            return jsonify({"error": str(e)}), 500
    
    @app.route('/api/admin/users/<int:user_id>', methods=['DELETE'])
    @admin_required
    def delete_user(user_id):
        """Удаление пользователя (только для админа)"""
        try:
            # Проверка, не пытается ли админ удалить свой аккаунт
            if user_id == request.user['id']:
                return jsonify({"error": "Cannot delete your own account"}), 400
            
            # Use the connection manager properly
            with pg_manager as conn:
                with conn.cursor() as cur:
                    # Проверяем, существует ли пользователь
                    cur.execute("SELECT id FROM users WHERE id = %s", (user_id,))
                    
                    if not cur.fetchone():
                        return jsonify({"error": "User not found"}), 404
                    
                    # Удаляем пользователя (каскадное удаление также удалит все каналы пользователя)
                    cur.execute("DELETE FROM users WHERE id = %s", (user_id,))
                    conn.commit()
            
            return jsonify({"message": "User deleted successfully"}), 200
        
        except Exception as e:
            logger.error(f"Error deleting user: {e}")
            return jsonify({"error": str(e)}), 500
