from flask import jsonify, request
import logging
from psycopg2.extras import RealDictCursor
from datetime import datetime
import traceback
import time
import requests

logger = logging.getLogger(__name__)

def get_filtered_query(user_id, pg_manager):
    """Возвращает список каналов пользователя для фильтрации"""
    try:
        with pg_manager as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT channel_name FROM user_channels 
                    WHERE user_id = %s
                """, (user_id,))
                channels = [row[0] for row in cur.fetchall()]
                
                if not channels:
                    logger.warning(f"User {user_id} has no channels configured")
                else:
                    logger.debug(f"User {user_id} has {len(channels)} channels: {', '.join(channels)}")
                    
                return channels
    
    except Exception as e:
        logger.error(f"Error getting user channels: {e}")
        logger.debug(traceback.format_exc())
        return []


def register_routes(app, db, es, token_required, admin_required, pg_manager):
    """Регистрация маршрутов API"""

    @app.route('/api/news', methods=['GET'])
    @token_required
    def get_news():
        """Получение новостей из MongoDB с пагинацией"""
        try:
            user_id = request.user['id']
            user_channels = get_filtered_query(user_id, pg_manager)
            
            if not user_channels:
                return jsonify({"message": "No channels available. Please add some channels first."}), 404
            
            page = max(1, int(request.args.get('page', 1)))
            per_page = min(50, max(1, int(request.args.get('per_page', 10))))
            
            mongo_filter = {"channel_name": {"$in": user_channels}}

            date_start = request.args.get('date_start')
            date_end = request.args.get('date_end')
            
            date_filter = {}
            if date_start:
                try:
                    date_filter["$gte"] = datetime.fromisoformat(date_start)
                except ValueError:
                    logger.warning(f"Invalid date_start format: {date_start}")
            
            if date_end:
                try:
                    date_filter["$lte"] = datetime.fromisoformat(date_end)
                except ValueError:
                    logger.warning(f"Invalid date_end format: {date_end}")
            
            if date_filter:
                mongo_filter["date"] = date_filter

            total = db.messages.count_documents(mongo_filter)

            sort_field = request.args.get('sort_field', 'date')
            sort_order = int(request.args.get('sort_order', -1))

            valid_sort_fields = ['date', 'views', 'forwards', 'created_at']
            if sort_field not in valid_sort_fields:
                sort_field = 'date'

            news = list(db.messages.find(
                mongo_filter,
                {'_id': 0}
            ).sort(sort_field, sort_order).skip((page-1)*per_page).limit(per_page))

            for item in news:
                if 'date' in item and isinstance(item['date'], datetime):
                    item['date'] = item['date'].isoformat()
                if 'created_at' in item and isinstance(item['created_at'], datetime):
                    item['created_at'] = item['created_at'].isoformat()
            
            return jsonify({
                'data': news,
                'total': total,
                'page': page,
                'per_page': per_page,
                'total_pages': (total + per_page - 1) // per_page
            })
        except ValueError as e:
            logger.warning(f"Invalid parameter value: {e}")
            return jsonify({'message': f'Invalid parameter: {str(e)}'}), 400
        except Exception as e:
            logger.error(f"Error getting news: {e}")
            logger.debug(traceback.format_exc())
            return jsonify({'message': 'Error retrieving news data'}), 500
    
    @app.route('/api/news/latest', methods=['GET'])
    @token_required
    def get_latest_news():
        """Получение последних новостей из Elasticsearch"""
        try:
            user_id = request.user.get('id')
            user_channels = get_filtered_query(user_id, pg_manager)

            limit = min(50, max(1, int(request.args.get('limit', 10))))
            skip = max(0, int(request.args.get('skip', 0)))
            channel = request.args.get('channel')

            if not user_channels:
                return jsonify({
                    "warning": "No channels configured",
                    "news": [],
                    "total": 0,
                    "page": 1,
                    "limit": limit
                }), 200
            
            logger.debug(f"User {user_id} fetching latest news, channels: {user_channels}")

            query = {
                "sort": [{"date": {"order": "desc"}}],
                "size": limit,
                "from": skip
            }

            if channel:
                if channel not in user_channels:
                    return jsonify({"error": "Channel not in user's channel list"}), 403
                    
                query["query"] = {
                    "term": {"channel_name.keyword": channel}
                }
            elif user_channels:
                query["query"] = {
                    "terms": {"channel_name.keyword": user_channels}
                }
            else:
                return jsonify({"error": "No channels available for this user"}), 403

            date_start = request.args.get('date_start')
            date_end = request.args.get('date_end')
            
            if date_start or date_end:
                date_range = {}
                if date_start:
                    date_range["gte"] = date_start
                if date_end:
                    date_range["lte"] = date_end

                if "query" in query:
                    current_query = query["query"]
                    query["query"] = {
                        "bool": {
                            "must": [
                                current_query,
                                {"range": {"date": date_range}}
                            ]
                        }
                    }
                else:
                    query["query"] = {"range": {"date": date_range}}

            if not es:
                logger.error("Elasticsearch connection not available")
                return jsonify({"error": "Search service temporarily unavailable"}), 503

            try:
                logger.debug(f"Elasticsearch query: {query}")
                result = es.search(index=app.config['ELASTICSEARCH_INDEX'], body=query)
                logger.debug(f"ES search results: {result['hits']['total']['value']} hits")
            except Exception as e:
                logger.error(f"Elasticsearch query failed: {e}")
                logger.debug(traceback.format_exc())
                return jsonify({"error": "Search request failed", "details": str(e)}), 500

            news = []
            for hit in result['hits']['hits']:
                item = hit['_source']
                if '_score' in hit and hit['_score'] is not None:
                    item['relevance_score'] = hit['_score']
                news.append(item)
            
            return jsonify({
                "news": news,
                "total": result['hits']['total']['value'],
                "page": skip // limit + 1 if limit > 0 else 1,
                "limit": limit
            })
        
        except ValueError as e:
            logger.warning(f"Invalid parameter value: {e}")
            return jsonify({"error": f"Invalid parameter: {str(e)}"}), 400
        except Exception as e:
            logger.error(f"Error retrieving latest news: {e}")
            logger.debug(traceback.format_exc())
            return jsonify({"error": "Failed to retrieve news", "details": str(e)}), 500
    
    @app.route('/api/news/search', methods=['GET'])
    @token_required
    def search_news():
        """Полнотекстовый поиск новостей"""
        try:
            user_id = request.user.get('id')
            user_channels = get_filtered_query(user_id, pg_manager)
            
            query_text = request.args.get('q', '').strip()
            limit = min(50, max(1, int(request.args.get('limit', 10))))
            skip = max(0, int(request.args.get('skip', 0)))
            
            if not query_text:
                return jsonify({"error": "Search query is required"}), 400

            if not user_channels:
                return jsonify({
                    "warning": "No channels configured",
                    "news": [],
                    "total": 0,
                    "query": query_text
                }), 200

            query = {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "multi_match": {
                                    "query": query_text,
                                    "fields": ["text^1", "channel_name^0.5", "topics^2"],
                                    "type": "best_fields",
                                    "fuzziness": "AUTO"
                                }
                            },
                            {
                                "terms": {
                                    "channel_name.keyword": user_channels
                                }
                            }
                        ]
                    }
                },
                "highlight": {
                    "fields": {
                        "text": {"number_of_fragments": 3, "fragment_size": 150}
                    },
                    "pre_tags": ["<mark>"],
                    "post_tags": ["</mark>"]
                },
                "sort": [
                    {"_score": {"order": "desc"}},
                    {"date": {"order": "desc"}}
                ],
                "size": limit,
                "from": skip
            }

            date_start = request.args.get('date_start')
            date_end = request.args.get('date_end')
            
            if date_start or date_end:
                date_range = {}
                if date_start:
                    date_range["gte"] = date_start
                if date_end:
                    date_range["lte"] = date_end
                
                query["query"]["bool"]["must"].append({"range": {"date": date_range}})

            if not es:
                logger.error("Elasticsearch connection not available")
                return jsonify({"error": "Search service temporarily unavailable"}), 503
                
            try:
                result = es.search(index=app.config['ELASTICSEARCH_INDEX'], body=query)
            except Exception as e:
                logger.error(f"Elasticsearch search failed: {e}")
                logger.debug(traceback.format_exc())
                return jsonify({"error": "Search request failed", "details": str(e)}), 500

            news = []
            for hit in result['hits']['hits']:
                item = hit['_source']
                item['score'] = hit['_score']

                if 'highlight' in hit and 'text' in hit['highlight']:
                    item['highlights'] = hit['highlight']['text']
                
                news.append(item)
            
            return jsonify({
                "news": news,
                "total": result['hits']['total']['value'],
                "page": skip // limit + 1 if limit > 0 else 1,
                "limit": limit,
                "query": query_text
            })
        
        except ValueError as e:
            logger.warning(f"Invalid parameter value: {e}")
            return jsonify({"error": f"Invalid parameter: {str(e)}"}), 400
        except Exception as e:
            logger.error(f"Error searching news: {e}")
            logger.debug(traceback.format_exc())
            return jsonify({"error": "Search failed", "details": str(e)}), 500
    
    @app.route('/api/trends', methods=['GET'])
    @token_required
    def get_trends():
        """Получение трендов и аналитики"""
        try:
            user_id = request.user.get('id')
            user_channels = get_filtered_query(user_id, pg_manager)
            time_period = request.args.get('period', '24h')

            if not user_channels:
                return jsonify({
                    "warning": "No channels configured",
                    "trends": {
                        "topics": [],
                        "entities": [],
                        "sentiments": []
                    },
                    "period": time_period
                }), 200
            
            now = int(datetime.now().timestamp() * 1000)
            if time_period == "1h":
                start_time = now - (3600 * 1000)
            elif time_period == "6h":
                start_time = now - (6 * 3600 * 1000)
            elif time_period == "12h":
                start_time = now - (12 * 3600 * 1000)
            elif time_period == "24h" or time_period == "1d":
                start_time = now - (24 * 3600 * 1000)
            elif time_period == "7d":
                start_time = now - (7 * 24 * 3600 * 1000)
            elif time_period == "30d":
                start_time = now - (30 * 24 * 3600 * 1000)
            else:
                start_time = now - (24 * 3600 * 1000)

            query = {
                "query": {
                    "bool": {
                        "must": [
                            {"terms": {"channel_name.keyword": user_channels}},
                            {
                                "range": {
                                    "date": {
                                        "gte": start_time,
                                        "lte": now
                                    }
                                }
                            }
                        ]
                    }
                },
                "aggs": {
                    "topics": {
                        "terms": {"field": "topics.label.keyword", "size": 20}
                    },
                    "main_entities": {
                        "terms": {
                            "field": "main_entity.name.keyword",
                            "size": 20,
                            "missing": "Unknown"
                        }
                    },
                    "channels": {
                        "terms": {
                            "field": "channel_name.keyword",
                            "size": 20
                        },
                        "aggs": {
                            "messages_over_time": {
                                "date_histogram": {
                                    "field": "date",
                                    "calendar_interval": "1d",
                                    "format": "yyyy-MM-dd"
                                }
                            }
                        }
                    },
                    "sentiment_distribution": {
                        "terms": {
                            "field": "sentiment.label.keyword",
                            "size": 5
                        }
                    },
                    "hourly_activity": {
                        "date_histogram": {
                            "field": "date",
                            "calendar_interval": "1h",
                            "format": "HH"
                        }
                    }
                },
                "size": 0
            }

            if not es:
                logger.error("Elasticsearch connection not available")
                return jsonify({"error": "Analytics service temporarily unavailable"}), 503

            try:
                logger.debug(f"Elasticsearch trends query: {query}")
                result = es.search(index=app.config['ELASTICSEARCH_INDEX'], body=query)
                logger.debug(f"Elasticsearch trends result: {result}")
            except Exception as e:
                logger.error(f"Elasticsearch aggregation failed: {e}")
                logger.debug(traceback.format_exc())
                return jsonify({"error": "Analytics request failed", "details": str(e)}), 500

            try:
                topics = [
                    {"topic": bucket["key"], "count": bucket["doc_count"]}
                    for bucket in result["aggregations"]["topics"]["buckets"]
                    if bucket["key"]
                ]
            except KeyError:
                logger.warning("Topics aggregation not available in results")
                topics = []
            entities = []
            if "main_entities" in result["aggregations"]:
                entities = [
                    {"entity": bucket["key"], "count": bucket["doc_count"]} 
                    for bucket in result["aggregations"]["main_entities"]["buckets"]
                    if bucket["key"] and bucket["key"] != "Unknown"
                ]

            sentiments = [
                {"sentiment": bucket["key"], "count": bucket["doc_count"]} 
                for bucket in result["aggregations"]["sentiment_distribution"]["buckets"]
            ]

            channels = []
            for bucket in result["aggregations"]["channels"]["buckets"]:
                channel_data = {
                    "name": bucket["key"],
                    "total_messages": bucket["doc_count"],
                    "time_series": [
                        {"date": time_bucket["key_as_string"], "count": time_bucket["doc_count"]}
                        for time_bucket in bucket["messages_over_time"]["buckets"]
                    ]
                }
                channels.append(channel_data)

            hourly_activity = [
                {"hour": time_bucket["key_as_string"], "count": time_bucket["doc_count"]}
                for time_bucket in result["aggregations"]["hourly_activity"]["buckets"]
            ]

            logger.info(f"Topics found: {len(topics)}")
            logger.info(f"Entities found: {len(entities)}")
            logger.info(f"Sentiments found: {len(sentiments)}")
            
            return jsonify({
                "trends": {
                    "topics": topics,
                    "entities": entities,
                    "sentiments": sentiments,
                    "channels": channels,
                    "hourly_activity": hourly_activity
                },
                "period": time_period
            })
        
        except Exception as e:
            logger.error(f"Error retrieving trends: {e}")
            logger.debug(traceback.format_exc())
            return jsonify({"error": "Failed to retrieve trends", "details": str(e)}), 500
        
    @app.route('/api/trends/topics', methods=['GET'])
    @token_required
    def get_trend_topics():
        """Получение популярных тем"""
        try:
            user_id = request.user.get('id')
            user_channels = get_filtered_query(user_id, pg_manager)
            limit = min(50, max(1, int(request.args.get('limit', 20))))
            period = request.args.get('period', '24h')

            if not user_channels:
                return jsonify({"topics": [], "message": "No channels configured"}), 200

            now = int(datetime.now().timestamp() * 1000)
            if period == "1h":
                start_time = now - (3600 * 1000)
            elif period == "6h":
                start_time = now - (6 * 3600 * 1000)
            elif period == "24h" or period == "1d":
                start_time = now - (24 * 3600 * 1000)
            elif period == "7d":
                start_time = now - (7 * 24 * 3600 * 1000)
            elif period == "30d":
                start_time = now - (30 * 24 * 3600 * 1000)
            else:
                start_time = now - (24 * 3600 * 1000)

            query = {
                "query": {
                    "bool": {
                        "must": [
                            {"terms": {"channel_name.keyword": user_channels}},
                            {"range": {"date": {"gte": start_time, "lte": now}}}
                        ]
                    }
                },
                "aggs": {
                    "topics": {
                        "terms": {
                            "field": "topics.label.keyword",
                            "size": limit
                        }
                    }
                },
                "size": 0
            }

            if not es:
                return jsonify({"error": "Elasticsearch not available"}), 503

            try:
                result = es.search(index=app.config['ELASTICSEARCH_INDEX'], body=query)
                topics = [
                    {"topic": bucket["key"], "count": bucket["doc_count"]}
                    for bucket in result["aggregations"]["topics"]["buckets"]
                    if bucket["key"]
                ]
                return jsonify({"topics": topics, "period": period})
            except Exception as e:
                logger.error(f"Elasticsearch query failed: {e}")
                return jsonify({"error": "Failed to retrieve topics"}), 500

        except ValueError as e:
            logger.warning(f"Invalid parameter value: {e}")
            return jsonify({"error": f"Invalid parameter: {str(e)}"}), 400
        except Exception as e:
            logger.error(f"Error retrieving topics: {e}")
            return jsonify({"error": "Failed to retrieve topics"}), 500

    @app.route('/api/trends/entities', methods=['GET'])
    @token_required
    def get_trend_entities():
        """Получение популярных сущностей"""
        try:
            user_id = request.user.get('id')
            user_channels = get_filtered_query(user_id, pg_manager)
            limit = min(50, max(1, int(request.args.get('limit', 30))))
            period = request.args.get('period', '24h')

            if not user_channels:
                return jsonify({"entities": [], "message": "No channels configured"}), 200

            now = int(datetime.now().timestamp() * 1000)
            if period == "1h":
                start_time = now - (3600 * 1000)
            elif period == "6h":
                start_time = now - (6 * 3600 * 1000)
            elif period == "24h" or period == "1d":
                start_time = now - (24 * 3600 * 1000)
            elif period == "7d":
                start_time = now - (7 * 24 * 3600 * 1000)
            elif period == "30d":
                start_time = now - (30 * 24 * 3600 * 1000)
            else:
                start_time = now - (24 * 3600 * 1000)

            query = {
                "query": {
                    "bool": {
                        "must": [
                            {"terms": {"channel_name.keyword": user_channels}},
                            {"range": {"date": {"gte": start_time, "lte": now}}}
                        ]
                    }
                },
                "aggs": {
                    "entities": {
                        "terms": {
                            "field": "main_entity.name.keyword",
                            "size": limit,
                            "missing": "Unknown"
                        }
                    }
                },
                "size": 0
            }

            if not es:
                return jsonify({"error": "Elasticsearch not available"}), 503

            try:
                result = es.search(index=app.config['ELASTICSEARCH_INDEX'], body=query)
                entities = [
                    {"entity": bucket["key"], "count": bucket["doc_count"]}
                    for bucket in result["aggregations"]["entities"]["buckets"]
                    if bucket["key"] and bucket["key"] != "Unknown"
                ]
                return jsonify({"entities": entities, "period": period})
            except Exception as e:
                logger.error(f"Elasticsearch query failed: {e}")
                return jsonify({"error": "Failed to retrieve entities"}), 500

        except ValueError as e:
            logger.warning(f"Invalid parameter value: {e}")
            return jsonify({"error": f"Invalid parameter: {str(e)}"}), 400
        except Exception as e:
            logger.error(f"Error retrieving entities: {e}")
            return jsonify({"error": "Failed to retrieve entities"}), 500
    
    @app.route('/api/reports/generate', methods=['POST'])
    @token_required
    def generate_report():
        """Генерация отчета по сущности или теме с использованием LLM"""
        try:
            user_id = request.user.get('id')
            data = request.json
            
            if not data:
                return jsonify({"error": "No data provided"}), 400
                
            report_type = data.get('type', 'entity')
            entity_or_topic = data.get('entity_or_topic')
            time_period = data.get('period', '7d')
            
            if not entity_or_topic:
                return jsonify({"error": "Entity or topic name is required"}), 400

            user_channels = get_filtered_query(user_id, pg_manager)
            
            now = int(datetime.now().timestamp() * 1000)
            if time_period == "1h":
                start_time = now - (3600 * 1000)
            elif time_period == "6h":
                start_time = now - (6 * 3600 * 1000)
            elif time_period == "12h":
                start_time = now - (12 * 3600 * 1000)
            elif time_period == "24h" or time_period == "1d":
                start_time = now - (24 * 3600 * 1000)
            elif time_period == "7d":
                start_time = now - (7 * 24 * 3600 * 1000)
            elif time_period == "30d":
                start_time = now - (30 * 24 * 3600 * 1000)
            else:
                start_time = now - (24 * 3600 * 1000)

            base_conditions = [
                {"terms": {"channel_name.keyword": user_channels}},
                {"range": {"date": {"gte": start_time, "lte": now}}},
            ]

            if report_type == 'entity':
                query = {
                    "query": {
                        "bool": {
                            "must": base_conditions,
                            "should": [
                                {"match": {"main_entity.name.keyword": entity_or_topic}},
                                {"match_phrase": {"text": entity_or_topic}},
                                {"match": {"entity_mentions.entity_name.keyword": entity_or_topic}}
                            ],
                            "minimum_should_match": 1
                        }
                    },
                    "sort": [{"date": {"order": "desc"}}],
                    "size": 20
                }
            else:
                query = {
                    "query": {
                        "bool": {
                            "must": base_conditions,
                            "should": [
                                {"match": {"topics.label.keyword": entity_or_topic}},
                                {"match_phrase": {"text": entity_or_topic}}
                            ],
                            "minimum_should_match": 1
                        }
                    },
                    "sort": [{"date": {"order": "desc"}}],
                    "size": 20
                }

            if not es:
                return jsonify({"error": "Elasticsearch not available"}), 503
  
            try:
                result = es.search(index=app.config['ELASTICSEARCH_INDEX'], body=query)

                if result['hits']['total']['value'] == 0:
                    return jsonify({
                        "error": f"No news found for {report_type} '{entity_or_topic}' in the selected period"
                    }), 404
                    
            except Exception as e:
                logger.error(f"Elasticsearch search error: {e}")
                return jsonify({"error": f"Search error: {str(e)}"}), 500             

            selected_news = result['hits']['hits'] 
            total_found = result['hits']['total']['value']

            news_items_text = ""
            for i, hit in enumerate(selected_news, 1):
                source = hit['_source']
                news_text = source.get('text', '')
                    
                try:
                    date_str = source.get('date')
                    if isinstance(date_str, (int, float)):
                        date_readable = datetime.fromtimestamp(date_str/1000).strftime('%d.%m.%Y')
                    else:
                        date_readable = date_str
                except:
                    date_readable = source.get('date', 'Не указана')
                    
                news_items_text += f"\nНовость #{i}:\n"
                news_items_text += f"Дата: {date_readable}\n"
                news_items_text += f"Канал: {source.get('channel_name')}\n"
                news_items_text += f"Текст: {news_text}\n"
                news_items_text += f"Тональность: {source.get('sentiment', {}).get('label', 'neutral')}\n"
                news_items_text += "-"*20 + "\n"

            llm_prompt = f"""
                <instruction>
                Ты опытный новостной аналитик с глубоким пониманием российской и международной современной ноаостной повестки. Тебе поручено составить профессиональный аналитический отчет о {"сущности" if report_type == "entity" else "теме"} "{entity_or_topic}" на основе {len(selected_news)} новостей за период {"день" if time_period == "1d" else "неделю" if time_period == "7d" else "месяц" if time_period == "30d" else "квартал"}.

                Формат отчета:
                # Аналитический отчет: {entity_or_topic}
                ## Период: {time_period}
                ## Дата: {datetime.now().strftime('%d.%m.%Y')}

                ### 1. Основные выводы
                - Определи 3-5 ключевых трендов или выводов на основе предоставленных новостей
                - Рассмотри контекст и возможные последствия описанных событий
                - Оцени значимость каждого вывода в более широком плане

                ### 2. Ключевые события
                - Перечисли наиболее важные события в хронологическом порядке
                - Для каждого события укажи его потенциальное значение
                - Выяви причинно-следственные связи между событиями, если они есть

                ### 3. Тональность и тенденции
                - Проведи детальный анализ тональности всех новостей (положительная, отрицательная, нейтральная)
                - Определи, как менялась риторика по данной теме/сущности в течение периода
                - Выяви скрытые намерения и подтексты в сообщениях
                - Сравни освещение событий в разных источниках, если это возможно
                - Сделай прогноз дальнейшего развития ситуации на основании выявленных тенденций

                ### 4. Медиа-стратегия и коммуникации
                - Проанализируй, как преподносится информация о {entity_or_topic}
                - Отметь ключевые месседжи и нарративы, которые продвигаются через новостную повестку
                - Оцени эффективность коммуникационной стратегии

                В своем анализе будь объективен, опирайся только на предоставленные факты, но применяй критическое мышление для выявления скрытых закономерностей. Избегай поверхностных выводов.

                Данные для анализа:
                {news_items_text}

                Пожалуйста, предоставь только текст отчета, без лишних символов в начале и в конце. ТОЛЬКО ТЕКСТ В ТОМ ЖЕ ФОРМАТЕ, ЧТО Я ОТПРАВИЛ и только НА РУССКОМ!
                </instruction>
            """


            llm_url = app.config.get('LLM_URL', 'http://llm-heavy:8000/generate')
            report_text = ""
            generation_method = "fallback"

            max_retries = 2
            retry_delay = 3
            
            for attempt in range(max_retries):
                try:
                    logger.info(f"Sending report request to LLM (attempt {attempt+1})")
                    
                    llm_response = requests.post(
                        llm_url,
                        json={
                            "prompt": llm_prompt,
                            "max_tokens": 7500,
                            "temperature": 0.4,
                            "is_report": True
                        },
                        timeout=800
                    )
                    
                    if llm_response.status_code == 200:
                        llm_result = llm_response.json()
                        report_text = llm_result.get('generated_text', '')
                        generation_method = "llm"
                        
                        if report_text:
                            break

                    logger.warning(f"LLM attempt {attempt+1} failed: {llm_response.status_code}")
                    time.sleep(retry_delay)
                    
                except requests.exceptions.Timeout:
                    logger.warning(f"LLM timeout on attempt {attempt+1}")
                    time.sleep(retry_delay)
                    continue
                except Exception as e:
                    logger.error(f"LLM error on attempt {attempt+1}: {e}")
                    time.sleep(retry_delay)
                    continue

            if not report_text:
                report_text = generate_basic_report(entity_or_topic, time_period, selected_news)
                logger.info("Using fallback report generation")

            return jsonify({
                "report": {
                    "type": report_type,
                    "entity_or_topic": entity_or_topic,
                    "period": time_period,
                    "generated_text": report_text,
                    "news_count": total_found,
                    "analyzed_news": len(selected_news),
                    "generated_at": datetime.now().isoformat(),
                    "generation_method": generation_method
                }
            })
            
        except Exception as e:
            logger.error(f"Error in generate_report: {e}")
            logger.error(traceback.format_exc())
            return jsonify({"error": f"Failed to generate report: {str(e)}"}), 500

    def generate_basic_report(entity_or_topic, time_period, news_items):
        period_text = {"1d": "день", "7d": "неделю", "30d": "месяц", "90d": "квартал"}.get(time_period, "период")

        channels = set()
        dates = []
        sentiments = {"positive": 0, "negative": 0, "neutral": 0}
        
        for item in news_items:
            source = item['_source']
            channels.add(source.get('channel_name', ''))
            dates.append(source.get('date', ''))
            sentiment = source.get('sentiment', {}).get('label', 'neutral')
            sentiments[sentiment] = sentiments.get(sentiment, 0) + 1

        main_sentiment = max(sentiments.items(), key=lambda x: x[1])[0] if sentiments else "neutral"
        sentiment_map = {
            "positive": "позитивная", 
            "negative": "негативная", 
            "neutral": "нейтральная"
        }

        report = f"""Аналитический отчет: {entity_or_topic}
            ## Период: {time_period}
            ## Дата: {datetime.now().strftime('%d.%m.%Y')}

            ### 1. Основные выводы
            За последний {period_text} обнаружено {len(news_items)} упоминаний о "{entity_or_topic}" в {len(channels)} Telegram-каналах. Преобладающая тональность упоминаний - {sentiment_map.get(main_sentiment, "смешанная")}.

            ### 2. Ключевые события
        """

        event_counter = 0
        for _, item in enumerate(news_items[:min(5, len(news_items))], 1):
            source = item['_source']
            try:
                date_str = source.get('date')
                if isinstance(date_str, (int, float)):
                    date = datetime.fromtimestamp(date_str/1000).strftime('%d.%m.%Y')
                else:
                    date = date_str
            except:
                date = "Не указана"
                
            text = source.get('text', '')
            if text:
                event_counter += 1
                short_text = text[:150] + "..." if len(text) > 150 else text
                report += f"{event_counter}. {date}: {short_text}\n\n"

        if event_counter == 0:
            report += "Нет подробных данных о конкретных событиях в указанный период.\n\n"

        total_news = len(news_items) or 1
        report += f"""### 3. Тональность и тенденции
            Распределение тональности упоминаний:
            - Позитивные: {sentiments.get('positive', 0)} ({int(sentiments.get('positive', 0) * 100 / total_news)}%)
            - Негативные: {sentiments.get('negative', 0)} ({int(sentiments.get('negative', 0) * 100 / total_news)}%)
            - Нейтральные: {sentiments.get('neutral', 0)} ({int(sentiments.get('neutral', 0) * 100 / total_news)}%)
        """

        if channels:
            channels_list = list(channels)
            if len(channels_list) <= 5:
                report += f"\nСообщения публиковались в следующих каналах: {', '.join(channels_list)}."
            else:
                report += f"\nСообщения публиковались в {len(channels_list)} каналах, включая: {', '.join(channels_list[:5])}..."

        return report

    @app.route('/api/channels', methods=['GET'])
    @token_required
    def get_channels():
        """Получение списка каналов пользователя с дополнительной статистикой"""
        try:
            user_id = request.user.get('id')
            if not user_id:
                return jsonify({"error": "User ID not found"}), 400
                
            channels = []

            with pg_manager as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT id, channel_name, channel_title, added_at 
                        FROM user_channels 
                        WHERE user_id = %s
                        ORDER BY added_at DESC
                    """, (user_id,))
                    
                    channels = cur.fetchall()

                    for channel in channels:
                        if 'added_at' in channel and channel['added_at']:
                            channel['added_at'] = channel['added_at'].isoformat()

            if not channels:
                return jsonify({"channels": [], "message": "No channels found for this user"})

            channel_names = [channel['channel_name'] for channel in channels]

            if not es:
                logger.warning("Elasticsearch not available for channel statistics")
                for channel in channels:
                    channel["stats"] = {"total_messages": 0, "last_24h_messages": 0}
                return jsonify({"channels": channels, "warning": "Statistics temporarily unavailable"})

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
                            },
                            "last_7d": {
                                "filter": {
                                    "range": {
                                        "date": {
                                            "gte": "now-7d",
                                            "lte": "now"
                                        }
                                    }
                                }
                            },
                            "recent_sentiment": {
                                "filter": {
                                    "range": {
                                        "date": {
                                            "gte": "now-3d",
                                            "lte": "now"
                                        }
                                    }
                                },
                                "aggs": {
                                    "sentiment": {
                                        "terms": {
                                            "field": "sentiment.label",
                                            "size": 3
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            
            try:
                result = es.search(index=app.config['ELASTICSEARCH_INDEX'], body=query)

                channels_stats = {}
                for bucket in result["aggregations"]["channels"]["buckets"]:
                    channel = bucket["key"]

                    sentiment_data = []
                    if "recent_sentiment" in bucket and "sentiment" in bucket["recent_sentiment"]:
                        sentiment_buckets = bucket["recent_sentiment"]["sentiment"]["buckets"]
                        for sentiment in sentiment_buckets:
                            sentiment_data.append({
                                "label": sentiment["key"],
                                "count": sentiment["doc_count"]
                            })
                    
                    channels_stats[channel] = {
                        "total_messages": bucket["doc_count"],
                        "last_24h_messages": bucket["last_24h"]["doc_count"],
                        "last_7d_messages": bucket["last_7d"]["doc_count"],
                        "sentiments": sentiment_data
                    }

                for channel in channels:
                    channel_name = channel.get("channel_name", "")
                    if channel_name in channels_stats:
                        channel["stats"] = channels_stats[channel_name]
                    else:
                        channel["stats"] = {
                            "total_messages": 0, 
                            "last_24h_messages": 0,
                            "last_7d_messages": 0,
                            "sentiments": []
                        }
                        
            except Exception as e:
                logger.error(f"Error getting channel statistics: {e}")
                for channel in channels:
                    channel["stats"] = {"total_messages": 0, "last_24h_messages": 0}
            
            return jsonify({"channels": channels})
        
        except Exception as e:
            logger.error(f"Error retrieving user channels: {e}")
            logger.debug(traceback.format_exc())
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
            
            channel_name = data.get('channel_name').strip()
            channel_title = data.get('channel_title', channel_name).strip()

            if not channel_name:
                return jsonify({"error": "Channel name cannot be empty"}), 400

            if channel_name.startswith('@'):
                channel_name = channel_name[1:]

            with pg_manager as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT id FROM user_channels 
                        WHERE user_id = %s AND channel_name = %s
                    """, (user_id, channel_name))
                    
                    if cur.fetchone():
                        return jsonify({"error": "Channel already added for this user"}), 409

                    cur.execute("""
                        INSERT INTO user_channels (user_id, channel_name, channel_title)
                        VALUES (%s, %s, %s)
                        RETURNING id
                    """, (user_id, channel_name, channel_title))
                    
                    channel_id = cur.fetchone()[0]
                    conn.commit()
            
            logger.info(f"User {user_id} added channel: {channel_name}")
            return jsonify({
                "message": "Channel added successfully",
                "channel_id": channel_id,
                "channel_name": channel_name,
                "channel_title": channel_title
            }), 201
            
        except Exception as e:
            logger.error(f"Error adding channel: {e}")
            logger.debug(traceback.format_exc())
            return jsonify({"error": "Failed to add channel"}), 500
    
    @app.route('/api/channels/<int:channel_id>', methods=['DELETE'])
    @token_required
    def delete_channel(channel_id):
        """Удаление канала пользователя"""
        try:
            user_id = request.user.get('id')
            
            with pg_manager as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT channel_name FROM user_channels 
                        WHERE id = %s AND user_id = %s
                    """, (channel_id, user_id))
                    
                    result = cur.fetchone()
                    if not result:
                        return jsonify({"error": "Channel not found or not owned by user"}), 404
                    
                    channel_name = result[0]

                    cur.execute("DELETE FROM user_channels WHERE id = %s", (channel_id,))
                    conn.commit()
            
            logger.info(f"User {user_id} deleted channel: {channel_name} (ID: {channel_id})")
            return jsonify({
                "message": "Channel deleted successfully",
                "channel_id": channel_id
            }), 200
        
        except Exception as e:
            logger.error(f"Error deleting channel: {e}")
            logger.debug(traceback.format_exc())
            return jsonify({"error": "Failed to delete channel"}), 500
    
    @app.route('/api/channels/<int:channel_id>', methods=['PATCH'])
    @token_required
    def update_channel(channel_id):
        """Обновление информации о канале"""
        try:
            user_id = request.user.get('id')
            data = request.json
            
            if not data:
                return jsonify({"error": "No data provided"}), 400

            channel_title = data.get('channel_title')
            
            if not channel_title or not channel_title.strip():
                return jsonify({"error": "Channel title cannot be empty"}), 400
                
            channel_title = channel_title.strip()
            
            with pg_manager as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT channel_name FROM user_channels 
                        WHERE id = %s AND user_id = %s
                    """, (channel_id, user_id))
                    
                    result = cur.fetchone()
                    if not result:
                        return jsonify({"error": "Channel not found or not owned by user"}), 404

                    cur.execute("""
                        UPDATE user_channels 
                        SET channel_title = %s
                        WHERE id = %s
                    """, (channel_title, channel_id))
                    
                    conn.commit()
            
            return jsonify({
                "message": "Channel updated successfully",
                "channel_id": channel_id,
                "channel_title": channel_title
            }), 200
            
        except Exception as e:
            logger.error(f"Error updating channel: {e}")
            logger.debug(traceback.format_exc())
            return jsonify({"error": "Failed to update channel"}), 500

    @app.route('/api/admin/users', methods=['GET'])
    @admin_required
    def get_users():
        """Получение списка всех пользователей (только для админа)"""
        try:
            users = []
            
            with pg_manager as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT id, username, email, is_admin, is_active, created_at,
                               (SELECT COUNT(*) FROM user_channels WHERE user_id = users.id) as channels_count
                        FROM users
                        ORDER BY created_at DESC
                    """)
                    
                    users = cur.fetchall()
                    for user in users:
                        if 'created_at' in user and user['created_at']:
                            user['created_at'] = user['created_at'].isoformat()
            
            return jsonify({"users": users, "total": len(users)})
        
        except Exception as e:
            logger.error(f"Error retrieving users: {e}")
            logger.debug(traceback.format_exc())
            return jsonify({"error": "Failed to retrieve users"}), 500
    
    @app.route('/api/admin/users/<int:user_id>', methods=['GET'])
    @admin_required
    def get_user_details(user_id):
        """Получение подробной информации о пользователе (только для админа)"""
        try:
            with pg_manager as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT id, username, email, is_admin, is_active, created_at,
                               (SELECT COUNT(*) FROM user_channels WHERE user_id = users.id) as channels_count
                        FROM users
                        WHERE id = %s
                    """, (user_id,))
                    
                    user = cur.fetchone()
                    
                    if not user:
                        return jsonify({"error": "User not found"}), 404

                    if 'created_at' in user and user['created_at']:
                        user['created_at'] = user['created_at'].isoformat()

                    cur.execute("""
                        SELECT id, channel_name, channel_title, added_at 
                        FROM user_channels
                        WHERE user_id = %s
                        ORDER BY added_at DESC
                    """, (user_id,))
                    
                    channels = cur.fetchall()

                    for channel in channels:
                        if 'added_at' in channel and channel['added_at']:
                            channel['added_at'] = channel['added_at'].isoformat()
                    user['channels'] = channels
            
            return jsonify({"user": user})
        
        except Exception as e:
            logger.error(f"Error retrieving user details: {e}")
            logger.debug(traceback.format_exc())
            return jsonify({"error": "Failed to retrieve user details"}), 500
    
    @app.route('/api/admin/users/<int:user_id>', methods=['PATCH'])
    @admin_required
    def update_user(user_id):
        """Обновление информации о пользователе (только для админа)"""
        try:
            data = request.json
            admin_id = request.user.get('id')
            
            if not data:
                return jsonify({"error": "No data provided"}), 400

            if user_id == admin_id and 'is_active' in data and not data['is_active']:
                return jsonify({"error": "Cannot deactivate your own account"}), 400

            if user_id == admin_id and 'is_admin' in data and not data['is_admin']:
                return jsonify({"error": "Cannot remove your own admin privileges"}), 400

            update_fields = []
            update_values = []
            valid_fields = ['is_active', 'is_admin', 'email']
            
            for field in valid_fields:
                if field in data:
                    update_fields.append(f"{field} = %s")
                    update_values.append(data[field])
            
            if not update_fields:
                return jsonify({"error": "No valid fields to update"}), 400
            
            with pg_manager as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT username FROM users WHERE id = %s", (user_id,))
                    result = cur.fetchone()
                    
                    if not result:
                        return jsonify({"error": "User not found"}), 404
                    
                    username = result[0]

                    update_query = f"UPDATE users SET {', '.join(update_fields)} WHERE id = %s RETURNING id"
                    update_values.append(user_id)
                    
                    cur.execute(update_query, update_values)
                    result = cur.fetchone()
                    
                    if not result:
                        conn.rollback()
                        return jsonify({"error": "Failed to update user"}), 500
                    
                    conn.commit()
            
            logger.info(f"Admin {admin_id} updated user {username} (ID: {user_id})")
            return jsonify({
                "message": "User updated successfully",
                "user_id": user_id,
                "updated_fields": [field.split(' = ')[0] for field in update_fields]
            }), 200
        
        except Exception as e:
            logger.error(f"Error updating user: {e}")
            logger.debug(traceback.format_exc())
            return jsonify({"error": "Failed to update user"}), 500
    
    @app.route('/api/admin/users/<int:user_id>', methods=['DELETE'])
    @admin_required
    def delete_user(user_id):
        """Удаление пользователя (только для админа)"""
        try:
            admin_id = request.user.get('id')

            if user_id == admin_id:
                return jsonify({"error": "Cannot delete your own account"}), 400
            
            with pg_manager as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT username FROM users WHERE id = %s", (user_id,))
                    result = cur.fetchone()
                    
                    if not result:
                        return jsonify({"error": "User not found"}), 404
                    
                    username = result[0]

                    cur.execute("SELECT COUNT(*) FROM user_channels WHERE user_id = %s", (user_id,))
                    channels_count = cur.fetchone()[0]

                    cur.execute("DELETE FROM users WHERE id = %s", (user_id,))
                    conn.commit()
            
            logger.info(f"Admin {admin_id} deleted user {username} (ID: {user_id}) with {channels_count} channels")
            return jsonify({
                "message": "User deleted successfully",
                "user_id": user_id,
                "channels_deleted": channels_count
            }), 200
        
        except Exception as e:
            logger.error(f"Error deleting user: {e}")
            logger.debug(traceback.format_exc())
            return jsonify({"error": "Failed to delete user"}), 500
    
    @app.route('/api/admin/analytics/overview', methods=['GET'])
    @admin_required
    def get_admin_analytics_overview():
        """Получение общей аналитики системы (только для админа)"""
        try:
            time_period = request.args.get('period', '7d')

            period_map = {
                '1d': 'now-1d',
                '7d': 'now-7d',
                '30d': 'now-30d',
                '90d': 'now-90d'
            }
            
            from_date = period_map.get(time_period, 'now-7d')

            with pg_manager as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM users")
                    total_users = cur.fetchone()[0]

                    cur.execute("SELECT COUNT(*) FROM users WHERE is_active = TRUE")
                    active_users = cur.fetchone()[0]

                    cur.execute("SELECT COUNT(*) FROM user_channels")
                    total_channels = cur.fetchone()[0]

                    cur.execute("""
                        SELECT 
                            COUNT(user_id) as user_count,
                            COUNT(channel_name) as channel_count,
                            AVG(channels_per_user) as avg_channels
                        FROM (
                            SELECT 
                                user_id,
                                COUNT(channel_name) as channels_per_user
                            FROM user_channels
                            GROUP BY user_id
                        ) as user_channels_count
                    """)
                    
                    user_channel_stats = cur.fetchone()
                    avg_channels_per_user = user_channel_stats[2] if user_channel_stats else 0

            if es:
                try:
                    time_query = {
                        "size": 0,
                        "query": {
                            "range": {
                                "date": {
                                    "gte": from_date,
                                    "lte": "now"
                                }
                            }
                        },
                        "aggs": {
                            "message_count": {
                                "value_count": {
                                    "field": "_id"
                                }
                            },
                            "messages_per_day": {
                                "date_histogram": {
                                    "field": "date",
                                    "calendar_interval": "day",
                                    "format": "yyyy-MM-dd"
                                }
                            },
                            "channels": {
                                "terms": {
                                    "field": "channel_name.keyword",
                                    "size": 10,
                                    "order": {
                                        "_count": "desc"
                                    }
                                }
                            },
                            "sentiment_distribution": {
                                "terms": {
                                    "field": "sentiment.label",
                                    "size": 5
                                }
                            }
                        }
                    }
                    
                    result = es.search(index=app.config['ELASTICSEARCH_INDEX'], body=time_query)

                    total_messages = result["hits"]["total"]["value"]

                    daily_counts = [
                        {"date": bucket["key_as_string"], "count": bucket["doc_count"]}
                        for bucket in result["aggregations"]["messages_per_day"]["buckets"]
                    ]

                    top_channels = [
                        {"name": bucket["key"], "count": bucket["doc_count"]}
                        for bucket in result["aggregations"]["channels"]["buckets"]
                    ]

                    sentiment_distribution = [
                        {"label": bucket["key"], "count": bucket["doc_count"]}
                        for bucket in result["aggregations"]["sentiment_distribution"]["buckets"]
                    ]
                    
                except Exception as e:
                    logger.error(f"Error retrieving Elasticsearch analytics: {e}")
                    logger.debug(traceback.format_exc())
                    total_messages = 0
                    daily_counts = []
                    top_channels = []
                    sentiment_distribution = []
            else:
                total_messages = 0
                daily_counts = []
                top_channels = []
                sentiment_distribution = []
            
            return jsonify({
                "users": {
                    "total": total_users,
                    "active": active_users,
                    "inactive": total_users - active_users
                },
                "channels": {
                    "total": total_channels,
                    "avg_per_user": round(avg_channels_per_user, 2) if avg_channels_per_user else 0
                },
                "messages": {
                    "total": total_messages,
                    "daily_counts": daily_counts
                },
                "top_channels": top_channels,
                "sentiment_distribution": sentiment_distribution,
                "period": time_period
            })
            
        except Exception as e:
            logger.error(f"Error retrieving analytics overview: {e}")
            logger.debug(traceback.format_exc())
            return jsonify({"error": "Failed to retrieve analytics overview"}), 500
    
    @app.route('/api/admin/elasticsearch/status', methods=['GET'])
    @admin_required
    def get_elasticsearch_status():
        """Получение статуса Elasticsearch (только для админа)"""
        try:
            if not es:
                return jsonify({
                    "status": "unavailable",
                    "message": "Elasticsearch connection not available"
                }), 503

            health = es.cluster.health()

            index_stats = es.indices.stats(index=app.config['ELASTICSEARCH_INDEX'])

            index_mapping = es.indices.get_mapping(index=app.config['ELASTICSEARCH_INDEX'])

            index_settings = es.indices.get_settings(index=app.config['ELASTICSEARCH_INDEX'])

            return jsonify({
                "cluster": {
                    "name": es.cluster.state()["cluster_name"],
                    "status": health["status"],
                    "nodes": health["number_of_nodes"],
                    "data_nodes": health["number_of_data_nodes"]
                },
                "index": {
                    "name": app.config['ELASTICSEARCH_INDEX'],
                    "docs_count": index_stats["indices"][app.config['ELASTICSEARCH_INDEX']]["total"]["docs"]["count"],
                    "docs_deleted": index_stats["indices"][app.config['ELASTICSEARCH_INDEX']]["total"]["docs"]["deleted"],
                    "size_bytes": index_stats["indices"][app.config['ELASTICSEARCH_INDEX']]["total"]["store"]["size_in_bytes"],
                    "size_human": f"{index_stats['indices'][app.config['ELASTICSEARCH_INDEX']]['total']['store']['size_in_bytes'] / (1024*1024):.2f} MB"
                },
                "shards": {
                    "total": health["active_shards"],
                    "primary": health["active_primary_shards"],
                    "relocating": health["relocating_shards"],
                    "initializing": health["initializing_shards"],
                    "unassigned": health["unassigned_shards"]
                },
                "mapping_fields_count": len(index_mapping[app.config['ELASTICSEARCH_INDEX']]["mappings"]["properties"]),
                "settings": {
                    "number_of_shards": index_settings[app.config['ELASTICSEARCH_INDEX']]["settings"]["index"]["number_of_shards"],
                    "number_of_replicas": index_settings[app.config['ELASTICSEARCH_INDEX']]["settings"]["index"]["number_of_replicas"]
                }
            })
            
        except Exception as e:
            logger.error(f"Error retrieving Elasticsearch status: {e}")
            logger.debug(traceback.format_exc())
            return jsonify({"error": "Failed to retrieve Elasticsearch status", "details": str(e)}), 500
    
    @app.route('/api/admin/mongodb/status', methods=['GET'])
    @admin_required
    def get_mongodb_status():
        """Получение статуса MongoDB (только для админа)"""
        try:
            if not db:
                return jsonify({
                    "status": "unavailable",
                    "message": "MongoDB connection not available"
                }), 503
            
            # Get database stats
            db_stats = db.command("dbStats")
            
            # Get collection stats
            collections_info = []
            for collection_name in db.list_collection_names():
                collection_stats = db.command("collStats", collection_name)
                collections_info.append({
                    "name": collection_name,
                    "docs_count": collection_stats["count"],
                    "size_bytes": collection_stats["size"],
                    "size_human": f"{collection_stats['size'] / (1024*1024):.2f} MB",
                    "avg_doc_size_bytes": collection_stats["avgObjSize"] if "avgObjSize" in collection_stats else 0
                })
            
            # Get server info
            server_info = db.client.server_info()
            
            return jsonify({
                "server": {
                    "version": server_info["version"],
                    "uptime_seconds": server_info["uptime"],
                    "uptime_days": round(server_info["uptime"] / 86400, 2)
                },
                "database": {
                    "name": db.name,
                    "collections": db_stats["collections"],
                    "views": db_stats["views"],
                    "objects": db_stats["objects"],
                    "data_size_bytes": db_stats["dataSize"],
                    "data_size_human": f"{db_stats['dataSize'] / (1024*1024):.2f} MB",
                    "storage_size_bytes": db_stats["storageSize"],
                    "storage_size_human": f"{db_stats['storageSize'] / (1024*1024):.2f} MB",
                    "indexes": db_stats["indexes"],
                    "index_size_bytes": db_stats["indexSize"],
                    "index_size_human": f"{db_stats['indexSize'] / (1024*1024):.2f} MB"
                },
                "collections": collections_info
            })
            
        except Exception as e:
            logger.error(f"Error retrieving MongoDB status: {e}")
            logger.debug(traceback.format_exc())
            return jsonify({"error": "Failed to retrieve MongoDB status", "details": str(e)}), 500

    # Additional utility endpoints
    @app.route('/api/search/suggest', methods=['GET'])
    @token_required
    def get_search_suggestions():
        """Получение подсказок для поиска"""
        try:
            query_prefix = request.args.get('q', '').strip()
            
            if not query_prefix or len(query_prefix) < 2:
                return jsonify({"suggestions": []}), 200
                
            user_id = request.user.get('id')
            user_channels = get_filtered_query(user_id, pg_manager)
            
            if not es:
                return jsonify({"error": "Search service unavailable"}), 503
                
            # Build completion suggester query
            suggest_query = {
                "suggest": {
                    "text_completion": {
                        "prefix": query_prefix,
                        "completion": {
                            "field": "text.completion",
                            "size": 5,
                            "skip_duplicates": True,
                            "fuzzy": {
                                "fuzziness": "AUTO"
                            }
                        }
                    },
                    "topic_completion": {
                        "prefix": query_prefix,
                        "completion": {
                            "field": "topics.completion",
                            "size": 3,
                            "skip_duplicates": True
                        }
                    }
                },
                "size": 0
            }
            
            # Add channel filter if user has channels
            if user_channels:
                suggest_query["query"] = {
                    "bool": {
                        "filter": [
                            {"terms": {"channel_name.keyword": user_channels}}
                        ]
                    }
                }
            
            # Execute suggestion query
            try:
                result = es.search(index=app.config['ELASTICSEARCH_INDEX'], body=suggest_query)
                
                # Extract suggestions
                text_suggestions = [
                    option["text"] for option in result["suggest"]["text_completion"][0]["options"]
                ] if "text_completion" in result["suggest"] else []
                
                topic_suggestions = [
                    option["text"] for option in result["suggest"]["topic_completion"][0]["options"]
                ] if "topic_completion" in result["suggest"] else []
                
                # Combine and deduplicate suggestions
                all_suggestions = list(set(text_suggestions + topic_suggestions))
                
                return jsonify({"suggestions": all_suggestions})
                
            except Exception as e:
                logger.error(f"Error getting search suggestions: {e}")
                return jsonify({"suggestions": [], "error": str(e)}), 500
                
        except Exception as e:
            logger.error(f"Error in search suggestions: {e}")
            logger.debug(traceback.format_exc())
            return jsonify({"error": "Failed to get search suggestions"}), 500
    
    @app.route('/api/user/profile', methods=['GET'])
    @token_required
    def get_user_profile():
        """Получение профиля текущего пользователя"""
        try:
            user_id = request.user.get('id')
            
            with pg_manager as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT id, username, email, is_admin, is_active, created_at,
                               (SELECT COUNT(*) FROM user_channels WHERE user_id = users.id) as channels_count
                        FROM users
                        WHERE id = %s
                    """, (user_id,))
                    
                    user = cur.fetchone()
                    
                    if not user:
                        return jsonify({"error": "User not found"}), 404
                    
                    # Convert datetime objects
                    if 'created_at' in user and user['created_at']:
                        user['created_at'] = user['created_at'].isoformat()
                    
                    # Remove sensitive fields
                    user.pop('password_hash', None)
            
            return jsonify({"profile": user})
        
        except Exception as e:
            logger.error(f"Error retrieving user profile: {e}")
            logger.debug(traceback.format_exc())
            return jsonify({"error": "Failed to retrieve user profile"}), 500

    @app.errorhandler(404)
    def not_found(error):
        return jsonify({"error": "Resource not found"}), 404
    
    @app.errorhandler(405)
    def method_not_allowed(error):
        return jsonify({"error": "Method not allowed"}), 405
    
    @app.errorhandler(500)
    def internal_server_error(error):
        logger.error(f"Internal server error: {error}")
        return jsonify({"error": "Internal server error"}), 500