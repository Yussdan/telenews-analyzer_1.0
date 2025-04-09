#!/bin/bash

set -e  # Остановка скрипта при любой ошибке

echo "Настройка системы анализа новостей из Telegram-каналов..."

# Проверка наличия podman или docker
if ! command -v podman > /dev/null && ! command -v docker > /dev/null; then
    echo "Ошибка: Не установлен ни podman, ни docker. Пожалуйста, установите один из них."
    exit 1
fi

# Определение compose-инструмента
CONTAINER_ENGINE="docker"
COMPOSE_CMD="docker-compose"

if command -v podman > /dev/null; then
    CONTAINER_ENGINE="podman"
    
    if command -v podman-compose > /dev/null; then
        COMPOSE_CMD="podman-compose"
    elif command -v docker-compose > /dev/null; then
        echo "Предупреждение: Используется docker-compose с podman"
        COMPOSE_CMD="docker-compose"
    else
        echo "Ошибка: Для podman не найден ни podman-compose, ни docker-compose"
        exit 1
    fi
else
    if ! command -v docker-compose > /dev/null; then
        echo "Ошибка: Для docker не найден docker-compose. Установите его."
        exit 1
    fi
    COMPOSE_CMD="docker-compose"
fi

# Проверка .env файла
if [ ! -f .env ]; then
    echo "Создание файла .env из шаблона..."
    cat > .env << 'EOF'
# Telegram API credentials
TELEGRAM_API_ID=your_api_id
TELEGRAM_API_HASH=your_api_hash
TELEGRAM_SESSION_NAME=telegram_news_session

# Список каналов для мониторинга (через запятую)
TELEGRAM_CHANNELS=channel1,channel2,channel3

# MongoDB
MONGODB_URI=mongodb://mongodb:27017
MONGODB_DB=telegram_news

# Elasticsearch
ELASTICSEARCH_HOST=elasticsearch
ELASTICSEARCH_PORT=9200
ELASTICSEARCH_INDEX=telegram_news

# API settings
API_HOST=0.0.0.0
API_PORT=5050
SECRET_KEY=your_secret_key

# Frontend settings
FRONTEND_HOST=0.0.0.0
FRONTEND_PORT=8080
API_URL=http://api:5050
EOF
    echo ""
    echo "Создан файл .env. Пожалуйста, отредактируйте его, указав свои API-ключи Telegram."
    echo "После этого запустите скрипт снова."
    exit 1
else
    echo "Файл .env уже существует, проверяем его заполнение..."
    
    if grep -q "your_api_id" .env || grep -q "your_secret_key" .env; then
        echo "Ошибка: В файле .env остались значения по умолчанию. Пожалуйста, отредактируйте его."
        exit 1
    fi
fi

# Меню выбора действия
echo "Текущее состояние системы:"
if $COMPOSE_CMD ps | grep -q "Up"; then
    echo "Система уже запущена. Выберите действие:"
    echo "1) Перезапустить (удалить контейнеры, сохранить данные)"
    echo "2) Пересобрать образы (удалить старые, пересобрать)"
    echo "3) Полная очистка (удалить все данные и образы)"
    echo "4) Запустить поверх текущей"
    echo -n "Введите номер выбора [1-4]: "
    read choice
    
    case $choice in
        1)
            echo "Остановка и удаление контейнеров..."
            $COMPOSE_CMD down
            ;;
        2)
            echo "Удаление старых образов и пересборка..."
            if [ "$CONTAINER_ENGINE" = "podman" ]; then
                # Для podman-compose сначала удаляем контейнеры
                $COMPOSE_CMD down
                # Получаем список образов, используемых в проекте
                images=$($COMPOSE_CMD images -q)
                if [ -n "$images" ]; then
                    # Удаляем каждый образ отдельно
                    for image in $images; do
                        $CONTAINER_ENGINE rmi -f $image || true
                    done
                fi
            else
                # Для docker-compose используем стандартный подход
                $COMPOSE_CMD down --rmi all
            fi
            rm -f .built_images
            ;;

        3)
            echo "Полная очистка системы..."
            if [ "$CONTAINER_ENGINE" = "podman" ]; then
                $COMPOSE_CMD down --volumes --remove-orphans
                images=$($COMPOSE_CMD images -q)
                if [ -n "$images" ]; then
                    for image in $images; do
                        $CONTAINER_ENGINE rmi -f $image || true
                    done
                fi
            else
                $COMPOSE_CMD down --volumes --remove-orphans --rmi all
            fi
            rm -f .built_images
            ;;
        4)
            echo "Запуск поверх текущей системы..."
            ;;
        *)
            echo "Неверный выбор. Выход."
            exit 1
            ;;
    esac
fi


# Сборка и запуск контейнеров
echo "Запуск системы через $COMPOSE_CMD..."
if [ ! -f ".built_images" ]; then
    $COMPOSE_CMD build
    touch .built_images
fi
$COMPOSE_CMD up -d


echo "Веб-интерфейс: http://localhost:8080"
echo "API: http://localhost:5050"
echo ""
echo "Для просмотра логов:"
echo "$COMPOSE_CMD logs -f collector"
echo "$COMPOSE_CMD logs -f processor"
echo "$COMPOSE_CMD logs -f api"
echo "$COMPOSE_CMD logs -f frontend"
echo "$COMPOSE_CMD logs -f postgres"