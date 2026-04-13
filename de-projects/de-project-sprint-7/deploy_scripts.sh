#!/bin/bash
set -e

# Константы подключения
SSH_KEY="$HOME/.ssh/ssh_private_key"
SSH_USER="yc-user"
SSH_HOST="158.160.159.232"
SSH_OPTS="-i $SSH_KEY -o StrictHostKeyChecking=no"

echo "=============================================="
echo "Развертывание Python скриптов на $SSH_HOST"
echo "=============================================="

# Проверка SSH подключения
echo "→ Проверка SSH подключения..."
ssh $SSH_OPTS ${SSH_USER}@${SSH_HOST} "echo 'SSH подключение работает'" || {
    echo "✗ Ошибка SSH подключения"
    echo "  Проверьте:"
    echo "  - SSH ключ существует: $SSH_KEY"
    echo "  - Доступность хоста: $SSH_HOST"
    echo "  - Права пользователя: $SSH_USER"
    exit 1
}
echo "✓ SSH подключение успешно"

# Список скриптов для развертывания
SCRIPTS=(
    "src/scripts/create_ods_layer.py"
    "src/scripts/geo_utils.py"
    "src/scripts/user_geo_mart.py"
    "src/scripts/zone_mart.py"
    "src/scripts/friend_recommendations.py"
)

# Проверка существования файлов локально
echo ""
echo "→ Проверка наличия файлов..."
for script in "${SCRIPTS[@]}"; do
    if [ ! -f "$script" ]; then
        echo "✗ Файл не найден: $script"
        exit 1
    fi
done
echo "✓ Все файлы найдены локально"

# Копирование скриптов
echo ""
echo "→ Создание директории scripts в контейнере (если не существует)..."
ssh $SSH_OPTS ${SSH_USER}@${SSH_HOST} \
    "docker exec \$(docker ps --filter 'name=ajdara1iev' --format '{{.Names}}' | head -1) \
     mkdir -p /lessons/scripts" || {
    echo "✗ Ошибка создания директории scripts"
    exit 1
}
echo "✓ Директория /lessons/scripts готова"

echo ""
echo "→ Копирование скриптов..."
for script in "${SCRIPTS[@]}"; do
    script_name=$(basename $script)
    echo "  Копирование $script_name..."

    # Копируем файл на сервер во временную директорию
    scp $SSH_OPTS "$script" ${SSH_USER}@${SSH_HOST}:/tmp/ || {
        echo "✗ Ошибка копирования $script_name на сервер"
        exit 1
    }

    # Копируем из временной директории в Docker контейнер
    ssh $SSH_OPTS ${SSH_USER}@${SSH_HOST} \
        "docker cp /tmp/$script_name \$(docker ps --filter 'name=ajdara1iev' --format '{{.Names}}' | head -1):/lessons/scripts/" || {
        echo "✗ Ошибка копирования $script_name в Docker контейнер"
        exit 1
    }

    echo "  ✓ $script_name развернут"
done

# Проверка развертывания
echo ""
echo "→ Проверка развертывания в Docker контейнере..."
ssh $SSH_OPTS ${SSH_USER}@${SSH_HOST} \
    "docker exec \$(docker ps --filter 'name=ajdara1iev' --format '{{.Names}}' | head -1) \
     sh -c 'ls -lh /lessons/scripts/*.py'" || {
    echo "✗ Ошибка проверки файлов в контейнере"
    exit 1
}

echo ""
echo "=============================================="
echo "✓ Все скрипты успешно развернуты!"
echo "=============================================="
echo ""
echo "Развернутые скрипты:"
for script in "${SCRIPTS[@]}"; do
    echo "  - $(basename $script)"
done
echo ""
echo "Следующий шаг: ./setup_hdfs.sh"
