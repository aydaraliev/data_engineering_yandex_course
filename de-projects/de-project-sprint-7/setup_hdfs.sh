#!/bin/bash
set -e

# Константы подключения
SSH_KEY="$HOME/.ssh/ssh_private_key"
SSH_USER="yc-user"
SSH_HOST="158.160.159.232"
SSH_OPTS="-i $SSH_KEY -o StrictHostKeyChecking=no"

echo "================================================"
echo "Настройка структуры HDFS для geo-analytics"
echo "================================================"

# Функция для выполнения HDFS команд через Docker
exec_hdfs() {
    local cmd="$1"
    ssh $SSH_OPTS ${SSH_USER}@${SSH_HOST} \
        "docker exec \$(docker ps --filter 'name=ajdara1iev' --format '{{.Names}}' | head -1) \
         hdfs dfs $cmd"
}

# Функция создания директории с проверкой существования (ИДЕМПОТЕНТНОСТЬ)
create_hdfs_dir_safe() {
    local dir_path="$1"

    # Проверяем, существует ли директория
    if exec_hdfs "-test -d $dir_path" 2>/dev/null; then
        echo "  ✓ Директория уже существует: $dir_path"
        # Устанавливаем правильные права доступа
        exec_hdfs "-chmod 775 $dir_path" 2>/dev/null || true
        # Устанавливаем владельца (требуется для Airflow user ajdara1iev)
        ssh $SSH_OPTS ${SSH_USER}@${SSH_HOST} \
            "docker exec \$(docker ps --filter 'name=ajdara1iev' --format '{{.Names}}' | head -1) \
             bash -c 'HADOOP_USER_NAME=hdfs hdfs dfs -chown ajdara1iev:hadoop $dir_path'" 2>/dev/null || true
    else
        echo "  → Создание директории: $dir_path"
        exec_hdfs "-mkdir -p $dir_path" || {
            echo "  ✗ Ошибка создания директории: $dir_path"
            return 1
        }
        # Устанавливаем права доступа для группы hadoop
        exec_hdfs "-chmod 775 $dir_path" || true
        # Устанавливаем владельца (требуется для Airflow user ajdara1iev)
        ssh $SSH_OPTS ${SSH_USER}@${SSH_HOST} \
            "docker exec \$(docker ps --filter 'name=ajdara1iev' --format '{{.Names}}' | head -1) \
             bash -c 'HADOOP_USER_NAME=hdfs hdfs dfs -chown ajdara1iev:hadoop $dir_path'" || true
        echo "  ✓ Создана: $dir_path"
    fi
}

# Функция загрузки файла с проверкой существования (ИДЕМПОТЕНТНОСТЬ)
upload_file_safe() {
    local local_file="$1"
    local hdfs_path="$2"

    # Проверяем существование файла в HDFS
    if exec_hdfs "-test -e $hdfs_path" 2>/dev/null; then
        echo "  ✓ Файл уже существует в HDFS: $hdfs_path"
        echo "    Пропуск загрузки (для перезагрузки удалите файл в HDFS)"
    else
        echo "  → Загрузка файла: $local_file -> $hdfs_path"

        # Проверяем существование файла локально
        if [ ! -f "$local_file" ]; then
            echo "  ✗ Локальный файл не найден: $local_file"
            return 1
        fi

        # Копируем файл на сервер
        scp $SSH_OPTS "$local_file" ${SSH_USER}@${SSH_HOST}:/tmp/ || {
            echo "  ✗ Ошибка копирования файла на сервер"
            return 1
        }

        # Загружаем в HDFS через Docker
        ssh $SSH_OPTS ${SSH_USER}@${SSH_HOST} \
            "docker exec \$(docker ps --filter 'name=ajdara1iev' --format '{{.Names}}' | head -1) \
             hdfs dfs -put /tmp/$(basename $local_file) $hdfs_path" || {
            echo "  ✗ Ошибка загрузки файла в HDFS"
            return 1
        }
        echo "  ✓ Файл загружен: $hdfs_path"
    fi
}

echo ""
echo "→ Создание структуры директорий HDFS..."
echo ""

# RAW слой
echo "RAW слой:"
create_hdfs_dir_safe "/user/ajdaral1ev/project/geo/raw"
create_hdfs_dir_safe "/user/ajdaral1ev/project/geo/raw/geo_csv"

echo ""
echo "ODS слой:"
create_hdfs_dir_safe "/user/ajdaral1ev/project/geo/ods"
create_hdfs_dir_safe "/user/ajdaral1ev/project/geo/ods/events_with_cities"

echo ""
echo "MART слой:"
create_hdfs_dir_safe "/user/ajdaral1ev/project/geo/mart"
create_hdfs_dir_safe "/user/ajdaral1ev/project/geo/mart/user_geo_report"
create_hdfs_dir_safe "/user/ajdaral1ev/project/geo/mart/zone_mart"
create_hdfs_dir_safe "/user/ajdaral1ev/project/geo/mart/friend_recommendations"

echo ""
echo "→ Загрузка справочных данных..."
echo ""
upload_file_safe "geo.csv" "/user/ajdaral1ev/project/geo/raw/geo_csv/geo.csv"

echo ""
echo "→ Проверка созданной структуры..."
echo ""
exec_hdfs "-ls -R /user/ajdaral1ev/project/geo" || true

echo ""
echo "================================================"
echo "✓ Структура HDFS успешно настроена!"
echo "================================================"
echo ""
echo "Созданные директории:"
echo "  RAW:  /user/ajdaral1ev/project/geo/raw"
echo "  ODS:  /user/ajdaral1ev/project/geo/ods"
echo "  MART: /user/ajdaral1ev/project/geo/mart"
echo ""
echo "Следующий шаг: ./first_run.sh (тестовый запуск с 10% выборкой)"
echo "         или: ./deploy_dag.sh (развертывание Airflow DAG)"
