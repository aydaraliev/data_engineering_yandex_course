#!/bin/bash
set -e

# Константы подключения
SSH_KEY="$HOME/.ssh/ssh_private_key"
SSH_USER="yc-user"
SSH_HOST="158.160.159.232"
SSH_OPTS="-i $SSH_KEY -o StrictHostKeyChecking=no"

DAG_NAME="geo_marts_update"
LOG_FILE="dag_run_full_$(date +%Y%m%d_%H%M%S).log"

echo "==================================================" | tee $LOG_FILE
echo "Запуск DAG '$DAG_NAME' на 100% данных" | tee -a $LOG_FILE
echo "==================================================" | tee -a $LOG_FILE

# Функция для выполнения команд Airflow в Docker
exec_airflow() {
    local cmd="$1"
    ssh $SSH_OPTS ${SSH_USER}@${SSH_HOST} \
        "docker exec \$(docker ps --filter 'name=ajdara1iev' --format '{{.Names}}' | head -1) \
         $cmd"
}

echo "" | tee -a $LOG_FILE
echo "→ Проверка подключения..." | tee -a $LOG_FILE
ssh $SSH_OPTS ${SSH_USER}@${SSH_HOST} \
    "docker exec \$(docker ps --filter 'name=ajdara1iev' --format '{{.Names}}' | head -1) \
     echo 'Подключение успешно'" || {
    echo "✗ Ошибка подключения к кластеру" | tee -a $LOG_FILE
    exit 1
}
echo "✓ Подключение установлено" | tee -a $LOG_FILE

# Проверка существования DAG
echo "" | tee -a $LOG_FILE
echo "→ Проверка DAG в Airflow..." | tee -a $LOG_FILE
exec_airflow "airflow dags list | grep $DAG_NAME" &>/dev/null || {
    echo "✗ DAG '$DAG_NAME' не найден в Airflow" | tee -a $LOG_FILE
    echo "  Запустите сначала: ./deploy_dag.sh" | tee -a $LOG_FILE
    exit 1
}
echo "✓ DAG найден: $DAG_NAME" | tee -a $LOG_FILE

# Настройка режима полных данных (100%)
echo "" | tee -a $LOG_FILE
echo "→ Настройка режима полных данных (100%)..." | tee -a $LOG_FILE

# Удаляем переменную sample_fraction если она существует (это вернет режим 100% по умолчанию)
exec_airflow "airflow variables delete geo_marts_sample_fraction" 2>/dev/null || true

# Или явно устанавливаем 1.0
exec_airflow "airflow variables set geo_marts_sample_fraction 1.0" 2>&1 | tee -a $LOG_FILE

# Проверяем установленное значение
SAMPLE_VALUE=$(exec_airflow "airflow variables get geo_marts_sample_fraction 2>/dev/null" || echo "1.0")
echo "✓ Режим выборки: ${SAMPLE_VALUE} (100% данных)" | tee -a $LOG_FILE

# Активация DAG (если не активирован)
echo "" | tee -a $LOG_FILE
echo "→ Активация DAG..." | tee -a $LOG_FILE
exec_airflow "airflow dags unpause $DAG_NAME" 2>&1 | tee -a $LOG_FILE
echo "✓ DAG активирован" | tee -a $LOG_FILE

# Запуск DAG
echo "" | tee -a $LOG_FILE
echo "→ Запуск DAG '$DAG_NAME'..." | tee -a $LOG_FILE
RUN_ID=$(exec_airflow "airflow dags trigger $DAG_NAME --conf '{\"sample_fraction\": 1.0}' 2>&1" | grep -o "Created <DagRun.*>" | grep -o "manual__[^>]*" || echo "manual_$(date +%Y-%m-%dT%H:%M:%S)")
echo "✓ DAG запущен" | tee -a $LOG_FILE
echo "  Run ID: $RUN_ID" | tee -a $LOG_FILE

# Мониторинг выполнения
echo "" | tee -a $LOG_FILE
echo "→ Мониторинг выполнения..." | tee -a $LOG_FILE
echo "  (Обновление каждые 30 секунд)" | tee -a $LOG_FILE
echo "" | tee -a $LOG_FILE

# Ожидание завершения с периодическими проверками
MAX_WAIT_MINUTES=120  # Максимум 2 часа
WAIT_SECONDS=30
ITERATIONS=$((MAX_WAIT_MINUTES * 60 / WAIT_SECONDS))

for i in $(seq 1 $ITERATIONS); do
    # Получаем статус DAG run
    DAG_STATE=$(exec_airflow "airflow dags state $DAG_NAME $RUN_ID 2>/dev/null" || echo "unknown")

    TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$TIMESTAMP] Статус: $DAG_STATE" | tee -a $LOG_FILE

    # Проверяем финальные статусы
    if [[ "$DAG_STATE" == "success" ]]; then
        echo "" | tee -a $LOG_FILE
        echo "✓ DAG выполнен успешно!" | tee -a $LOG_FILE
        break
    elif [[ "$DAG_STATE" == "failed" ]]; then
        echo "" | tee -a $LOG_FILE
        echo "✗ DAG завершился с ошибкой" | tee -a $LOG_FILE
        echo "" | tee -a $LOG_FILE
        echo "→ Последние задачи с ошибками:" | tee -a $LOG_FILE
        exec_airflow "airflow tasks states-for-dag-run $DAG_NAME $RUN_ID 2>/dev/null | grep failed" | tee -a $LOG_FILE || true
        exit 1
    fi

    # Ждем перед следующей проверкой
    if [ $i -lt $ITERATIONS ]; then
        sleep $WAIT_SECONDS
    fi
done

# Проверяем не превысили ли таймаут
if [[ "$DAG_STATE" != "success" ]]; then
    echo "" | tee -a $LOG_FILE
    echo "⚠ Превышен таймаут ожидания ($MAX_WAIT_MINUTES минут)" | tee -a $LOG_FILE
    echo "  DAG все еще выполняется: $DAG_STATE" | tee -a $LOG_FILE
    echo "  Проверьте статус в Airflow UI" | tee -a $LOG_FILE
fi

# Показываем статистику по HDFS
echo "" | tee -a $LOG_FILE
echo "==================================================" | tee -a $LOG_FILE
echo "Статистика данных в HDFS" | tee -a $LOG_FILE
echo "==================================================" | tee -a $LOG_FILE

echo "" | tee -a $LOG_FILE
echo "→ ODS слой (events_with_cities):" | tee -a $LOG_FILE
exec_airflow "hdfs dfs -du -s -h /user/ajdaral1ev/project/geo/ods/events_with_cities" 2>/dev/null | awk '{print "  Размер: " $1}' | tee -a $LOG_FILE
exec_airflow "hdfs dfs -ls /user/ajdaral1ev/project/geo/ods/events_with_cities | grep '^d' | wc -l" 2>/dev/null | awk '{print "  Партиций: " $1}' | tee -a $LOG_FILE

echo "" | tee -a $LOG_FILE
echo "→ MART: user_geo_report:" | tee -a $LOG_FILE
exec_airflow "hdfs dfs -du -s -h /user/ajdaral1ev/project/geo/mart/user_geo_report" 2>/dev/null | awk '{print "  Размер: " $1}' | tee -a $LOG_FILE

echo "" | tee -a $LOG_FILE
echo "→ MART: zone_mart:" | tee -a $LOG_FILE
exec_airflow "hdfs dfs -du -s -h /user/ajdaral1ev/project/geo/mart/zone_mart" 2>/dev/null | awk '{print "  Размер: " $1}' | tee -a $LOG_FILE

echo "" | tee -a $LOG_FILE
echo "→ MART: friend_recommendations:" | tee -a $LOG_FILE
exec_airflow "hdfs dfs -du -s -h /user/ajdaral1ev/project/geo/mart/friend_recommendations" 2>/dev/null | awk '{print "  Размер: " $1}' | tee -a $LOG_FILE

echo "" | tee -a $LOG_FILE
echo "==================================================" | tee -a $LOG_FILE
echo "✓ Выполнение завершено" | tee -a $LOG_FILE
echo "==================================================" | tee -a $LOG_FILE
echo "" | tee -a $LOG_FILE
echo "Лог сохранен в: $LOG_FILE" | tee -a $LOG_FILE
echo "" | tee -a $LOG_FILE
echo "Для просмотра логов DAG в Airflow:" | tee -a $LOG_FILE
echo "  airflow dags show $DAG_NAME" | tee -a $LOG_FILE
echo "" | tee -a $LOG_FILE