#!/bin/bash
set -e

# Константы подключения
SSH_KEY="$HOME/.ssh/ssh_private_key"
SSH_USER="yc-user"
SSH_HOST="158.160.159.232"
SSH_OPTS="-i $SSH_KEY -o StrictHostKeyChecking=no"

# Параметры выборки
SAMPLE_FRACTION="${SAMPLE_FRACTION:-0.1}"  # По умолчанию 10%
LOG_FILE="first_run_$(date +%Y%m%d_%H%M%S).log"

echo "==================================================" | tee $LOG_FILE
echo "Первый тестовый запуск geo-analytics pipeline" | tee -a $LOG_FILE
echo "Режим выборки: ${SAMPLE_FRACTION} ($(echo "$SAMPLE_FRACTION * 100" | bc)%)" | tee -a $LOG_FILE
echo "==================================================" | tee -a $LOG_FILE

# Функция для выполнения Spark job через Docker
exec_spark_job() {
    local script_name="$1"
    local step_name="$2"

    echo "" | tee -a $LOG_FILE
    echo "→ [$step_name] Запуск $script_name..." | tee -a $LOG_FILE
    start_time=$(date +%s)

    # Выполняем spark-submit в Docker контейнере
    ssh $SSH_OPTS ${SSH_USER}@${SSH_HOST} \
        "docker exec \$(docker ps --filter 'name=ajdara1iev' --format '{{.Names}}' | head -1) \
         spark-submit --master yarn \
         --conf spark.sql.adaptive.enabled=true \
         --conf spark.sql.shuffle.partitions=20 \
         --conf spark.sql.parquet.compression.codec=snappy \
         --py-files /lessons/scripts/geo_utils.py \
         --driver-memory 4g \
         --executor-memory 4g \
         --executor-cores 2 \
         --num-executors 4 \
         /lessons/scripts/$script_name --sample $SAMPLE_FRACTION" \
        2>&1 | tee -a $LOG_FILE

    local exit_code=${PIPESTATUS[0]}
    end_time=$(date +%s)
    duration=$((end_time - start_time))

    if [ $exit_code -eq 0 ]; then
        echo "✓ [$step_name] Завершено успешно за ${duration}s" | tee -a $LOG_FILE
    else
        echo "✗ [$step_name] Ошибка выполнения (код: $exit_code)" | tee -a $LOG_FILE
        echo "" | tee -a $LOG_FILE
        echo "Проверьте лог для деталей: $LOG_FILE" | tee -a $LOG_FILE
        exit $exit_code
    fi
}

# Проверка подключения
echo "" | tee -a $LOG_FILE
echo "→ Проверка подключения к кластеру..." | tee -a $LOG_FILE
ssh $SSH_OPTS ${SSH_USER}@${SSH_HOST} \
    "docker exec \$(docker ps --filter 'name=ajdara1iev' --format '{{.Names}}' | head -1) \
     echo 'Подключение успешно'" || {
    echo "✗ Ошибка подключения к кластеру" | tee -a $LOG_FILE
    exit 1
}
echo "✓ Подключение установлено" | tee -a $LOG_FILE

# Засекаем общее время
overall_start=$(date +%s)

# Последовательный запуск всех этапов pipeline
exec_spark_job "create_ods_layer.py" "1/4 ODS Layer"
exec_spark_job "user_geo_mart.py" "2/4 User Geo Mart"
exec_spark_job "zone_mart.py" "3/4 Zone Mart"
exec_spark_job "friend_recommendations.py" "4/4 Friend Recommendations"

# Подсчет общего времени
overall_end=$(date +%s)
overall_duration=$((overall_end - overall_start))

echo "" | tee -a $LOG_FILE
echo "==================================================" | tee -a $LOG_FILE
echo "✓ Тестовый запуск завершен успешно!" | tee -a $LOG_FILE
echo "==================================================" | tee -a $LOG_FILE
echo "" | tee -a $LOG_FILE
echo "Общее время выполнения: ${overall_duration}s ($(echo "scale=2; $overall_duration / 60" | bc) минут)" | tee -a $LOG_FILE
echo "Лог сохранен в: $LOG_FILE" | tee -a $LOG_FILE
echo "" | tee -a $LOG_FILE
echo "Следующие шаги:" | tee -a $LOG_FILE
echo "1. Проверьте результаты: ./monitor_pipeline.sh" | tee -a $LOG_FILE
echo "2. Если тесты OK, разверните DAG: ./deploy_dag.sh" | tee -a $LOG_FILE
echo "3. Настройте DAG на 100% данных или оставьте тестовый режим" | tee -a $LOG_FILE
echo "" | tee -a $LOG_FILE
