#!/bin/bash
set -e

# Константы подключения
SSH_KEY="$HOME/.ssh/ssh_private_key"
SSH_USER="yc-user"
SSH_HOST="158.160.159.232"
SSH_OPTS="-i $SSH_KEY -o StrictHostKeyChecking=no"

REPORT_FILE="pipeline_status_$(date +%Y%m%d_%H%M%S).txt"

echo "==================================================" | tee $REPORT_FILE
echo "Мониторинг geo-analytics pipeline" | tee -a $REPORT_FILE
echo "==================================================" | tee -a $REPORT_FILE

# Функция для выполнения команд в Docker
exec_in_docker() {
    local cmd="$1"
    ssh $SSH_OPTS ${SSH_USER}@${SSH_HOST} \
        "docker exec \$(docker ps --filter 'name=ajdara1iev' --format '{{.Names}}' | head -1) bash -c \"$cmd\""
}

# Функция для выполнения HDFS команд
exec_hdfs() {
    local cmd="$1"
    exec_in_docker "hdfs dfs $cmd"
}

echo "" | tee -a $REPORT_FILE
echo "→ Проверка Airflow DAG..." | tee -a $REPORT_FILE
echo "" | tee -a $REPORT_FILE

# Проверка DAG в Airflow
exec_in_docker "airflow dags list" | grep geo_marts_update && {
    echo "✓ DAG найден в Airflow" | tee -a $REPORT_FILE

    # Проверка статуса DAG
    dag_status=$(exec_in_docker "airflow dags list" | grep geo_marts_update | awk '{print $3}')
    echo "  Статус: $dag_status" | tee -a $REPORT_FILE

    # Последний запуск
    echo "" | tee -a $REPORT_FILE
    echo "  Последние запуски:" | tee -a $REPORT_FILE
    exec_in_docker "airflow dags list-runs -d geo_marts_update --limit 5" | tee -a $REPORT_FILE || true
} || {
    echo "⚠ DAG не найден в Airflow" | tee -a $REPORT_FILE
}

echo "" | tee -a $REPORT_FILE
echo "→ Проверка структуры HDFS..." | tee -a $REPORT_FILE
echo "" | tee -a $REPORT_FILE

# Проверка ODS слоя
echo "ODS слой (events_with_cities):" | tee -a $REPORT_FILE
if exec_hdfs "-test -d /user/ajdaral1ev/project/geo/ods/events_with_cities" 2>/dev/null; then
    echo "  ✓ Директория существует" | tee -a $REPORT_FILE
    ods_size=$(exec_hdfs "-du -s -h /user/ajdaral1ev/project/geo/ods/events_with_cities" | awk '{print $1}')
    echo "  Размер: $ods_size" | tee -a $REPORT_FILE

    # Количество партиций
    partitions=$(exec_hdfs "-ls /user/ajdaral1ev/project/geo/ods/events_with_cities" | grep "date=" | wc -l || echo "0")
    echo "  Партиций (по дате): $partitions" | tee -a $REPORT_FILE
else
    echo "  ✗ Директория не найдена" | tee -a $REPORT_FILE
fi

echo "" | tee -a $REPORT_FILE
echo "MART слой:" | tee -a $REPORT_FILE

# Проверка витрин
MARTS=(
    "user_geo_report"
    "zone_mart"
    "friend_recommendations"
)

for mart in "${MARTS[@]}"; do
    echo "" | tee -a $REPORT_FILE
    echo "Витрина: $mart" | tee -a $REPORT_FILE
    mart_path="/user/ajdaral1ev/project/geo/mart/$mart"

    if exec_hdfs "-test -d $mart_path" 2>/dev/null; then
        echo "  ✓ Директория существует" | tee -a $REPORT_FILE

        # Размер витрины
        mart_size=$(exec_hdfs "-du -s -h $mart_path" | awk '{print $1}')
        echo "  Размер: $mart_size" | tee -a $REPORT_FILE

        # Количество файлов
        file_count=$(exec_hdfs "-ls -R $mart_path" | grep "\.parquet$" | wc -l || echo "0")
        echo "  Parquet файлов: $file_count" | tee -a $REPORT_FILE

        # Партиционирование (для zone_mart)
        if [ "$mart" == "zone_mart" ]; then
            month_partitions=$(exec_hdfs "-ls $mart_path" | grep "month=" | wc -l || echo "0")
            echo "  Партиций (по месяцам): $month_partitions" | tee -a $REPORT_FILE
        fi
    else
        echo "  ✗ Директория не найдена" | tee -a $REPORT_FILE
    fi
done

echo "" | tee -a $REPORT_FILE
echo "→ Проверка качества данных..." | tee -a $REPORT_FILE
echo "" | tee -a $REPORT_FILE

# Data quality checks через PySpark
exec_in_docker "python3 -c \"
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('DataQualityCheck').getOrCreate()

print('Проверка ODS слоя:')
try:
    ods = spark.read.parquet('/user/ajdaral1ev/project/geo/ods/events_with_cities')
    total = ods.count()
    null_cities = ods.filter('city IS NULL').count()
    print(f'  Всего событий: {total:,}')
    print(f'  NULL городов: {null_cities} ({null_cities/total*100:.2f}%)')
    print(f'  Уникальных городов: {ods.select(\"city\").distinct().count()}')
except Exception as e:
    print(f'  Ошибка: {e}')

print('')
print('Проверка User Geo Mart:')
try:
    user_mart = spark.read.parquet('/user/ajdaral1ev/project/geo/mart/user_geo_report')
    total = user_mart.count()
    null_act = user_mart.filter('act_city IS NULL').count()
    print(f'  Всего пользователей: {total:,}')
    print(f'  NULL act_city: {null_act}')
    print(f'  С home_city: {user_mart.filter(\"home_city IS NOT NULL\").count():,}')
except Exception as e:
    print(f'  Ошибка: {e}')

print('')
print('Проверка Zone Mart:')
try:
    zone_mart = spark.read.parquet('/user/ajdaral1ev/project/geo/mart/zone_mart')
    print(f'  Всего записей: {zone_mart.count():,}')
    print(f'  Уникальных зон: {zone_mart.select(\"zone_id\").distinct().count()}')
    print(f'  Уникальных недель: {zone_mart.select(\"week\").distinct().count()}')
except Exception as e:
    print(f'  Ошибка: {e}')

print('')
print('Проверка Friend Recommendations:')
try:
    friends = spark.read.parquet('/user/ajdaral1ev/project/geo/mart/friend_recommendations')
    total = friends.count()
    print(f'  Всего рекомендаций: {total:,}')
    print(f'  Уникальных пользователей (left): {friends.select(\"user_left\").distinct().count():,}')
    print(f'  Уникальных зон: {friends.select(\"zone_id\").distinct().count()}')
    # Проверка корректности: user_left < user_right
    invalid = friends.filter('user_left >= user_right').count()
    print(f'  Некорректных пар (left >= right): {invalid}')
except Exception as e:
    print(f'  Ошибка: {e}')

spark.stop()
\"" 2>&1 | tee -a $REPORT_FILE || echo "⚠ Ошибка проверки качества данных" | tee -a $REPORT_FILE

echo "" | tee -a $REPORT_FILE
echo "==================================================" | tee -a $REPORT_FILE
echo "✓ Мониторинг завершен" | tee -a $REPORT_FILE
echo "==================================================" | tee -a $REPORT_FILE
echo "" | tee -a $REPORT_FILE
echo "Отчет сохранен в: $REPORT_FILE" | tee -a $REPORT_FILE
echo "" | tee -a $REPORT_FILE
