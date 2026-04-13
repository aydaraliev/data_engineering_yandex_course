#!/bin/bash
set -e

# Константы подключения
SSH_KEY="$HOME/.ssh/ssh_private_key"
SSH_USER="yc-user"
SSH_HOST="158.160.159.232"
SSH_OPTS="-i $SSH_KEY -o StrictHostKeyChecking=no"

DAG_FILE="dags/geo_marts_dag.py"
DAG_NAME="geo_marts_update"

echo "================================================"
echo "Развертывание Airflow DAG: $DAG_NAME"
echo "================================================"

# Проверка существования DAG файла локально
echo ""
echo "→ Проверка наличия DAG файла..."
if [ ! -f "$DAG_FILE" ]; then
    echo "✗ DAG файл не найден: $DAG_FILE"
    exit 1
fi
echo "✓ DAG файл найден: $DAG_FILE"

# Локальная валидация DAG (базовая проверка Python синтаксиса)
echo ""
echo "→ Валидация Python синтаксиса DAG..."
python3 -m py_compile "$DAG_FILE" || {
    echo "✗ Ошибка синтаксиса в DAG файле"
    exit 1
}
echo "✓ Синтаксис DAG корректен"

# Копирование DAG на сервер
echo ""
echo "→ Копирование DAG на сервер..."
scp $SSH_OPTS "$DAG_FILE" ${SSH_USER}@${SSH_HOST}:/tmp/geo_marts_dag.py || {
    echo "✗ Ошибка копирования DAG на сервер"
    exit 1
}
echo "✓ DAG скопирован на сервер"

# Копирование DAG в Docker контейнер
echo ""
echo "→ Копирование DAG в Airflow..."
ssh $SSH_OPTS ${SSH_USER}@${SSH_HOST} \
    "docker cp /tmp/geo_marts_dag.py \$(docker ps --filter 'name=ajdara1iev' --format '{{.Names}}' | head -1):/lessons/dags/" || {
    echo "✗ Ошибка копирования DAG в Airflow"
    exit 1
}
echo "✓ DAG развернут в /lessons/dags/"

# Проверка DAG в Airflow
echo ""
echo "→ Проверка импорта DAG в Airflow..."
echo "  (подождите 10 секунд для обновления Airflow...)"
sleep 10

ssh $SSH_OPTS ${SSH_USER}@${SSH_HOST} \
    "docker exec \$(docker ps --filter 'name=ajdara1iev' --format '{{.Names}}' | head -1) \
     airflow dags list | grep $DAG_NAME" && echo "  ✓ DAG успешно импортирован в Airflow" || {
    echo "  ⚠ DAG пока не виден в Airflow (может потребоваться больше времени)"
    echo "    Проверьте вручную через Airflow UI или команду: airflow dags list"
}

echo ""
echo "================================================"
echo "✓ DAG успешно развернут!"
echo "================================================"
echo ""
echo "Следующие шаги:"
echo ""
echo "1. Проверьте DAG в Airflow UI:"
echo "   - Откройте Airflow веб-интерфейс"
echo "   - Найдите DAG: $DAG_NAME"
echo "   - Проверьте отсутствие ошибок импорта"
echo ""
echo "2. Настройте Airflow Variable для тестового режима (опционально):"
echo "   airflow variables set geo_marts_sample_fraction 0.1"
echo "   (это включит режим выборки 10% для всех задач)"
echo ""
echo "3. Активируйте DAG:"
echo "   - Через UI: включите toggle для DAG '$DAG_NAME'"
echo "   - Или через CLI: airflow dags unpause $DAG_NAME"
echo ""
echo "4. Запустите вручную (опционально):"
echo "   airflow dags trigger $DAG_NAME"
echo ""
echo "5. Вернуть на 100% данных:"
echo "   airflow variables set geo_marts_sample_fraction 1.0"
echo "   (или удалите переменную: airflow variables delete geo_marts_sample_fraction)"
echo ""
