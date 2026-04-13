# Развёртывание DAG в Airflow

## Предварительные требования

### 1. Установленные компоненты
- Apache Airflow 2.0+
- Python 3.7+
- Apache Spark 3.3+
- Провайдер: `apache-airflow-providers-apache-spark`

### 2. Установка провайдера Spark (если не установлен)
```bash
pip install apache-airflow-providers-apache-spark
```

### 3. Структура проекта
```
/lessons/
├── dags/
│   └── geo_marts_dag.py          # DAG файл
└── scripts/
    ├── user_geo_mart.py          # Скрипт витрины пользователей
    ├── zone_mart.py              # Скрипт витрины зон
    ├── friend_recommendations.py # Скрипт рекомендаций
    └── geo_utils.py              # Утилиты (Haversine, etc.)
```

## Шаг 1: Подготовка скриптов

### 1.1. Копирование скриптов на сервер

```bash
# Если скрипты в локальной директории
LOCAL_SCRIPTS_DIR="src/scripts"
REMOTE_USER="yc-user"
REMOTE_HOST="158.160.218.207"
SSH_KEY="~/.ssh/ssh_private_key"

# Копирование на сервер
scp -i $SSH_KEY \
    $LOCAL_SCRIPTS_DIR/user_geo_mart.py \
    $LOCAL_SCRIPTS_DIR/zone_mart.py \
    $LOCAL_SCRIPTS_DIR/friend_recommendations.py \
    $LOCAL_SCRIPTS_DIR/geo_utils.py \
    $REMOTE_USER@$REMOTE_HOST:/lessons/scripts/
```

### 1.2. Проверка доступности скриптов

```bash
# Подключиться к серверу
ssh -i $SSH_KEY $REMOTE_USER@$REMOTE_HOST

# Проверить наличие файлов
ls -la /lessons/scripts/

# Ожидаемый вывод:
# user_geo_mart.py
# zone_mart.py
# friend_recommendations.py
# geo_utils.py
```

### 1.3. Проверка синтаксиса скриптов

```bash
# Проверка Python синтаксиса
python3 /lessons/scripts/user_geo_mart.py --help
python3 /lessons/scripts/zone_mart.py --help
python3 /lessons/scripts/friend_recommendations.py --help
```

## Шаг 2: Настройка Airflow Connection

### 2.1. Через Airflow UI

1. Откройте Airflow Web UI (обычно http://localhost:8080)
2. Перейдите: **Admin → Connections**
3. Нажмите кнопку **"+"** (Add Connection)
4. Заполните форму:

```
Connection Id: yarn_spark
Connection Type: Spark
Host: yarn://master-host:8032
Port: 8032
Extra: {"queue": "default"}
```

5. Нажмите **Save**

### 2.2. Через Airflow CLI

```bash
airflow connections add yarn_spark \
    --conn-type spark \
    --conn-host yarn://master-host:8032 \
    --conn-port 8032 \
    --conn-extra '{"queue": "default"}'
```

### 2.3. Через переменные окружения

```bash
export AIRFLOW_CONN_YARN_SPARK='spark://yarn://master-host:8032'
```

## Шаг 3: Развёртывание DAG

### 3.1. Копирование DAG файла

```bash
# Определите путь к папке dags в Airflow
AIRFLOW_DAGS_DIR=$(airflow config get-value core dags_folder)

# Или используйте стандартный путь
AIRFLOW_DAGS_DIR="/opt/airflow/dags"  # Или ~/airflow/dags

# Копирование DAG
cp dags/geo_marts_dag.py $AIRFLOW_DAGS_DIR/

# Проверка
ls -la $AIRFLOW_DAGS_DIR/geo_marts_dag.py
```

### 3.2. Альтернатива: создание символической ссылки

```bash
ln -s /path/to/project/dags/geo_marts_dag.py $AIRFLOW_DAGS_DIR/geo_marts_dag.py
```

## Шаг 4: Валидация DAG

### 4.1. Запуск валидационного скрипта

```bash
cd dags/
python3 validate_dag.py
```

Ожидаемый вывод:
```
======================================================================
ВАЛИДАЦИЯ DAG: geo_marts_dag.py
======================================================================

1. Проверка файла...
   ✓ Файл существует: /path/to/geo_marts_dag.py

2. Проверка синтаксиса Python...
   ✓ Синтаксис Python корректен

3. Импорт DAG...
   ✓ DAG успешно импортирован

4. Проверка атрибутов DAG...
   ✓ DAG ID: geo_marts_update
   ✓ Schedule: 0 0 * * *
   ✓ Default args: 7 параметров

5. Проверка задач...
   ✓ Количество задач: 5
   ✓ Задача найдена: start
   ✓ Задача найдена: update_user_geo_report
   ✓ Задача найдена: update_zone_mart
   ✓ Задача найдена: update_friend_recommendations
   ✓ Задача найдена: end

6. Проверка зависимостей...
   ✓ Зависимость: start → update_user_geo_report
   ✓ Зависимость: update_user_geo_report → update_zone_mart
   ✓ Зависимость: update_zone_mart → update_friend_recommendations
   ✓ Зависимость: update_friend_recommendations → end

7. Проверка типов операторов...
   - start: PythonOperator
   - update_user_geo_report: SparkSubmitOperator
     Application: /lessons/scripts/user_geo_mart.py
     Py files: /lessons/scripts/geo_utils.py
   - update_zone_mart: SparkSubmitOperator
     Application: /lessons/scripts/zone_mart.py
     Py files: /lessons/scripts/geo_utils.py
   - update_friend_recommendations: SparkSubmitOperator
     Application: /lessons/scripts/friend_recommendations.py
     Py files: /lessons/scripts/geo_utils.py
   - end: PythonOperator

8. Проверка на циклические зависимости...
   ✓ Циклические зависимости отсутствуют

======================================================================
ИТОГИ ВАЛИДАЦИИ
======================================================================
✅ DAG полностью валиден! Готов к развёртыванию.
======================================================================
```

### 4.2. Проверка через Airflow CLI

```bash
# Список всех DAG
airflow dags list | grep geo_marts

# Проверка на ошибки импорта
airflow dags list-import-errors

# Показать структуру DAG
airflow dags show geo_marts_update

# Список задач
airflow tasks list geo_marts_update
```

## Шаг 5: Активация DAG

### 5.1. Через Airflow UI

1. Откройте Airflow Web UI
2. Найдите DAG **geo_marts_update** в списке
3. Включите переключатель (toggle) слева от имени DAG
4. DAG станет активным

### 5.2. Через Airflow CLI

```bash
# Распаузить DAG
airflow dags unpause geo_marts_update

# Проверить статус
airflow dags state geo_marts_update $(date +%Y-%m-%d)
```

## Шаг 6: Тестовый запуск

### 6.1. Ручной запуск через UI

1. В Airflow UI найдите DAG **geo_marts_update**
2. Нажмите кнопку **"Trigger DAG"** (▶)
3. Подтвердите запуск
4. Наблюдайте за выполнением в **Graph View** или **Tree View**

### 6.2. Ручной запуск через CLI

```bash
# Триггер DAG
airflow dags trigger geo_marts_update

# Триггер с конкретной датой
airflow dags trigger geo_marts_update --exec-date 2024-01-15
```

### 6.3. Тест отдельной задачи

```bash
# Тест без реального выполнения
airflow tasks test geo_marts_update start 2024-01-15

# Проверка параметров задачи
airflow tasks render geo_marts_update update_user_geo_report 2024-01-15
```

## Шаг 7: Мониторинг

### 7.1. Просмотр логов

```bash
# Логи конкретной задачи
airflow tasks logs geo_marts_update update_user_geo_report $(date +%Y-%m-%d)

# Последние 100 строк
airflow tasks logs geo_marts_update update_user_geo_report $(date +%Y-%m-%d) | tail -100
```

### 7.2. Проверка статуса выполнения

```bash
# Статус DAG run
airflow dags state geo_marts_update $(date +%Y-%m-%d)

# Список всех запусков
airflow dags list-runs -d geo_marts_update
```

### 7.3. Airflow UI Views

- **Graph View**: Визуализация потока задач
- **Tree View**: История выполнений по датам
- **Gantt**: Временная диаграмма выполнения
- **Task Duration**: График длительности задач

## Шаг 8: Проверка результатов

### 8.1. Проверка HDFS

```bash
# Подключение к серверу
ssh -i $SSH_KEY $REMOTE_USER@$REMOTE_HOST

# Вход в контейнер
CONTAINER=$(docker ps --format '{{.Names}}' | grep ajdara1iev | head -1)
docker exec -it $CONTAINER bash

# Проверка витрин в HDFS
hdfs dfs -ls /user/ajdaral1ev/project/geo/mart/

# Ожидаемый вывод:
# /user/ajdaral1ev/project/geo/mart/user_geo_report
# /user/ajdaral1ev/project/geo/mart/zone_mart
# /user/ajdaral1ev/project/geo/mart/friend_recommendations
```

### 8.2. Проверка содержимого витрин

```bash
# Количество записей
hdfs dfs -count /user/ajdaral1ev/project/geo/mart/user_geo_report
hdfs dfs -count /user/ajdaral1ev/project/geo/mart/zone_mart
hdfs dfs -count /user/ajdaral1ev/project/geo/mart/friend_recommendations
```

## Troubleshooting

### Проблема: DAG не появляется в UI

**Решения**:
1. Проверьте путь к папке dags:
   ```bash
   airflow config get-value core dags_folder
   ```

2. Проверьте права доступа:
   ```bash
   chmod 644 $AIRFLOW_DAGS_DIR/geo_marts_dag.py
   ```

3. Проверьте ошибки импорта:
   ```bash
   airflow dags list-import-errors
   ```

4. Перезапустите Airflow scheduler:
   ```bash
   airflow scheduler restart
   # Или
   systemctl restart airflow-scheduler
   ```

### Проблема: Connection 'yarn_spark' not found

**Решение**: Создайте подключение (см. Шаг 2)

### Проблема: Задача падает с ошибкой

**Решения**:
1. Проверьте логи задачи:
   ```bash
   airflow tasks logs geo_marts_update <task_id> <date>
   ```

2. Проверьте доступность скриптов:
   ```bash
   ls -la /lessons/scripts/
   ```

3. Проверьте HDFS пути:
   ```bash
   hdfs dfs -ls /user/master/data/geo/events
   hdfs dfs -ls /user/ajdaral1ev/project/geo/raw/geo_csv/
   ```

### Проблема: OutOfMemoryError в Spark

**Решение**: Увеличьте ресурсы в DAG:
```python
driver_memory='8g'
executor_memory='8g'
num_executors=8
```

### Проблема: Задача зависла

**Решение**: Убейте задачу и перезапустите:
```bash
# Найдите активные Spark jobs
yarn application -list

# Убейте зависший job
yarn application -kill <application_id>

# Очистите задачу в Airflow
airflow tasks clear geo_marts_update <task_id> -t <date>
```

## Откат изменений

### Отключение DAG

```bash
# Через CLI
airflow dags pause geo_marts_update

# Через UI: выключите toggle переключатель
```

### Удаление DAG

```bash
# Удалите файл
rm $AIRFLOW_DAGS_DIR/geo_marts_dag.py

# Удалите из базы данных
airflow dags delete geo_marts_update
```

## Автоматическое развёртывание (CI/CD)

### Пример shell скрипта для развёртывания

```bash
#!/bin/bash
# deploy_dag.sh

set -e

AIRFLOW_DAGS_DIR="/opt/airflow/dags"
PROJECT_DIR="/path/to/project"

echo "Деплой DAG для geo marts..."

# 1. Валидация
echo "1. Валидация DAG..."
cd $PROJECT_DIR/dags
python3 validate_dag.py
if [ $? -ne 0 ]; then
    echo "❌ Валидация провалена!"
    exit 1
fi

# 2. Копирование скриптов
echo "2. Копирование скриптов..."
cp $PROJECT_DIR/src/scripts/*.py /lessons/scripts/

# 3. Копирование DAG
echo "3. Копирование DAG..."
cp $PROJECT_DIR/dags/geo_marts_dag.py $AIRFLOW_DAGS_DIR/

# 4. Проверка в Airflow
echo "4. Проверка в Airflow..."
sleep 5
airflow dags list | grep geo_marts_update
if [ $? -ne 0 ]; then
    echo "❌ DAG не найден в Airflow!"
    exit 1
fi

# 5. Активация
echo "5. Активация DAG..."
airflow dags unpause geo_marts_update

echo "✅ Деплой успешно завершён!"
```

## Полезные команды

```bash
# Список всех DAG
airflow dags list

# Информация о DAG
airflow dags show geo_marts_update

# Список задач
airflow tasks list geo_marts_update

# История запусков
airflow dags list-runs -d geo_marts_update --state success
airflow dags list-runs -d geo_marts_update --state failed

# Очистка задач для повторного запуска
airflow tasks clear geo_marts_update -s 2024-01-01 -e 2024-01-31

# Backfill (выполнение за прошлые даты)
airflow dags backfill geo_marts_update -s 2024-01-01 -e 2024-01-10
```

## Контакты

**Проект**: Data Lake Sprint 7 - Geo Recommendations
**Owner**: ajdaral1ev
**Создано**: 2024
