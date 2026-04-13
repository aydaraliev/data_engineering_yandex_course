# Проект Sprint 6: Анализ конверсии групп социальной сети

## Описание проекта

Проект расширяет существующее аналитическое хранилище данных (Data Vault) для анализа конверсии пользователей в группах социальной сети. Цель - определить группы с высокой конверсией в первое сообщение для эффективного размещения рекламы.

## Архитектура решения

### Data Vault структура

**Существующие хабы:**
- `VT251126648744__DWH.h_users` - пользователи
- `VT251126648744__DWH.h_groups` - группы

**Новые таблицы:**
- `VT251126648744__STAGING.group_log` - staging таблица для логов активности в группах
- `VT251126648744__DWH.l_user_group_activity` - линк между пользователями и группами
- `VT251126648744__DWH.s_auth_history` - сателлит с атрибутами событий

## Структура проекта

```
de-project-sprint-6/
├── src/
│   ├── dags/
│   │   └── sprint6_project_dag.py          # Основной Airflow DAG (содержит все DDL/DML)
│   └── sql/
│       └── analytics_group_conversion.sql  # Аналитический запрос
└── README.md
```

## Этапы выполнения DAG

1. **fetch_group_log** - скачивание group_log.csv из S3
2. **prepare_group_log_data** - обработка NULL значений (Int64)
3. **print_sample** - вывод примера данных
4. **create_staging_table** - создание staging таблицы
5. **create_link_table** - создание линка
6. **create_satellite_table** - создание сателлита
7. **load_staging** - загрузка данных в staging
8. **load_link** - инкрементальная миграция в линк
9. **load_satellite** - инкрементальная миграция в сателлит
10. **check_results** - проверка количества записей

## Результат аналитического запроса

Запрос возвращает для 10 самых старых групп:
- `hk_group_id` - хеш-ключ группы
- `cnt_added_users` - количество вступивших пользователей
- `cnt_users_in_group_with_messages` - количество активных пользователей
- `group_conversion` - конверсия в первое сообщение (от 0 до 1)

## Важные детали реализации

### Идемпотентность и инкрементальные загрузки

DAG спроектирован бездубликационным и допускает многократные перезапуски:

1. Создание таблиц: `CREATE TABLE IF NOT EXISTS`
2. Загрузка в линк: `NOT EXISTS` по (hk_user_id, hk_group_id)
3. Загрузка в сателлит: `NOT EXISTS` по (hk_l_user_group_activity, event, event_dt)

Повторный запуск с тем же файлом не создает дублей. Новый файл добавляет только новые события.

### Обработка NULL значений

В файле `group_log.csv` поле `user_id_from` может содержать NULL значения (когда пользователь вступил в группу самостоятельно). Для корректной обработки используется pandas с типом Int64:

```python
df['user_id_from'] = pd.array(df['user_id_from'], dtype="Int64")
```

### Хеширование в Vertica

Для генерации хеш-ключа связи используется встроенная функция HASH:

```sql
HASH(hu.hk_user_id, hg.hk_group_id) AS hk_l_user_group_activity
```

### Дедупликация

При загрузке в линк используется проверка на существование записи:

```sql
WHERE NOT EXISTS (
    SELECT 1 
    FROM VT251126648744__DWH.l_user_group_activity AS luga
    WHERE luga.hk_user_id = hu.hk_user_id 
      AND luga.hk_group_id = hg.hk_group_id
)
```

При загрузке в сателлит проверяется уникальность события:

```sql
WHERE NOT EXISTS (
    SELECT 1 
    FROM VT251126648744__DWH.s_auth_history AS sah
    WHERE sah.hk_l_user_group_activity = luga.hk_l_user_group_activity
      AND sah.event = gl.event
      AND sah.event_dt = gl.datetime
)
```

> **Примечание:** Vertica не поддерживает обычные индексы. Вместо этого используются **проекции** для оптимизации запросов. Запросы с `NOT EXISTS` работают эффективно благодаря встроенной оптимизации Vertica.

## Дополнительная информация

### Источник данных

- **S3 Bucket**: sprint6
- **Файл**: group_log.csv
- **Endpoint**: https://storage.yandexcloud.net

### Схема данных group_log.csv

| Поле | Тип | Описание |
|------|-----|----------|
| group_id | INT | ID группы |
| user_id | INT | ID пользователя |
| user_id_from | INT (nullable) | ID пользователя, пригласившего в группу |
| event | VARCHAR | Тип события (create/add/leave) |
| datetime | TIMESTAMP | Время события |

### Метрики

- **cnt_added_users**: количество уникальных пользователей с event='add'
- **cnt_users_in_group_with_messages**: количество уникальных пользователей, написавших сообщения
- **group_conversion**: cnt_users_in_group_with_messages / cnt_added_users
