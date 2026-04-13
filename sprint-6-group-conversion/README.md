# Sprint 6 Project: Social Network Group Conversion Analysis

## Project Description

The project extends the existing analytical Data Vault warehouse with entities that support analyzing user conversion inside social-network groups. The goal is to identify groups with a high conversion rate from joining to posting the first message, so that ad placements can be targeted more effectively.

## Solution Architecture

### Data Vault Structure

**Existing hubs:**
- `VT251126648744__DWH.h_users` — users
- `VT251126648744__DWH.h_groups` — groups

**New tables:**
- `VT251126648744__STAGING.group_log` — staging table for group activity logs
- `VT251126648744__DWH.l_user_group_activity` — link between users and groups
- `VT251126648744__DWH.s_auth_history` — satellite storing event attributes

## Project Layout

```
sprint-6-group-conversion/
├── src/
│   ├── dags/
│   │   └── sprint6_project_dag.py          # Main Airflow DAG (contains all DDL/DML)
│   └── sql/
│       └── analytics_group_conversion.sql  # Analytical query
└── README.md
```

## DAG Steps

1. **fetch_group_log** — download `group_log.csv` from S3.
2. **prepare_group_log_data** — handle NULL values using pandas `Int64`.
3. **print_sample** — log a data sample.
4. **create_staging_table** — create the staging table.
5. **create_link_table** — create the link.
6. **create_satellite_table** — create the satellite.
7. **load_staging** — load data into staging.
8. **load_link** — incremental migration into the link.
9. **load_satellite** — incremental migration into the satellite.
10. **check_results** — verify row counts.

## Analytical Query Output

For the ten oldest groups the query returns:

- `hk_group_id` — group hash key.
- `cnt_added_users` — number of users who joined.
- `cnt_users_in_group_with_messages` — number of active users (those who posted messages).
- `group_conversion` — conversion to the first message (value between 0 and 1).

## Implementation Notes

### Idempotency and Incremental Loads

The DAG is deduplication-safe and can be re-run as many times as needed:

1. Table creation uses `CREATE TABLE IF NOT EXISTS`.
2. Loading into the link uses `NOT EXISTS` on `(hk_user_id, hk_group_id)`.
3. Loading into the satellite uses `NOT EXISTS` on `(hk_l_user_group_activity, event, event_dt)`.

Re-running the DAG on the same file creates no duplicates. A new file only brings in new events.

### Handling NULL Values

In `group_log.csv` the `user_id_from` field can be NULL (when a user joined a group on their own). pandas with the `Int64` type is used to preserve nullability:

```python
df['user_id_from'] = pd.array(df['user_id_from'], dtype="Int64")
```

### Hashing in Vertica

The Vertica built-in `HASH()` function is used to generate link hash keys:

```sql
HASH(hu.hk_user_id, hg.hk_group_id) AS hk_l_user_group_activity
```

### Deduplication

When loading the link, a `NOT EXISTS` check prevents duplicates:

```sql
WHERE NOT EXISTS (
    SELECT 1
    FROM VT251126648744__DWH.l_user_group_activity AS luga
    WHERE luga.hk_user_id = hu.hk_user_id
      AND luga.hk_group_id = hg.hk_group_id
)
```

For the satellite the uniqueness of the event is checked as well:

```sql
WHERE NOT EXISTS (
    SELECT 1
    FROM VT251126648744__DWH.s_auth_history AS sah
    WHERE sah.hk_l_user_group_activity = luga.hk_l_user_group_activity
      AND sah.event = gl.event
      AND sah.event_dt = gl.datetime
)
```

> **Note:** Vertica does not support conventional indexes. Projections are used instead to optimize queries. `NOT EXISTS` queries run efficiently thanks to Vertica's built-in optimizer.

## Additional Information

### Data Source

- **S3 bucket**: `sprint6`
- **File**: `group_log.csv`
- **Endpoint**: `https://storage.yandexcloud.net`

### `group_log.csv` Schema

| Field | Type | Description |
|-------|------|-------------|
| group_id | INT | Group ID |
| user_id | INT | User ID |
| user_id_from | INT (nullable) | ID of the user who invited this user to the group |
| event | VARCHAR | Event type (`create` / `add` / `leave`) |
| datetime | TIMESTAMP | Event timestamp |

### Metrics

- **cnt_added_users** — number of unique users with `event = 'add'`.
- **cnt_users_in_group_with_messages** — number of unique users who posted a message.
- **group_conversion** = `cnt_users_in_group_with_messages` / `cnt_added_users`.
