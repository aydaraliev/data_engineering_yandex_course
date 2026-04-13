# Global Metrics Dashboard

Visualization of aggregated transaction metrics from the `VT251126648744__DWH.global_metrics` mart.

![Dashboard](metabase.png)

> The screenshot is from the original Metabase instance and keeps the Russian tile titles from the submission. English translations (matching the SQL queries below):
> - *Сумма переводов по валютам* → Transfer amounts by currency
> - *Уникальных пользователей* → Unique users
> - *Среднее транзакций на пользователя* → Average transactions per user
> - *Количество транзакций* → Transaction count
> - *Общий оборот компании* → Total company turnover
> - *Дата* → Date, *Валюта* → Currency (dashboard filters)

## Excluded Data

The date **2022-10-01** is excluded from all visualizations.

**Reason:** anomalous data — all ~17,000 transactions were made by only 2 unique accounts, producing an unrealistic `avg_transactions_per_account` value of ≈ 8,000 instead of the normal ≈ 1.

| Date       | Transactions | Unique accounts | Average per account |
|:-----------|:-------------|:----------------|:--------------------|
| 2022-10-01 | 17,708       | 2               | 8,854               |
| 2022-10-05 | 17,917       | 17,389          | 1.03                |

Likely cause: test or system operations on the first day the system was live.

---

## SQL Queries for the Visualizations

### Transfer amounts by currency

```sql
SELECT
    date_update,
    currency_from,
    amount_total
FROM VT251126648744__DWH.global_metrics
WHERE date_update > '2022-10-01'
  AND {{date_filter}}
  AND {{currency_filter}}
ORDER BY date_update
```

### Average transactions per user

```sql
SELECT
    date_update,
    currency_from,
    avg_transactions_per_account
FROM VT251126648744__DWH.global_metrics
WHERE date_update > '2022-10-01'
  AND {{date_filter}}
  AND {{currency_filter}}
ORDER BY date_update
```

### Unique users

```sql
SELECT
    date_update,
    currency_from,
    cnt_accounts_make_transactions
FROM VT251126648744__DWH.global_metrics
WHERE date_update > '2022-10-01'
  AND {{date_filter}}
  AND {{currency_filter}}
ORDER BY date_update
```

### Total company turnover

```sql
SELECT
    SUM(amount_total) AS total_turnover
FROM VT251126648744__DWH.global_metrics
WHERE date_update > '2022-10-01'
  AND {{date_filter}}
  AND {{currency_filter}}
```

### Transaction count

```sql
SELECT
    date_update,
    currency_from,
    cnt_transactions
FROM VT251126648744__DWH.global_metrics
WHERE date_update > '2022-10-01'
  AND {{date_filter}}
  AND {{currency_filter}}
ORDER BY date_update
```

---

## Metabase Variables

- `{{date_filter}}` — Field Filter -> `date_update`
- `{{currency_filter}}` — Field Filter -> `currency_from`
