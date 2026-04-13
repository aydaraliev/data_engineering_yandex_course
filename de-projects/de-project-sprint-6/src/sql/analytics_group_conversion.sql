-- Аналитический запрос для расчета конверсии в первое сообщение
-- Выводит метрики для 10 самых старых групп:
-- - Количество вступивших пользователей (event = 'add')
-- - Количество активных пользователей (написали хотя бы 1 сообщение)
-- - Конверсия в первое сообщение

WITH user_group_log AS (
    -- Подсчет пользователей, вступивших в каждую группу
    SELECT 
        hg.hk_group_id,
        COUNT(DISTINCT hu.hk_user_id) AS cnt_added_users
    FROM VT251126648744__DWH.s_auth_history AS sah
    LEFT JOIN VT251126648744__DWH.l_user_group_activity AS luga 
        ON sah.hk_l_user_group_activity = luga.hk_l_user_group_activity
    LEFT JOIN VT251126648744__DWH.h_groups AS hg 
        ON luga.hk_group_id = hg.hk_group_id
    LEFT JOIN VT251126648744__DWH.h_users AS hu 
        ON luga.hk_user_id = hu.hk_user_id
    WHERE sah.event = 'add'
        AND hg.hk_group_id IN (
            -- Выбираем 10 самых старых групп
            SELECT hk_group_id 
            FROM VT251126648744__DWH.h_groups 
            ORDER BY registration_dt 
            LIMIT 10
        )
    GROUP BY hg.hk_group_id
),
user_group_messages AS (
    -- Подсчет пользователей, написавших хотя бы одно сообщение в каждую группу
    SELECT 
        hg.hk_group_id,
        COUNT(DISTINCT hu.hk_user_id) AS cnt_users_in_group_with_messages
    FROM VT251126648744__STAGING.dialogs AS d
    LEFT JOIN VT251126648744__DWH.h_groups AS hg 
        ON d.message_group = hg.group_id
    LEFT JOIN VT251126648744__DWH.h_users AS hu 
        ON d.message_from = hu.user_id
    WHERE hg.hk_group_id IN (
        -- Те же 10 самых старых групп
        SELECT hk_group_id 
        FROM VT251126648744__DWH.h_groups 
        ORDER BY registration_dt 
        LIMIT 10
    )
    GROUP BY hg.hk_group_id
)
-- Финальный запрос с расчетом конверсии
SELECT  
    ugl.hk_group_id,
    ugl.cnt_added_users,
    COALESCE(ugm.cnt_users_in_group_with_messages, 0) AS cnt_users_in_group_with_messages,
    CASE 
        WHEN ugl.cnt_added_users > 0 
        THEN COALESCE(ugm.cnt_users_in_group_with_messages, 0)::FLOAT / ugl.cnt_added_users 
        ELSE 0 
    END AS group_conversion
FROM user_group_log AS ugl
LEFT JOIN user_group_messages AS ugm 
    ON ugl.hk_group_id = ugm.hk_group_id
ORDER BY group_conversion DESC;
