#!/usr/bin/env python3
"""
Скрипт для валидации DAG перед развёртыванием.

Проверяет:
- Синтаксис Python
- Импорты Airflow
- Структуру DAG
- Зависимости между задачами
- Параметры задач
"""

import sys
import os

def validate_dag():
    """Валидирует DAG файл."""
    print("=" * 70)
    print("ВАЛИДАЦИЯ DAG: geo_marts_dag.py")
    print("=" * 70)

    errors = []
    warnings = []

    # 1. Проверка существования файла
    print("\n1. Проверка файла...")
    dag_file = os.path.join(os.path.dirname(__file__), 'geo_marts_dag.py')

    if not os.path.exists(dag_file):
        errors.append(f"Файл не найден: {dag_file}")
        print(f"   ❌ Файл не найден")
        return errors, warnings

    print(f"   ✓ Файл существует: {dag_file}")

    # 2. Проверка синтаксиса Python
    print("\n2. Проверка синтаксиса Python...")
    try:
        with open(dag_file, 'r') as f:
            code = f.read()
            compile(code, dag_file, 'exec')
        print("   ✓ Синтаксис Python корректен")
    except SyntaxError as e:
        errors.append(f"Ошибка синтаксиса Python: {e}")
        print(f"   ❌ Ошибка синтаксиса: {e}")
        return errors, warnings

    # 3. Попытка импортировать DAG
    print("\n3. Импорт DAG...")
    try:
        sys.path.insert(0, os.path.dirname(__file__))
        from geo_marts_dag import dag
        print("   ✓ DAG успешно импортирован")
    except ImportError as e:
        errors.append(f"Ошибка импорта: {e}")
        print(f"   ❌ Ошибка импорта: {e}")
        return errors, warnings
    except Exception as e:
        errors.append(f"Ошибка при импорте DAG: {e}")
        print(f"   ❌ Ошибка: {e}")
        return errors, warnings

    # 4. Проверка атрибутов DAG
    print("\n4. Проверка атрибутов DAG...")

    if not hasattr(dag, 'dag_id'):
        errors.append("DAG не имеет dag_id")
        print("   ❌ Отсутствует dag_id")
    else:
        print(f"   ✓ DAG ID: {dag.dag_id}")

    if not hasattr(dag, 'schedule_interval'):
        warnings.append("DAG не имеет schedule_interval")
        print("   ⚠ Отсутствует schedule_interval")
    else:
        print(f"   ✓ Schedule: {dag.schedule_interval}")

    if not hasattr(dag, 'default_args'):
        warnings.append("DAG не имеет default_args")
        print("   ⚠ Отсутствует default_args")
    else:
        print(f"   ✓ Default args: {len(dag.default_args)} параметров")

    # 5. Проверка задач
    print("\n5. Проверка задач...")

    if not hasattr(dag, 'tasks') or len(dag.tasks) == 0:
        errors.append("DAG не содержит задач")
        print("   ❌ Задачи отсутствуют")
    else:
        print(f"   ✓ Количество задач: {len(dag.tasks)}")

        expected_tasks = [
            'start',
            'update_user_geo_report',
            'update_zone_mart',
            'update_friend_recommendations',
            'end'
        ]

        task_ids = [task.task_id for task in dag.tasks]

        for expected_task in expected_tasks:
            if expected_task in task_ids:
                print(f"   ✓ Задача найдена: {expected_task}")
            else:
                errors.append(f"Задача не найдена: {expected_task}")
                print(f"   ❌ Задача отсутствует: {expected_task}")

    # 6. Проверка зависимостей
    print("\n6. Проверка зависимостей...")

    try:
        # Проверяем цепочку зависимостей
        tasks_dict = {task.task_id: task for task in dag.tasks}

        expected_deps = [
            ('start', 'update_user_geo_report'),
            ('update_user_geo_report', 'update_zone_mart'),
            ('update_zone_mart', 'update_friend_recommendations'),
            ('update_friend_recommendations', 'end'),
        ]

        for upstream_id, downstream_id in expected_deps:
            if upstream_id in tasks_dict and downstream_id in tasks_dict:
                upstream_task = tasks_dict[upstream_id]
                downstream_task = tasks_dict[downstream_id]

                if downstream_task in upstream_task.downstream_list:
                    print(f"   ✓ Зависимость: {upstream_id} → {downstream_id}")
                else:
                    errors.append(f"Отсутствует зависимость: {upstream_id} → {downstream_id}")
                    print(f"   ❌ Зависимость отсутствует: {upstream_id} → {downstream_id}")
            else:
                errors.append(f"Задачи не найдены для проверки зависимости: {upstream_id} → {downstream_id}")

    except Exception as e:
        errors.append(f"Ошибка при проверке зависимостей: {e}")
        print(f"   ❌ Ошибка: {e}")

    # 7. Проверка типов операторов
    print("\n7. Проверка типов операторов...")

    for task in dag.tasks:
        task_type = type(task).__name__
        print(f"   - {task.task_id}: {task_type}")

        # Проверка SparkSubmitOperator
        if 'update' in task.task_id:
            if task_type != 'SparkSubmitOperator':
                warnings.append(f"Задача {task.task_id} должна быть SparkSubmitOperator, а не {task_type}")
            else:
                # Проверяем наличие обязательных параметров
                if not hasattr(task, 'application') or not task.application:
                    errors.append(f"Задача {task.task_id} не имеет параметра application")
                else:
                    print(f"     Application: {task.application}")

                if not hasattr(task, 'py_files') or not task.py_files:
                    warnings.append(f"Задача {task.task_id} не имеет py_files (geo_utils.py)")
                else:
                    print(f"     Py files: {task.py_files}")

    # 8. Проверка циклических зависимостей
    print("\n8. Проверка на циклические зависимости...")

    try:
        from airflow.utils.dag_cycle_tester import check_cycle
        check_cycle(dag)
        print("   ✓ Циклические зависимости отсутствуют")
    except Exception as e:
        errors.append(f"Обнаружена циклическая зависимость: {e}")
        print(f"   ❌ Циклическая зависимость: {e}")

    # Итоги
    print("\n" + "=" * 70)
    print("ИТОГИ ВАЛИДАЦИИ")
    print("=" * 70)

    if len(errors) == 0 and len(warnings) == 0:
        print("✅ DAG полностью валиден! Готов к развёртыванию.")
        return_code = 0
    elif len(errors) == 0:
        print(f"⚠️  DAG валиден с предупреждениями ({len(warnings)}):")
        for warning in warnings:
            print(f"   - {warning}")
        return_code = 0
    else:
        print(f"❌ DAG содержит ошибки ({len(errors)}):")
        for error in errors:
            print(f"   - {error}")
        if len(warnings) > 0:
            print(f"\nПредупреждения ({len(warnings)}):")
            for warning in warnings:
                print(f"   - {warning}")
        return_code = 1

    print("=" * 70)

    return return_code


if __name__ == '__main__':
    exit_code = validate_dag()
    sys.exit(exit_code)
