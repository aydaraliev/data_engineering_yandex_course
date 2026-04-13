#!/usr/bin/env python3
"""
Script for validating the DAG before deployment.

Checks:
- Python syntax
- Airflow imports
- DAG structure
- Task dependencies
- Task parameters
"""

import sys
import os

def validate_dag():
    """Validates the DAG file."""
    print("=" * 70)
    print("DAG VALIDATION: geo_marts_dag.py")
    print("=" * 70)

    errors = []
    warnings = []

    # 1. Check that the file exists
    print("\n1. Checking the file...")
    dag_file = os.path.join(os.path.dirname(__file__), 'geo_marts_dag.py')

    if not os.path.exists(dag_file):
        errors.append(f"File not found: {dag_file}")
        print(f"   File not found")
        return errors, warnings

    print(f"   File exists: {dag_file}")

    # 2. Check Python syntax
    print("\n2. Checking Python syntax...")
    try:
        with open(dag_file, 'r') as f:
            code = f.read()
            compile(code, dag_file, 'exec')
        print("   Python syntax is correct")
    except SyntaxError as e:
        errors.append(f"Python syntax error: {e}")
        print(f"   Syntax error: {e}")
        return errors, warnings

    # 3. Try importing the DAG
    print("\n3. Importing the DAG...")
    try:
        sys.path.insert(0, os.path.dirname(__file__))
        from geo_marts_dag import dag
        print("   DAG imported successfully")
    except ImportError as e:
        errors.append(f"Import error: {e}")
        print(f"   Import error: {e}")
        return errors, warnings
    except Exception as e:
        errors.append(f"Error while importing the DAG: {e}")
        print(f"   Error: {e}")
        return errors, warnings

    # 4. Check DAG attributes
    print("\n4. Checking DAG attributes...")

    if not hasattr(dag, 'dag_id'):
        errors.append("DAG has no dag_id")
        print("   Missing dag_id")
    else:
        print(f"   DAG ID: {dag.dag_id}")

    if not hasattr(dag, 'schedule_interval'):
        warnings.append("DAG has no schedule_interval")
        print("   Missing schedule_interval")
    else:
        print(f"   Schedule: {dag.schedule_interval}")

    if not hasattr(dag, 'default_args'):
        warnings.append("DAG has no default_args")
        print("   Missing default_args")
    else:
        print(f"   Default args: {len(dag.default_args)} parameters")

    # 5. Check tasks
    print("\n5. Checking tasks...")

    if not hasattr(dag, 'tasks') or len(dag.tasks) == 0:
        errors.append("DAG has no tasks")
        print("   Tasks are missing")
    else:
        print(f"   Number of tasks: {len(dag.tasks)}")

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
                print(f"   Task found: {expected_task}")
            else:
                errors.append(f"Task not found: {expected_task}")
                print(f"   Missing task: {expected_task}")

    # 6. Check dependencies
    print("\n6. Checking dependencies...")

    try:
        # Verify the dependency chain
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
                    print(f"   Dependency: {upstream_id} -> {downstream_id}")
                else:
                    errors.append(f"Missing dependency: {upstream_id} -> {downstream_id}")
                    print(f"   Dependency missing: {upstream_id} -> {downstream_id}")
            else:
                errors.append(f"Tasks not found for dependency check: {upstream_id} -> {downstream_id}")

    except Exception as e:
        errors.append(f"Error while checking dependencies: {e}")
        print(f"   Error: {e}")

    # 7. Check operator types
    print("\n7. Checking operator types...")

    for task in dag.tasks:
        task_type = type(task).__name__
        print(f"   - {task.task_id}: {task_type}")

        # Check SparkSubmitOperator
        if 'update' in task.task_id:
            if task_type != 'SparkSubmitOperator':
                warnings.append(f"Task {task.task_id} should be SparkSubmitOperator, not {task_type}")
            else:
                # Check required parameters are present
                if not hasattr(task, 'application') or not task.application:
                    errors.append(f"Task {task.task_id} has no application parameter")
                else:
                    print(f"     Application: {task.application}")

                if not hasattr(task, 'py_files') or not task.py_files:
                    warnings.append(f"Task {task.task_id} has no py_files (geo_utils.py)")
                else:
                    print(f"     Py files: {task.py_files}")

    # 8. Check for cyclic dependencies
    print("\n8. Checking for cyclic dependencies...")

    try:
        from airflow.utils.dag_cycle_tester import check_cycle
        check_cycle(dag)
        print("   No cyclic dependencies")
    except Exception as e:
        errors.append(f"Cyclic dependency detected: {e}")
        print(f"   Cyclic dependency: {e}")

    # Summary
    print("\n" + "=" * 70)
    print("VALIDATION SUMMARY")
    print("=" * 70)

    if len(errors) == 0 and len(warnings) == 0:
        print("DAG is fully valid! Ready for deployment.")
        return_code = 0
    elif len(errors) == 0:
        print(f"DAG is valid with warnings ({len(warnings)}):")
        for warning in warnings:
            print(f"   - {warning}")
        return_code = 0
    else:
        print(f"DAG contains errors ({len(errors)}):")
        for error in errors:
            print(f"   - {error}")
        if len(warnings) > 0:
            print(f"\nWarnings ({len(warnings)}):")
            for warning in warnings:
                print(f"   - {warning}")
        return_code = 1

    print("=" * 70)

    return return_code


if __name__ == '__main__':
    exit_code = validate_dag()
    sys.exit(exit_code)
