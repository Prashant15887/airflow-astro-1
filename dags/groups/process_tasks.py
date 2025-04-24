from airflow.decorators import task, task_group
from airflow.utils.task_group import TaskGroup

@task.python
def process_a(partner_name, partner_path):
    print(partner_name)
    print(partner_path)

@task.python
def process_b(partner_name, partner_path):
    print(partner_name)
    print(partner_path)

@task.python
def process_c(partner_name, partner_path):
    print(partner_name)
    print(partner_path)

@task.python
def check_a():
    print("checking")

@task.python
def check_b():
    print("checking")

@task.python
def check_c():
    print("checking")

def process_tasks(partner_settings):
    with TaskGroup(group_id='process_tasks') as process_tasks:
        with TaskGroup(group_id='test_tasks') as test_tasks:
            check_a()
            check_b()
            check_c()
        process_a(partner_settings['partner_name'], partner_settings['partner_path']) >> test_tasks
        process_b(partner_settings['partner_name'], partner_settings['partner_path']) >> test_tasks
        process_c(partner_settings['partner_name'], partner_settings['partner_path']) >> test_tasks
    return process_tasks