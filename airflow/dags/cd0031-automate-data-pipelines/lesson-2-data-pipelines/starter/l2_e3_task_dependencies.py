import pendulum
import logging

from airflow.decorators import dag, task

@dag(
    schedule_interval='@hourly',
    start_date=pendulum.now()
)
def task_dependencies():

    @task()
    def hello_world():
        logging.info("Hello World")

    @task()
    def addition(first,second):
        logging.info(f"{first} + {second} = {first+second}")
        return first+second

    @task()
    def subtraction(first,second):
        logging.info(f"{first -second} = {first-second}")
        return first-second

    @task()
    def division(first,second):
        logging.info(f"{first} / {second} = {int(first/second)}")   
        return int(first/second)     

    # TODO: call the hello world task function
    hello = hello_world()
    # TODO: call the addition function with some constants (numbers)
    two_plus_three=addition(2,3)
    # TODO: call the subtraction function with some constants (numbers)
    one_from_six=subtraction(6,1)
    # TODO: call the division function with some constants (numbers)
    eight_divided_by_four = division(8,4)
    # TODO: create the dependency graph for the first three tasks
    hello >> two_plus_three
    hello >> one_from_six

    two_plus_three >> eight_divided_by_four
    one_from_six >> eight_divided_by_four
    # TODO: Configure the task dependencies such that the graph looks like the following:
#
#                    ->  addition_task
#                   /                 \
#   hello_world_task                   -> division_task
#                   \                 /
#                    ->subtraction_task

#  TODO: assign the result of the addition function to a variable
#  TODO: assign the result of the subtraction function to a variable
#  TODO: pass the result of the addition function, and the subtraction functions to the division function
# TODO: create the dependency graph for the last three tasks


task_dependencies_dag=task_dependencies()