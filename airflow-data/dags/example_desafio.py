from airflow.utils.edgemodifier import Label
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from textwrap import dedent
from airflow import DAG
from airflow.models import Variable
import os
import logging
from etl_dag import extract_load, process

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,  # Desativado para evitar problemas com envio de e-mails
    'email_on_retry': False,    # Desativado para evitar problemas com envio de e-mails
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


## Do not change the code below this line ---------------------!!#
def export_final_answer():
    import base64

    # Caminho absoluto para o arquivo count.txt
    file_path = '/mnt/c/Users/thafe/Desktop/LH_desafio7/airflow_tooltorial/count.txt'

    # Verifica se o arquivo count.txt existe
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Arquivo {file_path} não foi encontrado.")

    logging.info(f"Lendo o arquivo {file_path}")

    # Import count
    with open(file_path, 'r') as f:
        count = f.readlines()[0]

    # Recupera a variável my_email
    my_email = Variable.get("my_email", default_var="default_email@example.com")
    message = my_email + count
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')

    # Escreve o resultado codificado em final_output.txt
    output_file_path = '/mnt/c/Users/thafe/Desktop/LH_desafio7/airflow_tooltorial/final_output.txt'
    with open(output_file_path, 'w') as f:
        f.write(base64_message)

    logging.info(f"Resultado final salvo em {output_file_path}")
    return None
## Do not change the code above this line-----------------------##


with DAG(
    'DesafioAirflow',
    default_args=default_args,
    description='Desafio de Airflow da Indicium',
    schedule_interval=timedelta(minutes=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    dag.doc_md = """
        Esse é o desafio de Airflow da Indicium.
    """

    # Task para exportar o resultado final
    export_final_output = PythonOperator(
        task_id='export_final_output',
        python_callable=export_final_answer,
        provide_context=True
    )

    # Task para extrair dados
    get_orders = PythonOperator(
        task_id='extract_data',
        python_callable=extract_load,
        op_kwargs={'table': 'Order',
                   'file_path': '/mnt/c/Users/thafe/Desktop/LH_desafio7/airflow_tooltorial/output_orders.csv'},  # Caminho absoluto para garantir que o arquivo seja salvo corretamente
    )

    get_orders.doc_md = dedent(
        """\
        #### Task Documentation

        This task extracts the data from the sqlite database and loads it into 'output_orders.csv'.
        The function to run the task (extract_load) receives two arguments: the table to be extracted and the path
        to save the query result.
        """
    )

    # Task para calcular vendas no Brasil
    calculate = PythonOperator(
        task_id='calculate_sales_brasil',
        python_callable=process,
        op_kwargs={'join_table': 'OrderDetail',
                   'left_key': 'Id',
                   'right_key': 'OrderId',
                   'how': 'inner',
                   'file_path': '/mnt/c/Users/thafe/Desktop/LH_desafio7/airflow_tooltorial/count.txt'},  # Caminho absoluto para salvar count.txt
    )

    calculate.doc_md = dedent(
        """\
        #### Task Documentation

        This task loads another table and executes a join with the Order data, obtained in the first task.
        To do the join, it needs to pass the table to be extracted from the sqlite database, the keys
        related to the tables, and the path to store the result. Note that this task calls
        the function used in the first task, but just to extract the data.
        """
    )

    # Definindo a sequência de execução das tasks
    get_orders >> calculate >> export_final_output
