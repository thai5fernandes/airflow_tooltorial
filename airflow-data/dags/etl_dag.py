import pandas as pd
import csv
import os
import logging
from typing import Optional, Tuple, List, Union
from airflow.providers.sqlite.hooks.sqlite import SqliteHook


def extract_load(
    table: str, file_path: Optional[str] = None
) -> Union[None, Tuple[List, List]]:
    """
    Função para extração de uma tabela e carregamento se especificado.

    Args:
        table (str): tabela desejada para a extração.
        file_path (Optional[str], optional): Arquivo que deseja salvar a tabela, ex: output.csv. Defaults to None.

    Returns:
        None | Tuple[List, List]: None se realizar o carregamento da tabela uma tupla de listas
        contendo as linhas extraídas e o nomes das colunas, respectivamente.
    """
    hook = SqliteHook(sqlite_conn_id="sqlite_connection")
    conn = hook.get_conn()
    cursor = conn.cursor()
    select_query = f"SELECT * FROM '{table}'"
    cursor.execute(select_query)
    lines = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    if file_path:
        with open(file_path, "w", newline="", encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(columns)
            for line in lines:
                writer.writerow(line)
    else:
        return (lines, columns)


def process(
    join_table: str, left_key: str, right_key: str, how: str, file_path: str
) -> None:
    """
    Função para calcular a quantidade vendida para o Rio de Janeiro.

    Args:
        join_table (str): Tabela desejada para fazer o join.
        left_key (str): Chave para o join da tabela à esquerda.
        right_key (str): Chave para o join da tabela à direita.
        how (str): De qual maneira o join será executado.
        file_path (str): Arquivo para salvar o resultado da operação.
    """
    logging.info(f"Iniciando o processamento para a tabela {join_table}")

    # Verificando se o arquivo output_orders.csv existe
    if not os.path.exists("output_orders.csv"):
        raise FileNotFoundError("O arquivo 'output_orders.csv' não foi encontrado.")
    
    lines, columns = extract_load(table=join_table)
    df2 = pd.DataFrame(lines, columns=columns)
    df1 = pd.read_csv("output_orders.csv")

    # Realizando o join entre as tabelas
    df_join = pd.merge(df1, df2, left_on=left_key, right_on=right_key, how=how)
    logging.info(f"Join realizado com sucesso. Tamanho do dataframe resultante: {df_join.shape}")

    # Calculando a soma das quantidades vendidas no Rio de Janeiro
    result = df_join.loc[df_join["ShipCity"] == "Rio de Janeiro", "Quantity"].sum()

    # Salvando o resultado no arquivo count.txt
    with open(file_path, "w") as f:
        f.write(str(result))
    logging.info(f"Arquivo '{file_path}' salvo com o resultado.")
