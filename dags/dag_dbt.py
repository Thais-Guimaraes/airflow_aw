import os
from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Carregar variáveis do .env
load_dotenv()

# Variáveis do .env
DBT_REPO_URL = os.getenv('DBT_REPO_URL')
DBT_LOCAL_PATH = os.getenv('DBT_LOCAL_PATH')
DBT_PROFILES_DIR = os.getenv('DBT_PROFILES_DIR')
DBT_VENV_PATH = os.getenv('DBT_VENV_PATH')

# Configurações padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definição da DAG
with DAG(
    'dbt_pipeline',
    default_args=default_args,
    description='dbt transformations and tests',
    schedule_interval='0 3 * * * ',  # 0 daily at 3 oclock
    start_date=datetime(2025, 1, 23),
    catchup=False,
    tags=['dbt'],
) as dag:

    # Task 1: Preparar o repositório DBT
    prepare_repo = BashOperator(
    task_id='prepare_dbt_repo',
    bash_command=f'''
    if [ -d "{DBT_LOCAL_PATH}/.git" ]; then
        cd {DBT_LOCAL_PATH} && git pull
    elif [ ! "$(ls -A {DBT_LOCAL_PATH})" ]; then
        git clone {DBT_REPO_URL} {DBT_LOCAL_PATH}
    else
        echo "Erro: O diretório já existe, mas não é um repositório Git válido. Por favor, remova manualmente."
        exit 1
    fi
    ''',
)

    # Task 2: Instalar dependências do DBT
    dbt_deps = BashOperator(
       task_id='run_dbt_deps',
        bash_command=f'''
        source {DBT_VENV_PATH}/bin/activate &&
        cd {DBT_LOCAL_PATH} &&
        dbt deps --profiles-dir {DBT_PROFILES_DIR}
        ''',
    )

    # Task 3: Executar o build do DBT
    dbt_build = BashOperator(
        task_id='run_dbt_build',
        bash_command=f'cd {DBT_LOCAL_PATH} && {DBT_VENV_PATH}/bin/dbt build --profiles-dir {DBT_PROFILES_DIR}',
    )

    # Ordem das tarefas
    prepare_repo >> dbt_deps >> dbt_build