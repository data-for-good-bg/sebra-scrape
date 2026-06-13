import pendulum
import textwrap

from airflow.models import DAG, Param
from airflow.operators.bash import BashOperator
from typing import Dict, Optional, List


# Common bash command template
BASH_COMMAND_TEMPLATE = textwrap.dedent('''\
    echo "PYTHON_EXE: $PYTHON_EXE"
    echo "DATE_STR: $DATE_STR"
    echo "LOOK_BACK_DAYS: $LOOK_BACK_DAYS"
    echo "WORK_DIR: $WORK_DIR"

    $PYTHON_EXE -m sebra_scrape.app \
        collect-to-gdrive-for-days \
        --date "$DATE_STR" \
        --lookback-days "$LOOK_BACK_DAYS" \
        --target-dir "$WORK_DIR" \
        --use-local-cache
    '''
)


def create_manual_sebra_scrape_dag(
    dag_id: str,
    gdrive_folder_var_name: str,
    gdrive_creds_var_name: str,
    tags: List[str]
) -> DAG:
    # Build params if use_params is True

    params = {
        'date_str': Param(
            default='',
            description_md='''
            Specifies the date for which to run the scrape.
            The expected format is <yyyy-mm-dd>.
            '''
        ),
        'look_back_days': Param(
            type='integer',
            description='Days to look back.'
        ),
    }

    env = {
        'SEBRA_GOOGLE_DRIVE_FOLDER_URL': f"{{{{ var.value.get('{gdrive_folder_var_name}') }}}}",
        'SEBRA_GOOGLE_DRIVE_CREDS': f"{{{{ var.value.get('{gdrive_creds_var_name}') }}}}",
        'PYTHON_EXE': "{{ var.value.get('VENVS_ROOT') }}/sebra_scrape/bin/python3",
        'WORK_DIR': "{{ var.value.get('WORKDIR_ROOT') }}/sebra_scrape",
        'ZYTE_API_KEY': '{{ var.value.ZYTE_API_KEY }}',
        'DATE_STR': "{{ params['date_str'] }}",
        'LOOK_BACK_DAYS': "{{ params['look_back_days'] }}",
    }

    # Create the DAG instance
    dag = DAG(
        dag_id=dag_id,
        schedule=None,
        start_date=pendulum.datetime(2020, 1, 1, tz="UTC"),
        catchup=False,
        max_active_runs=1,
        tags=tags,
        params=params,
    )

    BashOperator(
        task_id='sebra_scrape_app',
        bash_command=BASH_COMMAND_TEMPLATE,
        env=env,
        dag=dag,
    )

    return dag


def create_scheduled_sebra_scrape_dag(
    dag_id: str,
    gdrive_folder_var_name: str,
    gdrive_creds_var_name: str,
    schedule: str,
    start_date: pendulum.datetime,
    tags: List[str]
) -> DAG:
    # Build params if use_params is True

    env = {
        'SEBRA_GOOGLE_DRIVE_FOLDER_URL': f"{{{{ var.value.get('{gdrive_folder_var_name}') }}}}",
        'SEBRA_GOOGLE_DRIVE_CREDS': f"{{{{ var.value.get('{gdrive_creds_var_name}') }}}}",
        'PYTHON_EXE': "{{ var.value.get('VENVS_ROOT') }}/sebra_scrape/bin/python3",
        'WORK_DIR': "{{ var.value.get('WORKDIR_ROOT') }}/sebra_scrape",
        'ZYTE_API_KEY': '{{ var.value.ZYTE_API_KEY }}',
        'DATE_STR': "{{ macros.ds_add(ds, -1) }}",
        'LOOK_BACK_DAYS': "7",
    }

    # Create the DAG instance
    dag = DAG(
        dag_id=dag_id,
        schedule=schedule,
        start_date=start_date,
        catchup=True,
        max_active_runs=1,
        tags=tags,
    )

    BashOperator(
        task_id='sebra_scrape_app',
        bash_command=BASH_COMMAND_TEMPLATE,
        env=env,
        dag=dag,
    )

    return dag



# ===== Instantiate DAGs =====

# Manual DAG with runtime parameters
sebra_scrape_manual_test = create_manual_sebra_scrape_dag(
    dag_id='sebra_scrape_manual_test',
    gdrive_folder_var_name='TEST_SEBRA_GOOGLE_DRIVE_FOLDER_URL',
    gdrive_creds_var_name='SEBRA_GOOGLE_DRIVE_CREDS',
    tags=['sebra', 'manual', 'test']
)

sebra_scrape_manual = create_manual_sebra_scrape_dag(
    dag_id='sebra_scrape_manual',
    gdrive_folder_var_name='SEBRA_GOOGLE_DRIVE_FOLDER_URL',
    gdrive_creds_var_name='SEBRA_GOOGLE_DRIVE_CREDS',
    tags=['sebra', 'manual', 'official']
)

sebra_scrape_monday_test = create_scheduled_sebra_scrape_dag(
    dag_id='sebra_scrape_monday_test',
    gdrive_folder_var_name='TEST_SEBRA_GOOGLE_DRIVE_FOLDER_URL',
    gdrive_creds_var_name='SEBRA_GOOGLE_DRIVE_CREDS',
    schedule='0 20 * * 1',
    start_date=pendulum.datetime(2026, 4, 1, tz="UTC"),
    tags=['sebra', 'scheduled', 'test']
)

sebra_scrape_monday = create_scheduled_sebra_scrape_dag(
    dag_id='sebra_scrape_monday',
    gdrive_folder_var_name='SEBRA_GOOGLE_DRIVE_FOLDER_URL',
    gdrive_creds_var_name='SEBRA_GOOGLE_DRIVE_CREDS',
    schedule='0 20 * * 1',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    tags=['sebra', 'scheduled', 'official']
)
