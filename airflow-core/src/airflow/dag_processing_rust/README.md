Rust Dag Processor POC - How to run with Breeze
===============================================

0. `breeze down` and ensure that there are no Dags in `/files/dags`.
1. Copy contents from `airflow/dag_processing_rust/.env.template` to `airflow/dag_processing_rust/.env` (create a new file/rename the existing one if you don't commit it).
2. Generate 10,000 DAGs within `/files/dags`, you may utilize the following Python script within `/files` (**Important** - DAGs should not utilize Taskflow syntax):
```python
import os
NUM_OF_COPIES = 9999

# Original file content
file_content = """from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.standard.operators.python import PythonOperator

with DAG(
    dag_id="example_dag_<%id%>",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    task_hello = PythonOperator(
        task_id="say_hello",
        python_callable=lambda: print("hey"),
    )
    task_bye = PythonOperator(
        task_id="say_bye",
        python_callable=lambda: print("Goodbye!"),
    )

    task_hello >> task_bye
"""

# Function to create copies of the file
def create_copies(num_copies, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for i in range(0, num_copies):
        file_name = f"dag_copy_{i}.py"
        file_path = os.path.join(output_dir, file_name)
        with open(file_path, "w") as f:
            f.write(file_content.replace("<%id%>", str(i)))

# Create 5 copies in the 'output' directory
create_copies(NUM_OF_COPIES, "./dags/")
```
3. Run `breeze start-airflow` Dags processor is already configured to utilize the Rust POC. It should take about 1.5 minutes to process all Dags (**Warning:** the processor doesn't handle well SIGTERM signals :D).