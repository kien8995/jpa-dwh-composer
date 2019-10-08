from google.cloud import bigquery
from datetime import datetime
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from utils.bigquery import check_table_exist

class FLT_BLTask(object):
    def __init__(self, dag):
        self.dag = dag
        self.table_id = 'dwh-demo.staging_flight.flt_bl'
        self.task_id_1 = 'check_table_flt_bl'
        self.task_id_2 = 'delete_old_data'
        self.task_id_3 = 'load_avro'

    def tasks(self):
        t1 = PythonOperator(
            task_id=self.task_id_1,
            python_callable=self.__check_table,
            provide_context=True,
            dag=self.dag
        )

        t2 = ShortCircuitOperator(
            task_id=self.task_id_2,
            python_callable=self.__delete_data,
            provide_context=True,
            dag=self.dag,
        )

        t3 = PythonOperator(
            task_id=self.task_id_3,
            python_callable=self.__load_avro,
            provide_context=True,
            dag=self.dag
        )

        t1 >> t2 >> t3
        return t1, t3
    
    def __check_table(self, **context):
        if not check_table_exist(self.table_id):
            schema = [
                bigquery.SchemaField("flight_date", "DATE", mode="NULLABLE"),
                bigquery.SchemaField("carrier_code", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("flight_number", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("departure_date", "DATE", mode="NULLABLE"),
                bigquery.SchemaField("arrival_date", "DATE", mode="NULLABLE"),
                bigquery.SchemaField("leg_std", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("leg_sta", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("adt", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("chd", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("inf", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("departure_station", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("arrival_station", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("segment", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("capacity", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("bd", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("ns", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("pax_rev", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("baggage_amount", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("other_rev", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("cargo_rev", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("v_cost", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("f_cost", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("total_cost", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("qtqn", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("fls_type", "STRING", mode="NULLABLE"),
            ]

            client = bigquery.Client()
            table = bigquery.Table(self.table_id, schema=schema)
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="flight_date"
            )
            table.clustering_fields = ["flight_number"]
            table = client.create_table(table)
            print(
                "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
            )

    def __delete_data(self, **context):
        client = bigquery.Client()
        query = """
            #standardSQL
            DELETE `{table_id}` WHERE flight_date = '{date}'
        """.format(
            table_id=self.table_id,
            date=datetime.strptime(context['yesterday_ds_nodash'], '%Y%m%d').strftime('%Y-%m-%d')
        )

        try:
            query_job = client.query(query)
            query_job.result()
            return True
        except Exception as e:
            print(e)

        return False

    def __load_avro(self, **context):
        client = bigquery.Client()
        table = client.get_table(self.table_id)

        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        job_config.source_format = bigquery.SourceFormat.AVRO
        job_config.use_avro_logical_types = True
        uri = "gs://jpa_staging_demo/flt_bl/flt_bl.avro"
        load_job = client.load_table_from_uri(
            uri, table, job_config=job_config
        )
        print("Starting job {}".format(load_job.job_id))

        load_job.result()  # Waits for table load to complete.
        print("Job finished.")

        destination_table = client.get_table(table)
        print("Loaded {} rows.".format(destination_table.num_rows))

