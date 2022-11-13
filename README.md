# Data Engineer Test (Hand-On)

'''python

    from airflow.models import DAG
    from airflow.operators.python import PythonOperator
    from airflow.providers.mysql.hooks.mysql import MySqlHook
    from airflow.utils.dates import days_ago
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import lit
    from pyspark.sql import functions as f
    from pyspark.sql.functions import expr
    from pyspark.sql.functions import when
    import pandas as pd


    spark = SparkSession.builder.master("local[*]").getOrCreate()


    order_detail_input_path = "/home/airflow/gcs/data/order_detail.csv"
    restaurant_detail_input_path = "/home/airflow/gcs/data/restaurant_detail.csv"
    cooking_output_path = "/home/airflow/gcs/data/cooking.csv"
    discount_output_path = "/home/airflow/gcs/data/discount.csv"

    def get_data_discount(spark_order):

        order_detail = pd.read_csv(order_detail_input_path)
        spark_order = spark.createDataFrame(order_detail) 

    def clean_data_discount(discount_detail_path):

        order_detail_m1 = spark_order.withColumn("order_created_timestamp",
                          f.to_timestamp(spark_order.order_created_timestamp, 'yyyy-MM-dd HH:mm:ss')
                          )
        order_detail_m1_dt = order_detail_m1.withColumn("dt", expr(" date_format(order_created_timestamp, 'yyyyMMdd')  "))
        order_detail_clean = order_detail_m1_dt.withColumn("discount_no_null", 
                          when(order_detail_m1_dt['discount'] == 'NaN' , '0.0').otherwise(order_detail_m1_dt['discount'])
                         )
        order_detail_clean.coalesce(1).write.csv(discount_detail_path, header = True)
                                

    def get_data_cooking(spark_restaurant):

        restaurant_detail = pd.read_csv(restaurant_detail_input_path)
        spark_restaurant = spark.createDataFrame(restaurant_detail) 


    def clean_data_cooking(cooking_detail_path):

        restaurant_detail_m1 = spark_restaurant.withColumn("dt", lit("latest"))

        restaurant_detail_new = restaurant_detail_m1.withColumn("cooking_bin", \
        when((restaurant_detail_m1.esimated_cooking_time >= 10) & (restaurant_detail_m1.esimated_cooking_time <= 40), lit("1")) \
        .when((restaurant_detail_m1.esimated_cooking_time >= 41) & (restaurant_detail_m1.esimated_cooking_time <= 80), lit("2")) \
        .when((restaurant_detail_m1.esimated_cooking_time >= 81) & (restaurant_detail_m1.esimated_cooking_time <= 120), lit("3")) \
        .when((restaurant_detail_m1.esimated_cooking_time >= 121), lit("4")) \
        .otherwise(lit("0")) \
        )

        restaurant_detail_new.coalesce(1).write.csv(cooking_detail_path, header = True)

    with DAG(
        "lmwn_dag",
        start_date=days_ago(1),
        schedule_interval="@daily",
        tags=["workshop"]
    ) as dag:


        t1 = PythonOperator(
            task_id="Read_Order_Detail",
            python_callable = get_data_discount,
            op_kwargs = {"spark_order":order_detail_input_path}
            )
        t2 = PythonOperator(
            task_id="Read_Restaurant_Detail",
            python_callable = get_data_cooking,
            op_kwargs = {"spark_restaurant":restaurant_detail_input_path}
            )
        t3 = PythonOperator(
            task_id="Clean_Order_Detail",
            python_callable = clean_data_discount,
            op_kwargs = {
                "spark_order":order_detail_input_path,
                "discount_detail_path":discount_output_path
                }
            )
        t4 = PythonOperator(
            task_id="Clean_Restaurant_Detail",
            python_callable = clean_data_cooking,
            op_kwargs = {
                "spark_restaurant":restaurant_detail_input_path,
                "cooking_detail_path":cooking_output_path
                }
            )
        t1 >> t3
        t2 >> t4
'''
