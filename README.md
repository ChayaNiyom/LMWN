# Data Engineer Test (Hand-On)

I was trying to use Postgres but I got stuck on connecting to Hive. This is what I've done with Postgres DB

    test=# CREATE TABLE order_detail (
    test(#  order_created_timestamp TIMESTAMP,
    test(#  status VARCHAR(12),
    test(#  price INT,
    test(#  discount FLOAT,
    test(#  id VARCHAR(40),
    test(#  driver_id VARCHAR(40),
    test(#  user_id VARCHAR(40),
    test(#  restaurant_id VARCHAR(40) );
    CREATE TABLE
    test=# CREATE TABLE restaurant_detail (
    test(#  id VARCHAR(40),
    test(#  restaurant_name VARCHAR(15),
    test(#  category VARCHAR(10),
    test(#  estimated_cooking_time FLOAT,
    test(#  latitude FLOAT,
    test(#  longitude FLOAT );
    CREATE TABLE
    test=# \d
                 List of relations
     Schema |       Name        | Type  | Owner
    --------+-------------------+-------+-------
    public | order_detail      | table | GAP
    public | restaurant_detail | table | GAP
    (2 rows)

    test=# COPY order_detail FROM '/Users/GAP/Desktop/LMWN/order_detail.csv' DELIMITER ',' CSV HEADER;
    COPY 395361
    test=# COPY restaurant_detail FROM '/Users/GAP/Desktop/LMWN/restaurant_detail.csv' DELIMITER ',' CSV HEADER;
    ERROR:  value too long for type character varying(10)
    CONTEXT:  COPY restaurant_detail, line 3386, column category: "JAPANESE FOOD"
    test=# ALTER TABLE restaurant_detail
    test-#  ALTER COLUMN category TYPE VARCHAR(20);
    ALTER TABLE
    test=# COPY restaurant_detail FROM '/Users/GAP/Desktop/LMWN/restaurant_detail.csv' DELIMITER ',' CSV HEADER;
    COPY 12623

Since I couldn’t connect DB to Hive, so I do data cleansing on Google Colab and adjust the code for Airflow DAG

setting

    #Import Airflow Packages
    from airflow.models import DAG
    from airflow.operators.python import PythonOperator
    from airflow.providers.mysql.hooks.mysql import MySqlHook
    from airflow.utils.dates import days_ago
    
    #Import PySpark Packages
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import lit
    from pyspark.sql import functions as f
    from pyspark.sql.functions import expr
    from pyspark.sql.functions import when
    
    #Import Pandas
    import pandas as pd

    #Create Spark Session for Spark
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    #Cloud Directory 
    order_detail_input_path = "/home/airflow/gcs/data/order_detail.csv"
    restaurant_detail_input_path = "/home/airflow/gcs/data/restaurant_detail.csv"
    cooking_output_path = "/home/airflow/gcs/data/cooking.csv"
    discount_output_path = "/home/airflow/gcs/data/discount.csv"

function for DAG

    #Load order_detail.CSV file and create PySpark Dataframe
    def get_data_discount(spark_order):

        order_detail = pd.read_csv(order_detail_input_path)
        spark_order = spark.createDataFrame(order_detail) 
    
    #Create dt from Timestamp and new discount column
    def clean_data_discount(discount_detail_path):

        order_detail_m1 = spark_order.withColumn("order_created_timestamp",
                          f.to_timestamp(spark_order.order_created_timestamp, 'yyyy-MM-dd HH:mm:ss')
                          )
        order_detail_m1_dt = order_detail_m1.withColumn("dt", expr(" date_format(order_created_timestamp, 'yyyyMMdd')  "))
        order_detail_clean = order_detail_m1_dt.withColumn("discount_no_null", 
                          when(order_detail_m1_dt['discount'] == 'NaN' , '0.0').otherwise(order_detail_m1_dt['discount'])
                         )
        order_detail_clean.coalesce(1).write.csv(discount_detail_path, header = True)
                                
    #Load restaurant_detail.CSV file and create PySpark Dataframe
    def get_data_cooking(spark_restaurant):

        restaurant_detail = pd.read_csv(restaurant_detail_input_path)
        spark_restaurant = spark.createDataFrame(restaurant_detail) 

    #Create dt column and cooking bin with condition
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

DAG Assign

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

