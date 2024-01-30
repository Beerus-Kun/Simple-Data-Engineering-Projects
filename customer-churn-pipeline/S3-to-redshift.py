import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1706109192239 = glueContext.create_dynamic_frame.from_catalog(
    database="s3-glue-database",
    table_name="customer_churn_pipeline",
    transformation_ctx="AmazonS3_node1706109192239",
)

# Script generated for node Change Schema
ChangeSchema_node1706109198714 = ApplyMapping.apply(
    frame=AmazonS3_node1706109192239,
    mappings=[
        ("customerid", "string", "CustomerID", "string"),
        ("city", "string", "City", "string"),
        ("zip code", "long", "Zip_Code", "int"),
        ("gender", "string", "gender", "string"),
        ("senior citizen", "string", "Senior_Citizen", "string"),
        ("partner", "string", "partner", "string"),
        ("dependents", "string", "dependents", "string"),
        ("tenure months", "long", "Tenure_Months", "int"),
        ("phone service", "string", "Phone_Service", "string"),
        ("multiple lines", "string", "Multiple_Lines", "string"),
        ("internet service", "string", "Internet_Service", "string"),
        ("online security", "string", "Online_Security", "string"),
        ("online backup", "string", "Online_Backup", "string"),
        ("device protection", "string", "Device_Protection", "string"),
        ("tech support", "string", "Tech_Support", "string"),
        ("streaming tv", "string", "Streaming_TV", "string"),
        ("streaming movies", "string", "Streaming_Movies", "string"),
        ("contract", "string", "contract", "string"),
        ("paperless billing", "string", "Paperless_Billing", "string"),
        ("payment method", "string", "Payment_Method", "string"),
        ("monthly charges", "double", "monthly_charges", "double"),
        ("total charges", "double", "Total_Charges", "double"),
        ("churn label", "string", "Churn_Label", "string"),
        ("churn value", "long", "Churn_Value", "long"),
        ("churn score", "long", "Churn_Score", "long"),
        ("churn reason", "string", "Churn_Reason", "string"),
    ],
    transformation_ctx="ChangeSchema_node1706109198714",
)

# Script generated for node Amazon Redshift
AmazonRedshift_node1706109242780 = glueContext.write_dynamic_frame.from_options(
    frame=ChangeSchema_node1706109198714,
    connection_type="redshift",
    connection_options={
        "redshiftTmpDir": "s3://aws-glue-assets-013462464526-us-east-1/temporary/",
        "useConnectionProperties": "true",
        "dbtable": "public.customer_churn",
        "connectionName": "Redshift connection",
        "preactions": "CREATE TABLE IF NOT EXISTS public.customer_churn (CustomerID VARCHAR, City VARCHAR, Zip_Code INTEGER, gender VARCHAR, Senior_Citizen VARCHAR, partner VARCHAR, dependents VARCHAR, Tenure_Months INTEGER, Phone_Service VARCHAR, Multiple_Lines VARCHAR, Internet_Service VARCHAR, Online_Security VARCHAR, Online_Backup VARCHAR, Device_Protection VARCHAR, Tech_Support VARCHAR, Streaming_TV VARCHAR, Streaming_Movies VARCHAR, contract VARCHAR, Paperless_Billing VARCHAR, Payment_Method VARCHAR, monthly_charges DOUBLE PRECISION, Total_Charges DOUBLE PRECISION, Churn_Label VARCHAR, Churn_Value BIGINT, Churn_Score BIGINT, Churn_Reason VARCHAR); TRUNCATE TABLE public.customer_churn;",
    },
    transformation_ctx="AmazonRedshift_node1706109242780",
)

job.commit()
