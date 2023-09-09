import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Step Trainer Trusted Zone
StepTrainerTrustedZone_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-vgonzenb-lake-house/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerTrustedZone_node1",
)

# Script generated for node Accelerometer Trusted Zone
AccelerometerTrustedZone_node1693858312787 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://udacity-vgonzenb-lake-house/accelerometer/trusted/"],
            "recurse": True,
        },
        transformation_ctx="AccelerometerTrustedZone_node1693858312787",
    )
)

# Script generated for node Join on Timestamp
JoinonTimestamp_node1693858341527 = Join.apply(
    frame1=AccelerometerTrustedZone_node1693858312787,
    frame2=StepTrainerTrustedZone_node1,
    keys1=["timeStamp"],
    keys2=["sensorReadingTime"],
    transformation_ctx="JoinonTimestamp_node1693858341527",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node3 = glueContext.getSink(
    path="s3://udacity-vgonzenb-lake-house/ml/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="MachineLearningCurated_node3",
)
MachineLearningCurated_node3.setCatalogInfo(
    catalogDatabase="lakehouse", catalogTableName="machine_learning_curated"
)
MachineLearningCurated_node3.setFormat("json")
MachineLearningCurated_node3.writeFrame(JoinonTimestamp_node1693858341527)
job.commit()
