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

# Script generated for node Customer Curated Zone
CustomerCuratedZone_node1693852360471 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-vgonzenb-lake-house/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCuratedZone_node1693852360471",
)

# Script generated for node Step Trainer Landing Zone
StepTrainerLandingZone_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-vgonzenb-lake-house/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLandingZone_node1",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1693853614942 = ApplyMapping.apply(
    frame=CustomerCuratedZone_node1693852360471,
    mappings=[
        ("serialNumber", "string", "c_serialNumber", "string"),
        ("birthDay", "string", "c_birthDay", "string"),
        ("shareWithPublicAsOfDate", "bigint", "c_shareWithPublicAsOfDate", "bigint"),
        (
            "shareWithResearchAsOfDate",
            "bigint",
            "c_shareWithResearchAsOfDate",
            "bigint",
        ),
        ("registrationDate", "bigint", "c_registrationDate", "bigint"),
        ("customerName", "string", "c_customerName", "string"),
        ("email", "string", "c_email", "string"),
        ("lastUpdateDate", "bigint", "c_lastUpdateDate", "bigint"),
        ("phone", "string", "c_phone", "string"),
        ("shareWithFriendsAsOfDate", "bigint", "c_shareWithFriendsAsOfDate", "bigint"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1693853614942",
)

# Script generated for node Join
Join_node1693852447634 = Join.apply(
    frame1=StepTrainerLandingZone_node1,
    frame2=RenamedkeysforJoin_node1693853614942,
    keys1=["serialNumber"],
    keys2=["c_serialNumber"],
    transformation_ctx="Join_node1693852447634",
)

# Script generated for node Drop Fields
DropFields_node1693852564955 = DropFields.apply(
    frame=Join_node1693852447634,
    paths=[
        "c_serialNumber",
        "c_shareWithResearchAsOfDate",
        "c_birthDay",
        "c_shareWithPublicAsOfDate",
        "c_shareWithFriendsAsOfDate",
        "c_registrationDate",
        "c_customerName",
        "c_email",
        "c_lastUpdateDate",
        "c_phone",
    ],
    transformation_ctx="DropFields_node1693852564955",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node3 = glueContext.getSink(
    path="s3://udacity-vgonzenb-lake-house/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="StepTrainerTrusted_node3",
)
StepTrainerTrusted_node3.setCatalogInfo(
    catalogDatabase="lakehouse", catalogTableName="step_trainer_trusted"
)
StepTrainerTrusted_node3.setFormat("json")
StepTrainerTrusted_node3.writeFrame(DropFields_node1693852564955)
job.commit()
