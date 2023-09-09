import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Landing Zone
CustomerLandingZone_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-vgonzenb-lake-house/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLandingZone_node1",
)

# Script generated for node Filter
Filter_node1693768955914 = Filter.apply(
    frame=CustomerLandingZone_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="Filter_node1693768955914",
)

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node3 = glueContext.getSink(
    path="s3://udacity-vgonzenb-lake-house/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerTrustedZone_node3",
)
CustomerTrustedZone_node3.setCatalogInfo(
    catalogDatabase="lakehouse", catalogTableName="customer_trusted"
)
CustomerTrustedZone_node3.setFormat("json")
CustomerTrustedZone_node3.writeFrame(Filter_node1693768955914)
job.commit()
