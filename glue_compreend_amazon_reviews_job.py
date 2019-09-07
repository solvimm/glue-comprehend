import os
import sys
import boto3

from awsglue.job import Job
from awsglue.transforms import *
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

import pyspark.sql.functions as F
from pyspark.sql import Row, Window, SparkSession
from pyspark.sql.types import *
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

AWS_REGION = 'us-east-1'  # Try another region if you got Comprehend ThrottlingException
MIN_SENTENCE_LENGTH_IN_CHARS = 10
MAX_SENTENCE_LENGTH_IN_CHARS = 4500
COMPREHEND_BATCH_SIZE = 25  # This batch size results in groups no larger than 25 items
NUMBER_OF_BATCHES = 10
ROW_LIMIT = 10000
S3_BUCKET = '<<BUCKET_NAME>>'  # Insert your Bucket Name
FLAG_PROD = True
BUCKET_SUFFIX = 'notebook/'

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = SparkSession.builder.config("spark.sql.broadcastTimeout", "600").getOrCreate()

# Only use this in Prod Environment (Glue job)
try:
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    BUCKET_SUFFIX = 'glue/'
except:
    FLAG_PROD = False
    print('Init on Dev Environment.')

spark._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
spark._jsc.hadoopConfiguration().set("parquet.enable.summary-metadata", "false")

SentimentRow = Row("review_id", "sentiment")


def getBatchSentiment(input_list):
    # Nested Function for Encapsulation
    def callApi(text_list):
        response = client.batch_detect_sentiment(TextList=text_list, LanguageCode='en')
        return response

    # You can import the ratelimit module if you want to further rate limit API calls to Comprehend
    # https://pypi.org/project/ratelimit/
    # from ratelimit import rate_limited
    arr = []
    bodies = [i[1] for i in input_list]
    client = boto3.client('comprehend', region_name=AWS_REGION)

    for i in range(NUMBER_OF_BATCHES-1):
        text_list = bodies[COMPREHEND_BATCH_SIZE*i:COMPREHEND_BATCH_SIZE*(i+1)]
        response = callApi(text_list)
        for r in response['ResultList']:
            idx = COMPREHEND_BATCH_SIZE * i + r['Index']
            arr.append(SentimentRow(input_list[idx][0], r['Sentiment']))

    return arr

reviews = spark.read.parquet("s3://amazon-reviews-pds/parquet").distinct()

df = reviews.filter("marketplace = 'US'").withColumn('body_len', F.length('review_body')).filter(F.col('body_len') > MIN_SENTENCE_LENGTH_IN_CHARS).filter(F.col('body_len') < MAX_SENTENCE_LENGTH_IN_CHARS).limit(ROW_LIMIT)

if not FLAG_PROD:
    df.show(n=2, truncate=False)

record_count = df.count()
print('Number of records: {}'.format(record_count))

num_of_partitions = record_count / (NUMBER_OF_BATCHES * COMPREHEND_BATCH_SIZE)
num_of_partitions = int(round(num_of_partitions))
if num_of_partitions == 0:
    num_of_partitions = 1

df2 = df.repartition(num_of_partitions).sortWithinPartitions(['review_id'], ascending=True)

group_rdd = df2.rdd.map(lambda l: (l.review_id, l.review_body)).glom()

sentiment = group_rdd.coalesce(10).map(lambda l: getBatchSentiment(l)).flatMap(lambda x: x).toDF().repartition('review_id').cache()

joined = reviews.drop('review_body').join(sentiment, sentiment.review_id == reviews.review_id).drop(sentiment.review_id)

if not FLAG_PROD:
    record_count = joined.count()
    print('Number of records after join:', record_count)

joined.write.partitionBy('product_category').mode('overwrite').parquet('s3://{}/amazon_reviews/job/{}'.format(S3_BUCKET, BUCKET_SUFFIX))

# Only use this in Prod Environment (Glue job)
if FLAG_PROD:
    job.commit()
