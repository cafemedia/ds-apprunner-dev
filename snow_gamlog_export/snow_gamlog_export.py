import boto3
import json
import os
import sys
from datetime import datetime
import time
import traceback

from py4j.java_gateway import java_import
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

#
# Snowflake warehouse 
#
SNOW_WH = 'prod_s3_to_snowflake_wh'

#
# Snowflake database
#
SNOW_DB =  'RAW'

#
# Gamlog stage path. Where snowflake unloads data
#
GAMLOG_STAGE_PATH = 's3://cm-dps-unload-233585098807/GAM360/GAMLog'

#
# Athena Gamlog unload path. Final destination for gamlog data
#
ATHENA_UNLOAD_PATH = 's3://gamdtf-18190176/partitioned/gamlog'

#
# MAX_FILE_SIZE is the file size of unloaded snowflake parquet
#
MAX_FILE_SIZE = 150 * 1024 * 1024

#
# Argument parsing
#
args = getResolvedOptions(sys.argv,[])

#
# Glue Job Run: Use for  PROCESSOR_JOB_ID
#
JOB_RUN_ID = args['JOB_RUN_ID']

#
# Java path to Snowflake connector
#
SNOW_SOURCE_NAME = "net.snowflake.spark.snowflake"

#
# Basic Spark setup
#
sc = SparkContext()
sc.setLogLevel('INFO')
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()

#
# Config: Overwrite whole partition, not whole path
#
spark.conf.set('spark.sql.sources.partitionOverwriteMode','dynamic')

# 
# Spark And Snowflake synchrony
#
java_import(spark._jvm, SNOW_SOURCE_NAME)
spark._jvm.net.snowflake.spark.snowflake.SnowflakeConnectorUtils. \
    enablePushdownSession(
        spark._jvm.org.apache.spark.sql.SparkSession.builder().getOrCreate()
    )
#
# Function to run DDL/DML queries
#
runQuery = spark.sparkContext._jvm.net.snowflake.spark.snowflake.Utils.runQuery

#
# SSM snowflake credentials
#
params = json.loads(
    boto3 \
        .client('ssm', region_name='us-east-1') \
        .get_parameter(
            Name='prod.s3-snowflake-mirror.snowflake-connection', 
            WithDecryption=True
        )['Parameter']['Value']
)

# Snowflake options with credentials
sfOptions = {
    "sfURL":        f"{params['account']}.snowflakecomputing.com",
    "sfAccount":    params['account'],
    "sfUser":       params['user'],
    "sfPassword":   params['password'],
    "sfDatabase":   SNOW_DB,
    "sfSchema":    'gam360',
    "sfWarehouse": SNOW_WH
}

#
# Job log Started query.
#
# Claim jobs by finding those that have no start time
# and row count is greater than ROW_COUNT
#
ROW_COUNT = 10
JOB_LOG_STARTED_QUERY = f"""
    UPDATE EXPORT_JOB_LOG SET
        PROCESS_STARTED_AT = CURRENT_TIMESTAMP(),
        PROCESSOR_JOB_ID = '{JOB_RUN_ID}',
        STATUS='Started'
    WHERE 
        ROW_COUNT > {ROW_COUNT}
        AND
        PROCESS_STARTED_AT IS NULL 
        AND
        PROCESSOR_JOB_ID IS NULL
"""

#
# Job log query. Grab distinct date and hour pairs for our job id
#
JOB_LOG_QUERY = f"""
    SELECT DISTINCT
        data_date,
        data_hour
    FROM EXPORT_JOB_LOG
    WHERE
        PROCESSOR_JOB_ID = '{JOB_RUN_ID}'
    ORDER BY
        DATA_DATE,
        DATA_HOUR
"""

#
# Job Log Running query. Current date and hour
#
JOB_LOG_RUNNING_QUERY = """
    UPDATE EXPORT_JOB_LOG SET 
        PROCESS_STARTED_AT = CURRENT_TIMESTAMP(),
        STATUS='Running'
    WHERE
        DATA_DATE = '{data_date}'
        AND
        DATA_HOUR = {data_hour}
        AND
        PROCESSOR_JOB_ID = '{JOB_RUN_ID}'
        AND NOT EXISTS (
          SELECT 1 FROM EXPORT_JOB_LOG
          WHERE
            DATA_DATE = '{data_date}'
            AND
            DATA_HOUR = {data_hour}
            AND
            PROCESS_STARTED_AT IS NOT NULL
            AND
            PROCESS_COMPLETED_AT IS NULL
            AND
            STATUS = 'Running'
        )
"""

#
# Job Log Done query. Current date and hour
#
JOB_LOG_COMPLETED_QUERY = """
    UPDATE EXPORT_JOB_LOG SET
        PROCESS_COMPLETED_AT = CURRENT_TIMESTAMP(),
        STATUS='Done'
    WHERE
        DATA_DATE = '{data_date}'
        AND
        DATA_HOUR = {data_hour}
        AND
        PROCESSOR_JOB_ID = '{JOB_RUN_ID}'
"""

#
# Job log Concurrency check query. See if we actually
# got to set some rows to Running.
#
JOB_LOG_CONCURRENCY_CHECK_QUERY = """
    SELECT 
      1
    FROM EXPORT_JOB_LOG
    WHERE
        DATA_DATE = '{data_date}'
        AND
        DATA_HOUR = {data_hour}
        AND
        PROCESSOR_JOB_ID = '{JOB_RUN_ID}'
        AND
        STATUS = 'Running'
"""

#
# Job Log Backout query. Current date and hour and job run id
#
JOB_LOG_BACKOUT_QUERY = """
    UPDATE EXPORT_JOB_LOG SET 
        PROCESS_STARTED_AT = NULL,
        PROCESSOR_JOB_ID = NULL,
        STATUS = NULL
    WHERE
        DATA_DATE = '{data_date}'
        AND
        DATA_HOUR = {data_hour}
        AND
        PROCESSOR_JOB_ID = '{JOB_RUN_ID}'
"""

#
# Snowflake Gamlog query
#
SNOW_GAMLOG_UNLOAD_QUERY = """
  COPY INTO @GAM360.GAMLOG FROM (
    SELECT
      TO_CHAR(TIME,'YYYY-MM-dd-HH24:MI:SS') as TIME,
      TIMEUSEC2,
      KEYPART,
      CTMAP,
      ISFILLEDREQUEST,
      REQUESTEDADUNITSIZES,
      MOBILEDEVICE,
      OSVERSION,
      MOBILECAPABILITY,
      MOBILECARRIER,
      SERVINGRESTRICTION,
      PUBLISHERPROVIDEDID,
      ISCOMPANION,
      VIDEOPOSITION,
      PODPOSITION,
      DEVICECATEGORY,
      ISINTERSTITIAL,
      USERID,
      REFERERURL,
      REQUESTLANGUAGE,
      ADUNITID,
      TOPADUNITID,
      TOPADUNITNAME,
      ADUNITNAME,
      COUNTRY,
      REGION,
      BROWSER,
      OS,
      METRO,
      POSTALCODE,
      BANDWIDTH,
      GFPCONTENTID,
      ADVERTISERID,
      CREATIVESIZE,
      CREATIVEID,
      LINEITEMID,
      LINEITEMNAME,
      ORDERID,
      ORDERNAME,
      CREATIVESIZEDELIVERED,
      CREATIVEVERSION,
      PRODUCT,
      TARGETEDCUSTOMCRITERIA,
      DEALID,
      DEALTYPE,
      ESTIMATEDBACKFILLREVENUE,
      IMPRESSIONID,
      YIELDGROUPCOMPANYID,
      YIELDGROUPNAMES,
      ADVERTISER,
      BUYER,
      ADXACCOUNTID,
      VIDEOFALLBACKPOSITION,
      MEASURABLE,
      VIEWABLE,
      CODESERVES,
      REVENUE,
      IMPRESSIONS,
      PVK,
      SESS,
      SITEID,
      BUCKET,
      AMP,
      ABGROUP,
      LAZY,
      DEPLOYMENT,
      REFRESH,
      PLUGIN,
      FLAG,
      HBHO,
      VPWXVPH,
      VBHO,
      FPV,
      VPRENUM,
      NREF,
      DOC_REF,
      SITE_CODE,
      BRANCH,
      UTM_CAMPAIGN,
      UTM_MEDIUM,
      UTM_SOURCE,
      AT_CUSTOM_1,
      HB_PB,
      HB_BIDDER,
      CHILDNETWORKCODE,
      SELLERRESERVEPRICE,
      AUDIENCESEGMENTIDS,
      USERIDENTIFIERSTATUS,
      PPIDPRESENCE,
      VASTERROR,
      VASTERRORNAMES,
      yieldcompanynames,
      nativeformat,
      nativestyle,
      click,
      verticals,
      flag_slot,
      backfillkeypart,
      protectedaudienceapidelivery,
      hb_dsp,
      hb_crid
    from gamlog 
    where data_date = '{data_date}' and data_hour={data_hour}
  )
  PARTITION BY ('date={data_date}/hour={data_hour}/job_run_id={JOB_RUN_ID}')
  FILE_FORMAT = (TYPE = PARQUET)
  HEADER = true
  MAX_FILE_SIZE = {MAX_FILE_SIZE}
"""

#
# Spark SQL transform to Athena gamlog
#
SPARK_TRANSFORM_QUERY = """
  SELECT
    TIME                                     as Time,
    CAST(TIMEUSEC2 as bigint)                as TimeUsec2,
    KEYPART                                  as KeyPart,
    from_json(CTMAP, 'map<string, array<string>>')   as ctmap,          
    ISFILLEDREQUEST                          as IsFilledRequest,
    from_json(REQUESTEDADUNITSIZES, 'array<string>') as requestedadunitsizes,
    MOBILEDEVICE                           as MobileDevice,
    OSVERSION                              as OSVersion,
    MOBILECAPABILITY                       as MobileCapability,
    MOBILECARRIER                          as MobileCarrier,
    SERVINGRESTRICTION                     as ServingRestriction,
    PUBLISHERPROVIDEDID                    as PublisherProvidedID,
    ISCOMPANION                            as IsCompanion,
    CAST(VIDEOPOSITION as integer)         as VideoPosition,
    CAST(PODPOSITION as integer)           as PodPosition,
    DEVICECATEGORY                         as DeviceCategory,
    ISINTERSTITIAL                         as IsInterstitial,
    USERID                                 as UserId,
    REFERERURL                             as RefererURL,
    REQUESTLANGUAGE                        as RequestLanguage,
    cast(ADUNITID as bigint)               as AdUnitId,
    cast(TOPADUNITID as bigint)            as TopAdUnitId,
    TOPADUNITNAME                          as TopAdUnitName,
    ADUNITNAME                             as AdUnitName,
    COUNTRY                                as Country,
    REGION                                 as Region,
    BROWSER                                as Browser,
    OS,
    METRO                                  as Metro,
    POSTALCODE                             as PostalCode,
    BANDWIDTH                              as BandWidth,
    cast(GFPCONTENTID as integer)          as GfpContentId,
    cast(ADVERTISERID as bigint)           as AdvertiserId,
    CREATIVESIZE                           as CreativeSize,
    cast(CREATIVEID as bigint)             as CreativeId,
    cast(LINEITEMID as bigint)             as LineItemId,
    LINEITEMNAME                           as LineItemname,
    cast(ORDERID as bigint)                as OrderId,
    ORDERNAME                              as ordername,
    CREATIVESIZEDELIVERED                  as CreativeSizeDelivered,
    cast(CREATIVEVERSION as integer)       as CreativeVersion,
    PRODUCT                                as Product,
    TARGETEDCUSTOMCRITERIA                 as TargetedCustomCriteria,
    cast(DEALID as bigint)                 as DealId,
    DEALTYPE                               as DealType,
    ESTIMATEDBACKFILLREVENUE               as EstimatedBackfillRevenue,
    IMPRESSIONID                           as ImpressionId,
    cast(YIELDGROUPCOMPANYID as bigint)    as YieldGroupCompanyId,
    YIELDGROUPNAMES                        as YieldGroupNames,
    ADVERTISER                             as Advertiser,
    BUYER                                  as Buyer,
    cast(ADXACCOUNTID as integer)          as AdxAccountId,
    cast(VIDEOFALLBACKPOSITION as integer) as VideoFallbackPosition,
    cast(MEASURABLE as integer)            as measurable,
    cast(VIEWABLE as integer)              as viewable,
    cast(CODESERVES as integer)            as codeserves,
    cast(REVENUE as double)                as revenue,
    cast(IMPRESSIONS as integer)           as impressions,
    PVK                                    as pvk,
    SESS                                   as sess,
    SITEID                                 as siteid,
    BUCKET                                 as bucket,
    from_json(AMP, 'array<string>')        as amp,
    from_json(ABGROUP, 'array<string>')    as abgroup,
    from_json(LAZY, 'array<string>')       as lazy,
    from_json(DEPLOYMENT, 'array<string>') as deployment,
    from_json(REFRESH, 'array<string>')    as refresh,
    from_json(PLUGIN, 'array<string>')     as plugin,
    from_json(FLAG, 'array<string>')       as flag,
    from_json(HBHO, 'array<string>')       as hbho,
    from_json(VPWXVPH, 'array<string>')    as vpwxvph,
    from_json(VBHO, 'array<string>')       as vbho,
    from_json(FPV, 'array<string>')        as fpv,
    from_json(VPRENUM, 'array<string>')    as vprenum,
    from_json(NREF, 'array<string>')       as nref,
    from_json(DOC_REF, 'array<string>')    as doc_ref,
    from_json(SITE_CODE, 'array<string>')  as site_code,
    from_json(BRANCH, 'array<string>')     as branch,
    from_json(UTM_CAMPAIGN, 'array<string>') as utm_campaign,
    from_json(UTM_MEDIUM, 'array<string>')   as utm_medium,
    from_json(UTM_SOURCE, 'array<string>')   as utm_source,
    from_json(AT_CUSTOM_1, 'array<string>')  as at_custom_1,
    from_json(HB_PB, 'array<string>')        as hb_pb,            
    from_json(HB_BIDDER, 'array<string>')    as hb_bidder,
    CHILDNETWORKCODE,
    SELLERRESERVEPRICE,
    from_json(AUDIENCESEGMENTIDS, 'array<string>') as AUDIENCESEGMENTIDS,
    USERIDENTIFIERSTATUS,
    PPIDPRESENCE,
    VASTERROR,
    from_json(VASTERRORNAMES, 'array<string>') as VASTERRORNAMES,
    yieldcompanynames,
    nativeformat,
    nativestyle,
    cast(click as integer) as click,
    from_json(verticals, 'array<string>')    as verticals,
    from_json(flag_slot, 'array<string>')    as flag_slot,
    BACKFILLKEYPART,
    PROTECTEDAUDIENCEAPIDELIVERY,
    from_json(HB_DSP, 'array<string>')    as hb_dsp,
    from_json(HB_CRID, 'array<string>')    as hb_crid
  FROM 
    gamlog_unloaded
"""

# Claim our jobs
runQuery(sfOptions, JOB_LOG_STARTED_QUERY)

# Grab new jobs to proces
job_log = spark.read.format(SNOW_SOURCE_NAME) \
            .options(**sfOptions).option("query", JOB_LOG_QUERY) \
            .load().collect()

# Early exit if nothing to do
if len(job_log) == 0:
  logger.info("No jobs to perform")
  os._exit(0)

for job in job_log:

    data_date = job['DATA_DATE']
    data_hour = job['DATA_HOUR']

    # Update job log with current job info
    runQuery(
      sfOptions,
      JOB_LOG_RUNNING_QUERY.format(
        JOB_RUN_ID = JOB_RUN_ID,
        data_date = data_date,
        data_hour = data_hour
      )
    )

    # Concurrenty check
    sql = JOB_LOG_CONCURRENCY_CHECK_QUERY.format(
      JOB_RUN_ID = JOB_RUN_ID,
      data_date = data_date,
      data_hour = data_hour
    )
    concurrent_jobs = spark.read.format(SNOW_SOURCE_NAME) \
      .options(**sfOptions).option("query", sql) \
      .load().collect()
    if len(concurrent_jobs) == 0:
      runQuery(
        sfOptions,
        JOB_LOG_BACKOUT_QUERY.format(
          JOB_RUN_ID = JOB_RUN_ID,
          data_date = data_date,
          data_hour = data_hour
        )
      )
      continue

    logger.info(f"Exporting {data_date} {data_hour}")

    # Clear staging directory
    runQuery(
      sfOptions,
      f'REMOVE @GAM360.GAMLOG/date={data_date}/hour={data_hour}/job_run_id={JOB_RUN_ID}'
    )
    
    # delay execution to allow S3 changes to propagate
    time.sleep(5)

    # Populate stage
    runQuery(
      sfOptions, 
      SNOW_GAMLOG_UNLOAD_QUERY.format(
        JOB_RUN_ID = JOB_RUN_ID,
        data_date = data_date,
        data_hour = data_hour,
        MAX_FILE_SIZE = MAX_FILE_SIZE
      )
    )
    
    # delay execution to allow S3 changes to propagate
    time.sleep(5)

    try:
      # Create spark df from gamlog stage data
      gamlog = spark.read.parquet(f'{GAMLOG_STAGE_PATH}/date={data_date}/hour={data_hour}/job_run_id={JOB_RUN_ID}')

      # Set up spark view
      gamlog.createOrReplaceTempView('gamlog_unloaded')

      ########### TEMP TEST CODE 4/1/2024 to 4/3/2024 KS ################

      # Define the path where you want to save the file
      output_path = "s3://cmds-copy/lambda_logs/ken-code-test/glue_snow_test/"

      try:
          # Convert data_date and data_hour to datetime object for comparison
          job_datetime = datetime.strptime(f'{data_date} {data_hour}', '%Y-%m-%d %H')
    
          # Define the target date and time range
          target_date = datetime.strptime('2024-04-08', '%Y-%m-%d')
          target_start_time = datetime.strptime('20:00', '%H:%M').time()
          target_end_time = datetime.strptime('23:00', '%H:%M').time()
    
          # Check if the job_datetime falls within the target date and time range
          logger.info(f"Current job_datetime {job_datetime}")
          if job_datetime.date() == target_date.date() and target_start_time <= job_datetime.time() <= target_end_time:
              logger.info('Job in target range to output parquet file to temp test location (KS)')
              # Save the DataFrame to a Parquet file
              gamlog.write.mode('overwrite').parquet(f'{output_path}/date={data_date}/hour={data_hour}')
              logger.info('Parquet saved')
      except:
          logger.info('Error in KS process:')
          logger.error(traceback.format_exc())
    
      ########### TEMP TEST CODE 4/1/2024 to 4/3/2024 KS ################

      # Execute Transform
      transformed_gamlog = spark.sql(SPARK_TRANSFORM_QUERY)


      # Save to final location
      transformed_gamlog \
          .write \
          .mode("overwrite") \
          .parquet(f'{ATHENA_UNLOAD_PATH}/date={data_date}/hour={data_hour}')

      runQuery(
        sfOptions,
        JOB_LOG_COMPLETED_QUERY.format(
          JOB_RUN_ID = JOB_RUN_ID,
          data_date = data_date,
          data_hour = data_hour
        )
      )
    except:
      logger.error(traceback.format_exc())
      
      # Attempt to back out if transform didn't work above
      runQuery(
        sfOptions,
        JOB_LOG_BACKOUT_QUERY.format(
          JOB_RUN_ID = JOB_RUN_ID,
          data_date = data_date,
          data_hour = data_hour
        )
      )

