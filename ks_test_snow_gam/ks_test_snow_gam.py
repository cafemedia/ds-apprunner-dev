import time
import traceback

from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

version = ('orig','str')[0]
filedatetime = 'date=2024-04-08/hour=23/'

###################################################
# Gamlog stage path. Where snowflake unloads data
###################################################
# GAMLOG_STAGE_PATH = 's3://cm-dps-unload-233585098807/GAM360/GAMLog'
GAMLOG_STAGE_PATH = f's3://cmds-copy/lambda_logs/ken-code-test/glue_snow_test/{filedatetime}'

###################################################
# Athena Gamlog unload path. Final destination for gamlog data
###################################################
# ATHENA_UNLOAD_PATH = 's3://gamdtf-18190176/partitioned/gamlog'
ATHENA_UNLOAD_PATH = f's3://cmds-copy/lambda_logs/ken-code-test/glue_snow_test/output_test/{filedatetime}'

###################################################
# Basic Spark setup
###################################################
sc = SparkContext()
sc.setLogLevel('INFO')
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()

###################################################
# Config: Overwrite whole partition, not whole path
###################################################
spark.conf.set('spark.sql.sources.partitionOverwriteMode','dynamic')

###################################################
# Spark SQL transform to Athena gamlog
###################################################
SPARK_TRANSFORM_QUERY_ORIG = """
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

SPARK_TRANSFORM_QUERY_CTMAP_STR = """
  SELECT
    TIME                                     as Time,
    CAST(TIMEUSEC2 as bigint)                as TimeUsec2,
    KEYPART                                  as KeyPart,
    CTMAP                                    as ctmap,          
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
logger.info('Spark ready; beginning read')

try:
    # Create spark df from gamlog stage data
    gamlog = spark.read.parquet(GAMLOG_STAGE_PATH)

    logger.info('Read completed; beginning view creation')
    
    # Set up spark view
    gamlog.createOrReplaceTempView('gamlog_unloaded')

    logger.info('View completed; beginning transform')

    # Execute Transform
    if version == 'orig' : transformed_gamlog = spark.sql(SPARK_TRANSFORM_QUERY_ORIG)
    elif version == 'str' : transformed_gamlog = spark.sql(SPARK_TRANSFORM_QUERY_CTMAP_STR)
    else : raise ValueError('invalid version specified')

    logger.info('Transform complete; beginning write')    
    
    # Save to final location
    transformed_gamlog.write.mode("overwrite").parquet(ATHENA_UNLOAD_PATH)

    logger.info('Write completed; job done')
except:
    logger.error(traceback.format_exc())


########################################################################################
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
