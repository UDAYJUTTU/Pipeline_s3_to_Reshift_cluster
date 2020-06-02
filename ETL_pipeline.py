from pyspark.sql import SQLContext
from pyspark import SparkConf,SparkContext
from pyspark.sql.functions import split ,monotonically_increasing_id
from pyspark.sql.types import StringType,StructField,StructType,DoubleType,IntegerType
import pandas as pd
import boto3 
s3=boto3.client('s3')

con=SparkConf().setAppName("master").setMaster('yarn')
sc=SparkContext(conf=con)
spark=SQLContext(sc)

# read from s3
df_log=spark.read.json('s3://mm-music-data/log-data')

# read song_ data_ from s3
song_data_path=[]
for files in s3.list_objects_v2(Bucket='mm-music-data')['Contents']:
    if str(files['Key']).startswith('song_data/') and str(files['Key']).endswith('.json'):
        song_data_path.append('s3://mm-music-data/'+str(files['Key']))

df_data=spark.read.format('json').load(song_data_path)
df_data.registerTempTable("songdata")
spark.sql("select count(*) from songdata").show()

address=split(df_log['location'],',')
df_log=df_log.withColumn('city',address[0])
df_log=df_log.withColumn('state',address[1])
df_log=df_log.drop('location')
df_log.registerTempTable("logdata")

#artist_table
artist_table=df_data.select(['artist_id','artist_name','artist_location','num_songs','song_id','title','year'])
artist_table.createOrReplaceTempView('artisttable')
artist_table_dataframe=artist_table.toPandas()

#song_data_table
song_table=df_log.select(['song','length'])
song_table_dataframe=song_table.toPandas()
song_table_dataframe['song_id']=song_table_dataframe.index

df_song=pd.concat([song_table_dataframe,artist_table_dataframe['artist_id']],axis=1,sort=False)
df_song['length']=df_song['length'].fillna(0)
data_schema = StructType([
                          StructField('song', StringType(), True),
                          StructField('length', DoubleType(), True),
                          StructField('song_id', IntegerType(), False),
                          StructField('artist_id', StringType(), True)])

final_song_table = spark.createDataFrame(df_song, schema=data_schema)

#user_table
user_table=df_log.select(['userid','firstName','lastName','city','state','level'])

# popular_artist_data_table
popular_artist=df_log.select(['userid','state','song','level'])

#saving back to s3
artist_table.coalesce(1).write.format('parquet').option('header',True).mode('overwrite').option('path','s3://mm-music-data/processed-data/artist-table').bucketBy(100,'artist_id').saveAsTable('artisttable')
final_song_table.coalesce(1).write.format('parquet').option('header',True).mode('overwrite').option('path','s3://mm-music-data/processed-data/song-table').bucketBy(100,'artist_id').saveAsTable('songtable')
user_table.coalesce(1).write.format('parquet').option('header',True).mode('overwrite').option('path','s3://mm-music-data/processed-data/user-table').bucketBy(100,'userid').saveAsTable('usertable')
popular_artist.coalesce(1).write.format('parquet').option('header',True).mode('overwrite').option('path','s3://mm-music-data/processed-data/popular-artist-table').bucketBy(100,'userid').saveAsTable('popularartisttable')


