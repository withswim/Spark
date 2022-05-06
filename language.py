from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.functions import col, explode

S3_DATA_INPUT_PATH = 's3://spark-tutorial-dataset/survey_results_public.csv'
S3_DATA_OUTPUT_PATH = 's3://spark-tutorial-dataset/data-output'

spark = SparkSession.builder.appName('Language').getOrCreate()

df = spark.read.csv(S3_DATA_INPUT_PATH, header=True)
print('# of records {}'.format(df.count()))
df2 = df['ResponseId', 'LanguageHaveWorkedWith', 'LanguageWantToWorkWith']

df3 = df2.withColumn("language_have", func.split(func.trim(func.col("LanguageHaveWorkedWith")), ";"))\
        .withColumn("language_want", func.split(func.trim(func.col("LanguageWantToWorkWith")), ";"))


df_language_have = df3.select(df3.ResponseId,explode(df3.language_have).alias("language_have")).groupby("language_have").count()
df_language_want = df3.select(df3.ResponseId,explode(df3.language_want).alias("language_want")).groupby("language_want").count()

df_language50_have = df_language_have.sort("count", ascending=False).limit(50)
df_language50_want = df_language_want.sort("count", ascending=False).limit(50)


df_language50_have.write.mode('overwrite').csv(S3_DATA_OUTPUT_PATH)
df_language50_want.write.mode('overwrite').csv(S3_DATA_OUTPUT_PATH)

