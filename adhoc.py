# Some Imports
from numpy import outer
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext

import argparse

from pyspark.sql import types as T
from pyspark.sql import functions as F





def main():


    #Create SparkSessions
    spark = SparkSession.builder \
        .appName('test_DE') \
        .getOrCreate()

    #Read Data
    drugs_df = spark.read.option("header",True)\
                            .csv("drugs.csv")

    drugs_df.show(truncate=0)

    clinical_trials_df = spark.read.option("header",True)\
                            .csv("clinical_trials.csv")

    clinical_trials_df.show(truncate=0)

    #on aggrége les données partielles / incompletes / ou identiques
    clinical_trials_df = clinical_trials_df.groupBy('scientific_title','date')\
                                           .agg(F.max('id').alias('id'), F.max('journal').alias('journal'))
   
    clinical_trials_df.show(truncate=0)
    
    pubmed_df = spark.read.option("header",True)\
                            .csv("pubmed.csv")

    pubmed_df_json = spark.read.json("pubmed.json",multiLine=True)
    pubmed_df_json.show(truncate=0)
    pubmed_df_json = pubmed_df_json.select("id","title","date","journal")
    full_pubmed = pubmed_df.union(pubmed_df_json)
    
    
    full_pubmed.sort(F.col("id")).show(truncate=False)
    
    full_pubmed = full_pubmed.withColumn('date', F.regexp_replace('date', '-', '/'))


    full_pubmed = full_pubmed.withColumn('mots', F.explode(F.split(F.col('title'), ' '))) \
    .select(full_pubmed.journal,full_pubmed.date,full_pubmed.title,F.col("mots"))\
    .groupBy('mots','journal') \
    .count() \
    .sort('count', ascending=False)

    ########## convert column to upper case in pyspark
 
    full_pubmed = full_pubmed.select("*", F.upper(F.col('mots')).alias('drug')).drop("mots")
    
    ########## filter all non drugs and aggregate on journals with drugs count

    journals_drug_count = drugs_df.join(full_pubmed, "drug", "inner")\
                          .groupBy('journal').agg(F.sum('count').alias('count'))\
                          .sort((F.col("count").desc()))
    journals_drug_count.show()
    print("Journal with most cited drugs : " + journals_drug_count.first().journal + " has "+  journals_drug_count.first().count )

if __name__ == '__main__':
    
    main()

