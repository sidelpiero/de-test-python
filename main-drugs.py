

# Imports
from numpy import outer
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext

import argparse

from pyspark.sql import types as T
from pyspark.sql import functions as F


import datetime
from pyspark.sql import Window
 
#from utils import my_function



def main():
    #Create SparkSessions
    spark = SparkSession.builder \
        .appName('test_DE') \
        .getOrCreate()


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

    pubmed_df.show(truncate=0)

    print(str(pubmed_df.schema))
    pubmed_df_json = spark.read.json("pubmed.json",multiLine=True)
    pubmed_df_json.show(truncate=0)
    pubmed_df_json = pubmed_df_json.select("id","title","date","journal")
    full_pubmed = pubmed_df.union(pubmed_df_json)
    
    

    #uniformize date format
    full_pubmed = full_pubmed.withColumn('date', F.regexp_replace('date', '-', '/'))


    ########## convert column to upper case in pyspark
   ########## join pubmed titles with drugs dataframe

    full_pubmed_upper = full_pubmed.select("*", F.upper(F.col('title')).alias('upper_title'))
    drug_pubmed = drugs_df.join(full_pubmed_upper, full_pubmed_upper.upper_title.contains(drugs_df.drug), how='inner')
    drug_pubmed_distinct = drug_pubmed.select("drug","upper_title", "date").distinct()
    drug_pubmed_distinct.show()

    clinical_trials_df_upper = clinical_trials_df.select("*", F.upper(F.col('scientific_title')).alias('upper_title'))
    drug_clinical_trials     = drugs_df.join(clinical_trials_df_upper, \
                                             clinical_trials_df_upper.upper_title.contains(drugs_df.drug), how='inner')

    drug_clinical_trials_distinct = drug_clinical_trials.select("drug","upper_title", "date").distinct()
    drug_clinical_trials_distinct.show()

    drug_journal_trials      = drug_clinical_trials.select("drug","journal","date").distinct()
    drug_journal_publication = drug_pubmed.select("drug","journal","date").distinct()
    drug_journal             = drug_journal_publication.unionAll(drug_journal_trials)

    drug_journal.show()

    #########################
    # convert journals data to JSON format

    drug_journal_agg = drug_journal.select("drug",F.to_json(F.struct("journal","date")).alias("journals"))
    drug_journal_agg = drug_journal_agg.groupBy("drug").agg(F.collect_list("journals").alias("journals"))


    #########################
    # convert clinical_trials data to JSON format

    drug_clinical_trials_agg = drug_clinical_trials_distinct.select("drug",F.to_json(F.struct("upper_title","date")).alias("titles"))
    drug_clinical_trials_agg = drug_clinical_trials_agg.groupBy("drug").agg(F.collect_list("titles").alias("clinical_trials"))

    #########################
    # convert pubmed data to JSON format

    drug_pubmed_agg = drug_pubmed_distinct.select("drug",F.to_json(F.struct("upper_title","date")).alias("titles"))
    drug_pubmed_agg = drug_pubmed_agg.groupBy("drug").agg(F.collect_list("titles").alias("pubmed"))

    # last dataframe with all pubmed, journal and clinical trials suited info
    drugs_final = drugs_df.join(drug_journal_agg, "drug", "left")
    drugs_final = drugs_final.join(drug_clinical_trials_agg, "drug", "left")
    drugs_final = drugs_final.join(drug_pubmed_agg, "drug", "left")

    drugs_final.toPandas().to_json('result.json', orient='records', force_ascii=False, lines=True)


if __name__ == '__main__':
    
    main()


