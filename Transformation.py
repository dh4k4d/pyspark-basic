from DataFrameCreation import DFCreation
from pyspark.sql.functions import concat_ws

def dropColumns(df=DFCreation()):
    # df_col_na_drop = df.dropna()
    # Dropped some columns
    df_col_drop = df.select('npi', 'nppes_provider_last_org_name',
    'nppes_provider_first_name', 'nppes_provider_city',
    'specialty_description', 'drug_name', 'generic_name',
    'total_claim_count', 'total_day_supply', 'total_drug_cost', 'years_of_exp')

    # Columns name updated
    df_col_alias = df_col_drop.select("npi",concat_ws(" ", df_col_drop.nppes_provider_first_name, df_col_drop.nppes_provider_last_org_name).alias("fullName"),
    (df_col_drop.nppes_provider_city).alias("City"),
    (df_col_drop.specialty_description).alias("Specialty_Desc"), 'drug_name', 'generic_name',
    (df_col_drop.total_claim_count).alias("Claim_Count"), (df_col_drop.total_day_supply).alias("Day_Supply"),
    (df_col_drop.total_drug_cost).alias("Drug_Cost"), (df_col_drop.years_of_exp).alias("Year_Exp"))
    
    # DataType Casting
    df_col_alias = df_col_alias.withColumn("Drug_Cost", df_col_alias.Drug_Cost.cast("int"))
    df_col_alias = df_col_alias.withColumn("npi",df_col_alias.npi.cast("int"))
    df_col_alias = df_col_alias.withColumn("Claim_Count", df_col_alias.Claim_Count.cast("int"))
    df_col_alias = df_col_alias.withColumn("Day_Supply", df_col_alias.Day_Supply.cast("int"))
    df_col_alias = df_col_alias.withColumn("Year_Exp", df_col_alias.Year_Exp.cast("int"))
    df_col_alias.printSchema()
    # Print Result
    df_col_alias.sort(df_col_alias.Year_Exp, ascending=False).show(10)
    