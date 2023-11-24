from DataFrameCreation import DFCreation

def dropColumns(df=DFCreation()):
    df_col_drop = df.dropna()
    print(df_col_drop.printSchema)