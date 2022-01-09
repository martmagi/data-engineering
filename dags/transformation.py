import pandas as pd


def transform(augmented_file_path: str, transformed_file_path: str):
    df=pd.read_json(f"{augmented_file_path}", orient='records')
    df.columns=[df.values[0:1:][0][i] for i in range(len(df.values[0:1:][0]))]
    df=df.drop(index=0)

    df['added_datetime']=pd.to_datetime(df['added'], unit='s').dt.date
    df['added_weekday']=pd.to_datetime(df['added'], unit='s').dt.day_name()
    df['added_month']=pd.to_datetime(df['added'], unit='s').dt.month_name()
    df['added_year']=pd.to_datetime(df['added'], unit='s').dt.year
    df['how_long_till_upload']=df.added_year-df.year

    df=df.drop(columns=['added_datetime'], axis=1)

    df.to_json(path_or_buf=f"{transformed_file_path}", orient='records')
