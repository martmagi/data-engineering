import numpy as np
import pandas as pd
import pdpipe as pdp


def clean(kym_file_path: str, clean_file_path: str):
    df=pd.read_json(f"{kym_file_path}", orient='records')

    df[['status','origin','year']]=np.nan
    for i in range(len(df.details)):
        df.status[i]=df.details[i]['status']
        df.origin[i]=df.details[i]['origin']
        df.year[i]=df.details[i]['year']

    drop_columns=pdp.ColDrop(columns=["last_update_source","category","template_image_url", "ld",
                                      "additional_references", "search_keywords", "meta","details"])
    remove_duplicates=pdp.DropDuplicates(["title", "url"])
    drop_unconfirmed=pdp.ValKeep(values=["confirmed"], columns=["status"])
    pipeline_1=pdp.PdPipeline([drop_columns,remove_duplicates,drop_unconfirmed])
    df_2=pipeline_1(df)
    df_2.to_json(path_or_buf=f"{clean_file_path}", orient='columns')
