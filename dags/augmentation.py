import pandas as pd
import numpy as np
import json
import pdpipe as pdp


def augment(clean_file_path: str, vision_file_path: str, augmented_file_path: str):
    df=pd.read_json(f"{clean_file_path}")
    df_vision=pd.read_json(f"{vision_file_path}")

    df_cols=np.array(df.url.values.tolist()).flatten()
    vis_cols=df_vision.columns.values
    remove_list=np.setdiff1d(vis_cols,df_cols)
    df_vision=pdp.ColDrop(columns =remove_list).apply(df_vision)

    columns=['google_api_description','adult','spoof','medical','violence','racy' ]
    for i in columns:
        df[i]=''
        df[i]=df[i].astype(str)

    for i in df.url.values.flatten():
        try:
            if i in ['https://knowyourmeme.com/memes/dorito-pope', "https://knowyourmeme.com/memes/subcultures/mameshiba","https://knowyourmeme.com/memes/x-doesnt-change-facial-expressions", 'https://knowyourmeme.com/memes/liberal-douche-garofalo', 'https://knowyourmeme.com/memes/polybius']:
                continue
            y=np.where(df.url ==i)[0][0]+1
            if df_vision[df.loc[y].url].safeSearchAnnotation==float or type(df_vision[df.loc[y].url].safeSearchAnnotation)==float:
                continue
            annotation = df_vision[i].safeSearchAnnotation
            if pd.isnull(annotation):
                continue
            if (df_vision[i].safeSearchAnnotation or df_vision[i])==KeyError :
                continue
            df.at[y, 'adult']=df_vision[i].safeSearchAnnotation['adult']
            df.at[y, 'spoof']=str(df_vision[i].safeSearchAnnotation['spoof'])
            df.at[y, 'medical']=str(df_vision[i].safeSearchAnnotation['medical'])
            df.at[y, 'violence']=str(df_vision[i].safeSearchAnnotation['violence'])
            df.at[y, 'racy']=str(df_vision[i].safeSearchAnnotation['racy'])
        except KeyError:
            continue

    x=np.arange(len(df), dtype=object)
    aa=np.full_like(x, np.nan, dtype='object')
    g_list=[[] for x in aa]
    g_list[0]=str
    for i in df.url.values.flatten():
        try:
            if i in ['https://knowyourmeme.com/memes/dorito-pope', "https://knowyourmeme.com/memes/subcultures/mameshiba",
                     "https://knowyourmeme.com/memes/x-doesnt-change-facial-expressions",
                     'https://knowyourmeme.com/memes/liberal-douche-garofalo', 'https://knowyourmeme.com/memes/polybius']:
                continue
            y=np.where(df.url ==i)[0][0]+1
            print(y)
            if df_vision[df.loc[y].url].labelAnnotations==float or type(df_vision[df.loc[y].url].labelAnnotations)==float:
                continue
            if (df_vision[i].labelAnnotations or df_vision[i])==KeyError:
                continue
            if type(df_vision[i].labelAnnotations) == float:
                continue
            if len(df_vision[i].labelAnnotations) >=10:
                for j in range(10):
                    g_list[y].append(df_vision[i].labelAnnotations[j]['description'])
            else:
                for j in range(len(df_vision[i].labelAnnotations)):
                    g_list[y].append(df_vision[i].labelAnnotations[j]['description'])
        except KeyError:
            continue

    df['google_api_description']=g_list
    df['google_api_description']=df.google_api_description.shift(-1)

    a=df.columns.values.tolist()
    b=df.values.tolist()
    b.insert(0,a)
    with open(f"{augmented_file_path}", 'w', encoding='utf-8') as f:
        json.dump(b, f, ensure_ascii=True)
