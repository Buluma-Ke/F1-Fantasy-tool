from transform import F1FantasyFrameBuilder
import pandas as pd


jsontodf = F1FantasyFrameBuilder('C:/Users/MWITA/Desktop/F1DB/Data/f1fantasydata2024.json')
jsontodf.loadjson()
jsontodf.raceresults()


def TransformDf(df):
    columns = df.columns
    for column in columns:
        if column == 'start_times':
            print('time column found')
            try:
                for row in df['start_times']:

                    df['qualifying'] = pd.to_datetime(df['start_times'].apply(lambda x: x.get('qualifying') if isinstance(x, dict) else None), errors='coerce')

                    df['sprint'] = pd.to_datetime(df['start_times'].apply(lambda x: x.get('sprint') if isinstance(x, dict) else None), errors='coerce')

                    df['race'] = pd.to_datetime(df['start_times'].apply(lambda x: x.get('race') if isinstance(x, dict) else None), errors='coerce')

                df = df.drop(columns=['start_times'])
                print('quali innit!!')
                print(df)
            except Exception as e:
                print(f"Transformation failed for column '{column}': {e}")

TransformDf()



def PointTextColumnDeletion(df):
    columns = df.columns
    textcolumns = []

    for column in columns:
        if column.endswith('Points_text'):
            textcolumns.append(column)

    print('Dropping columns:')
    print(textcolumns)
    df = df.drop(textcolumns, axis=1)
    return df
