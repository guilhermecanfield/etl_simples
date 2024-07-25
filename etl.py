import pandas as pd
import os
import glob

# Extract
def extract_data(
        n_files: int, 
        path: str
        ) -> pd.DataFrame:
    if n_files == 1:
        df = pd.read_json(path)
    else:
        files = glob.glob(
            os.path.join(path, '*.json')
            )
        df_list = [pd.read_json(file) for file in files]
        df = pd.concat(df_list, ignore_index=True)
    return df

# Transform
def calculate_kpi(
        data: pd.DataFrame
        ) -> pd.DataFrame:
    data['Total'] = data['Quantidade'] * data['Venda']
    return data

# Load
def load_final_data(
        data: pd.DataFrame, 
        extension_to_save: str|list, 
        path_to_save: str
        ):
    """
    extension_to_save: aceita apenas os formatos "parquet" e "csv"
    """
    path_to_save = path_to_save.rstrip('/')
    for extension in extension_to_save:
        if extension == "csv":
            data.to_csv(
                f"{path_to_save}/dados.csv", 
                index=False
                )
        if extension == "parquet":
            data.to_parquet(
                f"{path_to_save}/dados.parquet", 
                index=False
                )

# Pipeline
def pipeline_transform_kpis(
        path_file_to_transform: str,
        n_files: int,
        output_extension: str
):
    df = extract_data(
        n_files=n_files, 
        path='data'
        )
    df_transformado = calculate_kpi(data=df)
    load_final_data(
        data=df_transformado, 
        extension_to_save=output_extension, 
        path_to_save=path_file_to_transform
        )
