from etl import pipeline_transform_kpis

path_file = 'data/'
formats = ['parquet']

pipeline_transform_kpis(
    path_file_to_transform=path_file,
    output_extension=formats,
    n_files=3
    )