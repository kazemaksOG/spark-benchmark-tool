import pyarrow.parquet as pq

parquet_file = pq.ParquetFile("../resources/test2")
print(f"Number of row groups: {parquet_file.num_row_groups}")
