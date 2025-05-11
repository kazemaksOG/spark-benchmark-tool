import pyarrow.dataset as ds

dataset = ds.dataset("../resources/tripdata-partitionBy-PULocationID.parquet", format="parquet")
num_row_groups = sum(fragment.num_row_groups for fragment in dataset.get_fragments())

print(f"Number of row groups: {num_row_groups}")
