import pyarrow.dataset as ds

dataset = ds.dataset("../resources/tripdata-partitionBy-PULocationID.parquet", format="parquet")
num_row_groups = 0
total_rows = 0

for fragment in dataset.get_fragments():
    metadata = fragment.metadata
    if metadata is not None:
        num_row_groups += metadata.num_row_groups
        total_rows += metadata.num_rows

print(f"Number of row groups: {num_row_groups}")
print(f"Total number of rows: {total_rows}")
