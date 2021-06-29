# S3-parquet-files-row-counter
Count rows in all parquet files using S3 SELECT


## Define bucket and directory
```Python
    bucket_name	= 'my-data-test'
    bucket_prefix='all_files/'
```


## Execute

```python s3_row_counter.py```

## Output

	S3 key:  in/test_1.parquet
	S3 key:  in/test_2.parquet
	S3 key:  in/test_3.parquet
	S3 key:  in/test_b1.parquet
	S3 key:  in/test_b2.parquet
	File line count: 42
	File line count: 23
	File line count: 31
	File line count: 8023
	File line count: 27715
	**Total rows: 35834**
	s3_count.py executed in 1.19 seconds.
	
## Total

**Total rows: 35834**


# Async counter

```python async_counter.py```

	Total: 35834
	async_counter.py executed in 0.23 seconds.
