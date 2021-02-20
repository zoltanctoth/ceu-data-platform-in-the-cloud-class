  CREATE TABLE zoltanctoth.silver_edits
    WITH (
          format = 'PARQUET',
          parquet_compression = 'SNAPPY',
          external_location = 's3://zoltan-sandbox/de4/silver_edits'
    ) AS SELECT * FROM zoltanctoth.bronze_edits
