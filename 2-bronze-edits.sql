-- ADD THE ATHENA SQL SCRIPT HERE WHICH CREATES THE `bronze_edits` TABLE
CREATE EXTERNAL TABLE
zoltanctoth.bronze_edits (
    title STRING,
    edits INT,
    date DATE,
    retrieved_at TIMESTAMP) 
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://{{your bucket}}/de4/edits/';
