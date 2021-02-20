## SUBJECT DATE
DATE_PARAM="2021-01-20"

date <- as.Date(DATE_PARAM, "%Y-%m-%d")

#> install.packages('httr', 'jsonlite', 'lubridate')
library(httr)
library(aws.s3)
library(jsonlite)
library(lubridate)

# See https://wikimedia.org/api/rest_v1/#/Edited%20pages%20data/get_metrics_edited_pages_top_by_edits__project___editor_type___page_type___year___month___day_
url <- paste(
  "https://wikimedia.org/api/rest_v1/metrics/edited-pages/top-by-edits/en.wikipedia/user/content/",
  format(date, "%Y/%m/%d"), sep='')

wiki.server.response = GET(url)
wiki.response.status = status_code(wiki.server.response)
wiki.response.body = content(wiki.server.response, 'text')

if (wiki.response.status != 200){
  print(paste("Recieved non-OK status code from Wiki Server: ",
             wiki.response.status,
             '. Response body: ',
             wiki.response.body, sep=''
             ))
}

# Save Raw Response and upload to S3
RAW_LOCATION_BASE='data/raw-edits'
dir.create(file.path(RAW_LOCATION_BASE), showWarnings = FALSE)

########
# LAB  #
########
#
# Save `wiki.response.body` to the local filesystem into the folder
# `RAW_LOCATION_BASE` under the name `raw-edits-YYYY-MM-DD.txt`,
# i.e: `raw-edits-2021-01-23.txt`.

#### ANSWER ####
raw.output.filename = paste("raw-edits-", format(date, "%Y-%m-%d"), '.txt',
                            sep='')
raw.output.fullpath = paste(RAW_LOCATION_BASE, '/', 
                            raw.output.filename, sep='')
write(wiki.response.body, raw.output.fullpath)

########
# LAB  #
########
#
# Upload the file you created to S3.
#
# Upload it to your bucket, place it under the folder called `de4/raw/` 
# The object name should be the same as your filename: `raw-edits-YYYY-MM-DD.txt`
# After you've uploaded it, make sure it's there
# by taking a look at the AWS Web Console

#> BUCKET="zoltan-sandbox"

## FILL IN AWS SETUP STEPS
#> put_object(file = "{{ ADD LOCAL FILE PATH }}",
#>            object = "{{ ADD FOLDER AND FILE NAME HERE in a form of FOLDER/FILE_NAME }}",
#>            bucket = BUCKET,
#>            verbose = TRUE)

#### ANSWER ####
keyTable <- read.csv("accessKeys.csv", header = T) # accessKeys.csv == the CSV downloaded from AWS containing your Acces & Secret keys
AWS_ACCESS_KEY_ID <- as.character(keyTable$Access.key.ID)
AWS_SECRET_ACCESS_KEY <- as.character(keyTable$Secret.access.key)

Sys.setenv("AWS_ACCESS_KEY_ID" = AWS_ACCESS_KEY_ID,
           "AWS_SECRET_ACCESS_KEY" = AWS_SECRET_ACCESS_KEY,
           "AWS_DEFAULT_REGION" = "eu-west-1") 

BUCKET_PREFIX="zoltan-sandbox" # Change this to your own

put_object(file = raw.output.fullpath,
           object = paste('de4/raw/', 
                                  raw.output.filename,
                                  sep = ""),
           bucket = BUCKET_PREFIX,
           verbose = TRUE)


# Parse the response and write the parsed string to "Bronze"

# We are extracting the top edits from the server's response
wiki.response.parsed = content(wiki.server.response, 'parsed')
top.edits = wiki.response.parsed$items[[1]]$results[[1]]$top

# Convert the server's response to JSON lines
current.time = Sys.time() 
json.lines = ""
for (page in top.edits){
  record = list(
    title = page$page_title[[1]],
    edits = page$edits,
    date = format(date, "%Y-%m-%d"),
    retrieved_at = current.time
    )

  json.lines = paste(json.lines,
                     toJSON(record,
                            auto_unbox=TRUE),
                     "\n",
                     sep='')
}

# Save the Top Edits JSON lines as a file and upload it to S3

JSON_LOCATION_BASE='data/edits'
dir.create(file.path(JSON_LOCATION_BASE), showWarnings = FALSE)

json.lines.filename = paste("edits-", format(date, "%Y-%m-%d"), '.json',
                            sep='')
json.lines.fullpath = paste(JSON_LOCATION_BASE, '/', 
                            json.lines.filename, sep='')

write(json.lines, file = json.lines.fullpath)

put_object(file = json.lines.fullpath,
           object = paste('de4/edits/', 
                          json.lines.filename,
                          sep = ""),
           bucket = BUCKET,
           verbose = TRUE)

