library(dotenv)
library(tidyverse)
library(RJDBC)
library(glue)
library(fs)
library(janitor)
library(data.table)
library(splitstackshape)

yesterday = Sys.Date() - 1

start = format(
  yesterday,
  "%Y-%m-%d 00:00:00"
  )
end = format(
  yesterday, "%Y-%m-%d 23:59:59"
  )

print(start)
print(end)

load_dot_env(
  'C:/QMS/Adeno VMISC/r_env/.env'
  )

jdbcDriver = JDBC(
  driverClass = "oracle.jdbc.OracleDriver",
  classPath = "C:/Oracle/instantclient_21_9/ojdbc8.jar"
  )

con = dbConnect(
  jdbcDriver,
  "jdbc:oracle:thin:@//plsdbscan.mskcc.org:1522/slisp.world",
  Sys.getenv("DB_USER"), 
  Sys.getenv("DB_PASS")
  ) 

query = read_file(
  'C:/QMS/Adeno VMISC/adeno_misc.sql'
  )

query_subbed = glue(
  query,
  .open = '{', .close = '}',
  start_time = start,
  end_time = end
  )

new_data = dbGetQuery(
  con,
  query_subbed
  ) %>% 
  mutate_all(
    as.character
    )

dbDisconnect(con)

numeric_datetime = format(
  yesterday, "%Y_%m_%d"
  )

filtered_df = subset(
  new_data,
  grepl(
    "Adenovirus",
    new_data$`Comment - Result`
    )
  )

file_path = paste0(
  "H:/MicroBiology/Validations and Verifications/Babady_AltoStar_AM16/viracor_adeno_data/raw data/adeno_misc_",
  numeric_datetime,
  ".csv"
  )

write.csv(
  filtered_df,
  file_path,
  row.names = FALSE,
  na = ""
  )

files = dir_ls(
  'H:/MicroBiology/Validations and Verifications/Babady_AltoStar_AM16/viracor_adeno_data/raw data/'
  )

combined_files = files %>% 
  map(
    fread,
    colClasses = "character"
    ) %>% 
  bind_rows() %>% 
  clean_names() %>% 
  distinct()

splitted = cSplit(
  combined_files,
  "comment_result", " _"
  ) %>% 
  select(
    person_name_full,
    alias_person_mrn,
    accession_nbr_formatted,
    order_procedure,
    date_time_verified,
    comment_result_08,
    comment_result_09
    )

colnames(splitted) = c("Name", "MRN", "Acession_Number", "Order", "Date", "Test", "Result")

unlink(
  "H:/MicroBiology/Validations and Verifications/Babady_AltoStar_AM16/viracor_adeno_data/backup/viracor_adeno_results.csv", recursive = FALSE
  )

file.copy(
  "H:/MicroBiology/Validations and Verifications/Babady_AltoStar_AM16/viracor_adeno_data/data/viracor_adeno_results.csv",
  "H:/MicroBiology/Validations and Verifications/Babady_AltoStar_AM16/viracor_adeno_data/backup/viracor_adeno_results.csv",
  overwrite = TRUE
  )

write_csv(
  splitted,
  "H:/MicroBiology/Validations and Verifications/Babady_AltoStar_AM16/viracor_adeno_data/data/viracor_adeno_results.csv",
  na = ""
  )
