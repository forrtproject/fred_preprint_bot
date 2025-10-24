# This script takes author information from XML files, names and orcids scraped 
# from preprint PDFs and combines all this information. It extracts additional 
# information from OSF and ORCIDs to make authors as identifiable as possible
# and get at least one email per preprint. 
# Input: 
#      * XML files from GROBID (path defined below)
#   OR * RDS named "paper_list.rds" of read in xmls with papercheck (same folder) 
#      * JSON file from "extract_orcids_from_pdfs.py" (same folder)
#      * JSON file from "extract_emails_from_pdfs.py" (same folder)
#
# Output: 
#      * CSV file containing author information: "authorList_ext.csv"
# 
# If save.int is TRUE, it also saves intermediate CSV files 

# load libraries > need to be installed with install.packages("name") first
library(papercheck)
library(tidyverse)
library(osfr)
library(RecordLinkage)
library(httr)
library(jsonlite)
library(stringdist)
library(fuzzyjoin)

# save intermediary CSV files?
save.int = TRUE

# path to GROBID xmls
path = "../data/preprints"

## Extract author information from the XML files > old grobid version, quite some info missing!

# read in all the xml files
if (file.exists("paper_list.rds")) {
  paper.lst = readRDS("paper_list.rds")
} else {
  paper.lst = read(path)
  saveRDS(paper.lst, "paper_list.rds")
}

# convert to table
df = author_table(paper.lst) %>%
  mutate(
    # separate the source and the OSF id
    source = dirname(id),
    id = basename(id),
    # add an NA if there is no email
    email = if_else(email == "", NA, email)
  )

df %>% group_by(id) %>% 
  summarise(checkEmail = sum(!is.na(email)) > 0) %>%
  ungroup() %>%
  summarise(percEmail = mean(checkEmail))

df %>% summarise(percOrcid = mean(!is.na(orcid)), nOrcid = sum(!is.na(orcid)))

if (save.int) {
  write_csv(df, "authorInfo_xml.csv")
}

# [CHECK] at this point, we have at least one email for 66% of the preprints

## Use preprint ID to extract information on author

# retrieve the author OSF ids and ORCID ids

ls.ids = split(df, df$id)
df.authors = data.frame()

osf_auth(token = "vxNKbbZar9alGO83uILU5euAtzuZkhbtB4MULCmB75wMlxQlKXujI5AnK4unJIZr1neS1C")

tictoc::tic()
for (i in 1:length(ls.ids)) {
  if (i%%100 == 0) {
    tictoc::toc()
    print(sprintf("Processed %d of %d preprints", i, length(ls.ids)))
    tictoc::tic()
  }
  # retrieve the contributor information of the preprint
  res = GET(sprintf("https://api.osf.io/v2/preprints/%s/contributors/", 
                    names(ls.ids)[i]))
  data = fromJSON(rawToChar(res$content))
  if (is.null(data$data)) {
    next
  }
  # get the authors' OSF ids and put them into a temporary dataframe
  tmp = data.frame(osf.id = data$data$relationships$users$data$id,
                   osf.name = NA, orcid = NA, id = names(ls.ids)[i])
  # loop through this dataframe and retrieve names and orcids based on OSF id
  for (j in 1:nrow(tmp)) {
    osf.info = osf_retrieve_user(tmp$osf.id[j])
    if (length(osf.info$meta[[1]]$attributes$social) == 0) {
      tmp$orcid[j] = NA
    } else {
      tmp$orcid[j] = check_orcid(osf.info$meta[[1]]$attributes$social$orcid)
    }
    tmp$osf.name[j]  = osf.info$meta[[1]]$attributes$full_name
  }
  # add dataframe to the author dataframe
  df.authors = rbind(df.authors, tmp)
}

# fuzzy join based on names
df.authors = stringdist_join(
     df %>% mutate(osf.name = sprintf("%s %s", name.given, name.surname)) %>% mutate(orcid = if_else(orcid == "FALSE", NA, orcid)), 
     df.authors,
     by = c("id", "osf.name"),  # Columns to match on
     mode = "left",  
     method = "hamming", 
     max_dist = 0.2  # Maximum allowable distance for a match
) %>% mutate(orcid = if_else(orcid == "FALSE", NA, orcid))

# check for preprint ID conflicts
df.authors %>% 
  mutate(match = id.x == id.y) %>%
  filter(!match)

# check for ORCID conflicts > problem with Van Hedger (same surname, same ID)
df.authors %>% 
  mutate(match = orcid.x == orcid.y) %>%
  filter(!match)

# remove columns
df.authors = df.authors %>%
  select(-c(id.y, osf.name.x)) %>%
  rename(id = id.x, orcid.xml = orcid.x, osf.name = osf.name.y,
         orcid.osf = orcid.y)

df.authors %>% summarise(percOrcid = mean(!is.na(orcid.xml) | !is.na(orcid.osf)), 
                         nOrcid = sum(!is.na(orcid.xml) | !is.na(orcid.osf)))

# save this author list
if (save.int) {
  write_csv(df.authors, "authorList_OSFid.csv")
}

# add ORCIDs from PDF scraping 
orcid.json = read_json("orcids_from_pdf.json")

df.orcid = data.frame()
for (i in 1:length(orcid.json)) {
  for (j in 1:length(orcid.json[[i]])) {
    if (orcid.json[[i]][j] != "false") {
      # extract information based on the orcid > only works on real ORCID
      orcid.info = orcid_person(orcid.json[[i]][[j]])
      # CHECK if there was an error > invalid orcid
      if (!is.null(orcid.info$error)) {
        next
      }
      # add this information to the dataframe
      df.orcid = rbind(df.orcid, 
                       data.frame(
                         orcid  = orcid.info$orcid,
                         given  = orcid.info$given,
                         family = orcid.info$family,
                         id = names(orcid.json)[i]
                       ))
    } else {
      next
    }
  }
}

# merge with the author dataframe
df.authors = stringdist_join(
  df.authors, 
  df.orcid %>% rename(orcid.pdf = orcid) %>%
    mutate(osf.name = sprintf("%s %s", given, family)),
  by = c("id", "osf.name"),  # Columns to match on
  mode = "left",  
  method = "hamming", 
  max_dist = 0.2  # Maximum allowable distance for a match
)

# check for preprint ID conflicts
df.authors %>% 
  mutate(match = id.x == id.y) %>%
  filter(!match)

# check for ORCID conflicts
df.authors %>% 
  mutate(match = orcid.pdf == orcid.osf) %>%
  filter(!match)

# remove some columns
df.authors = df.authors %>%
  select(!ends_with(".y")) %>%
  rename(id = id.x, osf.name = osf.name.x,
         name.given.orcid = given, name.surname.orcid = family)

df.authors %>% summarise(percOrcid = mean(!is.na(orcid.xml) | !is.na(orcid.osf) | !is.na(orcid.pdf)), 
                         nOrcid = sum(!is.na(orcid.xml) | !is.na(orcid.osf) | !is.na(orcid.pdf)))

if (save.int) {
  write_csv(df.authors, "authorList_ORCIDpdf.csv")
}

# add orcids based on names - only if perfect match (~45min for 1700)
idx = which(is.na(df.authors$orcid.pdf) & is.na(df.authors$orcid.xml) & 
              is.na(df.authors$orcid.osf) & df.authors$name.given != "")
df.authors$orcid.name = NA
count = 1
tictoc::tic()
for (i in idx) {
  check = get_orcid(gsub("[[:punct:]]", "", df.authors$name.surname[i]), 
                    gsub("[[:punct:]]", "", df.authors$name.given[i]))
  if (length(check) == 1) {
    df.authors$orcid.name[i] = check
  }
  count = count + 1
  if (count%%100 == 0) {
    tictoc::toc()
    print(sprintf("Checked %d of %d authors", count, length(idx)))
    tictoc::tic()
  }
}

df.authors = df.authors %>%
  mutate(
    orcid.name = if_else(orcid.name == "", NA, orcid.name)
  )


df.authors %>% summarise(percOrcid = mean(!is.na(orcid.xml) | !is.na(orcid.osf) | !is.na(orcid.pdf) | !is.na(orcid.name)), 
                         nOrcid = sum(!is.na(orcid.xml) | !is.na(orcid.osf) | !is.na(orcid.pdf) | !is.na(orcid.name)))

if (save.int) { 
  write_csv(df.authors, "authorList_ORCIDname.csv")
}

# get email from orcid numbers
df.authors = df.authors %>% mutate(email.source = if_else(!is.na(email), "xml", NA))

# combine the ORCID numbers to one orcid column
df.authors = df.authors %>% 
  mutate(
    email.source = if_else(!is.na(email), "xml", NA),
    orcid = case_when(
      !is.na(orcid.osf)  ~ orcid.osf,
      !is.na(orcid.xml)  ~ orcid.xml,
      !is.na(orcid.pdf)  ~ orcid.pdf,
      !is.na(orcid.name) ~ orcid.name
    ),
    orcid.source = case_when(
      !is.na(orcid.osf)  ~ "osf",
      !is.na(orcid.xml)  ~ "xml",
      !is.na(orcid.pdf)  ~ "pdf",
      !is.na(orcid.name) ~ "name"
    ),
    orcid = if_else(orcid == "", NA, orcid)
)

# check how many emails we have at this point
df.authors %>% mutate(checkEmail = !is.na(email)) %>% group_by(checkEmail) %>% count()

idx = which(is.na(df.authors$email) & !is.na(df.authors$orcid))
count = 1
tictoc::tic()
for (i in idx) {
  check = orcid_person(df.authors$orcid[i])
  if (is.null(check$error)) {
    if (check$email[[1]][1] != "") {
      df.authors$email[i] = check$email[[1]][1]
      df.authors$email.source[i] = "orcid"
    }
  }
  count = count + 1
  if (count%%250 == 0) {
    tictoc::toc()
    print(sprintf("%d of %d", count, length(idx)))
    tictoc::tic()
  }
}

# check again how many we have
df.authors %>% mutate(checkEmail = !is.na(email)) %>% group_by(checkEmail) %>% count()

if (save.int) {
  write_csv(df.authors, "authorList_ORCIDemail.csv")
}

# from 20% to almost 25%

df.authors %>% group_by(id) %>%
  summarise(checkEmail = sum(!is.na(email)) > 0) %>%
  ungroup() %>%
  summarise(percEmail = mean(checkEmail))

# [CHECK] we have at least one email address for 75% of the preprints

# add extracted emails from PDFs
email.json = read_json("emails_from_pdf.json")
df.email = enframe(email.json, name = "id", value = "pdf.email") %>%
  mutate(
    pdf.email = gsub("list\\(|\\)", "", as.character(pdf.email))
  )

df.authors = merge(df.authors, 
                df.email, 
                all.x = T)

# check how many more emails we can find from the PDF of the preprint
df.authors %>% 
  mutate(
    checkBoth = (!is.na(email)) & (!is.na(orcid))
  ) %>%
  group_by(id) %>%
  summarise(checkEmail = sum(!is.na(email) | pdf.email != "false") > 0,
            checkBoth  = sum(checkBoth) > 0) %>%
  ungroup() %>%
  summarise(percEmail = mean(checkEmail), 
            percBoth  = mean(checkBoth))

# [CHECK] OSF + ORCID + PDF brings us up to ~88% preprints with at least one email
# but only ~61% of the paper have at least one person with an email and an ORCID

# check whether the email is in one of the pdf emails: 70%
df.authors %>% 
  mutate(
    checkEmail = if_else(is.na(email), NA, str_detect(pdf.email, email))
  ) %>% summarise(sameEmail = mean(checkEmail, na.rm = T))

# save this extended author list
write_csv(df.authors, "authorList_ext.csv")
