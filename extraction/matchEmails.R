# load libraries > need to be installed with install.packages("name") first
library(tidyverse)
library(RecordLinkage)

# read in the author dataframe
df.authors = read_csv("authorList_ext.csv")

# select the relevant part of the author dataframe
df.sel = df.authors %>% 
  group_by(id) %>% mutate(n = sum(!is.na(email))) %>%
  ungroup() %>%
  filter(pdf.email != "false" & n > 0 & !is.na(id)) %>%
  select(id, pdf.email, name.surname, name.given) %>% 
  mutate(email.possible = NA, email.similarity = NA)

ls.ids = split(df.sel, df.sel$id)

for (i in 1:length(ls.ids)) { #
  preprint = names(ls.ids)[i]
  emails = str_split(ls.ids[[i]]$pdf.email[1], "\", \"")
  for (k in 1:length(ls.ids[[i]]$name.surname)) {
    similar = c()
    for (j in 1:length(emails[[1]])) {
      # getting rid of the @... for each of the email
      email = gsub("@.*", "", tolower(emails[[1]][j]))
      similar = c(similar, levenshteinSim(tolower(ls.ids[[i]]$name.surname[k]),
                              email))
    }
    idx = which(similar == max(similar))
    if (length(idx) > 1) idx = idx[1]
    if (sum(similar) > 0) {
      r = which(df.sel$id == preprint & df.sel$name.surname == ls.ids[[i]]$name.surname[k] & df.sel$name.given == ls.ids[[i]]$name.given[k])
      df.sel[r,]$email.possible   = emails[[1]][idx]
      df.sel[r,]$email.similarity = similar[idx]
    }
  }
}

# get rid of quotation marks
df.sel = df.sel %>%
  mutate(
    email.possible = gsub("\"", "", email.possible),
    # adjust the similarity for the length ratio
    email.similarity.adj = email.similarity * (nchar(name.surname)/nchar(gsub("@.*", "", tolower(email.possible))))
  )

# only keep best match for each email + preprint
df.test = df.sel %>%
  group_by(id, email.possible) %>%
  mutate(email.rank = max(rank(email.similarity)) - rank(email.similarity)) %>%
  filter(email.rank == 0)

# figure out a threshold that makes sense, either for adjusted or not

# there are some that don't fit despite really good similariy, e.g.:
# Melacarne Claudio is fit to elcra@unisi.it > probably not a fit? maybe? how do we tell?

# at the end, this can be merged back to df.authors
