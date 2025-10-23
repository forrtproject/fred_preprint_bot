library(jsonlite)
library(dplyr)

# 1. Load JSON
preprints <- fromJSON("~/Downloads/id_title_author.json")

# 2. Flatten into a tibble and check
preprints_df <- as_tibble(preprints)
str(preprints_df)

# 3. Extract unique authors
all_authors <- unique(unlist(preprints_df$author))

# 4. Randomly assign authors
set.seed(123)
author_assignment <- tibble(
  author = all_authors,
  group  = sample(c("treatment", "control"), length(all_authors), replace = TRUE)
)

# 5. Helper function — handle list elements properly
assign_paper_group <- function(authors_of_paper, author_assignment) {
  # authors_of_paper is a character vector
  if (length(authors_of_paper) == 0) return(NA_character_)
  groups <- author_assignment$group[match(authors_of_paper, author_assignment$author)]
  groups <- groups[!is.na(groups)]
  if (length(groups) == 0) return(NA_character_)
  if (length(unique(groups)) == 1) {
    unique(groups)
  } else {
    "mixed"
  }
}

# 6. Apply to each preprint row
assignment_df <- preprints_df |> 
  mutate(group = map_chr(author, assign_paper_group, author_assignment = author_assignment))


# 6. Save outputs
write.csv(author_assignment, "author_assignment.csv", row.names = FALSE)
write.csv(assignment_df, "preprint_assignment.csv", row.names = FALSE)

clean_assignment <- dplyr::filter(assignment_df, group != "mixed")
write.csv(clean_assignment, "preprint_assignment_clean.csv", row.names = FALSE)

table(assignment_df$group, useNA = "ifany")
library(igraph)


edges <- preprints_df |>
  # Only keep papers with at least 2 authors
  filter(lengths(author) >= 2) |>
  # Create all coauthor pairs
  mutate(pairs = map(author, ~ t(combn(.x, 2)))) |>
  unnest(pairs) 

library(igraph)

# Make sure 'edges' has columns 'from' and 'to' after your previous step
edge_matrix <- as.matrix(edges[, c("from", "to")])

g <- graph_from_edgelist(edge_matrix, directed = FALSE)
comps <- components(g)$membership
clusters <- tibble(author = names(comps), cluster = comps)


# randomise at cluster level
cluster_assign <- tibble(
  cluster = unique(comps),
  group = sample(c("treatment", "control"), length(unique(comps)), replace = TRUE)
)
author_assignment2 <- left_join(clusters, cluster_assign, by = "cluster")
author_assignment2 |> 
  group_by(cluster, group) |> 
  summarise(count = n())
