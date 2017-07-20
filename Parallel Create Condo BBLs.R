

# parallel create an expanded list of Condo BBL's from PAD's bbl directory
# Basic idea is that PAD gives you a low BBL and a high BBL, so in order to 
# map to PLUTO and/or sales data, you need to expand the list to include all
# BBL's in that range

# Notes: run time on 10 cores was approximately 55 minutes

library(tidyverse)
library(foreach)
library(doParallel)


bbl_dat <- read_csv("data/pad17b/bobabbl.txt")


## Regular for loop:
# bbl_expanded <- data_frame()
# for(i in 1:nrow(bbl_dat)){
#   cat(i,"of",nrow(bbl_dat),"\n")
#   row <- bbl_dat %>% filter(row_number()==i)
#   row$Id <- i 
#   lo <- row$lolot
#   hi <- row$hilot
#   a_seq <- seq(as.numeric(lo),as.numeric(hi))
#   df <- tibble("new_lot"=a_seq)
#   df <- df %>% mutate(Id=row$Id)
#   df_out <- left_join(df,row, by = "Id")
#   bbl_expanded <- bind_rows(bbl_expanded,df_out)
# }


## Parallel loop:
cl <- makeCluster(detectCores()-2)
registerDoParallel(cl)

rm(bbl_expanded)
start_time <- Sys.time()  

# 1,000 rows 21 seconds
# 10,000 rows 84 seconds

bbl_expanded <- 
  foreach(i = 1:nrow(head(bbl_dat,100000) ) , .combine=bind_rows) %dopar% {
    
  library(tidyverse)
  row <- bbl_dat %>% filter(row_number()==i)
  row$Id <- i 
  lo <- row$lolot
  hi <- row$hilot
  a_seq <- seq(as.numeric(lo),as.numeric(hi))
  df <- tibble("new_lot"=a_seq)
  df <- df %>% mutate(Id=row$Id)
  df_out <- left_join(df,row, by = "Id")
  df_out
};beepr::beep(4)

end_time <- Sys.time()
(tot_time <- end_time - start_time)


stopCluster(cl)



write_rds(bbl_expanded,"data/PAD_Condo_BBLs_expanded.rds",compress = "gz")


