

# Demonstrates how to link NYC ROlling Sales Data, PLUTO and PAD
# Using this methodology, one should be able to geo-locate all sales in 
# NYC, including Condo sales

library(tidyverse)

# sales data --------------------------------------------------------------
nyc_sales_raw <- read_rds('data/nyc-sales-data-raw.rds') %>% mutate_if(.predicate = is.factor,.funs = as.character)

nyc_sales_clean_1 <- 
  nyc_sales_raw %>% 
  mutate_if(.predicate = is.character, .funs = trimws) %>% 
  mutate(SALE.DATE1 =  lubridate::ymd(SALE.DATE, quiet = T)
         ,SALE.DATE2 =  lubridate::mdy(SALE.DATE, quiet = T)
         ,SALE_DATE = SALE.DATE1
         ,SALE_DATE = if_else(is.na(SALE_DATE),SALE.DATE2,SALE_DATE)
         ,SALE_YEAR = lubridate::year(SALE_DATE)
  ) %>% 
  select(-SALE.DATE,-SALE.DATE1,-SALE.DATE2,-SALE.YEAR) %>% 
  mutate(BOROUGH = as.integer(BOROUGH))

# combine with PLUTO ------------------------------------------------------

pluto_lean <- read_rds("data/pluto_lean.rds")

# Note: Only about half of BBL's map correctly. Majority of non-maps are condos
sales_not_in_pluto <-
  anti_join(nyc_sales_clean_2,pluto_lean_2
            , by = c(
              "BOROUGH" = "BOROUGH"
              , "BLOCK" = "Block"
              , "LOT" = "Lot" 
              , "SALE_YEAR" = "Year"
            )
  )

message("All sales in database: ",scales::comma(nrow(nyc_sales_clean_1)))
message("Sales that cannot be mapped to PLUTO BBL: ",scales::comma(nrow(sales_not_in_pluto)))


# combine with PAD --------------------------------------------------------

pad_address <- read_csv("data/pad17b/bobaadr.txt")
pad_bbl <- read_csv("data/pad17b/bobabbl.txt")
pad_bbl_expanded <- read_rds("data/PAD_Condo_BBLs_expanded.rds")



sales_not_in_pluto_or_pad <-
  anti_join(sales_not_in_pluto 
            ,addy_merge
            , by = c(
    "BOROUGH"="boro"
    ,"BLOCK"="block"
    ,"LOT"="lot"
  ))








