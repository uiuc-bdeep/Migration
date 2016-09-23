#########################################################
##  This file is A new file that added to get          ##
##  importparcelID from addresses.                     ##
##  Author: Yifang Zhang                               ##
#########################################################

#################################################################################
############################## Preliminaries ####################################
#################################################################################

# rm(list=ls(pattern="temp"))
# rm(list=setdiff(ls(), "x")) remove except "x"
rm(list=ls())

## This function will check if a package is installed, and if not, install it
pkgTest <- function(x) {
  if (!require(x, character.only = TRUE))
  {
    install.packages(x, dep = TRUE)
    if(!require(x, character.only = TRUE)) stop("Package not found")
  }
}

## These lines load the required packages
packages <- c("readxl", "data.table", "sp", "ff", "ffbase", "stringr", "plyr")
lapply(packages, pkgTest)

## These lines set several options
options(scipen = 999) # Do not print scientific notation
options(stringsAsFactors = FALSE) ## Do not load strings as factors

# for not prototype
prototyping <- FALSE

if(prototyping){
  rows2load <- 10000
}else{
  rows2load <- -1
}

#################################################################################
########################### 1. get tables from csv ##############################
#################################################################################


#################################################################################
## 1.1 get property trans table

## Setting the working directory
setwd("~/share/projects/Flint/")

# Change directory to where you've stored ZTRAX
dir <- "stores/DB26"

#  Pull in layout information
layoutZAsmt <- read_excel(file.path(dir, 'Layout.xlsx'), sheet = 1)
layoutZTrans <- read_excel(file.path(dir, 'Layout.xlsx'),
                           sheet = 2,
                           col_types = c("text", "text", "numeric", "text", "text"))

col_namesMain <- layoutZAsmt[layoutZAsmt$TableName == 'utMain', 'FieldName']

base <- read.table(file.path(dir, "ZAsmt/Main.txt"),
                   nrows = rows2load,                    
                   sep = '|',
                   header = FALSE,
                   stringsAsFactors = FALSE,             
                   skipNul = TRUE,                            # tells R to treat two ajacent delimiters as dividing a column 
                   comment.char="",                           # tells R not to read any symbol as a comment
                   quote = "",                                # this tells R not to read quotation marks as a special symbol
                   col.names = col_namesMain
)                                          

base <- as.data.table(base)
#base <- base[ , list(RowID, ImportParcelID, LoadID, 
#                     FIPS, State, County, 
#                     PropertyFullStreetAddress,
#                     PropertyHouseNumber, PropertyHouseNumberExt, PropertyStreetPreDirectional, PropertyStreetName, PropertyStreetSuffix, PropertyStreetPostDirectional,
#                     PropertyCity, PropertyState, PropertyZip,
#                     PropertyBuildingNumber, PropertyAddressUnitDesignator, PropertyAddressUnitNumber,
#                     PropertyAddressLatitude, PropertyAddressLongitude, PropertyAddressCensusTractAndBlock, 
#                     NoOfBuildings,
#                     LotSizeAcres, LotSizeSquareFeet,
#                     TaxAmount, TaxYear)]
propTrans <- base[ , list(RowID, ImportParcelID, LoadID, 
                     PropertyFullStreetAddress, PropertyCity, PropertyState,
                     PropertyBuildingNumber, PropertyAddressUnitDesignator, PropertyAddressUnitNumber)]


# Keep only one record for each ImportPropertyID. 
# ImportParcelID is the unique identifier of a parcel. Multiple entries for the same ImportParcelID are due to updated records.
# The most recent record is identified by the greatest LoadID. 
#   **** This step may not be necessary for the published dataset as we intend to only publish the updated records, but due dilligence demands we check. 

length(unique(propTrans$ImportParcelID))  # Number of unique ImportParcelIDs
dim(propTrans)[1]                         # Number of rows in the base dataset

if( length(unique(propTrans$ImportParcelID)) != dim(propTrans)[1] ){
  
  #Example: Print all entries for parcels with at least two records.
  propTrans[ImportParcelID %in% propTrans[duplicated(ImportParcelID), ImportParcelID], ][order(ImportParcelID)]
  
  setkeyv(propTrans, c("ImportParcelID", "LoadID"))  # Sets the index and also orders by ImportParcelID, then LoadID increasing
  keepRows <- propTrans[ ,.I[.N], by = c("ImportParcelID")]   # Creates a table where the 1st column is ImportParcelID and the second column 
  # gives the row number of the last row that ImportParcelID appears.
  propTrans <- propTrans[keepRows[[2]], ] # Keeps only those rows identified in previous step
  
  rm(keepRows)
  
}



#################################################################################
## 1.2 get buyerAtBuy & sellerAtSale tables

## Setting the working directory
setwd("~/share/projects/zillow/")

buyerAtBuy <- read.csv("production/TrackingPeopleMI/before_Aug15_noID/buyerAtBuy.csv")
sellerAtSale <- read.csv("production/TrackingPeopleMI/before_Aug15_noID/sellerAtSale.csv")
buyerAfterBuy <- read.csv("production/TrackingPeopleMI/before_Aug15_noID/buyerAfterBuy.csv")
sellerAfterSale <- read.csv("production/TrackingPeopleMI/before_Aug15_noID/sellerAfterSale.csv")

buyerAtBuy <- as.data.table(buyerAtBuy)
sellerAtSale <- as.data.table(sellerAtSale)
buyerAfterBuy <- as.data.table(buyerAfterBuy)
sellerAfterSale<- as.data.table(sellerAfterSale)

sellerAtSale$HomeUnitDesignator[which(is.na(sellerAtSale$HomeUnitDesignator))] <- ""

#################################################################################
############################ 2. clean the tables ################################
#################################################################################


preparedPropTable <- propTrans[which(!is.na(propTrans$ImportParcelID)), ]

#preparedPropTable <- preparedPropTable[which(str_trim(preparedPropTable$PropertyFullStreetAddress) != "" & str_trim(preparedPropTable$PropertyCity) != "" 
#                                             & str_trim(preparedPropTable$PropertyState) != ""), ]

preparedPropTable <- subset(preparedPropTable, select = c("ImportParcelID", "PropertyFullStreetAddress", "PropertyCity", "PropertyState", "PropertyBuildingNumber", "PropertyAddressUnitNumber", "PropertyAddressUnitDesignator"))

preparedPropTable <- preparedPropTable[!duplicated(preparedPropTable$ImportParcelID), ]

## Remove the duplicated terms, not just keeping the first duplicated term using unique
ind1 <- duplicated(preparedPropTable, by = c("PropertyFullStreetAddress", "PropertyCity", "PropertyState", "PropertyAddressUnitNumber", "PropertyAddressUnitDesignator"), fromLast = TRUE)
ind2 <- duplicated(preparedPropTable, by = c("PropertyFullStreetAddress", "PropertyCity", "PropertyState", "PropertyAddressUnitNumber", "PropertyAddressUnitDesignator"))
preparedPropTable_uniqueAddress <- preparedPropTable[(!(ind1 | ind2)), ]
preparedPropTable_dupAddress <- preparedPropTable[((ind1 | ind2)), ]
# hadUnitNumber <- preparedPropTable_dupAddress[which(preparedPropTable_dupAddress$PropertyAddressUnitNumber!= ""),] 
# 820/119147 < 1% 

#######################################################
# from Main Table ZAsmt                     5,343,547
# remove duplicate                          4,020,275




#######################################################
# propTrans                                10,664,370
# preparedPropTable                         9,710,985
# preparedPropTable_after_trim              9,636,898
# preparedPropTable_remove_dup              2,837,484
# preparedPropTable_only_keep_first_dup     2,766,235
# preparedpropTable_no_dup                  2,718,337

#################################################################################
########################## 3. perform the matching ##############################
#################################################################################

colnames(preparedPropTable_uniqueAddress) <- c("HomeParcelID", "HomeFullStreetAddress", "HomeCity", "HomeState", "HomeBuildingNumber", "HomeUnitNumber", "HomeUnitDesignator")

#rename(d, c("beta"="two", "gamma"="three"))
#>   alpha two three
#> 1     1   4     7
#> 2     2   5     8
#> 3     3   6     9

# for buyer at buy
buyerAtBuy_Complete <- merge(x = buyerAtBuy, y = preparedPropTable_uniqueAddress, 
                     by.x = c("HomeFullStreetAddress", "HomeCity", "HomeState", "HomeBuildingNumber", "HomeUnitNumber", "HomeUnitDesignator"), 
                     by.y = c("HomeFullStreetAddress", "HomeCity", "HomeState", "HomeBuildingNumber", "HomeUnitNumber", "HomeUnitDesignator"),
                     all.x = T, all.y = F)
buyerAtBuy_Complete$HomeParcelID.x <- NULL
#colnames(buyerAtBuy_Complete)[7] <- "X"
colnames(buyerAtBuy_Complete)[16] <- "HomeParcelID"
setcolorder(buyerAtBuy_Complete, c(7,8,9,1,2,3,4,5,6,10,11,12,13,14,15,16)) 

# for seller at sale
sellerAtSale_Complete <- merge(x = sellerAtSale, y = preparedPropTable_uniqueAddress, 
                             by.x = c("HomeFullStreetAddress", "HomeCity", "HomeState", "HomeBuildingNumber", "HomeUnitNumber", "HomeUnitDesignator"), 
                             by.y = c("HomeFullStreetAddress", "HomeCity", "HomeState", "HomeBuildingNumber", "HomeUnitNumber", "HomeUnitDesignator"),
                             all.x = T, all.y = F)
sellerAtSale_Complete$HomeParcelID.x <- NULL
#colnames(sellerAtSale_Complete)[7] <- "X"
colnames(sellerAtSale_Complete)[16] <- "HomeParcelID"
setcolorder(sellerAtSale_Complete, c(7,8,9,1,2,3,4,5,6,10,11,12,13,14,15,16)) 



# % of people buyerAtBuy & sellerAtSale not in MI out of their own table
# % of NA that is in MI
# % of NA that is not in MI
# % of NA out of all data
buyerNum <- nrow(buyerAtBuy_Complete)

buyerNotMI <- buyerAtBuy_Complete[which(buyerAtBuy_Complete$HomeState!="MI"),]
buyerMI <- buyerAtBuy_Complete[which(buyerAtBuy_Complete$HomeState=="MI"),]

buyer_NoImportParcelID <- buyerAtBuy_Complete[which(is.na(buyerAtBuy_Complete$HomeParcelID)),]
buyer_ImportParcelID <- buyerAtBuy_Complete[which(!is.na(buyerAtBuy_Complete$HomeParcelID)),]

buyer_NoImportParcelID_NotMI <- buyerNotMI[which(is.na(buyerNotMI$HomeParcelID)),]
buyer_NoImportParcelID_MI <- buyerMI[which(is.na(buyerMI$HomeParcelID)),]

#weird data
buyer_ImportParcelID_NotMI <- buyerNotMI[which(!is.na(buyerNotMI$HomeParcelID)),]


sellerNum <- nrow(sellerAtSale_Complete)

sellerNotMI <- sellerAtSale_Complete[which(sellerAtSale_Complete$HomeState!="MI"),]
sellerMI <- sellerAtSale_Complete[which(sellerAtSale_Complete$HomeState=="MI"),]

seller_NoImportParcelID <- sellerAtSale_Complete[which(is.na(sellerAtSale_Complete$HomeParcelID)),]
seller_ImportParcelID <- sellerAtSale_Complete[which(!is.na(sellerAtSale_Complete$HomeParcelID)),]

seller_NoImportParcelID_NotMI <- sellerNotMI[which(is.na(sellerNotMI$HomeParcelID)),]
seller_NoImportParcelID_MI <- sellerMI[which(is.na(sellerMI$HomeParcelID)),]

#weird data
seller_ImportParcelID_NotMI <- sellerNotMI[which(!is.na(sellerNotMI$HomeParcelID)),]

###################################################################################
## form the final table
finalTable <- rbind(buyerAtBuy_Complete, buyerAfterBuy, sellerAtSale_Complete, sellerAfterSale)

## write all tables
# write.csv(buyerAtBuy_Complete, "production/TrackingPeopleMI/buyerAtBuy.csv")
# write.csv(sellerAtSale_Complete, "production/TrackingPeopleMI/sellerAtSale.csv")
# write.csv(buyerAfterBuy, "production/TrackingPeopleMI/buyerAfterBuy.csv")
# write.csv(sellerAfterSale, "production/TrackingPeopleMI/sellerAfterSale.csv")
# write.csv(finalTable, "production/TrackingPeopleMI/allBuyerAndSeller.csv")
# write.csv(preparedPropTable_uniqueAddress, "production/TrackingPeopleMI/rawImportParcelTable.csv")


## 
#buyerMailAddressIsTransAddress <- rawTable[which(rawTable$BuyerMailFullStreetAddress == rawTable$PropertyFullStreetAddress 
#                                                 & rawTable$BuyerMailCity == rawTable$PropertyCity
#                                                 & rawTable$BuyerMailState == rawTable$PropertyState
#                                                 & rawTable$BuyerMailAddressUnitNumber == rawTable$PropertyAddressUnitNumber
#                                                 & rawTable$BuyerMailAddressUnitDesignatorCode == rawTable$PropertyAddressUnitDesignator), ]
# 15.31641%










if(FALSE){
  ## now we started to deal with the Transactions
  col_namesProp <- layoutZTrans[layoutZTrans$TableName == 'utPropertyInfo', 'FieldName']
  col_namesMainTr <- layoutZTrans[layoutZTrans$TableName == 'utMain', 'FieldName']
  
  ## extracting the table from the ZTrans/PropertyInfo.txt
  propTrans <- read.table(file.path(dir, "ZTrans/PropertyInfo.txt"),
                          nrows = rows2load,                    # this is set just to test it out. Remove when code runs smoothly.
                          sep = '|',
                          header = FALSE,
                          stringsAsFactors = FALSE,
                          skipNul = TRUE,                            # tells R to treat two ajacent delimiters as dividing a column
                          comment.char="",                           # tells R not to read any symbol as a comment
                          quote = "",                                # this tells R not to read quotation marks as a special symbol
                          col.names = col_namesProp
  )
  
  propTrans <- as.data.table(propTrans)
  
  propTrans <- propTrans[ , list(TransId, PropertySequenceNumber, LoadID, ImportParcelID,
                                 # for the people's tracking, we will need the property address
                                 PropertyFullStreetAddress, PropertyCity, PropertyState,  
                                 PropertyBuildingNumber, PropertyAddressUnitNumber, PropertyAddressUnitDesignator)]
  
  # Keep only one record for each TransID and PropertySequenceNumber.
  # TransID is the unique identifier of a transaction, which could have multiple properties sequenced by PropertySequenceNumber.
  # Multiple entries for the same TransID and PropertySequenceNumber are due to updated records.
  # The most recent record is identified by the greatest LoadID.
  #   **** This step may not be necessary for the published dataset as we intend to only publish most updated record.
  
  setkeyv(propTrans, c("TransId", "PropertySequenceNumber", "LoadID"))
  keepRows <- propTrans[ ,.I[.N], by = c("TransId", "PropertySequenceNumber")]
  propTrans <- propTrans[keepRows[[3]], ]
  propTrans[ , LoadID:= NULL]
  
  # Drop transactions of multiple parcels (transIDs associated with PropertySequenceNumber > 1)
  
  dropTrans <- unique(propTrans[PropertySequenceNumber > 1, TransId])
  propTrans <- propTrans[!(TransId %in% dropTrans), ]   # ! is "not"
}