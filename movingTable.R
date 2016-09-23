#########################################################
##  This file is A new file that focus on creatin      ##
##  moving table for tracking people's moving.         ##
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


#################################################################################
######################## 4. start the moving table ##############################
#################################################################################

#Base Result Table:
#  
#  if  buyerAtBuy.transID == buyerAfterBuy.transID
#      buyerAtBuy.addr != buyerAfterBuy.addr
#      secondHomeRider == 0
#
#      moveID, transID, name, startingAddr, movedAddr
#

## Setting the working directory
setwd("~/share/projects/zillow/")

rawTable <- read.csv("production/TrackingPeopleMI/rawTable.csv")
rawTable <- as.data.table(rawTable)

## read tables
buyerAtBuy <- read.csv("production/TrackingPeopleMI/buyerAtBuy.csv")
sellerAtSale <- read.csv("production/TrackingPeopleMI/sellerAtSale.csv")
buyerAtBuy <- as.data.table(buyerAtBuy)
sellerAtSale <- as.data.table(sellerAtSale)
sellerAtSale$HomeUnitDesignator[which(is.na(sellerAtSale$HomeUnitDesignator))] <- ""

#buyerAfterBuy <- read.csv("production/TrackingPeopleMI/buyerAfterBuy.csv")
#sellerAfterSale <- read.csv("production/TrackingPeopleMI/sellerAfterSale.csv")
#buyerAfterBuy <- as.data.table(buyerAfterBuy)
#sellerAfterSale<- as.data.table(sellerAfterSale)

baseResult <- rawTable[which(rawTable$BuyerIndividualFullName != ""),]
baseResult <- baseResult[which(baseResult$BuyerMailFullStreetAddress != baseResult$PropertyFullStreetAddress |
                                 baseResult$BuyerMailCity != baseResult$PropertyCity |
                                 baseResult$BuyerMailState != baseResult$PropertyState |
                                 baseResult$BuyerMailAddressUnitNumber != baseResult$PropertyAddressUnitNumber |
                                 baseResult$BuyerMailAddressUnitDesignatorCode != baseResult$PropertyAddressUnitDesignator), ]
baseResult <- baseResult[which(baseResult$SecondHomeRiderFlag != "Y"), ]
baseResult <- baseResult[!duplicated(baseResult)]

#baseResult$BuyerNonIndividualName <- NULL
baseResult$BuyerNameSequenceNumber <- NULL
baseResult$BuyerMailSequenceNumber <- NULL
baseResult[, (13:24):=NULL]

# remove the dups
setkey(baseResult)
baseResult <- baseResult[unique(baseResult, by= c(colnames(baseResult)[2:23])), ]

# remove self-selling
baseResult <- baseResult[which(baseResult$SelfSelling == 0),]

# add a moving ID 
baseResult$moveID <- seq.int(nrow(baseResult))
baseResult <- setcolorder(baseResult, c(length(colnames(baseResult)), 1:(length(colnames(baseResult))-1)))

# rename the fields
setnames(baseResult, "X", "rawTableID")


# raw table                 9,567,416
# no company buyer          7,376,257
# buyer != propTrans        6,031,194
# second home flag != Y     5,985,300
# duplication_remove none   4,395,339
# self-Selling              3,963,943


#############################################################################
## start the 1st filter

# TODO: method: using the moveID to identify those which 
#      Filter for the Base Result Table:
#        i is from Base Result Table, 
#      if  baseResult(i).name == buyerAtBuy(j).name (or sellerAtSale(k).name)
#          baseResult(i).date < buyerAtBuy(j).date  (or sellerAtSale(k).date)
#          baseResult(i).startingAddr == buyerAtBuy(j).addr (or sellerAtSale(k).addr)




#############################################################################
## buyerAtBuy filter

# perform the merge
filterTemp <- merge(x = baseResult, y = buyerAtBuy, 
                             by.x = c("BuyerIndividualFullName"), 
                             by.y = c("Name"),
                             all.x = F, all.y = F, allow.cartesian=TRUE)
# perform the date comparison
filterTemp <- filterTemp[which(as.Date(filterTemp$RecordingDate, format = "%Y-%m-%d") 
                               < as.Date(filterTemp$Date, format = "%Y-%m-%d")),]

# perform the address comparison TODO:
filterTemp <- filterTemp[which(filterTemp$BuyerMailFullStreetAddress == filterTemp$HomeFullStreetAddress &
                                 filterTemp$BuyerMailCity == filterTemp$HomeCity &
                                 filterTemp$BuyerMailState == filterTemp$HomeState &
                                 filterTemp$BuyerMailAddressUnitNumber == filterTemp$HomeUnitNumber &
                                 filterTemp$BuyerMailAddressUnitDesignatorCode == filterTemp$HomeUnitDesignator),]
  
# after merge:      19,088,246
# remove date:       7,163,311
# compare addr:      1,211,610

# length(unique(filterTemp$moveID)): 381,567

# this is what we need to remove after first filtering
removed <- unique(filterTemp$moveID)

# the baseResult after filtering
baseResult1 <- baseResult[which(!baseResult$moveID %in% removed), ]

# save space and remove the filterTemp
rm(filterTemp, removed)

## after removed on the buyerAtBuy: filter = 3,583,376

#############################################################################
## sellerAtSale filter

# perform the merge
filterTemp <- merge(x = baseResult, y = sellerAtSale, 
                    by.x = c("BuyerIndividualFullName"), 
                    by.y = c("Name"),
                    all.x = F, all.y = F, allow.cartesian=TRUE)
# perform the date comparison
filterTemp <- filterTemp[which(as.Date(filterTemp$RecordingDate, format = "%Y-%m-%d") 
                               < as.Date(filterTemp$Date, format = "%Y-%m-%d")),]

# perform the address comparison TODO:
filterTemp <- filterTemp[which(filterTemp$BuyerMailFullStreetAddress == filterTemp$HomeFullStreetAddress &
                                 filterTemp$BuyerMailCity == filterTemp$HomeCity &
                                 filterTemp$BuyerMailState == filterTemp$HomeState &
                                 filterTemp$BuyerMailAddressUnitNumber == filterTemp$HomeUnitNumber &
                                 filterTemp$BuyerMailAddressUnitDesignatorCode == filterTemp$HomeUnitDesignator),]


# after merge:           13,190,305
# remove date:            7,068,111
# compare addr:             590,011

#length(unique(filterTemp$moveID)): 159,633

# this is what we need to remove after first filtering
removed <- unique(filterTemp$moveID)

# the baseResult after filtering
baseResult2 <- baseResult1[which(!baseResult1$moveID %in% removed), ]

# save space and remove the filterTemp
rm(filterTemp, removed)
rm(rawTable)

## baseResult2 after SellerAfterSale: 3,484,540

#########################################################
## write the csv files

#write.csv(baseResult, "../migration/production/baseResult.csv")
#write.csv(baseResult2, "../migration/production/baseResult_bothCondition.csv")


#########################################################
## finding the moving pairs of the tables

leftBaseResult <- baseResult2
leftBaseResult$newAddress <- paste0(baseResult2$BuyerMailAddressUnitNumber, " ", baseResult2$BuyerMailAddressUnitDesignatorCode, " ",
                                    baseResult2$BuyerMailFullStreetAddress, " ", baseResult2$BuyerMailCity, " ", baseResult2$BuyerMailState)
leftBaseResult

## thinking about another way of merging
movingTable_raw <- merge(x = baseResult2, y = baseResult2,
                     by.x = c("BuyerMailFullStreetAddress", "BuyerMailCity", "BuyerMailState", "BuyerMailAddressUnitNumber", "BuyerMailAddressUnitDesignatorCode"), 
                     by.y = c("PropertyFullStreetAddress", "PropertyCity", "PropertyState", "PropertyAddressUnitNumber", "PropertyAddressUnitDesignator"),
                     all.x = F, all.y = F, allow.cartesian=TRUE)

movingTable <- movingTable

