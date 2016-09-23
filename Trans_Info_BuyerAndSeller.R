#########################################################
##  This file is collecting for buyer and seller info  ##
##  from Zillow Data. In this prototype, we will only  ##
##  collecting the data from Michigan.                 ##
##  Author: Yifang Zhang                               ##
#########################################################

# TODO: add date, remove nonindividuals, remove buyer == seller
# |  TransID  |  BuyerName  |  SellerName  |  TransAddress  |  BuyerAddress  |  Date  |  index  |
# |   111     |    Yifang   |   someone    |    Arkansas    |    other       |  2007  |    i    |
# |   290     |    Alice    |   Yifang     |    Arkansas    |    otherA      |  2010  |    j    |
# |   299     |    Yifang   |    Bob       |    UIUC        |    Arkansas    |  2010  |    k    |
#
# Formula: sellerName(j) == buyerName(k)
#          TransAddress(j) != TransAddress(k)
#          abs(Date(j)-Date(k)) < constant_time (for now we assume it is one year)
#          possible: buyeraddress(k) == transaddress(j)

#################################################################################
## Preliminaries
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

## Setting the working directory
setwd("~/share/projects/Flint/")

# Change directory to where you've stored ZTRAX
dir <- "stores/DB26"


#  Pull in layout information
layoutZAsmt <- read_excel(file.path(dir, 'Layout.xlsx'), sheet = 1)
layoutZTrans <- read_excel(file.path(dir, 'Layout.xlsx'),
                           sheet = 2,
                           col_types = c("text", "text", "numeric", "text", "text"))

# for not prototype
prototyping <- FALSE

if(prototyping){
  rows2load <- 10000
}else{
  rows2load <- -1
}


##########################################################################################
## now we started to deal with the Transactions

col_namesProp <- layoutZTrans[layoutZTrans$TableName == 'utPropertyInfo', 'FieldName']
col_namesMainTr <- layoutZTrans[layoutZTrans$TableName == 'utMain', 'FieldName']

###############################################################################
#   Load PropertyInfo table for later merge

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

## we do that

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

#######################################################################################
#  Load main table in Ztrans database, which provides information on real estate events

trans <- read.table(file.path(dir, "ZTrans/Main.txt"),
                    nrows = rows2load,                    # this is set just to test it out. Remove when code runs smoothly.
                    sep = '|',
                    header = FALSE,
                    stringsAsFactors = FALSE,
                    skipNul = TRUE,                            # tells R to treat two ajacent delimiters as dividing a column
                    comment.char="",                           # tells R not to read any symbol as a comment
                    quote = "",                                # this tells R not to read quotation marks as a special symbol
                    col.names = col_namesMainTr
)

trans <- as.data.table(trans)

trans <- trans[ , list(TransId, LoadID,
                       RecordingDate, DocumentDate, SignatureDate, EffectiveDate,
                       SalesPriceAmount, LoanAmount,
                       SalesPriceAmountStndCode, LoanAmountStndCode,
                       # These remaining variables may be helpful to, although possibly not sufficient for, data cleaning. See documentation for all possible variables.
                       DataClassStndCode, DocumentTypeStndCode,
                       PartialInterestTransferStndCode, IntraFamilyTransferFlag, TransferTaxExemptFlag,
                       PropertyUseStndCode, AssessmentLandUseStndCode,
                       OccupancyStatusStndCode, SecondHomeRiderFlag, UnpaidBalance
                       )]

# Keep only one record for each TransID.
# TransID is the unique identifier of a transaction.
# Multiple entries for the same TransID are due to updated records.
# The most recent record is identified by the greatest LoadID.
#   **** This step may not be necessary for the published dataset as we intend to only publish most updated record.

setkeyv(trans, c("TransId", "LoadID"))
keepRows <- trans[ ,.I[.N], by = "TransId"]
trans <- trans[keepRows[[2]], ]
trans[ , LoadID:= NULL]

#  Keep only events which are deed transfers (excludes mortgage records, foreclosures, etc. See documentation.)

trans <- trans[DataClassStndCode %in% c('D', 'H'), ]

# tempTrans <- subset(trans, select = c("TransId", "UnpaidBalance", "InstallmentAmount", "InstallmentDueDate", "TotalDelinquentAmount", "DelinquentAsOfDate", "CurrentLender"))

###############################################################################
#   Merge previous two datasets together to form transaction table

transComplete <- merge(propTrans, trans, by = "TransId")

transSimple <- subset(transComplete, select = c("ImportParcelID", "TransId", "RecordingDate", 
                                                "PropertyFullStreetAddress", "PropertyCity", "PropertyState", 
                                                "OccupancyStatusStndCode", "PropertyBuildingNumber", 
                                                "PropertyAddressUnitNumber", "PropertyAddressUnitDesignator", 
                                                "SecondHomeRiderFlag"))
## interesting point: some of the entries has no ImportParcelID!!!!
transSimple <- transSimple[which(!is.na(transSimple$ImportParcelID))]

# remove the incomplete address entries
transSimplest <- transSimple[which( str_trim(transSimple$PropertyState)!="" & str_trim(transSimple$PropertyCity)!="" & str_trim(transSimple$PropertyFullStreetAddress)!="")]


###############################################################################
###############################################################################
#   Collecting the buyer and seller info

col_buyerMailAddress <- layoutZTrans[layoutZTrans$TableName == 'utBuyerMailAddress', 'FieldName']
col_sellerMailAddress <- layoutZTrans[layoutZTrans$TableName == 'utSellerMailAddress', 'FieldName']
col_borrowerMailAddress <- layoutZTrans[layoutZTrans$TableName == 'utBorrowerMailAddress', "FieldName"]
col_buyerName <- layoutZTrans[layoutZTrans$TableName == 'utBuyerName', 'FieldName']
col_sellerName <- layoutZTrans[layoutZTrans$TableName == 'utSellerName', 'FieldName']
col_borrowerName <- layoutZTrans[layoutZTrans$TableName == 'utBorrowerName', "FieldName"]

#####################################
## buyer name

buyerName <- read.table(file.path(dir, "ZTrans/BuyerName.txt"),
                        nrows = rows2load,
                        sep = '|',
                        header = FALSE,
                        stringsAsFactors = FALSE,
                        skipNul = TRUE,
                        comment.char = "",
                        quote = "",
                        col.names = col_buyerName)

buyerName <- as.data.table(buyerName)

buyerName <- buyerName[, list(TransId, BuyerIndividualFullName, BuyerNonIndividualName, BuyerNameSequenceNumber, LoadID)]

#buyerName1 <- buyerName

## remove the repeated transID based on loadID
setkeyv(buyerName, c("TransId", "BuyerNameSequenceNumber", "LoadID"))
keepRows <- buyerName[ ,.I[.N], by = c("TransId", "BuyerNameSequenceNumber")]
buyerName <- buyerName[keepRows[[3]], ]
buyerName[ , LoadID:= NULL]

#####################################
## buyer mail

buyerMailAddress <- read.table(file.path(dir, "ZTrans/BuyerMailAddress.txt"),
                        nrows = rows2load,
                        sep = '|',
                        header = FALSE,
                        stringsAsFactors = FALSE,
                        skipNul = TRUE,
                        comment.char = "",
                        quote = "",
                        col.names = col_buyerMailAddress)

buyerMailAddress <- as.data.table(buyerMailAddress)

buyerMailAddress <- buyerMailAddress[, list(TransId, BuyerMailSequenceNumber, BuyerMailFullStreetAddress, BuyerMailCity, BuyerMailState, BuyerMailZip, LoadID, 
                                            BuyerMailBuildingNumber, BuyerMailBuildingName, BuyerMailAddressUnitNumber, BuyerMailAddressUnitDesignatorCode)]


## remove the repeated transID based on loadID
setkeyv(buyerMailAddress, c("TransId", "BuyerMailSequenceNumber", "LoadID"))
keepRows <- buyerMailAddress[ ,.I[.N], by = c("TransId", "BuyerMailSequenceNumber")]
buyerMailAddress <- buyerMailAddress[keepRows[[3]], ]
buyerMailAddress[ , LoadID:= NULL]


## remove the empty terms
buyerMailAddress <- buyerMailAddress[which(str_trim(buyerMailAddress$BuyerMailFullStreetAddress) != "" 
                                           & str_trim(buyerMailAddress$BuyerMailCity) != "" 
                                           & str_trim(buyerMailAddress$BuyerMailState) != "")]


#####################################
## seller name

sellerName <- read.table(file.path(dir, "ZTrans/SellerName.txt"),
                        nrows = rows2load,
                        sep = '|',
                        header = FALSE,
                        stringsAsFactors = FALSE,
                        skipNul = TRUE,
                        comment.char = "",
                        quote = "",
                        col.names = col_sellerName)

sellerName <- as.data.table(sellerName)

sellerName <- sellerName[, list(TransId, SellerIndividualFullName, SellerNonIndividualName, SellerNameSequenceNumber, LoadID)]

#sellerName1 <- sellerName

## remove the repeated transID based on loadID
setkeyv(sellerName, c("TransId", "SellerNameSequenceNumber", "LoadID"))
keepRows <- sellerName[ ,.I[.N], by = c("TransId", "SellerNameSequenceNumber")]
sellerName <- sellerName[keepRows[[3]], ]
sellerName[ , LoadID:= NULL]

#####################################
## seller mail

sellerMailAddress <- read.table(file.path(dir, "ZTrans/SellerMailAddress.txt"),
                               nrows = rows2load,
                               sep = '|',
                               header = FALSE,
                               stringsAsFactors = FALSE,
                               skipNul = TRUE,
                               comment.char = "",
                               quote = "",
                               col.names = col_sellerMailAddress)

sellerMailAddress <- as.data.table(sellerMailAddress)

sellerMailAddress <- sellerMailAddress[, list(TransId, SellerMailSequenceNumber, SellerMailFullStreetAddress, SellerMailCity, SellerMailState, SellerMailZip, LoadID,
                                              SellerMailBuildingName, SellerMailBuildingNumber, SellerMailAddressUnitNumber, SellerMailAddressUnitDesignatorCode)]

#sellerMailAddress1 <- sellerMailAddress

## remove the repeated transID based on loadID
setkeyv(sellerMailAddress, c("TransId", "SellerMailSequenceNumber", "LoadID"))
keepRows <- sellerMailAddress[ ,.I[.N], by = c("TransId","SellerMailSequenceNumber")]
sellerMailAddress <- sellerMailAddress[keepRows[[3]], ]
sellerMailAddress[ , LoadID:= NULL]

## remove the seller mail empty terms
sellerMailAddress <- sellerMailAddress[which(str_trim(sellerMailAddress$SellerMailFullStreetAddress) != "" 
                                           & str_trim(sellerMailAddress$SellerMailCity) != "" 
                                           & str_trim(sellerMailAddress$SellerMailState) != "")]


#####################################
## borrower name


borrowerName <- read.table(file.path(dir, "ZTrans/BorrowerName.txt"),
                         nrows = rows2load,
                         sep = '|',
                         header = FALSE,
                         stringsAsFactors = FALSE,
                         skipNul = TRUE,
                         comment.char = "",
                         quote = "",
                         col.names = col_borrowerName)

borrowerName <- as.data.table(borrowerName)

borrowerName <- borrowerName[, list(TransId, BorrowerIndividualFullName, BorrowerNonIndividualName, BorrowerNameSequenceNumber, LoadID)]



## remove the repeated transID based on loadID
setkeyv(borrowerName, c("TransId", "BorrowerNameSequenceNumber", "LoadID"))
keepRows <- borrowerName[ ,.I[.N], by = c("TransId", "BorrowerNameSequenceNumber")]
borrowerName <- borrowerName[keepRows[[3]], ]
borrowerName[ , LoadID:= NULL]

#####################################
## borrower mail

borrowerMailAddress <- read.table(file.path(dir, "ZTrans/BorrowerMailAddress.txt"),
                                nrows = rows2load,
                                sep = '|',
                                header = FALSE,
                                stringsAsFactors = FALSE,
                                skipNul = TRUE,
                                comment.char = "",
                                quote = "",
                                col.names = col_borrowerMailAddress)

borrowerMailAddress <- as.data.table(borrowerMailAddress)

borrowerMailAddress <- borrowerMailAddress[, list(TransId, BorrowerMailSequenceNumber, BorrowerMailFullStreetAddress, BorrowerMailCity, BorrowerMailState, BorrowerMailZip, LoadID,
                                                  BorrowerMailBuildingName, BorrowerMailBuildingNumber, BorrowerMailAddressUnitNumber, BorrowerMailAddressUnitDesignatorCode)]



## remove the repeated transID based on loadID
setkeyv(borrowerMailAddress, c("TransId", "BorrowerMailSequenceNumber", "LoadID"))
keepRows <- borrowerMailAddress[ ,.I[.N], by = c("TransId","BorrowerMailSequenceNumber")]
borrowerMailAddress <- borrowerMailAddress[keepRows[[3]], ]
borrowerMailAddress[ , LoadID:= NULL]




########################################################################
############### Merging all the tables together! #######################
########################################################################

# transComplete <- merge(propTrans, trans, by = "TransId")
allBuyer <- merge(buyerName, buyerMailAddress, by = "TransId")
#allNames <- merge(allNames, borrowerName, by = "TransId", all.x = T, all.y = F)
allSeller <- merge(sellerName, sellerMailAddress, by = "TransId") #, all.x = T, all.y = T
#allAddresses <- merge(allAddresses, borrowerMailAddress, by = "TransId", all.x = T, all.y = F)

## PPT: Borrower Info is included but not included in output
allBorrower <- merge(borrowerName, borrowerMailAddress, by = "TransId", all.x = T, all.y = F)

## PPT: When merging all buyers and all sellers, we included those terms which lacking of information on either buyer or seller
allPeople <- merge(allBuyer, allSeller, by = "TransId", all.x = T, all.y = T)

## PPT: When merging buyer+seller with property info, we merged by if `TransId` matches
allFinal <- merge(allPeople, transSimplest, by = "TransId")

## PPT: Removed the `rawTable` which has no `RecordingDate` rows
rawTable <- allFinal[which(allFinal$RecordingDate != "")]


## PPT: Added `selfSelling` & `Ocupancy` in `rawTable`

# adding the self-buying / self-selling flag
rawTable$SelfSelling <- 0
rawTable$SelfSelling[which(rawTable$BuyerIndividualFullName != "" & rawTable$BuyerIndividualFullName == rawTable$SellerIndividualFullName)] <- 1

# check occupancy status:
occupancyAvaliable <- rawTable[which(rawTable$OccupancyStatusStndCode != ""), ]
occupancyO <- occupancyAvaliable[which(occupancyAvaliable$OccupancyStatusStndCode == "O")]
occupancyA <- occupancyAvaliable[which(occupancyAvaliable$OccupancyStatusStndCode == "A")]
occupancyS <- occupancyAvaliable[which(occupancyAvaliable$OccupancyStatusStndCode == "S")]
occupancyH <- occupancyAvaliable[which(occupancyAvaliable$OccupancyStatusStndCode == "H")]

########################################################################
############### Using new method to tracking moving ####################
########################################################################

selectedTable <- rawTable

# modify the addresses
#selectedTable$BuyerMailAddress <- paste0(selectedTable$BuyerMailFullStreetAddress, ", ", selectedTable$BuyerMailCity, ", ", selectedTable$BuyerMailState)
#selectedTable$SellerMailAddress <- paste0(selectedTable$SellerMailFullStreetAddress, ", ", selectedTable$SellerMailCity, ", ", selectedTable$SellerMailState)
#selectedTable$BuyerMailAddress <- paste0(selectedTable$PropertyFullStreetAddress, ", ", selectedTable$PropertyCity, ", ", selectedTable$PropertyState)

## PPT: The four result tables are not included the entry if the individual buyer is null.

# first subset of table is buyer table
buyerTable <- selectedTable[which(selectedTable$BuyerIndividualFullName != ""), ]

# second subset of table buyer table
buyerLivingTable <- selectedTable[which(selectedTable$BuyerIndividualFullName != ""), ]

# third subset of table is seller table
sellerTable <- selectedTable[which(selectedTable$SellerIndividualFullName != ""), ]

# fourth subset of table is seller
sellerLivingTable <- sellerTable[which(selectedTable$SellerIndividualFullName != ""), ]

## merging all three tables together

# buyer table:
buyerTable <- subset(buyerTable, select = c("BuyerIndividualFullName", "TransId", 
                                            "BuyerMailFullStreetAddress", "BuyerMailCity", "BuyerMailState", 
                                            "BuyerMailBuildingNumber", "BuyerMailAddressUnitNumber", "BuyerMailAddressUnitDesignatorCode", 
                                            "RecordingDate", "ImportParcelID", "SecondHomeRiderFlag", "SelfSelling"))
buyerTable$Range <- -1
buyerTable$Type <- "Lived at Purchase"
buyerTable$HomeParcelID <- NA
## PPT: Four-Result-Tables have no self-duplicated terms
buyerTable <- buyerTable[!duplicated(buyerTable), ]


# buyer living table: 
buyerLivingTable <- subset(buyerLivingTable, select = c("BuyerIndividualFullName", "TransId", 
                                                        "PropertyFullStreetAddress", "PropertyCity", "PropertyState", 
                                                        "PropertyBuildingNumber", "PropertyAddressUnitNumber", "PropertyAddressUnitDesignator",
                                                        "RecordingDate", "ImportParcelID", "SecondHomeRiderFlag", "SelfSelling"))
buyerLivingTable$Range <- 1
buyerLivingTable$Type <- "Bought Residence"
buyerLivingTable$Type[which(buyerLivingTable$SecondHomeRiderFlag == "Y")] <- "Bought Property"
buyerLivingTable$HomeParcelID <- buyerLivingTable$ImportParcelID
## PPT: Four-Result-Tables have no self-duplicated terms
buyerLivingTable <- buyerLivingTable[!duplicated(buyerLivingTable), ]


# seller table:
sellerTable <- subset(sellerTable, select = c("SellerIndividualFullName", "TransId", 
                                              "SellerMailFullStreetAddress", "SellerMailCity", "SellerMailState", 
                                              "SellerMailBuildingNumber", "SellerMailAddressUnitNumber", "SellerMailAddressUnitDesignatorCode",
                                              "RecordingDate", "ImportParcelID", "SecondHomeRiderFlag", "SelfSelling"))
sellerTable$Range <- -1
sellerTable$Type <- "Lived at Sale"
sellerTable$HomeParcelID <- NA
## PPT: Four-Result-Tables have no self-duplicated terms
sellerTable <- sellerTable[!duplicated(sellerTable), ]

# seller at sell table:
# type = "Sold"
sellerLivingTable <- subset(sellerLivingTable, select = c("SellerIndividualFullName", "TransId", 
                                                          "PropertyFullStreetAddress", "PropertyCity", "PropertyState", 
                                                          "PropertyBuildingNumber", "PropertyAddressUnitNumber", "PropertyAddressUnitDesignator",
                                                          "RecordingDate", "ImportParcelID", "SecondHomeRiderFlag", "SelfSelling"))
sellerLivingTable$Range <- 0
sellerLivingTable$Type <- "Sold"
sellerLivingTable$HomeParcelID <- sellerLivingTable$ImportParcelID
## PPT: Four-Result-Tables have no self-duplicated terms
sellerLivingTable <- sellerLivingTable[!duplicated(sellerLivingTable), ]

# rename and merge
# colnames(X) <- c("good", "better")
colnames(buyerTable) <- c("Name", "TransId", "HomeFullStreetAddress", "HomeCity", "HomeState", "HomeBuildingNumber", "HomeUnitNumber", "HomeUnitDesignator", "Date", "ImportParcelID", "SecondHomeRiderFlag", "SelfSelling",  "Range", "Type", "HomeParcelID")
colnames(buyerLivingTable) <- c("Name", "TransId", "HomeFullStreetAddress", "HomeCity", "HomeState", "HomeBuildingNumber", "HomeUnitNumber", "HomeUnitDesignator", "Date", "ImportParcelID", "SecondHomeRiderFlag", "SelfSelling", "Range", "Type", "HomeParcelID")
colnames(sellerTable) <- c("Name", "TransId", "HomeFullStreetAddress", "HomeCity", "HomeState", "HomeBuildingNumber", "HomeUnitNumber", "HomeUnitDesignator", "Date", "ImportParcelID", "SecondHomeRiderFlag", "SelfSelling", "Range", "Type", "HomeParcelID")
colnames(sellerLivingTable) <- c("Name", "TransId", "HomeFullStreetAddress", "HomeCity", "HomeState", "HomeBuildingNumber", "HomeUnitNumber", "HomeUnitDesignator", "Date", "ImportParcelID", "SecondHomeRiderFlag", "SelfSelling", "Range", "Type", "HomeParcelID")

## modify the house parcel ID
# ZD.b <- merge(x = ZD.b, y = ZD.pid, by.x = "House", by.y = "Trans_Address", all.x = T, all.y = F)
# prepare the matching propTrans Table

## Final Merging
finalTable <- rbind(buyerTable, buyerLivingTable, sellerTable, sellerLivingTable)

