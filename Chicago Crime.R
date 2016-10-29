## Setting the Environment
require(h2o)
h2o.init(nthreads = -1)

## If running in EC2 cluster
data_source_is_s3 <- FALSE


locate_source <- function(s) {
    if (data_source_is_s3)
        myPath <- paste0("s3n://h2o-public-test-data/", s)
    else
        myPath <- h2o:::.h2o.locate(s)
}

# Takes column in the specified format 'MM/dd/yyyy hh:mm:ss a' and refines
# it into 8 columns: "Day", "Month", "Year", "WeekNum", "WeekDay", "Weekend",
# "Season", "HourOfDay"

ComputeDateCols <- function(col, datePattern, dateTimeZone = "Etc/UTC") {
    if(nzchar(dateTimeZone) > 0) h2o.setTimezone(dateTimeZone)
    
    ## it's now already auto-detected as date, no need to convert to a date column
    ## d <- as.Date(col, format = datePattern)
    d <- col
    
    ds <- c(Day = h2o.day(d), Month = h2o.month(d), Year = h2o.year(d), WeekNum = h2o.week(d),
            WeekDay = h2o.dayOfWeek(d), HourOfDay = h2o.hour(d))
    
    # Indicator column of whether day is on the weekend
    ds$Weekend <- ifelse(ds$WeekDay == "Sun" | ds$WeekDay == "Sat", 1, 0)
    # ds$Weekend <- as.factor(ds$Weekend)
    
    # Categorical column of season: Spring = 0, Summer = 1, Autumn = 2, Winter = 3
    ds$Season <- ifelse(ds$Month >= 2 & ds$Month <= 4, 0,        # Spring = Mar, Apr, May
                        ifelse(ds$Month >= 5 & ds$Month <= 7, 1,        # Summer = Jun, Jul, Aug
                               ifelse(ds$Month >= 8 & ds$Month <= 9, 2, 3)))   # Autumn = Sep, Oct
    ds$Season <- as.factor(ds$Season)
    h2o.setLevels(ds$Season, c("Spring", "Summer", "Autumn", "Winter"))
    # ds$Season <- cut(ds$Month, breaks = c(-1, 1, 4, 6, 9, 11), labels = c("Winter", "Spring", "Summer", "Autumn", "Winter"))
    return(ds)
}

RefineDateColumn <- function(train, dateCol, datePattern, dateTimeZone = "Etc/UTC") {
    refinedDateCols <- ComputeDateCols(train[,dateCol], datePattern, dateTimeZone)
    # mapply(function(val, nam) { do.call("$<-", list(train, nam, val)) }, refinedDateCols, names(refinedDateCols))
    train$Day <- refinedDateCols$Day
    train$Month <- refinedDateCols$Month + 1    # Since start counting from 0
    train$Year <- refinedDateCols$Year + 1900   # Since indexed starting from 1900
    train$WeekNum <- refinedDateCols$WeekNum
    train$WeekDay <- refinedDateCols$WeekDay
    train$HourOfDay <- refinedDateCols$HourOfDay
    train$Weekend <- refinedDateCols$Weekend
    train$Season <- refinedDateCols$Season
    train
}



weather <- h2o.importFile(path="https://github.com/h2oai/sparkling-water/raw/master/examples/smalldata/chicagoAllWeather.csv", destination_frame="weather.hex")
crimes <- h2o.importFile(path="https://github.com/h2oai/sparkling-water/raw/master/examples/smalldata/chicagoCrimes10k.csv", destination_frame="crimes.hex")
census_raw <- h2o.importFile(path = "https://github.com/h2oai/sparkling-water/blob/master/examples/smalldata/chicagoCensus.csv", parse = FALSE)
census_setup <- h2o.parseSetup(census_raw)
census_setup$column_types[2] <- "Enum"
census <- h2o.parseRaw(census_raw, col.types = census_setup$column_types)
names(census) <- make.names(names(census))
names(crimes) <- make.names(names(crimes))
crimes <- RefineDateColumn(crimes, which(colnames(crimes) == "Date"), datePattern = "%m/%d/%Y %I:%M:%S %p")
crimes$Date <- NULL
weather$date <- NULL
names(census)[names(census) == "Community.Area.Number"] <- "Community.Area"

crimeMerge <- h2o.merge(crimes, census, by = intersect("Community.Area","Community.Area"))
head(crimes)
args(h2o.merge)
names(census)
