require(h2o)
h2o.init(ip = 'localhost', port = 54321, nthreads= -1,max_mem_size = '6g')

# Import dataset and display summary
airlines.hex.2000 <- h2o.importFile(path = "2000.csv",destination_frame = "airlines2000.hex")
airlines.hex.2001 <- h2o.importFile(path = "2001.csv",destination_frame = "airlines2001.hex")
airlines.hex.2002 <- h2o.importFile(path = "2002.csv",destination_frame = "airlines2002.hex")
airlines.hex.2003 <- h2o.importFile(path = "2003.csv",destination_frame = "airlines2003.hex")
airlines.hex.2004 <- h2o.importFile(path = "2004.csv",destination_frame = "airlines2004.hex")
airlines.hex.2005 <- h2o.importFile(path = "2005.csv",destination_frame = "airlines2005.hex")
airlines.hex.2006 <- h2o.importFile(path = "2006.csv",destination_frame = "airlines2006.hex")
airlines.hex.2007 <- h2o.importFile(path = "2007.csv",destination_frame = "airlines2007.hex")
airlines.hex.2008 <- h2o.importFile(path = "2008.csv",destination_frame = "airlines2008.hex")

airlines.hex <- h2o.rbind(airlines.hex.2000,airlines.hex.2001,airlines.hex.2002,airlines.hex.2003,airlines.hex.2004,airlines.hex.2005,airlines.hex.2006,airlines.hex.2007,airlines.hex.2008)

summary(airlines.hex)


# View quantiles and histograms
quantile(x = airlines.hex$ArrDelay, na.rm = TRUE)
h2o.hist(airlines.hex$ArrDelay)


# Find number of flights by airport
originFlights <- h2o.group_by(data = airlines.hex, by = "Origin", nrow("Origin"),gb.control=list(na.methods="rm"))
originFlights.R = as.data.frame(originFlights)
head(originFlights.R)

# Find number of flights per month
flightsByMonth = h2o.group_by(data = airlines.hex, by= "Month", nrow("Month"),gb.control=list(na.methods="rm"))
flightsByMonth.R = as.data.frame(flightsByMonth)

# Find months with the highest cancellation ratio
which(colnames(airlines.hex)=="Cancelled")
cancellationsByMonth = h2o.group_by(data = airlines.hex, by = "Month", sum("Cancelled"),gb.control=list(na.methods="rm"))
cancellation_rate = cancellationsByMonth$sum_Cancelled/flightsByMonth$nrow_Month
rates_table = h2o.cbind(flightsByMonth$Month,cancellation_rate)
rates_table.R = as.data.frame(rates_table)

# Construct test and train sets using sampling

airlines.split = h2o.splitFrame(data = airlines.hex,ratios = 0.85)
airlines.train = airlines.split[[1]]
airlines.test = airlines.split[[2]]

# Display a summary using table-like functions
h2o.table(airlines.train$Cancelled)
h2o.table(airlines.test$Cancelled)

# Set predictor and response variables
Y <- "Cancelled"
X = c("Origin", "Dest", "DayofMonth", "Year", "UniqueCarrier", "DayOfWeek", "Month", "DepTime", "ArrTime", "Distance")
airlines.glm <- h2o.glm(training_frame=airlines.train,x=X, y=Y, family = "binomial", alpha = 0.5)
head(airlines.train)

# View model information: training statistics,performance, important variables
summary(airlines.glm)

# Predict using GLM model
pred = h2o.predict(object = airlines.glm, newdata = airlines.test)

summary(pred)
h2o.table(pred$predict,airlines.test$Cancelled)
h2o.table(airlines.test$Cancelled)
final <- cbind(final.actual,final.predicted)
final.actual <- as.data.frame(airlines.test$Cancelled)
final.predicted <- as.data.frame(pred$predict)
table(final)
(82148 + 20194)/(742583 + 7858)
