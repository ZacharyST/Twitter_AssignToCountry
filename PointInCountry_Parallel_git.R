#!/usr/bin/R  # Modify your shebang as necessary

# Purpose is to assign country based on GPS.  Twitter misses about 14% - 21% of tweets, appears to be from less developed countries.  That is, these data are not missing at random.


library(maps)  # find point in country
library(countrycode)  # convert output of map.where to 2-letter code
library(mapdata)  # to use high resolution map
library(parallel)  # to make parallel
library(jsonlite)

### Get files
files <- list.files(path=<insert path here>, full.names=TRUE)  # Modify this line

### Modify below to filter files by date and size
times <- file.info(files)
times <- times[times$mtime < '2014-12-31',]  # NB: Change this date as necessary.
times <- times[times$size > 100,]  # There may be blank files.  Execution will throw an error and stop on these files, so get rid of them.
files <- rownames(times)  # These are the new files to process.

### Modify below to filter files by date and size
times <- file.info(files)
temp <- strsplit(files, '//')
temp <- unlist(lapply(temp, FUN=function(x) x[2]))
temp <- substr(temp, 1, 10)
times$date <- temp
times <- times[times$date < '2014-04-01',]  # NB: Put files onto server on 01.30.2017, so mtimes is that date.

#times <- times[times$mtime < '2018-04-30',]  # NB: Change this date as necessary.
times <- times[times$size > 100,]  # There may be blank files.  Execution will throw an error and stop on these files, so get rid of them.
files <- rownames(times)  # These are the new files to process.




### Function that reads the data, adds ISO 3166-1-alpha-2 code
processFile <- function(file_path){
	## jsonlite
	data <- stream_in(file(file_path))

	#data <- stream_in(files[3])
	### Subset data
	data_short <- data[is.na(data$place$country_code) == TRUE,]  # Only look at countries for which Twitter cannot assign
	#blah <- data_short[data_short$geo.coordinates != " ",]  # Some tweets do not have GPS coordinates; ignore them
	# data_short <- data_short[is.null(data_short$geo$coordinates) == FALSE,]  # Some tweets do not have GPS coordinates; ignore them


	data_short$x <- NULL
	data_short$y <- NULL
	for(i in 1:nrow(data_short)){
		print(i)
		temp <- data_short$geo$coordinates[[i]]
		if(is.null(temp) == FALSE){
			x <- temp[[2]]
			y <- temp[[1]]
		}
		if(is.null(temp) == TRUE){
			x <- NA
			y <- NA
		}
		data_short$x[i] <- x
		data_short$y[i] <- y
		# country <- map.where('worldHires', x=x, y=y)
		# data_short$country_code[i] <- ifelse(length(country)==0, NA, country)
	}

	data_short <- data_short[is.na(data_short$x) == FALSE,]  # Some tweets do not have GPS coordinates; ignore them

	data_short$country_code <- map.where('worldHires', x=data_short$x, y=data_short$y)
	data_short$country_code <- countrycode(data_short$country_code, origin=c('country.name'), destination=c('iso2c'))
	# Map new country codes back into main dataframe
	data$place$country_code[as.numeric(rownames(data_short))] <- data_short$country_code

	### Write to zipped file
	### Deprecated because does not add .gz, so would be hard to notice
	here <- gsub('.gz', '', file_path,)
	#gz_file <- gzfile(file_path, 'w')
	jsonlite::stream_out(data, file(here))
	#close(gz_file)

}
		      


### Execute code, in parallel
no_cores <- round(detectCores()/2)  # Leave half the cores available.  You can modify as necessary.
cl <- makeCluster(no_cores, type="FORK")  # Make the cluster.
clusterExport(cl, varlist=c('processFile', 'map.where', 'worldHiresMapEnv', 'countrycode'))  # Need to pass variables that will be used, anything outside of base R
parLapply(cl, files, function(x) processFile(file_path=x))  # Parallel lapply function
stopCluster(cl)  # Make sure to close the cluster




