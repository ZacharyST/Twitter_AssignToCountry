#!/usr/bin/R  # Modify your shebang as necessary

# Purpose is to assign country based on GPS.  Twitter misses about 14% - 21% of tweets, appears to be from less developed countries.  That is, these data are not missing at random.


library(maps)  # find point in country
library(countrycode)  # convert output of map.where to 2-letter code
library(mapdata)  # to use high resolution map
library(parallel)  # to make parallel

### Get files
files <- list.files(path=<insert path here>, full.names=TRUE)  # Modify this line

### Modify below to filter files by date and size
times <- file.info(files)
times <- times[times$mtime < '2014-12-31',]  # NB: Change this date as necessary.
times <- times[times$size > 100,]  # There are some blank files.  Execution will throw an error and stop on these files, so get rid of them.
files <- rownames(times)  # These are the new files to process.

### Function that reads the data, adds ISO 3166-1-alpha-2 code
processFile <- function(file_path){

	data <- read.csv(file_path, stringsAsFactors=FALSE)

	header <- c('tweet_created_at','tweet_id','tweet_id_str','tweet_in_reply_to_status_id','tweet_in_reply_to_status_id_str','tweet_in_reply_to_user_id','tweet_in_reply_to_user_id_str','tweet_lang','tweet_retweet_count','tweet_source','tweet_text','tweet_truncated','user_created_at','user_favourites_count','user_followers_count','user_following','user_friends_count','user_id','user_id_str','user_lang','user_location','user_screen_name','user_statuses_count','user_utc_offset','coordinates_long','coordinates_lat','country_code','full_name','id','name','place_type','SW_long','SW_lat','NW_long','NW_lat','NE_long','NE_lat','SE_long','SE_lat','hashtag1','hashtag2','hashtag3','hashtag4','hashtag5','hashtag6','hashtag7','hashtag8','hashtag9','hashtag10','hashtag11','hashtag12','hashtag13','hashtag14','hashtag15','user_mention1','user_mention2','user_mention3','user_mention4','user_mention5','user_mention6','user_mention7','user_mention8','user_mention9','user_mention10')

	names(data) <- header

	### Subset data
	data_short <- data[data$country_code == "",]  # Only look at countries for which Twitter cannot assign
	data_short <- data_short[is.na(data_short$coordinates_lat) == FALSE | is.na(data_short$coordinates_long) == FALSE,]  # Some tweets do not have GPS coordinates; ignore them

	### Assign country code
	data_short$country_code <- map.where('worldHires', x=data_short$coordinates_long, y=data_short$coordinates_lat)  # Get country name for each point.
	data_short$country_code <- countrycode(data_short$country_code, origin=c('country.name'), destination=c('iso2c'))  # Get country code.

	# Map new country codes back into main dataframe
	data$country_code[as.numeric(rownames(data_short))] <- data_short$country_code

	### Write to zipped file
	gz_file <- gzfile(file_path, 'w')
	write.csv(data, gz_file, row.names=FALSE)
	close(gz_file)
}


### Execute code, in parallel
no_cores <- round(detectCores()/2)  # Leave half the cores available.  You can modify as necessary.
cl <- makeCluster(no_cores, type="FORK")  # Make the cluster.
clusterExport(cl, varlist=c('processFile', 'map.where', 'worldHiresMapEnv', 'countrycode'))  # Need to pass variables that will be used, anything outside of base R
parLapply(cl, files, function(x) processFile(file_path=x))  # Parallel lapply function
stopCluster(cl)  # Make sure to close the cluster








