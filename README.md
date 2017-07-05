# Insight-Coding-Challenge

The file process_log.py calls my_functions which has the main computational functions. These include: 

1. def getBatch(fname): gets the initial batch json data into a pandas df 
2. def getMean(df): calculates the mean  
3. def getAnom(sdf,dfmerge): identifies the anomalies in the data set of purchases - at the moment it considers all entries to be historical 
4. def findFriends(df): Finds the most recent transaction of "friend" "unfriend" and creates a dataframe of friends that can potentially be emailed fr anomalous purchases 
