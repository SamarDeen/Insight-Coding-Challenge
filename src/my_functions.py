#!python3
	
#cd OneDrive\PythonFiles\sample
#Py -3 test.py
	
import sys
if sys.version_info[0] < 3:
	from StringIO import StringIO
else:
	from io import StringIO
	
import pandas as pd
import json
import numpy as np
from pandas.io.json import json_normalize


def getBatch(fname):
	data = []
	with open(fname) as f:
    		for line in f:
        		data.append(json.loads(line))

	df = pd.read_json(json.dumps(data))

	#Index and get a  sequence to track events
	df['seq'] = list(range(len(df.index)))  
	return df
	
def getMean(df):
#get anomalies
	
	#Subset the panda
	df.subset = df.loc[df['event_type'] == 'purchase']

	df.subset['TotalCount'] = df.subset.groupby('id')['amount'].transform(sum)
	counts_df = pd.DataFrame(df.subset.groupby('id').size().rename('counts'))
	#print(counts_df)

	#merge the two dataframes
	df.merge = pd.merge(df.subset, counts_df, left_on='id', right_index=True, how='left', sort=False)

	#divide sum/count
	df.merge['mean_amount'] = df.merge['TotalCount'] / df.merge['counts'] 
	df.merge['sd_amount'] = df.merge['TotalCount'] / df.merge['counts'] 

	##The counts must be T or less for the average 
	## Still to be implemented
	
	df.merge['sd_amount']=df.merge.groupby('id')['amount'].apply(np.std)
	df.merge['anom'] = df.merge['mean_amount']+3*df.merge['sd_amount']
	
	return df.merge
	
def getStream(fsname):
	sdata = []
	with open(fsname) as f:
    		for line in f:
        		sdata.append(json.loads(line))

	sdf = pd.read_json(json.dumps(sdata))
	return sdf

def getAnom(sdf,dfmerge):
	#merge anomalies to the stream_log - and identify anomolies
	merge_anom = pd.merge(sdf,dfmerge, left_on='id', right_index=True,  how='left', sort=False)

	#assigning zeros to NaNs in anom - then replacing with the mean_amount to compare with the purchase 
	merge_anom['anom'] = merge_anom['anom'].replace(np.nan, 0)
	#merge_anom.loc[merge_anom['anom'] == 0, 'anom'] = merge_anom['mean_amount']
	merge_anom['flag'] = 0.0

	#print(merge_anom.dtypes)
	merge_anom.loc[(merge_anom['anom'] != 0) & (merge_anom['anom'] < merge_anom['amount_x']), 'flag'] = 1

	#print(merge_anom)

	#subset the flagged data entries and prepare data to write to JSON
	flagged = merge_anom.loc[:,['id','event_type','timestamp','amount', 'mean_amount','sd_amount', 'anom','flag']]
	flagged = flagged.loc[flagged['flag']==1]
	print(flagged)
	return flagged
	
def findFriends(df):
	#Subset the panda
	dfriend = df.loc[(df['event_type'] == 'befriend' )| (df['event_type'] == 'unfriend')] 
	#convert ids to string and swap to find friend/unfriend matches
	
	dfriend['ids1'] = dfriend['id1'].map(str) + dfriend['id2'].map(str)
	dfriend['ids2'] = dfriend['id2'].map(str) + dfriend['id1'].map(str)
	
	
	# construct friends 
	# (1) find friends id1 == id2 # to start with we can merge the same dframe with id1 and id2
	df.sameids = pd.merge(dfriend,dfriend, left_on='ids1', right_on ='ids2', sort=False)
	
	
	#do a for loop to find duplicates and drop the one with the larger 'seq' value
	#(1) set a flag to drop duplicates
	
	df.sameids['drop'] = 0
	df.sameids['checked'] = 0
	
	for x in range(len(df.sameids.index)):
		for y in range(len(df.sameids.index)):
   			print('in loop')
   			if((df.sameids.iloc[x]['id1_x']==df.sameids.iloc[y]['id2_x']) & (df.sameids.iloc[x]['checked']==df.sameids.iloc[y]['checked'] == 0)): 
    				if(df.sameids.iloc[x]['seq_x']>df.sameids.iloc[y]['seq_x']):
    					print('in if statement')
    					df.sameids.loc[y,'drop'] = 1
    					df.sameids.loc[x,'checked'] = 1
    					df.sameids.loc[y,'checked'] = 1
    				else: df.sameids.loc[x,'drop'] = 1

	#drop the new friend/unfriend request -- Now we have the old value, and we want to drop this from dfriend
	df.sameids = df.sameids.loc[(df.sameids['drop'] == 1 )] 
	
	#get a list of the rows that need to be dropped
	
	#clean up the duplicate columns
	dfriend2 =  df.sameids.loc[:,['id1_x', 'id2_x','event_type_x','timestamp_x', 'seq_x']]
	#rename the columns
	dfriend2.columns = dfriend2.columns.str.replace('_x','')
	
	#drop extra columns in dfriend
	dfriend =  dfriend.loc[:,['id1', 'id2','event_type','timestamp', 'seq']]
	
	#find common columns and drop the older list of friend
	common_cols = dfriend2.columns.tolist()                        
	df12 = pd.merge(dfriend2, dfriend, on=common_cols, how='inner')   
	df2 = dfriend[~dfriend['seq'].isin(df12['seq'])]
	
	return df2
	