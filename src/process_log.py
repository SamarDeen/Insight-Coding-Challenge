#!python3
#To run in Windows Terminal
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
from my_functions import *

print sys.argv[0] # prints python_script.py
#print sys.argv[1] # prints var1
#print sys.argv[2] # prints var2
#print sys.argv[3] # prints var2


##Assign path to the test Batch Log
#fname = '/Users/Samar Deen/OneDrive/Career Fair 2016/Insight Data Challenge/anomaly_detection-master/anomaly_detection-master/insight_testsuite/tests/test_1/log_input/batch_log.json'
## Assign path to sample data Batch
##fname = '/Users/Samar Deen/OneDrive/Career Fair 2016/Insight Data Challenge/anomaly_detection-master/anomaly_detection-master/sample_dataset/batch_log.json'
#Assign path to test Stream Log
#fsname = '/Users/Samar Deen/OneDrive/Career Fair 2016/Insight Data Challenge/anomaly_detection-master/anomaly_detection-master/insight_testsuite/tests/test_1/log_input/stream_log.json'
#fsname = '/Users/Samar Deen/OneDrive/Career Fair 2016/Insight Data Challenge/anomaly_detection-master/anomaly_detection-master/sample_dataset/stream_log.json'
#flagged_purchase = '/Users/Samar Deen/OneDrive/Career Fair 2016/Insight Data Challenge/anomaly_detection-master/anomaly_detection-master/insight_testsuite/tests/test_1/log_output/flagged_purchases.json'


fname = sys.argv[1]

#Get Batch Log
df = getBatch(fname)

#get the historical data and calculate anomalies
dfmerge = getMean(df)


fsname = sys.argv[2]
sdf = getStream(fsname)

#FindAnomalies
flagged = getAnom(sdf,dfmerge)

#Assign Friends dataframe
#friends = findFriends(df)

#save to .JSON file
out = flagged.to_json()

flagged_purchase = sys.argv[2]
with open(flagged_purchase, 'w') as f:
    f.write(out)
