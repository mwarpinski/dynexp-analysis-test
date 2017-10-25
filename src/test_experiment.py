from scipy import stats
import numpy as np
import pandas as pd
import math
import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext

cwd = os.getcwd()

conf = SparkConf().setAppName("my test")
conf = conf.setMaster("local[*]")
sc   = SparkContext(conf=conf)
sqlContext = HiveContext(sc)

# Load Data:
query = ("""SELECT 
         visit__visit_id,       
         partition_date_denver as Date,
         ExperimentID, 
         TreatmentID, 
         sum(minutes_watched) as Action
         FROM ( 
         select * from
         dynexp.dynexp_visit_agg 
         LATERAL VIEW EXPLODE(visit__experiment_uuids) temp as ExperimentID 
         LATERAL VIEW EXPLODE(visit__variant_uuids) temp as TreatmentID 
         where ExperimentID = "59c192c011353d001d691c44"
         and TreatmentID IN ("59c192c011353d001d691c46", "59c192c011353d001d691c45", "59c193b111353d001d691c47")
         ) agg
         group by visit__visit_id, partition_date_denver, ExperimentID, TreatmentID
""")
cleaned_data = sqlContext.sql(query).toPandas()

cleaned_data['Date'] = pd.to_datetime(cleaned_data['Date'])

# Define Functions
def get_TStats(mx,lx,vx,my,ly,vy):
  # Note: numpy.var() is BIASED by default. Tsk. Tsk. This took me 2 hours to figure out.
  # Corrected biased variance estimator below. 
  # vx = vx*(lx/(lx-1)) # * lowercase 'L' and #1
  # vy = vy*(ly/(ly-1))
  # Omit, found a better work-around: use ddof=1 flag in numpy.var() function call.
  # T-Statistic
  try:
    tstat = (mx - my) / math.sqrt(vx/lx + vy/ly)
  except:
    tstat = 0
  # Calculate Degrees of Freedom
  try:
    df = (((vx / lx) + (vy / ly))**2) / ( ((vx**2) / ((lx-1)*lx**2)) + (vy**2) / ((ly-1)*ly**2)  )
  except:
    df = 2
  # Calculate p-value
  pval = stats.t.sf(np.abs(tstat), df)*2
  #pval = pv()
  # difference in means
  diffxy = mx - my
  # Critical value
  tcrit = stats.t.isf(0.025,df)
  #pdf = 1/math.sqrt()
  # Confidence intervals
  interval = tcrit * math.sqrt((vx/lx) + (vy/ly))
  ci = dict()
  ci['Upper'] = diffxy + interval
  ci['Lower'] = diffxy - interval
  # return dictionary?
  valueDict = dict()
  valueDict['T_Stat'] = tstat
  valueDict['DF'] = df
  valueDict['P_Value'] = pval
  valueDict['DiffMean'] = diffxy
  valueDict['CI'] = ci
  valueDict['T_Critical'] = tcrit
  return(valueDict)

def vector_TStats(x,y):
  return(get_TStats(np.mean(x),len(x),np.var(x,ddof=1),np.mean(y),len(y),np.var(y,ddof=1)))

# initialize results:
columns = (['Date','Title','Experiment','TreatmentA','TreatmentB',
            'CurrentCountA','CurrentCountB','CurrentMeanA','CurrentMeanB',
           'AggCountA','AggCountB','AggMeanA','AggMeanB',
           'CurrentDiffMean','CumulativeDiffMean',
           'TStat','PValue','LowerBound','UpperBound','Degrees_of_Freedom'])
resultFrame = (pd.DataFrame(columns=columns))

# Ignore tunnel chart for now
"""
# Run Calculations:
for exp in sorted(cleaned_data['ExperimentID'].unique()):
  expSubset = cleaned_data.query('ExperimentID == "%s"' % exp)
  L = sorted(expSubset['TreatmentID'].unique())
  for trt1 in L:
    for trt2 in L:
      if trt1 != trt2:
        title = "EXP_%s_%s_v_%s" % (exp, trt1, trt2)
        # Tunnel Chart
        dateSeq = pd.date_range(min(cleaned_data['Date']),max(cleaned_data['Date']))
        #trtSubset = expSubset.loc[(expSubset.TreatmentID == trt1) | (expSubset.TreatmentID == trt2)]
        trtSubset = expSubset[expSubset.TreatmentID.isin([trt1,trt2])]
        for dt in dateSeq:
          # TODO: put time constraint for the dateSeq
          timeSubset = trtSubset.loc[trtSubset.Date <= dt]
          trtA = timeSubset.loc[timeSubset.TreatmentID == trt1]['Action']
          trtB = timeSubset.loc[timeSubset.TreatmentID == trt2]['Action']
          # Instantaneous Subset
          instantSubset = trtSubset.loc[trtSubset.Date == dt]
          instA = instantSubset.loc[instantSubset.TreatmentID == trt1]['Action']
          instB = instantSubset.loc[instantSubset.TreatmentID == trt2]['Action']
          if (len(instA) > 1) & (len(instB) > 1):
            instMeanA = np.mean(instA)
            instMeanB = np.mean(instB)
            instDiffMean = instMeanA - instMeanB
          else:
            instDiffMean = np.nan
          # counts
          count1 = len(trtA)
          count2 = len(trtB)
          # means
          meanA = np.mean(trtA)
          meanB = np.mean(trtB)
          cumDiffMean = meanA - meanB
          if (len(trtA) > 1) & (len(trtB) > 1):
            ttest = vector_TStats(trtA, trtB)
            tstat = ttest['T_Stat']
            pval = ttest['P_Value']
            degFree = ttest['DF']
            lower = ttest['CI']['Lower']
            upper = ttest['CI']['Upper']
          else:
            tstat = np.nan
            pval = np.nan
            degFree = np.nan
            lower = np.nan
            upper = np.nan
          newRow = ([dt,title,exp,trt1,trt2,
                     len(instA),len(instB),instMeanA,instMeanB,
                     count1,count2,meanA,meanB,
                     instDiffMean,cumDiffMean,
                     tstat,pval,lower,upper,degFree])
          newRow = pd.DataFrame([newRow],columns=columns)
          resultFrame = pd.concat([resultFrame,newRow])

# Write to file
resultFrame.to_csv("%s/out/SignificanceData.csv" % cwd, sep=',',index=False)
resultFrame.to_csv("/data/tmp/mwarpinski/analysis/output/SignificanceData.csv", sep=',',index=False)
"""

# Customer Allocation Data
customer_data = pd.crosstab((cleaned_data.ExperimentID),cleaned_data.TreatmentID).reset_index()
customer_data['fromDate'] = min(cleaned_data['Date'])
customer_data['toDate'] = max(cleaned_data['Date'])
cols = customer_data.columns.tolist()
cols = cols[-2:] + cols[:-2]
customer_data = customer_data[cols]


# Create customer counts file:
customer_data.to_csv("%s/out/customerCounts.csv" % cwd,index=False)
customer_data.to_csv("/data/tmp/mwarpinski/analysis/output/customerCounts.csv",index=False)

"""
# Traffic Allocation Data
traffic_data = pd.crosstab((cleaned_data.Date,cleaned_data.ExperimentID),cleaned_data.TreatmentID,margins=True).reset_index()

#traffic_data['Trt1'] = traffic_data['1'] / traffic_data['All']
#traffic_data['Trt2'] = traffic_data['2'] / traffic_data['All']


traffic_data = (traffic_data
               .sort_values(by=['ExperimentID','Date'])
               .query('ExperimentID !=0')
               .reset_index())

traffic_data.to_csv("%s/out/TrafficData.csv" % cwd, sep=',',index=False)
traffic_data.to_csv("/data/tmp/mwarpinski/analysis/output/TrafficData.csv", sep=',',index=False)


# sumamry data:
summary_df = (cleaned_data
              .groupby(['ExperimentID','TreatmentID'])
              .mean()
              .reset_index())

summary_df.to_csv("%s/out/SummaryData.csv" % cwd, sep=',',index=False)
summary_df.to_csv("/data/tmp/mwarpinski/analysis/output/SummaryData.csv", sep=',',index=False)
"""

# Build metrics data: Sample Metrics

# Initialize metrics dataframe:
metrics_cols = (['fromDate','toDate','ExperimentID','Metric','MetricType','Variant 1 UUID','Variant 2 UUID'
                 ,'Variant 1 Value','Variant 2 Value','Mean Effect','Significance','Mean Effect Value',
                 'Lower Bound','Upper Bound','Significance Flag'])
metricsFrame = (pd.DataFrame(columns=metrics_cols))

# Minutes watched
for exp in sorted(cleaned_data['ExperimentID'].unique()):
  expSubset = cleaned_data.query('ExperimentID == "%s"' % exp)
  fromDate = min(cleaned_data['Date'])
  toDate = max(cleaned_data['Date'])
  L = sorted(expSubset['TreatmentID'].unique())
  for trt1 in L:
    for trt2 in L:
      if trt1 != trt2:
        metric = 'Stream Minutes (avg) : OVP'
        metricType = 'Success'
        # TODO: filter by time
        trt1_vals = expSubset.loc[(expSubset.TreatmentID == trt1)]['Action']
        trt2_vals = expSubset.loc[(expSubset.TreatmentID == trt2)]['Action']
        mean1 = float(np.mean(trt1_vals))
        mean2 = float(np.mean(trt2_vals))
        ttest = vector_TStats(trt1_vals,trt2_vals)
        pval = ttest['P_Value']
        degFree = ttest['DF']
        lower = ttest['CI']['Lower']
        upper = ttest['CI']['Upper']
        diffMeans = float(mean1 - mean2)
        sig = "Not Significant" if pval > 0.05 else '{:,.1%}'.format((mean1-mean2)/mean1)
        sigFlag = "N" if pval > 0.05 else "Y" 
        newRow = ([fromDate,toDate,exp,metric,metricType,trt1,trt2,
                   "{0:.3f}".format(mean1),"{0:.2f}".format(mean2),
                   sig,"{0:.2f}".format(pval),"{0:.2f}".format(diffMeans),
                   "{0:.3f}".format(lower),"{0:.3f}".format(upper),sigFlag])
        newRow = pd.DataFrame([newRow],columns=metrics_cols)
        metricsFrame = pd.concat([metricsFrame,newRow])

# Errors
query = ("""
SELECT 
partition_date_denver as Date, 
ExperimentID, 
TreatmentID, 
event_error_code_count as Action
FROM (
select * from dynexp.dynexp_error_code_counts 
LATERAL VIEW EXPLODE(visit__experiment_uuids) temp as ExperimentID 
LATERAL VIEW EXPLODE(visit__variant_uuids) temp as TreatmentID
where ExperimentID = "59c192c011353d001d691c44"
and TreatmentID IN ("59c192c011353d001d691c46",
"59c192c011353d001d691c45",
"59c193b111353d001d691c47")) agg                
""")
errors = sqlContext.sql(query)
error_data = errors.toPandas()

error_data['Date'] = pd.to_datetime(error_data['Date'])
#error_data.to_csv("/data/tmp/mwarpinski/analysis/output/ErrorData.csv",index=False)

for exp in sorted(error_data['ExperimentID'].unique()):
  expSubset = error_data.query('ExperimentID == "%s"' % exp)
  fromDate = min(error_data['Date'])
  toDate = max(error_data['Date'])
  L = sorted(expSubset['TreatmentID'].unique())
  for trt1 in L:
    for trt2 in L:
      if trt1 != trt2:
        fromDate = min(error_data['Date'])
        toDate = max(error_data['Date'])
        metric = 'Errors (avg) : OVP'
        metricType = 'Safety'
        # TODO: filter by time
        # TODO: filter outliers
        trt1_vals = expSubset.loc[(expSubset.TreatmentID == trt1)]['Action']
        trt2_vals = expSubset.loc[(expSubset.TreatmentID == trt2)]['Action']
        mean1 = np.mean(trt1_vals)
        mean2 = np.mean(trt2_vals)
        ttest = vector_TStats(trt1_vals,trt2_vals)
        pval = ttest['P_Value']
        degFree = ttest['DF']
        lower = ttest['CI']['Lower']
        upper = ttest['CI']['Upper']
        diffMeans = float(mean1 - mean2)
        sig = "Not Significant" if pval > 0.05 else '{:,.1%}'.format((mean1-mean2)/mean1)
        sigFlag = "N" if pval > 0.05 else "Y" 
        newRow = ([fromDate,toDate,exp,metric,metricType,trt1,trt2,
                   "{0:.2f}".format(mean1),"{0:.2f}".format(mean2),
                   sig,"{0:.3f}".format(pval),"{0:.2f}".format(diffMeans),
                   "{0:.3f}".format(lower),"{0:.3f}".format(upper),sigFlag])
        newRow = pd.DataFrame([newRow],columns=metrics_cols)
        metricsFrame = pd.concat([metricsFrame,newRow])

      

# Pct to streaming
query = ("""select
partition_date_denver as Date,
ExperimentID,
TreatmentID,
case when minutes_watched > 0 then 1 else 0 end as Action
from (
select * from dynexp.dynexp_visit_agg
LATERAL VIEW EXPLODE(visit__experiment_uuids) temp as ExperimentID
LATERAL VIEW EXPLODE(visit__variant_uuids) temp as TreatmentID
where state__content__stream__playback_type = 'linear'
and ExperimentID = "59c192c011353d001d691c44"
and TreatmentID IN ("59c192c011353d001d691c46", "59c192c011353d001d691c45", "59c193b111353d001d691c47")) agg
""")
pctStreaming = sqlContext.sql(query).toPandas()
pctStreaming['Date'] = pd.to_datetime(pctStreaming['Date'])

for exp in sorted(pctStreaming['ExperimentID'].unique()):
  expSubset = pctStreaming.query('ExperimentID == "%s"' % exp)
  fromDate = min(pctStreaming['Date'])
  toDate = max(pctStreaming['Date'])
  L = sorted(expSubset['TreatmentID'].unique())
  for trt1 in L:
    for trt2 in L:
      if trt1 != trt2:
        metric = 'Streams (unique visit %) : OVP : Live TV'
        metricType = 'Behavior'
        # TODO: filter by time
        # TODO: filter outliers
        trt1_vals = expSubset.loc[(expSubset.TreatmentID == trt1)]['Action']
        trt2_vals = expSubset.loc[(expSubset.TreatmentID == trt2)]['Action']
        mean1 = np.mean(trt1_vals)
        mean2 = np.mean(trt2_vals)
        ttest = vector_TStats(trt1_vals,trt2_vals)
        pval = ttest['P_Value']
        degFree = ttest['DF']
        lower = ttest['CI']['Lower']
        upper = ttest['CI']['Upper']
        diffMeans = float(mean1 - mean2)
        sig = "Not Significant" if pval > 0.05 else '{:,.1%}'.format((mean1-mean2)/mean1)
        sigFlag = "N" if pval > 0.05 else "Y" 
        newRow = ([fromDate,toDate,exp,metric,metricType,trt1,trt2,
                   "{0:.2f}".format(mean1),"{0:.2f}".format(mean2),
                   sig,"{0:.3f}".format(pval),"{0:.2f}".format(diffMeans),
                   "{0:.3f}".format(lower),"{0:.3f}".format(upper),sigFlag])
        newRow = pd.DataFrame([newRow],columns=metrics_cols)
        metricsFrame = pd.concat([metricsFrame,newRow])
            
# Live streaming minutes
        
        
query = ("""select
visit__visit_id,
partition_date_denver as Date,
ExperimentID,
TreatmentID,
sum(minutes_watched) as Action
from (
select * from dynexp.dynexp_visit_agg
LATERAL VIEW EXPLODE(visit__experiment_uuids) temp as ExperimentID
LATERAL VIEW EXPLODE(visit__variant_uuids) temp as TreatmentID
where (state__content__stream__playback_type = 'linear'
or state__content__stream__playback_type is null)
and ExperimentID = "59c192c011353d001d691c44"
and TreatmentID IN ("59c192c011353d001d691c46", "59c192c011353d001d691c45", "59c193b111353d001d691c47")) agg
group by visit__visit_id, partition_date_denver, ExperimentID, TreatmentID
""")
minsStreaming = sqlContext.sql(query).toPandas()
minsStreaming['Date'] = pd.to_datetime(minsStreaming['Date'])

for exp in sorted(minsStreaming['ExperimentID'].unique()):
  expSubset = minsStreaming.query('ExperimentID == "%s"' % exp)
  fromDate = min(minsStreaming['Date'])
  toDate = max(minsStreaming['Date'])
  L = sorted(expSubset['TreatmentID'].unique())
  for trt1 in L:
    for trt2 in L:
      if trt1 != trt2:
        metric = 'Stream Minutes (avg) : OVP : Live TV'
        metricType = 'Behavior'
        # TODO: filter by time
        # TODO: filter outliers
        trt1_vals = expSubset.loc[(expSubset.TreatmentID == trt1)]['Action']
        trt2_vals = expSubset.loc[(expSubset.TreatmentID == trt2)]['Action']
        mean1 = np.mean(trt1_vals)
        mean2 = np.mean(trt2_vals)
        ttest = vector_TStats(trt1_vals,trt2_vals)
        pval = ttest['P_Value']
        degFree = ttest['DF']
        lower = ttest['CI']['Lower']
        upper = ttest['CI']['Upper']
        diffMeans = float(mean1 - mean2)
        sig = "Not Significant" if pval > 0.05 else '{:,.1%}'.format((mean1-mean2)/mean1)
        sigFlag = "N" if pval > 0.05 else "Y" 
        newRow = ([fromDate,toDate,exp,metric,metricType,trt1,trt2,
                   "{0:.2f}".format(mean1),"{0:.2f}".format(mean2),
                   sig,"{0:.3f}".format(pval),"{0:.2f}".format(diffMeans),
                   "{0:.3f}".format(lower),"{0:.3f}".format(upper),sigFlag])
        newRow = pd.DataFrame([newRow],columns=metrics_cols)
        metricsFrame = pd.concat([metricsFrame,newRow])
            
            
# Button Clicks       
query = ("""select
            partition_date_denver as Date,
            TreatmentID,
            state__view__previous_page__page_name as Previous,
            state__view__current_page__page_name as Current,
            event_user_action_count as Action
            from (
            select * from dynexp.dynexp_navigation_page_counts 
            lateral view explode(visit__variant_uuids) temp as TreatmentID
            where TreatmentID in 
            ("59c192c011353d001d691c46",
            "59c192c011353d001d691c45",
            "59c193b111353d001d691c47")) agg
            where state__view__previous_page__page_name in 
            ("playerLiveTv",
            "guide",
            "myLibrary",
            "curatedFeatured",
            "curatedTvShows",
            "curatedMovies",
            "curatedKids",
            "curatedNetworks",
            "settingsFavorites",
            "settingsParentalControls")
            and state__view__current_page__page_name in 
            ("playerLiveTv",
            "guide",
            "myLibrary",
            "curatedFeatured",
            --"curatedTvShows",
            --"curatedMovies",
            --"curatedKids",
            --"curatedNetworks",
            "settingsFavorites",
            --"settingsParentalControls",
            "search")
            and state__view__previous_page__page_name != state__view__current_page__page_name
""")
buttons = sqlContext.sql(query).toPandas()
buttons['Date'] = pd.to_datetime(buttons['Date'])

venona_buttons = (["playerLiveTv",
                   "guide",
                    "myLibrary",
                    "curatedFeatured",
                    "settingsFavorites",
                    "search"])
prod_buttons = (["Clicks (count): Global Nav: Live TV",
                 "Clicks (count): Global Nav: Guide",
                 "Clicks (count): Global Nav: My Library",
                 "Clicks (count): Global Nav: On Demand",
                 "Clicks (count): Global Nav: Settings",
                 "Clicks (count): Search"])
  
button_dict = {}

for i in range(len(venona_buttons)):
  button_dict[venona_buttons[i]] = prod_buttons[i]

for button in sorted(buttons['Current'].unique()):
  buttonSubset = buttons.query('Current == "%s"' % button)
  fromDate = min(buttons['Date'])
  toDate = max(buttons['Date'])
  L = sorted(buttonSubset['TreatmentID'].unique())
  for trt1 in L:
    for trt2 in L:
      if trt1 != trt2:
        if button == 'curatedFeatured':
          buttonSubset = (buttonSubset[buttonSubset.Previous.isin(["playerLiveTv",
                                                                   "guide",
                                                                   "myLibrary",
                                                                   "settingsFavorites"])])
        exp = '59c192c011353d001d691c44'
        # Name metric            
        metric = button_dict[button]
        metricType = 'Behavior'
        # TODO: filter by time
        # TODO: filter outliers
        trt1_vals = buttonSubset.loc[(buttonSubset.TreatmentID == trt1)]['Action']
        trt2_vals = buttonSubset.loc[(buttonSubset.TreatmentID == trt2)]['Action']
        mean1 = np.mean(trt1_vals)
        mean2 = np.mean(trt2_vals)
        ttest = vector_TStats(trt1_vals,trt2_vals)
        pval = ttest['P_Value']
        degFree = ttest['DF']
        lower = ttest['CI']['Lower']
        upper = ttest['CI']['Upper']
        diffMeans = float(mean1 - mean2)
        sig = "Not Significant" if pval > 0.05 else '{:,.1%}'.format((mean1-mean2)/mean1)
        sigFlag = "N" if pval > 0.05 else "Y" 
        newRow = ([fromDate,toDate,exp,metric,metricType,trt1,trt2,
                   "{0:.2f}".format(mean1),"{0:.2f}".format(mean2),
                   sig,"{0:.3f}".format(pval),"{0:.2f}".format(diffMeans),
                   "{0:.3f}".format(lower),"{0:.3f}".format(upper),sigFlag])
        newRow = pd.DataFrame([newRow],columns=metrics_cols)
        metricsFrame = pd.concat([metricsFrame,newRow])

# Churn        
query = ("""select
partition_date_denver as Date,
account_number,
TreatmentID,
case when customer__disconnect_date is not null then 1 else 0 end as Action
from (
select * from dynexp.dynexp_accounts
lateral view explode(variant_uuids) temp as TreatmentID
where TreatmentID IN ("59c192c011353d001d691c46", "59c192c011353d001d691c45", "59c193b111353d001d691c47")) agg
""")

churn = sqlContext.sql(query).toPandas()
churn['Date'] = pd.to_datetime(churn['Date'])

L = sorted(churn['TreatmentID'].unique())
fromDate = min(churn['Date'])
toDate = max(churn['Date'])
for trt1 in L:
  for trt2 in L:
    if trt1 != trt2:
      exp = '59c192c011353d001d691c44'
      # Name metric            
      metric = 'Churn Rate'
      metricType = 'Safety'
      # TODO: filter by time
      # TODO: filter outliers
      trt1_vals = churn.loc[(churn.TreatmentID == trt1)]['Action']
      trt2_vals = churn.loc[(churn.TreatmentID == trt2)]['Action']
      mean1 = np.mean(trt1_vals)
      mean2 = np.mean(trt2_vals)
      ttest = vector_TStats(trt1_vals,trt2_vals)
      pval = ttest['P_Value']
      degFree = ttest['DF']
      lower = ttest['CI']['Lower']
      upper = ttest['CI']['Upper']
      diffMeans = float(mean1 - mean2)
      sig = "Not Significant" if pval > 0.05 else '{:,.1%}'.format((mean1-mean2)/mean1)
      sigFlag = "N" if pval > 0.05 else "Y" 
      newRow = ([fromDate,toDate,exp,metric,metricType,trt1,trt2,
                 "{0:.4f}".format(mean1),"{0:.4f}".format(mean2),
                 sig,"{0:.3f}".format(pval),"{0:.4f}".format(diffMeans),
                 "{0:.3f}".format(lower),"{0:.3f}".format(upper),sigFlag])
      newRow = pd.DataFrame([newRow],columns=metrics_cols)
      metricsFrame = pd.concat([metricsFrame,newRow])

# Visits per customer
query = ("""select
partition_date_denver as Date,
visit__account__account_billing_id as custID,
count(visit__visit_id) as Action,
TreatmentID
from (
select * from dynexp.dynexp_visit_agg
lateral view explode(visit__variant_uuids) temp as TreatmentID
where TreatmentID IN ("59c192c011353d001d691c46", "59c192c011353d001d691c45", "59c193b111353d001d691c47")) agg
group by partition_date_denver, visit__account__account_billing_id, TreatmentID
""")

visits = sqlContext.sql(query).toPandas()
visits['Date'] = pd.to_datetime(visits['Date'])

L = sorted(visits['TreatmentID'].unique())
fromDate = min(visits['Date'])
toDate = max(visits['Date'])
for trt1 in L:
  for trt2 in L:
    if trt1 != trt2:
      exp = '59c192c011353d001d691c44'
      # Name metric            
      metric = 'Visits per Day (avg)'
      metricType = 'Behavior'
      # TODO: filter by time
      # TODO: filter outliers
      trt1_vals = visits.loc[(visits.TreatmentID == trt1)]['Action']
      trt2_vals = visits.loc[(visits.TreatmentID == trt2)]['Action']
      mean1 = np.mean(trt1_vals)
      mean2 = np.mean(trt2_vals)
      ttest = vector_TStats(trt1_vals,trt2_vals)
      pval = ttest['P_Value']
      degFree = ttest['DF']
      lower = ttest['CI']['Lower']
      upper = ttest['CI']['Upper']
      diffMeans = float(mean1 - mean2)
      sig = "Not Significant" if pval > 0.05 else '{:,.1%}'.format((mean1-mean2)/mean1)
      sigFlag = "N" if pval > 0.05 else "Y" 
      newRow = ([fromDate,toDate,exp,metric,metricType,trt1,trt2,
                 "{0:.2f}".format(mean1),"{0:.2f}".format(mean2),
                 sig,"{0:.3f}".format(pval),"{0:.1f}".format(diffMeans),
                 "{0:.3f}".format(lower),"{0:.3f}".format(upper),sigFlag])
      newRow = pd.DataFrame([newRow],columns=metrics_cols)
      metricsFrame = pd.concat([metricsFrame,newRow])

# bitrate
      
query = ("""select  
agg.Dte as Date,
agg.custID as CustID,
agg.Action as Action,
agg.TreatmentID as TreatmentID        
from(
select
agg2.partition_date_denver as Dte,
agg2.visit__account__account_billing_id as custID,
sum(agg2.visit_avg_bitrate * agg2.minutes_watched)/sum(agg2.minutes_watched) as Action,
agg2.TreatmentID
from (
select * from dynexp.dynexp_visit_agg
lateral view explode(visit__variant_uuids) temp as TreatmentID
where TreatmentID IN ("59c192c011353d001d691c46", "59c192c011353d001d691c45", "59c193b111353d001d691c47")) agg2
group by partition_date_denver, visit__account__account_billing_id, TreatmentID
) agg where agg.Action is not null""")
  
bitrate = sqlContext.sql(query).toPandas()
bitrate['Date'] = pd.to_datetime(bitrate['Date'])

bitrate = sqlContext.sql(query).toPandas()
fromDate = min(bitrate['Date'])
toDate = max(bitrate['Date'])
L = sorted(bitrate['TreatmentID'].unique())
for trt1 in L:
  for trt2 in L:
    if trt1 != trt2:
      exp = '59c192c011353d001d691c44'
      # Name metric            
      metric = 'Weighted Avg Bitrate'
      metricType = 'Safety'
      # TODO: filter by time
      # TODO: filter outliers
      trt1_vals = bitrate.loc[(bitrate.TreatmentID == trt1)]['Action']
      trt2_vals = bitrate.loc[(bitrate.TreatmentID == trt2)]['Action']
      mean1 = np.mean(trt1_vals)
      mean2 = np.mean(trt2_vals)
      ttest = vector_TStats(trt1_vals,trt2_vals)
      pval = ttest['P_Value']
      degFree = ttest['DF']
      lower = ttest['CI']['Lower']
      upper = ttest['CI']['Upper']
      diffMeans = float(mean1 - mean2)
      sig = "Not Significant" if pval > 0.05 else '{:,.1%}'.format((mean1-mean2)/mean1)
      sigFlag = "N" if pval > 0.05 else "Y" 
      newRow = ([fromDate,toDate,exp,metric,metricType,trt1,trt2,
                 "{0:.2f}".format(mean1),"{0:.2f}".format(mean2),
                 sig,"{0:.3f}".format(pval),"{0:.1f}".format(diffMeans),
                 "{0:.3f}".format(lower),"{0:.3f}".format(upper),sigFlag])
      newRow = pd.DataFrame([newRow],columns=metrics_cols)
      metricsFrame = pd.concat([metricsFrame,newRow])

     
# Time in app
      
query = ("""select
partition_date_denver as Date,
visit__account__account_billing_id as custID,
sum((visit_end_timestamp - visit_start_timestamp)/60000) as visitTime,
TreatmentID
from (
select * from dynexp.dynexp_visit_agg
lateral view explode(visit__variant_uuids) temp as TreatmentID
where TreatmentID IN ("59c192c011353d001d691c46", "59c192c011353d001d691c45", "59c193b111353d001d691c47")) agg
group by partition_date_denver, visit__account__account_billing_id, treatmentID
""")

time = sqlContext.sql(query).toPandas()
time['Date'] = pd.to_datetime(time['Date'])
fromDate = min(time['Date'])
toDate = max(time['Date'])
L = sorted(time['TreatmentID'].unique())
for trt1 in L:
  for trt2 in L:
    if trt1 != trt2:
      exp = '59c192c011353d001d691c44'
      # Name metric            
      metric = 'Time in App (Mins): (OVP)'
      metricType = 'Behavior'
      # TODO: filter by time
      # TODO: filter outliers
      trt1_vals = time.loc[(time.TreatmentID == trt1)]['visitTime']
      trt2_vals = time.loc[(time.TreatmentID == trt2)]['visitTime']
      mean1 = np.mean(trt1_vals)
      mean2 = np.mean(trt2_vals)
      ttest = vector_TStats(trt1_vals,trt2_vals)
      pval = ttest['P_Value']
      degFree = ttest['DF']
      lower = ttest['CI']['Lower']
      upper = ttest['CI']['Upper']
      diffMeans = float(mean1 - mean2)
      sig = "Not Significant" if pval > 0.05 else '{:,.1%}'.format((mean1-mean2)/mean1)
      sigFlag = "N" if pval > 0.05 else "Y" 
      newRow = ([fromDate,toDate,exp,metric,metricType,trt1,trt2,
                 "{0:.2f}".format(mean1),"{0:.2f}".format(mean2),
                 sig,"{0:.3f}".format(pval),"{0:.1f}".format(diffMeans),
                 "{0:.3f}".format(lower),"{0:.3f}".format(upper),sigFlag])
      newRow = pd.DataFrame([newRow],columns=metrics_cols)
      metricsFrame = pd.concat([metricsFrame,newRow])
            
metricsFrame = metricsFrame.loc[(metricsFrame.ExperimentID == '59c192c011353d001d691c44')]            
metricsFrame.to_csv("%s/out/SampleMetrics.csv" % cwd,index=False)
metricsFrame.to_csv("/data/tmp/mwarpinski/analysis/output/SampleMetrics.csv",index=False)