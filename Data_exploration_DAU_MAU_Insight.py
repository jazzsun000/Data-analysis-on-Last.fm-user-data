#!/usr/bin/env python
# coding: utf-8

# # DAU

# ## Data cleaning and formating

# In[1]:


import pandas as pd
import numpy as np


# In[2]:


#Use pandas function to read the tsv file in:
#Assign the result to a variable named “df”

df = pd.read_csv('userid-profile.tsv', sep='\t')


# In[3]:


#Use pandas function to read the tsv file in:
#Assign the result to a variable named “dr”
#Use error_bad_lines to skip error message
#Add header into dataframe
#userid \t timestamp \t musicbrainz-artist-id \t artist-name \t musicbrainz-track-id \t track-name

dr = pd.read_csv('userid-timestamp-artid-artname-traid-traname.tsv', sep='\t', error_bad_lines=False,                 names=["userid", "timestamp", "musicbrainz-artist-id", "artist-name","musicbrainz-track-id","track-name"])


# In[4]:


#"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
# this line converts the string object in Timestamp object
import datetime
dr['timestamp'] = [datetime.datetime.strptime(d, "%Y-%m-%dT%H:%M:%SZ") for d in dr["timestamp"]]


# In[5]:


dr['Date'] = [datetime.datetime.date(d) for d in dr['timestamp']]


# In[6]:


dr.describe()


# In[7]:


#Overview missing data


# In[8]:


for column in list(dr.columns):
    print ("{}% of the data from {} column is missing".format(round(dr[column].isnull().sum() * 100 / len(dr[column]),2), column))


# In[9]:


#count unique user id by date(year-month-day)
dau=dr.groupby('Date').userid.nunique()


# In[10]:


#convert series data to dataframe with to_frame() and reset index

dau_df = dau.to_frame()
dau_df.reset_index(inplace=True)
dau_df.head()


# In[11]:


#rename column userid to dau
dau_df = dau_df.rename(columns={'userid': 'dau'})
dau_df.head()


# In[12]:


#create a new column with year of date field 'year'
#pandas datetimeindex docs: https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DatetimeIndex.html
#efficient way to extract year from string format date
dau_df['year'] = pd.DatetimeIndex(dau_df['Date']).year
dau_df.head()


# In[13]:


#create a new column with month of date field 'month'
#pandas datetimeindex docs: https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DatetimeIndex.html
#efficient way to extract year from string format date
dau_df['month'] = pd.DatetimeIndex(dau_df['Date']).month
dau_df.head()


# In[14]:


#if the date format comes in datetime, we can also extract the day/month/year using the to_period function
#where 'D', 'M', 'Y' are inputs
dau_df['month_year'] = pd.to_datetime(dau_df['Date']).dt.to_period('M')
dau_df.head()


# In[15]:


import plotly.express as px


# In[96]:


fig = px.line(dau_df, x="Date", y="dau", title='DAU trend by date')
fig.show()


# # DAU/MAU

# In[17]:


#Task b:a graph of Daily Active Users divided by Monthly Active Users (or DAU/MAU);
#First, we need to calculate the correspond MAU


# In[18]:


#create a new column with year of date field 'year'
#pandas datetimeindex docs: https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DatetimeIndex.html
#efficient way to extract year from string format date
dr['year'] = pd.DatetimeIndex(dr['Date']).year
dr['month'] = pd.DatetimeIndex(dr['Date']).month
#if the date format comes in datetime, we can also extract the day/month/year using the to_period function
#where 'D', 'M', 'Y' are inputs
dr['month_year'] = pd.to_datetime(dr['Date']).dt.to_period('M')


# In[19]:


dr.head()


# In[20]:


#MAU
#count unique user id by month_year(year-month)
mau=dr.groupby('month_year').userid.nunique()
mau.head()


# In[21]:


#convert series data to dataframe with to_frame() and reset index
mau_df = mau.to_frame()
mau_df.reset_index(inplace=True)
mau_df.head()


# In[22]:


#rename column userid to mau
mau_df = mau_df.rename(columns={'userid': 'mau'})
mau_df.head()


# In[23]:


#Inner join data. Retain only rows in both sets.
#Use month_year as key
dau_mau_df=pd.merge(dau_df, mau_df, how='inner', on='month_year')
dau_mau_df.head()


# In[24]:


#dau/mau calculation
dau_mau_df['dau/mau']=dau_mau_df['dau']/dau_mau_df['mau']
dau_mau_df.head()


# In[25]:


new_dau_mau_df=dau_mau_df.drop(columns=['dau','year','month','month_year','mau'])
new_dau_mau_df.head()


# In[26]:


fig = px.line(new_dau_mau_df, x="Date", y="dau/mau", title='DAU/MAU trend by date')
fig.show()


# ## The graph show us there is a strong stickness of user on Last.fm music service since its dau/mau ratio keep above 50%.

# In[27]:


#Industry Benchmarks
#Of course, the closer to 100% engagement your product has, the better. When it comes to average benchmarks though, the ‘norm’ varies significantly between products, type of engagement, and industry. Look at the DAU/MAU Ratios for companies with a similar type of product or in the same industry.
#Sequoia tweeted the standard DAU/MAU ratio is 10-20% with only a handful of companies over 50%.
#Reference from https://www.geckoboard.com/best-practice/kpi-examples/dau-mau-ratio/


# # A graph or two that illustrates something about how these users engage with Last.fm. 

# In[28]:


import pandas as pd


# In[29]:


#It took too much time on computing the raw event data, so we use sample data from merged-subset.csv

ds = pd.read_csv('merged-subset.csv')
ds.head()


# ### Data cleaning and formating

# In[30]:


#"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
# this line converts the string object in Timestamp object
import datetime
ds['song_play_start_time'] = [datetime.datetime.strptime(d, "%Y-%m-%dT%H:%M:%SZ") for d in ds["song_play_start_time"]]
ds.head()


# In[31]:


#Order sample data by userId','song_play_start_time'
sa_df=ds.sort_values(['userId','song_play_start_time'], ascending=[1,0])
sa_df.head()


# In[32]:


#drop duplicated column
sa_df=sa_df.drop(columns=['gender_y','age_y','country_y','signupDate_y'])


# In[33]:


sa_df=sa_df.rename(columns={'gender_x': 'gender','age_x':'age','country_x':'country','signupDate_x':'signupDate'})


# In[34]:


sa_df.head()


# ### To calculate session length

# In[35]:


def time_passed(grp, session_length):
    grp['MinsElapsed'] = (grp['song_play_start_time'] - grp['song_play_start_time'].shift(-1)) / pd.Timedelta(minutes=1)
    grp['Session'] = (grp['MinsElapsed'] > session_length)[::-1].astype(int).cumsum()[::-1]
    return grp


# In[36]:


#Assumption:A session is defined where each song is started within 20 minutes of the previous song’s start time.
#We group by user, and work out the time difference between the row below (.shift(-1)) in minutes. 
#We then check if this column returns a value larger than the session length, convert that to an integer and apply a cumulative sum. 

sa_df['song_play_start_time'] = pd.to_datetime(sa_df['song_play_start_time'])

sa_df = sa_df.groupby('userId').apply(time_passed, session_length=20)

print(sa_df)


# In[37]:


sa_df.head()


# In[38]:


#To get the first and last times on songs in the session and the length of the session
session_length = sa_df.groupby(['userId', 'Session'])['song_play_start_time']                    .agg(['min', 'max'])                    .reset_index()

session_length['Length (mins)'] = (session_length['max'] -session_length['min']) / pd.Timedelta(minutes=1)


# In[39]:


session_length.head()


# In[40]:


import plotly.express as px
#df = px.data.tips()
fig = px.histogram(session_length, x="Length (mins)",
                   title='Histogram of session length',
                   labels={'Length (mins)':'Session Length (mins)'}, # can specify one label per df column
                   opacity=0.6,
                   log_y=True, # represent bars with log scale
                   color_discrete_sequence=['indianred'] # color of histogram bars
                   )
fig.show()


# In[41]:


session_length['Length (mins)'].describe()


# ### The average time of session is around 69 mins.

# In[42]:


import plotly.express as px
#df = px.data.tips()
fig = px.box(session_length, y="Length (mins)")
fig.show()


# ### However, there are almost 50% of users stop using the service after 32 mins.

# ### As a result, I would recommend musics ,marketing campaigns and advertisement monetization within 32mins when users start listening to music.

# ## User frequency of use:How many sessions per day by each user?

# In[43]:


session_length.head()


# In[44]:


import datetime
session_length['Date'] = [datetime.datetime.date(d) for d in session_length['min']]
session_length.head()


# In[45]:


session_count=session_length.groupby(['userId','Date']).userId.count()
session_count.head()


# In[46]:


session_count=session_count.rename(columns={'userid': 'count_of_sessions'})


# In[47]:


session_count=session_count.reset_index()
session_count.head()


# In[48]:


session_count=session_count.rename(columns={0: 'count_of_sessions'})
session_count.head()


# In[49]:


import plotly.express as px
#df = px.data.tips()
fig = px.box(session_count, y="count_of_sessions")
fig.show()


# In[50]:


import plotly.express as px
#df = px.data.tips()
fig = px.histogram(session_count, x="count_of_sessions",
                   title='Histogram of count of daily sessions',
                   labels={'count_of_sessions':'Frequency_of_daily_use(daily_sessions)'}, # can specify one label per df column
                   opacity=0.8,
                   log_y=True, # represent bars with log scale
                   color_discrete_sequence=['indianred'] # color of histogram bars
                   )
fig.show()


# In[51]:


session_count["count_of_sessions"].describe()


# ## The user daily usage average of frequency is around 2.78 times per day

# ### To get each song play times with user internal groups calculation

# In[52]:


#To get song played time with group calculation on groupby and transform
#After checking the song manually, the timestamp should be song start time
#shift(-1) will copy with values lagged by 1, then we use it to calculate each song play duration

sa_df['Duration']=sa_df.groupby(['userId'])['song_play_start_time'].transform(lambda x: x-x.shift(-1))


# In[53]:


#Overview each song duration with describe
sa_df['Duration'].describe()


# ### The information tell us 50 quantile song played duration is around 4 minutes
# 

# ### 75 quantile duration is around 5.5 minutes

# In[54]:


# Add additional time columns for more interpretable times
sa_df['minutes_played']=sa_df['Duration'].dt.total_seconds().div(60)


# In[55]:


#To get the 90 quantile on minutes_played of songs
sa_df['minutes_played'].quantile(0.90)


# ### 90 quantile duration is around 10.3 minutes

# In[ ]:





# In[56]:


#sa_df=sa_df.groupby(['userId','song_play_start_time','artist-name','track-name','gender','age','country','signupDate']).(lambda x: x['song_play_start_time'].shift(-1)-x['song_play_start_time'])


# In[57]:


#After checking the song manually, the timestamp should be song start time
#shift(-1) will copy with values lagged by 1, then we use it to calculate each song play duration
#sa_df['Duration']=sa_df['song_play_start_time'].shift(-1)-sa_df['song_play_start_time']
#sa_df.head()


# In[58]:


# Add additional time columns for more interpretable times
#sa_df['minutes_played']=sa_df['Duration'].dt.total_seconds().div(60)


# # Most popular artists -what users listen to?

# In[59]:


# Find the most popular artists by number of times played-top25

most_popular_artists_by_count = sa_df.groupby(by='artist-name')['track-name'].count().sort_values(ascending=False)[:25]

print('The most played artists by count were: \n\n{}'.format(most_popular_artists_by_count))


# In[60]:


import chart_studio
chart_studio.tools.set_credentials_file(username='jazzsun', api_key='FGsTIpbZUlUVZb0Iae3J')
#plotly.tools.set_credentials_file(username=(jazzsun), api_key=(FGsTIpbZUlUVZb0Iae3J))
import chart_studio.plotly as py
import plotly.graph_objs as go


# In[95]:


# Visualize the most popular artists with a standard bar chart

data = [
    
    go.Bar(
            x=most_popular_artists_by_count.index,
            y=most_popular_artists_by_count,
            text=most_popular_artists_by_count,
            textposition='auto',
            opacity=0.75
            
    )]

layout = go.Layout(
    title='Popularity of Artists by played times',
    
    yaxis= dict(
        title='Number of Times Played',
        gridcolor='rgb(255, 255, 255)',
        zerolinewidth=1,
        ticklen=5,
        gridwidth=2,
        titlefont=dict(size=15))
)

fig = go.Figure(data=data, layout=layout)
        
py.iplot(fig, filename='popular_artists')


# In[62]:


# Look at most popular artists by amount of time played-top20

most_popular_artists_by_time = sa_df.groupby(by='artist-name')['minutes_played'].sum().sort_values(ascending=False)[:20]

most_popular_artists_by_time


# In[63]:


sa_df=sa_df.rename(columns={'track-name': 'track_name'})


# In[64]:


sa_df.head()


# In[65]:


# Look at the most popular songs played-top20

most_popular_songs = sa_df.track_name.value_counts().sort_values(ascending=False)[:20]

most_popular_songs


# In[94]:


# Visualize the most popular songs with a standard bar chart

data = [
    
    go.Bar(
            x=most_popular_songs.index,
            y=most_popular_songs,
            text=most_popular_songs,
            textposition='auto',
            opacity=0.75
            
    )]

layout = go.Layout(
    title='Popularity of Songs by Playtimes',
    
    yaxis= dict(
        title='Number of Times Played',
        gridcolor='rgb(255, 255, 255)',
        zerolinewidth=1,
        ticklen=5,
        gridwidth=2,
        titlefont=dict(size=15))
)

fig = go.Figure(data=data, layout=layout)
        
py.iplot(fig, filename='popular_songs')


# # Time of day to listen-Need more time to research

# In[67]:


sa_df.describe()


# In[68]:


# Create time of day variable

def time_of_day(datetime_column, df=sa_df):
    
    """
    Takes in a datetime column and returns the time of day that the datetime happens.
    
    Before 12 PM is considered morning, between 12 PM and 5 PM afternoon, and after 5 PM evening.
    """
    
    time_of_day = []
    
    for i in df[datetime_column]:
        
        i = i.hour
        
        if i <= 12:
            
            time_of_day.append('morning')
            
        elif i < 17:
            
            time_of_day.append('afternoon')
            
        else:
            
            time_of_day.append('night')
    
    time_of_day = pd.Categorical(time_of_day, categories=['morning','afternoon','night'], ordered=True)
            
    return time_of_day


# In[69]:


from datetime import datetime
import time

def datetime_from_utc_to_local(utc_datetime):
    
    """
    Converts a column from a UTC timestamp to local time, then returns the local time.
    """
    now_timestamp = time.time()
    offset = datetime.fromtimestamp(now_timestamp) - datetime.utcfromtimestamp(now_timestamp)
    return utc_datetime + offset


# In[70]:


## Convert from UTC time to eastern time

sa_df['local_time'] = datetime_from_utc_to_local(sa_df.song_play_start_time)
sa_df['local_time_of_day'] = time_of_day('local_time')


# In[71]:


# Add day of week and organize days as categories

sa_df['local_day_of_week'] = sa_df['local_time'].dt.day_name()

sa_df['local_day_of_week'] = pd.Categorical(sa_df['local_day_of_week'], 
                                   categories=['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday', 'Sunday'], 
                                   ordered=True)


# In[72]:


## Create a pivot table by time of day and day of week

time_of_day_local_pivot = sa_df.pivot_table(columns='local_time_of_day', index='local_day_of_week', 
                                             values='minutes_played', aggfunc=np.sum)

start_date = sa_df.local_time.min()

end_date = sa_df.local_time.max()


difference_in_weeks = (end_date - start_date).days / (7)
time_of_day_utc_pivot = time_of_day_local_pivot.divide(difference_in_weeks)


# In[73]:


time_of_day_local_pivot


# In[74]:


#time_of_day_local_pivot/278


# In[75]:


#sa_df.groupby(['userId']).local_time.min()


# In[76]:


#sa_df.groupby(['userId']).local_time.max()


# In[77]:


#time_of_day_utc_pivot.values/278


# In[78]:


time_of_day_utc_pivot.values


# In[79]:


trace = go.Heatmap(z=time_of_day_utc_pivot.values,
                  x=time_of_day_utc_pivot.columns,
                  y=time_of_day_utc_pivot.index,
                  colorscale='Greens',
                  reversescale=True)
data=[trace]
py.iplot(data, filename='lastfm_sample_user_heatmap')


# # Number of minutes listened per day-Need more time to research

# In[80]:


# Determine the number of minutes per day that users listened to songs

number_of_minutes_per_day = sa_df.set_index('local_time')
number_of_minutes_per_day = pd.DataFrame(number_of_minutes_per_day.groupby(                            by=number_of_minutes_per_day.index.date)['minutes_played'].sum())


# In[81]:


number_of_minutes_per_day.head()


# In[ ]:





# In[ ]:





# In[ ]:




