import datetime
import numpy as np
import pandas as pd
import os
import glob
import eikon as ek
from bs4 import BeautifulSoup
from textblob import TextBlob
from pandas.api.types import is_numeric_dtype

def company_headlines(company):
    """
    Function to get the news headlines of a specific company
    WIP
    """
    
    query = "PresetTopic:[ESG: Social] AND Language:LEN AND R:"+company
    res = pd.DataFrame()
    date_max = datetime.datetime(2024,1,1).strftime('%Y/%m/%d %H:%M:%S')
    #date_min = (datetime.datetime.today()+datetime.timedelta(days=-456.25)).strftime('%Y/%m/%d %H:%M:%S') #15months ~ 456.25days
    
    #Initialization
    df = ek.get_news_headlines(query, date_to = date_max, count=100)
    if not df.empty:
        date_max = df['versionCreated'].min().to_pydatetime().strftime('%Y/%m/%d %H:%M:%S')
        res = res.append(df)
        res_storyId = res[res["versionCreated"] == res['versionCreated'].min()]["storyId"]

        df = ek.get_news_headlines(query, date_to = date_max, count=100)

        #Iteration
        while not (res_storyId.reset_index(drop=True) == df[df["versionCreated"] == df['versionCreated'].min()] ["storyId"].reset_index(drop=True)).all():
            res = res.append(df)
            res_storyId = res[res["versionCreated"] == res['versionCreated'].min()]["storyId"]
            date_max = df['versionCreated'].min().to_pydatetime().strftime('%Y/%m/%d %H:%M:%S')
            df = ek.get_news_headlines(query, date_to = date_max, count=100)
    return res

#Eikon carries a maximum of 15 months worth of news - which is a massive amount already. To go back further you need to use a different product called News Archive which is part of our News Analytics product group. Please reach out to your account manager who can arrange a trial for you. I hope this can help.
#https://community.developers.refinitiv.com/questions/81066/i-want-to-use-the-ekget-newsheadlines-and-ekget-ne.html

def comapny_sentiment_score(df):
    """
    Function to calcul the Polarity, Subjectivity and Sentiment Score of a news using TextBlob
    parameters df: a DataFrame that include the new headlines, date and storyId (PK) of a specific company
    The function call ek.get_news_story to get the news based on the storyId and TextBlob to calculate the sentiment score
    """
    df['Polarity'] = np.nan
    df['Subjectivity'] = np.nan
    df['Sentiment Score'] = np.nan
    df_err=[]
    
    for idx, storyId in enumerate(df['storyId'].values):  #for each row in our df dataframe
        
        try:
            newsText = ek.get_news_story(storyId) #get the news story
        except ek.EikonError as err:
            df_err.append(err)
        
        if newsText:
            soup = BeautifulSoup(newsText,"lxml") #create a BeautifulSoup object from our HTML news article
            df['newsText'].iloc[idx] = soup.get_text()
            sentA = TextBlob(soup.get_text()) #pass the text only article to TextBlob to anaylse
            df['Polarity'].iloc[idx] = sentA.sentiment.polarity #write sentiment polarity back to df
            df['Subjectivity'].iloc[idx] = sentA.sentiment.subjectivity #write sentiment subjectivity score back to df
            if sentA.sentiment.polarity >= 0.05: # attribute bucket to sentiment polartiy
                score = 'positive'
            elif  -.05 < sentA.sentiment.polarity < 0.05:
                score = 'neutral'
            else:
                score = 'negative'
            df['Sentiment Score'].iloc[idx] = score #write score back to df
    return df

def company_social_score(company_list):
    """
    Function to generate a DataFrame with the Refinitiv Social Score of each company from a company list placed in input
    Date parameters are hard coded but could be place as parameters of the function as well
    """
    FieldList = ["TR.TRESGScore.date", "TR.SocialPillarScore"]
    df1,err= ek.get_data(instruments = company_list,fields= FieldList, parameters = {'SDate':'2022-12-31', 'EDate':"2023-12-31"})
    numCols=[]
    for col in df1.columns:
        if is_numeric_dtype(df1[col]):
            numCols.append(col)
    df1[numCols]=df1[numCols].astype(np.float64)
    df1['Date']=pd.to_datetime(df1['Date']).dt.date
    return df1


def polarity_to_weight(df, company_ticker, res):
    """
    WIP
    """
    polarity=df.Polarity.mean()
    subjectivity=df.Subjectivity.mean()
    nb_positive_news= sum(df['Sentiment Score'] == 'positive')
    nb_negative_news= sum(df['Sentiment Score'] == 'negative')
    nb_neutral_news=sum(df['Sentiment Score'] == 'neutral')
    polarity_avg = (df.Polarity.mean()+1)/2
    datapoints = df.shape[0]
    data = pd.DataFrame({'Company':[company_ticker],
                         'Polarity':[polarity],
                         'Subjectivity':[subjectivity],
                         'Positive':[nb_positive_news],
                         'Negative':[nb_negative_news],
                         'Neutral':[nb_neutral_news],
                         'Sentiment Score':[polarity_avg], 
                         'Datapoints':[datapoints]})
    res = res.append(data)
    #data.to_csv(csv_file, sep=';', mode='a', index=False, header=False)
    return res

def generate_aggregate_csv(output_file):
    csv_files = glob.glob(os.path.join('Weights_*_companies.csv'))
    
    if not csv_files:
        print("No CSV files found in the specified folder.")
        return
    
    aggregate_df = pd.DataFrame()
    for file in csv_files:
        df = pd.read_csv(file, sep=";", index_col=0)
        sector = file.split('_')[1]
        df['Sector'] = sector
        aggregate_df = aggregate_df.append(df)
      
    aggregate_df.to_csv(output_file, sep=";", index_label="Company")
    print(f"Aggregate CSV file generated: {output_file}")
    