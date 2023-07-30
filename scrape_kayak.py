import json
import requests
import pandas as pd
import calendar
from datetime import datetime
from datetime import date
from time import gmtime, strftime
import glob
import os
import smtplib
from email.message import EmailMessage
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import ssl 

def scrape_kayak(start='', end='', airport = 'OPO'):
    """
    This function scrapes flight information from the kayak explore page.
    Parameters:
    start, end, airport - integer representing earliest possible departure date
    in YYYYMMDD format, integer representing latest return date, string with
    three letter code for starting airport. When both are start and end are
    left blank, results are returned from present date to one year in the
    future.
    Returns:
    df - a data frame containing all destination cities and corresponding
    flight information returned by the scraper
    """

    # Format the beginning and end dates to insert them into the URL
    #start = '&depart=' + str(start)
    #end = '&return=' + str(end)
    #"https://www.kayak.pt/s/horizon/exploreapi/destinations?airport=OPO&budget=&depart=20230601&return=20230630&tripdurationrange=4%2C7&duration=&flightMaxStops=&stopsFilterActive=false&topRightLat=51.82490080841914&topRightLon=8.962652968749989&bottomLeftLat=28.636584579286538&bottomLeftLon=-26.32543296874999&zoomLevel=5&selectedMarker=&themeCode=&selectedDestination="
    
    format = "%Y%m%d"
    res = False
    try:
        res = bool(datetime.strptime(start, format))
    except ValueError:
        res = False
    if res: 
        #print(res)
        url = "https://www.kayak.pt/s/horizon/exploreapi/destinations?airport=" + airport + "&budget=&depart=" + start + "&return="+ end + "&tripdurationrange=4%2C7&duration=&flightMaxStops=&stopsFilterActive=false&topRightLat=51.82490080841914&topRightLon=8.962652968749989&bottomLeftLat=28.636584579286538&bottomLeftLon=-26.32543296874999&zoomLevel=5&selectedMarker=&themeCode=&selectedDestination="
    else: 
        url = "https://www.kayak.pt/s/horizon/exploreapi/destinations?airport=" + airport + "&budget=&tripdurationrange=4%2C7&duration=&flightMaxStops=&stopsFilterActive=false&topRightLat=51.82490080841914&topRightLon=8.962652968749989&bottomLeftLat=28.636584579286538&bottomLeftLon=-26.32543296874999&zoomLevel=5&selectedMarker=&themeCode=&selectedDestination="
    
    response = requests.post(url).json()

    df = pd.DataFrame(columns=['City', 'Country', 'Duration','Price', 'Airline', 'Airport', 'Depart','Return', 'Link'])

    for i in range(len(response['destinations'])):
        destination = response['destinations'][i]
        row = list([destination['city']['name'], destination['country']['name'],
                    destination['flightMaxDuration'],
                    destination['flightInfo']['price'], destination['airline'],
                    destination['airport']['shortName'], pd.to_datetime(destination['departd']).date(),
                    pd.to_datetime(destination['returnd']).date(),
                    str('http://kayak.com'+destination['clickoutUrl'])])
        df.loc[i] = row
    
    city_mins = df.groupby(['City']).idxmin().astype(int)
    df['MinPrice'] = df.loc[city_mins['Price'].to_list()].Price
    df['is_MinPrice'] = df['Price'].eq(df['MinPrice']).astype(int)
    df = df.where(df['Price']!=999999).dropna()

    return df
  
    
def generate_baseline(city):
    """
    This function loads all files from a city origin stored in folder 'data' and generates a new baseline file, i.e. a file with the minimum prices for each route and month.
    The next step after this will be to upload this to a BigQuery database.
    Parameters:
    city: 
    Returns:
    A data frame containing all destination cities and minimum historical prices for each route.
    """
    all_files = glob.glob("data/*"+city+"*.csv") #reads all files from "data" folder
    all_files = [f for f in all_files if 'baseline' not in f and 'smallerprices' not in f and 'summary' not in f] #filter out files with these names
    df = pd.DataFrame()
    #loop through all the files and store them in the dataframe
    for f in all_files:
        file_name = f.split("/")[-1]
        filename = file_name.split(".")[0]
        df_temp = pd.read_csv(f)
        df_temp["filename"] = filename
        df = df.append(df_temp)
    #reorder the columns
    cols = df.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    df = df[cols]
    
    #some datetime columns
    df['date_query'] = pd.to_datetime(df['filename'].str[:11], format='%Y%m%d%H%M')
    df['year_query'] = pd.DatetimeIndex(df['date_query']).year
    df['month_query'] = pd.DatetimeIndex(df['date_query']).month
    df['day_query'] = pd.DatetimeIndex(df['date_query']).day
    df['weekday_query'] = pd.DatetimeIndex(df['date_query']).weekday
    df['hour_query'] = pd.DatetimeIndex(df['date_query']).hour
    df['year_depart'] = pd.DatetimeIndex(df['Depart']).year
    df['month_depart'] = pd.DatetimeIndex(df['Depart']).month
    df['days_advance'] = pd.to_datetime(df['Depart'], infer_datetime_format=True)-pd.to_datetime(df['date_query'], infer_datetime_format=True)
    df['CityOrigin'] = city

    #summarizing dataframe
    baseline = df.query("Depart >= date_query").groupby(['City','Country','year_depart','month_depart']).agg(minPrice=('Price', 'min'),meanPrice=('Price', 'mean'),medianPrice=('Price', 'median')).sort_values(['minPrice'],ascending=True).reset_index()
    baseline.insert(0, 'CityOrigin', city)
    baseline['timestamp'] = datetime.now()
    baseline['timestamp']  = baseline['timestamp'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))

    return(baseline)

def compare_prices(newdf,basedf,city):
    """
    Compares prices found in this run against a baseline file (as created by "generate_baseline" function defined above).
    Parameters:
    newdf: the dataframe with prices scraped in the current run 
    basedf: the baseline dataframe (as created by "generate_baseline" function defined above).
    city: the origin airport
    Returns:
    1.A data frame containing all flights that had prices below historical minimum prices.
    2.A data frame with summary of the comparison to report and send an email. 
    """

    newdf.sort_values(by=['Price'],ascending=True)
    newdf['year_depart'] = pd.DatetimeIndex(newdf['Depart']).year
    newdf['month_depart'] = pd.DatetimeIndex(newdf['Depart']).month

    compare = pd.merge(newdf, basedf, on=['City', 'Country', 'month_depart', 'year_depart'],how='left')
    compare['is_smaller'] = compare['Price'] < compare['minPrice']
    compare['difPrice'] = (compare['Price'] -compare['minPrice'])
    compare['difPricePct'] = ((compare['Price'] -compare['minPrice']) / compare['minPrice'])*100
    smaller = compare.query("is_smaller").sort_values('difPricePct')
    smaller['weekday_depart'] = pd.DatetimeIndex(smaller['Depart']).day_name()
    smaller['weekday_return'] = pd.DatetimeIndex(smaller['Return']).day_name()
    smaller['diff_days'] = (pd.DatetimeIndex(smaller['Return']) - pd.DatetimeIndex(smaller['Depart']))

    smallerUnder100=len(smaller.query("Price <= 100"))
    smallerUnder50=len(smaller.query("Price <= 50"))

    summarydf=pd.DataFrame(columns=['Date', 'CityOrigin','Filename', 'SmallerPrices', 'SmallerUnder100', 'SmallerUnder50'])
    filename = strftime("%Y%m%d%H%M", gmtime())+'_'+origin+'_2023'
    summarydf.loc[0] = [date.today(), city, filename, len(smaller), smallerUnder100, smallerUnder50]
    
    newbase = smaller.loc[:,["City","Country","year_depart","month_depart","Price","meanPrice","medianPrice","timestamp"]].rename(columns={"Price":"minPrice"})
    newbase.insert(0, 'CityOrigin', city)
    newbase['timestamp'] = datetime.now()
    newbase['timestamp']  = newbase['timestamp'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
    
    return(smaller,summarydf,newbase)

def send_mail(smallerprices,summarydf,city):
    """
    Send an automated e-mail for each origin airport reporting how many prices were found to be lower than the historical minimum for each
    combination of destination/month.
    Parameters:
    Dataframes generated in the "compare_prices" function.
    Returns:
    Nothing, only send the e-mail(s).
    """
    tableSummary = summarydf.to_html()
    tableSmallerprices = smallerprices.loc[:,["CityOrigin","City","Country","Price","Depart","weekday_depart","Return","weekday_return","diff_days","minPrice","difPrice","difPricePct","Link"]].sort_values('Price',ascending=True).reset_index(drop=True).sort_values('Depart').to_html(formatters={
        'difPricePct': '{:,.2f}%'.format,
        'difPrice': '{:,.2f}'.format,
        'Price': '€{:,.2f}'.format,
        'minPrice': '€{:,.2f}'.format},render_links=True)
    tableUnder100 = smallerprices.query("difPricePct < 0 & Price < 100").loc[:,["CityOrigin","City","Country","Depart","weekday_depart","Return","weekday_return","diff_days","minPrice","difPrice","difPricePct","Link"]].reset_index(drop=True).sort_values('Depart').to_html(formatters={
        'difPricePct': '{:,.2f}%'.format,
        'difPrice': '{:,.2f}'.format,
        'Price': '€{:,.2f}'.format,
        'minPrice': '€{:,.2f}'.format},render_links=True)
    sender = 'rafabelokurows@gmail.com' #if you're reading this and you're not me, change this e-mail to whichever e-mail you wanna use for this.
    recipient = 'rafabelokurows@gmail.com' #if you're reading this and you're not me, change this e-mail to whichever e-mail you wanna use for this.
    password = os.getenv('APP_PASSWORD') #the APP PASSWORD as generated on the Security Settings of the Gmail account configured above.
    
    if len(smallerprices) == 0: #if none of the prices are smaller than historical minimum prices
        subject = 'Sorry, no deals this time for '+city
        textBefore = "<p>Hey, we haven't found deals for airline tickets out of "+city+" this time.</p>"
        html = textBefore
    elif len(smallerprices.query("Price <= 100")) == 0: #if some prices are smaller than historical minimum prices, but none is under €100
        subject = 'Deals on airline tickets out of '+city
        textBefore = "<p>Hey, we've found a few deals for airline tickets out of "+city+" , although none for lass than €100.</p>This is the summary of the last run:\n"
        textMiddle = "<p>And here are the deals:</p>"
        html = textBefore + tableSummary + textMiddle + tableSmallerprices
    else: #yeah, we found some prices under €100 as well
        subject = 'Deals on airline tickets out of '+city
        textBefore = "<p>Hey, check out this new deals I've found for airline tickets out of "+city+".</p>This is the summary of the last run:\n"
        textMiddle = "<p>\nAnd even better, we've found flights with the best price yet for their routes (on each specific month) and under <b>100 Euros</b>!</p>\n"
        html = textBefore + tableSummary + textMiddle + tableUnder100
        
    message = MIMEMultipart()
    message['From'] = sender
    message['To'] = recipient
    message['Subject'] = subject
    message.attach(MIMEText(html, 'html'))

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL('smtp.gmail.com',465,context=context) as smtp:
        smtp.login(sender,password)
        smtp.sendmail(sender,recipient,message.as_string())

    print('Email with deals sent to ',recipient)
    
def auth_bgq():
    from google.cloud import bigquery
    from google.oauth2 import service_account
    import base64

    key = os.getenv('GCP_PRIVATE_KEY')
    key_b64 = base64.b64encode(key.encode())
    key_decoded = base64.b64decode(key_b64).decode()
    project = os.getenv('GCP_PROJECT_ID')
    project_b64 = base64.b64encode(project.encode())
    project_decoded = base64.b64decode(project_b64).decode()
    clientid = os.getenv('GCP_CLIENT_ID')
    client_b64 = base64.b64encode(clientid.encode())
    client_decoded = base64.b64decode(client_b64).decode()


    service_account_info = {
      "type": "service_account",
      "project_id": "project",
      "private_key_id": "34da29e1c968cf2bd1e5d16a47d5ea8030b90d7f",
      "private_key": "-----BEGIN PRIVATE KEY-----\nkey\n-----END PRIVATE KEY-----\n",
      "client_email": "teste-11@basedosdados-370918.iam.gserviceaccount.com",
      "client_id": "client",
      "auth_uri": "https://accounts.google.com/o/oauth2/auth",
      "token_uri": "https://oauth2.googleapis.com/token",
      "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
      "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/teste-11%40basedosdados-370918.iam.gserviceaccount.com"
    }
    service_account_info["private_key"] = service_account_info["private_key"].replace("key", key_decoded).replace("\\n", "\n")
    service_account_info["project_id"] = service_account_info["project_id"].replace("project", project_decoded).replace("\\n", "\n")
    service_account_info["client_id"] = service_account_info["client_id"].replace("client", client_decoded).replace("\\n", "\n")

    SCOPES = ['https://www.googleapis.com/auth/cloud-platform']

    credentials = service_account.Credentials.from_service_account_info(
                service_account_info, scopes=SCOPES)
    
    return(credentials)

def write_summary_bgq(summarydf, city):
    credentials = auth_bgq()
    #summarydf.to_gbq(destination_table='flightexplorer.resumo',
    #                                     if_exists="append",
    #                                     project_id='basedosdados-370918',credentials=credentials)
    try:
    # Save DataFrame to BigQuery table
        summarydf.to_gbq(destination_table='flightexplorer.resumo',
                                             if_exists="append",
                                             project_id='basedosdados-370918',credentials=credentials)
        print("New baseline for "+city+" dataframe written to BigQuery")
    except Exception as e:
        print(f"Error saving data to BigQuery table: {e}") 
    
def write_newbaseline_bgq(newbasedf, city):
    credentials = auth_bgq()
    
    try:
    # Save DataFrame to BigQuery table
        newbasedf.to_gbq(destination_table='flightexplorer.baseline',
                                             if_exists="append",
                                             project_id='basedosdados-370918',credentials=credentials)
        print("New baseline for "+city+" dataframe written to BigQuery")
    except Exception as e:
        print(f"Error saving data to BigQuery table: {e}") 

def scrape_destination(origin,destination):
    url = 'https://www.kayak.com/a/api/flightPricePrediction/predictCalendar?dateMode=range&distinct=true&origin='+origin+'&destination='+destination+'&locale=PT'
    response = requests.post(url).json()
    df2=pd.DataFrame(response['predictions'])
    df2['weekday_depart'] = pd.DatetimeIndex(df2['startDate']).day_name()
    df2['weekday_return'] = pd.DatetimeIndex(df2['endDate']).day_name()
    df2['diff_days'] = (pd.DatetimeIndex(df2['endDate']) - pd.DatetimeIndex(df2['startDate']))
    df2['diff_days'] = (pd.DatetimeIndex(df2['endDate']) - pd.DatetimeIndex(df2['startDate']))
    df2['days_advance'] = pd.to_datetime(df2['startDate'], infer_datetime_format=True)-pd.to_datetime(date.today())

    return(df2)

#### All prices ####
routes = pd.read_excel('routes.xlsx')
#df = pd.DataFrame({'origin': ['OPO','OPO', 'LIS', 'LIS'], 'destination': ['MAD', 'BCN','MAD', 'BCN']})
results = []
# Loop through the dataframe and call the scrape_destination function for each row
for index, row in routes.iterrows():
    origin = row['origin']
    destination = row['destination']
    res = scrape_destination(origin=origin, destination=destination)
    results.append(res)
# Concatenate the results into a single dataframe
all_prices = pd.concat(results)
min_prices = all_prices.groupby(['originAirport','destinationAirport']).agg(minPrice=('minPrice','min')).reset_index()
all_prices.to_csv('data/all_prices/all_prices'+strftime("%Y%m%d%H%M", gmtime())+'.csv',index=False)
min_prices.to_csv('data/all_prices/min_prices'+strftime("%Y%m%d%H%M", gmtime())+'.csv',index=False)

#### Lowest price for each destination ####
origins = ['OPO','MXP','NAP','LIS','MAD']  #airports to find ticket prices

for origin in origins:
    df = scrape_kayak(airport = origin)
    baseline = generate_baseline(city = origin) #after generating the csv file, recompiles the baseline file, finding minimum prices for each route/month
    df.to_csv('data/'+strftime("%Y%m%d%H%M", gmtime())+'_'+origin+'_2023.csv',index=False) #saves a CSV file with all prices for each origin airport
    a,b,c = compare_prices(newdf = df, basedf = baseline, city = origin) #compares prices obtained in this run against baseline of all files stored in "data" folder
    baseline.to_csv('data/baseline_'+strftime("%Y%m%d%H%M", gmtime())+'_'+origin+'_2023.csv',index=False) #saves new baseline
    b.to_csv('data/summary_'+strftime("%Y%m%d%H%M", gmtime())+'_'+origin+'_2023.csv',index=False) #and summary file, for good measure
    if (len(a)> 100):
        a.to_csv('data/smallerprices_'+strftime("%Y%m%d%H%M", gmtime())+'_'+origin+'_2023.csv',index=False) #saves CSV files with prices found in this run that were less than baseline minimum amounts
    if (len(c)> 100):
        c.to_csv('data/new_baseline'+strftime("%Y%m%d%H%M", gmtime())+'_'+origin+'_2023.csv',index=False) #and summary file, for good measure
    #send_mail(smallerprices = a,summarydf = b,city = origin) #sends an email for each origin airport reporting how many prices were lower than the historical minimum
    write_summary_bgq(summarydf = b,city = origin)
    write_newbaseline_bgq(newbasedf = c,city = origin)
