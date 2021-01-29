import os
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from ishares.spiders.collect_ishares_links_ls import isharesLS
from ishares.spiders.isharesbr import IsharesbrSpider
from ishares.spiders.isharesdwld import IsharesdwldSpider
import pandas as pd
import numpy as np
import datetime as dt
from openpyxl import load_workbook

### GET FILE PATH
path = os.path.dirname(__file__)
os.chdir(path)

# # RUN CRAWLERS TO GET LANG & SCHWARZ + ISHARES METADATA
# process = CrawlerProcess(settings=get_project_settings())
# process.crawl(isharesLS)
# process.crawl(IsharesbrSpider)
# process.start()

# # COMBINE LANG & SCHWARZ + ISHARES METADATA
# df_ls = pd.read_csv('ls.csv', encoding='unicode_escape')
# df_br = pd.read_csv('brlinks.csv', encoding='unicode_escape')
# df_ls_br = pd.merge(df_ls, df_br, how='inner', on=['etf_ISIN'])
# df_ls_br = df_ls_br.drop(columns='etf_WKN_y')
# df_ls_br = df_ls_br.dropna()
# df_ls_br.to_csv('brlinks_on_ls.csv', index=False)

# # DOWNLOAD ISHARES FILES WHERE PRESENT ON LANG & SCHWARZ
process = CrawlerProcess(settings=get_project_settings())
process.crawl(IsharesdwldSpider)
process.start()

# # RUN VBA TO CONVERT FILES FROM XML TO XLSX FORMAT
os.system("runvbscript.vbs")

# ### TAKE DATA FROM XLSX FILES, COMPILE AND TRANSFER TO ISHARES DASHBOARD
# # Function to convert German formatted dates to English
def to_locale(txt):
    txt_list = []
    c_dict = {"Mai":"May","Mär":"Mar","Dez":"Dec","Okt":"Oct"}
    for t in txt:
        for k,v in c_dict.items():
            t = t.replace(k,v)
        txt_list.append(t)
    return txt_list

files_unclean = os.listdir('./downloaded_files_xlsx/')

#%%
# Filter out files in wrong format (mostly islamic etfs)
files = list()
files_format2 = list()
for f in files_unclean:
    abc = pd.ExcelFile(f'./downloaded_files_xlsx/{f}')
    obj = abc.sheet_names
    if 'Überblick' in obj:
        files.append(f)
    else:
        files_format2.append(f)

print(files_format2)

df_overview_all = pd.DataFrame()
df_holdings_all = pd.DataFrame()
df_performance_all = pd.DataFrame()

#%%
# gather data from files for above empty dataframes
for fl in files:
    
    # IMPORT OVERVIEW
    df_overview = pd.read_excel(f'./downloaded_files_xlsx/{fl}',sheet_name="Überblick")
    etf_name = df_overview.columns[0]
    df_overview = pd.read_excel(f'./downloaded_files_xlsx/{fl}',sheet_name="Überblick",skiprows = 3,decimal=',')
    df_overview = df_overview.iloc[:,0:2]
    df_overview['etf_name'] = etf_name
    df_overview.columns = ['caption','value','etf_name']
    df_overview['caption_etf_name'] = df_overview['caption'] + '_' + df_overview['etf_name']
    df_overview_all = df_overview_all.append(df_overview)
    
    # IMPORT HOLDINGS
    try:
        df_holdings = pd.read_excel(f'./downloaded_files_xlsx/{fl}',sheet_name="Positionen",skiprows = 3)
        df_holdings = df_holdings[['ISIN','Name','Standort','Gewichtung (%)','Sektor']]
        df_holdings.columns = ['ISIN','Name','Location','Weighting','Sector']
        df_holdings['etf_name']=etf_name
        df_holdings_all = df_holdings_all.append(df_holdings)
    except KeyError:
        print('File did not fit the df template: ',fl)
        
    # IMPORT PERFORMANCE
    df_performance = pd.read_excel(f'./downloaded_files_xlsx/{fl}',sheet_name="Historisch",skiprows = 0)
    df_performance.columns = ['Date','Currency','NAV','Securities','Net_Assets','Fund_Return_Series','Benchmark_Return_Series']
    df_performance['Date'] = to_locale(df_performance['Date'])
    df_performance['Date'] = pd.to_datetime(df_performance['Date'],format = '%d.%b.%Y')
    df_performance['Date'] = df_performance['Date'].dt.strftime('%Y-%m-%d')
    df_performance['etf_name']=etf_name
    df_performance_all = df_performance_all.append(df_performance)


# extract only latest rows per month in performance dataset
temp = df_performance_all[["Date","etf_name","Currency"]].groupby(["Date","etf_name"]).count().reset_index()
temp['Date'] = pd.to_datetime(temp['Date'],format = '%Y-%m-%d')
temp["year_month"] = temp["Date"].dt.strftime('%Y-%m')
temp = temp.groupby(["etf_name","year_month"]).agg({'Date':np.max}).reset_index()
df_performance_all['Date']=pd.to_datetime(df_performance_all['Date'],format = '%Y-%m-%d')
df_performance_all = pd.merge(df_performance_all,temp,how='inner',on=['etf_name','Date'])

df_overview_all = df_overview_all.reset_index()
df_holdings_all = df_holdings_all.reset_index()
df_performance_all = df_performance_all.reset_index()


# print to excel workbook without overwriting other sheets
book = load_workbook('./dashboard/ISH_Dashboard.xlsx')
w  = pd.ExcelWriter('./dashboard/ISH_Dashboard.xlsx', engine='openpyxl') 
w.book = book
w.sheets = dict((ws.title, ws) for ws in book.worksheets)
df_overview_all.to_excel(w, sheet_name = 'overview')
df_holdings_all.to_excel(w, sheet_name = 'holdings')
df_performance_all.to_excel(w, sheet_name = 'performance')
w.save()


