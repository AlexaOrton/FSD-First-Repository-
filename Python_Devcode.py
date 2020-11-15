#!/usr/bin/env python
# coding: utf-8

# In[1]:



import pandas as pd
import pyodbc
import numpy as np

""" 
0. Import Raw Data
0.0 Establish connection with SQL Database
0.1 Import Index Constituents
0.2 Import Industry Classification Benchmark
0.3 Import BA Beta Output
"""
# 0.0 Establish connection with SQL Database
conn = pyodbc.connect(  'Driver={SQL Server};'
                     'Server=ZIPHO-WORKSTATI\SQLEXPRESS;'
                     'Database=AIFMRM_ERS;'
                     'Trusted_Connection=yes;')
                     
cursor = conn.cursor()

# 0.1 Import Index Constituents
tbl_Index_Constituents = pd.read_sql_query('SELECT * FROM dbo.tbl_Index_Constituents',conn)
tbl_Index_Constituents = tbl_Index_Constituents[['Date', 'Rank','Alpha','Status' ,'Instrument', 'ICB Sub-Sector', 'Gross Market Capitalisation', 
                                             'ALSI New', 'Index New', 'TOPI New', 'DTOP New', 'RESI New', 'FINI New', 'INDI New',
                                              'PCAP New', 'SAPY New', 'ALTI New']]

tbl_Consti =tbl_Index_Constituents
tbl_Consti.rename(columns = {'Instrument':'Company','Alpha':'Instrument'}, inplace=True)

print(tbl_Consti.head())  
#[4331 rows x 46 columns]

# 0.2 Import Industry Classification Benchmark
tbl_Industry_Classification_Benchmark = pd.read_sql_query('SELECT * FROM dbo.tbl_Industry_Classification_Benchmark',conn)
tbl_Industry_Classification_Benchmark = tbl_Industry_Classification_Benchmark[['Industry', 'Super Sector','Sector' ,'Sub-Sector Code', 'Sub-Sector']]
tbl_Industry_Classification_Benchmark.rename(columns = {'Super Sector':'Super_Sector','Sub Sector':'Sub_Sector'}, inplace=True)
print(tbl_Industry_Classification_Benchmark.head())
#[114 rows x 8 columns]

# 0.3 Import BA Beta Output
tbl_BA_Beta_Output = pd.read_sql_query('SELECT * FROM dbo.tbl_BA_Beta_Output',conn)
tbl_BA_Beta_Output = tbl_BA_Beta_Output[['Date','Instrument','Data Points','Start Date','End Date','% Days Traded','Index','Beta','Total Risk','Unique Risk']]
print(tbl_BA_Beta_Output.head()) 
#[26710 rows x 16 columns]


#rDate = (tbl_Index_Constituents['Date']).max()
rDate = sorted(list(set(tbl_Index_Constituents['Date'])))
type(rDate)
rDate
#or item in rDate:
 #rint(item)
#print(rDate.iloc[:,])


pd.set_option('mode.chained_assignment', None)    # or use iloc method










# In[2]:


# Find the distinct quartly dates from the Beta_output as rDate1. sort it to match rDate
rDate1 = sorted(list(set(tbl_BA_Beta_Output['Date']))) 


# In[3]:


# Match the data in the Beta_Output_tbl to the Constituents_tbl
for i in range (0,12):
    tbl_BA_Beta_Output.loc[tbl_BA_Beta_Output.Date == rDate1[i], 'Date'] = rDate[i]


# In[625]:


print(set(tbl_BA_Beta_Output['Date']))
print(set(tbl_Consti['Date']))


# In[5]:


tbl_Industry = tbl_Industry_Classification_Benchmark
tbl_Industry.rename(columns = {'Sub-Sector Code':'ICB Sub-Sector'}, inplace = True)
# Betas_ICs.rename(columns = {'Total Risk': 'mktVol', 'Unique Risk': 'specVols'}, inplace = True)
print(tbl_Industry)


# In[6]:


Constituents_tbl = pd.merge(tbl_Consti , tbl_Industry, on=['ICB Sub-Sector'])


# In[7]:


Constituents_tbl = Constituents_tbl.iloc[:,np.r_[0:5,17:20]] 


# In[8]:


Constituents_tbl[(Constituents_tbl["Instrument"] == 'ACE')]


# In[9]:


# test how the merge works
tbl_BA = tbl_BA_Beta_Output[(tbl_BA_Beta_Output['Date'] == rDate[1])]
genStats = Constituents_tbl[(Constituents_tbl['Date'] == rDate[1])]   


# In[10]:


print(tbl_BA[(tbl_BA['Instrument'] == 'ACE')])
print(genStats[(genStats['Instrument'] == 'ACE')])


# In[11]:


generalStats = pd.merge(tbl_BA_Beta_Output ,Constituents_tbl , on = ['Instrument','Date'],how='right')


# In[12]:


Constituents_tbl[(Constituents_tbl['Instrument'] == 'UAT')]  #there is an extra four


# In[13]:


generalStats.columns


# In[14]:


generalStats =generalStats.iloc[:,np.r_[0:7,10:15,7]] # rearrange columns. NB RUN ONCE OR ORDER WILL REVERT
generalStats


# In[15]:


Stats_J200 = generalStats[(generalStats['Index'] == 'J200')][['Date','Instrument','Data Points','Start Date','End Date','% Days Traded','Status','Company','Industry','Super_Sector','Beta']].reset_index(drop=True)
Stats_J203 = generalStats[(generalStats['Index'] == 'J203')][['Date','Instrument','Data Points','Start Date','End Date','% Days Traded','Status','Company','Industry','Super_Sector','Beta']].reset_index(drop=True)
Stats_J250 = generalStats[(generalStats['Index'] == 'J250')][['Date','Instrument','Data Points','Start Date','End Date','% Days Traded','Status','Company','Industry','Super_Sector','Beta']].reset_index(drop=True)
Stats_J257 = generalStats[(generalStats['Index'] == 'J257')][['Date','Instrument','Data Points','Start Date','End Date','% Days Traded','Status','Company','Industry','Super_Sector','Beta']].reset_index(drop=True)
Stats_J258 = generalStats[(generalStats['Index'] == 'J258')][['Date','Instrument','Data Points','Start Date','End Date','% Days Traded','Status','Company','Industry','Super_Sector','Beta']].reset_index(drop=True)


# In[16]:


Stats_J200.rename(columns = {'Beta':'Beta_J200','Data Points':'# Points','Start Date':'First Trade','End Date':'Last Trade','% Days Traded':'% Trade'}, inplace=True)
Stats_J203.rename(columns = {'Beta':'Beta_J203','Data Points':'# Points','Start Date':'First Trade','End Date':'Last Trade','% Days Traded':'% Trade'}, inplace=True)
Stats_J250.rename(columns = {'Beta':'Beta_J250','Data Points':'# Points','Start Date':'First Trade','End Date':'Last Trade','% Days Traded':'% Trade'}, inplace=True)
Stats_J257.rename(columns = {'Beta':'Beta_J257','Data Points':'# Points','Start Date':'First Trade','End Date':'Last Trade','% Days Traded':'% Trade'}, inplace=True)
Stats_J258.rename(columns = {'Beta':'Beta_J258','Data Points':'# Points','Start Date':'First Trade','End Date':'Last Trade','% Days Traded':'% Trade'}, inplace=True)


# In[17]:


print(len(Stats_J200))
print(len(Stats_J203))
print(len(Stats_J250))
print(len(Stats_J257))
print(len(Stats_J258))


# In[18]:


Stats_J258.columns


# In[19]:


generalStats_tbl =Stats_J200
generalStats_tbl = generalStats_tbl.assign(Beta_J203 = Stats_J203.iloc[:,[10]],Beta_J250 = Stats_J250.iloc[:,[10]],
                                           Beta_J257= Stats_J257.iloc[:,[10]],Beta_J258 =Stats_J258.iloc[:,[10]])


# In[20]:


generalStats_tbl[(generalStats_tbl['Industry'] == 'Oil & Gas') & (generalStats_tbl['Date'] == rDate[11])]


# In[21]:


generalStats_tbl[(generalStats_tbl['Instrument']=='ACE') & (generalStats_tbl['Date']==rDate[1])]  # matches above okay


# In[22]:


#generalStats_tbl.to_csv (r'C:\Users\ZL2706556\Documents\0.Personal_Career\INF5006S\Project\generalStats_tbl.csv',index = False, header=True)


# In[23]:


set(tbl_Industry['Industry']) # 10 distinct industries


# In[24]:


#Indices of interest as per the client
indexCode = ['ALSI','FLED','LRGC','MIDC','SMLC','TOPI','RESI','FINI','INDI','PCAP','SAPY' ,'ALTI']

#Check if the indice is a member 
def Indices(indexCode):
    
    if indexCode == 'ALSI':
        Indices = 'ALSI New'
    
    elif indexCode == 'FLED':
        Indices = 'ALSI New'

    elif indexCode == 'LRGC':
        Indices = 'Index New'

    elif indexCode == 'MIDC':
        Indices = 'Index New'

    elif indexCode == 'SMLC':
        Indices = 'Index New'

    elif indexCode == 'TOPI':
        Indices = 'TOPI New'

    elif indexCode == 'RESI':
        Indices = 'RESI New'

    elif indexCode == 'FINI':
        Indices = 'FINI New'

    elif indexCode == 'INDI':
        Indices = 'INDI New'
        
    elif indexCode == 'PCAP':
        Indices = 'PCAP New'

    elif indexCode == 'SAPY':
        Indices = 'SAPY New'

    elif indexCode == 'ALTI':
        Indices = 'ALTI New'

    return Indices


# In[25]:


#Create a list of all the industries

Industry = ['Basic Materials','Consumer Goods','Consumer Services','Financials','Health Care'
            ,'Industrials','Oil & Gas','Technology','Telecommunications','Utilities']


# # Function 1

# In[26]:


#### Function 1 ####  


def GetICsAndWeights(rDate,indexCode):
    
    ICs = tbl_Consti[tbl_Consti['Date'] == rDate]
   
  
    GetICs =ICs[(ICs['ALSI New'] == 'ALSI')|
                (ICs['ALSI New'] == 'FLED')|# Search each column for ALSI or FLED
                (ICs['Index New'] =='LRGC')|
                (ICs['Index New'] =='MIDC')|
                (ICs['Index New'] =='SMLC')|
                (ICs['TOPI New'] == 'TOPI')|
                (ICs['RESI New'] == 'RESI')| 
                (ICs['FINI New'] == 'FINI')|
                (ICs['INDI New'] == 'INDI')| 
                (ICs['PCAP New'] == 'PCAP')| 
                (ICs['SAPY New'] == 'SAPY')| 
                (ICs['ALTI New'] == 'ALTI')]
                          
    GMC_ICs = GetICs[GetICs[Indices(indexCode)] == indexCode]['Gross Market Capitalisation'].sum()
    
    GetICs[indexCode +'Weights'] = GetICs[GetICs[Indices(indexCode)] == indexCode]['Gross Market Capitalisation']/GMC_ICs
    
    GetICs = pd.merge(GetICs , tbl_Industry, on='ICB Sub-Sector')
    GetICs.rename(columns = {'Gross Market Capitalisation': 'Gross_Market_Capitalisation', 'Unique Risk': 'specVols','Sub-Sector':'Sub_Sector'}, inplace = True)
    
    #Get the weights
    GetICsAndWeights = GetICs[['Date','Instrument','Industry','Status','Super_Sector','Sub_Sector','Gross_Market_Capitalisation',indexCode +'Weights']]
    
    print(GetICsAndWeights[indexCode +'Weights'].sum())      ### Check weights add up to 1
    w = GetICsAndWeights[indexCode +'Weights']
    
    return GetICsAndWeights



# In[27]:


#Call function 1 and populate table for AlSI on all rDates
#indexCode = ['ALSI','FLED','LRGC','MIDC','SMLC','TOPI','RESI','FINI','INDI','PCAP','SAPY' ,'ALTI']
ICsAndWeights_tbl = pd.DataFrame({})
for i in range(0,12):
    ICsAndWeights = GetICsAndWeights(rDate[i],indexCode[0])
    ICsAndWeights_tbl =ICsAndWeights_tbl.append(ICsAndWeights,ignore_index=True)
    ICsAndWeights_ALSI= ICsAndWeights_tbl
    

ICsAndWeights_tbl = pd.DataFrame({})
for i in range(0,12):
    ICsAndWeights = GetICsAndWeights(rDate[i],indexCode[1])
    ICsAndWeights_tbl =ICsAndWeights_tbl.append(ICsAndWeights,ignore_index=True)
    ICsAndWeights_FLED =ICsAndWeights_tbl

ICsAndWeights_tbl = pd.DataFrame({})
for i in range(0,12):
    ICsAndWeights = GetICsAndWeights(rDate[i],indexCode[2])
    ICsAndWeights_tbl =ICsAndWeights_tbl.append(ICsAndWeights,ignore_index=True)
    ICsAndWeights_LRGC =ICsAndWeights_tbl

ICsAndWeights_tbl = pd.DataFrame({})
for i in range(0,12):
    ICsAndWeights = GetICsAndWeights(rDate[i],indexCode[3])
    ICsAndWeights_tbl =ICsAndWeights_tbl.append(ICsAndWeights,ignore_index=True)
    ICsAndWeights_MIDC =ICsAndWeights_tbl.append(ICsAndWeights,ignore_index=True)

ICsAndWeights_tbl = pd.DataFrame({})
for i in range(0,12):
    ICsAndWeights = GetICsAndWeights(rDate[i],indexCode[4])
    ICsAndWeights_tbl =ICsAndWeights_tbl.append(ICsAndWeights,ignore_index=True)
    ICsAndWeights_SMLC =ICsAndWeights_tbl

ICsAndWeights_tbl = pd.DataFrame({})
for i in range(0,12):
    ICsAndWeights = GetICsAndWeights(rDate[i],indexCode[5])
    ICsAndWeights_tbl =ICsAndWeights_tbl.append(ICsAndWeights,ignore_index=True)
    ICsAndWeights_TOPI =ICsAndWeights_tbl

ICsAndWeights_tbl = pd.DataFrame({})
for i in range(0,12):
    ICsAndWeights = GetICsAndWeights(rDate[i],indexCode[6])
    ICsAndWeights_tbl =ICsAndWeights_tbl.append(ICsAndWeights,ignore_index=True)
    ICsAndWeights_RESI =ICsAndWeights_tbl
        
ICsAndWeights_tbl = pd.DataFrame({})
for i in range(0,12):
    ICsAndWeights = GetICsAndWeights(rDate[i],indexCode[7])
    ICsAndWeights_tbl =ICsAndWeights_tbl.append(ICsAndWeights,ignore_index=True)
    ICsAndWeights_FINI =ICsAndWeights_tbl
        
ICsAndWeights_tbl = pd.DataFrame({})
for i in range(0,12):
    ICsAndWeights = GetICsAndWeights(rDate[i],indexCode[8])
    ICsAndWeights_tbl =ICsAndWeights_tbl.append(ICsAndWeights,ignore_index=True)
    ICsAndWeights_INDI =ICsAndWeights_tbl

ICsAndWeights_tbl = pd.DataFrame({})
for i in range(0,12):
    ICsAndWeights = GetICsAndWeights(rDate[i],indexCode[9])
    ICsAndWeights_tbl =ICsAndWeights_tbl.append(ICsAndWeights,ignore_index=True)
    ICsAndWeights_PCAP =ICsAndWeights_tbl
        
ICsAndWeights_tbl = pd.DataFrame({})
for i in range(0,12):
    ICsAndWeights = GetICsAndWeights(rDate[i],indexCode[10])
    ICsAndWeights_tbl =ICsAndWeights_tbl.append(ICsAndWeights,ignore_index=True)
    ICsAndWeights_SAPY =ICsAndWeights_tbl
    
ICsAndWeights_tbl = pd.DataFrame({})
for i in range(0,12):
    ICsAndWeights = GetICsAndWeights(rDate[i],indexCode[11])
    ICsAndWeights_tbl =ICsAndWeights_tbl.append(ICsAndWeights,ignore_index=True)
    ICsAndWeights_ALTI =ICsAndWeights_tbl

# CONCATENATE THE ROWS 


# In[488]:


ICsAndWeights_TOPI


# In[29]:


allICsAndWeights =ICsAndWeights_ALSI 
allICsAndWeights = allICsAndWeights.assign(FLEDWeights = ICsAndWeights_FLED.iloc[:,[7]],LRGCWeights = ICsAndWeights_LRGC.iloc[:,[7]],
                                        MIDCWeights = ICsAndWeights_MIDC.iloc[:,[7]], SMLCWeights = ICsAndWeights_SMLC.iloc[:,[7]],
                                        TOPIWeights = ICsAndWeights_TOPI.iloc[:,[7]], RESIWeights =ICsAndWeights_RESI.iloc[:,[7]],
                                        FINIWeights = ICsAndWeights_FINI.iloc[:,[7]], INDIWeights = ICsAndWeights_INDI.iloc[:,[7]],
                                        PCAPWeights= ICsAndWeights_PCAP.iloc[:,[7]], SAPYWeights = ICsAndWeights_SAPY.iloc[:,[7]],
                                        ALTIWeights = ICsAndWeights_ALTI.iloc[:,[7]])

# need to filter which weight belongs to which share


# In[30]:


allICsAndWeights =allICsAndWeights.fillna(0)


# In[31]:


allICsAndWeights


# In[530]:


topiinterest = allICsAndWeights[(allICsAndWeights['Date']==rDate[11]) 
                 &  (allICsAndWeights['TOPIWeights']> 0) ][['Date','Instrument','Industry']]


# In[478]:


topiinterest.sort_values(by=['Instrument'], inplace=True)
topiinterest.reset_index(drop = True)


# In[33]:


ICsAndWeights = GetICsAndWeights(rDate[9],indexCode[0])
print(ICsAndWeights.dropna().head())


# In[34]:


allICsAndWeights[(allICsAndWeights['Date']== rDate[9]) & (allICsAndWeights['ALSIWeights']>0)][['Instrument','ALSIWeights']].head()  

# Works , just take note that sometimes the Weights are zero thus increasing the index count. Matches the above for now.


# In[35]:


allICsAndWeights.columns


# In[36]:


IndustryWeights = allICsAndWeights.groupby(by=['Date','Industry'],as_index = False)[['ALSIWeights','FLEDWeights','LRGCWeights', 'MIDCWeights',
                                                                                    'SMLCWeights', 'TOPIWeights','RESIWeights', 'FINIWeights', 'INDIWeights', 
                                                                                    'PCAPWeights','SAPYWeights', 'ALTIWeights']].sum()

IndustryWeights   # when grouping make sure to turn index_as = False


# In[37]:


IndustryWeights[(IndustryWeights['Date']==rDate[1])]['ALSIWeights'].sum()


# In[38]:


IndustryWeights[(IndustryWeights['Date']==rDate[1])]


# In[39]:


allICsAndWeights = allICsAndWeights.fillna(0)    # remove nan in order to read weights as float type
print(set(allICsAndWeights['Industry']))


# In[40]:


IndustryWeights.to_csv (r'C:\Users\27605\Documents\0.Personal Career\INF5006S\IndustryWeights_tbl.csv',index = False, header=True)


# In[41]:


IndustryWeights[(IndustryWeights['Date'] == rDate[3])]['ALSIWeights'].sum()


# In[42]:


#allICsAndWeights.to_csv (r'C:\Users\ZL2706556\Documents\0.Personal_Career\INF5006S\Project\allICsAndWeights_tbl.csv',index = False, header=True)


# In[458]:


allICsAndWeights['TOPIWeights'].sum()    # the sum of this should be 12 since there are 12 quarters and each one adds to 1


# In[459]:


allICsAndWeights


# In[44]:


# cursor.execute('CREATE TABLE IndustryWeights_tbl (Date datetime, Industry nvarchar(50), ALSIWeights float,FLEDWeights float,LRGCWeights float,MIDCWeights float,SMLCWeights float,TOPIWeights float,RESIWeights float,FINIWeights float,INDIWeights float,PCAPWeights float,SAPYWeights float,ALTIWeights float)')

# for row in IndustryWeights.itertuples():
#     cursor.execute('''
#                 INSERT INTO AIFMRM_ERS.dbo.IndustryWeights_tbl (Date,Industry, ALSIWeights,FLEDWeights,LRGCWeights,MIDCWeights,
#                                                             SMLCWeights,TOPIWeights,RESIWeights,FINIWeights,INDIWeights,
#                                                             PCAPWeights,SAPYWeights,ALTIWeights)
#                 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
#                 ''',
#                 row.Date, 
#                 row.Industry,
#                 row.ALSIWeights,
#                 row.FLEDWeights,
#                 row.LRGCWeights,
#                 row.MIDCWeights,
#                 row.SMLCWeights,
#                 row.TOPIWeights,
#                 row.RESIWeights,
#                 row.FINIWeights,
#                 row.INDIWeights,
#                 row.PCAPWeights,
#                 row.SAPYWeights,
#                 row.ALTIWeights,
                
#                 )
# conn.commit()

 


# In[45]:


# cursor.execute('CREATE TABLE allICsAndWeights_tbl (Date datetime, Instrument nvarchar(50), Industry nvarchar(50),Super_Sector nvarchar(50),Sub_Sector nvarchar(50),Gross_Market_Capitalisation nvarchar(50), ALSIWeights float,FLEDWeights float,LRGCWeights float,MIDCWeights float,SMLCWeights float,TOPIWeights float,RESIWeights float,FINIWeights float,INDIWeights float,PCAPWeights float,SAPYWeights float,ALTIWeights float)')

# for row in allICsAndWeights.itertuples():
#     cursor.execute('''
#                 INSERT INTO AIFMRM_ERS.dbo.allICsAndWeights_tbl (Date, Instrument,Industry,Super_Sector,Sub_Sector ,Gross_Market_Capitalisation, ALSIWeights,FLEDWeights,LRGCWeights,MIDCWeights,
#                                                             SMLCWeights,TOPIWeights,RESIWeights,FINIWeights,INDIWeights,
#                                                             PCAPWeights,SAPYWeights,ALTIWeights)
#                 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
#                 ''',
#                 row.Date, 
#                 row.Instrument,
#                 row.Industry,
#                 row.Super_Sector,
#                 row.Sub_Sector,
#                 row.Gross_Market_Capitalisation,
#                 row.ALSIWeights,
#                 row.FLEDWeights,
#                 row.LRGCWeights,
#                 row.MIDCWeights,
#                 row.SMLCWeights,
#                 row.TOPIWeights,
#                 row.RESIWeights,
#                 row.FINIWeights,
#                 row.INDIWeights,
#                 row.PCAPWeights,
#                 row.SAPYWeights,
#                 row.ALTIWeights,
                
#                 )
# conn.commit()


# # FUNCTION 2

# In[46]:


#Market proxies into mktIndexCode variable
mktIndexCode = ['J200','J203','J250','J257','J258'] 

Beta_t = tbl_BA_Beta_Output


# In[47]:


Beta_t.corr()


# In[48]:


# #### Function 2 ####  
# def GetBetasMktAndSpecVols(rDate1,rDate,mktIndexCode,indexCode):
    
#     #Note to add if statement to ensure the correct rDate1 matches with rDate. Current method still works
#     #filter data according to quarter and market proxy
#     Beta_rdate = Beta_t[(Beta_t['Date'] == rDate1) & (Beta_t['Index'] == mktIndexCode)]

#     #read in function 1
#     ICs = GetICsAndWeights(rDate,indexCode)         #need to filter out the constituents with wieght nan
#     ICs = ICs.dropna()
    
#     #merge the dataaframes on the Instrument
#     Betas_ICs = pd.merge(ICs , Beta_rdate , on='Instrument')
#     Betas_ICs.insert(3,'Indices', indexCode)      #need a marker to see what constituent belongs to a particular Indices
   
#     # Change column titles for clarity
#     Betas_ICs.rename(columns = {'Total Risk': 'mktVol', 'Unique Risk': 'specVols','Date_x':'Date'}, inplace = True)

#     #give the required beta
#     Beta = Betas_ICs[['Date','Instrument', 'Beta', 'Industry']]
#     B = Betas_ICs['Beta']
#     Betas_Mkt_SpecVols = Betas_ICs.drop(['Date_y'], axis = 1)
    
#     #Specific volatility
#     specVols = Betas_ICs[['Date','Instrument','specVols']]
#     s = Betas_ICs['specVols']
    
#     #Market volatility
#     mktVol = Beta_t[(Beta_t['Date'] == rDate1) & (Beta_t['Index'] == mktIndexCode)
#                       & (Beta_t['Instrument'] == mktIndexCode)][['Date','Instrument','Total Risk']]
#     mktVol.rename(columns = {'Total Risk': 'mktVol', 'Unique Risk': 'specVols'}, inplace = True)
#     mktVol = mktVol[['Date','Instrument','mktVol']]
#     m = mktVol['mktVol']
    
#     #return Beta, specVols, mktVol , B, m, s     # run for function 3
#     return  Betas_Mkt_SpecVols             # populating DB_tbl


# In[512]:


Beta_rdate = Beta_t[(Beta_t['Date'] == rDate[11]) & (Beta_t['Index'] == mktIndexCode[0])]
Beta_rdate


# In[560]:


ICs = GetICsAndWeights(rDate[11],indexCode[5])

TOPItest =ICs[(ICs['TOPIWeights']>0)]
TOPItest.sort_values(by=['Instrument'], inplace=True)
TOPItest.reset_index(drop= True)


# In[513]:


Betas_ICs = pd.merge(TOPItest , Beta_rdate , on=['Instrument','Date'])

Betas_ICs


# In[581]:



#### Function 2 ####  
def GetBetasMktAndSpecVols(rDate,mktIndexCode,indexCode):
    
    
    #filter data according to quarter and market proxy
    Beta_rdate = Beta_t[(Beta_t['Date'] == rDate) & (Beta_t['Index'] == mktIndexCode)]

    #read in function 1
    ICs = GetICsAndWeights(rDate,indexCode)         #need to filter out the constituents with wieght nan
    
    
    #merge the dataaframes on the Instrument
    Betas_ICs = pd.merge(ICs , Beta_rdate , on=['Instrument','Date'])
    Betas_ICs.insert(3,'Indices', indexCode)      #need a marker to see what constituent belongs to a particular Indices
   
    # Change column titles for clarity
    Betas_ICs.rename(columns = {'Total Risk': 'mktVol', 'Unique Risk': 'specVols','Date_x':'Date'}, inplace = True)

    #give the required beta
    Beta = Betas_ICs[['Date','Instrument', 'Beta', 'Industry']]
    B = Betas_ICs['Beta']
    Betas_Mkt_SpecVols = Betas_ICs
    Betas_Mkt_SpecVols = Betas_Mkt_SpecVols.iloc[:,np.r_[0:3,13,3:7,14:17,8]]
    
    #Specific volatility
    specVols = Betas_ICs[['Date','Instrument','specVols']]
    s = Betas_ICs['specVols']
    
    #Market volatility
    mktVol = Beta_t[(Beta_t['Date'] == rDate) & (Beta_t['Index'] == mktIndexCode)
                      & (Beta_t['Instrument'] == mktIndexCode)][['Date','Instrument','Total Risk']]
    mktVol.rename(columns = {'Total Risk': 'mktVol', 'Unique Risk': 'specVols'}, inplace = True)
    mktVol = mktVol[['Date','Instrument','mktVol']]
    m = mktVol['mktVol']
    
  
    return Betas_Mkt_SpecVols,mktVol     
    #return  Betas_Mkt_SpecVols             # populating DB_tbl


# In[582]:


#Call function 2

#B, m, s,Betas_Mkt_SpecVols = GetBetasMktAndSpecVols(rDate1[i],rDate[i],mktIndexCode[y],indexCode[z])
Betas_Mkt_SpecVols,mktVol   = GetBetasMktAndSpecVols(rDate[11],mktIndexCode[0],indexCode[5])
Betas_Mkt_SpecVols = Betas_Mkt_SpecVols
mktVol = mktVol


# In[583]:


Topitest2 = Betas_Mkt_SpecVols[(Betas_Mkt_SpecVols['Date']==rDate[11]) 
                 &  (Betas_Mkt_SpecVols['TOPIWeights']> 0) ]
Topitest2.sort_values(by=['Instrument'], inplace=True)
Topitest2.reset_index(drop= True)  # matches SQL


# In[584]:


y = 0
mktVol_tbl = pd.DataFrame({})
for y in range(0,5):    # y for all index range is actually from 0-4
    
    for i in range(0,12):        #y for all indexcode
        c,mktVol = GetBetasMktAndSpecVols(rDate[i],mktIndexCode[y],indexCode[0])
        mktVol_tbl = mktVol_tbl.append(mktVol,ignore_index=True)


# In[585]:


print(mktVol_tbl.head(3))    # just note to use rDate1


# In[55]:


# z = 0
# i = 0
# y = 0
# BetasMktAndSpecVols_tbl = pd.DataFrame({})
# for z in range(0,5):    # z for all index
#     Betas_Mkt_SpecVols = GetBetasMktAndSpecVols(rDate[i],mktIndexCode[z],indexCode[y])
#     BetasMktAndSpecVols_tbl =BetasMktAndSpecVols_tbl.append(Betas_Mkt_SpecVols,ignore_index=True)
    
#     for y in range(0,12):        #y for all indexcode
#         i=1
#         Betas_Mkt_SpecVols = GetBetasMktAndSpecVols(rDate[i],mktIndexCode[z],indexCode[y])
#         BetasMktAndSpecVols_tbl =BetasMktAndSpecVols_tbl.append(Betas_Mkt_SpecVols,ignore_index=True)
        
#         for i in range(2,12):   #i is for all dates
#             Betas_Mkt_SpecVols = GetBetasMktAndSpecVols(rDate[i],mktIndexCode[z],indexCode[y])
#             BetasMktAndSpecVols_tbl =BetasMktAndSpecVols_tbl.append(Betas_Mkt_SpecVols,ignore_index=True)
            
# for z in range(0,5):    # z for all index
#     Betas_Mkt_SpecVols = GetBetasMktAndSpecVols(rDate[0],mktIndexCode[z],indexCode[y])
#     BetasMktAndSpecVols_tbl =BetasMktAndSpecVols_tbl.append(Betas_Mkt_SpecVols,ignore_index=True)
    
#     for y in range(1,12):        #y for all indexcode
#         Betas_Mkt_SpecVols = GetBetasMktAndSpecVols(rDate[0],mktIndexCode[z],indexCode[y])
#         BetasMktAndSpecVols_tbl =BetasMktAndSpecVols_tbl.append(Betas_Mkt_SpecVols,ignore_index=True)


# In[586]:


z = 0
i = 0
y = 0
BetasMktAndSpecVols_tbl = pd.DataFrame({})
for z in range(0,5):    # z for all index
    
    for y in range(0,12):        #y for all indexcode
        
        for i in range(0,12):   #i is for all dates
            Betas_Mkt_SpecVols,c = GetBetasMktAndSpecVols(rDate[i],mktIndexCode[z],indexCode[y])
            BetasMktAndSpecVols_tbl =BetasMktAndSpecVols_tbl.append(Betas_Mkt_SpecVols,ignore_index=True)
            


# In[596]:


Topitest3 = BetasMktAndSpecVols_tbl[(BetasMktAndSpecVols_tbl['Date'] == rDate[11]) 
                        & (BetasMktAndSpecVols_tbl['Indices'] ==indexCode[5] ) 
                                                    &  (BetasMktAndSpecVols_tbl['TOPIWeights']> 0)
                           &  (BetasMktAndSpecVols_tbl['Index']=='J200')]

Topitest3.sort_values(by=['Instrument'], inplace=True)
Topitest3.reset_index(drop= True)  # matches SQL


# In[598]:


BetasMktAndSpecVols_tbl


# In[58]:


# z = 0
# i=0
# y=0
# for z in range(0,2):    # z for all index
    
#     print(z)
#     print(y)
#     print(i)
#     print('First')
    
#     for y in range(0,3):        #y for all indexcode
#         i=1
#         print(z)
#         print(y)
#         print(i)
#         print('Second')
        
#         for i in range(2,4):   #i is for all date
#             print(z)
#             print(y)
#             print(i)
#             print('Third')
# for y,z in range()


# In[599]:


BetasMktAndSpecVols_tbl = BetasMktAndSpecVols_tbl.fillna(0)


# In[ ]:





# In[60]:


# BetasMktAndSpecVols_tbl = BetasMktAndSpecVols_tbl[['Date','Instrument','Indices','Index','Industry','Beta','mktVol','specVols']]
# print(BetasMktAndSpecVols_tbl)


# In[600]:


BetasMktAndSpecVols_tbl.rename(columns = {'Index': 'Index_A'}, inplace = True)   # index is a primary key in the database therefore the name needs to be changed


# In[601]:


i= 0
test = pd.DataFrame({})
test1 = pd.DataFrame({})
for i in range(0,11):    # z for all index
    test = GetBetasMktAndSpecVols(rDate[i],mktIndexCode[0],indexCode[11])
    test1 =test1.append(test,ignore_index=True)
    
print(test1[(test1['Date'] == rDate[0])])    # The bottom line of code should result in the same 
    
#### Okay ####


# In[602]:


BetasMktAndSpecVols_tbl
#Okay_everything_matches


# In[603]:


for i in range (0,12):
    print(len(set(BetasMktAndSpecVols_tbl[(BetasMktAndSpecVols_tbl['Indices']==indexCode[i])]['Date'])))


# In[66]:


#BetasMktAndSpecVols_tbl.to_csv (r'C:\Users\ZL2706556\Documents\0.Personal_Career\INF5006S\Project\BetasMktAndSpecVols_tbl.csv',index = False, header=True)


# In[605]:


IndustryWeights2 = BetasMktAndSpecVols_tbl.groupby(by=['Date','Industry','Index_A'],as_index = False)[['ALSIWeights','FLEDWeights','LRGCWeights', 'MIDCWeights',
                                                                                    'SMLCWeights', 'TOPIWeights','RESIWeights', 'FINIWeights', 'INDIWeights', 
                                                                                    'PCAPWeights','SAPYWeights', 'ALTIWeights']].sum()

IndustryWeights2.head()     # when grouping make sure to turn index_as = False


# In[68]:


#IndustryWeights2.to_csv (r'C:\Users\ZL2706556\Documents\0.Personal_Career\INF5006S\Project\IndustryWeights2.csv',index = False, header=True)


# In[69]:


# c['Betas'] = c['Betas'].apply(pd.to_numeric)
# c = c.astype({'Betas':'float64'})


# In[70]:


# cursor.execute('CREATE TABLE BetasMktAndSpecVols_tbl(Date datetime, Instrument nvarchar(50),Indices nvarchar(50),Index_A nvarchar(50), Industry nvarchar(50), Beta float,mktVol float,specVols float)')

# for row in BetasMktAndSpecVols_tbl.itertuples():
#     cursor.execute('''
#                 INSERT INTO AIFMRM_ERS.dbo.BetasMktAndSpecVols_tbl (Date,Instrument,Indices,Index_A,Industry,Beta,mktVol,specVols)
#                 VALUES (?,?,?,?,?,?,?,?)
#                 ''',
#                 row.Date, 
#                 row.Instrument,
#                 row.Indices,
#                 row.Index_A,
#                 row.Industry,
#                 row.Beta,
#                 row.mktVol,
#                 row.specVols,
                               
#                 )
# conn.commit()


# # FUNCTION 3

# In[681]:


def CalcStats(rDate,mktIndexCode,indexCode):
    
    # Diagonal 
    Shares = BetasMktAndSpecVols_tbl[(BetasMktAndSpecVols_tbl['Date'] == rDate) & (BetasMktAndSpecVols_tbl['Index_A'] == mktIndexCode) 
                        & (BetasMktAndSpecVols_tbl['Indices'] ==indexCode) & (BetasMktAndSpecVols_tbl[indexCode +'Weights']> 0 )][['Instrument']]
    Shares_ofinterest = Shares.Instrument.tolist()
    
    
    # Creating Diagonal matrices S and D
    s =  BetasMktAndSpecVols_tbl[(BetasMktAndSpecVols_tbl['Date'] == rDate) & (BetasMktAndSpecVols_tbl['Index_A'] == mktIndexCode) 
                        & (BetasMktAndSpecVols_tbl['Indices'] ==indexCode) & (BetasMktAndSpecVols_tbl[indexCode +'Weights']> 0 )]['specVols']
    S =np.diag(s)
    
    
    # Create numpy arrays
    B = BetasMktAndSpecVols_tbl[(BetasMktAndSpecVols_tbl['Date'] == rDate) & (BetasMktAndSpecVols_tbl['Index_A'] == mktIndexCode) 
                        & (BetasMktAndSpecVols_tbl['Indices'] ==indexCode) & (BetasMktAndSpecVols_tbl[indexCode +'Weights']> 0 )]['Beta']
    B = np.array(B)          # Create numpy arrays
    m = mktVol_tbl[(mktVol_tbl['Date']==rDate) & (mktVol_tbl['Instrument']==mktIndexCode)]['mktVol']
    m = m 
    m = m.item()       # m is a scalar
    w = BetasMktAndSpecVols_tbl[(BetasMktAndSpecVols_tbl['Date'] == rDate) & (BetasMktAndSpecVols_tbl['Index_A'] == mktIndexCode) 
                        & (BetasMktAndSpecVols_tbl['Indices'] ==indexCode) & (BetasMktAndSpecVols_tbl[indexCode +'Weights']> 0 )][indexCode +'Weights']
    w = np.array(w)
    # remove nan's
    B = np.nan_to_num(B)
    w = np.nan_to_num(w) 

    
    #  fixing the matrices shape
    w = np.array(w)[np.newaxis].transpose()
    B = np.array(B)[np.newaxis].transpose()
    wT = w.transpose()      #transpose w for a 1-D array Numpy cant tell the difference, doesnt seem to result in issues
    bT = B.transpose()
    
    #Portfolio Beta
    pfBeta = wT.dot(B)
    #print(Portfolio_Beta)
    
    # Systematic_Covariance_Matrix
    # Systematic_Covariance_Matrix = B.dot(bT)*(m**2)   # Also works
    sysCov = np.dot(B,bT)*(m**2) 
    #print(Systematic_Covariance_Matrix)
    df_sysCov = pd.DataFrame(sysCov,columns= Shares_ofinterest , index = Shares_ofinterest)
    
    
    #Portfolio_Systematic_Variance
    pfSysVol = np.dot(np.dot(np.dot(wT,B),bT),w)*(m**2)
    #print(Portfolio_Systematic_Variance )
    
    #Specific_Covariance_Matrix
    specCov = S**2
    #print(Specific_Covariance_Matrix)
    df_specCov = pd.DataFrame(specCov,columns= Shares_ofinterest , index = Shares_ofinterest)
    
    #Portfolio_Spefic_Variance
    pfSpecVol = np.dot(np.dot(wT,S**2),w)
    #print(Portfolio_Spefic_Variance)
    
    #Total_Covariance_Matrix)
    totCov = np.dot(B,bT)*(m**2) + S**2
    #print(Total_Covariance_Matrix)
    df_totCov = pd.DataFrame(totCov,columns= Shares_ofinterest , index = Shares_ofinterest)
    
    #Portfolio_Variance
    pfVol = np.dot(np.dot(np.dot(wT,B),bT),w)*(m**2) + np.dot(np.dot(wT,S**2),w)
    #print(Portfolio_Variance)

    d = totCov.diagonal()
    D = np.sqrt(d)
    D = np.diag(D)
    D_inv = np.linalg.pinv(D)   
    
    #Correlation Matrix
    CorrMat = np.dot(np.dot(D_inv,totCov),D_inv)
    df_CorrMat = pd.DataFrame(CorrMat,columns= Shares_ofinterest , index = Shares_ofinterest)

    
    return pfBeta,pfSysVol,pfSpecVol
    #return df_CorrMat


# In[683]:


pfBeta,pfSysVol,pfSpecVol= CalcStats(rDate[11],mktIndexCode[0],indexCode[5])
print(pfBeta)
print(pfSysVol)
print(pfSpecVol)


# In[674]:


#df_CorrMat_11.to_csv (r'C:\Users\27605\Documents\0.Personal Career\INF5006S\df_CorrMat_tbl_11.csv',index = True, header=True)


# In[685]:


#Create a Date dataframe
Date_tbl = pd.DataFrame({})
temp = pd.DataFrame({})
for z in range(0,5):    # z for all index
    i=0
    temp['Index'] = mktIndexCode[z]
    temp['Date'] = rDate[i]
    Date_tbl =Date_tbl.append(temp,ignore_index=True)

    
    for i in range(1,12):        #y for all indexcode
        temp['Index'] = mktIndexCode[z]
        temp['Date'] = rDate[i]
        Date_tbl =Date_tbl.append(temp,ignore_index=True)


# In[686]:


z = 0
pfBeta_ALSI = np.array({})
pfBeta_FLED = np.array({})
pfBeta_LRGC = np.array({})
pfBeta_MIDC = np.array({})
pfBeta_SMLC = np.array({})
pfBeta_TOPI = np.array({})
pfBeta_RESI = np.array({})
pfBeta_FINI = np.array({})
pfBeta_INDI = np.array({})
pfBeta_PCAP = np.array({})
pfBeta_SAPY = np.array({})
pfBeta_ALTI = np.array({})
pfBeta_tbl = pd.DataFrame({})
pfSysVol_ALSI = np.array({})
pfSysVol_FLED = np.array({})
pfSysVol_LRGC = np.array({})
pfSysVol_MIDC = np.array({})
pfSysVol_SMLC = np.array({})
pfSysVol_TOPI = np.array({})
pfSysVol_RESI = np.array({})
pfSysVol_FINI = np.array({})
pfSysVol_INDI = np.array({})
pfSysVol_PCAP = np.array({})
pfSysVol_SAPY = np.array({})
pfSysVol_ALTI = np.array({})
pfSysVol_tbl = pd.DataFrame({})
pfSpecVol_ALSI = np.array({})
pfSpecVol_FLED = np.array({})
pfSpecVol_LRGC = np.array({})
pfSpecVol_MIDC = np.array({})
pfSpecVol_SMLC = np.array({})
pfSpecVol_TOPI = np.array({})
pfSpecVol_RESI = np.array({})
pfSpecVol_FINI = np.array({})
pfSpecVol_INDI = np.array({})
pfSpecVol_PCAP = np.array({})
pfSpecVol_SAPY = np.array({})
pfSpecVol_ALTI = np.array({})
pfSpecVol_tbl = pd.DataFrame({})
Date_tbl = pd.DataFrame({})



for z in range(0,5):    # z for all index
   
    
    for i in range(0,12):        #y for all indexcode
        pfBeta,pfSysVol,pfSpecVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[0])
        pfBeta_ALSI =np.append(pfBeta_ALSI,pfBeta)
        pfSysVol_ALSI =np.append(pfSysVol_ALSI,pfSysVol)
        pfSpecVol_ALSI =np.append(pfSpecVol_ALSI,pfSpecVol)
        
pfBeta_tbl['pfBeta_ALSI'] = pfBeta_ALSI
pfSysVol_tbl['pfSysVol_ALSI'] = pfSysVol_ALSI
pfSpecVol_tbl['pfSpecVol_ALSI'] = pfSpecVol_ALSI
        
for z in range(0,5):    # z for all index
 
    
    for i in range(0,12):        #y for all indexcode
        pfBeta,pfSysVol,pfSpecVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[0])
        pfBeta_FLED =np.append(pfBeta_FLED,pfBeta)
        pfSysVol_FLED =np.append(pfSysVol_FLED,pfSysVol)
        pfSpecVol_FLED =np.append(pfSpecVol_FLED,pfSpecVol)
        
pfBeta_tbl['pfBeta_FLED'] = pfBeta_FLED
pfSysVol_tbl['pfSysVol_FLED'] = pfSysVol_FLED
pfSpecVol_tbl['pfSpecVol_FLED'] = pfSpecVol_FLED

for z in range(0,5):    # z for all index
 
    
    for i in range(0,12):        #y for all indexcode
        pfBeta,pfSysVol,pfSpecVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[0])
        pfBeta_LRGC =np.append(pfBeta_LRGC,pfBeta)
        pfSysVol_LRGC =np.append(pfSysVol_LRGC,pfSysVol)
        pfSpecVol_LRGC =np.append(pfSpecVol_LRGC,pfSpecVol)
        
pfBeta_tbl['pfBeta_LRGC'] = pfBeta_LRGC
pfSysVol_tbl['pfSysVol_LRGC'] = pfSysVol_LRGC
pfSpecVol_tbl['pfSpecVol_LRGC'] = pfSpecVol_LRGC


for z in range(0,5):    # z for all index
   
    for i in range(0,12):        #y for all indexcode
        pfBeta,pfSysVol,pfSpecVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[0])
        pfBeta_MIDC =np.append(pfBeta_MIDC,pfBeta)
        pfSysVol_MIDC =np.append(pfSysVol_MIDC,pfSysVol)
        pfSpecVol_MIDC =np.append(pfSpecVol_MIDC,pfSpecVol)
        
pfBeta_tbl['pfBeta_MIDC'] = pfBeta_MIDC
pfSysVol_tbl['pfSysVol_MIDC'] = pfSysVol_MIDC
pfSpecVol_tbl['pfSpecVol_MIDC'] = pfSpecVol_MIDC


for z in range(0,5):    # z for all index

    
    for i in range(0,12):        #y for all indexcode
        pfBeta,pfSysVol,pfSpecVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[0])
        pfBeta_SMLC =np.append(pfBeta_SMLC,pfBeta)
        pfSysVol_SMLC =np.append(pfSysVol_SMLC,pfSysVol)
        pfSpecVol_SMLC =np.append(pfSpecVol_SMLC,pfSpecVol)
        
pfBeta_tbl['pfBeta_SMLC'] = pfBeta_SMLC
pfSysVol_tbl['pfSysVol_SMLC'] = pfSysVol_SMLC
pfSpecVol_tbl['pfSpecVol_SMLC'] = pfSpecVol_SMLC

for z in range(0,5):    # z for all index
    
    
    for i in range(0,12):        #y for all indexcode
        pfBeta,pfSysVol,pfSpecVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[0])
        pfBeta_TOPI =np.append(pfBeta_TOPI,pfBeta)
        pfSysVol_TOPI =np.append(pfSysVol_TOPI,pfSysVol)
        pfSpecVol_TOPI =np.append(pfSpecVol_TOPI,pfSpecVol)
        
pfBeta_tbl['pfBeta_TOPI'] = pfBeta_TOPI
pfSysVol_tbl['pfSysVol_TOPI'] = pfSysVol_TOPI
pfSpecVol_tbl['pfSpecVol_TOPI'] = pfSpecVol_TOPI

        
for z in range(0,5):    # z for all index
   
    for i in range(0,12):        #y for all indexcode
        pfBeta,pfSysVol,pfSpecVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[0])
        pfBeta_RESI =np.append(pfBeta_RESI,pfBeta)
        pfSysVol_RESI =np.append(pfSysVol_RESI,pfSysVol)
        pfSpecVol_RESI =np.append(pfSpecVol_RESI,pfSpecVol)
        
pfBeta_tbl['pfBeta_RESI'] = pfBeta_RESI
pfSysVol_tbl['pfSysVol_RESI'] = pfSysVol_RESI
pfSpecVol_tbl['pfSpecVol_RESI'] = pfSpecVol_RESI


for z in range(0,5):    # z for all index
    
    
    for i in range(0,12):        #y for all indexcode
        pfBeta,pfSysVol,pfSpecVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[0])
        pfBeta_FINI =np.append(pfBeta_FINI,pfBeta)
        pfSysVol_FINI =np.append(pfSysVol_FINI,pfSysVol)
        pfSpecVol_FINI =np.append(pfSpecVol_FINI,pfSpecVol)
        
pfBeta_tbl['pfBeta_FINI'] = pfBeta_FINI
pfSysVol_tbl['pfSysVol_FINI'] = pfSysVol_FINI
pfSpecVol_tbl['pfSpecVol_FINI'] = pfSpecVol_FINI


for z in range(0,5):    # z for all index
    
    
    for i in range(0,12):        #y for all indexcode
        pfBeta,pfSysVol,pfSpecVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[0])
        pfBeta_INDI =np.append(pfBeta_INDI,pfBeta)
        pfSysVol_INDI =np.append(pfSysVol_INDI,pfSysVol)
        pfSpecVol_INDI =np.append(pfSpecVol_INDI,pfSpecVol)
        
pfBeta_tbl['pfBeta_INDI'] = pfBeta_INDI
pfSysVol_tbl['pfSysVol_INDI'] = pfSysVol_INDI
pfSpecVol_tbl['pfSpecVol_INDI'] = pfSpecVol_INDI


for z in range(0,5):    # z for all index
    
    
    for i in range(0,12):        #y for all indexcode
        pfBeta,pfSysVol,pfSpecVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[0])
        pfBeta_PCAP =np.append(pfBeta_PCAP,pfBeta)
        pfSysVol_PCAP =np.append(pfSysVol_PCAP,pfSysVol)
        pfSpecVol_PCAP =np.append(pfSpecVol_PCAP,pfSpecVol)
        
pfBeta_tbl['pfBeta_PCAP'] = pfBeta_PCAP
pfSysVol_tbl['pfSysVol_PCAP'] = pfSysVol_PCAP
pfSpecVol_tbl['pfSpecVol_PCAP'] = pfSpecVol_PCAP

    
for z in range(0,5):    # z for all index
    
    
    for i in range(0,12):        #y for all indexcode
        pfBeta,pfSysVol,pfSpecVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[0])
        pfBeta_SAPY =np.append(pfBeta_SAPY,pfBeta)
        pfSysVol_SAPY =np.append(pfSysVol_SAPY,pfSysVol)
        pfSpecVol_SAPY =np.append(pfSpecVol_SAPY,pfSpecVol)
        
pfBeta_tbl['pfBeta_SAPY'] = pfBeta_SAPY
pfSysVol_tbl['pfSysVol_SAPY'] = pfSysVol_SAPY
pfSpecVol_tbl['pfSpecVol_SAPY'] = pfSpecVol_SAPY


for z in range(0,5):    # z for all index
    
    
    for i in range(0,12):        #y for all indexcode
        pfBeta,pfSysVol,pfSpecVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[0])
        pfBeta_ALTI =np.append(pfBeta_ALTI,pfBeta)
        pfSysVol_ALTI =np.append(pfSysVol_ALTI,pfSysVol)
        pfSpecVol_ALTI =np.append(pfSpecVol_ALTI,pfSpecVol)
        
pfBeta_tbl['pfBeta_ALTI'] = pfBeta_ALTI
pfSysVol_tbl['pfSysVol_ALTI'] = pfSysVol_ALTI
pfSpecVol_tbl['pfSpecVol_ALTI'] = pfSpecVol_ALTI
    


# In[687]:


pfBeta_tbl = pfBeta_tbl.drop(pfBeta_tbl.index[0]).reset_index(drop=True)
pfBeta_tbl.head()

# run only once or it will keep deleting your output


# In[688]:



Date_Index=BetasMktAndSpecVols_tbl.groupby(by=['Index_A','Date'],as_index = False)[['ALSIWeights','FLEDWeights']].sum()  
Date_Index= Date_Index[(Date_Index['Index_A'] != 0)][['Date','Index_A']].reset_index(drop=True)
# Just for get the date and in


# In[79]:


#Date_Index


# In[689]:


pfBetas =pfBeta_tbl.assign(Date = Date_Index['Date'],Index = Date_Index["Index_A"])
pfBetas = pfBetas.iloc[:,np.r_[12:14,0:12]]


# In[690]:


pfBetas.rename(columns = {'Index': 'Index_A'}, inplace = True)
pfBetas.head()


# In[691]:


pfSysVol_tbl = pfSysVol_tbl.drop(pfBeta_tbl.index[0]).reset_index(drop=True)
pfSysVol_tbl.head()

#Given as percentage
# run only once or it will keep deleting your output


# In[692]:


pfSysVol =pfSysVol_tbl.assign(Date = Date_Index['Date'],Index = Date_Index["Index_A"])
pfSysVol = pfSysVol.iloc[:,np.r_[12:14,0:12]]


# In[693]:


pfSysVol.rename(columns = {'Index': 'Index_A'}, inplace = True)
pfSysVol.head()


# In[694]:


pfSpecVol_tbl = pfSpecVol_tbl.drop(pfSpecVol_tbl.index[0]).reset_index(drop=True)
pfSpecVol_tbl.head()

# run only once or it will keep deleting your output


# In[695]:


pfSpecVol =pfSpecVol_tbl.assign(Date = Date_Index['Date'],Index = Date_Index["Index_A"])
pfSpecVol = pfSpecVol.iloc[:,np.r_[12:14,0:12]]


# In[696]:


#pfSpecVol[(pfSpecVol['Index'] == 'J200')]


# In[697]:


pfSpecVol.rename(columns = {'Index': 'Index_A'}, inplace = True)
pfSpecVol.head()


# In[89]:


#pfBetas.to_csv (r'C:\Users\ZL2706556\Documents\0.Personal_Career\INF5006S\Project\pfBetas.csv',index = False, header=True)


# In[90]:


#pfSysVol.to_csv (r'C:\Users\ZL2706556\Documents\0.Personal_Career\INF5006S\Project\pfSysVol.csv',index = False, header=True)


# In[91]:


#pfSpecVol.to_csv (r'C:\Users\ZL2706556\Documents\0.Personal_Career\INF5006S\Project\pfSpecVol.csv',index = False, header=True)


# In[706]:


Synthetic_tbl_temp= pd.merge(IndustryWeights2,pfSpecVol , how='left', on=['Date', 'Index_A'])
Synthetic_tbl_temp= pd.merge(Synthetic_tbl_temp, pfSysVol, how='left', on=['Date', 'Index_A'])
Synthetic_tbl_temp= pd.merge(Synthetic_tbl_temp, pfBetas, how='left', on=['Date', 'Index_A'])
Synthetic_tbl_temp.head()


# In[699]:


Synthetic_tbl_temp.columns


# In[700]:


# Create industry specific stats
Synthetic_tbl_temp['Beta_ALSI'] = Synthetic_tbl_temp['ALSIWeights']*Synthetic_tbl_temp['pfBeta_ALSI']
Synthetic_tbl_temp['Beta_FLED'] = Synthetic_tbl_temp['FLEDWeights']*Synthetic_tbl_temp['pfBeta_FLED']
Synthetic_tbl_temp['Beta_LRGC'] = Synthetic_tbl_temp['LRGCWeights']*Synthetic_tbl_temp['pfBeta_LRGC']
Synthetic_tbl_temp['Beta_MIDC'] = Synthetic_tbl_temp['MIDCWeights']*Synthetic_tbl_temp['pfBeta_MIDC']
Synthetic_tbl_temp['Beta_SMLC'] = Synthetic_tbl_temp['SMLCWeights']*Synthetic_tbl_temp['pfBeta_SMLC']
Synthetic_tbl_temp['Beta_TOPI'] = Synthetic_tbl_temp['TOPIWeights']*Synthetic_tbl_temp['pfBeta_TOPI']
Synthetic_tbl_temp['Beta_RESI'] = Synthetic_tbl_temp['RESIWeights']*Synthetic_tbl_temp['pfBeta_RESI']
Synthetic_tbl_temp['Beta_FINI'] = Synthetic_tbl_temp['FINIWeights']*Synthetic_tbl_temp['pfBeta_FINI']
Synthetic_tbl_temp['Beta_INDI'] = Synthetic_tbl_temp['INDIWeights']*Synthetic_tbl_temp['pfBeta_INDI']
Synthetic_tbl_temp['Beta_PCAP'] = Synthetic_tbl_temp['PCAPWeights']*Synthetic_tbl_temp['pfBeta_PCAP']
Synthetic_tbl_temp['Beta_SAPY'] = Synthetic_tbl_temp['SAPYWeights']*Synthetic_tbl_temp['pfBeta_SAPY']
Synthetic_tbl_temp['Beta_ALTI'] = Synthetic_tbl_temp['ALTIWeights']*Synthetic_tbl_temp['pfBeta_ALTI']

Synthetic_tbl_temp['SpecVol_ALSI'] = Synthetic_tbl_temp['ALSIWeights']*Synthetic_tbl_temp['pfSpecVol_ALSI']
Synthetic_tbl_temp['SpecVol_FLED'] = Synthetic_tbl_temp['FLEDWeights']*Synthetic_tbl_temp['pfSpecVol_FLED']
Synthetic_tbl_temp['SpecVol_LRGC'] = Synthetic_tbl_temp['LRGCWeights']*Synthetic_tbl_temp['pfSpecVol_LRGC']
Synthetic_tbl_temp['SpecVol_MIDC'] = Synthetic_tbl_temp['MIDCWeights']*Synthetic_tbl_temp['pfSpecVol_MIDC']
Synthetic_tbl_temp['SpecVol_SMLC'] = Synthetic_tbl_temp['SMLCWeights']*Synthetic_tbl_temp['pfSpecVol_SMLC']
Synthetic_tbl_temp['SpecVol_TOPI'] = Synthetic_tbl_temp['TOPIWeights']*Synthetic_tbl_temp['pfSpecVol_TOPI']
Synthetic_tbl_temp['SpecVol_RESI'] = Synthetic_tbl_temp['RESIWeights']*Synthetic_tbl_temp['pfSpecVol_RESI']
Synthetic_tbl_temp['SpecVol_FINI'] = Synthetic_tbl_temp['FINIWeights']*Synthetic_tbl_temp['pfSpecVol_FINI']
Synthetic_tbl_temp['SpecVol_INDI'] = Synthetic_tbl_temp['INDIWeights']*Synthetic_tbl_temp['pfSpecVol_INDI']
Synthetic_tbl_temp['SpecVol_PCAP'] = Synthetic_tbl_temp['PCAPWeights']*Synthetic_tbl_temp['pfSpecVol_PCAP']
Synthetic_tbl_temp['SpecVol_SAPY'] = Synthetic_tbl_temp['SAPYWeights']*Synthetic_tbl_temp['pfSpecVol_SAPY']
Synthetic_tbl_temp['SpecVol_ALTI'] = Synthetic_tbl_temp['ALTIWeights']*Synthetic_tbl_temp['pfSpecVol_ALTI']

Synthetic_tbl_temp['SysVol_ALSI'] = Synthetic_tbl_temp['ALSIWeights']*Synthetic_tbl_temp['pfSysVol_ALSI']
Synthetic_tbl_temp['SysVol_FLED'] = Synthetic_tbl_temp['FLEDWeights']*Synthetic_tbl_temp['pfSysVol_FLED']
Synthetic_tbl_temp['SysVol_LRGC'] = Synthetic_tbl_temp['LRGCWeights']*Synthetic_tbl_temp['pfSysVol_LRGC']
Synthetic_tbl_temp['SysVol_MIDC'] = Synthetic_tbl_temp['MIDCWeights']*Synthetic_tbl_temp['pfSysVol_MIDC']
Synthetic_tbl_temp['SysVol_SMLC'] = Synthetic_tbl_temp['SMLCWeights']*Synthetic_tbl_temp['pfSysVol_SMLC']
Synthetic_tbl_temp['SysVol_TOPI'] = Synthetic_tbl_temp['TOPIWeights']*Synthetic_tbl_temp['pfSysVol_TOPI']
Synthetic_tbl_temp['SysVol_RESI'] = Synthetic_tbl_temp['RESIWeights']*Synthetic_tbl_temp['pfSysVol_RESI']
Synthetic_tbl_temp['SysVol_FINI'] = Synthetic_tbl_temp['FINIWeights']*Synthetic_tbl_temp['pfSysVol_FINI']
Synthetic_tbl_temp['SysVol_INDI'] = Synthetic_tbl_temp['INDIWeights']*Synthetic_tbl_temp['pfSysVol_INDI']
Synthetic_tbl_temp['SysVol_PCAP'] = Synthetic_tbl_temp['PCAPWeights']*Synthetic_tbl_temp['pfSysVol_PCAP']
Synthetic_tbl_temp['SysVol_SAPY'] = Synthetic_tbl_temp['SAPYWeights']*Synthetic_tbl_temp['pfSysVol_SAPY']
Synthetic_tbl_temp['SysVol_ALTI'] = Synthetic_tbl_temp['ALTIWeights']*Synthetic_tbl_temp['pfSysVol_ALTI']


# In[707]:


#Create the actual synthetic stat table
# remove all columns starting with pf
Synthetic_tbl  = Synthetic_tbl_temp[Synthetic_tbl_temp.columns.drop(list(Synthetic_tbl_temp.filter(regex='pf')))]
Synthetic_tbl   

#Okay final table for synthetic stat works


# In[702]:


pd.set_option('display.max_columns', None)
print(Synthetic_tbl.head())


# In[708]:


Synthetic_tbl


# In[709]:


Synthetic_tbl.to_csv (r'C:\Users\27605\Documents\0.Personal Career\INF5006S\Synthetic_tbl.csv',index = False, header=True)


# In[710]:


IndustryWeights2.to_csv (r'C:\Users\27605\Documents\0.Personal Career\INF5006S\IndustryWeights21.csv',index = False, header=True)


# # END ####
# ###EVERYTHING BELOW WAS USED TO BUILD FUNCTION 3

# In[749]:


Shares =  BetasMktAndSpecVols_tbl[(BetasMktAndSpecVols_tbl['Date'] == rDate[0]) & (BetasMktAndSpecVols_tbl['Index_A'] == mktIndexCode[0]) 
                        & (BetasMktAndSpecVols_tbl['Indices'] ==indexCode[5]) & (BetasMktAndSpecVols_tbl['TOPIWeights']> 0 )][['Instrument']].head(8)
Shares_ofinterest = Shares.Instrument.tolist()
Shares_ofinterest


# In[750]:


# Creating Diagonal matrices S and D

s = BetasMktAndSpecVols_tbl[(BetasMktAndSpecVols_tbl['Date'] == rDate[0]) & (BetasMktAndSpecVols_tbl['Index_A'] == mktIndexCode[0]) 
                        & (BetasMktAndSpecVols_tbl['Indices'] ==indexCode[5]) & (BetasMktAndSpecVols_tbl['TOPIWeights']> 0 )]['specVols'].head(8)

Shares_Check = BetasMktAndSpecVols_tbl[(BetasMktAndSpecVols_tbl['Date'] == rDate[0]) & (BetasMktAndSpecVols_tbl['Index_A'] == mktIndexCode[0]) 
                        & (BetasMktAndSpecVols_tbl['Indices'] ==indexCode[5]) & (BetasMktAndSpecVols_tbl['TOPIWeights']> 0 )]['Instrument'].head(8)
S = np.diag(s) 


print(Shares_Check)
print(S)
print(s)


# In[167]:


BetasMktAndSpecVols_tbl.columns


# In[386]:


pd.set_option('display.max_columns', None)
BetasMktAndSpecVols_tbl.head()


# In[751]:


B =  BetasMktAndSpecVols_tbl[(BetasMktAndSpecVols_tbl['Date'] == rDate[0]) & (BetasMktAndSpecVols_tbl['Index_A'] == mktIndexCode[0]) 
                        & (BetasMktAndSpecVols_tbl['Indices'] ==indexCode[5]) & (BetasMktAndSpecVols_tbl['TOPIWeights']> 0 )]['Beta'].head(8)
B = np.array(B)    # Create numpy arrays
m = mktVol_tbl[(mktVol_tbl['Date']==rDate[0]) & (mktVol_tbl['Instrument']==mktIndexCode[0])]['mktVol']
print(m)
m = m.item()

w =  BetasMktAndSpecVols_tbl[(BetasMktAndSpecVols_tbl['Date'] == rDate[0]) & (BetasMktAndSpecVols_tbl['Index_A'] == mktIndexCode[0]) 
                        & (BetasMktAndSpecVols_tbl['Indices'] ==indexCode[5]) & (BetasMktAndSpecVols_tbl['TOPIWeights']> 0 )]['TOPI'+'Weights'].head(8)
w = np.array(w)


# In[752]:


B = np.nan_to_num(B)
w = np.nan_to_num(w) 

print(w)


# In[753]:


#  fixing the matrices shape
w = np.array(w)[np.newaxis].transpose()
B = np.array(B)[np.newaxis].transpose()
wT = w.transpose()      #transpose w for a 1-D array Numpy cant tell the difference, doesnt seem to result in issues
bT = B.transpose()


# In[754]:


# If number of row is different for any of the measurements then something is wrong
print(B.shape)    # 314 rows and 1 column as expected
print(S.shape)    # 314 rows and 314 columns as expected
print(w.shape)    # 314 rows and 1 column as expected
print(wT.shape)
print(bT.shape)


# In[110]:


# # aT = np.array(w)[np.newaxis] results in the correct transposing of w
# # a = np.array(a)[np.newaxis]

# wT = w.transpose()      #transpose w for a 1-D array Numpy cant tell the difference, doesnt seem to result in issues
# bT = B.transpose() 


# In[755]:


print(wT.shape)
print(w.shape)
print(bT.shape)
print(B.shape)

print(type(m))


# ### Portfolio Beta

# In[756]:


#(1, 5)*(5, 1)

# expected (1,1)
Portfolio_Beta = wT.dot(B)

print(Portfolio_Beta)  # previous answer is CHANGES


# ### Systematic_Covariance_Matrix 

# In[757]:


# Systematic_Covariance_Matrix = B.dot(bT)*(m**2)   # Also works
#(5, 1)*(1, 5)
# expect a (5,5) matrix
Systematic_Covariance_Matrix = np.dot(B,bT)*(m**2) 
print(Systematic_Covariance_Matrix.shape)
print(Systematic_Covariance_Matrix) 


# In[758]:


df_Systematic_Covariance_Matrix = pd.DataFrame(Systematic_Covariance_Matrix,columns= Shares_ofinterest , index = Shares_ofinterest)
df_Systematic_Covariance_Matrix


# In[325]:


df_Systematic_Covariance_Matrix.to_csv (r'C:\Users\27605\Documents\0.Personal Career\INF5006S\df_Systematic_Covariance_Matrix.csv',index = True, header=True)


# ### Portfolio_Systematic_Variance

# In[759]:


#(1, 5)(5, 1)*(1, 5)*(5, 1)
#(1,1)(1,5)*(5,1)
#(1,5)*(5,1)
#expected (1,1)

Portfolio_Systematic_Variance = np.dot(np.dot(np.dot(wT,B),bT),w)*(m**2)
Portfolio_Systematic_Variance                                          


# ### Specific_Covariance_Matrix

# In[760]:



Specific_Covariance_Matrix = S**2
print(Specific_Covariance_Matrix)


# In[761]:


df_Specific_Covariance_Matrix = pd.DataFrame(Specific_Covariance_Matrix,columns= Shares_ofinterest , index = Shares_ofinterest)
df_Specific_Covariance_Matrix


# In[326]:


df_Specific_Covariance_Matrix.to_csv (r'C:\Users\27605\Documents\0.Personal Career\INF5006S\df_Specific_Covariance_Matrix.csv',index = True, header=True)


# ### Portfolio_Specific_Variance

# In[762]:


#(1, 5)(5, 5)*(5, 1)
#(1,1)    single value to be expected

Portfolio_Spefic_Variance = np.dot(np.dot(wT,S**2),w)
print(Portfolio_Spefic_Variance)


# ### Total_Covariance_Matrix 

# In[763]:


#(5, 1)(1,5)
#(5,5)
Total_Covariance_Matrix = np.dot(B,bT)*(m**2) + S**2

print(Total_Covariance_Matrix)


# In[764]:


df_Total_Covariance_Matrix = pd.DataFrame(Total_Covariance_Matrix,columns= Shares_ofinterest , index = Shares_ofinterest)
df_Total_Covariance_Matrix


# In[327]:


df_Total_Covariance_Matrix.to_csv (r'C:\Users\27605\Documents\0.Personal Career\INF5006S\df_Total_Covariance_Matrix.csv',index = True, header=True)


# ### Portfolio_Variance 

# In[765]:


Portfolio_Variance = np.dot(np.dot(np.dot(wT,B),bT),w)*(m**2) + np.dot(np.dot(wT,S**2),w)
print(Portfolio_Variance)     # should result in single value


# In[767]:


d = Total_Covariance_Matrix.diagonal()
D = np.sqrt(d)
D = np.diag(D)
print(D.shape)
print(D)
D_inv =  np.linalg.pinv(D)  



# ### Correlation Matrix
# 

# In[768]:


Corr_Mat = np.dot(np.dot(D_inv,Total_Covariance_Matrix),D_inv)


# In[769]:


df_Correlation_Matrix = pd.DataFrame(Corr_Mat,columns= Shares_ofinterest , index = Shares_ofinterest)
df_Correlation_Matrix


# In[328]:


df_Correlation_Matrix.to_csv (r'C:\Users\27605\Documents\0.Personal Career\INF5006S\df_Correlation_Matrix.csv',index = True, header=True)

