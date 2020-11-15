#!/usr/bin/env python
# coding: utf-8

# In[1]:
#lets see if this really works out the way I hope it can


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
                     'Server=ZACTNB1808001\SQLEXPRESS;'
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


# In[4]:


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


generalStats_tbl.to_csv (r'C:\Users\ZL2706556\Documents\0.Personal_Career\INF5006S\Project\generalStats_tbl.csv',index = False, header=True)


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


# In[ ]:





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


# In[28]:


ICsAndWeights_ALSI


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


# In[32]:


allICsAndWeights[(allICsAndWeights['Date']==rDate[0]) & (allICsAndWeights['Industry']=='Basic Materials') 
                 &  (allICsAndWeights['ALSIWeights']> 0) ]


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


IndustryWeights.to_csv (r'C:\Users\ZL2706556\Documents\0.Personal_Career\INF5006S\Project\IndustryWeights_tbl.csv',index = False, header=True)


# In[41]:


IndustryWeights[(IndustryWeights['Date'] == rDate[3])]['ALSIWeights'].sum()


# In[42]:


allICsAndWeights.to_csv (r'C:\Users\ZL2706556\Documents\0.Personal_Career\INF5006S\Project\allICsAndWeights_tbl.csv',index = False, header=True)


# In[43]:


allICsAndWeights['TOPIWeights'].sum()    # the sum of this should be 12 since there are 12 quarters and each one adds to 1


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


# In[48]:



#### Function 2 ####  
def GetBetasMktAndSpecVols(rDate,mktIndexCode,indexCode):
    
    #Note to add if statement to ensure the correct rDate1 matches with rDate. Current method still works
    #filter data according to quarter and market proxy
    Beta_rdate = Beta_t[(Beta_t['Date'] == rDate) & (Beta_t['Index'] == mktIndexCode)]

    #read in function 1
    ICs = GetICsAndWeights(rDate,indexCode)         #need to filter out the constituents with wieght nan
    ICs = ICs.dropna()
    
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
    
    #return Beta, specVols, mktVol  
    return Betas_Mkt_SpecVols, mktVol      
 #   return  Betas_Mkt_SpecVols             # populating DB_tbl


# In[49]:


#Call function 2

#B, m, s,Betas_Mkt_SpecVols = GetBetasMktAndSpecVols(rDate1[i],rDate[i],mktIndexCode[y],indexCode[z])
Betas_Mkt_SpecVols, mktVol   = GetBetasMktAndSpecVols(rDate[6],mktIndexCode[0],indexCode[6])
print(mktVol)


# In[50]:



indexCode


# In[51]:


y = 0
mktVol_tbl = pd.DataFrame({})
for y in range(0,5):    # y for all index range is actually from 0-4
    i=0
    c,mktVol = GetBetasMktAndSpecVols(rDate[i],mktIndexCode[y],indexCode[0])    # c is just a placeholder
    mktVol_tbl =mktVol_tbl.append(mktVol,ignore_index=True)
    
    for i in range(1,12):        #y for all indexcode
        c,mktVol = GetBetasMktAndSpecVols(rDate[i],mktIndexCode[y],indexCode[0])
        mktVol_tbl = mktVol_tbl.append(mktVol,ignore_index=True)


# In[52]:


# i,y = 1,1     

# for y in range(2,6):    # y for all index
#     print(y)
#     print(i)
    
#     for i in range(3,11):
#         print(y)
#         print(i)


# In[53]:


print(mktVol_tbl)    # just note to use rDate1


# In[54]:


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


# In[55]:


z = 0
i = 0
y = 0
BetasMktAndSpecVols_tbl = pd.DataFrame({})
for z in range(0,5):    # z for all index

    
    for y in range(0,12):        #y for all indexcode
       
        
        for i in range(2,12):   #i is for all dates
            Betas_Mkt_SpecVols = GetBetasMktAndSpecVols(rDate[i],mktIndexCode[z],indexCode[y])
            BetasMktAndSpecVols_tbl =BetasMktAndSpecVols_tbl.append(Betas_Mkt_SpecVols,ignore_index=True)
            


# In[56]:


z = 0
i = 0
y = 0
BetasMktAndSpecVols_tbl = pd.DataFrame({})
for z in range(0,5):    # z for all index
    
    for y in range(0,12):        #y for all indexcode
        
        for i in range(0,12):   #i is for all dates
            Betas_Mkt_SpecVols = GetBetasMktAndSpecVols(rDate[i],mktIndexCode[z],indexCode[y])
            BetasMktAndSpecVols_tbl =BetasMktAndSpecVols_tbl.append(Betas_Mkt_SpecVols,ignore_index=True)
            


# In[57]:


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


# In[58]:


BetasMktAndSpecVols_tbl = BetasMktAndSpecVols_tbl.fillna(0)


# In[ ]:





# In[59]:


# BetasMktAndSpecVols_tbl = BetasMktAndSpecVols_tbl[['Date','Instrument','Indices','Index','Industry','Beta','mktVol','specVols']]
# print(BetasMktAndSpecVols_tbl)


# In[60]:


BetasMktAndSpecVols_tbl.rename(columns = {'Index': 'Index_A'}, inplace = True)   # index is a primary key in the database therefore the name needs to be changed


# In[61]:


i= 0
test = pd.DataFrame({})
test1 = pd.DataFrame({})
for i in range(0,11):    # z for all index
    test = GetBetasMktAndSpecVols(rDate[i],mktIndexCode[0],indexCode[11])
    test1 =test1.append(test,ignore_index=True)
    
print(test1[(test1['Date'] == rDate[0])])    # The bottom line of code should result in the same 
    
#### Okay ####


# In[62]:


BetasMktAndSpecVols_tbl
#Okay_everything_matches


# In[63]:


for i in range (0,12):
    print(len(set(BetasMktAndSpecVols_tbl[(BetasMktAndSpecVols_tbl['Indices']==indexCode[i])]['Date'])))


# In[64]:


BetasMktAndSpecVols_tbl[ (BetasMktAndSpecVols_tbl['Index_A'] == 'J258') & (BetasMktAndSpecVols_tbl['Date'] == rDate[0]) &(BetasMktAndSpecVols_tbl['Indices'] =='ALSI')]


# In[65]:


BetasMktAndSpecVols_tbl.to_csv (r'C:\Users\ZL2706556\Documents\0.Personal_Career\INF5006S\Project\BetasMktAndSpecVols_tbl.csv',index = False, header=True)


# In[66]:


IndustryWeights2 = BetasMktAndSpecVols_tbl.groupby(by=['Date','Industry','Index_A'],as_index = False)[['ALSIWeights','FLEDWeights','LRGCWeights', 'MIDCWeights',
                                                                                    'SMLCWeights', 'TOPIWeights','RESIWeights', 'FINIWeights', 'INDIWeights', 
                                                                                    'PCAPWeights','SAPYWeights', 'ALTIWeights']].sum()

IndustryWeights2.head()     # when grouping make sure to turn index_as = False


# In[67]:


IndustryWeights2.to_csv (r'C:\Users\ZL2706556\Documents\0.Personal_Career\INF5006S\Project\IndustryWeights2.csv',index = False, header=True)


# In[68]:


# c['Betas'] = c['Betas'].apply(pd.to_numeric)
# c = c.astype({'Betas':'float64'})


# In[69]:


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

# In[70]:


def CalcStats(rDate,mktIndexCode,indexCode):
    
    # Creating Diagonal matrices S and D
    s = BetasMktAndSpecVols_tbl[(BetasMktAndSpecVols_tbl['Date'] == rDate) & (BetasMktAndSpecVols_tbl['Index_A'] == mktIndexCode) 
                        & (BetasMktAndSpecVols_tbl['Indices'] ==indexCode )]['specVols']
    S =np.diag(s)
    
    # D_invr = np.linalg.inv(D)
    #print(D_inv)       # Has a determinant of zero. This is the definition of a Singular matrix 
                    #[one for which an inverse does not exist]
    
    # Create numpy arrays
    B = BetasMktAndSpecVols_tbl[(BetasMktAndSpecVols_tbl['Date'] == rDate) & (BetasMktAndSpecVols_tbl['Index_A'] == mktIndexCode) 
                        & (BetasMktAndSpecVols_tbl['Indices'] == indexCode)]['Beta']
    B = np.array(B)          # Create numpy arrays
    m = mktVol_tbl[(mktVol_tbl['Date']==rDate) & (mktVol_tbl['Instrument']==mktIndexCode)]['mktVol']
    m = m.item()       # m is a scalar
    w = BetasMktAndSpecVols_tbl[(BetasMktAndSpecVols_tbl['Date'] == rDate) & (BetasMktAndSpecVols_tbl['Index_A'] == mktIndexCode) 
                        & (BetasMktAndSpecVols_tbl['Indices'] == indexCode)][indexCode +'Weights']
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
    
    #Portfolio_Systematic_Variance
    pfSysVol = np.dot(np.dot(np.dot(wT,B),bT),w)*(m**2)
    #print(Portfolio_Systematic_Variance )
    
    #Specific_Covariance_Matrix
    specCov = S**2
    #print(Specific_Covariance_Matrix)
    
    #Portfolio_Spefic_Variance
    pfSpecVol = np.dot(np.dot(wT,S**2),w)
    #print(Portfolio_Spefic_Variance)
    
    #Total_Covariance_Matrix)
    totCov = np.dot(B,bT)*(m**2) + S**2
    #print(Total_Covariance_Matrix)
    
    #Portfolio_Variance
    pfVol = np.dot(np.dot(np.dot(wT,B),bT),w)*(m**2) + np.dot(np.dot(wT,S**2),w)
    #print(Portfolio_Variance)

    D = totCov.diagonal()
    D = np.diag(D)
    #D_inv =  np.linalg.inv(D)  # Has a determinant of zero. This is the definition of a Singular matrix (one for which an inverse does not exist)
    #D_inv =  np.invert(D)   # also doesnt work for some reason
    
    #CorrMat = D_inv.dot(B.dot(bT))*(m**2) + S**2   #need D_inv
    
    
    return pfSpecVol


# In[99]:


pfBeta = CalcStats(rDate[0],mktIndexCode[2],indexCode[0])
print(pfBeta)


# In[72]:


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


# In[73]:


Date_tbl


# In[74]:


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
Date_tbl = pd.DataFrame({})



for z in range(0,5):    # z for all index
    i=0
    pfBeta = CalcStats(rDate[i],mktIndexCode[z],indexCode[0])
    pfBeta_ALSI =np.append(pfBeta_ALSI,pfBeta)
    
    
    for i in range(1,12):        #y for all indexcode
        pfBeta = CalcStats(rDate[i],mktIndexCode[z],indexCode[0])
        pfBeta_ALSI =np.append(pfBeta_ALSI,pfBeta)
        
pfBeta_tbl['pfBeta_ALSI'] = pfBeta_ALSI
        
for z in range(0,5):    # z for all index
    i=0
    pfBeta = CalcStats(rDate[i],mktIndexCode[z],indexCode[1])
    pfBeta_FLED =np.append(pfBeta_FLED,pfBeta)
    
    
    for i in range(1,12):        #y for all indexcode
        pfBeta = CalcStats(rDate[i],mktIndexCode[z],indexCode[1])
        pfBeta_FLED =np.append(pfBeta_FLED,pfBeta)
        
pfBeta_tbl['pfBeta_FLED'] = pfBeta_FLED

for z in range(0,5):    # z for all index
    i=0
    pfBeta = CalcStats(rDate[i],mktIndexCode[z],indexCode[2])
    pfBeta_LRGC =np.append(pfBeta_LRGC,pfBeta)
    
    
    for i in range(1,12):        #y for all indexcode
        pfBeta = CalcStats(rDate[i],mktIndexCode[z],indexCode[2])
        pfBeta_LRGC =np.append(pfBeta_LRGC,pfBeta)

pfBeta_tbl['pfBeta_LRGC'] = pfBeta_LRGC

for z in range(0,5):    # z for all index
    i=0
    pfBeta = CalcStats(rDate[i],mktIndexCode[z],indexCode[3])
    pfBeta_MIDC =np.append(pfBeta_MIDC,pfBeta)
    
    
    for i in range(1,12):        #y for all indexcode
        pfBeta = CalcStats(rDate[i],mktIndexCode[z],indexCode[3])
        pfBeta_MIDC =np.append(pfBeta_MIDC,pfBeta)
        
pfBeta_tbl['pfBeta_MIDC'] = pfBeta_MIDC

for z in range(0,5):    # z for all index
    i=0
    pfBeta = CalcStats(rDate[i],mktIndexCode[z],indexCode[4])
    pfBeta_SMLC =np.append(pfBeta_SMLC,pfBeta)
    
    
    for i in range(1,12):        #y for all indexcode
        pfBeta = CalcStats(rDate[i],mktIndexCode[z],indexCode[4])
        pfBeta_SMLC =np.append(pfBeta_SMLC,pfBeta)
        
pfBeta_tbl['pfBeta_SMLC'] = pfBeta_SMLC

for z in range(0,5):    # z for all index
    i=0
    pfBeta = CalcStats(rDate[i],mktIndexCode[z],indexCode[5])
    pfBeta_TOPI =np.append(pfBeta_TOPI,pfBeta)
    
    
    for i in range(1,12):        #y for all indexcode
        pfBeta = CalcStats(rDate[i],mktIndexCode[z],indexCode[5])
        pfBeta_TOPI =np.append(pfBeta_TOPI,pfBeta)
        
pfBeta_tbl['pfBeta_TOPI'] = pfBeta_TOPI
        
for z in range(0,5):    # z for all index
    i=0
    pfBeta = CalcStats(rDate[i],mktIndexCode[z],indexCode[6])
    pfBeta_RESI =np.append(pfBeta_RESI,pfBeta)
    
    
    for i in range(1,12):        #y for all indexcode
        pfBeta = CalcStats(rDate[i],mktIndexCode[z],indexCode[6])
        pfBeta_RESI =np.append(pfBeta_RESI,pfBeta)
        
pfBeta_tbl['pfBeta_RESI'] = pfBeta_RESI

for z in range(0,5):    # z for all index
    i=0
    pfBeta = CalcStats(rDate[i],mktIndexCode[z],indexCode[7])
    pfBeta_FINI =np.append(pfBeta_FINI,pfBeta)
    
    
    for i in range(1,12):        #y for all indexcode
        pfBeta = CalcStats(rDate[i],mktIndexCode[z],indexCode[7])
        pfBeta_FINI =np.append(pfBeta_FINI,pfBeta)

pfBeta_tbl['pfBeta_FINI'] = pfBeta_FINI

for z in range(0,5):    # z for all index
    i=0
    pfBeta = CalcStats(rDate[i],mktIndexCode[z],indexCode[8])
    pfBeta_INDI =np.append(pfBeta_INDI,pfBeta)
    
    
    for i in range(1,12):        #y for all indexcode
        pfBeta = CalcStats(rDate[i],mktIndexCode[z],indexCode[8])
        pfBeta_INDI =np.append(pfBeta_INDI,pfBeta)
        
pfBeta_tbl['pfBeta_INDI'] = pfBeta_INDI

for z in range(0,5):    # z for all index
    i=0
    pfBeta = CalcStats(rDate[i],mktIndexCode[z],indexCode[9])
    pfBeta_PCAP =np.append(pfBeta_PCAP,pfBeta)
    
    
    for i in range(1,12):        #y for all indexcode
        pfBeta = CalcStats(rDate[i],mktIndexCode[z],indexCode[9])
        pfBeta_PCAP =np.append(pfBeta_PCAP,pfBeta)
        
pfBeta_tbl['pfBeta_PCAP'] = pfBeta_PCAP
    
for z in range(0,5):    # z for all index
    i=0
    pfBeta = CalcStats(rDate[i],mktIndexCode[z],indexCode[10])
    pfBeta_SAPY =np.append(pfBeta_SAPY,pfBeta)
    
    
    for i in range(1,12):        #y for all indexcode
        pfBeta = CalcStats(rDate[i],mktIndexCode[z],indexCode[10])
        pfBeta_SAPY =np.append(pfBeta_SAPY,pfBeta)
        
pfBeta_tbl['pfBeta_SAPY'] = pfBeta_SAPY

for z in range(0,5):    # z for all index
    i=0
    pfBeta = CalcStats(rDate[i],mktIndexCode[z],indexCode[11])
    pfBeta_ALTI =np.append(pfBeta_ALTI,pfBeta)
    
    
    for i in range(1,12):        #y for all indexcode
        pfBeta = CalcStats(rDate[i],mktIndexCode[z],indexCode[11])
        pfBeta_ALTI =np.append(pfBeta_ALTI,pfBeta)
        
pfBeta_tbl['pfBeta_ALTI'] = pfBeta_ALTI        


# In[75]:


pfBeta_tbl = pfBeta_tbl.drop(pfBeta_tbl.index[0]).reset_index(drop=True)
pfBeta_tbl.head()

# run only once or it will keep deleting your output


# In[76]:



Date_Index=BetasMktAndSpecVols_tbl.groupby(by=['Index_A','Date'],as_index = False)[['ALSIWeights','FLEDWeights']].sum()  
Date_Index= Date_Index[(Date_Index['Index_A'] != 0)][['Date','Index_A']].reset_index(drop=True)
# Just for get the date and in


# In[77]:


Date_Index


# In[78]:


pfBetas =pfBeta_tbl.assign(Date = Date_Index['Date'],Index = Date_Index["Index_A"])
pfBetas = pfBetas.iloc[:,np.r_[12:14,0:12]]


# In[79]:


pfBetas


# In[80]:


pfBetas_t = pfBetas[(pfBetas['Index'] == 'J200')]


# In[81]:


pfBetas[(pfBetas['Index'] == 'J200')]


# In[82]:


pfBetas


# In[83]:


pfBetas.to_csv (r'C:\Users\ZL2706556\Documents\0.Personal_Career\INF5006S\Project\pfBetas.csv',index = False, header=True)


# In[84]:


z = 0
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
Date_tbl = pd.DataFrame({})



for z in range(0,5):    # z for all index
    i=0
    pfSysVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[0])
    pfSysVol_ALSI =np.append(pfSysVol_ALSI,pfSysVol)
    
    
    for i in range(1,12):        #y for all indexcode
        pfSysVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[0])
        pfSysVol_ALSI =np.append(pfSysVol_ALSI,pfSysVol)
        
pfSysVol_tbl['pfSysVol_ALSI'] = pfSysVol_ALSI
        
for z in range(0,5):    # z for all index
    i=0
    pfSysVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[1])
    pfSysVol_FLED =np.append(pfSysVol_FLED,pfSysVol)
    
    
    for i in range(1,12):        #y for all indexcode
        pfSysVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[1])
        pfSysVol_FLED =np.append(pfSysVol_FLED,pfSysVol)
        
pfSysVol_tbl['pfSysVol_FLED'] = pfSysVol_FLED

for z in range(0,5):    # z for all index
    i=0
    pfSysVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[2])
    pfSysVol_LRGC =np.append(pfSysVol_LRGC,pfSysVol)
    
    
    for i in range(1,12):        #y for all indexcode
        pfSysVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[2])
        pfSysVol_LRGC =np.append(pfSysVol_LRGC,pfSysVol)

pfSysVol_tbl['pfSysVol_LRGC'] = pfSysVol_LRGC

for z in range(0,5):    # z for all index
    i=0
    pfSysVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[3])
    pfSysVol_MIDC =np.append(pfSysVol_MIDC,pfSysVol)
    
    
    for i in range(1,12):        #y for all indexcode
        pfSysVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[3])
        pfSysVol_MIDC =np.append(pfSysVol_MIDC,pfSysVol)
        
pfSysVol_tbl['pfSysVol_MIDC'] = pfSysVol_MIDC

for z in range(0,5):    # z for all index
    i=0
    pfSysVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[4])
    pfSysVol_SMLC =np.append(pfSysVol_SMLC,pfSysVol)
    
    
    for i in range(1,12):        #y for all indexcode
        pfSysVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[4])
        pfSysVol_SMLC =np.append(pfSysVol_SMLC,pfSysVol)
        
pfSysVol_tbl['pfSysVol_SMLC'] = pfSysVol_SMLC

for z in range(0,5):    # z for all index
    i=0
    pfSysVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[5])
    pfSysVol_TOPI =np.append(pfSysVol_TOPI,pfSysVol)
    
    
    for i in range(1,12):        #y for all indexcode
        pfSysVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[5])
        pfSysVol_TOPI =np.append(pfSysVol_TOPI,pfSysVol)
        
pfSysVol_tbl['pfSysVol_TOPI'] = pfSysVol_TOPI
        
for z in range(0,5):    # z for all index
    i=0
    pfSysVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[6])
    pfSysVol_RESI =np.append(pfSysVol_RESI,pfSysVol)
    
    
    for i in range(1,12):        #y for all indexcode
        pfSysVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[6])
        pfSysVol_RESI =np.append(pfSysVol_RESI,pfSysVol)
        
pfSysVol_tbl['pfSysVol_RESI'] = pfSysVol_RESI

for z in range(0,5):    # z for all index
    i=0
    pfSysVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[7])
    pfSysVol_FINI =np.append(pfSysVol_FINI,pfSysVol)
    
    
    for i in range(1,12):        #y for all indexcode
        pfSysVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[7])
        pfSysVol_FINI =np.append(pfSysVol_FINI,pfSysVol)

pfSysVol_tbl['pfSysVol_FINI'] = pfSysVol_FINI

for z in range(0,5):    # z for all index
    i=0
    pfSysVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[8])
    pfSysVol_INDI =np.append(pfSysVol_INDI,pfSysVol)
    
    
    for i in range(1,12):        #y for all indexcode
        pfSysVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[8])
        pfSysVol_INDI =np.append(pfSysVol_INDI,pfSysVol)
        
pfSysVol_tbl['pfSysVol_INDI'] = pfSysVol_INDI

for z in range(0,5):    # z for all index
    i=0
    pfSysVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[9])
    pfSysVol_PCAP =np.append(pfSysVol_PCAP,pfSysVol)
    
    
    for i in range(1,12):        #y for all indexcode
        pfSysVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[9])
        pfSysVol_PCAP =np.append(pfSysVol_PCAP,pfSysVol)
        
pfSysVol_tbl['pfSysVol_PCAP'] = pfSysVol_PCAP
    
for z in range(0,5):    # z for all index
    i=0
    pfSysVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[10])
    pfSysVol_SAPY =np.append(pfSysVol_SAPY,pfSysVol)
    
    
    for i in range(1,12):        #y for all indexcode
        pfSysVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[10])
        pfSysVol_SAPY =np.append(pfSysVol_SAPY,pfSysVol)
        	
pfSysVol_tbl['pfSysVol_SAPY'] = pfSysVol_SAPY

for z in range(0,5):    # z for all index
    i=0
    pfSysVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[11])
    pfSysVol_ALTI =np.append(pfSysVol_ALTI,pfSysVol)
    
    
    for i in range(1,12):        #y for all indexcode
        pfSysVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[11])
        pfSysVol_ALTI =np.append(pfSysVol_ALTI,pfSysVol)
        
pfSysVol_tbl['pfSysVol_ALTI'] = pfSysVol_ALTI        


# In[85]:


pfSysVol_tbl = pfSysVol_tbl.drop(pfBeta_tbl.index[0]).reset_index(drop=True)
pfSysVol_tbl.head()

#Given as percentage
# run only once or it will keep deleting your output


# In[86]:


pfSysVol =pfSysVol_tbl.assign(Date = Date_Index['Date'],Index = Date_Index["Index_A"])
pfSysVol = pfSysVol.iloc[:,np.r_[12:14,0:12]]


# In[100]:


pfSysVol[(pfSysVol['Index'] == 'J200')]


# In[88]:


pfSysVol


# In[89]:


pfSysVol.to_csv (r'C:\Users\ZL2706556\Documents\0.Personal_Career\INF5006S\Project\pfSysVol.csv',index = False, header=True)


# In[90]:


z = 0
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
    i=0
    pfSpecVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[0])
    pfSpecVol_ALSI =np.append(pfSpecVol_ALSI,pfSpecVol)
    
    
    for i in range(1,12):        #y for all indexcode
        pfSpecVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[0])
        pfSpecVol_ALSI =np.append(pfSpecVol_ALSI,pfSpecVol)
        
pfSpecVol_tbl['pfSpecVol_ALSI'] = pfSpecVol_ALSI
        
for z in range(0,5):    # z for all index
    i=0
    pfSpecVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[1])
    pfSpecVol_FLED =np.append(pfSpecVol_FLED,pfSpecVol)
    
    
    for i in range(1,12):        #y for all indexcode
        pfSpecVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[1])
        pfSpecVol_FLED =np.append(pfSpecVol_FLED,pfSpecVol)
        
pfSpecVol_tbl['pfSpecVol_FLED'] = pfSpecVol_FLED

for z in range(0,5):    # z for all index
    i=0
    pfSpecVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[2])
    pfSpecVol_LRGC =np.append(pfSpecVol_LRGC,pfSpecVol)
    
    
    for i in range(1,12):        #y for all indexcode
        pfSpecVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[2])
        pfSpecVol_LRGC =np.append(pfSpecVol_LRGC,pfSpecVol)

pfSpecVol_tbl['pfSpecVol_LRGC'] = pfSpecVol_LRGC

for z in range(0,5):    # z for all index
    i=0
    pfSpecVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[3])
    pfSpecVol_MIDC =np.append(pfSpecVol_MIDC,pfSpecVol)
    
    
    for i in range(1,12):        #y for all indexcode
        pfSpecVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[3])
        pfSpecVol_MIDC =np.append(pfSpecVol_MIDC,pfSpecVol)
        
pfSpecVol_tbl['pfSpecVol_MIDC'] = pfSpecVol_MIDC

for z in range(0,5):    # z for all index
    i=0
    pfSpecVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[4])
    pfSpecVol_SMLC =np.append(pfSpecVol_SMLC,pfSpecVol)
    
    
    for i in range(1,12):        #y for all indexcode
        pfSpecVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[4])
        pfSpecVol_SMLC =np.append(pfSpecVol_SMLC,pfSpecVol)
        
pfSpecVol_tbl['pfSpecVol_SMLC'] = pfSpecVol_SMLC

for z in range(0,5):    # z for all index
    i=0
    pfSpecVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[5])
    pfSpecVol_TOPI =np.append(pfSpecVol_TOPI,pfSpecVol)
    
    
    for i in range(1,12):        #y for all indexcode
        pfSpecVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[5])
        pfSpecVol_TOPI =np.append(pfSpecVol_TOPI,pfSpecVol)
        
pfSpecVol_tbl['pfSpecVol_TOPI'] = pfSpecVol_TOPI
        
for z in range(0,5):    # z for all index
    i=0
    pfSpecVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[6])
    pfSpecVol_RESI =np.append(pfSpecVol_RESI,pfSpecVol)
    
    
    for i in range(1,12):        #y for all indexcode
        pfSpecVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[6])
        pfSpecVol_RESI =np.append(pfSpecVol_RESI,pfSpecVol)
        
pfSpecVol_tbl['pfSpecVol_RESI'] = pfSpecVol_RESI

for z in range(0,5):    # z for all index
    i=0
    pfSpecVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[7])
    pfSpecVol_FINI =np.append(pfSpecVol_FINI,pfSpecVol)
    
    
    for i in range(1,12):        #y for all indexcode
        pfSpecVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[7])
        pfSpecVol_FINI =np.append(pfSpecVol_FINI,pfSpecVol)

pfSpecVol_tbl['pfSpecVol_FINI'] = pfSpecVol_FINI

for z in range(0,5):    # z for all index
    i=0
    pfSpecVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[8])
    pfSpecVol_INDI =np.append(pfSpecVol_INDI,pfSpecVol)
    
    
    for i in range(1,12):        #y for all indexcode
        pfSpecVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[8])
        pfSpecVol_INDI =np.append(pfSpecVol_INDI,pfSpecVol)
        
pfSpecVol_tbl['pfSpecVol_INDI'] = pfSpecVol_INDI

for z in range(0,5):    # z for all index
    i=0
    pfSpecVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[9])
    pfSpecVol_PCAP =np.append(pfSpecVol_PCAP,pfSpecVol)
    
    
    for i in range(1,12):        #y for all indexcode
        pfSpecVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[9])
        pfSpecVol_PCAP =np.append(pfSpecVol_PCAP,pfSpecVol)
        
pfSpecVol_tbl['pfSpecVol_PCAP'] = pfSpecVol_PCAP
    
for z in range(0,5):    # z for all index
    i=0
    pfSpecVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[10])
    pfSpecVol_SAPY =np.append(pfSpecVol_SAPY,pfSpecVol)
    
    
    for i in range(1,12):        #y for all indexcode
        pfSpecVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[10])
        pfSpecVol_SAPY =np.append(pfSpecVol_SAPY,pfSpecVol)
        
pfSpecVol_tbl['pfSpecVol_SAPY'] = pfSpecVol_SAPY

for z in range(0,5):    # z for all index
    i=0
    pfSpecVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[11])
    pfSpecVol_ALTI =np.append(pfSpecVol_ALTI,pfSpecVol)
    
    
    for i in range(1,12):        #y for all indexcode
        pfSpecVol = CalcStats(rDate[i],mktIndexCode[z],indexCode[11])
        pfSpecVol_ALTI =np.append(pfSpecVol_ALTI,pfSpecVol)
        
pfSpecVol_tbl['pfSpecVol_ALTI'] = pfSpecVol_ALTI          


# In[91]:


pfSpecVol_tbl = pfSpecVol_tbl.drop(pfSpecVol_tbl.index[0]).reset_index(drop=True)
pfSpecVol_tbl.head()

# run only once or it will keep deleting your output


# In[92]:


pfSpecVol =pfSpecVol_tbl.assign(Date = Date_Index['Date'],Index = Date_Index["Index_A"])
pfSpecVol = pfSpecVol.iloc[:,np.r_[12:14,0:12]]


# In[93]:


pfSpecVol[(pfSpecVol['Index'] == 'J200')]


# In[94]:


pfSpecVol


# In[95]:


pfSpecVol.to_csv (r'C:\Users\ZL2706556\Documents\0.Personal_Career\INF5006S\Project\pfSpecVol.csv',index = False, header=True)


# In[96]:


#res = pfBetas.merge(IndustryWeights2, how='inner', left_on=['Date', 'Date'], right_on=['Index_A', 'Index_A'])


# In[97]:


IndustryWeights2[(IndustryWeights2['Date']==rDate[0]) & (IndustryWeights2['Index_A']=='J203')]


# # END ####
# ###EVERYTHING BELOW WAS USED TO BUILD FUNCTION 3

# In[101]:


# Creating Diagonal matrices S and D
s = BetasMktAndSpecVols_tbl[(BetasMktAndSpecVols_tbl['Date'] == rDate[5]) & (BetasMktAndSpecVols_tbl['Index_A'] == 'J200') 
                        & (BetasMktAndSpecVols_tbl['Indices'] == 'ALSI')]['specVols']
S =np.diag(s) 



# print(s)
# print(S)


# In[102]:


BetasMktAndSpecVols_tbl.columns


# In[104]:


m = mktVol_tbl[(mktVol_tbl['Date']==rDate[0]) & (mktVol_tbl['Instrument']==mktIndexCode[0])]['mktVol']
m


# In[105]:


B = BetasMktAndSpecVols_tbl[(BetasMktAndSpecVols_tbl['Date'] == rDate[0]) & (BetasMktAndSpecVols_tbl['Index_A'] == 'J200') 
                        & (BetasMktAndSpecVols_tbl['Indices'] == 'ALSI')]['Beta']
B = np.array(B)    # Create numpy arrays
m = mktVol_tbl[(mktVol_tbl['Date']==rDate[0]) & (mktVol_tbl['Instrument']==mktIndexCode[0])]['mktVol']
m = m.item()
# s = np.array(S)
w = BetasMktAndSpecVols_tbl[(BetasMktAndSpecVols_tbl['Date'] == rDate[0]) & (BetasMktAndSpecVols_tbl['Index_A'] == 'J200') 
                        & (BetasMktAndSpecVols_tbl['Indices'] == 'ALSI')]['ALSI'+'Weights']
w = np.array(w)
print(w)


# In[ ]:


set(BetasMktAndSpecVols_tbl[ (BetasMktAndSpecVols_tbl['Indices'] == 'ALSI')]['Date'])


# In[ ]:


B = np.nan_to_num(B)
w = np.nan_to_num(w) 

print(w)


# In[ ]:


#  fixing the matrices shape
w = np.array(w)[np.newaxis].transpose()
B = np.array(B)[np.newaxis].transpose()
wT = w.transpose()      #transpose w for a 1-D array Numpy cant tell the difference, doesnt seem to result in issues
bT = B.transpose()


# In[ ]:


# If number of row is different for any of the measurements then something is wrong
print(B.shape)    # 314 rows and 1 column as expected
print(S.shape)    # 314 rows and 314 columns as expected
print(w.shape)    # 314 rows and 1 column as expected
print(wT.shape)
print(bT.shape)


# In[ ]:



print(w)
print(w.shape)


# In[ ]:


# # aT = np.array(w)[np.newaxis] results in the correct transposing of w
# # a = np.array(a)[np.newaxis]

# wT = w.transpose()      #transpose w for a 1-D array Numpy cant tell the difference, doesnt seem to result in issues
# bT = B.transpose() 


# In[ ]:


print(wT.shape)
print(w.shape)
print(bT.shape)
print(B.shape)

print(type(m))


# ### Portfolio Beta

# In[ ]:


Portfolio_Beta = wT.dot(B)

print(Portfolio_Beta)  # previous answer is CHANGES


# ### Systematic_Covariance_Matrix 

# In[ ]:


# Systematic_Covariance_Matrix = B.dot(bT)*(m**2)   # Also works
#(1, 314)*(314, 1)
Systematic_Covariance_Matrix = np.dot(B,bT)*(m**2) 
print(Systematic_Covariance_Matrix.shape)
print(Systematic_Covariance_Matrix) 


# ### Portfolio_Systematic_Variance

# In[ ]:


#(1*314)(314*1)x(1*314)x(314*1)x(314*1)
#(1*1)(1*314)x(314*1)x(314*1)
#(1*314)(314*1)x(314*1)
#(1*1)x(314*1)

Portfolio_Systematic_Variance = np.dot(np.dot(np.dot(wT,B),bT),w)*(m**2)
Portfolio_Systematic_Variance                                          


# ### Specific_Covariance_Matrix

# In[ ]:


Specific_Covariance_Matrix = S**2
print(Specific_Covariance_Matrix)


# ### Portfolio_Specific_Variance

# In[ ]:


#(1*314)(314*314) x (314*1)
#(1*314) x (314*1)
#(1*1)    single value to be expected

Portfolio_Spefic_Variance = np.dot(np.dot(wT,S**2),w)
print(Portfolio_Spefic_Variance)


# ### Total_Covariance_Matrix 

# In[ ]:


Total_Covariance_Matrix = np.dot(B,bT)*(m**2) + S**2

print(Total_Covariance_Matrix)


# ### Portfolio_Variance 

# In[ ]:


Portfolio_Variance = np.dot(np.dot(np.dot(wT,B),bT),w)*(m**2) + np.dot(np.dot(wT,S**2),w)
print(Portfolio_Variance)     # should result in single value


# In[ ]:


D = Total_Covariance_Matrix.diagonal()
D = np.diag(D)
print(D.shape)
print(D)
D_inv =  np.linalg.inv(D)  # says determinant of matrix is zero, thus singular matrix
#D_inv =  np.invert(D)   # also doesnt work for some reason


# ### Correlation Matrix
# 

# In[ ]:


# Need to fix the issue of my inverse D
D_inv.dot(B.dot(bT))*(m**2) + S**2

