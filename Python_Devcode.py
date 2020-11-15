   
import pandas as pd
import pyodbc

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
cursor = conn.cursor

# 0.1 Import Index Constituents
tbl_Index_Constituents = pd.read_sql_query('SELECT * FROM dbo.tbl_Index_Constituents',conn)
tbl_Index_Constituents = tbl_Index_Constituents[['Date', 'Rank','Alpha' ,'Instrument', 'ICB Sub-Sector', 'Gross Market Capitalisation', 
                                                'ALSI New', 'Index New', 'TOPI New', 'DTOP New', 'RESI New', 'FINI New', 'INDI New',
                                                 'PCAP New', 'SAPY New', 'ALTI New']]

tbl_Consti =tbl_Index_Constituents
tbl_Consti.rename(columns = {'Instrument':'Company','Alpha':'Instrument'}, inplace=True)

print(tbl_Index_Constituents)
print(type(tbl_Index_Constituents))    
#[4331 rows x 46 columns]

# 0.2 Import Industry Classification Benchmark
tbl_Industry_Classification_Benchmark = pd.read_sql_query('SELECT * FROM dbo.tbl_Industry_Classification_Benchmark',conn)
tbl_Industry_Classification_Benchmark = tbl_Industry_Classification_Benchmark[['Industry', 'Super Sector','Sector' ,'Sub-Sector Code', 'Sub-Sector']]
print(tbl_Industry_Classification_Benchmark)
print(type(tbl_Industry_Classification_Benchmark))
#[114 rows x 8 columns]

# 0.3 Import BA Beta Output
tbl_BA_Beta_Output = pd.read_sql_query('SELECT * FROM dbo.tbl_BA_Beta_Output',conn)
tbl_BA_Beta_Output = tbl_BA_Beta_Output[['Date','Instrument','Index','Beta','Total Risk','Unique Risk']]
print(tbl_BA_Beta_Output)
print(type(tbl_BA_Beta_Output))  
#[26710 rows x 16 columns]


#rDate = (tbl_Index_Constituents['Date']).max()
rDate = sorted(list(set(tbl_Index_Constituents['Date'])))
type(rDate)
rDate
#or item in rDate:
    #rint(item)
#print(rDate.iloc[:,])

#rDate = (tbl_Index_Constituents['Date']).max()
rDate = sorted(list(set(tbl_Index_Constituents['Date'])))
type(rDate)
rDate
#or item in rDate:
    #rint(item)
#print(rDate.iloc[:,])
pd.set_option('mode.chained_assignment', None)    # or use iloc method

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
        
    elif indexCode == 'PACP':
        Indices = 'PACP New'

    elif indexCode == 'SAPY':
        Indices = 'SAPY New'

    elif indexCode == 'ALTI':
        Indices = 'ALTI New'

    return Indices

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
    
    #Get the weights
    GetWeights = GetICs[['Date','Instrument',indexCode +'Weights']]
    
    print(GetWeights[indexCode +'Weights'].sum())      ### Check weights add up to 1
    
    return GetWeights

#Call function 1
print(GetICsAndWeights(rDate[7],indexCode[10]))

# Find the distinct quartly dates from the Beta_output as rDate1. sort it to match rDate
rDate1 = sorted(list(set(tbl_BA_Beta_Output['Date'])))  

#Market proxies into mktIndexCode variable
mktIndexCode = ['J200','J203','J250','J257','J258'] 

Beta_t = tbl_BA_Beta_Output

#### Function 2 ####  
def GetBetasMktAndSpecVols(rDate1,rDate,mktIndexCode,indexCode):
    
    #Note to add if statement to ensure the correct rDate1 matches with rDate. Current method still works
    #filter data according to quarter and market proxy
    Beta_rdate = Beta_t[(Beta_t['Date'] == rDate1) & (Beta_t['Index'] == mktIndexCode)]

    #read in function 1
    ICs = GetICsAndWeights(rDate,indexCode)

    #merge the dataaframes on the Instrument
    Betas_ICs = pd.merge(ICs , Beta_rdate , on='Instrument')
   
    # Change column titles for clarity
    Betas_ICs.rename(columns = {'Total Risk': 'mktVol', 'Unique Risk': 'specVols'}, inplace = True)

    #give the required beta
    Beta = Betas_ICs[['Instrument', 'Beta']]

    #Specific volatility
    specVols = Betas_ICs[['Date_x','Instrument','specVols']]

    #Market volatility
    mktVol = Betas_ICs[['Date_x','mktVol']]
    
    return Beta, specVols, mktVol

# Weights add up to one s

#Call function 2
GetBetasMktAndSpecVols(rDate1[0],rDate[0],mktIndexCode[0],indexCode[0])

