## FSD-First-Repository-
An open-source application developed for AIFMRM in order to disseminate the Equity Risk Statistics data in an intuitive and interactive manner.
Created by EvenBeta.

## QUERIES & COLLABORATION
Should you wish to contribute to this project, please contact EvenBeta at ortonalexa@gmail.com and outline your proposed changes. 
Once this is approved, create a branch on which you may work to change or update the component of your interest.
Direct any queries to ortonalexa@gmail.com

## APP DESCRIPTION & EXPLANATION
This app has been designed for the use of AIFMRM's financial industry clients in order to access curated risk statistic data.
The end use of this data will fall into one of three categories: Market Risk, Portfolio Risk and Portfolio Performance.
The aim of the application is to improve upon the current static dissemination methods of AIFMRM in order to grow their client base and improve upon the experience of current users.

# Technologies & Libraries
- Vue.js is the main library used in the app development.
- Bootstrap has been used for ease of templating.
- npm is used to organise the app and integrate with GitHub.
- The app makes use of an SQL database which is stored in the Azure Cloud Data Studio. This virtual database is password-protected and acts as a data repository for future data updates.
- The data is then converted to JSON, Google data, which is fed into Google Charts.
- All data visualisations are made using Google Charts and the Google Visualisation API.

# Structure of the App:
- File structure:
  - The App.vue includes the style and app template code.
  - The HomePage.vue is the MAIN COMPONENT of thr app and includes the overall <div> structure outlining the graphs and table placement, along with the applicable styles.
  - The index.html file outlines the setup of the Ajax API and Cloudflare host of the app url. It also containf the functions that convert the .csv input
    into JSON, and then the functions that convert the JSON into thr Google Visualisation API.
    The Google Visualisation API links the data to the Google Charts functions in order to display the risk statistic data.
  - The index.js file indicates where the HomePage.vue component shoudl be accessed from.
  - The main.js file initialises the use of Vue.js, bootstrap, ESLINT and Google Charts.
    
- Layout:
  - A landing page comprising the AIFMRM logo, table mountain imagery and an Index page of hyperlinks.
  - The hyperlinks are connected to different components of the app.
  - these link to the different <div> components.
  - these have been coded as buttons.
  - the initial page is also associated with a back to top button.
  
  - 3 main sections: Market Risk, Portfolio Risk and Portfolio Performance.
  - MARKET RISK:
    Discount rates are key input values when performing investment or equity valuation analysis. 
    Modern portfolio theory supports the use of the Markowitz CAPM approach in deriving a first-order approximation of a fair risky rate of return, or market return.
    The Beta statistic is used to quantify the risky relationship, or level of sensitivity, of a particular industry or instrument, with the selected market proxy, which here may be one of the following: 
    The JSE’s All Share (J203), Top 40 (J200), Financials and Industrials (J250), Industrials (J257), and Resources (J258) indices. 
    It may be used as an input parameter of the CAPM model. Industries are specified by the FTSE’s Industry Classification Benchmark (ICB). 
    
    BETA PROGRESSION PLOT 1: View the progression of an Instrument Beta quarterly for the years 2017-2020. 
    This Beta value represents the sensitivity of changes in the instrument price to changes in the levels of each of the five market proxies. 
    It may thus be used to quantify the proportion of changes in the Instrument price attributable to changes in the market, as well as the expected return of the Instrument.
    BETA PROGRESSION PLOT 2: View the progression of an Industry-level Beta quarterly between 2017 and 2020. 
    This Beta value represents the level of sensitivity of the industry as a whole to changes in the specified market proxy index. 
    This beta may also be used to find an expected return which may further be used as a benchmark against which to measure a portfolio invested in a particular industry weighted by market capitalization.
    
    PORTFOLIO RISK:
    Portfolio risk may be disaggregated into specific risk and market risk, or idiosyncratic volatility and systemic variance. 
    This risk may also be attributable to specific Instruments or Industries. 
    Data here may be used in this endeavor to identify sources of risk as being attributable to industry or instrument exposure and may be used to find portfolio beta and performance with respect to a market proxy benchmark. 
    These statistics may also be calculated for an index-tracking portfolio across the following 12 specified Indexes: ALSI, FLED, LRGC, MIDC, SMLC, TOPI, RESI, FINI, INDI, PCAP, and SAPY
    
    SYS VOL PLOT 1: View the systemic volatility (percent) of the selected Index per quarter over the full 12-quarter timeframe. 
    This systemic volatility is disaggregated over the 10 available Industries and represents risk attributable to changes in the market. 
    This provides insight as to how the composition of systemic risk changes with time and how it changes per Industry.
    
    SPEC VOL PLOT 2: View the specific variance (value) of the selected Index per quarter over the 12-quarter timeframe between 2017 and 2020. 
    The specific variance breakdown represents the volatility of the selected index that is attributable to idiosyncratic characteristics of each industry. 
    This is useful when tracking a particular Index.
    
    WEIGHTS PLOT 3: View the weights decomposition over time for the selected Index per Industry. 
    This weight may act as an input in calculating a portfolio’s total risk using the specific and systemic volatility values. 
    
    TOTAL BETA PLOT 4: View the total Beta of the selected Index and its decomposition across the 10 different Industries over time. 
    This is useful in determining which industries the index is most sensitive to, and in which timeframes, 
    which will act as useful information to take into account when selecting a benchmark index, tracking index, or market proxy based on riskiness in different macroeconomic market environments.

    WEIGHTS TABLE 1: View the market capitalisation-based weight of either a particular index or industry per Index for a selected point in time. 
    
    SYNTH STATS TABLE 2: View the alpha, beta, total risk, unique risk, R-squared, Beta SE and alpha SE for either a selected instrument or index, 
    at a particular point in time. The R-squared may be used to inform data on the risk that is attributable to movements in the instrument or index itself.
    
    PORTFOLIO PERFORMANCE:
    Portfolio performance is determined by the unique interaction of selected stocks included in that portfolio. 
    This portfolio may be either a combination of stocks selected based on views held by a manager, or an index-tracking combination. 
    The relationship between these Instruments may be quantified by the variance-covariance or correlation matrices which describe mutual Instrument sensitivities and their contributions to portfolio risk. 
    Instrument selection decisions must be informed by both fundamental and market analysis. 
    This Black-Litterman approach requires considering the interactions of combinations of stock return volatilities as contributing to the views-based approach in stock-picking. 
    It is of further interest to consider particular portfolios in order to determine how the relationships between securities may inform portfolio strategy in the future.
    
    The following four tables are constructed based on a hypothetical worked example where an analyst may wish to examine a portfolio comprising of the NPN, SOL, SBK, SGL, and APN instruments, 
    which span a wide array of the available Industries and may be interesting in considering diversification effects. 
    These provide insight as to how the risk statistics may be used in determining a portfolio’s performance. 
    
    SYS COV TABLE 1: View the portfolio systematic covariance matrix which describes the market-attributable risk relationships between the five selected stocks.
    SPEC VAR TABLE 2: View the portfolio specific risk here as quantified by the specific covariance matrix. 
    This provides insight into the contribution to the portfolio’s risk profile that idiosyncratic instrument characteristics make. 
    TOT COV TABLE 3: The total covariance is calculated as a linear combination of the specific and systemic covariance matrices.
    CORR MAT TABLE 4: The portfolio correlation matrix is calculated by taking the individual instrument weightings within the portfolio into account. 
    
    TOPI MATRIX: The TOPI Index is considered here: This correlation matrix may be used to identify the relationships between this index’s constituent Instruments for a particular quarter.
    
## APPLICATION DEPLOYMENT INSTRUCTIONS
Clear instructions that an end-user will be able to follow in order to install, test and run your application on their local machine.

1. Download the full set of files from the Master Repository
2. Open your terminal
3. cd evenbeta-ers-dashboard
4. npm run dev

## SECURITY MEASURES
The Data set has been stored in the virtual Azure SQL data management system. This is protected in that it only allows access by particular IP addresses due to its firewall.
Furthermore the dataset is protected by password.
Thereafter the data is read straight into the app and so no security measures wre required in terms of API keys.

## LINK TO DEPLOYED APPLICATION
https://evenbeta-ers-dashboard.web.app/#/
