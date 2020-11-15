<template>
  <div class="container-fluid m-0 p-0">
    <back-to-top text="Back to top" visibleoffset="500"></back-to-top> <!--Creating the Back to top button-->
      <div id = "home-page" class="full-height">
          <div class="container pt-3">
              <div class="row pb-5">
                  <div class="col-lg-6 pt-4 my-auto">
                      <div class="text-center mb-3 d-block d-lrg-none">
                      <h1 class="display-3">{{title}}</h1>
                      <h3>By EvenBeta</h3>
                    </div>
                    <img id= "aifmrm-img" src="../assets/AIFMRM.jpg" class="img-fluig">
                  </div>
                <div class="col-lg-6 my-auto pt-4">
                  <div class="text-center mb-3 d-none d-lrg-block">
                      <h1 class="display-3">{{title}}</h1>
                      <h3>By EvenBeta</h3>
                    </div>
                    <div class="border p-3 p-md-5 bg-white rounded shadow">
                        <h2 class="text-dark">Page Index</h2>
                        <form>
                            <div class="form-group text-dark text-left"> <!--Creating Scroll to certain Div ID's buttons-->
                                <h6>Market Risk</h6>
                                <button v-scroll-to="'#betaplots'">Beta Plots</button>
                                <h6>Portfolio Risk</h6>
                                <button v-scroll-to="'#synthetic'">Synthetic Statistics Decomposition</button>
                                <br>
                                <button v-scroll-to="'#ba_table'">Risk Statistics</button>
                                <br>
                                <button v-scroll-to="'#AllC'">Index Weights</button>
                                <h6>Portfolio Perfomance</h6>
                                <button v-scroll-to="'#quant_tables'">Worked Example</button>
                                <br>
                                <button v-scroll-to="'#corr_topi'">TOPI Correlation Matrix</button>
                            </div>
                        </form>
                    </div>
                  </div>
            </div>
        </div>
    </div>


<!--The first plots for Share and Industry Beta Progression-->
<div id="betaplots" class="container-fluid m-0 p-0 full-height bg-light">
    <h2 class="text-dark p-4">Market Risk</h2>
    <h6 class="text-dark text-left m-4">Discount rates are key input values when performing investment or equity valuation analysis. Modern portfolio theory supports the use of the Markowitz CAPM approach in deriving a first-order approximation of a fair risky rate of return, or market return. The Beta statistic is used to quantify the risky relationship, or level of sensitivity, of a particular industry or instrument, with the selected market proxy, which here may be one of the following: The JSE’s All Share (J203), Top 40 (J200), Financials and Industrials (J250), Industrials (J257), and Resources (J258) indices. It may be used as an input parameter of the CAPM model. Industries are specified by the FTSE’s Industry Classification Benchmark (ICB). </h6>
    <h3 class="text-dark p-4">Beta Progession for Selected Share</h3>
    <div id="dashboard_div7">
      <!--Divs that will hold each control and chart-->
        <div id="filter_div8"></div>
        <div id="filter_div7"></div>
        <div id="linechart_div"></div>
    </div>
    <h6 class="text-dark m-4 text-left">View the progression of an Instrument Beta quarterly for the years 2017-2020. This Beta value represents the sensitivity of changes in the instrument price to changes in the levels of each of the five market proxies. It may thus be used to quantify the proportion of changes in the Instrument price attributable to changes in the market, as well as the expected return of the Instrument.
</h6>

    <h3 class="text-dark p-4">Beta Progession for Selected Industry</h3>
    <div id="dashboard_div9">
      <!--Divs that will hold each control and chart-->
        <div id="filter_div9"></div>
        <div id="filter_div11"></div>
        <div id="pflinechart_div"></div>
    </div>
<h6 class="text-dark m-4 text-left">View the progression of an Industry-level Beta quarterly between 2017 and 2020. This Beta value represents the level of sensitivity of the industry as a whole to changes in the specified market proxy index. This beta may also be used to find an expected return which may further be used as a benchmark against which to measure a portfolio invested in a particular industry weighted by market capitalization.
</h6>

</div>


<!--The second plots for Decomposition of the various portfolio's-->

<div id="synthetic" class="container-fluid m-0 p-0 full-height bg-light">
  <h2 class="text-dark p-4">Portfolio Risk</h2>
  <h6 class="text-dark m-4 text-left">Discount rates are key input values when performing investment or equity valuation analysis. Modern portfolio theory supports the use of the Markowitz CAPM approach in deriving a first-order approximation of a fair risky rate of return, or market return. The Beta statistic is used to quantify the risky relationship, or level of sensitivity, of a particular industry or instrument, with the selected market proxy, which here may be one of the following: The JSE’s All Share (J203), Top 40 (J200), Financials and Industrials (J250), Industrials (J257), and Resources (J258) indices. It may be used as an input parameter of the CAPM model. Industries are specified by the FTSE’s Industry Classification Benchmark (ICB). 
</h6>
    <h3 class="text-dark p-4">Synthetic Index Statistics Decomposition</h3>
    <h4 class="text-dark">Synthetic Weights Decomposition</h4>
    <div id="syn_weights_dash">
      <!--Divs that will hold each control and chart-->
        <div id="weights_proxy_filter"></div>
        <div id="weights_index_filter"></div>
        <div id="weights_chart"></div>
    </div>
    <h6 class="text-dark m-4 text-left">View the weights decomposition over time for the selected Index per Industry. This weight may act as an input in calculating a portfolio’s total risk using the specific and systemic volatility values. 
</h6>
    
    <h4 class="text-dark">Synthetic Betas Decomposition</h4>
    <div id="syn_betas_dash">
      <!--Divs that will hold each control and chart-->
        <div id="betas_index_filter"></div>
        <div id="betas_proxy_filter"></div>
        <div id="betas_chart"></div>
    </div>
    <h6 class="text-dark m-4 text-left">View the total Beta of the selected Index and its decomposition across the 10 different Industries over time. This is useful in determining which industries the index is most sensitive to, and in which timeframes, which will act as useful information to take into account when selecting a benchmark index, tracking index, or market proxy based on riskiness in different macroeconomic market environments.</h6>

    <h4 class="text-dark">Synthetic Specific Volatility Decomposition</h4>
    <div id="syn_spec_dash">
      <!--Divs that will hold each control and chart-->
        <div id="spec_index_filter"></div>
        <div id="spec_proxy_filter"></div>
        <div id="spec_chart"></div>
    </div>
    <h6 class="text-dark m-4 text-left">View the specific variance (value) of the selected Index per quarter over the 12-quarter timeframe between 2017 and 2020. The specific variance breakdown represents the volatility of the selected index that is attributable to idiosyncratic characteristics of each industry. This is useful when tracking a particular Index.
</h6>
    
    <h4 class="text-dark">Synthetic Systematic Volatility Decomposition</h4>
    <div id="syn_sys_dash">
      <!--Divs that will hold each control and chart-->
        <div id="sys_index_filter"></div>
        <div id="sys_proxy_filter"></div>
        <div id="sys_chart"></div>
    </div>
    <h6 class="text-dark m-4 text-left">View the systemic volatility (percent) of the selected Index per quarter over the full 12-quarter timeframe. This systemic volatility is disaggregated over the 10 available Industries and represents risk attributable to changes in the market. This provides insight as to how the composition of systemic risk changes with time and how it changes per Industry.
</h6>


</div>


<!--The first table for Industry Risk Decomposition-->

<div id="ba_table" class="container-fluid m-0 p-0 full-height bg-light">
    <h3 class="text-dark p-4">Risk Statistics</h3>
    <div id="ba_output">
          <div class="row">
              <div class="col-lg p-4" >
                <!--Divs that will hold each control-->
                    <div id="date_filter"></div>
                    <br>
                    <div id="ba_index"></div>
                    <br>
                    <div id="ba_share"></div>
              </div>
                <!--Divs that will hold the table-->
              <div class="col-lg">
                    <div id="ba_chart"></div>
              </div>
        </div>
        <!--Button to download data-->
      <button  class="btn btn-success mt-4 m-3" type="submit" id="CSVDownload" onclick="downloadCSV('download.csv')" title="Download to CSV">Download to CSV</Button>
    </div>
    <h6 class="text-dark m-4 text-left"> View the alpha, beta, total risk, unique risk, R-squared, Beta SE and alpha SE for either a selected instrument or index, at a particular point in time. The R-squared may be used to inform data on the risk that is attributable to movements in the instrument or index itself.
</h6>
</div>


<!--Second Table - All C's Weights - filterable on columns-->

<div id="AllC" class="container-fluid m-0 p-0 full-height bg-light">
    <h3 class="text-dark p-4">Index Weights</h3>
    <div id="dashboard">
    <table>
        <tr style='vertical-align: top'>
            <td style='width: 300px; font-size: 0.9em;'>
                <div id="control1"></div>
                <div id="filter_div4"></div>
                <div id="superfilter_div"></div>
                <div id="subfilter_div"></div>
                <div id="filter_div3"></div>
                <div id="control2"></div>

            </td>
            <td style='width: 600px'>
                <div style="float: left;" id="chart1"></div>
            </td>
        </tr>
    </table>
    </div>
    <h6 class="text-dark m-4 text-left">View the market capitalisation-based weight of either a particular index or industry per Index for a selected point in time. 
</h6>
</div>

<!--Quants Tables -->
<!--make table like structure to hold different elements-->
<div id="quant_tables" class="container-fluid m-0 p-0 full-height bg-light">
  <h2 class="text-dark p-4">Portfolio Performance</h2>
    <h6 class="text-dark m-4 text-left">Portfolio performance is determined by the unique interaction of selected stocks included in that portfolio. This portfolio may be either a combination of stocks selected based on views held by a manager, or an index-tracking combination.</h6>
    <h6 class="text-dark m-4 text-left">The relationship between these Instruments may be quantified by the variance-covariance or correlation matrices which describe mutual Instrument sensitivities and their contributions to portfolio risk. Instrument selection decisions must be informed by both fundamental and market analysis. This Black-Litterman approach requires considering the interactions of combinations of stock return volatilities as contributing to the views-based approach in stock-picking. It is of further interest to consider particular portfolios in order to determine how the relationships between securities may inform portfolio strategy in the future.</h6>
        <h3 class="text-dark p-4">Worked Example</h3>
            <h6 class="text-dark m-4 text-left">The following four tables are constructed based on a hypothetical worked example where an analyst may wish to examine a portfolio comprising of the NPN, SOL, SBK, SGL, and APN instruments which span a wide array of the available Industries and may be interesting in considering diversification effects. These provide insight as to how the risk statistics may be used in determining a portfolio’s performance.</h6>
        <div class="row">
            <div class="col-lg">
                <img id= "image-img" src="../assets/image.png" class="img-fluig p-4">
            </div>
        </div>

         
        <div class="row">
              <div class="col-lg">
                <h4 class="text-dark">Portfolio Specific Risk</h4>
                <h5 class="text-dark">Specific Covariance Matrix</h5>
                <h6 class="text-dark">Portfolio Specific Risk = 0.00063833</h6>
                <div id="specific_table"></div>
                <h6 class="text-dark text-left m-4">View the portfolio specific risk here as quantified by the specific covariance matrix. This provides insight into the contribution to the portfolio’s risk profile that idiosyncratic instrument characteristics make.</h6>
            </div>  

            <div class="col-lg">
                <h4 class="text-dark">Portfolio Systematic Risk</h4>
                <h5 class="text-dark">Portfolio Beta = 0.19959</h5>
                <h5 class="text-dark">Systematic Covariance Matrix</h5>
                <h6 class="text-dark">Portfolio Systematic Risk = 0.00058167</h6>
                <div id="systematic_table"></div>
                <h6 class="text-dark text-left m-4">View the portfolio systematic covariance matrix which describes the market-attributable risk relationships between the five selected stocks.</h6>
            </div>

          </div>

          <div class="row">
            
            <div class="col-lg">
                <h4 class="text-dark">Total Covariance</h4>
                <h5 class="text-dark">Total Covariance Matrix</h5>
                <div id="total_cov_table"></div>
                <h6 class="text-dark text-left m-4">The total covariance is calculated as a linear combination of the specific and systemic covariance matrices.</h6>
            </div>

            <div class="col-lg">
                <h4 class="text-dark">Portfolio Variance</h4>
                <h5 class="text-dark">Correlation Matrix</h5>
                <h6 class="text-dark">Portfolio Variance = 0.00121999</h6>
                <div id="port_cov_table"></div>
                <h6 class="text-dark text-left m-4">The portfolio correlation matrix is calculated by taking the individual instrument weightings within the portfolio into account.</h6>
                
            </div>

          </div>
  </div>


<!--Fourth Table - Correlation for TOPI - filterable on columns-->

<div id="corr_topi" class="container-fluid m-0 p-0 full-height bg-light">
    <h3 class="text-dark p-4">TOPI Correlation Matrix</h3>
    <div id="corr_dashboard">
    <table>
        <tr style='vertical-align: top'>
            <td style='width: 300px; font-size: 0.9em;'>
                <div id="corr_share_filter"></div>
                <div id="corr_column_filter"></div>
            </td>
            <td style='width: 600px'>
                <div style="float: left;" id="corr_table"></div>
            </td>
        </tr>
    </table>
    </div>
    <h6 class="text-dark m-4">The TOPI Index is considered here: This correlation matrix may be used to identify the relationships between this index’s constituent Instruments for a particular quarter.</h6>
</div>

<div id = "about" class="bg-light p-3 p-md-5">
          <div class="row">
              <div class="col-lg">
                  <img src="../assets/computer.svg" height="1" width="80" class="m4">
                  <h5 class="ml-md-4 mr-md-4 text-dark">Dive deeper into the data to get the complete picture </h5>
              </div>

              <div class="col-lg">
                  <img src="../assets/volatility.svg" height="1" width="80" class="m4">
                  <h5 class="ml-md-4 mr-md-4 text-dark">See the specific volatility for any share</h5>
              </div>
              
              <div class="col-lg">
                  <img src="../assets/beta.svg" height="1" width="80" class="m4">
                  <h5 class="ml-md-4 mr-md-4 text-dark">Get an industry specific breakdown on the Beta Statistic for any share </h5>
              </div>

          </div>
  </div>



  <div id="footer" class=" p-4">
    <footer class="text-light p-4">
      <small>&copy; 2020, EvenBeta</small>
    </footer>
  </div>

</div>


  
</template>



<script>
  export default {
    data () {
      return {
        props: ["desserts"],
        title: 'Equity Risk Service',
        weights: [
    {"Date":"2020-06-22","Industry":"Basic Materials","ALSIWeights":"0.2301938159698937","FLEDWeights":"0.12572726097292597","LRGCWeights":"0.2392064301675427","MIDCWeights":"0.15669131537272257","SMLCWeights":"0.15345866201717676","TOPIWeights":"0.22377773940313056","RESIWeights":"1.0","FINIWeights":"0.0","INDIWeights":"0.0","PCAPWeights":"0.0","SAPYWeights":"0.0","ALTIWeights":"0.13769367508253394"},
    {"Date":"2020-06-22","Industry":"Consumer Goods","ALSIWeights":"0.271030965678495","FLEDWeights":"0.06303614457918841","LRGCWeights":"0.29503755817291816","MIDCWeights":"0.06792132911639696","SMLCWeights":"0.10044275218673808","TOPIWeights":"0.21558377164456077","RESIWeights":"0.0","FINIWeights":"0.0","INDIWeights":"0.42606298112453184","PCAPWeights":"0.0","SAPYWeights":"0.0","ALTIWeights":"0.002187327894080848"},
    {"Date":"2020-06-22","Industry":"Consumer Services","ALSIWeights":"0.03665682897828086","FLEDWeights":"0.1177808116008433","LRGCWeights":"0.011667595230183164","MIDCWeights":"0.273605337984557","SMLCWeights":"0.09639247787451265","TOPIWeights":"0.032071247750204875","RESIWeights":"0.0","FINIWeights":"0.0","INDIWeights":"0.04623740385038671","PCAPWeights":"0.0","SAPYWeights":"0.0","ALTIWeights":"0.002953092152964828"},
    {"Date":"2020-06-22","Industry":"Financials","ALSIWeights":"0.12364220342620688","FLEDWeights":"0.46485118053856567","LRGCWeights":"0.09700070075118716","MIDCWeights":"0.33691630905195513","SMLCWeights":"0.36894367177139215","TOPIWeights":"0.11125796276593825","RESIWeights":"0.0","FINIWeights":"1.0","INDIWeights":"0.0","PCAPWeights":"1.0","SAPYWeights":"1.0","ALTIWeights":"0.6244700158659405"},
    {"Date":"2020-06-22","Industry":"Health Care","ALSIWeights":"0.011950321908280898","FLEDWeights":"0.024465979630800373","LRGCWeights":"0.0","MIDCWeights":"0.1273144887030427","SMLCWeights":"0.03104905892288868","TOPIWeights":"0.006176938971956074","RESIWeights":"0.0","FINIWeights":"0.0","INDIWeights":"0.018108774940941146","PCAPWeights":"0.0","SAPYWeights":"0.0","ALTIWeights":"0.03455616788710622"},
    {"Date":"2020-06-22","Industry":"Industrials","ALSIWeights":"0.009699593796344042","FLEDWeights":"0.1419899611557073","LRGCWeights":"0.004268424673771856","MIDCWeights":"0.029590538804245968","SMLCWeights":"0.1685964997591518","TOPIWeights":"0.004914443552396519","RESIWeights":"0.0","FINIWeights":"0.0","INDIWeights":"0.00776091880824969","PCAPWeights":"0.0","SAPYWeights":"0.0","ALTIWeights":"0.028042488637690734"},
    {"Date":"2020-06-22","Industry":"Oil & Gas","ALSIWeights":"0.0004190118956966169","FLEDWeights":"0.001679593003792559","LRGCWeights":"0.0","MIDCWeights":"0.0","SMLCWeights":"0.021696427992867875","TOPIWeights":"0.0","RESIWeights":"0.0","FINIWeights":"0.0","INDIWeights":"0.0","PCAPWeights":"0.0","SAPYWeights":"0.0","ALTIWeights":"0.1361241328916616"},
    {"Date":"2020-06-22","Industry":"Technology","ALSIWeights":"0.29038193989243516","FLEDWeights":"0.05218481831073692","LRGCWeights":"0.3246232953907578","MIDCWeights":"0.0","SMLCWeights":"0.050207169107809535","TOPIWeights":"0.3737544838951257","RESIWeights":"0.0","FINIWeights":"0.0","INDIWeights":"0.4617255544800524","PCAPWeights":"0.0","SAPYWeights":"0.0","ALTIWeights":"0.03198180259530649"},
    {"Date":"2020-06-22","Industry":"Telecommunications","ALSIWeights":"0.026025318454366935","FLEDWeights":"0.008284250207439675","LRGCWeights":"0.0281959956136392","MIDCWeights":"0.007960680967079532","SMLCWeights":"0.009213280367462388","TOPIWeights":"0.03246341201668727","RESIWeights":"0.0","FINIWeights":"0.0","INDIWeights":"0.04010436679583821","PCAPWeights":"0.0","SAPYWeights":"0.0","ALTIWeights":"0.0019912969927148336"}
    ],
    methods: {
    "sortTable": function sortTable(ALSIWeights) {
      this.weights.sort(function(a, b) {
        if (a[ALSIWeights] > b[ALSIWeights]) {
          return 1;
        } else if (a[ALSIWeights] < b[ALSIWeights]) {
          return  -1;
        }
        return 0;
      })
    }
  },
    computed: {
    "columns": function columns() {
      if (this.weights.length == 0) {
        return [];
      }
      return Object.keys(this.weights[0])
      }
    },
    chartData: [
        ['Industry', 'Weight'],
        ['Basic Materials', 0.2301938159698937],
        ['Consumer Goods', 0.271030965678495],
        ['Consumer Services', 0.03665682897828086],
        ['Financials', 0.12364220342620688],
        ['Health Care', 0.011950321908280898],
        ['Industrials', 0.009699593796344042],
        ['Oil & Gas', 0.0004190118956966169],
        ['Technology', 0.29038193989243516],
        ['Telecommunications', 0.026025318454366935]
      ],
      chartOptions: {
        chart: {
          title: 'ALSI Weight Decomposition',
          subtitle: 'As at 2020-06-22',
        }
      },
    

  }
  }
  }

</script>



<!-- Add "scoped" attribute to limit CSS to this component only -->
<style scoped>
    #home-page {
        background-color: aquamarine;
        background: url('../assets/tm2.jpg') no-repeat center center;
    }

    #aifmrm-img {
        max-height: 40vh;
    }

    #about {
        min-height: 10vh;
    }

    #footer {
        background-color: black;
    }

    #maintable {
      background-color: black;
    }

  #table {
  font-family: 'Avenir', Helvetica, Arial, sans-serif;
  width: 450px;
  padding: 20px;
  margin: center;
  

  }

#table th {
  text-transform: uppercase;
  text-align: left;
  background: #44475C;
  color: #FFF;
  cursor: pointer;
  padding: 8px;
  min-width: 30px;
}
#table th:hover {
        background: #717699;
      }
#table td {
  text-align: left;
  padding: 8px;
  border-right: 2px solid #7D82A8;
}
#table td:last-child {
  border-right: none;
}
#table tbody tr:nth-child(2n) td {
  background: #D4D8F9;
}


</style>
