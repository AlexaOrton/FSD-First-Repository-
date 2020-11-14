<template>
  <div class="container-fluid m-0 p-0">
    <back-to-top text="Back to top" visibleoffset="500"></back-to-top>
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
                            <div class="form-group text-dark">
                                <button v-scroll-to="'#betaplots'">Beta Plots</button>
                                <br>
                                <button v-scroll-to="'#fiddle'">ALL C's Weights</button>
                                <br>
                                <button v-scroll-to="'#synweights'">Systhetic Statistics Decomposition</button>
                            </div>
                        </form>
                    </div>
                  </div>
              </div>
          </div>
      </div>


<div id="betaplots" class="container-fluid m-0 p-0 full-height bg-light">
    <h3 class="text-dark p-4">Beta Progession for Selected Share</h3>
    <div id="dashboard_div7">
      <!--Divs that will hold each control and chart-->
      <div id="filter_div8"></div>
      <div id="filter_div7"></div>
      <div id="linechart_div"></div>
    </div>

    <h3 class="text-dark p-4">Beta Progession for Selected Industry</h3>
    <div id="dashboard_div9">
      <!--Divs that will hold each control and chart-->
      <div id="filter_div9"></div>
      <div id="filter_div11"></div>
      <div id="pflinechart_div"></div>
    </div>


</div>


<div id="synweights" class="container-fluid m-0 p-0 full-height bg-light">
    <h3 class="text-dark p-4">Synthetic Index Statistics Decomposition</h3>
    <div id="synthetic_weights"></div>
    <div id="synthetic_betas"></div>
    <div id="synthetic_spec"></div>
    <div id="synthetic_sys"></div>
</div>


<div id="fiddle" class="container-fluid m-0 p-0 full-height bg-light">
    <h3 class="text-dark p-4"> All C's Weights</h3>
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
</div>








<div id = "about" class="bg-light p-3 p-md-5">
          <div class="row">
              <div class="col-lg">
                  <img src="../assets/computer.svg" height="60" class="m4">
                  <h5 class="ml-md-4 mr-md-4 text-dark">Dive deeper into the data to get the complete picture </h5>
              </div>
              
              <div class="col-lg">
                  <img src="../assets/beta.svg" height="60" class="m4">
                  <h5 class="ml-md-4 mr-md-4 text-dark">Get an industry specific breakdown on the Beta Statistic for any share </h5>
              </div>

              <div class="col-lg">
                  <img src="../assets/volatility.svg" height="60" class="m4">
                  <h5 class="ml-md-4 mr-md-4 text-dark">See the specific volatility for any share</h5>
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
