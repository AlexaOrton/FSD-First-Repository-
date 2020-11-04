<template>
  <div id="app">
    <router-view/>
  </div>
</template>

<script>
export default {
  name: 'App'
}
</script>

<style>
#app {
  font-family: 'Avenir', Helvetica, Arial, sans-serif;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  text-align: center;
  color: white;
}

.full-height{
  min-height: 100vh;
}

  .blue-background {
    background-color: lightblue;
  }

  .white-background {
    background-color: white;
  }

  .pink-background {
    background-color: pink;
  }
 
 .bold-font {
    font-weight: bold;
  }

  .italic-darkblue-font {
    font-style: italic;
    color: darkblue;
  }

  .black-font{
    color: black
  }


</style>
