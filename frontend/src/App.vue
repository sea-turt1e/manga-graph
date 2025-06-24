<template>
  <div id="app">
    <Header />
    <main class="main-content">
      <SearchPanel 
        @search="handleSearch" 
        @clear="handleClear"
      />
      <GraphVisualization 
        :graph-data="graphData"
        :loading="loading"
      />
    </main>
  </div>
</template>

<script>
import { ref, reactive } from 'vue'
import Header from './components/Header.vue'
import SearchPanel from './components/SearchPanel.vue'
import GraphVisualization from './components/GraphVisualization.vue'
import { searchManga } from './services/api'

export default {
  name: 'App',
  components: {
    Header,
    SearchPanel,
    GraphVisualization
  },
  setup() {
    const graphData = reactive({
      nodes: [],
      edges: []
    })
    const loading = ref(false)

    const handleSearch = async (searchQuery) => {
      loading.value = true
      try {
        const result = await searchManga(searchQuery)
        graphData.nodes = result.nodes
        graphData.edges = result.edges
      } catch (error) {
        console.error('Search failed:', error)
        alert('検索に失敗しました。サーバーが起動しているか確認してください。')
      } finally {
        loading.value = false
      }
    }

    const handleClear = () => {
      graphData.nodes = []
      graphData.edges = []
    }

    return {
      graphData,
      loading,
      handleSearch,
      handleClear
    }
  }
}
</script>

<style>
#app {
  min-height: 100vh;
  display: flex;
  flex-direction: column;
}

.main-content {
  display: flex;
  flex: 1;
  height: calc(100vh - 80px);
}
</style>