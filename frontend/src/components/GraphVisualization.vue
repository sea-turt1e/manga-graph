<template>
  <div class="graph-container">
    <div class="graph-header">
      <div class="graph-stats" v-if="graphData.nodes.length > 0">
        <span class="stat-item">
          <span class="stat-icon">📊</span>
          ノード: {{ graphData.nodes.length }}
        </span>
        <span class="stat-item">
          <span class="stat-icon">🔗</span>
          エッジ: {{ graphData.edges.length }}
        </span>
      </div>
      <div class="graph-controls">
        <button @click="fitToScreen" class="control-button" title="画面にフィット">
          <span class="control-icon">🎯</span>
        </button>
        <button @click="resetLayout" class="control-button" title="レイアウトをリセット">
          <span class="control-icon">🔄</span>
        </button>
      </div>
    </div>
    
    <div 
      ref="graphContainer"
      class="graph-viewport"
      :class="{ 'loading': loading }"
    >
      <div v-if="loading" class="loading-overlay">
        <div class="loading-spinner"></div>
        <p class="loading-text">グラフを生成中...</p>
      </div>
      
      <div v-if="!loading && graphData.nodes.length === 0" class="empty-state">
        <div class="empty-icon">🔍</div>
        <h3>検索してください</h3>
        <p>左側の検索パネルから作品名を入力して検索してください</p>
      </div>
    </div>
    
    <div v-if="selectedNode" class="node-info-panel">
      <div class="node-info-header">
        <h3>{{ selectedNode.label }}</h3>
        <button @click="selectedNode = null" class="close-button">×</button>
      </div>
      <div class="node-info-content">
        <p><strong>タイプ:</strong> {{ getNodeTypeLabel(selectedNode.type) }}</p>
        <div v-if="selectedNode.properties">
          <div v-for="(value, key) in selectedNode.properties" :key="key" class="property-item">
            <strong>{{ formatPropertyKey(key) }}:</strong> {{ value }}
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import { ref, onMounted, watch, nextTick } from 'vue'
import cytoscape from 'cytoscape'
import coseBilkent from 'cytoscape-cose-bilkent'

cytoscape.use(coseBilkent)

export default {
  name: 'GraphVisualization',
  props: {
    graphData: {
      type: Object,
      default: () => ({ nodes: [], edges: [] })
    },
    loading: {
      type: Boolean,
      default: false
    }
  },
  setup(props) {
    const graphContainer = ref(null)
    const selectedNode = ref(null)
    let cy = null

    const nodeTypeColors = {
      work: '#4CAF50',
      author: '#2196F3', 
      magazine: '#FF9800',
      unknown: '#9E9E9E'
    }

    const getNodeTypeLabel = (type) => {
      const labels = {
        work: '作品',
        author: '作者',
        magazine: '雑誌',
        unknown: '不明'
      }
      return labels[type] || type
    }

    const formatPropertyKey = (key) => {
      const keyLabels = {
        title: 'タイトル',
        name: '名前',
        publisher: '出版社',
        publication_date: '出版日',
        birth_date: '生年月日',
        nationality: '国籍',
        genre: 'ジャンル'
      }
      return keyLabels[key] || key
    }

    const initializeCytoscape = () => {
      if (!graphContainer.value) return

      cy = cytoscape({
        container: graphContainer.value,
        
        style: [
          {
            selector: 'node',
            style: {
              'background-color': (ele) => nodeTypeColors[ele.data('type')] || nodeTypeColors.unknown,
              'label': 'data(label)',
              'width': 60,
              'height': 60,
              'text-valign': 'bottom',
              'text-margin-y': 10,
              'color': '#333',
              'font-size': '12px',
              'font-weight': 'bold',
              'text-wrap': 'wrap',
              'text-max-width': '120px',
              'border-width': 3,
              'border-color': '#fff',
              'box-shadow-blur': 6,
              'box-shadow-color': 'rgba(0,0,0,0.3)',
              'box-shadow-offset-x': 2,
              'box-shadow-offset-y': 2
            }
          },
          {
            selector: 'node:selected',
            style: {
              'border-color': '#FFD700',
              'border-width': 4,
              'box-shadow-blur': 10,
              'box-shadow-color': 'rgba(255,215,0,0.5)'
            }
          },
          {
            selector: 'edge',
            style: {
              'width': 3,
              'line-color': '#ccc',
              'target-arrow-color': '#ccc',
              'target-arrow-shape': 'triangle',
              'curve-style': 'bezier',
              'control-point-step-size': 40,
              'opacity': 0.8
            }
          },
          {
            selector: 'edge[type="created"]',
            style: {
              'line-color': '#4CAF50',
              'target-arrow-color': '#4CAF50'
            }
          },
          {
            selector: 'edge[type="same_publisher"]',
            style: {
              'line-color': '#FF9800',
              'target-arrow-color': '#FF9800',
              'line-style': 'dashed'
            }
          },
          {
            selector: 'edge[type="mentor_of"]',
            style: {
              'line-color': '#9C27B0',
              'target-arrow-color': '#9C27B0',
              'width': 4
            }
          }
        ],

        layout: {
          name: 'cose-bilkent',
          quality: 'default',
          nodeDimensionsIncludeLabels: true,
          refresh: 30,
          fit: true,
          padding: 50,
          randomize: false,
          nodeRepulsion: 4500,
          idealEdgeLength: 100,
          edgeElasticity: 0.45,
          nestingFactor: 0.1,
          gravity: 0.25,
          numIter: 2500,
          tile: true,
          animate: 'during',
          animationDuration: 1000
        }
      })

      cy.on('tap', 'node', (evt) => {
        const node = evt.target
        selectedNode.value = {
          id: node.data('id'),
          label: node.data('label'),
          type: node.data('type'),
          properties: node.data('properties')
        }
      })

      cy.on('tap', (evt) => {
        if (evt.target === cy) {
          selectedNode.value = null
        }
      })
    }

    const updateGraph = () => {
      if (!cy || !props.graphData) return

      const elements = [
        ...props.graphData.nodes.map(node => ({
          data: {
            id: node.id,
            label: node.label,
            type: node.type,
            properties: node.properties
          }
        })),
        ...props.graphData.edges.map(edge => ({
          data: {
            id: edge.id,
            source: edge.source,
            target: edge.target,
            type: edge.type
          }
        }))
      ]

      cy.elements().remove()
      cy.add(elements)
      
      if (elements.length > 0) {
        cy.layout({
          name: 'cose-bilkent',
          quality: 'default',
          nodeDimensionsIncludeLabels: true,
          refresh: 30,
          fit: true,
          padding: 50,
          randomize: true,
          nodeRepulsion: 4500,
          idealEdgeLength: 100,
          edgeElasticity: 0.45,
          nestingFactor: 0.1,
          gravity: 0.25,
          numIter: 2500,
          animate: 'during',
          animationDuration: 1000
        }).run()
      }
    }

    const fitToScreen = () => {
      if (cy) {
        cy.fit(null, 50)
      }
    }

    const resetLayout = () => {
      if (cy) {
        cy.layout({
          name: 'cose-bilkent',
          quality: 'default',
          randomize: true,
          fit: true,
          animate: 'during',
          animationDuration: 1000
        }).run()
      }
    }

    onMounted(() => {
      nextTick(() => {
        initializeCytoscape()
      })
    })

    watch(() => props.graphData, () => {
      nextTick(() => {
        updateGraph()
      })
    }, { deep: true })

    return {
      graphContainer,
      selectedNode,
      getNodeTypeLabel,
      formatPropertyKey,
      fitToScreen,
      resetLayout
    }
  }
}
</script>

<style scoped>
.graph-container {
  flex: 1;
  display: flex;
  flex-direction: column;
  position: relative;
  background: rgba(255, 255, 255, 0.9);
  backdrop-filter: blur(10px);
}

.graph-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 15px 20px;
  background: rgba(255, 255, 255, 0.95);
  border-bottom: 1px solid rgba(255, 255, 255, 0.2);
  box-shadow: 0 2px 10px rgba(0, 0, 0, 0.1);
}

.graph-stats {
  display: flex;
  gap: 20px;
}

.stat-item {
  display: flex;
  align-items: center;
  gap: 6px;
  color: #666;
  font-weight: 500;
}

.stat-icon {
  font-size: 1.1rem;
}

.graph-controls {
  display: flex;
  gap: 10px;
}

.control-button {
  padding: 8px 12px;
  background: rgba(255, 255, 255, 0.8);
  border: 1px solid rgba(255, 255, 255, 0.3);
  border-radius: 6px;
  cursor: pointer;
  transition: all 0.3s ease;
}

.control-button:hover {
  background: rgba(255, 255, 255, 1);
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
}

.control-icon {
  font-size: 1.1rem;
}

.graph-viewport {
  flex: 1;
  position: relative;
  min-height: 400px;
}

.loading-overlay {
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background: rgba(255, 255, 255, 0.9);
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  z-index: 10;
}

.loading-spinner {
  width: 40px;
  height: 40px;
  border: 4px solid #f3f3f3;
  border-top: 4px solid #667eea;
  border-radius: 50%;
  animation: spin 1s linear infinite;
  margin-bottom: 16px;
}

@keyframes spin {
  0% { transform: rotate(0deg); }
  100% { transform: rotate(360deg); }
}

.loading-text {
  color: #666;
  font-size: 1.1rem;
}

.empty-state {
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
  text-align: center;
  color: #666;
}

.empty-icon {
  font-size: 4rem;
  margin-bottom: 16px;
  opacity: 0.5;
}

.empty-state h3 {
  margin-bottom: 12px;
  font-size: 1.5rem;
}

.empty-state p {
  font-size: 1rem;
  line-height: 1.5;
}

.node-info-panel {
  position: absolute;
  top: 20px;
  right: 20px;
  width: 300px;
  background: rgba(255, 255, 255, 0.95);
  backdrop-filter: blur(10px);
  border-radius: 12px;
  box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
  border: 1px solid rgba(255, 255, 255, 0.2);
  z-index: 20;
}

.node-info-header {
  padding: 16px 20px;
  border-bottom: 1px solid rgba(255, 255, 255, 0.2);
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.node-info-header h3 {
  margin: 0;
  color: #333;
  font-size: 1.2rem;
}

.close-button {
  background: none;
  border: none;
  font-size: 1.5rem;
  cursor: pointer;
  color: #666;
  padding: 0;
  width: 24px;
  height: 24px;
  display: flex;
  align-items: center;
  justify-content: center;
}

.close-button:hover {
  color: #333;
}

.node-info-content {
  padding: 16px 20px;
}

.property-item {
  margin-bottom: 8px;
  color: #666;
  font-size: 0.9rem;
  line-height: 1.4;
}

.property-item strong {
  color: #333;
}

@media (max-width: 768px) {
  .graph-header {
    flex-direction: column;
    gap: 12px;
  }
  
  .node-info-panel {
    position: relative;
    width: 100%;
    margin: 10px;
    top: auto;
    right: auto;
  }
}
</style>