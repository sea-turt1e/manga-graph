<template>
  <div class="search-panel">
    <div class="search-container">
      <h2 class="search-title">作品検索</h2>
      
      <div class="search-form">
        <div class="search-input-group">
          <input 
            v-model="searchQuery"
            @keyup.enter="handleSearch"
            type="text" 
            placeholder="作品名を入力してください（例：ONE PIECE）"
            class="search-input"
          />
          <button 
            @click="handleSearch"
            :disabled="!searchQuery.trim()"
            class="search-button"
          >
            <span class="search-icon">🔍</span>
            検索
          </button>
        </div>
        
        <div class="search-options">
          <label class="option-label">
            <input 
              v-model="searchDepth" 
              type="range" 
              min="1" 
              max="3" 
              class="depth-slider"
            />
            検索深度: {{ searchDepth }}
          </label>
        </div>
        
        <button 
          @click="handleClear"
          class="clear-button"
        >
          クリア
        </button>
      </div>

      <div class="search-tips">
        <h3>使い方</h3>
        <ul>
          <li>作品名で検索すると、関連する作者や作品を表示します</li>
          <li>検索深度を調整して、関係の範囲を変更できます</li>
          <li>ノードをドラッグして位置を調整できます</li>
        </ul>
      </div>
    </div>
  </div>
</template>

<script>
import { ref } from 'vue'

export default {
  name: 'SearchPanel',
  emits: ['search', 'clear'],
  setup(props, { emit }) {
    const searchQuery = ref('')
    const searchDepth = ref(2)

    const handleSearch = () => {
      if (searchQuery.value.trim()) {
        emit('search', {
          query: searchQuery.value.trim(),
          depth: searchDepth.value
        })
      }
    }

    const handleClear = () => {
      searchQuery.value = ''
      emit('clear')
    }

    return {
      searchQuery,
      searchDepth,
      handleSearch,
      handleClear
    }
  }
}
</script>

<style scoped>
.search-panel {
  width: 350px;
  background: rgba(255, 255, 255, 0.95);
  backdrop-filter: blur(10px);
  border-right: 1px solid rgba(255, 255, 255, 0.2);
  padding: 20px;
  overflow-y: auto;
  box-shadow: 2px 0 20px rgba(0, 0, 0, 0.1);
}

.search-container {
  height: 100%;
  display: flex;
  flex-direction: column;
}

.search-title {
  font-size: 1.5rem;
  font-weight: bold;
  color: #333;
  margin-bottom: 20px;
  text-align: center;
}

.search-form {
  margin-bottom: 30px;
}

.search-input-group {
  display: flex;
  flex-direction: column;
  gap: 12px;
  margin-bottom: 20px;
}

.search-input {
  padding: 12px 16px;
  border: 2px solid #e0e0e0;
  border-radius: 8px;
  font-size: 1rem;
  transition: all 0.3s ease;
}

.search-input:focus {
  outline: none;
  border-color: #667eea;
  box-shadow: 0 0 0 3px rgba(102, 126, 234, 0.1);
}

.search-button {
  padding: 12px 20px;
  background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
  color: white;
  border: none;
  border-radius: 8px;
  font-size: 1rem;
  font-weight: bold;
  cursor: pointer;
  transition: all 0.3s ease;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
}

.search-button:hover:not(:disabled) {
  transform: translateY(-2px);
  box-shadow: 0 4px 15px rgba(102, 126, 234, 0.4);
}

.search-button:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.search-icon {
  font-size: 1.1rem;
}

.search-options {
  margin-bottom: 20px;
}

.option-label {
  display: block;
  color: #666;
  font-size: 0.9rem;
  margin-bottom: 8px;
}

.depth-slider {
  width: 100%;
  margin-top: 8px;
}

.clear-button {
  width: 100%;
  padding: 10px;
  background: #f5f5f5;
  color: #666;
  border: 1px solid #ddd;
  border-radius: 6px;
  cursor: pointer;
  transition: all 0.3s ease;
}

.clear-button:hover {
  background: #e0e0e0;
}

.search-tips {
  flex: 1;
  padding: 20px;
  background: rgba(255, 255, 255, 0.7);
  border-radius: 8px;
  border: 1px solid rgba(255, 255, 255, 0.3);
}

.search-tips h3 {
  color: #333;
  margin-bottom: 12px;
  font-size: 1.1rem;
}

.search-tips ul {
  list-style: none;
  padding: 0;
}

.search-tips li {
  color: #666;
  font-size: 0.9rem;
  line-height: 1.5;
  margin-bottom: 8px;
  padding-left: 16px;
  position: relative;
}

.search-tips li:before {
  content: '•';
  color: #667eea;
  position: absolute;
  left: 0;
}

@media (max-width: 768px) {
  .search-panel {
    width: 100%;
    height: auto;
    border-right: none;
    border-bottom: 1px solid rgba(255, 255, 255, 0.2);
  }
}
</style>