<template>
  <header class="header">
    <div class="header-content">
      <h1 class="title">
        <span class="icon">📚</span>
        Manga Graph Visualizer
      </h1>
      <p class="subtitle">メディア芸術データベースを活用した漫画関係図</p>
    </div>
  </header>
</template>

<script>
export default {
  name: 'Header'
}
</script>

<style scoped>
.header {
  background: rgba(255, 255, 255, 0.95);
  backdrop-filter: blur(10px);
  border-bottom: 1px solid rgba(255, 255, 255, 0.2);
  padding: 20px 0;
  box-shadow: 0 2px 20px rgba(0, 0, 0, 0.1);
}

.header-content {
  max-width: 1200px;
  margin: 0 auto;
  padding: 0 20px;
  text-align: center;
}

.title {
  font-size: 2.5rem;
  font-weight: bold;
  background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
  background-clip: text;
  margin-bottom: 8px;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 12px;
}

.icon {
  font-size: 2rem;
}

.subtitle {
  color: #666;
  font-size: 1.1rem;
  font-weight: 300;
}

@media (max-width: 768px) {
  .title {
    font-size: 2rem;
    flex-direction: column;
    gap: 8px;
  }
  
  .subtitle {
    font-size: 1rem;
  }
}
</style>