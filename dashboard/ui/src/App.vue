<script setup>
import { ref, onMounted, onUnmounted } from 'vue'
import { Monitor, Message, Timer, Warning, Sunny, Moon } from '@element-plus/icons-vue'
import TopicDetail from './components/TopicDetail.vue'

const overview = ref({ topics: 0, total_msgs: 0, total_dead: 0, total_delay: 0 })
const topics = ref([])
const selectedTopic = ref(null)
const isDark = ref(true)
let timer = null

function toggleTheme() {
  isDark.value = !isDark.value
  document.documentElement.classList.toggle('dark', isDark.value)
  localStorage.setItem('fxmq-theme', isDark.value ? 'dark' : 'light')
}

function initTheme() {
  const saved = localStorage.getItem('fxmq-theme')
  if (saved === 'light') {
    isDark.value = false
    document.documentElement.classList.remove('dark')
  } else {
    isDark.value = true
    document.documentElement.classList.add('dark')
  }
}

async function fetchData() {
  try {
    const [ov, tp] = await Promise.all([
      fetch('/api/overview').then(r => r.json()),
      fetch('/api/topics').then(r => r.json())
    ])
    overview.value = ov
    topics.value = tp || []
  } catch (e) {
    console.error('fetch error:', e)
  }
}

function selectTopic(topic) {
  selectedTopic.value = topic
}

function closeTopic() {
  selectedTopic.value = null
}

onMounted(() => {
  initTheme()
  fetchData()
  timer = setInterval(fetchData, 5000)
})

onUnmounted(() => {
  clearInterval(timer)
})
</script>

<template>
  <div class="layout" :class="{ dark: isDark }">
    <el-container>
      <el-header class="header">
        <div class="logo">
          <el-icon :size="24"><Monitor /></el-icon>
          <span>forxi-mq</span>
        </div>
        <el-button class="theme-btn" :icon="isDark ? Sunny : Moon" circle size="small" @click="toggleTheme" :title="isDark ? '切换亮色' : '切换暗色'" />
      </el-header>

      <el-main class="main">
        <!-- Overview Cards -->
        <el-row :gutter="16" class="overview-cards">
          <el-col :span="6">
            <el-card shadow="never">
              <div class="stat-card">
                <el-icon :size="28" color="#409eff"><Message /></el-icon>
                <div class="stat-info">
                  <div class="stat-value">{{ overview.topics }}</div>
                  <div class="stat-label">Topics</div>
                </div>
              </div>
            </el-card>
          </el-col>
          <el-col :span="6">
            <el-card shadow="never">
              <div class="stat-card">
                <el-icon :size="28" color="#67c23a"><Message /></el-icon>
                <div class="stat-info">
                  <div class="stat-value">{{ (overview.total_msgs || 0).toLocaleString() }}</div>
                  <div class="stat-label">Stored</div>
                </div>
              </div>
            </el-card>
          </el-col>
          <el-col :span="6">
            <el-card shadow="never">
              <div class="stat-card">
                <el-icon :size="28" color="#f56c6c"><Warning /></el-icon>
                <div class="stat-info">
                  <div class="stat-value">{{ overview.total_dead }}</div>
                  <div class="stat-label">Dead Letters</div>
                </div>
              </div>
            </el-card>
          </el-col>
          <el-col :span="6">
            <el-card shadow="never">
              <div class="stat-card">
                <el-icon :size="28" color="#e6a23c"><Timer /></el-icon>
                <div class="stat-info">
                  <div class="stat-value">{{ overview.total_delay }}</div>
                  <div class="stat-label">Delayed</div>
                </div>
              </div>
            </el-card>
          </el-col>
        </el-row>

        <!-- Topics Table -->
        <el-card shadow="never" class="table-card">
          <template #header>
            <span class="card-title">Topics</span>
          </template>
          <el-table :data="topics" stripe style="width: 100%" @row-click="selectTopic">
            <el-table-column prop="name" label="Topic" min-width="180" />
            <el-table-column label="Stored" min-width="100" align="center">
              <template #default="{ row }">{{ (row.stored || 0).toLocaleString() }}</template>
            </el-table-column>
            <el-table-column label="Lag" min-width="80" align="center">
              <template #default="{ row }">
                <span :class="{ 'num-danger': row.lag > 0 }">{{ row.lag || 0 }}</span>
              </template>
            </el-table-column>
            <el-table-column label="Pending" min-width="80" align="center">
              <template #default="{ row }">
                <span :class="{ 'num-warn': row.pending > 0 }">{{ row.pending || 0 }}</span>
              </template>
            </el-table-column>
            <el-table-column label="Dead" min-width="80" align="center">
              <template #default="{ row }">
                <span :class="{ 'num-danger': row.dead > 0 }">{{ row.dead || 0 }}</span>
              </template>
            </el-table-column>
            <el-table-column label="Delay" min-width="80" align="center">
              <template #default="{ row }">
                <span :class="{ 'num-warn': row.delay > 0 }">{{ row.delay || 0 }}</span>
              </template>
            </el-table-column>
          </el-table>
        </el-card>

        <!-- Topic Detail Drawer -->
        <TopicDetail
          v-if="selectedTopic"
          :topic="selectedTopic.name"
          @close="closeTopic"
        />
      </el-main>
    </el-container>
  </div>
</template>

<style scoped>
.layout {
  min-height: 100vh;
  background: var(--fxmq-bg);
  color: var(--fxmq-text);
  transition: background 0.3s, color 0.3s;
}

.layout.dark {
  --fxmq-bg: #1d1e1f;
  --fxmq-text: #e5eaf3;
  --fxmq-header-bg: #141414;
  --fxmq-border: #363637;
  --fxmq-card-bg: #1d1e1f;
  --fxmq-table-bg: #1d1e1f;
  --fxmq-table-header: #141414;
  --fxmq-table-hover: #262727;
  --fxmq-muted: #a3a6ad;
}

.layout:not(.dark) {
  --fxmq-bg: #f5f7fa;
  --fxmq-text: #303133;
  --fxmq-header-bg: #fff;
  --fxmq-border: #e4e7ed;
  --fxmq-card-bg: #fff;
  --fxmq-table-bg: #fff;
  --fxmq-table-header: #fafafa;
  --fxmq-table-hover: #f5f7fa;
  --fxmq-muted: #909399;
}

.header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  border-bottom: 1px solid var(--fxmq-border);
  background: var(--fxmq-header-bg);
}

.logo {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 18px;
  font-weight: 600;
  color: var(--fxmq-text);
}

.theme-btn {
  margin-left: auto;
}

.main {
  padding: 24px;
  max-width: 1200px;
  margin: 0 auto;
  width: 100%;
  box-sizing: border-box;
}

.overview-cards {
  margin-bottom: 24px;
}

.stat-card {
  display: flex;
  align-items: center;
  gap: 16px;
}

.stat-info {
  display: flex;
  flex-direction: column;
}

.stat-value {
  font-size: 24px;
  font-weight: 700;
  color: var(--fxmq-text);
}

.stat-label {
  font-size: 13px;
  color: var(--fxmq-muted);
}

.table-card {
  margin-bottom: 24px;
}

.card-title {
  font-weight: 600;
  font-size: 16px;
}

:deep(.el-card) {
  background: var(--fxmq-card-bg);
  border-color: var(--fxmq-border);
}

:deep(.el-table) {
  --el-table-bg-color: var(--fxmq-table-bg);
  --el-table-tr-bg-color: var(--fxmq-table-bg);
  --el-table-header-bg-color: var(--fxmq-table-header);
  --el-table-row-hover-bg-color: var(--fxmq-table-hover);
  --el-table-border-color: var(--fxmq-border);
  --el-table-text-color: var(--fxmq-text);
  --el-table-header-text-color: var(--fxmq-muted);
  cursor: pointer;
}

.num-danger {
  color: #f56c6c;
  font-weight: 600;
}

.num-warn {
  color: #e6a23c;
  font-weight: 600;
}
</style>
