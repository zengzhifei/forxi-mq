<script setup>
import { ref, onMounted, onUnmounted } from 'vue'
import { Monitor, Message, Timer, Warning } from '@element-plus/icons-vue'
import TopicDetail from './components/TopicDetail.vue'

const overview = ref({ topics: 0, total_msgs: 0, total_dead: 0, total_delay: 0 })
const topics = ref([])
const selectedTopic = ref(null)
let timer = null

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
  fetchData()
  timer = setInterval(fetchData, 3000)
})

onUnmounted(() => {
  clearInterval(timer)
})
</script>

<template>
  <div class="layout dark">
    <el-container>
      <el-header class="header">
        <div class="logo">
          <el-icon :size="24"><Monitor /></el-icon>
          <span>forxi-mq</span>
        </div>
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
                  <div class="stat-value">{{ overview.total_msgs.toLocaleString() }}</div>
                  <div class="stat-label">Total Messages</div>
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
        </el-row>

        <!-- Topics Table -->
        <el-card shadow="never" class="table-card">
          <template #header>
            <span class="card-title">Topics</span>
          </template>
          <el-table :data="topics" stripe style="width: 100%" @row-click="selectTopic">
            <el-table-column prop="name" label="Topic" />
            <el-table-column prop="stored" label="Stored">
              <template #default="{ row }">
                {{ (row.stored || 0).toLocaleString() }}
              </template>
            </el-table-column>
            <el-table-column prop="lag" label="Lag">
              <template #default="{ row }">
                <el-tag v-if="row.lag > 0" type="danger" size="small">{{ row.lag }}</el-tag>
                <el-tag v-else type="success" size="small">0</el-tag>
              </template>
            </el-table-column>
            <el-table-column prop="pending" label="Pending">
              <template #default="{ row }">
                <el-tag v-if="row.pending > 0" type="warning" size="small">{{ row.pending }}</el-tag>
                <el-tag v-else type="success" size="small">0</el-tag>
              </template>
            </el-table-column>
            <el-table-column prop="dead" label="Dead">
              <template #default="{ row }">
                <el-tag v-if="row.dead > 0" type="danger" size="small">{{ row.dead }}</el-tag>
                <el-tag v-else type="info" size="small">0</el-tag>
              </template>
            </el-table-column>
            <el-table-column prop="delay" label="Delayed">
              <template #default="{ row }">
                <el-tag v-if="row.delay > 0" type="warning" size="small">{{ row.delay }}</el-tag>
                <span v-else>0</span>
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
  background: #1d1e1f;
  color: #e5eaf3;
}

.header {
  display: flex;
  align-items: center;
  border-bottom: 1px solid #363637;
  background: #141414;
}

.logo {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 18px;
  font-weight: 600;
  color: #e5eaf3;
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
  color: #e5eaf3;
}

.stat-label {
  font-size: 13px;
  color: #a3a6ad;
}

.table-card {
  margin-bottom: 24px;
}

.card-title {
  font-weight: 600;
  font-size: 16px;
}

:deep(.el-card) {
  background: #1d1e1f;
  border-color: #363637;
}

:deep(.el-table) {
  --el-table-bg-color: #1d1e1f;
  --el-table-tr-bg-color: #1d1e1f;
  --el-table-header-bg-color: #141414;
  --el-table-row-hover-bg-color: #262727;
  --el-table-border-color: #363637;
  --el-table-text-color: #e5eaf3;
  --el-table-header-text-color: #a3a6ad;
  cursor: pointer;
}
</style>
