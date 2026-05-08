<script setup>
import { ref, watch, onUnmounted } from 'vue'
import { Refresh } from '@element-plus/icons-vue'
import { ElMessage } from 'element-plus'

const props = defineProps({ topic: String })
const emit = defineEmits(['close'])

const visible = ref(true)
const detail = ref(null)
const messages = ref([])
const pendingMessages = ref([])
const deadMessages = ref([])
const delayMessages = ref([])
const activeTab = ref('groups')
const requeueLoading = ref(false)
let timer = null

async function fetchDetail() {
  try {
    const res = await fetch(`/api/topics/${props.topic}`)
    detail.value = await res.json()
  } catch (e) {
    console.error(e)
  }
}

async function fetchMessages() {
  try {
    const res = await fetch(`/api/topics/${props.topic}/messages?count=50`)
    messages.value = (await res.json()) || []
  } catch (e) {
    console.error(e)
  }
}

async function fetchPending() {
  try {
    const res = await fetch(`/api/topics/${props.topic}/pending?count=50`)
    pendingMessages.value = (await res.json()) || []
  } catch (e) {
    console.error(e)
  }
}

async function fetchDead() {
  try {
    const res = await fetch(`/api/topics/${props.topic}/dead`)
    deadMessages.value = (await res.json()) || []
  } catch (e) {
    console.error(e)
  }
}

async function fetchDelay() {
  try {
    const res = await fetch(`/api/topics/${props.topic}/delay`)
    delayMessages.value = (await res.json()) || []
  } catch (e) {
    console.error(e)
  }
}

async function requeue() {
  requeueLoading.value = true
  try {
    const res = await fetch(`/api/topics/${props.topic}/dead/requeue`, { method: 'POST' })
    const data = await res.json()
    ElMessage.success(`Requeued ${data.requeued} messages`)
    await fetchDead()
    await fetchDetail()
  } catch (e) {
    ElMessage.error('Requeue failed')
  } finally {
    requeueLoading.value = false
  }
}

function formatValues(values) {
  if (!values) return ''
  try {
    if (values.body) {
      const parsed = JSON.parse(values.body)
      return JSON.stringify(parsed, null, 2)
    }
    return JSON.stringify(values, null, 2)
  } catch {
    return JSON.stringify(values, null, 2)
  }
}

function handleClose() {
  visible.value = false
  emit('close')
}

watch(() => props.topic, () => {
  fetchDetail()
  fetchMessages()
  fetchPending()
  fetchDead()
  fetchDelay()
}, { immediate: true })

timer = setInterval(() => {
  fetchDetail()
}, 3000)

onUnmounted(() => clearInterval(timer))
</script>

<template>
  <el-drawer
    v-model="visible"
    :title="topic"
    size="60%"
    @close="handleClose"
  >
    <!-- Summary -->
    <el-row :gutter="12" style="margin-bottom: 16px;">
      <el-col :span="6">
        <el-statistic title="Stored" :value="detail?.stored || 0" />
      </el-col>
      <el-col :span="6">
        <el-statistic title="Lag" :value="detail?.lag || 0" />
      </el-col>
      <el-col :span="6">
        <el-statistic title="Pending" :value="detail?.pending || 0" />
      </el-col>
      <el-col :span="6">
        <el-statistic title="Dead" :value="detail?.dead || 0" />
      </el-col>
    </el-row>

    <el-tabs v-model="activeTab">
      <!-- Groups Tab -->
      <el-tab-pane label="Groups" name="groups">
        <div v-for="group in (detail?.groups || [])" :key="group.name" style="margin-bottom: 20px;">
          <div style="margin-bottom: 8px; font-weight: 600;">
            {{ group.name }}
            <el-tag type="info" size="small" style="margin-left: 8px;">lag: {{ group.lag }}</el-tag>
            <el-tag type="warning" size="small" style="margin-left: 4px;">pending: {{ group.pending }}</el-tag>
          </div>
          <el-table :data="group.consumers || []" stripe style="width: 100%" size="small">
            <el-table-column prop="name" label="Consumer" />
            <el-table-column prop="pending" label="Pending">
              <template #default="{ row }">
                <el-tag v-if="row.pending > 0" type="warning" size="small">{{ row.pending }}</el-tag>
                <el-tag v-else type="success" size="small">0</el-tag>
              </template>
            </el-table-column>
            <el-table-column prop="idle" label="Idle" />
          </el-table>
        </div>
        <el-empty v-if="!detail?.groups?.length" description="No consumer groups" />
      </el-tab-pane>

      <!-- Messages Tab -->
      <el-tab-pane label="Messages" name="messages">
        <el-button size="small" style="margin-bottom: 12px;" @click="fetchMessages">Refresh</el-button>
        <el-tag type="info" size="small" style="margin-left: 8px;">{{ messages.length }} messages</el-tag>
        <el-table :data="messages" stripe style="width: 100%" max-height="500">
          <el-table-column prop="id" label="ID" width="180" />
          <el-table-column label="Body" show-overflow-tooltip>
            <template #default="{ row }">
              <pre class="msg-body">{{ formatValues(row.values) }}</pre>
            </template>
          </el-table-column>
        </el-table>
      </el-tab-pane>

      <!-- Pending Tab -->
      <el-tab-pane label="Pending" name="pending">
        <el-button size="small" style="margin-bottom: 12px;" @click="fetchPending">Refresh</el-button>
        <el-tag type="warning" size="small" style="margin-left: 8px;">{{ pendingMessages.length }} messages</el-tag>
        <el-table :data="pendingMessages" stripe style="width: 100%" max-height="500">
          <el-table-column prop="id" label="ID" width="180" />
          <el-table-column prop="group" label="Group" width="120" />
          <el-table-column prop="consumer" label="Consumer" width="120" />
          <el-table-column prop="idle" label="Idle" width="100" />
          <el-table-column prop="retry_count" label="Retry" width="70" />
          <el-table-column label="Body" show-overflow-tooltip>
            <template #default="{ row }">
              <pre class="msg-body">{{ formatValues(row.values) }}</pre>
            </template>
          </el-table-column>
        </el-table>
      </el-tab-pane>

      <!-- Dead Letters Tab -->
      <el-tab-pane label="Dead Letters" name="dead">
        <div style="margin-bottom: 12px;">
          <el-button
            type="primary"
            :icon="Refresh"
            :loading="requeueLoading"
            :disabled="deadMessages.length === 0"
            size="small"
            @click="requeue"
          >
            Requeue All
          </el-button>
          <el-tag type="danger" size="small" style="margin-left: 8px;">{{ deadMessages.length }} messages</el-tag>
        </div>
        <el-table :data="deadMessages" stripe style="width: 100%" max-height="500">
          <el-table-column prop="id" label="ID" width="180" />
          <el-table-column prop="reason" label="Reason" width="200" show-overflow-tooltip />
          <el-table-column prop="body" label="Body" show-overflow-tooltip>
            <template #default="{ row }">
              <pre class="msg-body">{{ row.body }}</pre>
            </template>
          </el-table-column>
        </el-table>
      </el-tab-pane>

      <!-- Delay Queue Tab -->
      <el-tab-pane label="Delay Queue" name="delay">
        <el-button size="small" style="margin-bottom: 12px;" @click="fetchDelay">Refresh</el-button>
        <el-tag type="info" size="small" style="margin-left: 8px;">{{ delayMessages.length }} messages</el-tag>
        <el-table :data="delayMessages" stripe style="width: 100%" max-height="500">
          <el-table-column prop="due_at" label="Due At" width="200" />
          <el-table-column prop="body" label="Body" show-overflow-tooltip>
            <template #default="{ row }">
              <pre class="msg-body">{{ row.body }}</pre>
            </template>
          </el-table-column>
        </el-table>
      </el-tab-pane>
    </el-tabs>
  </el-drawer>
</template>

<style scoped>
.msg-body {
  margin: 0;
  font-size: 12px;
  white-space: pre-wrap;
  word-break: break-all;
  max-height: 80px;
  overflow: auto;
}
</style>
