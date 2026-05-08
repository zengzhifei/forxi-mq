<script setup>
import { ref, watch, onUnmounted } from 'vue'
import { Refresh } from '@element-plus/icons-vue'

const props = defineProps({ topic: String })
const emit = defineEmits(['close'])

const visible = ref(true)
const detail = ref(null)
const deadMessages = ref([])
const delayMessages = ref([])
const activeTab = ref('consumers')
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

function handleClose() {
  visible.value = false
  emit('close')
}

watch(() => props.topic, () => {
  fetchDetail()
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
    size="50%"
    @close="handleClose"
  >
    <el-tabs v-model="activeTab">
      <!-- Consumers Tab -->
      <el-tab-pane label="Consumers" name="consumers">
        <el-table :data="detail?.consumers || []" stripe style="width: 100%">
          <el-table-column prop="name" label="Consumer" />
          <el-table-column prop="pending" label="Pending">
            <template #default="{ row }">
              <el-tag v-if="row.pending > 0" type="warning" size="small">{{ row.pending }}</el-tag>
              <el-tag v-else type="success" size="small">0</el-tag>
            </template>
          </el-table-column>
          <el-table-column prop="idle" label="Idle" />
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
            @click="requeue"
          >
            Requeue All
          </el-button>
          <el-tag type="info" style="margin-left: 8px;">{{ deadMessages.length }} messages</el-tag>
        </div>
        <el-table :data="deadMessages" stripe style="width: 100%" max-height="400">
          <el-table-column prop="id" label="ID" width="180" />
          <el-table-column prop="reason" label="Reason" />
        </el-table>
      </el-tab-pane>

      <!-- Delay Queue Tab -->
      <el-tab-pane label="Delay Queue" name="delay">
        <el-table :data="delayMessages" stripe style="width: 100%" max-height="400">
          <el-table-column prop="due_at" label="Due At" width="200" />
          <el-table-column prop="body" label="Body" show-overflow-tooltip />
        </el-table>
      </el-tab-pane>
    </el-tabs>
  </el-drawer>
</template>
