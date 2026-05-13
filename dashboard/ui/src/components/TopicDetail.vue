<script setup>
import { ref, computed, watch, onUnmounted } from 'vue'
import { Refresh, Search, CopyDocument, RefreshRight, ArrowLeft, ArrowRight, Delete, Download, Plus, Position } from '@element-plus/icons-vue'
import { ElMessage, ElMessageBox } from 'element-plus'

const props = defineProps({ topic: String })
const emit = defineEmits(['close'])

const visible = ref(true)
const detail = ref(null)
const selfGroup = ref('')
const messages = ref([])
const lagMessages = ref([])
const pendingMessages = ref([])
const deadMessages = ref([])
const delayMessages = ref([])
const activeTab = ref('overview')
const requeueLoading = ref(false)
const deleteTopicLoading = ref(false)
const searchId = ref('')
const searchResult = ref(null)
const searchLoading = ref(false)
let timer = null

// Publish dialog
const publishVisible = ref(false)
const publishBody = ref('')
const publishDueAt = ref(null)
const publishLoading = ref(false)

// Pagination state
const PAGE_SIZE = 20
const cursors = ref({
  messages: [],
  lag: [],
  pending: [],
  dead: [],
  delay: [],
})
const nextCursors = ref({
  messages: '',
  lag: '',
  pending: '',
  dead: '',
  delay: '',
})
const delayOffset = ref(0)
const delayHasMore = ref(false)

const sortedGroups = computed(() => {
  if (!detail.value?.groups) return []
  return [...detail.value.groups].sort((a, b) => {
    if (a.name === selfGroup.value) return -1
    if (b.name === selfGroup.value) return 1
    return 0
  })
})

// --- Fetch ---

async function fetchConfig() {
  try {
    const res = await fetch('/api/config')
    const data = await res.json()
    selfGroup.value = data.group || ''
  } catch (e) { console.error(e) }
}

async function fetchDetail() {
  try {
    const res = await fetch(`/api/topics/${props.topic}`)
    detail.value = await res.json()
  } catch (e) { console.error(e) }
}

async function fetchMessages(cursor = '') {
  try {
    let url = `/api/topics/${props.topic}/messages?count=${PAGE_SIZE}`
    if (cursor) url += `&cursor=${cursor}`
    const res = await fetch(url)
    const data = await res.json()
    messages.value = data.items || []
    nextCursors.value.messages = data.next_cursor || ''
  } catch (e) { console.error(e) }
}

async function fetchLag(cursor = '') {
  try {
    let url = `/api/topics/${props.topic}/lag?count=${PAGE_SIZE}`
    if (cursor) url += `&cursor=${cursor}`
    const res = await fetch(url)
    const data = await res.json()
    lagMessages.value = data.items || []
    nextCursors.value.lag = data.next_cursor || ''
  } catch (e) { console.error(e) }
}

async function fetchPending(cursor = '') {
  try {
    let url = `/api/topics/${props.topic}/pending?count=${PAGE_SIZE}`
    if (cursor) url += `&cursor=${cursor}`
    const res = await fetch(url)
    const data = await res.json()
    pendingMessages.value = data.items || []
    nextCursors.value.pending = data.next_cursor || ''
  } catch (e) { console.error(e) }
}

async function fetchDead(cursor = '') {
  try {
    let url = `/api/topics/${props.topic}/dead?count=${PAGE_SIZE}`
    if (cursor) url += `&cursor=${cursor}`
    const res = await fetch(url)
    const data = await res.json()
    deadMessages.value = data.items || []
    nextCursors.value.dead = data.next_cursor || ''
  } catch (e) { console.error(e) }
}

async function fetchDelay(offset = 0) {
  try {
    const res = await fetch(`/api/topics/${props.topic}/delay?count=${PAGE_SIZE}&offset=${offset}`)
    const data = await res.json()
    delayMessages.value = data.items || []
    delayOffset.value = offset
    delayHasMore.value = data.has_more || false
  } catch (e) { console.error(e) }
}

function fetchAll() {
  cursors.value = { messages: [], lag: [], pending: [], dead: [], delay: [] }
  nextCursors.value = { messages: '', lag: '', pending: '', dead: '', delay: '' }
  delayOffset.value = 0
  fetchDetail()
  fetchMessages()
  fetchLag()
  fetchPending()
  fetchDead()
  fetchDelay()
}

// --- Pagination ---

function nextPage(tab) {
  const cursor = nextCursors.value[tab]
  if (!cursor) return
  if (tab === 'delay') {
    const newOffset = delayOffset.value + PAGE_SIZE
    cursors.value.delay.push(delayOffset.value)
    fetchDelay(newOffset)
  } else {
    // Push the cursor used to navigate to the next page (for going back later)
    cursors.value[tab].push(cursor)
    const fetcher = { messages: fetchMessages, lag: fetchLag, pending: fetchPending, dead: fetchDead }[tab]
    fetcher(cursor)
  }
}

function prevPage(tab) {
  if (cursors.value[tab].length === 0) return
  if (tab === 'delay') {
    const prevOffset = cursors.value.delay.pop()
    fetchDelay(prevOffset)
  } else {
    // Pop the cursor that brought us to the current page
    cursors.value[tab].pop()
    // Use the last remaining cursor (or '' for page 1)
    const prevCursor = cursors.value[tab].length > 0
      ? cursors.value[tab][cursors.value[tab].length - 1]
      : ''
    const fetcher = { messages: fetchMessages, lag: fetchLag, pending: fetchPending, dead: fetchDead }[tab]
    fetcher(prevCursor)
  }
}

function hasPrev(tab) {
  return cursors.value[tab].length > 0
}

function hasNext(tab) {
  if (tab === 'delay') return delayHasMore.value
  return !!nextCursors.value[tab]
}

function pageNum(tab) {
  return cursors.value[tab].length + 1
}

// --- Search ---

async function searchById() {
  const id = searchId.value.trim()
  if (!id) {
    searchResult.value = null
    return
  }
  searchLoading.value = true
  try {
    const res = await fetch(`/api/topics/${props.topic}/search?id=${encodeURIComponent(id)}`)
    const data = await res.json()
    searchResult.value = data
  } catch (e) {
    ElMessage.error('Search failed')
    searchResult.value = null
  } finally {
    searchLoading.value = false
  }
}

function clearSearch() {
  searchId.value = ''
  searchResult.value = null
}

// --- Publish ---

async function publishMessage() {
  if (!publishBody.value.trim()) {
    ElMessage.warning('消息体不能为空')
    return
  }
  // Validate payload is valid JSON
  let payload
  try {
    payload = JSON.parse(publishBody.value)
  } catch {
    ElMessage.warning('Payload 必须是合法 JSON')
    return
  }
  publishLoading.value = true
  try {
    const reqBody = { payload }
    if (publishDueAt.value) {
      const dueMs = new Date(publishDueAt.value).getTime()
      if (dueMs <= Date.now()) {
        ElMessage.warning('投递时间必须在当前时间之后')
        publishLoading.value = false
        return
      }
      reqBody.due_at = dueMs
    }
    const res = await fetch(`/api/topics/${props.topic}/publish`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(reqBody)
    })
    const data = await res.json()
    if (data.ok) {
      const typeLabel = data.type === 'delay' ? '延迟消息' : '普通消息'
      publishVisible.value = false
      publishBody.value = ''
      publishDueAt.value = null
      fetchAll()
      const msgId = data.id
      ElMessageBox.alert(
        `<div id="pub-id-box" style="word-break:break-all;font-family:monospace;font-size:13px;padding:8px;background:var(--el-bg-color-page);border-radius:4px;cursor:pointer;">${msgId}</div><div style="margin-top:8px;font-size:12px;color:var(--el-text-color-secondary);">点击 ID 可复制</div>`,
        `${typeLabel}发布成功`,
        {
          dangerouslyUseHTMLString: true,
          confirmButtonText: '确定',
          callback: () => {}
        }
      ).then(() => {})
      // Attach click handler after DOM update
      setTimeout(() => {
        const el = document.getElementById('pub-id-box')
        if (el) {
          el.addEventListener('click', () => {
            navigator.clipboard.writeText(msgId).then(() => {
              ElMessage.success('已复制 ID')
            })
          })
        }
      }, 100)
    } else {
      ElMessage.error(data.error || '发布失败')
    }
  } catch (e) {
    ElMessage.error('发布失败')
  } finally {
    publishLoading.value = false
  }
}

// --- Delete ---

async function deleteDead(row) {
  try {
    await ElMessageBox.confirm('确认删除该死信消息？', '删除确认', { type: 'warning' })
  } catch { return }
  try {
    const res = await fetch(`/api/topics/${props.topic}/dead/${row.id}`, { method: 'DELETE' })
    const data = await res.json()
    if (data.ok) {
      ElMessage.success('已删除')
      fetchDead()
      fetchDetail()
    } else {
      ElMessage.error(data.error || '删除失败')
    }
  } catch (e) {
    ElMessage.error('删除失败')
  }
}

async function deleteDelay(row) {
  try {
    await ElMessageBox.confirm('确认删除该延迟消息？', '删除确认', { type: 'warning' })
  } catch { return }
  try {
    const res = await fetch(`/api/topics/${props.topic}/delay/delete`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ id: row.id })
    })
    const data = await res.json()
    if (data.ok) {
      ElMessage.success('已删除')
      fetchDelay(delayOffset.value)
      fetchDetail()
    } else {
      ElMessage.error(data.error || '删除失败')
    }
  } catch (e) {
    ElMessage.error('删除失败')
  }
}

// --- Destroy Group ---

async function destroyGroup(group) {
  try {
    await ElMessageBox.confirm(
      `确认销毁消费组 "${group.name}"？\n销毁后该组的消费位点、Pending 消息等数据将永久丢失。`,
      '销毁确认',
      { type: 'warning', confirmButtonText: '确认销毁', confirmButtonClass: 'el-button--danger' }
    )
  } catch { return }
  try {
    const res = await fetch(`/api/topics/${props.topic}/groups/${group.name}`, { method: 'DELETE' })
    const data = await res.json()
    if (data.ok) {
      ElMessage.success(`消费组 "${group.name}" 已销毁`)
      fetchDetail()
    } else {
      ElMessage.error(data.error || '销毁失败')
    }
  } catch (e) {
    ElMessage.error('销毁失败')
  }
}

// --- Reset Group ---

async function resetGroup(group, mode) {
  const isLatest = mode === 'latest'
  const desc = isLatest
    ? `重置消费组 "${group.name}" 的消费位点到最新？\n将跳过所有历史消息，只消费之后产生的新消息。`
    : `重置消费组 "${group.name}" 的消费位点到起始点？\n将从头开始重新消费所有消息。`
  try {
    await ElMessageBox.confirm(desc, '重置确认', { type: 'warning' })
  } catch { return }
  try {
    const res = await fetch(`/api/topics/${props.topic}/groups/${group.name}/reset`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ id: isLatest ? '$' : '0' })
    })
    const data = await res.json()
    if (data.ok) {
      ElMessage.success('重置成功')
      fetchDetail()
    } else {
      ElMessage.error(data.error || '重置失败')
    }
  } catch (e) {
    ElMessage.error('重置失败')
  }
}

// --- Export ---

function exportMessages(tab) {
  const dataMap = {
    messages: messages.value,
    lag: lagMessages.value,
    pending: pendingMessages.value,
    dead: deadMessages.value,
    delay: delayMessages.value,
  }
  const data = dataMap[tab] || []
  if (data.length === 0) {
    ElMessage.warning('当前页无数据')
    return
  }
  const json = JSON.stringify(data, null, 2)
  const blob = new Blob([json], { type: 'application/json' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = `${props.topic}_${tab}_page${pageNum(tab)}.json`
  a.click()
  URL.revokeObjectURL(url)
  ElMessage.success('导出成功')
}

// --- Actions ---

async function deleteTopic() {
  try {
    await ElMessageBox.confirm(
      `确认删除 Topic "${props.topic}"？\n仅当 Stream、死信、延迟队列均为空时可删除。`,
      '删除 Topic',
      { type: 'warning' }
    )
  } catch { return }
  deleteTopicLoading.value = true
  try {
    const res = await fetch(`/api/topics/${props.topic}`, { method: 'DELETE' })
    const data = await res.json()
    if (data.ok) {
      ElMessage.success('Topic 已删除')
      handleClose()
    } else {
      ElMessage.error(data.error || '删除失败')
    }
  } catch (e) {
    ElMessage.error('删除失败')
  } finally {
    deleteTopicLoading.value = false
  }
}

async function requeue() {
  requeueLoading.value = true
  try {
    let total = 0
    // Loop until all dead messages are requeued (API batches 200 per request)
    while (true) {
      const res = await fetch(`/api/topics/${props.topic}/dead/requeue`, { method: 'POST' })
      const data = await res.json()
      total += data.requeued || 0
      if (!data.requeued || data.requeued === 0) break
    }
    ElMessage.success(`Requeued ${total} messages`)
    fetchAll()
  } catch (e) {
    ElMessage.error('Requeue failed')
  } finally {
    requeueLoading.value = false
  }
}

function copyText(text) {
  navigator.clipboard.writeText(text).then(() => {
    ElMessage.success('Copied')
  }).catch(() => {
    ElMessage.error('Copy failed')
  })
}

async function resendMsg(row) {
  const body = row.values?.body || row.body || JSON.stringify(row.values || '')
  try {
    const res = await fetch(`/api/topics/${props.topic}/resend`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ body })
    })
    const data = await res.json()
    if (data.ok) {
      ElMessage.success('Resent')
      fetchAll()
    } else {
      ElMessage.error(data.error || 'Resend failed')
    }
  } catch (e) {
    ElMessage.error('Resend failed')
  }
}

function formatBody(row) {
  try {
    if (row.values?.body) {
      const parsed = JSON.parse(row.values.body)
      return JSON.stringify(parsed, null, 2)
    }
    if (row.values) return JSON.stringify(row.values, null, 2)
    if (row.body) {
      try { return JSON.stringify(JSON.parse(row.body), null, 2) }
      catch { return row.body }
    }
    return ''
  } catch {
    return row.values?.body || JSON.stringify(row.values || row.body || '', null, 2)
  }
}

function formatSearchBody(msg) {
  if (msg.values?.body) {
    try { return JSON.stringify(JSON.parse(msg.values.body), null, 2) }
    catch { return msg.values.body }
  }
  if (msg.values) return JSON.stringify(msg.values, null, 2)
  if (msg.body) {
    try { return JSON.stringify(JSON.parse(msg.body), null, 2) }
    catch { return msg.body }
  }
  return ''
}

function handleClose() {
  visible.value = false
  emit('close')
}

function statusTagType(status) {
  const map = {
    consumed: 'success',
    pending: 'warning',
    retrying: 'danger',
    lag: 'danger',
    dead: 'danger',
    waiting: 'warning',
    trimmed: 'info',
    unknown: 'info',
  }
  return map[status] || 'info'
}

function statusLabel(status) {
  const map = {
    consumed: '已消费',
    pending: '处理中',
    retrying: '重试中',
    lag: '积压',
    dead: '死信',
    waiting: '等待投递',
    trimmed: '已裁剪',
    unknown: '未知',
  }
  return map[status] || status
}

fetchConfig()
watch(() => props.topic, () => { fetchAll() }, { immediate: true })
timer = setInterval(fetchDetail, 3000)
onUnmounted(() => clearInterval(timer))
</script>

<template>
  <el-drawer v-model="visible" :title="`Topic: ${topic}`" size="65%" @close="handleClose">
    <!-- Stats -->
    <el-row :gutter="12" class="stats-row">
      <el-col :span="5">
        <div class="stat-box">
          <div class="stat-num">{{ (detail?.stored || 0).toLocaleString() }}</div>
          <div class="stat-desc">Stored (已存储)</div>
        </div>
      </el-col>
      <el-col :span="5">
        <div class="stat-box danger">
          <div class="stat-num">{{ detail?.lag || 0 }}</div>
          <div class="stat-desc">Lag (积压)</div>
        </div>
      </el-col>
      <el-col :span="5">
        <div class="stat-box warn">
          <div class="stat-num">{{ detail?.pending || 0 }}</div>
          <div class="stat-desc">Pending (处理中)</div>
        </div>
      </el-col>
      <el-col :span="5">
        <div class="stat-box danger">
          <div class="stat-num">{{ detail?.dead || 0 }}</div>
          <div class="stat-desc">Dead (死信)</div>
        </div>
      </el-col>
      <el-col :span="4">
        <div class="stat-box warn">
          <div class="stat-num">{{ detail?.delay || 0 }}</div>
          <div class="stat-desc">Delay (延迟)</div>
        </div>
      </el-col>
    </el-row>

    <!-- Toolbar -->
    <div class="search-bar">
      <el-input
        v-model="searchId"
        :prefix-icon="Search"
        placeholder="精确搜索 Message ID / Delay ID..."
        size="small"
        clearable
        style="width: 280px;"
        @keyup.enter="searchById"
        @clear="clearSearch"
      />
      <el-button size="small" :loading="searchLoading" @click="searchById" title="从Redis精确查找">搜索</el-button>
      <el-button :icon="Refresh" size="small" @click="fetchAll" title="刷新数据">Refresh</el-button>
      <el-button :icon="Plus" size="small" type="primary" @click="publishVisible = true" title="发布消息到该Topic">发布</el-button>
      <el-button :icon="Delete" size="small" type="danger" :loading="deleteTopicLoading" :disabled="(detail?.stored || 0) > 0 || (detail?.dead || 0) > 0 || (detail?.delay || 0) > 0" @click="deleteTopic" title="删除空 Topic（需Stream/死信/延迟均为空）">删除 Topic</el-button>
    </div>

    <!-- Search Result -->
    <div v-if="searchResult" class="search-result">
      <div v-if="searchResult.found" class="search-found">
        <div class="search-found-header">
          <el-tag size="small" :type="statusTagType(searchResult.status)">
            {{ statusLabel(searchResult.status) }}
          </el-tag>
          <span class="id-copy" @click="copyText(searchResult.message.id)">{{ searchResult.message.id }}</span>
          <span v-if="searchResult.message.stream_id" class="id-copy" style="margin-left:4px;" @click="copyText(searchResult.message.stream_id)">
            → {{ searchResult.message.stream_id }}
          </span>
          <el-button :icon="CopyDocument" size="small" circle class="copy-btn" title="复制消息体" @click="copyText(formatSearchBody(searchResult.message))" />
          <el-button :icon="RefreshRight" size="small" circle class="copy-btn" title="重新投递" @click="resendMsg(searchResult.message)" />
        </div>
        <div v-if="searchResult.message.due_at" style="font-size:12px;color:var(--el-text-color-secondary);margin-bottom:6px;">
          投递时间: {{ searchResult.message.due_at }}
        </div>
        <div v-if="searchResult.message.delay_id" style="font-size:12px;color:var(--el-text-color-secondary);margin-bottom:6px;">
          延迟消息 ID: {{ searchResult.message.delay_id }}
        </div>
        <pre class="msg-expand">{{ formatSearchBody(searchResult.message) }}</pre>
      </div>
      <el-empty v-else description="未找到该消息" :image-size="40" />
    </div>

    <!-- Tabs -->
    <el-tabs v-model="activeTab" type="border-card">

      <!-- Overview / Groups -->
      <el-tab-pane name="overview">
        <template #label>
          <span title="消费者组">Groups</span>
        </template>
        <div v-if="detail?.groups?.length" class="groups-list">
          <div v-for="group in sortedGroups" :key="group.name" class="group-card">
            <div class="group-header">
              <strong>{{ group.name }}</strong>
              <el-tag v-if="group.name === selfGroup" type="primary" size="small" effect="dark">当前</el-tag>
              <el-tag v-if="group.lag > 0" type="danger" size="small" effect="dark">lag: {{ group.lag }}</el-tag>
              <el-tag v-if="group.pending > 0" type="warning" size="small">pending: {{ group.pending }}</el-tag>
              <el-tag v-if="(!group.consumers || group.consumers.length === 0)" type="info" size="small">inactive</el-tag>
              <el-tag v-else-if="group.lag === 0 && group.pending === 0" type="success" size="small">healthy</el-tag>
              <template v-if="group.name === selfGroup">
                <el-button size="small" :icon="Position" @click="resetGroup(group, 'start')" title="重置到起始点（从头消费）">重置到起始</el-button>
                <el-button size="small" @click="resetGroup(group, 'latest')" title="重置到最新点（跳过历史）">重置到最新</el-button>
              </template>
              <el-button v-if="group.name !== selfGroup" size="small" :icon="Delete" type="danger" plain @click="destroyGroup(group)" title="销毁该消费组">
                销毁
              </el-button>
            </div>
            <div class="group-meta">
              <span class="meta-label">消费位点:</span>
              <span class="meta-value id-copy" @click="copyText(group.last_delivered || '')">{{ group.last_delivered || '-' }}</span>
            </div>
            <el-table :data="group.consumers || []" size="small" stripe>
              <el-table-column prop="name" label="Consumer" />
              <el-table-column prop="pending" label="Pending" width="80" align="center" />
              <el-table-column prop="idle" label="Idle" width="120" />
            </el-table>
          </div>
        </div>
        <el-empty v-else description="No consumer groups" :image-size="60" />
      </el-tab-pane>

      <!-- All Messages -->
      <el-tab-pane name="messages">
        <template #label>
          <span title="当前 Group 已投递的消息">Messages (Group)</span>
        </template>
        <div class="tab-actions">
          <el-button :icon="Download" size="small" @click="exportMessages('messages')" title="导出当前页为JSON">导出</el-button>
        </div>
        <el-table :data="messages" stripe max-height="420" size="small">
          <el-table-column type="expand">
            <template #default="{ row }">
              <pre class="msg-expand">{{ formatBody(row) }}</pre>
            </template>
          </el-table-column>
          <el-table-column label="ID" width="190">
            <template #default="{ row }">
              <span class="id-copy" @click.stop="copyText(row.id)">{{ row.id }}</span>
            </template>
          </el-table-column>
          <el-table-column label="Body">
            <template #default="{ row }">
              <span class="body-preview">{{ formatBody(row) }}</span>
            </template>
          </el-table-column>
          <el-table-column label="" width="80" align="center">
            <template #default="{ row }">
              <el-button :icon="CopyDocument" size="small" circle class="copy-btn" title="复制消息体" @click.stop="copyText(formatBody(row))" />
              <el-button :icon="RefreshRight" size="small" circle class="copy-btn" title="重新投递" @click.stop="resendMsg(row)" />
            </template>
          </el-table-column>
        </el-table>
        <div class="pagination-bar">
          <el-button :icon="ArrowLeft" size="small" :disabled="!hasPrev('messages')" @click="prevPage('messages')" title="上一页" />
          <span class="page-num">第 {{ pageNum('messages') }} 页</span>
          <el-button :icon="ArrowRight" size="small" :disabled="!hasNext('messages')" @click="nextPage('messages')" title="下一页" />
        </div>
      </el-tab-pane>

      <!-- Lag -->
      <el-tab-pane name="lag">
        <template #label>
          <span title="当前 Group 未投递的积压消息">Lag (Group)</span>
        </template>
        <div class="tab-actions">
          <el-button :icon="Download" size="small" @click="exportMessages('lag')" title="导出当前页为JSON">导出</el-button>
        </div>
        <el-table :data="lagMessages" stripe max-height="420" size="small">
          <el-table-column type="expand">
            <template #default="{ row }">
              <pre class="msg-expand">{{ formatBody(row) }}</pre>
            </template>
          </el-table-column>
          <el-table-column label="ID" width="190">
            <template #default="{ row }">
              <span class="id-copy" @click.stop="copyText(row.id)">{{ row.id }}</span>
            </template>
          </el-table-column>
          <el-table-column label="Body">
            <template #default="{ row }">
              <span class="body-preview">{{ formatBody(row) }}</span>
            </template>
          </el-table-column>
          <el-table-column label="" width="80" align="center">
            <template #default="{ row }">
              <el-button :icon="CopyDocument" size="small" circle class="copy-btn" title="复制消息体" @click.stop="copyText(formatBody(row))" />
              <el-button :icon="RefreshRight" size="small" circle class="copy-btn" title="重新投递" @click.stop="resendMsg(row)" />
            </template>
          </el-table-column>
        </el-table>
        <div class="pagination-bar">
          <el-button :icon="ArrowLeft" size="small" :disabled="!hasPrev('lag')" @click="prevPage('lag')" title="上一页" />
          <span class="page-num">第 {{ pageNum('lag') }} 页</span>
          <el-button :icon="ArrowRight" size="small" :disabled="!hasNext('lag')" @click="nextPage('lag')" title="下一页" />
        </div>
        <el-empty v-if="lagMessages.length === 0" description="No lag - all messages delivered" :image-size="60" />
      </el-tab-pane>

      <!-- Pending -->
      <el-tab-pane name="pending">
        <template #label>
          <span title="当前 Group 已投递未确认的消息">Pending (Group)</span>
        </template>
        <div class="tab-actions">
          <el-button :icon="Download" size="small" @click="exportMessages('pending')" title="导出当前页为JSON">导出</el-button>
        </div>
        <el-table :data="pendingMessages" stripe max-height="420" size="small">
          <el-table-column type="expand">
            <template #default="{ row }">
              <pre class="msg-expand">{{ formatBody(row) }}</pre>
            </template>
          </el-table-column>
          <el-table-column label="ID" width="170">
            <template #default="{ row }">
              <span class="id-copy" @click.stop="copyText(row.id)">{{ row.id }}</span>
            </template>
          </el-table-column>
          <el-table-column prop="consumer" label="Consumer" width="120" />
          <el-table-column prop="idle" label="Idle" width="80" />
          <el-table-column prop="retry_count" label="Retry" width="55" align="center" />
          <el-table-column label="Body">
            <template #default="{ row }">
              <span class="body-preview">{{ formatBody(row) }}</span>
            </template>
          </el-table-column>
          <el-table-column label="" width="80" align="center">
            <template #default="{ row }">
              <el-button :icon="CopyDocument" size="small" circle class="copy-btn" title="复制消息体" @click.stop="copyText(formatBody(row))" />
              <el-button :icon="RefreshRight" size="small" circle class="copy-btn" title="重新投递" @click.stop="resendMsg(row)" />
            </template>
          </el-table-column>
        </el-table>
        <div class="pagination-bar">
          <el-button :icon="ArrowLeft" size="small" :disabled="!hasPrev('pending')" @click="prevPage('pending')" title="上一页" />
          <span class="page-num">第 {{ pageNum('pending') }} 页</span>
          <el-button :icon="ArrowRight" size="small" :disabled="!hasNext('pending')" @click="nextPage('pending')" title="下一页" />
        </div>
      </el-tab-pane>

      <!-- Dead Letters -->
      <el-tab-pane name="dead">
        <template #label>
          <span title="死信队列">Dead</span>
        </template>
        <div class="tab-actions">
          <el-button type="primary" size="small" :icon="Refresh" :loading="requeueLoading" :disabled="deadMessages.length === 0" @click="requeue" title="全部重新入队">
            Requeue All
          </el-button>
          <el-button :icon="Download" size="small" @click="exportMessages('dead')" title="导出当前页为JSON">导出</el-button>
        </div>
        <el-table :data="deadMessages" stripe max-height="420" size="small">
          <el-table-column type="expand">
            <template #default="{ row }">
              <pre class="msg-expand">{{ row.body }}</pre>
            </template>
          </el-table-column>
          <el-table-column label="ID" width="190">
            <template #default="{ row }">
              <span class="id-copy" @click.stop="copyText(row.id)">{{ row.id }}</span>
            </template>
          </el-table-column>
          <el-table-column prop="reason" label="Reason" width="180" show-overflow-tooltip />
          <el-table-column label="Body">
            <template #default="{ row }">
              <span class="body-preview">{{ row.body }}</span>
            </template>
          </el-table-column>
          <el-table-column label="" width="110" align="center">
            <template #default="{ row }">
              <el-button :icon="CopyDocument" size="small" circle class="copy-btn" title="复制消息体" @click.stop="copyText(row.body || '')" />
              <el-button :icon="RefreshRight" size="small" circle class="copy-btn" title="重新投递" @click.stop="resendMsg(row)" />
              <el-button :icon="Delete" size="small" circle class="copy-btn del-btn" title="删除该消息" @click.stop="deleteDead(row)" />
            </template>
          </el-table-column>
        </el-table>
        <div class="pagination-bar">
          <el-button :icon="ArrowLeft" size="small" :disabled="!hasPrev('dead')" @click="prevPage('dead')" title="上一页" />
          <span class="page-num">第 {{ pageNum('dead') }} 页</span>
          <el-button :icon="ArrowRight" size="small" :disabled="!hasNext('dead')" @click="nextPage('dead')" title="下一页" />
        </div>
      </el-tab-pane>

      <!-- Delay -->
      <el-tab-pane name="delay">
        <template #label>
          <span title="延迟队列">Delay</span>
        </template>
        <div class="tab-actions">
          <el-button :icon="Download" size="small" @click="exportMessages('delay')" title="导出当前页为JSON">导出</el-button>
        </div>
        <el-table :data="delayMessages" stripe max-height="420" size="small">
          <el-table-column type="expand">
            <template #default="{ row }">
              <pre class="msg-expand">{{ row.body }}</pre>
            </template>
          </el-table-column>
          <el-table-column label="Delay ID" width="220">
            <template #default="{ row }">
              <span class="id-copy" @click.stop="copyText(row.id)">{{ row.id }}</span>
            </template>
          </el-table-column>
          <el-table-column prop="due_at" label="Due At" width="200" />
          <el-table-column label="Body">
            <template #default="{ row }">
              <span class="body-preview">{{ row.body }}</span>
            </template>
          </el-table-column>
          <el-table-column label="" width="110" align="center">
            <template #default="{ row }">
              <el-button :icon="CopyDocument" size="small" circle class="copy-btn" title="复制消息体" @click.stop="copyText(row.body || '')" />
              <el-button :icon="RefreshRight" size="small" circle class="copy-btn" title="重新投递" @click.stop="resendMsg(row)" />
              <el-button :icon="Delete" size="small" circle class="copy-btn del-btn" title="删除该消息" @click.stop="deleteDelay(row)" />
            </template>
          </el-table-column>
        </el-table>
        <div class="pagination-bar">
          <el-button :icon="ArrowLeft" size="small" :disabled="!hasPrev('delay')" @click="prevPage('delay')" title="上一页" />
          <span class="page-num">第 {{ pageNum('delay') }} 页</span>
          <el-button :icon="ArrowRight" size="small" :disabled="!hasNext('delay')" @click="nextPage('delay')" title="下一页" />
        </div>
      </el-tab-pane>

    </el-tabs>

    <!-- Publish Dialog -->
    <el-dialog v-model="publishVisible" title="发布消息" width="500" append-to-body>
      <el-form label-position="top">
        <el-form-item label="Payload (JSON)">
          <el-input
            v-model="publishBody"
            type="textarea"
            :rows="8"
            placeholder='{"order_id": "12345", "amount": 99.9}'
          />
        </el-form-item>
        <el-form-item label="定时投递（可选，不选则立即投递）">
          <el-date-picker
            v-model="publishDueAt"
            type="datetime"
            placeholder="选择投递时间"
            format="YYYY-MM-DD HH:mm:ss"
            :disabled-date="(date) => date.getTime() < Date.now() - 86400000"
          />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="publishVisible = false">取消</el-button>
        <el-button type="primary" :loading="publishLoading" @click="publishMessage">发布</el-button>
      </template>
    </el-dialog>
  </el-drawer>
</template>

<style scoped>
.stats-row {
  margin-bottom: 16px;
}

.stat-box {
  text-align: center;
  padding: 10px 0;
  border-radius: 6px;
  background: var(--el-bg-color-overlay, #262727);
}

.stat-box .stat-num {
  font-size: 20px;
  font-weight: 700;
  color: var(--el-text-color-primary, #e5eaf3);
}

.stat-box.danger .stat-num { color: #f56c6c; }
.stat-box.warn .stat-num { color: #e6a23c; }

.stat-box .stat-desc {
  font-size: 11px;
  color: var(--el-text-color-secondary, #a3a6ad);
  margin-top: 2px;
}

.search-bar {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 16px;
}

.search-result {
  margin-bottom: 16px;
  padding: 12px;
  background: var(--el-bg-color-overlay, #262727);
  border-radius: 6px;
}

.search-found-header {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 8px;
}

.groups-list {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.group-card {
  background: var(--el-bg-color-overlay, #262727);
  border-radius: 6px;
  padding: 12px;
}

.group-header {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 4px;
}

.group-meta {
  display: flex;
  align-items: center;
  gap: 6px;
  margin-bottom: 8px;
  font-size: 12px;
}

.meta-label {
  color: var(--el-text-color-secondary, #a3a6ad);
}

.meta-value {
  font-family: monospace;
  color: var(--el-text-color-regular, #cfd3dc);
}

.tab-actions {
  margin-bottom: 12px;
  display: flex;
  gap: 8px;
}

.pagination-bar {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 12px;
  margin-top: 12px;
  padding: 8px 0;
}

.page-num {
  font-size: 13px;
  color: var(--el-text-color-secondary, #a3a6ad);
}

.id-copy {
  cursor: pointer;
  font-family: monospace;
  font-size: 12px;
  color: #409eff;
}
.id-copy:hover {
  text-decoration: underline;
}

.body-preview {
  display: block;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-size: 12px;
  font-family: monospace;
  max-width: 100%;
}

.msg-expand {
  margin: 0;
  padding: 12px 16px;
  font-size: 13px;
  white-space: pre-wrap;
  word-break: break-all;
  background: var(--el-bg-color, #1a1a1a);
  border-radius: 4px;
  max-height: 300px;
  overflow: auto;
}

.copy-btn {
  opacity: 0.4;
}
.copy-btn:hover {
  opacity: 1;
}

.del-btn:hover {
  color: #f56c6c;
}
</style>
