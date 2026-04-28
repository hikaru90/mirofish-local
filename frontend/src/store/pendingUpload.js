/**
 * 临时存储待提交的文本和需求
 * 用于首页点击启动引擎后立即跳转，在Process页面再进行API调用
 */
import { reactive } from 'vue'

const state = reactive({
  sourceText: '',
  simulationRequirement: '',
  isPending: false
})

export function setPendingUpload(sourceText, requirement) {
  state.sourceText = sourceText
  state.simulationRequirement = requirement
  state.isPending = true
}

export function getPendingUpload() {
  return {
    sourceText: state.sourceText,
    simulationRequirement: state.simulationRequirement,
    isPending: state.isPending
  }
}

export function clearPendingUpload() {
  state.sourceText = ''
  state.simulationRequirement = ''
  state.isPending = false
}

export default state
