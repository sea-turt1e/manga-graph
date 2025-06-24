import axios from 'axios'

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000'

const api = axios.create({
  baseURL: API_BASE_URL,
  timeout: 30000,
  headers: {
    'Content-Type': 'application/json'
  }
})

export const searchManga = async (searchRequest) => {
  try {
    const response = await api.post('/search', searchRequest)
    return response.data
  } catch (error) {
    console.error('Search API error:', error)
    throw error
  }
}

export const getAuthors = async () => {
  try {
    const response = await api.get('/authors')
    return response.data
  } catch (error) {
    console.error('Authors API error:', error)
    throw error
  }
}

export const getWorks = async () => {
  try {
    const response = await api.get('/works')
    return response.data
  } catch (error) {
    console.error('Works API error:', error)
    throw error
  }
}

export const getMagazines = async () => {
  try {
    const response = await api.get('/magazines')
    return response.data
  } catch (error) {
    console.error('Magazines API error:', error)
    throw error
  }
}

export const healthCheck = async () => {
  try {
    const response = await api.get('/health')
    return response.data
  } catch (error) {
    console.error('Health check error:', error)
    throw error
  }
}