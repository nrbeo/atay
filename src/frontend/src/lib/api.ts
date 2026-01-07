import axios from 'axios';

// Use environment variable or default to local backend
const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Meta endpoints
export const meta = {
  health: () => api.get('/meta/health'),
  tables: () => api.get('/meta/tables'),
  rowCounts: () => api.get('/meta/row-counts'),
  info: () => api.get('/meta/info'),
};

// Dimensions endpoints
export const dimensions = {
  shapes: () => api.get('/dimensions/shapes'),
  shapeByKey: (key: string) => api.get(`/dimensions/shapes/${key}`),
  frshtt: () => api.get('/dimensions/frshtt'),
  frshttByKey: (key: string) => api.get(`/dimensions/frshtt/${key}`),
  locations: (params?: { country?: string; city?: string; limit?: number; offset?: number }) =>
    api.get('/dimensions/locations', { params }),
  locationByKey: (key: string) => api.get(`/dimensions/locations/${key}`),
  weatherStations: (params?: { limit?: number; offset?: number }) =>
    api.get('/dimensions/weather-stations', { params }),
  weatherStationByKey: (key: string) => api.get(`/dimensions/weather-stations/${key}`),
  dateRange: () => api.get('/dimensions/date-range'),
};

// UFO observations endpoints
export interface ObservationsParams {
  date_from?: string;
  date_to?: string;
  city?: string;
  country?: string;
  state?: string;
  shape_key?: number;
  min_duration?: number;
  max_duration?: number;
  only_commented?: boolean;
  limit?: number;
  offset?: number;
}

export const ufo = {
  observations: (params?: ObservationsParams) => api.get('/ufo/observations', { params }),
  observationById: (factId: string) => api.get(`/ufo/observations/${factId}`),
  observationDetail: (factId: string) => api.get(`/ufo/observations/${factId}/detail`),
  count: (params?: Partial<ObservationsParams>) => api.get('/ufo/count', { params }),
};

// Map endpoints
export interface MapParams {
  country?: string;
  date_from?: string;
  date_to?: string;
  max_points?: number;
}

export const map = {
  points: (params?: MapParams) => api.get('/ufo/map/points', { params }),
  heatmap: (params?: MapParams) => api.get('/ufo/map/heatmap', { params }),
  bounds: (params?: { country?: string }) => api.get('/ufo/map/bounds', { params }),
};

// Stats endpoints
export const stats = {
  overview: () => api.get('/stats/overview'),
  topLocations: (params?: { country?: string; limit?: number }) =>
    api.get('/stats/top-locations', { params }),
  topCountries: (params?: { limit?: number }) => api.get('/stats/top-countries', { params }),
  bySeason: () => api.get('/stats/by-season'),
  byWeather: () => api.get('/stats/by-weather'),
  byShape: () => api.get('/stats/by-shape'),
  byYear: () => api.get('/stats/by-year'),
  timeSeriesMonthly: (params?: { country?: string; shape_key?: number }) =>
    api.get('/stats/time-series/monthly', { params }),
  durationDistribution: () => api.get('/stats/duration-distribution'),
  
  // Climate × UFO correlation endpoints
  shapeByWeather: () => api.get('/stats/shape-by-weather'),
  shapeBySeasonData: () => api.get('/stats/shape-by-season'),
  durationByWeather: () => api.get('/stats/duration-by-weather'),
  byTemperature: () => api.get('/stats/by-temperature'),
  byVisibility: () => api.get('/stats/by-visibility'),
  topShapesByWeather: () => api.get('/stats/top-shapes-by-weather'),
  seasonWeatherMatrix: () => api.get('/stats/season-weather-matrix'),
};

export default api;
