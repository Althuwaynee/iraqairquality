/**
 * FINAL WORKING VERSION - Iraq Air Quality Monitor
 */

// Global variables
let mapNow, mapRisk;
let nowData = [], alertData = [];

// Debug function
function debugLog(message, data = null) {
  console.log(`[DEBUG] ${message}`, data || '');
  const debugEl = document.getElementById('debug-status');
  if (debugEl) debugEl.textContent = message.substring(0, 100);
}

// Initialize when page loads
document.addEventListener('DOMContentLoaded', function() {
  debugLog('Page loaded, starting initialization...');
  initApp();
});

async function initApp() {
  try {
    // 1. Initialize maps
    debugLog('Step 1: Initializing maps...');
    mapNow = L.map('map-now').setView([33.0, 44.0], 6);
    mapRisk = L.map('map-risk').setView([33.0, 44.0], 6);
    
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: '© OpenStreetMap'
    }).addTo(mapNow);
    
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: '© OpenStreetMap'
    }).addTo(mapRisk);
    
    // Add test markers to verify maps work
    L.marker([33.0, 44.0])
      .addTo(mapNow)
      .bindTooltip('TEST: Maps are working!')
      .openTooltip();
    
    // 2. Load BOTH data files
    debugLog('Step 2: Loading data files...');
    
    // IMPORTANT: Check which file we're actually loading
    console.log('Testing file paths...');
    
    // Test 1: Try your REAL data file (the one you showed me earlier)
    try {
      const testResponse = await fetch('./data/pm10_now.json');
      const testData = await testResponse.json();
      console.log('ACTUAL pm10_now.json loaded:', testData);
      console.log('Is array?', Array.isArray(testData));
      console.log('Length:', testData.length);
      console.log('First item:', testData[0]);
      
      if (testData.length === 5) {
        console.warn('WARNING: Only 5 items found. This might be TEST data!');
        console.warn('Your real data should have 100+ items');
      }
    } catch (error) {
      console.error('Cannot load pm10_now.json:', error);
    }
    
    // Load current data
    try {
      debugLog('Loading current PM10 data...');
      const response = await fetch('./data/pm10_now.json');
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      
      const jsonData = await response.json();
      console.log('Current data structure:', jsonData);
      
      // Handle different data structures
      if (Array.isArray(jsonData)) {
        nowData = jsonData; // Your data is already an array
      } else if (jsonData.districts && Array.isArray(jsonData.districts)) {
        nowData = jsonData.districts;
      } else if (jsonData.data && Array.isArray(jsonData.data)) {
        nowData = jsonData.data;
      } else {
        console.warn('Unexpected data structure:', jsonData);
        nowData = [];
      }
      
      debugLog(`Loaded ${nowData.length} current records`);
      
    } catch (error) {
      console.error('Failed to load current data:', error);
      debugLog(`Error: ${error.message}`);
      // Use fallback test data
      nowData = createTestData();
    }
    
    // Load alert data
    try {
      debugLog('Loading alert data...');
      const response = await fetch('./data/pm10_alerts.json');
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      
      const jsonData = await response.json();
      console.log('Alert data structure:', jsonData);
      
      // Handle different data structures
      if (Array.isArray(jsonData)) {
        alertData = jsonData;
      } else if (jsonData.districts && Array.isArray(jsonData.districts)) {
        alertData = jsonData.districts;
      } else if (jsonData.data && Array.isArray(jsonData.data)) {
        alertData = jsonData.data;
      } else {
        console.warn('Unexpected data structure:', jsonData);
        alertData = [];
      }
      
      debugLog(`Loaded ${alertData.length} alert records`);
      
    } catch (error) {
      console.error('Failed to load alert data:', error);
      debugLog(`Error: ${error.message}`);
      // Use fallback test data
      alertData = createTestAlertData();
    }
    
    // 3. Render maps
    debugLog('Step 3: Rendering maps...');
    renderCurrentMap();
    renderRiskMap();
    updateStatistics();
    
    // 4. Finalize
    debugLog('Dashboard ready!');
    
    // Fix map sizing
    setTimeout(() => {
      mapNow.invalidateSize();
      mapRisk.invalidateSize();
    }, 100);
    
  } catch (error) {
    console.error('Fatal error:', error);
    debugLog(`FATAL: ${error.message}`);
    alert(`Dashboard failed to load: ${error.message}\nCheck console for details.`);
  }
}

function renderCurrentMap() {
  debugLog(`Rendering current map with ${nowData.length} markers...`);
  
  // Clear existing markers (except base layers)
  mapNow.eachLayer(layer => {
    if (layer instanceof L.CircleMarker || layer instanceof L.Marker) {
      mapNow.removeLayer(layer);
    }
  });
  
  if (nowData.length === 0) {
    debugLog('No current data to display');
    return;
  }
  
  let sumNow = 0;
  let countNow = 0;
  
  nowData.forEach((district, index) => {
    // Extract data - handle multiple field names
    const lat = getNumber(district, ['latitude', 'lat']);
    const lng = getNumber(district, ['longitude', 'lng', 'lon']);
    const pm10 = getNumber(district, ['dust_final', 'pm10', 'pm10_now', 'value']);
    const name = district.district_name || district.name || `District ${index + 1}`;
    
    if (!lat || !lng) {
      console.warn(`Skipping ${name}: Invalid coordinates`, district);
      return;
    }
    
    // Add to statistics
    if (pm10 > 0) {
      sumNow += pm10;
      countNow++;
    }
    
    // Determine color based on PM10
    const color = getColorForPM10(pm10);
    const radius = Math.min(Math.max(pm10 / 20, 5), 15);
    
    // Create marker
    L.circleMarker([lat, lng], {
      radius: radius,
      color: color,
      fillColor: color,
      fillOpacity: 0.8,
      weight: 1
    })
    .addTo(mapNow)
    .bindTooltip(`
      <div style="min-width: 200px;">
        <strong>${name}</strong><br>
        PM10: <b>${pm10.toFixed(1)} µg/m³</b><br>
        ${district.province_name ? `Province: ${district.province_name}<br>` : ''}
        AQI: <span style="color: ${color}"><b>${getAQILabel(pm10)}</b></span>
      </div>
    `);
  });
  
  // Update current average
  const avgNow = countNow > 0 ? (sumNow / countNow).toFixed(1) : '0.0';
  document.getElementById('avg-now').textContent = avgNow;
  
  debugLog(`Current map: ${countNow} markers, average: ${avgNow}`);
}

function renderRiskMap() {
  debugLog(`Rendering risk map with ${alertData.length} markers...`);
  
  // Clear existing markers
  mapRisk.eachLayer(layer => {
    if (layer instanceof L.CircleMarker || layer instanceof L.Marker) {
      mapRisk.removeLayer(layer);
    }
  });
  
  if (alertData.length === 0) {
    debugLog('No alert data to display');
    return;
  }
  
  // Process and sort data
  const processedAlerts = alertData.map(district => {
    // Extract 24h PM10 from your data structure
    let pm24h = 0;
    
    // Try the alert value first (this is the 24h mean)
    if (district.alert && district.alert.value) {
      pm24h = district.alert.value;
    }
    // Try the nested pm10.mean_24h
    else if (district.pm10 && district.pm10.mean_24h) {
      pm24h = district.pm10.mean_24h;
    }
    
    // Extract coordinates
    const lat = district.latitude;
    const lng = district.longitude;
    const name = district.district_name;
    
    return {
      ...district,
      pm24h: Number(pm24h) || 0,
      lat: lat,
      lng: lng,
      name: name
    };
  });
  
  // Filter out invalid entries
  const validAlerts = processedAlerts.filter(d => 
    d.pm24h > 0 && d.lat && d.lng && d.name
  );
  
  // Sort by 24h value (highest risk first)
  const sortedAlerts = validAlerts.sort((a, b) => b.pm24h - a.pm24h);
  
  debugLog(`Found ${sortedAlerts.length} valid districts with 24h data`);
  console.log('Top 5 districts by 24h PM10:', sortedAlerts.slice(0, 5));
  
  // Update risk list (top 5)
  const riskList = document.getElementById('risk-list');
  riskList.innerHTML = '';
  
  sortedAlerts.slice(0, 5).forEach((district, index) => {
    const li = document.createElement('li');
    const color = getColorForPM10(district.pm24h);
    
    li.innerHTML = `
      <div style="display: flex; align-items: center; gap: 8px; margin-bottom: 4px;">
        <span style="background: ${color}; width: 12px; height: 12px; border-radius: 50%;"></span>
        <strong>${index + 1}. ${district.name}</strong>
      </div>
      <div style="font-size: 0.9em; color: #475569;">
        24h mean: <strong>${district.pm24h.toFixed(1)}</strong> µg/m³
      </div>
      <div style="font-size: 0.8em; color: #64748b; margin-top: 2px;">
        ${getHealthAdvice(district.pm24h)}
      </div>
    `;
    riskList.appendChild(li);
  });
  
  // Add markers to risk map
  sortedAlerts.forEach(district => {
    const color = getColorForPM10(district.pm24h);
    const radius = Math.min(Math.max(district.pm24h / 25, 6), 20);
    
    // Create marker
    L.circleMarker([district.lat, district.lng], {
      radius: radius,
      color: color,
      fillColor: color,
      fillOpacity: 0.7,
      weight: 1
    })
    .addTo(mapRisk)
    .bindTooltip(`
      <div style="min-width: 200px;">
        <strong>${district.name}</strong><br>
        24h mean: <b>${district.pm24h.toFixed(1)} µg/m³</b><br>
        ${district.province_name ? `Province: ${district.province_name}<br>` : ''}
        Alert Level: <span style="color: ${color}; text-transform: capitalize;"><b>${district.alert?.level?.replace('_', ' ') || getAQILabel(district.pm24h)}</b></span>
      </div>
    `);
  });
  
  debugLog(`Risk map rendered with ${sortedAlerts.length} markers`);
  
  // Force map refresh
  setTimeout(() => {
    mapRisk.invalidateSize();
  }, 100);
}

// Add this to main.js after the maps are initialized
function testRiskMap() {
  // Check if map container exists and has proper dimensions
  const riskMapDiv = document.getElementById('map-risk');
  console.log('Risk map container:', riskMapDiv);
  console.log('Risk map dimensions:', riskMapDiv?.offsetWidth, 'x', riskMapDiv?.offsetHeight);
  
  // Add a test marker
  if (mapRisk) {
    L.marker([33.0, 44.0])
      .addTo(mapRisk)
      .bindTooltip('Test marker - Risk map is working!')
      .openTooltip();
    
    console.log('Test marker added to risk map');
  }
}

// Call it after initialization
setTimeout(testRiskMap, 1000);
function updateStatistics() {
  // Update 3-hour average if available
  if (alertData.length > 0) {
    let sum3h = 0, count3h = 0;
    
    alertData.forEach(district => {
      let pm3h = 0;
      if (district.pm10 && district.pm10.mean_3h) {
        pm3h = district.pm10.mean_3h;
      } else if (district.pm10 && district.pm10.mean_6h) {
        pm3h = district.pm10.mean_6h;
      } else if (district.mean_3h) {
        pm3h = district.mean_3h;
      }
      
      if (pm3h > 0) {
        sum3h += pm3h;
        count3h++;
      }
    });
    
    const avg3h = count3h > 0 ? (sum3h / count3h).toFixed(1) : '0.0';
    document.getElementById('avg-3h').textContent = avg3h;
  }
  
  // Update timestamp
  const now = new Date();
  document.getElementById('last-update').textContent = 
    `Last update: ${now.toLocaleDateString()} ${now.toLocaleTimeString([], {hour: '2-digit', minute:'2-digit'})}`;
}

// Helper functions
function getNumber(obj, keys) {
  for (const key of keys) {
    if (obj[key] !== undefined && obj[key] !== null) {
      const num = Number(obj[key]);
      if (!isNaN(num)) return num;
    }
  }
  return 0;
}

function getColorForPM10(pm10) {
  if (pm10 <= 50) return '#2ecc71';      // Green
  if (pm10 <= 100) return '#f1c40f';     // Yellow
  if (pm10 <= 150) return '#e67e22';     // Orange
  if (pm10 <= 300) return '#e74c3c';     // Red
  return '#8e44ad';                      // Purple
}

function getAQILabel(pm10) {
  if (pm10 <= 50) return 'Good';
  if (pm10 <= 100) return 'Moderate';
  if (pm10 <= 150) return 'Unhealthy';
  if (pm10 <= 300) return 'Very Unhealthy';
  return 'Hazardous';
}

function getHealthAdvice(pm10) {
  if (pm10 <= 50) return '✅ Good air quality';
  if (pm10 <= 100) return '⚠️ Sensitive groups should limit outdoor exertion';
  if (pm10 <= 150) return '🚫 Avoid prolonged outdoor activities';
  if (pm10 <= 300) return '🚫 Stay indoors with air purifier if possible';
  return '🚫🚫 Avoid all outdoor activities';
}

// Test data fallback
function createTestData() {
  console.log('Creating test data...');
  return [
    { district_name: "Baghdad", latitude: 33.3152, longitude: 44.3661, dust_final: 150.5 },
    { district_name: "Basra", latitude: 30.5, longitude: 47.8, dust_final: 85.3 },
    { district_name: "Erbil", latitude: 36.19, longitude: 44.01, dust_final: 25.8 },
    { district_name: "Mosul", latitude: 36.34, longitude: 43.13, dust_final: 280.2 },
    { district_name: "Najaf", latitude: 32.03, longitude: 44.35, dust_final: 320.7 }
  ];
}

function createTestAlertData() {
  return [
    {
      district_name: "Najaf",
      latitude: 32.03,
      longitude: 44.35,
      pm10: { mean_24h: 320.7, mean_3h: 280.2 },
      alert: { level: "hazardous", value: 320.7 }
    },
    {
      district_name: "Mosul", 
      latitude: 36.34,
      longitude: 43.13,
      pm10: { mean_24h: 280.2, mean_3h: 250.1 },
      alert: { level: "very_unhealthy", value: 280.2 }
    }
  ];
}

// Debug commands for console
window.showData = function() {
  console.log('=== CURRENT DATA ===');
  console.log('Length:', nowData.length);
  console.log('First 3 items:', nowData.slice(0, 3));
  
  console.log('=== ALERT DATA ===');
  console.log('Length:', alertData.length);
  console.log('First 3 items:', alertData.slice(0, 3));
};

window.testMaps = function() {
  // Add test markers to verify maps work
  L.marker([33.5, 44.5])
    .addTo(mapNow)
    .bindTooltip('Test marker 1')
    .openTooltip();
  
  L.marker([32.5, 43.5])
    .addTo(mapRisk)
    .bindTooltip('Test marker 2')
    .openTooltip();
  
  console.log('Test markers added to both maps');
};