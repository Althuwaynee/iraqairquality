let map;
let districts = [];
let showAllDistricts = false;
const MAX_VISIBLE_DISTRICTS = 15; // Number of districts to show by default

document.addEventListener('DOMContentLoaded', init);

async function init() {
  initMap();
  await loadData();
  renderMap();
  renderList();
  
  // Setup "View all districts" button
  setupViewAllButton();
}

function initMap() {
  map = L.map('map').setView([33, 44], 6);

  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: '© OpenStreetMap'
  }).addTo(map);
}

async function loadData() {
  try {
    const res = await fetch('./data/pm10_now.json');
    const data = await res.json();
    
    // Extract the districts array from the response
    districts = data.districts || [];
    
    // Add pm10_now field to match your code
    districts.forEach(d => {
      d.pm10_now = d.dust_final; // Copy dust_final to pm10_now
    });
    
    // Sort districts by PM10 value (highest to lowest)
    districts.sort((a, b) => b.pm10_now - a.pm10_now);
    
    console.log(`Loaded ${districts.length} districts`);
  } catch (error) {
    console.error('Error loading data:', error);
  }
}

function renderMap() {
  if (districts.length === 0) {
    console.log('No districts to display');
    return;
  }
  
  districts.forEach(d => {
    const lat = d.latitude;
    const lon = d.longitude;
    const pm10 = d.pm10_now;

    if (!lat || !lon) {
      console.warn(`Missing coordinates for ${d.district_name}`);
      return;
    }

    const color = getColor(pm10);

    L.circleMarker([lat, lon], {
      radius: getRadius(pm10),
      color,
      fillColor: color,
      fillOpacity: 0.8,
      weight: 1
    })
      .addTo(map)
      .bindPopup(`
        <div style="min-width: 200px;">
          <strong>${d.district_name}</strong><br>
          Province: ${d.province_name}<br>
          PM10: ${Math.round(pm10)} µg/m³<br>
          Status: ${getAQILevel(pm10)}
        </div>
      `)
      .on('click', () => openDistrict(d));
  });
}

function renderList(filter = '') {
  const list = document.getElementById('district-list');
  list.innerHTML = '';

  // Filter districts based on search
  const filteredDistricts = districts.filter(d =>
    d.district_name.toLowerCase().includes(filter.toLowerCase()) ||
    d.province_name.toLowerCase().includes(filter.toLowerCase())
  );

  if (filteredDistricts.length === 0) {
    const li = document.createElement('li');
    li.innerHTML = '<span style="width: 100%; text-align: center;">No districts found</span>';
    list.appendChild(li);
    return;
  }

  // Determine how many districts to show
  const districtsToShow = showAllDistricts ? filteredDistricts : filteredDistricts.slice(0, MAX_VISIBLE_DISTRICTS);

  districtsToShow.forEach(d => {
    const li = document.createElement('li');
    li.style.borderLeftColor = getColor(d.pm10_now);
    li.style.borderLeftWidth = '5px';
    li.style.borderLeftStyle = 'solid';
    li.style.cursor = 'pointer';
    li.style.transition = 'background-color 0.2s';

    li.innerHTML = `
      <div style="flex: 1;">
        <div style="font-weight: 500; margin-bottom: 2px;">${d.district_name}</div>
        <div style="font-size: 0.8em; color: #666;">${d.province_name}</div>
      </div>
      <div style="text-align: right;">
        <div style="font-weight: 600; font-size: 1.1em;">${Math.round(d.pm10_now)}</div>
        <div style="font-size: 0.7em; color: #666;">µg/m³</div>
      </div>
    `;

    li.onclick = () => openDistrict(d);
    
    // Add hover effect
    li.onmouseenter = () => {
      li.style.backgroundColor = '#edf2f7';
    };
    li.onmouseleave = () => {
      li.style.backgroundColor = '#f8fafc';
    };
    
    list.appendChild(li);
  });

  // Update "View all districts" button text
  updateViewAllButton(filteredDistricts.length);
}

function setupViewAllButton() {
  const viewAllBtn = document.querySelector('.view-all');
  viewAllBtn.onclick = () => {
    showAllDistricts = !showAllDistricts;
    renderList(document.getElementById('search').value);
  };
}

function updateViewAllButton(totalFilteredCount) {
  const viewAllBtn = document.querySelector('.view-all');
  
  if (totalFilteredCount <= MAX_VISIBLE_DISTRICTS) {
    viewAllBtn.style.display = 'none';
  } else {
    viewAllBtn.style.display = 'block';
    if (showAllDistricts) {
      viewAllBtn.innerHTML = `Show top ${MAX_VISIBLE_DISTRICTS} districts only ↑`;
    } else {
      const remaining = totalFilteredCount - MAX_VISIBLE_DISTRICTS;
      viewAllBtn.innerHTML = `View all ${totalFilteredCount} districts (${remaining} more) →`;
    }
  }
}

document.getElementById('search').addEventListener('input', e => {
  renderList(e.target.value);
});

function openDistrict(d) {
  // Fit the map to the district with some zoom
  map.setView([d.latitude, d.longitude], 10);
  
  // Open the popup for this district
  map.eachLayer(layer => {
    if (layer.getPopup && layer.getPopup()) {
      const popup = layer.getPopup();
      if (popup.getContent().includes(d.district_name)) {
        layer.openPopup();
      }
    }
  });
}

function getColor(pm10) {
  if (pm10 <= 50) return '#2ecc71';
  if (pm10 <= 100) return '#f1c40f';
  if (pm10 <= 150) return '#e67e22';
  if (pm10 <= 300) return '#e74c3c';
  return '#8e44ad';
}

function getRadius(pm10) {
  // Scale radius based on PM10 value (min 5, max 15)
  const baseRadius = 5;
  const scaleFactor = 10;
  const scaledRadius = baseRadius + (pm10 / 300) * scaleFactor;
  return Math.min(Math.max(scaledRadius, 5), 15);
}

function getAQILevel(pm10) {
  if (pm10 <= 50) return 'Good';
  if (pm10 <= 100) return 'Moderate';
  if (pm10 <= 150) return 'Unhealthy';
  if (pm10 <= 300) return 'Very Unhealthy';
  return 'Hazardous';
}