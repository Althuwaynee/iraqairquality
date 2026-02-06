/* ===============================
   Iraq Air Quality – Main JS
   Uses pm10_alerts.json
   =============================== */

/* ---------- Globals ---------- */
let pm10Data = null;
let districtLayer = L.layerGroup();

/* ---------- Init ---------- */
document.addEventListener("DOMContentLoaded", () => {
  initMap();
  loadPM10Alerts();
});

/* ---------- Map ---------- */
function initMap() {

  map = L.map("map", {
    center: [33.2, 44.3],
    zoom: 6,
    zoomControl: true
  });

  L.tileLayer(
    "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
    {
      attribution: "&copy; OpenStreetMap contributors"
    }
  ).addTo(map);

  districtLayer.addTo(map);
}

/* ---------- Load JSON ---------- */
async function loadPM10Alerts() {
  try {
    const res = await fetch("data/pm10_alerts.json");
    pm10Data = await res.json();

    updateHeaderInfo(pm10Data.metadata);
    drawDistrictMarkers(pm10Data.districts);

    console.log(
      "PM10 alerts loaded:",
      pm10Data.districts.length,
      "districts"
    );

  } catch (err) {
    console.error("Failed to load pm10_alerts.json", err);
  }
}

/* ---------- Header ---------- */
function updateHeaderInfo(metadata) {
  const refTimeEl = document.getElementById("ref-time");
  const noteEl = document.getElementById("data-note");

  if (refTimeEl) {
    refTimeEl.textContent =
      `Reference time: ${new Date(metadata.reference_time).toUTCString()}`;
  }

  if (noteEl) {
    noteEl.textContent =
      `3-hourly data • Rolling windows: ${metadata.rolling_windows_hours.join(", ")}h • Gov limit: ${metadata.government_compliance_limit}`;
  }
}

/* ---------- Colors ---------- */
function getAlertColor(level) {
  switch (level) {
    case "good": return "#22c55e";
    case "moderate": return "#eab308";
    case "unhealthy": return "#f97316";
    case "very_unhealthy": return "#ef4444";
    case "hazardous": return "#7c2d12";
    default: return "#94a3b8";
  }
}

/* ---------- Draw Markers ---------- */
function drawDistrictMarkers(districts) {

  districtLayer.clearLayers();

  districts.forEach(d => {

    if (!d.latitude || !d.longitude) return;

    const color = getAlertColor(d.alert.level);

    const marker = L.circleMarker(
      [d.latitude, d.longitude],
      {
        radius: 8,
        fillColor: color,
        fillOpacity: 0.85,
        color: "#0f172a",
        weight: 1
      }
    );

    marker.bindPopup(buildDistrictPopup(d));
    marker.addTo(districtLayer);
  });
}

/* ---------- Popup ---------- */
function buildDistrictPopup(d) {

  return `
    <div class="popup">
      <h4>${d.district_name}</h4>
      <small>${d.province_name}</small>
      <hr>

      <b>PM10 (µg/m³)</b><br>
      Now: ${d.pm10.now.toFixed(1)}<br>
      6h mean: ${d.pm10.mean_6h.toFixed(1)} (${d.pm10.mean_6h_points} pts)<br>
      12h mean: ${d.pm10.mean_12h.toFixed(1)} (${d.pm10.mean_12h_points} pts)<br>
      24h mean: ${d.pm10.mean_24h.toFixed(1)} (${d.pm10.mean_24h_points} pts)<br>

      <hr>
      <b>AQI</b>: ${d.aqi.value} (${d.aqi.level})<br>

      <b>Government compliance</b>:<br>
      ${d.government_compliance.status}
      (limit ${d.government_compliance.limit_24h_ug_m3} µg/m³)

      <hr>
      <b>Alert</b>:
      <span style="color:${getAlertColor(d.alert.level)}; font-weight:600">
        ${d.alert.level.toUpperCase()}
      </span>
    </div>
  `;
}

/* ---------- Optional: Refresh ---------- */
async function refreshData() {
  await loadPM10Alerts();
}
