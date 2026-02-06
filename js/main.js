/* ===============================
   Iraq Air Quality – Main JS
   =============================== */

let map;
let pm10Data = null;
let districtLayer = L.layerGroup();
let showAllDistricts = false;

const MAX_VISIBLE_DISTRICTS = 15;

/* ---------- Init ---------- */
document.addEventListener("DOMContentLoaded", () => {
  initMap();
  loadPM10Alerts();

  // Search bar listener (FIX #1)
  const search = document.getElementById("search");
  search.addEventListener("input", () => {
    renderDistrictList(pm10Data?.districts || [], search.value);
  });
});

/* ---------- Map ---------- */
function initMap() {
  map = L.map("map").setView([33.2, 44.3], 6);

  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    attribution: "© OpenStreetMap"
  }).addTo(map);

  districtLayer.addTo(map);
}

/* ---------- Load JSON ---------- */
async function loadPM10Alerts() {
  try {
    const res = await fetch("data/pm10_alerts.json");
    pm10Data = await res.json();

    updateHeaderInfo(pm10Data.metadata);

    // Sort by AQI descending
    pm10Data.districts.sort((a, b) => b.aqi.value - a.aqi.value);

    drawDistrictMarkers(pm10Data.districts);
    renderDistrictList(pm10Data.districts);

  } catch (err) {
    console.error("Failed to load pm10_alerts.json", err);
  }
}

/* ---------- Header ---------- */
function updateHeaderInfo(metadata) {
  const refTime = document.getElementById("ref-time");
  const note = document.getElementById("data-note");

  if (refTime)
    refTime.textContent =
      `Reference time: ${new Date(metadata.reference_time).toUTCString()}`;

  if (note)
    note.textContent = metadata.note;
}

/* ---------- Colors ---------- */
function getAlertColor(level) {
  switch (level) {
    case "good": return "#22c55e";
    case "moderate": return "#eab308";
    case "unhealthy_for_sensitive_groups": return "#b675bdfb";
    case "unhealthy": return "#ee60dbfb";
    case "very_unhealthy": return "#ed1515";
    case "hazardous": return "#7c2d12";
    default: return "#261504";
  }
}

/* ---------- Markers ---------- */
function drawDistrictMarkers(districts) {
  districtLayer.clearLayers();

  districts.forEach(d => {
    if (!d.latitude || !d.longitude) return;

    const marker = L.circleMarker(
      [d.latitude, d.longitude],
      {
        radius: 8,
        fillColor: getAlertColor(d.alert.level),
        fillOpacity: 0.85,
        color: "#0f172a",
        weight: 1
      }
    );

    marker.bindPopup(buildDistrictPopup(d));

    // Attach district id for lookup
    marker._district_id = d.district_id;

    districtLayer.addLayer(marker);
  });
}

/* ---------- Popup ---------- */
function buildDistrictPopup(d) {

  // Collect forecast entries in order
  const forecasts = [
    d.pm10_forecast_3h,
    d.pm10_forecast_6h,
    d.pm10_forecast_9h,
    d.pm10_forecast_12h,
    d.pm10_forecast_15h,
    d.pm10_forecast_18h,
    d.pm10_forecast_21h,
    d.pm10_forecast_24h
  ].filter(Boolean);

  const forecastHTML = forecasts.map(f => {
    const date = new Date(f.timestamp);
    const label = date.toLocaleString("en-GB", {
      weekday: "short",
      hour: "2-digit",
      minute: "2-digit",
      hour12: false
    });

    return `
      <div class="forecast-item">
        <div class="forecast-time">${label}</div>
        <div
          class="forecast-dot"
          style="background:${getAlertColor(f.aqi_level)}"
          title="AQI ${f.aqi}"
        ></div>
      </div>
    `;
  }).join("");

  return `
    <div>
      <strong>${d.district_name}</strong><br>
      <small>${d.province_name}</small>
      <hr>
      PM10 now: <b>${d.pm10.now.toFixed(1)}</b> µg/m³<br>
      AQI: <b>${d.aqi.value}</b> (${d.aqi.level})<br>
      24h mean: ${d.pm10.mean_24h.toFixed(1)} µg/m³

      <div class="forecast-row">
        ${forecastHTML}
      </div>
    </div>
  `;
}


/* ---------- District List (FIXED) ---------- */
function renderDistrictList(districts, filter = "") {

  const list = document.getElementById("district-list");
  list.innerHTML = "";

  // Filter by district or province (FIX #1)
  const filtered = districts.filter(d =>
    d.district_name.toLowerCase().includes(filter.toLowerCase()) ||
    d.province_name.toLowerCase().includes(filter.toLowerCase())
  );

  const visible = showAllDistricts
    ? filtered
    : filtered.slice(0, MAX_VISIBLE_DISTRICTS);

  visible.forEach(d => {

    const li = document.createElement("li");
    li.style.borderLeftColor = getAlertColor(d.alert.level);

    li.innerHTML = `
      <div>
        <strong>${d.district_name}</strong><br>
        <small>${d.province_name}</small>
      </div>
      <div class="value">
        <span>${d.aqi.value}</span>
        <em>AQI</em>
      </div>
    `;

    // FIX #2 + #3: zoom + popup immediately
    li.onclick = () => zoomToDistrict(d);

    list.appendChild(li);
  });

  updateViewAllButton(filtered.length);
}

/* ---------- Zoom + Open Popup ---------- */
function zoomToDistrict(d) {
  map.setView([d.latitude, d.longitude], 10);

  // Open popup automatically (KEY FIX)
  districtLayer.eachLayer(layer => {
    if (layer._district_id === d.district_id) {
      layer.openPopup();
    }
  });
}

/* ---------- View All ---------- */
function updateViewAllButton(total) {
  const btn = document.querySelector(".view-all");

  if (total <= MAX_VISIBLE_DISTRICTS) {
    btn.style.display = "none";
    return;
  }

  btn.style.display = "block";
  btn.textContent = showAllDistricts
    ? `Show top ${MAX_VISIBLE_DISTRICTS}`
    : `View all ${total}`;

  btn.onclick = () => {
    showAllDistricts = !showAllDistricts;
    renderDistrictList(pm10Data.districts, document.getElementById("search").value);
  };
}
