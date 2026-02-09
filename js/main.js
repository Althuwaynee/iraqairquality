/* ===============================
   Iraq Air Quality – Main JS
   =============================== */

let map;
let pm10Data = null;
let districtLayer = L.layerGroup();
let showAllDistricts = false;

const MAX_VISIBLE_DISTRICTS = 15;

/* ---------- Time helpers ---------- */
function formatIraqTime(utcString, options = {}) {
  const d = new Date(utcString);
  d.setHours(d.getHours() + 3); // Iraq UTC+3

  return d.toLocaleString("en-GB", {
    timeZone: "UTC",
    ...options
  });
}

function isNightTimeIraq(utcTimestamp) {
  const d = new Date(utcTimestamp);
  d.setHours(d.getHours() + 3);
  const h = d.getHours();
  return (h >= 19 || h < 6);
}

/* ---------- Geo helpers ---------- */
function haversineDistance(lat1, lon1, lat2, lon2) {
  const R = 6371;
  const toRad = d => d * Math.PI / 180;

  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);

  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) *
    Math.cos(toRad(lat2)) *
    Math.sin(dLon / 2) ** 2;

  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function findNearestDistrict(lat, lon, districts) {
  let nearest = null;
  let minDist = Infinity;

  districts.forEach(d => {
    if (!d.latitude || !d.longitude) return;
    const dist = haversineDistance(lat, lon, d.latitude, d.longitude);
    if (dist < minDist) {
      minDist = dist;
      nearest = d;
    }
  });

  return nearest;
}

/* ---------- Dust storm logic ---------- */
function isDustStormCondition(pm10, aqi) {
  return (pm10 >= 300 || aqi >= 200);
}

/* ---------- Health icons logic ---------- */
function getHealthIcons({ aqi, pm10, timestamp }) {
  const icons = [];
  const night = isNightTimeIraq(timestamp);
  const dust = isDustStormCondition(pm10, aqi);

  if (dust) {
    icons.push({
      icon: "🌪️",
      label: "Possible dust storm conditions – severe dust exposure"
    });
  }

  if (aqi <= 50) {
    icons.push({
      icon: "🚴",
      label: night
        ? "Night-time: light outdoor activity is acceptable"
        : "Safe for outdoor activity"
    });
  }
  else if (aqi <= 100) {
    icons.push({ icon: "🚴", label: "Outdoor activity generally safe" });
    icons.push({ icon: "👶", label: "Sensitive people should be cautious" });
  }
  else if (aqi <= 150) {
    icons.push({ icon: "😷", label: "Wear a mask outdoors" });
    icons.push({ icon: "👶", label: "Sensitive groups should limit exposure" });
    if (night) {
      icons.push({
        icon: "🏠",
        label: "Night-time: keep windows closed while sleeping"
      });
    }
  }
  else if (aqi <= 200) {
    icons.push({ icon: "😷", label: "Wear a mask outdoors" });
    icons.push({ icon: "🏠", label: "Stay indoors if possible" });
    icons.push({ icon: "🚫", label: "Avoid outdoor activity" });
  }
  else {
    icons.push({ icon: "😷", label: "Wear a mask outdoors" });
    icons.push({ icon: "🏠", label: "Stay indoors" });
    icons.push({ icon: "👶", label: "Sensitive groups at high risk" });
    icons.push({ icon: "🚫", label: "Avoid outdoor activity" });
  }

  return icons;
}

/* ---------- Map ---------- */
function initMap() {
  // Only initialize map if #map element exists
  const mapElement = document.getElementById("map");
  if (!mapElement) return;
  
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

    pm10Data.districts.sort((a, b) => b.aqi.value - a.aqi.value);

    drawDistrictMarkers(pm10Data.districts);
    renderDistrictList(pm10Data.districts);
    autoLocateUser(pm10Data.districts);

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
    case "unhealthy_for_sensitive_groups": return "#b675bd";
    case "unhealthy": return "#ee60db";
    case "very_unhealthy": return "#ed1515";
    case "hazardous": return "#7c2d12";
    default: return "#334155";
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
        radius: 9,
        fillColor: getAlertColor(d.alert.level),
        fillOpacity: 0.85,
        color: "#0f172a",
        weight: 1
      }
    );

    marker.bindPopup(buildDistrictPopup(d));
    marker._district_id = d.district_id;

    districtLayer.addLayer(marker);
  });
}

/* ---------- Popup ---------- */
function buildDistrictPopup(d) {

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

    const day = formatIraqTime(f.timestamp, { weekday: "short" });
    const time = formatIraqTime(f.timestamp, {
      hour: "2-digit",
      minute: "2-digit",
      hour12: false
    });

    const icons = getHealthIcons({
      aqi: f.aqi,
      pm10: f.value,
      timestamp: f.timestamp
    });

    const iconsHTML = icons.map(i =>
      `<span class="aqi-icon" data-tip="${i.label}">${i.icon}</span>`
    ).join("");


    return `
      <div class="forecast-item">
        <div class="forecast-day">${day}</div>
        <div class="forecast-time">${time}</div>

        <div class="forecast-dot"
             style="background:${getAlertColor(f.aqi_level)}">
          ${f.aqi}
        </div>

        <div class="forecast-pm">${f.value.toFixed(0)}</div>
        <div class="forecast-unit">µg/m³</div>

        <div class="aqi-icons">${iconsHTML}</div>
      </div>
    `;
  }).join("");

  const measurementTime = formatIraqTime(d.pm10.timestamp, {
    weekday: "short",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false
  });

  return `
    <div>
      <strong>${d.district_name}</strong><br>
      <small>${d.province_name}</small>

      <hr>

      <div style="font-size:0.7rem;color:#475569;">
        Measurement time (Iraq local): ${measurementTime}
      </div>

      Dust: <b>${d.pm10.now.toFixed(1)}</b> µg/m³<br>
      AQI: <b>${d.aqi.value}</b> (${d.aqi.level})<br>
      Dust (24h mean): ${d.pm10.mean_24h.toFixed(1)} µg/m³

      <hr>

      <strong>Forecast (next 24h)</strong>
      <div class="forecast-row">${forecastHTML}</div>
    </div>
  `;
}

/* ---------- Sidebar list ---------- */
function renderDistrictList(districts, filter = "") {
  const list = document.getElementById("district-list");
  if (!list) return; // Only run if element exists
  
  list.innerHTML = "";

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

    li.onclick = () => zoomToDistrict(d);
    list.appendChild(li);
  });

  updateViewAllButton(filtered.length);
}

/* ---------- Zoom + popup ---------- */
function zoomToDistrict(d) {
  if (!map) return; // Only if map exists
  
  map.setView([d.latitude, d.longitude], 11);
  districtLayer.eachLayer(layer => {
    if (layer._district_id === d.district_id) {
      layer.openPopup();
    }
  });
}

/* ---------- Auto locate ---------- */
function autoLocateUser(districts) {
  if (!navigator.geolocation) return;

  navigator.geolocation.getCurrentPosition(
    pos => {
      const nearest = findNearestDistrict(
        pos.coords.latitude,
        pos.coords.longitude,
        districts
      );

      if (!nearest) return;
      zoomToDistrict(nearest);
    },
    () => {},
    { timeout: 8000 }
  );
}

/* ---------- View all ---------- */
function updateViewAllButton(total) {
  const btn = document.querySelector(".view-all");
  if (!btn) return;

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
    renderDistrictList(
      pm10Data.districts,
      document.getElementById("search")?.value || ""
    );
  };
}

/* ---------- Mobile tooltip support for AQI icons ---------- */
document.addEventListener("click", (e) => {
  const icon = e.target.closest(".aqi-icon");

  // Close any open tooltips
  document.querySelectorAll(".aqi-icon.active").forEach(el => {
    if (el !== icon) el.classList.remove("active");
  });

  if (!icon) return;

  // Toggle tooltip on tap
  icon.classList.toggle("active");
});

/* ---------- Logo Navigation ---------- */
function setupLogoNavigation() {
  const logo = document.querySelector(".logo");
  if (!logo) return;
  
  logo.style.cursor = 'pointer';
  
  logo.addEventListener("click", function(e) {
    e.preventDefault();
    
    // Get current page
    const currentPage = window.location.pathname.split('/').pop();
    
    // Check if we're on the main page (index.html or root)
    const isMainPage = currentPage === 'index.html' || currentPage === '' || currentPage.endsWith('/');
    
    if (isMainPage) {
      // If on main page, scroll to map
      const mapSection = document.getElementById('map');
      if (mapSection) {
        mapSection.scrollIntoView({ behavior: 'smooth' });
      }
    } else {
      // If on another page, navigate to main page
      window.location.href = 'index.html';
    }
  });
}

/* ---------- Mobile Menu ---------- */
function setupMobileMenu() {
  const menuBtn = document.getElementById("menu-toggle");
  const mobileMenu = document.getElementById("mobile-menu");
  const overlay = document.getElementById("menu-overlay");
  
  if (!menuBtn || !mobileMenu || !overlay) {
    console.log("Mobile menu elements not found");
    return;
  }
  
  // Ensure burger button is visible on mobile
  if (window.innerWidth <= 768) {
    menuBtn.style.display = "block";
  }
  
  // Toggle menu function
  function toggleMenu() {
    const isOpen = mobileMenu.classList.contains("open");
    
    if (isOpen) {
      // Close menu
      mobileMenu.classList.remove("open");
      overlay.classList.remove("show");
      document.body.style.overflow = "";
      menuBtn.innerHTML = "☰";
      menuBtn.setAttribute("aria-expanded", "false");
    } else {
      // Open menu
      mobileMenu.classList.add("open");
      overlay.classList.add("show");
      document.body.style.overflow = "hidden";
      menuBtn.innerHTML = "✕";
      menuBtn.setAttribute("aria-expanded", "true");
    }
  }
  
  // Attach event listeners
  menuBtn.addEventListener("click", toggleMenu);
  
  // Close menu when clicking overlay
  overlay.addEventListener("click", function() {
    mobileMenu.classList.remove("open");
    overlay.classList.remove("show");
    document.body.style.overflow = "";
    menuBtn.innerHTML = "☰";
    menuBtn.setAttribute("aria-expanded", "false");
  });
  
  // Close menu when clicking any link inside
  const menuLinks = mobileMenu.querySelectorAll("a");
  menuLinks.forEach(link => {
    link.addEventListener("click", function() {
      // Small delay to allow navigation
      setTimeout(() => {
        mobileMenu.classList.remove("open");
        overlay.classList.remove("show");
        document.body.style.overflow = "";
        menuBtn.innerHTML = "☰";
        menuBtn.setAttribute("aria-expanded", "false");
      }, 100);
    });
  });
  
  // Close menu on window resize (if resizing to desktop)
  window.addEventListener("resize", function() {
    if (window.innerWidth > 768) {
      mobileMenu.classList.remove("open");
      overlay.classList.remove("show");
      document.body.style.overflow = "";
      menuBtn.innerHTML = "☰";
      menuBtn.setAttribute("aria-expanded", "false");
    }
  });
}


/* ---------- App Bootstrap ---------- */
document.addEventListener("DOMContentLoaded", () => {
  initMap();
  loadPM10Alerts();
  setupMobileMenu();
  setupLogoNavigation();
});
