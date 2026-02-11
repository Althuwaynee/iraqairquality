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
  if (!utcString) return "N/A";
  
  try {
    const d = new Date(utcString);
    d.setHours(d.getHours() + 3); // Iraq UTC+3

    return d.toLocaleString("en-GB", {
      timeZone: "UTC",
      ...options
    });
  } catch (e) {
    return "N/A";
  }
}

function isNightTimeIraq(utcTimestamp) {
  if (!utcTimestamp) return false;
  
  try {
    const d = new Date(utcTimestamp);
    d.setHours(d.getHours() + 3);
    const h = d.getHours();
    return (h >= 19 || h < 6);
  } catch (e) {
    return false;
  }
}

/* ---------- Search ---------- */
function setupSearch() {
  const searchInput = document.getElementById("search");
  if (!searchInput) return;

  searchInput.addEventListener("input", () => {
    if (!pm10Data || !pm10Data.districts) return;

    renderDistrictList(
      pm10Data.districts,
      searchInput.value
    );
  });
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
  pm10 = pm10 || 0;
  aqi = aqi || 0;
  return (pm10 >= 300 || aqi >= 200);
}

/* ---------- Health icons logic ---------- */
function getHealthIcons({ aqi, pm10, timestamp }) {
  // Handle null/undefined values
  aqi = aqi || 0;
  pm10 = pm10 || 0;
  
  const icons = [];
  const night = timestamp ? isNightTimeIraq(timestamp) : false;
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
    console.log("Loading PM10 data from: data/pm10_alerts.json");
    const res = await fetch("data/pm10_alerts.json");
    
    if (!res.ok) {
      throw new Error(`HTTP ${res.status}: ${res.statusText}`);
    }
    
    pm10Data = await res.json();
    
    if (!pm10Data || !pm10Data.districts || !Array.isArray(pm10Data.districts)) {
      throw new Error("Invalid JSON structure: missing 'districts' array");
    }
    
    console.log(`Loaded ${pm10Data.districts.length} districts`);
    
    // Filter out invalid districts
    const validDistricts = pm10Data.districts.filter(d => {
      const hasCoords = d.latitude && d.longitude;
      const hasAQI = d.aqi && typeof d.aqi.value === 'number';
      const hasPM10 = d.pm10 && typeof d.pm10.now === 'number';
      
      if (!hasCoords || !hasAQI || !hasPM10) {
        console.warn(`Skipping district ${d.district_name || 'Unknown'}: missing required data`);
        return false;
      }
      return true;
    });
    
    console.log(`${validDistricts.length} valid districts after filtering`);
    
    if (validDistricts.length === 0) {
      throw new Error("No valid district data found. Check JSON structure.");
    }
    
    if (pm10Data.metadata) {
      updateHeaderInfo(pm10Data.metadata);
    }
    
    validDistricts.sort((a, b) => (b.aqi.value || 0) - (a.aqi.value || 0));
    drawDistrictMarkers(validDistricts);
    renderDistrictList(validDistricts);
    autoLocateUser(validDistricts);
    
  } catch (err) {
    console.error("Failed to load PM10 data:", err);
    
    // Show user-friendly error
    const list = document.getElementById("district-list");
    if (list) {
      list.innerHTML = `
        <li style="color: #dc2626; text-align: center; padding: 2rem;">
          <strong>⚠️ Data Loading Error</strong><br>
          <small>${err.message}</small><br>
          <small>Check console for details</small>
        </li>
      `;
    }
    
    // Still show map with message
    if (map) {
      L.marker([33.2, 44.3]).addTo(map)
        .bindPopup(`
          <div style="padding: 10px;">
            <strong>⚠️ Data Loading Failed</strong><br>
            <small>${err.message}</small><br>
            <small>Check that <code>data/pm10_alerts.json</code> exists and has valid data.</small>
          </div>
        `)
        .openPopup();
    }
  }
}

/* ---------- Header ---------- */
function updateHeaderInfo(metadata) {
  const refTime = document.getElementById("ref-time");
  const note = document.getElementById("data-note");

  if (refTime && metadata.reference_time) {
    try {
      refTime.textContent = `Reference time: ${new Date(metadata.reference_time).toUTCString()}`;
    } catch (e) {
      refTime.textContent = "Reference time: N/A";
    }
  }

  if (note && metadata.note) {
    note.textContent = metadata.note;
  }
}

/* ---------- Colors ---------- */
function getAlertColor(level) {
  if (!level) return "#334155";
  
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
    if (!d.latitude || !d.longitude) {
      console.warn(`Skipping ${d.district_name}: missing coordinates`);
      return;
    }

    const marker = L.circleMarker(
      [d.latitude, d.longitude],
      {
        radius: 9,
        fillColor: getAlertColor(d.alert?.level),
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
  // Safely get values with defaults
  const pm10Now = d.pm10?.now ?? 0;
  const mean24h = d.pm10?.mean_24h ?? 0;
  const pm10Timestamp = d.pm10?.timestamp;
  const aqiValue = d.aqi?.value ?? 0;
  const aqiLevel = d.aqi?.level ?? 'unknown';
  
  const measurementTime = pm10Timestamp 
    ? formatIraqTime(pm10Timestamp, {
        weekday: "short",
        hour: "2-digit",
        minute: "2-digit",
        hour12: false
      })
    : "N/A";

  const forecasts = [
    d.pm10_forecast_3h,
    d.pm10_forecast_6h,
    d.pm10_forecast_9h,
    d.pm10_forecast_12h,
    d.pm10_forecast_15h,
    d.pm10_forecast_18h,
    d.pm10_forecast_21h,
    d.pm10_forecast_24h
  ].filter(f => f && typeof f.value === 'number' && typeof f.aqi === 'number');

  const forecastHTML = forecasts.map(f => {
    const day = f.timestamp ? formatIraqTime(f.timestamp, { weekday: "short" }) : "N/A";
    const time = f.timestamp ? formatIraqTime(f.timestamp, {
      hour: "2-digit",
      minute: "2-digit",
      hour12: false
    }) : "N/A";

    const icons = getHealthIcons({
      aqi: f.aqi || 0,
      pm10: f.value || 0,
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
             style="background:${getAlertColor(f.aqi_level || 'unknown')}">
          ${f.aqi || 0}
        </div>
        <div class="forecast-pm">${(f.value || 0).toFixed(0)}</div>
        <div class="forecast-unit">µg/m³</div>
        <div class="aqi-icons">${iconsHTML}</div>
      </div>
    `;
  }).join("");

  return `
    <div>
      <strong>${d.district_name || "Unknown District"}</strong><br>
      <small>${d.province_name || "Unknown Province"}</small>
      <hr>
      <div style="font-size:0.7rem;color:#475569;">
        Measurement time (Iraq local): ${measurementTime}
      </div>
      Dust: <b>${pm10Now.toFixed(1)}</b> µg/m³<br>
      AQI: <b>${aqiValue}</b> (${aqiLevel})<br>
      Dust (24h mean): ${mean24h.toFixed(1)} µg/m³
      <hr>
      ${forecasts.length > 0 ? `
        <strong>التنبؤ (next 24h)</strong>
        <div class="forecast-row">${forecastHTML}</div>
      ` : '<em>No forecast data available</em>'}
    </div>
  `;
}

/* ---------- Sidebar list ---------- */
function renderDistrictList(districts, filter = "") {
  const list = document.getElementById("district-list");
  if (!list) return; // Only run if element exists
  
  list.innerHTML = "";

  const filtered = districts.filter(d =>
    d.district_name?.toLowerCase().includes(filter.toLowerCase()) ||
    d.province_name?.toLowerCase().includes(filter.toLowerCase())
  );

  const visible = showAllDistricts
    ? filtered
    : filtered.slice(0, MAX_VISIBLE_DISTRICTS);

  visible.forEach(d => {
    const li = document.createElement("li");
    li.style.borderLeftColor = getAlertColor(d.alert?.level);

    li.innerHTML = `
      <div>
        <strong>${d.district_name || "Unknown"}</strong><br>
        <small>${d.province_name || "Unknown"}</small>
      </div>
      <div class="value">
        <span>${d.aqi?.value ?? 0}</span>
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

  logo.style.cursor = "pointer";

  logo.addEventListener("click", e => {
    e.preventDefault();

    const isIndex =
      window.location.pathname === "/" ||
      window.location.pathname.endsWith("index.html");

    if (isIndex) {
      const map = document.getElementById("map");
      if (map) map.scrollIntoView({ behavior: "smooth" });
    } else {
      window.location.href = "index.html";
    }
  });
}


/* ---------- Mobile Menu ---------- */
function setupMobileMenu() {
  const menuBtn = document.getElementById("menu-toggle");
  const mobileMenu = document.getElementById("mobile-menu");
  const overlay = document.getElementById("menu-overlay");
  
  // Early exit if no mobile menu elements exist
  if (!menuBtn) {
    return; // No mobile menu on this page
  }
  
  // If mobile menu exists but overlay doesn't, create it
  if (!overlay && mobileMenu) {
    const newOverlay = document.createElement("div");
    newOverlay.id = "menu-overlay";
    newOverlay.style.cssText = `
      position: fixed;
      top: 0;
      left: 0;
      width: 100%;
      height: 100%;
      background: rgba(0,0,0,0.5);
      z-index: 9998;
      display: none;
    `;
    document.body.appendChild(newOverlay);
    overlay = newOverlay;
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
  if (overlay) {
    overlay.addEventListener("click", function() {
      mobileMenu.classList.remove("open");
      overlay.classList.remove("show");
      document.body.style.overflow = "";
      menuBtn.innerHTML = "☰";
      menuBtn.setAttribute("aria-expanded", "false");
    });
  }
  
  // Close menu when clicking any link inside
  if (mobileMenu) {
    const menuLinks = mobileMenu.querySelectorAll("a");
    menuLinks.forEach(link => {
      link.addEventListener("click", function() {
        // Small delay to allow navigation
        setTimeout(() => {
          mobileMenu.classList.remove("open");
          if (overlay) overlay.classList.remove("show");
          document.body.style.overflow = "";
          menuBtn.innerHTML = "☰";
          menuBtn.setAttribute("aria-expanded", "false");
        }, 100);
      });
    });
  }
  
  // Close menu on window resize (if resizing to desktop)
  window.addEventListener("resize", function() {
    if (window.innerWidth > 768 && mobileMenu && mobileMenu.classList.contains("open")) {
      mobileMenu.classList.remove("open");
      if (overlay) overlay.classList.remove("show");
      document.body.style.overflow = "";
      menuBtn.innerHTML = "☰";
      menuBtn.setAttribute("aria-expanded", "false");
    }
  });
}

const backToTopBtn = document.getElementById('back-to-top');

if (backToTopBtn) {
  window.addEventListener('scroll', () => {
    if (window.scrollY > 300) {
      backToTopBtn.style.display = 'block';
    } else {
      backToTopBtn.style.display = 'none';
    }
  });

  backToTopBtn.addEventListener('click', () => {
    window.scrollTo({ top: 0, behavior: 'smooth' });
  });
}

// Define the bounds for all of Iraq (approximate coordinates)
const IRAQ_BOUNDS = [
  [29.0, 38.5], // Southwest corner (lat, lng)
  [37.5, 48.5]  // Northeast corner (lat, lng)
];

// Function to fit map to Iraq bounds
function fitMapToIraq() {
  if (map) { // Make sure map exists
    map.fitBounds(IRAQ_BOUNDS, {
      padding: [50, 50], // Optional padding
      animate: true,
      duration: 1 // Animation duration in seconds
    });
  }
}

// Event listener for logo clicks
document.addEventListener('DOMContentLoaded', function() {
  // Home page logo
  const logoHome = document.getElementById('logo-home');
  const footerLogo = document.getElementById('footer-logo');
  
  if (logoHome) {
    logoHome.addEventListener('click', function(e) {
      e.preventDefault(); // Prevent default link behavior
      fitMapToIraq();
      
      // If on a different page, navigate to index.html first
      if (!window.location.pathname.includes('index.html') && 
          !window.location.pathname.endsWith('/')) {
        setTimeout(() => {
          window.location.href = 'index.html';
        }, 300); // Small delay to show animation first
      }
    });
  }
  
  if (footerLogo) {
    footerLogo.addEventListener('click', function(e) {
      e.preventDefault();
      fitMapToIraq();
      
      if (!window.location.pathname.includes('index.html') && 
          !window.location.pathname.endsWith('/')) {
        setTimeout(() => {
          window.location.href = 'index.html';
        }, 300);
      }
    });
  }
  
  // Optional: Add keyboard shortcut (Home key) to reset view
  document.addEventListener('keydown', function(e) {
    if (e.key === 'Home' && map) {
      e.preventDefault();
      fitMapToIraq();
    }
  });
});


//* ---------- Mobile double-click zoom (BOTH in AND out) ---------- */
function enableMobileDoubleClickZoom() {
  if (!map) return;
  
  const isMobile = window.innerWidth <= 768 || 'ontouchstart' in window;
  
  if (isMobile) {
    // Disable Leaflet's default doubleClickZoom
    map.doubleClickZoom.disable();
    
    // Remove any existing double-click handlers to avoid duplicates
    map.off('dblclick');
    
    // Add custom double-click handler
    map.on('dblclick', function(e) {
      // Get current zoom level
      const currentZoom = map.getZoom();
      
      // Check if it's a double-tap on touch device
      const isTouch = e.originalEvent && e.originalEvent.pointerType === 'touch';
      
      if (isTouch || isMobile) {
        e.originalEvent.preventDefault();
          /* 
        // Determine zoom direction based on tap position
        // Option A: Tap on upper half = zoom in, lower half = zoom out
        const mapContainer = map.getContainer();
        const containerHeight = mapContainer.clientHeight;
        const clickY = e.containerPoint.y;
        
        if (clickY < containerHeight / 2) {
          // Tap on top half - ZOOM IN
          map.setView(e.latlng, currentZoom + 1, { animate: true });
        } else {
          // Tap on bottom half - ZOOM OUT
          map.setView(e.latlng, currentZoom - 1, { animate: true });
        }
        
         */ 
        // Option B: Single tap = zoom in, Double tap = zoom out
        // Uncomment this and comment Option A to use this instead
        const now = Date.now();
        if (!map._lastDoubleTap) {
          map._lastDoubleTap = now;
          // Zoom in
          map.setView(e.latlng, currentZoom + 1, { animate: true });
        } else {
          const timeSince = now - map._lastDoubleTap;
          map._lastDoubleTap = now;
          
          if (timeSince < 500) {
            // Zoom out on rapid double tap
            map.setView(e.latlng, currentZoom - 1, { animate: true });
          } else {
            // Zoom in on slower double tap
            map.setView(e.latlng, currentZoom + 1, { animate: true });
          }
        }
       
      }
    });
    
    // Optional: Visual hint for mobile users
    showMobileZoomHint();
    
  } else {
    // On desktop, restore default double-click behavior
    map.doubleClickZoom.enable();
    map.off('dblclick');
  }
}

/* ---------- Show mobile hint (optional) ---------- */
function showMobileZoomHint() {
  // Only show once per session
  if (sessionStorage.getItem('mobileZoomHintShown')) return;
  
  setTimeout(() => {
    map.openPopup(`
      <div style="text-align: center; padding: 10px; direction: rtl;">
        <strong>👆 تكبير/تصغير</strong><br>
        <small style="color: #666;">
          اضغط مرتين على:<br>
          • النصف العلوي = تكبير<br>
          • النصف السفلي = تصغير
        </small>
      </div>
    `, [33.2, 44.3], { 
      className: 'mobile-zoom-hint',
      closeButton: true,
      closeOnClick: true
    });
    
    sessionStorage.setItem('mobileZoomHintShown', 'true');
    
    // Auto close after 5 seconds
    setTimeout(() => map.closePopup(), 5000);
  }, 1500);
}

/* ---------- App Bootstrap ---------- */
document.addEventListener("DOMContentLoaded", () => {
  initMap();
  loadPM10Alerts();
  setupMobileMenu();
  setupLogoNavigation();
  setupSearch();
});