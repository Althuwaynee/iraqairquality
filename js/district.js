const params = new URLSearchParams(window.location.search);
const districtCode = params.get('district');

fetch('./data/pm10_alerts.json')
  .then(r => r.json())
  .then(data => {
    const d = data.districts.find(x =>
      x.district_code === districtCode ||
      x.district_name === districtCode
    );

    if (!d) return;

    document.getElementById('district-name').textContent = d.district_name;
    document.getElementById('district-subtitle').textContent = d.province_name;
    document.getElementById('aqi-value').textContent = d.aqi;
    document.getElementById('health-text').textContent = getHealthAdvice(d.aqi);
  });

function getHealthAdvice(aqi) {
  if (aqi <= 50) return 'Air quality is good. Enjoy outdoor activities.';
  if (aqi <= 100) return 'Sensitive groups should limit prolonged exertion.';
  if (aqi <= 150) return 'Reduce outdoor activities.';
  if (aqi <= 300) return 'Stay indoors if possible.';
  return 'Avoid all outdoor activities.';
}
