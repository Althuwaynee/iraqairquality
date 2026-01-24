// Main JavaScript for Iraq Air Quality Website

document.addEventListener('DOMContentLoaded', function() {
    // ===== Province Circle Selection =====
    const provinceCircles = document.querySelectorAll('.province-circle');
    const gaugeValue = document.querySelector('.gauge-value');
    const gaugeLabel = document.querySelector('.gauge-label');
    const aqiLevel = document.querySelector('.aqi-level');
    const aqiIndex = document.querySelector('.aqi-index');
    
    // Province data (mock data for demo)
    const provinceData = {
        baghdad: {
            name: "بغداد",
            value: 142,
            aqi: 185,
            level: "very-unhealthy",
            levelText: "غير صحي جداً",
            trend: "+5%",
            aboveSafe: "3.2x"
        },
        basra: {
            name: "البصرة",
            value: 125,
            aqi: 165,
            level: "unhealthy",
            levelText: "غير صحي",
            trend: "+3%",
            aboveSafe: "2.8x"
        },
        erbil: {
            name: "أربيل",
            value: 85,
            aqi: 112,
            level: "moderate",
            levelText: "متوسط",
            trend: "0%",
            aboveSafe: "1.7x"
        }
    };
    
    // Color mapping for AQI levels
    const levelColors = {
        "very-unhealthy": "#8f3f97",
        "unhealthy": "#ff0000",
        "moderate": "#ffff00"
    };
    
    provinceCircles.forEach(circle => {
        circle.addEventListener('click', function() {
            const province = this.getAttribute('data-province');
            const data = provinceData[province];
            
            // Update active state
            provinceCircles.forEach(c => c.classList.remove('active'));
            this.classList.add('active');
            
            // Update gauge
            gaugeValue.textContent = data.value;
            gaugeLabel.textContent = data.name;
            
            // Update AQI status
            aqiLevel.textContent = data.levelText;
            aqiLevel.className = `aqi-level ${data.level}`;
            aqiLevel.style.backgroundColor = levelColors[data.level];
            aqiIndex.textContent = `مؤشر جودة الهواء: ${data.aqi}`;
            
            // Update gauge marker position (simplified - based on value)
            const gaugeMarker = document.querySelector('.gauge-marker');
            const rotation = (data.value / 200) * 180; // Scale to 0-180 degrees
            gaugeMarker.style.transform = `rotate(${rotation}deg)`;
            
            // Update quick stats
            document.querySelector('.stat-content .stat-value:nth-child(1)').textContent = data.trend;
            document.querySelector('.stat-content .stat-value:nth-child(2)').textContent = data.aboveSafe;
        });
    });
    
    // ===== Time Period Tabs =====
    const timeButtons = document.querySelectorAll('.time-buttons .btn');
    const chartTitle = document.querySelector('.chart-title h4');
    const monthlyBars = document.querySelector('.monthly-bars');
    
    timeButtons.forEach(button => {
        button.addEventListener('click', function() {
            // Remove active class from all buttons
            timeButtons.forEach(btn => btn.classList.remove('active'));
            // Add active class to clicked button
            this.classList.add('active');
            
            const period = this.getAttribute('data-period');
            updateChartForPeriod(period);
        });
    });
    
    function updateChartForPeriod(period) {
        const periodTitles = {
            hourly: "تركيز الغبار الساعي - بغداد (اليوم)",
            daily: "تركيز الغبار اليومي - بغداد (الشهر)",
            weekly: "تركيز الغبار الأسبوعي - بغداد (السنة)",
            monthly: "تركيز الغبار الشهري - بغداد (2024)",
            yearly: "تركيز الغبار السنوي - بغداد (5 سنوات)",
            custom: "تركيز الغبار المخصص - بغداد"
        };
        
        // Update chart title
        chartTitle.textContent = periodTitles[period];
        
        // Simulate data loading
        monthlyBars.style.opacity = '0.5';
        setTimeout(() => {
            monthlyBars.style.opacity = '1';
            // Here you would load real data based on period
        }, 300);
    }
    
    // ===== Chart Navigation =====
    const prevBtn = document.querySelector('.prev-btn');
    const nextBtn = document.querySelector('.next-btn');
    
    prevBtn.addEventListener('click', function() {
        navigateChart(-1);
    });
    
    nextBtn.addEventListener('click', function() {
        navigateChart(1);
    });
    
    let currentChartIndex = 0;
    const chartDataSets = ['baghdad', 'basra', 'erbil', 'kirkuk', 'mosul'];
    
    function navigateChart(direction) {
        currentChartIndex += direction;
        
        // Loop around
        if (currentChartIndex < 0) {
            currentChartIndex = chartDataSets.length - 1;
        } else if (currentChartIndex >= chartDataSets.length) {
            currentChartIndex = 0;
        }
        
        const province = chartDataSets[currentChartIndex];
        const provinceNames = {
            baghdad: "بغداد",
            basra: "البصرة", 
            erbil: "أربيل",
            kirkuk: "كركوك",
            mosul: "الموصل"
        };
        
        // Update chart title
        const activePeriodBtn = document.querySelector('.time-buttons .btn.active');
        const period = activePeriodBtn ? activePeriodBtn.getAttribute('data-period') : 'monthly';
        
        chartTitle.textContent = `تركيز الغبار ${getPeriodText(period)} - ${provinceNames[province]}`;
        
        // Update province selector
        document.querySelector('#provinceSelect').value = province;
        
        // Here you would load new data for the selected province
        // For now, we'll just simulate loading
        monthlyBars.style.opacity = '0.5';
        setTimeout(() => {
            monthlyBars.style.opacity = '1';
        }, 300);
    }
    
    function getPeriodText(period) {
        const periodTexts = {
            hourly: "الساعي",
            daily: "اليومي",
            weekly: "الأسبوعي",
            monthly: "الشهري",
            yearly: "السنوي",
            custom: "المخصص"
        };
        return periodTexts[period] || "الشهري";
    }
    
    // ===== Province Selector =====
    const provinceSelect = document.querySelector('#provinceSelect');
    
    provinceSelect.addEventListener('change', function() {
        const selectedProvince = this.value;
        if (selectedProvince === 'all') {
            // Show all provinces comparison
            chartTitle.textContent = "مقارنة تركيز الغبار بين المحافظات";
            // Here you would load comparison data
        } else {
            // Show single province
            const provinceNames = {
                baghdad: "بغداد",
                basra: "البصرة",
                mosul: "الموصل",
                erbil: "أربيل",
                sulaymaniyah: "السليمانية",
                kirkuk: "كركوك"
            };
            
            const activePeriodBtn = document.querySelector('.time-buttons .btn.active');
            const period = activePeriodBtn ? activePeriodBtn.getAttribute('data-period') : 'monthly';
            
            chartTitle.textContent = `تركيز الغبار ${getPeriodText(period)} - ${provinceNames[selectedProvince]}`;
            
            // Update chart data for selected province
            monthlyBars.style.opacity = '0.5';
            setTimeout(() => {
                monthlyBars.style.opacity = '1';
                // Here you would load real data
            }, 300);
        }
    });
    
    // ===== Initialize Gauge Marker =====
    const initialRotation = (provinceData.baghdad.value / 200) * 180;
    document.querySelector('.gauge-marker').style.transform = `rotate(${initialRotation}deg)`;
});
