// Main JavaScript for Iraq Air Quality Website
import { translations } from './translations.js';

document.addEventListener('DOMContentLoaded', function() {
    // Initialize all functionality
    initApp();
});

function initApp() {
    // Language state
    let currentLang = 'ar';
    
    // Initialize AOS
    if (typeof AOS !== 'undefined') {
        AOS.init({
            duration: 800,
            offset: 100,
            once: true
        });
    }
    
    // Initialize components
    initSmoothScrolling();
    initNavbarScroll();
    initLanguageToggle();
    initMapControls();
    initProvinceGauge();
    initTimePeriodTabs();
    initChartNavigation();
    initProvinceSelector();
    initFAQFunctionality();
}

// ===== SMOOTH SCROLLING =====
function initSmoothScrolling() {
    document.querySelectorAll('a[href^="#"]').forEach(anchor => {
        anchor.addEventListener('click', function(e) {
            e.preventDefault();
            const targetId = this.getAttribute('href');
            if (targetId === '#') return;
            
            const target = document.querySelector(targetId);
            if (target) {
                window.scrollTo({
                    top: target.offsetTop - 80,
                    behavior: 'smooth'
                });
            }
        });
    });
}

// ===== NAVBAR SCROLL EFFECT =====
function initNavbarScroll() {
    window.addEventListener('scroll', function() {
        const navbar = document.getElementById('mainNav');
        if (navbar) {
            if (window.scrollY > 100) {
                navbar.style.backgroundColor = 'rgba(11, 60, 93, 0.95)';
                navbar.style.boxShadow = '0 2px 10px rgba(0,0,0,0.1)';
            } else {
                navbar.style.backgroundColor = 'transparent';
                navbar.style.boxShadow = 'none';
            }
        }
    });
}

// ===== LANGUAGE TOGGLE =====
function initLanguageToggle() {
    const langToggle = document.getElementById('langToggle');
    let currentLang = 'ar';
    
    if (langToggle) {
        langToggle.addEventListener('click', function() {
            currentLang = currentLang === 'ar' ? 'en' : 'ar';
            this.textContent = currentLang === 'ar' ? 'English' : 'العربية';
            
            // Update direction
            document.documentElement.dir = currentLang === 'ar' ? 'rtl' : 'ltr';
            document.documentElement.lang = currentLang;
            
            // Update all translations
            updateTranslations(currentLang);
        });
    }
}

function updateTranslations(lang) {
    // Update navigation
    const navLinks = document.querySelectorAll('.nav-link');
    const navTexts = ['nav_home', 'nav_map', 'nav_insights', 'nav_data', 'nav_methodology', 'nav_about'];
    
    navLinks.forEach((link, index) => {
        if (translations[lang][navTexts[index]]) {
            link.textContent = translations[lang][navTexts[index]];
        }
    });
    
    // Update section titles
    document.querySelectorAll('[data-translate]').forEach(element => {
        const key = element.getAttribute('data-translate');
        if (translations[lang][key]) {
            element.textContent = translations[lang][key];
        }
    });
    
    // Update map layer buttons
    const layerButtons = document.querySelectorAll('.map-controls .btn[data-layer]');
    layerButtons.forEach(btn => {
        const layer = btn.getAttribute('data-layer');
        const key = `layer_${layer}`;
        if (translations[lang][key]) {
            btn.textContent = translations[lang][key];
        }
    });
    
    // Update scale label
    const scaleLabel = document.querySelector('.scale-label');
    if (scaleLabel && translations[lang].scale_label) {
        scaleLabel.textContent = translations[lang].scale_label;
    }
}

// ===== MAP CONTROLS =====
function initMapControls() {
    // Map layer switching
    const mapButtons = document.querySelectorAll('.map-controls .btn');
    
    if (mapButtons.length > 0) {
        mapButtons.forEach(btn => {
            btn.addEventListener('click', function() {
                mapButtons.forEach(b => b.classList.remove('active'));
                this.classList.add('active');
                
                // Update map overlay text
                const layer = this.getAttribute('data-layer');
                updateMapOverlayText(layer);
            });
        });
    }
    
    // Interpolation toggle
    const interpolationToggle = document.getElementById('interpolationToggle');
    if (interpolationToggle) {
        interpolationToggle.addEventListener('change', function() {
            updateInterpolationText(this.checked);
        });
    }
}

function updateMapOverlayText(layer) {
    const overlayText = document.querySelector('.map-overlay h6');
    if (overlayText) {
        const currentLang = document.documentElement.lang || 'ar';
        const layerNames = {
            ar: {
                dust: 'خريطة تركيز الغبار التفاعلية',
                aqi: 'خريطة مؤشر جودة الهواء', 
                wind: 'خريطة اتجاه الرياح'
            },
            en: {
                dust: 'Interactive Dust Concentration Map',
                aqi: 'Air Quality Index Map',
                wind: 'Wind Direction Map'
            }
        };
        overlayText.textContent = layerNames[currentLang][layer] || layerNames[currentLang].dust;
    }
}

function updateInterpolationText(isChecked) {
    const mapOverlay = document.querySelector('.map-overlay p.small');
    if (mapOverlay) {
        const currentLang = document.documentElement.lang || 'ar';
        const texts = {
            ar: {
                checked: 'تم تفعيل طبقة التدرج المكاني',
                unchecked: 'جاري العمل على خريطة تفاعلية تعرض تركيز الغبار عبر العراق'
            },
            en: {
                checked: 'Spatial gradient layer activated',
                unchecked: 'Working on interactive map showing dust concentration across Iraq'
            }
        };
        mapOverlay.textContent = isChecked ? texts[currentLang].checked : texts[currentLang].unchecked;
    }
}

// ===== PROVINCE GAUGE =====
function initProvinceGauge() {
    const provinceCircles = document.querySelectorAll('.province-circle');
    const gaugeValue = document.querySelector('.gauge-value');
    const gaugeLabel = document.querySelector('.gauge-label');
    const aqiLevel = document.querySelector('.aqi-level');
    const aqiIndex = document.querySelector('.aqi-index');
    
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
    
    if (provinceCircles.length > 0) {
        provinceCircles.forEach(circle => {
            circle.addEventListener('click', function() {
                const province = this.getAttribute('data-province');
                const data = provinceData[province];
                
                if (!data) return;
                
                provinceCircles.forEach(c => c.classList.remove('active'));
                this.classList.add('active');
                
                if (gaugeValue) gaugeValue.textContent = data.value;
                if (gaugeLabel) gaugeLabel.textContent = data.name;
                
                if (aqiLevel) {
                    aqiLevel.textContent = data.levelText;
                    aqiLevel.className = `aqi-level ${data.level}`;
                }
                
                if (aqiIndex) aqiIndex.textContent = `مؤشر جودة الهواء: ${data.aqi}`;
                
                const gaugeMarker = document.querySelector('.gauge-marker');
                if (gaugeMarker) {
                    const rotation = (data.value / 200) * 180;
                    gaugeMarker.style.transform = `rotate(${rotation}deg)`;
                }
                
                const statValues = document.querySelectorAll('.stat-content .stat-value');
                if (statValues.length >= 2) {
                    statValues[0].textContent = data.trend;
                    statValues[1].textContent = data.aboveSafe;
                }
            });
        });
    }
}

// ===== TIME PERIOD TABS =====
function initTimePeriodTabs() {
    const timeButtons = document.querySelectorAll('.time-buttons .btn');
    
    if (timeButtons.length > 0) {
        timeButtons.forEach(button => {
            button.addEventListener('click', function() {
                timeButtons.forEach(btn => btn.classList.remove('active'));
                this.classList.add('active');
                
                const period = this.getAttribute('data-period');
                updateChartForPeriod(period);
            });
        });
    }
}

function updateChartForPeriod(period) {
    const chartTitle = document.querySelector('.chart-title h4');
    const currentLang = document.documentElement.lang || 'ar';
    
    const periodTitles = {
        ar: {
            hourly: "تركيز الغبار الساعي - بغداد (اليوم)",
            daily: "تركيز الغبار اليومي - بغداد (الشهر)",
            weekly: "تركيز الغبار الأسبوعي - بغداد (السنة)",
            monthly: "تركيز الغبار الشهري - بغداد (2024)",
            yearly: "تركيز الغبار السنوي - بغداد (5 سنوات)",
            custom: "تركيز الغبار المخصص - بغداد"
        },
        en: {
            hourly: "Hourly Dust Concentration - Baghdad (Today)",
            daily: "Daily Dust Concentration - Baghdad (Month)",
            weekly: "Weekly Dust Concentration - Baghdad (Year)",
            monthly: "Monthly Dust Concentration - Baghdad (2024)",
            yearly: "Yearly Dust Concentration - Baghdad (5 Years)",
            custom: "Custom Dust Concentration - Baghdad"
        }
    };
    
    if (chartTitle) {
        chartTitle.textContent = periodTitles[currentLang][period] || periodTitles[currentLang].monthly;
    }
    
    const monthlyBars = document.querySelector('.monthly-bars');
    if (monthlyBars) {
        monthlyBars.style.opacity = '0.5';
        setTimeout(() => monthlyBars.style.opacity = '1', 300);
    }
}

// ===== CHART NAVIGATION =====
function initChartNavigation() {
    const prevBtn = document.querySelector('.prev-btn');
    const nextBtn = document.querySelector('.next-btn');
    
    if (prevBtn && nextBtn) {
        let currentChartIndex = 0;
        const chartDataSets = ['baghdad', 'basra', 'erbil', 'kirkuk', 'mosul'];
        
        prevBtn.addEventListener('click', () => {
            currentChartIndex = navigateChart(-1, currentChartIndex, chartDataSets);
        });
        
        nextBtn.addEventListener('click', () => {
            currentChartIndex = navigateChart(1, currentChartIndex, chartDataSets);
        });
    }
}

function navigateChart(direction, currentIndex, dataSets) {
    currentIndex = (currentIndex + direction + dataSets.length) % dataSets.length;
    
    const province = dataSets[currentIndex];
    const currentLang = document.documentElement.lang || 'ar';
    
    const provinceNames = {
        ar: {
            baghdad: "بغداد",
            basra: "البصرة", 
            erbil: "أربيل",
            kirkuk: "كركوك",
            mosul: "الموصل"
        },
        en: {
            baghdad: "Baghdad",
            basra: "Basra", 
            erbil: "Erbil",
            kirkuk: "Kirkuk",
            mosul: "Mosul"
        }
    };
    
    const chartTitle = document.querySelector('.chart-title h4');
    const activePeriodBtn = document.querySelector('.time-buttons .btn.active');
    const period = activePeriodBtn ? activePeriodBtn.getAttribute('data-period') : 'monthly';
    
    if (chartTitle) {
        const periodTexts = {
            ar: {
                hourly: "الساعي",
                daily: "اليومي",
                weekly: "الأسبوعي",
                monthly: "الشهري",
                yearly: "السنوي",
                custom: "المخصص"
            },
            en: {
                hourly: "Hourly",
                daily: "Daily",
                weekly: "Weekly",
                monthly: "Monthly",
                yearly: "Yearly",
                custom: "Custom"
            }
        };
        
        const periodText = periodTexts[currentLang][period] || periodTexts[currentLang].monthly;
        const provinceName = provinceNames[currentLang][province] || provinceNames[currentLang].baghdad;
        
        chartTitle.textContent = currentLang === 'ar' 
            ? `تركيز الغبار ${periodText} - ${provinceName}`
            : `${periodText} Dust Concentration - ${provinceName}`;
    }
    
    const provinceSelect = document.querySelector('#provinceSelect');
    if (provinceSelect) provinceSelect.value = province;
    
    const monthlyBars = document.querySelector('.monthly-bars');
    if (monthlyBars) {
        monthlyBars.style.opacity = '0.5';
        setTimeout(() => monthlyBars.style.opacity = '1', 300);
    }
    
    return currentIndex;
}

// ===== PROVINCE SELECTOR =====
function initProvinceSelector() {
    const provinceSelect = document.querySelector('#provinceSelect');
    
    if (provinceSelect) {
        provinceSelect.addEventListener('change', function() {
            const selectedProvince = this.value;
            const chartTitle = document.querySelector('.chart-title h4');
            const currentLang = document.documentElement.lang || 'ar';
            
            if (!chartTitle) return;
            
            const provinceNames = {
                ar: {
                    all: "جميع المحافظات",
                    baghdad: "بغداد",
                    basra: "البصرة",
                    mosul: "الموصل",
                    erbil: "أربيل",
                    sulaymaniyah: "السليمانية",
                    kirkuk: "كركوك"
                },
                en: {
                    all: "All Provinces",
                    baghdad: "Baghdad",
                    basra: "Basra",
                    mosul: "Mosul",
                    erbil: "Erbil",
                    sulaymaniyah: "Sulaymaniyah",
                    kirkuk: "Kirkuk"
                }
            };
            
            if (selectedProvince === 'all') {
                chartTitle.textContent = currentLang === 'ar' 
                    ? "مقارنة تركيز الغبار بين المحافظات"
                    : "Dust Concentration Comparison Between Provinces";
            } else {
                const activePeriodBtn = document.querySelector('.time-buttons .btn.active');
                const period = activePeriodBtn ? activePeriodBtn.getAttribute('data-period') : 'monthly';
                
                const periodTexts = {
                    ar: {
                        hourly: "الساعي",
                        daily: "اليومي",
                        weekly: "الأسبوعي",
                        monthly: "الشهري",
                        yearly: "السنوي",
                        custom: "المخصص"
                    },
                    en: {
                        hourly: "Hourly",
                        daily: "Daily",
                        weekly: "Weekly",
                        monthly: "Monthly",
                        yearly: "Yearly",
                        custom: "Custom"
                    }
                };
                
                const periodText = periodTexts[currentLang][period] || periodTexts[currentLang].monthly;
                const provinceName = provinceNames[currentLang][selectedProvince] || provinceNames[currentLang].baghdad;
                
                chartTitle.textContent = currentLang === 'ar'
                    ? `تركيز الغبار ${periodText} - ${provinceName}`
                    : `${periodText} Dust Concentration - ${provinceName}`;
            }
            
            const monthlyBars = document.querySelector('.monthly-bars');
            if (monthlyBars) {
                monthlyBars.style.opacity = '0.5';
                setTimeout(() => monthlyBars.style.opacity = '1', 300);
            }
        });
    }
}

// ===== FAQ FUNCTIONALITY =====
function initFAQFunctionality() {
    const loadMoreBtn = document.getElementById('loadMoreQuestions');
    
    if (loadMoreBtn) {
        loadMoreBtn.addEventListener('click', function() {
            loadMoreQuestions(this);
        });
    }
    
    // Check for FAQ in URL hash
    const urlHash = window.location.hash;
    if (urlHash && urlHash.startsWith('#faq')) {
        const targetAccordion = document.querySelector(`.accordion-button[data-bs-target="${urlHash}"]`);
        if (targetAccordion) {
            setTimeout(() => targetAccordion.click(), 500);
        }
    }
}

function loadMoreQuestions(button) {
    const faqAccordion = document.getElementById('faqAccordion');
    if (!faqAccordion) return;
    
    const additionalQuestions = [
        {
            id: 'faq9',
            icon: 'fa-tint',
            question: 'كيف تؤثر العواصف الترابية على مصادر المياه في العراق؟',
            answer: `
                <p>تسبب العواصف الترابية عدة مشاكل لمصادر المياه:</p>
                <ul>
                    <li><strong>تلوث المياه السطحية:</strong> ترسب الغبار في الأنهار والبحيرات يزيد من العكارة</li>
                    <li><strong>انسداد محطات التحلية:</strong> زيادة تكاليف الصيانة بنسبة 30%</li>
                    <li><strong>تأثير على الزراعة:</strong> ترسب المعادن الثقيلة على المحاصيل</li>
                    <li><strong>تكاليف إضافية:</strong> 50 مليون دولار سنوياً لتنقية المياه من الغبار</li>
                </ul>
            `
        },
        {
            id: 'faq10',
            icon: 'fa-graduation-cap',
            question: 'ما هو تأثير تلوث الهواء على التعليم في العراق؟',
            answer: `
                <div class="alert alert-info">
                    <i class="fas fa-chalkboard-teacher me-2"></i>
                    <strong>دراسة وزارة التربية 2023:</strong> 15% من الغياب المدرسي مرتبط بأمراض تنفسية
                </div>
                <ul>
                    <li><strong>الغياب:</strong> 1.2 مليون يوم دراسي مفقود سنوياً</li>
                    <li><strong>التعلم:</strong> انخفاض في التحصيل الدراسي بنسبة 7% في المناطق الملوثة</li>
                    <li><strong>الصحة النفسية:</strong> زيادة القلق لدى الطلاب في المناطق الصناعية</li>
                </ul>
            `
        }
    ];
    
    additionalQuestions.forEach(q => {
        const newItem = document.createElement('div');
        newItem.className = 'accordion-item';
        newItem.innerHTML = `
            <h2 class="accordion-header">
                <button class="accordion-button collapsed" type="button" data-bs-toggle="collapse" data-bs-target="#${q.id}">
                    <i class="fas ${q.icon} me-2"></i>
                    ${q.question}
                </button>
            </h2>
            <div id="${q.id}" class="accordion-collapse collapse" data-bs-parent="#faqAccordion">
                <div class="accordion-body">
                    ${q.answer}
                </div>
            </div>
        `;
        faqAccordion.appendChild(newItem);
    });
    
    // Update button state
    button.innerHTML = '<i class="fas fa-check-circle me-2"></i>تم تحميل جميع الأسئلة';
    button.disabled = true;
    button.classList.remove('btn-primary');
    button.classList.add('btn-success');
}
