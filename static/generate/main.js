let grade = null;
let gradeSchools = [];          // end 시트에 해당 학년으로 존재하는 학교들
let selectedSchools = new Set(); // 사용자가 선택한 학교들
let allSchools = [];            // 전체 학교 목록

let renderToken = 0;

window.onload = function () {
    loadSchools();
    bindGradeClicks();
    bindRefreshCacheButton(); // ✅ 추가
};

function bindGradeClicks() {
    document.querySelectorAll("[data-grade]").forEach(li => {
        li.onclick = async () => {
            grade = li.dataset.grade;

            // 학년 active 표시
            document.querySelectorAll("[data-grade]").forEach(g => g.classList.remove("active"));
            li.classList.add("active");

            selectedSchools.clear(); // 학년 변경 시 학교 선택 초기화
            await loadGradeSchools();
            updateSelectedInfo();
            updateSchoolStyles();
            renderUnits();
        };
    });
}

// 전체 학교 목록 로딩
async function loadSchools() {
    const res = await fetch("/generate/api/schools");
    allSchools = await res.json();

    const ul = document.getElementById("school-list");
    ul.innerHTML = "";

    allSchools.forEach(s => {
        const li = document.createElement("li");
        li.textContent = s;
        li.dataset.school = s;
        li.classList.add("school-item");
        li.onclick = () => toggleSchoolSelection(s);
        ul.appendChild(li);
    });
}

// 특정 학년에 대해 end 시트에 존재하는 학교 목록 로딩
async function loadGradeSchools() {
    if (!grade) {
        gradeSchools = [];
        return;
    }
    const res = await fetch("/generate/api/grade_schools", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({ grade: grade })
    });
    gradeSchools = await res.json();
}

// 학교 선택 토글
function toggleSchoolSelection(schoolName) {
    if (!grade) {
        alert("먼저 학년을 선택하세요.");
        return;
    }
    if (selectedSchools.has(schoolName)) {
        selectedSchools.delete(schoolName);
    } else {
        selectedSchools.add(schoolName);
    }
    updateSelectedInfo();
    updateSchoolStyles();
    renderUnits();
}

// 상단 선택 정보 업데이트
function updateSelectedInfo() {
    const box = document.getElementById("selected-info");
    if (!grade && selectedSchools.size === 0) {
        box.textContent = "학년과 학교를 선택해주세요.";
        return;
    }

    let parts = [];
    if (grade) parts.push(grade + "학년");
    if (selectedSchools.size > 0) {
        parts.push("학교: " + Array.from(selectedSchools).join(", "));
    }

    box.textContent = parts.join(" / ");
}

// 학교 리스트 색상/스타일 업데이트
function updateSchoolStyles() {
    const lis = document.querySelectorAll("#school-list li");
    lis.forEach(li => {
        const name = li.dataset.school;
        li.classList.remove("has-end", "selected");

        if (grade && gradeSchools.includes(name)) {
            li.classList.add("has-end");
        }
        if (selectedSchools.has(name)) {
            li.classList.add("selected");
        }
    });
}

// 학교별 카드 렌더링 (A안: 번들 1회 호출)
async function renderUnits() {
    const container = document.getElementById("unit-columns");
    const myToken = ++renderToken;
    container.innerHTML = "";

    if (!grade || selectedSchools.size === 0) return;

    const schoolsArr = Array.from(selectedSchools);

    // ✅ 한 번에 다 가져오기
    const res = await fetch("/generate/api/bundle_units", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({ grade: grade, schools: schoolsArr })
    });

    if (myToken !== renderToken) return;

    const bundle = await res.json();

    if (myToken !== renderToken) return;

    for (const sch of schoolsArr) {
        const data = bundle[sch] || { codes: [], names: {} };
        const card = buildSchoolCardFromData(grade, sch, data.codes, data.names);
        container.appendChild(card);
    }
}

// 학교 카드 생성 (A안: 이미 받은 codes/mapping으로 DOM만 생성)
function buildSchoolCardFromData(gradeVal, schoolName, codes, mapping) {
    const card = document.createElement("div");
    card.classList.add("school-card");

    const header = document.createElement("div");
    header.classList.add("school-card-header");
    header.textContent = `${gradeVal}학년 ${schoolName}`;
    card.appendChild(header);

    const body = document.createElement("div");
    body.classList.add("school-card-body");

    if (!codes || codes.length === 0) {
        body.textContent = "등록된 단원이 없습니다.";
    } else {
        const ul = document.createElement("ul");
        codes.forEach(code => {
            const li = document.createElement("li");
            li.textContent = (mapping && mapping[code]) ? `${code} ${mapping[code]}` : code;
            ul.appendChild(li);
        });
        body.appendChild(ul);
    }
    card.appendChild(body);

    /* ===== 버튼 영역 (2x2) ===== */
    const footer = document.createElement("div");
    footer.classList.add("school-card-footer", "button-grid");

    const b1 = document.createElement("button");
    b1.textContent = "서술형 전체 합치기";
    b1.onclick = () => mergeAll(gradeVal, schoolName, "서술형", card);

    const b2 = document.createElement("button");
    b2.textContent = "최다빈출 전체 합치기";
    b2.onclick = () => mergeAll(gradeVal, schoolName, "최다빈출", card);

    const b3 = document.createElement("button");
    b3.textContent = "Final 모의고사 합치기";
    b3.onclick = () => mergeFinal(gradeVal, schoolName, card);

    const b4 = document.createElement("button");
    b4.textContent = "오투 모의고사 합치기";
    b4.onclick = () => mergeOtoo(gradeVal, schoolName, card);

    footer.append(b1, b2, b3, b4);
    card.appendChild(footer);

    /* ===== 진행 바 ===== */
    const progressBar = document.createElement("div");
    progressBar.classList.add("progress-bar");
    for (let i = 0; i < 10; i++) {
        const cell = document.createElement("div");
        cell.classList.add("progress-cell");
        progressBar.appendChild(cell);
    }
    card.appendChild(progressBar);

    return card;
}

// 공통 병합
function mergeAll(gradeVal, schoolName, type, card) {
    runProgress(card, () =>
        fetch("/generate/api/merge_all", {
            method: "POST",
            headers: {"Content-Type": "application/json"},
            body: JSON.stringify({ grade: gradeVal, school: schoolName, type })
        }),
        `${gradeVal}학년_${schoolName}_${type}_전체.pdf`
    );
}

// Final 병합
function mergeFinal(gradeVal, schoolName, card) {
    runProgress(card, () =>
        fetch("/generate/api/merge_final", {
            method: "POST",
            headers: {"Content-Type": "application/json"},
            body: JSON.stringify({ grade: gradeVal, school: schoolName })
        }),
        `${gradeVal}학년_${schoolName}_FINAL모의고사.pdf`
    );
}

// ===== 오투 모의고사 병합 (추가) =====
function mergeOtoo(gradeVal, schoolName, card) {
    runProgress(card, () =>
        fetch("/generate/api/merge_otoo", {
            method: "POST",
            headers: {"Content-Type": "application/json"},
            body: JSON.stringify({ grade: gradeVal, school: schoolName })
        }),
        `${gradeVal}학년_${schoolName}_오투모의고사.pdf`
    );
}

// 진행바 공통 처리
function runProgress(card, fetchFn, filename) {
    const cells = card.querySelectorAll(".progress-cell");
    cells.forEach(c => {
        c.classList.remove("filled", "done");
        c.style.background = "";
    });

    let i = 0;
    const timer = setInterval(() => {
        if (i < cells.length) {
            cells[i++].classList.add("filled");
        } else {
            clearInterval(timer);
        }
    }, 120);

    fetchFn().then(async r => {
        clearInterval(timer);

        if (!r.ok) {
            cells.forEach(c => c.style.background = "#ef4444");
            return;
        }

        cells.forEach(c => {
            c.classList.remove("filled");
            c.classList.add("done");
        });

        const blob = await r.blob();
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = filename;
        a.click();
    });
}

function bindRefreshCacheButton() {
    const btn = document.getElementById("refresh-cache-btn");
    if (!btn) return;

    btn.onclick = async () => {
        btn.disabled = true;
        btn.textContent = "갱신 중...";

        try {
            const res = await fetch("/generate/api/refresh_cache", { method: "POST" });

            if (!res.ok) {
                alert("캐시 갱신 실패");
                return;
            }

            // ✅ 갱신 후 화면도 최신화
            if (grade) {
                await loadGradeSchools();
                updateSchoolStyles();
            }
            await renderUnits();

            alert("end시트 반영 완료");
        } catch (e) {
            alert("캐시 갱신 중 오류");
        } finally {
            btn.disabled = false;
            btn.textContent = "end시트 반영";
        }
    };
}

