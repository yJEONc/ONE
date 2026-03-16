let grades = [];
let schools = [];
let unitsByGrade = {};
let endSchoolMap = {};

let selectedGrade = null;
let selectedSchool = null;
let selectedUnits = new Set(); // "number|unit" 형태로 저장

let isSaving = false;
let isRefreshingEnd = false;

/* ✅ 저장 UI 토글 (로딩바 + 버튼 잠금/문구 변경) */
function setSavingUI(on) {
    const loading = document.getElementById("saveLoading");
    const btn = document.getElementById("saveBtn");
    if (!btn) return;

    if (on) {
        isSaving = true;

        if (loading) {
            loading.classList.add("active");
            loading.setAttribute("aria-hidden", "false");
        }

        btn.disabled = true;
        btn.dataset.prevText = btn.textContent;
        btn.textContent = "저장 중...";
    } else {
        isSaving = false;

        if (loading) {
            loading.classList.remove("active");
            loading.setAttribute("aria-hidden", "true");
        }

        btn.textContent = btn.dataset.prevText || "선택 저장";
        updateSaveButton();
    }
}

function updateHeaderInfo(data) {
    const currentTermDisplay = document.getElementById("currentTermDisplay");
    const endCacheInfo = document.getElementById("endCacheInfo");

    if (currentTermDisplay) {
        currentTermDisplay.textContent = `현재 기준 : ${data.currentTermName || "-"}`;
    }

    if (endCacheInfo) {
        endCacheInfo.textContent = `end 캐시 갱신 시각 : ${data.endCacheUpdatedAt || "-"}`;
    }
}

async function fetchData() {
    try {
        const res = await fetch("/survey/api/data");
        const data = await res.json();

        if (!data.ok) {
            console.error("API error:", data);
            alert("데이터 로딩 중 오류가 발생했습니다.\n" + (data.error || ""));
            return;
        }

        grades = data.grades || [];
        schools = data.schools || [];
        unitsByGrade = data.unitsByGrade || {};
        endSchoolMap = data.endSchoolMap || {};

        updateHeaderInfo(data);
        renderGradeList();
        renderSchoolList();
        renderUnits();
        updateSummary();
        updateSaveButton();
    } catch (err) {
        console.error(err);
        alert("서버와 통신 중 오류가 발생했습니다.");
    }
}

function renderGradeList() {
    const ul = document.getElementById("gradeList");
    ul.innerHTML = "";

    grades.forEach(g => {
        const li = document.createElement("li");
        li.className = "sidebar-item" + (selectedGrade === g ? " active" : "");
        li.textContent = `중학교 ${g}학년`;

        li.onclick = () => {
            selectedGrade = g;
            selectedUnits.clear();
            renderGradeList();
            renderSchoolList();
            renderUnits();
            updateSummary();
            updateSaveButton();
        };

        ul.appendChild(li);
    });
}

function renderSchoolList() {
    const ul = document.getElementById("schoolList");
    ul.innerHTML = "";

    const highlightedSchools = new Set(endSchoolMap[selectedGrade] || []);

    schools.forEach(school => {
        const li = document.createElement("li");
        let className = "sidebar-item";

        if (selectedSchool === school) {
            className += " active";
        }

        if (selectedGrade && highlightedSchools.has(school)) {
            className += " school-done";
        }

        li.className = className;
        li.textContent = school;

        if (selectedGrade && highlightedSchools.has(school)) {
            li.title = "end 시트에 이미 등록된 학교입니다.";
        }

        li.onclick = () => {
            selectedSchool = school;
            selectedUnits.clear();
            renderUnits();
            renderSchoolList();
            updateSummary();
            updateSaveButton();
        };

        ul.appendChild(li);
    });
}

function renderUnits() {
    const container = document.getElementById("unitList");
    container.innerHTML = "";

    if (!selectedGrade) {
        const p = document.createElement("p");
        p.textContent = "왼쪽에서 학년을 먼저 선택하세요.";
        p.style.fontSize = "14px";
        p.style.color = "#6b7280";
        container.appendChild(p);
        return;
    }

    const list = unitsByGrade[selectedGrade] || [];
    if (list.length === 0) {
        const p = document.createElement("p");
        p.textContent = "등록된 단원이 없습니다.";
        container.appendChild(p);
        return;
    }

    list.forEach(item => {
        const key = `${item.number}|${item.unit}`;

        const wrapper = document.createElement("div");
        wrapper.className = "unit-item";

        const checkbox = document.createElement("input");
        checkbox.type = "checkbox";
        checkbox.checked = selectedUnits.has(key);
        checkbox.onchange = () => {
            if (checkbox.checked) {
                selectedUnits.add(key);
            } else {
                selectedUnits.delete(key);
            }
            updateSaveButton();
            updateSummary();
        };

        const codeSpan = document.createElement("span");
        codeSpan.className = "unit-code";
        codeSpan.textContent = item.number;

        const nameSpan = document.createElement("span");
        nameSpan.className = "unit-name";
        nameSpan.textContent = item.unit;

        wrapper.onclick = (e) => {
            if (e.target !== checkbox) {
                checkbox.checked = !checkbox.checked;
                checkbox.onchange();
            }
        };

        wrapper.appendChild(checkbox);
        wrapper.appendChild(codeSpan);
        wrapper.appendChild(nameSpan);
        container.appendChild(wrapper);
    });
}

function updateSummary() {
    const summary = document.getElementById("selectionSummary");
    const unitCount = selectedUnits.size;

    if (selectedGrade && selectedSchool) {
        summary.textContent = `중학교 ${selectedGrade}학년 / ${selectedSchool} / 선택 단원: ${unitCount}개`;
    } else if (selectedGrade) {
        summary.textContent = `중학교 ${selectedGrade}학년을 선택했습니다. 학교를 선택하세요.`;
    } else if (selectedSchool) {
        summary.textContent = `${selectedSchool}을(를) 선택했습니다. 학년을 선택하세요.`;
    } else {
        summary.textContent = "학년과 학교를 선택하세요.";
    }
}

function updateSaveButton() {
    const btn = document.getElementById("saveBtn");
    if (!btn) return;

    if (isSaving) {
        btn.disabled = true;
        return;
    }

    btn.disabled = !(selectedGrade && selectedSchool && selectedUnits.size > 0);
}

async function saveSelection() {
    if (isSaving) return;
    if (!selectedGrade || !selectedSchool || selectedUnits.size === 0) return;

    setSavingUI(true);

    const units = Array.from(selectedUnits).map(key => {
        const [number, unit] = key.split("|");
        return { number, unit };
    });

    const payload = {
        grade: selectedGrade,
        school: selectedSchool,
        units
    };

    try {
        const res = await fetch("/survey/api/save", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload)
        });
        const data = await res.json();

        if (data.ok) {
            alert(`저장 완료! (${data.saved}개 단원)`);
        } else {
            alert("저장 중 오류: " + (data.error || "알 수 없는 오류"));
        }
    } catch (err) {
        console.error(err);
        alert("저장 요청 중 오류가 발생했습니다.");
    } finally {
        setSavingUI(false);
    }
}

async function refreshEndCache() {
    if (isRefreshingEnd) return;

    const btn = document.getElementById("refreshEndBtn");
    if (!btn) return;

    try {
        isRefreshingEnd = true;
        btn.disabled = true;
        btn.dataset.prevText = btn.textContent;
        btn.textContent = "업데이트 중...";

        const res = await fetch("/survey/api/refresh_end_cache", {
            method: "POST"
        });
        const data = await res.json();

        if (!data.ok) {
            alert("end 시트 업데이트 실패: " + (data.error || "알 수 없는 오류"));
            return;
        }

        await fetchData();
        alert("end 시트 캐시를 업데이트했습니다.");
    } catch (err) {
        console.error(err);
        alert("end 시트 업데이트 중 오류가 발생했습니다.");
    } finally {
        isRefreshingEnd = false;
        btn.disabled = false;
        btn.textContent = btn.dataset.prevText || "end 시트 업데이트";
    }
}

document.addEventListener("DOMContentLoaded", () => {
    fetchData();

    const saveBtn = document.getElementById("saveBtn");
    if (saveBtn) {
        saveBtn.addEventListener("click", saveSelection);
    }

    const refreshEndBtn = document.getElementById("refreshEndBtn");
    if (refreshEndBtn) {
        refreshEndBtn.addEventListener("click", refreshEndCache);
    }
});
