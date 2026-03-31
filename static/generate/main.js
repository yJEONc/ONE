let surveyedSchools = [];
let grade = null;
let gradeSchools = [];
let selectedSchools = new Set();
let renderToken = 0;
let isBulkDownloading = false;
let bundleAbortController = null;

const STATUS_STORAGE_KEY = "vm_generate_download_status_v1";
const MATERIAL_KEYS = ["서술형", "최다빈출", "오투", "Final"];

let downloadStatus = loadDownloadStatus();

window.onload = function () {
    bindGradeClicks();
    bindRefreshCacheButton();
    bindBulkButtons();
    bindSelectionButtons();
    bindResetStatusButton();
    renderSchoolList();
    updateSelectedInfo();
    updateBulkActions();
};

function loadDownloadStatus() {
    try {
        const raw = localStorage.getItem(STATUS_STORAGE_KEY);
        return raw ? JSON.parse(raw) : {};
    } catch (e) {
        return {};
    }
}

function saveDownloadStatus() {
    localStorage.setItem(STATUS_STORAGE_KEY, JSON.stringify(downloadStatus));
}

function clearDownloadStatus() {
    try {
        localStorage.removeItem(STATUS_STORAGE_KEY);
    } catch (e) {}
    downloadStatus = {};
}

function getStatusBucket(gradeVal, schoolName) {
    if (!downloadStatus[gradeVal]) downloadStatus[gradeVal] = {};
    if (!downloadStatus[gradeVal][schoolName]) downloadStatus[gradeVal][schoolName] = {};
    return downloadStatus[gradeVal][schoolName];
}

function getMaterialStatus(gradeVal, schoolName, materialKey) {
    const bucket = (((downloadStatus || {})[gradeVal] || {})[schoolName] || {});
    return bucket[materialKey] || "idle";
}

function setMaterialStatus(gradeVal, schoolName, materialKey, status) {
    const bucket = getStatusBucket(gradeVal, schoolName);
    bucket[materialKey] = status;
    saveDownloadStatus();
}

function getStatusLabel(status) {
    if (status === "done") return "완료";
    if (status === "downloading") return "진행중";
    if (status === "error") return "실패";
    if (status === "queued") return "대기";
    return "미실행";
}

function getMainScrollEl() {
    return document.querySelector(".main") || document.querySelector(".main-content");
}

function getMainScrollTop() {
    const el = getMainScrollEl();
    return el ? el.scrollTop : window.scrollY;
}

function restoreMainScrollTop(prevScrollTop) {
    const el = getMainScrollEl();
    setTimeout(() => {
        requestAnimationFrame(() => {
            requestAnimationFrame(() => {
                if (el) {
                    el.scrollTop = prevScrollTop;
                } else {
                    window.scrollTo(0, prevScrollTop);
                }
            });
        });
    }, 0);
}

function cancelBundleRequest() {
    if (bundleAbortController) {
        try {
            bundleAbortController.abort();
        } catch (e) {}
        bundleAbortController = null;
    }
}

function canMutateSelection() {
    if (!isBulkDownloading) return true;
    alert("일괄 다운로드가 진행 중이라 선택을 변경할 수 없습니다.");
    return false;
}

function bindGradeClicks() {
    document.querySelectorAll("[data-grade]").forEach(li => {
        li.onclick = async () => {
            if (!canMutateSelection()) return;

            grade = li.dataset.grade;

            document.querySelectorAll("[data-grade]").forEach(g => g.classList.remove("active"));
            li.classList.add("active");

            selectedSchools.clear();
            setBulkStatus("");
            await loadGradeSchools();
            renderSchoolList();
            updateSelectedInfo();
            await renderUnits();
            updateBulkActions();
        };
    });
}

async function loadGradeSchools() {
    if (!grade) {
        gradeSchools = [];
        surveyedSchools = [];
        return;
    }

    const res = await fetch("/generate/api/grade_schools", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({ grade: grade })
    });

    const data = await res.json();
    gradeSchools = Array.isArray(data?.schools) ? data.schools : [];
    surveyedSchools = Array.isArray(data?.surveyedSchools) ? data.surveyedSchools : [];
}

function bindSelectionButtons() {
    const selectAllBtn = document.getElementById("select-all-btn");
    const clearAllBtn = document.getElementById("clear-all-btn");

    if (selectAllBtn) {
        selectAllBtn.onclick = async () => {
            if (!canMutateSelection()) return;
            if (!grade || gradeSchools.length === 0) {
                alert("먼저 학년을 선택하세요.");
                return;
            }
            gradeSchools.forEach(s => selectedSchools.add(s));
            updateSelectedInfo();
            updateSchoolStyles();
            await renderUnits();
            updateBulkActions();
        };
    }

    if (clearAllBtn) {
        clearAllBtn.onclick = async () => {
            if (!canMutateSelection()) return;
            selectedSchools.clear();
            setBulkStatus("");
            updateSelectedInfo();
            updateSchoolStyles();
            await renderUnits();
            updateBulkActions();
        };
    }
}

function bindResetStatusButton() {
    const btn = document.getElementById("reset-status-btn");
    if (!btn) return;

    btn.onclick = async () => {
        if (isBulkDownloading) {
            alert("일괄 다운로드 중에는 초기화할 수 없습니다.");
            return;
        }
        const ok = confirm("다운로드 완료 상태를 모두 초기화할까요?");
        if (!ok) return;

        const prevScrollTop = getMainScrollTop();
        clearDownloadStatus();
        await renderUnits();
        restoreMainScrollTop(prevScrollTop);
        setBulkStatus("다운로드 상태를 초기화했습니다.");
    };
}

function renderSchoolList() {
    const ul = document.getElementById("school-list");
    ul.innerHTML = "";

    if (!grade) {
        ul.innerHTML = '<li class="empty-item">먼저 학년을 선택하세요.</li>';
        return;
    }

    if (gradeSchools.length === 0) {
        ul.innerHTML = '<li class="empty-item">해당 학년에 표시할 학교가 없습니다.</li>';
        return;
    }

    gradeSchools.forEach(s => {
        const li = document.createElement("li");
        li.textContent = s;
        li.dataset.school = s;
        li.classList.add("school-item");
        li.onclick = () => toggleSchoolSelection(s);
        ul.appendChild(li);
    });

    updateSchoolStyles();
}

async function toggleSchoolSelection(schoolName) {
    if (!canMutateSelection()) return;
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
    await renderUnits();
    updateBulkActions();
}

async function removeSelectedSchool(schoolName) {
    if (!canMutateSelection()) return;
    if (!selectedSchools.has(schoolName)) return;

    const prevScrollTop = getMainScrollTop();

    selectedSchools.delete(schoolName);
    updateSelectedInfo();
    updateSchoolStyles();
    await renderUnits();
    updateBulkActions();

    restoreMainScrollTop(prevScrollTop);
}

function updateSelectedInfo() {
    const box = document.getElementById("selected-info");
    if (!grade && selectedSchools.size === 0) {
        box.textContent = "학년과 학교를 선택해주세요.";
        return;
    }

    let parts = [];
    if (grade) parts.push(grade + "학년");
    parts.push(`선택 학교 ${selectedSchools.size}개`);

    if (selectedSchools.size > 0) {
        const preview = Array.from(selectedSchools).slice(0, 4).join(", ");
        const extra = selectedSchools.size > 4 ? ` 외 ${selectedSchools.size - 4}개` : "";
        parts.push(`학교: ${preview}${extra}`);
    }

    box.textContent = parts.join(" / ");
}

function updateSchoolStyles() {
    const lis = document.querySelectorAll("#school-list li.school-item");
    lis.forEach(li => {
        const name = li.dataset.school;
        li.classList.remove("has-end", "selected");

        if (grade && selectedSchools.has(name)) {
            li.classList.add("selected");
        }
        if (grade && hasEndDataForSchool(grade, name)) {
            li.classList.add("has-end");
        }
    });
}

function hasEndDataForSchool(gradeVal, schoolName) {
    return gradeVal === grade && surveyedSchools.includes(schoolName);
}

async function renderUnits() {
    const container = document.getElementById("unit-columns");
    const bulkActions = document.getElementById("bulk-actions");
    const myToken = ++renderToken;

    cancelBundleRequest();
    container.innerHTML = "";

    if (!grade || selectedSchools.size === 0) {
        if (bulkActions) bulkActions.classList.add("hidden");
        return;
    }

    const schoolsArr = Array.from(selectedSchools);
    bundleAbortController = new AbortController();

    try {
        const res = await fetch("/generate/api/bundle_units", {
            method: "POST",
            headers: {"Content-Type": "application/json"},
            body: JSON.stringify({ grade: grade, schools: schoolsArr }),
            signal: bundleAbortController.signal,
        });

        if (myToken !== renderToken) return;

        const bundle = await res.json();
        if (myToken !== renderToken) return;

        for (const sch of schoolsArr) {
            const data = bundle[sch] || { codes: [], names: {}, range: "", exam_period: "", science_date: "" };
            const card = buildSchoolCardFromData(grade, sch, data);
            container.appendChild(card);
        }

        if (bulkActions) {
            bulkActions.classList.toggle("hidden", schoolsArr.length === 0);
        }
    } catch (e) {
        if (e && e.name === "AbortError") return;
        console.error(e);
        if (myToken !== renderToken) return;
        container.innerHTML = "";
        if (bulkActions) {
            bulkActions.classList.toggle("hidden", schoolsArr.length === 0);
        }
    } finally {
        if (bundleAbortController && bundleAbortController.signal.aborted) {
            bundleAbortController = null;
        } else if (myToken === renderToken) {
            bundleAbortController = null;
        }
    }
}

function buildSchoolCardFromData(gradeVal, schoolName, data) {
    const card = document.createElement("div");
    card.classList.add("school-card");

    const header = document.createElement("div");
    header.classList.add("school-card-header");

    const titleWrap = document.createElement("div");
    titleWrap.classList.add("school-card-title-wrap");

    const title = document.createElement("div");
    title.classList.add("school-card-title");
    title.textContent = `${gradeVal}학년 ${schoolName}`;

    const meta = document.createElement("div");
    meta.classList.add("school-card-meta");

    const metaParts = [];
    if (data.exam_period) metaParts.push(`시험기간 ${data.exam_period}`);
    if (data.science_date) metaParts.push(`과학일 ${data.science_date}`);
    meta.textContent = metaParts.join(" / ") || "시험 정보 없음";

    titleWrap.appendChild(title);
    titleWrap.appendChild(meta);

    const closeBtn = document.createElement("button");
    closeBtn.type = "button";
    closeBtn.className = "remove-card-btn";
    closeBtn.textContent = "✕";
    closeBtn.title = "선택 해제";
    closeBtn.onclick = async (e) => {
        e.preventDefault();
        e.stopPropagation();
        closeBtn.blur();
        await removeSelectedSchool(schoolName);
    };

    header.appendChild(titleWrap);
    header.appendChild(closeBtn);
    card.appendChild(header);

    const body = document.createElement("div");
    body.classList.add("school-card-body");

    const rangeBox = document.createElement("div");
    rangeBox.classList.add("range-box");
    rangeBox.innerHTML = `<div class="range-label">시험범위</div><div class="range-text">${escapeHtml(data.range || "미입력")}</div>`;
    body.appendChild(rangeBox);

    if (!data.codes || data.codes.length === 0) {
        const empty = document.createElement("div");
        empty.classList.add("units-empty");
        empty.textContent = "등록된 단원이 없습니다.";
        body.appendChild(empty);
    } else {
        const ul = document.createElement("ul");
        ul.classList.add("unit-code-list");
        data.codes.forEach(code => {
            const li = document.createElement("li");
            li.textContent = (data.names && data.names[code]) ? `${code} ${data.names[code]}` : code;
            ul.appendChild(li);
        });
        body.appendChild(ul);
    }
    card.appendChild(body);

    const statusRow = document.createElement("div");
    statusRow.classList.add("status-row");
    MATERIAL_KEYS.forEach(materialKey => {
        const chip = document.createElement("div");
        const status = getMaterialStatus(gradeVal, schoolName, materialKey);
        chip.className = `status-chip ${status}`;
        chip.dataset.material = materialKey;
        chip.textContent = `${materialKey}: ${getStatusLabel(status)}`;
        statusRow.appendChild(chip);
    });
    card.appendChild(statusRow);

    const footer = document.createElement("div");
    footer.classList.add("school-card-footer", "button-grid");

    const buttons = [
        { label: "서술형 전체 합치기", key: "서술형" },
        { label: "최다빈출 전체 합치기", key: "최다빈출" },
        { label: "Final 모의고사 합치기", key: "Final" },
        { label: "오투 모의고사 합치기", key: "오투" },
    ];

    buttons.forEach(info => {
        const btn = document.createElement("button");
        btn.type = "button";
        btn.textContent = info.label;
        btn.onclick = () => runSingleDownload(gradeVal, schoolName, info.key);
        footer.appendChild(btn);
    });

    card.appendChild(footer);
    return card;
}

function setBulkStatus(text) {
    const el = document.getElementById("bulk-status");
    if (!el) return;
    el.textContent = text || "";
    el.classList.toggle("visible", !!text);
}

function updateCardStatusUI(gradeVal, schoolName) {
    const cards = document.querySelectorAll(".school-card");
    cards.forEach(card => {
        const title = card.querySelector(".school-card-title");
        if (!title) return;
        if (title.textContent !== `${gradeVal}학년 ${schoolName}`) return;
        card.querySelectorAll(".status-chip").forEach(chip => {
            const materialKey = chip.dataset.material;
            const status = getMaterialStatus(gradeVal, schoolName, materialKey);
            chip.className = `status-chip ${status}`;
            chip.textContent = `${materialKey}: ${getStatusLabel(status)}`;
        });
    });
}

function updateBulkActions() {
    const wrap = document.getElementById("bulk-actions");
    if (!wrap) return;
    wrap.classList.toggle("hidden", selectedSchools.size === 0);
}

function getDownloadConfig(materialKey) {
    if (materialKey === "서술형") {
        return {
            endpoint: "/generate/api/merge_all",
            body: (g, s) => ({ grade: g, school: s, type: "서술형" }),
            filename: (g, s) => `${g}학년_${s}_서술형_전체.pdf`
        };
    }
    if (materialKey === "최다빈출") {
        return {
            endpoint: "/generate/api/merge_all",
            body: (g, s) => ({ grade: g, school: s, type: "최다빈출" }),
            filename: (g, s) => `${g}학년_${s}_최다빈출_전체.pdf`
        };
    }
    if (materialKey === "Final") {
        return {
            endpoint: "/generate/api/merge_final",
            body: (g, s) => ({ grade: g, school: s }),
            filename: (g, s) => `${g}학년_${s}_FINAL모의고사.pdf`
        };
    }
    return {
        endpoint: "/generate/api/merge_otoo",
        body: (g, s) => ({ grade: g, school: s }),
        filename: (g, s) => `${g}학년_${s}_오투모의고사.pdf`
    };
}

async function triggerDownloadFromResponse(response, filename) {
    const blob = await response.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(() => URL.revokeObjectURL(url), 1500);
}

async function runSingleDownload(gradeVal, schoolName, materialKey) {
    const config = getDownloadConfig(materialKey);

    setMaterialStatus(gradeVal, schoolName, materialKey, "downloading");
    updateCardStatusUI(gradeVal, schoolName);
    setBulkStatus(`${schoolName} / ${materialKey} 다운로드 중...`);

    try {
        const response = await fetch(config.endpoint, {
            method: "POST",
            headers: {"Content-Type": "application/json"},
            body: JSON.stringify(config.body(gradeVal, schoolName))
        });

        if (!response.ok) {
            setMaterialStatus(gradeVal, schoolName, materialKey, "error");
            updateCardStatusUI(gradeVal, schoolName);
            setBulkStatus(`${schoolName} / ${materialKey} 다운로드 실패`);
            return false;
        }

        await triggerDownloadFromResponse(response, config.filename(gradeVal, schoolName));
        setMaterialStatus(gradeVal, schoolName, materialKey, "done");
        updateCardStatusUI(gradeVal, schoolName);
        setBulkStatus(`${schoolName} / ${materialKey} 다운로드 완료`);
        return true;
    } catch (e) {
        setMaterialStatus(gradeVal, schoolName, materialKey, "error");
        updateCardStatusUI(gradeVal, schoolName);
        setBulkStatus(`${schoolName} / ${materialKey} 다운로드 오류`);
        return false;
    }
}

function bindBulkButtons() {
    document.querySelectorAll("[data-bulk-type]").forEach(btn => {
        btn.onclick = async () => {
            const materialKey = btn.dataset.bulkType;
            if (!grade || selectedSchools.size === 0) {
                alert("먼저 학교를 선택하세요.");
                return;
            }
            if (isBulkDownloading) {
                alert("현재 일괄 다운로드가 진행 중입니다.");
                return;
            }

            isBulkDownloading = true;
            document.querySelectorAll("[data-bulk-type]").forEach(b => b.disabled = true);

            const schoolsArr = Array.from(selectedSchools);
            let successCount = 0;

            try {
                for (let i = 0; i < schoolsArr.length; i++) {
                    const schoolName = schoolsArr[i];
                    setMaterialStatus(grade, schoolName, materialKey, "queued");
                    updateCardStatusUI(grade, schoolName);
                    setBulkStatus(`${materialKey} 일괄 다운로드 진행 중... (${i + 1}/${schoolsArr.length}) ${schoolName}`);
                    const ok = await runSingleDownload(grade, schoolName, materialKey);
                    if (ok) successCount += 1;
                }
                setBulkStatus(`${materialKey} 일괄 다운로드 완료 (${successCount}/${schoolsArr.length})`);
            } finally {
                isBulkDownloading = false;
                document.querySelectorAll("[data-bulk-type]").forEach(b => b.disabled = false);
            }
        };
    });
}

function bindRefreshCacheButton() {
    const btn = document.getElementById("refresh-cache-btn");
    if (!btn) return;

    btn.onclick = async () => {
        if (isBulkDownloading) {
            alert("일괄 다운로드가 진행 중이라 지금은 갱신할 수 없습니다.");
            return;
        }

        btn.disabled = true;
        btn.textContent = "갱신 중...";

        try {
            const res = await fetch("/generate/api/refresh_cache", { method: "POST" });

            if (!res.ok) {
                alert("캐시 갱신 실패");
                return;
            }

            if (grade) {
                const prevSelected = new Set(selectedSchools);
                await loadGradeSchools();
                selectedSchools = new Set(Array.from(prevSelected).filter(s => gradeSchools.includes(s)));
                renderSchoolList();
            }
            await renderUnits();
            updateSelectedInfo();
            updateBulkActions();

            alert("end시트 반영 완료");
        } catch (e) {
            alert("캐시 갱신 중 오류");
        } finally {
            btn.disabled = false;
            btn.textContent = "end시트 반영";
        }
    };
}

function escapeHtml(text) {
    return String(text ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
}
