from flask import (
    Flask,
    render_template,
    request,
    jsonify,
    send_file,
    redirect,
    url_for,
    session,
    Response,
)
from flask_caching import Cache
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from PyPDF2 import PdfMerger
import gspread

import os
import io
import re
import json
import time
import traceback
import threading
import datetime
from threading import Lock


app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev-secret-change-me")
cache = Cache(app, config={"CACHE_TYPE": "SimpleCache"})

SPREADSHEET_ID = "1rsplfNq4e7d-nrp-Wlg1Mn9dsgjAcNn49yPQDXdzwg8"

# -----------------------------
# 공통 상수
# -----------------------------
SCOPES_READONLY = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
SCOPES_RW = ["https://www.googleapis.com/auth/spreadsheets"]

PUBLIC_PATHS = {
    "/",
    "/login",
    "/logout",
}

GRADE_SHEETS = {"1": "M1", "2": "M2", "3": "M3"}

# 내신관리 체크 컬럼
CHECK_COLS = ["G", "H", "I", "J"]
ALLOWED_MARKS = {"⭕", "△", "✕", ""}

# 직전보강 입력 컬럼
RECENT_TEXT_COLS = ["K", "L", "M"]

# 내신자료 생성 시트명
SHEET_SCHOOL = "class+"
SHEET_END = "end"
SHEET_UNITS = "units"

# -----------------------------
# 공통 로그인
# -----------------------------
@app.before_request
def require_login():
    path = request.path

    # 정적 파일 허용
    if path.startswith("/static/"):
        return None

    # 기능별 개별 asset route 허용
    if path.startswith("/survey-assets/") or path.startswith("/generate-assets/"):
        return None

    # 공개 경로 허용
    if path in PUBLIC_PATHS:
        return None

    if not session.get("logged_in"):
        if path.startswith("/survey/api/") or path.startswith("/manage/api/") or path.startswith("/generate/api/"):
            return jsonify({"ok": False, "error": "unauthorized"}), 401
        return redirect(url_for("login"))

    return None


# -----------------------------
# 공통 credential / service
# -----------------------------
def load_service_account_info():
    """
    우선순위:
    1) SERVICE_KEY
    2) GOOGLE_CREDENTIALS
    3) GOOGLE_CREDENTIALS_JSON
    4) service_account.json 파일
    """
    for env_name in ["SERVICE_KEY", "GOOGLE_CREDENTIALS", "GOOGLE_CREDENTIALS_JSON"]:
        raw = os.environ.get(env_name)
        if raw:
            try:
                return json.loads(raw)
            except Exception as e:
                raise RuntimeError(f"{env_name} JSON 파싱 오류: {e}")

    if os.path.exists("service_account.json"):
        with open("service_account.json", "r", encoding="utf-8") as f:
            return json.load(f)

    raise RuntimeError(
        "서비스 계정 정보를 찾을 수 없습니다. "
        "SERVICE_KEY / GOOGLE_CREDENTIALS / GOOGLE_CREDENTIALS_JSON / service_account.json 중 하나가 필요합니다."
    )


def get_google_credentials(scopes):
    info = load_service_account_info()
    return Credentials.from_service_account_info(info, scopes=scopes)


def get_sheets_service(readonly=False):
    scopes = SCOPES_READONLY if readonly else SCOPES_RW
    creds = get_google_credentials(scopes)
    return build("sheets", "v4", credentials=creds)


def get_gspread_client():
    creds = get_google_credentials(SCOPES_RW)
    return gspread.authorize(creds)


def get_spreadsheet():
    gc = get_gspread_client()
    return gc.open_by_key(SPREADSHEET_ID)


# =========================================================
# 1) 내신대비조사
# =========================================================
END_CACHE = {
    "loaded": False,
    "end_school_map": {},
    "updated_at": None
}
END_CACHE_LOCK = threading.Lock()


def survey_get_sheets():
    sh = get_spreadsheet()

    try:
        units_ws = sh.worksheet("units")
        school_ws = sh.worksheet("class+")
        records_ws = sh.worksheet("records")
        settings_ws = sh.worksheet("settings")
        end_ws = sh.worksheet("end")
    except Exception as e:
        raise RuntimeError(f"시트 이름(units/class+/records/settings/end)을 찾을 수 없습니다: {e}")

    return units_ws, school_ws, records_ws, settings_ws, end_ws


def get_header_index(headers, target_name):
    try:
        return headers.index(target_name)
    except ValueError:
        raise RuntimeError(f"헤더 '{target_name}' 를 찾을 수 없습니다. 현재 헤더: {headers}")


def safe_cell(row, idx):
    return row[idx].strip() if len(row) > idx and row[idx] is not None else ""


def get_current_sort_header(settings_ws):
    val = settings_ws.acell("A1").value
    val = (val or "").strip()

    if not val:
        raise RuntimeError("settings 시트 A1 이 비어 있습니다.")

    return val


def get_current_term_name(settings_ws):
    header_name = get_current_sort_header(settings_ws)
    if header_name.endswith("_시험기간"):
        return header_name[:-5]
    return header_name


def build_end_school_map_from_rows(end_rows):
    end_school_map = {}

    if not end_rows:
        return end_school_map

    start_idx = 0
    if len(end_rows[0]) >= 3:
        b0 = (end_rows[0][1] or "").strip().lower()
        c0 = (end_rows[0][2] or "").strip().lower()
        if b0 == "grade" or c0 == "school":
            start_idx = 1

    for row in end_rows[start_idx:]:
        grade_val = row[1].strip() if len(row) > 1 and row[1] else ""
        school_val = row[2].strip() if len(row) > 2 and row[2] else ""

        if not grade_val or not school_val:
            continue

        end_school_map.setdefault(grade_val, set()).add(school_val)

    return {
        grade: sorted(list(schools_set))
        for grade, schools_set in end_school_map.items()
    }


def refresh_end_cache():
    _, _, _, _, end_ws = survey_get_sheets()
    end_rows = end_ws.get_all_values()
    end_school_map = build_end_school_map_from_rows(end_rows)

    with END_CACHE_LOCK:
        END_CACHE["loaded"] = True
        END_CACHE["end_school_map"] = end_school_map
        END_CACHE["updated_at"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return END_CACHE


def ensure_end_cache():
    with END_CACHE_LOCK:
        if END_CACHE["loaded"]:
            return END_CACHE
    return refresh_end_cache()


# =========================================================
# 2) 내신관리
# =========================================================
def sheet_name_by_grade(grade: str) -> str:
    if grade not in GRADE_SHEETS:
        raise ValueError("invalid grade")
    return GRADE_SHEETS[grade]


def parse_mmdd(s: str):
    if not s:
        return None
    t = str(s).strip()
    if not t:
        return None
    nums = re.findall(r"\d+", t)
    if len(nums) < 2:
        return None
    m = int(nums[0])
    d = int(nums[1])
    if not (1 <= m <= 12 and 1 <= d <= 31):
        return None
    return (m, d)


def parse_period_start(s: str):
    if not s:
        return None

    t = str(s).strip()
    if not t or "미확정" in t:
        return None

    m = re.search(r"(\d{1,2})\s*[./-]\s*(\d{1,2})", t)
    if not m:
        return None

    month = int(m.group(1))
    day = int(m.group(2))

    if not (1 <= month <= 12 and 1 <= day <= 31):
        return None

    return (month, day)


def parse_grade_num(s):
    try:
        return int(str(s).strip())
    except Exception:
        return 999


# =========================================================
# 3) 내신자료 생성
# =========================================================
CACHE_LOCK = Lock()
CACHE = {
    "end_rows": None,
    "units_rows": None,
    "school_list": None,
    "loaded_at": None
}


def parse_science_date(text):
    text = (text or "").strip()
    if not text:
        return (9999, 12, 31)

    m = re.search(r"(\d{4})[./-](\d{1,2})[./-](\d{1,2})", text)
    if m:
        return (int(m.group(1)), int(m.group(2)), int(m.group(3)))

    m = re.search(r"(\d{1,2})[./-](\d{1,2})", text)
    if m:
        return (9999, int(m.group(1)), int(m.group(2)))

    return (9999, 12, 31)


def parse_exam_period(text):
    text = (text or "").strip()
    if not text:
        return (99, 99)

    m = re.search(r"(\d{1,2})[./-](\d{1,2})", text)
    if m:
        return (int(m.group(1)), int(m.group(2)))

    return (99, 99)


def school_sort_key(science_date, exam_period, school_name):
    return (
        parse_science_date(science_date),
        parse_exam_period(exam_period),
        school_name or ""
    )


def refresh_generate_cache():
    service = get_sheets_service(readonly=True)

    end_res = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_END}!A2:D"
    ).execute()
    end_rows = end_res.get("values", [])

    units_res = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_UNITS}!A2:C"
    ).execute()
    units_rows = units_res.get("values", [])

    school_res = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_SCHOOL}!I2:S"
    ).execute()
    school_rows = school_res.get("values", [])

    best_by_school = {}

    for row in school_rows:
        school_name = row[0].strip() if len(row) > 0 and row[0] else ""
        exam_period = row[9].strip() if len(row) > 9 and row[9] else ""
        science_date = row[10].strip() if len(row) > 10 and row[10] else ""

        if not school_name:
            continue

        current_key = school_sort_key(science_date, exam_period, school_name)

        if school_name not in best_by_school:
            best_by_school[school_name] = {
                "school": school_name,
                "science_date": science_date,
                "exam_period": exam_period,
                "sort_key": current_key
            }
        else:
            if current_key < best_by_school[school_name]["sort_key"]:
                best_by_school[school_name] = {
                    "school": school_name,
                    "science_date": science_date,
                    "exam_period": exam_period,
                    "sort_key": current_key
                }

    school_list = [
        item["school"]
        for item in sorted(best_by_school.values(), key=lambda x: x["sort_key"])
    ]

    CACHE["end_rows"] = end_rows
    CACHE["units_rows"] = units_rows
    CACHE["school_list"] = school_list
    CACHE["loaded_at"] = time.time()


def ensure_generate_cache():
    with CACHE_LOCK:
        if CACHE["end_rows"] is None or CACHE["units_rows"] is None or CACHE["school_list"] is None:
            refresh_generate_cache()


def read_school_list():
    ensure_generate_cache()
    return CACHE["school_list"]


def read_units_codes(grade, school):
    ensure_generate_cache()
    rows = CACHE["end_rows"]
    for r in rows:
        if len(r) >= 4 and str(r[1]) == str(grade) and r[2] == school:
            return [u.strip() for u in r[3].split(",") if u.strip()]
    return []


def read_grade_schools(grade):
    ensure_generate_cache()
    rows = CACHE["end_rows"]
    seen = set()
    schools = []
    for r in rows:
        if len(r) >= 3 and str(r[1]) == str(grade):
            if r[2] not in seen:
                seen.add(r[2])
                schools.append(r[2])
    return schools


def get_unit_name_map(grade, codes):
    if not codes:
        return {}
    ensure_generate_cache()
    rows = CACHE["units_rows"]
    codes_set = set(codes)
    mapping = {}
    for r in rows:
        if len(r) >= 3 and str(r[0]) == str(grade) and r[1] in codes_set:
            mapping[r[1]] = r[2]
    return mapping


def find_pdfs(material_type, grade, unit_code):
    folder = f"data/{material_type}/{grade}학년"
    if not os.path.isdir(folder):
        return []
    pattern = re.compile(rf"{re.escape(unit_code)}\b")
    return [
        os.path.join(folder, f)
        for f in os.listdir(folder)
        if f.lower().endswith(".pdf") and pattern.search(f)
    ]


# =========================================================
# 공통 페이지
# =========================================================
@app.route("/")
def home():
    if session.get("logged_in"):
        return redirect(url_for("menu"))
    return redirect(url_for("login"))


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "GET":
        return render_template("login.html")

    password = request.form.get("password", "")
    real = os.getenv("APP_PASSWORD", "")

    if real and password == real:
        session["logged_in"] = True
        return redirect(url_for("menu"))

    return render_template("login.html", error="비밀번호가 올바르지 않습니다.")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/menu")
def menu():
    return Response(
        """
<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>내신 통합 메뉴</title>
    <style>
        * { box-sizing: border-box; }
        body {
            margin: 0;
            font-family: Arial, sans-serif;
            background: #f4f6f8;
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
        }
        .wrap {
            width: 100%;
            max-width: 1100px;
            padding: 40px 24px;
            text-align: center;
        }
        h1 {
            margin: 0 0 32px;
            font-size: 32px;
            color: #222;
        }
        .menu-grid {
            display: grid;
            grid-template-columns: repeat(3, minmax(220px, 1fr));
            gap: 24px;
        }
        .card {
            display: flex;
            align-items: center;
            justify-content: center;
            min-height: 220px;
            padding: 24px;
            text-decoration: none;
            border-radius: 18px;
            background: white;
            box-shadow: 0 10px 30px rgba(0,0,0,0.08);
            color: #111;
            font-size: 28px;
            font-weight: 700;
            transition: transform .15s ease, box-shadow .15s ease;
        }
        .card:hover {
            transform: translateY(-4px);
            box-shadow: 0 14px 36px rgba(0,0,0,0.12);
        }
        .footer {
            margin-top: 28px;
        }
        .logout {
            color: #666;
            text-decoration: none;
            font-size: 15px;
        }
        @media (max-width: 900px) {
            .menu-grid { grid-template-columns: 1fr; }
            .card { min-height: 120px; font-size: 24px; }
        }
    </style>
</head>
<body>
    <div class="wrap">
        <h1>내신 통합 메뉴</h1>
        <div class="menu-grid">
            <a class="card" href="/survey">내신대비조사</a>
            <a class="card" href="/manage">내신관리</a>
            <a class="card" href="/generate">내신자료 생성</a>
        </div>
        <div class="footer">
            <a class="logout" href="/logout">로그아웃</a>
        </div>
    </div>
</body>
</html>
        """,
        mimetype="text/html",
    )


# =========================================================
# 페이지 라우트
# =========================================================
@app.route("/survey")
def survey_page():
    return render_template("survey.html")


@app.route("/manage")
def manage_page():
    return render_template("manage.html")


@app.route("/generate")
def generate_page():
    return render_template("generate.html")


# =========================================================
# 선택적 asset route
# 템플릿 수정해서 이 경로를 쓰면 충돌 없이 관리 가능
# =========================================================
@app.route("/survey-assets/<path:filename>")
def survey_assets(filename):
    path = os.path.join("static", "survey", filename)
    if not os.path.exists(path):
        return "Not Found", 404
    with open(path, "rb") as f:
        data = f.read()

    if filename.endswith(".css"):
        mimetype = "text/css"
    elif filename.endswith(".js"):
        mimetype = "application/javascript"
    else:
        mimetype = "application/octet-stream"

    return Response(data, mimetype=mimetype)


@app.route("/generate-assets/<path:filename>")
def generate_assets(filename):
    path = os.path.join("static", "generate", filename)
    if not os.path.exists(path):
        return "Not Found", 404
    with open(path, "rb") as f:
        data = f.read()

    if filename.endswith(".css"):
        mimetype = "text/css"
    elif filename.endswith(".js"):
        mimetype = "application/javascript"
    else:
        mimetype = "application/octet-stream"

    return Response(data, mimetype=mimetype)


# =========================================================
# 1) 내신대비조사 API
# =========================================================
@app.route("/survey/api/data")
def survey_api_data():
    try:
        units_ws, school_ws, _, settings_ws, _ = survey_get_sheets()

        units_rows = units_ws.get_all_values()
        school_rows = school_ws.get_all_values()

        if not units_rows:
            raise RuntimeError("units 시트가 비어 있습니다.")
        if not school_rows:
            raise RuntimeError("class+ 시트가 비어 있습니다.")

        units_headers = units_rows[0]
        units_data = units_rows[1:]

        school_headers = school_rows[0]
        school_data = school_rows[1:]

        grade_idx = get_header_index(units_headers, "grade")
        number_idx = get_header_index(units_headers, "number")
        unit_idx = get_header_index(units_headers, "units")
        current_school_idx = get_header_index(school_headers, "현재 학교")

        grade_set = set()
        units_by_grade = {}

        for row in units_data:
            grade_raw = safe_cell(row, grade_idx)
            number = safe_cell(row, number_idx)
            unit_name = safe_cell(row, unit_idx)

            if not grade_raw or not number or not unit_name:
                continue

            grade_set.add(grade_raw)
            units_by_grade.setdefault(grade_raw, []).append({
                "number": number,
                "unit": unit_name,
            })

        def grade_key(g):
            try:
                return int(g)
            except ValueError:
                return 9999

        grades = sorted(grade_set, key=grade_key)

        school_set = set()
        for row in school_data:
            school_name = safe_cell(row, current_school_idx)
            if school_name:
                school_set.add(school_name)

        schools = sorted(school_set)

        end_cache = ensure_end_cache()

        return jsonify({
            "ok": True,
            "grades": grades,
            "schools": schools,
            "unitsByGrade": units_by_grade,
            "currentSortHeader": get_current_sort_header(settings_ws),
            "currentTermName": get_current_term_name(settings_ws),
            "endSchoolMap": end_cache["end_school_map"],
            "endCacheUpdatedAt": end_cache["updated_at"],
        })

    except Exception as e:
        return jsonify({
            "ok": False,
            "error": str(e),
            "trace": traceback.format_exc(),
        }), 500


@app.route("/survey/api/refresh_end_cache", methods=["POST"])
def survey_api_refresh_end_cache():
    try:
        cache_data = refresh_end_cache()
        return jsonify({
            "ok": True,
            "updatedAt": cache_data["updated_at"],
            "gradeCount": len(cache_data["end_school_map"]),
        })
    except Exception as e:
        return jsonify({
            "ok": False,
            "error": str(e),
            "trace": traceback.format_exc(),
        }), 500


@app.route("/survey/api/save", methods=["POST"])
def survey_api_save():
    try:
        _, _, records_ws, settings_ws, _ = survey_get_sheets()

        data = request.get_json(force=True) or {}
        grade = str(data.get("grade") or "").strip()
        school = str(data.get("school") or "").strip()
        units = data.get("units") or []

        if not grade or not school or not units:
            return jsonify({"ok": False, "error": "grade, school, units 정보가 필요합니다."}), 400

        today = datetime.date.today().isoformat()
        current_term_name = get_current_term_name(settings_ws)

        rows = []
        for item in units:
            number = str(item.get("number") or "").strip()
            unit_name = str(item.get("unit") or "").strip()
            if not number or not unit_name:
                continue

            rows.append([today, grade, school, number, unit_name, current_term_name])

        if not rows:
            return jsonify({"ok": False, "error": "저장할 단원이 없습니다."}), 400

        records_ws.append_rows(rows)

        return jsonify({
            "ok": True,
            "saved": len(rows),
            "currentTermName": current_term_name
        })

    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "trace": traceback.format_exc()}), 500


# =========================================================
# 2) 내신관리 API
# =========================================================
@app.get("/manage/api/classes")
@cache.cached(timeout=600, query_string=True)
def manage_api_classes():
    try:
        grade = request.args.get("grade")
        sheet = sheet_name_by_grade(grade)

        svc = get_sheets_service(readonly=False)
        resp = svc.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{sheet}!A2:A"
        ).execute()

        vals = resp.get("values", [])
        classes = []
        seen = set()
        for v in vals:
            name = (v[0] if v else "").strip()
            if name and name not in seen:
                seen.add(name)
                classes.append(name)
        classes.sort()

        return jsonify({"ok": True, "grade": grade, "classes": classes})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.get("/manage/api/students")
@cache.cached(timeout=30, query_string=True)
def manage_api_students():
    try:
        grade = request.args.get("grade")
        class_name = request.args.get("class")
        sheet = sheet_name_by_grade(grade)

        svc = get_sheets_service(readonly=False)
        resp = svc.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{sheet}!A2:J"
        ).execute()

        rows = resp.get("values", [])
        students = []

        for i, row in enumerate(rows):
            a = (row[0] if len(row) > 0 else "").strip()
            if not a or a != class_name:
                continue

            def get(idx):
                return row[idx] if len(row) > idx else ""

            sheet_row = i + 2

            students.append({
                "sheet": sheet,
                "grade": grade,
                "class": a,
                "sheet_row": sheet_row,
                "name": get(1),
                "school": get(2),
                "range": get(3),
                "period": get(4),
                "exam_date": get(5),
                "otwo": get(6),
                "essay": get(7),
                "freq": get(8),
                "freq_essay": get(9),
            })

        return jsonify({"ok": True, "grade": grade, "class": class_name, "students": students})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.get("/manage/api/recent")
@cache.cached(timeout=20, query_string=True)
def manage_api_recent():
    try:
        svc = get_sheets_service(readonly=False)
        all_students = []

        for grade, sheet in [("1", "M1"), ("2", "M2"), ("3", "M3")]:
            resp = svc.spreadsheets().values().get(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{sheet}!A2:M"
            ).execute()
            rows = resp.get("values", [])

            for i, row in enumerate(rows):
                def get(idx):
                    return row[idx] if len(row) > idx else ""

                name = str(get(1)).strip()
                if not name:
                    continue

                sheet_row = i + 2
                exam_date = str(get(5)).strip()
                period = str(get(4)).strip()

                science_mmdd = parse_mmdd(exam_date)
                period_start = parse_period_start(period)

                all_students.append({
                    "sheet": sheet,
                    "grade": grade,
                    "class": str(get(0)).strip(),
                    "sheet_row": sheet_row,
                    "name": get(1),
                    "school": get(2),
                    "range": get(3),
                    "period": period,
                    "exam_date": exam_date,
                    "otwo": get(6),
                    "essay": get(7),
                    "freq": get(8),
                    "freq_essay": get(9),
                    "jb1": get(10),
                    "jb2": get(11),
                    "jb3": get(12),
                    "_science_mmdd": science_mmdd,
                    "_period_start": period_start,
                })

        def sort_key(st):
            science_mmdd = st.get("_science_mmdd")
            science_key = (99, 99) if science_mmdd is None else science_mmdd

            period_start = st.get("_period_start")
            period_key = (99, 99) if period_start is None else period_start

            school_key = str(st.get("school") or "").strip()
            grade_key = parse_grade_num(st.get("grade"))

            return (
                science_key[0], science_key[1],
                period_key[0], period_key[1],
                school_key,
                grade_key
            )

        all_students.sort(key=sort_key)

        for st in all_students:
            st.pop("_science_mmdd", None)
            st.pop("_period_start", None)

        return jsonify({"ok": True, "students": all_students})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.post("/manage/api/apply")
def manage_api_apply():
    try:
        data = request.get_json(force=True)
        changes = data.get("changes", [])

        grade = data.get("grade")
        default_sheet = sheet_name_by_grade(str(grade)) if grade else None

        allowed_cols = set(CHECK_COLS + RECENT_TEXT_COLS)

        updates = []
        for ch in changes:
            sheet_row = int(ch.get("sheet_row"))
            col = str(ch.get("col")).upper()
            value = "" if ch.get("value") is None else str(ch.get("value"))
            sheet = ch.get("sheet") or default_sheet

            if not sheet:
                return jsonify({"ok": False, "error": "missing sheet/grade"}), 400
            if col not in allowed_cols:
                return jsonify({"ok": False, "error": f"invalid col: {col}"}), 400
            if sheet_row < 2:
                return jsonify({"ok": False, "error": "invalid row"}), 400

            if col in CHECK_COLS and value not in ALLOWED_MARKS:
                return jsonify({"ok": False, "error": f"invalid value for {col}: {value}"}), 400

            if col in RECENT_TEXT_COLS and len(value) > 200:
                return jsonify({"ok": False, "error": f"value too long for {col}"}), 400

            updates.append({
                "range": f"{sheet}!{col}{sheet_row}",
                "values": [[value]]
            })

        svc = get_sheets_service(readonly=False)
        if updates:
            svc.spreadsheets().values().batchUpdate(
                spreadsheetId=SPREADSHEET_ID,
                body={"valueInputOption": "USER_ENTERED", "data": updates}
            ).execute()

        cache.clear()
        return jsonify({"ok": True, "applied": len(updates)})

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# =========================================================
# 3) 내신자료 생성 API
# =========================================================
@app.route("/generate/api/schools")
def generate_api_schools():
    try:
        return jsonify(read_school_list())
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/generate/api/grade_schools", methods=["POST"])
def generate_api_grade_schools():
    try:
        data = request.get_json(force=True) or {}
        return jsonify(read_grade_schools(data["grade"]))
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/generate/api/units", methods=["POST"])
def generate_api_units():
    try:
        d = request.get_json(force=True) or {}
        return jsonify(read_units_codes(d["grade"], d["school"]))
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/generate/api/unit_names", methods=["POST"])
def generate_api_unit_names():
    try:
        d = request.get_json(force=True) or {}
        return jsonify(get_unit_name_map(d["grade"], d.get("codes", [])))
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/generate/api/bundle_units", methods=["POST"])
def generate_api_bundle_units():
    try:
        d = request.get_json(force=True) or {}
        grade = str(d["grade"])
        schools = d.get("schools", [])
        if not schools:
            return jsonify({})

        school_codes = {}
        all_codes = set()

        for sch in schools:
            codes = read_units_codes(grade, sch)
            school_codes[sch] = codes
            all_codes.update(codes)

        name_map = get_unit_name_map(grade, list(all_codes))

        out = {}
        for sch, codes in school_codes.items():
            out[sch] = {
                "codes": codes,
                "names": {c: name_map.get(c, "") for c in codes}
            }

        return jsonify(out)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/generate/api/refresh_cache", methods=["POST"])
def generate_api_refresh_cache():
    try:
        with CACHE_LOCK:
            refresh_generate_cache()
            loaded_at = CACHE["loaded_at"]
        return jsonify({"ok": True, "loaded_at": loaded_at})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/generate/api/merge_all", methods=["POST"])
def generate_api_merge_all():
    try:
        d = request.get_json(force=True) or {}
        merger = PdfMerger()
        count = 0

        for unit in read_units_codes(d["grade"], d["school"]):
            for p in find_pdfs(d["type"], d["grade"], unit):
                merger.append(p)
                count += 1

        if count == 0:
            return jsonify({"error": "no_files"}), 404

        buf = io.BytesIO()
        merger.write(buf)
        buf.seek(0)
        return send_file(
            buf,
            as_attachment=True,
            download_name=f'{d["grade"]}학년_{d["school"]}_{d["type"]}_전체.pdf',
            mimetype="application/pdf"
        )
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/generate/api/merge_final", methods=["POST"])
def generate_api_merge_final():
    try:
        d = request.get_json(force=True) or {}
        grade = str(d["grade"])
        units = read_units_codes(grade, d["school"])
        nums = sorted({int(u.split("-")[0]) for u in units if "-" in u})

        folder = f"data/Final모의고사/{grade}학년"
        if not os.path.isdir(folder):
            return jsonify({"error": "folder_not_found", "folder": folder}), 404

        merger = PdfMerger()
        appended = 0

        for n in nums:
            if grade == "1" and n == 1:
                continue
            pat = re.compile(rf"{n}\s*단원")
            for f in os.listdir(folder):
                if pat.search(f):
                    merger.append(os.path.join(folder, f))
                    appended += 1
                    break

        if appended == 0:
            return jsonify({"error": "no_files"}), 404

        buf = io.BytesIO()
        merger.write(buf)
        buf.seek(0)
        return send_file(
            buf,
            as_attachment=True,
            download_name=f'{grade}학년_{d["school"]}_FINAL모의고사.pdf',
            mimetype="application/pdf"
        )
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/generate/api/merge_otoo", methods=["POST"])
def generate_api_merge_otoo():
    try:
        d = request.get_json(force=True) or {}
        grade = str(d["grade"])
        units = read_units_codes(grade, d["school"])
        nums = sorted({int(u.split("-")[0]) for u in units if "-" in u})

        folder = f"data/오투모의고사/{grade}학년"
        if not os.path.isdir(folder):
            return jsonify({"error": "folder_not_found", "folder": folder}), 404

        merger = PdfMerger()
        appended = 0

        for n in nums:
            if grade == "1" and n == 1:
                continue
            pat = re.compile(rf"{n}\s*단원")
            for f in os.listdir(folder):
                if pat.search(f):
                    merger.append(os.path.join(folder, f))
                    appended += 1
                    break

        if appended == 0:
            return jsonify({"error": "no_files"}), 404

        buf = io.BytesIO()
        merger.write(buf)
        buf.seek(0)
        return send_file(
            buf,
            as_attachment=True,
            download_name=f'{grade}학년_{d["school"]}_오투모의고사.pdf',
            mimetype="application/pdf"
        )
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# =========================================================
# 실행
# =========================================================
if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=True)