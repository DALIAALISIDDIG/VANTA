import hashlib
import hmac
import secrets
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

try:
    import requests
except ModuleNotFoundError:
    requests = None

try:
    import streamlit as st  # type: ignore
    STREAMLIT_AVAILABLE = True
except ModuleNotFoundError:
    STREAMLIT_AVAILABLE = False

    class _DummyCacheResource:
        def __call__(self, func):
            return func

    class _DummyStreamlit:
        cache_resource = _DummyCacheResource()
        secrets: Dict[str, Dict[str, str]] = {}
        session_state: Dict[str, Any] = {}

    st = _DummyStreamlit()  # type: ignore


SUPPORTED_LANGUAGES: List[Tuple[str, str]] = [
    ("ar-levant", "Arabic – Levantine"),
    ("ar-maghribi-tn", "Arabic – Maghribi (Tunisian)"),
    ("ar-sudanese", "Arabic – Sudanese"),
    ("ar-gulf", "Arabic – Gulf"),
    ("fa-af", "Dari (Afghan Persian)"),
    ("fa", "Farsi (Persian)"),
    ("ur", "Urdu"),
    ("sw", "Swahili"),
    ("ha", "Hausa"),
    ("en", "English"),
    ("de", "German"),
    ("zh", "Chinese"),
    ("es", "Spanish"),
]

DEMO_ADMIN_EMAIL = "admin@example.com"
DEMO_ADMIN_PASSWORD = "admin123"
PBKDF2_ITERATIONS = 120000
FALLBACK_ITERATIONS = 200000


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_language_label(language_code: str) -> str:
    return dict(SUPPORTED_LANGUAGES).get(language_code, language_code)


def lines_to_list(text: str) -> List[str]:
    return [line.strip() for line in text.splitlines() if line.strip()]


def list_to_lines(items: List[str]) -> str:
    return "\n".join(items)


def require_nonempty(value: str, label: str) -> Optional[str]:
    if not value or not value.strip():
        return f"{label} is required."
    return None


def _derive_digest(password: str, salt: bytes) -> Tuple[str, str]:
    password_bytes = password.encode("utf-8")
    pbkdf2 = getattr(hashlib, "pbkdf2_hmac", None)
    if callable(pbkdf2):
        digest = pbkdf2("sha256", password_bytes, salt, PBKDF2_ITERATIONS)
        return "pbkdf2_sha256", digest.hex()

    digest = salt + password_bytes
    for _ in range(FALLBACK_ITERATIONS):
        digest = hashlib.sha256(digest + salt + password_bytes).digest()
    return "sha256_iter", digest.hex()


def hash_password(password: str, salt_hex: Optional[str] = None) -> str:
    salt = bytes.fromhex(salt_hex) if salt_hex else secrets.token_bytes(16)
    algorithm, digest_hex = _derive_digest(password, salt)
    return f"{algorithm}${salt.hex()}${digest_hex}"


def verify_password(password: str, stored: str) -> bool:
    try:
        parts = stored.split("$")
        legacy_format = False
        if len(parts) == 3:
            algorithm, salt_hex, digest_hex = parts
        elif len(parts) == 2:
            legacy_format = True
            algorithm = "pbkdf2_sha256"
            salt_hex, digest_hex = parts
        else:
            return False
    except ValueError:
        return False

    salt = bytes.fromhex(salt_hex)
    derived_algorithm, candidate_digest_hex = _derive_digest(password, salt)

    if algorithm not in {"pbkdf2_sha256", "sha256_iter"}:
        return False
    if algorithm != derived_algorithm:
        if algorithm == "pbkdf2_sha256" and not callable(getattr(hashlib, "pbkdf2_hmac", None)):
            return False
        if algorithm == "sha256_iter" and callable(getattr(hashlib, "pbkdf2_hmac", None)):
            fallback_digest = salt + password.encode("utf-8")
            for _ in range(FALLBACK_ITERATIONS):
                fallback_digest = hashlib.sha256(fallback_digest + salt + password.encode("utf-8")).digest()
            candidate_digest_hex = fallback_digest.hex()
        else:
            return False

    if legacy_format:
        return hmac.compare_digest(candidate_digest_hex, digest_hex)

    candidate = f"{algorithm}${salt_hex}${candidate_digest_hex}"
    return hmac.compare_digest(candidate, stored)


def validate_task_form(payload: Dict[str, Any]) -> List[str]:
    errors = []
    for key, label in [("task_code", "Task code"), ("environment", "Environment"), ("instruction_en", "English instruction")]:
        err = require_nonempty(payload.get(key, ""), label)
        if err:
            errors.append(err)
    if payload.get("difficulty") not in {"Easy", "Medium", "Hard"}:
        errors.append("Difficulty must be Easy, Medium, or Hard.")
    return errors


def validate_translation_form(translation_text: str) -> List[str]:
    err = require_nonempty(translation_text, "Translation")
    return [err] if err else []


def validate_signup(email: str, password: str, full_name: str) -> List[str]:
    errors = []
    if require_nonempty(full_name, "Full name"):
        errors.append("Full name is required.")
    if require_nonempty(email, "Email"):
        errors.append("Email is required.")
    elif "@" not in email:
        errors.append("Email must be valid.")
    if len(password) < 8:
        errors.append("Password must be at least 8 characters.")
    return errors


class Storage:
    def list_tasks(self) -> List[Dict[str, Any]]:
        raise NotImplementedError

    def create_task(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError

    def update_task(self, task_id: str, payload: Dict[str, Any]) -> None:
        raise NotImplementedError

    def list_translations(self, task_id: Optional[str] = None) -> List[Dict[str, Any]]:
        raise NotImplementedError

    def upsert_translation(self, payload: Dict[str, Any]) -> None:
        raise NotImplementedError

    def create_user(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError

    def get_user_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        raise NotImplementedError


class InMemoryStorage(Storage):
    def __init__(self):
        self.tasks = [
            {
                "id": str(uuid.uuid4()),
                "task_code": "moodle_create_course_001",
                "environment": "Moodle LMS",
                "difficulty": "Easy",
                "instruction_en": "Create a new course named 'Intro to AI', short name 'AI101', format 'Topics'.",
                "created_by": DEMO_ADMIN_EMAIL,
                "created_at": utc_now_iso(),
            }
        ]
        self.translations: List[Dict[str, Any]] = []
        self.users = [
            {
                "id": str(uuid.uuid4()),
                "email": DEMO_ADMIN_EMAIL,
                "full_name": "Demo Admin",
                "password_hash": hash_password(DEMO_ADMIN_PASSWORD),
                "role": "admin",
                "created_at": utc_now_iso(),
            }
        ]

    def list_tasks(self) -> List[Dict[str, Any]]:
        return sorted(self.tasks, key=lambda x: x["task_code"])

    def create_task(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        row = {"id": str(uuid.uuid4()), **payload, "created_at": utc_now_iso()}
        self.tasks.append(row)
        return row

    def update_task(self, task_id: str, payload: Dict[str, Any]) -> None:
        for i, row in enumerate(self.tasks):
            if row["id"] == task_id:
                self.tasks[i] = {**row, **payload}
                return
        raise KeyError("Task not found")

    def list_translations(self, task_id: Optional[str] = None) -> List[Dict[str, Any]]:
        rows = list(self.translations)
        if task_id:
            rows = [r for r in rows if r["task_id"] == task_id]
        return sorted(rows, key=lambda x: x["updated_at"], reverse=True)

    def upsert_translation(self, payload: Dict[str, Any]) -> None:
        for i, row in enumerate(self.translations):
            if row["task_id"] == payload["task_id"] and row["language_code"] == payload["language_code"] and row["user_email"] == payload["user_email"]:
                self.translations[i] = {**row, **payload}
                return
        self.translations.append({"id": str(uuid.uuid4()), **payload})

    def create_user(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if self.get_user_by_email(payload["email"]):
            raise ValueError("User already exists")
        row = {"id": str(uuid.uuid4()), **payload, "created_at": utc_now_iso()}
        self.users.append(row)
        return row

    def get_user_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        email = email.strip().lower()
        for user in self.users:
            if user["email"].lower() == email:
                return user
        return None


class SupabaseStorage(Storage):
    def __init__(self, base_url: str, api_key: str):
        if requests is None:
            raise RuntimeError("requests is required for SupabaseStorage")
        self.base_url = base_url.rstrip("/") + "/rest/v1"
        self.headers = {
            "apikey": api_key,
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Prefer": "return=representation",
        }

    def _request(self, method: str, path: str, *, params: Optional[Dict[str, str]] = None, json_body: Any = None):
        response = requests.request(method, f"{self.base_url}/{path}", headers=self.headers, params=params, json=json_body, timeout=30)
        response.raise_for_status()
        return response

    def list_tasks(self) -> List[Dict[str, Any]]:
        return self._request("GET", "tasks", params={"select": "*", "order": "task_code.asc"}).json()

    def create_task(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._request("POST", "tasks", json_body=payload).json()[0]

    def update_task(self, task_id: str, payload: Dict[str, Any]) -> None:
        self._request("PATCH", "tasks", params={"id": f"eq.{task_id}"}, json_body=payload)

    def list_translations(self, task_id: Optional[str] = None) -> List[Dict[str, Any]]:
        params = {"select": "*", "order": "updated_at.desc"}
        if task_id:
            params["task_id"] = f"eq.{task_id}"
        return self._request("GET", "translations", params=params).json()

    def upsert_translation(self, payload: Dict[str, Any]) -> None:
        headers = dict(self.headers)
        headers["Prefer"] = "resolution=merge-duplicates,return=representation"
        response = requests.post(
            f"{self.base_url}/translations",
            headers=headers,
            params={"on_conflict": "task_id,language_code,user_email"},
            json=payload,
            timeout=30,
        )
        response.raise_for_status()

    def create_user(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._request("POST", "users", json_body=payload).json()[0]

    def get_user_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        rows = self._request("GET", "users", params={"select": "*", "email": f"eq.{email.strip().lower()}"}).json()
        return rows[0] if rows else None


@st.cache_resource
def get_storage() -> Storage:
    supabase_cfg = getattr(st, "secrets", {}).get("supabase", {}) if hasattr(st, "secrets") else {}
    url = supabase_cfg.get("url") if supabase_cfg else None
    key = supabase_cfg.get("key") if supabase_cfg else None
    if url and key:
        return SupabaseStorage(url, key)
    return InMemoryStorage()


def using_persistent_storage() -> bool:
    supabase_cfg = getattr(st, "secrets", {}).get("supabase", {}) if hasattr(st, "secrets") else {}
    return bool(supabase_cfg.get("url") and supabase_cfg.get("key"))


def normalize_task_payload(task_code: str, environment: str, difficulty: str, instruction_en: str, created_by: str) -> Dict[str, Any]:
    return {
        "task_code": task_code.strip(),
        "environment": environment.strip(),
        "difficulty": difficulty,
        "instruction_en": instruction_en.strip(),
        "created_by": created_by,
        "updated_at": utc_now_iso(),
    }


def render_cli_summary() -> None:
    print("Streamlit is not installed here.")
    print("Use this file on Streamlit Community Cloud.")
    print(f"Demo admin login: {DEMO_ADMIN_EMAIL} / {DEMO_ADMIN_PASSWORD}")


def render_auth_box() -> Optional[Dict[str, Any]]:
    storage = get_storage()
    if "user" not in st.session_state:
        st.session_state.user = None

    if st.session_state.user:
        return st.session_state.user

    login_tab, signup_tab = st.tabs(["Login", "Create account"])

    with login_tab:
        with st.form("login_form"):
            email = st.text_input("Email")
            password = st.text_input("Password", type="password")
            submitted = st.form_submit_button("Login", use_container_width=True)
        if submitted:
            user = storage.get_user_by_email(email)
            if not user or not verify_password(password, user["password_hash"]):
                st.error("Invalid email or password.")
            else:
                st.session_state.user = user
                st.rerun()

    with signup_tab:
        with st.form("signup_form"):
            full_name = st.text_input("Full name")
            email = st.text_input("Email", key="signup_email")
            password = st.text_input("Password", type="password", key="signup_password")
            role = st.selectbox("Account type", ["translator", "admin"])
            submitted = st.form_submit_button("Create account", use_container_width=True)
        if submitted:
            errors = validate_signup(email, password, full_name)
            if storage.get_user_by_email(email):
                errors.append("An account with this email already exists.")
            if errors:
                for err in errors:
                    st.error(err)
            else:
                user = storage.create_user(
                    {
                        "email": email.strip().lower(),
                        "full_name": full_name.strip(),
                        "password_hash": hash_password(password),
                        "role": role,
                    }
                )
                st.session_state.user = user
                st.success("Account created.")
                st.rerun()

    return None


def render_streamlit_app() -> None:
    st.set_page_config(page_title="Task Translation Manager", page_icon="🌍", layout="wide")
    st.title("🌍 Task Translation Manager")
    st.caption("Simple app: admins add English tasks, translators choose a language and save their translation.")

    if using_persistent_storage():
        st.success("Connected to persistent Supabase storage.")
    else:
        st.warning("Demo mode only. Add Supabase secrets on Streamlit Cloud to keep users, tasks, and translations saved.")
        st.info(f"Demo admin login: {DEMO_ADMIN_EMAIL} / {DEMO_ADMIN_PASSWORD}")

    user = render_auth_box()
    if not user:
        with st.expander("Minimal Supabase schema"):
            st.code(
                """
create table if not exists users (
  id uuid primary key default gen_random_uuid(),
  email text unique not null,
  full_name text not null,
  password_hash text not null,
  role text not null,
  created_at timestamptz not null default now()
);

create table if not exists tasks (
  id uuid primary key default gen_random_uuid(),
  task_code text unique not null,
  environment text not null,
  difficulty text not null,
  instruction_en text not null,
  created_by text not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz
);

create table if not exists translations (
  id uuid primary key default gen_random_uuid(),
  task_id uuid not null references tasks(id) on delete cascade,
  language_code text not null,
  language_label text not null,
  translation_text text not null,
  user_email text not null,
  updated_at timestamptz not null default now(),
  unique(task_id, language_code, user_email)
);
                """.strip(),
                language="sql",
            )
        return

    top_left, top_right = st.columns([0.8, 0.2])
    with top_left:
        st.write(f"Logged in as **{user['full_name']}** ({user['role']})")
    with top_right:
        if st.button("Logout", use_container_width=True):
            st.session_state.user = None
            st.rerun()

    storage = get_storage()
    tasks = storage.list_tasks()
    translations = storage.list_translations()

    tabs = ["Translate", "My translations", "Overview"]
    if user["role"] == "admin":
        tabs = ["Add English tasks"] + tabs
    pages = st.tabs(tabs)
    tab_index = 0

    if user["role"] == "admin":
        with pages[tab_index]:
            st.subheader("Add English tasks")
            existing_map = {f"{t['task_code']} — {t['environment']}": t for t in tasks}
            choice = st.selectbox("Edit existing or create new", ["Create new task"] + list(existing_map.keys()))
            selected = existing_map.get(choice)
            defaults = selected or {"task_code": "", "environment": "", "difficulty": "Easy", "instruction_en": ""}
            with st.form("task_form"):
                task_code = st.text_input("Task code", value=defaults["task_code"])
                environment = st.text_input("Environment", value=defaults["environment"])
                difficulty = st.selectbox("Difficulty", ["Easy", "Medium", "Hard"], index=["Easy", "Medium", "Hard"].index(defaults["difficulty"]))
                instruction_en = st.text_area("English instruction", value=defaults["instruction_en"], height=140)
                submitted = st.form_submit_button("Save task", use_container_width=True)
            if submitted:
                payload = {"task_code": task_code, "environment": environment, "difficulty": difficulty, "instruction_en": instruction_en}
                errors = validate_task_form(payload)
                duplicate = any(t["task_code"] == task_code.strip() and (not selected or t["id"] != selected["id"]) for t in tasks)
                if duplicate:
                    errors.append("Task code already exists.")
                if errors:
                    for err in errors:
                        st.error(err)
                else:
                    clean = normalize_task_payload(task_code, environment, difficulty, instruction_en, user["email"])
                    if selected:
                        storage.update_task(selected["id"], clean)
                        st.success("Task updated.")
                    else:
                        storage.create_task(clean)
                        st.success("Task created.")
                    st.rerun()
        tab_index += 1

    with pages[tab_index]:
        st.subheader("Translate")
        if not tasks:
            st.info("No English tasks yet.")
        else:
            task_map = {f"{t['task_code']} — {t['environment']}": t for t in tasks}
            selected_label = st.selectbox("Choose English task", list(task_map.keys()))
            task = task_map[selected_label]
            lang_code = st.selectbox("Choose target language", [code for code, _ in SUPPORTED_LANGUAGES], format_func=get_language_label)
            lang_label = get_language_label(lang_code)
            previous = None
            for row in translations:
                if row["task_id"] == task["id"] and row["language_code"] == lang_code and row["user_email"] == user["email"]:
                    previous = row
                    break
            st.markdown("**English source**")
            st.info(task["instruction_en"])
            with st.form("translation_form"):
                translation_text = st.text_area(f"Your {lang_label} translation", value=(previous or {}).get("translation_text", ""), height=180)
                submitted = st.form_submit_button("Save translation", use_container_width=True)
            if submitted:
                errors = validate_translation_form(translation_text)
                if errors:
                    for err in errors:
                        st.error(err)
                else:
                    storage.upsert_translation(
                        {
                            "task_id": task["id"],
                            "language_code": lang_code,
                            "language_label": lang_label,
                            "translation_text": translation_text.strip(),
                            "user_email": user["email"],
                            "updated_at": utc_now_iso(),
                        }
                    )
                    st.success("Translation saved.")
                    st.rerun()
    tab_index += 1

    with pages[tab_index]:
        st.subheader("My translations")
        my_rows = [r for r in translations if r["user_email"] == user["email"]]
        if not my_rows:
            st.info("You have not saved any translations yet.")
        else:
            for row in my_rows:
                task = next((t for t in tasks if t["id"] == row["task_id"]), None)
                with st.container(border=True):
                    st.write(f"**{task['task_code'] if task else row['task_id']}** — {row['language_label']}")
                    st.write(row["translation_text"])
                    st.caption(f"Updated: {row['updated_at']}")
    tab_index += 1

    with pages[tab_index]:
        st.subheader("Overview")
        st.metric("Tasks", len(tasks))
        st.metric("Translations", len(translations))
        if tasks:
            for task in tasks:
                langs = [row["language_label"] for row in translations if row["task_id"] == task["id"]]
                with st.container(border=True):
                    st.write(f"**{task['task_code']}** — {task['environment']}")
                    st.caption(task["instruction_en"])
                    st.write("Translated into: " + (", ".join(sorted(set(langs))) if langs else "None yet"))


def _run_self_tests() -> None:
    assert get_language_label("ar-sudanese") == "Arabic – Sudanese"
    assert get_language_label("xx") == "xx"
    assert lines_to_list("a\n\n b ") == ["a", "b"]
    assert list_to_lines(["a", "b"]) == "a\nb"
    assert require_nonempty("", "Email") == "Email is required."
    assert require_nonempty("ok", "Email") is None

    pw = hash_password("secret123")
    parts = pw.split("$")
    assert len(parts) == 3
    assert parts[0] in {"pbkdf2_sha256", "sha256_iter"}
    assert verify_password("secret123", pw)
    assert not verify_password("wrong", pw)

    legacy_pw = "$".join(pw.split("$")[1:])
    if pw.startswith("pbkdf2_sha256$"):
        assert verify_password("secret123", legacy_pw)
        assert not verify_password("wrong", legacy_pw)
    else:
        assert not verify_password("secret123", legacy_pw)

    signup_errors = validate_signup("user@example.com", "password1", "Test User")
    assert signup_errors == []
    assert any("Password must be at least 8 characters." == e for e in validate_signup("user@example.com", "123", "Test User"))

    valid_task = {"task_code": "task_1", "environment": "Moodle", "difficulty": "Easy", "instruction_en": "Do it"}
    assert validate_task_form(valid_task) == []
    invalid_task = validate_task_form({"task_code": "", "environment": "", "difficulty": "Bad", "instruction_en": ""})
    assert any("Task code is required." == e for e in invalid_task)
    assert any("Environment is required." == e for e in invalid_task)
    assert any("English instruction is required." == e for e in invalid_task)
    assert any("Difficulty must be Easy, Medium, or Hard." == e for e in invalid_task)

    assert validate_translation_form("Hallo") == []
    assert validate_translation_form("") == ["Translation is required."]

    storage = InMemoryStorage()
    admin = storage.get_user_by_email(DEMO_ADMIN_EMAIL)
    assert admin is not None
    assert verify_password(DEMO_ADMIN_PASSWORD, admin["password_hash"])

    user = storage.create_user({
        "email": "translator@example.com",
        "full_name": "Translator",
        "password_hash": hash_password("password123"),
        "role": "translator",
    })
    assert storage.get_user_by_email("translator@example.com")["id"] == user["id"]

    created = storage.create_task(normalize_task_payload("task_x", "HR", "Medium", "Review this application", admin["email"]))
    assert any(t["task_code"] == "task_x" for t in storage.list_tasks())

    storage.upsert_translation({
        "task_id": created["id"],
        "language_code": "de",
        "language_label": "German",
        "translation_text": "Prüfen Sie diese Bewerbung",
        "user_email": user["email"],
        "updated_at": utc_now_iso(),
    })
    storage.upsert_translation({
        "task_id": created["id"],
        "language_code": "de",
        "language_label": "German",
        "translation_text": "Überarbeitete Übersetzung",
        "user_email": user["email"],
        "updated_at": utc_now_iso(),
    })
    rows = storage.list_translations(created["id"])
    assert len([r for r in rows if r["user_email"] == user["email"] and r["language_code"] == "de"]) == 1
    assert any(r["translation_text"] == "Überarbeitete Übersetzung" for r in rows)


if __name__ == "__main__":
    run_self_tests = False
    try:
        run_self_tests = bool(getattr(st, "secrets", {}).get("app", {}).get("run_self_tests", False))
    except Exception:
        run_self_tests = False

    if not STREAMLIT_AVAILABLE or run_self_tests:
        _run_self_tests()

    if STREAMLIT_AVAILABLE:
        render_streamlit_app()
    else:
        render_cli_summary()
