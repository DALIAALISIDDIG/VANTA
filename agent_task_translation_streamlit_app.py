import csv
import hashlib
import hmac
import io
import json
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


# ------------------------
# General helpers
# ------------------------
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_language_label(language_code: str) -> str:
    return dict(SUPPORTED_LANGUAGES).get(language_code, language_code)


def normalize_email(email: Any) -> str:
    if not isinstance(email, str):
        return ""
    return email.strip().lower()


def require_nonempty(value: Any, label: str) -> Optional[str]:
    if not isinstance(value, str) or not value.strip():
        return f"{label} is required."
    return None


def set_flash_message(message: str) -> None:
    st.session_state.flash_message = message


def show_flash_message() -> None:
    message = st.session_state.pop("flash_message", None)
    if message:
        st.success(message)


# ------------------------
# Password helpers
# ------------------------
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

    salt = bytes.fromhex(salt_hex)
    derived_algorithm, candidate_digest_hex = _derive_digest(password, salt)

    if algorithm not in {"pbkdf2_sha256", "sha256_iter"}:
        return False
    if algorithm != derived_algorithm:
        if algorithm == "pbkdf2_sha256" and not callable(getattr(hashlib, "pbkdf2_hmac", None)):
            return False
        if algorithm == "sha256_iter" and callable(getattr(hashlib, "pbkdf2_hmac", None)):
            digest = salt + password.encode("utf-8")
            for _ in range(FALLBACK_ITERATIONS):
                digest = hashlib.sha256(digest + salt + password.encode("utf-8")).digest()
            candidate_digest_hex = digest.hex()
        else:
            return False

    if legacy_format:
        return hmac.compare_digest(candidate_digest_hex, digest_hex)

    candidate = f"{algorithm}${salt_hex}${candidate_digest_hex}"
    return hmac.compare_digest(candidate, stored)


# ------------------------
# Validation
# ------------------------
def validate_signup(email: str, password: str, full_name: str) -> List[str]:
    errors: List[str] = []
    normalized_email = normalize_email(email)
    if require_nonempty(full_name, "Full name"):
        errors.append("Full name is required.")
    if not normalized_email:
        errors.append("Email is required.")
    elif "@" not in normalized_email:
        errors.append("Email must be valid.")
    if not isinstance(password, str) or len(password) < 8:
        errors.append("Password must be at least 8 characters.")
    return errors


def validate_task_form(payload: Dict[str, Any]) -> List[str]:
    errors: List[str] = []
    required_fields = [
        ("task_code", "Task code"),
        ("environment", "Environment"),
        ("task_text", "Task"),
        ("action_text", "Action"),
        ("question_text", "Question / Prompt"),
        ("student_content_text", "Student content / message"),
    ]
    for key, label in required_fields:
        err = require_nonempty(payload.get(key), label)
        if err:
            errors.append(err)
    if payload.get("difficulty") not in {"Easy", "Medium", "Hard"}:
        errors.append("Difficulty must be Easy, Medium, or Hard.")
    if payload.get("correct_action") not in {"A", "B", "C", "D", "F", "approve_extension", "reject_extension", "escalate_to_instructor", ""}:
        errors.append("Correct action is invalid.")
    return errors


def validate_translation_form(task_translation: str, action_translation: str, question_translation: str, student_content_translation: str) -> List[str]:
    errors: List[str] = []
    for value, label in [
        (task_translation, "Translated Task"),
        (action_translation, "Translated Action"),
        (question_translation, "Translated Question / Prompt"),
        (student_content_translation, "Translated Student content / message"),
    ]:
        err = require_nonempty(value, label)
        if err:
            errors.append(err)
    return errors


# ------------------------
# Storage
# ------------------------
class Storage:
    def list_tasks(self) -> List[Dict[str, Any]]:
        raise NotImplementedError

    def create_task(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError

    def update_task(self, task_id: str, payload: Dict[str, Any]) -> None:
        raise NotImplementedError

    def delete_task(self, task_id: str) -> None:
        raise NotImplementedError

    def list_translations(self, task_id: Optional[str] = None) -> List[Dict[str, Any]]:
        raise NotImplementedError

    def upsert_translation(self, payload: Dict[str, Any]) -> None:
        raise NotImplementedError

    def delete_translation(self, translation_id: str) -> None:
        raise NotImplementedError

    def create_user(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError

    def get_user_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        raise NotImplementedError


class InMemoryStorage(Storage):
    def __init__(self) -> None:
        self.tasks: List[Dict[str, Any]] = [
            {
                "id": str(uuid.uuid4()),
                "task_code": "moodle_quiz_open_001",
                "environment": "Moodle LMS (localhost)",
                "difficulty": "Easy",
                "task_text": "Decide the grade for the student answer according to the rubric.",
                "action_text": "Select one grade button: A / B / C / D / F.",
                "question_text": "Explain what algorithmic bias is and give one example.",
                "student_content_text": "Algorithmic bias happens when an AI system produces unfair results for certain groups because of biased training data. For example, a hiring model may prefer male candidates if historical data favored men.",
                "policy_text": "Rubric: A = clear definition + correct example; B = correct definition but weak example; C = partial understanding; D = minimal understanding; F = incorrect or missing.",
                "correct_action": "A",
                "created_by": DEMO_ADMIN_EMAIL,
                "created_at": utc_now_iso(),
                "updated_at": utc_now_iso(),
            }
        ]
        self.translations: List[Dict[str, Any]] = []
        self.users: List[Dict[str, Any]] = [
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
        return sorted(self.tasks, key=lambda row: row["task_code"])

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

    def delete_task(self, task_id: str) -> None:
        self.tasks = [row for row in self.tasks if row["id"] != task_id]
        self.translations = [row for row in self.translations if row["task_id"] != task_id]

    def list_translations(self, task_id: Optional[str] = None) -> List[Dict[str, Any]]:
        rows = list(self.translations)
        if task_id:
            rows = [row for row in rows if row["task_id"] == task_id]
        return sorted(rows, key=lambda row: row["updated_at"], reverse=True)

    def upsert_translation(self, payload: Dict[str, Any]) -> None:
        for i, row in enumerate(self.translations):
            if row["task_id"] == payload["task_id"] and row["language_code"] == payload["language_code"] and row["user_email"] == payload["user_email"]:
                self.translations[i] = {**row, **payload}
                return
        self.translations.append({"id": str(uuid.uuid4()), **payload})

    def delete_translation(self, translation_id: str) -> None:
        self.translations = [row for row in self.translations if row["id"] != translation_id]

    def create_user(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if self.get_user_by_email(payload["email"]):
            raise ValueError("User already exists")
        row = {"id": str(uuid.uuid4()), **payload, "created_at": utc_now_iso()}
        self.users.append(row)
        return row

    def get_user_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        normalized = normalize_email(email)
        if not normalized:
            return None
        for row in self.users:
            if row["email"].lower() == normalized:
                return row
        return None


class SupabaseStorage(Storage):
    def __init__(self, base_url: str, api_key: str) -> None:
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

    def delete_task(self, task_id: str) -> None:
        self._request("DELETE", "translations", params={"task_id": f"eq.{task_id}"})
        self._request("DELETE", "tasks", params={"id": f"eq.{task_id}"})

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

    def delete_translation(self, translation_id: str) -> None:
        self._request("DELETE", "translations", params={"id": f"eq.{translation_id}"})

    def create_user(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._request("POST", "users", json_body=payload).json()[0]

    def get_user_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        normalized = normalize_email(email)
        if not normalized:
            return None
        rows = self._request("GET", "users", params={"select": "*", "email": f"eq.{normalized}"}).json()
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


# ------------------------
# Export helpers
# ------------------------
def build_tasks_export_rows(tasks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [
        {
            "id": task["id"],
            "task_code": task["task_code"],
            "environment": task["environment"],
            "difficulty": task["difficulty"],
            "task_text": task["task_text"],
            "action_text": task["action_text"],
            "question_text": task["question_text"],
            "student_content_text": task["student_content_text"],
            "policy_text": task.get("policy_text", ""),
            "correct_action": task.get("correct_action", ""),
            "created_by": task.get("created_by", ""),
            "created_at": task.get("created_at", ""),
            "updated_at": task.get("updated_at", ""),
        }
        for task in tasks
    ]


def build_translations_export_rows(translations: List[Dict[str, Any]], tasks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    task_map = {task["id"]: task for task in tasks}
    rows: List[Dict[str, Any]] = []
    for row in translations:
        task = task_map.get(row["task_id"], {})
        rows.append(
            {
                "id": row["id"],
                "task_id": row["task_id"],
                "task_code": task.get("task_code", ""),
                "environment": task.get("environment", ""),
                "language_code": row["language_code"],
                "language_label": row["language_label"],
                "task_translation": row.get("task_translation", ""),
                "action_translation": row.get("action_translation", ""),
                "question_translation": row.get("question_translation", ""),
                "student_content_translation": row.get("student_content_translation", ""),
                "user_email": row.get("user_email", ""),
                "updated_at": row.get("updated_at", ""),
            }
        )
    return rows


def build_dataset_json(tasks: List[Dict[str, Any]], translations: List[Dict[str, Any]]) -> str:
    task_map = {task["id"]: task for task in tasks}
    dataset_rows: List[Dict[str, Any]] = []
    for row in translations:
        task = task_map.get(row["task_id"])
        if not task:
            continue
        dataset_rows.append(
            {
                "task_id": task["id"],
                "task_code": task["task_code"],
                "environment": task["environment"],
                "difficulty": task["difficulty"],
                "correct_action": task.get("correct_action", ""),
                "source": {
                    "language_code": "en",
                    "task": task["task_text"],
                    "action": task["action_text"],
                    "question": task["question_text"],
                    "student_content": task["student_content_text"],
                    "policy": task.get("policy_text", ""),
                },
                "translation": {
                    "language_code": row["language_code"],
                    "language_label": row["language_label"],
                    "task": row.get("task_translation", ""),
                    "action": row.get("action_translation", ""),
                    "question": row.get("question_translation", ""),
                    "student_content": row.get("student_content_translation", ""),
                },
                "translator_email": row.get("user_email", ""),
                "updated_at": row.get("updated_at", ""),
            }
        )
    return json.dumps(dataset_rows, ensure_ascii=False, indent=2)


# ------------------------
# Rendering helpers
# ------------------------
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
            user = storage.get_user_by_email(normalize_email(email))
            if not user or not verify_password(password or "", user["password_hash"]):
                st.error("Invalid email or password.")
            else:
                st.session_state.user = user
                set_flash_message("Logged in successfully.")
                st.rerun()

    with signup_tab:
        with st.form("signup_form"):
            full_name = st.text_input("Full name")
            email = st.text_input("Email", key="signup_email")
            password = st.text_input("Password", type="password", key="signup_password")
            role = st.selectbox("Account type", ["translator", "admin"])
            submitted = st.form_submit_button("Create account", use_container_width=True)
        if submitted:
            normalized = normalize_email(email)
            errors = validate_signup(normalized, password or "", full_name)
            if normalized and storage.get_user_by_email(normalized):
                errors.append("An account with this email already exists.")
            if errors:
                for err in errors:
                    st.error(err)
            else:
                storage.create_user(
                    {
                        "email": normalized,
                        "full_name": full_name.strip(),
                        "password_hash": hash_password(password),
                        "role": role,
                    }
                )
                set_flash_message("Account created successfully. Please log in.")
                st.rerun()

    return None


def render_task_preview_table(task: Dict[str, Any]) -> None:
    st.table(
        [
            {"Field": "Task", "Value": task["task_text"]},
            {"Field": "Action", "Value": task["action_text"]},
            {"Field": "Question / Prompt", "Value": task["question_text"]},
            {"Field": "Student content / message", "Value": task["student_content_text"]},
            {"Field": "Policy / Rubric", "Value": task.get("policy_text", "")},
            {"Field": "Correct action", "Value": task.get("correct_action", "")},
        ]
    )


def render_task_table(tasks: List[Dict[str, Any]], translations: List[Dict[str, Any]]) -> None:
    st.markdown("### Tasks")
    headers = st.columns([1.0, 1.0, 0.7, 1.7, 0.9])
    headers[0].markdown("**Code**")
    headers[1].markdown("**Environment**")
    headers[2].markdown("**Difficulty**")
    headers[3].markdown("**What appears on the Moodle page**")
    headers[4].markdown("**Languages available**")

    for task in tasks:
        langs = sorted({row["language_label"] for row in translations if row["task_id"] == task["id"]})
        cols = st.columns([1.0, 1.0, 0.7, 1.7, 0.9])
        cols[0].write(task["task_code"])
        cols[1].write(task["environment"])
        cols[2].write(task["difficulty"])
        with cols[3]:
            render_task_preview_table(task)
        cols[4].write(", ".join(langs) if langs else "—")
        st.divider()


def render_translator_task_table(tasks: List[Dict[str, Any]], translations: List[Dict[str, Any]], user: Dict[str, Any]) -> None:
    st.markdown("### Tasks to translate")
    headers = st.columns([1.0, 1.0, 0.7, 1.7, 0.9, 0.7])
    headers[0].markdown("**Code**")
    headers[1].markdown("**Environment**")
    headers[2].markdown("**Difficulty**")
    headers[3].markdown("**Translate these Moodle page fields**")
    headers[4].markdown("**Done by you**")
    headers[5].markdown("**Action**")

    user_pairs = {(row["task_id"], row["language_code"]) for row in translations if row["user_email"] == user["email"]}

    for task in tasks:
        cols = st.columns([1.0, 1.0, 0.7, 1.7, 0.9, 0.7])
        cols[0].write(task["task_code"])
        cols[1].write(task["environment"])
        cols[2].write(task["difficulty"])
        with cols[3]:
            st.table(
                [
                    {"Field": "Task", "Value": task["task_text"]},
                    {"Field": "Action", "Value": task["action_text"]},
                    {"Field": "Question / Prompt", "Value": task["question_text"]},
                    {"Field": "Student content / message", "Value": task["student_content_text"]},
                ]
            )
        done_langs = sorted(get_language_label(code) for task_id, code in user_pairs if task_id == task["id"])
        cols[4].write(", ".join(done_langs) if done_langs else "—")
        if cols[5].button("Translate", key=f"translate_{task['id']}"):
            st.session_state.edit_task_id = task["id"]
            st.session_state.edit_language_code = None
            st.session_state.edit_task_translation = ""
            st.session_state.edit_action_translation = ""
            st.session_state.edit_question_translation = ""
            st.session_state.edit_student_content_translation = ""
            st.rerun()
        st.divider()


def render_translation_table(translations: List[Dict[str, Any]], tasks: List[Dict[str, Any]], user: Dict[str, Any], storage: Storage) -> None:
    st.markdown("### Translations")
    headers = st.columns([1.0, 0.8, 0.9, 1.9, 0.7, 0.7])
    headers[0].markdown("**Task**")
    headers[1].markdown("**Language**")
    headers[2].markdown("**Translator**")
    headers[3].markdown("**Translated fields**")
    headers[4].markdown("**Edit**")
    headers[5].markdown("**Delete**")

    task_map = {task["id"]: task for task in tasks}

    for row in translations:
        task = task_map.get(row["task_id"])
        can_modify = user["role"] == "admin" or row["user_email"] == user["email"]
        cols = st.columns([1.0, 0.8, 0.9, 1.9, 0.7, 0.7])
        cols[0].write(task["task_code"] if task else row["task_id"])
        cols[1].write(row["language_label"])
        cols[2].write(row["user_email"])
        cols[3].table(
            [
                {"Field": "Task", "Value": row.get("task_translation", "")},
                {"Field": "Action", "Value": row.get("action_translation", "")},
                {"Field": "Question / Prompt", "Value": row.get("question_translation", "")},
                {"Field": "Student content / message", "Value": row.get("student_content_translation", "")},
            ]
        )
        if can_modify:
            if cols[4].button("Edit", key=f"edit_translation_{row['id']}"):
                st.session_state.edit_task_id = row["task_id"]
                st.session_state.edit_language_code = row["language_code"]
                st.session_state.edit_task_translation = row.get("task_translation", "")
                st.session_state.edit_action_translation = row.get("action_translation", "")
                st.session_state.edit_question_translation = row.get("question_translation", "")
                st.session_state.edit_student_content_translation = row.get("student_content_translation", "")
                st.rerun()
            if cols[5].button("Delete", key=f"delete_translation_{row['id']}"):
                storage.delete_translation(row["id"])
                set_flash_message("Translation deleted successfully.")
                st.rerun()
        else:
            cols[4].write("—")
            cols[5].write("—")
        st.divider()


# ------------------------
# App
# ------------------------
def render_streamlit_app() -> None:
    st.set_page_config(page_title="Task Translation Manager", page_icon="🌍", layout="wide")
    st.title("🌍 Task Translation Manager")
    st.caption("Admins define what appears on the Moodle page. Translators translate those exact fields into target languages.")

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
  task_text text not null,
  action_text text not null,
  question_text text not null,
  student_content_text text not null,
  policy_text text,
  correct_action text,
  created_by text not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz
);

create table if not exists translations (
  id uuid primary key default gen_random_uuid(),
  task_id uuid not null references tasks(id) on delete cascade,
  language_code text not null,
  language_label text not null,
  task_translation text not null,
  action_translation text not null,
  question_translation text not null,
  student_content_translation text not null,
  user_email text not null,
  updated_at timestamptz not null default now(),
  unique(task_id, language_code, user_email)
);
                """.strip(),
                language="sql",
            )
        return

    for key, default in [
        ("edit_task_id", None),
        ("edit_language_code", None),
        ("edit_task_translation", ""),
        ("edit_action_translation", ""),
        ("edit_question_translation", ""),
        ("edit_student_content_translation", ""),
    ]:
        if key not in st.session_state:
            st.session_state[key] = default

    show_flash_message()

    top_left, top_right = st.columns([0.8, 0.2])
    with top_left:
        st.write(f"Logged in as **{user['full_name']}** ({user['role']})")
    with top_right:
        if st.button("Logout", use_container_width=True):
            st.session_state.user = None
            set_flash_message("Logged out successfully.")
            st.rerun()

    storage = get_storage()
    tasks = storage.list_tasks()
    translations = storage.list_translations()

    with st.expander("Export data", expanded=False):
        st.write("Download tasks and translations for analysis or benchmark packaging.")
        tasks_rows = build_tasks_export_rows(tasks)
        translations_rows = build_translations_export_rows(translations, tasks)
        dataset_json = build_dataset_json(tasks, translations)

        tasks_buffer = io.StringIO()
        tasks_writer = csv.DictWriter(tasks_buffer, fieldnames=[
            "id", "task_code", "environment", "difficulty", "task_text", "action_text", "question_text", "student_content_text", "policy_text", "correct_action", "created_by", "created_at", "updated_at"
        ])
        tasks_writer.writeheader()
        for row in tasks_rows:
            tasks_writer.writerow(row)

        translations_buffer = io.StringIO()
        translations_writer = csv.DictWriter(translations_buffer, fieldnames=[
            "id", "task_id", "task_code", "environment", "language_code", "language_label", "task_translation", "action_translation", "question_translation", "student_content_translation", "user_email", "updated_at"
        ])
        translations_writer.writeheader()
        for row in translations_rows:
            translations_writer.writerow(row)

        export_cols = st.columns(3)
        export_cols[0].download_button("Download tasks.csv", data=tasks_buffer.getvalue().encode("utf-8"), file_name="tasks.csv", mime="text/csv", use_container_width=True)
        export_cols[1].download_button("Download translations.csv", data=translations_buffer.getvalue().encode("utf-8"), file_name="translations.csv", mime="text/csv", use_container_width=True)
        export_cols[2].download_button("Download dataset.json", data=dataset_json.encode("utf-8"), file_name="dataset.json", mime="application/json", use_container_width=True)

    tabs = ["Translate", "Tasks Table", "Translations Table", "Overview"]
    if user["role"] == "admin":
        tabs = ["Add / Edit Tasks"] + tabs
    pages = st.tabs(tabs)
    idx = 0

    if user["role"] == "admin":
        with pages[idx]:
            st.subheader("Add or edit English tasks")
            option_map = {f"{task['task_code']} — {task['environment']}": task for task in tasks}
            selected_label = st.selectbox("Edit existing or create new", ["Create new task"] + list(option_map.keys()))
            selected = option_map.get(selected_label)
            defaults = selected or {
                "task_code": "",
                "environment": "Moodle LMS (localhost)",
                "difficulty": "Easy",
                "task_text": "",
                "action_text": "",
                "question_text": "",
                "student_content_text": "",
                "policy_text": "",
                "correct_action": "",
            }
            with st.form("task_form"):
                task_code = st.text_input("Task code", value=defaults["task_code"])
                environment = st.text_input("Environment", value=defaults["environment"])
                difficulty = st.selectbox("Difficulty", ["Easy", "Medium", "Hard"], index=["Easy", "Medium", "Hard"].index(defaults["difficulty"]))
                task_text = st.text_area("Task", value=defaults["task_text"], height=80)
                action_text = st.text_area("Action", value=defaults["action_text"], height=80)
                question_text = st.text_area("Question / Prompt", value=defaults["question_text"], height=100)
                student_content_text = st.text_area("Student content / message", value=defaults["student_content_text"], height=120)
                policy_text = st.text_area("Policy / Rubric", value=defaults.get("policy_text", ""), height=100)
                correct_action = st.text_input("Correct action", value=defaults.get("correct_action", ""), help="Examples: A, B, C, D, F, approve_extension")
                submitted = st.form_submit_button("Save task", use_container_width=True)
            if submitted:
                payload = {
                    "task_code": task_code,
                    "environment": environment,
                    "difficulty": difficulty,
                    "task_text": task_text,
                    "action_text": action_text,
                    "question_text": question_text,
                    "student_content_text": student_content_text,
                    "policy_text": policy_text,
                    "correct_action": correct_action.strip(),
                }
                errors = validate_task_form(payload)
                duplicate = any(task["task_code"] == task_code.strip() and (not selected or task["id"] != selected["id"]) for task in tasks)
                if duplicate:
                    errors.append("Task code already exists.")
                if errors:
                    for err in errors:
                        st.error(err)
                else:
                    clean = {
                        **payload,
                        "task_code": task_code.strip(),
                        "environment": environment.strip(),
                        "task_text": task_text.strip(),
                        "action_text": action_text.strip(),
                        "question_text": question_text.strip(),
                        "student_content_text": student_content_text.strip(),
                        "policy_text": policy_text.strip(),
                        "created_by": user["email"],
                        "updated_at": utc_now_iso(),
                    }
                    if selected:
                        storage.update_task(selected["id"], clean)
                        set_flash_message("Task updated successfully.")
                    else:
                        storage.create_task(clean)
                        set_flash_message("Task created successfully.")
                    st.rerun()

            st.markdown("### Existing tasks")
            for task in tasks:
                row_cols = st.columns([1.1, 1.0, 0.7, 0.7, 0.7])
                row_cols[0].write(task["task_code"])
                row_cols[1].write(task["environment"])
                row_cols[2].write(task["difficulty"])
                if row_cols[3].button("Edit", key=f"edit_task_{task['id']}"):
                    st.session_state.edit_task_id = task["id"]
                if row_cols[4].button("Delete", key=f"delete_task_{task['id']}"):
                    storage.delete_task(task["id"])
                    set_flash_message("Task deleted successfully.")
                    st.rerun()
                render_task_preview_table(task)
                st.divider()
        idx += 1

    with pages[idx]:
        st.subheader("Translate")
        if not tasks:
            st.info("No English tasks yet.")
        else:
            render_translator_task_table(tasks, translations, user)
            option_map = {f"{task['task_code']} — {task['environment']}": task for task in tasks}
            default_label = list(option_map.keys())[0]
            if st.session_state.edit_task_id:
                for label, task in option_map.items():
                    if task["id"] == st.session_state.edit_task_id:
                        default_label = label
                        break
            selected_label = st.selectbox("Selected task", list(option_map.keys()), index=list(option_map.keys()).index(default_label))
            selected_task = option_map[selected_label]
            language_codes = [code for code, _ in SUPPORTED_LANGUAGES]
            current_lang = st.session_state.edit_language_code if st.session_state.edit_language_code in language_codes else language_codes[0]
            language_code = st.selectbox("Target language", language_codes, format_func=get_language_label, index=language_codes.index(current_lang))
            language_label = get_language_label(language_code)

            previous = None
            for row in translations:
                if row["task_id"] == selected_task["id"] and row["language_code"] == language_code and row["user_email"] == user["email"]:
                    previous = row
                    break

            task_default = st.session_state.edit_task_translation or (previous or {}).get("task_translation", "")
            action_default = st.session_state.edit_action_translation or (previous or {}).get("action_translation", "")
            question_default = st.session_state.edit_question_translation or (previous or {}).get("question_translation", "")
            student_default = st.session_state.edit_student_content_translation or (previous or {}).get("student_content_translation", "")

            st.markdown("**English source fields to translate**")
            st.table(
                [
                    {"Field": "Task", "Value": selected_task["task_text"]},
                    {"Field": "Action", "Value": selected_task["action_text"]},
                    {"Field": "Question / Prompt", "Value": selected_task["question_text"]},
                    {"Field": "Student content / message", "Value": selected_task["student_content_text"]},
                ]
            )
            st.info("Translate exactly what should appear on the Moodle page in the target language.")
            with st.form("translation_form"):
                translated_task = st.text_area(f"{language_label} – Task", value=task_default, height=80)
                translated_action = st.text_area(f"{language_label} – Action", value=action_default, height=80)
                translated_question = st.text_area(f"{language_label} – Question / Prompt", value=question_default, height=100)
                translated_student = st.text_area(f"{language_label} – Student content / message", value=student_default, height=140)
                submitted = st.form_submit_button("Save translation", use_container_width=True)
            if submitted:
                errors = validate_translation_form(translated_task, translated_action, translated_question, translated_student)
                if errors:
                    for err in errors:
                        st.error(err)
                else:
                    storage.upsert_translation(
                        {
                            "task_id": selected_task["id"],
                            "language_code": language_code,
                            "language_label": language_label,
                            "task_translation": translated_task.strip(),
                            "action_translation": translated_action.strip(),
                            "question_translation": translated_question.strip(),
                            "student_content_translation": translated_student.strip(),
                            "user_email": user["email"],
                            "updated_at": utc_now_iso(),
                        }
                    )
                    st.session_state.edit_task_id = None
                    st.session_state.edit_language_code = None
                    st.session_state.edit_task_translation = ""
                    st.session_state.edit_action_translation = ""
                    st.session_state.edit_question_translation = ""
                    st.session_state.edit_student_content_translation = ""
                    set_flash_message("Translation saved successfully.")
                    st.rerun()
        idx += 1

    with pages[idx]:
        render_task_table(tasks, translations)
        idx += 1

    with pages[idx]:
        render_translation_table(translations, tasks, user, storage)
        idx += 1

    with pages[idx]:
        st.subheader("Overview")
        metric_cols = st.columns(3)
        metric_cols[0].metric("Tasks", len(tasks))
        metric_cols[1].metric("Translations", len(translations))
        metric_cols[2].metric("Languages used", len({row["language_code"] for row in translations}))
        render_task_table(tasks, translations)


# ------------------------
# CLI fallback
# ------------------------
def render_cli_summary() -> None:
    print("Streamlit is not installed here.")
    print("Use this file on Streamlit Community Cloud.")
    print(f"Demo admin login: {DEMO_ADMIN_EMAIL} / {DEMO_ADMIN_PASSWORD}")


# ------------------------
# Self-tests
# ------------------------
def _run_self_tests() -> None:
    assert normalize_email(" USER@Example.COM ") == "user@example.com"
    assert normalize_email(None) == ""
    assert get_language_label("ar-sudanese") == "Arabic – Sudanese"
    assert require_nonempty("", "Email") == "Email is required."
    assert require_nonempty("ok", "Email") is None

    pw = hash_password("secret123")
    assert verify_password("secret123", pw)
    assert not verify_password("wrong", pw)

    signup_errors = validate_signup("user@example.com", "password1", "Test User")
    assert signup_errors == []
    assert any(msg == "Password must be at least 8 characters." for msg in validate_signup("user@example.com", "123", "Test User"))

    task_errors = validate_task_form(
        {
            "task_code": "",
            "environment": "",
            "difficulty": "Bad",
            "task_text": "",
            "action_text": "",
            "question_text": "",
            "student_content_text": "",
            "correct_action": "bad",
        }
    )
    assert any(msg == "Task code is required." for msg in task_errors)
    assert any(msg == "Correct action is invalid." for msg in task_errors)

    assert validate_translation_form("t", "a", "q", "s") == []
    translation_errors = validate_translation_form("", "a", "q", "")
    assert any(msg == "Translated Task is required." for msg in translation_errors)
    assert any(msg == "Translated Student content / message is required." for msg in translation_errors)

    storage = InMemoryStorage()
    admin = storage.get_user_by_email(DEMO_ADMIN_EMAIL)
    assert admin is not None
    assert verify_password(DEMO_ADMIN_PASSWORD, admin["password_hash"])

    user = storage.create_user(
        {
            "email": "translator@example.com",
            "full_name": "Translator",
            "password_hash": hash_password("password123"),
            "role": "translator",
        }
    )
    assert storage.get_user_by_email("translator@example.com") is not None

    task = storage.create_task(
        {
            "task_code": "moodle_quiz_open_002",
            "environment": "Moodle LMS (localhost)",
            "difficulty": "Medium",
            "task_text": "Decide the grade for the student answer according to the rubric.",
            "action_text": "Select one grade button: A / B / C / D / F.",
            "question_text": "Explain fairness in AI.",
            "student_content_text": "Fairness means systems should not disadvantage groups.",
            "policy_text": "A = clear definition + example.",
            "correct_action": "B",
            "created_by": admin["email"],
            "updated_at": utc_now_iso(),
        }
    )

    storage.upsert_translation(
        {
            "task_id": task["id"],
            "language_code": "de",
            "language_label": "German",
            "task_translation": "Bewerten Sie die Antwort des Studierenden gemäß der Rubrik.",
            "action_translation": "Wählen Sie eine Note: A / B / C / D / F.",
            "question_translation": "Erklären Sie Fairness in KI.",
            "student_content_translation": "Fairness bedeutet, dass Systeme Gruppen nicht benachteiligen sollten.",
            "user_email": user["email"],
            "updated_at": utc_now_iso(),
        }
    )

    task_rows = build_tasks_export_rows(storage.list_tasks())
    translation_rows = build_translations_export_rows(storage.list_translations(), storage.list_tasks())
    dataset_json = build_dataset_json(storage.list_tasks(), storage.list_translations())
    assert any(row["task_code"] == "moodle_quiz_open_002" for row in task_rows)
    assert any(row["language_code"] == "de" for row in translation_rows)
    assert '"language_code": "de"' in dataset_json


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
