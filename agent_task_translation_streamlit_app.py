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

SUPPORTED_ENVIRONMENTS: List[str] = [
    "HR",
    "Healthcare",
    "Education",
    "Moodle LMS",
    "Admissions",
    "Customer Service",
    "Public Services",
    "Legal",
    "Other",
]

SUPPORTED_DIFFICULTIES: List[str] = ["Easy", "Medium", "Hard"]

DEMO_ADMIN_EMAIL = "admin@example.com"
DEMO_ADMIN_PASSWORD = "admin123"
PBKDF2_ITERATIONS = 120000
FALLBACK_ITERATIONS = 200000


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_language_label(language_code: str) -> str:
    return dict(SUPPORTED_LANGUAGES).get(language_code, language_code)


def require_nonempty(value: str, label: str) -> Optional[str]:
    if not isinstance(value, str) or not value.strip():
        return f"{label} is required."
    return None


def normalize_email(email: Any) -> str:
    if not isinstance(email, str):
        return ""
    return email.strip().lower()


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
                fallback_digest = hashlib.sha256(
                    fallback_digest + salt + password.encode("utf-8")
                ).digest()
            candidate_digest_hex = fallback_digest.hex()
        else:
            return False

    if legacy_format:
        return hmac.compare_digest(candidate_digest_hex, digest_hex)

    candidate = f"{algorithm}${salt_hex}${candidate_digest_hex}"
    return hmac.compare_digest(candidate, stored)


def validate_task_form(payload: Dict[str, Any]) -> List[str]:
    errors = []
    for key, label in [
        ("task_code", "Task code"),
        ("environment", "Environment"),
        ("task_text", "Task"),
        ("observation_text", "Observation"),
        ("action_text", "Decision space"),
    ]:
        err = require_nonempty(payload.get(key, ""), label)
        if err:
            errors.append(err)
    if payload.get("difficulty") not in set(SUPPORTED_DIFFICULTIES):
        errors.append("Difficulty must be Easy, Medium, or Hard.")
    return errors


def validate_translation_form(
    task_translation: str,
    observation_translation: str,
    action_translation: Optional[str] = None,
) -> List[str]:
    errors = []
    for value, label in [
        (task_translation, "Translated Task"),
        (observation_translation, "Translated Observation"),
    ]:
        err = require_nonempty(value, label)
        if err:
            errors.append(err)
    # action_translation is optional by design
    return errors


def validate_signup(email: str, password: str, full_name: str) -> List[str]:
    errors = []
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


def set_flash_message(message: str) -> None:
    st.session_state.flash_message = message


def show_flash_message() -> None:
    message = st.session_state.pop("flash_message", None)
    if message:
        st.success(message)


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
    def __init__(self):
        self.tasks = [
            {
                "id": str(uuid.uuid4()),
                "task_code": "hr_screen_candidate_001",
                "environment": "HR",
                "difficulty": "Medium",
                "task_text": "Assess whether the applicant should be shortlisted for interview.",
                "observation_text": (
                    "Job description:\n"
                    "- Data analyst role requiring SQL, dashboarding, and stakeholder communication.\n\n"
                    "Candidate CV:\n"
                    "- 3 years experience in reporting and analytics.\n"
                    "- Strong SQL and Excel.\n"
                    "- Limited dashboard tooling experience.\n"
                ),
                "action_text": "shortlist, reject, request more information, escalate",
                "created_by": DEMO_ADMIN_EMAIL,
                "created_at": utc_now_iso(),
                "updated_at": utc_now_iso(),
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

    def delete_task(self, task_id: str) -> None:
        self.tasks = [row for row in self.tasks if row["id"] != task_id]
        self.translations = [row for row in self.translations if row["task_id"] != task_id]

    def list_translations(self, task_id: Optional[str] = None) -> List[Dict[str, Any]]:
        rows = list(self.translations)
        if task_id:
            rows = [r for r in rows if r["task_id"] == task_id]
        return sorted(rows, key=lambda x: x["updated_at"], reverse=True)

    def upsert_translation(self, payload: Dict[str, Any]) -> None:
        for i, row in enumerate(self.translations):
            if (
                row["task_id"] == payload["task_id"]
                and row["language_code"] == payload["language_code"]
                and row["user_email"] == payload["user_email"]
            ):
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
        email = normalize_email(email)
        if not email:
            return None
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

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, str]] = None,
        json_body: Any = None,
    ):
        response = requests.request(
            method,
            f"{self.base_url}/{path}",
            headers=self.headers,
            params=params,
            json=json_body,
            timeout=30,
        )
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
        normalized_email = normalize_email(email)
        if not normalized_email:
            return None
        rows = self._request(
            "GET",
            "users",
            params={"select": "*", "email": f"eq.{normalized_email}"},
        ).json()
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


def normalize_task_payload(
    task_code: str,
    environment: str,
    difficulty: str,
    task_text: str,
    observation_text: str,
    action_text: str,
    created_by: str,
) -> Dict[str, Any]:
    return {
        "task_code": task_code.strip(),
        "environment": environment.strip(),
        "difficulty": difficulty,
        "task_text": task_text.strip(),
        "observation_text": observation_text.strip(),
        "action_text": action_text.strip(),
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
            normalized_email = normalize_email(email)
            user = storage.get_user_by_email(normalized_email)
            if not user or not verify_password(password or "", user["password_hash"]):
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
            normalized_email = normalize_email(email)
            errors = validate_signup(normalized_email, password or "", full_name)
            if normalized_email and storage.get_user_by_email(normalized_email):
                errors.append("An account with this email already exists.")
            if errors:
                for err in errors:
                    st.error(err)
            else:
                user = storage.create_user(
                    {
                        "email": normalized_email,
                        "full_name": full_name.strip(),
                        "password_hash": hash_password(password),
                        "role": role,
                    }
                )
                st.session_state.user = user
                set_flash_message("Account created successfully.")
                st.rerun()

    return None


def render_task_table(tasks: List[Dict[str, Any]], translations: List[Dict[str, Any]]) -> None:
    st.markdown("### Benchmark items")
    headers = st.columns([1.0, 1.0, 0.8, 1.8, 1.0])
    headers[0].markdown("**Code**")
    headers[1].markdown("**Environment**")
    headers[2].markdown("**Difficulty**")
    headers[3].markdown("**Task / Observation / Decision space**")
    headers[4].markdown("**Translations**")

    for task in tasks:
        langs = sorted({row["language_label"] for row in translations if row["task_id"] == task["id"]})
        cols = st.columns([1.0, 1.0, 0.8, 1.8, 1.0])
        cols[0].write(task["task_code"])
        cols[1].write(task["environment"])
        cols[2].write(task["difficulty"])
        cols[3].table(
            [
                {"Field": "Task", "Value": task["task_text"]},
                {"Field": "Observation", "Value": task["observation_text"]},
                {"Field": "Decision space", "Value": task["action_text"]},
            ]
        )
        cols[4].write(", ".join(langs) if langs else "—")
        st.divider()


def render_translator_task_table(
    tasks: List[Dict[str, Any]],
    translations: List[Dict[str, Any]],
    user: Dict[str, Any],
) -> None:
    st.markdown("### Tasks to translate")
    headers = st.columns([1.0, 1.0, 0.7, 1.6, 0.8, 0.8])
    headers[0].markdown("**Code**")
    headers[1].markdown("**Environment**")
    headers[2].markdown("**Difficulty**")
    headers[3].markdown("**Task / Observation / Decision space**")
    headers[4].markdown("**Done by you**")
    headers[5].markdown("**Translate**")

    user_translation_pairs = {
        (row["task_id"], row["language_code"])
        for row in translations
        if row["user_email"] == user["email"]
    }

    for task in tasks:
        cols = st.columns([1.0, 1.0, 0.7, 1.6, 0.8, 0.8])
        cols[0].write(task["task_code"])
        cols[1].write(task["environment"])
        cols[2].write(task["difficulty"])
        cols[3].table(
            [
                {"Field": "Task", "Value": task["task_text"]},
                {"Field": "Observation", "Value": task["observation_text"]},
                {"Field": "Decision space", "Value": task["action_text"]},
            ]
        )
        done_langs = sorted(
            get_language_label(code)
            for task_id, code in user_translation_pairs
            if task_id == task["id"]
        )
        cols[4].write(", ".join(done_langs) if done_langs else "—")
        if cols[5].button("Translate", key=f"translate_task_{task['id']}"):
            st.session_state.edit_task_id = task["id"]
            st.session_state.edit_language_code = None
            st.session_state.edit_translation_id = None
            st.session_state.edit_task_translation = ""
            st.session_state.edit_observation_translation = ""
            st.session_state.edit_action_translation = ""
            st.rerun()
        st.divider()


def render_translation_table(
    translations: List[Dict[str, Any]],
    tasks: List[Dict[str, Any]],
    user: Dict[str, Any],
    storage: Storage,
) -> None:
    st.markdown("### Translations")
    headers = st.columns([1.0, 0.9, 0.9, 1.8, 0.7, 0.7])
    headers[0].markdown("**Task**")
    headers[1].markdown("**Language**")
    headers[2].markdown("**Translator**")
    headers[3].markdown("**Translated Task / Observation / Decision space**")
    headers[4].markdown("**Edit**")
    headers[5].markdown("**Delete**")

    task_map = {task["id"]: task for task in tasks}
    can_delete_all = user["role"] == "admin"

    for row in translations:
        task = task_map.get(row["task_id"])
        can_modify = can_delete_all or row["user_email"] == user["email"]
        cols = st.columns([1.0, 0.9, 0.9, 1.8, 0.7, 0.7])
        cols[0].write(task["task_code"] if task else row["task_id"])
        cols[1].write(row["language_label"])
        cols[2].write(row["user_email"])
        cols[3].table(
            [
                {"Field": "Task", "Value": row.get("task_translation", "")},
                {"Field": "Observation", "Value": row.get("observation_translation", "")},
                {"Field": "Decision space", "Value": row.get("action_translation", "") or "—"},
            ]
        )
        if can_modify:
            if cols[4].button("Edit", key=f"edit_translation_{row['id']}"):
                st.session_state.edit_translation_id = row["id"]
                st.session_state.edit_task_id = row["task_id"]
                st.session_state.edit_language_code = row["language_code"]
                st.session_state.edit_task_translation = row.get("task_translation", "")
                st.session_state.edit_observation_translation = row.get("observation_translation", "")
                st.session_state.edit_action_translation = row.get("action_translation", "")
                st.rerun()
            if cols[5].button("Delete", key=f"delete_translation_{row['id']}"):
                storage.delete_translation(row["id"])
                set_flash_message("Translation deleted successfully.")
                st.rerun()
        else:
            cols[4].write("—")
            cols[5].write("—")
        st.divider()


def build_tasks_export_rows(tasks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [
        {
            "id": task["id"],
            "task_code": task["task_code"],
            "environment": task["environment"],
            "difficulty": task["difficulty"],
            "task_text": task["task_text"],
            "observation_text": task["observation_text"],
            "action_text": task["action_text"],
            "created_by": task.get("created_by", ""),
            "created_at": task.get("created_at", ""),
            "updated_at": task.get("updated_at", ""),
        }
        for task in tasks
    ]


def build_translations_export_rows(
    translations: List[Dict[str, Any]],
    tasks: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
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
                "observation_translation": row.get("observation_translation", ""),
                "action_translation": row.get("action_translation", ""),
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
                "source": {
                    "language_code": "en",
                    "language_label": "English",
                    "task": task["task_text"],
                    "observation": task["observation_text"],
                    "decision_space": task["action_text"],
                },
                "translation": {
                    "language_code": row["language_code"],
                    "language_label": row["language_label"],
                    "task": row.get("task_translation", ""),
                    "observation": row.get("observation_translation", ""),
                    "decision_space": row.get("action_translation", ""),
                },
                "translator_email": row.get("user_email", ""),
                "updated_at": row.get("updated_at", ""),
            }
        )
    return json.dumps(dataset_rows, ensure_ascii=False, indent=2)


def render_streamlit_app() -> None:
    st.set_page_config(page_title="VANTA Benchmark Manager", page_icon="🌍", layout="wide")
    st.title("🌍 VANTA Benchmark Manager")
    st.caption(
        "Create multilingual benchmark items across environments such as HR, healthcare, "
        "education, Moodle, admissions, and more. Each item contains a Task, Observation, "
        "and Decision space."
    )

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
  observation_text text not null,
  action_text text not null,
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
  observation_translation text not null,
  action_translation text,
  user_email text not null,
  updated_at timestamptz not null default now(),
  unique(task_id, language_code, user_email)
);
                """.strip(),
                language="sql",
            )
        return

    if "edit_translation_id" not in st.session_state:
        st.session_state.edit_translation_id = None
    if "edit_task_translation" not in st.session_state:
        st.session_state.edit_task_translation = ""
    if "edit_observation_translation" not in st.session_state:
        st.session_state.edit_observation_translation = ""
    if "edit_action_translation" not in st.session_state:
        st.session_state.edit_action_translation = ""
    if "edit_task_id" not in st.session_state:
        st.session_state.edit_task_id = None
    if "edit_language_code" not in st.session_state:
        st.session_state.edit_language_code = None

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
        st.write("Download benchmark items and translations for analysis or packaging.")

        tasks_rows = build_tasks_export_rows(tasks)
        translations_rows = build_translations_export_rows(translations, tasks)
        dataset_json = build_dataset_json(tasks, translations)

        tasks_buffer = io.StringIO()
        tasks_fieldnames = [
            "id",
            "task_code",
            "environment",
            "difficulty",
            "task_text",
            "observation_text",
            "action_text",
            "created_by",
            "created_at",
            "updated_at",
        ]
        tasks_writer = csv.DictWriter(tasks_buffer, fieldnames=tasks_fieldnames)
        tasks_writer.writeheader()
        for row in tasks_rows:
            tasks_writer.writerow(row)

        translations_buffer = io.StringIO()
        translations_fieldnames = [
            "id",
            "task_id",
            "task_code",
            "environment",
            "language_code",
            "language_label",
            "task_translation",
            "observation_translation",
            "action_translation",
            "user_email",
            "updated_at",
        ]
        translations_writer = csv.DictWriter(translations_buffer, fieldnames=translations_fieldnames)
        translations_writer.writeheader()
        for row in translations_rows:
            translations_writer.writerow(row)

        export_cols = st.columns(3)
        export_cols[0].download_button(
            "Download tasks.csv",
            data=tasks_buffer.getvalue().encode("utf-8"),
            file_name="tasks.csv",
            mime="text/csv",
            use_container_width=True,
        )
        export_cols[1].download_button(
            "Download translations.csv",
            data=translations_buffer.getvalue().encode("utf-8"),
            file_name="translations.csv",
            mime="text/csv",
            use_container_width=True,
        )
        export_cols[2].download_button(
            "Download dataset.json",
            data=dataset_json.encode("utf-8"),
            file_name="dataset.json",
            mime="application/json",
            use_container_width=True,
        )

    tabs = ["Translate", "Tasks Table", "Translations Table", "Overview"]
    if user["role"] == "admin":
        tabs = ["Add / Edit Tasks"] + tabs
    pages = st.tabs(tabs)
    tab_index = 0

    if user["role"] == "admin":
        with pages[tab_index]:
            st.subheader("Add or edit English benchmark items")
            existing_map = {f"{t['task_code']} — {t['environment']}": t for t in tasks}
            choice = st.selectbox("Edit existing or create new", ["Create new task"] + list(existing_map.keys()))
            selected = existing_map.get(choice)

            defaults = selected or {
                "task_code": "",
                "environment": SUPPORTED_ENVIRONMENTS[0],
                "difficulty": "Easy",
                "task_text": "",
                "observation_text": "",
                "action_text": "",
            }

            with st.form("task_form"):
                task_code = st.text_input("Task code", value=defaults["task_code"])
                environment = st.selectbox(
                    "Environment",
                    SUPPORTED_ENVIRONMENTS,
                    index=SUPPORTED_ENVIRONMENTS.index(defaults["environment"])
                    if defaults["environment"] in SUPPORTED_ENVIRONMENTS
                    else 0,
                )
                difficulty = st.selectbox(
                    "Difficulty",
                    SUPPORTED_DIFFICULTIES,
                    index=SUPPORTED_DIFFICULTIES.index(defaults["difficulty"])
                    if defaults["difficulty"] in SUPPORTED_DIFFICULTIES
                    else 0,
                )
                task_text = st.text_area("Task", value=defaults["task_text"], height=100)
                observation_text = st.text_area("Observation", value=defaults["observation_text"], height=160)
                action_text = st.text_area("Decision space", value=defaults["action_text"], height=100)
                submitted = st.form_submit_button("Save task", use_container_width=True)

            if submitted:
                payload = {
                    "task_code": task_code,
                    "environment": environment,
                    "difficulty": difficulty,
                    "task_text": task_text,
                    "observation_text": observation_text,
                    "action_text": action_text,
                }
                errors = validate_task_form(payload)
                duplicate = any(
                    t["task_code"] == task_code.strip() and (not selected or t["id"] != selected["id"])
                    for t in tasks
                )
                if duplicate:
                    errors.append("Task code already exists.")
                if errors:
                    for err in errors:
                        st.error(err)
                else:
                    clean = normalize_task_payload(
                        task_code,
                        environment,
                        difficulty,
                        task_text,
                        observation_text,
                        action_text,
                        user["email"],
                    )
                    if selected:
                        storage.update_task(selected["id"], clean)
                        set_flash_message("Task updated successfully.")
                    else:
                        storage.create_task(clean)
                        set_flash_message("Task created successfully.")
                    st.rerun()

            st.markdown("### Existing tasks")
            for task in tasks:
                cols = st.columns([1.2, 1.0, 0.8, 0.8, 0.8])
                cols[0].write(task["task_code"])
                cols[1].write(task["environment"])
                cols[2].write(task["difficulty"])
                if cols[3].button("Edit", key=f"edit_task_{task['id']}"):
                    st.session_state.edit_task_id = task["id"]
                if cols[4].button("Delete", key=f"delete_task_{task['id']}"):
                    storage.delete_task(task["id"])
                    set_flash_message("Task deleted successfully.")
                    st.rerun()
                st.table(
                    [
                        {"Field": "Task", "Value": task["task_text"]},
                        {"Field": "Observation", "Value": task["observation_text"]},
                        {"Field": "Decision space", "Value": task["action_text"]},
                    ]
                )
                st.divider()
        tab_index += 1

    with pages[tab_index]:
        st.subheader("Translate")
        if not tasks:
            st.info("No English tasks yet.")
        else:
            render_translator_task_table(tasks, translations, user)
            task_map = {f"{t['task_code']} — {t['environment']}": t for t in tasks}
            default_task_label = list(task_map.keys())[0]

            if st.session_state.edit_task_id:
                for label, task in task_map.items():
                    if task["id"] == st.session_state.edit_task_id:
                        default_task_label = label
                        break

            selected_label = st.selectbox(
                "Selected task",
                list(task_map.keys()),
                index=list(task_map.keys()).index(default_task_label),
            )
            task = task_map[selected_label]

            lang_codes = [code for code, _ in SUPPORTED_LANGUAGES]
            default_lang_code = (
                st.session_state.edit_language_code
                if st.session_state.edit_language_code in lang_codes
                else lang_codes[0]
            )
            lang_code = st.selectbox(
                "Choose target language",
                lang_codes,
                format_func=get_language_label,
                index=lang_codes.index(default_lang_code),
            )
            lang_label = get_language_label(lang_code)

            previous = None
            for row in translations:
                if (
                    row["task_id"] == task["id"]
                    and row["language_code"] == lang_code
                    and row["user_email"] == user["email"]
                ):
                    previous = row
                    break

            task_translation_default = st.session_state.edit_task_translation or (previous or {}).get("task_translation", "")
            observation_translation_default = st.session_state.edit_observation_translation or (previous or {}).get("observation_translation", "")
            action_translation_default = st.session_state.edit_action_translation or (previous or {}).get("action_translation", "")

            st.markdown("**English source**")
            st.table(
                [
                    {"Field": "Task", "Value": task["task_text"]},
                    {"Field": "Observation", "Value": task["observation_text"]},
                    {"Field": "Decision space", "Value": task["action_text"]},
                ]
            )

            with st.form("translation_form"):
                translated_task = st.text_area(f"{lang_label} – Task", value=task_translation_default, height=100)
                translated_observation = st.text_area(
                    f"{lang_label} – Observation",
                    value=observation_translation_default,
                    height=160,
                )
                translated_action = st.text_area(
                    f"{lang_label} – Decision space (optional)",
                    value=action_translation_default,
                    height=100,
                )
                submitted = st.form_submit_button("Save translation", use_container_width=True)

            if submitted:
                errors = validate_translation_form(
                    translated_task,
                    translated_observation,
                    translated_action,
                )
                if errors:
                    for err in errors:
                        st.error(err)
                else:
                    storage.upsert_translation(
                        {
                            "task_id": task["id"],
                            "language_code": lang_code,
                            "language_label": lang_label,
                            "task_translation": translated_task.strip(),
                            "observation_translation": translated_observation.strip(),
                            "action_translation": translated_action.strip(),
                            "user_email": user["email"],
                            "updated_at": utc_now_iso(),
                        }
                    )
                    st.session_state.edit_translation_id = None
                    st.session_state.edit_task_translation = ""
                    st.session_state.edit_observation_translation = ""
                    st.session_state.edit_action_translation = ""
                    st.session_state.edit_task_id = None
                    st.session_state.edit_language_code = None
                    set_flash_message("Translation saved successfully.")
                    st.rerun()
        tab_index += 1

    with pages[tab_index]:
        render_task_table(tasks, translations)
        tab_index += 1

    with pages[tab_index]:
        render_translation_table(translations, tasks, user, storage)
        tab_index += 1

    with pages[tab_index]:
        st.subheader("Overview")
        metric_cols = st.columns(3)
        metric_cols[0].metric("Tasks", len(tasks))
        metric_cols[1].metric("Translations", len(translations))
        metric_cols[2].metric("Languages used", len({row["language_code"] for row in translations}))
        render_task_table(tasks, translations)


def _run_self_tests() -> None:
    assert normalize_email(" USER@Example.COM ") == "user@example.com"
    assert normalize_email(None) == ""
    assert get_language_label("ar-sudanese") == "Arabic – Sudanese"
    assert get_language_label("xx") == "xx"
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
    assert any(
        msg == "Password must be at least 8 characters."
        for msg in validate_signup("user@example.com", "123", "Test User")
    )

    valid_task = {
        "task_code": "task_1",
        "environment": "HR",
        "difficulty": "Easy",
        "task_text": "Assess candidate eligibility.",
        "observation_text": "Job description and CV.",
        "action_text": "shortlist, reject",
    }
    assert validate_task_form(valid_task) == []

    invalid_task = validate_task_form(
        {
            "task_code": "",
            "environment": "",
            "difficulty": "Bad",
            "task_text": "",
            "observation_text": "",
            "action_text": "",
        }
    )
    assert any(msg == "Task code is required." for msg in invalid_task)
    assert any(msg == "Environment is required." for msg in invalid_task)
    assert any(msg == "Task is required." for msg in invalid_task)
    assert any(msg == "Observation is required." for msg in invalid_task)
    assert any(msg == "Decision space is required." for msg in invalid_task)
    assert any(msg == "Difficulty must be Easy, Medium, or Hard." for msg in invalid_task)

    assert validate_translation_form("Hallo", "Lebenslauf und Stellenbeschreibung") == []
    translation_errors = validate_translation_form("", "")
    assert any(msg == "Translated Task is required." for msg in translation_errors)
    assert any(msg == "Translated Observation is required." for msg in translation_errors)

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
    fetched_user = storage.get_user_by_email("translator@example.com")
    assert fetched_user is not None
    assert fetched_user["id"] == user["id"]

    created = storage.create_task(
        normalize_task_payload(
            "task_x",
            "Healthcare",
            "Medium",
            "Determine the triage priority for the patient.",
            "Patient note with symptoms and history.",
            "urgent, routine, refer",
            admin["email"],
        )
    )
    assert any(task["task_code"] == "task_x" for task in storage.list_tasks())

    storage.upsert_translation(
        {
            "task_id": created["id"],
            "language_code": "de",
            "language_label": "German",
            "task_translation": "Bestimmen Sie die Triage-Priorität für den Patienten.",
            "observation_translation": "Patientennotiz mit Symptomen und Vorgeschichte.",
            "action_translation": "dringend, routinemäßig, überweisen",
            "user_email": user["email"],
            "updated_at": utc_now_iso(),
        }
    )
    storage.upsert_translation(
        {
            "task_id": created["id"],
            "language_code": "de",
            "language_label": "German",
            "task_translation": "Überarbeitete Aufgabe",
            "observation_translation": "Überarbeitete Beobachtung",
            "action_translation": "Überarbeiteter Entscheidungsraum",
            "user_email": user["email"],
            "updated_at": utc_now_iso(),
        }
    )

    rows = storage.list_translations(created["id"])
    assert len([r for r in rows if r["user_email"] == user["email"] and r["language_code"] == "de"]) == 1
    assert any(r["task_translation"] == "Überarbeitete Aufgabe" for r in rows)
    assert any(r["observation_translation"] == "Überarbeitete Beobachtung" for r in rows)
    assert any(r["action_translation"] == "Überarbeiteter Entscheidungsraum" for r in rows)

    translation_id = rows[0]["id"]
    storage.delete_translation(translation_id)
    assert storage.list_translations(created["id"]) == []

    storage.delete_task(created["id"])
    assert not any(task["task_code"] == "task_x" for task in storage.list_tasks())

    export_task = storage.create_task(
        normalize_task_payload(
            "task_y",
            "Education",
            "Easy",
            "Grade the student short answer.",
            "Student short-answer response.",
            "A, B, C, D, Fail",
            admin["email"],
        )
    )
    storage.upsert_translation(
        {
            "task_id": export_task["id"],
            "language_code": "es",
            "language_label": "Spanish",
            "task_translation": "Califique la respuesta corta del estudiante.",
            "observation_translation": "Respuesta corta del estudiante.",
            "action_translation": "A, B, C, D, Reprobado",
            "user_email": user["email"],
            "updated_at": utc_now_iso(),
        }
    )
    task_export_rows = build_tasks_export_rows(storage.list_tasks())
    translation_export_rows = build_translations_export_rows(storage.list_translations(), storage.list_tasks())
    dataset_export = build_dataset_json(storage.list_tasks(), storage.list_translations())
    assert any(row["task_code"] == "task_y" for row in task_export_rows)
    assert any(row["language_code"] == "es" for row in translation_export_rows)
    assert '"language_code": "es"' in dataset_export


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
