import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

try:
    import pandas as pd
except ModuleNotFoundError:
    pd = None

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

DEMO_TASKS = [
    {
        "id": str(uuid.uuid4()),
        "task_code": "moodle_create_course_001",
        "environment": "Moodle LMS",
        "category": "Education / Operational",
        "difficulty": "Easy",
        "instruction_en": "Create a new course named 'Intro to AI', short name 'AI101', format 'Topics'.",
        "start_state": "Admin dashboard logged in.",
        "action_space": ["click(element)", "type(element, text)", "select(element, option)", "submit"],
        "observation": ["HTML DOM", "visible text", "clickable elements", "optional screenshot"],
        "success_conditions": [
            "course with short name 'AI101' exists",
            "course format = Topics",
            "course page loads successfully",
        ],
        "failure_conditions": ["max rounds exceeded", "invalid navigation outside Moodle"],
        "metrics": ["success_rate", "steps_to_completion", "invalid_action_rate"],
        "max_rounds": 20,
        "notes": "Starter Moodle task for multilingual administrative workflows.",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
]

DEMO_TRANSLATIONS: List[Dict[str, Any]] = []


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def lines_to_list(text: str) -> List[str]:
    return [line.strip() for line in text.splitlines() if line.strip()]


def list_to_lines(items: List[str]) -> str:
    return "\n".join(items)


def require_nonempty(value: str, label: str) -> Optional[str]:
    if not value or not value.strip():
        return f"{label} is required."
    return None


def normalize_task_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "task_code": payload["task_code"].strip(),
        "environment": payload["environment"].strip(),
        "category": payload.get("category", "").strip(),
        "difficulty": payload["difficulty"],
        "instruction_en": payload["instruction_en"].strip(),
        "start_state": payload.get("start_state", "").strip(),
        "action_space": list(payload.get("action_space", [])),
        "observation": list(payload.get("observation", [])),
        "success_conditions": list(payload.get("success_conditions", [])),
        "failure_conditions": list(payload.get("failure_conditions", [])),
        "metrics": list(payload.get("metrics", [])),
        "max_rounds": int(payload.get("max_rounds", 20)),
        "notes": payload.get("notes", "").strip(),
        "updated_at": utc_now_iso(),
    }


def validate_task_form(payload: Dict[str, Any]) -> List[str]:
    errors = []
    for key, label in [
        ("task_code", "Task code"),
        ("environment", "Environment"),
        ("instruction_en", "English instruction"),
    ]:
        err = require_nonempty(payload.get(key, ""), label)
        if err:
            errors.append(err)
    if payload.get("difficulty") not in {"Easy", "Medium", "Hard"}:
        errors.append("Difficulty must be Easy, Medium, or Hard.")
    if int(payload.get("max_rounds", 0)) < 1:
        errors.append("Max rounds must be at least 1.")
    return errors


def validate_translation_form(translation_text: str, translator_name: str) -> List[str]:
    errors = []
    err = require_nonempty(translation_text, "Translation")
    if err:
        errors.append(err)
    err = require_nonempty(translator_name, "Translator name")
    if err:
        errors.append(err)
    return errors


def get_language_label(language_code: str) -> str:
    return dict(SUPPORTED_LANGUAGES).get(language_code, language_code)


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


class InMemoryStorage(Storage):
    def __init__(self):
        self.tasks = [dict(task) for task in DEMO_TASKS]
        self.translations = [dict(item) for item in DEMO_TRANSLATIONS]

    def list_tasks(self) -> List[Dict[str, Any]]:
        return sorted(self.tasks, key=lambda x: x["task_code"])

    def create_task(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        task = {"id": str(uuid.uuid4()), **payload, "created_at": utc_now_iso()}
        self.tasks.append(task)
        return task

    def update_task(self, task_id: str, payload: Dict[str, Any]) -> None:
        for i, task in enumerate(self.tasks):
            if task["id"] == task_id:
                self.tasks[i] = {**task, **payload}
                return
        raise KeyError("Task not found")

    def delete_task(self, task_id: str) -> None:
        self.tasks = [task for task in self.tasks if task["id"] != task_id]
        self.translations = [t for t in self.translations if t["task_id"] != task_id]

    def list_translations(self, task_id: Optional[str] = None) -> List[Dict[str, Any]]:
        rows = list(self.translations)
        if task_id:
            rows = [r for r in rows if r["task_id"] == task_id]
        return rows

    def upsert_translation(self, payload: Dict[str, Any]) -> None:
        for i, row in enumerate(self.translations):
            if row["task_id"] == payload["task_id"] and row["language_code"] == payload["language_code"]:
                self.translations[i] = {**row, **payload}
                return
        self.translations.append({"id": str(uuid.uuid4()), **payload})


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
        params = {"select": "*", "order": "task_code.asc"}
        return self._request("GET", "tasks", params=params).json()

    def create_task(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        rows = self._request("POST", "tasks", json_body=payload).json()
        return rows[0]

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
            params={"on_conflict": "task_id,language_code"},
            json=payload,
            timeout=30,
        )
        response.raise_for_status()


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


def fetch_tasks() -> List[Dict[str, Any]]:
    return get_storage().list_tasks()


def fetch_translations(task_id: Optional[str] = None) -> List[Dict[str, Any]]:
    return get_storage().list_translations(task_id)


def get_translation_lookup(translations: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {f"{row['task_id']}::{row['language_code']}": row for row in translations}


def build_overview_rows(tasks: List[Dict[str, Any]], translations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    lang_index = {(row["task_id"], row["language_code"]) for row in translations}
    overview_rows = []
    for task in tasks:
        row = {
            "task_code": task["task_code"],
            "environment": task["environment"],
        }
        for code, label in SUPPORTED_LANGUAGES:
            row[label] = "✅" if (task["id"], code) in lang_index else "—"
        overview_rows.append(row)
    return overview_rows


def render_cli_summary() -> None:
    storage = InMemoryStorage()
    tasks = storage.list_tasks()
    print("Agent Task Translation Manager")
    print("Streamlit is not installed in this environment, so this script is running in CLI fallback mode.")
    print(f"Tasks loaded: {len(tasks)}")
    for task in tasks:
        print(f"- {task['task_code']} | {task['environment']} | {task['difficulty']}")
        print(f"  EN: {task['instruction_en']}")
    print("\nSupported languages:")
    for code, label in SUPPORTED_LANGUAGES:
        print(f"- {code}: {label}")
    print("\nTo run the web app, install streamlit and start with:")
    print("streamlit run agent_task_translation_streamlit_app.py")


def render_streamlit_app() -> None:
    st.set_page_config(page_title="Agent Task Translation Manager", page_icon="🌍", layout="wide")

    st.title("🌍 Agent Task Translation Manager")
    st.caption(
        "Admin page for creating English benchmark tasks, plus a translator page where collaborators choose a target language and save their translations."
    )

    if using_persistent_storage():
        st.success("Persistent storage is active via Supabase.")
    else:
        st.warning(
            "Running in demo mode with temporary in-memory storage. Add Supabase credentials in Streamlit secrets for real persistence."
        )

    with st.expander("Recommended database schema for Supabase", expanded=False):
        st.code(
            """
create table if not exists tasks (
  id uuid primary key default gen_random_uuid(),
  task_code text unique not null,
  environment text not null,
  category text,
  difficulty text not null,
  instruction_en text not null,
  start_state text,
  action_space jsonb not null default '[]'::jsonb,
  observation jsonb not null default '[]'::jsonb,
  success_conditions jsonb not null default '[]'::jsonb,
  failure_conditions jsonb not null default '[]'::jsonb,
  metrics jsonb not null default '[]'::jsonb,
  max_rounds integer not null default 20,
  notes text,
  created_at timestamptz not null default now(),
  updated_at timestamptz
);

create table if not exists translations (
  id uuid primary key default gen_random_uuid(),
  task_id uuid not null references tasks(id) on delete cascade,
  language_code text not null,
  language_label text not null,
  translation_text text not null,
  translator_name text not null,
  reviewer_note text,
  updated_at timestamptz not null default now(),
  unique(task_id, language_code)
);
            """.strip(),
            language="sql",
        )

    admin_tab, translator_tab, overview_tab = st.tabs(["Admin: Add English Tasks", "Translator Workspace", "Overview"])

    with admin_tab:
        st.subheader("Create and manage the English tasks")

        admin_password = getattr(st, "secrets", {}).get("app", {}).get("admin_password", "") if hasattr(st, "secrets") else ""
        if admin_password:
            entered = st.text_input("Admin password", type="password")
            admin_ok = entered == admin_password
            if not entered:
                st.info("Enter the admin password to access this page.")
            elif not admin_ok:
                st.error("Incorrect password.")
        else:
            admin_ok = True
            st.info("No admin password set. Add `app.admin_password` in Streamlit secrets if you want to lock this page.")

        if admin_ok:
            tasks = fetch_tasks()
            task_options = {f"{task['task_code']} — {task['environment']}": task for task in tasks}
            selected_label = st.selectbox("Select a task to edit", ["Create new task"] + list(task_options.keys()))
            selected_task = task_options.get(selected_label)

            form_defaults = selected_task or {
                "task_code": "",
                "environment": "",
                "category": "",
                "difficulty": "Easy",
                "instruction_en": "",
                "start_state": "",
                "action_space": [],
                "observation": [],
                "success_conditions": [],
                "failure_conditions": [],
                "metrics": [],
                "max_rounds": 20,
                "notes": "",
            }

            with st.form("task_form", clear_on_submit=False):
                c1, c2, c3 = st.columns([1.2, 1.2, 0.8])
                task_code = c1.text_input("Task code", value=form_defaults["task_code"], help="Example: moodle_create_course_001")
                environment = c2.text_input("Environment", value=form_defaults["environment"], help="Example: Moodle LMS")
                difficulty = c3.selectbox("Difficulty", ["Easy", "Medium", "Hard"], index=["Easy", "Medium", "Hard"].index(form_defaults["difficulty"]))

                category = st.text_input("Category", value=form_defaults["category"])
                instruction_en = st.text_area("English instruction", value=form_defaults["instruction_en"], height=100)
                start_state = st.text_area("Start state", value=form_defaults["start_state"], height=80)

                col_a, col_b = st.columns(2)
                action_space = col_a.text_area("Action space (one per line)", value=list_to_lines(form_defaults["action_space"]), height=120)
                observation = col_b.text_area("Observation (one per line)", value=list_to_lines(form_defaults["observation"]), height=120)

                col_c, col_d = st.columns(2)
                success_conditions = col_c.text_area(
                    "Success conditions (one per line)",
                    value=list_to_lines(form_defaults["success_conditions"]),
                    height=120,
                )
                failure_conditions = col_d.text_area(
                    "Failure conditions (one per line)",
                    value=list_to_lines(form_defaults["failure_conditions"]),
                    height=120,
                )

                col_e, col_f = st.columns([2, 1])
                metrics = col_e.text_area("Metrics (one per line)", value=list_to_lines(form_defaults["metrics"]), height=100)
                max_rounds = col_f.number_input("Max rounds", min_value=1, max_value=200, value=int(form_defaults["max_rounds"]))
                notes = st.text_area("Notes", value=form_defaults["notes"], height=90)

                save = st.form_submit_button("Save task", use_container_width=True)

            if save:
                payload = {
                    "task_code": task_code,
                    "environment": environment,
                    "category": category,
                    "difficulty": difficulty,
                    "instruction_en": instruction_en,
                    "start_state": start_state,
                    "action_space": lines_to_list(action_space),
                    "observation": lines_to_list(observation),
                    "success_conditions": lines_to_list(success_conditions),
                    "failure_conditions": lines_to_list(failure_conditions),
                    "metrics": lines_to_list(metrics),
                    "max_rounds": int(max_rounds),
                    "notes": notes,
                }
                errors = validate_task_form(payload)
                duplicate_task_code = any(
                    t["task_code"] == task_code.strip() and t.get("id") != selected_task.get("id")
                    if selected_task
                    else t["task_code"] == task_code.strip()
                    for t in tasks
                )
                if duplicate_task_code:
                    errors.append("Task code already exists.")

                if errors:
                    for err in errors:
                        st.error(err)
                else:
                    storage = get_storage()
                    cleaned = normalize_task_payload(payload)
                    try:
                        if selected_task:
                            storage.update_task(selected_task["id"], cleaned)
                            st.success("Task updated.")
                        else:
                            storage.create_task(cleaned)
                            st.success("Task created.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Could not save task: {e}")

            st.markdown("### Existing tasks")
            current_tasks = fetch_tasks()
            if not current_tasks:
                st.info("No tasks yet.")
            else:
                for task in current_tasks:
                    with st.container(border=True):
                        top_left, top_right = st.columns([0.82, 0.18])
                        with top_left:
                            st.markdown(f"**{task['task_code']}** — {task['environment']}")
                            st.caption(task["instruction_en"])
                        with top_right:
                            if st.button("Delete", key=f"delete_{task['id']}", use_container_width=True):
                                try:
                                    get_storage().delete_task(task["id"])
                                    st.success("Task deleted.")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Could not delete task: {e}")

    with translator_tab:
        st.subheader("Translate an English task into a target language")
        tasks = fetch_tasks()
        translations = fetch_translations()
        translation_lookup = get_translation_lookup(translations)

        if not tasks:
            st.info("No English tasks available yet. Add them on the admin page first.")
        else:
            task_map = {f"{task['task_code']} — {task['environment']}": task for task in tasks}
            selected_task_label = st.selectbox("Choose task", list(task_map.keys()))
            selected_task = task_map[selected_task_label]

            lang_code = st.selectbox(
                "Choose target language",
                options=[code for code, _ in SUPPORTED_LANGUAGES],
                format_func=lambda code: get_language_label(code),
            )
            lang_label = get_language_label(lang_code)
            existing_translation = translation_lookup.get(f"{selected_task['id']}::{lang_code}")

            left, right = st.columns([1.0, 1.0])
            with left:
                st.markdown("#### English source task")
                st.write(f"**Task code:** {selected_task['task_code']}")
                st.write(f"**Environment:** {selected_task['environment']}")
                st.write(f"**Difficulty:** {selected_task['difficulty']}")
                st.write("**Instruction:**")
                st.info(selected_task["instruction_en"])
                with st.expander("Show full task details"):
                    st.json(selected_task)

            with right:
                st.markdown(f"#### {lang_label} translation")
                with st.form("translation_form", clear_on_submit=False):
                    translator_name = st.text_input(
                        "Your name",
                        value=(existing_translation or {}).get("translator_name", ""),
                    )
                    translation_text = st.text_area(
                        f"Translate the English instruction into {lang_label}",
                        value=(existing_translation or {}).get("translation_text", ""),
                        height=180,
                    )
                    reviewer_note = st.text_area(
                        "Optional note",
                        value=(existing_translation or {}).get("reviewer_note", ""),
                        height=100,
                        help="Use this for wording comments, ambiguity notes, or cultural adaptation notes.",
                    )
                    submit_translation = st.form_submit_button("Save translation", use_container_width=True)

                if submit_translation:
                    errors = validate_translation_form(translation_text, translator_name)
                    if errors:
                        for err in errors:
                            st.error(err)
                    else:
                        payload = {
                            "task_id": selected_task["id"],
                            "language_code": lang_code,
                            "language_label": lang_label,
                            "translation_text": translation_text.strip(),
                            "translator_name": translator_name.strip(),
                            "reviewer_note": reviewer_note.strip(),
                            "updated_at": utc_now_iso(),
                        }
                        try:
                            get_storage().upsert_translation(payload)
                            st.success("Translation saved.")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Could not save translation: {e}")

            st.markdown("### Existing translations for this task")
            rows = [r for r in translations if r["task_id"] == selected_task["id"]]
            if rows:
                if pd is not None:
                    df = pd.DataFrame(rows)[["language_label", "translator_name", "translation_text", "reviewer_note", "updated_at"]]
                    st.dataframe(df, use_container_width=True, hide_index=True)
                else:
                    st.json(rows)
            else:
                st.info("No translations yet for this task.")

    with overview_tab:
        st.subheader("Coverage overview")
        tasks = fetch_tasks()
        translations = fetch_translations()

        st.metric("English tasks", len(tasks))
        st.metric("Saved translations", len(translations))

        if tasks:
            overview_rows = build_overview_rows(tasks, translations)
            if pd is not None:
                st.dataframe(pd.DataFrame(overview_rows), use_container_width=True, hide_index=True)
            else:
                st.json(overview_rows)

        with st.expander("Deployment notes", expanded=True):
            st.markdown(
                """
1. Deploy this repository on Streamlit Community Cloud.
2. Add your Supabase URL and API key under app secrets so translations persist remotely.
3. The admin page is where you create the English source tasks.
4. Translators open the translator page, choose a language, and save their translations.
5. Because Community Cloud session state is not long-term storage, use a remote database for persistence.
                """.strip()
            )

        with st.expander("Minimal requirements.txt"):
            st.code("""streamlit\npandas\nrequests""", language="text")


def _run_self_tests() -> None:
    assert lines_to_list("a\n\n b ") == ["a", "b"]
    assert list_to_lines(["a", "b"]) == "a\nb"
    assert require_nonempty("  hi  ", "X") is None
    assert require_nonempty("", "Task") == "Task is required."
    assert get_language_label("ar-sudanese") == "Arabic – Sudanese"
    assert get_language_label("xx") == "xx"

    valid_task_errors = validate_task_form({
        "task_code": "x",
        "environment": "Moodle",
        "instruction_en": "Do thing",
        "difficulty": "Easy",
        "max_rounds": 5,
    })
    assert valid_task_errors == []

    invalid_task_errors = validate_task_form({
        "task_code": "",
        "environment": "",
        "instruction_en": "",
        "difficulty": "Bad",
        "max_rounds": 0,
    })
    assert any("Task code is required" in e for e in invalid_task_errors)
    assert any("Environment is required" in e for e in invalid_task_errors)
    assert any("English instruction is required" in e for e in invalid_task_errors)
    assert any("Difficulty must be Easy, Medium, or Hard." == e for e in invalid_task_errors)
    assert any("Max rounds must be at least 1." == e for e in invalid_task_errors)

    assert validate_translation_form("Hallo", "Dalia") == []
    assert any("Translation is required" in e for e in validate_translation_form("", "Dalia"))
    assert any("Translator name is required" in e for e in validate_translation_form("Hi", ""))

    normalized = normalize_task_payload({
        "task_code": "  code_1 ",
        "environment": " Moodle ",
        "category": " cat ",
        "difficulty": "Easy",
        "instruction_en": " Hello ",
        "start_state": " start ",
        "action_space": ["click"],
        "observation": ["dom"],
        "success_conditions": ["ok"],
        "failure_conditions": ["fail"],
        "metrics": ["success_rate"],
        "max_rounds": 7,
        "notes": " note ",
    })
    assert normalized["task_code"] == "code_1"
    assert normalized["environment"] == "Moodle"
    assert normalized["instruction_en"] == "Hello"
    assert normalized["max_rounds"] == 7

    storage = InMemoryStorage()
    initial_count = len(storage.list_tasks())
    created = storage.create_task(normalized)
    assert len(storage.list_tasks()) == initial_count + 1
    storage.update_task(created["id"], {"notes": "updated"})
    assert any(t["id"] == created["id"] and t["notes"] == "updated" for t in storage.list_tasks())
    storage.upsert_translation({
        "task_id": created["id"],
        "language_code": "de",
        "language_label": "German",
        "translation_text": "Hallo",
        "translator_name": "Dalia",
        "reviewer_note": "",
        "updated_at": utc_now_iso(),
    })
    storage.upsert_translation({
        "task_id": created["id"],
        "language_code": "de",
        "language_label": "German",
        "translation_text": "Guten Tag",
        "translator_name": "Dalia",
        "reviewer_note": "rev",
        "updated_at": utc_now_iso(),
    })
    translations = storage.list_translations(created["id"])
    assert len(translations) == 1
    assert translations[0]["translation_text"] == "Guten Tag"

    overview = build_overview_rows(storage.list_tasks(), storage.list_translations())
    assert any(row["German"] == "✅" for row in overview if row["task_code"] == "code_1")

    storage.delete_task(created["id"])
    assert len(storage.list_tasks()) == initial_count
    assert storage.list_translations(created["id"]) == []


_run_self_tests()

if __name__ == "__main__":
    if STREAMLIT_AVAILABLE:
        render_streamlit_app()
    else:
        render_cli_summary()
