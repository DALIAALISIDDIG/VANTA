# app.py
import sqlite3
import json
from pathlib import Path
from datetime import datetime

import pandas as pd
import streamlit as st


DB_PATH = Path("tasks.db")


DEFAULT_ROWS = [
    {
        "task_id": "moodle_01",
        "task_group": "Grading",
        "goal": "Grade a clearly correct short-answer submission using a rubric.",
        "context_inputs": "Question, rubric, student answer",
        "subtasks": "Retrieve submission -> Read rubric -> Compare answer -> Compute score -> Save",
        "decision_rule": "If all rubric criteria are satisfied, assign full marks.",
        "allowed_actions": "open_submission, read_rubric, enter_grade, save_grade",
        "expected_output": "grade=full, decision=correct, action=save",
        "owner": "",
        "status": "draft",
        "notes": "",
    },
    {
        "task_id": "moodle_02",
        "task_group": "Grading",
        "goal": "Grade a clearly incorrect short-answer submission using a rubric.",
        "context_inputs": "Question, rubric, incorrect answer",
        "subtasks": "Retrieve -> Compare -> Identify mismatch -> Assign score -> Save",
        "decision_rule": "If no rubric criteria are satisfied, assign zero.",
        "allowed_actions": "open_submission, read_rubric, enter_grade, save_grade",
        "expected_output": "grade=0, decision=incorrect",
        "owner": "",
        "status": "draft",
        "notes": "",
    },
    {
        "task_id": "moodle_03",
        "task_group": "Grading",
        "goal": "Assign partial credit for a partially correct answer.",
        "context_inputs": "Question, rubric, partial answer",
        "subtasks": "Retrieve -> Identify correct parts -> Map to rubric -> Compute score",
        "decision_rule": "Score equals the sum of satisfied criteria.",
        "allowed_actions": "open_submission, read_rubric, enter_grade",
        "expected_output": "grade=partial",
        "owner": "",
        "status": "draft",
        "notes": "",
    },
    {
        "task_id": "moodle_04",
        "task_group": "Grading",
        "goal": "Decide pass or fail for a borderline submission.",
        "context_inputs": "Rubric, pass threshold, answer",
        "subtasks": "Retrieve -> Evaluate -> Compare with threshold -> Decide",
        "decision_rule": "If score is greater than or equal to threshold, pass; otherwise fail.",
        "allowed_actions": "read_submission, enter_grade",
        "expected_output": "decision=pass/fail",
        "owner": "",
        "status": "draft",
        "notes": "",
    },
    {
        "task_id": "moodle_05",
        "task_group": "Grading",
        "goal": "Grade an essay using multiple rubric criteria.",
        "context_inputs": "Essay, multi-criteria rubric",
        "subtasks": "Retrieve -> Evaluate each criterion -> Aggregate -> Save",
        "decision_rule": "Aggregate weighted rubric score.",
        "allowed_actions": "read_submission, read_rubric, enter_grade",
        "expected_output": "grade=number",
        "owner": "",
        "status": "draft",
        "notes": "",
    },
    {
        "task_id": "moodle_06",
        "task_group": "Grading",
        "goal": "Penalize flawed reasoning despite a correct conclusion.",
        "context_inputs": "Answer, rubric",
        "subtasks": "Identify reasoning flaw -> Reduce score",
        "decision_rule": "If reasoning is incorrect, reduce score even if final conclusion is correct.",
        "allowed_actions": "read_submission, enter_grade",
        "expected_output": "grade=reduced",
        "owner": "",
        "status": "draft",
        "notes": "",
    },
    {
        "task_id": "moodle_07",
        "task_group": "Grading",
        "goal": "Assign partial credit for incomplete but correct reasoning.",
        "context_inputs": "Answer, rubric",
        "subtasks": "Identify missing components -> Adjust score",
        "decision_rule": "If reasoning is correct but required components are missing, assign partial credit.",
        "allowed_actions": "read_submission, enter_grade",
        "expected_output": "grade=partial",
        "owner": "",
        "status": "draft",
        "notes": "",
    },
    {
        "task_id": "moodle_08",
        "task_group": "Grading",
        "goal": "Re-grade a submission after a student appeal.",
        "context_inputs": "Original answer, appeal text, rubric",
        "subtasks": "Retrieve previous grade -> Re-evaluate -> Update",
        "decision_rule": "If appeal is valid under the rubric, adjust the grade; otherwise keep it unchanged.",
        "allowed_actions": "read_submission, update_grade",
        "expected_output": "grade=updated/unchanged",
        "owner": "",
        "status": "draft",
        "notes": "",
    },
    {
        "task_id": "moodle_09",
        "task_group": "Grading",
        "goal": "Assign a final numeric grade for a summative assignment.",
        "context_inputs": "Rubric, submission",
        "subtasks": "Compute total score -> Save",
        "decision_rule": "Sum rubric scores.",
        "allowed_actions": "enter_grade, save_grade",
        "expected_output": "grade=number",
        "owner": "",
        "status": "draft",
        "notes": "",
    },
    {
        "task_id": "moodle_10",
        "task_group": "Grading",
        "goal": "Assign a pass/fail outcome based on grading criteria.",
        "context_inputs": "Threshold, submission",
        "subtasks": "Evaluate -> Compare threshold",
        "decision_rule": "If score is greater than or equal to threshold, pass; otherwise fail.",
        "allowed_actions": "enter_grade",
        "expected_output": "decision=pass/fail",
        "owner": "",
        "status": "draft",
        "notes": "",
    },
    {
        "task_id": "moodle_11",
        "task_group": "Grading",
        "goal": "Ensure consistent grading across translated versions of the same submission.",
        "context_inputs": "Same answer in multiple languages",
        "subtasks": "Interpret translated submission -> Apply same rubric",
        "decision_rule": "Same meaning should produce the same grade.",
        "allowed_actions": "read_submission, enter_grade",
        "expected_output": "consistent_grade=true",
        "owner": "",
        "status": "draft",
        "notes": "",
    },
    {
        "task_id": "moodle_12",
        "task_group": "Grade State",
        "goal": "Decide whether to keep a grade hidden or release it.",
        "context_inputs": "Grade, workflow state",
        "subtasks": "Check policy -> Decide visibility",
        "decision_rule": "If grading is complete and release conditions are met, release; otherwise keep hidden.",
        "allowed_actions": "update_visibility",
        "expected_output": "state=hidden/released",
        "owner": "",
        "status": "draft",
        "notes": "",
    },
    {
        "task_id": "moodle_13",
        "task_group": "Feedback",
        "goal": "Generate feedback aligned with the rubric.",
        "context_inputs": "Rubric, answer, grade",
        "subtasks": "Identify strengths/weaknesses -> Write feedback",
        "decision_rule": "Feedback must match rubric criteria and assigned grade.",
        "allowed_actions": "write_feedback",
        "expected_output": "feedback=text",
        "owner": "",
        "status": "draft",
        "notes": "",
    },
    {
        "task_id": "moodle_14",
        "task_group": "Feedback",
        "goal": "Generate concise and actionable feedback.",
        "context_inputs": "Answer, rubric",
        "subtasks": "Identify top improvements -> Write concise note",
        "decision_rule": "Keep feedback short, clear, and actionable.",
        "allowed_actions": "write_feedback",
        "expected_output": "feedback=short",
        "owner": "",
        "status": "draft",
        "notes": "",
    },
    {
        "task_id": "moodle_15",
        "task_group": "Extension",
        "goal": "Decide whether to grant an assignment extension.",
        "context_inputs": "Request, policy, evidence",
        "subtasks": "Read request -> Check policy -> Decide",
        "decision_rule": "If valid evidence is provided and the requested duration is within policy, approve.",
        "allowed_actions": "read_request, set_due_date",
        "expected_output": "decision=approve/deny",
        "owner": "",
        "status": "draft",
        "notes": "",
    },
    {
        "task_id": "moodle_16",
        "task_group": "Extension",
        "goal": "Deny an invalid extension request.",
        "context_inputs": "Request, policy",
        "subtasks": "Compare request to policy",
        "decision_rule": "If request violates policy, deny.",
        "allowed_actions": "read_request",
        "expected_output": "decision=deny",
        "owner": "",
        "status": "draft",
        "notes": "",
    },
    {
        "task_id": "moodle_17",
        "task_group": "Extension",
        "goal": "Apply a new due date after approval.",
        "context_inputs": "Approved request, policy",
        "subtasks": "Compute new deadline -> Apply",
        "decision_rule": "New due date must not exceed policy limit.",
        "allowed_actions": "set_due_date",
        "expected_output": "new_due_date=datetime",
        "owner": "",
        "status": "draft",
        "notes": "",
    },
    {
        "task_id": "moodle_18",
        "task_group": "Extension",
        "goal": "Allow a late submission within the grace period.",
        "context_inputs": "Submission time, policy",
        "subtasks": "Compare time to grace window",
        "decision_rule": "If submission time is within allowed grace period, accept.",
        "allowed_actions": "accept_submission",
        "expected_output": "accepted=true",
        "owner": "",
        "status": "draft",
        "notes": "",
    },
    {
        "task_id": "moodle_19",
        "task_group": "Extension",
        "goal": "Deny a submission after cutoff.",
        "context_inputs": "Submission time, cutoff",
        "subtasks": "Compare -> Reject",
        "decision_rule": "If submission is after cutoff, deny.",
        "allowed_actions": "reject_submission",
        "expected_output": "accepted=false",
        "owner": "",
        "status": "draft",
        "notes": "",
    },
    {
        "task_id": "moodle_20",
        "task_group": "Extension",
        "goal": "Override assignment availability for a specific student.",
        "context_inputs": "Policy, student case",
        "subtasks": "Set custom availability",
        "decision_rule": "If approved under policy, apply override only to target student.",
        "allowed_actions": "set_availability",
        "expected_output": "override=true",
        "owner": "",
        "status": "draft",
        "notes": "",
    },
    {
        "task_id": "moodle_21",
        "task_group": "Resubmission",
        "goal": "Reopen an assignment after a technical issue.",
        "context_inputs": "Request, evidence",
        "subtasks": "Verify issue -> Reopen",
        "decision_rule": "If evidence supports a genuine technical issue, reopen.",
        "allowed_actions": "reopen_submission",
        "expected_output": "reopened=true",
        "owner": "",
        "status": "draft",
        "notes": "",
    },
    {
        "task_id": "moodle_22",
        "task_group": "Resubmission",
        "goal": "Reopen an assignment after approval.",
        "context_inputs": "Approval signal",
        "subtasks": "Apply reopening",
        "decision_rule": "If explicit approval exists, reopen.",
        "allowed_actions": "reopen_submission",
        "expected_output": "reopened=true",
        "owner": "",
        "status": "draft",
        "notes": "",
    },
    {
        "task_id": "moodle_23",
        "task_group": "Resubmission",
        "goal": "Deny resubmission when it is not allowed.",
        "context_inputs": "Attempts, policy",
        "subtasks": "Compare attempts to policy",
        "decision_rule": "If maximum attempts are reached or policy forbids reopening, deny.",
        "allowed_actions": "no_action",
        "expected_output": "reopened=false",
        "owner": "",
        "status": "draft",
        "notes": "",
    },
    {
        "task_id": "moodle_24",
        "task_group": "Resubmission",
        "goal": "Revert a submission to draft.",
        "context_inputs": "Submission state",
        "subtasks": "Change to draft",
        "decision_rule": "If resubmission is allowed, revert to draft.",
        "allowed_actions": "revert_to_draft",
        "expected_output": "state=draft",
        "owner": "",
        "status": "draft",
        "notes": "",
    },
    {
        "task_id": "moodle_25",
        "task_group": "Resubmission",
        "goal": "Allow an additional submission attempt.",
        "context_inputs": "Attempts, policy",
        "subtasks": "Increase attempt count",
        "decision_rule": "If policy permits, add one attempt.",
        "allowed_actions": "allow_attempt",
        "expected_output": "attempts_plus_one=true",
        "owner": "",
        "status": "draft",
        "notes": "",
    },
    {
        "task_id": "moodle_26",
        "task_group": "Resubmission",
        "goal": "Keep a submission closed when criteria are unmet.",
        "context_inputs": "Policy, request",
        "subtasks": "Evaluate -> Do nothing",
        "decision_rule": "If criteria are unmet, keep closed.",
        "allowed_actions": "no_action",
        "expected_output": "state=closed",
        "owner": "",
        "status": "draft",
        "notes": "",
    },
    {
        "task_id": "moodle_27",
        "task_group": "Quiz Override",
        "goal": "Grant extra quiz time.",
        "context_inputs": "Request, policy",
        "subtasks": "Check eligibility -> Apply override",
        "decision_rule": "If student is eligible under policy, extend time.",
        "allowed_actions": "set_quiz_time",
        "expected_output": "time_extended=true",
        "owner": "",
        "status": "draft",
        "notes": "",
    },
    {
        "task_id": "moodle_28",
        "task_group": "Quiz Override",
        "goal": "Deny extra quiz time.",
        "context_inputs": "Request, policy",
        "subtasks": "Compare to rules",
        "decision_rule": "If request is not eligible, deny.",
        "allowed_actions": "no_action",
        "expected_output": "decision=deny",
        "owner": "",
        "status": "draft",
        "notes": "",
    },
    {
        "task_id": "moodle_29",
        "task_group": "Quiz Override",
        "goal": "Grant an additional quiz attempt.",
        "context_inputs": "Attempts, policy",
        "subtasks": "Check eligibility -> Update",
        "decision_rule": "If permitted under policy, allow extra attempt.",
        "allowed_actions": "set_attempts",
        "expected_output": "attempts_plus_one=true",
        "owner": "",
        "status": "draft",
        "notes": "",
    },
    {
        "task_id": "moodle_30",
        "task_group": "Quiz Override",
        "goal": "Deny an additional quiz attempt.",
        "context_inputs": "Attempts, policy",
        "subtasks": "Compare -> Deny",
        "decision_rule": "If policy does not allow additional attempts, deny.",
        "allowed_actions": "no_action",
        "expected_output": "decision=deny",
        "owner": "",
        "status": "draft",
        "notes": "",
    },
    {
        "task_id": "moodle_31",
        "task_group": "Quiz Override",
        "goal": "Set a different quiz availability window.",
        "context_inputs": "Request, policy",
        "subtasks": "Set new start/end time",
        "decision_rule": "If approved, apply updated availability window.",
        "allowed_actions": "set_quiz_window",
        "expected_output": "window_updated=true",
        "owner": "",
        "status": "draft",
        "notes": "",
    },
    {
        "task_id": "moodle_32",
        "task_group": "Quiz Override",
        "goal": "Create a make-up quiz opportunity.",
        "context_inputs": "Missed attempt, policy",
        "subtasks": "Create new window/attempt",
        "decision_rule": "If make-up is valid under policy, enable alternative access.",
        "allowed_actions": "create_makeup",
        "expected_output": "available=true",
        "owner": "",
        "status": "draft",
        "notes": "",
    },
    {
        "task_id": "moodle_33",
        "task_group": "Academic Integrity",
        "goal": "Flag a submission based on a plagiarism report.",
        "context_inputs": "Submission, similarity score, report",
        "subtasks": "Evaluate similarity -> Flag",
        "decision_rule": "If similarity exceeds threshold and context suggests misconduct, flag.",
        "allowed_actions": "flag_submission",
        "expected_output": "flagged=true",
        "owner": "",
        "status": "draft",
        "notes": "",
    },
    {
        "task_id": "moodle_34",
        "task_group": "Academic Integrity",
        "goal": "Avoid flagging when plagiarism is not present.",
        "context_inputs": "Submission, low similarity, citation context",
        "subtasks": "Evaluate -> No flag",
        "decision_rule": "If report is below threshold or citations are valid, do not flag.",
        "allowed_actions": "no_action",
        "expected_output": "flagged=false",
        "owner": "",
        "status": "draft",
        "notes": "",
    },
    {
        "task_id": "moodle_35",
        "task_group": "Academic Integrity",
        "goal": "Defer grading due to suspected misconduct.",
        "context_inputs": "Submission, report",
        "subtasks": "Detect issue -> Stop grading",
        "decision_rule": "If integrity concern is strong enough under policy, hold grading.",
        "allowed_actions": "hold_grade",
        "expected_output": "status=deferred",
        "owner": "",
        "status": "draft",
        "notes": "",
    },
    {
        "task_id": "moodle_36",
        "task_group": "Academic Integrity",
        "goal": "Proceed with grading when safe.",
        "context_inputs": "Submission, report",
        "subtasks": "Verify clean -> Grade",
        "decision_rule": "If no integrity issue is found, continue normal grading.",
        "allowed_actions": "enter_grade",
        "expected_output": "graded=true",
        "owner": "",
        "status": "draft",
        "notes": "",
    },
    {
        "task_id": "moodle_37",
        "task_group": "Academic Integrity",
        "goal": "Escalate a borderline integrity case to a human.",
        "context_inputs": "Submission, ambiguous report",
        "subtasks": "Detect uncertainty -> Escalate",
        "decision_rule": "If evidence is borderline or ambiguous, request manual review.",
        "allowed_actions": "escalate_case",
        "expected_output": "escalated=true",
        "owner": "",
        "status": "draft",
        "notes": "",
    },
    {
        "task_id": "moodle_38",
        "task_group": "Academic Integrity",
        "goal": "Avoid false-positive plagiarism flags.",
        "context_inputs": "Submission, citation context, report",
        "subtasks": "Distinguish legitimate citation from misconduct",
        "decision_rule": "If overlap is legitimate quotation or citation, do not flag.",
        "allowed_actions": "no_action",
        "expected_output": "flagged=false",
        "owner": "",
        "status": "draft",
        "notes": "",
    },
]


COLUMNS = [
    "task_id",
    "task_group",
    "goal",
    "context_inputs",
    "subtasks",
    "decision_rule",
    "allowed_actions",
    "expected_output",
    "owner",
    "status",
    "notes",
    "updated_at",
]


def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS tasks (
            task_id TEXT PRIMARY KEY,
            task_group TEXT NOT NULL,
            goal TEXT NOT NULL,
            context_inputs TEXT NOT NULL,
            subtasks TEXT NOT NULL,
            decision_rule TEXT NOT NULL,
            allowed_actions TEXT NOT NULL,
            expected_output TEXT NOT NULL,
            owner TEXT DEFAULT '',
            status TEXT DEFAULT 'draft',
            notes TEXT DEFAULT '',
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.commit()

    count = conn.execute("SELECT COUNT(*) AS c FROM tasks").fetchone()["c"]
    if count == 0:
        now = datetime.utcnow().isoformat(timespec="seconds")
        rows = [{**row, "updated_at": now} for row in DEFAULT_ROWS]
        conn.executemany(
            """
            INSERT INTO tasks (
                task_id, task_group, goal, context_inputs, subtasks,
                decision_rule, allowed_actions, expected_output,
                owner, status, notes, updated_at
            ) VALUES (
                :task_id, :task_group, :goal, :context_inputs, :subtasks,
                :decision_rule, :allowed_actions, :expected_output,
                :owner, :status, :notes, :updated_at
            )
            """,
            rows,
        )
        conn.commit()
    conn.close()


def load_tasks() -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query("SELECT * FROM tasks ORDER BY task_id", conn)
    conn.close()
    return df


def save_dataframe(df: pd.DataFrame):
    df = df.copy()
    now = datetime.utcnow().isoformat(timespec="seconds")
    df["updated_at"] = now
    df = df[COLUMNS]

    conn = get_conn()
    conn.execute("DELETE FROM tasks")
    conn.executemany(
        """
        INSERT INTO tasks (
            task_id, task_group, goal, context_inputs, subtasks,
            decision_rule, allowed_actions, expected_output,
            owner, status, notes, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        df.itertuples(index=False, name=None),
    )
    conn.commit()
    conn.close()


def export_json(df: pd.DataFrame) -> str:
    records = df.fillna("").to_dict(orient="records")
    return json.dumps(records, indent=2, ensure_ascii=False)


st.set_page_config(page_title="VANTA Task Editor", page_icon="🧩", layout="wide")
init_db()

st.title("🧩 VANTA Task Editor")
st.caption("Structured task specs for collaborative review and editing.")

df = load_tasks()

with st.sidebar:
    st.header("Filters")
    groups = sorted(df["task_group"].dropna().unique().tolist())
    selected_groups = st.multiselect("Task group", groups, default=groups)

    statuses = sorted(df["status"].dropna().unique().tolist())
    selected_statuses = st.multiselect("Status", statuses, default=statuses)

    owners = sorted([o for o in df["owner"].dropna().unique().tolist() if o])
    selected_owners = st.multiselect("Owner", owners, default=owners)

    query = st.text_input("Search")
    st.divider()

    csv_data = df.to_csv(index=False).encode("utf-8")
    st.download_button("Download CSV", csv_data, "vanta_tasks.csv", "text/csv")

    json_data = export_json(df).encode("utf-8")
    st.download_button("Download JSON", json_data, "vanta_tasks.json", "application/json")

filtered = df.copy()
if selected_groups:
    filtered = filtered[filtered["task_group"].isin(selected_groups)]
if selected_statuses:
    filtered = filtered[filtered["status"].isin(selected_statuses)]
if selected_owners:
    filtered = filtered[filtered["owner"].isin(selected_owners)]
if query:
    q = query.lower()
    mask = filtered.astype(str).apply(lambda col: col.str.lower().str.contains(q, na=False))
    filtered = filtered[mask.any(axis=1)]

tab1, tab2, tab3 = st.tabs(["Table Editor", "Single Task View", "Add New Task"])

with tab1:
    st.subheader("Editable table")
    edited = st.data_editor(
        filtered,
        use_container_width=True,
        hide_index=True,
        num_rows="dynamic",
        column_config={
            "task_id": st.column_config.TextColumn("Task ID", required=True),
            "task_group": st.column_config.TextColumn("Task Group", required=True),
            "goal": st.column_config.TextColumn("Goal", required=True, width="large"),
            "context_inputs": st.column_config.TextColumn("Context (Inputs)", width="large"),
            "subtasks": st.column_config.TextColumn("Subtasks", width="large"),
            "decision_rule": st.column_config.TextColumn("Decision Rule", width="large"),
            "allowed_actions": st.column_config.TextColumn("Allowed Actions", width="large"),
            "expected_output": st.column_config.TextColumn("Expected Output", width="large"),
            "owner": st.column_config.TextColumn("Owner"),
            "status": st.column_config.SelectboxColumn(
                "Status",
                options=["draft", "review", "ready", "archived"],
                required=True,
            ),
            "notes": st.column_config.TextColumn("Notes", width="large"),
            "updated_at": st.column_config.TextColumn("Updated At", disabled=True),
        },
        key="task_editor",
    )

    col1, col2 = st.columns([1, 1])
    with col1:
        if st.button("Save table changes", type="primary"):
            current = df.set_index("task_id", drop=False).copy()
            edited_copy = edited.copy()

            if edited_copy["task_id"].duplicated().any():
                st.error("Task ID values must be unique.")
            elif edited_copy["task_id"].astype(str).str.strip().eq("").any():
                st.error("Task ID cannot be empty.")
            else:
                save_dataframe(edited_copy.reindex(columns=COLUMNS, fill_value=""))
                st.success("Changes saved.")
                st.rerun()

    with col2:
        if st.button("Reset database to default seed"):
            if DB_PATH.exists():
                DB_PATH.unlink()
            init_db()
            st.success("Reset complete.")
            st.rerun()

with tab2:
    st.subheader("Single task view")
    if filtered.empty:
        st.info("No tasks match the current filters.")
    else:
        task_options = filtered["task_id"].tolist()
        selected = st.selectbox("Choose task", task_options)
        row = filtered[filtered["task_id"] == selected].iloc[0]

        st.markdown(f"### {row['task_id']} — {row['task_group']}")
        st.write(f"**Goal:** {row['goal']}")
        st.write(f"**Context (Inputs):** {row['context_inputs']}")
        st.write(f"**Subtasks:** {row['subtasks']}")
        st.write(f"**Decision Rule:** {row['decision_rule']}")
        st.write(f"**Allowed Actions:** {row['allowed_actions']}")
        st.write(f"**Expected Output:** {row['expected_output']}")
        st.write(f"**Owner:** {row['owner'] or '-'}")
        st.write(f"**Status:** {row['status']}")
        st.write(f"**Notes:** {row['notes'] or '-'}")
        st.caption(f"Updated at: {row['updated_at']}")

with tab3:
    st.subheader("Add new task")
    with st.form("new_task_form", clear_on_submit=True):
        task_id = st.text_input("Task ID")
        task_group = st.text_input("Task Group")
        goal = st.text_area("Goal", height=80)
        context_inputs = st.text_area("Context (Inputs)", height=100)
        subtasks = st.text_area("Subtasks", height=120)
        decision_rule = st.text_area("Decision Rule", height=100)
        allowed_actions = st.text_area("Allowed Actions", height=80)
        expected_output = st.text_area("Expected Output", height=80)
        owner = st.text_input("Owner")
        status = st.selectbox("Status", ["draft", "review", "ready", "archived"])
        notes = st.text_area("Notes", height=80)

        submitted = st.form_submit_button("Add task")
        if submitted:
            task_id = task_id.strip()
            if not task_id:
                st.error("Task ID is required.")
            elif task_id in df["task_id"].tolist():
                st.error("Task ID already exists.")
            else:
                new_row = pd.DataFrame(
                    [
                        {
                            "task_id": task_id,
                            "task_group": task_group.strip(),
                            "goal": goal.strip(),
                            "context_inputs": context_inputs.strip(),
                            "subtasks": subtasks.strip(),
                            "decision_rule": decision_rule.strip(),
                            "allowed_actions": allowed_actions.strip(),
                            "expected_output": expected_output.strip(),
                            "owner": owner.strip(),
                            "status": status,
                            "notes": notes.strip(),
                            "updated_at": datetime.utcnow().isoformat(timespec="seconds"),
                        }
                    ]
                )
                updated = pd.concat([df, new_row], ignore_index=True)
                save_dataframe(updated)
                st.success("Task added.")
                st.rerun()
