# PayLens usability and documentation study protocol

**Purpose:** Measure task effort and documentation usefulness for the same FinTech database tasks on ClickHouse and Exasol. This file is a protocol, not a completed study; no participant data has been collected.

## Study setup

1. Recruit at least 3 participants who regularly write SQL. Record their database and ClickHouse/Exasol experience before the tasks.
2. Use the same laptop class, dataset size, schema, network, and client for each participant. Give each participant isolated/resettable environments so earlier tasks do not alter later tasks.
3. Use a within-participant crossover: randomly assign half the participants to ClickHouse first, and half to Exasol first. Keep the task order the same within each engine.
4. Give only the task prompt and connection details. Allow official documentation and normal search; record each documentation page consulted. Do not coach participants unless they are blocked, and record any assistance.
5. Start a screen recording or observer log only with participant consent. Do not collect credentials or payment data.

## Tasks and completion checks

| ID | Task prompt | Completion check |
|---|---|---|
| T1 Connect | Connect to the provided local database and identify the version. | Query returns the expected server/version. |
| T2 Table layout | Create an authorization-event table with monthly retention/partition intent and a card lookup access path. | DDL succeeds; participant can explain the chosen sort/distribution/partition/index mechanism. |
| T3 Selective query | Find one card's recent authorization count and spend; inspect why the engine reads the data it does. | Correct result; participant identifies relevant plan/profile evidence. |
| T4 Retention | Configure or implement removal of events older than the given retention window and explain when query visibility and physical storage change. | Policy or procedure works on expired/unexpired scratch rows; timing semantics are correctly described. |
| T5 Ledger update | Change one synthetic authorization from AUTH to SETTLED and verify the visible current state. | Correct state and row count; participant explains mutation/transaction behavior. |
| T6 Documentation lookup | Locate official documentation for index behavior, retention, and one SQL/JSON feature. | Source URL and a one-sentence supported/unsupported/unclear conclusion for each item. |

Use a fixed dataset and the same SQL result oracle for both systems. Permit each participant to use their preferred editor, but record it and keep it constant across that participant's two sessions.

## Measurements

Record for every task and engine:

- time from prompt reveal to successful completion (seconds); record timeout as 900 seconds;
- completion outcome: success, partial, timeout, or abandon;
- number of distinct SQL/CLI commands, documentation pages opened, errors, and requests for help;
- tuning/actions taken (for example, changing sort key, adding an index, materializing an index, scheduling a delete);
- participant confidence from 1 (guessing) to 5 (certain), plus correctness of their explanation;
- documentation findability (time to relevant page), clarity (1–5), and whether the page answered the task.

Do not combine these into a winner score until the weighting is agreed in advance. Report per-task medians and ranges, participant experience, task failures, and order effects. Treat the small sample as exploratory, not representative of all developers.

## Blank observation row

Copy one row per participant, task, and engine into the study log. Use ISO dates and seconds. Keep notes factual; do not record secrets.

| participant_id | experience_years_sql | experience_clickhouse | experience_exasol | engine_order | engine | task_id | outcome | elapsed_seconds | commands | errors | help_requests | docs_pages | docs_find_seconds | docs_clarity_1_to_5 | confidence_1_to_5 | explanation_correct | tuning_actions | notes |
|---|---:|---|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|
| | | | | | | | | | | | | | | | | | | |

## Reporting template

For each task, report ClickHouse and Exasol completion counts, median and range of elapsed time, median command/error/help counts, documentation find time and clarity, and explanation correctness. Include anonymized participant experience and task order. Attach the blank protocol and anonymized raw observations to the final report. Never present an unrun protocol or opinion scorecard as measured usability.
