import os
from datetime import datetime

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st

API_URL = os.getenv("ATLAS_API_URL", "http://127.0.0.1:8000")

st.set_page_config(
    page_title="ATLAS | Command Center",
    page_icon="◈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------
# DESIGN SYSTEM
# ---------------------------------------------------------

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=Inter:wght@300;400;500;600;700;800&display=swap');

:root {
    --bg: #080B12;
    --panel: #101622;
    --panel2: #141D2B;
    --line: rgba(148,163,184,.13);
    --blue: #54BFFF;
    --violet: #A78BFA;
    --green: #45E0B0;
    --muted: #8491A6;
}

.stApp {
    background:
      radial-gradient(ellipse at 70% -10%, rgba(53,91,170,.17), transparent 38%),
      radial-gradient(ellipse at 5% 35%, rgba(80,54,150,.08), transparent 35%),
      var(--bg);
    color: #F1F5F9;
    font-family: 'Inter', sans-serif;
}

header[data-testid="stHeader"] { background: transparent; }
.block-container { padding-top: 2rem; padding-bottom: 3rem; max-width: 1800px; }

[data-testid="stSidebar"] {
    background: #0B101A;
    border-right: 1px solid var(--line);
}

[data-testid="stSidebar"] * { color: #DCE5F2; }

.brand {
    font-size: 26px;
    font-weight: 800;
    letter-spacing: 5px;
    color: #F8FAFC;
}
.brand span { color: var(--blue); }

.eyebrow {
    color: var(--muted);
    font-family: 'DM Mono', monospace;
    font-size: 10px;
    letter-spacing: 2px;
    text-transform: uppercase;
}

.hero {
    padding: 8px 0 24px 0;
    border-bottom: 1px solid var(--line);
    margin-bottom: 25px;
}

.hero h1 {
    font-size: clamp(30px, 4vw, 43px);
    font-weight: 700;
    letter-spacing: -2px;
    margin: 5px 0;
    color: #F8FAFC;
}

.hero p { color: #8F9DB1; font-size: 13px; }

.metric-card {
    background: linear-gradient(145deg, rgba(24,35,52,.96), rgba(14,20,31,.96));
    border: 1px solid var(--line);
    border-radius: 15px;
    padding: 19px 20px;
    min-height: 133px;
    box-shadow: 0 10px 35px rgba(0,0,0,.13);
}

.metric-label {
    font-size: 10px;
    color: #91A0B5;
    letter-spacing: 1.4px;
    text-transform: uppercase;
}
.metric-value {
    font-size: 31px;
    font-weight: 700;
    letter-spacing: -1.4px;
    margin: 12px 0 4px;
    color: #F8FAFC;
}
.metric-foot { font-size: 11px; color: #8796AB; }

.panel {
    background: linear-gradient(145deg, rgba(19,28,42,.95), rgba(13,19,29,.95));
    border: 1px solid var(--line);
    border-radius: 15px;
    padding: 18px;
    margin-bottom: 15px;
}

.panel-title {
    font-size: 14px;
    font-weight: 600;
    color: #E8EEF7;
}
.panel-subtitle {
    font-size: 10px;
    color: #8290A4;
    letter-spacing: 1px;
    margin-top: 4px;
}

.status-dot {
    display: inline-block;
    width: 7px;
    height: 7px;
    border-radius: 50%;
    background: #45E0B0;
    box-shadow: 0 0 12px #45E0B0;
    margin-right: 7px;
}

.status-text {
    font-family: 'DM Mono', monospace;
    font-size: 10px;
    color: #45E0B0;
    letter-spacing: 1px;
}

div[data-testid="stMetric"] {
    background: transparent;
}
div[data-testid="stMetricValue"] {
    color: #F8FAFC;
}
div[data-testid="stDataFrame"] {
    border: 1px solid var(--line);
    border-radius: 10px;
}

.stButton button {
    background: linear-gradient(110deg, #237BC1, #6154C7);
    color: white;
    border: none;
    border-radius: 8px;
    font-weight: 600;
}

.stButton button:hover {
    border: 1px solid #54BFFF;
    color: white;
}

section[data-testid="stSidebar"] .stRadio label {
    font-size: 13px;
}

footer {visibility: hidden;}
#MainMenu {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------
# API CLIENT
# ---------------------------------------------------------

@st.cache_data(ttl=5)
def fetch_endpoint(endpoint):
    response = requests.get(
        f"{API_URL}{endpoint}",
        timeout=5,
    )
    response.raise_for_status()
    return response.json()


def load_data():
    data = {}
    errors = {}

    endpoints = {
        "health": "/health",
        "summary": "/api/v1/analytics/summary",
        "telemetry": "/api/v1/analytics/telemetry",
        "types": "/api/v1/analytics/event-types",
        "daily": "/api/v1/analytics/daily",
        "events": "/api/v1/events?limit=100",
        "pipeline_runs": "/api/v1/pipeline/runs?limit=50",
    }

    for key, endpoint in endpoints.items():
        try:
            data[key] = fetch_endpoint(endpoint)
        except Exception as exc:
            errors[key] = str(exc)

    return data, errors


data, errors = load_data()

health = data.get("health", {})
summary = data.get("summary", {})
types = data.get("types", [])
daily = data.get("daily", [])
events = data.get("events", [])
telemetry = data.get("telemetry", {})
pipeline_runs = data.get("pipeline_runs", [])
services_health = health.get("services", {})
services_health = health.get("services", {})

api_online = services_health.get("api", {}).get("status") == "online"
platform_online = health.get("platform_status") == "healthy"

# ---------------------------------------------------------
# SIDEBAR
# ---------------------------------------------------------

with st.sidebar:
    st.markdown(
        '<div class="brand">ATLAS<span>.</span></div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        '<div class="eyebrow">DATA INTELLIGENCE PLATFORM</div>',
        unsafe_allow_html=True,
    )

    st.markdown("---")

    page = st.radio(
        "NAVIGATION",
        [
            "Command Center",
            "Data Explorer",
            "Event Intelligence",
            "Pipeline Operations",
            "System Health",
            "Pipeline System Health",
        ],
        label_visibility="visible",
    )

    st.markdown("---")

    st.markdown('<div class="eyebrow">ENVIRONMENT</div>', unsafe_allow_html=True)
    st.markdown("**LOCAL DEVELOPMENT**")

    if api_online:
        st.markdown(
            '<span class="status-dot"></span><span class="status-text">API CONNECTED</span>',
            unsafe_allow_html=True,
        )
    else:
        st.error("API DISCONNECTED")

    st.markdown("---")

    if st.button("↻  REFRESH DATA", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.caption("ATLAS PLATFORM · VERSION 1.0")
    st.caption("Analytics Command Interface")

# ---------------------------------------------------------
# HEADER
# ---------------------------------------------------------

now = datetime.now().astimezone().strftime("%Y-%m-%d  %H:%M:%S %Z")

st.markdown(f"""
<div class="hero">
    <div class="eyebrow">ATLAS / INTELLIGENCE SYSTEM / 01</div>
    <h1>{page}</h1>
    <p>Unified data intelligence, operational visibility and pipeline analytics.</p>
    <span class="status-dot"></span>
    <span class="status-text">{"SYSTEM OPERATIONAL" if platform_online else "SYSTEM DEGRADED"}</span>
    <span style="float:right;color:#8290A4;font-size:10px;font-family:monospace">
        {now}
    </span>
</div>
""", unsafe_allow_html=True)

# ---------------------------------------------------------
# LIVE INFRASTRUCTURE STATUS
# ---------------------------------------------------------

service_items = [
    ("POSTGRESQL", services_health.get("postgres", {})),
    ("REDPANDA", services_health.get("redpanda", {})),
    ("SEAWEEDFS OBJECT STORAGE", services_health.get("object_storage", {})),
]

service_columns = st.columns(3)

for column, (service_name, service_info) in zip(service_columns, service_items):
    service_state = service_info.get("status", "unknown").upper()
    service_online = service_state == "ONLINE"
    status_color = "#45E0B0" if service_online else "#FF6B7A"
    latency = service_info.get("latency_ms")
    latency_label = f"{latency:.1f} ms" if isinstance(latency, (int, float)) else "Latency unavailable"

    with column:
        st.markdown(
            f"""
            <div class="panel" style="padding:13px 15px">
              <div class="panel-subtitle">{service_name}</div>
              <div style="display:flex;align-items:center;justify-content:space-between;margin-top:9px">
                <span style="color:{status_color};font-family:monospace;font-size:12px;font-weight:700">
                  ● {service_state}
                </span>
                <span style="color:#8796AB;font-size:11px">{latency_label}</span>
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

if errors:
    with st.expander("Connection diagnostics"):
        for key, error in errors.items():
            st.warning(f"{key}: {error}")

# ---------------------------------------------------------
# METRIC COMPONENT
# ---------------------------------------------------------

def metric_card(label, value, foot, accent="#54BFFF"):
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">{label}</div>
        <div class="metric-value" style="color:{accent}">{value}</div>
        <div class="metric-foot">{foot}</div>
    </div>
    """, unsafe_allow_html=True)


def panel_heading(title, subtitle):
    st.markdown(f"""
    <div style="margin:12px 0 15px">
      <div class="panel-title">{title}</div>
      <div class="panel-subtitle">{subtitle}</div>
    </div>
    """, unsafe_allow_html=True)


# ---------------------------------------------------------
# COMMAND CENTER
# ---------------------------------------------------------

if page == "Command Center":

    if not summary:
        st.error("Analytics summary unavailable. Check the FastAPI service.")
        st.stop()

    total = int(summary.get("total_events", 0))
    unique = int(summary.get("unique_events", 0))
    sources = int(summary.get("total_sources", 0))
    event_types = int(summary.get("total_event_types", 0))

    c1, c2, c3, c4 = st.columns(4)

    with c1:
        metric_card("TOTAL EVENTS", f"{total:,}", "Processed event records")

    with c2:
        metric_card("UNIQUE EVENTS", f"{unique:,}", "Unique event identifiers", "#45E0B0")

    with c3:
        metric_card("DATA SOURCES", f"{sources:,}", "Connected event sources", "#A78BFA")

    with c4:
        metric_card("EVENT TYPES", f"{event_types:,}", "Distinct event categories", "#F6B96B")

    st.markdown("")
    panel_heading("LIVE INGESTION TELEMETRY", "MEASURED FROM POSTGRESQL EVENT RECORDS")

    t1, t2, t3, t4 = st.columns(4)

    last_minute = int(telemetry.get("events_last_minute") or 0)
    last_five = int(telemetry.get("events_last_5_minutes") or 0)
    partitions = int(telemetry.get("active_partitions") or 0)
    latest_offset = telemetry.get("latest_kafka_offset")
    event_age = telemetry.get("seconds_since_latest_event")

    with t1:
        metric_card("EVENTS / LAST MINUTE", f"{last_minute:,}", "Newly ingested records", "#54BFFF")
    with t2:
        metric_card("EVENTS / LAST 5 MIN", f"{last_five:,}", "Recent ingestion activity", "#45E0B0")
    with t3:
        metric_card("ACTIVE PARTITIONS", f"{partitions:,}", "Observed Kafka partitions", "#A78BFA")
    with t4:
        offset_label = f"{int(latest_offset):,}" if latest_offset is not None else "—"
        age_label = f"Last event {event_age / 60:.1f} min ago" if isinstance(event_age, (int, float)) else "No event timestamp"
        metric_card("LATEST KAFKA OFFSET", offset_label, age_label, "#F6B96B")

    if last_five == 0:
        st.caption("Stream status: idle — no events were recorded in the last five minutes. Metrics are not simulated.")

    st.markdown("")

    left, right = st.columns([1.65, 1])

    with left:
        panel_heading("EVENT STREAM ANALYTICS", "EVENT DISTRIBUTION OVER TIME")

        df = pd.DataFrame(daily)

        if not df.empty:
            df["event_date"] = pd.to_datetime(df["event_date"])

            fig = go.Figure()

            fig.add_trace(go.Scatter(
                x=df["event_date"],
                y=df["total_events"],
                mode="lines+markers",
                name="Events",
                line=dict(color="#54BFFF", width=3, shape="spline"),
                marker=dict(size=8, color="#54BFFF"),
                fill="tozeroy",
                fillcolor="rgba(84,191,255,0.09)",
                hovertemplate="%{x|%Y-%m-%d}<br>Events: %{y}<extra></extra>",
            ))

            fig.update_layout(
                height=310,
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#9AA9BE", family="Inter"),
                margin=dict(l=10, r=10, t=20, b=10),
                xaxis=dict(showgrid=False, zeroline=False),
                yaxis=dict(
                    showgrid=True,
                    gridcolor="rgba(148,163,184,.10)",
                    zeroline=False,
                ),
                hovermode="x unified",
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No daily analytics available yet.")

    with right:
        panel_heading("EVENT COMPOSITION", "BREAKDOWN BY SOURCE")

        df_types = pd.DataFrame(types)

        if not df_types.empty:
            fig = px.pie(
                df_types,
                names="event_type",
                values="total_events",
                hole=0.75,
            )
            fig.update_traces(
                textinfo="percent",
                textfont=dict(color="white"),
                marker=dict(line=dict(color="#101622", width=3)),
            )
            fig.update_layout(
                height=310,
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#AAB7CA"),
                showlegend=True,
                legend=dict(font=dict(size=10)),
                margin=dict(l=5, r=5, t=15, b=5),
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No event composition data.")

    st.markdown("---")

    a, b = st.columns([1.1, 1])

    with a:
        panel_heading("SOURCE PERFORMANCE", "EVENT VOLUME BY SOURCE")

        if not df_types.empty:
            fig = px.bar(
                df_types,
                x="source",
                y="total_events",
                color="event_type",
                barmode="group",
                color_discrete_sequence=["#54BFFF", "#A78BFA", "#45E0B0"],
            )
            fig.update_layout(
                height=280,
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#AAB7CA"),
                margin=dict(l=10, r=10, t=20, b=10),
                xaxis=dict(showgrid=False),
                yaxis=dict(gridcolor="rgba(148,163,184,.10)"),
                legend=dict(font=dict(size=10)),
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No source metrics.")

    with b:
        panel_heading("PLATFORM STATUS", "SERVICE CONNECTIVITY")

        services = [
            ("FastAPI Analytics", api_online),
            ("PostgreSQL Analytics", api_online),
        ]

        for service, status in services:
            color = "#45E0B0" if status else "#F87171"
            label = "CONNECTED" if status else "UNAVAILABLE"

            st.markdown(f"""
            <div style="display:flex;justify-content:space-between;
                padding:15px 5px;border-bottom:1px solid rgba(148,163,184,.10)">
                <span style="font-size:13px;color:#DCE5F2">{service}</span>
                <span style="font-size:10px;color:{color};font-family:monospace">
                    ● {label}
                </span>
            </div>
            """, unsafe_allow_html=True)

        st.caption("Kafka, Spark and object-storage health are not yet instrumented in the API.")

    st.markdown("---")

    panel_heading("RECENT ACTIVITY", "LATEST INGESTED EVENTS")

    if events:
        recent = pd.DataFrame(events)
        columns = [
            col for col in [
                "event_type", "source", "event_timestamp",
                "kafka_partition", "kafka_offset"
            ] if col in recent.columns
        ]
        st.dataframe(
            recent[columns],
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No events available.")

# ---------------------------------------------------------
# PIPELINE OPERATIONS
# ---------------------------------------------------------

elif page == "Pipeline Operations":
    panel_heading(
        "PIPELINE OPERATIONS",
        "ORCHESTRATED SPARK RUNS, STAGE RETRIES AND DATA-QUALITY GATES",
    )

    if not pipeline_runs:
        st.info(
            "No pipeline runs are recorded yet. Start an orchestration run with "
            "`PYTHONPATH=src python -m atlas.processing.pipeline_runner`."
        )
    else:
        runs_df = pd.DataFrame(pipeline_runs)
        successful = int((runs_df["status"] == "SUCCESS").sum()) if "status" in runs_df else 0
        failed = int((runs_df["status"] == "FAILED").sum()) if "status" in runs_df else 0
        latest = runs_df.iloc[0]

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            metric_card("RECENT RUNS", f"{len(runs_df):,}", "Most recent 50 pipeline runs")
        with c2:
            metric_card("SUCCEEDED", f"{successful:,}", "Within the displayed run history", "#45E0B0")
        with c3:
            metric_card("FAILED", f"{failed:,}", "Within the displayed run history", "#F87171" if failed else "#45E0B0")
        with c4:
            metric_card("LATEST STATUS", str(latest.get("status", "UNKNOWN")), str(latest.get("started_at", "")), "#45E0B0" if latest.get("status") == "SUCCESS" else "#F6B96B")

        st.markdown("---")
        panel_heading("PIPELINE RUN HISTORY", "DURABLE RUN AND STAGE METADATA FROM POSTGRESQL")
        visible_columns = [
            name for name in [
                "run_id", "status", "started_at", "finished_at",
                "input_records", "output_records", "quality_passed",
            ] if name in runs_df.columns
        ]
        st.dataframe(runs_df[visible_columns], use_container_width=True, hide_index=True)

        run_ids = [str(value) for value in runs_df["run_id"].tolist()]
        selected_run = st.selectbox(
            "INSPECT PIPELINE RUN",
            run_ids,
            format_func=lambda value: f"{value[:8]}…  |  {next((r.get('status', 'UNKNOWN') for r in pipeline_runs if str(r.get('run_id')) == value), 'UNKNOWN')}",
        )
        try:
            detail = fetch_endpoint(f"/api/v1/pipeline/runs/{selected_run}")
            steps = detail.get("steps", [])
            checks = detail.get("quality_checks", [])
            left, right = st.columns(2)
            with left:
                panel_heading("EXECUTION STAGES", "EACH RETRY IS RECORDED AS A SEPARATE ATTEMPT")
                if steps:
                    st.dataframe(pd.DataFrame(steps), use_container_width=True, hide_index=True)
                else:
                    st.caption("No stage records found for this run.")
            with right:
                panel_heading("DATA-QUALITY GATES", "SPARK VALIDATION RESULTS FOR THIS RUN")
                if checks:
                    st.dataframe(pd.DataFrame(checks), use_container_width=True, hide_index=True)
                else:
                    st.caption("No quality checks were persisted for this run.")
            run_error = detail.get("run", {}).get("error_message")
            if run_error:
                st.error(run_error)
        except Exception as exc:
            st.warning(f"Could not load pipeline run details: {exc}")


# ---------------------------------------------------------
# DATA EXPLORER
# ---------------------------------------------------------
# ---------------------------------------------------------
# DATA EXPLORER
# ---------------------------------------------------------

elif page == "Data Explorer":
    panel_heading("DATA EXPLORER", "QUERY THE ATLAS ANALYTICS DATASET")

    if events:
        df = pd.DataFrame(events)

        if "event_type" in df.columns:
            selected = st.multiselect(
                "Filter by event type",
                sorted(df["event_type"].dropna().unique()),
                default=sorted(df["event_type"].dropna().unique()),
            )
            df = df[df["event_type"].isin(selected)]

        if "source" in df.columns:
            selected_sources = st.multiselect(
                "Filter by source",
                sorted(df["source"].dropna().unique()),
                default=sorted(df["source"].dropna().unique()),
            )
            df = df[df["source"].isin(selected_sources)]

        st.metric("MATCHING RECORDS", len(df))
        st.dataframe(df, use_container_width=True, hide_index=True)

        st.download_button(
            "↓ EXPORT CSV",
            df.to_csv(index=False),
            file_name="atlas_events.csv",
            mime="text/csv",
        )
    else:
        st.warning("No event records returned by the API.")

# ---------------------------------------------------------
# EVENT INTELLIGENCE
# ---------------------------------------------------------

elif page == "Event Intelligence":
    panel_heading("EVENT INTELLIGENCE", "EVENT TYPES, SOURCES AND DISTRIBUTION")

    if types:
        df = pd.DataFrame(types)

        st.dataframe(df, use_container_width=True, hide_index=True)

        fig = px.bar(
            df,
            x="event_type",
            y="total_events",
            color="source",
            text="total_events",
            color_discrete_sequence=["#54BFFF", "#A78BFA"],
        )
        fig.update_layout(
            template="plotly_dark",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            height=400,
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Event intelligence data unavailable.")

# ---------------------------------------------------------
# SYSTEM HEALTH
# ---------------------------------------------------------

elif page in ("System Health", "Pipeline System Health"):
    panel_heading(
        "PIPELINE SYSTEM HEALTH" if page == "Pipeline System Health" else "SYSTEM HEALTH",
        "BACKEND CONNECTIVITY AND DATA QUALITY",
    )

    c1, c2 = st.columns(2)

    with c1:
        metric_card(
            "API STATUS",
            "ONLINE" if api_online else "OFFLINE",
            "FastAPI health endpoint",
            "#45E0B0" if api_online else "#F87171",
        )

    with c2:
        metric_card(
            "DATABASE STATUS",
            "CONNECTED" if api_online else "UNKNOWN",
            "PostgreSQL connectivity through API",
            "#45E0B0" if api_online else "#F87171",
        )

    st.markdown("---")
    st.markdown("**ENDPOINT DIAGNOSTICS**")

    for endpoint in [
        "/health",
        "/api/v1/analytics/summary",
        "/api/v1/analytics/event-types",
        "/api/v1/analytics/daily",
        "/api/v1/events",
    ]:
        try:
            response = requests.get(API_URL + endpoint, timeout=5)
            st.write(
                f"{'🟢' if response.ok else '🔴'}  `{endpoint}` — HTTP {response.status_code}"
            )
        except requests.RequestException:
            st.write(f"🔴  `{endpoint}` — Connection failed")

st.markdown("""
<div style="text-align:center;padding:35px 0 5px;
border-top:1px solid rgba(148,163,184,.12);margin-top:35px">
<div style="font-size:11px;letter-spacing:4px;color:#7C8BA1;font-weight:700">
ATLAS INTELLIGENCE
</div>
<div style="font-size:9px;color:#56647A;margin-top:7px">
ENGINEERED FOR DATA. BUILT FOR SCALE.
</div>
</div>
""", unsafe_allow_html=True)
