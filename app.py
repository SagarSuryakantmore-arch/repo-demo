# app.py

import streamlit as st
import pandas as pd
from agent import ChatAgent, get_error_analysis, check_openai_connection
from azure_tools import (
    list_pipelines,
    get_pipeline_runs,
    get_run_activity_logs,
    list_all_data_factories_in_subscription,
)

st.set_page_config(layout="wide", page_icon="🤖", page_title="ADF AI Agent")

# --- STATE MANAGEMENT ---
def initialize_state():
    """Initializes session state variables and performs initial data load."""
    if "initialized" in st.session_state:
        return

    st.session_state.initialized = True
    st.session_state.error = None
    st.session_state.all_adfs = []
    st.session_state.selected_rg = None
    st.session_state.selected_adf = None
    st.session_state.chat_agent = ChatAgent()
    if "langchain_messages" not in st.session_state:
        st.session_state.langchain_messages = []
    st.session_state.azure_status = "Disconnected"
    st.session_state.openai_status = "Disconnected"

    with st.spinner("Connecting to Azure..."):
        adfs = list_all_data_factories_in_subscription.invoke({})
        if adfs and isinstance(adfs[0], dict) and "error" in adfs[0]:
            st.session_state.error = adfs[0]["error"]
            st.session_state.azure_status = "Failed"
        else:
            st.session_state.all_adfs = adfs
            st.session_state.azure_status = "Connected"
    
    with st.spinner("Checking OpenAI connection..."):
        if check_openai_connection():
            st.session_state.openai_status = "Connected"
        else:
            st.session_state.openai_status = "Failed"

initialize_state()

# --- HELPER FUNCTIONS ---
def get_status_icon(status):
    status_map = {"Succeeded": "✅", "Failed": "❌", "InProgress": "⏳", "Cancelled": "⚫"}
    return status_map.get(status, "❓")

def metric_card(title, value, color_name="default", icon=""):
    color_map = {
        "green": {"background": "#E8F5E9", "border": "#4CAF50"},
        "red": {"background": "#FFEBEE", "border": "#F44336"},
        "blue": {"background": "#E3F2FD", "border": "#2196F3"},
        "default": {"background": "#f9f9f9", "border": "#ddd"}
    }
    selected_color = color_map.get(color_name, {"background": color_name, "border": color_name})
    st.markdown(
        f"""
        <div style="border-left: 5px solid {selected_color['border']}; background-color: {selected_color['background']}; padding: 8px 12px; border-radius: 5px; margin-bottom: 10px;">
            <div style="font-size: 14px; font-weight: bold; color: #333;">{icon} {title}</div>
            <div style="font-size: 16px; font-weight: bold; color: #000;">{value}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

def display_metrics(df):
    total_runs, succeeded_runs, failed_runs = len(df), len(df[df["status"] == "Succeeded"]), len(df[df["status"] == "Failed"])
    col1, col2, col3 = st.columns(3)
    with col1:
        metric_card("Total Runs", total_runs, color_name="blue", icon="📊")
    with col2:
        metric_card("Succeeded", succeeded_runs, color_name="green", icon="✅")
    with col3:
        metric_card("Failed", failed_runs, color_name="red", icon="❌")

def format_dataframe(df):
    if not df.empty:
        df_display = df.copy()
        df_display["status_icon"] = df_display["status"].apply(get_status_icon)
        df_display = df_display[["status_icon", "pipelineName", "status", "runStart", "runEnd", "durationInMs", "runId"]]
        df_display.rename(columns={"status_icon": ""}, inplace=True)
        return df_display
    return df

def sync_days(source_key, dest_key):
    st.session_state[dest_key] = st.session_state[source_key]

# --- SIDEBAR ---
with st.sidebar:
    st.title("ADF AI Agent 🤖")
    st.subheader("🗂 Resource Selection")
    if st.session_state.error:
        st.error(f"Azure Connection Failed: {st.session_state.error}")
        st.stop()
    if not st.session_state.all_adfs:
        st.warning("No Azure Data Factories found in your subscription.")
        st.stop()
    adf_display_names = [f"{adf['factory_name']} ({adf['resource_group']})" for adf in st.session_state.all_adfs]
    selected_display_name = st.selectbox("Select Data Factory", options=adf_display_names)
    if selected_display_name:
        selected_index = adf_display_names.index(selected_display_name)
        selected_adf_obj = st.session_state.all_adfs[selected_index]
        st.session_state.selected_adf = selected_adf_obj['factory_name']
        st.session_state.selected_rg = selected_adf_obj['resource_group']
    if st.session_state.selected_rg:
        st.info(f"🏷 Resource Group: `{st.session_state.selected_rg}`")
    st.divider()
    st.subheader("🧭 Navigation")
    page = st.radio("Go to", ["Dashboard", "Pipelines", "Error Diagnosis", "Chat Assistance"], label_visibility="collapsed")
    st.divider()
    st.subheader("🔌 Connectivity Status")
    col1, col2 = st.columns(2)
    with col1:
        status_color_azure = "green" if st.session_state.azure_status == "Connected" else "red"
        metric_card("Azure", st.session_state.azure_status, color_name=status_color_azure, icon="☁️")
    with col2:
        status_color_openai = "green" if st.session_state.openai_status == "Connected" else "red"
        metric_card("OpenAI", st.session_state.openai_status, color_name=status_color_openai, icon="🤖")
    st.divider()
    if st.button("🔄 Refresh Data"):
        keys_to_clear = ['initialized', 'all_adfs', 'error', 'azure_status', 'openai_status']
        for key in keys_to_clear:
            if key in st.session_state:
                del st.session_state[key]
        st.rerun()

# --- MAIN CONTENT ---
if not st.session_state.selected_rg or not st.session_state.selected_adf:
    st.info("Please select a Data Factory from the sidebar to continue.")
    st.stop()

if page == "Dashboard":
    st.header("📊 Dashboard")
    st.write(f"Showing data for **{st.session_state.selected_adf}**")
    col1, col2 = st.columns([3, 1])
    with col1:
        st.slider("Select date range (days)", 1, 60, 7, key="dash_slider", on_change=sync_days, args=("dash_slider", "dash_input"))
    with col2:
        st.number_input("Days", 1, 60, 7, key="dash_input", on_change=sync_days, args=("dash_input", "dash_slider"), label_visibility="collapsed")
    days = st.session_state.get("dash_slider", 7)
    with st.spinner(f"Fetching pipeline runs from last {days} days..."):
        runs = get_pipeline_runs.invoke({"resource_group_name": st.session_state.selected_rg, "data_factory_name": st.session_state.selected_adf, "days": days})
        runs_df = pd.DataFrame(runs)
    if not runs_df.empty:
        display_metrics(runs_df)
        st.dataframe(format_dataframe(runs_df), use_container_width=True)
    else:
        st.warning("No pipeline runs found in the selected time frame.")

elif page == "Pipelines":
    st.header("⚙️ Pipelines")
    all_pipelines = list_pipelines.invoke({"resource_group_name": st.session_state.selected_rg, "data_factory_name": st.session_state.selected_adf})
    pipeline_name = st.selectbox("Select a pipeline to view its runs", options=all_pipelines)
    st.markdown("##### Select Date Range")
    col1, col2 = st.columns([3, 1])
    with col1:
        st.slider("Select date range (days)", 1, 60, 7, key="pipe_slider", on_change=sync_days, args=("pipe_slider", "pipe_input"))
    with col2:
        st.number_input("Days", 1, 60, 7, key="pipe_input", on_change=sync_days, args=("pipe_input", "pipe_slider"), label_visibility="collapsed")
    days = st.session_state.get("pipe_slider", 7)
    if pipeline_name:
        with st.spinner(f"Fetching runs for pipeline: {pipeline_name}..."):
            runs = get_pipeline_runs.invoke({"resource_group_name": st.session_state.selected_rg, "data_factory_name": st.session_state.selected_adf, "days": days, "pipeline_name": pipeline_name})
            runs_df = pd.DataFrame(runs)
        if not runs_df.empty:
            st.dataframe(format_dataframe(runs_df), use_container_width=True)
        else:
            st.info(f"No runs found for pipeline '{pipeline_name}' in the last {days} days.")

# -------- Error Diagnosis --------
elif page == "Error Diagnosis":
    st.header("🔍 Error Diagnosis")
    st.write("Select a pipeline run ID to view failed activities.")

    # Fetch all the runs for the past 30 days
    all_runs = get_pipeline_runs.invoke({
        "resource_group_name": st.session_state.selected_rg,
        "data_factory_name": st.session_state.selected_adf,
        "days": 30
    })

    # Extract the run IDs from the runs
    run_options = [run['runId'] for run in all_runs] if all_runs else []

    # Select the run ID from the list
    selected_run_id = st.selectbox("Select Run ID", options=run_options)

    if selected_run_id:
        # Fetch the logs for the selected run ID
        with st.spinner("Fetching activity logs..."):
            logs = get_run_activity_logs.invoke({
                "resource_group_name": st.session_state.selected_rg,
                "data_factory_name": st.session_state.selected_adf,
                "run_id": selected_run_id
            })
        
        # Filter failed activities
        failed_activities = [log for log in logs if log.get("status") == "Failed"]

        # If no failed activities, show success message
        if not failed_activities:
            st.success("No failed activities found for this run.")
        else:
            # Loop through each failed activity
            for activity in failed_activities:
                # Expand each failed activity for better readability
                with st.expander(f"❌ {activity['activityName']}"):
                    # Show the activity log details in JSON format
                    st.json(activity, expanded=False)

                    # AI Suggestion button
                    if st.button("Get AI Suggestion", key=f"ai_{selected_run_id}_{activity['activityName']}"):
                        with st.spinner("Analyzing error with AI..."):
                            # Analyzing the error using AI
                            suggestion = get_error_analysis(str(activity.get('error', {})))
                            # Display AI suggestion
                            st.info(f"💡 **AI Suggestion:**\n{suggestion}")



elif page == "Chat Assistance":
    st.header("💬 Chat Assistance")
    for msg in st.session_state.langchain_messages:
        with st.chat_message(msg.type):
            st.markdown(msg.content)
    if prompt := st.chat_input("Ask about your Data Factory..."):
        st.chat_message("human").markdown(prompt)
        with st.chat_message("ai"), st.spinner("Thinking..."):
            response = st.session_state.chat_agent.get_agent_response(prompt, st.session_state.selected_rg, st.session_state.selected_adf)
            st.markdown(response)