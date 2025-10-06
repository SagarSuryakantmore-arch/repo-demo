# app.py

import streamlit as st
import pandas as pd
import json
import time
from agent import ChatAgent, get_error_analysis, check_openai_connection, get_pipeline_fix_json
from azure_tools import (
    list_pipelines,
    get_pipeline_runs,
    get_run_activity_logs,
    list_all_data_factories_in_subscription,
    get_pipeline_definition,
    update_pipeline,
    create_pipeline_run,
    get_pipeline_run,
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
    st.session_state.fix_in_progress = None # To track which fix to run

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
        keys_to_clear = ['initialized', 'all_adfs', 'error', 'azure_status', 'openai_status', 'fix_in_progress']
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

elif page == "Error Diagnosis":
    st.header("🔍 Error Diagnosis")
    st.write("Select a failed pipeline run ID to diagnose and attempt an automated fix.")
    MAX_FIX_ATTEMPTS = 3

    # --- AUTO-FIX LOGIC (placed here to run outside the expander) ---
    if st.session_state.fix_in_progress:
        fix_data = st.session_state.fix_in_progress
        activity = fix_data['activity']
        pipeline_name = fix_data['pipeline_name']
        current_error = activity.get('error', {})
        
        for attempt in range(MAX_FIX_ATTEMPTS):
            with st.status(f"**Auto-Fix for '{pipeline_name}': Attempt {attempt + 1}**", expanded=True) as status:
                try:
                    # 1. Get current pipeline definition
                    status.update(label="Fetching current pipeline definition...")
                    pipeline_def = get_pipeline_definition.invoke({"resource_group_name": st.session_state.selected_rg, "data_factory_name": st.session_state.selected_adf, "pipeline_name": pipeline_name})
                    if "error" in pipeline_def: raise ValueError(f"Failed to get pipeline definition: {pipeline_def['error']}")

                    # 2. Get AI-generated fix in JSON format
                    status.update(label="Generating AI fix...")
                    ai_fix_str = get_pipeline_fix_json(str(pipeline_def), str(current_error), activity['activityName'])
                    fix_json = json.loads(ai_fix_str)

                    # 3. Check for manual intervention

                    if "manual_intervention_required" in fix_json:
                        # 1. Mark the status as 'complete' because the automated process is done.
                        status.update(label="Auto-fix stopped. Manual action required.", state="complete")
                        
                        # 2. Display the actual warning message to the user outside the status context.
                        st.warning(f"🤖 **AI Agent:** {fix_json['manual_intervention_required']}")
                        
                        # 3. Stop the loop.
                        break

                    # 4. Apply the fix
                    status.update(label="Applying fix and updating pipeline...")
                    update_result = update_pipeline.invoke({"resource_group_name": st.session_state.selected_rg, "data_factory_name": st.session_state.selected_adf, "pipeline_name": pipeline_name, "pipeline_definition": fix_json})
                    if "error" in update_result: raise ValueError(f"Failed to update pipeline: {update_result['error']}")

                    # 5. Retrigger
                    status.update(label="Retriggering the pipeline...")
                    new_run = create_pipeline_run.invoke({"resource_group_name": st.session_state.selected_rg, "data_factory_name": st.session_state.selected_adf, "pipeline_name": pipeline_name})
                    if "error" in new_run: raise ValueError(f"Failed to retrigger pipeline: {new_run['error']}")
                    new_run_id = new_run['runId']

                    # 6. Monitor
                    while True:
                        status.update(label=f"Monitoring new run... (`{new_run_id}`)")
                        time.sleep(10)
                        run_status = get_pipeline_run.invoke({"resource_group_name": st.session_state.selected_rg, "data_factory_name": st.session_state.selected_adf, "run_id": new_run_id})
                        if run_status['status'] == 'Succeeded':
                            status.update(label=f"Pipeline fixed and ran successfully! 🎉", state="complete")
                            st.balloons()
                            solution_used = get_error_analysis(str(current_error))
                            st.success(f"**Successfully fixed with the following logic:**\n\n{solution_used}")
                            break
                        elif run_status['status'] in ['Failed', 'Cancelled']:
                            current_error = run_status.get('message', 'New run failed without a specific message.')
                            raise RuntimeError(f"New run failed. Error: {current_error}")
                
                except (ValueError, RuntimeError, json.JSONDecodeError) as e:
                    status.update(label=f"Attempt {attempt + 1} failed: {e}", state="error")
                    if attempt < MAX_FIX_ATTEMPTS - 1:
                        time.sleep(2) # Brief pause before next attempt
                        continue
                    else:
                        st.error("Auto-fix failed after all attempts.")
                break
        
        st.session_state.fix_in_progress = None # Clear the state after processing

    # --- UI and Data Fetching ---
    with st.spinner("Fetching recent pipeline runs..."):
        all_runs = get_pipeline_runs.invoke({"resource_group_name": st.session_state.selected_rg, "data_factory_name": st.session_state.selected_adf, "days": 30})
    
    if not all_runs or isinstance(all_runs, str) or ("error" in all_runs[0]):
        st.error(f"Could not fetch pipeline runs: {all_runs}")
        st.stop()
    
    failed_runs = [run for run in all_runs if run['status'] == 'Failed']
    # CORRECTED: Display Run ID in the selectbox for clarity
    run_options = {run['runId']: f"{run['pipelineName']} ({run['runId']}) | {pd.to_datetime(run['runStart']).strftime('%Y-%m-%d %H:%M')}" for run in failed_runs}

    selected_run_id = st.selectbox("Select Failed Run ID", options=list(run_options.keys()), format_func=lambda x: run_options.get(x, "Unknown Run"))

    if selected_run_id:
        run_details = next((run for run in all_runs if run['runId'] == selected_run_id), None)
        pipeline_name = run_details['pipelineName'] if run_details else None

        with st.spinner("Fetching activity logs..."):
            logs = get_run_activity_logs.invoke({"resource_group_name": st.session_state.selected_rg, "data_factory_name": st.session_state.selected_adf, "run_id": selected_run_id})
        
        failed_activities = [log for log in logs if log.get("status") == "Failed"]

        if not failed_activities:
            st.success("No failed activities found for this run. The pipeline itself may have failed.")
            st.json(run_details.get('message', 'No specific pipeline-level error message.'))
        else:
            for activity in failed_activities:
                with st.expander(f"❌ Failed Activity: **{activity['activityName']}**"):
                    st.write("**Error Details:**")
                    st.json(activity.get('error', {}))
                    col1, col2 = st.columns(2)
                    with col1:
                        if st.button("Get AI Suggestion", key=f"ai_{activity['activityName']}"):
                            with st.spinner("Analyzing error with AI..."):
                                st.info(f"💡 **AI Suggestion:**\n{get_error_analysis(str(activity.get('error', {})))}")
                    with col2:
                        if st.button("🚀 Fix Pipeline & Retrigger", key=f"fix_{activity['activityName']}"):
                            # Set state to trigger the fix logic on rerun
                            st.session_state.fix_in_progress = {'activity': activity, 'pipeline_name': pipeline_name}
                            st.rerun()

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