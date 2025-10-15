import streamlit as st
from agent import ChatAgent, check_openai_connection
from azure_tools import list_all_data_factories_in_subscription
st.set_page_config(layout="wide", page_icon="🤖", page_title="ADF AI Assistant")
# --- STATE MANAGEMENT ---
def initialize_state():
   """Initializes session state variables."""
   if "initialized" in st.session_state:
       return
   st.session_state.initialized = True
   st.session_state.error = None
   st.session_state.all_adfs = []
   st.session_state.selected_rg = None
   st.session_state.selected_adf = None
   st.session_state.chat_agent = None
   if "langchain_messages" not in st.session_state:
       st.session_state.langchain_messages = []
   st.session_state.azure_status = "Disconnected"
   st.session_state.openai_status = "Disconnected"
   # Check Azure connection
   with st.spinner("🔄 Connecting to Azure..."):
       try:
           adfs = list_all_data_factories_in_subscription.invoke({})
           if adfs and isinstance(adfs, list) and len(adfs) > 0:
               if isinstance(adfs[0], dict) and "error" in adfs[0]:
                   st.session_state.error = adfs[0]["error"]
                   st.session_state.azure_status = "Failed"
               else:
                   st.session_state.all_adfs = adfs
                   st.session_state.azure_status = "Connected"
           else:
               st.session_state.all_adfs = []
               st.session_state.azure_status = "Connected"
       except Exception as e:
           st.session_state.error = str(e)
           st.session_state.azure_status = "Failed"
   # Check OpenAI connection
   with st.spinner("🔄 Checking Azure OpenAI connection..."):
       if check_openai_connection():
           st.session_state.openai_status = "Connected"
       else:
           st.session_state.openai_status = "Failed"
initialize_state()
# --- SIDEBAR ---
with st.sidebar:
   st.title("🤖 ADF AI Assistant")
   st.markdown("Your intelligent Azure Data Factory companion")
   st.divider()
   st.subheader("🗂️ Select Data Factory")
   if st.session_state.error:
       st.error(f"❌ Azure Connection Failed:\n{st.session_state.error}")
       st.info("Please check your credentials in the .env file")
   if not st.session_state.all_adfs:
       st.warning("⚠️ No Azure Data Factories found in your subscription.")
   else:
       # Data Factory selection
       adf_display_names = [
           f"{adf['factory_name']} ({adf['resource_group']})"
           for adf in st.session_state.all_adfs
       ]
       selected_display_name = st.selectbox(
           "Choose a Data Factory",
           options=adf_display_names,
           help="Select the Azure Data Factory you want to work with"
       )
       if selected_display_name:
           selected_index = adf_display_names.index(selected_display_name)
           selected_adf_obj = st.session_state.all_adfs[selected_index]
           st.session_state.selected_adf = selected_adf_obj['factory_name']
           st.session_state.selected_rg = selected_adf_obj['resource_group']
           st.success(f"✅ Connected to: **{st.session_state.selected_adf}**")
           st.info(f"📦 Resource Group: `{st.session_state.selected_rg}`")
   st.divider()
   # Connection Status
   st.subheader("🔌 Connection Status")
   # Azure Status
   if st.session_state.azure_status == "Connected":
       st.success("☁️ Azure: Connected")
   else:
       st.error("☁️ Azure: Disconnected")
   # OpenAI Status
   if st.session_state.openai_status == "Connected":
       st.success("🤖 Azure OpenAI: Connected")
   else:
       st.error("🤖 Azure OpenAI: Disconnected")
   st.divider()
   # Refresh button
   if st.button("🔄 Refresh Connection", use_container_width=True):
       keys_to_clear = ['initialized', 'all_adfs', 'error', 'azure_status', 'openai_status', 'chat_agent']
       for key in keys_to_clear:
           if key in st.session_state:
               del st.session_state[key]
       st.rerun()
   # Clear chat button
   if st.button("🗑️ Clear Chat History", use_container_width=True):
       st.session_state.langchain_messages = []
       st.session_state.chat_agent = None
       st.rerun()
   st.divider()
   # Help section
   with st.expander("ℹ️ How to use this assistant"):
       st.markdown("""
       **Ask me anything about your Azure Data Factory!**
       Examples:
       - List all pipelines
       - Show me failed pipeline runs from last 7 days
       - Diagnose errors in pipeline [name]
       - Get activity logs for run ID [id]
       - Fix pipeline [name]
       - What's the status of my pipelines?
       **Note:** I can only answer questions related to Azure Data Factory.
       For other queries, please use appropriate tools.
       """)
# --- MAIN CONTENT ---
st.title("💬 Azure Data Factory AI Assistant")
st.markdown("Ask me anything about your Azure Data Factory pipelines, runs, errors, and more!")
# Check if Data Factory is selected
if not st.session_state.selected_rg or not st.session_state.selected_adf:
   st.info("👈 Please select a Data Factory from the sidebar to start chatting.")
   st.stop()
# Initialize chat agent if not exists
if st.session_state.chat_agent is None:
   with st.spinner("🔄 Initializing AI assistant..."):
       st.session_state.chat_agent = ChatAgent()
# Display chat history
for msg in st.session_state.langchain_messages:
   with st.chat_message(msg.type):
       st.markdown(msg.content)
# Chat input
if prompt := st.chat_input("Ask about your Data Factory..."):
   # Display user message
   st.chat_message("human").markdown(prompt)
   # Generate AI response with verbose feedback
   with st.chat_message("ai"):
       # Create a status container that updates in real-time
       status = st.status("🔍 Processing your query...", expanded=True)
       try:
           # Get response from agent with streaming status updates
           response = st.session_state.chat_agent.get_agent_response(
               prompt,
               st.session_state.selected_rg,
               st.session_state.selected_adf,
               status  # Pass status container for real-time updates
           )
           # Mark status as complete and collapse it
           status.update(label="", state="complete", expanded=False)
           # Show response
           st.markdown(response)
       except Exception as e:
           status.update(label="❌ Error occurred", state="error", expanded=False)
           st.error(f"An error occurred: {str(e)}\n\nPlease try again or rephrase your question.")
# Footer with tips
st.divider()
st.markdown("""
<div style='text-align: center; color: gray; font-size: 0.9em;'>
   💡 <strong>Tip:</strong> I can diagnose errors, suggest fixes, and even automatically repair your pipelines!
   Just ask me about any failed runs or pipeline issues.
</div>
""", unsafe_allow_html=True)