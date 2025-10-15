# agent.py

import os

import json

import streamlit as st

from langchain_openai import AzureChatOpenAI

from langchain.agents import AgentExecutor, create_openai_tools_agent

from langchain.prompts import ChatPromptTemplate, MessagesPlaceholder

from langchain.memory import ConversationBufferMemory

from langchain_community.chat_message_histories import StreamlitChatMessageHistory

from langchain.callbacks.base import BaseCallbackHandler

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

llm = AzureChatOpenAI(

    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),

    api_key=os.getenv("AZURE_OPENAI_API_KEY"),

    azure_deployment=os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME"),

    api_version=os.getenv("AZURE_OPENAI_API_VERSION"),

)

tools = [

    list_pipelines,

    get_pipeline_runs,

    get_run_activity_logs,

    list_all_data_factories_in_subscription,

    get_pipeline_definition,

    update_pipeline,

    create_pipeline_run,

    get_pipeline_run,

]

system_prompt_template = """You are an expert AI assistant specialized EXCLUSIVELY in Azure Data Factory (ADF).

**IMPORTANT SCOPE RESTRICTIONS:**

- You ONLY answer questions related to Azure Data Factory, pipelines, data integration, ETL/ELT processes, and related Azure services.

- If a user asks questions outside of Azure Data Factory scope (e.g., "What's the latest ChatGPT version?", "Tell me about Python programming", "What's the weather?"), you MUST respond with:

  "I apologize, but I'm specifically designed to assist with Azure Data Factory queries only. I can help you with:

  - Pipeline management and monitoring

  - Error diagnosis and troubleshooting

  - Pipeline run analysis

  - Activity logs and debugging

  - Pipeline fixes and optimizations

  Please ask me anything related to Azure Data Factory!"

**YOUR CAPABILITIES:**

You have access to powerful tools to:

1. List and inspect pipelines

2. Retrieve pipeline run history and status

3. Analyze activity logs and error messages

4. Diagnose pipeline failures

5. Suggest and implement pipeline fixes

6. Monitor pipeline executions

7. Get detailed pipeline definitions

8. Update pipeline definitions

9. Create pipeline runs

**AUTOMATIC PIPELINE FIXING WORKFLOW:**

When a user asks you to "fix a pipeline", be EFFICIENT and follow these steps:

1. **Get Recent Runs**: Use `get_pipeline_runs` to find the most recent failed run

2. **Get Activity Logs**: Use `get_run_activity_logs` to see the error

3. **Analyze**: Based on the error, determine if you can fix it programmatically

4. **If fixable**: Get pipeline definition, explain the issue clearly, then suggest the fix (but DON'T automatically update unless user confirms)

5. **If not fixable**: Explain what manual steps are needed

IMPORTANT:

- Do NOT automatically update pipelines without explaining the issue first

- Be concise in your explanations

- If the error is simple (like missing file), just explain it - don't go through the whole workflow

- Only use tools that are absolutely necessary

**IMPORTANT FOR update_pipeline TOOL:**

When calling update_pipeline, you MUST provide the complete pipeline_definition dictionary with these keys:

- activities (required): The list of pipeline activities

- parameters (optional): Pipeline parameters

- variables (optional): Pipeline variables

- annotations (optional): Pipeline annotations

Example call:

update_pipeline(

    resource_group_name="rg-name",

    data_factory_name="adf-name",

    pipeline_name="pipeline-name",

    pipeline_definition={{

        "activities": [...],  # Full activities list from get_pipeline_definition

        "parameters": {{...}},

        "variables": {{...}},

        "annotations": [...]

    }}

)

**RESPONSE STYLE:**

- Be clear and concise in your responses

- Provide actionable information

- Use structured formatting for better readability

- Always confirm the context (Resource Group, Data Factory) when performing actions

- When fixing pipelines, explain each step you're taking

**REQUIRED INFORMATION:**

- If you need a resource group name, data factory name, or pipeline name that the user hasn't provided, ASK for it.

- Always confirm you understand the user's request before executing tools.

Always be professional, thorough, and helpful within your Azure Data Factory expertise.

"""

prompt = ChatPromptTemplate.from_messages(

    [

        ("system", system_prompt_template),

        MessagesPlaceholder(variable_name="chat_history"),

        ("human", "{input}"),

        MessagesPlaceholder(variable_name="agent_scratchpad"),

    ]

)


class StreamlitCallbackHandler(BaseCallbackHandler):

    """Callback handler for streaming updates to Streamlit."""

    def __init__(self, status_container):

        self.status_container = status_container

        self.current_step = ""

    def on_tool_start(self, serialized, input_str, **kwargs):

        """Called when a tool starts executing."""

        tool_name = serialized.get("name", "Unknown Tool")

        # Map tool names to user-friendly messages

        tool_messages = {

            "list_pipelines": "📋 Fetching list of pipelines...",

            "get_pipeline_runs": "🔄 Retrieving pipeline run history...",

            "get_run_activity_logs": "📊 Analyzing activity logs...",

            "list_all_data_factories_in_subscription": "🏭 Loading data factories...",

            "get_pipeline_definition": "📝 Reading pipeline definition...",

            "update_pipeline": "🔧 Applying pipeline fix...",

            "create_pipeline_run": "▶️ Starting pipeline execution...",

            "get_pipeline_run": "⏱️ Checking pipeline status...",

        }

        message = tool_messages.get(tool_name, f"🔧 Using tool: {tool_name}")

        self.current_step = message

        self.status_container.update(label=message)

    def on_tool_end(self, output, **kwargs):

        """Called when a tool finishes executing."""

        pass

    def on_tool_error(self, error, **kwargs):

        """Called when a tool encounters an error."""

        self.status_container.update(label=f"❌ Error: {str(error)[:50]}...")

    def on_llm_start(self, serialized, prompts, **kwargs):

        """Called when LLM starts."""

        self.status_container.update(label="🤖 AI analyzing your request...")

    def on_agent_action(self, action, **kwargs):

        """Called when agent decides on an action."""

        self.status_container.update(label="💭 Processing...")


class ChatAgent:

    def __init__(self, session_key="langchain_messages"):

        try:

            # Use simpler memory without token counting issues

            self.memory = ConversationBufferMemory(

                memory_key="chat_history",

                return_messages=True,

                chat_memory=StreamlitChatMessageHistory(key=session_key),

            )

            agent = create_openai_tools_agent(llm, tools, prompt)

            self.agent_executor = AgentExecutor(

                agent=agent,

                tools=tools,

                memory=self.memory,

                verbose=True,

                handle_parsing_errors=True,

                max_iterations=5,  #

                return_intermediate_steps=False,

                early_stopping_method="generate",  # Stop early if possible

            )

        except Exception as e:

            print(f"Error initializing ChatAgent: {e}")

            raise

    def get_agent_response(self, user_query, resource_group, data_factory, status_container):

        """

        Process user query and return AI response.

        Includes context about current resource group and data factory.

        """

        # Build contextual query

        contextual_query = f"""

        Current context:

        - Resource Group: '{resource_group}'

        - Data Factory: '{data_factory}'

        User's question: {user_query}

        """

        try:

            # Create callback handler

            callback_handler = StreamlitCallbackHandler(status_container)

            # Execute agent with callbacks

            status_container.update(label="🤖 AI processing your request...")

            response = self.agent_executor.invoke(

                {"input": contextual_query},

                config={"callbacks": [callback_handler]}

            )

            # Debug: Check response structure

            if not isinstance(response, dict):

                return f"Unexpected response type: {type(response)}. Please try again."

            # Extract output with fallback

            agent_output = response.get("output")

            if agent_output is None:

                # Try alternative keys

                agent_output = response.get("result") or response.get("text")

            if agent_output is None or not isinstance(agent_output, str):

                return f"I received an incomplete response. Response keys: {list(response.keys())}. Please try rephrasing your question."

            if not agent_output.strip():

                return "I couldn't generate a meaningful response. Please try asking your question in a different way."

            return agent_output

        except Exception as e:

            import traceback

            error_msg = str(e)

            traceback_str = traceback.format_exc()

            print(f"Agent error: {error_msg}")

            print(f"Traceback: {traceback_str}")

            return f"I encountered an error while processing your request: {error_msg}\n\nPlease try rephrasing your question or ask something else about Azure Data Factory."


def get_error_analysis(error_message: str) -> str:

    """

    Asks the LLM to provide a human-readable analysis of an error message.

    """

    analysis_prompt = f"""

    As an expert Azure Data Factory developer, please analyze the following error message from a pipeline run.

    Provide a clear, step-by-step explanation of:

    1. What the error means in plain language

    2. The likely root cause

    3. Specific steps to fix the issue

    4. Any preventive measures for the future

    Error Message:

    {error_message}

    Format your response in a clear, structured way with bullet points and sections.

    """

    response = llm.invoke(analysis_prompt)

    return response.content


def get_pipeline_fix_json(pipeline_definition: str, error_message: str, activity_name: str) -> str:

    """

    Asks the LLM to analyze a pipeline definition and an error, and return a modified JSON to fix it.

    """

    analysis_prompt = f"""

    You are an expert Azure Data Factory automated debugging agent. Your task is to fix a broken pipeline.

    You will be given the full JSON definition of an ADF pipeline, the name of the failed activity, and the error message from that activity.

    Your goal is to modify the pipeline JSON to correct the error.

    **Instructions:**

    1.  Analyze the provided `pipeline_definition` JSON and the `error_message`.

    2.  Identify the root cause of the error within the activity named `{activity_name}`.

    3.  Modify the JSON to implement a plausible fix. Common fixes might involve correcting typos in properties, changing linked service names, fixing dynamic content expressions, or adjusting activity settings.

    4.  **Output Format**: You MUST respond in one of two formats:

        a. **If a programmatic fix is possible**: Return ONLY the complete, modified, and valid JSON for the entire pipeline. Do not include any explanations, apologies, or introductory text. The output must be parsable as JSON.

        b. **If a fix requires manual intervention**: If the error is due to expired credentials, incorrect permissions, network connectivity issues, or problems in external systems that cannot be fixed by modifying the pipeline JSON, return a JSON object with a single key "manual_intervention_required". The value should be a string explaining the problem and the steps the user must take manually. For example: {json.dumps({"manual_intervention_required": "The error indicates a credential issue with the source linked service 'AzureBlobStorage1'. Please navigate to the Azure portal, open this linked service, test the connection, and update the credentials."})}

    **IMPORTANT**: Do not suggest placeholder changes. The modifications should be specific and directly address the error. Do not change the pipeline name or activity names.

    ---

    **Pipeline Definition (JSON):**

    {pipeline_definition}

    ---

    **Failed Activity Name:**

    {activity_name}

    ---

    **Error Message:**

    {error_message}

    ---

    Now, provide your response based on the instructions.

    """

    response = llm.invoke(analysis_prompt)

    return response.content


def check_openai_connection():

    """Performs a quick, low-cost check to verify the OpenAI connection and credentials."""

    try:

        llm.invoke("test")

        return True

    except Exception as e:

        print(f"OpenAI connection check failed: {e}")

        return False
 