# agent.py

import os
import json
from langchain_openai import AzureChatOpenAI
from langchain.agents import AgentExecutor, create_openai_tools_agent
from langchain.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain.memory import ConversationSummaryBufferMemory
from langchain_community.chat_message_histories import StreamlitChatMessageHistory
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
    model_name=os.getenv("AZURE_OPENAI_MODEL_NAME"),
    api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
    streaming=True,
    # temperature=0, # Set to 0 for deterministic JSON output
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

system_prompt_template = """You are a helpful and expert assistant for Azure Data Factory.
You have access to a suite of tools to fetch information about pipelines, their runs, and their logs.
When a user asks a question, use the available tools to find the most relevant and up-to-date information.
If you need a resource group name, a data factory name, or a pipeline name that the user has not provided,
you MUST ask the user for the missing information.
Always be polite and provide clear, concise answers based on the tool outputs.
"""

prompt = ChatPromptTemplate.from_messages(
    [
        ("system", system_prompt_template),
        MessagesPlaceholder(variable_name="chat_history"),
        ("human", "{input}"),
        MessagesPlaceholder(variable_name="agent_scratchpad"),
    ]
)

class ChatAgent:
    def __init__(self, session_key="langchain_messages"):
        self.memory = ConversationSummaryBufferMemory(
            llm=llm,
            max_token_limit=1000,
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
        )

    def get_agent_response(self, user_query, resource_group, data_factory):
        contextual_query = f"""
        Current context:
        - Resource Group: '{resource_group}'
        - Data Factory: '{data_factory}'

        User's question: {user_query}
        """
        response = self.agent_executor.invoke({"input": contextual_query})
        return response.get("output", "Sorry, I encountered an error.")

def get_error_analysis(error_message: str) -> str:
    """
    Asks the LLM to provide a human-readable analysis of an error message.
    """
    analysis_prompt = f"""
    As an expert Azure Data Factory developer, please analyze the following error message from a pipeline run.
    Provide a clear, step-by-step explanation of the likely cause and suggest a solution.

    Error Message:
    {error_message}
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
        llm.invoke("test", config={"max_tokens": 5})
        return True
    except Exception as e:
        print(f"OpenAI connection check failed: {e}")
        return False