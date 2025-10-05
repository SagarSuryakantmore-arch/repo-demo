# agent.py

import os
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
)

llm = AzureChatOpenAI(
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    azure_deployment=os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME"),
    model_name=os.getenv("AZURE_OPENAI_MODEL_NAME"),
    api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
    streaming=True,
)

tools = [
    list_pipelines,
    get_pipeline_runs,
    get_run_activity_logs,
    list_all_data_factories_in_subscription,
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
    analysis_prompt = f"""
    As an expert Azure Data Factory developer, please analyze the following error message from a pipeline run.
    Provide a clear, step-by-step explanation of the likely cause and suggest a solution.

    Error Message:
    {error_message}
    """
    response = llm.invoke(analysis_prompt)
    return response.content

# --- THIS FUNCTION WAS MISSING ---
def check_openai_connection():
    """Performs a quick, low-cost check to verify the OpenAI connection and credentials."""
    try:
        # A minimal, non-streaming call to check the connection
        llm.invoke("test", config={"max_tokens": 5})
        return True
    except Exception as e:
        print(f"OpenAI connection check failed: {e}")
        return False