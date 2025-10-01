import os
from langchain.agents import AgentExecutor, create_openai_tools_agent
from langchain_groq import ChatGroq
from langchain.prompts import ChatPromptTemplate, MessagesPlaceholder
from azure_tools import list_pipelines, get_pipeline_runs, get_run_activity_logs

# Initialize the language model using Groq
# It will automatically find the GROQ_API_KEY in your .env file
llm = ChatGroq(model_name="llama-3.3-70b-versatile")


# --- NEW: Get resource names to inject into the prompt ---
data_factory_name = os.getenv("AZURE_DATA_FACTORY_NAME")
resource_group_name = os.getenv("AZURE_RESOURCE_GROUP")

# Define the tools the agent can use
tools = [list_pipelines, get_pipeline_runs, get_run_activity_logs]


# --- MODIFIED: Update the system prompt to include resource names ---
system_prompt_template = f"""You are a helpful assistant for Azure Data Factory.
When you need to call a tool that requires a resource group name or a data factory name, you MUST use the following values:
- Data Factory Name: '{data_factory_name}'
- Resource Group Name: '{resource_group_name}'
Do not ask the user for these values, just use them."""

prompt = ChatPromptTemplate.from_messages(
    [
        ("system", system_prompt_template),
        ("human", "{input}"),
        MessagesPlaceholder(variable_name="agent_scratchpad"),
    ]
)

# Create the agent
agent = create_openai_tools_agent(llm, tools, prompt)

# Create the agent executor
agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True)

def get_agent_response(user_query):
    """Gets a response from the agent for a given user query."""
    # --- MODIFIED: The invoke call is now simpler ---
    response = agent_executor.invoke(
        {
            "input": user_query,
        }
    )
    return response["output"]