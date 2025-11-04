"""
LangGraph Agent Team Implementation
Demonstrates coordinating multiple specialized agents using LangGraph.
"""

import os
from typing import Annotated, Literal, Optional
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
from langchain_core.tools import tool
from langgraph.graph import StateGraph, MessagesState, START, END
from langgraph.prebuilt import ToolNode
from langgraph.checkpoint.memory import MemorySaver
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


# Define tools
@tool
def get_weather(city: str) -> dict:
    """Retrieves the current weather report for a specified city.

    Args:
        city: The name of the city for which to retrieve the weather report.

    Returns:
        dict: Weather report with status and information.
    """
    city_normalized = city.lower().replace(" ", "")

    mock_weather_db = {
        "newyork": {
            "status": "success",
            "report": "The weather in New York is sunny with a temperature of 25°C (77°F)."
        },
        "london": {
            "status": "success",
            "report": "It's cloudy in London with a temperature of 15°C (59°F)."
        },
        "tokyo": {
            "status": "success",
            "report": "Tokyo is experiencing light rain and a temperature of 18°C (64°F)."
        },
    }

    if city_normalized in mock_weather_db:
        return mock_weather_db[city_normalized]
    else:
        return {
            "status": "error",
            "error_message": f"Sorry, I don't have weather information for '{city}'."
        }


@tool
def say_hello(name: Optional[str] = None) -> str:
    """Greets the user with a friendly hello message.

    Args:
        name: The name of the person to greet.

    Returns:
        str: A greeting message.
    """
    if name:
        return f"Hello, {name}! How can I assist you today?"
    else:
        return "Hello there! How can I assist you today?"


@tool
def say_goodbye() -> str:
    """Says goodbye to the user.

    Returns:
        str: A farewell message.
    """
    return "Goodbye! Have a great day!"


# Initialize specialized LLMs for different agents
weather_llm = ChatGoogleGenerativeAI(
    model="gemini-2.5-flash",
    temperature=0,
    google_api_key=os.getenv("GOOGLE_API_KEY")
).bind_tools([get_weather])

greeting_llm = ChatGoogleGenerativeAI(
    model="gemini-2.5-flash",
    temperature=0.7,
    google_api_key=os.getenv("GOOGLE_API_KEY")
).bind_tools([say_hello, say_goodbye])

coordinator_llm = ChatGoogleGenerativeAI(
    model="gemini-2.5-flash",
    temperature=0,
    google_api_key=os.getenv("GOOGLE_API_KEY")
)


# Define the agent state
class AgentTeamState(MessagesState):
    """State for the agent team with routing information."""
    next_agent: str = "coordinator"


# Define agent nodes
def coordinator_node(state: AgentTeamState):
    """Coordinator agent that routes to specialized agents."""
    messages = state["messages"]

    # Add system message for routing
    system_msg = SystemMessage(content="""You are a coordinator assistant. Analyze the user's request and decide:
    - If it's about weather, respond with 'ROUTE: weather'
    - If it's a greeting or farewell, respond with 'ROUTE: greeting'
    - Otherwise, respond with 'ROUTE: direct' and answer the question directly.

    Start your response with the routing decision.""")

    response = coordinator_llm.invoke([system_msg] + messages)

    # Determine routing
    content = response.content.lower()
    if "route: weather" in content:
        return {"next_agent": "weather", "messages": [response]}
    elif "route: greeting" in content:
        return {"next_agent": "greeting", "messages": [response]}
    else:
        return {"next_agent": "end", "messages": [response]}


def weather_agent_node(state: AgentTeamState):
    """Weather specialist agent."""
    messages = state["messages"]
    system_msg = SystemMessage(
        content="You are a weather specialist. Provide accurate weather information using the get_weather tool."
    )
    response = weather_llm.invoke([system_msg] + messages)
    return {"messages": [response], "next_agent": "tools"}


def greeting_agent_node(state: AgentTeamState):
    """Greeting specialist agent."""
    messages = state["messages"]
    system_msg = SystemMessage(
        content="You are a friendly greeter. Welcome users warmly and say goodbye courteously using the say_hello and say_goodbye tools."
    )
    response = greeting_llm.invoke([system_msg] + messages)
    return {"messages": [response], "next_agent": "tools"}


# Define routing function
def route_to_agent(state: AgentTeamState) -> Literal["weather", "greeting", "end"]:
    """Route to the appropriate agent based on coordinator decision."""
    return state.get("next_agent", "end")


def should_use_tools(state: AgentTeamState) -> Literal["tools", "end"]:
    """Determine if tools should be called."""
    last_message = state["messages"][-1]
    if hasattr(last_message, "tool_calls") and last_message.tool_calls:
        return "tools"
    return "end"


# Create the graph
def create_agent_team():
    """Create and compile the agent team graph."""

    # Initialize the graph
    workflow = StateGraph(AgentTeamState)

    # Add nodes
    workflow.add_node("coordinator", coordinator_node)
    workflow.add_node("weather", weather_agent_node)
    workflow.add_node("greeting", greeting_agent_node)
    workflow.add_node("tools", ToolNode([get_weather, say_hello, say_goodbye]))

    # Set entry point
    workflow.add_edge(START, "coordinator")

    # Add conditional edges from coordinator
    workflow.add_conditional_edges(
        "coordinator",
        route_to_agent,
        {
            "weather": "weather",
            "greeting": "greeting",
            "end": END
        }
    )

    # Add conditional edges from specialized agents
    workflow.add_conditional_edges(
        "weather",
        should_use_tools,
        {
            "tools": "tools",
            "end": END
        }
    )

    workflow.add_conditional_edges(
        "greeting",
        should_use_tools,
        {
            "tools": "tools",
            "end": END
        }
    )

    # Tools route back to end
    workflow.add_edge("tools", END)

    # Compile with memory
    memory = MemorySaver()
    return workflow.compile(checkpointer=memory)


# Create the agent team
agent_team = create_agent_team()


def run_agent_team(user_input: str, thread_id: str = "1"):
    """Run the agent team with a user input.

    Args:
        user_input: The user's question or request
        thread_id: The conversation thread ID for memory

    Returns:
        The agent's response
    """
    config = {"configurable": {"thread_id": thread_id}}

    # Invoke the agent team
    result = agent_team.invoke(
        {"messages": [HumanMessage(content=user_input)]},
        config=config
    )

    # Get the last message
    last_message = result["messages"][-1]
    return last_message.content


# Main execution
if __name__ == "__main__":
    print("LangGraph Agent Team")
    print("=" * 50)
    print("I can help with weather, greetings, and general questions!")
    print("Type 'quit' to exit\n")

    thread_id = "main_conversation"

    while True:
        user_input = input("You: ").strip()

        if user_input.lower() in ["quit", "exit", "q"]:
            print("Goodbye!")
            break

        if not user_input:
            continue

        try:
            response = run_agent_team(user_input, thread_id)
            print(f"Agent: {response}\n")
        except Exception as e:
            print(f"Error: {e}\n")
