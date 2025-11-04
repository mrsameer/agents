from google.adk.agents import Agent
from typing import Optional

# Tool definitions
def get_weather(city: str) -> dict:
    """Retrieves the current weather report for a specified city.

    Args:
        city (str): The name of the city for which to retrieve the weather report.

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


def say_hello(name: Optional[str] = None) -> str:
    """Greets the user with a friendly hello message.

    Args:
        name (str, optional): The name of the person to greet.

    Returns:
        str: A greeting message.
    """
    if name:
        return f"Hello, {name}! How can I assist you today?"
    else:
        return "Hello there! How can I assist you today?"


def say_goodbye() -> str:
    """Says goodbye to the user.

    Returns:
        str: A farewell message.
    """
    return "Goodbye! Have a great day!"


# Define specialized sub-agents
weather_agent = Agent(
    name="weather_agent",
    model="gemini-2.5-flash",
    description="Provides weather information for cities. Use this agent when the user asks about weather.",
    instruction="You are a weather specialist. Provide accurate weather information using the get_weather tool.",
    tools=[get_weather],
)

greeting_agent = Agent(
    name="greeting_agent",
    model="gemini-2.5-flash",
    description="Handles greetings and farewells. Use this agent when the user says hello or goodbye.",
    instruction="You are a friendly greeter. Welcome users warmly and say goodbye courteously using the say_hello and say_goodbye tools.",
    tools=[say_hello, say_goodbye],
)

# Root agent that coordinates the sub-agents
root_agent = Agent(
    name="coordinator_agent",
    model="gemini-2.5-flash",
    description="Main coordinator agent that delegates tasks to specialized agents.",
    instruction=(
        "You are a helpful coordinator assistant. "
        "Delegate weather-related questions to the weather_agent and "
        "greetings/farewells to the greeting_agent. "
        "For other queries, respond directly with helpful information."
    ),
    sub_agents=[weather_agent, greeting_agent],
)
