from pydantic_ai import Agent
import inspect
print([n for n in dir(Agent) if 'tool' in n])
