from pydantic_ai import Agent
print([n for n in dir(Agent) if 'tool' in n])
