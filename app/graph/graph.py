from __future__ import annotations

from langgraph.graph import END, START, StateGraph

from app.deps.runtime import RuntimeDeps
from app.graph.checkpointing import build_checkpointer
from app.graph.nodes import NodeRunner
from app.graph.routing import route_specialist
from app.graph.state import WorkflowState


async def build_graph(runtime: RuntimeDeps):
    nodes = NodeRunner(runtime)
    workflow = StateGraph(WorkflowState)
    workflow.add_node("correlate_and_dedupe", nodes.correlate_and_dedupe)
    workflow.add_node("recall_history", nodes.recall_history)
    workflow.add_node("supervisor_route", nodes.supervisor_route)
    workflow.add_node("bgp_specialist", nodes.bgp_specialist)
    workflow.add_node("firewall_specialist", nodes.firewall_specialist)
    workflow.add_node("infrastructure_specialist", nodes.infrastructure_specialist)
    workflow.add_node("evidence_validation", nodes.evidence_validation)
    workflow.add_node("golden_state_drift_check", nodes.golden_state_drift_check)
    workflow.add_node("proposal_build", nodes.proposal_build)
    workflow.add_node("prepare_approval", nodes.prepare_approval)
    workflow.add_node("approval_interrupt", nodes.approval_interrupt)

    workflow.add_edge(START, "correlate_and_dedupe")
    workflow.add_edge("correlate_and_dedupe", "recall_history")
    workflow.add_edge("recall_history", "supervisor_route")
    workflow.add_conditional_edges(
        "supervisor_route",
        route_specialist,
        {
            "bgp_specialist": "bgp_specialist",
            "firewall_specialist": "firewall_specialist",
            "infrastructure_specialist": "infrastructure_specialist",
        },
    )
    for node in ("bgp_specialist", "firewall_specialist", "infrastructure_specialist"):
        workflow.add_edge(node, "evidence_validation")
    workflow.add_edge("evidence_validation", "golden_state_drift_check")
    workflow.add_edge("golden_state_drift_check", "proposal_build")
    workflow.add_edge("proposal_build", "prepare_approval")
    workflow.add_edge("prepare_approval", "approval_interrupt")
    workflow.add_edge("approval_interrupt", END)

    checkpointer = await build_checkpointer()
    return workflow.compile(checkpointer=checkpointer)

