"""Build stream processors from external definition files."""

import yaml
from typing import Dict, List
from pathlib import Path
from kaspr.types import KasprAppT, AppBuilderT, KasprStreamT
from kaspr.types.models import AppSpec, AgentSpec
from kaspr.types.schemas import AppSpecSchema

from mode.utils.objects import cached_property


class AppBuilder(AppBuilderT):
    """Build stream processors from definition files."""

    app: KasprAppT = None

    _apps: List[AppSpec] = None
    _agents: List[AgentSpec] = None

    def __init__(self, app: KasprAppT) -> None:
        self.app = app

    def _files(self, directory: Path) -> List[str]:
        """Find all JSON files in the given directory and its subdirectories."""
        return list(directory.rglob("*.yml")) + list(directory.rglob("*.yaml"))

    def _load(self, files: List[Path]) -> List[AppSpec]:
        """Load the content of definition files and convert them to dictionaries."""
        apps = []
        for file in files:
            with file.open("r") as file:
                try:
                    apps.append(AppSpecSchema.from_file(file, app=self.app))
                except yaml.YAMLError as exc:
                    print(f"Error loading file `{file}`: {exc}")
        return apps

    def _load_apps(self) -> List[AppSpec]:
        """Load app component definitions."""
        return self._load(self._files(self.app.conf.definitionssdir))

    def _prepare_agents(self) -> List[AgentSpec]:
        """Prepare agents from loaded definitions."""
        agents = []
        for app in self.apps:
            agents.extend(app.agents_spec)
        return agents

    def build(self) -> None:
        """Build agents, tasks, etc. from external definition files."""
        for app in self.apps:
            app.agents

    @cached_property
    def apps(self) -> List[AppSpec]:
        if self._apps is None:
            self._apps = self._load_apps()
        return self._apps

    @cached_property
    def agents(self) -> List[AgentSpec]:
        if self._agents is None:
            self._agents = self._prepare_agents()
        return self._agents




# # operators are a form of data processing. 
# # i.e. Arithamic operators, logical operators, comparison operators, 
# # assignment operators, bitwise operators, etc. are some of the operators used in Python.

# # terminal functions:
# # - take

# models: []
# tables: []
# agents:
#   - name: inspector
#     description: Compute transaction risk score
#     inputs:
#       topic:
#         names: 
#           - transactions
#         #pattern: "transactions/*"
#         key_serializer: json
#         value_serializer: json
#     processors: # AgentProcessors
#       pipeline:
#         #- remove-deposits
#         - debug
#         # - repartition
#         # - compute-risk-score
#         # - reformat
#         # - batch
#         # - send-to-output-topic
#       init:
#         python: |
#           import uuid
#           things = { 'id': "ABC" }
#       operations: # AgentProcessorOperation
#         # - name: remove-deposits
#         #   filter:
#         #     python: |
#         #       def filter(transaction):
#         #         return transaction['amount'] > 1000
#         - name: debug
#           map:
#             python: |
#               def simplify2(transaction):
#                 print(things['id'])
#                 return {
#                   'transaction_id': things['id'],
#                   'sales_dollars': transaction['amount'],
#                 }
#   - name: risk-evaluator
#     description: Compute transaction risk score
#     inputs:
#       topic:
#         names: 
#           - transactions
#         #pattern: "transactions/*"
#         key_serializer: json
#         value_serializer: json
#     processors: # AgentProcessors
#       pipeline:
#         #- remove-deposits
#         - reformat
#         # - repartition
#         # - compute-risk-score
#         # - reformat
#         # - batch
#         # - send-to-output-topic
#       init:
#         python: |
#           import uuid
#           context = { 'id': str(uuid.uuid4()) }
#       operations: # AgentProcessorOperation
#         # - name: remove-deposits
#         #   filter:
#         #     python: |
#         #       def filter(transaction):
#         #         return transaction['amount'] > 1000
#         - name: reformat
#           map:
#             python: |
#               def simplify(transaction):
#                 print(context['id'])
#                 return {
#                   'transaction_id': str(uuid.uuid4()),
#                   'sales_dollars': transaction['amount'],
#                 }
#     #     - name: compute-risk-score
#     #       custom:
#     #         python:
#     #           function: |
#     #             def process(transaction):
#     #               return {
#     #                 'transaction_id': transaction['transaction_id'],
#     #                 'risk_score': transaction['amount'] * 0.1
#     #               }
#     #     - name: reformat
#     #       map:
#     #         python: |
#     #           def process(transaction):
#     #             return {
#     #               'transaction_id': transaction['transaction_id'],
#     #               'risk_score': transaction['risk_score']
#     #             }
#     #     - name: repartition
#     #       group_by:
#     #         key:
#     #           python: |
#     #             def get_key(transaction):
#     #               return transaction['user_id']
#     #         topic: transactions-by-user-id
#     #     - name: batch
#     #       take:
#     #         size: 100
#     #         within: 0.5s
#     #     - name: send-to-output-topic
#     #       echo:
#     #         topics: 
#     #           - risk-scores
#     # outputs:
#     #   - topics:
#     #       - risk-scores
#     #       - risk-alerts