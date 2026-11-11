from typing import Container, List, Mapping, Optional
from kaspr.types.models.base import SpecComponent
from kaspr.types.models.agent.input import AgentInputSpec
from kaspr.types.models.agent.output import AgentOutputSpec
from kaspr.types.models.agent.processor import AgentProcessorSpec
from kaspr.types.app import KasprAppT
from kaspr.types.agent import KasprAgentT

E_JOIN_KEY_SERIALIZER = (
    "Agent '{agent}' uses table '{table}', joined by {joins}, but its input "
    "topic '{topic}' has key serializer '{topic_serializer}' and the table "
    "'{table_serializer}'. A join finds a row only on the partition its "
    "source record's key puts it: give the topic and the table the same "
    "keySerializer. Unset, a topic's is the app's key serializer "
    "('{app_serializer}') and a table's is 'json'."
)


class AgentSpec(SpecComponent):
    name: str
    description: Optional[str]
    isolated_partitions: Optional[bool]
    noack: Optional[bool]
    input: AgentInputSpec
    output: AgentOutputSpec
    processors: AgentProcessorSpec

    app: KasprAppT = None

    _agent: KasprAgentT = None

    def prepare_agent(self) -> KasprAgentT:
        self.log.info("Preparing...")
        processors = self.processors
        processors.input = self.input
        processors.output = self.output
        processors.noack = self.noack
        return self.app.agent(
            self.input.channel,
            name=self.name,
            isolated_partitions=self.isolated_partitions,
        )(processors.processor)

    def join_key_mismatches(
        self, joins_by_table: Mapping[str, List[str]]
    ) -> List[str]:
        """Describe each joined table this agent uses whose key serializer
        differs from its input topic's, the defaults resolved."""
        topic_spec = self._input_topic_spec()
        if topic_spec is None:
            return []
        app_serializer = self.app.conf.key_serializer
        topic_serializer = topic_spec.key_serializer or app_serializer
        problems = []
        for table in self._collect_table_refs():
            joins = joins_by_table.get(table.name)
            if joins and table.key_serializer != topic_serializer:
                problems.append(
                    E_JOIN_KEY_SERIALIZER.format(
                        agent=self.name,
                        table=table.name,
                        joins=", ".join(f"'{join}'" for join in joins),
                        topic=topic_spec.name or topic_spec.pattern,
                        topic_serializer=topic_serializer,
                        table_serializer=table.key_serializer,
                        app_serializer=app_serializer,
                    )
                )
        return problems

    def warn_serializer_mismatches(self, joined: Container[str] = ()) -> None:
        """Log a warning for each table whose serializers differ from the
        ones set on the input topic. A key mismatch on a joined table is an
        error instead (see join_key_mismatches)."""
        topic_spec = self._input_topic_spec()
        if topic_spec is None:
            return
        topic_key_ser = topic_spec.key_serializer
        topic_val_ser = topic_spec.value_serializer
        for table in self._collect_table_refs():
            if (
                table.name not in joined
                and topic_key_ser is not None
                and topic_key_ser != table.key_serializer
            ):
                self.log.warning(
                    "Agent '%s': input topic key_serializer '%s' does not "
                    "match table '%s' key_serializer '%s'. This may cause "
                    "double-encoded keys. Consider aligning them.",
                    self.name, topic_key_ser, table.name, table.key_serializer,
                )
            if topic_val_ser is not None and topic_val_ser != table.value_serializer:
                self.log.warning(
                    "Agent '%s': input topic value_serializer '%s' does not "
                    "match table '%s' value_serializer '%s'. This may cause "
                    "serialization issues. Consider aligning them.",
                    self.name, topic_val_ser, table.name, table.value_serializer,
                )

    def _input_topic_spec(self):
        """The input topic's spec, or None when the input is a channel."""
        topic_spec = getattr(self.input, "topic_spec", None)
        if topic_spec is None or not (topic_spec.name or topic_spec.pattern):
            return None
        return topic_spec

    def _collect_table_refs(self):
        """Collect resolved table objects from all operations."""
        tables = []
        for op in self.processors.operations:
            if op.table_refs:
                for ref in op.table_refs:
                    table = self.app.tables.get(ref.name)
                    if table is not None and table not in tables:
                        tables.append(table)
        return tables

    @property
    def agent(self) -> KasprAgentT:
        if self._agent is None:
            self._agent = self.prepare_agent()
        return self._agent

    @property
    def label(self) -> str:
        """Return description of component, used in logs."""
        return f"{type(self).__name__}: {self.__repr__()}"

    @property
    def shortlabel(self) -> str:
        """Return short description of processor."""
        return f"{type(self).__name__}: {self.name}"
