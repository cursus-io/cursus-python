class CommandBuilder:
    @staticmethod
    def create(topic: str, partitions: int, *, event_sourcing: bool = False) -> str:
        cmd = f"CREATE topic={topic} partitions={partitions}"
        if event_sourcing:
            cmd += " event_sourcing=true"
        return cmd

    @staticmethod
    def consume(
        topic: str, partition: int, offset: int, member: str, group: str = "",
        generation: int = -1,
    ) -> str:
        cmd = f"CONSUME topic={topic} partition={partition} offset={offset} member={member}"
        if group:
            cmd += f" group={group}"
        if generation >= 0:
            cmd += f" generation={generation}"
        return cmd

    @staticmethod
    def stream(
        topic: str, partition: int, group: str, member: str, generation: int,
        offset: int = 0,
    ) -> str:
        cmd = (
            f"STREAM topic={topic} partition={partition} group={group} "
            f"member={member} generation={generation}"
        )
        if offset > 0:
            cmd += f" offset={offset}"
        return cmd

    @staticmethod
    def join_group(topic: str, group: str, member: str) -> str:
        return f"JOIN_GROUP topic={topic} group={group} member={member}"

    @staticmethod
    def sync_group(topic: str, group: str, member: str, generation: int) -> str:
        return f"SYNC_GROUP topic={topic} group={group} member={member} generation={generation}"

    @staticmethod
    def leave_group(topic: str, group: str, member: str) -> str:
        return f"LEAVE_GROUP topic={topic} group={group} member={member}"

    @staticmethod
    def heartbeat(topic: str, group: str, member: str, generation: int) -> str:
        return f"HEARTBEAT topic={topic} group={group} member={member} generation={generation}"

    @staticmethod
    def commit_offset(
        topic: str, group: str, partition: int, offset: int, generation: int, member: str
    ) -> str:
        return (
            f"COMMIT_OFFSET topic={topic} partition={partition} group={group} "
            f"offset={offset} generation={generation} member={member}"
        )

    @staticmethod
    def batch_commit(
        topic: str, group: str, member: str, generation: int, offsets: dict[int, int]
    ) -> str:
        parts = ",".join(f"{pid}:{off}" for pid, off in offsets.items())
        return (
            f"BATCH_COMMIT topic={topic} group={group} generation={generation} "
            f"member={member} {parts}"
        )

    @staticmethod
    def fetch_offset(topic: str, partition: int, group: str) -> str:
        return f"FETCH_OFFSET topic={topic} partition={partition} group={group}"

    @staticmethod
    def append_stream(
        topic: str,
        key: str,
        version: int,
        event_type: str,
        schema_version: int,
        producer_id: str,
        payload: str,
        metadata: str = "",
    ) -> str:
        cmd = (
            f"APPEND_STREAM topic={topic} key={key} version={version} "
            f"event_type={event_type} schema_version={schema_version} "
            f"producerId={producer_id}"
        )
        if metadata:
            cmd += f" metadata={metadata}"
        cmd += f" message={payload}"
        return cmd

    @staticmethod
    def read_stream(topic: str, key: str, from_version: int = 0) -> str:
        cmd = f"READ_STREAM topic={topic} key={key}"
        if from_version > 0:
            cmd += f" from_version={from_version}"
        return cmd

    @staticmethod
    def save_snapshot(topic: str, key: str, version: int, payload: str) -> str:
        return f"SAVE_SNAPSHOT topic={topic} key={key} version={version} message={payload}"

    @staticmethod
    def read_snapshot(topic: str, key: str) -> str:
        return f"READ_SNAPSHOT topic={topic} key={key}"

    @staticmethod
    def stream_version(topic: str, key: str) -> str:
        return f"STREAM_VERSION topic={topic} key={key}"
