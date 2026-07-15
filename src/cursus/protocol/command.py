class CommandBuilder:
    @staticmethod
    def auth_fields(principal: str | None = None, auth_token: str | None = None) -> str:
        fields = []
        if principal:
            fields.append(f"principal={principal}")
        if auth_token:
            fields.append(f"auth_token={auth_token}")
        return " ".join(fields)

    @staticmethod
    def _append_auth(cmd: str, principal: str | None = None, auth_token: str | None = None) -> str:
        auth = CommandBuilder.auth_fields(principal, auth_token)
        return f"{cmd} {auth}" if auth else cmd

    @staticmethod
    def create(topic: str, partitions: int, *, event_sourcing: bool = False) -> str:
        cmd = f"CREATE topic={topic} partitions={partitions}"
        if event_sourcing:
            cmd += " event_sourcing=true"
        return cmd

    @staticmethod
    def consume(
        topic: str,
        partition: int,
        offset: int,
        member: str,
        group: str = "",
        generation: int = -1,
        isolation_level: str | None = None,
        principal: str | None = None,
        auth_token: str | None = None,
    ) -> str:
        cmd = f"CONSUME topic={topic} partition={partition} offset={offset} member={member}"
        if group:
            cmd += f" group={group}"
        if generation >= 0:
            cmd += f" generation={generation}"
        if isolation_level:
            cmd += f" isolation_level={isolation_level}"
        return CommandBuilder._append_auth(cmd, principal, auth_token)

    @staticmethod
    def stream(
        topic: str,
        partition: int,
        group: str,
        member: str,
        generation: int,
        offset: int = 0,
        isolation_level: str | None = None,
        principal: str | None = None,
        auth_token: str | None = None,
    ) -> str:
        cmd = (
            f"STREAM topic={topic} partition={partition} group={group} "
            f"member={member} generation={generation}"
        )
        if offset > 0:
            cmd += f" offset={offset}"
        if isolation_level:
            cmd += f" isolation_level={isolation_level}"
        return CommandBuilder._append_auth(cmd, principal, auth_token)

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
        parts = ",".join(f"P{pid}:{offsets[pid]}" for pid in sorted(offsets))
        return (
            f"BATCH_COMMIT topic={topic} group={group} member={member} "
            f"generation={generation} {parts}"
        )

    @staticmethod
    def fetch_offset(topic: str, partition: int, group: str) -> str:
        return f"FETCH_OFFSET topic={topic} partition={partition} group={group}"

    @staticmethod
    def find_coordinator(*, group: str | None = None, transactional_id: str | None = None) -> str:
        if transactional_id:
            return f"FIND_COORDINATOR transactional_id={transactional_id}"
        if group:
            return f"FIND_COORDINATOR group={group}"
        raise ValueError("group or transactional_id is required")

    @staticmethod
    def list_offsets(
        topic: str,
        partition: int | None = None,
        principal: str | None = None,
        auth_token: str | None = None,
    ) -> str:
        cmd = f"LIST_OFFSETS topic={topic}"
        if partition is not None:
            cmd += f" partition={partition}"
        return CommandBuilder._append_auth(cmd, principal, auth_token)

    @staticmethod
    def init_producer_id(transactional_id: str) -> str:
        return f"INIT_PRODUCER_ID transactional_id={transactional_id}"

    @staticmethod
    def begin_txn(transactional_id: str, producer_id: str, epoch: int) -> str:
        return (
            f"BEGIN_TXN transactional_id={transactional_id} producerId={producer_id} epoch={epoch}"
        )

    @staticmethod
    def txn_publish(
        transactional_id: str,
        topic: str,
        partition: int,
        producer_id: str,
        seq_num: int,
        epoch: int,
        message: str,
        *,
        key: str = "",
        principal: str | None = None,
        auth_token: str | None = None,
    ) -> str:
        cmd = (
            f"TXN_PUBLISH transactional_id={transactional_id} topic={topic} partition={partition} "
            f"producerId={producer_id} seqNum={seq_num} epoch={epoch}"
        )
        if key:
            cmd += f" key={key}"
        cmd += f" message={message}"
        return CommandBuilder._append_auth(cmd, principal, auth_token)

    @staticmethod
    def send_offsets_to_txn(
        transactional_id: str,
        producer_id: str,
        epoch: int,
        topic: str,
        group: str,
        member: str,
        generation: int,
        offsets: dict[int, int],
    ) -> str:
        parts = ",".join(f"P{pid}:{offsets[pid]}" for pid in sorted(offsets))
        return (
            f"SEND_OFFSETS_TO_TXN transactional_id={transactional_id} producerId={producer_id} "
            f"epoch={epoch} topic={topic} group={group} member={member} "
            f"generation={generation} {parts}"
        )

    @staticmethod
    def end_txn(transactional_id: str, producer_id: str, epoch: int, *, commit: bool = True) -> str:
        result = "commit" if commit else "abort"
        return (
            f"END_TXN transactional_id={transactional_id} producerId={producer_id} "
            f"epoch={epoch} result={result}"
        )

    @staticmethod
    def txn_status(transactional_id: str) -> str:
        return f"TXN_STATUS transactional_id={transactional_id}"

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
