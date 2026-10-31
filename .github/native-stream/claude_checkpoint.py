"""Immutable native foreground evidence collected before offline pixel analysis."""
import copy


def make_checkpoint(stage, emitted_ns, finalized_ns, checked_ns, evidence, still_blocked):
    if not all(type(value) is int for value in (emitted_ns, finalized_ns, checked_ns)) or not (
        emitted_ns <= finalized_ns <= checked_ns
    ):
        raise ValueError("Checkpoint must follow emission and normal recording finalization")
    return {"stage": str(stage), "emittedNs": emitted_ns, "recordingFinalizedNs": finalized_ns,
            "checkpointNs": checked_ns, "foregroundVerified": evidence.get("verified") is True,
            "stillBlocked": still_blocked is True, "nativeEvidence": copy.deepcopy(evidence),
            "foregroundEvidence": "native-evidence/stage-" + str(stage) + ".json"}


def resolve_checkpoint(checkpoint, stage, emitted_ns, captured_ns, acknowledged_ns):
    if checkpoint.get("stage") != str(stage) or checkpoint.get("emittedNs") != emitted_ns:
        raise ValueError("Checkpoint belongs to another native emission")
    values = (emitted_ns, captured_ns, checkpoint.get("recordingFinalizedNs"),
              checkpoint.get("checkpointNs"), acknowledged_ns)
    if not all(type(value) is int for value in values) or not all(
        earlier <= later for earlier, later in zip(values, values[1:])
    ):
        raise ValueError("Pixels must precede the immutable foreground checkpoint")
    return {"foregroundVerified": checkpoint["foregroundVerified"],
            "stillBlocked": checkpoint["stillBlocked"],
            "foregroundEvidence": checkpoint["foregroundEvidence"],
            "foregroundCheckpointNs": checkpoint["checkpointNs"],
            "foregroundStateScope": "native state verified after recording, before completion and offline analysis"}
