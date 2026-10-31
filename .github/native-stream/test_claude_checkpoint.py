import unittest

from claude_checkpoint import make_checkpoint, resolve_checkpoint


class NativeCheckpointTests(unittest.TestCase):
    def test_offline_analysis_does_not_replace_verified_capture_state(self):
        native = {"sessionId": "native-session", "toolId": "foreground-bash",
                  "verified": True, "toolReceipts": []}
        checkpoint = make_checkpoint(1, 100, 400, 500, native, True)
        # The real command may complete while its already frozen pixels are decoded.
        native["verified"] = False
        native["toolReceipts"].append({"completed": True})
        state = resolve_checkpoint(checkpoint, 1, 100, 250, 900_000)
        self.assertTrue(state["foregroundVerified"])
        self.assertTrue(state["stillBlocked"])
        self.assertEqual(checkpoint["nativeEvidence"]["toolReceipts"], [])
        self.assertEqual(state["foregroundCheckpointNs"], 500)

    def test_pixels_after_recording_or_checkpoint_cannot_borrow_earlier_foreground_proof(self):
        checkpoint = make_checkpoint(1, 100, 400, 500, {"verified": True}, True)
        for stage, emitted, captured, acknowledged in [
            (2, 100, 200, 600), (1, 99, 200, 600), (1, 100, 99, 600),
            (1, 100, 401, 600), (1, 100, 200, 499),
            (1, 100, 200.5, 600),
        ]:
            with self.subTest(stage=stage, emitted=emitted, captured=captured, acknowledged=acknowledged):
                with self.assertRaises(ValueError):
                    resolve_checkpoint(checkpoint, stage, emitted, captured, acknowledged)

    def test_failed_foreground_or_released_command_is_never_upgraded(self):
        for verified, blocked in [(False, True), (True, False), (False, False)]:
            checkpoint = make_checkpoint(3, 100, 400, 500, {"verified": verified}, blocked)
            state = resolve_checkpoint(checkpoint, 3, 100, 250, 1_000)
            self.assertFalse(state["foregroundVerified"] and state["stillBlocked"])

    def test_checkpoint_requires_normal_finalization_before_native_inspection_completes(self):
        for emitted, finalized, checked in [(100, 99, 200), (100, 200, 199), (100, 200, True)]:
            with self.assertRaises(ValueError):
                make_checkpoint(1, emitted, finalized, checked, {"verified": True}, True)


if __name__ == "__main__":
    unittest.main()
