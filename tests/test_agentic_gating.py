import unittest

from soul_anchor.manager import MemoryManager


class TestMemoryGating(unittest.TestCase):
    def setUp(self):
        self.manager = MemoryManager(db_path=":memory:")
        self.manager.connect()

        # Contract (TDD): implementation should live in soul_anchor/agentic/gating.py
        from soul_anchor.agentic.gating import MemoryGating  # noqa: PLC0415

        self.gating = MemoryGating(self.manager)

    def tearDown(self):
        self.manager.close()

    def test_noise_filter_rejects_low_signal_chat(self):
        result = self.gating.gate_episode(
            {
                "session_id": "s1",
                "user_id": "u1",
                "event_type": "user_message",
                "content": "好的",
            }
        )

        self.assertFalse(result.accepted)
        self.assertEqual(result.target_layer, "drop")
        self.assertIn("noise", result.reasons)

    def test_duplicate_detection_rejects_equivalent_knowledge_candidate(self):
        existing_id = self.manager.save_knowledge(
            {
                "user_id": "u2",
                "knowledge_type": "workflow",
                "title": "Commit Discipline",
                "canonical_text": "Every change should be committed with a short subject and a detailed body.",
                "keywords": "commit,discipline",
                "stability_score": 0.95,
            }
        )

        candidate = {
            "user_id": "u2",
            "knowledge_type": "workflow",
            "title": "Commit Discipline (duplicate)",
            "canonical_text": "Every change should be committed with a short subject and a detailed body.",
            "candidate_payload": {"reason": "user preference"},
        }

        result = self.gating.gate_knowledge_candidate(candidate)

        self.assertFalse(result.accepted)
        self.assertEqual(result.target_layer, "drop")
        self.assertEqual(result.duplicate_of, existing_id)
        self.assertIn("duplicate", result.reasons)

    def test_l3_update_is_not_auto_written(self):
        result = self.gating.gate_core_contract_update(
            {
                "contract_key": "memory_principle",
                "contract_value": "以后所有提交都必须包含 emoji。",
                "priority": 999,
            }
        )

        self.assertFalse(result.accepted)
        self.assertEqual(result.target_layer, "L3")
        self.assertTrue(result.requires_review)
        self.assertIn("l3_requires_review", result.reasons)


if __name__ == "__main__":
    unittest.main()

