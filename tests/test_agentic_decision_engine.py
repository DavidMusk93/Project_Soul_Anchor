import unittest


class TestDecisionEngine(unittest.TestCase):
    def setUp(self):
        from soul_anchor.agentic.decision_engine import DecisionEngine  # noqa: PLC0415

        self.engine = DecisionEngine()

    def test_history_reference_triggers_recall(self):
        decision = self.engine.decide(
            session_id="s1",
            user_id="u1",
            event_type="user_message",
            content="还是按上次那种方式来，先写测试再实现。",
        )

        self.assertTrue(decision.should_recall_context)
        self.assertTrue(decision.should_recall_knowledge)
        self.assertIn("user_referenced_history", decision.reasons)

    def test_preference_statement_creates_knowledge_candidate(self):
        decision = self.engine.decide(
            session_id="s2",
            user_id="u2",
            event_type="user_message",
            content="请记住我偏好简短提交，并且必须带 detail。",
        )

        self.assertTrue(decision.should_write_episode)
        self.assertTrue(decision.should_create_knowledge_candidate)
        self.assertIn("user_stated_preference", decision.reasons)

    def test_normal_message_only_writes_episode(self):
        decision = self.engine.decide(
            session_id="s3",
            user_id="u3",
            event_type="user_message",
            content="今天我们继续推进 Phase 3.1。",
        )

        self.assertTrue(decision.should_write_episode)
        self.assertFalse(decision.should_create_knowledge_candidate)
        self.assertFalse(decision.should_recall_knowledge)


if __name__ == "__main__":
    unittest.main()

