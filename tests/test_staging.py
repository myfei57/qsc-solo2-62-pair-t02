"""Behavioural tests for hierarchical staged loading."""

from __future__ import annotations

import tempfile
import unittest

from coldstore.comp import Bank
from coldstore.load import LoadCalculator
from coldstore.room import RoomRegistry
from coldstore.staging import (
    ORDER_COMP_EFFICIENCY,
    POLICY_MAX_COOLING,
    POLICY_MEAN_QUEUE,
    LoadingScheduler,
    PriorityGate,
    decode_code,
    encode_stage,
    gear_for_stage,
)
from coldstore.staging.budget import allocate, fair_targets
from coldstore.store import Store


def build_scheduler(store: Store) -> LoadingScheduler:
    rooms = RoomRegistry()
    bank = Bank()
    rooms.add_room("r1", 1, -5.0)   # load 19.5 -> 5 stages at 4.0/stage
    rooms.add_room("r2", 2, -10.0)  # load 12.0 -> 3 stages
    rooms.add_room("r3", 3, -20.0)  # no load -> no demand
    rooms.assign_compressor("r1", "c1")
    rooms.assign_compressor("r2", "c2")
    bank.add_compressor("c1", 4)
    bank.add_compressor("c2", 8)
    load = LoadCalculator(store)
    return LoadingScheduler(store, rooms, bank, load)


class EncodingCase(unittest.TestCase):
    def test_four_gear_legacy_encoding(self) -> None:
        self.assertEqual(gear_for_stage(2, 4), 2)
        self.assertEqual(encode_stage(2, 4).code, "10")
        self.assertEqual(encode_stage(0, 8).label, "off")
        self.assertEqual(encode_stage(7, 8).code, "11")
        # fine grained stages are quantized down onto the four gears
        self.assertEqual(gear_for_stage(1, 8), 0)
        self.assertEqual(gear_for_stage(2, 8), 1)
        self.assertEqual(gear_for_stage(4, 8), 2)
        self.assertEqual(decode_code("01"), 1)
        with self.assertRaises(ValueError):
            decode_code("xx")


class BudgetCase(unittest.TestCase):
    def test_fair_targets_use_the_mean(self) -> None:
        self.assertEqual(fair_targets({"a": 6, "b": 0}), {"a": 3, "b": 0})

    def test_max_cooling_satisfies_largest_load_first(self) -> None:
        grants = allocate({"a": 5, "b": 3}, 4, POLICY_MAX_COOLING, ["b", "a"])
        self.assertEqual(grants, {"a": 4, "b": 0})

    def test_mean_queue_funds_every_room_then_wraps(self) -> None:
        grants = allocate({"a": 5, "b": 3}, 4, POLICY_MEAN_QUEUE, ["a", "b"])
        # mean is 4, a takes 4 first; the next round feeds b
        self.assertEqual(grants["a"] + grants["b"], 4)
        full = allocate({"a": 5, "b": 3}, 8, POLICY_MEAN_QUEUE, ["a", "b"])
        self.assertEqual(full, {"a": 5, "b": 3})

    def test_unknown_policy_rejected(self) -> None:
        with self.assertRaises(ValueError):
            allocate({"a": 1}, 1, "bogus", ["a"])


class GateCase(unittest.TestCase):
    def test_priority_order_and_token_limit(self) -> None:
        gate = PriorityGate(rate=0.0, burst=1)
        gate.submit("late", 9)
        gate.submit("early", 1)
        first = gate.admit()
        second = gate.admit()
        self.assertEqual(first.room_id, "early")
        self.assertIsNone(second)
        state = gate.state()
        self.assertEqual(state.depth, 1)
        self.assertEqual(state.admitted, 1)

    def test_refill_admits_over_time(self) -> None:
        ticks = [0.0]
        gate = PriorityGate(rate=1.0, burst=1, clock=lambda: ticks[0])
        gate.submit("a", 1)
        gate.admit()
        gate.submit("b", 2)
        self.assertIsNone(gate.admit())
        ticks[0] = 1.0
        self.assertEqual(gate.admit().room_id, "b")

    def test_queue_full_is_rejected(self) -> None:
        gate = PriorityGate(rate=1.0, burst=1, max_depth=1)
        gate.submit("a", 1)
        with self.assertRaises(ValueError):
            gate.submit("b", 1)


class SchedulerCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.store = Store.open(f"{self._tmp.name}/state.json")
        self.scheduler = build_scheduler(self.store)

    def tearDown(self) -> None:
        self.store.close()
        self._tmp.cleanup()

    def test_target_stages_follow_demand(self) -> None:
        self.assertEqual(self.scheduler.target_for(-5.0), 5)
        self.assertEqual(self.scheduler.target_for(-10.0), 3)
        self.assertEqual(self.scheduler.target_for(-20.0), 0)

    def test_plan_matches_machines_and_legacy_codes(self) -> None:
        plan = self.scheduler.plan()
        self.assertEqual(plan.target_stages, 8)
        by_comp = {row.comp_id: row for row in plan.allocations}
        self.assertEqual(by_comp["c1"].granted, 4)
        self.assertEqual(by_comp["c1"].code.code, "11")
        self.assertEqual(by_comp["c2"].granted, 3)
        self.assertEqual(by_comp["c2"].code.code, "01")

    def test_machine_order_policy_switch(self) -> None:
        self.scheduler.set_efficiency("c2", 3.0)
        self.scheduler.set_order_policy(ORDER_COMP_EFFICIENCY)
        self.assertEqual(self.scheduler.ranked_machines(), ["c2", "c1"])

    def test_apply_stages_bank_and_records_batch(self) -> None:
        record = self.scheduler.apply(budget=3, reason="test")
        self.assertEqual(record.used, 3)
        self.assertTrue(record.applied)
        self.assertEqual(self.scheduler.archive().count(), 1)
        self.assertIsNotNone(self.scheduler.archive().version(record.seq))
        self.assertEqual(record.policy, POLICY_MAX_COOLING)

    def test_per_machine_cap_limits_single_step_up(self) -> None:
        self.scheduler.set_cap_comp(2)
        plan = self.scheduler.plan()
        self.assertTrue(all(row.granted <= 2 for row in plan.allocations))
        capped = {row.comp_id: row for row in plan.allocations}
        self.assertIn("cap-comp:2", capped["c1"].basis)

    def test_policy_validation_persisted(self) -> None:
        self.scheduler.set_policy(POLICY_MEAN_QUEUE)
        self.assertEqual(self.scheduler.policy(), POLICY_MEAN_QUEUE)
        with self.assertRaises(ValueError):
            self.scheduler.set_policy("nope")

    def test_throttle_validation(self) -> None:
        with self.assertRaises(ValueError):
            self.scheduler.set_throttle(0.0, 1)
        with self.assertRaises(ValueError):
            self.scheduler.set_throttle(1.0, 0)


if __name__ == "__main__":
    unittest.main()
