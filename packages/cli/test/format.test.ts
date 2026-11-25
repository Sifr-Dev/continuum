import assert from "node:assert/strict";
import test from "node:test";
import { parseWorkflowSelector, table } from "../src/format.js";

test("parses workflow selectors with and without versions", () => {
  assert.deepEqual(parseWorkflowSelector("checkout"), { name: "checkout" });
  assert.deepEqual(parseWorkflowSelector("checkout@2"), { name: "checkout", version: 2 });
  assert.throws(() => parseWorkflowSelector("checkout@nope"), /version/);
});

test("formats aligned tables", () => {
  const output = table(["name", "status"], [["a", "ok"], ["longer", "bad"]]);
  assert.match(output, /name\s+status/);
  assert.match(output, /longer\s+bad/);
});
