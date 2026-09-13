import assert from "node:assert/strict";
import fs from "node:fs";
import test from "node:test";
import ts from "typescript";

process.env.TZ = "America/Los_Angeles";
const source = fs.readFileSync(new URL("../lib/videoSchedule.ts", import.meta.url), "utf8");
const compiled = ts.transpileModule(source, {compilerOptions:{module:ts.ModuleKind.ESNext,target:ts.ScriptTarget.ES2020}}).outputText;
const {parseVideoSchedule, formatVideoSchedule} = await import(`data:text/javascript;base64,${Buffer.from(compiled).toString("base64")}`);
const now = Date.parse("2029-01-01T00:00:00Z");

test("converts local winter and summer times to the correct UTC instant", () => {
  assert.equal(parseVideoSchedule("2030-01-15T09:30",now).iso,"2030-01-15T17:30:00.000Z");
  assert.equal(parseVideoSchedule("2030-07-15T09:30",now).iso,"2030-07-15T16:30:00.000Z");
});
test("rejects missing, malformed, impossible and daylight-saving skipped times", () => {
  for (const value of ["", "invalid", "2030-02-30T09:00", "2030-03-10T02:30"])
    assert.equal(parseVideoSchedule(value,now).iso,null,value);
});
test("rejects past times and the preparation window", () => {
  const instant=Date.parse("2030-07-15T16:00:00Z");
  assert.equal(parseVideoSchedule("2030-07-15T08:00",instant).iso,null);
  assert.equal(parseVideoSchedule("2030-07-15T09:09",instant).iso,null);
  assert.equal(parseVideoSchedule("2030-07-15T09:10",instant).iso,"2030-07-15T16:10:00.000Z");
});
test("the confirmation shows the actual timezone for ambiguous fall-back times", () => {
  const value=parseVideoSchedule("2030-11-03T01:30",now);
  assert.equal(value.iso,"2030-11-03T08:30:00.000Z");
  assert.match(formatVideoSchedule(value.iso,"America/Los_Angeles"),/1:30 AM PDT/);
});
