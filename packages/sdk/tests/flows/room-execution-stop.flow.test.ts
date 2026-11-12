import { agentControlTopic } from "@band-ai/band-sdk-core";
import { BandClient } from "@band-ai/rest-client";
import { afterEach, describe, expect, it, vi } from "vitest";

import { ACPClientAdapter } from "../../src/adapters/acp/ACPClientAdapter";
import { CursorACPAdapter } from "../../src/adapters/cursor-acp";
import { OmpRpcAdapter, type OmpRpcAdapterOptions } from "../../src/adapters/omp-rpc";
import { Agent } from "../../src/agent/Agent";
import { FernRestAdapter } from "../../src/client/rest/FernRestAdapter";
import type { RestApi } from "../../src/client/rest/types";
import type { AgentEvent } from "../../src/contracts/events";
import type { AdapterToolsProtocol, FrameworkAdapter } from "../../src/contracts/protocols";
import { createDeferred } from "../../src/core/deferred";
import { BandLink } from "../../src/platform/BandLink";
import { sessionConfigSchema } from "../../src/runtime/types";
import { fakeOmpProcesses } from "../support/fakeOmp";
import { RecordLog, tmpRoot } from "../testUtils";
import { NO_REPLY_ARGS } from "../turnOutcomeContract";
import { AGENT_API_KEY, AGENT_ID, BandPlatform, person } from "./support/bandPlatform";
import { FakeCursorAgent } from "./support/fakeCursorAgent";

const ROOM = "room-1";
const KEEPALIVE_MS = sessionConfigSchema.parse({}).workingKeepAliveSeconds * 1000;

/** Real generated activity replies, with only fetch replaced by the platform's stopped gate. */
function activityServer() {
  const stopped = new Set<string>();
  const lifecycleStopped = new Set<string>();
  const postStopped = new Set<string>();
  const postFailure = { message: "Room execution is stopped" };
  const transportFailed = new Set<string>();
  const transportErrors = new RecordLog<string>();
  const writes = new RecordLog<{ roomId: string; status: number }>();
  const requests = new RecordLog<{ roomId: string; working: boolean }>();
  const fetch: typeof globalThis.fetch = async (input, init) => {
    const url = new URL(input instanceof Request ? input.url : input.toString());
    const roomId = url.pathname.split("/chats/")[1]!.split("/")[0]!;
    const body = JSON.parse(String(init?.body ?? "{}")) as { working?: boolean };
    if (typeof body.working !== "boolean") {
      const status = lifecycleStopped.has(roomId) ? 204 : 403;
      writes.record({ roomId, status });
      return status === 204 ? new Response(null, { status }) : Response.json(
        { error: { code: "forbidden", message: postFailure.message, request_id: "stop-test" } }, { status },
      );
    }
    if (transportFailed.has(roomId)) {
      transportErrors.record(roomId);
      throw new TypeError("Activity transport disconnected");
    }
    const working = body.working && !stopped.has(roomId);
    requests.record({ roomId, working });
    return Response.json({ data: { working } });
  };
  const rest = new FernRestAdapter(new BandClient({ apiKey: AGENT_API_KEY, baseUrl: "http://band.test", fetch }));
  return { stopped, lifecycleStopped, postStopped, postFailure, transportFailed, transportErrors, requests, writes, rest };
}

async function runningRoom(adapter: FrameworkAdapter, journalDir?: string) {
  const server = activityServer();
  const platform = BandPlatform.host([person("owner"), person("teammate")]);
  const rest: RestApi = platform.rest;
  rest.reportActivity = (roomId, working) => server.rest.reportActivity(roomId, working);
  const acceptedMessage = rest.createChatMessage.bind(rest);
  const acceptedEvent = rest.createChatEvent.bind(rest);
  const processing = rest.markMessageProcessing.bind(rest);
  const processed = rest.markMessageProcessed.bind(rest);
  const failed = rest.markMessageFailed.bind(rest);
  rest.createChatMessage = (roomId, body) => server.postStopped.has(roomId)
    ? server.rest.createChatMessage(roomId, body) : acceptedMessage(roomId, body);
  rest.createChatEvent = (roomId, body) => server.postStopped.has(roomId)
    ? server.rest.createChatEvent(roomId, body) : acceptedEvent(roomId, body);
  rest.markMessageProcessing = (roomId, id) => server.lifecycleStopped.has(roomId)
    ? server.rest.markMessageProcessing(roomId, id) : processing(roomId, id);
  rest.markMessageProcessed = (roomId, id) => server.lifecycleStopped.has(roomId)
    ? server.rest.markMessageProcessed(roomId, id) : processed(roomId, id);
  rest.markMessageFailed = (roomId, id, error) => server.lifecycleStopped.has(roomId)
    ? server.rest.markMessageFailed(roomId, id, error) : failed(roomId, id, error);
  const nextRequests = new RecordLog<string>();
  const getNext = platform.rest.getNextMessage.bind(platform.rest);
  rest.getNextMessage = async (request) => {
    nextRequests.record(request.chatId);
    return server.stopped.has(request.chatId) ? null : await getNext(request);
  };
  const tools = new RecordLog<AdapterToolsProtocol>();
  const onEvent = adapter.onEvent.bind(adapter);
  adapter.onEvent = async (input) => {
    tools.record(input.tools);
    await onEvent(input);
  };
  const room = await platform.room(ROOM, [person("owner")]);
  const link = new BandLink({ agentId: AGENT_ID, apiKey: AGENT_API_KEY, ...platform.link });
  const agent = Agent.create({
    agentId: AGENT_ID,
    apiKey: AGENT_API_KEY,
    adapter,
    link,
    sessionConfig: { journalDir },
  });
  const events = new RecordLog<AgentEvent>();
  agent.observe((event) => events.record(event));
  await agent.start();
  return { agent, platform, room, server, events, tools, link, nextRequests, async [Symbol.asyncDispose]() { await agent.stop(0); } };
}

function ompAdapter(options: Partial<OmpRpcAdapterOptions> = {}) {
  const omp = fakeOmpProcesses();
  return { omp, adapter: new OmpRpcAdapter({ cwd: tmpRoot(), spawnProcess: omp.spawnProcess, ...options }) };
}

interface RunningPrompt {
  reply(text: string): Promise<void>;
  decline(): Promise<void>;
  finish(): Promise<void>;
  cancelled(): boolean;
}

/** Protocol peers only; the runtime and each adapter remain real. */
function protocolPeer(protocol: "ACP" | "OMP") {
  if (protocol === "OMP") {
    const { omp, adapter } = ompAdapter();
    return {
      adapter,
      async next(roomId = ROOM): Promise<RunningPrompt> {
        const peer = omp.peers.find((candidate) => candidate.options.cwd?.endsWith(roomId)) ?? await omp.nextProcess();
        const prompt = await peer.nextOfType("prompt");
        return {
          reply: async (text) => { peer.send({ type: "message_end", message: { role: "assistant", content: [{ type: "text", text }] } }); },
          decline: async () => { await peer.callHostTool(`decline-${String(prompt.id)}`, "band_no_reply", NO_REPLY_ARGS); },
          finish: async () => { peer.finish(prompt.id); },
          cancelled: () => peer.received.some((frame) => frame.type === "abort"),
        };
      },
    };
  }
  const acp = new FakeCursorAgent();
  const adapter = new ACPClientAdapter({ cwd: tmpRoot(), command: ["fake-acp"], connectionFactory: acp.connectionFactory, enableMcpTools: false });
  return {
    adapter,
    next(roomId = ROOM): Promise<RunningPrompt> {
      const prompted = createDeferred<RunningPrompt>();
      const release = createDeferred();
      const result = acp.room(roomId).nextTurn(async (turn) => {
        prompted.resolve({
          reply: (text) => turn.say(text),
          decline: () => turn.callTool("band_no_reply", NO_REPLY_ARGS),
          finish: async () => { release.resolve(); await result; },
          cancelled: () => acp.receivedOf("session/cancel").length > 0,
        });
        await release.promise;
      });
      return prompted.promise;
    },
  };
}

describe("room Stop through the runtime and adapters", () => {
  afterEach(() => vi.useRealTimers());

  it("cancels a running OMP turn when its keep-alive is answered working:false", async () => {
    const { omp, adapter } = ompAdapter();
    await using session = await runningRoom(adapter);
    const { room, server, events } = session;
    vi.useFakeTimers();
    const prompted = omp.nextPrompt();
    const messageId = await room.say("owner", "Keep working until stopped");
    const { peer, prompt } = await prompted;
    try {
      server.stopped.add(ROOM);
      await vi.advanceTimersByTimeAsync(KEEPALIVE_MS);
      expect(server.requests.entries).toContainEqual({ roomId: ROOM, working: false });
      expect(peer.received.some((frame) => frame.type === "abort"), "the platform reply must cancel the adapter's active prompt").toBe(true);
      peer.finish(prompt.id, "aborted");
      const ended = await events.next((event) => event.type === "turn.completed" && event.messageId === messageId);
      expect(ended).toMatchObject({ outcome: "stopped", cause: "platform" });
      expect(room.posted).toEqual([]);
      expect(room.outcomes(messageId)).toEqual([]);
    } finally {
      peer.finish(prompt.id, "aborted");
      vi.useRealTimers();
    }
  }, 10_000);

  it("cancels a running ACP turn when its keep-alive is answered working:false", async () => {
    const acp = new FakeCursorAgent();
    const adapter = new ACPClientAdapter({ cwd: tmpRoot(), command: ["fake-acp"], connectionFactory: acp.connectionFactory, enableMcpTools: false });
    await using session = await runningRoom(adapter);
    const { room, server, events } = session;
    const prompted = createDeferred();
    const release = createDeferred();
    const result = acp.nextTurn(async () => { prompted.resolve(); await release.promise; });
    vi.useFakeTimers();
    const messageId = await room.say("owner", "Keep working until stopped");
    await prompted.promise;
    try {
      server.stopped.add(ROOM);
      await vi.advanceTimersByTimeAsync(KEEPALIVE_MS);
      expect(server.requests.entries).toContainEqual({ roomId: ROOM, working: false });
      expect(acp.receivedOf("session/cancel"), "the platform reply must cancel the adapter's active prompt").toHaveLength(1);
      release.resolve();
      await result;
      const ended = await events.next((event) => event.type === "turn.completed" && event.messageId === messageId);
      expect(ended).toMatchObject({ outcome: "stopped", cause: "platform" });
      expect(room.posted).toEqual([]);
      expect(room.outcomes(messageId)).toEqual([]);
    } finally {
      release.resolve();
      await result;
      vi.useRealTimers();
    }
  }, 10_000);

  it("reports a host withdrawal during an OMP turn as stopped and posts no provider failure", async () => {
    const { omp, adapter } = ompAdapter();
    await using session = await runningRoom(adapter);
    const { agent, room, events } = session;
    const prompted = omp.nextPrompt();
    const messageId = await room.say("owner", "Keep working until withdrawn");
    await prompted;
    const ended = events.next((event) => (event.type === "turn.completed" || event.type === "turn.failed") && event.messageId === messageId);
    expect(await agent.rooms.deny(ROOM)).toBe(true);
    const terminal = await ended;
    expect(room.posted, "withdrawal is not a provider failure to report").toEqual([]);
    expect(terminal).toMatchObject({ type: "turn.completed", outcome: "stopped", cause: "withdrawal" });
    expect(room.outcomes(messageId)).toEqual([]);
  }, 10_000);

  it.each(["ACP", "OMP"] as const)("%s observes a processed-write 204 before completing a running turn", async (protocol) => {
    const provider = protocolPeer(protocol);
    await using session = await runningRoom(provider.adapter);
    const prompted = provider.next();
    const messageId = await session.room.say("owner", "Do not reply");
    const prompt = await prompted;
    try {
      await prompt.decline();
      session.server.lifecycleStopped.add(ROOM);
      await prompt.finish();
      const ended = await session.events.next((event) => (event.type === "turn.completed" || event.type === "turn.failed") && event.messageId === messageId);
      expect(session.server.writes.entries).toContainEqual({ roomId: ROOM, status: 204 });
      expect(ended).toMatchObject({ type: "turn.completed", outcome: "stopped", cause: "platform" });
      expect(session.room.outcomes(messageId)).toEqual([]);
    } finally { await prompt.finish(); }
  });

  it.each(["ACP", "OMP"] as const)("%s observes a stopped-execution 403 on a reply post", async (protocol) => {
    const provider = protocolPeer(protocol);
    await using session = await runningRoom(provider.adapter);
    const prompted = provider.next();
    const messageId = await session.room.say("owner", "Answer after the gate closes");
    const prompt = await prompted;
    try {
      session.server.postStopped.add(ROOM);
      await prompt.reply("A reply the stopped room refuses");
      await prompt.finish();
      const ended = await session.events.next((event) => (event.type === "turn.completed" || event.type === "turn.failed") && event.messageId === messageId);
      expect(session.server.writes.entries).toContainEqual({ roomId: ROOM, status: 403 });
      expect(ended).toMatchObject({ type: "turn.completed", outcome: "stopped", cause: "platform" });
      expect(session.room.posted).toEqual([]);
      expect(session.room.outcomes(messageId)).toEqual([]);
      await expect(session.tools.entries[0]!.sendMessage("Late callback", ["owner"])).rejects.toMatchObject({ name: "RoomExecutionStoppedError" });
    } finally { await prompt.finish(); }
  });

  it.each(["ACP", "OMP"] as const)("%s keeps running after an activity transport error and later delivers its reply", async (protocol) => {
    const provider = protocolPeer(protocol);
    await using session = await runningRoom(provider.adapter);
    vi.useFakeTimers();
    const prompted = provider.next();
    const messageId = await session.room.say("owner", "Finish despite a lost heartbeat");
    const prompt = await prompted;
    try {
      session.server.transportFailed.add(ROOM);
      await vi.advanceTimersByTimeAsync(KEEPALIVE_MS);
      expect(session.server.transportErrors.entries).toEqual([ROOM]);
      expect(prompt.cancelled()).toBe(false);
      session.server.transportFailed.delete(ROOM);
      await prompt.reply("The answer survived");
      await prompt.finish();
      expect(await session.room.outcome(messageId)).toBe("processed");
      expect(session.room.messages.map((message) => message.content)).toEqual(["The answer survived"]);
    } finally { await prompt.finish(); vi.useRealTimers(); }
  });

  it.each([
    ["ACP", "room"], ["OMP", "room"], ["ACP", "execution"], ["OMP", "execution"],
  ] as const)("%s replays through the %s Play push without a reconnect or revived old tools", async (protocol, scope) => {
    const provider = protocolPeer(protocol);
    await using session = await runningRoom(provider.adapter, tmpRoot());
    await session.platform.room("room-2");
    await session.events.next((event) => event.type === "room.joined" && event.roomId === "room-2");
    await session.nextRequests.next((roomId) => roomId === "room-2");
    vi.useFakeTimers();
    const first = provider.next();
    const messageId = await session.room.say("owner", "Replay this after Play");
    const prompt = await first;
    try {
      session.server.stopped.add(ROOM);
      await vi.advanceTimersByTimeAsync(KEEPALIVE_MS);
      await prompt.finish();
      await session.events.next((event) => event.type === "turn.completed" && event.messageId === messageId);
      const oldTools = session.tools.entries[0]!;
      session.server.stopped.delete(ROOM);
      const replay = provider.next();
      const nextFrom = session.nextRequests.entries.length;
      await session.platform.transport.emit(agentControlTopic(AGENT_ID), "agent.control", {
        type: "agent.control", mode: "play", scope, agent_id: AGENT_ID,
        execution_id: scope === "execution" ? "execution-1" : null,
        room_id: scope === "room" ? ROOM : null,
      });
      await vi.waitFor(() => expect(session.tools.entries).toHaveLength(2), { timeout: 200 });
      const replayed = await replay;
      const swept = session.nextRequests.entries.slice(nextFrom);
      expect(swept).toContain(ROOM);
      if (scope === "room") expect(swept).not.toContain("room-2");
      else expect(swept).toContain("room-2");
      await expect(oldTools.sendMessage("Obsolete reply", ["owner"])).rejects.toMatchObject({ name: "RoomExecutionStoppedError" });
      await replayed.reply("Replayed answer");
      await replayed.finish();
      expect(await session.room.outcome(messageId)).toBe("processed");
      expect(session.room.messages.map((message) => message.content)).toEqual(["Replayed answer"]);
      expect(session.tools.entries).toHaveLength(2);
    } finally { await prompt.finish(); vi.useRealTimers(); }
  });

  it.each(["stop", "interrupt"] as const)("%s pushes cancel neither of two running turns", async (mode) => {
    const provider = protocolPeer("OMP");
    await using session = await runningRoom(provider.adapter);
    const otherRoom = await session.platform.room("room-2");
    const first = provider.next();
    const messageId = await session.room.say("owner", "Keep this turn running");
    const prompt = await first;
    const second = provider.next("room-2");
    const otherId = await otherRoom.say("owner", "Keep this room running too");
    const other = await second;
    try {
      await session.platform.transport.emit(agentControlTopic(AGENT_ID), "agent.control", {
        type: "agent.control", mode, scope: "execution", agent_id: AGENT_ID, execution_id: "execution-1", room_id: null,
      });
      await prompt.reply("First answer");
      await prompt.finish();
      await other.reply("Other answer");
      await other.finish();
      expect(await session.room.outcome(messageId)).toBe("processed");
      expect(await otherRoom.outcome(otherId)).toBe("processed");
      expect(prompt.cancelled()).toBe(false);
      expect(other.cancelled()).toBe(false);
      expect(session.room.messages.map((message) => message.content)).toEqual(["First answer"]);
      expect(otherRoom.messages.map((message) => message.content)).toEqual(["Other answer"]);
    } finally { await prompt.finish(); await other.finish(); }
  });

  it.each(["processing", "activity"] as const)("a %s Stop before startup emits no turn completion", async (signal) => {
    const { adapter } = ompAdapter();
    const cancelled = createDeferred();
    const cancel = adapter.cancelTurn.bind(adapter);
    adapter.cancelTurn = async (roomId) => { const result = await cancel(roomId); cancelled.resolve(); return result; };
    await using session = await runningRoom(adapter);
    if (signal === "processing") session.server.lifecycleStopped.add(ROOM);
    else session.server.stopped.add(ROOM);
    const messageId = await session.room.say("owner", "Do not start while stopped");
    await cancelled.promise;
    await session.agent.stop(1000);
    expect(session.events.entries.filter((event) => "messageId" in event && event.messageId === messageId)).toEqual([]);
    expect(session.room.posted).toEqual([]);
    expect(session.room.outcomes(messageId)).toEqual([]);
    expect(session.tools.entries).toEqual([]);
  });

  it("cancels a held OMP approval and waits for prompt_result followed by session_settled", async () => {
    const lateAnswer = createDeferred<"Approve">();
    const { omp, adapter } = ompAdapter({ resolvePermission: () => lateAnswer.promise });
    await using session = await runningRoom(adapter);
    vi.useFakeTimers();
    const prompted = omp.nextPrompt();
    const messageId = await session.room.say("owner", "Ask before changing files");
    const { peer, prompt } = await prompted;
    try {
      const answer = peer.ask("held-permission", { method: "select", title: "Allow tool: bash", options: ["Approve", "Deny"] });
      await session.events.next((event) => event.type === "approval.requested");
      session.server.stopped.add(ROOM);
      await vi.advanceTimersByTimeAsync(KEEPALIVE_MS);
      expect(await answer).toMatchObject({ cancelled: true });
      await session.events.next((event) => event.type === "approval.resolved" && event.outcome === "cancelled");
      lateAnswer.resolve("Approve");
      const terminal = () => session.events.entries.filter((event) => (event.type === "turn.completed" || event.type === "turn.failed") && event.messageId === messageId);
      expect(terminal()).toEqual([]);
      peer.send({ type: "prompt_result", id: prompt.id, agentInvoked: true, status: "aborted", sessionSettled: true });
      await vi.advanceTimersByTimeAsync(0);
      expect(terminal()).toEqual([]);
      peer.send({ type: "session_settled" });
      expect(await session.events.next((event) => event.type === "turn.completed" && event.messageId === messageId)).toMatchObject({ outcome: "stopped" });
      expect(session.room.posted).toEqual([]);
      expect(session.room.outcomes(messageId)).toEqual([]);
    } finally { lateAnswer.resolve("Approve"); peer.finish(prompt.id, "aborted"); vi.useRealTimers(); }
  });

  it("cancels a held ACP permission and refuses its late selection", async () => {
    const acp = new FakeCursorAgent();
    const lateAnswer = createDeferred<"allow">();
    const resolving = createDeferred();
    const adapter = new ACPClientAdapter({
      cwd: tmpRoot(), command: ["fake-acp"], connectionFactory: acp.connectionFactory, enableMcpTools: false,
      resolvePermission: () => { resolving.resolve(); return lateAnswer.promise; },
    });
    await using session = await runningRoom(adapter);
    const release = createDeferred();
    const result = acp.nextTurn(async (turn) => {
      const answer = await turn.requestPermission({
        toolCall: { toolCallId: "write-stop", title: "Write notes" },
        options: [{ optionId: "allow", name: "Allow", kind: "allow_once" }, { optionId: "deny", name: "Deny", kind: "reject_once" }],
      });
      await release.promise;
      return answer;
    });
    vi.useFakeTimers();
    const messageId = await session.room.say("owner", "Ask before writing");
    try {
      await session.events.next((event) => event.type === "approval.requested");
      await resolving.promise;
      const beforeStop = session.room.posted.length;
      session.server.stopped.add(ROOM);
      await vi.advanceTimersByTimeAsync(KEEPALIVE_MS);
      await session.events.next((event) => event.type === "approval.resolved" && event.outcome === "cancelled");
      lateAnswer.resolve("allow");
      release.resolve();
      expect(await result).toEqual({ outcome: { outcome: "cancelled" } });
      expect(await session.events.next((event) => event.type === "turn.completed" && event.messageId === messageId)).toMatchObject({ outcome: "stopped" });
      expect(session.room.outcomes(messageId)).toEqual([]);
      expect(session.room.posted.slice(beforeStop)).toEqual([]);
    } finally { lateAnswer.resolve("allow"); release.resolve(); vi.useRealTimers(); }
  });

  it("stopping one running room leaves another room able to answer", async () => {
    const provider = protocolPeer("OMP");
    await using session = await runningRoom(provider.adapter);
    const otherRoom = await session.platform.room("room-2");
    vi.useFakeTimers();
    const first = provider.next();
    const messageId = await session.room.say("owner", "Stop this room");
    const stopped = await first;
    const second = provider.next("room-2");
    const otherId = await otherRoom.say("owner", "Keep this room working");
    const running = await second;
    try {
      session.server.stopped.add(ROOM);
      await vi.advanceTimersByTimeAsync(KEEPALIVE_MS);
      expect(stopped.cancelled()).toBe(true);
      expect(running.cancelled()).toBe(false);
      await running.reply("Unaffected room answer");
      await running.finish();
      expect(await otherRoom.outcome(otherId)).toBe("processed");
      await stopped.finish();
      expect(await session.events.next((event) => event.type === "turn.completed" && event.messageId === messageId)).toMatchObject({ outcome: "stopped" });
      expect(session.room.posted).toEqual([]);
      expect(otherRoom.messages.map((message) => message.content)).toEqual(["Unaffected room answer"]);
    } finally { await stopped.finish(); await running.finish(); vi.useRealTimers(); }
  });

  it("fences a reply whose participant lookup completes after the stop", async () => {
    const provider = protocolPeer("OMP");
    await using session = await runningRoom(provider.adapter);
    vi.useFakeTimers();
    const prompted = provider.next();
    const messageId = await session.room.say("owner", "Work until stopped");
    const prompt = await prompted;
    await session.platform.rest.addChatParticipant(ROOM, { participantId: "teammate" });
    const lookup = session.platform.rest.participantHolds.hold(() => true);
    const late = expect(session.tools.entries[0]!.sendMessage("Must not escape", ["teammate"])).rejects.toMatchObject({ name: "RoomExecutionStoppedError" });
    try {
      await lookup.sending;
      session.server.stopped.add(ROOM);
      await vi.advanceTimersByTimeAsync(KEEPALIVE_MS);
      lookup.release();
      await late;
      await prompt.finish();
      await session.events.next((event) => event.type === "turn.completed" && event.messageId === messageId);
      expect(session.room.posted).toEqual([]);
      expect(session.room.outcomes(messageId)).toEqual([]);
    } finally { lookup.release(); await prompt.finish(); vi.useRealTimers(); }
  });

  it("observes an event-post 403 inside an active OMP host tool, rather than swallowing it as telemetry", async () => {
    const { omp, adapter } = ompAdapter();
    await using session = await runningRoom(adapter);
    const prompted = omp.nextPrompt();
    const messageId = await session.room.say("owner", "Report progress until stopped");
    const { peer, prompt } = await prompted;
    try {
      session.server.postFailure.message = "EXECUTION IS STOPPED";
      session.server.postStopped.add(ROOM);
      await peer.callHostTool("stopped-event", "band_send_event", { content: "Progress", message_type: "task" });
      await peer.next((frame) => frame.type === "abort");
      peer.finish(prompt.id, "aborted");
      expect(await session.events.next((event) => event.type === "turn.completed" && event.messageId === messageId)).toMatchObject({ outcome: "stopped", cause: "platform" });
      expect(session.server.writes.entries).toEqual([{ roomId: ROOM, status: 403 }]);
      expect(session.room.posted).toEqual([]);
      expect(session.room.outcomes(messageId)).toEqual([]);
    } finally { peer.finish(prompt.id, "aborted"); }
  });

  it("keeps an unrelated forbidden reply as a delivery failure, not a platform stop", async () => {
    const provider = protocolPeer("OMP");
    await using session = await runningRoom(provider.adapter);
    const prompted = provider.next();
    const messageId = await session.room.say("owner", "Reply without the necessary permission");
    const prompt = await prompted;
    try {
      session.server.postFailure.message = "You cannot post in this room";
      session.server.postStopped.add(ROOM);
      await prompt.reply("Refused for an ordinary permission error");
      await prompt.finish();
      expect(await session.events.next((event) => (event.type === "turn.completed" || event.type === "turn.failed") && event.messageId === messageId)).toMatchObject({ type: "turn.failed" });
      expect(session.room.outcomes(messageId)).toEqual(["failed"]);
      expect(prompt.cancelled()).toBe(false);
    } finally { await prompt.finish(); }
  });

  it("withdraws a handed-back Cursor question without a late failure post or terminal mark", async () => {
    const acp = new FakeCursorAgent();
    acp.room(ROOM).lingersOnStop = true;
    const adapter = new CursorACPAdapter({ cwd: tmpRoot(), connectionFactory: acp.connectionFactory, enableMcpTools: false });
    await using session = await runningRoom(adapter);
    const result = acp.nextTurn((turn) => turn.ask({ questions: [{ id: "mode", options: [{ id: "plan" }] }] }));
    const messageId = await session.room.say("owner", "Wait for my decision");
    await session.room.nextMessage((message) => message.content.includes("/cursor answer"));
    const beforeStop = session.room.posted.length;
    expect(session.room.outcomes(messageId)).toEqual([]);
    expect(await session.agent.rooms.deny(ROOM)).toBe(true);
    expect(await result).toEqual({ outcome: { outcome: "cancelled" } });
    expect(await session.events.next((event) => event.type === "turn.completed" && event.messageId === messageId)).toMatchObject({ outcome: "stopped", cause: "withdrawal" });
    expect(session.room.posted.slice(beforeStop)).toEqual([]);
    expect(session.room.outcomes(messageId)).toEqual([]);
  });
});
