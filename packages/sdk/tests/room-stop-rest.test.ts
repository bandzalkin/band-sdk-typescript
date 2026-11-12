import { BandClient } from "@band-ai/rest-client";
import { describe, expect, it } from "vitest";

import { FernRestAdapter } from "../src/client/rest/FernRestAdapter";
import { RoomExecutionStoppedError } from "../src/core/errors";

const writes = {
  processing: (rest: FernRestAdapter) => rest.markMessageProcessing("room-1", "message-1"),
  processed: (rest: FernRestAdapter) => rest.markMessageProcessed("room-1", "message-1"),
  failed: (rest: FernRestAdapter) => rest.markMessageFailed("room-1", "message-1", "Provider failed"),
};

function noContentRest() {
  return new FernRestAdapter(new BandClient({
    apiKey: "stop-test-key", baseUrl: "http://band.test",
    fetch: async () => new Response(null, { status: 204 }),
  }));
}

describe("execution HTTP 204", () => {
  it.each(Object.entries(writes))("%s reports a stopped room rather than an accepted write", async (_name, write) => {
    await expect(write(noContentRest())).rejects.toBeInstanceOf(RoomExecutionStoppedError);
  });

  it("next's 204 still means an empty backlog, not a stopped execution", async () => {
    expect(await noContentRest().getNextMessage({ chatId: "room-1" })).toBeNull();
  });
});
