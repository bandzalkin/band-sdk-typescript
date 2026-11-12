import { AsyncLocalStorage } from "node:async_hooks";
import type { AdapterToolsProtocol } from "../contracts/protocols";
import { hasStatusCode, RoomExecutionStoppedError } from "./errors";
import type { Logger } from "./logger";
import { overrideTools } from "./overrideTools";

type StopCause = "platform" | "withdrawal";
const currentTurn = new AsyncLocalStorage<RoomTurn>();

/** One room's control revision. Stopping invalidates existing turns, not future turns. */
export class RoomTurnControl {
  private revision = 0;
  private readonly turns = new Set<RoomTurn>();

  public constructor(public readonly roomId: string, private readonly logger: Logger) {}

  public open(cancel: () => Promise<unknown>): RoomTurn {
    const turn = new RoomTurn(this, this.revision, cancel);
    this.turns.add(turn);
    return turn;
  }

  public isCurrent(revision: number): boolean {
    return revision === this.revision;
  }

  public owns(turn: RoomTurn): boolean {
    return this.isCurrent(turn.revision) && this.turns.has(turn);
  }

  public stop(cause: StopCause, source?: RoomTurn): void {
    if (source && !this.owns(source)) return;
    this.revision++;
    for (const turn of this.turns) turn.stop(cause, this.logger);
  }

  public close(turn: RoomTurn): void {
    this.turns.delete(turn);
  }
}

export class RoomTurn {
  public stopCause?: StopCause;
  private cancellation = Promise.resolve();

  public constructor(
    private readonly control: RoomTurnControl,
    public readonly revision: number,
    private readonly cancel: () => Promise<unknown>,
  ) {}

  public get roomId(): string { return this.control.roomId; }

  public assertCurrent(): void {
    if (!this.control.isCurrent(this.revision)) throw new RoomExecutionStoppedError(this.roomId);
  }

  public observeStop(): void { this.control.stop("platform", this); }

  public stop(cause: StopCause, logger: Logger): void {
    if (this.stopCause) return;
    this.stopCause = cause;
    // Invalidate synchronously, before cancellation can run provider callbacks.
    this.cancellation = Promise.resolve().then(this.cancel).then(() => undefined, (error: unknown) => {
      logger.warn("Stopped turn cancellation failed", { roomId: this.roomId, error });
    });
  }

  public async close(): Promise<void> {
    this.control.close(this);
    await this.cancellation;
  }

  public run<T>(work: () => T): T { return currentTurn.run(this, work); }

  /** Native callbacks need an explicit binding: EventEmitter does not retain the caller's ALS scope. */
  public bind(tools: AdapterToolsProtocol): AdapterToolsProtocol {
    const run = <T>(work: () => Promise<T>): Promise<T> => this.run(async () => {
      this.assertCurrent();
      return await work();
    });
    return overrideTools(tools, {
      sendMessage: (...args) => run(() => tools.sendMessage(...args)),
      sendNotice: (...args) => run(() => tools.sendNotice(...args)),
      sendEvent: (...args) => run(() => tools.sendEvent(...args)),
      sendFailure: (...args) => run(() => tools.sendFailure(...args)),
      executeToolCall: (...args) => run(() => tools.executeToolCall(...args)),
    });
  }
}

/** Check at the network boundary, after any asynchronous preparation such as mention lookup. */
export async function roomWrite<T>(roomId: string, write: () => Promise<T>, post = false): Promise<T> {
  const active = currentTurn.getStore();
  const turn = active?.roomId === roomId ? active : undefined;
  turn?.assertCurrent();
  try {
    const result = await write();
    turn?.assertCurrent();
    return result;
  } catch (error) {
    if (post && isStoppedPost(error)) {
      turn?.observeStop();
      throw new RoomExecutionStoppedError(roomId);
    }
    if (error instanceof RoomExecutionStoppedError && error.roomId === roomId) turn?.observeStop();
    throw error;
  }
}

/** Other forbidden posts and transport errors are not execution controls. */
function isStoppedPost(error: unknown): boolean {
  if (!hasStatusCode(error, 403) || typeof error !== "object" || error === null || !("body" in error)) return false;
  const body: unknown = error.body;
  if (typeof body === "string") return body.toLowerCase().includes("execution is stopped");
  if (typeof body !== "object" || body === null || !("error" in body)) return false;
  const detail: unknown = body.error;
  return typeof detail === "object" && detail !== null && "message" in detail
    && typeof detail.message === "string" && detail.message.toLowerCase().includes("execution is stopped");
}
