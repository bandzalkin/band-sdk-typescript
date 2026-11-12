import { randomUUID } from "node:crypto";

import type { AdapterToolsProtocol } from "../contracts/protocols";
import { forwardTools } from "../core/overrideTools";

export const TURN_ID_ARG = "band_turn_id";

/** A prompt's capability, never a lookup of whichever turn replaced it. */
export class TurnToolContext {
  public readonly id = randomUUID();
  public readonly tools: AdapterToolsProtocol;

  public constructor(
    public readonly roomId: string,
    tools: AdapterToolsProtocol,
    private readonly isCurrent: () => boolean,
  ) {
    this.tools = forwardTools(() => {
      if (!this.isCurrent()) throw new Error("The tool call's turn is no longer current.");
      return tools;
    });
  }

  public resolve(id: string): AdapterToolsProtocol | undefined {
    return id === this.id && this.isCurrent() ? this.tools : undefined;
  }

  public get prompt(): string {
    return `Band tool turn: ${this.id}\nInclude ${TURN_ID_ARG}: "${this.id}" in every Band tool call for this prompt. Never reuse a previous prompt's turn ID.`;
  }
}
