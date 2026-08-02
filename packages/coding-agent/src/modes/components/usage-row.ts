import type { ContextProjection } from "@oh-my-pi/lcm-context";
import type { Usage } from "@oh-my-pi/pi-ai";
import { Container, Spacer, Text } from "@oh-my-pi/pi-tui";
import { formatNumber } from "@oh-my-pi/pi-utils";
import { theme } from "../../modes/theme/theme";
import { LcmProjectionFooterComponent } from "./lcm-projection-footer";

/** Below this the rate is nonsense (cached/instant responses yield absurd tok/s). */
const MIN_DURATION_MS = 100;

/** Local `YYYY-MM-DD HH:mm:ss` stamp for the per-turn usage row. */
function formatUsageTimestamp(ms: number): string {
	const d = new Date(ms);
	const pad = (n: number): string => String(n).padStart(2, "0");
	const date = `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
	const time = `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
	return `${date} ${time}`;
}

/** Format the metrics shared by standalone usage blocks and compact tool groups. */
export function formatUsageRow(usage: Usage, durationMs?: number, ttftMs?: number, timestamp?: number): string {
	const totalInput = usage.input + usage.cacheWrite;
	const parts: string[] = [];
	// Lead with the turn's local wall-clock time (down to the second), log-line style.
	if (timestamp !== undefined && Number.isFinite(timestamp) && timestamp > 0) {
		parts.push(formatUsageTimestamp(timestamp));
	}
	parts.push(`${theme.icon.input} ${formatNumber(totalInput)}`);
	parts.push(`${theme.icon.output} ${formatNumber(usage.output)}`);
	if (usage.cacheRead > 0) {
		parts.push(`${theme.icon.cache} ${formatNumber(usage.cacheRead)}`);
	}
	if (ttftMs && ttftMs > 0) {
		parts.push(`${theme.icon.time} ${(ttftMs / 1000).toFixed(1)}s`);
	}
	if (durationMs && durationMs > MIN_DURATION_MS && usage.output > 0) {
		// TPS over the total request duration — the post-TTFT window undercounts
		// generation time when reasoning tokens are hidden before the first
		// visible byte, inflating the rate.
		const tokPerSec = (usage.output / durationMs) * 1000;
		parts.push(`${theme.icon.throughput} ${tokPerSec.toFixed(1)}/s`);
	}
	return parts.join("  ");
}

export interface ResponseFooterOptions {
	usage?: Usage;
	durationMs?: number;
	ttftMs?: number;
	timestamp?: number;
	lcmProjection?: ContextProjection;
}

export class ResponseFooterComponent extends Container {
	readonly #lcmFooter: LcmProjectionFooterComponent | undefined;

	constructor(options: ResponseFooterOptions) {
		super();
		this.addChild(new Spacer(1));
		if (options.usage) {
			this.addChild(
				new Text(
					theme.fg("dim", formatUsageRow(options.usage, options.durationMs, options.ttftMs, options.timestamp)),
					1,
					0,
				),
			);
		}
		this.#lcmFooter = options.lcmProjection ? new LcmProjectionFooterComponent(options.lcmProjection) : undefined;
		if (this.#lcmFooter) this.addChild(this.#lcmFooter);
	}

	setExpanded(expanded: boolean): void {
		this.#lcmFooter?.setExpanded(expanded);
	}
}

export function createResponseFooterBlock(options: ResponseFooterOptions): ResponseFooterComponent {
	return new ResponseFooterComponent(options);
}

/** Stable public factory for a usage-only response footer. */
export function createUsageRowBlock(usage: Usage, durationMs?: number, ttftMs?: number, timestamp?: number): Container {
	return createResponseFooterBlock({ usage, durationMs, ttftMs, timestamp });
}
