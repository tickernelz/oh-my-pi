import type { ContextProjection } from "@oh-my-pi/lcm-context";
import { type Component, Ellipsis, truncateToWidth } from "@oh-my-pi/pi-tui";
import { formatNumber } from "@oh-my-pi/pi-utils";
import { theme } from "../../modes/theme/theme";

const MARKER_RULE_WIDTH = 10;

type LevelCount = { level: number; count: number };

function selectedLevels(projection: ContextProjection): LevelCount[] {
	const levels: LevelCount[] = [];
	for (const [rawLevel, rawCount] of Object.entries(projection.selectedLevelCounts)) {
		const level = Number(rawLevel);
		const count = Math.trunc(rawCount);
		if (Number.isInteger(level) && level >= 0 && Number.isFinite(count) && count > 0) {
			levels.push({ level, count });
		}
	}
	return levels.sort((a, b) => b.level - a.level);
}

function plural(count: number, singular: string): string {
	const noun = count === 1 ? singular : singular.endsWith("y") ? `${singular.slice(0, -1)}ies` : `${singular}s`;
	return `${count} ${noun}`;
}

/**
 * Stable, presentation-level identity for a meaningful fitted DAG projection.
 * Storage revisions are intentionally excluded: background convergence that
 * does not change the selected projection must not create another marker.
 */
export function lcmProjectionFingerprint(projection: ContextProjection): string | undefined {
	const levels = selectedLevels(projection);
	if (
		!projection.ready ||
		projection.pendingJobs > 0 ||
		projection.historical.length === 0 ||
		projection.uncoveredSourceIds.length > 0 ||
		levels.length === 0
	) {
		return undefined;
	}
	return JSON.stringify([
		projection.historical.map(item => [item.level, item.summaryHandle]),
		projection.freshTailSourceIds,
		projection.sourceTokens,
		projection.estimatedTokens,
		projection.coveredSourceCount,
		projection.freshSourceCount,
		levels.map(({ level, count }) => [level, count]),
	]);
}

/**
 * Renderer-only evidence that the response used a fitted LCM projection. The
 * component is never reconstructed from session messages, so replay cannot
 * fabricate a historical marker. Ctrl+O expands it once; later global collapse
 * toggles leave the evidence visible.
 */
export class LcmProjectionMarkerComponent implements Component {
	#expanded = false;
	#cache?: { width: number; expanded: boolean; lines: string[] };

	constructor(
		private readonly projection: ContextProjection,
		private readonly padded = true,
	) {}

	setExpanded(expanded: boolean): void {
		if (!expanded || this.#expanded) return;
		this.#expanded = true;
		this.#cache = undefined;
	}

	invalidate(): void {
		this.#cache = undefined;
	}

	render(width: number): readonly string[] {
		width = Math.max(1, width);
		if (this.#cache?.width === width && this.#cache.expanded === this.#expanded) return this.#cache.lines;

		const body = [this.#divider(width), ...(this.#expanded ? this.#detailLines(width) : [])];
		const lines = this.padded ? ["", ...body, ""] : body;
		this.#cache = { width, expanded: this.#expanded, lines };
		return lines;
	}

	#divider(width: number): string {
		const levels = selectedLevels(this.projection);
		const depth = levels.length === 0 ? 0 : levels[0]!.level + 1;
		const summaries = levels.reduce((total, item) => total + item.count, 0);
		const sep = theme.sep.dot.trim();
		const coverage = `${plural(summaries, "summary")} / ${this.projection.coveredSourceCount} covered`;
		const hint = this.#expanded ? "" : ` ${sep} ctrl+o`;
		const full = `LCM context ${sep} DAG depth ${depth} ${sep} ${coverage}${hint}`;
		const compact = `LCM context ${sep} depth ${depth} ${sep} ${coverage}`;
		const concise = `LCM context ${sep} depth ${depth} ${sep} ${summaries}/${this.projection.coveredSourceCount}`;
		const label =
			Bun.stringWidth(full, { countAnsiEscapeCodes: false }) <= width
				? full
				: Bun.stringWidth(compact, { countAnsiEscapeCodes: false }) <= width
					? compact
					: Bun.stringWidth(concise, { countAnsiEscapeCodes: false }) <= width
						? concise
						: truncateToWidth("LCM context", width, Ellipsis.Ascii);
		const labelWidth = Bun.stringWidth(label, { countAnsiEscapeCodes: false });
		const ruleWidth = Math.min(MARKER_RULE_WIDTH, width - labelWidth - 1);
		if (ruleWidth < 1) return theme.fg("muted", label);
		return `${theme.fg("dim", theme.tree.horizontal.repeat(ruleWidth))} ${theme.fg("muted", label)}`;
	}

	#detailLines(width: number): string[] {
		const levels = selectedLevels(this.projection);
		const details = levels.map(({ level, count }) => `Depth ${level + 1}: ${plural(count, "summary")}`);
		details.push(`Fresh tail: ${plural(this.projection.freshSourceCount, "source")}`);
		details.push(
			`Estimated tokens: ${formatNumber(this.projection.sourceTokens)} -> ${formatNumber(this.projection.estimatedTokens)}`,
		);
		details.push("Journal unchanged");

		return details.map((text, index) => {
			const connector = index === details.length - 1 ? theme.tree.last : theme.tree.branch;
			return truncateToWidth(`  ${theme.fg("dim", connector)} ${theme.fg("muted", text)}`, width, Ellipsis.Ascii);
		});
	}
}
