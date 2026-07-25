import { Buffer } from "node:buffer";
import * as path from "node:path";
import { type Citation, openLcmContext, type SearchHit, type SourceDescription } from "@oh-my-pi/lcm-context";
import { replaceTabs, truncateToWidth } from "@oh-my-pi/pi-tui";
import { escapeXmlText, getLcmDir, normalizePathForComparison, prompt } from "@oh-my-pi/pi-utils";
import recallPrompt from "../prompts/lcm/recall.md" with { type: "text" };
import recallSystemPrompt from "../prompts/lcm/recall-system.md" with { type: "text" };
import type { LcmCompletionRequest } from "../session/session-lcm";
import { TRUNCATE_LENGTHS } from "../tools/render-utils";
import { resolveLcmProjectSelector } from "./project-catalog";

const CITATION_PREFIX = "lcm-citation:v1:";
export const LCM_SEARCH_DEFAULT_LIMIT = 8;
export const LCM_SEARCH_MAX_LIMIT = 20;
export const LCM_RECALL_MAX_QUERY_CHARS = 2_048;
export const LCM_RECALL_MAX_HITS = 6;
export const LCM_RECALL_MAX_CITATIONS = 8;
export const LCM_RECALL_MAX_SLICE_CHARS = 2_400;
export const LCM_RECALL_MAX_SOURCE_CHARS = 12_000;
export const LCM_RECALL_MAX_OUTPUT_TOKENS = 1_200;
const LCM_RECALL_MAX_OUTPUT_CHARS = 12_000;
const LCM_SEARCH_MAX_EXCERPT_CHARS = 1_600;
const LCM_DESCRIBE_MAX_CHARS = 8_000;
const LCM_MAX_CITATIONS_PER_HIT = 8;
const LCM_MAX_ARTIFACT_REFS = 16;
const LCM_MAX_CITATION_TOKEN_CHARS = 4_096;

export interface LcmRetrievalRuntime {
	lcmSearch(query: string, limit?: number): Promise<SearchHit[]>;
	lcmDescribe(citation: Citation): Promise<SourceDescription | null>;
	lcmComplete(request: LcmCompletionRequest): Promise<string>;
}

export interface LcmRecallResult {
	text: string;
	citations: readonly Citation[];
}

export type ArtifactExists = (id: string) => Promise<boolean>;

function isCitation(value: unknown): value is Citation {
	if (!value || typeof value !== "object") return false;
	const citation = value as Record<string, unknown>;
	return (
		typeof citation.projectId === "string" &&
		citation.projectId.length > 0 &&
		typeof citation.sessionId === "string" &&
		citation.sessionId.length > 0 &&
		typeof citation.branchId === "string" &&
		citation.branchId.length > 0 &&
		typeof citation.sourceId === "string" &&
		citation.sourceId.length > 0 &&
		typeof citation.sourceKey === "string" &&
		citation.sourceKey.length > 0 &&
		typeof citation.contentHash === "string" &&
		citation.contentHash.length > 0 &&
		typeof citation.position === "number" &&
		Number.isSafeInteger(citation.position) &&
		citation.position >= 0
	);
}

export function encodeLcmCitation(citation: Citation): string {
	if (!isCitation(citation)) throw new Error("Invalid LCM citation");
	return `${CITATION_PREFIX}${Buffer.from(JSON.stringify(citation)).toString("base64url")}`;
}

export function decodeLcmCitation(value: string): Citation {
	const token = value.trim();
	if (!token.startsWith(CITATION_PREFIX) || token.length > LCM_MAX_CITATION_TOKEN_CHARS) {
		throw new Error("Invalid LCM citation token");
	}
	let parsed: unknown;
	try {
		parsed = JSON.parse(Buffer.from(token.slice(CITATION_PREFIX.length), "base64url").toString("utf8"));
	} catch (error) {
		throw new Error("Invalid LCM citation token", { cause: error });
	}
	if (!isCitation(parsed)) throw new Error("Invalid LCM citation token");
	return { ...parsed };
}

export function normalizeLcmSearchLimit(limit?: number): number {
	if (limit === undefined) return LCM_SEARCH_DEFAULT_LIMIT;
	if (!Number.isSafeInteger(limit) || limit < 1) throw new Error("LCM search limit must be a positive integer");
	return Math.min(limit, LCM_SEARCH_MAX_LIMIT);
}

function boundedDisplayText(value: string, maxChars: number): string {
	let remaining = maxChars;
	const output: string[] = [];
	let truncated = false;
	for (const sourceLine of replaceTabs(value).split("\n")) {
		if (remaining <= 0) {
			truncated = true;
			break;
		}
		const line = truncateToWidth(sourceLine, TRUNCATE_LENGTHS.LINE);
		if (line.length > remaining) {
			output.push(line.slice(0, remaining));
			truncated = true;
			remaining = 0;
			break;
		}
		output.push(line);
		remaining -= line.length + 1;
		if (sourceLine !== line) truncated = true;
	}
	const text = output.join("\n").trim();
	return truncated ? `${text}\n[truncated]`.trim() : text;
}

function boundedPromptText(value: string, maxChars: number): string {
	const text = replaceTabs(value).trim();
	if (text.length <= maxChars) return text;
	const marker = "\n[truncated]";
	return `${text.slice(0, Math.max(0, maxChars - marker.length))}${marker}`.slice(0, maxChars);
}

function citationKey(citation: Citation): string {
	return [
		citation.projectId,
		citation.sessionId,
		citation.branchId,
		citation.sourceId,
		citation.sourceKey,
		citation.contentHash,
		String(citation.position),
	].join("\0");
}

function artifactId(ref: string): string | null {
	return ref.match(/^(?:artifact:\/\/)?(\d+)$/)?.[1] ?? null;
}

function opaqueArtifactRef(ref: string): string {
	return `opaque:${new Bun.CryptoHasher("sha256").update(`omp-lcm-artifact\0${ref}`).digest("hex").slice(0, 16)}`;
}

export async function sanitizeLcmArtifactUris(text: string, exists?: ArtifactExists): Promise<string> {
	const ids = [...new Set([...text.matchAll(/artifact:\/\/(\d+)/g)].map(match => match[1]))];
	if (ids.length === 0) return text;
	const available = new Set<string>();
	if (exists) {
		await Promise.all(
			ids.map(async id => {
				if (await exists(id)) available.add(id);
			}),
		);
	}
	return text.replace(/artifact:\/\/(\d+)/g, (_match, id: string) =>
		available.has(id) ? `artifact://${id}` : `artifact-ref:${id} [unavailable in current session]`,
	);
}

export async function renderLcmSearchHits(
	hits: readonly SearchHit[],
	options: { artifactExists?: ArtifactExists } = {},
): Promise<string> {
	if (hits.length === 0) return "No LCM matches found.";
	const lines: string[] = [`LCM matches (${hits.length}):`];
	for (let index = 0; index < hits.length; index++) {
		const hit = hits[index];
		const safeText = await sanitizeLcmArtifactUris(hit.redactedText, options.artifactExists);
		lines.push("", `${index + 1}. ${hit.kind} (rank ${hit.rank.toFixed(4)})`);
		lines.push(boundedDisplayText(safeText, LCM_SEARCH_MAX_EXCERPT_CHARS));
		const citations = hit.citations.slice(0, LCM_MAX_CITATIONS_PER_HIT);
		for (const citation of citations) lines.push(`   Citation: ${encodeLcmCitation(citation)}`);
		if (hit.citations.length > citations.length) {
			lines.push(`   [${hit.citations.length - citations.length} additional citations omitted]`);
		}
	}
	return lines.join("\n");
}

export async function renderLcmSourceDescription(
	description: SourceDescription,
	options: { artifactExists?: ArtifactExists } = {},
): Promise<string> {
	const safeText = await sanitizeLcmArtifactUris(description.redactedText, options.artifactExists);
	const lines = [
		`Citation: ${encodeLcmCitation(description)}`,
		`Kind: ${replaceTabs(description.kind)}`,
		`Timestamp: ${new Date(description.timestamp).toISOString()}`,
		`Position: ${description.position}`,
		"",
		boundedDisplayText(safeText, LCM_DESCRIBE_MAX_CHARS),
	];
	if (description.artifactRefs.length > 0) {
		lines.push("", "Artifacts:");
		for (const ref of description.artifactRefs.slice(0, LCM_MAX_ARTIFACT_REFS)) {
			const id = artifactId(ref);
			const available = id && options.artifactExists ? await options.artifactExists(id) : false;
			lines.push(available ? `- artifact://${id}` : `- ${opaqueArtifactRef(ref)} [unavailable in current session]`);
		}
		if (description.artifactRefs.length > LCM_MAX_ARTIFACT_REFS) {
			lines.push(`[${description.artifactRefs.length - LCM_MAX_ARTIFACT_REFS} additional artifact refs omitted]`);
		}
	}
	return lines.join("\n");
}

interface RecallSlice {
	label: number;
	text: string;
	citation: Citation;
	degraded: boolean;
}

async function selectRecallSlices(runtime: LcmRetrievalRuntime, hits: readonly SearchHit[]): Promise<RecallSlice[]> {
	const selected: Array<{ citation: Citation; fallback: string }> = [];
	const seen = new Set<string>();
	for (const hit of hits.slice(0, LCM_RECALL_MAX_HITS)) {
		for (const citation of hit.citations) {
			const key = citationKey(citation);
			if (seen.has(key)) continue;
			seen.add(key);
			selected.push({ citation, fallback: hit.redactedText });
			if (selected.length >= LCM_RECALL_MAX_CITATIONS) break;
		}
		if (selected.length >= LCM_RECALL_MAX_CITATIONS) break;
	}
	if (selected.length === 0) return [];

	const descriptions = await Promise.all(selected.map(({ citation }) => runtime.lcmDescribe(citation)));
	const slices: RecallSlice[] = [];
	let remaining = LCM_RECALL_MAX_SOURCE_CHARS;
	for (let index = 0; index < selected.length && remaining > 0; index++) {
		const description = descriptions[index];
		const source = description?.redactedText ?? selected[index].fallback;
		const safeSource = await sanitizeLcmArtifactUris(source);
		const text = boundedPromptText(escapeXmlText(safeSource), Math.min(LCM_RECALL_MAX_SLICE_CHARS, remaining));
		if (!text) continue;
		slices.push({
			label: slices.length + 1,
			text,
			citation: selected[index].citation,
			degraded: description === null,
		});
		remaining -= text.length;
	}
	return slices;
}

export async function runLcmRecall(
	runtime: LcmRetrievalRuntime,
	query: string,
	signal?: AbortSignal,
): Promise<LcmRecallResult | null> {
	const normalizedQuery = replaceTabs(query).trim().slice(0, LCM_RECALL_MAX_QUERY_CHARS);
	if (!normalizedQuery) throw new Error("LCM recall query is required");
	signal?.throwIfAborted();
	const hits = await runtime.lcmSearch(normalizedQuery, LCM_RECALL_MAX_HITS);
	const slices = await selectRecallSlices(runtime, hits);
	if (slices.length === 0) return null;
	signal?.throwIfAborted();
	const completion = await runtime.lcmComplete({
		systemPrompt: prompt.render(recallSystemPrompt),
		prompt: prompt.render(recallPrompt, {
			query: boundedPromptText(escapeXmlText(normalizedQuery), LCM_RECALL_MAX_QUERY_CHARS),
			sources: slices.map(({ label, text, degraded }) => ({
				label,
				text,
				status: degraded ? "description unavailable; bounded search excerpt used" : "verified source description",
			})),
		}),
		maxOutputTokens: LCM_RECALL_MAX_OUTPUT_TOKENS,
		oneshotKind: "lcm_recall",
		signal,
	});
	const answer = boundedDisplayText(await sanitizeLcmArtifactUris(completion), LCM_RECALL_MAX_OUTPUT_CHARS);
	const citations = slices.map(slice => slice.citation);
	const legend = slices
		.map(
			(slice, index) =>
				`[${index + 1}] ${encodeLcmCitation(slice.citation)}${slice.degraded ? " [description unavailable; search excerpt used]" : ""}`,
		)
		.join("\n");
	return { text: `${answer}\n\nSources:\n${legend}`, citations };
}

/** Search one catalog-authorized project store without exposing its path or SQLite handle to a tool. */
export async function searchKnownLcmProject(
	projectId: string,
	query: string,
	limit: number,
	agentDir?: string,
): Promise<SearchHit[]> {
	const project = await resolveLcmProjectSelector(projectId, agentDir);
	const expectedStorePath = path.join(getLcmDir(agentDir), "projects", project.projectId, "context.sqlite");
	if (normalizePathForComparison(project.storePath) !== normalizePathForComparison(expectedStorePath)) {
		throw new Error(`LCM catalog store path is invalid for project ${project.projectId}`);
	}
	if (!(await Bun.file(project.storePath).exists())) {
		throw new Error(`LCM derived store is unavailable for project ${project.projectId}`);
	}
	const context = await openLcmContext({ dbPath: project.storePath, recoverCorrupt: false });
	try {
		return context.searchProject({ projectId: project.projectId, query, limit });
	} finally {
		context.close();
	}
}
