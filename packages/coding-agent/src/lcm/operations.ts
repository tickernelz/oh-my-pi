import { Buffer } from "node:buffer";
import * as path from "node:path";
import {
	type Citation,
	type ContextScope,
	type FileDescription,
	type FileReference,
	type LcmFileMetadata,
	openLcmContext,
	type SearchHit,
	type SourceDescription,
	type SummaryDescription,
	type SummaryExpansion,
	type SummaryReference,
} from "@oh-my-pi/lcm-context";
import { replaceTabs, truncateToWidth } from "@oh-my-pi/pi-tui";
import { escapeXmlText, getLcmDir, normalizePathForComparison, prompt } from "@oh-my-pi/pi-utils";
import recallPrompt from "../prompts/lcm/recall.md" with { type: "text" };
import recallSystemPrompt from "../prompts/lcm/recall-system.md" with { type: "text" };
import type { LcmCompletionRequest } from "../session/session-lcm";
import { shortenPath, TRUNCATE_LENGTHS } from "../tools/render-utils";
import { resolveLcmProjectSelector } from "./project-catalog";

const HANDLE_PREFIX = "lcm-handle:v1:";
const LCM_MAX_HANDLE_CHARS = 4_096;
export const LCM_SEARCH_DEFAULT_LIMIT = 8;
export const LCM_SEARCH_MAX_LIMIT = 20;
export const LCM_SEARCH_MAX_OFFSET = 1_000;
export const LCM_EXPAND_DEFAULT_DEPTH = 1;
export const LCM_EXPAND_MAX_DEPTH = 4;
export const LCM_EXPAND_DEFAULT_LIMIT = 20;
export const LCM_EXPAND_MAX_LIMIT = 50;
export const LCM_EXPAND_DEFAULT_TOKENS = 4_000;
export const LCM_EXPAND_MIN_TOKENS = 1_024;
export const LCM_EXPAND_MAX_TOKENS = 8_000;
export const LCM_RECALL_MAX_QUERY_CHARS = 2_048;
export const LCM_RECALL_MAX_HITS = 6;
export const LCM_RECALL_MAX_CITATIONS = 8;
export const LCM_RECALL_MAX_SLICE_CHARS = 2_400;
export const LCM_RECALL_MAX_SOURCE_CHARS = 12_000;
export const LCM_RECALL_MAX_OUTPUT_TOKENS = 1_200;
const LCM_RECALL_MAX_OUTPUT_CHARS = 12_000;
const LCM_SEARCH_MAX_EXCERPT_CHARS = 1_600;
const LCM_DESCRIBE_MAX_CHARS = 8_000;
const LCM_EXPAND_ITEM_MAX_CHARS = 4_000;
const LCM_MAX_HANDLES_PER_HIT = 8;
const LCM_MAX_ARTIFACT_REFS = 16;
const LCM_MAX_FILES = 16;

export type LcmHandle =
	| { kind: "source"; citation: Citation }
	| { kind: "summary"; reference: SummaryReference }
	| { kind: "file"; reference: FileReference };

export interface LcmSearchOptions {
	limit?: number;
	offset?: number;
	summary?: SummaryReference;
}

export type LcmDescription =
	| { kind: "source"; value: SourceDescription }
	| { kind: "summary"; value: SummaryDescription }
	| { kind: "file"; value: FileDescription & { available: boolean } };

export type LcmResolvedExpansionItem =
	| Extract<SummaryExpansion["items"][number], { kind: "summary" }>
	| {
			kind: "source";
			depth: number;
			citation: Citation;
			tokenCount: number;
			files: readonly LcmFileMetadata[];
			available: boolean;
			redactedText?: string;
	  };

export interface LcmResolvedExpansion extends Omit<SummaryExpansion, "items"> {
	items: readonly LcmResolvedExpansionItem[];
}

export interface LcmExpandOptions {
	reference: SummaryReference;
	depth: number;
	offset: number;
	limit: number;
	maxTokens: number;
}

export interface LcmRetrievalRuntime {
	lcmSearch(query: string, options?: LcmSearchOptions): Promise<SearchHit[]>;
	lcmDescribe(handle: LcmHandle): Promise<LcmDescription | null>;
	lcmExpand(options: LcmExpandOptions): Promise<LcmResolvedExpansion | null>;
	lcmComplete(request: LcmCompletionRequest): Promise<string>;
}

export interface LcmRecallResult {
	text: string;
	citations: readonly Citation[];
}

export type ArtifactExists = (id: string) => Promise<boolean>;

function isScope(value: unknown): value is ContextScope {
	if (!value || typeof value !== "object") return false;
	const scope = value as Record<string, unknown>;
	return (
		typeof scope.projectId === "string" &&
		scope.projectId.length > 0 &&
		typeof scope.sessionId === "string" &&
		scope.sessionId.length > 0 &&
		typeof scope.branchId === "string" &&
		scope.branchId.length > 0
	);
}

function isCitation(value: unknown): value is Citation {
	if (!isScope(value)) return false;
	const citation = value as unknown as Record<string, unknown>;
	return (
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

function isHandle(value: unknown): value is LcmHandle {
	if (!value || typeof value !== "object") return false;
	const handle = value as Record<string, unknown>;
	if (handle.kind === "source") return isCitation(handle.citation);
	if (!isScope(handle.reference)) return false;
	const reference = handle.reference as unknown as Record<string, unknown>;
	if (handle.kind === "summary") {
		return typeof reference.summaryHandle === "string" && reference.summaryHandle.length > 0;
	}
	return handle.kind === "file" && typeof reference.fileId === "string" && reference.fileId.length > 0;
}

function canonicalLcmHandle(handle: LcmHandle): LcmHandle {
	if (handle.kind === "source") {
		const { projectId, sessionId, branchId, sourceId, sourceKey, contentHash, position } = handle.citation;
		return {
			kind: "source",
			citation: { projectId, sessionId, branchId, sourceId, sourceKey, contentHash, position },
		};
	}
	if (handle.kind === "summary") {
		const { projectId, sessionId, branchId, summaryHandle } = handle.reference;
		return { kind: "summary", reference: { projectId, sessionId, branchId, summaryHandle } };
	}
	const { projectId, sessionId, branchId, fileId } = handle.reference;
	return { kind: "file", reference: { projectId, sessionId, branchId, fileId } };
}

export function encodeLcmHandle(handle: LcmHandle): string {
	if (!isHandle(handle)) throw new Error("Invalid LCM handle");
	const token = `${HANDLE_PREFIX}${Buffer.from(JSON.stringify(canonicalLcmHandle(handle))).toString("base64url")}`;
	if (token.length > LCM_MAX_HANDLE_CHARS) {
		throw new Error(`LCM handle token exceeds ${LCM_MAX_HANDLE_CHARS} characters`);
	}
	return token;
}

function renderLcmHandle(handle: LcmHandle): string {
	try {
		return encodeLcmHandle(handle);
	} catch {
		return "[handle unavailable]";
	}
}

export function decodeLcmHandle(value: string): LcmHandle {
	const token = value.trim();
	if (!token.startsWith(HANDLE_PREFIX) || token.length > LCM_MAX_HANDLE_CHARS) {
		throw new Error("Invalid LCM handle token");
	}
	let parsed: unknown;
	try {
		parsed = JSON.parse(Buffer.from(token.slice(HANDLE_PREFIX.length), "base64url").toString("utf8"));
	} catch (error) {
		throw new Error("Invalid LCM handle token", { cause: error });
	}
	if (!isHandle(parsed)) throw new Error("Invalid LCM handle token");
	return canonicalLcmHandle(parsed);
}

function normalizePositive(value: number | undefined, fallback: number, maximum: number, label: string): number {
	if (value === undefined) return fallback;
	if (!Number.isSafeInteger(value) || value < 1) throw new Error(`${label} must be a positive integer`);
	return Math.min(value, maximum);
}

function normalizeExpandTokenLimit(value?: number): number {
	const normalized = normalizePositive(
		value,
		LCM_EXPAND_DEFAULT_TOKENS,
		LCM_EXPAND_MAX_TOKENS,
		"LCM expand token limit",
	);
	if (normalized < LCM_EXPAND_MIN_TOKENS) {
		throw new Error(`LCM expand token limit must be at least ${LCM_EXPAND_MIN_TOKENS}`);
	}
	return normalized;
}

export function normalizeLcmSearchLimit(limit?: number): number {
	return normalizePositive(limit, LCM_SEARCH_DEFAULT_LIMIT, LCM_SEARCH_MAX_LIMIT, "LCM search limit");
}

export function normalizeLcmOffset(offset?: number): number {
	if (offset === undefined) return 0;
	if (!Number.isSafeInteger(offset) || offset < 0) throw new Error("LCM offset must be a non-negative integer");
	return Math.min(offset, LCM_SEARCH_MAX_OFFSET);
}

export function normalizeLcmExpandOptions(options: {
	depth?: number;
	offset?: number;
	limit?: number;
	maxTokens?: number;
}): Omit<LcmExpandOptions, "reference"> {
	return {
		depth: normalizePositive(options.depth, LCM_EXPAND_DEFAULT_DEPTH, LCM_EXPAND_MAX_DEPTH, "LCM expand depth"),
		offset: normalizeLcmOffset(options.offset),
		limit: normalizePositive(options.limit, LCM_EXPAND_DEFAULT_LIMIT, LCM_EXPAND_MAX_LIMIT, "LCM expand limit"),
		maxTokens: normalizeExpandTokenLimit(options.maxTokens),
	};
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

function hardBound(value: string, maxChars: number): string {
	if (value.length <= maxChars) return value;
	const marker = "\n[token cap reached]";
	return `${value.slice(0, Math.max(0, maxChars - marker.length))}${marker}`.slice(0, maxChars);
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

function scopeKey(scope: ContextScope): string {
	return [scope.projectId, scope.sessionId, scope.branchId].join("\0");
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

function fileLines(files: readonly LcmFileMetadata[], scope: ContextScope): string[] {
	const lines: string[] = [];
	for (const file of files.slice(0, LCM_MAX_FILES)) {
		lines.push(
			`- ${renderLcmHandle({ kind: "file", reference: { ...scope, fileId: file.fileId } })} · ${truncateToWidth(replaceTabs(shortenPath(file.path)), TRUNCATE_LENGTHS.CONTENT)} · ${truncateToWidth(replaceTabs(file.fileType), TRUNCATE_LENGTHS.CONTENT)} · ${file.byteSize} bytes · ~${file.tokenCount} tokens`,
		);
	}
	if (files.length > LCM_MAX_FILES) lines.push(`[${files.length - LCM_MAX_FILES} additional file refs omitted]`);
	return lines;
}

export async function renderLcmSearchHits(
	hits: readonly SearchHit[],
	options: { artifactExists?: ArtifactExists; offset?: number; limit?: number; includeSummaryHandles?: boolean } = {},
): Promise<string> {
	if (hits.length === 0) return "No LCM matches found.";
	const offset = options.offset ?? 0;
	const lines: string[] = [`LCM matches (${hits.length}; offset ${offset}):`];
	for (let index = 0; index < hits.length; index++) {
		const hit = hits[index];
		const safeText = await sanitizeLcmArtifactUris(hit.redactedText, options.artifactExists);
		lines.push("", `${offset + index + 1}. ${hit.kind} (rank ${hit.rank.toFixed(4)})`);
		lines.push(boundedDisplayText(safeText, LCM_SEARCH_MAX_EXCERPT_CHARS));
		const citations = hit.citations.slice(0, LCM_MAX_HANDLES_PER_HIT);
		if (options.includeSummaryHandles !== false && hit.kind === "summary" && hit.summaryHandle) {
			const seenScopes = new Set<string>();
			for (const citation of citations) {
				const reference: SummaryReference = {
					projectId: citation.projectId,
					sessionId: citation.sessionId,
					branchId: citation.branchId,
					summaryHandle: hit.summaryHandle,
				};
				const key = scopeKey(reference);
				if (seenScopes.has(key)) continue;
				seenScopes.add(key);
				lines.push(`   Summary: ${renderLcmHandle({ kind: "summary", reference })}`);
			}
		}
		for (const citation of citations) {
			lines.push(`   Source: ${renderLcmHandle({ kind: "source", citation })}`);
		}
		if (hit.citations.length > citations.length) {
			lines.push(`   [${hit.citations.length - citations.length} additional source handles omitted]`);
		}
	}
	if (options.limit !== undefined && hits.length >= options.limit) {
		lines.push("", `Next offset: ${offset + hits.length}`);
	}
	return lines.join("\n");
}

async function renderSourceDescription(
	description: SourceDescription,
	options: { artifactExists?: ArtifactExists },
): Promise<string> {
	const safeText = await sanitizeLcmArtifactUris(description.redactedText, options.artifactExists);
	const scope: ContextScope = {
		projectId: description.projectId,
		sessionId: description.sessionId,
		branchId: description.branchId,
	};
	const lines = [
		`Handle: ${renderLcmHandle({ kind: "source", citation: description })}`,
		`Kind: ${replaceTabs(description.kind)}`,
		`Timestamp: ${new Date(description.timestamp).toISOString()}`,
		`Position: ${description.position}`,
		"",
		boundedDisplayText(safeText, LCM_DESCRIBE_MAX_CHARS),
	];
	if (description.files.length > 0) lines.push("", "Files:", ...fileLines(description.files, scope));
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

async function renderSummaryDescription(
	description: SummaryDescription,
	options: { artifactExists?: ArtifactExists },
): Promise<string> {
	const safeText = await sanitizeLcmArtifactUris(description.redactedText, options.artifactExists);
	const lines = [
		`Handle: ${renderLcmHandle({ kind: "summary", reference: description })}`,
		`Kind: summary:${description.kind}`,
		`Level: ${description.level}`,
		`Tokens: ${description.tokenCount}`,
		`Sources: ${description.sourceCount}`,
		`Children: ${description.childCount}`,
	];
	if (description.parentHandles.length > 0) {
		lines.push("Parents:");
		for (const summaryHandle of description.parentHandles) {
			lines.push(`- ${renderLcmHandle({ kind: "summary", reference: { ...description, summaryHandle } })}`);
		}
	}
	if (description.files.length > 0) lines.push("Files:", ...fileLines(description.files, description));
	lines.push("", boundedDisplayText(safeText, LCM_DESCRIBE_MAX_CHARS));
	return lines.join("\n");
}

function renderFileDescription(description: FileDescription & { available: boolean }): string {
	const lines = [
		`Handle: ${renderLcmHandle({ kind: "file", reference: description })}`,
		`Path: ${truncateToWidth(replaceTabs(shortenPath(description.path)), TRUNCATE_LENGTHS.CONTENT)}`,
		`Type: ${truncateToWidth(replaceTabs(description.fileType), TRUNCATE_LENGTHS.CONTENT)}`,
		`Hash: ${description.contentHash}`,
		`Size: ${description.byteSize} bytes`,
		`Tokens: ${description.tokenCount}`,
		`Availability: ${description.available ? "available" : "unavailable"}`,
		`Sources: ${description.sources.length}`,
	];
	for (const source of description.sources.slice(0, LCM_MAX_HANDLES_PER_HIT)) {
		lines.push(`- ${renderLcmHandle({ kind: "source", citation: source })}`);
	}
	lines.push("", "Exploration summary:", boundedDisplayText(description.explorationSummary, LCM_DESCRIBE_MAX_CHARS));
	return lines.join("\n");
}

export async function renderLcmDescription(
	description: LcmDescription,
	options: { artifactExists?: ArtifactExists } = {},
): Promise<string> {
	switch (description.kind) {
		case "source":
			return renderSourceDescription(description.value, options);
		case "summary":
			return renderSummaryDescription(description.value, options);
		case "file":
			return renderFileDescription(description.value);
	}
}

export interface RenderedLcmExpansion {
	text: string;
	renderedItems: number;
	nextOffset?: number;
}

export async function renderLcmExpansionPage(
	expansion: LcmResolvedExpansion,
	maxTokens: number,
	options: { artifactExists?: ArtifactExists } = {},
): Promise<RenderedLcmExpansion> {
	const maxChars = normalizeExpandTokenLimit(maxTokens) * 4;
	const rootHandle = renderLcmHandle({ kind: "summary", reference: expansion.root });
	let expandedLine = `Expanded: ${rootHandle}`;
	const renderHeader = (renderedCount: number): string =>
		`${expandedLine}\nItems: ${renderedCount}/${expansion.totalItems} · offset ${expansion.offset} · ~${expansion.estimatedTokens} tokens`;
	const renderFooter = (nextOffset: number | undefined, bounded: boolean): string => {
		const lines: string[] = [];
		if (nextOffset !== undefined) lines.push("", `Next offset: ${nextOffset}`);
		if (bounded) lines.push("[expansion bounded by item or token limit]");
		return lines.length > 0 ? `\n${lines.join("\n")}` : "";
	};

	const initialNextOffset = expansion.items.length > 0 ? expansion.offset : expansion.nextOffset;
	if (
		renderHeader(0).length +
			renderFooter(initialNextOffset, expansion.truncated || expansion.items.length > 0).length >
		maxChars
	) {
		expandedLine = "Expanded: [handle omitted: output budget]";
	}

	const renderedBlocks: string[] = [];
	let renderedChars = 0;
	let renderedCount = 0;
	let clipped = false;
	for (let index = 0; index < expansion.items.length; index++) {
		const item = expansion.items[index];
		const label = expansion.offset + index + 1;
		const heading =
			item.kind === "summary"
				? `${label}. Summary L${item.summary.level}: ${renderLcmHandle({ kind: "summary", reference: item.summary })}`
				: `${label}. Source: ${renderLcmHandle({ kind: "source", citation: item.citation })}`;
		const lines = [heading];
		const files = item.kind === "summary" ? item.summary.files : item.files;
		const scope: ContextScope = item.kind === "summary" ? item.summary : item.citation;
		if (files.length > 0) lines.push("Files:", ...fileLines(files, scope));
		const safePayload =
			item.kind === "summary"
				? await sanitizeLcmArtifactUris(item.summary.redactedText, options.artifactExists)
				: "available" in item && item.available && item.redactedText !== undefined
					? await sanitizeLcmArtifactUris(item.redactedText, options.artifactExists)
					: "[authoritative journal source unavailable]";
		const normalizedPayload = replaceTabs(safePayload).trim();
		const payload = hardBound(boundedDisplayText(safePayload, LCM_EXPAND_ITEM_MAX_CHARS), LCM_EXPAND_ITEM_MAX_CHARS);
		lines.push(payload);
		const block = `\n\n${lines.join("\n")}`;
		const candidateCount = renderedCount + 1;
		const candidateClipped: boolean = clipped || payload !== normalizedPayload;
		const candidateNextOffset =
			candidateCount < expansion.items.length ? expansion.offset + candidateCount : expansion.nextOffset;
		const candidateBounded = expansion.truncated || candidateClipped || candidateCount < expansion.items.length;
		const candidateChars = renderedChars + block.length;
		if (
			renderHeader(candidateCount).length +
				candidateChars +
				renderFooter(candidateNextOffset, candidateBounded).length >
			maxChars
		) {
			break;
		}
		renderedBlocks.push(block);
		renderedChars = candidateChars;
		renderedCount = candidateCount;
		clipped = candidateClipped;
	}

	const omitted = renderedCount < expansion.items.length;
	const nextOffset = omitted ? expansion.offset + renderedCount : expansion.nextOffset;
	const bounded = expansion.truncated || clipped || omitted;
	return {
		text: `${renderHeader(renderedCount)}${renderedBlocks.join("")}${renderFooter(nextOffset, bounded)}`,
		renderedItems: renderedCount,
		...(nextOffset === undefined ? {} : { nextOffset }),
	};
}

export async function renderLcmExpansion(
	expansion: LcmResolvedExpansion,
	maxTokens: number,
	options: { artifactExists?: ArtifactExists } = {},
): Promise<string> {
	return (await renderLcmExpansionPage(expansion, maxTokens, options)).text;
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

	const descriptions = await Promise.all(
		selected.map(({ citation }) => runtime.lcmDescribe({ kind: "source", citation })),
	);
	const slices: RecallSlice[] = [];
	let remaining = LCM_RECALL_MAX_SOURCE_CHARS;
	for (let index = 0; index < selected.length && remaining > 0; index++) {
		const wrapped = descriptions[index];
		const description = wrapped?.kind === "source" ? wrapped.value : null;
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
	const hits = await runtime.lcmSearch(normalizedQuery, { limit: LCM_RECALL_MAX_HITS });
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
				`[${index + 1}] ${renderLcmHandle({ kind: "source", citation: slice.citation })}${slice.degraded ? " [description unavailable; search excerpt used]" : ""}`,
		)
		.join("\n");
	return { text: `${answer}\n\nSources:\n${legend}`, citations };
}

/** Search one catalog-authorized project store without exposing its path or SQLite handle to a tool. */
export async function searchKnownLcmProject(
	projectId: string,
	query: string,
	options: { limit: number; offset?: number },
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
		return context.searchProject({
			projectId: project.projectId,
			query,
			limit: options.limit,
			offset: options.offset,
		});
	} finally {
		context.close();
	}
}
