#!/usr/bin/env bun

export const DOWNSTREAM_REPOSITORY = "tickernelz/oh-my-pi";

/** Official npm, Homebrew, mise, and macOS publication is never valid from this fork. */
export function refuseOfficialChannelPublishing(operation: string): void {
	throw new Error(
		`${operation} is disabled in ${DOWNSTREAM_REPOSITORY}; downstream releases may publish only the authenticated Linux x64 GitHub prerelease.`,
	);
}

if (import.meta.main) {
	refuseOfficialChannelPublishing(process.argv.slice(2).join(" ") || "Official-channel publication");
}
