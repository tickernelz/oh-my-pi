import { describe, expect, it } from "bun:test";
import * as path from "node:path";
import { pathIsWithin } from "../src/dirs";
import { stripWindowsExtendedLengthPathPrefix } from "../src/path";

describe("stripWindowsExtendedLengthPathPrefix", () => {
	it("removes drive and UNC extended-length prefixes on Windows", () => {
		expect(stripWindowsExtendedLengthPathPrefix("\\\\?\\C:\\Users\\Shi Xin\\omp.exe", "win32")).toBe(
			"C:\\Users\\Shi Xin\\omp.exe",
		);
		expect(stripWindowsExtendedLengthPathPrefix("\\\\?\\UNC\\server\\share\\omp.exe", "win32")).toBe(
			"\\\\server\\share\\omp.exe",
		);
	});

	it("leaves non-Windows paths unchanged", () => {
		const path = "\\\\?\\C:\\Users\\Shi Xin\\omp.exe";
		expect(stripWindowsExtendedLengthPathPrefix(path, "linux")).toBe(path);
	});
});

describe("pathIsWithin", () => {
	it("accepts dot-prefixed child segments while rejecting a parent traversal", () => {
		const root = path.join(process.cwd(), "project");
		expect(pathIsWithin(root, path.join(root, "..cache", "report.bin"))).toBe(true);
		expect(pathIsWithin(root, path.resolve(root, "..", "outside", "report.bin"))).toBe(false);
	});
});
