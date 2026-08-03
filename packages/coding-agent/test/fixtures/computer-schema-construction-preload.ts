import { spyOn } from "bun:test";
import * as arktype from "arktype";

declare global {
	var __computerSchemaConstructionCount: number;
}

globalThis.__computerSchemaConstructionCount = 0;
const originalType = arktype.type;
const countedType = new Proxy(originalType, {
	apply(target, thisArg, args) {
		const definition = args[0];
		if (
			definition !== null &&
			typeof definition === "object" &&
			Object.hasOwn(definition, "code") &&
			Object.hasOwn(definition, "read_only?")
		) {
			globalThis.__computerSchemaConstructionCount += 1;
		}
		return Reflect.apply(target, thisArg, args);
	},
});
const typeSpy = spyOn(arktype, "type").mockImplementation(countedType);
Object.assign(typeSpy, originalType);
