/**
 * Eager public facade for the auth-broker wire schemas.
 *
 * Direct consumers of this module receive the shared real ArkType objects.
 * Internal client/server startup imports the lazy resource instead.
 */
import { type } from "@oh-my-pi/omptype";

import { getAuthBrokerWireSchemas } from "./wire-schema-resource";

const wireSchemas = getAuthBrokerWireSchemas();

export const {
	oauthCredentialSchema,
	remoteOauthCredentialSchema,
	apiKeyCredentialSchema,
	writableAuthCredentialSchema,
	snapshotCredentialSchema,
	credentialSnapshotEntrySchema,
	credentialBlockSnapshotSchema,
	snapshotEntrySchema,
	refresherScheduleSchema,
	snapshotResponseSchema,
	snapshotStreamSnapshotEventSchema,
	snapshotStreamEntryEventSchema,
	snapshotStreamRemovedEventSchema,
	snapshotStreamEventSchema,
	healthzResponseSchema,
	usageResponseSchema,
	usageHistoryResponseSchema,
	clientUsageReportRequestSchema,
	clientUsageReportResponseSchema,
	clientUsageSummaryResponseSchema,
	credentialRefreshResponseSchema,
	credentialDisableRequestSchema,
	credentialDisableResponseSchema,
	disabledCredentialSummarySchema,
	disabledCredentialsResponseSchema,
	credentialBlockRequestSchema,
	credentialBlockResponseSchema,
	credentialBlocksDeleteResponseSchema,
	usageStaleResponseSchema,
	credentialUploadRequestSchema,
	credentialUploadResponseSchema,
} = wireSchemas;

export const credentialBlockDeleteRequestSchema = type({
	"+": "reject",
	providerKey: type("string").atLeastLength(1),
	blockScope: "string",
	blockedUntilMs: "number",
	updatedAtMs: "number",
});

export const credentialBlockDeleteResponseSchema = type({
	"+": "reject",
	deleted: "boolean",
});
