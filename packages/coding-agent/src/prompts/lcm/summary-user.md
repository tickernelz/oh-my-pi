Compress the following ordered history into at most {{maxOutputTokens}} tokens.

{{#if aggressive}}Compression strategy: bullet_points
Use terse bullet points. Keep only essential facts, current state, constraints, errors, and next actions.
{{else}}Compression strategy: preserve_details
Preserve concrete details and causal context while removing repetition and conversational filler.
{{/if}}

{{#if condense}}The inputs below are already summaries. Consolidate them: merge overlapping facts, keep decisions, current state, constraints, unresolved errors, and next actions, and do not re-expand detail that the child summaries already condensed.
{{/if}}

<lcm-history>
{{#list inputs join="\n\n"}}[{{kind}} {{id}}]
{{text}}{{/list}}
</lcm-history>
