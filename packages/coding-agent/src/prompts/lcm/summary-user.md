Compress the following ordered history into at most {{maxOutputTokens}} tokens.

{{#if aggressive}}Compression strategy: bullet_points
Use terse bullet points. Keep only essential facts, current state, constraints, errors, and next actions.
{{else}}Compression strategy: preserve_details
Preserve concrete details and causal context while removing repetition and conversational filler.
{{/if}}

<lcm-history>
{{#list inputs join="\n\n"}}[{{kind}} {{id}}]
{{text}}{{/list}}
</lcm-history>
