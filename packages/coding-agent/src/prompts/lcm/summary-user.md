Compress the following ordered history into at most {{maxOutputTokens}} tokens.

Compression strategy: {{strategy}}
{{strategyInstructions}}

<lcm-history>
{{#list inputs join="\n\n"}}[{{kind}} {{id}}]
{{text}}{{/list}}
</lcm-history>
