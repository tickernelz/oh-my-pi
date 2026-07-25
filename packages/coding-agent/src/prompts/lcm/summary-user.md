Compress the following ordered history into at most {{maxOutputTokens}} tokens.

<lcm-history>
{{#list inputs join="\n\n"}}[{{kind}} {{id}}]
{{text}}{{/list}}
</lcm-history>
