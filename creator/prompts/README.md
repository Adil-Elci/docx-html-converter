# Prompt Registry

Versioned prompts for the creator pipeline. Each prompt lives in its own folder with one file per version, named `vN.md`.

## Layout

```
creator/prompts/
  contract_generator/
    v1.md
    v2.md
  section_writer/
    v1.md
  voice_pass/
    v1.md
```

## Conventions

- Markdown body holds the system prompt. The user prompt is built in code from the contract payload.
- Front matter (optional) holds metadata: `model`, `temperature`, `max_tokens`.
- A prompt is invoked by name + version: `prompt_registry.load("contract_generator", "v2")`.
- "Latest" resolves to the highest `vN` in the folder.
- Never overwrite a published version. Bump the version when changing semantics.
- Track which version produced each article so eval reports can attribute regressions.
