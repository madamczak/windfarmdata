# Prompt Logging Behavior

## Description

Every user prompt in this project must be saved directly into `prompts.md` in the project root directory — **without using any external scripts, tools, or code execution**.

## Rules

1. **Every prompt gets saved.** Each message the user sends must be appended to `prompts.md` as a new entry.
2. **No scripts.** Entries are written by directly editing the `prompts.md` file using file-editing capabilities, not by running Python scripts or terminal commands.
3. **Sequential numbering.** Each entry is numbered incrementally (Entry 1, Entry 2, Entry 3, …), continuing from the last existing entry in the file.
4. **Date stamp.** Each entry heading includes the current date in `YYYY-MM-DD` format.
5. **Format.** Each entry follows this exact structure:

```
## Entry N - YYYY-MM-DD

> <user prompt text>
```

6. **File header.** If `prompts.md` does not exist, create it with `# Prompt Log` as the first line before adding entries.

## Example

```markdown
# Prompt Log

## Entry 1 - 2026-04-03

> I wonder if you can save a prompt to a file in a project directory.

## Entry 2 - 2026-04-03

> ok let's check it - so can you save this prompt in prompts.md?
```

## Purpose

This behavior ensures a running log of all user prompts is maintained in the project directory for reference, auditing, and context continuity across sessions.

