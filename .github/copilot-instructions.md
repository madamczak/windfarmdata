# GitHub Copilot Custom Instructions

## Language & Framework Preferences

- **Backend:** Always use **Python** for any backend code, APIs, scripts, or server-side logic.
- **Frontend:** Always use **Vue.js** for any frontend code, UI components, or client-side logic.

## Script Execution

- **Never run code directly in the terminal.**
- **Never use `python -c "..."` or any inline code execution** — this is strictly forbidden.
- Every piece of executable code must exist as its own file inside the `scripts/` directory (e.g., `scripts/migrate.py`, `scripts/seed_db.sh`, `scripts/build.ps1`).
- Scripts should be clearly named and self-contained so they can be run manually by the user later.

## Prompt Logging

- Every user prompt must be saved directly into `promptlogger/prompts.md` by editing the file — **no scripts or terminal commands**.
- Each entry is appended in the following format:

```
## Entry N - YYYY-MM-DD HH:MM

> <user prompt text>
```

- Entry numbers increment sequentially from the last existing entry in the file.
- If `promptlogger/prompts.md` does not exist, create it with `# Prompt Log` as the first line.

## General Preferences

- Prefer clean, readable, well-commented code.
- Follow PEP 8 for Python code.
- Follow Vue 3 Composition API conventions for frontend code.
- Keep backend and frontend code clearly separated in the project structure.
- **Do not create README files** unless explicitly asked by the user.

