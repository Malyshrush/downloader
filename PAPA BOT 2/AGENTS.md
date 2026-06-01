# Repository Instructions

- Before finishing any change that adds, removes, renames, or materially changes PAPA BOT functionality, check `FUNCTIONALITY.md` and update it in the same change.
- Keep `FUNCTIONALITY.md` aligned with `src/handler.js`, `src/modules/*`, `adminPanelHTML.js`, and the relevant tests.
- Project status: PAPA BOT is being prepared for a public/mass launch, but current validation is still pre-release testing performed by the owner on their own/admin test accounts. Build decisions should keep future mass usage in mind, while avoiding unnecessary migrations for real external users until the user explicitly says the project has launched/released for everyone.
- UI rule: when several buttons belong to one action group, make them the same visual size so the group reads as one control set.
- Deployment rule: after fixing and verifying a PAPA BOT task, deploy the working version before the final response unless the user explicitly asks not to deploy, deployment credentials are unavailable, or verification/deployment is blocked; if blocked, say exactly why.
