# Week 4 Progress Report

- Week 4: 11/3-11/9

## Progress in this week

- Updated and organized milestone (`doc/milestone.md`).
- Established the initial code scaffolding for the project.
- Documented the Project Structure (`doc/project-structure.md`) and Coding Standard (`doc/coding-standard.md`).
- Determined presentation roles and contents.
- Created [new revision](doc/design-doc-draft-3.pdf) of design documentation.

## Goal of the next week

- Finalize the design document.
- Integrate initial codes from all members and refine the code-level architecture
- Develop a prototype that demonstrates the core sorting job in an ideal condition.

## Goal of the next week for each individual member

- Junseong:
    - Implement the core scheduler component responsible for managing and dispatching jobs.
- Jaehwan:
    - Develop the worker component that executes the jobs.
    - Implement atomic job handling to ensure data integritiy.  
    (e.g., writing to a temporary file followed by an atomic rename operation).
- Dana:
    - Implement the file server that will store and manage the job files.
