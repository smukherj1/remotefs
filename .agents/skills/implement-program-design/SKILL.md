---
name: implement-program-design
description: Use only when asked to implement a program design. Do not use to write, edit, review, or discuss a program-design document, or for ordinary code changes without an explicit program design implementation request.
---

# Implement a Program Design

Implement the supplied program-design document one component at a time. Use
simple language in messages and handoffs.

## Prepare the work

- Read the full program-design document and the project's technical design.
- List every changing component and its direct dependencies. Make a dependency
  order: implement dependencies before the components that use them.
- Keep unrelated existing changes. Do not start implementation until the order
  is clear.

## Implement each component

For each component, working from the leaves of the dependency tree toward its
users:

1. Create one code-implementor subagent. Give it the complete program-design
   document and technical design as content, identify the current component,
   and tell it which completed dependency components it may use.
2. Tell the implementor to implement only that component and its directly
   required tests, run the relevant component tests, and report the changed
   files, test command and result, and any blocker.
3. After the code-implementor finishes, create a separate code-reviewer subagent.
   Give it the complete technical design and program design documents as content
   and ask it to review the current component.
4. Send the review findings to the same code-implementor subagent. Ask it to fix
   every valid finding, rerun the relevant tests, and report what it changed
   and the final test result. If the review has no findings, record that.
5. Inspect the final result. Do not move to a component that depends on this
   one until its required tests pass and its review findings are addressed.

Do not combine independent components in one code-implementor task. A small shared
component that several others need must be completed, tested, and reviewed
before its users begin.

## Finish

After every component is complete, run the program design's full required test
set. Review the final diff for missed integration work. Report the dependency
order, implemented components, review fixes, test results, and any remaining
blocker.
