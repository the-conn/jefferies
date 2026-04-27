## AGENTS.md

This document outlines the mandatory coding standards and workflow expectations for all contributors and automated agents.

### **Code Style & Philosophy**
* **Self-Documenting Code**: Inline comments are prohibited. Use descriptive naming and clear logical flow to explain intent.
* **Functional Decomposition**: Large functions must be split into smaller, focused helper functions.
* **Encapsulation**: Do not mark items `pub` unless they are explicitly required by an external module. Adhere to the principle of least privilege.
* **Standard Characters**: No emojis, non-standard Unicode, or decorative ASCII art.

### **Architecture & Execution**
* **Asynchronous Design**: All logic must be non-blocking. Utilize `async/await` and structured logging (`tracing`) to maintain visibility into the **Axum** execution flow.
* **Error Handling**: `unwrap()` and `expect()` are strictly prohibited. All errors must be handled gracefully through established unified error enum patterns.
* **Resource Efficiency**: Prioritize streaming and zero-copy operations for IO, network, and S3 tasks to minimize memory overhead.

### **Documentation & Synchronization**
* **Living Documentation**: Agents are responsible for ensuring that documentation remains an accurate reflection of the codebase.
* **README.md**: Must be updated if changes affect the configuration schema (environment variables), project lifecycle, or deployment strategy.
* **docs/architecture.md**: Must be updated if the relationship between modules, the data flow (e.g., S3/NooBaa integration), or the external interaction model changes.
* **Context**: Documentation should focus on high-level architecture and "the why" rather than duplicating implementation details found in the code.

### **Testing & Quality**
* **Strategic Testing**: Tests are required for new features and integration points. Focus on "happy paths" and common failure modes.
* **Exemptions**: Purely structural changes (e.g., refactoring file locations) do not require new tests unless logic is altered.

### **Development Workflow**
A contribution is only considered complete if it passes the following `make` targets in order:

1.  **`make fmt`**: Code must be formatted using the project's nightly standard.
2.  **`make lint`**: `clippy` must return zero warnings or errors (`-D warnings`).
3.  **`make test`**: All unit and integration tests must pass.

---

Refer to [architecture.md](./docs/architecture.md) for a high-level description of the **The Conn** framework.
