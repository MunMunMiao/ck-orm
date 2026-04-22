// Repo-local shim so E2E files exercise the same package-root API surface that
// published consumers see, while still running from the repository checkout.
export * from "../src/public_api";
