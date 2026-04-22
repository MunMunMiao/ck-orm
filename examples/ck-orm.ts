// Repo-local shim so examples can import from "./ck-orm" and mirror package-root
// usage without depending on Bun package self-resolution inside this workspace.
export * from "../src/public_api";
