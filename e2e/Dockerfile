ARG BUN_VERSION=latest
FROM oven/bun:${BUN_VERSION}

WORKDIR /workspace

COPY . .

RUN bun install
 

ENV CI=1
