ARG BUN_VERSION=1.3.13
FROM oven/bun:${BUN_VERSION}

WORKDIR /workspace

COPY . .

RUN bun install
 

ENV CI=1
