import { createServer } from "node:http";
import { connect } from "node:net";

const proxy = createServer((_request, response) => {
  response.writeHead(405).end();
});

proxy.on("connect", (request, clientSocket, head) => {
  if (!request.url) {
    clientSocket.destroy();
    return;
  }

  const target = new URL(`http://${request.url}`);
  const upstreamSocket = connect(Number(target.port || 443), target.hostname, () => {
    clientSocket.write("HTTP/1.1 200 Connection Established\r\n\r\n");
    if (head.length > 0) upstreamSocket.write(head);
    upstreamSocket.pipe(clientSocket);
    clientSocket.pipe(upstreamSocket);
  });

  upstreamSocket.on("error", () => clientSocket.destroy());
  clientSocket.on("error", () => upstreamSocket.destroy());
});

proxy.listen(3128, "0.0.0.0", async () => {
  await Bun.write("/tmp/ck-orm-proxy-ready", "ready");
  console.log("CONNECT proxy http://0.0.0.0:3128 is ready");
});
