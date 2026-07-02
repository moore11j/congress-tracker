export class ServerTimeoutError extends Error {
  constructor(label: string, timeoutMs: number) {
    super(`${label} timed out after ${timeoutMs}ms`);
    this.name = "ServerTimeoutError";
  }
}

export function withServerTimeout<T>(request: Promise<T>, label: string, timeoutMs = 4500): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new ServerTimeoutError(label, timeoutMs)), timeoutMs);
  });
  return Promise.race([request, timeout]).finally(() => {
    if (timer) clearTimeout(timer);
  });
}
