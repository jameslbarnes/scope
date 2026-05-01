import { getDaydreamAPIKey, getDaydreamUserId } from "./auth";

/**
 * Connect to the cloud relay. Reads credentials from local auth storage
 * internally so callers don't need to pass them around.
 *
 * Returns the fetch Response so callers can inspect status if needed,
 * or `null` if no user is signed in.
 */
interface ConnectToCloudOptions {
  remoteUrl?: string;
}

export async function connectToCloud(
  options: ConnectToCloudOptions = {}
): Promise<Response | null> {
  const userId = getDaydreamUserId();
  const apiKey = getDaydreamAPIKey();
  const remoteUrl = options.remoteUrl?.trim();
  if (!userId && !remoteUrl) return null;

  return fetch("/api/v1/cloud/connect", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      user_id: userId,
      api_key: apiKey,
      ...(remoteUrl ? { remote_url: remoteUrl } : {}),
    }),
  });
}
