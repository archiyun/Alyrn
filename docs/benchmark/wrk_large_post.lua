-- Large POST keep-alive workload for wrk.
-- REQUEST_BODY: request body bytes (default 65536).

local body_size = tonumber(os.getenv("REQUEST_BODY") or "65536")
local body = string.rep("x", body_size)

wrk.method = "POST"
wrk.body = body
wrk.headers["Content-Type"] = "application/octet-stream"
wrk.headers["Content-Length"] = tostring(body_size)
