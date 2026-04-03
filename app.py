import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

from address_matcher import create_matcher


DB_PATH = os.path.join(os.path.dirname(__file__), "dist", "data.sqlite")
PORT = int(os.environ.get("PORT", "3000"))

matcher = create_matcher(DB_PATH)


def send_json(handler, status, payload):
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(data)))
    handler.end_headers()
    handler.wfile.write(data)


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            return send_json(self, 200, {"status": "ok"})
        if parsed.path == "/match":
            params = parse_qs(parsed.query)
            input_text = (params.get("q") or params.get("address") or [""])[0]
            deep = (params.get("deep") or [""])[0] in ("1", "true", "True")
            topn = int((params.get("topn") or ["1"])[0] or 1)
            debug = (params.get("debug") or [""])[0] in ("1", "true", "True")
            if topn > 1:
                result = matcher.match_topn(input_text, topn=topn, deep=deep, debug=debug)
            else:
                result = matcher.match_address(input_text, deep=deep, debug=debug)
            return send_json(self, 200, result)
        return send_json(self, 404, {"error": "Not found"})

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path != "/match":
            return send_json(self, 404, {"error": "Not found"})
        length = int(self.headers.get("Content-Length", "0"))
        if length > 2 * 1024 * 1024:
            return send_json(self, 413, {"error": "Payload too large"})
        body = self.rfile.read(length) if length else b""
        try:
            payload = json.loads(body.decode("utf-8") or "{}")
        except json.JSONDecodeError:
            return send_json(self, 400, {"error": "Invalid JSON body"})
        input_text = payload.get("address") or payload.get("q") or ""
        deep = payload.get("deep") in (True, 1, "1")
        topn = int(payload.get("topn") or 1)
        debug = payload.get("debug") in (True, 1, "1")
        if topn > 1:
            result = matcher.match_topn(input_text, topn=topn, deep=deep, debug=debug)
        else:
            result = matcher.match_address(input_text, deep=deep, debug=debug)
        return send_json(self, 200, result)

    def log_message(self, format, *args):
        return


def main():
    server = HTTPServer(("", PORT), Handler)
    print(f"Address match service listening on {PORT}")
    server.serve_forever()


if __name__ == "__main__":
    main()
