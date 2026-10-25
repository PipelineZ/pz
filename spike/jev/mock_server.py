import http.server, sys, json

# SPIKE: a fake TypeSafe endpoint. A row is "angry" when its text has an angry word; every
# request is logged to stderr so the spike can count what reaches the API.
ANGRY = ("sue", "refund", "worst", "angry")


def judge(row):
    text = json.dumps(row).lower()
    return 0.93 if any(w in text for w in ANGRY) else 0.04


class H(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        n = int(self.headers.get('content-length', 0))
        req = json.loads(self.rfile.read(n))
        rows = req["state"]["rows"]
        answers = {}
        for key, q in req["questions"].items():
            p = judge(rows[int(key[1:])])
            answers[key] = {"noul": p}
        sys.stderr.write(f"REQUEST rows={len(rows)} condition={req['state'].get('condition')!r}\n")
        sys.stderr.flush()
        out = json.dumps({"answers": answers}).encode()
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(out)))
        self.end_headers()
        self.wfile.write(out)

    def log_message(self, *a):
        pass


http.server.HTTPServer(('127.0.0.1', 18765), H).serve_forever()
