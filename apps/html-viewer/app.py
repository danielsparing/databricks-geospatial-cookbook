import requests
from databricks.sdk import WorkspaceClient
from flask import Flask, Response, redirect, request

w = WorkspaceClient()

app = Flask(__name__)


@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        path = request.form.get("path", "").lstrip("/")
        return redirect(f"/{path}")

    return """
        <form method="post">
            <label>Enter API path:</label>
            <input type="text" name="path" placeholder="/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/{FILE_PATH}" required>
            <input type="submit" value="Fetch HTML">
        </form>
    """


@app.route("/<path:path>")
def proxy_html(path):
    headers = request.headers
    user_token = headers.get("x-forwarded-access-token")

    api_url = f"{w.config.host}/api/2.0/fs/files/{path}"
    headers = {"Authorization": f"Bearer {user_token}"}
    resp = requests.get(api_url, headers=headers)

    if resp.status_code == 200:
        return Response(resp.text, mimetype="text/html")
    else:
        return Response(
            f"Error fetching {api_url}: {resp.status_code}", status=resp.status_code
        )


if __name__ == "__main__":
    app.run(debug=True)
